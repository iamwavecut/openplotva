//! Single-exit dialog turn engine.
//!
//! `execute_dialog_turn` returns a `TurnResolution` on every branch — the
//! compiler forbids falling off — and `finalize_turn` is the only code that
//! writes job status, appends the `turn_outcome` event, records the ledger
//! row, and fires the terminal user signal. Event-append failures never abort
//! the status write (S13).

use std::collections::BTreeMap;
use std::time::Instant;

use openplotva_dialog::turn::{
    ANTI_LOOP_HINT, EmptyReason, QueuedSideEffect, ReplyMaterial, classify_reply_material,
};
use openplotva_llm::ChatProvider;
use openplotva_taskman::{DialogJobParams, TaskQueueJobEvent};
use serde_json::{Value, json};
use time::{Duration as TimeDuration, OffsetDateTime};

use super::budget::{TURN_DEADLINE, TURN_STARTED_STAGE, TurnBudget, turn_started_at};
use super::ledger::{DialogTurnObserver, DialogTurnOutcomeRecord};
use super::outcome::{JobDisposition, TurnOutcome, TurnResolution, UserSignalPlan};
use super::signal::{SignalTarget, TERMINAL_USER_SIGNAL_STAGE, TurnSignalPolicy, UserSignalResult};
use crate::dialog_jobs::{
    DialogInputMaterializer, DialogJobEffects, DialogJobWorkItem, DialogJobWorkerQueue,
    DialogJobWorkerReport, DialogToolCallHistoryStore, PROVIDER_EMPTY_RETRY_CODES,
    PROVIDER_ERROR_RETRY_CODES, RetryableDialogProviderFailure, SANITIZED_EMPTY_RETRY_CODES,
    UNDELIVERABLE_RETRY_CODES, dialog_job_answer, dialog_job_has_payload,
    handle_retryable_dialog_provider_error, next_dialog_llm_job_attempt, persist_dialog_tool_calls,
    prepare_dialog_chat_response, record_dialog_fallback_event,
    should_suppress_duplicate_bot_reply, validate_dialog_answer_deliverable,
};

/// Job event stage carrying the recorded outcome of every handled turn.
pub const TURN_OUTCOME_STAGE: &str = "turn_outcome";

/// Job event stage appended per in-process duplicate-answer regeneration.
pub const DIALOG_TURN_REGENERATE_STAGE: &str = "dialog_turn_regenerate";

/// A turn never starts generation it cannot plausibly finish inside the budget.
const MIN_GENERATION_BUDGET: TimeDuration = TimeDuration::seconds(15);

/// A duplicate answer is regenerated only with this much budget left.
const MIN_REGENERATION_BUDGET: TimeDuration = TimeDuration::seconds(10);

/// Immutable inputs of one dialog turn attempt.
pub(crate) struct TurnContext<'a> {
    pub item: &'a DialogJobWorkItem,
    pub params: &'a DialogJobParams,
    pub queue_name: &'static str,
    pub max_llm_job_attempts: i32,
    pub max_queue_age: TimeDuration,
    /// In-process duplicate-answer regenerations allowed for this turn.
    pub max_regenerations: i32,
    pub budget: TurnBudget,
    pub now: OffsetDateTime,
    pub routing_events: Option<&'a crate::runtime_routing::RoutingEventReporter>,
}

pub(crate) async fn execute_dialog_turn<Queue, Provider, Effects, Materializer, ToolHistory>(
    ctx: TurnContext<'_>,
    queue: &Queue,
    provider: &Provider,
    effects: &Effects,
    materializer: &Materializer,
    tool_history: &ToolHistory,
    report: &mut DialogJobWorkerReport,
) -> TurnResolution
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
    Materializer: DialogInputMaterializer + Sync + ?Sized,
    ToolHistory: DialogToolCallHistoryStore + Sync + ?Sized,
{
    if turn_started_at(&ctx.item.events).is_none()
        && ctx.now - ctx.item.job.created > ctx.max_queue_age
    {
        report.skipped_queue_backlog = true;
        let error = format!(
            "dialog job expired in queue backlog after {}s without processing",
            (ctx.now - ctx.item.job.created).whole_seconds()
        );
        return TurnResolution {
            outcome: TurnOutcome::SkippedQueueBacklog,
            disposition: JobDisposition::Fail(error),
        };
    }

    if ctx.budget.remaining(ctx.now) < MIN_GENERATION_BUDGET {
        let error = format!(
            "dialog turn budget exhausted after {}s",
            ctx.budget.elapsed(ctx.now).whole_seconds()
        );
        return TurnResolution {
            outcome: TurnOutcome::TerminalFailed {
                reason: "budget_exhausted",
                error: error.clone(),
                user_signal: UserSignalPlan::React,
            },
            disposition: JobDisposition::Fail(error),
        };
    }

    if !dialog_job_has_payload(ctx.params) {
        report.skipped_empty_payload = true;
        return TurnResolution {
            outcome: TurnOutcome::SkippedEmptyPayload,
            disposition: JobDisposition::Complete,
        };
    }

    let base_input = materializer
        .materialize_dialog_input(ctx.params, ctx.now)
        .await;
    let duplicate_guard_history = base_input.history.clone();
    // tokio Instant so paused-clock tests can advance generation time; in
    // production it is the monotonic clock.
    let processing_started = tokio::time::Instant::now();
    let mut regenerations: i32 = 0;

    loop {
        let mut input = base_input.clone();
        if regenerations > 0 {
            input.reference_context.push(ANTI_LOOP_HINT.to_owned());
        }
        let round_now =
            ctx.now + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default();
        let provider_deadline = Instant::now()
            + std::time::Duration::try_from(ctx.budget.remaining(round_now)).unwrap_or_default();
        let result = TURN_DEADLINE
            .scope(Some(provider_deadline), provider.run_dialog(input))
            .await;
        // Generation is the only long-running step of a tick: fold its real wall
        // time into `now` so budget comparisons stay honest across rounds.
        let failure_now =
            ctx.now + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default();
        let output = match result {
            Ok(output) => output,
            Err(error) if openplotva_llm::is_content_blocked_error(error.as_ref()) => {
                report.content_blocked = true;
                return TurnResolution {
                    outcome: TurnOutcome::NoReplyIntentional {
                        reason: "content_blocked",
                    },
                    disposition: JobDisposition::Complete,
                };
            }
            Err(error) => {
                let retryable_reason = openplotva_llm::retry::retryable_reason(error.as_ref());
                let retry_provider =
                    openplotva_llm::retry::provider_name(error.as_ref()).to_owned();
                let error = error.to_string();
                report.provider_error = Some(error.clone());
                if let Some(reason) = retryable_reason {
                    return handle_retryable_dialog_provider_error(
                        queue,
                        ctx.item,
                        ctx.params,
                        ctx.routing_events,
                        RetryableDialogProviderFailure {
                            queue_name: ctx.queue_name,
                            provider_name: &retry_provider,
                            reason,
                            codes: PROVIDER_ERROR_RETRY_CODES,
                            error: &error,
                            max_attempts: ctx.max_llm_job_attempts.max(1),
                            now: failure_now,
                            budget_deadline: ctx.budget.deadline(),
                        },
                        report,
                    )
                    .await;
                }
                return TurnResolution {
                    outcome: TurnOutcome::TerminalFailed {
                        reason: "provider_error",
                        error: error.clone(),
                        user_signal: UserSignalPlan::React,
                    },
                    disposition: JobDisposition::Fail(error),
                };
            }
        };

        record_dialog_fallback_event(queue, ctx.item.id, &output, ctx.now, report).await;

        match persist_dialog_tool_calls(tool_history, ctx.params, &output.tool_calls).await {
            Ok(persisted) => report.persisted_tool_call_history = persisted,
            Err(error) => {
                let error = error.to_string();
                tracing::debug!(
                    %error,
                    chat_id = ctx.params.chat_id,
                    message_id = ctx.params.message_id,
                    "failed to persist dialog tool-call history"
                );
                report.tool_call_history_error = Some(error);
            }
        }

        let raw_answer = dialog_job_answer(&output);
        let sanitized_answer = prepare_dialog_chat_response(&raw_answer);
        match classify_reply_material(&output, &sanitized_answer, &raw_answer) {
            ReplyMaterial::Empty {
                reason: EmptyReason::SanitizedEmpty,
            } => {
                let error = "dialog answer became empty after sanitization".to_owned();
                report.empty_answer_error = Some(error.clone());
                return handle_retryable_dialog_provider_error(
                    queue,
                    ctx.item,
                    ctx.params,
                    ctx.routing_events,
                    RetryableDialogProviderFailure {
                        queue_name: ctx.queue_name,
                        provider_name: output.provider.as_str(),
                        reason: openplotva_llm::retry::FailureReason::ProviderProtocolError,
                        codes: SANITIZED_EMPTY_RETRY_CODES,
                        error: &error,
                        max_attempts: ctx.max_llm_job_attempts.max(1),
                        now: failure_now,
                        budget_deadline: ctx.budget.deadline(),
                    },
                    report,
                )
                .await;
            }
            ReplyMaterial::Empty {
                reason: EmptyReason::ProviderEmpty,
            } => {
                report.answer_empty_all_sources = true;
                let error = "dialog provider returned no answer, response, or queued tool material"
                    .to_owned();
                report.empty_answer_error = Some(error.clone());
                return handle_retryable_dialog_provider_error(
                    queue,
                    ctx.item,
                    ctx.params,
                    ctx.routing_events,
                    RetryableDialogProviderFailure {
                        queue_name: ctx.queue_name,
                        provider_name: output.provider.as_str(),
                        reason: openplotva_llm::retry::FailureReason::ProviderProtocolError,
                        codes: PROVIDER_EMPTY_RETRY_CODES,
                        error: &error,
                        max_attempts: ctx.max_llm_job_attempts.max(1),
                        now: failure_now,
                        budget_deadline: ctx.budget.deadline(),
                    },
                    report,
                )
                .await;
            }
            ReplyMaterial::IntentionalSilence { reason } => {
                return TurnResolution {
                    outcome: TurnOutcome::NoReplyIntentional {
                        reason: reason.as_str(),
                    },
                    disposition: JobDisposition::Complete,
                };
            }
            ReplyMaterial::Delegated { side_effects } => {
                return TurnResolution {
                    outcome: TurnOutcome::SideEffectDelegated {
                        tickets: side_effect_tickets(&side_effects),
                        kinds: side_effects
                            .iter()
                            .map(|side_effect| side_effect.kind.clone())
                            .collect(),
                    },
                    disposition: JobDisposition::Complete,
                };
            }
            ReplyMaterial::Text { text, side_effects } => {
                let (duplicate_message_id, duplicate) =
                    should_suppress_duplicate_bot_reply(&duplicate_guard_history, &text);
                if duplicate {
                    if regeneration_allowed(
                        regenerations,
                        ctx.max_regenerations,
                        &ctx.budget,
                        failure_now,
                    ) {
                        regenerations += 1;
                        report.regenerations = regenerations;
                        append_regeneration_event(
                            queue,
                            ctx.item.id,
                            regenerations,
                            duplicate_message_id,
                            failure_now,
                        )
                        .await;
                        tracing::info!(
                            chat_id = ctx.params.chat_id,
                            message_id = ctx.params.message_id,
                            duplicate_message_id,
                            regeneration = regenerations,
                            "dialog answer duplicated the last bot reply; regenerating with anti-loop hint"
                        );
                        continue;
                    }
                    report.suppressed_duplicate_message_id = Some(duplicate_message_id);
                    let error = format!(
                        "dialog answer duplicated bot message {duplicate_message_id} after {regenerations} regeneration(s)"
                    );
                    return TurnResolution {
                        outcome: TurnOutcome::TerminalFailed {
                            reason: "duplicate_exhausted",
                            error: error.clone(),
                            user_signal: UserSignalPlan::React,
                        },
                        disposition: JobDisposition::Fail(error),
                    };
                }
                if let Err(validation) = validate_dialog_answer_deliverable(&text) {
                    let error =
                        format!("dialog answer rejected by outbound validation: {validation}");
                    report.empty_answer_error = Some(error.clone());
                    return handle_retryable_dialog_provider_error(
                        queue,
                        ctx.item,
                        ctx.params,
                        ctx.routing_events,
                        RetryableDialogProviderFailure {
                            queue_name: ctx.queue_name,
                            provider_name: output.provider.as_str(),
                            reason: openplotva_llm::retry::FailureReason::ProviderProtocolError,
                            codes: UNDELIVERABLE_RETRY_CODES,
                            error: &error,
                            max_attempts: ctx.max_llm_job_attempts.max(1),
                            now: failure_now,
                            budget_deadline: ctx.budget.deadline(),
                        },
                        report,
                    )
                    .await;
                }
                return match effects.send_dialog_answer(ctx.params, &text).await {
                    Ok(()) => {
                        report.sent_answer = true;
                        TurnResolution {
                            outcome: TurnOutcome::Sent {
                                parts: 1,
                                side_effect_tickets: side_effect_tickets(&side_effects),
                            },
                            disposition: JobDisposition::Complete,
                        }
                    }
                    Err(error) => {
                        let error = error.to_string();
                        report.send_error = Some(error.clone());
                        TurnResolution {
                            outcome: TurnOutcome::TerminalFailed {
                                reason: "send_error",
                                error: error.clone(),
                                user_signal: UserSignalPlan::React,
                            },
                            disposition: JobDisposition::Fail(error),
                        }
                    }
                };
            }
        }
    }
}

/// Duplicate regeneration is bounded by its own cap and the shared budget.
fn regeneration_allowed(
    regenerations_done: i32,
    max_regenerations: i32,
    budget: &TurnBudget,
    now: OffsetDateTime,
) -> bool {
    regenerations_done < max_regenerations.max(0)
        && budget.remaining(now) >= MIN_REGENERATION_BUDGET
}

/// Regenerations record their own event stage and deliberately do not consume
/// `llm_job_retry` attempts. Append failure is non-fatal.
async fn append_regeneration_event<Queue>(
    queue: &Queue,
    job_id: i64,
    attempt: i32,
    duplicate_message_id: i32,
    at: OffsetDateTime,
) where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let mut data = BTreeMap::new();
    data.insert(
        "duplicate_message_id".to_owned(),
        duplicate_message_id.to_string(),
    );
    data.insert("reason".to_owned(), "dedup_regenerate".to_owned());
    let event = TaskQueueJobEvent {
        level: "info".to_owned(),
        stage: DIALOG_TURN_REGENERATE_STAGE.to_owned(),
        attempt,
        message: "dialog answer duplicated the last bot reply; regenerating".to_owned(),
        data,
        ..TaskQueueJobEvent::default()
    };
    if let Err(error) = queue.append_dialog_job_event(job_id, event, at).await {
        tracing::debug!(
            error = %error,
            job_id,
            "failed to append dialog regeneration event"
        );
    }
}

/// The single writer of job status, `turn_outcome` job event, ledger row, and
/// terminal user signal.
pub(crate) async fn finalize_turn<Queue>(
    queue: &Queue,
    item: &DialogJobWorkItem,
    resolution: TurnResolution,
    budget: &TurnBudget,
    now: OffsetDateTime,
    observer: Option<&DialogTurnObserver>,
    signals: TurnSignalPolicy<'_>,
    report: &mut DialogJobWorkerReport,
) where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    // (a) Durable budget anchor on first processing. Skipped for backlog drops:
    // those turns never started. Append failure must not block the status write.
    if turn_started_at(&item.events).is_none()
        && resolution.outcome != TurnOutcome::SkippedQueueBacklog
    {
        let event = TaskQueueJobEvent {
            level: "info".to_owned(),
            stage: TURN_STARTED_STAGE.to_owned(),
            message: "dialog turn processing started".to_owned(),
            ..TaskQueueJobEvent::default()
        };
        if let Err(error) = queue
            .append_dialog_job_event(item.id, event, budget.anchor)
            .await
        {
            report.status_error = Some(error.to_string());
        }
    }

    // (b) Terminal user signal, before the status write: a crash or failed
    // status write reruns the turn and can at worst re-react (setMessageReaction
    // is replace-idempotent); the marker appended after a delivered signal gates
    // only the non-idempotent text fallback, so it is sent at most once.
    let user_signal = match &resolution.outcome {
        TurnOutcome::TerminalFailed {
            user_signal: plan, ..
        } => Some(dispatch_terminal_user_signal(queue, item, *plan, &signals, now, report).await),
        _ => None,
    };

    // (c) Job status per disposition — status transition wins over event fidelity.
    match &resolution.disposition {
        JobDisposition::Complete => match queue.complete_dialog_job(item.id).await {
            Ok(()) => report.completed = true,
            Err(error) => report.status_error = Some(error.to_string()),
        },
        JobDisposition::Fail(error) => match queue.fail_dialog_job(item.id, error).await {
            Ok(()) => report.failed = true,
            Err(error) => report.status_error = Some(error.to_string()),
        },
        JobDisposition::Requeue(target_queue) => {
            match queue
                .requeue_retryable_dialog_job(item.id, target_queue)
                .await
            {
                Ok(()) => report.retry_requeued = true,
                Err(error) => report.status_error = Some(error.to_string()),
            }
        }
    }

    // (d) Per-job outcome event for the taskman drill-down view.
    let mut data = BTreeMap::new();
    data.insert(
        "outcome".to_owned(),
        resolution.outcome.ledger_outcome().to_owned(),
    );
    if let Some(reason) = resolution.outcome.ledger_reason() {
        data.insert("reason".to_owned(), reason.to_owned());
    }
    if let Some(user_signal) = &user_signal {
        data.insert("user_signal".to_owned(), user_signal.clone());
    }
    let event = TaskQueueJobEvent {
        level: "info".to_owned(),
        stage: TURN_OUTCOME_STAGE.to_owned(),
        message: "dialog turn resolved".to_owned(),
        data,
        ..TaskQueueJobEvent::default()
    };
    if let Err(error) = queue.append_dialog_job_event(item.id, event, now).await {
        tracing::debug!(
            error = %error,
            job_id = item.id,
            "failed to append dialog turn outcome event"
        );
    }

    // (e) Ledger row — exactly one per handled turn.
    if let Some(observer) = observer {
        observer.record(record_from_resolution(
            &resolution,
            item,
            budget,
            now,
            user_signal,
            report,
        ));
    }
}

/// Resolve and fire the user signal for one terminal failure, returning the
/// ledger `user_signal` value. "none" is recorded when no signal is wired.
async fn dispatch_terminal_user_signal<Queue>(
    queue: &Queue,
    item: &DialogJobWorkItem,
    plan: UserSignalPlan,
    signals: &TurnSignalPolicy<'_>,
    now: OffsetDateTime,
    report: &mut DialogJobWorkerReport,
) -> String
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let Some(signal) = signals.signal else {
        return "none".to_owned();
    };
    let result = match plan {
        UserSignalPlan::SkipTooOld => UserSignalResult::Skipped("skipped_too_old"),
        UserSignalPlan::SkipUnaddressable => UserSignalResult::Skipped("unaddressable"),
        UserSignalPlan::React => {
            let chat_id = report.chat_id.unwrap_or_default();
            let message_id = report.message_id.unwrap_or_default();
            if chat_id == 0 || message_id == 0 {
                UserSignalResult::Skipped("unaddressable")
            } else if now - item.job.created > signals.max_age {
                // Post-downtime backlogs must not produce reaction storms.
                UserSignalResult::Skipped("skipped_too_old")
            } else {
                // The reaction is attempted without checking the marker: it is
                // replace-idempotent. The marker gates only the text fallback.
                let already_signaled = item
                    .events
                    .iter()
                    .any(|event| event.stage == TERMINAL_USER_SIGNAL_STAGE);
                let result = signal
                    .signal_turn_failure(SignalTarget {
                        chat_id,
                        thread_id: report.thread_id,
                        message_id,
                        emoji: signals.reaction_emoji.to_owned(),
                        text_fallback_allowed: !already_signaled,
                    })
                    .await;
                if !already_signaled
                    && matches!(
                        result,
                        UserSignalResult::ReactionSent | UserSignalResult::TextFallbackSent
                    )
                {
                    append_terminal_signal_marker(queue, item.id, &result, signals, now).await;
                }
                result
            }
        }
    };
    if let UserSignalResult::Failed(error) = &result {
        report.user_signal_error = Some(error.clone());
        tracing::warn!(
            job_id = item.id,
            error = %error,
            "terminal user signal failed"
        );
    }
    result.ledger_value().to_owned()
}

/// Marker append failure is non-fatal: a rerun may then re-send the text
/// fallback, which beats going silent.
async fn append_terminal_signal_marker<Queue>(
    queue: &Queue,
    job_id: i64,
    result: &UserSignalResult,
    signals: &TurnSignalPolicy<'_>,
    now: OffsetDateTime,
) where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let mut data = BTreeMap::new();
    data.insert("result".to_owned(), result.ledger_value().to_owned());
    data.insert("emoji".to_owned(), signals.reaction_emoji.to_owned());
    let event = TaskQueueJobEvent {
        level: "info".to_owned(),
        stage: TERMINAL_USER_SIGNAL_STAGE.to_owned(),
        message: "terminal turn failure signalled to the user".to_owned(),
        data,
        ..TaskQueueJobEvent::default()
    };
    if let Err(error) = queue.append_dialog_job_event(job_id, event, now).await {
        tracing::warn!(
            error = %error,
            job_id,
            "failed to append terminal user signal marker; a rerun may re-send the text fallback"
        );
    }
}

fn side_effect_tickets(side_effects: &[QueuedSideEffect]) -> Vec<i64> {
    side_effects
        .iter()
        .filter_map(|side_effect| side_effect.ticket_job_id)
        .collect()
}

fn record_from_resolution(
    resolution: &TurnResolution,
    item: &DialogJobWorkItem,
    budget: &TurnBudget,
    now: OffsetDateTime,
    user_signal: Option<String>,
    report: &DialogJobWorkerReport,
) -> DialogTurnOutcomeRecord {
    let attempt = report
        .retry_attempt
        .unwrap_or_else(|| next_dialog_llm_job_attempt(&item.events));
    let (sent_message_parts, side_effect_ticket_id) = match &resolution.outcome {
        TurnOutcome::Sent {
            parts,
            side_effect_tickets,
        } => (
            Some(i32::try_from(*parts).unwrap_or(i32::MAX)),
            side_effect_tickets.first().copied(),
        ),
        TurnOutcome::SideEffectDelegated { tickets, .. } => (None, tickets.first().copied()),
        _ => (None, None),
    };
    DialogTurnOutcomeRecord {
        created_at: now,
        job_id: item.id,
        queue_name: report.queue_name.clone(),
        chat_id: report.chat_id,
        thread_id: report.thread_id,
        user_id: report.user_id,
        trigger_message_id: report.message_id,
        attempt,
        outcome: resolution.outcome.ledger_outcome().to_owned(),
        reason: resolution.outcome.ledger_reason().map(str::to_owned),
        provider: report.provider.clone(),
        model: None,
        elapsed_ms: Some(clamped_ms(budget.elapsed(now))),
        budget_ms: Some(clamped_ms(budget.limit)),
        user_signal,
        sent_message_parts,
        side_effect_ticket_id,
        detail: resolution_detail(resolution, report),
    }
}

fn clamped_ms(duration: TimeDuration) -> i32 {
    i32::try_from(duration.whole_milliseconds()).unwrap_or(i32::MAX)
}

fn resolution_detail(resolution: &TurnResolution, report: &DialogJobWorkerReport) -> Value {
    let mut detail = serde_json::Map::new();
    let mut put = |key: &str, value: Option<&String>| {
        if let Some(value) = value {
            detail.insert(key.to_owned(), json!(value));
        }
    };
    put("provider_error", report.provider_error.as_ref());
    put("send_error", report.send_error.as_ref());
    put("empty_answer_error", report.empty_answer_error.as_ref());
    put("status_error", report.status_error.as_ref());
    put("retry_target_queue", report.retry_target_queue.as_ref());
    put("user_signal_error", report.user_signal_error.as_ref());
    if let Some(max_attempts) = report.retry_max_attempts {
        detail.insert("retry_max_attempts".to_owned(), json!(max_attempts));
    }
    if report.regenerations > 0 {
        detail.insert("regenerations".to_owned(), json!(report.regenerations));
    }
    if let Some(duplicate_message_id) = report.suppressed_duplicate_message_id {
        detail.insert(
            "suppressed_duplicate_message_id".to_owned(),
            json!(duplicate_message_id),
        );
    }
    if let TurnOutcome::SideEffectDelegated { kinds, .. } = &resolution.outcome {
        detail.insert("side_effect_kinds".to_owned(), json!(kinds));
    }
    Value::Object(detail)
}
