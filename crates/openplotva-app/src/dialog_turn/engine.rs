//! Single-exit dialog turn engine.
//!
//! `execute_dialog_turn` returns a `TurnResolution` on every branch — the
//! compiler forbids falling off — and `finalize_turn` is the only code that
//! writes job status, appends the `turn_outcome` event, and records the
//! ledger row. Event-append failures never abort the status write (S13).

use std::collections::BTreeMap;
use std::time::Instant;

use openplotva_dialog::turn::{
    EmptyReason, QueuedSideEffect, ReplyMaterial, classify_reply_material,
};
use openplotva_llm::ChatProvider;
use openplotva_taskman::{DialogJobParams, TaskQueueJobEvent};
use serde_json::{Value, json};
use time::{Duration as TimeDuration, OffsetDateTime};

use super::budget::{TURN_DEADLINE, TURN_STARTED_STAGE, TurnBudget, turn_started_at};
use super::ledger::{DialogTurnObserver, DialogTurnOutcomeRecord};
use super::outcome::{JobDisposition, TurnOutcome, TurnResolution, UserSignalPlan};
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

/// A turn never starts generation it cannot plausibly finish inside the budget.
const MIN_GENERATION_BUDGET: TimeDuration = TimeDuration::seconds(15);

/// Immutable inputs of one dialog turn attempt.
pub(crate) struct TurnContext<'a> {
    pub item: &'a DialogJobWorkItem,
    pub params: &'a DialogJobParams,
    pub queue_name: &'static str,
    pub max_llm_job_attempts: i32,
    pub max_queue_age: TimeDuration,
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

    let input = materializer
        .materialize_dialog_input(ctx.params, ctx.now)
        .await;
    let duplicate_guard_history = input.history.clone();
    let generation_started = Instant::now();
    let provider_deadline = generation_started
        + std::time::Duration::try_from(ctx.budget.remaining(ctx.now)).unwrap_or_default();
    let result = TURN_DEADLINE
        .scope(Some(provider_deadline), provider.run_dialog(input))
        .await;
    // Generation is the only long-running step of a tick: fold its real wall
    // time into `now` so the retry handler compares against the budget honestly.
    let failure_now =
        ctx.now + TimeDuration::try_from(generation_started.elapsed()).unwrap_or_default();
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
            let retry_provider = openplotva_llm::retry::provider_name(error.as_ref()).to_owned();
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
            handle_retryable_dialog_provider_error(
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
            .await
        }
        ReplyMaterial::Empty {
            reason: EmptyReason::ProviderEmpty,
        } => {
            report.answer_empty_all_sources = true;
            let error =
                "dialog provider returned no answer, response, or queued tool material".to_owned();
            report.empty_answer_error = Some(error.clone());
            handle_retryable_dialog_provider_error(
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
            .await
        }
        ReplyMaterial::IntentionalSilence { reason } => TurnResolution {
            outcome: TurnOutcome::NoReplyIntentional {
                reason: reason.as_str(),
            },
            disposition: JobDisposition::Complete,
        },
        ReplyMaterial::Delegated { side_effects } => TurnResolution {
            outcome: TurnOutcome::SideEffectDelegated {
                tickets: side_effect_tickets(&side_effects),
                kinds: side_effects
                    .iter()
                    .map(|side_effect| side_effect.kind.clone())
                    .collect(),
            },
            disposition: JobDisposition::Complete,
        },
        ReplyMaterial::Text { text, side_effects } => {
            let (duplicate_message_id, suppressed) =
                should_suppress_duplicate_bot_reply(&duplicate_guard_history, &text);
            if suppressed {
                report.suppressed_duplicate_message_id = Some(duplicate_message_id);
                tracing::warn!(
                    chat_id = ctx.params.chat_id,
                    message_id = ctx.params.message_id,
                    duplicate_message_id,
                    "suppressed duplicate dialog answer"
                );
                return TurnResolution {
                    outcome: TurnOutcome::NoReplyIntentional {
                        reason: "duplicate_suppressed",
                    },
                    disposition: JobDisposition::Complete,
                };
            }
            if let Err(validation) = validate_dialog_answer_deliverable(&text) {
                let error = format!("dialog answer rejected by outbound validation: {validation}");
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
            match effects.send_dialog_answer(ctx.params, &text).await {
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
            }
        }
    }
}

/// The single writer of job status, `turn_outcome` job event, and ledger row.
pub(crate) async fn finalize_turn<Queue>(
    queue: &Queue,
    item: &DialogJobWorkItem,
    resolution: TurnResolution,
    budget: &TurnBudget,
    now: OffsetDateTime,
    observer: Option<&DialogTurnObserver>,
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

    // (b) Job status per disposition — status transition wins over event fidelity.
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

    // (c) Per-job outcome event for the taskman drill-down view.
    let mut data = BTreeMap::new();
    data.insert(
        "outcome".to_owned(),
        resolution.outcome.ledger_outcome().to_owned(),
    );
    if let Some(reason) = resolution.outcome.ledger_reason() {
        data.insert("reason".to_owned(), reason.to_owned());
    }
    if let Some(user_signal) = ledger_user_signal(&resolution.outcome) {
        data.insert("user_signal".to_owned(), user_signal.to_owned());
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

    // (d) Ledger row — exactly one per handled turn.
    if let Some(observer) = observer {
        observer.record(record_from_resolution(
            &resolution,
            item,
            budget,
            now,
            report,
        ));
    }
}

fn ledger_user_signal(outcome: &TurnOutcome) -> Option<&'static str> {
    // Phase 3 ships reactions; until then every terminal failure records "none".
    matches!(outcome, TurnOutcome::TerminalFailed { .. }).then_some("none")
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
        user_signal: ledger_user_signal(&resolution.outcome).map(str::to_owned),
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
    if let Some(max_attempts) = report.retry_max_attempts {
        detail.insert("retry_max_attempts".to_owned(), json!(max_attempts));
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
