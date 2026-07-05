//! Single-exit dialog turn engine.
//!
//! `execute_dialog_turn` returns a `TurnResolution` on every branch — the
//! compiler forbids falling off — and `finalize_turn` is the only code that
//! writes job status, appends the `turn_outcome` event, records the ledger
//! row, and fires the terminal user signal. Event-append failures never abort
//! the status write (S13).

use std::collections::BTreeMap;

use openplotva_llm::ChatProvider;
use openplotva_taskman::{DialogJobParams, TaskQueueJobEvent};
use serde_json::{Value, json};
use time::{Duration as TimeDuration, OffsetDateTime};

use super::budget::{TURN_STARTED_STAGE, TurnBudget, turn_started_at};
use super::ledger::{DialogTurnObserver, DialogTurnOutcomeRecord};
use super::outcome::{JobDisposition, TurnOutcome, TurnResolution, UserSignalPlan};
use super::signal::{SignalTarget, TERMINAL_USER_SIGNAL_STAGE, TurnSignalPolicy, UserSignalResult};
use crate::dialog_jobs::{
    DialogInputMaterializer, DialogJobEffects, DialogJobWorkItem, DialogJobWorkerQueue,
    DialogJobWorkerReport, DialogToolCallHistoryStore, dialog_job_has_payload,
    next_dialog_llm_job_attempt,
};

/// Job event stage carrying the recorded outcome of every handled turn.
pub const TURN_OUTCOME_STAGE: &str = "turn_outcome";

/// Job event stage appended per in-process duplicate-answer regeneration.
pub const DIALOG_TURN_REGENERATE_STAGE: &str = "dialog_turn_regenerate";

/// Job event stage appended right after a successfully queued dialog answer.
/// On turn re-entry (crash between send and status write) the marker makes
/// the turn resolve `Sent` without re-sending the answer.
pub const ANSWER_SENT_STAGE: &str = "answer_sent";

/// A turn never starts generation it cannot plausibly finish inside the budget.
const MIN_GENERATION_BUDGET: TimeDuration = TimeDuration::seconds(15);

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
    pub session: super::session::SessionTurnConfig<'a>,
    /// Live inbox for this turn when the worker claimed the session key.
    pub session_inbox: Option<std::sync::Arc<super::inbox::SessionInbox>>,
    /// Agent-run buffer collecting tool results and sent markers.
    pub llm_runs: Option<&'a crate::runtime_llm_runs::RuntimeLlmRunBuffer>,
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
    // Lift the capture-only context X-ray onto the open run (admin detail only).
    if let (Some(runs), Some(context)) = (ctx.llm_runs, base_input.context_capture.clone()) {
        runs.record_context(&format!("job-{}", ctx.item.id), context);
    }
    let duplicate_guard_history = base_input.history.clone();

    // The session engine is the only dialog path; a provider without the
    // single-shot step seam cannot serve dialog turns.
    let Some(step_provider) = provider.as_chat_step() else {
        let error = format!(
            "dialog provider {} has no chat-step support",
            provider.provider_name()
        );
        return TurnResolution {
            outcome: TurnOutcome::TerminalFailed {
                reason: "provider_error",
                error: error.clone(),
                user_signal: UserSignalPlan::React,
            },
            disposition: JobDisposition::Fail(error),
        };
    };
    let session_ctx = super::session::SessionRunContext {
        item_id: ctx.item.id,
        item_events: &ctx.item.events,
        params: ctx.params,
        queue_name: ctx.queue_name,
        max_llm_job_attempts: ctx.max_llm_job_attempts,
        max_regenerations: ctx.max_regenerations,
        budget: ctx.budget,
        now: ctx.now,
        routing_events: ctx.routing_events,
        item: ctx.item,
        inbox: ctx.session_inbox.clone(),
        llm_runs: ctx.llm_runs,
    };
    super::session::run_dialog_session(
        session_ctx,
        &ctx.session,
        step_provider,
        base_input,
        &duplicate_guard_history,
        queue,
        effects,
        tool_history,
        report,
    )
    .await
}

/// The single writer of job status, `turn_outcome` job event, ledger row, and
/// terminal user signal. Delivery obligations were already inserted by the
/// schedulers at ticket-assignment time; finalize only annotates them with the
/// dialog job id (best-effort).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn finalize_turn<Queue>(
    queue: &Queue,
    item: &DialogJobWorkItem,
    resolution: TurnResolution,
    budget: &TurnBudget,
    now: OffsetDateTime,
    observer: Option<&DialogTurnObserver>,
    signals: TurnSignalPolicy<'_>,
    obligations: Option<&dyn super::obligations::DeliveryObligationAnnotator>,
    report: &mut DialogJobWorkerReport,
) where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    if let Some(annotator) = obligations {
        annotate_delegated_obligations(annotator, item.id, &resolution).await;
    }
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

/// Backfill the dialog job id onto obligations recorded at schedule time.
/// Annotation is diagnostics-only linkage; failure never affects the turn.
async fn annotate_delegated_obligations(
    annotator: &dyn super::obligations::DeliveryObligationAnnotator,
    dialog_job_id: i64,
    resolution: &TurnResolution,
) {
    let tickets: &[i64] = match &resolution.outcome {
        TurnOutcome::SideEffectDelegated { tickets, .. } => tickets,
        TurnOutcome::Sent {
            side_effect_tickets,
            ..
        } => side_effect_tickets,
        _ => return,
    };
    for ticket_job_id in tickets {
        if let Err(error) = annotator
            .annotate_dialog_job(*ticket_job_id, dialog_job_id)
            .await
        {
            tracing::debug!(
                ticket_job_id,
                dialog_job_id,
                %error,
                "failed to annotate delivery obligation with dialog job id"
            );
        }
    }
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
    if report.session_iterations > 0 {
        detail.insert("iterations".to_owned(), json!(report.session_iterations));
    }
    if report.resent_skipped {
        detail.insert("resent_skipped".to_owned(), json!(true));
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
    if let TurnOutcome::MergedIntoSession { session_job_id }
    | TurnOutcome::DeferredAfterSession { session_job_id } = &resolution.outcome
    {
        detail.insert("merged_into_job_id".to_owned(), json!(session_job_id));
    }
    Value::Object(detail)
}
