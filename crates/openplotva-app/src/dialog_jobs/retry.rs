//! Cross-tick retry policy for retryable dialog provider failures: reason
//! codes, attempt counting, requeue target selection, and retry events.

use std::collections::BTreeMap;

use openplotva_dialog::DialogOutput;
use openplotva_taskman::{
    DIALOG_AIFARM_QUEUE_NAME, DialogJobParams, LLM_JOB_RETRY_EXHAUSTED_STAGE, LLM_JOB_RETRY_STAGE,
    TaskQueueJobEvent,
};
use time::OffsetDateTime;

use super::{DialogJobWorkItem, DialogJobWorkerQueue, DialogJobWorkerReport};

/// Ledger reason codes for one retryable failure class: the string recorded
/// while attempts remain, and the string recorded on exhaustion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RetryReasonCodes {
    pub scheduled: &'static str,
    pub exhausted: &'static str,
}

/// Retryable provider error that escaped the routed walker.
pub(crate) const PROVIDER_ERROR_RETRY_CODES: RetryReasonCodes = RetryReasonCodes {
    scheduled: "provider_retryable",
    exhausted: "retry_exhausted",
};
/// Raw text existed but collapsed during outbound sanitization.
pub(crate) const SANITIZED_EMPTY_RETRY_CODES: RetryReasonCodes = RetryReasonCodes {
    scheduled: "sanitized_empty",
    exhausted: "sanitized_empty_exhausted",
};
/// Answer, response, and tool material were all empty.
pub(crate) const PROVIDER_EMPTY_RETRY_CODES: RetryReasonCodes = RetryReasonCodes {
    scheduled: "provider_empty",
    exhausted: "provider_empty_exhausted",
};
/// The outbound boundary would reject the answer verbatim.
pub(crate) const UNDELIVERABLE_RETRY_CODES: RetryReasonCodes = RetryReasonCodes {
    scheduled: "undeliverable_answer",
    exhausted: "undeliverable_answer_exhausted",
};

pub(crate) struct RetryableDialogProviderFailure<'a> {
    pub queue_name: &'a str,
    pub provider_name: &'a str,
    pub reason: openplotva_llm::retry::FailureReason,
    pub codes: RetryReasonCodes,
    pub error: &'a str,
    pub max_attempts: i32,
    pub now: OffsetDateTime,
    pub budget_deadline: OffsetDateTime,
}

/// Decide retry vs terminal for one retryable failure. Appends the retry job
/// event (append failure never blocks the resolution — the job must not stick
/// in `Processing`) and returns the resolution; `finalize_turn` applies it.
pub(crate) async fn handle_retryable_dialog_provider_error<Queue>(
    queue: &Queue,
    item: &DialogJobWorkItem,
    params: &DialogJobParams,
    routing_events: Option<&crate::runtime_routing::RoutingEventReporter>,
    failure: RetryableDialogProviderFailure<'_>,
    report: &mut DialogJobWorkerReport,
) -> crate::dialog_turn::TurnResolution
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let attempt = next_dialog_llm_job_attempt(&item.events);
    let max_attempts = failure.max_attempts.max(1);
    let provider = infer_dialog_retry_provider(failure.provider_name, failure.queue_name);
    let target_queue = retryable_dialog_job_target_queue(failure.queue_name);
    let attempts_exhausted = attempt >= max_attempts;
    let budget_exhausted = failure.now >= failure.budget_deadline;
    let exhausted = attempts_exhausted || budget_exhausted;
    let stage = if exhausted {
        LLM_JOB_RETRY_EXHAUSTED_STAGE
    } else {
        LLM_JOB_RETRY_STAGE
    };
    let mut event = dialog_retry_job_event(
        stage,
        attempt,
        max_attempts,
        &provider,
        failure.reason,
        &target_queue,
        failure.error,
    );
    if exhausted {
        event.message = if budget_exhausted && !attempts_exhausted {
            "retryable LLM provider error exhausted the turn budget".to_owned()
        } else {
            "retryable LLM provider error exhausted job attempts".to_owned()
        };
    }

    report.retryable_provider_error = Some(failure.reason.to_string());
    report.retry_attempt = Some(attempt);
    report.retry_max_attempts = Some(max_attempts);
    report.retry_target_queue = Some(target_queue.clone());

    if let Err(error) = queue
        .append_dialog_job_event(item.id, event, failure.now)
        .await
    {
        report.status_error = Some(error.to_string());
    }

    if exhausted {
        report.retry_exhausted = true;
        if let Some(reporter) = routing_events {
            reporter.record(dialog_retry_exhausted_routing_event(
                item,
                params,
                &provider,
                &target_queue,
                &failure,
                attempt,
                max_attempts,
            ));
        }
        let reason = if budget_exhausted && !attempts_exhausted {
            "budget_exhausted"
        } else {
            failure.codes.exhausted
        };
        return crate::dialog_turn::TurnResolution {
            outcome: crate::dialog_turn::TurnOutcome::TerminalFailed {
                reason,
                error: failure.error.to_owned(),
                user_signal: crate::dialog_turn::UserSignalPlan::React,
            },
            disposition: crate::dialog_turn::JobDisposition::Fail(failure.error.to_owned()),
        };
    }

    crate::dialog_turn::TurnResolution {
        outcome: crate::dialog_turn::TurnOutcome::RetryScheduled {
            reason: failure.codes.scheduled,
            attempt,
            max_attempts,
            target_queue: target_queue.clone(),
        },
        disposition: crate::dialog_turn::JobDisposition::Requeue(target_queue),
    }
}

pub(crate) async fn record_dialog_fallback_event<Queue>(
    queue: &Queue,
    job_id: i64,
    output: &DialogOutput,
    now: OffsetDateTime,
    report: &mut DialogJobWorkerReport,
) where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let Some(event) = dialog_fallback_job_event(output) else {
        return;
    };
    match queue.append_dialog_job_event(job_id, event, now).await {
        Ok(()) => report.recorded_dialog_fallback_event = true,
        Err(error) => {
            let error = error.to_string();
            tracing::debug!(%error, job_id, "failed to append dialog fallback event");
            report.dialog_fallback_event_error = Some(error);
        }
    }
}

fn dialog_fallback_job_event(output: &DialogOutput) -> Option<TaskQueueJobEvent> {
    let primary_provider = output.fallback_from.trim();
    let primary_error = output.fallback_error.trim();
    if primary_provider.is_empty() || primary_error.is_empty() {
        return None;
    }
    let fallback_provider = output.provider.trim();
    let fallback_reason = openplotva_llm::retry::retryable_reason_from_message(primary_error)
        .map(|reason| reason.to_string())
        .unwrap_or_default();
    let mut data = BTreeMap::new();
    data.insert("fallback_provider".to_owned(), fallback_provider.to_owned());
    data.insert("fallback_reason".to_owned(), fallback_reason);

    Some(TaskQueueJobEvent {
        level: "warn".to_owned(),
        stage: "dialog_fallback".to_owned(),
        provider: primary_provider.to_owned(),
        message: "primary dialog backend failed, trying fallback".to_owned(),
        error: primary_error.to_owned(),
        data,
        ..TaskQueueJobEvent::default()
    })
}

pub(crate) fn next_dialog_llm_job_attempt(events: &[TaskQueueJobEvent]) -> i32 {
    events
        .iter()
        .filter(|event| {
            event.stage == LLM_JOB_RETRY_STAGE || event.stage == LLM_JOB_RETRY_EXHAUSTED_STAGE
        })
        .map(|event| event.attempt)
        .max()
        .unwrap_or(0)
        + 1
}

fn retryable_dialog_job_target_queue(queue_name: &str) -> String {
    if queue_name.trim().is_empty() {
        DIALOG_AIFARM_QUEUE_NAME.to_owned()
    } else {
        queue_name.to_owned()
    }
}

fn infer_dialog_retry_provider(provider_name: &str, queue_name: &str) -> String {
    let provider_name = provider_name.trim();
    if !provider_name.is_empty() {
        return provider_name.to_owned();
    }
    if queue_name == DIALOG_AIFARM_QUEUE_NAME {
        "aifarm".to_owned()
    } else {
        "llm".to_owned()
    }
}

fn dialog_retry_job_event(
    stage: &str,
    attempt: i32,
    max_attempts: i32,
    provider: &str,
    reason: openplotva_llm::retry::FailureReason,
    target_queue: &str,
    error: &str,
) -> TaskQueueJobEvent {
    let mut data = BTreeMap::new();
    data.insert("fallback_reason".to_owned(), reason.to_string());
    data.insert("max_attempts".to_owned(), max_attempts.to_string());
    data.insert("target_queue".to_owned(), target_queue.to_owned());

    TaskQueueJobEvent {
        level: "warn".to_owned(),
        stage: stage.to_owned(),
        attempt,
        provider: provider.to_owned(),
        message: "retryable LLM provider error, requeueing job".to_owned(),
        error: error.to_owned(),
        data,
        ..TaskQueueJobEvent::default()
    }
}

fn dialog_retry_exhausted_routing_event(
    item: &DialogJobWorkItem,
    params: &DialogJobParams,
    provider: &str,
    target_queue: &str,
    failure: &RetryableDialogProviderFailure<'_>,
    attempt: i32,
    max_attempts: i32,
) -> crate::runtime_routing::RoutingEvent {
    crate::runtime_routing::RoutingEvent {
        severity: "error".to_owned(),
        event_type: "all_attempts_exhausted".to_owned(),
        workflow_key: "dialog".to_owned(),
        provider_id: None,
        model_id: None,
        queue_name: Some(failure.queue_name.to_owned()),
        job_id: Some(item.id),
        chat_id: (params.chat_id != 0).then_some(params.chat_id),
        thread_id: params.thread_id,
        message_id: (params.message_id != 0).then_some(params.message_id),
        dedupe_key: format!("all_attempts_exhausted:dialog:job_retry_exhausted:{provider}"),
        summary: "dialog job retry budget exhausted".to_owned(),
        detail: serde_json::json!({
            "job_attempts": attempt,
            "max_attempts": max_attempts,
            "last_retryable_reason": failure.reason.as_str(),
            "provider": provider,
            "target_queue": target_queue,
        }),
    }
}
