//! Per-tick and per-run worker reports, plus the tick trace line.

use crate::telegram_activity::TelegramActivitySnapshot;

/// Result of one dialog taskman worker tick.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DialogJobWorkerReport {
    /// Queue checked by this tick.
    pub queue_name: String,
    /// Whether a job was dequeued.
    pub dequeued: bool,
    /// Dequeued taskman job ID.
    pub job_id: Option<i64>,
    /// Provider chosen for execution.
    pub provider: Option<String>,
    pub skipped_empty_payload: bool,
    /// Never-processed job outlived the queue-age gate and was dropped.
    pub skipped_queue_backlog: bool,
    pub content_blocked: bool,
    pub sent_answer: bool,
    /// Final answer was committed to Postgres and awaits Telegram receipts.
    pub queued_answer: bool,
    /// Dialog answer matched the latest comparable bot reply and was suppressed.
    pub suppressed_duplicate_message_id: Option<i32>,
    /// Job was finalized as completed.
    pub completed: bool,
    /// Job transitioned to taskman's active `WaitingDelivery` state.
    pub waiting_delivery: bool,
    /// Job was finalized as failed.
    pub failed: bool,
    /// Queue dequeue failed.
    pub dequeue_error: Option<String>,
    pub decode_error: Option<String>,
    /// Provider failure reason classified as retryable by the LLM layer.
    pub retryable_provider_error: Option<String>,
    /// Retryable LLM failure attempt number.
    pub retry_attempt: Option<i32>,
    /// Retryable LLM failure max attempt count.
    pub retry_max_attempts: Option<i32>,
    /// Target queue selected for retryable LLM requeue.
    pub retry_target_queue: Option<String>,
    /// Retryable LLM failure was requeued without failing the job.
    pub retry_requeued: bool,
    /// Retryable LLM failure exhausted attempts and failed the job.
    pub retry_exhausted: bool,
    /// Provider execution failed.
    pub provider_error: Option<String>,
    /// Answer side effect failed.
    pub send_error: Option<String>,
    /// Provider returned content that became empty after outbound sanitization.
    pub empty_answer_error: Option<String>,
    /// Provider tool calls were persisted to chat history.
    pub persisted_tool_call_history: bool,
    /// Tool-call history persistence failed non-fatally.
    pub tool_call_history_error: Option<String>,
    pub recorded_dialog_fallback_event: bool,
    pub dialog_fallback_event_error: Option<String>,
    /// Completion/failure status write failed.
    pub status_error: Option<String>,
    /// Chat the job answered (from decoded params).
    pub chat_id: Option<i64>,
    /// Forum thread of the trigger message, when present.
    pub thread_id: Option<i32>,
    /// User whose message triggered the job.
    pub user_id: Option<i64>,
    /// Trigger message ID.
    pub message_id: Option<i32>,
    /// answer, response, and queued tool material were all empty; the turn
    /// engine retries this class instead of completing silently.
    pub answer_empty_all_sources: bool,
    /// In-process duplicate-answer regenerations performed this tick.
    pub regenerations: i32,
    /// Turn re-entry found the `answer_sent` marker and resolved `Sent`
    /// without re-sending the answer.
    pub resent_skipped: bool,
    /// LLM iterations the session engine ran this tick (0 on the legacy path).
    pub session_iterations: i32,
    /// This job's trigger was absorbed by the named running session.
    pub merged_into_session: Option<i64>,
    /// This third-party job parked behind the named running session.
    pub deferred_after_session: Option<i64>,
    /// Follow-up job respawned for leftover injected messages at release.
    pub followup_respawned: Option<i64>,
    /// Terminal user signal failed after reaction and fallback attempts.
    pub user_signal_error: Option<String>,
    /// Best-effort Telegram activity pulse report for this active turn.
    pub activity: TelegramActivitySnapshot,
}

/// Aggregate report for a long-running dialog taskman worker.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DialogJobWorkerRunReport {
    /// Number of poll ticks.
    pub ticks: u64,
    /// Number of ticks that dequeued a job.
    pub dequeued: u64,
    /// Number of jobs completed.
    pub completed: u64,
    /// Number of jobs failed.
    pub failed: u64,
    /// Number of empty-payload jobs skipped.
    pub skipped_empty_payload: u64,
    /// Number of expired-backlog jobs dropped without provider execution.
    pub skipped_queue_backlog: u64,
    /// Number of content-blocked provider results treated as completed.
    pub content_blocked: u64,
    /// Number of answers queued.
    pub sent_answers: u64,
    /// Number of answers committed to the durable Telegram outbox.
    pub queued_answers: u64,
    /// Number of duplicate answers suppressed before send.
    pub suppressed_duplicate_answers: u64,
    /// Number of retryable provider errors requeued.
    pub retry_requeued: u64,
    /// Number of retryable provider errors exhausted.
    pub retry_exhausted: u64,
    /// Number of queue dequeue errors.
    pub dequeue_errors: u64,
    /// Number of completion/failure write errors.
    pub status_errors: u64,
    /// Number of accepted Telegram activity pulse sends.
    pub activity_pulses_sent: u64,
    /// Number of failed Telegram activity pulse sends.
    pub activity_pulse_failures: u64,
    /// Number of skipped Telegram activity pulse sends.
    pub activity_pulse_skips: u64,
}

impl DialogJobWorkerRunReport {
    pub(crate) fn record_tick(&mut self, tick: &DialogJobWorkerReport) {
        self.ticks += 1;
        if tick.dequeued {
            self.dequeued += 1;
        }
        if tick.completed {
            self.completed += 1;
        }
        if tick.failed {
            self.failed += 1;
        }
        if tick.skipped_empty_payload {
            self.skipped_empty_payload += 1;
        }
        if tick.skipped_queue_backlog {
            self.skipped_queue_backlog += 1;
        }
        if tick.content_blocked {
            self.content_blocked += 1;
        }
        if tick.sent_answer {
            self.sent_answers += 1;
        }
        if tick.queued_answer {
            self.queued_answers += 1;
        }
        if tick.suppressed_duplicate_message_id.is_some() {
            self.suppressed_duplicate_answers += 1;
        }
        if tick.retry_requeued {
            self.retry_requeued += 1;
        }
        if tick.retry_exhausted {
            self.retry_exhausted += 1;
        }
        if tick.dequeue_error.is_some() {
            self.dequeue_errors += 1;
        }
        if tick.status_error.is_some() {
            self.status_errors += 1;
        }
        self.activity_pulses_sent += tick.activity.sent;
        self.activity_pulse_failures += tick.activity.failed;
        self.activity_pulse_skips += tick.activity.skipped;
    }
}

pub(crate) fn trace_dialog_job_tick(tick: &DialogJobWorkerReport) {
    if !tick.dequeued
        && tick.dequeue_error.is_none()
        && tick.decode_error.is_none()
        && tick.provider_error.is_none()
        && tick.send_error.is_none()
        && tick.empty_answer_error.is_none()
        && tick.dialog_fallback_event_error.is_none()
        && tick.status_error.is_none()
        && tick.user_signal_error.is_none()
        && !tick.activity.has_activity()
    {
        return;
    }

    tracing::debug!(
        queue_name = tick.queue_name,
        job_id = tick.job_id,
        provider = tick.provider.as_deref(),
        dequeued = tick.dequeued,
        completed = tick.completed,
        failed = tick.failed,
        skipped_empty_payload = tick.skipped_empty_payload,
        skipped_queue_backlog = tick.skipped_queue_backlog,
        content_blocked = tick.content_blocked,
        sent_answer = tick.sent_answer,
        queued_answer = tick.queued_answer,
        waiting_delivery = tick.waiting_delivery,
        suppressed_duplicate_message_id = tick.suppressed_duplicate_message_id,
        persisted_tool_call_history = tick.persisted_tool_call_history,
        recorded_dialog_fallback_event = tick.recorded_dialog_fallback_event,
        retry_requeued = tick.retry_requeued,
        retry_exhausted = tick.retry_exhausted,
        retry_attempt = tick.retry_attempt,
        retry_max_attempts = tick.retry_max_attempts,
        retry_target_queue = tick.retry_target_queue.as_deref(),
        activity_action = tick
            .activity
            .action
            .map(|action| action.as_telegram_action()),
        activity_pulses_sent = tick.activity.sent,
        activity_pulse_failures = tick.activity.failed,
        activity_pulse_skips = tick.activity.skipped,
        activity_pulse_last_error = tick.activity.last_error.as_deref(),
        dequeue_error = tick.dequeue_error.as_deref(),
        decode_error = tick.decode_error.as_deref(),
        retryable_provider_error = tick.retryable_provider_error.as_deref(),
        provider_error = tick.provider_error.as_deref(),
        send_error = tick.send_error.as_deref(),
        empty_answer_error = tick.empty_answer_error.as_deref(),
        tool_call_history_error = tick.tool_call_history_error.as_deref(),
        dialog_fallback_event_error = tick.dialog_fallback_event_error.as_deref(),
        status_error = tick.status_error.as_deref(),
        regenerations = tick.regenerations,
        user_signal_error = tick.user_signal_error.as_deref(),
        "processed dialog taskman worker tick"
    );

    if tick.empty_answer_error.is_some() && tick.retry_exhausted {
        tracing::warn!(
            queue_name = tick.queue_name,
            job_id = tick.job_id,
            provider = tick.provider.as_deref(),
            retry_attempt = tick.retry_attempt,
            empty_answer_error = tick.empty_answer_error.as_deref(),
            "dialog turn produced no sendable answer after retries; no reply sent"
        );
    }
}
