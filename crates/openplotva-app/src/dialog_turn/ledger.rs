//! Reply-outcome ledger: ring buffer + async Postgres writer, mirroring the
//! `RuntimeLlmObserver` pattern in `runtime_llm.rs`. One record per dialog
//! worker tick that dequeued a job.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicI64, Ordering},
};
use std::time::Duration;

use openplotva_server::{
    RuntimeTurnOutcomeData, RuntimeTurnOutcomeInspector, RuntimeTurnOutcomesFilter,
};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, QueryBuilder};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

use crate::dialog_jobs::DialogJobWorkerReport;

const TURN_OUTCOME_BUFFER_CAPACITY: usize = 2048;
const TURN_OUTCOME_WRITER_CHANNEL_CAPACITY: usize = 1024;
const TURN_OUTCOME_WRITER_BATCH_SIZE: usize = 50;
const TURN_OUTCOME_WRITER_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// A text answer was accepted by the outbound queue.
pub const TURN_OUTCOME_SENT: &str = "sent";
/// A generation side effect is the reply (delivery watched separately).
pub const TURN_OUTCOME_SIDE_EFFECT_DELEGATED: &str = "side_effect_delegated";
/// The turn deliberately produced no reply, with a classified reason.
pub const TURN_OUTCOME_NO_REPLY_INTENTIONAL: &str = "no_reply_intentional";
/// Non-final attempt outcome: the job was requeued for another try.
pub const TURN_OUTCOME_RETRY_SCHEDULED: &str = "retry_scheduled";
/// The turn gave up; the user-signal column records what they saw.
pub const TURN_OUTCOME_TERMINAL_FAILED: &str = "terminal_failed";
/// Completed with nothing delivered and no classified reason — the defect
/// class this ledger exists to measure (eliminated by the turn engine).
pub const TURN_OUTCOME_SILENT: &str = "silent";
/// Pre-turn skip (decode error, stale, empty payload).
pub const TURN_OUTCOME_SKIPPED: &str = "skipped";

/// One classified dialog turn tick, ready for the ring buffer and Postgres.
#[derive(Clone, Debug, PartialEq)]
pub struct DialogTurnOutcomeRecord {
    pub created_at: OffsetDateTime,
    pub job_id: i64,
    pub queue_name: String,
    pub chat_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub user_id: Option<i64>,
    pub trigger_message_id: Option<i32>,
    pub attempt: i32,
    pub outcome: String,
    pub reason: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub elapsed_ms: Option<i32>,
    pub budget_ms: Option<i32>,
    pub user_signal: Option<String>,
    pub sent_message_parts: Option<i32>,
    pub side_effect_ticket_id: Option<i64>,
    pub detail: Value,
}

impl DialogTurnOutcomeRecord {
    fn to_runtime_data(&self, id: i64) -> RuntimeTurnOutcomeData {
        RuntimeTurnOutcomeData {
            id,
            at: self
                .created_at
                .format(&Rfc3339)
                .unwrap_or_else(|_| self.created_at.to_string()),
            job_id: self.job_id,
            queue_name: self.queue_name.clone(),
            chat_id: self.chat_id,
            thread_id: self.thread_id,
            user_id: self.user_id,
            trigger_message_id: self.trigger_message_id,
            attempt: self.attempt,
            outcome: self.outcome.clone(),
            reason: self.reason.clone(),
            provider: self.provider.clone(),
            model: self.model.clone(),
            elapsed_ms: self.elapsed_ms,
            budget_ms: self.budget_ms,
            user_signal: self.user_signal.clone(),
            sent_message_parts: self.sent_message_parts,
            side_effect_ticket_id: self.side_effect_ticket_id,
            detail: self.detail.clone(),
        }
    }
}

/// Classify one worker tick into a ledger record. Returns `None` for ticks
/// that never dequeued a job. Pure mapping over the legacy report flags so
/// Phase 1 stays behavior-neutral.
#[must_use]
pub fn outcome_from_report(
    report: &DialogJobWorkerReport,
    now: OffsetDateTime,
) -> Option<DialogTurnOutcomeRecord> {
    let job_id = report.job_id?;
    let (outcome, reason): (&str, Option<String>) = if report.decode_error.is_some() {
        (TURN_OUTCOME_SKIPPED, Some("decode_error".to_owned()))
    } else if report.skipped_stale {
        (TURN_OUTCOME_SKIPPED, Some("stale".to_owned()))
    } else if report.skipped_empty_payload {
        (TURN_OUTCOME_SKIPPED, Some("empty_payload".to_owned()))
    } else if report.content_blocked {
        (
            TURN_OUTCOME_NO_REPLY_INTENTIONAL,
            Some("content_blocked".to_owned()),
        )
    } else if report.retry_requeued {
        (TURN_OUTCOME_RETRY_SCHEDULED, Some(retry_reason(report)))
    } else if report.failed {
        (TURN_OUTCOME_TERMINAL_FAILED, Some(terminal_reason(report)))
    } else if report.suppressed_duplicate_message_id.is_some() {
        (
            TURN_OUTCOME_NO_REPLY_INTENTIONAL,
            Some("duplicate_suppressed".to_owned()),
        )
    } else if report.sent_answer {
        (TURN_OUTCOME_SENT, None)
    } else if report.answer_empty_all_sources {
        let reason = if report.persisted_tool_call_history {
            "tool_call_only"
        } else {
            "provider_empty"
        };
        (TURN_OUTCOME_SILENT, Some(reason.to_owned()))
    } else {
        (TURN_OUTCOME_SKIPPED, Some("unclassified".to_owned()))
    };

    Some(DialogTurnOutcomeRecord {
        created_at: now,
        job_id,
        queue_name: report.queue_name.clone(),
        chat_id: report.chat_id,
        thread_id: report.thread_id,
        user_id: report.user_id,
        trigger_message_id: report.message_id,
        attempt: report.retry_attempt.unwrap_or(1),
        outcome: outcome.to_owned(),
        reason,
        provider: report.provider.clone(),
        model: None,
        elapsed_ms: None,
        budget_ms: None,
        user_signal: None,
        sent_message_parts: report.sent_answer.then_some(1),
        side_effect_ticket_id: None,
        detail: report_detail(report),
    })
}

fn retry_reason(report: &DialogJobWorkerReport) -> String {
    if report.empty_answer_error.is_some() {
        "sanitized_empty".to_owned()
    } else {
        "provider_retryable".to_owned()
    }
}

fn terminal_reason(report: &DialogJobWorkerReport) -> String {
    if report.send_error.is_some() {
        "send_error".to_owned()
    } else if report.retry_exhausted && report.empty_answer_error.is_some() {
        "sanitized_empty_exhausted".to_owned()
    } else if report.retry_exhausted {
        "retry_exhausted".to_owned()
    } else if report.provider_error.is_some() {
        "provider_error".to_owned()
    } else {
        "unknown".to_owned()
    }
}

fn report_detail(report: &DialogJobWorkerReport) -> Value {
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
    Value::Object(detail)
}

/// Live ring buffer of recent turn outcomes served over runtime GraphQL.
#[derive(Clone)]
pub struct RuntimeTurnOutcomeBuffer {
    inner: Arc<Mutex<TurnOutcomeBufferInner>>,
    next_id: Arc<AtomicI64>,
}

struct TurnOutcomeBufferInner {
    ring: Vec<Option<RuntimeTurnOutcomeData>>,
    write: usize,
    count: usize,
}

impl Default for RuntimeTurnOutcomeBuffer {
    fn default() -> Self {
        Self::new(TURN_OUTCOME_BUFFER_CAPACITY)
    }
}

impl RuntimeTurnOutcomeBuffer {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let capacity = if capacity == 0 {
            TURN_OUTCOME_BUFFER_CAPACITY
        } else {
            capacity
        };
        Self {
            inner: Arc::new(Mutex::new(TurnOutcomeBufferInner {
                ring: vec![None; capacity],
                write: 0,
                count: 0,
            })),
            next_id: Arc::new(AtomicI64::new(0)),
        }
    }

    fn record(&self, record: &DialogTurnOutcomeRecord) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let data = record.to_runtime_data(id);
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.ring.is_empty() {
            return;
        }
        let write = inner.write;
        inner.ring[write] = Some(data);
        inner.write = (write + 1) % inner.ring.len();
        inner.count = inner.count.saturating_add(1).min(inner.ring.len());
    }

    fn list(&self) -> Vec<RuntimeTurnOutcomeData> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.count == 0 || inner.ring.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(inner.count);
        let mut idx = inner.write.checked_sub(1).unwrap_or(inner.ring.len() - 1);
        for _ in 0..inner.count {
            if let Some(data) = inner.ring[idx].clone() {
                out.push(data);
            }
            idx = idx.checked_sub(1).unwrap_or(inner.ring.len() - 1);
        }
        out
    }
}

impl RuntimeTurnOutcomeInspector for RuntimeTurnOutcomeBuffer {
    fn turn_outcomes(
        &self,
        filter: RuntimeTurnOutcomesFilter,
    ) -> Result<Vec<RuntimeTurnOutcomeData>, String> {
        let limit = filter.limit.max(1) as usize;
        let mut out = Vec::with_capacity(limit.min(TURN_OUTCOME_BUFFER_CAPACITY));
        for data in self.list() {
            let matches = filter
                .chat_id
                .is_none_or(|chat_id| data.chat_id.is_some_and(|value| value == chat_id))
                && filter.job_id.is_none_or(|job_id| data.job_id == job_id)
                && (filter.outcome.is_empty() || data.outcome == filter.outcome);
            if matches {
                out.push(data);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }
}

/// SQLx-backed turn outcome recorder with a buffered background writer.
#[derive(Clone, Debug)]
pub struct PostgresDialogTurnOutcomeRecorder {
    sender: mpsc::Sender<DialogTurnOutcomeRecord>,
}

impl PostgresDialogTurnOutcomeRecorder {
    /// Build a recorder and background buffered writer over an existing pool.
    pub fn spawn(pool: PgPool, stop: watch::Receiver<bool>) -> (Self, JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(TURN_OUTCOME_WRITER_CHANNEL_CAPACITY);
        let handle = tokio::spawn(run_turn_outcome_writer(pool, receiver, stop));
        (Self { sender }, handle)
    }

    fn enqueue(&self, record: DialogTurnOutcomeRecord) {
        if let Err(error) = self.sender.try_send(record) {
            match error {
                mpsc::error::TrySendError::Full(record) => {
                    tracing::warn!(
                        outcome = %record.outcome,
                        "dropping dialog turn outcome because writer channel is full"
                    );
                }
                mpsc::error::TrySendError::Closed(record) => {
                    tracing::debug!(
                        outcome = %record.outcome,
                        "dropping dialog turn outcome because writer is stopped"
                    );
                }
            }
        }
    }
}

/// Fan-in point for turn outcomes: live ring buffer plus optional Postgres
/// analytics. Called from exactly one place per turn.
#[derive(Clone)]
pub struct DialogTurnObserver {
    buffer: RuntimeTurnOutcomeBuffer,
    recorder: Option<PostgresDialogTurnOutcomeRecorder>,
}

impl DialogTurnObserver {
    #[must_use]
    pub fn new(
        buffer: RuntimeTurnOutcomeBuffer,
        recorder: Option<PostgresDialogTurnOutcomeRecorder>,
    ) -> Self {
        Self { buffer, recorder }
    }

    pub fn record(&self, record: DialogTurnOutcomeRecord) {
        self.buffer.record(&record);
        if let Some(recorder) = &self.recorder {
            recorder.enqueue(record);
        }
    }

    #[must_use]
    pub fn buffer(&self) -> RuntimeTurnOutcomeBuffer {
        self.buffer.clone()
    }
}

async fn run_turn_outcome_writer(
    pool: PgPool,
    mut receiver: mpsc::Receiver<DialogTurnOutcomeRecord>,
    mut stop: watch::Receiver<bool>,
) {
    let mut pending = Vec::with_capacity(TURN_OUTCOME_WRITER_BATCH_SIZE);
    let mut interval = tokio::time::interval(TURN_OUTCOME_WRITER_FLUSH_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        if *stop.borrow() {
            drain_and_flush(&pool, &mut receiver, &mut pending).await;
            break;
        }
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    drain_and_flush(&pool, &mut receiver, &mut pending).await;
                    break;
                }
            }
            maybe_record = receiver.recv() => {
                let Some(record) = maybe_record else {
                    flush(&pool, &mut pending).await;
                    break;
                };
                pending.push(record);
                if pending.len() >= TURN_OUTCOME_WRITER_BATCH_SIZE {
                    flush(&pool, &mut pending).await;
                }
            }
            _ = interval.tick() => {
                flush(&pool, &mut pending).await;
            }
        }
    }
}

async fn drain_and_flush(
    pool: &PgPool,
    receiver: &mut mpsc::Receiver<DialogTurnOutcomeRecord>,
    pending: &mut Vec<DialogTurnOutcomeRecord>,
) {
    loop {
        while pending.len() < TURN_OUTCOME_WRITER_BATCH_SIZE {
            match receiver.try_recv() {
                Ok(record) => pending.push(record),
                Err(_) => break,
            }
        }
        if pending.is_empty() {
            break;
        }
        flush(pool, pending).await;
    }
}

async fn flush(pool: &PgPool, pending: &mut Vec<DialogTurnOutcomeRecord>) {
    if pending.is_empty() {
        return;
    }
    let batch = std::mem::take(pending);
    if let Err(error) = insert_turn_outcomes(pool, &batch).await {
        tracing::warn!(%error, count = batch.len(), "failed to insert dialog turn outcome batch");
    }
}

const SQL_INSERT_TURN_OUTCOMES_PREFIX: &str = "INSERT INTO dialog_turn_outcomes \
    (created_at, job_id, queue_name, chat_id, thread_id, user_id, trigger_message_id, \
     attempt, outcome, reason, provider, model, elapsed_ms, budget_ms, user_signal, \
     sent_message_parts, side_effect_ticket_id, detail) VALUES";

async fn insert_turn_outcomes(
    pool: &PgPool,
    records: &[DialogTurnOutcomeRecord],
) -> Result<(), sqlx::Error> {
    if records.is_empty() {
        return Ok(());
    }
    let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(SQL_INSERT_TURN_OUTCOMES_PREFIX);
    builder.push(" ");
    builder.push_values(records.iter(), |mut row, record| {
        row.push_bind(record.created_at)
            .push_bind(record.job_id)
            .push_bind(record.queue_name.clone())
            .push_bind(record.chat_id)
            .push_bind(record.thread_id)
            .push_bind(record.user_id)
            .push_bind(record.trigger_message_id)
            .push_bind(record.attempt)
            .push_bind(record.outcome.clone())
            .push_bind(record.reason.clone())
            .push_bind(record.provider.clone())
            .push_bind(record.model.clone())
            .push_bind(record.elapsed_ms)
            .push_bind(record.budget_ms)
            .push_bind(record.user_signal.clone())
            .push_bind(record.sent_message_parts)
            .push_bind(record.side_effect_ticket_id)
            .push_bind(sqlx::types::Json(record.detail.clone()));
    });
    builder.build().execute(pool).await?;
    Ok(())
}

/// Delete ledger rows older than the retention window, in bounded batches.
pub async fn delete_old_turn_outcomes_batch(
    pool: &PgPool,
    retention_days: i32,
    batch_size: i64,
) -> Result<u64, sqlx::Error> {
    if retention_days <= 0 || batch_size <= 0 {
        return Ok(0);
    }
    let result = sqlx::query(
        "DELETE FROM dialog_turn_outcomes WHERE id IN (\
             SELECT id FROM dialog_turn_outcomes \
             WHERE created_at < now() - make_interval(days => $1) \
             ORDER BY id ASC LIMIT $2)",
    )
    .bind(retention_days)
    .bind(batch_size)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_report() -> DialogJobWorkerReport {
        DialogJobWorkerReport {
            queue_name: "dialog-aifarm".to_owned(),
            dequeued: true,
            job_id: Some(42),
            provider: Some("aifarm".to_owned()),
            chat_id: Some(100),
            user_id: Some(200),
            message_id: Some(300),
            ..DialogJobWorkerReport::default()
        }
    }

    #[test]
    fn no_job_means_no_record() {
        let report = DialogJobWorkerReport::default();
        assert!(outcome_from_report(&report, OffsetDateTime::UNIX_EPOCH).is_none());
    }

    #[test]
    fn classifies_every_outcome_path() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let cases: Vec<(DialogJobWorkerReport, &str, Option<&str>)> = vec![
            (
                DialogJobWorkerReport {
                    sent_answer: true,
                    completed: true,
                    ..base_report()
                },
                TURN_OUTCOME_SENT,
                None,
            ),
            (
                DialogJobWorkerReport {
                    decode_error: Some("bad".to_owned()),
                    failed: true,
                    ..base_report()
                },
                TURN_OUTCOME_SKIPPED,
                Some("decode_error"),
            ),
            (
                DialogJobWorkerReport {
                    skipped_stale: true,
                    completed: true,
                    ..base_report()
                },
                TURN_OUTCOME_SKIPPED,
                Some("stale"),
            ),
            (
                DialogJobWorkerReport {
                    skipped_empty_payload: true,
                    completed: true,
                    ..base_report()
                },
                TURN_OUTCOME_SKIPPED,
                Some("empty_payload"),
            ),
            (
                DialogJobWorkerReport {
                    content_blocked: true,
                    completed: true,
                    ..base_report()
                },
                TURN_OUTCOME_NO_REPLY_INTENTIONAL,
                Some("content_blocked"),
            ),
            (
                DialogJobWorkerReport {
                    retry_requeued: true,
                    retry_attempt: Some(2),
                    empty_answer_error: Some("empty".to_owned()),
                    ..base_report()
                },
                TURN_OUTCOME_RETRY_SCHEDULED,
                Some("sanitized_empty"),
            ),
            (
                DialogJobWorkerReport {
                    retry_requeued: true,
                    retryable_provider_error: Some("503".to_owned()),
                    ..base_report()
                },
                TURN_OUTCOME_RETRY_SCHEDULED,
                Some("provider_retryable"),
            ),
            (
                DialogJobWorkerReport {
                    failed: true,
                    retry_exhausted: true,
                    ..base_report()
                },
                TURN_OUTCOME_TERMINAL_FAILED,
                Some("retry_exhausted"),
            ),
            (
                DialogJobWorkerReport {
                    failed: true,
                    send_error: Some("boom".to_owned()),
                    ..base_report()
                },
                TURN_OUTCOME_TERMINAL_FAILED,
                Some("send_error"),
            ),
            (
                DialogJobWorkerReport {
                    completed: true,
                    suppressed_duplicate_message_id: Some(7),
                    ..base_report()
                },
                TURN_OUTCOME_NO_REPLY_INTENTIONAL,
                Some("duplicate_suppressed"),
            ),
            (
                DialogJobWorkerReport {
                    completed: true,
                    answer_empty_all_sources: true,
                    ..base_report()
                },
                TURN_OUTCOME_SILENT,
                Some("provider_empty"),
            ),
            (
                DialogJobWorkerReport {
                    completed: true,
                    answer_empty_all_sources: true,
                    persisted_tool_call_history: true,
                    ..base_report()
                },
                TURN_OUTCOME_SILENT,
                Some("tool_call_only"),
            ),
        ];

        for (report, outcome, reason) in cases {
            let record = outcome_from_report(&report, now).expect("record");
            assert_eq!(record.outcome, outcome, "report: {report:?}");
            assert_eq!(record.reason.as_deref(), reason, "report: {report:?}");
            assert_eq!(record.job_id, 42);
            assert_eq!(record.chat_id, Some(100));
        }
    }

    #[test]
    fn buffer_serves_filtered_outcomes_most_recent_first() {
        let buffer = RuntimeTurnOutcomeBuffer::new(8);
        for (job_id, outcome) in [(1, TURN_OUTCOME_SENT), (2, TURN_OUTCOME_SILENT)] {
            let record = DialogTurnOutcomeRecord {
                created_at: OffsetDateTime::UNIX_EPOCH,
                job_id,
                queue_name: "dialog-aifarm".to_owned(),
                chat_id: Some(5),
                thread_id: None,
                user_id: None,
                trigger_message_id: None,
                attempt: 1,
                outcome: outcome.to_owned(),
                reason: None,
                provider: None,
                model: None,
                elapsed_ms: None,
                budget_ms: None,
                user_signal: None,
                sent_message_parts: None,
                side_effect_ticket_id: None,
                detail: Value::Object(serde_json::Map::new()),
            };
            buffer.record(&record);
        }

        let all = buffer
            .turn_outcomes(RuntimeTurnOutcomesFilter {
                limit: 10,
                ..RuntimeTurnOutcomesFilter::default()
            })
            .expect("outcomes");
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].job_id, 2, "most recent first");

        let silent = buffer
            .turn_outcomes(RuntimeTurnOutcomesFilter {
                outcome: TURN_OUTCOME_SILENT.to_owned(),
                limit: 10,
                ..RuntimeTurnOutcomesFilter::default()
            })
            .expect("outcomes");
        assert_eq!(silent.len(), 1);
        assert_eq!(silent[0].job_id, 2);

        let other_chat = buffer
            .turn_outcomes(RuntimeTurnOutcomesFilter {
                chat_id: Some(6),
                limit: 10,
                ..RuntimeTurnOutcomesFilter::default()
            })
            .expect("outcomes");
        assert!(other_chat.is_empty());
    }
}
