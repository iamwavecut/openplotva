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
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

const TURN_OUTCOME_BUFFER_CAPACITY: usize = 2048;
const TURN_OUTCOME_WRITER_CHANNEL_CAPACITY: usize = 1024;
const TURN_OUTCOME_WRITER_BATCH_SIZE: usize = 50;
const TURN_OUTCOME_WRITER_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// A text answer was accepted by the outbound queue.
pub const TURN_OUTCOME_SENT: &str = "sent";
/// A final answer is durable but still awaits real Telegram receipts.
pub const TURN_OUTCOME_QUEUED_FOR_DELIVERY: &str = "queued_for_delivery";
/// A generation side effect is the reply (delivery watched separately).
pub const TURN_OUTCOME_SIDE_EFFECT_DELEGATED: &str = "side_effect_delegated";
/// The turn deliberately produced no reply, with a classified reason.
pub const TURN_OUTCOME_NO_REPLY_INTENTIONAL: &str = "no_reply_intentional";
/// Non-final attempt outcome: the job was requeued for another try.
pub const TURN_OUTCOME_RETRY_SCHEDULED: &str = "retry_scheduled";
/// The turn gave up; the user-signal column records what they saw.
pub const TURN_OUTCOME_TERMINAL_FAILED: &str = "terminal_failed";
/// Pre-turn skip (decode error, expired queue backlog, empty payload).
pub const TURN_OUTCOME_SKIPPED: &str = "skipped";

/// The trigger was absorbed by the chat's running session.
pub const TURN_OUTCOME_MERGED_INTO_SESSION: &str = "merged_into_session";

/// The turn parked behind the chat's running session and respawns after it.
pub const TURN_OUTCOME_DEFERRED_AFTER_SESSION: &str = "deferred_after_session";

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
    pub delivery_state: String,
    pub outbox_operation_ids: Vec<String>,
    pub delivered_at: Option<OffsetDateTime>,
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
    runs: Option<crate::runtime_llm_runs::RuntimeLlmRunBuffer>,
}

impl DialogTurnObserver {
    #[must_use]
    pub fn new(
        buffer: RuntimeTurnOutcomeBuffer,
        recorder: Option<PostgresDialogTurnOutcomeRecorder>,
    ) -> Self {
        Self {
            buffer,
            recorder,
            runs: None,
        }
    }

    /// Close the matching agent-run record whenever a turn outcome lands.
    #[must_use]
    pub fn with_run_buffer(mut self, runs: crate::runtime_llm_runs::RuntimeLlmRunBuffer) -> Self {
        self.runs = Some(runs);
        self
    }

    pub fn record(&self, record: DialogTurnOutcomeRecord) {
        if let Some(runs) = &self.runs {
            let status = match record.outcome.as_str() {
                TURN_OUTCOME_TERMINAL_FAILED | TURN_OUTCOME_SKIPPED => {
                    crate::runtime_llm_runs::RunStatus::Failed
                }
                _ => crate::runtime_llm_runs::RunStatus::Completed,
            };
            let error = matches!(status, crate::runtime_llm_runs::RunStatus::Failed).then(|| {
                record
                    .reason
                    .clone()
                    .unwrap_or_else(|| record.outcome.clone())
            });
            runs.finish_run(
                &format!("job-{}", record.job_id),
                status,
                Some(crate::runtime_llm_runs::RunOutcome {
                    outcome: record.outcome.clone(),
                    reason: record.reason.clone(),
                    user_signal: record.user_signal.clone(),
                    sent_message_parts: record.sent_message_parts,
                    side_effect_ticket_id: record.side_effect_ticket_id,
                    detail: record.detail.clone(),
                }),
                error,
                record.created_at,
            );
        }
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

// `push_values` emits the `VALUES` keyword itself, so the prefix must stop at
// the column list; a trailing `VALUES` here produced `VALUES VALUES` and every
// ledger insert failed silently.
const SQL_INSERT_TURN_OUTCOMES_PREFIX: &str = "INSERT INTO dialog_turn_outcomes \
    (created_at, job_id, queue_name, chat_id, thread_id, user_id, trigger_message_id, \
     attempt, outcome, reason, provider, model, elapsed_ms, budget_ms, user_signal, \
     sent_message_parts, side_effect_ticket_id, detail, delivery_state, \
     outbox_operation_ids, delivered_at)";

async fn insert_turn_outcomes(
    pool: &PgPool,
    records: &[DialogTurnOutcomeRecord],
) -> Result<(), sqlx::Error> {
    if records.is_empty() {
        return Ok(());
    }
    let mut builder = build_insert_turn_outcomes(records);
    builder.build().execute(pool).await?;
    Ok(())
}

fn build_insert_turn_outcomes(records: &[DialogTurnOutcomeRecord]) -> QueryBuilder<Postgres> {
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
            .push_bind(sqlx::types::Json(record.detail.clone()))
            .push_bind(record.delivery_state.clone())
            .push_bind(record.outbox_operation_ids.clone())
            .push_bind(record.delivered_at);
    });
    builder
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
    use sqlx::Execute;

    fn sample_record(job_id: i64, outcome: &str) -> DialogTurnOutcomeRecord {
        DialogTurnOutcomeRecord {
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
            delivery_state: "legacy_unverified".to_owned(),
            outbox_operation_ids: Vec::new(),
            delivered_at: None,
        }
    }

    #[test]
    fn insert_query_emits_values_keyword_exactly_once() {
        let records = vec![
            sample_record(1, TURN_OUTCOME_SENT),
            sample_record(2, TURN_OUTCOME_TERMINAL_FAILED),
        ];
        let mut builder = build_insert_turn_outcomes(&records);
        let sql = builder.build().sql().as_str().to_owned();
        assert_eq!(
            sql.matches("VALUES").count(),
            1,
            "insert must stay syntactically valid: {sql}"
        );
        assert_eq!(sql.matches("($").count(), 2, "one tuple per record: {sql}");
        assert!(sql.contains("delivery_state"));
        assert!(sql.contains("outbox_operation_ids"));
        assert!(sql.contains("delivered_at"));
    }

    #[test]
    fn buffer_serves_filtered_outcomes_most_recent_first() {
        let buffer = RuntimeTurnOutcomeBuffer::new(8);
        for (job_id, outcome) in [(1, TURN_OUTCOME_SENT), (2, TURN_OUTCOME_TERMINAL_FAILED)] {
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
                delivery_state: "legacy_unverified".to_owned(),
                outbox_operation_ids: Vec::new(),
                delivered_at: None,
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

        let failed = buffer
            .turn_outcomes(RuntimeTurnOutcomesFilter {
                outcome: TURN_OUTCOME_TERMINAL_FAILED.to_owned(),
                limit: 10,
                ..RuntimeTurnOutcomesFilter::default()
            })
            .expect("outcomes");
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].job_id, 2);

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
