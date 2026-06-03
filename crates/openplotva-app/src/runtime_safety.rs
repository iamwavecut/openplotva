//! Runtime API WhiteCircle safety-check reader backed by Postgres.

use std::time::Duration;

use openplotva_server::{
    RuntimeSafetyCheckConnectionData, RuntimeSafetyCheckData, RuntimeSafetyCheckReader,
    RuntimeSafetyCheckReaderFuture, RuntimeSafetyChecksFilter,
};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, Row};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

const WHITE_CIRCLE_EVENT_WRITER_CHANNEL_CAPACITY: usize = 10_000;
const WHITE_CIRCLE_EVENT_WRITER_BATCH_SIZE: usize = 100;
const WHITE_CIRCLE_EVENT_WRITER_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const SQL_INSERT_RUNTIME_SAFETY_CHECKS_PREFIX: &str = r#"INSERT INTO whitecircle_checks (
    created_at,
    source,
    flow,
    mode,
    chat_id,
    thread_id,
    message_id,
    user_id,
    deployment_id,
    external_session_id,
    request_messages,
    flagged,
    internal_session_id,
    policies,
    response_json,
    duration_ms,
    error
)"#;

const SQL_LIST_RUNTIME_SAFETY_CHECKS: &str = r#"
SELECT id,
       created_at,
       source,
       flow,
       mode,
       chat_id,
       thread_id,
       message_id,
       user_id,
       deployment_id,
       external_session_id,
       COALESCE(request_messages::text, '') AS request_messages,
       flagged,
       internal_session_id,
       COALESCE(policies::text, '') AS policies,
       COALESCE(response_json::text, '') AS response_json,
       duration_ms,
       error
FROM whitecircle_checks
WHERE ($1::bool IS NULL OR flagged = $1::bool)
  AND (
      $2::text IS NULL
      OR source ILIKE '%' || $2::text || '%'
      OR COALESCE(flow, '') ILIKE '%' || $2::text || '%'
      OR COALESCE(mode, '') ILIKE '%' || $2::text || '%'
      OR CAST(COALESCE(chat_id, 0) AS text) ILIKE '%' || $2::text || '%'
      OR CAST(COALESCE(user_id, 0) AS text) ILIKE '%' || $2::text || '%'
      OR CAST(COALESCE(message_id, 0) AS text) ILIKE '%' || $2::text || '%'
      OR COALESCE(external_session_id, '') ILIKE '%' || $2::text || '%'
      OR COALESCE(internal_session_id, '') ILIKE '%' || $2::text || '%'
      OR COALESCE(error, '') ILIKE '%' || $2::text || '%'
  )
ORDER BY created_at DESC
LIMIT $3
OFFSET $4"#;

/// SQLx-backed runtime API safety-check reader.
#[derive(Clone, Debug)]
pub struct PostgresRuntimeSafetyCheckReader {
    pool: PgPool,
}

/// SQLx-backed WhiteCircle event recorder.
#[derive(Clone, Debug)]
pub struct PostgresWhiteCircleCheckEventRecorder {
    sender: mpsc::Sender<openplotva_llm::whitecircle::WhiteCircleCheckEvent>,
}

impl PostgresRuntimeSafetyCheckReader {
    /// Build a reader over an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl PostgresWhiteCircleCheckEventRecorder {
    /// Build a recorder and background buffered writer over an existing Postgres pool.
    pub fn spawn(pool: PgPool, stop: watch::Receiver<bool>) -> (Self, JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(WHITE_CIRCLE_EVENT_WRITER_CHANNEL_CAPACITY);
        let handle = tokio::spawn(run_whitecircle_check_event_writer(
            pool,
            receiver,
            stop,
            WHITE_CIRCLE_EVENT_WRITER_BATCH_SIZE,
            WHITE_CIRCLE_EVENT_WRITER_FLUSH_INTERVAL,
        ));
        (Self { sender }, handle)
    }
}

impl openplotva_llm::whitecircle::WhiteCircleCheckEventRecorder
    for PostgresWhiteCircleCheckEventRecorder
{
    fn enqueue_white_circle_check(
        &self,
        event: openplotva_llm::whitecircle::WhiteCircleCheckEvent,
    ) {
        if let Err(error) = self.sender.try_send(event) {
            match error {
                mpsc::error::TrySendError::Full(event) => {
                    tracing::warn!(
                        source = %event.source,
                        "dropping whitecircle check because writer channel is full"
                    );
                }
                mpsc::error::TrySendError::Closed(event) => {
                    tracing::debug!(
                        source = %event.source,
                        "dropping whitecircle check because writer is stopped"
                    );
                }
            }
        }
    }
}

impl RuntimeSafetyCheckReader for PostgresRuntimeSafetyCheckReader {
    fn safety_checks<'a>(
        &'a self,
        filter: RuntimeSafetyChecksFilter,
    ) -> RuntimeSafetyCheckReaderFuture<'a> {
        Box::pin(async move { self.list_safety_checks(filter).await.map_err(error_text) })
    }
}

impl PostgresRuntimeSafetyCheckReader {
    async fn list_safety_checks(
        &self,
        filter: RuntimeSafetyChecksFilter,
    ) -> Result<RuntimeSafetyCheckConnectionData, sqlx::Error> {
        let q = optional_search(&filter.q);
        let rows = sqlx::query(SQL_LIST_RUNTIME_SAFETY_CHECKS)
            .bind(filter.flagged)
            .bind(q.as_deref())
            .bind(filter.limit)
            .bind(filter.offset)
            .fetch_all(&self.pool)
            .await?;
        let items = rows
            .into_iter()
            .map(runtime_safety_check_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RuntimeSafetyCheckConnectionData {
            count: items.len().min(i32::MAX as usize) as i32,
            offset: filter.offset,
            limit: filter.limit,
            items,
        })
    }
}

async fn run_whitecircle_check_event_writer(
    pool: PgPool,
    mut receiver: mpsc::Receiver<openplotva_llm::whitecircle::WhiteCircleCheckEvent>,
    mut stop: watch::Receiver<bool>,
    batch_size: usize,
    flush_interval: Duration,
) {
    let batch_size = batch_size.max(1);
    let mut pending = Vec::with_capacity(batch_size);
    let mut interval = tokio::time::interval(flush_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        if *stop.borrow() {
            drain_and_flush_whitecircle_check_event_batches(
                &pool,
                &mut receiver,
                &mut pending,
                batch_size,
            )
            .await;
            break;
        }

        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    drain_and_flush_whitecircle_check_event_batches(
                        &pool,
                        &mut receiver,
                        &mut pending,
                        batch_size,
                    )
                    .await;
                    break;
                }
            }
            maybe_event = receiver.recv() => {
                let Some(event) = maybe_event else {
                    flush_whitecircle_check_event_batch(&pool, &mut pending).await;
                    break;
                };
                pending.push(event);
                if pending.len() >= batch_size {
                    flush_whitecircle_check_event_batch(&pool, &mut pending).await;
                }
            }
            _ = interval.tick() => {
                flush_whitecircle_check_event_batch(&pool, &mut pending).await;
            }
        }
    }
}

async fn drain_and_flush_whitecircle_check_event_batches(
    pool: &PgPool,
    receiver: &mut mpsc::Receiver<openplotva_llm::whitecircle::WhiteCircleCheckEvent>,
    pending: &mut Vec<openplotva_llm::whitecircle::WhiteCircleCheckEvent>,
    batch_size: usize,
) {
    loop {
        drain_whitecircle_check_event_channel(receiver, pending, batch_size);
        if pending.is_empty() {
            break;
        }
        flush_whitecircle_check_event_batch(pool, pending).await;
    }
}

fn drain_whitecircle_check_event_channel(
    receiver: &mut mpsc::Receiver<openplotva_llm::whitecircle::WhiteCircleCheckEvent>,
    pending: &mut Vec<openplotva_llm::whitecircle::WhiteCircleCheckEvent>,
    batch_size: usize,
) {
    while pending.len() < batch_size {
        match receiver.try_recv() {
            Ok(event) => pending.push(event),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        }
    }
}

async fn flush_whitecircle_check_event_batch(
    pool: &PgPool,
    pending: &mut Vec<openplotva_llm::whitecircle::WhiteCircleCheckEvent>,
) {
    if pending.is_empty() {
        return;
    }
    let batch = std::mem::take(pending);
    if let Err(error) = insert_white_circle_check_events(pool, &batch).await {
        let sources = batch
            .iter()
            .map(|event| event.source.as_str())
            .collect::<Vec<_>>()
            .join(",");
        tracing::warn!(%error, sources, count = batch.len(), "failed to insert whitecircle check batch");
    }
}

pub async fn insert_white_circle_check_events(
    pool: &PgPool,
    events: &[openplotva_llm::whitecircle::WhiteCircleCheckEvent],
) -> Result<(), sqlx::Error> {
    if events.is_empty() {
        return Ok(());
    }
    let mut builder = whitecircle_check_event_insert_builder(events);
    builder.build().execute(pool).await?;
    Ok(())
}

fn whitecircle_check_event_insert_builder(
    events: &[openplotva_llm::whitecircle::WhiteCircleCheckEvent],
) -> QueryBuilder<Postgres> {
    let mut builder = QueryBuilder::new(SQL_INSERT_RUNTIME_SAFETY_CHECKS_PREFIX);
    builder.push(" ");
    builder.push_values(events.iter(), |mut row, event| {
        row.push_bind(event.created_at)
            .push_bind(event.source.clone())
            .push_bind(event.flow.clone())
            .push_bind(event.mode.clone())
            .push_bind(event.chat_id)
            .push_bind(event.thread_id)
            .push_bind(event.message_id)
            .push_bind(event.user_id)
            .push_bind(event.deployment_id.clone())
            .push_bind(event.external_session_id.clone())
            .push_bind(sqlx::types::Json(event.request_messages.clone()))
            .push_bind(event.flagged)
            .push_bind(event.internal_session_id.clone())
            .push_bind(event.policies.clone().map(sqlx::types::Json))
            .push_bind(event.response_json.clone().map(sqlx::types::Json))
            .push_bind(event.duration_ms)
            .push_bind(event.error.clone());
    });
    builder
}

#[cfg(test)]
fn whitecircle_check_batch_insert_sql_for_test(
    events: &[openplotva_llm::whitecircle::WhiteCircleCheckEvent],
) -> String {
    whitecircle_check_event_insert_builder(events).into_string()
}

#[cfg(test)]
fn whitecircle_check_shutdown_batch_sizes_for_test(
    mut receiver: mpsc::Receiver<openplotva_llm::whitecircle::WhiteCircleCheckEvent>,
    batch_size: usize,
) -> Vec<usize> {
    let batch_size = batch_size.max(1);
    let mut pending = Vec::with_capacity(batch_size);
    let mut batch_sizes = Vec::new();
    loop {
        drain_whitecircle_check_event_channel(&mut receiver, &mut pending, batch_size);
        if pending.is_empty() {
            break;
        }
        batch_sizes.push(pending.len());
        pending.clear();
    }
    batch_sizes
}

fn runtime_safety_check_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<RuntimeSafetyCheckData, sqlx::Error> {
    let created_at: OffsetDateTime = row.try_get("created_at")?;
    let request_messages: String = row.try_get("request_messages")?;
    let policies: String = row.try_get("policies")?;
    let response_json: String = row.try_get("response_json")?;
    Ok(RuntimeSafetyCheckData {
        id: row.try_get("id")?,
        created_at: created_at
            .format(&Rfc3339)
            .unwrap_or_else(|_| created_at.to_string()),
        source: row.try_get("source")?,
        flow: row.try_get("flow")?,
        mode: row.try_get("mode")?,
        chat_id: row.try_get("chat_id")?,
        thread_id: row.try_get("thread_id")?,
        message_id: row.try_get("message_id")?,
        user_id: row.try_get("user_id")?,
        deployment_id: row.try_get("deployment_id")?,
        external_session_id: row.try_get("external_session_id")?,
        request_messages: runtime_json_from_payload(&request_messages),
        flagged: row.try_get("flagged")?,
        internal_session_id: row.try_get("internal_session_id")?,
        policies: runtime_json_from_payload(&policies),
        response_json: runtime_json_from_payload(&response_json),
        duration_ms: row.try_get("duration_ms")?,
        error: row.try_get("error")?,
    })
}

fn optional_search(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn runtime_json_from_payload(payload: &str) -> Option<Value> {
    if payload.is_empty() {
        return None;
    }
    Some(serde_json::from_str(payload).unwrap_or_else(|_| Value::String(payload.to_owned())))
}

fn error_text<E: std::fmt::Display>(error: E) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{optional_search, runtime_json_from_payload};

    #[test]
    fn runtime_safety_payload_helpers_match_go_json_and_search_defaults() {
        assert_eq!(optional_search(" risk "), Some("risk".to_owned()));
        assert_eq!(optional_search("   "), None);
        assert_eq!(runtime_json_from_payload(""), None);
        assert_eq!(
            runtime_json_from_payload(r#"[{"role":"user"}]"#),
            Some(json!([{"role": "user"}]))
        );
        assert_eq!(
            runtime_json_from_payload("not-json"),
            Some(json!("not-json"))
        );
    }

    #[test]
    fn whitecircle_check_batch_insert_sql_uses_one_multi_row_statement() {
        let first = whitecircle_event("dialog");
        let second = whitecircle_event("fallback");

        let sql = super::whitecircle_check_batch_insert_sql_for_test(&[first, second]);

        assert!(sql.starts_with("INSERT INTO whitecircle_checks"));
        assert!(sql.contains("), ("));
        assert!(!sql.contains("VALUES (\n    $1,"));
    }

    #[test]
    fn whitecircle_check_shutdown_drain_flushes_all_queued_batches() {
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        for index in 0..5 {
            sender
                .try_send(whitecircle_event(&format!("dialog-{index}")))
                .expect("test channel should accept all queued events");
        }
        drop(sender);

        let batch_sizes = super::whitecircle_check_shutdown_batch_sizes_for_test(receiver, 2);

        assert_eq!(batch_sizes, vec![2, 2, 1]);
    }

    fn whitecircle_event(source: &str) -> openplotva_llm::whitecircle::WhiteCircleCheckEvent {
        openplotva_llm::whitecircle::WhiteCircleCheckEvent {
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            source: source.to_owned(),
            mode: None,
            flow: None,
            chat_id: None,
            thread_id: None,
            message_id: None,
            user_id: None,
            deployment_id: "whitecircle".to_owned(),
            external_session_id: None,
            request_messages: json!([]),
            flagged: None,
            internal_session_id: None,
            policies: None,
            response_json: None,
            duration_ms: 0,
            error: None,
        }
    }
}
