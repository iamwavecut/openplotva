//! Runtime API WhiteCircle safety-check reader backed by Postgres.

use openplotva_server::{
    RuntimeSafetyCheckConnectionData, RuntimeSafetyCheckData, RuntimeSafetyCheckReader,
    RuntimeSafetyCheckReaderFuture, RuntimeSafetyChecksFilter,
};
use serde_json::Value;
use sqlx::{PgPool, Row};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const SQL_INSERT_RUNTIME_SAFETY_CHECK: &str = r#"
INSERT INTO whitecircle_checks (
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
) VALUES (
    $1,
    $2,
    $3::text,
    $4::text,
    $5::bigint,
    $6::int,
    $7::int,
    $8::bigint,
    $9,
    $10::text,
    $11::jsonb,
    $12::bool,
    $13::text,
    $14::jsonb,
    $15::jsonb,
    $16,
    $17::text
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
    pool: PgPool,
}

impl PostgresRuntimeSafetyCheckReader {
    /// Build a reader over an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl PostgresWhiteCircleCheckEventRecorder {
    /// Build a recorder over an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl openplotva_llm::whitecircle::WhiteCircleCheckEventRecorder
    for PostgresWhiteCircleCheckEventRecorder
{
    fn enqueue_white_circle_check(
        &self,
        event: openplotva_llm::whitecircle::WhiteCircleCheckEvent,
    ) {
        let pool = self.pool.clone();
        let source = event.source.clone();
        tokio::spawn(async move {
            if let Err(error) = insert_white_circle_check_event(&pool, &event).await {
                tracing::warn!(%error, source, "failed to insert whitecircle check");
            }
        });
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

pub async fn insert_white_circle_check_event(
    pool: &PgPool,
    event: &openplotva_llm::whitecircle::WhiteCircleCheckEvent,
) -> Result<(), sqlx::Error> {
    sqlx::query(SQL_INSERT_RUNTIME_SAFETY_CHECK)
        .bind(event.created_at)
        .bind(&event.source)
        .bind(event.flow.as_deref())
        .bind(event.mode.as_deref())
        .bind(event.chat_id)
        .bind(event.thread_id)
        .bind(event.message_id)
        .bind(event.user_id)
        .bind(&event.deployment_id)
        .bind(event.external_session_id.as_deref())
        .bind(sqlx::types::Json(event.request_messages.clone()))
        .bind(event.flagged)
        .bind(event.internal_session_id.as_deref())
        .bind(event.policies.clone().map(sqlx::types::Json))
        .bind(event.response_json.clone().map(sqlx::types::Json))
        .bind(event.duration_ms)
        .bind(event.error.as_deref())
        .execute(pool)
        .await?;
    Ok(())
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
}
