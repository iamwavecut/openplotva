//! Runtime API pending virtual-message operation reader backed by Postgres.

use openplotva_server::{
    RuntimePendingOpData, RuntimePendingOpsReader, RuntimePendingOpsReaderFuture,
};
use serde_json::Value;
use sqlx::{PgPool, Row};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const SQL_LIST_RUNTIME_PENDING_OPS: &str = r#"
SELECT id,
       vmsg_id,
       chat_id,
       op,
       COALESCE(payload::text, '') AS payload,
       status,
       created_at,
       attempts
FROM message_ops_queue
WHERE status = 'pending'
ORDER BY created_at ASC
LIMIT $1"#;

/// SQLx-backed runtime API pending-op reader.
#[derive(Clone, Debug)]
pub struct PostgresRuntimePendingOpsReader {
    pool: PgPool,
}

impl PostgresRuntimePendingOpsReader {
    /// Build a reader over an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl RuntimePendingOpsReader for PostgresRuntimePendingOpsReader {
    fn pending_ops<'a>(&'a self, limit: i32) -> RuntimePendingOpsReaderFuture<'a> {
        Box::pin(async move { self.list_pending_ops(limit).await.map_err(error_text) })
    }
}

impl PostgresRuntimePendingOpsReader {
    async fn list_pending_ops(&self, limit: i32) -> Result<Vec<RuntimePendingOpData>, sqlx::Error> {
        let rows = sqlx::query(SQL_LIST_RUNTIME_PENDING_OPS)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(runtime_pending_op_from_row).collect()
    }
}

fn runtime_pending_op_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<RuntimePendingOpData, sqlx::Error> {
    let payload: String = row.try_get("payload")?;
    let created_at: OffsetDateTime = row.try_get("created_at")?;
    Ok(RuntimePendingOpData {
        id: row.try_get("id")?,
        vmsg_id: row.try_get("vmsg_id")?,
        chat_id: row.try_get("chat_id")?,
        op: row.try_get("op")?,
        payload: runtime_json_from_payload(&payload),
        status: row.try_get("status")?,
        created_at: created_at
            .format(&Rfc3339)
            .unwrap_or_else(|_| created_at.to_string()),
        attempts: row.try_get("attempts")?,
    })
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

    use super::runtime_json_from_payload;

    #[test]
    fn runtime_pending_op_payload_json_matches_go_null_and_string_fallback() {
        assert_eq!(runtime_json_from_payload(""), None);
        assert_eq!(
            runtime_json_from_payload(r#"{"text":"edited","parse_mode":"HTML"}"#),
            Some(json!({"text": "edited", "parse_mode": "HTML"}))
        );
        assert_eq!(
            runtime_json_from_payload("not-json"),
            Some(json!("not-json"))
        );
    }
}
