use serde::Serialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use time::OffsetDateTime;

use crate::StorageError;

const MAX_INCIDENT_PAGE: i64 = 100;
const MAX_TECHNICAL_NAME_CHARS: usize = 160;
const MAX_ROUTE_ATTEMPTS: i64 = 12;
const MAX_QUEUE_COUNTERS: i64 = 64;

const CAPTURE_CANDIDATES_SQL: &str = r#"
SELECT
    event.id,
    event.created_at,
    event.event_type,
    event.workflow_key,
    provider.name AS provider_name,
    model.model_name,
    event.queue_name,
    event.job_id,
    event.chat_id,
    event.message_id,
    event.detail->>'last_retryable_reason' AS last_retryable_reason,
    event.detail->>'retryable_reason' AS retryable_reason,
    event.detail->>'reason' AS reason
FROM llm_routing_events AS event
LEFT JOIN llm_providers AS provider ON provider.id = event.provider_id
LEFT JOIN provider_models AS model
  ON model.id = event.model_id AND model.provider_id = event.provider_id
WHERE event.created_at >= $1 - interval '5 minutes'
  AND event.created_at <= $1
  AND event.event_type IN (
      'route_unavailable',
      'no_candidates',
      'all_attempts_exhausted'
  )
  AND (
      event.workflow_key IN ('dialog', 'vision', 'asr', 'youtube_summary', 'music_generation')
      OR event.workflow_key LIKE 'image\_generation%' ESCAPE '\'
      OR event.workflow_key LIKE 'image\_edit%' ESCAPE '\'
      OR event.workflow_key LIKE 'agentic\_%' ESCAPE '\'
  )
  AND (event.job_id IS NOT NULL OR event.chat_id IS NOT NULL OR event.user_id IS NOT NULL)
  AND COALESCE(event.detail->>'admin_actionable', 'true') <> 'false'
  AND NOT (
      event.event_type = 'all_attempts_exhausted'
      AND COALESCE(event.detail->>'admin_actionable', '') <> 'true'
      AND COALESCE(event.detail->>'failed_attempts', '') = '1'
      AND COALESCE(event.detail->>'last_retryable_reason', '') <> ''
  )
ORDER BY event.id ASC
"#;

const INSERT_INCIDENT_SQL: &str = r#"
INSERT INTO maintenance_incidents (
    source_event_id,
    source_workflow_key,
    source_job_id,
    source_chat_id,
    source_message_id,
    source_created_at,
    signature,
    first_seen,
    last_seen,
    snapshot,
    captured_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $6, $6, $8, $9)
ON CONFLICT (source_event_id) DO NOTHING
RETURNING id
"#;

const LIST_INCIDENTS_SQL: &str = r#"
SELECT id, signature, first_seen, last_seen, snapshot
FROM maintenance_incidents
WHERE id > $1
ORDER BY id ASC
LIMIT $2
"#;

const LOAD_INCIDENT_SOURCE_SQL: &str = r#"
SELECT
    source_workflow_key,
    source_job_id,
    source_chat_id,
    source_message_id,
    source_created_at
FROM maintenance_incidents
WHERE id = $1
"#;

const ROUTE_ATTEMPTS_SQL: &str = r#"
SELECT
    event.created_at,
    event.event_type,
    provider.name AS provider_name,
    model.model_name,
    event.detail->>'retryable_reason' AS retryable_reason,
    event.detail->>'reason' AS reason
FROM llm_routing_events AS event
LEFT JOIN llm_providers AS provider ON provider.id = event.provider_id
LEFT JOIN provider_models AS model
  ON model.id = event.model_id AND model.provider_id = event.provider_id
WHERE event.event_type IN ('attempt_failed', 'circuit_open_exhaustion', 'capacity_unavailable')
  AND event.workflow_key = $1
  AND event.chat_id = $2
  AND event.message_id = $3
  AND event.created_at BETWEEN $4 - interval '10 minutes' AND $4
ORDER BY event.created_at DESC, event.id DESC
LIMIT $5
"#;

const TASK_STATE_SQL: &str = r#"
SELECT queue_name, status, job_type, created_at, started_at, completed_at
FROM taskman_jobs
WHERE id = $1 AND deleted_at IS NULL
"#;

const QUEUE_COUNTERS_SQL: &str = r#"
SELECT queue_name, status, COUNT(*)::BIGINT AS count
FROM taskman_jobs
WHERE deleted_at IS NULL
GROUP BY queue_name, status
ORDER BY count DESC, queue_name ASC, status ASC
LIMIT $1
"#;

const DATABASE_COUNTERS_SQL: &str = r#"
SELECT
    COUNT(*)::BIGINT AS connections_total,
    COUNT(*) FILTER (WHERE state = 'active')::BIGINT AS connections_active,
    COUNT(*) FILTER (WHERE state = 'idle')::BIGINT AS connections_idle,
    COUNT(*) FILTER (WHERE wait_event IS NOT NULL)::BIGINT AS connections_waiting,
    COALESCE((
        SELECT COUNT(*)::BIGINT
        FROM pg_locks AS lock
        JOIN pg_stat_activity AS activity ON activity.pid = lock.pid
        WHERE activity.datname = current_database() AND lock.granted
    ), 0) AS locks_granted,
    COALESCE((
        SELECT COUNT(*)::BIGINT
        FROM pg_locks AS lock
        JOIN pg_stat_activity AS activity ON activity.pid = lock.pid
        WHERE activity.datname = current_database() AND NOT lock.granted
    ), 0) AS locks_waiting
FROM pg_stat_activity
WHERE datname = current_database()
"#;

/// Durable, privacy-preserving incident projection used by the maintenance controller.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MaintenanceIncident {
    pub id: i64,
    pub signature: String,
    #[serde(with = "time::serde::timestamp")]
    pub first_seen: OffsetDateTime,
    #[serde(with = "time::serde::timestamp")]
    pub last_seen: OffsetDateTime,
    pub snapshot: Value,
}

/// Postgres-backed maintenance incident outbox and evidence reader.
#[derive(Clone, Debug)]
pub struct MaintenanceStore {
    pool: PgPool,
}

impl MaintenanceStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Capture every fresh actionable user-facing terminal routing event once.
    pub async fn capture(&self, now: OffsetDateTime) -> Result<u64, StorageError> {
        let mut transaction = self.pool.begin().await?;
        let candidates = sqlx::query(CAPTURE_CANDIDATES_SQL)
            .bind(now)
            .fetch_all(&mut *transaction)
            .await?;
        let mut captured = 0_u64;
        for row in candidates {
            let source_workflow_key: String = row.try_get("workflow_key")?;
            let raw = RawIncident {
                event_type: row.try_get("event_type")?,
                workflow_key: source_workflow_key.clone(),
                provider_name: row.try_get("provider_name")?,
                model_name: row.try_get("model_name")?,
                queue_name: row.try_get("queue_name")?,
                detail: json!({
                    "last_retryable_reason": row
                        .try_get::<Option<String>, _>("last_retryable_reason")?,
                    "retryable_reason": row.try_get::<Option<String>, _>("retryable_reason")?,
                    "reason": row.try_get::<Option<String>, _>("reason")?
                }),
            };
            let Some(sanitized) = sanitize_incident(raw) else {
                continue;
            };
            let source_event_id: i64 = row.try_get("id")?;
            let source_created_at: OffsetDateTime = row.try_get("created_at")?;
            let inserted = sqlx::query_scalar::<_, i64>(INSERT_INCIDENT_SQL)
                .bind(source_event_id)
                .bind(&source_workflow_key)
                .bind(row.try_get::<Option<i64>, _>("job_id")?)
                .bind(row.try_get::<Option<i64>, _>("chat_id")?)
                .bind(row.try_get::<Option<i32>, _>("message_id")?)
                .bind(source_created_at)
                .bind(&sanitized.signature)
                .bind(&sanitized.snapshot)
                .bind(now)
                .fetch_optional(&mut *transaction)
                .await?;
            captured += u64::from(inserted.is_some());
        }
        transaction.commit().await?;
        Ok(captured)
    }

    /// List incidents after an exclusive ascending id cursor.
    pub async fn incidents(
        &self,
        after: i64,
        limit: i64,
    ) -> Result<Vec<MaintenanceIncident>, StorageError> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(LIST_INCIDENTS_SQL)
            .bind(after.max(0))
            .bind(limit.min(MAX_INCIDENT_PAGE))
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|row| {
                Ok(MaintenanceIncident {
                    id: row.try_get("id")?,
                    signature: row.try_get("signature")?,
                    first_seen: row.try_get("first_seen")?,
                    last_seen: row.try_get("last_seen")?,
                    snapshot: row.try_get("snapshot")?,
                })
            })
            .collect()
    }

    /// Return bounded sanitized evidence for one incident.
    pub async fn evidence(&self, incident_id: i64) -> Result<Option<Value>, StorageError> {
        let Some(source) = sqlx::query(LOAD_INCIDENT_SOURCE_SQL)
            .bind(incident_id)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };
        let workflow_key: String = source.try_get("source_workflow_key")?;
        let job_id: Option<i64> = source.try_get("source_job_id")?;
        let chat_id: Option<i64> = source.try_get("source_chat_id")?;
        let message_id: Option<i32> = source.try_get("source_message_id")?;
        let source_created_at: OffsetDateTime = source.try_get("source_created_at")?;

        let route_attempts = self
            .route_attempt_evidence(
                incident_id,
                &workflow_key,
                chat_id,
                message_id,
                source_created_at,
            )
            .await;
        let task = self.task_evidence(incident_id, job_id).await;
        let queues = self.queue_evidence().await;
        let database = self.database_evidence().await;

        Ok(Some(json!({
            "route_attempts": route_attempts,
            "task": task,
            "queues": queues,
            "database": database
        })))
    }

    async fn route_attempt_evidence(
        &self,
        incident_id: i64,
        workflow_key: &str,
        chat_id: Option<i64>,
        message_id: Option<i32>,
        source_created_at: OffsetDateTime,
    ) -> Value {
        let (Some(chat_id), Some(message_id)) = (chat_id, message_id) else {
            return unavailable("not_linked");
        };
        let rows = match sqlx::query(ROUTE_ATTEMPTS_SQL)
            .bind(workflow_key)
            .bind(chat_id)
            .bind(message_id)
            .bind(source_created_at)
            .bind(MAX_ROUTE_ATTEMPTS)
            .fetch_all(&self.pool)
            .await
        {
            Ok(rows) => rows,
            Err(_) => return unavailable("query_failed"),
        };
        let items = rows
            .into_iter()
            .enumerate()
            .filter_map(|(index, row)| {
                let event_type =
                    explanatory_event_type(row.try_get::<String, _>("event_type").ok()?)?;
                let provider = row
                    .try_get::<Option<String>, _>("provider_name")
                    .ok()
                    .flatten()
                    .and_then(technical_name);
                let model = row
                    .try_get::<Option<String>, _>("model_name")
                    .ok()
                    .flatten()
                    .and_then(technical_name);
                let at = row.try_get::<OffsetDateTime, _>("created_at").ok()?;
                let reason = ["retryable_reason", "reason"]
                    .into_iter()
                    .find_map(|column| {
                        row.try_get::<Option<String>, _>(column)
                            .ok()
                            .flatten()
                            .and_then(known_reason)
                    });
                let mut item = Map::new();
                item.insert(
                    "reference".to_owned(),
                    Value::String(format!("incident:{incident_id}:attempt:{}", index + 1)),
                );
                item.insert("event_type".to_owned(), Value::String(event_type));
                if let Some(provider) = provider {
                    item.insert("provider".to_owned(), Value::String(provider));
                }
                if let Some(model) = model {
                    item.insert("model".to_owned(), Value::String(model));
                }
                if let Some(reason) = reason {
                    item.insert("reason".to_owned(), Value::String(reason));
                }
                item.insert("at".to_owned(), Value::from(at.unix_timestamp()));
                Some(Value::Object(item))
            })
            .collect::<Vec<_>>();
        if items.is_empty() {
            return unavailable("no_matching_evidence");
        }
        json!({"available": true, "items": items})
    }

    async fn task_evidence(&self, incident_id: i64, job_id: Option<i64>) -> Value {
        let Some(job_id) = job_id else {
            return unavailable("not_linked");
        };
        let row = match sqlx::query(TASK_STATE_SQL)
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await
        {
            Ok(Some(row)) => row,
            Ok(None) => return unavailable("not_found"),
            Err(_) => return unavailable("query_failed"),
        };
        let Some(queue) = row
            .try_get::<String, _>("queue_name")
            .ok()
            .and_then(technical_name)
        else {
            return unavailable("invalid_projection");
        };
        let Some(state) = row.try_get::<String, _>("status").ok().and_then(task_state) else {
            return unavailable("invalid_projection");
        };
        let Some(kind) = row
            .try_get::<String, _>("job_type")
            .ok()
            .and_then(technical_name)
        else {
            return unavailable("invalid_projection");
        };
        let timestamp = |column| {
            row.try_get::<Option<OffsetDateTime>, _>(column)
                .ok()
                .flatten()
                .map(OffsetDateTime::unix_timestamp)
        };
        json!({
            "available": true,
            "reference": format!("incident:{incident_id}:task"),
            "queue": queue,
            "state": state,
            "kind": kind,
            "created_at": timestamp("created_at"),
            "started_at": timestamp("started_at"),
            "completed_at": timestamp("completed_at")
        })
    }

    async fn queue_evidence(&self) -> Value {
        let rows = match sqlx::query(QUEUE_COUNTERS_SQL)
            .bind(MAX_QUEUE_COUNTERS)
            .fetch_all(&self.pool)
            .await
        {
            Ok(rows) => rows,
            Err(_) => return unavailable("query_failed"),
        };
        let counters = rows
            .into_iter()
            .filter_map(|row| {
                let queue = technical_name(row.try_get::<String, _>("queue_name").ok()?)?;
                let state = task_state(row.try_get::<String, _>("status").ok()?)?;
                let count = row.try_get::<i64, _>("count").ok()?.max(0);
                Some(json!({"queue": queue, "state": state, "count": count}))
            })
            .collect::<Vec<_>>();
        json!({"available": true, "counters": counters})
    }

    async fn database_evidence(&self) -> Value {
        let row = match sqlx::query(DATABASE_COUNTERS_SQL)
            .fetch_one(&self.pool)
            .await
        {
            Ok(row) => row,
            Err(_) => return unavailable("query_failed"),
        };
        let counter = |column| row.try_get::<i64, _>(column).unwrap_or_default().max(0);
        json!({
            "available": true,
            "pool": {
                "size": self.pool.size(),
                "idle": self.pool.num_idle()
            },
            "connections": {
                "total": counter("connections_total"),
                "active": counter("connections_active"),
                "idle": counter("connections_idle"),
                "waiting": counter("connections_waiting")
            },
            "locks": {
                "granted": counter("locks_granted"),
                "waiting": counter("locks_waiting")
            }
        })
    }
}

#[derive(Clone, Debug)]
struct RawIncident {
    event_type: String,
    workflow_key: String,
    provider_name: Option<String>,
    model_name: Option<String>,
    queue_name: Option<String>,
    detail: Value,
}

#[derive(Clone, Debug)]
struct SanitizedIncident {
    signature: String,
    snapshot: Value,
}

fn sanitize_incident(raw: RawIncident) -> Option<SanitizedIncident> {
    let event_type = actionable_event_type(raw.event_type)?;
    let workflow_key = user_facing_workflow(raw.workflow_key)?;
    let provider = raw.provider_name.and_then(technical_name);
    let model = raw.model_name.and_then(technical_name);
    let queue = raw.queue_name.and_then(technical_name);
    let reason = ["last_retryable_reason", "retryable_reason", "reason"]
        .into_iter()
        .find_map(|key| {
            raw.detail
                .get(key)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .and_then(known_reason)
        });

    let mut route = Map::new();
    if let Some(value) = provider.as_ref() {
        route.insert("provider".to_owned(), Value::String(value.clone()));
    }
    if let Some(value) = model.as_ref() {
        route.insert("model".to_owned(), Value::String(value.clone()));
    }
    let mut snapshot = Map::new();
    snapshot.insert("event_type".to_owned(), Value::String(event_type.clone()));
    snapshot.insert("workflow".to_owned(), Value::String(workflow_key.clone()));
    snapshot.insert("route".to_owned(), Value::Object(route));
    if let Some(value) = queue.as_ref() {
        snapshot.insert("queue".to_owned(), Value::String(value.clone()));
    }
    if let Some(value) = reason.as_ref() {
        snapshot.insert("reason".to_owned(), Value::String(value.clone()));
    }

    let canonical = [
        event_type.as_str(),
        workflow_key.as_str(),
        provider.as_deref().unwrap_or(""),
        model.as_deref().unwrap_or(""),
        queue.as_deref().unwrap_or(""),
        reason.as_deref().unwrap_or(""),
    ]
    .join("\u{1f}");
    let signature = hex::encode(Sha256::digest(canonical.as_bytes()));
    Some(SanitizedIncident {
        signature,
        snapshot: Value::Object(snapshot),
    })
}

fn actionable_event_type(value: String) -> Option<String> {
    matches!(
        value.as_str(),
        "route_unavailable" | "no_candidates" | "all_attempts_exhausted"
    )
    .then_some(value)
}

fn explanatory_event_type(value: String) -> Option<String> {
    matches!(
        value.as_str(),
        "attempt_failed" | "circuit_open_exhaustion" | "capacity_unavailable"
    )
    .then_some(value)
}

fn known_reason(value: String) -> Option<String> {
    matches!(
        value.as_str(),
        "provider_unavailable"
            | "provider_overloaded"
            | "capacity_unavailable"
            | "provider_timeout"
            | "provider_protocol_error"
            | "rate_limited"
            | "provider_rate_limited"
            | "attempt_deadline_exceeded"
            | "deadline_exceeded"
            | "missing_route"
            | "zero_selected_attempts"
    )
    .then_some(value)
}

fn user_facing_workflow(value: String) -> Option<String> {
    let value = technical_name(value)?;
    (matches!(
        value.as_str(),
        "dialog" | "vision" | "asr" | "youtube_summary" | "music_generation"
    ) || value.starts_with("image_generation")
        || value.starts_with("image_edit")
        || value.starts_with("agentic_"))
    .then_some(value)
}

fn task_state(value: String) -> Option<String> {
    matches!(
        value.as_str(),
        "pending" | "processing" | "waiting_delivery" | "completed" | "failed" | "cancelled"
    )
    .then_some(value)
}

fn technical_name(value: String) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()
        && value.chars().count() <= MAX_TECHNICAL_NAME_CHARS
        && !value.contains("://")
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || matches!(character, '-' | '_' | '.' | '/' | ':' | '@' | '+')
        }))
    .then(|| value.to_owned())
}

fn unavailable(reason: &'static str) -> Value {
    json!({"available": false, "reason": reason})
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use serde_json::json;
    use sqlx::{PgPool, postgres::PgPoolOptions};
    use time::OffsetDateTime;

    use super::*;

    fn raw_incident() -> RawIncident {
        RawIncident {
            event_type: "all_attempts_exhausted".to_owned(),
            workflow_key: "dialog".to_owned(),
            provider_name: Some("vram-cloud".to_owned()),
            model_name: Some("vram.cloud/qwen3.6-27b".to_owned()),
            queue_name: Some("dialog".to_owned()),
            detail: json!({
                "last_retryable_reason": "provider_unavailable",
                "summary": "user prompt must not escape",
                "url": "https://provider.invalid/private",
                "token": "secret-token"
            }),
        }
    }

    #[test]
    fn sanitized_snapshot_projects_only_allowlisted_technical_fields() {
        let sanitized = sanitize_incident(raw_incident()).expect("allowlisted incident");

        assert_eq!(
            sanitized.snapshot,
            json!({
                "event_type": "all_attempts_exhausted",
                "workflow": "dialog",
                "route": {
                    "provider": "vram-cloud",
                    "model": "vram.cloud/qwen3.6-27b"
                },
                "queue": "dialog",
                "reason": "provider_unavailable"
            })
        );
        let encoded = sanitized.snapshot.to_string();
        for forbidden in [
            "prompt",
            "provider.invalid",
            "secret-token",
            "summary",
            "url",
        ] {
            assert!(
                !encoded.contains(forbidden),
                "leaked {forbidden}: {encoded}"
            );
        }
    }

    #[test]
    fn signature_ignores_reasonable_event_counts_timestamps_and_identities() {
        let first = sanitize_incident(raw_incident()).expect("first incident");
        let mut repeated = raw_incident();
        repeated.detail = json!({
            "last_retryable_reason": "provider_unavailable",
            "occurrences": 99,
            "created_at": 1_788_451_500,
            "user_id": 42,
            "chat_id": -100123,
            "message_id": 789
        });
        let second = sanitize_incident(repeated).expect("second incident");

        assert_eq!(first.signature, second.signature);
        assert_eq!(first.signature.len(), 64);
    }

    #[test]
    fn untrusted_reason_and_non_catalog_route_names_are_omitted() {
        let mut raw = raw_incident();
        raw.provider_name = Some("https://provider.invalid/key=secret".to_owned());
        raw.model_name = Some("model name with user payload".to_owned());
        raw.detail = json!({"reason": "error for user 42: bearer secret"});

        let sanitized = sanitize_incident(raw).expect("technical event remains valid");

        assert_eq!(
            sanitized.snapshot,
            json!({
                "event_type": "all_attempts_exhausted",
                "workflow": "dialog",
                "route": {},
                "queue": "dialog"
            })
        );
    }

    #[test]
    fn first_known_reason_wins_when_an_earlier_detail_value_is_untrusted() {
        let mut raw = raw_incident();
        raw.detail = json!({
            "last_retryable_reason": "Bearer private-value",
            "retryable_reason": "provider_timeout"
        });

        let sanitized = sanitize_incident(raw).expect("technical event remains valid");

        assert_eq!(sanitized.snapshot["reason"], "provider_timeout");
        assert!(!sanitized.snapshot.to_string().contains("private-value"));
    }

    #[test]
    fn non_allowlisted_event_type_is_rejected() {
        let mut raw = raw_incident();
        raw.event_type = "attempt_failed".to_owned();

        assert!(sanitize_incident(raw).is_none());

        let mut intermediate = raw_incident();
        intermediate.event_type = "capacity_unavailable".to_owned();
        assert!(sanitize_incident(intermediate).is_none());
    }

    #[test]
    fn background_workflow_is_rejected() {
        let mut raw = raw_incident();
        raw.workflow_key = "embedding".to_owned();

        assert!(sanitize_incident(raw).is_none());
    }

    #[test]
    fn public_incident_serializes_exact_fields_with_unix_timestamps() {
        let incident = MaintenanceIncident {
            id: 7,
            signature: "a".repeat(64),
            first_seen: OffsetDateTime::from_unix_timestamp(1_700_000_000)
                .expect("first timestamp"),
            last_seen: OffsetDateTime::from_unix_timestamp(1_700_000_030).expect("last timestamp"),
            snapshot: json!({"event_type": "no_candidates"}),
        };

        assert_eq!(
            serde_json::to_value(incident).expect("serialize incident"),
            json!({
                "id": 7,
                "signature": "a".repeat(64),
                "first_seen": 1_700_000_000,
                "last_seen": 1_700_000_030,
                "snapshot": {"event_type": "no_candidates"}
            })
        );
    }

    async fn live_store() -> Result<Option<(PgPool, MaintenanceStore)>, Box<dyn Error>> {
        let Ok(dsn) = std::env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(None);
        };
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        sqlx::raw_sql(
            r#"
            CREATE TEMP TABLE llm_routing_events (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL,
                severity TEXT NOT NULL DEFAULT 'error',
                event_type TEXT NOT NULL,
                workflow_key TEXT NOT NULL,
                provider_id BIGINT,
                model_id BIGINT,
                queue_name TEXT,
                job_id BIGINT,
                chat_id BIGINT,
                user_id BIGINT,
                thread_id INTEGER,
                message_id INTEGER,
                dedupe_key TEXT NOT NULL DEFAULT '',
                summary TEXT NOT NULL DEFAULT '',
                detail JSONB NOT NULL DEFAULT '{}'
            );
            CREATE TEMP TABLE llm_providers (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
            CREATE TEMP TABLE provider_models (
                id BIGINT PRIMARY KEY,
                provider_id BIGINT NOT NULL,
                model_name TEXT NOT NULL
            );
            CREATE TEMP TABLE taskman_jobs (
                id BIGINT PRIMARY KEY,
                queue_name TEXT NOT NULL,
                status TEXT NOT NULL,
                job_type TEXT NOT NULL,
                deleted_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL,
                started_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ
            );
            CREATE TEMP TABLE maintenance_incidents (
                id BIGSERIAL PRIMARY KEY,
                source_event_id BIGINT NOT NULL UNIQUE,
                source_workflow_key TEXT NOT NULL,
                source_job_id BIGINT,
                source_chat_id BIGINT,
                source_message_id INTEGER,
                source_created_at TIMESTAMPTZ NOT NULL,
                signature TEXT NOT NULL,
                first_seen TIMESTAMPTZ NOT NULL,
                last_seen TIMESTAMPTZ NOT NULL,
                snapshot JSONB NOT NULL,
                captured_at TIMESTAMPTZ NOT NULL
            );
            INSERT INTO llm_providers VALUES (8, 'vram-cloud'), (18, 'other-provider');
            INSERT INTO provider_models VALUES
                (9, 8, 'vram.cloud/qwen3.6-27b'),
                (19, 18, 'other-model');
            "#,
        )
        .execute(&pool)
        .await?;
        let store = MaintenanceStore::new(pool.clone());
        Ok(Some((pool, store)))
    }

    async fn insert_terminal(
        pool: &PgPool,
        at: OffsetDateTime,
        job_id: i64,
    ) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar(
            r#"INSERT INTO llm_routing_events (
                created_at, event_type, workflow_key, provider_id, model_id,
                queue_name, job_id, chat_id, user_id, message_id, summary, detail
            ) VALUES (
                $1, 'all_attempts_exhausted', 'dialog', 8, 9,
                'dialog', $2, -100123, 456, 789, 'prompt=must-not-leak',
                '{"failed_attempts":2,"last_retryable_reason":"provider_unavailable","token":"secret"}'
            ) RETURNING id"#,
        )
        .bind(at)
        .bind(job_id)
        .fetch_one(pool)
        .await
    }

    #[tokio::test]
    async fn live_capture_is_idempotent_and_suppresses_non_actionable_events()
    -> Result<(), Box<dyn Error>> {
        let Some((pool, store)) = live_store().await? else {
            return Ok(());
        };
        let now = OffsetDateTime::from_unix_timestamp(1_788_451_500)?;
        insert_terminal(&pool, now - time::Duration::minutes(1), 100).await?;
        sqlx::query(
            r#"INSERT INTO llm_routing_events
                (created_at, event_type, workflow_key, job_id, chat_id, message_id, detail)
               VALUES
                ($1, 'all_attempts_exhausted', 'dialog', 101, 1, 1,
                 '{"failed_attempts":1,"last_retryable_reason":"provider_unavailable"}'),
                ($1, 'route_unavailable', 'dialog', 102, 1, 1,
                 '{"admin_actionable":false}'),
                ($1, 'attempt_failed', 'dialog', 103, 1, 1, '{}'),
                ($1, 'circuit_open_exhaustion', 'dialog', 105, 1, 1, '{}'),
                ($1, 'capacity_unavailable', 'dialog', 106, 1, 1,
                 '{"retryable_reason":"capacity_unavailable"}'),
                ($1, 'router_reload_failed', 'routing', NULL, NULL, NULL, '{}'),
                ($2, 'no_candidates', 'dialog', 104, 1, 1, '{}')"#,
        )
        .bind(now - time::Duration::minutes(1))
        .bind(now - time::Duration::minutes(6))
        .execute(&pool)
        .await?;

        assert_eq!(store.capture(now).await?, 1);
        assert_eq!(store.capture(now).await?, 0);
        let incidents = store.incidents(0, 100).await?;
        assert_eq!(incidents.len(), 1);
        let encoded = serde_json::to_string(&incidents[0])?;
        assert!(!encoded.contains("must-not-leak"));
        assert!(!encoded.contains("secret"));
        assert!(!encoded.contains("-100123"));
        assert!(!encoded.contains("456"));
        assert!(!encoded.contains("789"));
        Ok(())
    }

    #[tokio::test]
    async fn live_successful_probe_and_failover_intermediates_do_not_create_incidents()
    -> Result<(), Box<dyn Error>> {
        let Some((pool, store)) = live_store().await? else {
            return Ok(());
        };
        let now = OffsetDateTime::from_unix_timestamp(1_788_451_500)?;
        sqlx::query(
            r#"INSERT INTO llm_routing_events (
                created_at, event_type, workflow_key, provider_id, model_id,
                queue_name, job_id, chat_id, user_id, message_id, detail
            ) VALUES
                ($1, 'circuit_open_exhaustion', 'dialog', NULL, NULL,
                 'dialog', 300, -300, 30, 3, '{}'),
                ($1, 'attempt_failed', 'dialog', 8, 9,
                 'dialog', 301, -301, 31, 4,
                 '{"retryable_reason":"capacity_unavailable"}'),
                ($1, 'capacity_unavailable', 'dialog', 8, 9,
                 'dialog', 301, -301, 31, 4,
                 '{"retryable_reason":"capacity_unavailable"}')"#,
        )
        .bind(now - time::Duration::minutes(1))
        .execute(&pool)
        .await?;

        assert_eq!(store.capture(now).await?, 0);
        assert!(store.incidents(0, 100).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn live_actual_exhaustion_after_intermediates_creates_one_incident()
    -> Result<(), Box<dyn Error>> {
        let Some((pool, store)) = live_store().await? else {
            return Ok(());
        };
        let now = OffsetDateTime::from_unix_timestamp(1_788_451_500)?;
        sqlx::query(
            r#"INSERT INTO llm_routing_events (
                created_at, event_type, workflow_key, provider_id, model_id,
                queue_name, job_id, chat_id, user_id, message_id, detail
            ) VALUES
                ($1 - interval '2 seconds', 'attempt_failed', 'dialog', 8, 9,
                 'dialog', 400, -400, 40, 4,
                 '{"retryable_reason":"capacity_unavailable"}'),
                ($1 - interval '1 second', 'capacity_unavailable', 'dialog', 8, 9,
                 'dialog', 400, -400, 40, 4,
                 '{"retryable_reason":"capacity_unavailable"}'),
                ($1, 'all_attempts_exhausted', 'dialog', NULL, NULL,
                 'dialog', 400, -400, 40, 4,
                 '{"failed_attempts":2,"last_retryable_reason":"capacity_unavailable"}')"#,
        )
        .bind(now - time::Duration::minutes(1))
        .execute(&pool)
        .await?;

        assert_eq!(store.capture(now).await?, 1);
        let incident = store.incidents(0, 100).await?.remove(0);
        let evidence = store.evidence(incident.id).await?.expect("evidence");
        let attempts = evidence["route_attempts"]["items"]
            .as_array()
            .expect("route context");
        assert_eq!(attempts.len(), 2);
        assert!(
            attempts
                .iter()
                .any(|item| item["event_type"] == "attempt_failed")
        );
        assert!(
            attempts
                .iter()
                .any(|item| item["event_type"] == "capacity_unavailable")
        );
        Ok(())
    }

    #[tokio::test]
    async fn live_incidents_use_ascending_cursor_pagination() -> Result<(), Box<dyn Error>> {
        let Some((pool, store)) = live_store().await? else {
            return Ok(());
        };
        let now = OffsetDateTime::from_unix_timestamp(1_788_451_500)?;
        for offset in 1..=3 {
            insert_terminal(
                &pool,
                now - time::Duration::seconds(i64::from(4 - offset)),
                i64::from(offset),
            )
            .await?;
        }
        assert_eq!(store.capture(now).await?, 3);

        let first = store.incidents(0, 2).await?;
        assert_eq!(first.len(), 2);
        assert!(first[0].id < first[1].id);
        let second = store.incidents(first[1].id, 2).await?;
        assert_eq!(second.len(), 1);
        assert!(second[0].id > first[1].id);
        Ok(())
    }

    #[tokio::test]
    async fn live_evidence_scopes_attempts_and_never_projects_private_identifiers()
    -> Result<(), Box<dyn Error>> {
        let Some((pool, store)) = live_store().await? else {
            return Ok(());
        };
        let now = OffsetDateTime::from_unix_timestamp(1_788_451_500)?;
        insert_terminal(&pool, now - time::Duration::minutes(1), 100).await?;
        sqlx::query(
            r#"INSERT INTO llm_routing_events (
                created_at, event_type, workflow_key, provider_id, model_id,
                queue_name, job_id, chat_id, user_id, message_id, summary, detail
            ) VALUES
                ($1, 'attempt_failed', 'dialog', 8, 9, 'dialog', 100, -100123, 456, 789,
                 'raw provider response', '{"retryable_reason":"provider_timeout","response":"secret"}'),
                ($1, 'attempt_failed', 'dialog', 18, 19, 'dialog', 999, -999, 999, 999,
                 'unrelated', '{"retryable_reason":"provider_unavailable"}')"#,
        )
        .bind(now - time::Duration::minutes(2))
        .execute(&pool)
        .await?;
        sqlx::query(
            r#"INSERT INTO taskman_jobs
                (id, queue_name, status, job_type, created_at, started_at)
               VALUES
                (100, 'dialog', 'processing', 'dialog', $1, $2),
                (200, 'dialog', 'pending', 'dialog', $1, NULL),
                (201, 'image', 'pending', 'image_gen', $1, NULL)"#,
        )
        .bind(now - time::Duration::minutes(3))
        .bind(now - time::Duration::minutes(2))
        .execute(&pool)
        .await?;
        assert_eq!(store.capture(now).await?, 1);
        let incident = store.incidents(0, 1).await?.remove(0);

        let evidence = store.evidence(incident.id).await?.expect("evidence");
        assert_eq!(evidence["route_attempts"]["available"], true);
        assert_eq!(
            evidence["route_attempts"]["items"].as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(
            evidence["route_attempts"]["items"][0]["provider"],
            "vram-cloud"
        );
        assert_eq!(
            evidence["route_attempts"]["items"][0]["reason"],
            "provider_timeout"
        );
        assert_eq!(evidence["task"]["available"], true);
        assert_eq!(evidence["task"]["state"], "processing");
        assert_eq!(evidence["queues"]["available"], true);
        assert_eq!(evidence["database"]["available"], true);
        let encoded = evidence.to_string();
        for forbidden in [
            "-100123",
            "456",
            "789",
            "-999",
            "raw provider response",
            "secret",
            "other-provider",
            "SELECT",
        ] {
            assert!(
                !encoded.contains(forbidden),
                "leaked {forbidden}: {encoded}"
            );
        }
        assert!(store.evidence(incident.id + 10_000).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn live_evidence_reports_missing_attempt_rows_explicitly() -> Result<(), Box<dyn Error>> {
        let Some((pool, store)) = live_store().await? else {
            return Ok(());
        };
        let now = OffsetDateTime::from_unix_timestamp(1_788_451_500)?;
        insert_terminal(&pool, now - time::Duration::minutes(1), 500).await?;
        assert_eq!(store.capture(now).await?, 1);
        let incident = store.incidents(0, 1).await?.remove(0);

        let evidence = store.evidence(incident.id).await?.expect("evidence");
        assert_eq!(evidence["route_attempts"]["available"], false);
        assert_eq!(evidence["route_attempts"]["reason"], "no_matching_evidence");
        Ok(())
    }

    #[tokio::test]
    async fn live_private_workflow_preserves_exact_attempt_correlation()
    -> Result<(), Box<dyn Error>> {
        let Some((pool, store)) = live_store().await? else {
            return Ok(());
        };
        let now = OffsetDateTime::from_unix_timestamp(1_788_451_500)?;
        sqlx::query(
            r#"INSERT INTO llm_routing_events (
                created_at, event_type, workflow_key, provider_id, model_id,
                queue_name, job_id, chat_id, user_id, message_id, detail
            ) VALUES
                ($1 - interval '1 second', 'attempt_failed', 'image_generation ', 8, 9,
                 'image', 600, -600, 60, 6,
                 '{"retryable_reason":"provider_timeout"}'),
                ($1, 'all_attempts_exhausted', 'image_generation ', NULL, NULL,
                 'image', 600, -600, 60, 6,
                 '{"failed_attempts":2,"last_retryable_reason":"provider_timeout"}')"#,
        )
        .bind(now - time::Duration::minutes(1))
        .execute(&pool)
        .await?;
        assert_eq!(store.capture(now).await?, 1);
        let incident = store.incidents(0, 1).await?.remove(0);

        assert_eq!(incident.snapshot["workflow"], "image_generation");
        let evidence = store.evidence(incident.id).await?.expect("evidence");
        assert_eq!(evidence["route_attempts"]["available"], true);
        assert_eq!(
            evidence["route_attempts"]["items"].as_array().map(Vec::len),
            Some(1)
        );
        Ok(())
    }
}
