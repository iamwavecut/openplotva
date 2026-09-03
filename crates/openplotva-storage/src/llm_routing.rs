//! Postgres persistence for the admin-configurable LLM routing system.
//!
//! Provides the consistent snapshot read the app loader turns into a routing
//! table, the admin CRUD over providers/models/workflows/assignments/triggers,
//! and AES-GCM sealing of admin-entered provider keys. JSONB columns follow the
//! crate convention of binding `$N::jsonb` from a JSON string and selecting
//! `column::text`; `TEXT[]` maps directly to `Vec<String>`.

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow};
use time::OffsetDateTime;

use crate::StorageError;

const AES_GCM_NONCE_LEN: usize = 12;

/// A provider endpoint row. Credentials are never returned in plaintext to the
/// admin API; `api_key_ref` names an env/secret var and `api_key_encrypted` holds
/// AES-GCM ciphertext.
#[derive(Clone, Debug, PartialEq)]
pub struct ProviderRecord {
    pub id: i64,
    pub name: String,
    pub kind: String,
    pub protocol: Option<String>,
    pub runtime_hint: Option<String>,
    pub endpoint: Option<String>,
    pub discovery_service_name: Option<String>,
    pub discovery_endpoint_name: Option<String>,
    pub api_key_ref: Option<String>,
    pub api_key_encrypted: Option<Vec<u8>>,
    pub enabled: bool,
    pub config: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ModelRecord {
    pub id: i64,
    pub provider_id: i64,
    pub model_name: String,
    pub display_name: Option<String>,
    pub base_url: Option<String>,
    pub capabilities: Vec<String>,
    pub embedding_dim: Option<i32>,
    pub pool_id: Option<i64>,
    pub enabled: bool,
    pub config: Value,
}

/// A capacity pool: a shared concurrency budget over one physical resource.
/// `max_concurrency` NULL means unlimited (gauge-only, never blocks).
#[derive(Clone, Debug, PartialEq)]
pub struct PoolRecord {
    pub id: i64,
    pub name: String,
    pub max_concurrency: Option<i32>,
    pub description: Option<String>,
    pub config: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowRecord {
    pub key: String,
    pub kind: String,
    pub full_routing: bool,
    pub retry_max_hops: i32,
    pub retry_wall_ms: i32,
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AssignmentRecord {
    pub id: i64,
    pub workflow_key: String,
    pub scope: String,
    pub role: String,
    pub provider_model_id: i64,
    pub weight: Option<i32>,
    pub fallback_order: Option<i32>,
    pub canary_percent: Option<i32>,
    pub enabled: bool,
    pub inference_overrides: Value,
    pub cb_failure_threshold: i32,
    pub cb_cooldown_ms: i32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TriggerRecord {
    pub id: i64,
    pub workflow_key: String,
    pub trigger_type: String,
    pub engage_assignment_id: i64,
    pub enabled: bool,
    pub queue_name: Option<String>,
    pub high_watermark: Option<i32>,
    pub low_watermark: Option<i32>,
    pub params: Value,
}

/// New-provider input for an admin insert/update. Exactly one key source is set.
#[derive(Clone, Debug, Default)]
pub struct ProviderInput {
    pub name: String,
    pub kind: String,
    pub protocol: Option<String>,
    pub runtime_hint: Option<String>,
    pub endpoint: Option<String>,
    pub discovery_service_name: Option<String>,
    pub discovery_endpoint_name: Option<String>,
    pub api_key_ref: Option<String>,
    pub api_key_encrypted: Option<Vec<u8>>,
    pub enabled: bool,
    pub config: Value,
}

#[derive(Clone, Debug, Default)]
pub struct ModelInput {
    pub provider_id: i64,
    pub model_name: String,
    pub display_name: Option<String>,
    pub base_url: Option<String>,
    pub capabilities: Vec<String>,
    pub embedding_dim: Option<i32>,
    pub pool_id: Option<i64>,
    pub enabled: bool,
    pub config: Value,
}

#[derive(Clone, Debug, Default)]
pub struct PoolInput {
    pub name: String,
    pub max_concurrency: Option<i32>,
    pub description: Option<String>,
    pub config: Value,
}

#[derive(Clone, Debug, Default)]
pub struct AssignmentInput {
    pub workflow_key: String,
    pub scope: String,
    pub role: String,
    pub provider_model_id: i64,
    pub weight: Option<i32>,
    pub fallback_order: Option<i32>,
    pub canary_percent: Option<i32>,
    pub enabled: bool,
    pub inference_overrides: Value,
    pub cb_failure_threshold: i32,
    pub cb_cooldown_ms: i32,
}

#[derive(Clone, Debug, Default)]
pub struct TriggerInput {
    pub workflow_key: String,
    pub trigger_type: String,
    pub engage_assignment_id: i64,
    pub enabled: bool,
    pub queue_name: Option<String>,
    pub high_watermark: Option<i32>,
    pub low_watermark: Option<i32>,
    pub params: Value,
}

/// Consistent snapshot of the whole routing configuration.
#[derive(Clone, Debug, Default)]
pub struct RoutingSnapshot {
    pub providers: Vec<ProviderRecord>,
    pub models: Vec<ModelRecord>,
    pub workflows: Vec<WorkflowRecord>,
    pub assignments: Vec<AssignmentRecord>,
    pub triggers: Vec<TriggerRecord>,
    pub pools: Vec<PoolRecord>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RoutingEventRecord {
    pub id: i64,
    pub created_at: OffsetDateTime,
    pub severity: String,
    pub event_type: String,
    pub workflow_key: String,
    pub provider_id: Option<i64>,
    pub model_id: Option<i64>,
    pub queue_name: Option<String>,
    pub job_id: Option<i64>,
    pub chat_id: Option<i64>,
    pub user_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub message_id: Option<i32>,
    pub dedupe_key: String,
    pub summary: String,
    pub detail: Value,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RoutingEventInput {
    pub severity: String,
    pub event_type: String,
    pub workflow_key: String,
    pub provider_id: Option<i64>,
    pub model_id: Option<i64>,
    pub queue_name: Option<String>,
    pub job_id: Option<i64>,
    pub chat_id: Option<i64>,
    pub user_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub message_id: Option<i32>,
    pub dedupe_key: String,
    pub summary: String,
    pub detail: Value,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct RoutingAdminIncidentSample {
    pub user_id: Option<i64>,
    pub user_name: Option<String>,
    pub user_username: Option<String>,
    pub chat_id: Option<i64>,
    pub chat_name: Option<String>,
    pub chat_username: Option<String>,
    pub job_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub message_id: Option<i32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RoutingAdminIncidentGroup {
    pub dedupe_key: String,
    pub severity: String,
    pub event_type: String,
    pub workflow_key: String,
    pub provider_id: Option<i64>,
    pub provider_name: Option<String>,
    pub model_id: Option<i64>,
    pub model_name: Option<String>,
    pub queue_name: Option<String>,
    pub summary: String,
    pub reason_counts: Value,
    pub occurrences: i64,
    pub affected_users: i64,
    pub affected_chats: i64,
    pub affected_jobs: i64,
    pub first_seen: OffsetDateTime,
    pub last_seen: OffsetDateTime,
    pub samples: Vec<RoutingAdminIncidentSample>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RoutingAdminIncidentSnapshot {
    pub total_occurrences: i64,
    pub affected_users: i64,
    pub affected_chats: i64,
    pub affected_jobs: i64,
    pub total_groups: i64,
    pub groups: Vec<RoutingAdminIncidentGroup>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoutingAdminReportOperationKind {
    Send,
    Edit,
}

impl RoutingAdminReportOperationKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Send => "send",
            Self::Edit => "edit",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "send" => Some(Self::Send),
            "edit" => Some(Self::Edit),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RoutingAdminReportState {
    pub admin_id: i64,
    pub telegram_message_id: Option<i64>,
    pub last_new_message_at: Option<OffsetDateTime>,
    pub last_rendered_fingerprint: Option<String>,
    pub pending_virtual_id: Option<String>,
    pub pending_kind: Option<RoutingAdminReportOperationKind>,
    pub pending_fingerprint: Option<String>,
    pub pending_started_at: Option<OffsetDateTime>,
    pub last_delivery_attempt_at: Option<OffsetDateTime>,
    pub last_delivery_error_class: Option<String>,
    pub updated_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RoutingAdminReportPendingClaim {
    pub admin_id: i64,
    pub virtual_id: String,
    pub kind: RoutingAdminReportOperationKind,
    pub fingerprint: String,
    pub expected_message_id: Option<i64>,
    pub now: OffsetDateTime,
    pub stale_pending_before: OffsetDateTime,
    pub retry_before: OffsetDateTime,
    pub send_before: OffsetDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RoutingAdminReportDeliveryResult {
    pub virtual_id: String,
    pub sent_message_id: Option<i32>,
    pub succeeded: bool,
    pub error_class: Option<String>,
    pub at: OffsetDateTime,
}

#[derive(Clone, Debug)]
pub struct PostgresRoutingAdminReportStore {
    pool: PgPool,
}

impl PostgresRoutingAdminReportStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[must_use]
pub fn next_admin_report_delivery_state(
    mut state: RoutingAdminReportState,
    result: &RoutingAdminReportDeliveryResult,
) -> RoutingAdminReportState {
    if state.pending_virtual_id.as_deref() != Some(result.virtual_id.as_str()) {
        return state;
    }

    let kind = state.pending_kind;
    let successful_send =
        kind != Some(RoutingAdminReportOperationKind::Send) || result.sent_message_id.is_some();
    if result.succeeded && successful_send {
        if kind == Some(RoutingAdminReportOperationKind::Send) {
            state.telegram_message_id = result.sent_message_id.map(i64::from);
            state.last_new_message_at = Some(result.at);
        }
        state.last_rendered_fingerprint = state.pending_fingerprint.take();
        state.last_delivery_attempt_at = None;
        state.last_delivery_error_class = None;
    } else {
        let error_class = result
            .error_class
            .clone()
            .unwrap_or_else(|| "missing_message_id".to_owned());
        if kind == Some(RoutingAdminReportOperationKind::Edit)
            && matches!(
                error_class.as_str(),
                "terminal_bad_request" | "terminal_permission"
            )
        {
            state.telegram_message_id = None;
        }
        state.last_delivery_attempt_at = Some(result.at);
        state.last_delivery_error_class = Some(error_class);
    }

    state.pending_virtual_id = None;
    state.pending_kind = None;
    state.pending_fingerprint = None;
    state.pending_started_at = None;
    state.updated_at = Some(result.at);
    state
}

const SQL_LIST_PROVIDERS: &str = "SELECT id, name, kind, protocol, runtime_hint, endpoint, discovery_service_name, discovery_endpoint_name, api_key_ref, api_key_encrypted, enabled, config::text AS config FROM llm_providers ORDER BY id ASC";
const SQL_GET_PROVIDER: &str = "SELECT id, name, kind, protocol, runtime_hint, endpoint, discovery_service_name, discovery_endpoint_name, api_key_ref, api_key_encrypted, enabled, config::text AS config FROM llm_providers WHERE id = $1";
const SQL_INSERT_PROVIDER: &str = "INSERT INTO llm_providers (name, kind, protocol, runtime_hint, endpoint, discovery_service_name, discovery_endpoint_name, api_key_ref, api_key_encrypted, enabled, config) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb) RETURNING id";
const SQL_UPDATE_PROVIDER: &str = "UPDATE llm_providers SET name = $2, kind = $3, protocol = $4, runtime_hint = $5, endpoint = $6, discovery_service_name = $7, discovery_endpoint_name = $8, api_key_ref = $9, api_key_encrypted = $10, enabled = $11, config = $12::jsonb, updated_at = now() WHERE id = $1";
const SQL_SET_PROVIDER_PROTOCOL_IF_NULL: &str =
    "UPDATE llm_providers SET protocol = $2, updated_at = now() WHERE id = $1 AND protocol IS NULL";
const SQL_SET_PROVIDER_ENABLED: &str =
    "UPDATE llm_providers SET enabled = $2, updated_at = now() WHERE id = $1";
const SQL_PATCH_PROVIDER_CONFIG: &str = "UPDATE llm_providers SET config = COALESCE(config, '{}'::jsonb) || $2::jsonb, updated_at = now() WHERE id = $1";
const SQL_DELETE_PROVIDER: &str = "DELETE FROM llm_providers WHERE id = $1";

const SQL_LIST_MODELS: &str = "SELECT id, provider_id, model_name, display_name, base_url, capabilities, embedding_dim, pool_id, enabled, config::text AS config FROM provider_models ORDER BY id ASC";
const SQL_INSERT_MODEL: &str = "INSERT INTO provider_models (provider_id, model_name, display_name, base_url, capabilities, embedding_dim, pool_id, enabled, config) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb) RETURNING id";
const SQL_UPDATE_MODEL: &str = "UPDATE provider_models SET provider_id = $2, model_name = $3, display_name = $4, base_url = $5, capabilities = $6, embedding_dim = $7, pool_id = $8, enabled = $9, config = $10::jsonb WHERE id = $1";
const SQL_SET_MODEL_POOL: &str = "UPDATE provider_models SET pool_id = $2 WHERE id = $1";
const SQL_SET_PROVIDER_MODELS_POOL_IF_NULL: &str =
    "UPDATE provider_models SET pool_id = $2 WHERE provider_id = $1 AND pool_id IS NULL";
const SQL_SET_MODEL_ENABLED: &str = "UPDATE provider_models SET enabled = $2 WHERE id = $1";
const SQL_PATCH_MODEL_CONFIG: &str =
    "UPDATE provider_models SET config = COALESCE(config, '{}'::jsonb) || $2::jsonb WHERE id = $1";
const SQL_DELETE_MODEL: &str = "DELETE FROM provider_models WHERE id = $1";

const SQL_LIST_POOLS: &str = "SELECT id, name, max_concurrency, description, config::text AS config FROM llm_capacity_pools ORDER BY id ASC";
const SQL_INSERT_POOL: &str = "INSERT INTO llm_capacity_pools (name, max_concurrency, description, config) VALUES ($1, $2, $3, $4::jsonb) RETURNING id";
const SQL_INSERT_POOL_IF_MISSING: &str = "INSERT INTO llm_capacity_pools (name, max_concurrency, description, config) VALUES ($1, $2, $3, $4::jsonb) ON CONFLICT (name) DO NOTHING";
const SQL_GET_POOL_BY_NAME: &str = "SELECT id, name, max_concurrency, description, config::text AS config FROM llm_capacity_pools WHERE name = $1";
const SQL_UPDATE_POOL: &str = "UPDATE llm_capacity_pools SET name = $2, max_concurrency = $3, description = $4, config = $5::jsonb, updated_at = now() WHERE id = $1";
const SQL_DELETE_POOL: &str = "DELETE FROM llm_capacity_pools WHERE id = $1";

const SQL_LIST_WORKFLOWS: &str = "SELECT key, kind, full_routing, retry_max_hops, retry_wall_ms, enabled FROM workflows ORDER BY key ASC";
const SQL_INSERT_WORKFLOW_IF_MISSING: &str = "INSERT INTO workflows (key, kind, full_routing, retry_max_hops, retry_wall_ms, enabled) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (key) DO NOTHING";
const SQL_UPDATE_WORKFLOW: &str = "UPDATE workflows SET full_routing = $2, retry_max_hops = $3, retry_wall_ms = $4, enabled = $5 WHERE key = $1";
const SQL_SET_WORKFLOW_ENABLED: &str = "UPDATE workflows SET enabled = $2 WHERE key = $1";

const SQL_LIST_ASSIGNMENTS: &str = "SELECT id, workflow_key, scope, role, provider_model_id, weight, fallback_order, canary_percent, enabled, inference_overrides::text AS inference_overrides, cb_failure_threshold, cb_cooldown_ms FROM workflow_assignments ORDER BY id ASC";
const SQL_INSERT_ASSIGNMENT: &str = "INSERT INTO workflow_assignments (workflow_key, scope, role, provider_model_id, weight, fallback_order, canary_percent, enabled, inference_overrides, cb_failure_threshold, cb_cooldown_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11) RETURNING id";
const SQL_UPDATE_ASSIGNMENT: &str = "UPDATE workflow_assignments SET workflow_key = $2, scope = $3, role = $4, provider_model_id = $5, weight = $6, fallback_order = $7, canary_percent = $8, enabled = $9, inference_overrides = $10::jsonb, cb_failure_threshold = $11, cb_cooldown_ms = $12 WHERE id = $1";
const SQL_DELETE_ASSIGNMENT: &str = "DELETE FROM workflow_assignments WHERE id = $1";
const SQL_DELETE_ASSIGNMENTS_FOR_SCOPE: &str =
    "DELETE FROM workflow_assignments WHERE workflow_key = $1 AND scope = $2";
const SQL_SET_ASSIGNMENT_WEIGHT: &str = "UPDATE workflow_assignments SET weight = $2 WHERE id = $1";
const SQL_SET_ASSIGNMENT_FALLBACK_ORDER: &str =
    "UPDATE workflow_assignments SET fallback_order = $2 WHERE id = $1";

const SQL_LIST_TRIGGERS: &str = "SELECT id, workflow_key, trigger_type, engage_assignment_id, enabled, queue_name, high_watermark, low_watermark, params::text AS params FROM workflow_triggers ORDER BY id ASC";
const SQL_INSERT_TRIGGER: &str = "INSERT INTO workflow_triggers (workflow_key, trigger_type, engage_assignment_id, enabled, queue_name, high_watermark, low_watermark, params) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb) RETURNING id";
const SQL_UPDATE_TRIGGER: &str = "UPDATE workflow_triggers SET workflow_key = $2, trigger_type = $3, engage_assignment_id = $4, enabled = $5, queue_name = $6, high_watermark = $7, low_watermark = $8, params = $9::jsonb WHERE id = $1";
const SQL_DELETE_TRIGGER: &str = "DELETE FROM workflow_triggers WHERE id = $1";
const SQL_INSERT_ROUTING_EVENT_RETURNING_ID: &str = r#"INSERT INTO llm_routing_events (
    severity,
    event_type,
    workflow_key,
    provider_id,
    model_id,
    queue_name,
    job_id,
    chat_id,
    user_id,
    thread_id,
    message_id,
    dedupe_key,
    summary,
    detail
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb)
RETURNING id"#;
const SQL_INSERT_ROUTING_EVENTS_PREFIX: &str = r#"INSERT INTO llm_routing_events (
    severity,
    event_type,
    workflow_key,
    provider_id,
    model_id,
    queue_name,
    job_id,
    chat_id,
    user_id,
    thread_id,
    message_id,
    dedupe_key,
    summary,
    detail
)"#;
const SQL_LIST_ROUTING_EVENTS: &str = r#"SELECT
    id,
    created_at,
    severity,
    event_type,
    workflow_key,
    provider_id,
    model_id,
    queue_name,
    job_id,
    chat_id,
    user_id,
    thread_id,
    message_id,
    dedupe_key,
    summary,
    detail::text AS detail
FROM llm_routing_events
ORDER BY created_at DESC, id DESC
LIMIT $1"#;
const SQL_ROUTING_ADMIN_INCIDENT_SNAPSHOT: &str = r#"
WITH recent_base AS (
    SELECT
        e.*,
        COALESCE(p.name, NULLIF(e.detail->>'provider', '')) AS provider_name,
        COALESCE(m.model_name, NULLIF(e.detail->>'model', '')) AS model_name,
        NULLIF(BTRIM(CONCAT_WS(' ', u.first_name, u.last_name)), '') AS user_name,
        u.username AS user_username,
        COALESCE(
            NULLIF(c.title, ''),
            NULLIF(BTRIM(CONCAT_WS(' ', c.first_name, c.last_name)), '')
        ) AS chat_name,
        c.username AS chat_username,
        COALESCE(
            NULLIF(e.detail->>'last_retryable_reason', ''),
            NULLIF(e.detail->>'retryable_reason', ''),
            NULLIF(e.detail->>'reason', ''),
            CASE
                WHEN e.event_type IN ('router_reload_failed', 'routing_backfill_failed')
                THEN NULLIF(e.detail->>'error', '')
            END,
            NULLIF(e.summary, ''),
            e.event_type
        ) AS reason
    FROM llm_routing_events e
    LEFT JOIN llm_providers p ON p.id = e.provider_id
    LEFT JOIN provider_models m ON m.id = e.model_id
    LEFT JOIN telegram_users_effective u ON u.id = e.user_id
    LEFT JOIN telegram_chats_effective c ON c.id = e.chat_id
    WHERE e.created_at >= $1
      AND e.event_type IN (
          'route_unavailable',
          'no_candidates',
          'all_attempts_exhausted',
          'circuit_open_exhaustion',
          'capacity_unavailable',
          'router_reload_failed',
          'routing_backfill_failed'
      )
      AND COALESCE(e.detail->>'admin_actionable', 'true') <> 'false'
      AND NOT (
          e.event_type = 'all_attempts_exhausted'
          AND COALESCE(e.detail->>'admin_actionable', '') <> 'true'
          AND COALESCE(e.detail->>'failed_attempts', '') = '1'
          AND COALESCE(e.detail->>'last_retryable_reason', '') <> ''
      )
),
recent AS (
    SELECT
        recent_base.*,
        ROW_NUMBER() OVER (
            PARTITION BY dedupe_key
            ORDER BY
                CASE
                    WHEN user_id IS NOT NULL OR chat_id IS NOT NULL OR job_id IS NOT NULL
                    THEN 1 ELSE 0
                END DESC,
                created_at DESC,
                id DESC
        ) AS context_rank
    FROM recent_base
),
reason_counts AS (
    SELECT dedupe_key, reason, COUNT(*)::BIGINT AS occurrences
    FROM recent
    GROUP BY dedupe_key, reason
),
reason_maps AS (
    SELECT dedupe_key, jsonb_object_agg(reason, occurrences) AS reason_counts
    FROM reason_counts
    GROUP BY dedupe_key
),
grouped AS (
    SELECT
        r.dedupe_key,
        (array_agg(
            r.severity
            ORDER BY CASE r.severity
                WHEN 'critical' THEN 5
                WHEN 'error' THEN 4
                WHEN 'warn' THEN 3
                WHEN 'info' THEN 2
                ELSE 1
            END DESC, r.created_at DESC, r.id DESC
        ))[1] AS severity,
        (array_agg(r.event_type ORDER BY r.created_at DESC, r.id DESC))[1] AS event_type,
        (array_agg(r.workflow_key ORDER BY r.created_at DESC, r.id DESC))[1] AS workflow_key,
        (array_agg(r.provider_id ORDER BY r.created_at DESC, r.id DESC)
            FILTER (WHERE r.provider_id IS NOT NULL))[1] AS provider_id,
        (array_agg(r.provider_name ORDER BY r.created_at DESC, r.id DESC)
            FILTER (WHERE r.provider_name IS NOT NULL))[1] AS provider_name,
        (array_agg(r.model_id ORDER BY r.created_at DESC, r.id DESC)
            FILTER (WHERE r.model_id IS NOT NULL))[1] AS model_id,
        (array_agg(r.model_name ORDER BY r.created_at DESC, r.id DESC)
            FILTER (WHERE r.model_name IS NOT NULL))[1] AS model_name,
        (array_agg(r.queue_name ORDER BY r.created_at DESC, r.id DESC)
            FILTER (WHERE r.queue_name IS NOT NULL))[1] AS queue_name,
        (array_agg(r.summary ORDER BY r.created_at DESC, r.id DESC))[1] AS summary,
        COUNT(*)::BIGINT AS occurrences,
        COUNT(DISTINCT r.user_id)::BIGINT AS affected_users,
        COUNT(DISTINCT r.chat_id)::BIGINT AS affected_chats,
        COUNT(DISTINCT r.job_id)::BIGINT AS affected_jobs,
        MIN(r.created_at) AS first_seen,
        MAX(r.created_at) AS last_seen,
        COALESCE(
            jsonb_agg(
                jsonb_strip_nulls(jsonb_build_object(
                    'user_id', r.user_id,
                    'user_name', r.user_name,
                    'user_username', r.user_username,
                    'chat_id', r.chat_id,
                    'chat_name', r.chat_name,
                    'chat_username', r.chat_username,
                    'job_id', r.job_id,
                    'thread_id', r.thread_id,
                    'message_id', r.message_id
                ))
                ORDER BY r.created_at DESC, r.id DESC
            ) FILTER (
                WHERE r.context_rank <= 3
                  AND (r.user_id IS NOT NULL OR r.chat_id IS NOT NULL OR r.job_id IS NOT NULL)
            ),
            '[]'::jsonb
        ) AS samples
    FROM recent r
    GROUP BY r.dedupe_key
),
totals AS (
    SELECT
        COUNT(*)::BIGINT AS total_occurrences,
        COUNT(DISTINCT user_id)::BIGINT AS affected_users,
        COUNT(DISTINCT chat_id)::BIGINT AS affected_chats,
        COUNT(DISTINCT job_id)::BIGINT AS affected_jobs,
        COUNT(DISTINCT dedupe_key)::BIGINT AS total_groups
    FROM recent
)
SELECT
    t.total_occurrences,
    t.affected_users AS total_affected_users,
    t.affected_chats AS total_affected_chats,
    t.affected_jobs AS total_affected_jobs,
    t.total_groups,
    g.dedupe_key,
    g.severity,
    g.event_type,
    g.workflow_key,
    g.provider_id,
    g.provider_name,
    g.model_id,
    g.model_name,
    g.queue_name,
    g.summary,
    rm.reason_counts::text AS reason_counts,
    g.occurrences,
    g.affected_users,
    g.affected_chats,
    g.affected_jobs,
    g.first_seen,
    g.last_seen,
    g.samples::text AS samples
FROM totals t
LEFT JOIN grouped g ON TRUE
LEFT JOIN reason_maps rm ON rm.dedupe_key = g.dedupe_key
ORDER BY
    (g.affected_users > 0 OR g.affected_chats > 0 OR g.affected_jobs > 0) DESC NULLS LAST,
    CASE g.severity
        WHEN 'critical' THEN 5
        WHEN 'error' THEN 4
        WHEN 'warn' THEN 3
        WHEN 'info' THEN 2
        ELSE 1
    END DESC,
    g.occurrences DESC,
    g.last_seen DESC
LIMIT 50"#;
const SQL_ROUTING_ADMIN_REPORT_STATE: &str = r#"SELECT
    admin_id,
    telegram_message_id,
    last_new_message_at,
    last_rendered_fingerprint,
    pending_virtual_id,
    pending_kind,
    pending_fingerprint,
    pending_started_at,
    last_delivery_attempt_at,
    last_delivery_error_class,
    updated_at
FROM llm_admin_report_state
WHERE admin_id = $1"#;
const SQL_CLAIM_ROUTING_ADMIN_REPORT_SEND: &str = r#"INSERT INTO llm_admin_report_state (
    admin_id,
    pending_virtual_id,
    pending_kind,
    pending_fingerprint,
    pending_started_at,
    updated_at
)
VALUES ($1, $2, 'send', $3, $4, $4)
ON CONFLICT (admin_id) DO UPDATE SET
    pending_virtual_id = EXCLUDED.pending_virtual_id,
    pending_kind = EXCLUDED.pending_kind,
    pending_fingerprint = EXCLUDED.pending_fingerprint,
    pending_started_at = EXCLUDED.pending_started_at,
    updated_at = EXCLUDED.updated_at
WHERE (
        llm_admin_report_state.pending_virtual_id IS NULL
        OR llm_admin_report_state.pending_started_at <= $5
    )
  AND (
        llm_admin_report_state.last_delivery_error_class IS NULL
        OR llm_admin_report_state.last_delivery_attempt_at <= $6
    )
  AND llm_admin_report_state.last_rendered_fingerprint IS DISTINCT FROM $3
  AND (
        llm_admin_report_state.last_new_message_at IS NULL
        OR llm_admin_report_state.last_new_message_at <= $7
    )
RETURNING admin_id"#;
const SQL_CLAIM_ROUTING_ADMIN_REPORT_EDIT: &str = r#"UPDATE llm_admin_report_state SET
    pending_virtual_id = $2,
    pending_kind = 'edit',
    pending_fingerprint = $3,
    pending_started_at = $4,
    updated_at = $4
WHERE admin_id = $1
  AND (
        pending_virtual_id IS NULL
        OR pending_started_at <= $6
    )
  AND (
        last_delivery_error_class IS NULL
        OR last_delivery_attempt_at <= $7
    )
  AND last_rendered_fingerprint IS DISTINCT FROM $3
  AND telegram_message_id = $5
RETURNING admin_id"#;
const SQL_ROUTING_ADMIN_REPORT_STATE_BY_PENDING_FOR_UPDATE: &str = r#"SELECT
    admin_id,
    telegram_message_id,
    last_new_message_at,
    last_rendered_fingerprint,
    pending_virtual_id,
    pending_kind,
    pending_fingerprint,
    pending_started_at,
    last_delivery_attempt_at,
    last_delivery_error_class,
    updated_at
FROM llm_admin_report_state
WHERE pending_virtual_id = $1
FOR UPDATE"#;
const SQL_UPDATE_ROUTING_ADMIN_REPORT_STATE: &str = r#"UPDATE llm_admin_report_state SET
    telegram_message_id = $2,
    last_new_message_at = $3,
    last_rendered_fingerprint = $4,
    pending_virtual_id = $5,
    pending_kind = $6,
    pending_fingerprint = $7,
    pending_started_at = $8,
    last_delivery_attempt_at = $9,
    last_delivery_error_class = $10,
    updated_at = $11
WHERE admin_id = $1"#;
pub const SQL_DELETE_OLD_LLM_ROUTING_EVENTS_BATCH: &str = r#"
WITH doomed AS (
    SELECT id
    FROM llm_routing_events
    WHERE created_at < now() - ($1::int * interval '1 day')
    ORDER BY created_at ASC
    LIMIT $2
)
DELETE FROM llm_routing_events e
USING doomed
WHERE e.id = doomed.id"#;

fn parse_json(text: Option<String>) -> Result<Value, StorageError> {
    match text {
        None => Ok(Value::Object(serde_json::Map::new())),
        Some(raw) => {
            serde_json::from_str(&raw).map_err(|source| StorageError::RoutingJsonCodec { source })
        }
    }
}

fn provider_from_row(row: PgRow) -> Result<ProviderRecord, StorageError> {
    Ok(ProviderRecord {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        kind: row.try_get("kind")?,
        protocol: row.try_get("protocol")?,
        runtime_hint: row.try_get("runtime_hint")?,
        endpoint: row.try_get("endpoint")?,
        discovery_service_name: row.try_get("discovery_service_name")?,
        discovery_endpoint_name: row.try_get("discovery_endpoint_name")?,
        api_key_ref: row.try_get("api_key_ref")?,
        api_key_encrypted: row.try_get("api_key_encrypted")?,
        enabled: row.try_get("enabled")?,
        config: parse_json(row.try_get("config")?)?,
    })
}

fn model_from_row(row: PgRow) -> Result<ModelRecord, StorageError> {
    Ok(ModelRecord {
        id: row.try_get("id")?,
        provider_id: row.try_get("provider_id")?,
        model_name: row.try_get("model_name")?,
        display_name: row.try_get("display_name")?,
        base_url: row.try_get("base_url")?,
        capabilities: row.try_get("capabilities")?,
        embedding_dim: row.try_get("embedding_dim")?,
        pool_id: row.try_get("pool_id")?,
        enabled: row.try_get("enabled")?,
        config: parse_json(row.try_get("config")?)?,
    })
}

fn pool_from_row(row: PgRow) -> Result<PoolRecord, StorageError> {
    Ok(PoolRecord {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        max_concurrency: row.try_get("max_concurrency")?,
        description: row.try_get("description")?,
        config: parse_json(row.try_get("config")?)?,
    })
}

fn workflow_from_row(row: PgRow) -> Result<WorkflowRecord, StorageError> {
    Ok(WorkflowRecord {
        key: row.try_get("key")?,
        kind: row.try_get("kind")?,
        full_routing: row.try_get("full_routing")?,
        retry_max_hops: row.try_get("retry_max_hops")?,
        retry_wall_ms: row.try_get("retry_wall_ms")?,
        enabled: row.try_get("enabled")?,
    })
}

fn assignment_from_row(row: PgRow) -> Result<AssignmentRecord, StorageError> {
    Ok(AssignmentRecord {
        id: row.try_get("id")?,
        workflow_key: row.try_get("workflow_key")?,
        scope: row.try_get("scope")?,
        role: row.try_get("role")?,
        provider_model_id: row.try_get("provider_model_id")?,
        weight: row.try_get("weight")?,
        fallback_order: row.try_get("fallback_order")?,
        canary_percent: row.try_get("canary_percent")?,
        enabled: row.try_get("enabled")?,
        inference_overrides: parse_json(row.try_get("inference_overrides")?)?,
        cb_failure_threshold: row.try_get("cb_failure_threshold")?,
        cb_cooldown_ms: row.try_get("cb_cooldown_ms")?,
    })
}

fn trigger_from_row(row: PgRow) -> Result<TriggerRecord, StorageError> {
    Ok(TriggerRecord {
        id: row.try_get("id")?,
        workflow_key: row.try_get("workflow_key")?,
        trigger_type: row.try_get("trigger_type")?,
        engage_assignment_id: row.try_get("engage_assignment_id")?,
        enabled: row.try_get("enabled")?,
        queue_name: row.try_get("queue_name")?,
        high_watermark: row.try_get("high_watermark")?,
        low_watermark: row.try_get("low_watermark")?,
        params: parse_json(row.try_get("params")?)?,
    })
}

fn routing_event_from_row(row: PgRow) -> Result<RoutingEventRecord, StorageError> {
    Ok(RoutingEventRecord {
        id: row.try_get("id")?,
        created_at: row.try_get("created_at")?,
        severity: row.try_get("severity")?,
        event_type: row.try_get("event_type")?,
        workflow_key: row.try_get("workflow_key")?,
        provider_id: row.try_get("provider_id")?,
        model_id: row.try_get("model_id")?,
        queue_name: row.try_get("queue_name")?,
        job_id: row.try_get("job_id")?,
        chat_id: row.try_get("chat_id")?,
        user_id: row.try_get("user_id")?,
        thread_id: row.try_get("thread_id")?,
        message_id: row.try_get("message_id")?,
        dedupe_key: row.try_get("dedupe_key")?,
        summary: row.try_get("summary")?,
        detail: parse_json(row.try_get("detail")?)?,
    })
}

fn routing_admin_report_state_from_row(
    row: &PgRow,
) -> Result<RoutingAdminReportState, StorageError> {
    let pending_kind = row
        .try_get::<Option<String>, _>("pending_kind")?
        .as_deref()
        .and_then(RoutingAdminReportOperationKind::parse);
    Ok(RoutingAdminReportState {
        admin_id: row.try_get("admin_id")?,
        telegram_message_id: row.try_get("telegram_message_id")?,
        last_new_message_at: row.try_get("last_new_message_at")?,
        last_rendered_fingerprint: row.try_get("last_rendered_fingerprint")?,
        pending_virtual_id: row.try_get("pending_virtual_id")?,
        pending_kind,
        pending_fingerprint: row.try_get("pending_fingerprint")?,
        pending_started_at: row.try_get("pending_started_at")?,
        last_delivery_attempt_at: row.try_get("last_delivery_attempt_at")?,
        last_delivery_error_class: row.try_get("last_delivery_error_class")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Read the whole routing configuration in one REPEATABLE READ transaction so the
/// five tables are seen at a single consistent snapshot, even while an admin write
/// is committing concurrently.
pub async fn load_snapshot(pool: &PgPool) -> Result<RoutingSnapshot, StorageError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await?;

    let providers = sqlx::query(SQL_LIST_PROVIDERS)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(provider_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let models = sqlx::query(SQL_LIST_MODELS)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(model_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let workflows = sqlx::query(SQL_LIST_WORKFLOWS)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(workflow_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let assignments = sqlx::query(SQL_LIST_ASSIGNMENTS)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(assignment_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let triggers = sqlx::query(SQL_LIST_TRIGGERS)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(trigger_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let pools = sqlx::query(SQL_LIST_POOLS)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(pool_from_row)
        .collect::<Result<Vec<_>, _>>()?;

    tx.commit().await?;
    Ok(RoutingSnapshot {
        providers,
        models,
        workflows,
        assignments,
        triggers,
        pools,
    })
}

fn json_text(value: &Value) -> String {
    value.to_string()
}

pub async fn list_providers(pool: &PgPool) -> Result<Vec<ProviderRecord>, StorageError> {
    sqlx::query(SQL_LIST_PROVIDERS)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(provider_from_row)
        .collect()
}

pub async fn get_provider(pool: &PgPool, id: i64) -> Result<Option<ProviderRecord>, StorageError> {
    sqlx::query(SQL_GET_PROVIDER)
        .bind(id)
        .fetch_optional(pool)
        .await?
        .map(provider_from_row)
        .transpose()
}

pub async fn insert_provider(pool: &PgPool, input: &ProviderInput) -> Result<i64, StorageError> {
    let row = sqlx::query(SQL_INSERT_PROVIDER)
        .bind(&input.name)
        .bind(&input.kind)
        .bind(input.protocol.as_deref())
        .bind(input.runtime_hint.as_deref())
        .bind(input.endpoint.as_deref())
        .bind(input.discovery_service_name.as_deref())
        .bind(input.discovery_endpoint_name.as_deref())
        .bind(input.api_key_ref.as_deref())
        .bind(input.api_key_encrypted.as_deref())
        .bind(input.enabled)
        .bind(json_text(&input.config))
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i64, _>("id")?)
}

pub async fn update_provider(
    pool: &PgPool,
    id: i64,
    input: &ProviderInput,
) -> Result<(), StorageError> {
    sqlx::query(SQL_UPDATE_PROVIDER)
        .bind(id)
        .bind(&input.name)
        .bind(&input.kind)
        .bind(input.protocol.as_deref())
        .bind(input.runtime_hint.as_deref())
        .bind(input.endpoint.as_deref())
        .bind(input.discovery_service_name.as_deref())
        .bind(input.discovery_endpoint_name.as_deref())
        .bind(input.api_key_ref.as_deref())
        .bind(input.api_key_encrypted.as_deref())
        .bind(input.enabled)
        .bind(json_text(&input.config))
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn set_provider_enabled(
    pool: &PgPool,
    id: i64,
    enabled: bool,
) -> Result<(), StorageError> {
    sqlx::query(SQL_SET_PROVIDER_ENABLED)
        .bind(id)
        .bind(enabled)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn patch_provider_config(
    pool: &PgPool,
    id: i64,
    patch: &Value,
) -> Result<(), StorageError> {
    sqlx::query(SQL_PATCH_PROVIDER_CONFIG)
        .bind(id)
        .bind(json_text(patch))
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_provider(pool: &PgPool, id: i64) -> Result<(), StorageError> {
    sqlx::query(SQL_DELETE_PROVIDER)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn list_models(pool: &PgPool) -> Result<Vec<ModelRecord>, StorageError> {
    sqlx::query(SQL_LIST_MODELS)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(model_from_row)
        .collect()
}

pub async fn insert_model(pool: &PgPool, input: &ModelInput) -> Result<i64, StorageError> {
    let row = sqlx::query(SQL_INSERT_MODEL)
        .bind(input.provider_id)
        .bind(&input.model_name)
        .bind(input.display_name.as_deref())
        .bind(input.base_url.as_deref())
        .bind(&input.capabilities)
        .bind(input.embedding_dim)
        .bind(input.pool_id)
        .bind(input.enabled)
        .bind(json_text(&input.config))
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i64, _>("id")?)
}

pub async fn update_model(pool: &PgPool, id: i64, input: &ModelInput) -> Result<(), StorageError> {
    sqlx::query(SQL_UPDATE_MODEL)
        .bind(id)
        .bind(input.provider_id)
        .bind(&input.model_name)
        .bind(input.display_name.as_deref())
        .bind(input.base_url.as_deref())
        .bind(&input.capabilities)
        .bind(input.embedding_dim)
        .bind(input.pool_id)
        .bind(input.enabled)
        .bind(json_text(&input.config))
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn set_model_pool(
    pool: &PgPool,
    id: i64,
    pool_id: Option<i64>,
) -> Result<(), StorageError> {
    sqlx::query(SQL_SET_MODEL_POOL)
        .bind(id)
        .bind(pool_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Attach every still-unpooled model of a provider to a pool. Backfill helper:
/// never overwrites an operator-set pool.
pub async fn attach_provider_models_to_pool_if_unpooled(
    pool: &PgPool,
    provider_id: i64,
    pool_id: i64,
) -> Result<u64, StorageError> {
    let result = sqlx::query(SQL_SET_PROVIDER_MODELS_POOL_IF_NULL)
        .bind(provider_id)
        .bind(pool_id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// Fill a provider's protocol only when unset. Backfill helper: never
/// overwrites an operator-set protocol.
pub async fn set_provider_protocol_if_null(
    pool: &PgPool,
    id: i64,
    protocol: &str,
) -> Result<(), StorageError> {
    sqlx::query(SQL_SET_PROVIDER_PROTOCOL_IF_NULL)
        .bind(id)
        .bind(protocol)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn list_pools(pool: &PgPool) -> Result<Vec<PoolRecord>, StorageError> {
    sqlx::query(SQL_LIST_POOLS)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(pool_from_row)
        .collect()
}

pub async fn insert_pool(pool: &PgPool, input: &PoolInput) -> Result<i64, StorageError> {
    let row = sqlx::query(SQL_INSERT_POOL)
        .bind(&input.name)
        .bind(input.max_concurrency)
        .bind(input.description.as_deref())
        .bind(input.config.to_string())
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i64, _>("id")?)
}

/// Idempotent pool creation for seeds/backfills; returns the pool id whether it
/// was just created or already existed.
pub async fn insert_pool_if_missing(pool: &PgPool, input: &PoolInput) -> Result<i64, StorageError> {
    sqlx::query(SQL_INSERT_POOL_IF_MISSING)
        .bind(&input.name)
        .bind(input.max_concurrency)
        .bind(input.description.as_deref())
        .bind(input.config.to_string())
        .execute(pool)
        .await?;
    let row = sqlx::query(SQL_GET_POOL_BY_NAME)
        .bind(&input.name)
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i64, _>("id")?)
}

pub async fn update_pool(pool: &PgPool, id: i64, input: &PoolInput) -> Result<(), StorageError> {
    sqlx::query(SQL_UPDATE_POOL)
        .bind(id)
        .bind(&input.name)
        .bind(input.max_concurrency)
        .bind(input.description.as_deref())
        .bind(input.config.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_pool(pool: &PgPool, id: i64) -> Result<(), StorageError> {
    sqlx::query(SQL_DELETE_POOL).bind(id).execute(pool).await?;
    Ok(())
}

pub async fn set_model_enabled(pool: &PgPool, id: i64, enabled: bool) -> Result<(), StorageError> {
    sqlx::query(SQL_SET_MODEL_ENABLED)
        .bind(id)
        .bind(enabled)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn patch_model_config(pool: &PgPool, id: i64, patch: &Value) -> Result<(), StorageError> {
    sqlx::query(SQL_PATCH_MODEL_CONFIG)
        .bind(id)
        .bind(json_text(patch))
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_model(pool: &PgPool, id: i64) -> Result<(), StorageError> {
    sqlx::query(SQL_DELETE_MODEL).bind(id).execute(pool).await?;
    Ok(())
}

pub async fn list_workflows(pool: &PgPool) -> Result<Vec<WorkflowRecord>, StorageError> {
    sqlx::query(SQL_LIST_WORKFLOWS)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(workflow_from_row)
        .collect()
}

pub async fn insert_workflow_if_missing(
    pool: &PgPool,
    key: &str,
    kind: &str,
    full_routing: bool,
    retry_max_hops: i32,
    retry_wall_ms: i32,
    enabled: bool,
) -> Result<(), StorageError> {
    sqlx::query(SQL_INSERT_WORKFLOW_IF_MISSING)
        .bind(key)
        .bind(kind)
        .bind(full_routing)
        .bind(retry_max_hops)
        .bind(retry_wall_ms)
        .bind(enabled)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn update_workflow(
    pool: &PgPool,
    key: &str,
    full_routing: bool,
    retry_max_hops: i32,
    retry_wall_ms: i32,
    enabled: bool,
) -> Result<(), StorageError> {
    sqlx::query(SQL_UPDATE_WORKFLOW)
        .bind(key)
        .bind(full_routing)
        .bind(retry_max_hops)
        .bind(retry_wall_ms)
        .bind(enabled)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn set_workflow_enabled(
    pool: &PgPool,
    key: &str,
    enabled: bool,
) -> Result<(), StorageError> {
    sqlx::query(SQL_SET_WORKFLOW_ENABLED)
        .bind(key)
        .bind(enabled)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn list_assignments(pool: &PgPool) -> Result<Vec<AssignmentRecord>, StorageError> {
    sqlx::query(SQL_LIST_ASSIGNMENTS)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(assignment_from_row)
        .collect()
}

pub async fn insert_assignment(
    pool: &PgPool,
    input: &AssignmentInput,
) -> Result<i64, StorageError> {
    let row = sqlx::query(SQL_INSERT_ASSIGNMENT)
        .bind(&input.workflow_key)
        .bind(&input.scope)
        .bind(&input.role)
        .bind(input.provider_model_id)
        .bind(input.weight)
        .bind(input.fallback_order)
        .bind(input.canary_percent)
        .bind(input.enabled)
        .bind(json_text(&input.inference_overrides))
        .bind(input.cb_failure_threshold)
        .bind(input.cb_cooldown_ms)
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i64, _>("id")?)
}

pub async fn update_assignment(
    pool: &PgPool,
    id: i64,
    input: &AssignmentInput,
) -> Result<(), StorageError> {
    sqlx::query(SQL_UPDATE_ASSIGNMENT)
        .bind(id)
        .bind(&input.workflow_key)
        .bind(&input.scope)
        .bind(&input.role)
        .bind(input.provider_model_id)
        .bind(input.weight)
        .bind(input.fallback_order)
        .bind(input.canary_percent)
        .bind(input.enabled)
        .bind(json_text(&input.inference_overrides))
        .bind(input.cb_failure_threshold)
        .bind(input.cb_cooldown_ms)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_assignment(pool: &PgPool, id: i64) -> Result<(), StorageError> {
    sqlx::query(SQL_DELETE_ASSIGNMENT)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_assignments_for_scope(
    pool: &PgPool,
    workflow_key: &str,
    scope: &str,
) -> Result<(), StorageError> {
    sqlx::query(SQL_DELETE_ASSIGNMENTS_FOR_SCOPE)
        .bind(workflow_key)
        .bind(scope)
        .execute(pool)
        .await?;
    Ok(())
}

/// Batched weight save: update every (id, weight) pair in one transaction so a
/// draft rebalance lands atomically and triggers exactly one router reload.
pub async fn set_assignment_weights(
    pool: &PgPool,
    weights: &[(i64, Option<i32>)],
) -> Result<(), StorageError> {
    let mut tx = pool.begin().await?;
    for (id, weight) in weights {
        sqlx::query(SQL_SET_ASSIGNMENT_WEIGHT)
            .bind(id)
            .bind(weight)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Batched fallback reorder: assign ascending `fallback_order` following the
/// given id order, in one transaction (fixes the old two-POST swap race).
pub async fn set_assignment_fallback_orders(
    pool: &PgPool,
    ordered_ids: &[i64],
) -> Result<(), StorageError> {
    let mut tx = pool.begin().await?;
    for (position, id) in ordered_ids.iter().enumerate() {
        sqlx::query(SQL_SET_ASSIGNMENT_FALLBACK_ORDER)
            .bind(id)
            .bind(i32::try_from(position).unwrap_or(i32::MAX))
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Create an overflow assignment and its trigger in one transaction, so a
/// failed trigger insert can never strand an orphan assignment.
pub async fn insert_trigger_with_assignment(
    pool: &PgPool,
    assignment: &AssignmentInput,
    trigger: &TriggerInput,
) -> Result<(i64, i64), StorageError> {
    let mut tx = pool.begin().await?;
    let assignment_id = sqlx::query(SQL_INSERT_ASSIGNMENT)
        .bind(&assignment.workflow_key)
        .bind(&assignment.scope)
        .bind(&assignment.role)
        .bind(assignment.provider_model_id)
        .bind(assignment.weight)
        .bind(assignment.fallback_order)
        .bind(assignment.canary_percent)
        .bind(assignment.enabled)
        .bind(json_text(&assignment.inference_overrides))
        .bind(assignment.cb_failure_threshold)
        .bind(assignment.cb_cooldown_ms)
        .fetch_one(&mut *tx)
        .await?
        .try_get::<i64, _>("id")?;
    let trigger_id = sqlx::query(SQL_INSERT_TRIGGER)
        .bind(&trigger.workflow_key)
        .bind(&trigger.trigger_type)
        .bind(assignment_id)
        .bind(trigger.enabled)
        .bind(trigger.queue_name.as_deref())
        .bind(trigger.high_watermark)
        .bind(trigger.low_watermark)
        .bind(json_text(&trigger.params))
        .fetch_one(&mut *tx)
        .await?
        .try_get::<i64, _>("id")?;
    tx.commit().await?;
    Ok((assignment_id, trigger_id))
}

pub async fn list_triggers(pool: &PgPool) -> Result<Vec<TriggerRecord>, StorageError> {
    sqlx::query(SQL_LIST_TRIGGERS)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(trigger_from_row)
        .collect()
}

pub async fn insert_trigger(pool: &PgPool, input: &TriggerInput) -> Result<i64, StorageError> {
    let row = sqlx::query(SQL_INSERT_TRIGGER)
        .bind(&input.workflow_key)
        .bind(&input.trigger_type)
        .bind(input.engage_assignment_id)
        .bind(input.enabled)
        .bind(input.queue_name.as_deref())
        .bind(input.high_watermark)
        .bind(input.low_watermark)
        .bind(json_text(&input.params))
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i64, _>("id")?)
}

pub async fn update_trigger(
    pool: &PgPool,
    id: i64,
    input: &TriggerInput,
) -> Result<(), StorageError> {
    sqlx::query(SQL_UPDATE_TRIGGER)
        .bind(id)
        .bind(&input.workflow_key)
        .bind(&input.trigger_type)
        .bind(input.engage_assignment_id)
        .bind(input.enabled)
        .bind(input.queue_name.as_deref())
        .bind(input.high_watermark)
        .bind(input.low_watermark)
        .bind(json_text(&input.params))
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_trigger(pool: &PgPool, id: i64) -> Result<(), StorageError> {
    sqlx::query(SQL_DELETE_TRIGGER)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn insert_routing_event(
    pool: &PgPool,
    input: &RoutingEventInput,
) -> Result<i64, StorageError> {
    let row = sqlx::query(SQL_INSERT_ROUTING_EVENT_RETURNING_ID)
        .bind(&input.severity)
        .bind(&input.event_type)
        .bind(&input.workflow_key)
        .bind(input.provider_id)
        .bind(input.model_id)
        .bind(input.queue_name.as_deref())
        .bind(input.job_id)
        .bind(input.chat_id)
        .bind(input.user_id)
        .bind(input.thread_id)
        .bind(input.message_id)
        .bind(&input.dedupe_key)
        .bind(&input.summary)
        .bind(json_text(&input.detail))
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i64, _>("id")?)
}

pub async fn insert_routing_events(
    pool: &PgPool,
    events: &[RoutingEventInput],
) -> Result<(), StorageError> {
    if events.is_empty() {
        return Ok(());
    }
    let mut builder = routing_event_insert_builder(events);
    builder.build().execute(pool).await?;
    Ok(())
}

pub async fn list_routing_events(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<RoutingEventRecord>, StorageError> {
    let limit = limit.clamp(1, 1_000);
    sqlx::query(SQL_LIST_ROUTING_EVENTS)
        .bind(limit)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(routing_event_from_row)
        .collect()
}

const SQL_LIST_ROUTING_EVENTS_PAGE: &str = r#"SELECT
    id,
    created_at,
    severity,
    event_type,
    workflow_key,
    provider_id,
    model_id,
    queue_name,
    job_id,
    chat_id,
    user_id,
    thread_id,
    message_id,
    dedupe_key,
    summary,
    detail::text AS detail
FROM llm_routing_events
WHERE ($2::bigint IS NULL OR id < $2)
  AND ($3::text IS NULL OR workflow_key = $3)
  AND ($4::text IS NULL OR severity = $4)
ORDER BY id DESC
LIMIT $1"#;

/// Keyset-paginated event feed for the admin UI: newest first, `before_id`
/// continues past the previous page, optional workflow/severity filters.
pub async fn list_routing_events_page(
    pool: &PgPool,
    limit: i64,
    before_id: Option<i64>,
    workflow_key: Option<&str>,
    severity: Option<&str>,
) -> Result<Vec<RoutingEventRecord>, StorageError> {
    let limit = limit.clamp(1, 500);
    sqlx::query(SQL_LIST_ROUTING_EVENTS_PAGE)
        .bind(limit)
        .bind(before_id)
        .bind(workflow_key)
        .bind(severity)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(routing_event_from_row)
        .collect()
}

impl PostgresRoutingAdminReportStore {
    pub async fn snapshot(
        &self,
        since: OffsetDateTime,
    ) -> Result<RoutingAdminIncidentSnapshot, StorageError> {
        let rows = sqlx::query(SQL_ROUTING_ADMIN_INCIDENT_SNAPSHOT)
            .bind(since)
            .fetch_all(&self.pool)
            .await?;
        let Some(first) = rows.first() else {
            return Ok(RoutingAdminIncidentSnapshot::default());
        };
        let mut snapshot = RoutingAdminIncidentSnapshot {
            total_occurrences: first.try_get("total_occurrences")?,
            affected_users: first.try_get("total_affected_users")?,
            affected_chats: first.try_get("total_affected_chats")?,
            affected_jobs: first.try_get("total_affected_jobs")?,
            total_groups: first.try_get("total_groups")?,
            groups: Vec::with_capacity(rows.len()),
        };
        for row in rows {
            let Some(dedupe_key) = row.try_get::<Option<String>, _>("dedupe_key")? else {
                continue;
            };
            let samples = parse_json(row.try_get("samples")?)?;
            snapshot.groups.push(RoutingAdminIncidentGroup {
                dedupe_key,
                severity: row.try_get("severity")?,
                event_type: row.try_get("event_type")?,
                workflow_key: row.try_get("workflow_key")?,
                provider_id: row.try_get("provider_id")?,
                provider_name: row.try_get("provider_name")?,
                model_id: row.try_get("model_id")?,
                model_name: row.try_get("model_name")?,
                queue_name: row.try_get("queue_name")?,
                summary: row.try_get("summary")?,
                reason_counts: parse_json(row.try_get("reason_counts")?)?,
                occurrences: row.try_get("occurrences")?,
                affected_users: row.try_get("affected_users")?,
                affected_chats: row.try_get("affected_chats")?,
                affected_jobs: row.try_get("affected_jobs")?,
                first_seen: row.try_get("first_seen")?,
                last_seen: row.try_get("last_seen")?,
                samples: serde_json::from_value(samples)
                    .map_err(|source| StorageError::RoutingJsonCodec { source })?,
            });
        }
        Ok(snapshot)
    }

    pub async fn state(
        &self,
        admin_id: i64,
    ) -> Result<Option<RoutingAdminReportState>, StorageError> {
        sqlx::query(SQL_ROUTING_ADMIN_REPORT_STATE)
            .bind(admin_id)
            .fetch_optional(&self.pool)
            .await?
            .as_ref()
            .map(routing_admin_report_state_from_row)
            .transpose()
    }

    pub async fn claim_pending(
        &self,
        claim: &RoutingAdminReportPendingClaim,
    ) -> Result<bool, StorageError> {
        let claimed = match claim.kind {
            RoutingAdminReportOperationKind::Send => {
                sqlx::query_scalar::<_, i64>(SQL_CLAIM_ROUTING_ADMIN_REPORT_SEND)
                    .bind(claim.admin_id)
                    .bind(&claim.virtual_id)
                    .bind(&claim.fingerprint)
                    .bind(claim.now)
                    .bind(claim.stale_pending_before)
                    .bind(claim.retry_before)
                    .bind(claim.send_before)
                    .fetch_optional(&self.pool)
                    .await?
            }
            RoutingAdminReportOperationKind::Edit => {
                let Some(message_id) = claim.expected_message_id else {
                    return Ok(false);
                };
                sqlx::query_scalar::<_, i64>(SQL_CLAIM_ROUTING_ADMIN_REPORT_EDIT)
                    .bind(claim.admin_id)
                    .bind(&claim.virtual_id)
                    .bind(&claim.fingerprint)
                    .bind(claim.now)
                    .bind(message_id)
                    .bind(claim.stale_pending_before)
                    .bind(claim.retry_before)
                    .fetch_optional(&self.pool)
                    .await?
            }
        };
        Ok(claimed.is_some())
    }

    pub async fn record_delivery(
        &self,
        result: &RoutingAdminReportDeliveryResult,
    ) -> Result<bool, StorageError> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(SQL_ROUTING_ADMIN_REPORT_STATE_BY_PENDING_FOR_UPDATE)
            .bind(&result.virtual_id)
            .fetch_optional(&mut *tx)
            .await?;
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(false);
        };
        let state = routing_admin_report_state_from_row(&row)?;
        let next = next_admin_report_delivery_state(state, result);
        sqlx::query(SQL_UPDATE_ROUTING_ADMIN_REPORT_STATE)
            .bind(next.admin_id)
            .bind(next.telegram_message_id)
            .bind(next.last_new_message_at)
            .bind(next.last_rendered_fingerprint.as_deref())
            .bind(next.pending_virtual_id.as_deref())
            .bind(
                next.pending_kind
                    .map(RoutingAdminReportOperationKind::as_str),
            )
            .bind(next.pending_fingerprint.as_deref())
            .bind(next.pending_started_at)
            .bind(next.last_delivery_attempt_at)
            .bind(next.last_delivery_error_class.as_deref())
            .bind(next.updated_at.unwrap_or(result.at))
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }
}

pub async fn delete_old_llm_routing_events_batch(
    pool: &PgPool,
    retention_days: i32,
    batch_size: i64,
) -> Result<u64, sqlx::Error> {
    if retention_days <= 0 || batch_size <= 0 {
        return Ok(0);
    }
    let result = sqlx::query(SQL_DELETE_OLD_LLM_ROUTING_EVENTS_BATCH)
        .bind(retention_days)
        .bind(batch_size)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

fn routing_event_insert_builder(events: &[RoutingEventInput]) -> QueryBuilder<Postgres> {
    let mut builder = QueryBuilder::new(SQL_INSERT_ROUTING_EVENTS_PREFIX);
    builder.push(" ");
    builder.push_values(events.iter(), |mut row, event| {
        row.push_bind(event.severity.clone())
            .push_bind(event.event_type.clone())
            .push_bind(event.workflow_key.clone())
            .push_bind(event.provider_id)
            .push_bind(event.model_id)
            .push_bind(event.queue_name.clone())
            .push_bind(event.job_id)
            .push_bind(event.chat_id)
            .push_bind(event.user_id)
            .push_bind(event.thread_id)
            .push_bind(event.message_id)
            .push_bind(event.dedupe_key.clone())
            .push_bind(event.summary.clone())
            .push_bind(sqlx::types::Json(event.detail.clone()));
    });
    builder
}

#[cfg(test)]
fn routing_event_insert_sql_for_test(events: &[RoutingEventInput]) -> String {
    use sqlx::Execute as _;

    routing_event_insert_builder(events)
        .build()
        .sql()
        .as_ref()
        .to_owned()
}

/// Derive a 32-byte AES-256 key from the operator's `MASTER_KEY` secret string.
fn derive_key(master_secret: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(master_secret.as_bytes());
    hasher.finalize().into()
}

/// Seal an admin-entered provider key: `nonce(12) || AES-256-GCM ciphertext`.
pub fn seal_key(master_secret: &str, plaintext: &str) -> Result<Vec<u8>, StorageError> {
    if master_secret.is_empty() {
        return Err(StorageError::RoutingMasterKeyMissing);
    }
    let key = derive_key(master_secret);
    let cipher =
        Aes256Gcm::new_from_slice(&key).map_err(|err| StorageError::RoutingKeyEncrypt {
            message: err.to_string(),
        })?;
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ciphertext = cipher
        .encrypt(&nonce, plaintext.as_bytes())
        .map_err(|err| StorageError::RoutingKeyEncrypt {
            message: err.to_string(),
        })?;
    let mut sealed = Vec::with_capacity(AES_GCM_NONCE_LEN + ciphertext.len());
    sealed.extend_from_slice(nonce.as_slice());
    sealed.extend_from_slice(&ciphertext);
    Ok(sealed)
}

/// Open a sealed provider key. Used only to register the plaintext with the log
/// masker and to build adapters; never returned to the admin API.
pub fn open_key(master_secret: &str, sealed: &[u8]) -> Result<String, StorageError> {
    if master_secret.is_empty() {
        return Err(StorageError::RoutingMasterKeyMissing);
    }
    if sealed.len() <= AES_GCM_NONCE_LEN {
        return Err(StorageError::RoutingKeyDecrypt {
            message: "sealed blob too short".to_owned(),
        });
    }
    let key = derive_key(master_secret);
    let cipher =
        Aes256Gcm::new_from_slice(&key).map_err(|err| StorageError::RoutingKeyDecrypt {
            message: err.to_string(),
        })?;
    let (nonce_bytes, ciphertext) = sealed.split_at(AES_GCM_NONCE_LEN);
    let nonce = Nonce::from_slice(nonce_bytes);
    let plaintext =
        cipher
            .decrypt(nonce, ciphertext)
            .map_err(|err| StorageError::RoutingKeyDecrypt {
                message: err.to_string(),
            })?;
    String::from_utf8(plaintext).map_err(|err| StorageError::RoutingKeyDecrypt {
        message: err.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_event_insert_sql_targets_dedicated_table() {
        let sql = routing_event_insert_sql_for_test(&[RoutingEventInput {
            severity: "error".to_owned(),
            event_type: "route_unavailable".to_owned(),
            workflow_key: "dialog".to_owned(),
            provider_id: Some(10),
            model_id: Some(20),
            queue_name: Some("text".to_owned()),
            job_id: Some(30),
            chat_id: Some(-100),
            user_id: Some(42),
            thread_id: Some(5),
            message_id: Some(77),
            dedupe_key: "route:dialog".to_owned(),
            summary: "dialog route is unavailable".to_owned(),
            detail: serde_json::json!({"reason": "workflow_disabled"}),
        }]);

        assert!(sql.starts_with("INSERT INTO llm_routing_events"));
        assert!(sql.contains("provider_id"));
        assert!(sql.contains("user_id"));
        assert!(sql.contains("dedupe_key"));
        assert!(!sql.contains("llm_request_events"));
    }

    #[test]
    fn admin_report_send_delivery_records_real_message_and_hourly_gate() {
        let at = OffsetDateTime::from_unix_timestamp(1_780_000_000).expect("timestamp");
        let state = RoutingAdminReportState {
            admin_id: 42,
            pending_virtual_id: Some("routing-admin-report:send:42:1".to_owned()),
            pending_kind: Some(RoutingAdminReportOperationKind::Send),
            pending_fingerprint: Some("digest-a".to_owned()),
            ..RoutingAdminReportState::default()
        };

        let next = next_admin_report_delivery_state(
            state,
            &RoutingAdminReportDeliveryResult {
                virtual_id: "routing-admin-report:send:42:1".to_owned(),
                sent_message_id: Some(77),
                succeeded: true,
                error_class: None,
                at,
            },
        );

        assert_eq!(next.telegram_message_id, Some(77));
        assert_eq!(next.last_new_message_at, Some(at));
        assert_eq!(next.last_rendered_fingerprint.as_deref(), Some("digest-a"));
        assert!(next.pending_virtual_id.is_none());
        assert!(next.pending_kind.is_none());
        assert!(next.pending_fingerprint.is_none());
    }

    #[test]
    fn terminal_edit_failure_drops_only_the_unusable_edit_target() {
        let sent_at = OffsetDateTime::from_unix_timestamp(1_779_996_400).expect("timestamp");
        let failed_at = OffsetDateTime::from_unix_timestamp(1_780_000_000).expect("timestamp");
        let state = RoutingAdminReportState {
            admin_id: 42,
            telegram_message_id: Some(77),
            last_new_message_at: Some(sent_at),
            last_rendered_fingerprint: Some("digest-a".to_owned()),
            pending_virtual_id: Some("routing-admin-report:edit:42:2".to_owned()),
            pending_kind: Some(RoutingAdminReportOperationKind::Edit),
            pending_fingerprint: Some("digest-b".to_owned()),
            ..RoutingAdminReportState::default()
        };

        let next = next_admin_report_delivery_state(
            state,
            &RoutingAdminReportDeliveryResult {
                virtual_id: "routing-admin-report:edit:42:2".to_owned(),
                sent_message_id: None,
                succeeded: false,
                error_class: Some("terminal_bad_request".to_owned()),
                at: failed_at,
            },
        );

        assert_eq!(next.telegram_message_id, None);
        assert_eq!(next.last_new_message_at, Some(sent_at));
        assert_eq!(next.last_rendered_fingerprint.as_deref(), Some("digest-a"));
        assert_eq!(next.last_delivery_attempt_at, Some(failed_at));
        assert_eq!(
            next.last_delivery_error_class.as_deref(),
            Some("terminal_bad_request")
        );
    }

    #[test]
    fn admin_report_migration_adds_identity_and_restart_safe_state() {
        const UP: &str = include_str!("../../../migrations/184_llm_admin_incident_reports.up.sql");
        const DOWN: &str =
            include_str!("../../../migrations/184_llm_admin_incident_reports.down.sql");

        assert!(UP.contains("ADD COLUMN user_id BIGINT"));
        assert!(UP.contains("CREATE TABLE llm_admin_report_state"));
        assert!(UP.contains("pending_virtual_id TEXT UNIQUE"));
        assert!(DOWN.contains("DROP TABLE IF EXISTS llm_admin_report_state"));
        assert!(DOWN.contains("DROP COLUMN IF EXISTS user_id"));
    }

    #[test]
    fn routing_event_cleanup_sql_uses_retention_window() {
        assert!(SQL_DELETE_OLD_LLM_ROUTING_EVENTS_BATCH.contains("llm_routing_events"));
        assert!(SQL_DELETE_OLD_LLM_ROUTING_EVENTS_BATCH.contains("created_at < now()"));
        assert!(SQL_DELETE_OLD_LLM_ROUTING_EVENTS_BATCH.contains("LIMIT $2"));
    }

    #[test]
    fn safe_provider_enabled_sql_preserves_key_material() {
        assert!(SQL_SET_PROVIDER_ENABLED.contains("SET enabled = $2"));
        assert!(!SQL_SET_PROVIDER_ENABLED.contains("api_key_ref"));
        assert!(!SQL_SET_PROVIDER_ENABLED.contains("api_key_encrypted"));
    }

    #[test]
    fn safe_config_patch_sql_merges_jsonb_without_full_replacement() {
        assert!(SQL_PATCH_PROVIDER_CONFIG.contains("config = COALESCE(config"));
        assert!(SQL_PATCH_PROVIDER_CONFIG.contains("|| $2::jsonb"));
        assert!(SQL_PATCH_MODEL_CONFIG.contains("config = COALESCE(config"));
        assert!(SQL_PATCH_MODEL_CONFIG.contains("|| $2::jsonb"));
    }

    #[test]
    fn model_list_sql_exposes_config_for_runtime_snapshots() {
        assert!(SQL_LIST_MODELS.contains("config::text AS config"));
    }

    #[test]
    fn pool_sql_targets_capacity_pools_table() {
        assert!(SQL_LIST_POOLS.contains("FROM llm_capacity_pools"));
        assert!(SQL_LIST_POOLS.contains("config::text AS config"));
        assert!(SQL_INSERT_POOL.contains("INSERT INTO llm_capacity_pools"));
        assert!(SQL_INSERT_POOL.contains("$4::jsonb"));
        assert!(SQL_INSERT_POOL_IF_MISSING.contains("ON CONFLICT (name) DO NOTHING"));
        assert!(SQL_INSERT_POOL_IF_MISSING.contains("$4::jsonb"));
        assert!(SQL_UPDATE_POOL.contains("max_concurrency = $3"));
        assert!(SQL_UPDATE_POOL.contains("config = $5::jsonb"));
        assert!(SQL_DELETE_POOL.contains("DELETE FROM llm_capacity_pools"));
    }

    #[test]
    fn model_sql_carries_pool_id() {
        assert!(SQL_LIST_MODELS.contains("pool_id"));
        assert!(SQL_INSERT_MODEL.contains("pool_id"));
        assert!(SQL_UPDATE_MODEL.contains("pool_id = $8"));
        assert!(SQL_SET_MODEL_POOL.contains("SET pool_id = $2"));
    }

    #[test]
    fn provider_sql_carries_protocol_and_runtime_hint() {
        assert!(SQL_LIST_PROVIDERS.contains("protocol"));
        assert!(SQL_LIST_PROVIDERS.contains("runtime_hint"));
        assert!(SQL_INSERT_PROVIDER.contains("protocol"));
        assert!(SQL_UPDATE_PROVIDER.contains("protocol = $4"));
        assert!(SQL_UPDATE_PROVIDER.contains("runtime_hint = $5"));
    }

    #[test]
    fn backfill_helpers_never_overwrite_operator_values() {
        assert!(SQL_SET_PROVIDER_PROTOCOL_IF_NULL.contains("AND protocol IS NULL"));
        assert!(SQL_SET_PROVIDER_MODELS_POOL_IF_NULL.contains("AND pool_id IS NULL"));
    }

    #[test]
    fn batched_assignment_updates_touch_only_their_column() {
        assert!(SQL_SET_ASSIGNMENT_WEIGHT.contains("SET weight = $2"));
        assert!(!SQL_SET_ASSIGNMENT_WEIGHT.contains("fallback_order"));
        assert!(SQL_SET_ASSIGNMENT_FALLBACK_ORDER.contains("SET fallback_order = $2"));
        assert!(!SQL_SET_ASSIGNMENT_FALLBACK_ORDER.contains("weight ="));
    }

    #[test]
    fn routing_events_page_sql_uses_keyset_pagination_and_filters() {
        assert!(SQL_LIST_ROUTING_EVENTS_PAGE.contains("id < $2"));
        assert!(SQL_LIST_ROUTING_EVENTS_PAGE.contains("workflow_key = $3"));
        assert!(SQL_LIST_ROUTING_EVENTS_PAGE.contains("severity = $4"));
        assert!(SQL_LIST_ROUTING_EVENTS_PAGE.contains("ORDER BY id DESC"));
    }

    #[test]
    fn workflow_insert_sql_is_additive() {
        assert!(SQL_INSERT_WORKFLOW_IF_MISSING.contains("ON CONFLICT (key) DO NOTHING"));
        assert!(SQL_INSERT_WORKFLOW_IF_MISSING.contains("full_routing"));
        assert!(!SQL_INSERT_WORKFLOW_IF_MISSING.contains("UPDATE"));
    }

    #[test]
    fn seal_then_open_roundtrips() {
        let sealed = seal_key("master-secret", "sk-provider-key").expect("seal");
        assert!(sealed.len() > AES_GCM_NONCE_LEN);
        let opened = open_key("master-secret", &sealed).expect("open");
        assert_eq!(opened, "sk-provider-key");
    }

    #[test]
    fn open_with_wrong_secret_fails() {
        let sealed = seal_key("right", "secret").expect("seal");
        assert!(open_key("wrong", &sealed).is_err());
    }

    #[test]
    fn empty_master_secret_is_rejected() {
        assert!(matches!(
            seal_key("", "x"),
            Err(StorageError::RoutingMasterKeyMissing)
        ));
    }

    #[test]
    fn nonce_randomizes_ciphertext() {
        let a = seal_key("m", "same").expect("seal a");
        let b = seal_key("m", "same").expect("seal b");
        assert_ne!(a, b, "nonce must make ciphertext non-deterministic");
    }
}
