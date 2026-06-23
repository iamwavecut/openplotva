//! Postgres persistence for the admin-configurable LLM routing system.
//!
//! Provides the consistent snapshot read the app loader turns into a routing
//! table, the admin CRUD over providers/models/workflows/assignments/triggers,
//! and AES-GCM sealing of admin-entered provider keys. JSONB columns follow the
//! crate convention of binding `$N::jsonb` from a JSON string and selecting
//! `column::text`; `TEXT[]` maps directly to `Vec<String>`.

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row, postgres::PgRow};

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
    pub enabled: bool,
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
    pub enabled: bool,
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
}

const SQL_LIST_PROVIDERS: &str = "SELECT id, name, kind, endpoint, discovery_service_name, discovery_endpoint_name, api_key_ref, api_key_encrypted, enabled, config::text AS config FROM llm_providers ORDER BY id ASC";
const SQL_GET_PROVIDER: &str = "SELECT id, name, kind, endpoint, discovery_service_name, discovery_endpoint_name, api_key_ref, api_key_encrypted, enabled, config::text AS config FROM llm_providers WHERE id = $1";
const SQL_INSERT_PROVIDER: &str = "INSERT INTO llm_providers (name, kind, endpoint, discovery_service_name, discovery_endpoint_name, api_key_ref, api_key_encrypted, enabled, config) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb) RETURNING id";
const SQL_UPDATE_PROVIDER: &str = "UPDATE llm_providers SET name = $2, kind = $3, endpoint = $4, discovery_service_name = $5, discovery_endpoint_name = $6, api_key_ref = $7, api_key_encrypted = $8, enabled = $9, config = $10::jsonb, updated_at = now() WHERE id = $1";
const SQL_DELETE_PROVIDER: &str = "DELETE FROM llm_providers WHERE id = $1";

const SQL_LIST_MODELS: &str = "SELECT id, provider_id, model_name, display_name, base_url, capabilities, embedding_dim, enabled, config::text AS config FROM provider_models ORDER BY id ASC";
const SQL_INSERT_MODEL: &str = "INSERT INTO provider_models (provider_id, model_name, display_name, base_url, capabilities, embedding_dim, enabled, config) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb) RETURNING id";
const SQL_UPDATE_MODEL: &str = "UPDATE provider_models SET provider_id = $2, model_name = $3, display_name = $4, base_url = $5, capabilities = $6, embedding_dim = $7, enabled = $8, config = $9::jsonb WHERE id = $1";
const SQL_DELETE_MODEL: &str = "DELETE FROM provider_models WHERE id = $1";

const SQL_LIST_WORKFLOWS: &str = "SELECT key, kind, full_routing, retry_max_hops, retry_wall_ms, enabled FROM workflows ORDER BY key ASC";
const SQL_UPDATE_WORKFLOW: &str = "UPDATE workflows SET full_routing = $2, retry_max_hops = $3, retry_wall_ms = $4, enabled = $5 WHERE key = $1";

const SQL_LIST_ASSIGNMENTS: &str = "SELECT id, workflow_key, scope, role, provider_model_id, weight, fallback_order, canary_percent, enabled, inference_overrides::text AS inference_overrides, cb_failure_threshold, cb_cooldown_ms FROM workflow_assignments ORDER BY id ASC";
const SQL_INSERT_ASSIGNMENT: &str = "INSERT INTO workflow_assignments (workflow_key, scope, role, provider_model_id, weight, fallback_order, canary_percent, enabled, inference_overrides, cb_failure_threshold, cb_cooldown_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11) RETURNING id";
const SQL_DELETE_ASSIGNMENT: &str = "DELETE FROM workflow_assignments WHERE id = $1";
const SQL_DELETE_ASSIGNMENTS_FOR_SCOPE: &str =
    "DELETE FROM workflow_assignments WHERE workflow_key = $1 AND scope = $2";

const SQL_LIST_TRIGGERS: &str = "SELECT id, workflow_key, trigger_type, engage_assignment_id, enabled, queue_name, high_watermark, low_watermark, params::text AS params FROM workflow_triggers ORDER BY id ASC";
const SQL_INSERT_TRIGGER: &str = "INSERT INTO workflow_triggers (workflow_key, trigger_type, engage_assignment_id, enabled, queue_name, high_watermark, low_watermark, params) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb) RETURNING id";
const SQL_DELETE_TRIGGER: &str = "DELETE FROM workflow_triggers WHERE id = $1";

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
        enabled: row.try_get("enabled")?,
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

    tx.commit().await?;
    Ok(RoutingSnapshot {
        providers,
        models,
        workflows,
        assignments,
        triggers,
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
        .bind(input.enabled)
        .bind(json_text(&input.config))
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

pub async fn delete_trigger(pool: &PgPool, id: i64) -> Result<(), StorageError> {
    sqlx::query(SQL_DELETE_TRIGGER)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
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
