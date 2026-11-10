//! Provider configuration, routing controls, and routing journal.
use super::{
    HttpRouterState,
    auth::{admin_auth_failure_response, require_admin_request},
    common::{
        admin_app_setting, admin_error_response, admin_json_no_cache_response, admin_json_response,
        admin_upsert_app_setting, query_values as admin_auth_query_values,
    },
};
use crate::{dialog_workers, model_routing, openrouter_free_pool, runtime_routing};
use axum::{
    body::Bytes,
    extract::{RawQuery, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
};
use openplotva_storage::database as sqlx;
use serde_json::json;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
#[derive(Clone)]
pub(crate) struct RoutingHttpState {
    admin_ids: Arc<[i64]>,
    bot_token: Arc<str>,
    dialog_worker_gauge: Option<Arc<dialog_workers::WorkerGauge>>,
    openrouter_free_gate: Option<Arc<openrouter_free_pool::OpenRouterFreeQuotaGate>>,
    postgres: Option<PgPool>,
    router_breakers: Option<Arc<openplotva_llm::router::BreakerSet>>,
    router_handle: Option<Arc<openplotva_llm::router::RouterHandle>>,
    router_pools: Option<Arc<openplotva_llm::router::PoolRegistry>>,
    router_runtime: Option<Arc<model_routing::RouterRuntime>>,
    router_triggers: Option<Arc<openplotva_llm::router::TriggerState>>,
    routing_event_buffer: Option<runtime_routing::RoutingEventBuffer>,
    routing_event_reporter: Option<runtime_routing::RoutingEventReporter>,
}
impl axum::extract::FromRef<Arc<HttpRouterState>> for RoutingHttpState {
    fn from_ref(routes: &Arc<HttpRouterState>) -> Self {
        Self {
            admin_ids: routes.admin_ids.clone(),
            bot_token: routes.bot_token.clone(),
            dialog_worker_gauge: routes.dialog_worker_gauge.clone(),
            openrouter_free_gate: routes.openrouter_free_gate.clone(),
            postgres: routes.postgres.clone(),
            router_breakers: routes.router_breakers.clone(),
            router_handle: routes.router_handle.clone(),
            router_pools: routes.router_pools.clone(),
            router_runtime: routes.router_runtime.clone(),
            router_triggers: routes.router_triggers.clone(),
            routing_event_buffer: routes.routing_event_buffer.clone(),
            routing_event_reporter: routes.routing_event_reporter.clone(),
        }
    }
}
pub(crate) fn routing_master_secret() -> String {
    std::env::var("MASTER_KEY").unwrap_or_default()
}

pub(crate) fn routing_json_str(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

pub(crate) fn routing_json_i32(value: &serde_json::Value, key: &str) -> Option<i32> {
    value
        .get(key)
        .and_then(serde_json::Value::as_i64)
        .map(|n| n as i32)
}

pub(crate) fn routing_json_bool(value: &serde_json::Value, key: &str, default: bool) -> bool {
    value
        .get(key)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(default)
}

pub(crate) fn routing_json_required_bool(
    value: &serde_json::Value,
    key: &str,
) -> Result<bool, String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| format!("{key} is required"))
}

pub(crate) fn routing_json_config_patch(
    value: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let patch = value
        .get("config")
        .or_else(|| value.get("patch"))
        .cloned()
        .ok_or_else(|| "config patch is required".to_owned())?;
    if !patch.is_object() {
        return Err("config patch must be a JSON object".to_owned());
    }
    Ok(patch)
}

pub(crate) fn routing_capabilities(value: &serde_json::Value) -> Vec<String> {
    value
        .get("capabilities")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn provider_key_state(
    provider: &openplotva_storage::llm_routing::ProviderRecord,
) -> &'static str {
    if provider.api_key_encrypted.is_some() {
        "encrypted"
    } else if provider.api_key_ref.is_some() {
        "ref"
    } else {
        "unset"
    }
}

/// Build a provider input from admin JSON, sealing a plaintext `api_key` under the
/// master key. The plaintext is never persisted or echoed; only the ciphertext is
/// stored, and `api_key_ref`/`api_key_encrypted` are mutually exclusive.
pub(crate) fn provider_input_from_json(
    value: &serde_json::Value,
) -> Result<openplotva_storage::llm_routing::ProviderInput, String> {
    let protocol = routing_json_str(value, "protocol");
    if let Some(protocol) = protocol.as_deref() {
        let Some(parsed) = openplotva_llm::provider_schema::Protocol::from_db(protocol) else {
            return Err(format!("unknown protocol {protocol}"));
        };
        let config = value.get("config").cloned().unwrap_or_else(|| json!({}));
        openplotva_llm::provider_schema::validate_provider_config(parsed, &config)
            .map_err(|error| error.to_string())?;
    }
    let runtime_hint = routing_json_str(value, "runtime_hint");
    if let Some(hint) = runtime_hint.as_deref()
        && openplotva_llm::provider_schema::RuntimeHint::from_db(hint).is_none()
    {
        return Err(format!("unknown runtime_hint {hint}"));
    }
    let mut input = openplotva_storage::llm_routing::ProviderInput {
        name: routing_json_str(value, "name").ok_or("name is required")?,
        kind: routing_json_str(value, "kind").unwrap_or_else(|| "chat".to_owned()),
        protocol,
        runtime_hint,
        endpoint: routing_json_str(value, "endpoint"),
        discovery_service_name: routing_json_str(value, "discovery_service_name"),
        discovery_endpoint_name: routing_json_str(value, "discovery_endpoint_name"),
        api_key_ref: routing_json_str(value, "api_key_ref"),
        api_key_encrypted: None,
        enabled: routing_json_bool(value, "enabled", true),
        config: value.get("config").cloned().unwrap_or_else(|| json!({})),
    };
    if let Some(plaintext) = routing_json_str(value, "api_key")
        && !plaintext.trim().is_empty()
    {
        let sealed =
            openplotva_storage::llm_routing::seal_key(&routing_master_secret(), &plaintext)
                .map_err(|error| error.to_string())?;
        input.api_key_encrypted = Some(sealed);
        input.api_key_ref = None;
    }
    Ok(input)
}

pub(crate) fn model_input_from_json(
    value: &serde_json::Value,
) -> Result<openplotva_storage::llm_routing::ModelInput, String> {
    if let Some(config) = value.get("config") {
        openplotva_llm::provider_schema::validate_model_config(config)
            .map_err(|error| error.to_string())?;
    }
    Ok(openplotva_storage::llm_routing::ModelInput {
        provider_id: value
            .get("provider_id")
            .and_then(serde_json::Value::as_i64)
            .ok_or("provider_id is required")?,
        model_name: routing_json_str(value, "model_name").ok_or("model_name is required")?,
        display_name: routing_json_str(value, "display_name"),
        base_url: routing_json_str(value, "base_url"),
        capabilities: routing_capabilities(value),
        embedding_dim: routing_json_i32(value, "embedding_dim"),
        pool_id: value.get("pool_id").and_then(serde_json::Value::as_i64),
        enabled: routing_json_bool(value, "enabled", true),
        config: value.get("config").cloned().unwrap_or_else(|| json!({})),
    })
}

pub(crate) fn pool_input_from_json(
    value: &serde_json::Value,
) -> Result<openplotva_storage::llm_routing::PoolInput, String> {
    let config = value.get("config").cloned().unwrap_or_else(|| json!({}));
    if !config.is_object() {
        return Err("config must be a JSON object".to_owned());
    }
    Ok(openplotva_storage::llm_routing::PoolInput {
        name: routing_json_str(value, "name").ok_or("name is required")?,
        max_concurrency: routing_json_i32(value, "max_concurrency"),
        description: routing_json_str(value, "description"),
        config,
    })
}

pub(crate) fn assignment_input_from_json(
    value: &serde_json::Value,
) -> Result<openplotva_storage::llm_routing::AssignmentInput, String> {
    Ok(openplotva_storage::llm_routing::AssignmentInput {
        workflow_key: routing_json_str(value, "workflow_key").ok_or("workflow_key is required")?,
        scope: routing_json_str(value, "scope").unwrap_or_else(|| "global".to_owned()),
        role: routing_json_str(value, "role").unwrap_or_else(|| "primary".to_owned()),
        provider_model_id: value
            .get("provider_model_id")
            .and_then(serde_json::Value::as_i64)
            .ok_or("provider_model_id is required")?,
        weight: routing_json_i32(value, "weight"),
        fallback_order: routing_json_i32(value, "fallback_order"),
        canary_percent: routing_json_i32(value, "canary_percent"),
        enabled: routing_json_bool(value, "enabled", true),
        inference_overrides: value
            .get("inference_overrides")
            .cloned()
            .unwrap_or_else(|| json!({})),
        cb_failure_threshold: routing_json_i32(value, "cb_failure_threshold").unwrap_or(5),
        cb_cooldown_ms: routing_json_i32(value, "cb_cooldown_ms").unwrap_or(30_000),
    })
}

pub(crate) fn trigger_input_from_json(
    value: &serde_json::Value,
) -> Result<openplotva_storage::llm_routing::TriggerInput, String> {
    let mut input = trigger_input_from_json_without_engage(value)?;
    input.engage_assignment_id = value
        .get("engage_assignment_id")
        .and_then(serde_json::Value::as_i64)
        .ok_or("engage_assignment_id is required")?;
    Ok(input)
}

/// Trigger fields without the engage target, for the atomic
/// assignment-plus-trigger create where the target id does not exist yet.
pub(crate) fn trigger_input_from_json_without_engage(
    value: &serde_json::Value,
) -> Result<openplotva_storage::llm_routing::TriggerInput, String> {
    Ok(openplotva_storage::llm_routing::TriggerInput {
        workflow_key: routing_json_str(value, "workflow_key").ok_or("workflow_key is required")?,
        trigger_type: routing_json_str(value, "trigger_type").ok_or("trigger_type is required")?,
        engage_assignment_id: 0,
        enabled: routing_json_bool(value, "enabled", true),
        queue_name: routing_json_str(value, "queue_name"),
        high_watermark: routing_json_i32(value, "high_watermark"),
        low_watermark: routing_json_i32(value, "low_watermark"),
        params: value.get("params").cloned().unwrap_or_else(|| json!({})),
    })
}

/// Server-side model-list fetch for the admin "Fetch models" flow: calls the
/// provider's listing endpoint and returns a diff against its imported models
/// (`new` / `existing` / `gone`). Read-only: import is a separate action.
pub(crate) async fn admin_fetch_provider_models(
    pool: &PgPool,
    provider_id: i64,
    body: &serde_json::Value,
) -> Result<serde_json::Value, (StatusCode, String)> {
    let bad = |error: String| (StatusCode::BAD_REQUEST, error);
    let provider = openplotva_storage::llm_routing::get_provider(pool, provider_id)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| bad(format!("unknown provider {provider_id}")))?;
    let protocol = provider
        .protocol
        .as_deref()
        .and_then(openplotva_llm::provider_schema::Protocol::from_db)
        .ok_or_else(|| bad("provider has no protocol; set one first".to_owned()))?;
    if !protocol.supports_model_listing() {
        return Err(bad(format!(
            "protocol {} has no model listing endpoint; add models manually",
            protocol.as_str()
        )));
    }
    let base_url = routing_json_str(body, "base_url")
        .or_else(|| provider.endpoint.clone())
        .filter(|url| !url.trim().is_empty())
        .ok_or_else(|| bad("provider has no endpoint to list models from".to_owned()))?;
    let api_key = provider
        .api_key_ref
        .as_deref()
        .filter(|reference| !reference.trim().is_empty())
        .and_then(|reference| std::env::var(reference.trim()).ok())
        .or_else(|| {
            provider.api_key_encrypted.as_deref().and_then(|sealed| {
                openplotva_storage::llm_routing::open_key(&routing_master_secret(), sealed).ok()
            })
        });
    let remote = openplotva_llm::model_listing::list_openai_compat_models(
        &base_url,
        api_key.as_deref(),
        Duration::from_secs(10),
    )
    .await
    .map_err(|error| (StatusCode::BAD_GATEWAY, error.to_string()))?;

    let imported: Vec<(i64, String)> = openplotva_storage::llm_routing::list_models(pool)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .filter(|model| model.provider_id == provider_id)
        .map(|model| (model.id, model.model_name))
        .collect();
    let imported_names: std::collections::HashSet<&str> =
        imported.iter().map(|(_, name)| name.as_str()).collect();
    let remote_names: std::collections::HashSet<&str> =
        remote.iter().map(|model| model.name.as_str()).collect();

    let new: Vec<&str> = remote
        .iter()
        .map(|model| model.name.as_str())
        .filter(|name| !imported_names.contains(name))
        .collect();
    let existing: Vec<serde_json::Value> = imported
        .iter()
        .filter(|(_, name)| remote_names.contains(name.as_str()))
        .map(|(id, name)| json!({ "id": id, "model_name": name }))
        .collect();
    let gone: Vec<serde_json::Value> = imported
        .iter()
        .filter(|(_, name)| !remote_names.contains(name.as_str()))
        .map(|(id, name)| json!({ "id": id, "model_name": name }))
        .collect();
    Ok(json!({
        "ok": true,
        "provider_id": provider_id,
        "base_url": base_url,
        "models": remote
            .iter()
            .map(|model| json!({ "model_name": model.name, "meta": model.raw }))
            .collect::<Vec<_>>(),
        "diff": { "new": new, "existing": existing, "gone": gone },
    }))
}

pub(crate) async fn admin_routing_snapshot_json(
    pool: &PgPool,
) -> Result<serde_json::Value, String> {
    let snapshot = openplotva_storage::llm_routing::load_snapshot(pool)
        .await
        .map_err(|error| error.to_string())?;
    let providers: Vec<serde_json::Value> = snapshot
        .providers
        .iter()
        .map(|provider| {
            json!({
                "id": provider.id,
                "name": provider.name,
                "kind": provider.kind,
                "protocol": provider.protocol,
                "runtime_hint": provider.runtime_hint,
                "endpoint": provider.endpoint,
                "discovery_service_name": provider.discovery_service_name,
                "discovery_endpoint_name": provider.discovery_endpoint_name,
                "key_state": provider_key_state(provider),
                "api_key_ref": provider.api_key_ref,
                "enabled": provider.enabled,
                "config": provider.config,
            })
        })
        .collect();
    let models: Vec<serde_json::Value> = snapshot
        .models
        .iter()
        .map(|model| {
            json!({
                "id": model.id,
                "provider_id": model.provider_id,
                "model_name": model.model_name,
                "display_name": model.display_name,
                "base_url": model.base_url,
                "capabilities": model.capabilities,
                "embedding_dim": model.embedding_dim,
                "pool_id": model.pool_id,
                "enabled": model.enabled,
                "config": model.config,
            })
        })
        .collect();
    let workflows: Vec<serde_json::Value> = snapshot
        .workflows
        .iter()
        .map(|workflow| {
            json!({
                "key": workflow.key,
                "kind": workflow.kind,
                "full_routing": workflow.full_routing,
                "retry_max_hops": workflow.retry_max_hops,
                "retry_wall_ms": workflow.retry_wall_ms,
                "enabled": workflow.enabled,
            })
        })
        .collect();
    let assignments: Vec<serde_json::Value> = snapshot
        .assignments
        .iter()
        .map(|assignment| {
            json!({
                "id": assignment.id,
                "workflow_key": assignment.workflow_key,
                "scope": assignment.scope,
                "role": assignment.role,
                "provider_model_id": assignment.provider_model_id,
                "weight": assignment.weight,
                "fallback_order": assignment.fallback_order,
                "canary_percent": assignment.canary_percent,
                "enabled": assignment.enabled,
                "inference_overrides": assignment.inference_overrides,
                "cb_failure_threshold": assignment.cb_failure_threshold,
                "cb_cooldown_ms": assignment.cb_cooldown_ms,
            })
        })
        .collect();
    let triggers: Vec<serde_json::Value> = snapshot
        .triggers
        .iter()
        .map(|trigger| {
            json!({
                "id": trigger.id,
                "workflow_key": trigger.workflow_key,
                "trigger_type": trigger.trigger_type,
                "engage_assignment_id": trigger.engage_assignment_id,
                "enabled": trigger.enabled,
                "queue_name": trigger.queue_name,
                "high_watermark": trigger.high_watermark,
                "low_watermark": trigger.low_watermark,
                "params": trigger.params,
            })
        })
        .collect();
    let pools: Vec<serde_json::Value> = snapshot
        .pools
        .iter()
        .map(|pool| {
            json!({
                "id": pool.id,
                "name": pool.name,
                "max_concurrency": pool.max_concurrency,
                "description": pool.description,
                "config": pool.config,
            })
        })
        .collect();
    Ok(json!({
        "providers": providers,
        "models": models,
        "workflows": workflows,
        "assignments": assignments,
        "triggers": triggers,
        "pools": pools,
        "param_descriptors": routing_param_descriptors(),
    }))
}

/// Static parameter-form descriptors for every (protocol, runtime hint) pair,
/// so the admin UI renders typed provider/model forms without hardcoding them.
pub(crate) fn routing_param_descriptors() -> serde_json::Value {
    use openplotva_llm::provider_schema::{Protocol, RuntimeHint, param_descriptor};
    let mut descriptors = Vec::new();
    for protocol in Protocol::all() {
        descriptors.push(param_descriptor(*protocol, None));
        if *protocol == Protocol::OpenAiCompat {
            for hint in RuntimeHint::all() {
                descriptors.push(param_descriptor(*protocol, Some(*hint)));
            }
        }
    }
    serde_json::Value::Array(descriptors)
}

pub(crate) async fn admin_routing_apply_action(
    pool: &PgPool,
    routes: &RoutingHttpState,
    body: &serde_json::Value,
) -> Result<serde_json::Value, (StatusCode, String)> {
    let action = body
        .get("action")
        .and_then(serde_json::Value::as_str)
        .ok_or((StatusCode::BAD_REQUEST, "action is required".to_owned()))?;
    let bad = |error: String| (StatusCode::BAD_REQUEST, error);
    let storage_err = |error: openplotva_storage::StorageError| {
        (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    };
    let id_of = |body: &serde_json::Value| body.get("id").and_then(serde_json::Value::as_i64);

    use openplotva_storage::llm_routing as routing;
    let result = match action {
        "create_provider" => {
            let input = provider_input_from_json(body).map_err(bad)?;
            let id = routing::insert_provider(pool, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true, "id": id })
        }
        "update_provider" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let input = provider_input_from_json(body).map_err(bad)?;
            routing::update_provider(pool, id, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "set_provider_enabled" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let enabled = routing_json_required_bool(body, "enabled").map_err(bad)?;
            routing::set_provider_enabled(pool, id, enabled)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "patch_provider_config" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let patch = routing_json_config_patch(body).map_err(bad)?;
            routing::patch_provider_config(pool, id, &patch)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "delete_provider" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            routing::delete_provider(pool, id)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "create_model" => {
            let input = model_input_from_json(body).map_err(bad)?;
            let id = routing::insert_model(pool, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true, "id": id })
        }
        "update_model" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let input = model_input_from_json(body).map_err(bad)?;
            routing::update_model(pool, id, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "set_model_enabled" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let enabled = routing_json_required_bool(body, "enabled").map_err(bad)?;
            routing::set_model_enabled(pool, id, enabled)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "patch_model_config" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let patch = routing_json_config_patch(body).map_err(bad)?;
            routing::patch_model_config(pool, id, &patch)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "delete_model" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            routing::delete_model(pool, id).await.map_err(storage_err)?;
            json!({ "ok": true })
        }
        "create_assignment" => {
            let input = assignment_input_from_json(body).map_err(bad)?;
            let id = routing::insert_assignment(pool, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true, "id": id })
        }
        "update_assignment" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let input = assignment_input_from_json(body).map_err(bad)?;
            routing::update_assignment(pool, id, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "delete_assignment" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            routing::delete_assignment(pool, id)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "delete_assignments_for_scope" => {
            let workflow = routing_json_str(body, "workflow_key")
                .ok_or(bad("workflow_key is required".to_owned()))?;
            let scope = routing_json_str(body, "scope").unwrap_or_else(|| "global".to_owned());
            routing::delete_assignments_for_scope(pool, &workflow, &scope)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "create_trigger" => {
            let input = trigger_input_from_json(body).map_err(bad)?;
            let id = routing::insert_trigger(pool, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true, "id": id })
        }
        "update_trigger" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let input = trigger_input_from_json(body).map_err(bad)?;
            routing::update_trigger(pool, id, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "delete_trigger" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            routing::delete_trigger(pool, id)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "update_workflow" => {
            let key = routing_json_str(body, "key").ok_or(bad("key is required".to_owned()))?;
            routing::update_workflow(
                pool,
                &key,
                routing_json_bool(body, "full_routing", true),
                routing_json_i32(body, "retry_max_hops").unwrap_or(3),
                routing_json_i32(body, "retry_wall_ms").unwrap_or(60_000),
                routing_json_bool(body, "enabled", true),
            )
            .await
            .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "set_workflow_enabled" => {
            let key = routing_json_str(body, "key").ok_or(bad("key is required".to_owned()))?;
            let enabled = routing_json_required_bool(body, "enabled").map_err(bad)?;
            routing::set_workflow_enabled(pool, &key, enabled)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "create_pool" => {
            let input = pool_input_from_json(body).map_err(bad)?;
            if input.max_concurrency.is_some_and(|max| max < 1) {
                return Err(bad("max_concurrency must be positive or null".to_owned()));
            }
            let id = routing::insert_pool(pool, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true, "id": id })
        }
        "update_pool" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let input = pool_input_from_json(body).map_err(bad)?;
            if input.max_concurrency.is_some_and(|max| max < 1) {
                return Err(bad("max_concurrency must be positive or null".to_owned()));
            }
            routing::update_pool(pool, id, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "delete_pool" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            // FK is ON DELETE SET NULL: attached models degrade to unpooled.
            routing::delete_pool(pool, id).await.map_err(storage_err)?;
            json!({ "ok": true })
        }
        "set_model_pool" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let pool_id = body.get("pool_id").and_then(serde_json::Value::as_i64);
            routing::set_model_pool(pool, id, pool_id)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "set_primary_weights" => {
            let weights = body
                .get("weights")
                .and_then(serde_json::Value::as_array)
                .ok_or(bad("weights array is required".to_owned()))?
                .iter()
                .map(|entry| {
                    let id = entry
                        .get("id")
                        .and_then(serde_json::Value::as_i64)
                        .ok_or_else(|| "each weight entry needs an id".to_owned())?;
                    let weight = routing_json_i32(entry, "weight");
                    if weight.is_some_and(|weight| !(0..=100).contains(&weight)) {
                        return Err(format!("weight for assignment {id} must be 0..=100"));
                    }
                    Ok((id, weight))
                })
                .collect::<Result<Vec<_>, String>>()
                .map_err(bad)?;
            routing::set_assignment_weights(pool, &weights)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true, "updated": weights.len() })
        }
        "set_fallback_order" => {
            let ordered_ids = body
                .get("ordered_ids")
                .and_then(serde_json::Value::as_array)
                .ok_or(bad("ordered_ids array is required".to_owned()))?
                .iter()
                .map(|entry| {
                    entry
                        .as_i64()
                        .ok_or_else(|| "ordered_ids must be assignment ids".to_owned())
                })
                .collect::<Result<Vec<_>, String>>()
                .map_err(bad)?;
            routing::set_assignment_fallback_orders(pool, &ordered_ids)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true, "updated": ordered_ids.len() })
        }
        "create_trigger_with_assignment" => {
            let mut assignment = assignment_input_from_json(body).map_err(bad)?;
            assignment.role = "overflow".to_owned();
            if assignment.weight.is_none() {
                assignment.weight = Some(100);
            }
            let mut trigger = trigger_input_from_json_without_engage(body).map_err(bad)?;
            trigger.workflow_key = assignment.workflow_key.clone();
            let (assignment_id, trigger_id) =
                routing::insert_trigger_with_assignment(pool, &assignment, &trigger)
                    .await
                    .map_err(storage_err)?;
            json!({ "ok": true, "assignment_id": assignment_id, "trigger_id": trigger_id })
        }
        "fetch_provider_models" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            return admin_fetch_provider_models(pool, id, body).await;
        }
        "import_models" => {
            let provider_id = body
                .get("provider_id")
                .and_then(serde_json::Value::as_i64)
                .ok_or(bad("provider_id is required".to_owned()))?;
            let names: Vec<String> = body
                .get("names")
                .and_then(serde_json::Value::as_array)
                .ok_or(bad("names array is required".to_owned()))?
                .iter()
                .filter_map(|name| name.as_str())
                .map(str::to_owned)
                .collect();
            if names.is_empty() {
                return Err(bad("names must contain at least one model name".to_owned()));
            }
            let defaults = body.get("defaults").cloned().unwrap_or_else(|| json!({}));
            let capabilities = routing_capabilities(&defaults);
            let capabilities = if capabilities.is_empty() {
                vec!["chat".to_owned()]
            } else {
                capabilities
            };
            let pool_id = defaults.get("pool_id").and_then(serde_json::Value::as_i64);
            let existing: std::collections::HashSet<String> = routing::list_models(pool)
                .await
                .map_err(storage_err)?
                .into_iter()
                .filter(|model| model.provider_id == provider_id)
                .map(|model| model.model_name)
                .collect();
            let mut created = Vec::new();
            for name in names {
                if existing.contains(&name) {
                    continue;
                }
                let id = routing::insert_model(
                    pool,
                    &openplotva_storage::llm_routing::ModelInput {
                        provider_id,
                        model_name: name,
                        display_name: None,
                        base_url: None,
                        capabilities: capabilities.clone(),
                        embedding_dim: None,
                        pool_id,
                        enabled: true,
                        config: json!({}),
                    },
                )
                .await
                .map_err(storage_err)?;
                created.push(id);
            }
            json!({ "ok": true, "created_ids": created })
        }
        "refresh_openrouter_free_pool" => {
            if let Some(reporter) = routes.routing_event_reporter.as_ref() {
                reporter.record(runtime_routing::RoutingEvent {
                    severity: "info".to_owned(),
                    event_type: "openrouter_free_refresh_started".to_owned(),
                    workflow_key: "routing".to_owned(),
                    provider_id: None,
                    model_id: None,
                    queue_name: None,
                    job_id: None,
                    chat_id: None,
                    user_id: None,
                    thread_id: None,
                    message_id: None,
                    dedupe_key: "openrouter_free_refresh_started:manual".to_owned(),
                    summary: "OpenRouter Free refresh started".to_owned(),
                    detail: json!({ "source": "admin" }),
                });
            }
            let http = reqwest::Client::new();
            match openrouter_free_pool::refresh_openrouter_free_pool(pool, &http).await {
                Ok(report) => {
                    if let Some(reporter) = routes.routing_event_reporter.as_ref() {
                        reporter.record(runtime_routing::RoutingEvent {
                            severity: "info".to_owned(),
                            event_type: "openrouter_free_refresh_succeeded".to_owned(),
                            workflow_key: "routing".to_owned(),
                            provider_id: None,
                            model_id: None,
                            queue_name: None,
                            job_id: None,
                            chat_id: None,
                            user_id: None,
                            thread_id: None,
                            message_id: None,
                            dedupe_key: "openrouter_free_refresh_succeeded:manual".to_owned(),
                            summary: "OpenRouter Free refresh succeeded".to_owned(),
                            detail: json!({
                                "active_models": report.active_models,
                                "disabled_models": report.disabled_models,
                                "assignments": report.assignments,
                                "ranking_version": report.ranking_version,
                                "source_updated_at": report.source_updated_at,
                                "key_status_error": report.key_status_error.clone(),
                            }),
                        });
                        if let Some(error) = report.key_status_error.as_deref() {
                            runtime_routing::record_openrouter_free_key_status_failed(
                                reporter, "manual", error,
                            );
                        }
                    }
                    json!({
                        "ok": true,
                        "refreshed": report.refreshed,
                        "active_models": report.active_models,
                        "disabled_models": report.disabled_models,
                        "assignments": report.assignments,
                        "ranking_version": report.ranking_version,
                        "source_updated_at": report.source_updated_at,
                        "key_status_error": report.key_status_error,
                        "message": report.message,
                    })
                }
                Err(error) => {
                    if let Some(reporter) = routes.routing_event_reporter.as_ref() {
                        reporter.record(runtime_routing::RoutingEvent {
                            severity: "warn".to_owned(),
                            event_type: "openrouter_free_refresh_failed".to_owned(),
                            workflow_key: "routing".to_owned(),
                            provider_id: None,
                            model_id: None,
                            queue_name: None,
                            job_id: None,
                            chat_id: None,
                            user_id: None,
                            thread_id: None,
                            message_id: None,
                            dedupe_key: "openrouter_free_refresh_failed:manual".to_owned(),
                            summary: "OpenRouter Free refresh failed".to_owned(),
                            detail: json!({ "error": error.to_string(), "source": "admin" }),
                        });
                    }
                    return Err((StatusCode::BAD_GATEWAY, error.to_string()));
                }
            }
        }
        "reload" => json!({ "ok": true }),
        other => return Err(bad(format!("unknown action {other}"))),
    };

    // Every mutating action bumps the revision and atomically reloads the router so
    // the change takes effect without a restart.
    if action != "reload" {
        let revision = admin_app_setting(routes.postgres.as_ref(), "llm.routing.revision")
            .await
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        let _ = admin_upsert_app_setting(
            routes.postgres.as_ref(),
            "llm.routing.revision",
            &(revision + 1).to_string(),
        )
        .await;
    }
    let reload_result = if let Some(runtime) = routes.router_runtime.as_ref() {
        model_routing::reload_router_runtime(runtime, pool).await
    } else if let Some(handle) = routes.router_handle.as_ref() {
        let reloaded = model_routing::reload_router(handle, pool).await;
        if reloaded.is_ok()
            && let Some(pools) = routes.router_pools.as_ref()
        {
            pools.apply(&handle.snapshot().pool_specs());
        }
        reloaded
    } else {
        Ok(())
    };
    if let Err(error) = reload_result {
        if let Some(reporter) = routes.routing_event_reporter.as_ref() {
            reporter.record(runtime_routing::router_reload_failed_event(
                "admin_routing_reload",
                &error.to_string(),
            ));
        }
        tracing::warn!(%error, "failed to reload router after admin routing change");
    }
    Ok(result)
}

pub(crate) async fn admin_routing(
    method: Method,
    headers: HeaderMap,
    State(routes): State<RoutingHttpState>,
    body: Bytes,
) -> Response {
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(pool) = routes.postgres.clone() else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
    };

    if method == Method::GET {
        return match admin_routing_snapshot_json(&pool).await {
            Ok(value) => admin_json_response(StatusCode::OK, value),
            Err(error) => admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, &error),
        };
    }
    if method != Method::POST && method != Method::PUT {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&body) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "invalid json");
    };
    match admin_routing_apply_action(&pool, &routes, &parsed).await {
        Ok(value) => admin_json_response(StatusCode::OK, value),
        Err((status, error)) => admin_error_response(status, &error),
    }
}

/// Cheap, poll-safe live view for the routing cockpit: pool occupancy,
/// breaker states, trigger engagement, capacity cooldowns, worker scale, and
/// the in-process event ring. No database round-trip.
pub(crate) async fn admin_routing_status(
    method: Method,
    headers: HeaderMap,
    State(routes): State<RoutingHttpState>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let now = std::time::Instant::now();
    let table = routes
        .router_handle
        .as_ref()
        .map(|handle| handle.snapshot());
    let pools: Vec<serde_json::Value> = routes
        .router_pools
        .as_ref()
        .map(|pools| pools.occupancy())
        .unwrap_or_default()
        .into_iter()
        .map(|occupancy| {
            json!({
                "id": occupancy.id,
                "name": table.as_ref().and_then(|table| table.pool_name(occupancy.id)),
                "max_concurrency": occupancy.max_concurrency,
                "in_flight": occupancy.in_flight,
                "queued": {
                    "interactive": occupancy.queued[0],
                    "user_tool": occupancy.queued[1],
                    "background": occupancy.queued[2],
                },
                "mean_hold_ms": occupancy.mean_hold_ms,
                "wait_p50_ms": occupancy.wait_p50_ms,
                "wait_p95_ms": occupancy.wait_p95_ms,
            })
        })
        .collect();
    let breakers: Vec<serde_json::Value> = routes
        .router_breakers
        .as_ref()
        .map(|breakers| breakers.states_at(now))
        .unwrap_or_default()
        .into_iter()
        .map(|state| {
            json!({
                "provider_id": state.provider,
                "model_id": state.model,
                "consecutive_failures": state.consecutive_failures,
                "open": state.open,
                "cooldown_remaining_ms": state
                    .cooldown_remaining
                    .map(|left| u64::try_from(left.as_millis()).unwrap_or(u64::MAX)),
            })
        })
        .collect();
    let engaged_triggers = routes
        .router_triggers
        .as_ref()
        .map(|triggers| triggers.engaged_ids())
        .unwrap_or_default();
    let capacity_cooldowns: Vec<serde_json::Value> = routes
        .router_triggers
        .as_ref()
        .map(|triggers| triggers.capacity_snapshot_at(now))
        .unwrap_or_default()
        .into_iter()
        .map(|(provider, model, left)| {
            json!({
                "provider_id": provider,
                "model_id": model,
                "remaining_ms": u64::try_from(left.as_millis()).unwrap_or(u64::MAX),
            })
        })
        .collect();
    let workers = routes
        .dialog_worker_gauge
        .as_ref()
        .map(|gauge| {
            json!({
                "dialog": { "desired": gauge.desired(), "running": gauge.running() }
            })
        })
        .unwrap_or_else(|| json!({}));
    let recent_events: Vec<serde_json::Value> = routes
        .routing_event_buffer
        .as_ref()
        .map(|buffer| buffer.routing_events(20))
        .unwrap_or_default()
        .into_iter()
        .map(|event| routing_event_data_json(&event))
        .collect();
    let revision = admin_app_setting(routes.postgres.as_ref(), "llm.routing.revision")
        .await
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    let mut openrouter_free = admin_app_setting(
        routes.postgres.as_ref(),
        "llm.routing.openrouter_free.status",
    )
    .await
    .and_then(|value| serde_json::from_str::<serde_json::Value>(&value).ok());
    if let Some(gate) = routes.openrouter_free_gate.as_ref() {
        let quota = gate.status_snapshot().await;
        let value = openrouter_free.get_or_insert_with(|| json!({}));
        if let Some(object) = value.as_object_mut() {
            object.insert("quota".to_owned(), quota);
        }
    }
    admin_json_no_cache_response(
        StatusCode::OK,
        json!({
            "revision": revision,
            "pools": pools,
            "breakers": breakers,
            "engaged_triggers": engaged_triggers,
            "capacity_cooldowns": capacity_cooldowns,
            "workers": workers,
            "recent_events": recent_events,
            "openrouter_free": openrouter_free,
        }),
    )
}

/// Keyset-paginated journal over `llm_routing_events` with optional
/// `workflow` / `severity` filters and a `before_id` cursor.
pub(crate) async fn admin_routing_events(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<RoutingHttpState>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(pool) = routes.postgres.clone() else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
    };
    let values = admin_auth_query_values(raw_query.as_deref());
    let limit = values
        .get("limit")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(50);
    let before_id = values
        .get("before_id")
        .and_then(|value| value.parse::<i64>().ok());
    let workflow = values
        .get("workflow")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let severity = values
        .get("severity")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    match openplotva_storage::llm_routing::list_routing_events_page(
        &pool,
        limit,
        before_id,
        workflow.as_deref(),
        severity.as_deref(),
    )
    .await
    {
        Ok(events) => {
            let next_before_id = events.last().map(|event| event.id);
            let events: Vec<serde_json::Value> = events
                .iter()
                .map(|event| {
                    json!({
                        "id": event.id,
                        "created_at": event
                            .created_at
                            .format(&time::format_description::well_known::Rfc3339)
                            .unwrap_or_default(),
                        "severity": event.severity,
                        "event_type": event.event_type,
                        "workflow_key": event.workflow_key,
                        "provider_id": event.provider_id,
                        "model_id": event.model_id,
                        "queue_name": event.queue_name,
                        "job_id": event.job_id,
                        "chat_id": event.chat_id,
                        "summary": event.summary,
                        "detail": event.detail,
                    })
                })
                .collect();
            admin_json_no_cache_response(
                StatusCode::OK,
                json!({ "events": events, "next_before_id": next_before_id }),
            )
        }
        Err(error) => {
            tracing::warn!(%error, "failed to list routing events");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) fn routing_event_data_json(
    event: &runtime_routing::RoutingEventData,
) -> serde_json::Value {
    json!({
        "id": event.id,
        "at_millis": event.at_millis,
        "severity": event.severity,
        "event_type": event.event_type,
        "workflow_key": event.workflow_key,
        "provider_id": event.provider_id,
        "model_id": event.model_id,
        "queue_name": event.queue_name,
        "summary": event.summary,
        "detail": event.detail,
    })
}
