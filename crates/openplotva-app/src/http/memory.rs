use super::HttpRouterState;
use super::auth::{admin_auth_failure_response, current_admin_user_id, require_admin_request};
use super::common::{
    admin_error_response, admin_json_response, admin_request_is_json, parse_go_bool,
    parse_memory_limit, query_values as admin_auth_query_values,
};
use crate::{embedder, memory_runtime, routed_attempts};
use axum::{
    body::Bytes,
    extract::{RawQuery, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
};
use openplotva_config::AppConfig;
use openplotva_storage::PostgresMemoryStore;
use serde::Deserialize;
use std::{sync::Arc, time::Duration};
use time::OffsetDateTime;

#[derive(Clone)]
pub(crate) struct MemoryHttpState {
    admin_ids: Arc<[i64]>,
    bot_token: Arc<str>,
    memory_admin_enabled: bool,
    memory_consolidation_model: Arc<str>,
    memory_enqueue_policy: openplotva_memory::MemoryRunEnqueuePolicy,
    memory_max_input_tokens: i32,
    memory_max_messages_per_run: i32,
    memory_override_runtime: Option<AdminMemoryOverrideRuntime>,
    memory_restart_trigger: Option<Arc<tokio::sync::Notify>>,
    memory_retention: Duration,
    memory_store: Option<PostgresMemoryStore>,
    memory_token_estimator_source: Arc<str>,
}

impl axum::extract::FromRef<Arc<HttpRouterState>> for MemoryHttpState {
    fn from_ref(routes: &Arc<HttpRouterState>) -> Self {
        Self {
            admin_ids: routes.admin_ids.clone(),
            bot_token: routes.bot_token.clone(),
            memory_admin_enabled: routes.memory_admin_enabled,
            memory_consolidation_model: routes.memory_consolidation_model.clone(),
            memory_enqueue_policy: routes.memory_enqueue_policy,
            memory_max_input_tokens: routes.memory_max_input_tokens,
            memory_max_messages_per_run: routes.memory_max_messages_per_run,
            memory_override_runtime: routes.memory_override_runtime.clone(),
            memory_restart_trigger: routes.memory_restart_trigger.clone(),
            memory_retention: routes.memory_retention,
            memory_store: routes.memory_store.clone(),
            memory_token_estimator_source: routes.memory_token_estimator_source.clone(),
        }
    }
}

#[cfg(test)]
pub(crate) fn test_state(routes: &HttpRouterState) -> MemoryHttpState {
    use axum::extract::FromRef as _;
    MemoryHttpState::from_ref(&Arc::new(routes.clone()))
}

pub(crate) async fn admin_memory_cards(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<MemoryHttpState>,
) -> Response {
    let headers = &headers;
    let raw_query = raw_query.as_deref();
    let routes = &routes;
    match method {
        Method::GET => admin_memory_cards_list_response(routes, headers, raw_query).await,
        Method::DELETE => admin_memory_card_delete_response(routes, headers, raw_query).await,
        _ => admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
    }
}

pub(crate) async fn admin_memory_card(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<MemoryHttpState>,
    body: Bytes,
) -> Response {
    let headers = &headers;
    let raw_query = raw_query.as_deref();
    let routes = &routes;
    let body = body.as_ref();
    match method {
        Method::GET => admin_memory_card_detail_response(routes, headers, raw_query).await,
        Method::PATCH => admin_memory_card_update_response(routes, headers, body).await,
        _ => admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
    }
}

pub(crate) async fn admin_memory_overview(
    method: Method,
    headers: HeaderMap,
    State(routes): State<MemoryHttpState>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    admin_memory_overview_response(&routes, &headers).await
}

pub(crate) async fn admin_memory_runs(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<MemoryHttpState>,
    body: Bytes,
) -> Response {
    let headers = &headers;
    let raw_query = raw_query.as_deref();
    let routes = &routes;
    let body = body.as_ref();
    match method {
        Method::GET => admin_memory_runs_list_response(routes, headers, raw_query).await,
        Method::POST => admin_memory_run_retry_response(routes, headers, raw_query, body).await,
        _ => admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
    }
}

pub(crate) async fn admin_memory_restart(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<MemoryHttpState>,
    body: Bytes,
) -> Response {
    let headers = &headers;
    let raw_query = raw_query.as_deref();
    let routes = &routes;
    let body = body.as_ref();
    if method != Method::POST {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let id = match admin_memory_optional_restart_id(raw_query) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    let run_now = admin_auth_query_values(raw_query)
        .get("run_now")
        .and_then(|value| parse_go_bool(value))
        .unwrap_or(false);
    admin_memory_restart_execute_response(routes, headers, raw_query, body, id, run_now).await
}

#[cfg(test)]
pub(crate) async fn admin_memory_cards_response(
    routes: &MemoryHttpState,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    admin_memory_cards(
        method,
        headers.clone(),
        RawQuery(raw_query.map(str::to_owned)),
        State(routes.clone()),
    )
    .await
}

pub(crate) async fn admin_memory_card_update_response(
    routes: &MemoryHttpState,
    headers: &HeaderMap,
    body: &[u8],
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let req: MemoryCardUpdate = match serde_json::from_slice(body) {
        Ok(req) => req,
        Err(_) => return admin_error_response(StatusCode::BAD_REQUEST, "invalid body"),
    };
    if req.id == 0 {
        return admin_error_response(StatusCode::BAD_REQUEST, "id required");
    }
    let Some(store) = routes
        .memory_store
        .as_ref()
        .filter(|_| routes.memory_admin_enabled)
    else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "memory disabled");
    };
    let confidence = req.confidence.map(|v| v.clamp(0.0, 1.0));
    let salience = req.salience.map(|v| v.clamp(0.0, 1.0));
    if (confidence.is_some() || salience.is_some() || req.portable.is_some())
        && let Err(error) = store
            .update_card_fields(req.id, confidence, salience, req.portable)
            .await
    {
        tracing::warn!(%error, id = req.id, "failed to update memory card fields");
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    }
    if let Some(status) = req.status.as_deref() {
        let result = match status {
            "deleted" => {
                store
                    .soft_delete_card(req.id, current_admin_user_id(headers, &routes.bot_token))
                    .await
            }
            "active" => store.restore_card(req.id).await,
            _ => Ok(()),
        };
        if let Err(error) = result {
            tracing::warn!(%error, id = req.id, "failed to change memory card status");
            return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
        }
    }
    admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true }))
}

pub(crate) async fn admin_memory_card_detail_response(
    routes: &MemoryHttpState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let id = match admin_memory_required_id(raw_query, "id required") {
        Ok(id) => id,
        Err(response) => return *response,
    };
    let Some(store) = routes
        .memory_store
        .as_ref()
        .filter(|_| routes.memory_admin_enabled)
    else {
        return admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "card": serde_json::Value::Null, "links": [] }),
        );
    };
    let card = match store.get_card(id).await {
        Ok(Some(card)) => card,
        Ok(None) => return admin_error_response(StatusCode::NOT_FOUND, "not found"),
        Err(error) => {
            tracing::warn!(%error, id, "failed to load admin memory card");
            return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
        }
    };
    let links = store.list_card_links(id).await.unwrap_or_default();
    admin_json_response(
        StatusCode::OK,
        serde_json::json!({ "card": card, "links": links }),
    )
}

pub(crate) async fn admin_memory_overview_response(
    routes: &MemoryHttpState,
    headers: &HeaderMap,
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(store) = routes
        .memory_store
        .as_ref()
        .filter(|_| routes.memory_admin_enabled)
    else {
        return admin_json_response(StatusCode::OK, serde_json::json!({}));
    };
    match store.memory_overview().await {
        Ok(overview) => admin_json_response(
            StatusCode::OK,
            serde_json::to_value(&overview).unwrap_or(serde_json::Value::Null),
        ),
        Err(error) => {
            tracing::warn!(%error, "failed to load admin memory overview");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_memory_cards_list_response(
    routes: &MemoryHttpState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(store) = routes
        .memory_store
        .as_ref()
        .filter(|_| routes.memory_admin_enabled)
    else {
        return admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "count": 0, "cards": serde_json::Value::Null }),
        );
    };
    let filter = admin_memory_card_filter(raw_query);
    match store.list_cards(&filter).await {
        Ok(cards) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({
                "count": cards.len(),
                "cards": cards,
            }),
        ),
        Err(error) => {
            tracing::warn!(%error, "failed to list admin memory cards");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_memory_card_delete_response(
    routes: &MemoryHttpState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let id = match admin_memory_required_id(raw_query, "id required") {
        Ok(id) => id,
        Err(response) => return *response,
    };
    if let Some(store) = routes
        .memory_store
        .as_ref()
        .filter(|_| routes.memory_admin_enabled)
        && let Err(error) = store
            .soft_delete_card(id, current_admin_user_id(headers, &routes.bot_token))
            .await
    {
        tracing::warn!(%error, id, "failed to delete admin memory card");
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    }
    admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true }))
}

#[cfg(test)]
pub(crate) async fn admin_memory_runs_response(
    routes: &MemoryHttpState,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &[u8],
) -> Response {
    admin_memory_runs(
        method,
        headers.clone(),
        RawQuery(raw_query.map(str::to_owned)),
        State(routes.clone()),
        Bytes::copy_from_slice(body),
    )
    .await
}

pub(crate) async fn admin_memory_runs_list_response(
    routes: &MemoryHttpState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(store) = routes
        .memory_store
        .as_ref()
        .filter(|_| routes.memory_admin_enabled)
    else {
        return admin_json_response(
            StatusCode::OK,
            serde_json::json!({
                "count": 0,
                "runs": serde_json::Value::Null,
                "policy": admin_memory_policy_json(routes),
                "enqueue_rollups": serde_json::Value::Null,
            }),
        );
    };
    let limit = parse_memory_limit(
        admin_auth_query_values(raw_query)
            .get("limit")
            .map(String::as_str),
    );
    let runs = match store.list_runs(limit as i32).await {
        Ok(runs) => runs,
        Err(error) => {
            tracing::warn!(%error, "failed to list admin memory runs");
            return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
        }
    };
    let enqueue_rollups = match store.list_enqueue_rollups(24).await {
        Ok(rollups) => rollups,
        Err(error) => {
            tracing::warn!(%error, "failed to list admin memory enqueue rollups");
            return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
        }
    };
    admin_json_response(
        StatusCode::OK,
        serde_json::json!({
            "count": runs.len(),
            "runs": runs,
            "policy": admin_memory_policy_json(routes),
            "enqueue_rollups": enqueue_rollups,
        }),
    )
}

pub(crate) fn admin_memory_policy_json(routes: &MemoryHttpState) -> serde_json::Value {
    serde_json::json!({
        "min_messages_per_run": routes.memory_enqueue_policy.min_messages_per_run,
        "max_queued_runs": null,
        "max_messages_per_run": routes.memory_max_messages_per_run,
        "max_input_tokens": routes.memory_max_input_tokens,
        "token_estimator": routes.memory_token_estimator_source.as_ref(),
        "consolidation_model": routes.memory_consolidation_model.as_ref(),
    })
}

pub(crate) async fn admin_memory_run_retry_response(
    routes: &MemoryHttpState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &[u8],
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let id = match admin_memory_required_id(raw_query, "id required") {
        Ok(id) => id,
        Err(response) => return *response,
    };
    admin_memory_restart_execute_response(routes, headers, raw_query, body, id, false).await
}

#[cfg(test)]
pub(crate) async fn admin_memory_restart_response(
    routes: &MemoryHttpState,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &[u8],
) -> Response {
    admin_memory_restart(
        method,
        headers.clone(),
        RawQuery(raw_query.map(str::to_owned)),
        State(routes.clone()),
        Bytes::copy_from_slice(body),
    )
    .await
}

pub(crate) async fn admin_memory_restart_execute_response(
    routes: &MemoryHttpState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &[u8],
    id: i64,
    run_now: bool,
) -> Response {
    let Some(store) = routes
        .memory_store
        .as_ref()
        .filter(|_| routes.memory_admin_enabled)
    else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "memory disabled");
    };
    let (override_, explicit_override) =
        match admin_memory_restart_override(headers, raw_query, body) {
            Ok(value) => value,
            Err(response) => return *response,
        };
    if explicit_override {
        return admin_memory_restart_override_execute_response(routes, override_, id, run_now)
            .await;
    }
    let mut retried_failed_runs = 0_i64;
    let mut queued_runs = 0_i64;
    let result = if run_now {
        store
            .ensure_hourly_runs(
                OffsetDateTime::now_utc(),
                routes.memory_retention,
                routes.memory_retention,
                routes.memory_enqueue_policy,
            )
            .await
            .map(|queued| queued_runs = admin_u64_to_i64(queued))
    } else if id > 0 {
        store.retry_run(id).await.map(|()| retried_failed_runs = 1)
    } else {
        store
            .retry_failed_runs()
            .await
            .map(|retried| retried_failed_runs = admin_u64_to_i64(retried))
    };
    if let Err(error) = result {
        tracing::warn!(%error, id, run_now, "failed to restart admin memory runs");
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    }
    let started = routes
        .memory_restart_trigger
        .as_ref()
        .map(|trigger| {
            trigger.notify_one();
            true
        })
        .unwrap_or(false);
    let provider = "aifarm".to_owned();
    let model = admin_memory_restart_model(&routes.memory_consolidation_model);
    admin_memory_restart_ok_response(
        id,
        retried_failed_runs,
        queued_runs,
        started,
        explicit_override,
        provider,
        model,
    )
}

pub(crate) async fn admin_memory_restart_override_execute_response(
    routes: &MemoryHttpState,
    override_: AdminMemoryOverride,
    id: i64,
    run_now: bool,
) -> Response {
    let Some(runtime) = routes.memory_override_runtime.clone() else {
        tracing::warn!(
            provider = override_.provider,
            model = override_.model,
            "admin memory override drain is unavailable"
        );
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    let (provider, model) = match admin_memory_resolve_override(
        &override_,
        &runtime.config,
        &routes.memory_consolidation_model,
    ) {
        Ok(value) => value,
        Err(unsupported_provider) => {
            tracing::warn!(
                provider = unsupported_provider,
                model = override_.model,
                "admin memory override provider is unsupported"
            );
            return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
        }
    };
    let extractor =
        memory_runtime::RoutedMemoryExtractor::new(runtime.attempt_walker.clone(), &runtime.config);
    let embedder = match memory_runtime::memory_write_embedder_from_config(
        &runtime.config.memory,
        &runtime.config.llm.discovery.base_url,
    ) {
        Ok(embedder) => embedder.map(|client| {
            embedder::RoutedDiscoveryEmbedder::new(
                runtime.attempt_walker.clone(),
                client.config().clone(),
            )
        }),
        Err(error) => {
            tracing::warn!(%error, provider, model, "failed to build admin memory override embedder");
            return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
        }
    };
    let Ok(guard) = runtime.lock.clone().try_lock_owned() else {
        return admin_memory_restart_ok_response(id, 0, 0, false, true, provider, model);
    };
    let mut retried_failed_runs = 0_i64;
    let mut queued_runs = 0_i64;
    let result = if run_now {
        runtime
            .store
            .ensure_hourly_runs(
                OffsetDateTime::now_utc(),
                routes.memory_retention,
                routes.memory_retention,
                routes.memory_enqueue_policy,
            )
            .await
            .map(|queued| queued_runs = admin_u64_to_i64(queued))
    } else if id > 0 {
        runtime
            .store
            .retry_run(id)
            .await
            .map(|()| retried_failed_runs = 1)
    } else {
        runtime
            .store
            .retry_failed_runs()
            .await
            .map(|retried| retried_failed_runs = admin_u64_to_i64(retried))
    };
    if let Err(error) = result {
        tracing::warn!(
            %error,
            id,
            run_now,
            provider,
            model,
            "failed to restart admin memory runs with override"
        );
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    }
    let mut cfg =
        memory_runtime::memory_service_worker_config_from_memory_config(&runtime.config.memory)
            .process;
    cfg.episode_model = model.clone();
    tokio::spawn(admin_memory_override_drain(
        runtime,
        extractor,
        embedder,
        cfg,
        model.clone(),
        guard,
    ));
    admin_memory_restart_ok_response(
        id,
        retried_failed_runs,
        queued_runs,
        true,
        true,
        provider,
        model,
    )
}

pub(crate) async fn admin_memory_override_drain(
    runtime: AdminMemoryOverrideRuntime,
    extractor: memory_runtime::RoutedMemoryExtractor,
    embedder: Option<embedder::RoutedDiscoveryEmbedder>,
    cfg: memory_runtime::MemoryRunProcessConfig,
    model: String,
    _guard: tokio::sync::OwnedMutexGuard<()>,
) {
    let started_at = std::time::Instant::now();
    let mut completed = 0_i64;
    let mut failed = 0_i64;
    let mut store_errors = 0_i64;
    tracing::info!(model, workers = 1, "admin memory override drain started");
    loop {
        match memory_runtime::process_next_memory_run(
            &extractor,
            &runtime.store,
            embedder.as_ref(),
            cfg.clone(),
        )
        .await
        {
            Ok(report) if report.processed => {
                completed += 1;
                tracing::info!(?report, "admin memory override run processed");
            }
            Ok(_) => break,
            Err(memory_runtime::MemoryRunProcessError::Process { run_id, source }) => {
                failed += 1;
                tracing::warn!(%source, run_id, "admin memory override run failed");
            }
            Err(memory_runtime::MemoryRunProcessError::Store { source }) => {
                store_errors += 1;
                tracing::warn!(%source, "admin memory override store operation failed");
                break;
            }
        }
    }
    tracing::info!(
        model,
        completed,
        failed,
        store_errors,
        duration = ?started_at.elapsed(),
        "admin memory override drain finished"
    );
}

pub(crate) fn admin_memory_resolve_override(
    override_: &AdminMemoryOverride,
    config: &AppConfig,
    default_model: &str,
) -> Result<(String, String), String> {
    let provider = normalize_memory_restart_provider(&override_.provider);
    let provider = if provider.is_empty() {
        "aifarm".to_owned()
    } else {
        provider
    };
    match provider.as_str() {
        "aifarm" => {
            let model = if override_.model.trim().is_empty() {
                admin_memory_restart_model(default_model)
            } else {
                override_.model.trim().to_owned()
            };
            Ok((provider, model))
        }
        "genkit" | "gemini" => {
            let model =
                memory_runtime::genkit_memory_extractor_model(config, Some(&override_.model));
            Ok(("genkit".to_owned(), model))
        }
        _ => Err(provider),
    }
}

pub(crate) fn admin_memory_restart_ok_response(
    id: i64,
    retried_failed_runs: i64,
    queued_runs: i64,
    started: bool,
    override_: bool,
    provider: String,
    model: String,
) -> Response {
    let mut value = serde_json::Map::new();
    value.insert("ok".to_owned(), serde_json::json!(true));
    if id != 0 {
        value.insert("run_id".to_owned(), serde_json::json!(id));
    }
    value.insert(
        "retried_failed_runs".to_owned(),
        serde_json::json!(retried_failed_runs),
    );
    value.insert("queued_runs".to_owned(), serde_json::json!(queued_runs));
    value.insert("started".to_owned(), serde_json::json!(started));
    value.insert("override".to_owned(), serde_json::json!(override_));
    if !provider.is_empty() {
        value.insert("provider".to_owned(), serde_json::json!(provider));
    }
    if !model.is_empty() {
        value.insert("model".to_owned(), serde_json::json!(model));
    }
    admin_json_response(StatusCode::OK, serde_json::Value::Object(value))
}

pub(crate) fn admin_memory_card_filter(raw_query: Option<&str>) -> openplotva_memory::CardFilter {
    let values = admin_auth_query_values(raw_query);
    openplotva_memory::CardFilter {
        chat_id: values
            .get("chat_id")
            .and_then(|value| value.trim().parse::<i64>().ok())
            .unwrap_or_default(),
        thread_id: values
            .get("thread_id")
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .and_then(|value| value.parse::<i32>().ok()),
        user_id: values
            .get("user_id")
            .and_then(|value| value.trim().parse::<i64>().ok())
            .unwrap_or_default(),
        status: values
            .get("status")
            .map(|value| value.trim().to_owned())
            .unwrap_or_default(),
        card_type: values
            .get("card_type")
            .map(|value| value.trim().to_owned())
            .unwrap_or_default(),
        visibility: values
            .get("visibility")
            .map(|value| value.trim().to_owned())
            .unwrap_or_default(),
        as_of: values.get("as_of").and_then(|value| {
            time::OffsetDateTime::parse(
                value.trim(),
                &time::format_description::well_known::Rfc3339,
            )
            .ok()
        }),
        limit: parse_memory_limit(values.get("limit").map(String::as_str)) as i32,
    }
}

pub(crate) fn admin_memory_required_id(
    raw_query: Option<&str>,
    missing: &str,
) -> Result<i64, Box<Response>> {
    let values = admin_auth_query_values(raw_query);
    let Some(raw) = values.get("id") else {
        return Err(Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            missing,
        )));
    };
    let id = raw.trim().parse::<i64>().map_err(|_| {
        Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            if missing == "invalid id" {
                "invalid id"
            } else {
                missing
            },
        ))
    })?;
    if id == 0 {
        return Err(Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            missing,
        )));
    }
    Ok(id)
}

pub(crate) fn admin_memory_optional_restart_id(
    raw_query: Option<&str>,
) -> Result<i64, Box<Response>> {
    let values = admin_auth_query_values(raw_query);
    let Some(raw) = values.get("id").map(String::as_str).map(str::trim) else {
        return Ok(0);
    };
    if raw.is_empty() {
        return Ok(0);
    }
    let id = raw
        .parse::<i64>()
        .map_err(|_| Box::new(admin_error_response(StatusCode::BAD_REQUEST, "invalid id")))?;
    if id <= 0 {
        return Err(Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            "invalid id",
        )));
    }
    Ok(id)
}

pub(crate) fn admin_memory_restart_override(
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &[u8],
) -> Result<(AdminMemoryOverride, bool), Box<Response>> {
    let mut req = if admin_request_is_json(headers) {
        if body.is_empty() {
            AdminMemoryRestartRequest::default()
        } else {
            serde_json::from_slice::<AdminMemoryRestartRequest>(body).map_err(|_| {
                Box::new(admin_error_response(
                    StatusCode::BAD_REQUEST,
                    "invalid override",
                ))
            })?
        }
    } else {
        AdminMemoryRestartRequest::default()
    };
    let values = admin_auth_query_values(raw_query);
    if let Some(raw) = values.get("override").map(String::as_str).map(str::trim)
        && !raw.is_empty()
        && let Some(value) = parse_go_bool(raw)
    {
        req.override_ = value;
    }
    if let Some(raw) = values.get("provider").map(String::as_str).map(str::trim)
        && !raw.is_empty()
    {
        req.provider = raw.to_owned();
    }
    if let Some(raw) = values.get("model").map(String::as_str).map(str::trim)
        && !raw.is_empty()
    {
        req.model = raw.to_owned();
    }
    let provider = req.provider.trim().to_lowercase();
    if !matches!(provider.as_str(), "" | "aifarm" | "genkit" | "gemini") {
        return Err(Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            "invalid override",
        )));
    }
    let model = req.model.trim().to_owned();
    let explicit = req.override_ || !provider.is_empty() || !model.is_empty();
    Ok((AdminMemoryOverride { provider, model }, explicit))
}

pub(crate) fn admin_memory_restart_model(model: &str) -> String {
    let model = model.trim();
    if model.is_empty() {
        openplotva_memory::DEFAULT_MEMORY_CONSOLIDATION_MODEL.to_owned()
    } else {
        model.to_owned()
    }
}

pub(crate) fn normalize_memory_restart_provider(provider: &str) -> String {
    let provider = provider.trim();
    if provider.eq_ignore_ascii_case("aifarm") {
        "aifarm".to_owned()
    } else if provider.eq_ignore_ascii_case("genkit") {
        "genkit".to_owned()
    } else if provider.eq_ignore_ascii_case("gemini") {
        "gemini".to_owned()
    } else {
        provider.to_lowercase()
    }
}

pub(crate) fn admin_u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct AdminMemoryOverride {
    pub(crate) provider: String,
    pub(crate) model: String,
}

#[derive(Clone)]
pub(crate) struct AdminMemoryOverrideRuntime {
    pub(crate) config: Arc<AppConfig>,
    pub(crate) store: PostgresMemoryStore,
    pub(crate) lock: Arc<tokio::sync::Mutex<()>>,
    pub(crate) attempt_walker: routed_attempts::RoutedAttemptWalker,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct AdminMemoryRestartRequest {
    #[serde(default, rename = "override")]
    pub(crate) override_: bool,
    #[serde(default)]
    pub(crate) provider: String,
    #[serde(default)]
    pub(crate) model: String,
}
#[derive(serde::Deserialize)]
struct MemoryCardUpdate {
    #[serde(default)]
    id: i64,
    #[serde(default)]
    confidence: Option<f64>,
    #[serde(default)]
    salience: Option<f64>,
    #[serde(default)]
    portable: Option<bool>,
    #[serde(default)]
    status: Option<String>,
}
