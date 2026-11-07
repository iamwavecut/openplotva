use super::HttpRouterState;
use super::auth::{admin_auth_failure_response, require_admin_request};
use super::common::{
    admin_app_setting, admin_error_response, admin_json_no_cache_response, admin_json_response,
    admin_upsert_app_setting, parse_admin_non_negative_i32, parse_admin_optional_bool,
    parse_admin_positive_i32, query_values as admin_auth_query_values,
};
use crate::{
    runtime_analytics_overview, runtime_gpu_arbiter, runtime_llm, runtime_llm_analytics,
    runtime_llm_runs, runtime_safety,
};
use crate::{runtime_cache, runtime_dispatcher, runtime_taskman};
use axum::{
    body::Bytes,
    extract::{RawQuery, State},
    http::{HeaderMap, Method, StatusCode},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use futures_util::{Stream, stream};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::{collections::VecDeque, convert::Infallible, sync::Arc, time::Duration};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::time::Interval;

#[derive(Clone)]
pub(crate) struct ObservabilityHttpState {
    admin_ids: Arc<[i64]>,
    bot_token: Arc<str>,
    cache_inspector: runtime_cache::RuntimeCacheInspectorHandle,
    default_log_level: Arc<str>,
    dispatcher_inspector: runtime_dispatcher::RuntimeDispatcherInspectorHandle,
    gradius_audit_reader: Option<Arc<dyn openplotva_server::RuntimeGradiusAuditReader>>,
    llm_discovery_base_url: Arc<str>,
    llm_discovery_service_name: Arc<str>,
    llm_run_buffer: Option<runtime_llm_runs::RuntimeLlmRunBuffer>,
    llm_trace_buffer: Option<runtime_llm::RuntimeLlmTraceBuffer>,
    log_buffer: Option<Arc<openplotva_observability::RuntimeLogBuffer>>,
    postgres: Option<PgPool>,
    runtime_sql_timeout_ms: i32,
    taskman_inspector: runtime_taskman::RuntimeTaskmanInspectorHandle,
}

impl axum::extract::FromRef<Arc<HttpRouterState>> for ObservabilityHttpState {
    fn from_ref(routes: &Arc<HttpRouterState>) -> Self {
        Self {
            admin_ids: routes.admin_ids.clone(),
            bot_token: routes.bot_token.clone(),
            cache_inspector: routes.cache_inspector.clone(),
            default_log_level: routes.default_log_level.clone(),
            dispatcher_inspector: routes.dispatcher_inspector.clone(),
            gradius_audit_reader: routes.gradius_audit_reader.clone(),
            llm_discovery_base_url: routes.llm_discovery_base_url.clone(),
            llm_discovery_service_name: routes.llm_discovery_service_name.clone(),
            llm_run_buffer: routes.llm_run_buffer.clone(),
            llm_trace_buffer: routes.llm_trace_buffer.clone(),
            log_buffer: routes.log_buffer.clone(),
            postgres: routes.postgres.clone(),
            runtime_sql_timeout_ms: routes.runtime_sql_timeout_ms,
            taskman_inspector: routes.taskman_inspector.clone(),
        }
    }
}

#[cfg(test)]
pub(crate) fn test_state(routes: &HttpRouterState) -> ObservabilityHttpState {
    use axum::extract::FromRef as _;
    ObservabilityHttpState::from_ref(&Arc::new(routes.clone()))
}

pub(crate) async fn admin_state(
    headers: HeaderMap,
    State(routes): State<ObservabilityHttpState>,
) -> Response {
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    admin_state_response(&routes).await
}

pub(crate) async fn admin_loglevel(
    method: Method,
    headers: HeaderMap,
    State(routes): State<ObservabilityHttpState>,
    body: Bytes,
) -> Response {
    let headers = &headers;
    let routes = &routes;
    let body = body.as_ref();
    if method != Method::POST && method != Method::PUT {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Ok(req) = serde_json::from_slice::<AdminLogLevelRequest>(body) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "bad request");
    };
    let level = req.level.trim().to_lowercase();
    match level.as_str() {
        "debug" | "info" | "warn" | "warning" | "error" => {}
        _ => return admin_error_response(StatusCode::BAD_REQUEST, "invalid level"),
    }
    if let Err(error) = openplotva_observability::set_log_level(&level) {
        tracing::warn!(%error, level, "failed to update runtime log level");
    }
    if let Err(error) =
        admin_upsert_app_setting(routes.postgres.as_ref(), "log_level", &level).await
    {
        tracing::warn!(%error, level, "failed to persist admin log level");
    }
    admin_json_response(
        StatusCode::OK,
        serde_json::json!({ "ok": true, "level": level }),
    )
}

pub(crate) async fn admin_logs_stream(
    headers: HeaderMap,
    State(routes): State<ObservabilityHttpState>,
) -> Response {
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(buffer) = routes.log_buffer.clone() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    Sse::new(admin_logs_sse_stream(buffer))
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response()
}

pub(crate) async fn admin_safety_checks(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<ObservabilityHttpState>,
) -> Response {
    let headers = &headers;
    let raw_query = raw_query.as_deref();
    let routes = &routes;
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(pool) = routes.postgres.clone() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    let reader = runtime_safety::PostgresRuntimeSafetyCheckReader::new(pool);
    match openplotva_server::RuntimeSafetyCheckReader::safety_checks(
        &reader,
        admin_safety_checks_filter(raw_query),
    )
    .await
    {
        Ok(connection) => {
            let checks = connection
                .items
                .iter()
                .map(admin_safety_check_json)
                .collect::<Vec<_>>();
            admin_json_no_cache_response(
                StatusCode::OK,
                serde_json::json!({
                    "count": checks.len(),
                    "checks": checks,
                }),
            )
        }
        Err(error) => {
            tracing::warn!(%error, "failed to list admin safety checks");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_gradius_opportunities(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<ObservabilityHttpState>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(reader) = routes.gradius_audit_reader.as_deref() else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "Gradius audit unavailable");
    };
    let values = admin_auth_query_values(raw_query.as_deref());
    let filter = match admin_gradius_opportunities_filter(&values) {
        Ok(filter) => filter,
        Err(error) => return admin_error_response(StatusCode::BAD_REQUEST, &error),
    };
    match reader.gradius_ad_opportunities(filter).await {
        Ok(payload) => admin_json_response(StatusCode::OK, payload),
        Err(error) => {
            if error == "invalid Gradius range" {
                admin_error_response(StatusCode::BAD_REQUEST, &error)
            } else {
                tracing::warn!(%error, "failed to read Gradius opportunities for admin");
                admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
            }
        }
    }
}

pub(crate) async fn admin_gradius_summary(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<ObservabilityHttpState>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(reader) = routes.gradius_audit_reader.as_deref() else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "Gradius audit unavailable");
    };
    let values = admin_auth_query_values(raw_query.as_deref());
    let list_filter = match admin_gradius_opportunities_filter(&values) {
        Ok(filter) => filter,
        Err(error) => return admin_error_response(StatusCode::BAD_REQUEST, &error),
    };
    let filter = openplotva_server::RuntimeGradiusSummaryFilter {
        range: list_filter.range,
        integration_kind: list_filter.integration_kind,
        source: list_filter.source,
        outcome: list_filter.outcome,
        delivery_state: list_filter.delivery_state,
        user_id: list_filter.user_id,
        chat_id: list_filter.chat_id,
        dialog_job_id: list_filter.dialog_job_id,
        model: list_filter.model,
        q: list_filter.q,
    };
    match reader.gradius_ad_summary(filter).await {
        Ok(payload) => admin_json_response(StatusCode::OK, payload),
        Err(error) => {
            let status = if error == "invalid Gradius range" {
                StatusCode::BAD_REQUEST
            } else {
                tracing::warn!(%error, "failed to read Gradius summary for admin");
                StatusCode::INTERNAL_SERVER_ERROR
            };
            admin_error_response(status, &error)
        }
    }
}

pub(crate) fn admin_gradius_opportunities_filter(
    values: &std::collections::BTreeMap<String, String>,
) -> Result<openplotva_server::RuntimeGradiusOpportunitiesFilter, String> {
    let parse_id = |key: &str| -> Result<Option<i64>, String> {
        values
            .get(key)
            .filter(|value| !value.trim().is_empty())
            .map(|value| {
                value
                    .parse::<i64>()
                    .map_err(|_| format!("{key} must be an integer"))
            })
            .transpose()
    };
    let parse_page = |key: &str, default: i32| -> Result<i32, String> {
        values.get(key).map_or(Ok(default), |value| {
            value
                .parse::<i32>()
                .map_err(|_| format!("{key} must be an integer"))
        })
    };
    Ok(openplotva_server::RuntimeGradiusOpportunitiesFilter {
        range: values.get("range").cloned().unwrap_or_default(),
        integration_kind: values.get("integration_kind").cloned().unwrap_or_default(),
        source: values.get("source").cloned().unwrap_or_default(),
        outcome: values.get("outcome").cloned().unwrap_or_default(),
        delivery_state: values.get("delivery_state").cloned().unwrap_or_default(),
        user_id: parse_id("user_id")?,
        chat_id: parse_id("chat_id")?,
        dialog_job_id: parse_id("dialog_job_id")?,
        model: values.get("model").cloned().unwrap_or_default(),
        q: values.get("q").cloned().unwrap_or_default(),
        offset: parse_page("offset", 0)?.max(0),
        limit: parse_page("limit", 100)?.clamp(1, 500),
    })
}

pub(crate) async fn admin_llm_analytics_summary(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<ObservabilityHttpState>,
) -> Response {
    let headers = &headers;
    let raw_query = raw_query.as_deref();
    let routes = &routes;
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(pool) = routes.postgres.clone() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    let taskman: Arc<dyn openplotva_server::RuntimeTaskmanInspector> =
        Arc::new(routes.taskman_inspector.clone());
    let reader = runtime_llm_analytics::PostgresRuntimeLlmAnalyticsReader::new(pool)
        .with_discovery_capacity(
            routes.llm_discovery_base_url.to_string(),
            routes.llm_discovery_service_name.to_string(),
        )
        .with_sql_timeout_ms(routes.runtime_sql_timeout_ms)
        .with_taskman(taskman);
    let range = admin_auth_query_values(raw_query)
        .remove("range")
        .map(|value| value.trim().to_owned())
        .unwrap_or_default();
    match openplotva_server::RuntimeLlmAnalyticsReader::llm_analytics(&reader, &range).await {
        Ok(summary) => {
            let since = admin_llm_analytics_since_time(&summary);
            let tool_calls = openplotva_dialog::tool_telemetry::snapshot_since(since, 50);
            admin_json_no_cache_response(
                StatusCode::OK,
                admin_llm_analytics_summary_json(&summary, &tool_calls),
            )
        }
        Err(error) => {
            tracing::warn!(%error, "failed to build admin llm analytics summary");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_analytics_overview(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<ObservabilityHttpState>,
) -> Response {
    let headers = &headers;
    let raw_query = raw_query.as_deref();
    let routes = &routes;
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(pool) = routes.postgres.clone() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    let range = admin_auth_query_values(raw_query)
        .remove("range")
        .map(|value| value.trim().to_owned())
        .unwrap_or_default();
    let reader = runtime_analytics_overview::AnalyticsOverviewReader::new(
        pool,
        routes.runtime_sql_timeout_ms,
    );
    match reader.overview(&range).await {
        Ok(value) => admin_json_no_cache_response(StatusCode::OK, value),
        Err(error) => {
            tracing::warn!(%error, "failed to build admin analytics overview");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_gpu1_status(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<ObservabilityHttpState>,
) -> Response {
    let headers = &headers;
    let raw_query = raw_query.as_deref();
    let routes = &routes;
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let range = admin_auth_query_values(raw_query)
        .remove("range")
        .map(|value| value.trim().to_owned())
        .unwrap_or_default();
    let reader = runtime_gpu_arbiter::GpuArbiterReader::new(
        routes.llm_discovery_base_url.to_string(),
        routes.postgres.clone(),
        routes.runtime_sql_timeout_ms,
    );
    admin_json_no_cache_response(StatusCode::OK, reader.snapshot(&range).await)
}

pub(crate) async fn admin_llm_dialogs(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<ObservabilityHttpState>,
) -> Response {
    admin_llm_dialogs_response(&routes, method, &headers, raw_query.as_deref())
}

pub(crate) async fn admin_llm_dialogs_detail(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<ObservabilityHttpState>,
) -> Response {
    let headers = &headers;
    let raw_query = raw_query.as_deref();
    let routes = &routes;
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let id = admin_auth_query_values(raw_query)
        .remove("id")
        .and_then(|value| value.trim().parse::<i64>().ok());
    let Some(id) = id else {
        return admin_error_response(StatusCode::BAD_REQUEST, "id is required");
    };
    let record = routes
        .llm_run_buffer
        .as_ref()
        .and_then(|buffer| buffer.get(id));
    let Some(record) = record else {
        return admin_json_no_cache_response(
            StatusCode::OK,
            serde_json::json!({ "run": serde_json::Value::Null }),
        );
    };
    let mut run_json = record.detail_json();
    let live_traces: Vec<Option<openplotva_server::RuntimeLlmRequestData>> = record
        .rounds
        .iter()
        .map(|round| {
            routes
                .llm_trace_buffer
                .as_ref()
                .and_then(|traces| traces.get(round.trace_id))
        })
        .collect();
    let mut persisted: std::collections::HashMap<
        i32,
        (Option<serde_json::Value>, Option<serde_json::Value>),
    > = std::collections::HashMap::new();
    if live_traces.iter().any(Option::is_none)
        && let Some(pool) = routes.postgres.as_ref()
    {
        match runtime_llm::fetch_llm_raw_bodies_for_run(pool, &record.run_id).await {
            Ok(bodies) => persisted = bodies,
            Err(error) => {
                tracing::warn!(%error, run_id = %record.run_id, "failed to load persisted llm raw bodies");
            }
        }
    }
    if let Some(rounds) = run_json["rounds"].as_array_mut() {
        for (index, round_json) in rounds.iter_mut().enumerate() {
            let Some(round) = record.rounds.get(index) else {
                continue;
            };
            match live_traces.get(index).and_then(Option::as_ref) {
                Some(trace) => {
                    round_json["raw_source"] = serde_json::json!("live");
                    round_json["raw_request"] =
                        trace.raw_request.clone().unwrap_or(serde_json::Value::Null);
                    round_json["raw_response"] = trace
                        .raw_response
                        .clone()
                        .unwrap_or(serde_json::Value::Null);
                    if let Some(usage) = trace.usage.clone() {
                        round_json["usage"] = usage;
                    }
                    if let Some(timings) = trace.timings.clone() {
                        round_json["timings"] = timings;
                    }
                    if let Some(params) = trace.inference_params.clone() {
                        round_json["inference_params"] = params;
                    }
                }
                None => match persisted.remove(&round.seq) {
                    Some((request, response)) if request.is_some() || response.is_some() => {
                        round_json["raw_source"] = serde_json::json!("db");
                        round_json["raw_request"] = request.unwrap_or(serde_json::Value::Null);
                        round_json["raw_response"] = response.unwrap_or(serde_json::Value::Null);
                    }
                    _ => {
                        round_json["raw_source"] = serde_json::json!("rotated_out");
                    }
                },
            }
        }
    }
    admin_json_no_cache_response(StatusCode::OK, serde_json::json!({ "run": run_json }))
}

pub(crate) async fn admin_llm_dialogs_clear(
    method: Method,
    headers: HeaderMap,
    State(routes): State<ObservabilityHttpState>,
) -> Response {
    admin_llm_dialogs_clear_response(&routes, method, &headers)
}

pub(crate) async fn admin_state_response(routes: &ObservabilityHttpState) -> Response {
    let level = admin_app_setting(routes.postgres.as_ref(), "log_level")
        .await
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| routes.default_log_level.to_string());
    let dispatcher_stats =
        openplotva_server::RuntimeDispatcherInspector::stats(&routes.dispatcher_inspector);
    let cache_stats = openplotva_server::RuntimeCacheInspector::stats(&routes.cache_inspector);
    admin_json_response(
        StatusCode::OK,
        serde_json::json!({
            "log_level": level,
            "queue": {
                "regularQueueSize": dispatcher_stats.regular_queue_size,
                "immediateQueueSize": dispatcher_stats.immediate_queue_size,
                "processedTotal": dispatcher_stats.processed_total,
                "dedupedTotal": dispatcher_stats.deduped_total,
            },
            "cache": admin_cache_stats_json(cache_stats.cache),
            "planner": admin_cache_stats_json(cache_stats.planner_cache),
        }),
    )
}

pub(crate) fn admin_cache_stats_json(
    stats: openplotva_server::RuntimeCacheStatsData,
) -> serde_json::Value {
    serde_json::json!({
        "Size": stats.size,
        "Capacity": stats.capacity,
        "Hits": stats.hits,
        "Misses": stats.misses,
        "MemSize": stats.mem_size,
    })
}

#[cfg(test)]
pub(crate) async fn admin_loglevel_response(
    routes: &ObservabilityHttpState,
    method: Method,
    headers: &HeaderMap,
    body: &[u8],
) -> Response {
    admin_loglevel(
        method,
        headers.clone(),
        State(routes.clone()),
        Bytes::copy_from_slice(body),
    )
    .await
}

pub(crate) fn admin_llm_dialogs_filter(raw_query: Option<&str>) -> runtime_llm_runs::RunListFilter {
    let values = admin_auth_query_values(raw_query);
    runtime_llm_runs::RunListFilter {
        kind: values
            .get("kind")
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty() && value != "all"),
        chat_id: values
            .get("chat_id")
            .and_then(|value| value.trim().parse::<i64>().ok()),
        errors_only: parse_admin_optional_bool(values.get("errors_only").map(String::as_str))
            .unwrap_or(false),
        q: values
            .get("q")
            .map(|value| value.trim().to_owned())
            .unwrap_or_default(),
        limit: parse_admin_positive_i32(values.get("limit").map(String::as_str), 200, 1000)
            as usize,
    }
}

pub(crate) fn admin_llm_dialogs_response(
    routes: &ObservabilityHttpState,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(buffer) = routes.llm_run_buffer.as_ref() else {
        return admin_json_no_cache_response(
            StatusCode::OK,
            serde_json::json!({ "count": 0, "runs": [] }),
        );
    };
    let filter = admin_llm_dialogs_filter(raw_query);
    let runs = buffer
        .list(&filter, OffsetDateTime::now_utc())
        .iter()
        .map(runtime_llm_runs::RunRecord::skeleton_json)
        .collect::<Vec<_>>();
    admin_json_no_cache_response(
        StatusCode::OK,
        serde_json::json!({ "count": runs.len(), "runs": runs }),
    )
}

pub(crate) fn admin_llm_dialogs_clear_response(
    routes: &ObservabilityHttpState,
    method: Method,
    headers: &HeaderMap,
) -> Response {
    if method != Method::POST {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    if let Some(buffer) = routes.llm_run_buffer.as_ref() {
        buffer.clear();
    }
    admin_json_no_cache_response(StatusCode::OK, serde_json::json!({ "ok": true }))
}

pub(crate) fn admin_llm_analytics_since_time(
    summary: &openplotva_server::RuntimeLlmAnalyticsData,
) -> OffsetDateTime {
    OffsetDateTime::parse(&summary.since, &Rfc3339).unwrap_or_else(|_| OffsetDateTime::now_utc())
}

pub(crate) fn admin_safety_checks_filter(
    raw_query: Option<&str>,
) -> openplotva_server::RuntimeSafetyChecksFilter {
    let values = admin_auth_query_values(raw_query);
    openplotva_server::RuntimeSafetyChecksFilter {
        q: values
            .get("q")
            .map(|value| value.trim())
            .unwrap_or("")
            .to_owned(),
        flagged: parse_admin_optional_bool(values.get("flagged").map(String::as_str)),
        offset: parse_admin_non_negative_i32(values.get("offset").map(String::as_str), 0),
        limit: parse_admin_positive_i32(values.get("limit").map(String::as_str), 200, 1000),
    }
}

pub(crate) fn admin_safety_check_json(
    check: &openplotva_server::RuntimeSafetyCheckData,
) -> serde_json::Value {
    let mut root = serde_json::Map::new();
    admin_insert(&mut root, "id", check.id);
    admin_insert(&mut root, "created_at", &check.created_at);
    admin_insert(&mut root, "source", &check.source);
    admin_insert_opt(&mut root, "flow", check.flow.as_deref());
    admin_insert_opt(&mut root, "mode", check.mode.as_deref());
    admin_insert_opt(&mut root, "chat_id", check.chat_id);
    admin_insert_opt(&mut root, "thread_id", check.thread_id);
    admin_insert_opt(&mut root, "message_id", check.message_id);
    admin_insert_opt(&mut root, "user_id", check.user_id);
    admin_insert(&mut root, "deployment_id", &check.deployment_id);
    admin_insert_opt(
        &mut root,
        "external_session_id",
        check.external_session_id.as_deref(),
    );
    admin_insert(
        &mut root,
        "request_messages",
        check
            .request_messages
            .clone()
            .unwrap_or(serde_json::Value::Null),
    );
    admin_insert_opt(&mut root, "flagged", check.flagged);
    admin_insert_opt(
        &mut root,
        "internal_session_id",
        check.internal_session_id.as_deref(),
    );
    admin_insert_opt_value(&mut root, "policies", check.policies.as_ref());
    admin_insert_opt_value(&mut root, "response_json", check.response_json.as_ref());
    admin_insert(&mut root, "duration_ms", check.duration_ms);
    admin_insert_opt(&mut root, "error", check.error.as_deref());
    serde_json::Value::Object(root)
}

pub(crate) fn admin_llm_analytics_summary_json(
    summary: &openplotva_server::RuntimeLlmAnalyticsData,
    tool_calls: &openplotva_dialog::tool_telemetry::ToolTelemetrySnapshot,
) -> serde_json::Value {
    let mut root = serde_json::Map::new();
    admin_insert(&mut root, "range", &summary.range);
    admin_insert(&mut root, "bucket", &summary.bucket);
    admin_insert(&mut root, "since", &summary.since);
    admin_insert(
        &mut root,
        "totals",
        serde_json::json!({
            "total_count": summary.totals.total_count,
            "error_count": summary.totals.error_count,
            "avg_duration_ms": summary.totals.avg_duration_ms,
        }),
    );
    admin_insert(&mut root, "series", admin_llm_series_json(&summary.series));
    admin_insert(
        &mut root,
        "model_series",
        admin_llm_model_series_json(&summary.model_series),
    );
    admin_insert(
        &mut root,
        "top_chats",
        admin_llm_top_chats_json(&summary.top_chats),
    );
    admin_insert(&mut root, "models", admin_llm_models_json(&summary.models));
    admin_insert(
        &mut root,
        "providers",
        admin_llm_providers_json(&summary.providers),
    );
    admin_insert(
        &mut root,
        "inference_params",
        admin_llm_inference_params_json(&summary.inference_params),
    );
    admin_insert(
        &mut root,
        "stage_metrics",
        admin_llm_stage_metrics_json(&summary.stage_metrics),
    );
    admin_insert(
        &mut root,
        "runtime_jobs",
        admin_runtime_jobs_json(&summary.runtime_jobs),
    );
    admin_insert_opt(
        &mut root,
        "runtime_jobs_error",
        summary.runtime_jobs_error.as_deref(),
    );
    admin_insert(&mut root, "tool_calls", tool_calls.to_json());
    let ai_farm_capacity = summary
        .ai_farm_capacity
        .as_ref()
        .map(admin_aifarm_capacity_json);
    admin_insert_opt_value(&mut root, "aifarm_capacity", ai_farm_capacity.as_ref());
    serde_json::Value::Object(root)
}

pub(crate) fn admin_llm_series_json(
    series: &[openplotva_server::RuntimeLlmAnalyticsSeriesPointData],
) -> Vec<serde_json::Value> {
    series
        .iter()
        .map(|point| {
            serde_json::json!({
                "ts": point.ts,
                "total_count": point.total_count,
                "error_count": point.error_count,
                "avg_duration_ms": point.avg_duration_ms,
            })
        })
        .collect()
}

pub(crate) fn admin_llm_model_series_json(
    series: &[openplotva_server::RuntimeLlmAnalyticsModelSeriesPointData],
) -> Vec<serde_json::Value> {
    series
        .iter()
        .map(|point| {
            serde_json::json!({
                "ts": point.ts,
                "model": point.model,
                "request_count": point.request_count,
                "error_count": point.error_count,
                "avg_duration_ms": point.avg_duration_ms,
                "avg_generation_tps": point.avg_generation_tps,
                "avg_effective_output_tps": point.avg_effective_output_tps,
                "output_tokens": point.output_tokens,
            })
        })
        .collect()
}

pub(crate) fn admin_llm_top_chats_json(
    chats: &[openplotva_server::RuntimeLlmAnalyticsTopChatData],
) -> Vec<serde_json::Value> {
    chats
        .iter()
        .map(|chat| {
            let mut root = serde_json::Map::new();
            admin_insert(&mut root, "chat_id", chat.chat_id);
            admin_insert_opt(&mut root, "title", chat.title.as_deref());
            admin_insert_opt(&mut root, "username", chat.username.as_deref());
            admin_insert(&mut root, "request_count", chat.request_count);
            serde_json::Value::Object(root)
        })
        .collect()
}

pub(crate) fn admin_llm_models_json(
    models: &[openplotva_server::RuntimeLlmAnalyticsModelStatData],
) -> Vec<serde_json::Value> {
    models
        .iter()
        .map(|model| {
            serde_json::json!({
                "model": model.model,
                "request_count": model.request_count,
                "error_count": model.error_count,
                "avg_duration_ms": model.avg_duration_ms,
                "p50_duration_ms": model.p50_duration_ms,
                "p95_duration_ms": model.p95_duration_ms,
                "input_tokens": model.input_tokens,
                "output_tokens": model.output_tokens,
                "total_tokens": model.total_tokens,
                "avg_generation_tps": model.avg_generation_tps,
                "avg_effective_output_tps": model.avg_effective_output_tps,
                "p50_effective_output_tps": model.p50_effective_output_tps,
                "p95_effective_output_tps": model.p95_effective_output_tps,
            })
        })
        .collect()
}

pub(crate) fn admin_llm_providers_json(
    providers: &[openplotva_server::RuntimeLlmAnalyticsProviderStatData],
) -> Vec<serde_json::Value> {
    providers
        .iter()
        .map(|provider| {
            serde_json::json!({
                "provider": provider.provider,
                "source": provider.source,
                "request_count": provider.request_count,
                "error_count": provider.error_count,
                "avg_duration_ms": provider.avg_duration_ms,
                "p50_duration_ms": provider.p50_duration_ms,
                "p95_duration_ms": provider.p95_duration_ms,
                "input_tokens": provider.input_tokens,
                "output_tokens": provider.output_tokens,
                "total_tokens": provider.total_tokens,
                "avg_generation_tps": provider.avg_generation_tps,
                "avg_effective_output_tps": provider.avg_effective_output_tps,
            })
        })
        .collect()
}

pub(crate) fn admin_llm_inference_params_json(
    params: &[openplotva_server::RuntimeLlmAnalyticsInferenceParamStatData],
) -> Vec<serde_json::Value> {
    params
        .iter()
        .map(|param| {
            let mut root = serde_json::Map::new();
            admin_insert(&mut root, "provider", &param.provider);
            admin_insert(&mut root, "source", &param.source);
            admin_insert(&mut root, "model", &param.model);
            admin_insert_opt(&mut root, "max_tokens", param.max_tokens);
            admin_insert_opt(&mut root, "temperature", param.temperature);
            admin_insert_opt(&mut root, "top_p", param.top_p);
            admin_insert_opt(&mut root, "top_k", param.top_k);
            admin_insert_opt(&mut root, "candidate_count", param.candidate_count);
            admin_insert(&mut root, "tool_mode", &param.tool_mode);
            admin_insert(&mut root, "response_format", &param.response_format);
            admin_insert(&mut root, "request_count", param.request_count);
            admin_insert(&mut root, "error_count", param.error_count);
            admin_insert(&mut root, "avg_duration_ms", param.avg_duration_ms);
            admin_insert(
                &mut root,
                "avg_effective_output_tps",
                param.avg_effective_output_tps,
            );
            serde_json::Value::Object(root)
        })
        .collect()
}

pub(crate) fn admin_llm_stage_metrics_json(
    metrics: &[openplotva_server::RuntimeLlmAnalyticsStageMetricData],
) -> Vec<serde_json::Value> {
    metrics
        .iter()
        .map(|metric| {
            serde_json::json!({
                "stage": metric.stage,
                "source": metric.source,
                "request_count": metric.request_count,
                "error_count": metric.error_count,
                "avg_duration_ms": metric.avg_duration_ms,
                "p50_duration_ms": metric.p50_duration_ms,
                "p95_duration_ms": metric.p95_duration_ms,
                "avg_iteration": metric.avg_iteration,
                "max_iteration": metric.max_iteration,
            })
        })
        .collect()
}

pub(crate) fn admin_runtime_jobs_json(
    jobs: &[openplotva_server::RuntimeJobAnalyticsStatData],
) -> Vec<serde_json::Value> {
    jobs.iter()
        .map(|job| {
            serde_json::json!({
                "job_type": job.job_type,
                "queue_name": job.queue_name,
                "provider": job.provider,
                "created_count": job.created_count,
                "completed_count": job.completed_count,
                "failed_count": job.failed_count,
                "avg_wait_ms": job.avg_wait_ms,
                "p95_wait_ms": job.p95_wait_ms,
                "avg_processing_ms": job.avg_processing_ms,
                "p95_processing_ms": job.p95_processing_ms,
            })
        })
        .collect()
}

pub(crate) fn admin_aifarm_capacity_json(
    capacity: &openplotva_server::RuntimeAifarmCapacitySnapshotData,
) -> serde_json::Value {
    let mut root = serde_json::Map::new();
    admin_insert(&mut root, "service", &capacity.service);
    admin_insert(
        &mut root,
        "max_concurrent_jobs",
        capacity.max_concurrent_jobs,
    );
    admin_insert(&mut root, "running", capacity.running);
    admin_insert(&mut root, "queued", capacity.queued);
    admin_insert(&mut root, "available", capacity.available);
    admin_insert(&mut root, "locked", capacity.locked);
    admin_insert(&mut root, "ready", capacity.ready);
    admin_insert(&mut root, "observed_at", &capacity.observed_at);
    admin_insert_opt(&mut root, "error", capacity.error.as_deref());
    serde_json::Value::Object(root)
}

pub(crate) fn admin_insert<T: Serialize>(
    map: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: T,
) {
    map.insert(key.to_owned(), serde_json::json!(value));
}

pub(crate) fn admin_insert_opt<T: Serialize>(
    map: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: Option<T>,
) {
    if let Some(value) = value {
        admin_insert(map, key, value);
    }
}

pub(crate) fn admin_insert_opt_value(
    map: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: Option<&serde_json::Value>,
) {
    if let Some(value) = value {
        map.insert(key.to_owned(), value.clone());
    }
}

pub(crate) fn admin_logs_sse_stream(
    buffer: Arc<openplotva_observability::RuntimeLogBuffer>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    let after_seq = buffer.latest_seq();
    stream::unfold(
        AdminLogStreamState {
            buffer,
            after_seq,
            pending: VecDeque::new(),
            interval: tokio::time::interval(Duration::from_secs(1)),
        },
        |mut state| async move {
            loop {
                if let Some(entry) = state.pending.pop_front() {
                    state.after_seq = state.after_seq.max(entry.seq);
                    let data = serde_json::json!({
                        "seq": entry.seq,
                        "time": entry.time,
                        "level": entry.level,
                        "message": entry.message,
                        "attrs": entry.attrs,
                    })
                    .to_string();
                    return Some((Ok(Event::default().data(data)), state));
                }
                state.interval.tick().await;
                state.pending = VecDeque::from(state.buffer.logs(state.after_seq, 100, "", ""));
            }
        },
    )
}

#[derive(Debug, Deserialize)]
pub(crate) struct AdminLogLevelRequest {
    pub(crate) level: String,
}

#[derive(Debug)]
pub(crate) struct AdminLogStreamState {
    pub(crate) buffer: Arc<openplotva_observability::RuntimeLogBuffer>,
    pub(crate) after_seq: u64,
    pub(crate) pending: VecDeque<openplotva_observability::RuntimeLogEntry>,
    pub(crate) interval: Interval,
}
