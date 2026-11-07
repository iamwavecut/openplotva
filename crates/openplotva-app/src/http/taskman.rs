use super::HttpRouterState;
use super::auth::{admin_auth_failure_response, require_admin_request};
use super::common::{
    admin_error_response, admin_format_time, admin_json_no_cache_response, admin_json_response,
    query_values as admin_auth_query_values,
};
use crate::runtime_taskman;
use axum::{
    extract::{RawQuery, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
};
use std::{collections::BTreeMap, sync::Arc};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Clone)]
pub(crate) struct TaskmanHttpState {
    admin_ids: Arc<[i64]>,
    bot_token: Arc<str>,
    taskman_inspector: runtime_taskman::RuntimeTaskmanInspectorHandle,
}

impl axum::extract::FromRef<Arc<HttpRouterState>> for TaskmanHttpState {
    fn from_ref(routes: &Arc<HttpRouterState>) -> Self {
        Self {
            admin_ids: routes.admin_ids.clone(),
            bot_token: routes.bot_token.clone(),
            taskman_inspector: routes.taskman_inspector.clone(),
        }
    }
}

#[cfg(test)]
pub(crate) fn test_state(routes: &HttpRouterState) -> TaskmanHttpState {
    use axum::extract::FromRef as _;
    TaskmanHttpState::from_ref(&Arc::new(routes.clone()))
}

pub(crate) async fn admin_taskman_jobs(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<TaskmanHttpState>,
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
    if !routes.taskman_inspector.is_configured() {
        return admin_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "task manager not configured",
        );
    }
    let filter = match admin_taskman_jobs_filter(raw_query) {
        Ok(filter) => filter,
        Err(response) => return *response,
    };
    match openplotva_server::RuntimeTaskmanInspector::list_jobs(&routes.taskman_inspector, filter)
        .await
    {
        Ok(result) => {
            admin_json_no_cache_response(StatusCode::OK, admin_taskman_list_json(&result))
        }
        Err(error) => {
            tracing::warn!(%error, "failed to list admin taskman jobs");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_taskman_jobs_clear(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<TaskmanHttpState>,
) -> Response {
    let headers = &headers;
    let raw_query = raw_query.as_deref();
    let routes = &routes;
    if method != Method::POST && method != Method::DELETE {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    if !routes.taskman_inspector.is_configured() {
        return admin_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "task manager not configured",
        );
    }
    let mut filter = match admin_taskman_jobs_filter(raw_query) {
        Ok(filter) => filter,
        Err(response) => return *response,
    };
    filter.offset = 0;
    filter.limit = 0;
    match routes.taskman_inspector.delete_jobs(filter).await {
        Ok(result) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({
                "ok": true,
                "matched": result.matched,
                "deleted": result.deleted,
                "deleted_active": result.deleted_active,
                "skipped_active": result.skipped_active,
            }),
        ),
        Err(error) => {
            tracing::warn!(%error, "failed to clear admin taskman jobs");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_taskman_job(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<TaskmanHttpState>,
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
    if !routes.taskman_inspector.is_configured() {
        return admin_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "task manager not configured",
        );
    }
    let job_id = match admin_taskman_job_id(raw_query) {
        Ok(job_id) => job_id,
        Err(response) => return *response,
    };
    match openplotva_server::RuntimeTaskmanInspector::job(&routes.taskman_inspector, job_id).await {
        Ok(Some(details)) => {
            admin_json_no_cache_response(StatusCode::OK, admin_taskman_details_json(&details))
        }
        Ok(None) => admin_error_response(StatusCode::NOT_FOUND, "not found"),
        Err(error) => {
            tracing::warn!(%error, job_id, "failed to load admin taskman job");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_taskman_job_cancel(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<TaskmanHttpState>,
) -> Response {
    admin_taskman_job_cancel_response(&routes, method, &headers, raw_query.as_deref())
}

pub(crate) async fn admin_taskman_job_restart(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<TaskmanHttpState>,
) -> Response {
    let headers = &headers;
    let raw_query = raw_query.as_deref();
    let routes = &routes;
    if method != Method::POST && method != Method::PUT {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    if !routes.taskman_inspector.is_configured() {
        return admin_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "task manager not configured",
        );
    }
    let job_id = match admin_taskman_job_id(raw_query) {
        Ok(job_id) => job_id,
        Err(response) => return *response,
    };
    match routes.taskman_inspector.restart_job(job_id).await {
        Ok(new_job_id) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "ok": true, "new_job_id": new_job_id }),
        ),
        Err(error) => {
            tracing::warn!(%error, job_id, "failed to restart admin taskman job");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

#[cfg(test)]
pub(crate) async fn admin_taskman_jobs_response(
    routes: &TaskmanHttpState,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    admin_taskman_jobs(
        method,
        headers.clone(),
        RawQuery(raw_query.map(str::to_owned)),
        State(routes.clone()),
    )
    .await
}

#[cfg(test)]
pub(crate) async fn admin_taskman_jobs_clear_response(
    routes: &TaskmanHttpState,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    admin_taskman_jobs_clear(
        method,
        headers.clone(),
        RawQuery(raw_query.map(str::to_owned)),
        State(routes.clone()),
    )
    .await
}

#[cfg(test)]
pub(crate) async fn admin_taskman_job_response(
    routes: &TaskmanHttpState,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    admin_taskman_job(
        method,
        headers.clone(),
        RawQuery(raw_query.map(str::to_owned)),
        State(routes.clone()),
    )
    .await
}

pub(crate) fn admin_taskman_job_cancel_response(
    routes: &TaskmanHttpState,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::POST && method != Method::PUT {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    if !routes.taskman_inspector.is_configured() {
        return admin_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "task manager not configured",
        );
    }
    let job_id = match admin_taskman_job_id(raw_query) {
        Ok(job_id) => job_id,
        Err(response) => return *response,
    };
    match routes.taskman_inspector.cancel_job(job_id) {
        Ok(()) => admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true })),
        Err(error) => {
            tracing::warn!(%error, job_id, "failed to cancel admin taskman job");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

#[cfg(test)]
pub(crate) async fn admin_taskman_job_restart_response(
    routes: &TaskmanHttpState,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    admin_taskman_job_restart(
        method,
        headers.clone(),
        RawQuery(raw_query.map(str::to_owned)),
        State(routes.clone()),
    )
    .await
}

pub(crate) fn admin_taskman_jobs_filter(
    raw_query: Option<&str>,
) -> Result<openplotva_server::RuntimeTaskmanJobsFilter, Box<Response>> {
    let values = admin_auth_query_values(raw_query);
    Ok(openplotva_server::RuntimeTaskmanJobsFilter {
        q: values
            .get("q")
            .map(|value| value.trim().to_owned())
            .unwrap_or_default(),
        status: admin_taskman_csv_values(&values, "status")
            .into_iter()
            .map(|status| {
                admin_taskman_valid_choice(
                    &status,
                    &["pending", "processing", "completed", "failed", "cancelled"],
                    "invalid status",
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
        queue: admin_taskman_csv_values(&values, "queue"),
        user_id: admin_taskman_i64(&values, "user_id", "invalid user_id")?,
        chat_id: admin_taskman_i64(&values, "chat_id", "invalid chat_id")?,
        time_field: values
            .get("time_field")
            .map(|value| {
                admin_taskman_optional_choice(
                    value,
                    &["created_at", "started_at", "completed_at"],
                    "invalid time_field",
                )
            })
            .transpose()?
            .unwrap_or_default(),
        from: values
            .get("from")
            .map(|value| admin_taskman_time(value, "invalid from"))
            .transpose()?
            .unwrap_or_default(),
        to: values
            .get("to")
            .map(|value| admin_taskman_time(value, "invalid to"))
            .transpose()?
            .unwrap_or_default(),
        sort_by: values
            .get("sort_by")
            .map(|value| {
                admin_taskman_optional_choice(
                    value,
                    &["id", "priority", "created_at", "started_at", "completed_at"],
                    "invalid sort_by",
                )
            })
            .transpose()?
            .unwrap_or_default(),
        sort_dir: values
            .get("sort_dir")
            .map(|value| admin_taskman_optional_choice(value, &["asc", "desc"], "invalid sort_dir"))
            .transpose()?
            .unwrap_or_default(),
        offset: admin_taskman_i32(&values, "offset", "invalid offset")?.unwrap_or_default(),
        limit: admin_taskman_i32(&values, "limit", "invalid limit")?.unwrap_or_default(),
    })
}

pub(crate) fn admin_taskman_job_id(raw_query: Option<&str>) -> Result<i64, Box<Response>> {
    let values = admin_auth_query_values(raw_query);
    let raw = values
        .get("job_id")
        .or_else(|| values.get("id"))
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Box::new(admin_error_response(
                StatusCode::BAD_REQUEST,
                "job_id required",
            ))
        })?;
    let id = raw.parse::<i64>().map_err(|_| {
        Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            "invalid job_id",
        ))
    })?;
    if id <= 0 {
        return Err(Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            "invalid job_id",
        )));
    }
    Ok(id)
}

pub(crate) fn admin_taskman_csv_values(
    values: &BTreeMap<String, String>,
    key: &str,
) -> Vec<String> {
    values
        .get(key)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn admin_taskman_i64(
    values: &BTreeMap<String, String>,
    key: &str,
    invalid: &str,
) -> Result<Option<i64>, Box<Response>> {
    let Some(value) = values
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    value
        .parse::<i64>()
        .map(Some)
        .map_err(|_| Box::new(admin_error_response(StatusCode::BAD_REQUEST, invalid)))
}

pub(crate) fn admin_taskman_i32(
    values: &BTreeMap<String, String>,
    key: &str,
    invalid: &str,
) -> Result<Option<i32>, Box<Response>> {
    let Some(value) = values
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    value
        .parse::<i32>()
        .map(Some)
        .map_err(|_| Box::new(admin_error_response(StatusCode::BAD_REQUEST, invalid)))
}

pub(crate) fn admin_taskman_optional_choice(
    value: &str,
    allowed: &[&str],
    invalid: &str,
) -> Result<String, Box<Response>> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(String::new());
    }
    admin_taskman_valid_choice(value, allowed, invalid)
}

pub(crate) fn admin_taskman_valid_choice(
    value: &str,
    allowed: &[&str],
    invalid: &str,
) -> Result<String, Box<Response>> {
    if allowed.contains(&value) {
        Ok(value.to_owned())
    } else {
        Err(Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            invalid,
        )))
    }
}

pub(crate) fn admin_taskman_time(value: &str, invalid: &str) -> Result<String, Box<Response>> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(String::new());
    }
    if let Ok(parsed) = OffsetDateTime::parse(value, &Rfc3339) {
        return Ok(admin_format_time(parsed));
    }
    if let Ok(timestamp) = value.parse::<i64>()
        && let Ok(parsed) = OffsetDateTime::from_unix_timestamp(timestamp)
    {
        return Ok(admin_format_time(parsed));
    }
    Err(Box::new(admin_error_response(
        StatusCode::BAD_REQUEST,
        invalid,
    )))
}

pub(crate) fn admin_taskman_list_json(
    result: &openplotva_server::RuntimeTaskmanJobListResultData,
) -> serde_json::Value {
    serde_json::json!({
        "total": result.total,
        "offset": result.offset,
        "limit": result.limit,
        "summary": {
            "by_status": result.summary.by_status,
            "by_queue": result.summary.by_queue,
        },
        "items": result.items.iter().map(admin_taskman_list_entry_json).collect::<Vec<_>>(),
    })
}

pub(crate) fn admin_taskman_list_entry_json(
    item: &openplotva_server::RuntimeTaskmanJobListEntryData,
) -> serde_json::Value {
    let mut value = admin_taskman_job_base_json(
        item.id,
        &item.queue_name,
        item.priority,
        &item.title,
        Some(serde_json::json!(item.job_type)),
        &item.status,
        item.user_id,
        item.chat_id,
        item.trigger_message_id,
        item.thread_message_id,
        item.progress_message_id,
        item.result_message_id,
        item.worker_id.as_deref(),
        &item.created_at,
        item.started_at.as_deref(),
        item.completed_at.as_deref(),
        item.error_message.as_deref(),
        item.processing_timeout_seconds,
        item.prompt_hash.as_deref(),
        item.estimated_processing_time,
        item.actual_processing_time,
    );
    if let Some(preview) = item.preview.as_deref().filter(|value| !value.is_empty()) {
        value.insert("preview".to_owned(), serde_json::json!(preview));
    }
    serde_json::Value::Object(value)
}

pub(crate) fn admin_taskman_details_json(
    details: &openplotva_server::RuntimeTaskmanJobDetailsData,
) -> serde_json::Value {
    let mut value = serde_json::Map::new();
    value.insert("job".to_owned(), admin_taskman_job_json(&details.job));
    value.insert(
        "messages".to_owned(),
        serde_json::json!(
            details
                .messages
                .iter()
                .map(admin_taskman_message_json)
                .collect::<Vec<_>>()
        ),
    );
    if let Some(events) = details.events.as_ref() {
        value.insert("events".to_owned(), events.clone());
    }
    serde_json::Value::Object(value)
}

pub(crate) fn admin_taskman_job_json(
    job: &openplotva_server::RuntimeTaskmanJobData,
) -> serde_json::Value {
    serde_json::Value::Object(admin_taskman_job_base_json(
        job.id,
        &job.queue_name,
        job.priority,
        &job.title,
        job.payload.clone(),
        &job.status,
        job.user_id,
        job.chat_id,
        job.trigger_message_id,
        job.thread_message_id,
        job.progress_message_id,
        job.result_message_id,
        job.worker_id.as_deref(),
        &job.created_at,
        job.started_at.as_deref(),
        job.completed_at.as_deref(),
        job.error_message.as_deref(),
        job.processing_timeout_seconds,
        job.prompt_hash.as_deref(),
        job.estimated_processing_time,
        job.actual_processing_time,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn admin_taskman_job_base_json(
    id: i64,
    queue_name: &str,
    priority: i32,
    title: &str,
    payload_or_type: Option<serde_json::Value>,
    status: &str,
    user_id: i64,
    chat_id: i64,
    trigger_message_id: i32,
    thread_message_id: Option<i32>,
    progress_message_id: Option<i32>,
    result_message_id: Option<i32>,
    worker_id: Option<&str>,
    created_at: &str,
    started_at: Option<&str>,
    completed_at: Option<&str>,
    error_message: Option<&str>,
    processing_timeout_seconds: i32,
    prompt_hash: Option<&str>,
    estimated_processing_time: Option<i32>,
    actual_processing_time: Option<i32>,
) -> serde_json::Map<String, serde_json::Value> {
    let mut value = serde_json::Map::new();
    value.insert("id".to_owned(), serde_json::json!(id));
    value.insert("queue_name".to_owned(), serde_json::json!(queue_name));
    value.insert("priority".to_owned(), serde_json::json!(priority));
    value.insert("title".to_owned(), serde_json::json!(title));
    if let Some(payload_or_type) = payload_or_type {
        let key = if payload_or_type.is_string() {
            "job_type"
        } else {
            "payload"
        };
        value.insert(key.to_owned(), payload_or_type);
    }
    value.insert("status".to_owned(), serde_json::json!(status));
    value.insert("user_id".to_owned(), serde_json::json!(user_id));
    value.insert("chat_id".to_owned(), serde_json::json!(chat_id));
    value.insert(
        "trigger_message_id".to_owned(),
        serde_json::json!(trigger_message_id),
    );
    admin_insert_i32_option(&mut value, "thread_message_id", thread_message_id);
    admin_insert_i32_option(&mut value, "progress_message_id", progress_message_id);
    admin_insert_i32_option(&mut value, "result_message_id", result_message_id);
    admin_insert_str_option(&mut value, "worker_id", worker_id);
    value.insert("created_at".to_owned(), serde_json::json!(created_at));
    admin_insert_str_option(&mut value, "started_at", started_at);
    admin_insert_str_option(&mut value, "completed_at", completed_at);
    admin_insert_str_option(&mut value, "error_message", error_message);
    value.insert(
        "processing_timeout_seconds".to_owned(),
        serde_json::json!(processing_timeout_seconds),
    );
    admin_insert_str_option(&mut value, "prompt_hash", prompt_hash);
    admin_insert_i32_option(
        &mut value,
        "estimated_processing_time",
        estimated_processing_time,
    );
    admin_insert_i32_option(&mut value, "actual_processing_time", actual_processing_time);
    value
}

pub(crate) fn admin_taskman_message_json(
    message: &openplotva_server::RuntimeTaskmanJobMessageData,
) -> serde_json::Value {
    serde_json::json!({
        "id": message.id,
        "job_id": message.job_id,
        "message_type": message.message_type,
        "chat_id": message.chat_id,
        "message_id": message.message_id,
        "created_at": message.created_at,
        "status": message.status,
    })
}

pub(crate) fn admin_insert_i32_option(
    value: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    item: Option<i32>,
) {
    if let Some(item) = item {
        value.insert(key.to_owned(), serde_json::json!(item));
    }
}

pub(crate) fn admin_insert_str_option(
    value: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    item: Option<&str>,
) {
    if let Some(item) = item {
        value.insert(key.to_owned(), serde_json::json!(item));
    }
}
