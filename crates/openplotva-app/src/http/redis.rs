use super::{
    HttpRouterState,
    auth::{admin_auth_failure_response, require_admin_request},
    common::{admin_error_response, admin_json_response, query_values as admin_auth_query_values},
};
use axum::{
    extract::{RawQuery, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
};
use openplotva_storage::redis_inspector::{
    RedisInspectorStore, prefix_groups_from_keys as admin_redis_prefix_groups_from_keys,
};
use std::sync::Arc;
#[derive(Clone)]
pub(crate) struct RedisHttpState {
    admin_ids: Arc<[i64]>,
    bot_token: Arc<str>,
    redis: Option<redis::Client>,
}
impl axum::extract::FromRef<Arc<HttpRouterState>> for RedisHttpState {
    fn from_ref(routes: &Arc<HttpRouterState>) -> Self {
        Self {
            admin_ids: routes.admin_ids.clone(),
            bot_token: routes.bot_token.clone(),
            redis: routes.redis.clone(),
        }
    }
}
pub(crate) async fn admin_redis_list(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<RedisHttpState>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let pattern = admin_auth_query_values(raw_query.as_deref())
        .remove("pattern")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "*".to_owned());
    let Some(redis) = routes.redis.clone().map(RedisInspectorStore::new) else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    match redis.scan_keys(&pattern, None).await {
        Ok(keys) => admin_json_response(StatusCode::OK, serde_json::json!({ "keys": keys })),
        Err(error) => {
            tracing::warn!(%error, pattern, "failed to list admin redis keys");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_redis_get(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<RedisHttpState>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query.as_deref());
    let Some(key) = values.get("key").filter(|value| !value.is_empty()) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "key required");
    };
    let Some(redis) = routes.redis.clone().map(RedisInspectorStore::new) else {
        return admin_error_response(StatusCode::NOT_FOUND, "not found");
    };
    match redis.value(key).await {
        Ok(Some(value)) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "key": key, "value": String::from_utf8_lossy(&value) }),
        ),
        Ok(None) => admin_error_response(StatusCode::NOT_FOUND, "not found"),
        Err(error) => {
            tracing::warn!(%error, key, "failed to get admin redis value");
            admin_error_response(StatusCode::NOT_FOUND, "not found")
        }
    }
}

pub(crate) async fn admin_redis_delete_prefix(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<RedisHttpState>,
) -> Response {
    if method != Method::DELETE {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query.as_deref());
    let Some(prefix) = values.get("prefix").filter(|value| !value.is_empty()) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "prefix required");
    };
    let Some(redis) = routes.redis.clone().map(RedisInspectorStore::new) else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    match redis.delete_by_prefix(prefix).await {
        Ok(()) => admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true })),
        Err(error) => {
            tracing::warn!(%error, prefix, "failed to delete admin redis prefix");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_redis_prefixes(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<RedisHttpState>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let prefix = admin_auth_query_values(raw_query.as_deref())
        .remove("prefix")
        .unwrap_or_default();
    let Some(redis) = routes.redis.clone().map(RedisInspectorStore::new) else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    match redis.scan_keys(&format!("{prefix}*"), None).await {
        Ok(keys) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "groups": admin_redis_prefix_groups_from_keys(&prefix, keys) }),
        ),
        Err(error) => {
            tracing::warn!(%error, prefix, "failed to list admin redis prefixes");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_redis_delete_key(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<RedisHttpState>,
) -> Response {
    if method != Method::DELETE {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query.as_deref());
    let Some(key) = values.get("key").filter(|value| !value.is_empty()) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "key required");
    };
    let Some(redis) = routes.redis.clone().map(RedisInspectorStore::new) else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    match redis.delete_key(key).await {
        Ok(()) => admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true })),
        Err(error) => {
            tracing::warn!(%error, key, "failed to delete admin redis key");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_redis_flushdb(
    method: Method,
    headers: HeaderMap,
    State(routes): State<RedisHttpState>,
) -> Response {
    if method != Method::POST {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(redis) = routes.redis.clone().map(RedisInspectorStore::new) else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    match redis.flushdb().await {
        Ok(()) => admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true })),
        Err(error) => {
            tracing::warn!(%error, "failed to flush admin redis db");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}
