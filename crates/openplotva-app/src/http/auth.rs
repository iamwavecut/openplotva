//! Admin login, signed sessions, and protected asset access.

use super::{
    HttpRouterState,
    assets::{found, moved_permanently, static_web_asset_response},
    common::{admin_error_response, admin_json_response, query_values as admin_auth_query_values},
};
use axum::{
    extract::{Path, RawQuery, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use openplotva_storage::PostgresVirtualMessageStore;
use std::{collections::BTreeMap, sync::Arc};
use time::OffsetDateTime;

#[derive(Clone)]
pub(crate) struct AuthHttpState {
    pub(crate) admin_ids: Arc<[i64]>,
    pub(crate) bot_token: Arc<str>,
    pub(crate) bot_username: Arc<str>,
    pub(crate) state_store: Option<PostgresVirtualMessageStore>,
    pub(crate) webapp_url: Arc<str>,
}

impl axum::extract::FromRef<Arc<HttpRouterState>> for AuthHttpState {
    fn from_ref(routes: &Arc<HttpRouterState>) -> Self {
        Self {
            admin_ids: routes.admin_ids.clone(),
            bot_token: routes.bot_token.clone(),
            bot_username: routes.bot_username.clone(),
            state_store: routes.state_store.clone(),
            webapp_url: routes.webapp_url.clone(),
        }
    }
}

#[cfg(test)]
pub(crate) fn test_state(routes: &HttpRouterState) -> AuthHttpState {
    use axum::extract::FromRef as _;
    AuthHttpState::from_ref(&Arc::new(routes.clone()))
}

pub(crate) async fn admin_redirect() -> Response {
    moved_permanently("/admin/")
}

pub(crate) async fn admin_auth(
    method: Method,
    RawQuery(raw_query): RawQuery,
    State(routes): State<AuthHttpState>,
) -> Response {
    let raw_query = raw_query.as_deref();
    let routes = &routes;
    if method != Method::GET && method != Method::POST {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }

    let values = admin_auth_query_values(raw_query);
    let Some(user_id_raw) = values.get("id").filter(|value| !value.is_empty()) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "missing parameters");
    };
    let Some(hash) = values.get("hash").filter(|value| !value.is_empty()) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "missing parameters");
    };
    let Ok(user_id) = user_id_raw.parse::<i64>() else {
        return admin_error_response(StatusCode::BAD_REQUEST, "invalid user id");
    };
    if routes.bot_token.is_empty() {
        return admin_error_response(StatusCode::BAD_REQUEST, "bot token not configured");
    }

    let pairs = values
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()));
    if !openplotva_web::validate_telegram_auth(pairs, &routes.bot_token, hash) {
        tracing::error!("invalid admin auth signature");
        return admin_error_response(StatusCode::FORBIDDEN, "invalid auth");
    }

    if !admin_auth_date_is_fresh(&values, OffsetDateTime::now_utc()) {
        tracing::error!("admin auth rejected: stale or missing auth_date");
        return admin_error_response(StatusCode::FORBIDDEN, "auth expired");
    }

    if !routes.admin_ids.contains(&user_id) {
        tracing::error!(user_id, "authenticated Telegram user is not an admin");
        return admin_error_response(StatusCode::FORBIDDEN, "forbidden");
    }

    persist_admin_session_user(routes, &values, user_id).await;
    (
        StatusCode::FOUND,
        [
            (header::LOCATION, "/admin/".to_owned()),
            (
                header::SET_COOKIE,
                openplotva_web::admin_session_cookie(user_id, &routes.bot_token),
            ),
        ],
    )
        .into_response()
}

pub(crate) async fn admin_auth_check(
    State(routes): State<AuthHttpState>,
    headers: HeaderMap,
) -> Response {
    let authenticated_user_id = admin_session_user_id(&headers, &routes.bot_token)
        .filter(|user_id| routes.admin_ids.contains(user_id));
    match authenticated_user_id {
        Some(user_id) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({
                "authenticated": true,
                "user_id": user_id,
            }),
        ),
        None => admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "authenticated": false }),
        ),
    }
}

pub(crate) async fn admin_bootstrap(State(routes): State<AuthHttpState>) -> Response {
    admin_json_response(
        StatusCode::OK,
        serde_json::json!({
            "webapp_url": routes.webapp_url.as_ref(),
            "bot_username": routes.bot_username.as_ref(),
        }),
    )
}

pub(crate) async fn admin_index(
    State(routes): State<AuthHttpState>,
    headers: HeaderMap,
) -> Response {
    admin_web_asset_response(&routes, &headers, "")
}

pub(crate) async fn admin_asset(
    Path(path): Path<String>,
    State(routes): State<AuthHttpState>,
    headers: HeaderMap,
) -> Response {
    admin_web_asset_response(&routes, &headers, &path)
}

pub(crate) fn admin_web_asset_response(
    routes: &AuthHttpState,
    headers: &HeaderMap,
    path: &str,
) -> Response {
    if admin_static_asset_requires_auth(path)
        && !admin_session_is_authorized(headers, &routes.admin_ids, &routes.bot_token)
    {
        return found("/admin/login.html");
    }
    static_web_asset_response(openplotva_web::StaticAssetGroup::Admin, path)
}

/// Returns true if `auth_date` (unix seconds) is within the accepted freshness window
/// relative to `now`. Missing/unparseable dates and far-future dates are rejected.
pub(crate) fn admin_auth_date_is_fresh(
    values: &BTreeMap<String, String>,
    now: OffsetDateTime,
) -> bool {
    let Some(auth_date) = values
        .get("auth_date")
        .and_then(|v| v.trim().parse::<i64>().ok())
    else {
        return false;
    };
    let now_unix = now.unix_timestamp();
    let age = now_unix - auth_date;
    (-ADMIN_AUTH_FUTURE_SKEW_SECONDS..=ADMIN_AUTH_MAX_AGE_SECONDS).contains(&age)
}

#[cfg(test)]
pub(crate) async fn admin_auth_response(
    routes: &AuthHttpState,
    method: Method,
    raw_query: Option<&str>,
) -> Response {
    admin_auth(
        method,
        RawQuery(raw_query.map(str::to_owned)),
        State(routes.clone()),
    )
    .await
}

pub(crate) async fn persist_admin_session_user(
    routes: &AuthHttpState,
    values: &BTreeMap<String, String>,
    user_id: i64,
) {
    let Some(store) = &routes.state_store else {
        return;
    };
    let user = admin_auth_user_state(values, user_id);
    if let Err(error) = store.upsert_user_state(&user).await {
        tracing::warn!(%error, user_id, "failed to persist admin session user");
    }
}

pub(crate) fn admin_auth_user_state(
    values: &BTreeMap<String, String>,
    user_id: i64,
) -> openplotva_core::UserState {
    openplotva_core::UserState::new(
        user_id,
        trimmed_auth_value(values, "first_name").unwrap_or_else(|| "Telegram Admin".to_owned()),
        trimmed_auth_value(values, "last_name"),
        trimmed_auth_value(values, "username"),
        trimmed_auth_value(values, "language_code"),
        None,
    )
}

pub(crate) fn trimmed_auth_value(values: &BTreeMap<String, String>, key: &str) -> Option<String> {
    values
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

pub(crate) fn admin_auth_failure_response(error: AdminAuthFailure) -> Response {
    match error {
        AdminAuthFailure::Unauthorized => {
            admin_error_response(StatusCode::FORBIDDEN, "unauthorized")
        }
        AdminAuthFailure::Forbidden => admin_error_response(StatusCode::FORBIDDEN, "forbidden"),
    }
}

pub(crate) fn require_admin_request(
    headers: &HeaderMap,
    admin_ids: &[i64],
    secret: &str,
) -> Result<(), AdminAuthFailure> {
    let Some(user_id) = admin_session_user_id(headers, secret) else {
        return Err(AdminAuthFailure::Unauthorized);
    };
    if !admin_ids.contains(&user_id) {
        return Err(AdminAuthFailure::Forbidden);
    }
    Ok(())
}

pub(crate) fn admin_static_asset_requires_auth(path: &str) -> bool {
    let path = path.trim_matches('/');
    path.is_empty() || path == "index.html"
}

pub(crate) fn admin_session_is_authorized(
    headers: &HeaderMap,
    admin_ids: &[i64],
    secret: &str,
) -> bool {
    admin_session_user_ids(headers, secret)
        .into_iter()
        .any(|user_id| admin_ids.contains(&user_id))
}

pub(crate) fn admin_session_user_id(headers: &HeaderMap, secret: &str) -> Option<i64> {
    admin_session_user_ids(headers, secret).into_iter().next()
}

pub(crate) fn admin_session_user_ids(headers: &HeaderMap, secret: &str) -> Vec<i64> {
    let Some(cookie) = headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())
    else {
        return Vec::new();
    };
    cookie
        .split(';')
        .filter_map(|part| {
            part.trim()
                .strip_prefix(openplotva_web::ADMIN_SESSION_COOKIE_NAME)
                .and_then(|value| value.strip_prefix('='))
        })
        .filter_map(|value| openplotva_web::verify_admin_session_value(value, secret))
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdminAuthFailure {
    Unauthorized,
    Forbidden,
}

/// Maximum age of a Telegram Login `auth_date` accepted for admin authentication.
const ADMIN_AUTH_MAX_AGE_SECONDS: i64 = 86_400;
/// Small tolerance for clock skew on `auth_date` values slightly in the future.
const ADMIN_AUTH_FUTURE_SKEW_SECONDS: i64 = 60;

pub(crate) fn current_admin_user_id(headers: &HeaderMap, secret: &str) -> i64 {
    admin_session_user_id(headers, secret).unwrap_or(0)
}
