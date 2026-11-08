//! HTTP query parsing shared by the existing request contracts.

use axum::{
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use std::collections::BTreeMap;

pub(crate) fn query_values(raw_query: Option<&str>) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    let Some(raw_query) = raw_query else {
        return values;
    };
    for (key, value) in url::form_urlencoded::parse(raw_query.as_bytes()) {
        values
            .entry(key.into_owned())
            .or_insert_with(|| value.into_owned());
    }
    values
}

pub(crate) fn parse_memory_limit(value: Option<&str>) -> usize {
    match value.and_then(|value| value.trim().parse::<usize>().ok()) {
        Some(limit) if limit > 500 => 500,
        Some(limit) if limit > 0 => limit,
        _ => 100,
    }
}

pub(crate) fn admin_error_response(status: StatusCode, error: &str) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        format!(r#"{{"error":"{error}"}}"#) + "\n",
    )
        .into_response()
}

pub(crate) fn admin_json_response(status: StatusCode, value: serde_json::Value) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        format!("{value}\n"),
    )
        .into_response()
}

pub(crate) fn admin_json_no_cache_response(
    status: StatusCode,
    value: serde_json::Value,
) -> Response {
    (
        status,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        format!("{value}\n"),
    )
        .into_response()
}

pub(crate) fn admin_required_i64(
    values: &BTreeMap<String, String>,
    key: &str,
    missing: &str,
    invalid: &str,
) -> Result<i64, Box<Response>> {
    let Some(value) = values.get(key).filter(|value| !value.is_empty()) else {
        return Err(Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            missing,
        )));
    };
    value
        .parse::<i64>()
        .map_err(|_| Box::new(admin_error_response(StatusCode::BAD_REQUEST, invalid)))
}

pub(crate) fn admin_non_empty_string(value: &str) -> Option<String> {
    let normalized = value.trim().to_owned();
    (!normalized.is_empty()).then_some(normalized)
}

pub(crate) fn admin_i64_from_json(value: &serde_json::Value) -> Option<i64> {
    value.as_i64().or_else(|| {
        value
            .as_str()
            .and_then(|value| value.trim().parse::<i64>().ok())
    })
}

pub(crate) fn admin_format_time(value: OffsetDateTime) -> String {
    value
        .format(&Rfc3339)
        .unwrap_or_else(|_| value.unix_timestamp().to_string())
}

pub(crate) fn admin_time_json(value: Option<OffsetDateTime>) -> serde_json::Value {
    value
        .map(admin_format_time)
        .map(serde_json::Value::String)
        .unwrap_or(serde_json::Value::Null)
}

use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub(crate) fn admin_request_is_json(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| contains_ascii_fold(value, "application/json"))
}

pub(crate) fn contains_ascii_fold(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    haystack.as_bytes().windows(needle.len()).any(|window| {
        std::str::from_utf8(window).is_ok_and(|part| part.eq_ignore_ascii_case(needle))
    })
}

pub(crate) fn parse_go_bool(value: &str) -> Option<bool> {
    match value.trim() {
        "1" => Some(true),
        "0" => Some(false),
        value if value.eq_ignore_ascii_case("t") => Some(true),
        value if value.eq_ignore_ascii_case("true") => Some(true),
        value if value.eq_ignore_ascii_case("f") => Some(false),
        value if value.eq_ignore_ascii_case("false") => Some(false),
        _ => None,
    }
}

pub(crate) fn parse_admin_positive_i32(
    value: Option<&str>,
    default_value: i32,
    max_value: i32,
) -> i32 {
    match value
        .map(str::trim)
        .and_then(|value| value.parse::<i32>().ok())
    {
        Some(parsed) if parsed > 0 => parsed.min(max_value),
        _ => default_value,
    }
}

pub(crate) fn parse_admin_non_negative_i32(value: Option<&str>, default_value: i32) -> i32 {
    match value
        .map(str::trim)
        .and_then(|value| value.parse::<i32>().ok())
    {
        Some(parsed) if parsed >= 0 => parsed,
        _ => default_value,
    }
}

pub(crate) fn parse_admin_optional_bool(value: Option<&str>) -> Option<bool> {
    match value.map(str::trim).map(str::to_lowercase).as_deref() {
        Some("true" | "1" | "yes") => Some(true),
        Some("false" | "0" | "no") => Some(false),
        _ => None,
    }
}

pub(crate) async fn admin_app_setting(pool: Option<&sqlx::PgPool>, key: &str) -> Option<String> {
    let pool = pool?;
    openplotva_storage::app_settings::app_setting(pool, key)
        .await
        .ok()
        .flatten()
}

pub(crate) async fn admin_upsert_app_setting(
    pool: Option<&sqlx::PgPool>,
    key: &str,
    value: &str,
) -> Result<(), openplotva_storage::StorageError> {
    let Some(pool) = pool else {
        return Ok(());
    };
    openplotva_storage::app_settings::upsert_app_setting(pool, key, value).await
}
