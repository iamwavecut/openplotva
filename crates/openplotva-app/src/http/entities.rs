//! Admin users, chats, permissions, and VIP adjustments.

use super::{
    HttpRouterState,
    auth::{admin_auth_failure_response, current_admin_user_id, require_admin_request},
    common::{
        admin_error_response, admin_format_time, admin_i64_from_json, admin_json_response,
        admin_non_empty_string, admin_required_i64, admin_time_json,
        query_values as admin_auth_query_values,
    },
};
use crate::settings;
use axum::{
    body::Bytes,
    extract::{RawQuery, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use openplotva_storage::database as sqlx;
use openplotva_storage::{
    PostgresChatMemberStore, PostgresChatSettingsStore, PostgresPaymentStore, PostgresVipStore,
    RedisBlockedChatStore,
    entities::{PostgresEntityStore, TelegramChatProfile, TelegramUserProfile, VipUserProfile},
};
use serde::Deserialize;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use time::OffsetDateTime;
const ADMIN_PAGE_LIMIT: i64 = 1000;

#[derive(Clone)]
pub(crate) struct EntitiesHttpState {
    admin_ids: Arc<[i64]>,
    bot_token: Arc<str>,
    member_store: Option<PostgresChatMemberStore>,
    postgres: Option<PgPool>,
    redis: Option<redis::Client>,
    settings_store: Option<PostgresChatSettingsStore>,
    telegram: Option<openplotva_telegram::TelegramClient>,
    vip_store: Option<PostgresVipStore>,
}

impl axum::extract::FromRef<Arc<HttpRouterState>> for EntitiesHttpState {
    fn from_ref(routes: &Arc<HttpRouterState>) -> Self {
        Self {
            admin_ids: routes.admin_ids.clone(),
            bot_token: routes.bot_token.clone(),
            member_store: routes.member_store.clone(),
            postgres: routes.postgres.clone(),
            redis: routes.redis.clone(),
            settings_store: routes.settings_store.clone(),
            telegram: routes.telegram.clone(),
            vip_store: routes.vip_store.clone(),
        }
    }
}

#[cfg(test)]
pub(crate) fn test_state(routes: &HttpRouterState) -> EntitiesHttpState {
    use axum::extract::FromRef as _;
    EntitiesHttpState::from_ref(&Arc::new(routes.clone()))
}

pub(crate) async fn admin_chat_get(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<EntitiesHttpState>,
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
    let values = admin_auth_query_values(raw_query);
    let chat_id =
        match admin_required_i64(&values, "chat_id", "chat_id required", "invalid chat_id") {
            Ok(value) => value,
            Err(response) => return *response,
        };
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_json_response(
            StatusCode::OK,
            serde_json::json!({
                "info": null,
                "settings": null,
                "permissions": null,
                "members_count": 0,
                "blocked": false,
            }),
        );
    };
    sync_admin_members_for_web(routes, chat_id).await;
    let info = PostgresEntityStore::new(pool.clone())
        .chat(chat_id)
        .await
        .ok()
        .flatten()
        .as_ref()
        .map(admin_chat_json);
    let settings = PostgresEntityStore::new(pool.clone())
        .chat_settings(chat_id)
        .await
        .ok()
        .flatten()
        .as_ref()
        .map(admin_settings_json);
    let permissions = PostgresEntityStore::new(pool.clone())
        .chat_permissions(chat_id)
        .await
        .ok()
        .flatten()
        .as_ref()
        .map(admin_permissions_json);
    let members_count = PostgresEntityStore::new(pool.clone())
        .member_count(chat_id)
        .await
        .unwrap_or(0);
    let blocked = match routes.redis.clone() {
        Some(redis) => RedisBlockedChatStore::new(redis)
            .blocked_until(chat_id)
            .await
            .ok()
            .flatten()
            .is_some(),
        None => false,
    };
    admin_json_response(
        StatusCode::OK,
        serde_json::json!({
            "info": info,
            "settings": settings,
            "permissions": permissions,
            "members_count": members_count,
            "blocked": blocked,
        }),
    )
}

pub(crate) async fn admin_chat_settings(
    method: Method,
    headers: HeaderMap,
    State(routes): State<EntitiesHttpState>,
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
    let Ok(mut req) = serde_json::from_slice::<AdminChatSettingsUpdateRequest>(body) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "bad request");
    };
    if req.chat_id == 0 {
        return admin_error_response(StatusCode::BAD_REQUEST, "chat_id required");
    }
    req.custom_persona = openplotva_web::truncate_custom_persona(req.custom_persona.take());
    let Some(settings_store) = routes.settings_store.as_ref() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    let cur = settings_store
        .get_chat_settings(req.chat_id)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| openplotva_core::ChatSettings::defaults(req.chat_id));
    let chat_type = admin_chat_type(routes, req.chat_id).await;
    let update = openplotva_core::ChatSettingsUpdate {
        chat_id: req.chat_id,
        chat_type,
        mood_alignment: req.mood_alignment,
        custom_persona: req.custom_persona,
        reactivity_percentage: req
            .reactivity_percentage
            .unwrap_or(cur.reactivity_percentage),
        proactivity_percentage: req
            .proactivity_percentage
            .unwrap_or(cur.proactivity_percentage),
        enable_global_text_reply: req
            .enable_global_text_reply
            .unwrap_or(cur.enable_global_text_reply),
        enable_global_draw_reply: req
            .enable_global_draw_reply
            .unwrap_or(cur.enable_global_draw_reply),
        enable_obscenifier: req.enable_obscenifier.unwrap_or(cur.enable_obscenifier),
        enable_profanity: req.enable_profanity.unwrap_or(cur.enable_profanity),
        enable_greet_joiners: req.enable_greet_joiners.unwrap_or(cur.enable_greet_joiners),
        enable_daily_game: req
            .enable_daily_game
            .unwrap_or_else(|| cur.enable_daily_game.unwrap_or(true)),
        daily_game_theme: req
            .daily_game_theme
            .unwrap_or_else(|| cur.daily_game_theme.unwrap_or_default()),
        greeting_html: req.greeting_html,
    };
    match settings_store.upsert_chat_settings(&update).await {
        Ok(()) => admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true })),
        Err(error) => {
            tracing::warn!(%error, chat_id = req.chat_id, "failed to update admin chat settings");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_chat_block(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<EntitiesHttpState>,
) -> Response {
    let headers = &headers;
    let raw_query = raw_query.as_deref();
    let routes = &routes;
    if method != Method::POST {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query);
    let chat_id =
        match admin_required_i64(&values, "chat_id", "chat_id required", "invalid chat_id") {
            Ok(value) => value,
            Err(response) => return *response,
        };
    let minutes = values
        .get("minutes")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(10);
    let until = OffsetDateTime::now_utc() + time::Duration::minutes(minutes);
    let Some(redis) = routes.redis.clone() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    let ttl = Duration::from_secs((minutes as u64).saturating_mul(60));
    match RedisBlockedChatStore::new(redis)
        .block_chat_until_with_ttl(chat_id, until, ttl)
        .await
    {
        Ok(()) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "ok": true, "until": admin_format_time(until) }),
        ),
        Err(error) => {
            tracing::warn!(%error, chat_id, "failed to block chat from admin web");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_chat_unblock(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<EntitiesHttpState>,
) -> Response {
    let headers = &headers;
    let raw_query = raw_query.as_deref();
    let routes = &routes;
    if method != Method::DELETE {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query);
    let chat_id =
        match admin_required_i64(&values, "chat_id", "chat_id required", "invalid chat_id") {
            Ok(value) => value,
            Err(response) => return *response,
        };
    let Some(redis_client) = routes.redis.clone() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    match RedisBlockedChatStore::new(redis_client)
        .unblock_chat(chat_id)
        .await
    {
        Ok(()) => admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true })),
        Err(error) => {
            tracing::warn!(%error, chat_id, "failed to unblock chat from admin web");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_chat_members(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<EntitiesHttpState>,
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
    let values = admin_auth_query_values(raw_query);
    let chat_id =
        match admin_required_i64(&values, "chat_id", "chat_id required", "invalid chat_id") {
            Ok(value) => value,
            Err(response) => return *response,
        };
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to load members");
    };
    sync_admin_members_for_web(routes, chat_id).await;
    match PostgresEntityStore::new(pool.clone())
        .members_with_users(chat_id)
        .await
    {
        Ok(rows) => {
            let members = rows
                .into_iter()
                .map(|(member, user)| {
                    serde_json::json!({
                        "member": admin_chat_member_json(&member),
                        "user": user.as_ref().map(admin_user_json),
                    })
                })
                .collect::<Vec<_>>();
            admin_json_response(StatusCode::OK, serde_json::json!({ "members": members }))
        }
        Err(error) => {
            tracing::warn!(%error, chat_id, "failed to list admin chat members");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to load members")
        }
    }
}

pub(crate) async fn admin_chats_list(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<EntitiesHttpState>,
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
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    let q = admin_auth_query_values(raw_query)
        .remove("q")
        .map(|value| value.trim().to_lowercase())
        .filter(|value| !value.is_empty());
    let rows = PostgresEntityStore::new(pool.clone())
        .chats(q.as_deref(), ADMIN_PAGE_LIMIT, 0)
        .await;
    match rows {
        Ok(rows) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "chats": rows.iter().map(admin_chat_json).collect::<Vec<_>>() }),
        ),
        Err(error) => {
            tracing::warn!(%error, "failed to list admin chats");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_chats_search_by_member(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<EntitiesHttpState>,
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
    let values = admin_auth_query_values(raw_query);
    let username = values
        .get("username")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let user_id = values
        .get("user_id")
        .and_then(|value| value.parse::<i64>().ok());
    if username.is_none() && user_id.is_none() {
        return admin_error_response(StatusCode::BAD_REQUEST, "username or user_id required");
    }
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "database error");
    };
    match PostgresEntityStore::new(pool.clone())
        .search_chats_by_member(username.as_deref(), user_id, ADMIN_PAGE_LIMIT)
        .await
    {
        Ok(rows) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "chats": rows.iter().map(admin_chat_json).collect::<Vec<_>>() }),
        ),
        Err(error) => {
            tracing::warn!(%error, "failed to search admin chats by member");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "database error")
        }
    }
}

pub(crate) async fn admin_users_list(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<EntitiesHttpState>,
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
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "database error");
    };
    let q = admin_auth_query_values(raw_query)
        .remove("q")
        .map(|value| value.trim().to_lowercase())
        .filter(|value| !value.is_empty());
    match PostgresEntityStore::new(pool.clone())
        .users(q.as_deref(), ADMIN_PAGE_LIMIT, 0)
        .await
    {
        Ok(rows) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CACHE_CONTROL, "no-cache"),
            ],
            format!(
                "{}\n",
                serde_json::json!({ "users": rows.iter().map(admin_user_json).collect::<Vec<_>>() })
            ),
        )
            .into_response(),
        Err(error) => {
            tracing::warn!(%error, "failed to list admin users");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "database error")
        }
    }
}

pub(crate) async fn admin_vip_users_list(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<EntitiesHttpState>,
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
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "database error");
    };
    let values = admin_auth_query_values(raw_query);
    let status = values
        .get("status")
        .map(|value| value.trim().to_lowercase())
        .filter(|value| matches!(value.as_str(), "active" | "expired" | "all"))
        .unwrap_or_else(|| "active".to_owned());
    let q = values
        .get("q")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    match PostgresEntityStore::new(pool.clone())
        .vip_users(&status, q.as_deref(), ADMIN_PAGE_LIMIT, 0)
        .await
    {
        Ok(rows) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "users": rows.iter().map(admin_vip_user_json).collect::<Vec<_>>() }),
        ),
        Err(error) => {
            tracing::warn!(%error, "failed to list admin vip users");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "database error")
        }
    }
}

pub(crate) async fn admin_user_get(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<EntitiesHttpState>,
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
    let values = admin_auth_query_values(raw_query);
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_error_response(StatusCode::NOT_FOUND, "not found");
    };
    let user_row = if let Some(id_raw) = values.get("id").filter(|value| !value.is_empty()) {
        let Ok(id) = id_raw.parse::<i64>() else {
            return admin_error_response(StatusCode::BAD_REQUEST, "invalid id");
        };
        PostgresEntityStore::new(pool.clone()).user(id).await
    } else if let Some(username) = values.get("username").filter(|value| !value.is_empty()) {
        PostgresEntityStore::new(pool.clone())
            .user_by_username(username)
            .await
    } else {
        return admin_error_response(StatusCode::BAD_REQUEST, "id or username required");
    };
    let user_row = match user_row {
        Ok(Some(row)) => row,
        Ok(None) | Err(_) => return admin_error_response(StatusCode::NOT_FOUND, "not found"),
    };
    let user_id = user_row.id;
    let payment_store = PostgresPaymentStore::new(pool.clone());
    let vip_store = routes
        .vip_store
        .clone()
        .unwrap_or_else(|| PostgresVipStore::new(pool.clone()));
    let subscription = payment_store
        .get_active_subscription(user_id)
        .await
        .ok()
        .flatten()
        .map(admin_subscription_json);
    let vip_cache = vip_store
        .get_vip_cache(user_id)
        .await
        .ok()
        .flatten()
        .map(admin_vip_cache_json);
    let vip_summary = match vip_store.get_vip_summary_by_user(user_id).await {
        Ok(summary) => admin_vip_summary_json(summary.as_ref()),
        Err(_) => admin_vip_summary_json(None),
    };
    let vip_events = match vip_store.list_vip_events_by_user(user_id).await {
        Ok(events) => events.iter().map(admin_vip_event_json).collect::<Vec<_>>(),
        Err(error) => {
            tracing::warn!(%error, user_id, "failed to load admin vip events");
            return admin_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load vip events",
            );
        }
    };
    let subscriptions = match payment_store.list_subscriptions_by_user(user_id).await {
        Ok(items) => items
            .iter()
            .map(admin_subscription_artifact_json)
            .collect::<Vec<_>>(),
        Err(error) => {
            tracing::warn!(%error, user_id, "failed to load admin subscriptions");
            return admin_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load subscriptions",
            );
        }
    };
    admin_json_response(
        StatusCode::OK,
        serde_json::json!({
            "user": admin_user_json(&user_row),
            "subscription": subscription,
            "vip": vip_cache,
            "vip_summary": vip_summary,
            "vip_events": vip_events,
            "subscriptions": subscriptions,
        }),
    )
}

pub(crate) async fn admin_user_grant_vip(
    method: Method,
    headers: HeaderMap,
    State(routes): State<EntitiesHttpState>,
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
    let Ok(req) = serde_json::from_slice::<AdminVipGrantRequest>(body) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "bad request");
    };
    let Some(user_id) = admin_i64_from_json(&req.user_id) else {
        return admin_error_response(
            StatusCode::BAD_REQUEST,
            "user_id and non-zero signed days required",
        );
    };
    if user_id == 0 || req.days == 0 {
        return admin_error_response(
            StatusCode::BAD_REQUEST,
            "user_id and non-zero signed days required",
        );
    }
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_error_response(StatusCode::NOT_FOUND, "user not found");
    };
    let placeholder_name = format!("Telegram user {user_id}");
    if let Err(error) = PostgresEntityStore::new(pool.clone())
        .ensure_user(user_id, &placeholder_name)
        .await
    {
        tracing::warn!(%error, user_id, "failed to ensure admin vip user");
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "user not found");
    }
    let admin_id = current_admin_user_id(headers, &routes.bot_token);
    let reason = admin_non_empty_string(&req.reason)
        .unwrap_or_else(|| format!("web admin adjustment by {admin_id}"));
    let actor_user_id = admin_existing_user_id(pool, admin_id).await;
    let vip_store = routes
        .vip_store
        .clone()
        .unwrap_or_else(|| PostgresVipStore::new(pool.clone()));
    match vip_store
        .create_vip_event(openplotva_storage::VipEventCreate {
            user_id,
            event_type: openplotva_core::VIP_EVENT_TYPE_ADMIN_ADJUSTMENT,
            delta_seconds: openplotva_core::vip_days_to_seconds(req.days),
            subscription_id: None,
            actor_user_id,
            reason: Some(&reason),
        })
        .await
    {
        Ok(event) => {
            let cache_invalidated = match vip_store.delete_vip_cache(user_id).await {
                Ok(()) => true,
                Err(error) => {
                    tracing::warn!(%error, user_id, "failed to invalidate admin vip cache");
                    false
                }
            };
            admin_json_response(
                StatusCode::OK,
                serde_json::json!({
                    "ok": true,
                    "event_id": event.id,
                    "expires_at": admin_format_time(event.effective_expires_at),
                    "cache_invalidated": cache_invalidated,
                }),
            )
        }
        Err(error) => {
            tracing::warn!(%error, user_id, "failed to grant admin vip");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_user_revoke_vip(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<EntitiesHttpState>,
) -> Response {
    let headers = &headers;
    let raw_query = raw_query.as_deref();
    let routes = &routes;
    if method != Method::DELETE {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query);
    let user_id =
        match admin_required_i64(&values, "user_id", "user_id required", "invalid user_id") {
            Ok(value) => value,
            Err(response) => return *response,
        };
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "ok": true, "revoked": false }),
        );
    };
    let vip_store = routes
        .vip_store
        .clone()
        .unwrap_or_else(|| PostgresVipStore::new(pool.clone()));
    let summary = vip_store
        .get_vip_summary_by_user(user_id)
        .await
        .ok()
        .flatten();
    if !admin_vip_summary_can_be_revoked(summary.as_ref()) {
        let cache_invalidated = match vip_store.delete_vip_cache(user_id).await {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, user_id, "failed to invalidate inactive admin vip cache");
                false
            }
        };
        return admin_json_response(
            StatusCode::OK,
            serde_json::json!({
                "ok": true,
                "revoked": false,
                "cache_invalidated": cache_invalidated,
            }),
        );
    }
    let summary = summary.expect("checked");
    let admin_id = current_admin_user_id(headers, &routes.bot_token);
    let reason = values
        .get("reason")
        .and_then(|value| admin_non_empty_string(value))
        .unwrap_or_else(|| format!("web admin revoke by {admin_id}"));
    let actor_user_id = admin_existing_user_id(pool, admin_id).await;
    match vip_store
        .create_vip_event(openplotva_storage::VipEventCreate {
            user_id,
            event_type: openplotva_core::VIP_EVENT_TYPE_ADMIN_REVOKE,
            delta_seconds: -summary.remaining_seconds,
            subscription_id: None,
            actor_user_id,
            reason: Some(&reason),
        })
        .await
    {
        Ok(_) => {
            let cache_invalidated = match vip_store.delete_vip_cache(user_id).await {
                Ok(()) => true,
                Err(error) => {
                    tracing::warn!(%error, user_id, "failed to invalidate revoked admin vip cache");
                    false
                }
            };
            admin_json_response(
                StatusCode::OK,
                serde_json::json!({
                    "ok": true,
                    "revoked": true,
                    "cache_invalidated": cache_invalidated,
                }),
            )
        }
        Err(error) => {
            tracing::warn!(%error, user_id, "failed to revoke admin vip");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

pub(crate) async fn admin_user_delete(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<EntitiesHttpState>,
) -> Response {
    let headers = &headers;
    let raw_query = raw_query.as_deref();
    let routes = &routes;
    if method != Method::DELETE {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query);
    let user_id = match admin_required_i64(&values, "id", "id required", "invalid id") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    match PostgresEntityStore::new(pool.clone())
        .delete_user(user_id)
        .await
    {
        Ok(_) => admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true })),
        Err(error) => {
            tracing::warn!(%error, user_id, "failed to delete admin user");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

#[cfg(test)]
pub(crate) async fn admin_chat_get_response(
    routes: &EntitiesHttpState,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    admin_chat_get(
        method,
        headers.clone(),
        RawQuery(raw_query.map(str::to_owned)),
        State(routes.clone()),
    )
    .await
}

pub(crate) async fn sync_admin_members_for_web(routes: &EntitiesHttpState, chat_id: i64) {
    let (Some(member_store), Some(telegram)) = (&routes.member_store, &routes.telegram) else {
        return;
    };
    match settings::sync_chat_admins_with_sources(member_store, telegram, chat_id).await {
        Ok(report) => tracing::debug!(
            chat_id,
            source = ?report.source,
            admin_count = report.admin_count,
            member_upsert_errors = report.member_upsert_errors,
            user_upsert_errors = report.user_upsert_errors,
            cache_errors = report.cache_errors,
            "refreshed admin members for web route"
        ),
        Err(error) => {
            tracing::debug!(%error, chat_id, "Telegram admin freshness failed for web route");
        }
    }
}

#[cfg(test)]
pub(crate) async fn admin_chats_search_by_member_response(
    routes: &EntitiesHttpState,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    admin_chats_search_by_member(
        method,
        headers.clone(),
        RawQuery(raw_query.map(str::to_owned)),
        State(routes.clone()),
    )
    .await
}

#[cfg(test)]
pub(crate) async fn admin_user_get_response(
    routes: &EntitiesHttpState,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    admin_user_get(
        method,
        headers.clone(),
        RawQuery(raw_query.map(str::to_owned)),
        State(routes.clone()),
    )
    .await
}

#[cfg(test)]
pub(crate) async fn admin_user_grant_vip_response(
    routes: &EntitiesHttpState,
    method: Method,
    headers: &HeaderMap,
    body: &[u8],
) -> Response {
    admin_user_grant_vip(
        method,
        headers.clone(),
        State(routes.clone()),
        Bytes::copy_from_slice(body),
    )
    .await
}

pub(crate) async fn admin_chat_type(routes: &EntitiesHttpState, chat_id: i64) -> String {
    let Some(pool) = routes.postgres.as_ref() else {
        return "private".to_owned();
    };
    PostgresEntityStore::new(pool.clone())
        .chat_type(chat_id)
        .await
        .ok()
        .flatten()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "private".to_owned())
}

pub(crate) async fn admin_existing_user_id(pool: &PgPool, user_id: i64) -> Option<i64> {
    if user_id <= 0 {
        return None;
    }
    PostgresEntityStore::new(pool.clone())
        .user(user_id)
        .await
        .ok()
        .flatten()
        .map(|user| user.id)
}

pub(crate) fn admin_json_value_base64(value: Option<&serde_json::Value>) -> serde_json::Value {
    value
        .and_then(|value| serde_json::to_vec(value).ok())
        .map(|bytes| serde_json::Value::String(BASE64_STANDARD.encode(bytes)))
        .unwrap_or(serde_json::Value::Null)
}

pub(crate) fn admin_chat_json(row: &TelegramChatProfile) -> serde_json::Value {
    serde_json::json!({
        "id": row.id,
        "type": row.chat_type,
        "title": row.title,
        "username": row.username,
        "first_name": row.first_name,
        "last_name": row.last_name,
        "is_forum": row.is_forum,
        "active_usernames": admin_json_value_base64(row.active_usernames.as_ref()),
        "available_reactions": admin_json_value_base64(row.available_reactions.as_ref()),
        "bio": row.bio,
        "has_private_forwards": row.has_private_forwards,
        "has_restricted_voice_and_video_messages": row.has_restricted_voice_and_video_messages,
        "join_to_send_messages": row.join_to_send_messages,
        "join_by_request": row.join_by_request,
        "description": row.description,
        "invite_link": row.invite_link,
        "pinned_message": admin_json_value_base64(row.pinned_message.as_ref()),
        "permissions": admin_json_value_base64(row.permissions.as_ref()),
        "slow_mode_delay": row.slow_mode_delay,
        "message_auto_delete_time": row.message_auto_delete_time,
        "has_aggressive_anti_spam_enabled": row.has_aggressive_anti_spam_enabled,
        "has_hidden_members": row.has_hidden_members,
        "has_protected_content": row.has_protected_content,
        "has_visible_history": row.has_visible_history,
        "sticker_set_name": row.sticker_set_name,
        "can_set_sticker_set": row.can_set_sticker_set,
        "linked_chat_id": row.linked_chat_id,
        "location": admin_json_value_base64(row.location.as_ref()),
        "discovered": admin_time_json(row.discovered),
        "updated": admin_time_json(row.updated),
    })
}

pub(crate) fn admin_user_json(row: &TelegramUserProfile) -> serde_json::Value {
    serde_json::json!({
        "id": row.id,
        "is_premium": row.is_premium,
        "first_name": row.first_name,
        "last_name": row.last_name,
        "username": row.username,
        "language_code": row.language_code,
        "is_vip": row.is_vip,
        "settings": admin_json_value_base64(row.settings.as_ref()),
        "discovered": admin_time_json(row.discovered),
        "updated": admin_time_json(row.updated),
    })
}

pub(crate) fn admin_vip_user_json(row: &VipUserProfile) -> serde_json::Value {
    let remaining_seconds = row.remaining_seconds.unwrap_or_default();
    let remaining_days = ((remaining_seconds as f64 / openplotva_core::VIP_SECONDS_PER_DAY as f64)
        .ceil() as i64)
        .max(0);
    serde_json::json!({
        "user": admin_user_json(&row.user),
        "vip_summary": {
            "active": row.vip_active.unwrap_or(false),
            "has_history": true,
            "expires_at": admin_time_json(row.vip_expires_at),
            "remaining_seconds": remaining_seconds,
            "remaining_days": remaining_days,
            "latest_event_id": row.latest_event_id,
            "latest_event_type": row.latest_event_type,
            "latest_delta_seconds": row.latest_delta_seconds.unwrap_or_default(),
            "latest_reason": row.latest_reason,
            "latest_created_at": admin_time_json(row.latest_created_at),
        },
        "subscriptions_count": row.subscriptions_count.unwrap_or_default(),
        "latest_subscription_expires_at": admin_time_json(row.latest_subscription_expires_at),
    })
}

pub(crate) fn admin_settings_json(
    row: &openplotva_storage::entities::StoredChatSettings,
) -> serde_json::Value {
    serde_json::json!({
        "chat_id": row.chat_id,
        "mood_alignment": row.mood_alignment,
        "custom_persona": row.custom_persona,
        "updated": admin_time_json(row.updated),
        "reactivity_percentage": row.reactivity_percentage,
        "proactivity_percentage": row.proactivity_percentage,
        "enable_global_text_reply": row.enable_global_text_reply,
        "enable_global_draw_reply": row.enable_global_draw_reply,
        "enable_obscenifier": row.enable_obscenifier,
        "enable_profanity": row.enable_profanity,
        "enable_greet_joiners": row.enable_greet_joiners,
        "enable_daily_game": row.enable_daily_game,
        "daily_game_theme": row.daily_game_theme,
        "greeting_html": row.greeting_html,
    })
}

pub(crate) fn admin_permissions_json(
    row: &openplotva_storage::entities::StoredChatPermissions,
) -> serde_json::Value {
    serde_json::json!({
        "chat_id": row.chat_id,
        "status": row.status.as_deref().unwrap_or_default(),
        "can_manage_chat": row.can_manage_chat,
        "can_delete_messages": row.can_delete_messages,
        "can_manage_video_chats": row.can_manage_video_chats,
        "can_restrict_members": row.can_restrict_members,
        "can_promote_members": row.can_promote_members,
        "can_change_info": row.can_change_info,
        "can_invite_users": row.can_invite_users,
        "can_post_messages": row.can_post_messages,
        "can_edit_messages": row.can_edit_messages,
        "can_pin_messages": row.can_pin_messages,
        "can_manage_topics": row.can_manage_topics,
        "can_send_messages": row.can_send_messages,
        "can_send_media_messages": row.can_send_media_messages,
        "can_send_polls": row.can_send_polls,
        "can_send_other_messages": row.can_send_other_messages,
        "can_add_web_page_previews": row.can_add_web_page_previews,
        "last_checked_at": admin_time_json(row.last_checked_at),
        "last_error_at": admin_time_json(row.last_error_at),
        "error_count": row.error_count,
        "error_message": row.error_message,
        "created_at": admin_time_json(row.created_at),
        "updated_at": admin_time_json(row.updated_at),
    })
}

pub(crate) fn admin_chat_member_json(
    row: &openplotva_storage::entities::ChatMemberProfile,
) -> serde_json::Value {
    serde_json::json!({
        "chat_id": row.membership.chat_id,
        "user_id": row.membership.user_id,
        "status": row.membership.status,
        "is_anonymous": row.membership.is_anonymous,
        "custom_title": row.membership.custom_title,
        "can_be_edited": row.membership.can_be_edited,
        "can_manage_chat": row.membership.can_manage_chat,
        "can_delete_messages": row.membership.can_delete_messages,
        "can_manage_video_chats": row.membership.can_manage_video_chats,
        "can_restrict_members": row.membership.can_restrict_members,
        "can_promote_members": row.membership.can_promote_members,
        "can_change_info": row.membership.can_change_info,
        "can_invite_users": row.membership.can_invite_users,
        "can_post_messages": row.membership.can_post_messages,
        "can_edit_messages": row.membership.can_edit_messages,
        "can_pin_messages": row.membership.can_pin_messages,
        "can_manage_topics": row.membership.can_manage_topics,
        "can_send_messages": row.membership.can_send_messages,
        "can_send_media_messages": row.membership.can_send_media_messages,
        "can_send_polls": row.membership.can_send_polls,
        "can_send_other_messages": row.membership.can_send_other_messages,
        "can_add_web_page_previews": row.membership.can_add_web_page_previews,
        "until_date": admin_time_json(row.membership.until_date),
        "created_at": admin_time_json(row.created_at),
        "updated_at": admin_time_json(row.updated_at),
        "last_message_at": admin_time_json(row.last_message_at),
    })
}

pub(crate) fn admin_subscription_json(
    item: openplotva_storage::SubscriptionRecord,
) -> serde_json::Value {
    serde_json::json!({
        "id": item.id,
        "user_id": item.user_id,
        "telegram_payment_charge_id": item.telegram_payment_charge_id,
        "provider_payment_charge_id": item.provider_payment_charge_id,
        "expires_at": admin_format_time(item.expires_at),
        "created_at": admin_format_time(item.created_at),
        "updated_at": admin_format_time(item.updated_at),
        "canceled_at": admin_time_json(item.canceled_at),
        "refunded_at": admin_time_json(item.refunded_at),
    })
}

pub(crate) fn admin_subscription_artifact_json(
    item: &openplotva_storage::SubscriptionRecord,
) -> serde_json::Value {
    serde_json::json!({
        "id": item.id,
        "telegram_payment_charge_id": item.telegram_payment_charge_id,
        "provider_payment_charge_id": item.provider_payment_charge_id,
        "expires_at": admin_format_time(item.expires_at),
        "created_at": admin_format_time(item.created_at),
        "updated_at": admin_format_time(item.updated_at),
        "canceled_at": admin_time_json(item.canceled_at),
        "refunded_at": admin_time_json(item.refunded_at),
        "status": admin_subscription_status(item.expires_at, item.canceled_at, item.refunded_at),
    })
}

pub(crate) fn admin_vip_cache_json(item: openplotva_storage::VipCacheRecord) -> serde_json::Value {
    serde_json::json!({
        "user_id": item.user_id,
        "is_vip": item.is_vip,
        "expires_at": admin_format_time(item.expires_at),
        "created_at": admin_format_time(item.created_at),
        "updated_at": admin_format_time(item.updated_at),
    })
}

pub(crate) fn admin_vip_summary_json(
    summary: Option<&openplotva_storage::VipSummaryRecord>,
) -> serde_json::Value {
    let Some(summary) = summary else {
        return serde_json::json!({
            "active": false,
            "has_history": false,
            "remaining_seconds": 0,
            "remaining_days": 0,
        });
    };
    let active = summary.is_active && OffsetDateTime::now_utc() < summary.effective_expires_at;
    let remaining_days = if active {
        let seconds = (summary.effective_expires_at - OffsetDateTime::now_utc()).whole_seconds();
        ((seconds as f64 / openplotva_core::VIP_SECONDS_PER_DAY as f64).round() as i64).max(0)
    } else {
        0
    };
    serde_json::json!({
        "active": active,
        "has_history": true,
        "expires_at": admin_format_time(summary.effective_expires_at),
        "remaining_seconds": summary.remaining_seconds,
        "remaining_days": remaining_days.max(0),
        "latest_event_id": summary.latest_event_id,
        "latest_event_type": summary.latest_event_type,
        "latest_reason": summary.latest_reason,
        "latest_created_at": admin_format_time(summary.latest_created_at),
    })
}

pub(crate) fn admin_vip_event_json(
    item: &openplotva_storage::VipEventListRecord,
) -> serde_json::Value {
    let actor_label = item
        .actor_username
        .as_ref()
        .filter(|value| !value.is_empty())
        .map(|value| format!("@{value}"))
        .or_else(|| {
            item.actor_first_name
                .clone()
                .filter(|value| !value.is_empty())
        })
        .or_else(|| item.actor_user_id.map(|value| value.to_string()))
        .unwrap_or_default();
    let subscription_status = if item.subscription_id.is_some() {
        admin_subscription_status(
            item.subscription_expires_at
                .unwrap_or(item.effective_expires_at),
            item.subscription_canceled_at,
            item.subscription_refunded_at,
        )
    } else {
        String::new()
    };
    serde_json::json!({
        "id": item.id,
        "event_type": item.event_type,
        "delta_seconds": item.delta_seconds,
        "delta_days": item.delta_seconds as f64 / openplotva_core::VIP_SECONDS_PER_DAY as f64,
        "effective_expires_at": admin_format_time(item.effective_expires_at),
        "actor_user_id": item.actor_user_id,
        "actor_label": actor_label,
        "reason": item.reason,
        "created_at": admin_format_time(item.created_at),
        "subscription_id": item.subscription_id,
        "telegram_payment_charge_id": item.telegram_payment_charge_id,
        "provider_payment_charge_id": item.provider_payment_charge_id,
        "subscription_status": subscription_status,
    })
}

pub(crate) fn admin_subscription_status(
    expires_at: OffsetDateTime,
    canceled_at: Option<OffsetDateTime>,
    refunded_at: Option<OffsetDateTime>,
) -> String {
    if refunded_at.is_some() {
        "refunded".to_owned()
    } else if canceled_at.is_some() {
        "canceled".to_owned()
    } else if OffsetDateTime::now_utc() < expires_at {
        "active".to_owned()
    } else {
        "expired".to_owned()
    }
}

pub(crate) fn admin_vip_summary_can_be_revoked(
    summary: Option<&openplotva_storage::VipSummaryRecord>,
) -> bool {
    summary
        .map(|summary| {
            summary.is_active && OffsetDateTime::now_utc() < summary.effective_expires_at
        })
        .unwrap_or(false)
}

#[derive(Debug, Deserialize)]
struct AdminChatSettingsUpdateRequest {
    chat_id: i64,
    mood_alignment: Option<String>,
    custom_persona: Option<String>,
    reactivity_percentage: Option<i32>,
    proactivity_percentage: Option<i32>,
    enable_obscenifier: Option<bool>,
    enable_profanity: Option<bool>,
    enable_greet_joiners: Option<bool>,
    enable_global_text_reply: Option<bool>,
    enable_global_draw_reply: Option<bool>,
    enable_daily_game: Option<bool>,
    daily_game_theme: Option<String>,
    greeting_html: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AdminVipGrantRequest {
    user_id: serde_json::Value,
    days: i64,
    #[serde(default)]
    reason: String,
}
