//! Settings WebApp HTTP contract and its required dependencies.

use super::{
    HttpRouterState,
    assets::{moved_permanently, static_web_asset_response},
    common::{parse_memory_limit, query_values},
};
use crate::{payments, settings};
use axum::{
    body::Bytes,
    extract::{Path, RawQuery, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use openplotva_storage::{
    PostgresChatMemberStore, PostgresChatSettingsStore, PostgresMemoryStore, PostgresVipStore,
    PostgresVirtualMessageStore,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};
use time::OffsetDateTime;

#[derive(Clone)]
pub(crate) struct SettingsHttpState {
    pub(crate) admin_ids: Arc<[i64]>,
    pub(crate) bot_id: Option<i64>,
    pub(crate) bot_token: Arc<str>,
    pub(crate) member_store: Option<PostgresChatMemberStore>,
    pub(crate) memory_store: Option<PostgresMemoryStore>,
    pub(crate) settings_store: Option<PostgresChatSettingsStore>,
    pub(crate) state_store: Option<PostgresVirtualMessageStore>,
    pub(crate) telegram: Option<openplotva_telegram::TelegramClient>,
    pub(crate) vip_status: Option<Arc<dyn payments::VipStatusChecker + Send + Sync>>,
    pub(crate) vip_store: Option<PostgresVipStore>,
}

impl axum::extract::FromRef<Arc<HttpRouterState>> for SettingsHttpState {
    fn from_ref(routes: &Arc<HttpRouterState>) -> Self {
        Self {
            admin_ids: routes.admin_ids.clone(),
            bot_id: routes.bot_id,
            bot_token: routes.bot_token.clone(),
            member_store: routes.member_store.clone(),
            memory_store: routes.memory_store.clone(),
            settings_store: routes.settings_store.clone(),
            state_store: routes.state_store.clone(),
            telegram: routes.telegram.clone(),
            vip_status: routes.vip_status.clone(),
            vip_store: routes.vip_store.clone(),
        }
    }
}

#[cfg(test)]
pub(crate) fn test_state(routes: &HttpRouterState) -> SettingsHttpState {
    use axum::extract::FromRef as _;
    SettingsHttpState::from_ref(&Arc::new(routes.clone()))
}

pub(crate) async fn settings_redirect() -> Response {
    moved_permanently("/settings/")
}

pub(crate) async fn settings_index() -> Response {
    static_web_asset_response(openplotva_web::StaticAssetGroup::Settings, "")
}

pub(crate) async fn settings_asset(Path(path): Path<String>) -> Response {
    static_web_asset_response(openplotva_web::StaticAssetGroup::Settings, &path)
}

pub(crate) async fn settings_api(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<SettingsHttpState>,
    body: Bytes,
) -> Response {
    let init_data = settings_init_data_header(&headers);
    match method {
        Method::OPTIONS => settings_options_response(),
        Method::GET => settings_get_response(&routes, raw_query.as_deref(), init_data).await,
        Method::POST | Method::PUT => settings_update_response(&routes, &body, init_data).await,
        _ => settings_error_response(StatusCode::METHOD_NOT_ALLOWED, "Method not allowed"),
    }
}

pub(crate) async fn settings_deputies_api(
    method: Method,
    headers: HeaderMap,
    RawQuery(_raw_query): RawQuery,
    State(routes): State<SettingsHttpState>,
    body: Bytes,
) -> Response {
    let init_data = settings_init_data_header(&headers);
    match method {
        Method::OPTIONS => settings_side_options_response("GET, PUT, OPTIONS"),
        Method::PUT => settings_deputies_update_response(&routes, &body, init_data).await,
        _ => settings_side_error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "Method not allowed",
            "GET, PUT, OPTIONS",
        ),
    }
}

pub(crate) async fn settings_deputy_candidates_api(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<SettingsHttpState>,
) -> Response {
    let init_data = settings_init_data_header(&headers);
    match method {
        Method::OPTIONS => settings_side_options_response("GET, PUT, OPTIONS"),
        Method::GET => {
            settings_deputy_candidates_response(&routes, raw_query.as_deref(), init_data).await
        }
        _ => settings_side_error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "Method not allowed",
            "GET, PUT, OPTIONS",
        ),
    }
}

pub(crate) async fn settings_memory_api(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<SettingsHttpState>,
) -> Response {
    let init_data = settings_init_data_header(&headers);
    match method {
        Method::OPTIONS => settings_side_options_response("GET, DELETE, OPTIONS"),
        Method::GET => settings_memory_get_response(&routes, raw_query.as_deref(), init_data).await,
        Method::DELETE => {
            settings_memory_delete_response(&routes, raw_query.as_deref(), init_data).await
        }
        _ => settings_side_error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
            "GET, DELETE, OPTIONS",
        ),
    }
}

pub(crate) async fn settings_chats_api(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    State(routes): State<SettingsHttpState>,
) -> Response {
    if method != Method::GET {
        return (StatusCode::METHOD_NOT_ALLOWED, "Method not allowed\n").into_response();
    }
    let init_data = settings_init_data_header(&headers);
    settings_chats_response(&routes, raw_query.as_deref(), init_data).await
}

#[derive(Debug, Deserialize)]
pub(crate) struct SettingsUpdateRequest {
    pub(crate) chat_id: i64,
    #[serde(default)]
    pub(crate) user_id: i64,
    pub(crate) mood_alignment: Option<String>,
    pub(crate) custom_persona: Option<String>,
    pub(crate) reactivity_percentage: Option<i32>,
    pub(crate) proactivity_percentage: Option<i32>,
    pub(crate) enable_obscenifier: Option<bool>,
    pub(crate) enable_profanity: Option<bool>,
    pub(crate) enable_greet_joiners: Option<bool>,
    pub(crate) enable_global_text_reply: Option<bool>,
    pub(crate) enable_global_draw_reply: Option<bool>,
    pub(crate) signature: String,
    pub(crate) enable_daily_game: Option<bool>,
    pub(crate) daily_game_theme: Option<String>,
    pub(crate) greeting_html: Option<String>,
    pub(crate) disable_random_reactivity: Option<bool>,
    pub(crate) hide_original_draw_prompt: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct SettingsDeputySummary {
    pub(crate) id: i64,
    pub(crate) first_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) username: Option<String>,
    pub(crate) status: String,
    pub(crate) display_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct SettingsResponseBody {
    pub(crate) chat_id: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub(crate) chat_title: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub(crate) chat_type: String,
    pub(crate) mood_alignment: Option<String>,
    pub(crate) custom_persona: Option<String>,
    pub(crate) reactivity_percentage: i32,
    pub(crate) proactivity_percentage: i32,
    pub(crate) enable_obscenifier: bool,
    pub(crate) enable_profanity: bool,
    pub(crate) enable_greet_joiners: bool,
    pub(crate) enable_global_text_reply: bool,
    pub(crate) enable_global_draw_reply: bool,
    pub(crate) enable_daily_game: bool,
    pub(crate) daily_game_theme: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) greeting_html: Option<String>,
    pub(crate) is_vip: bool,
    pub(crate) disable_random_reactivity: bool,
    pub(crate) hide_original_draw_prompt: bool,
    pub(crate) is_deputy: bool,
    pub(crate) can_manage_deputies: bool,
    pub(crate) deputies: Vec<SettingsDeputySummary>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SettingsAccess {
    pub(crate) chat_id: i64,
    pub(crate) user_id: i64,
    pub(crate) is_global_admin: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SettingsAccessError {
    InvalidChatId,
    MissingSignature,
    InvalidSignature,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DeputyUpdateRequest {
    pub(crate) chat_id: i64,
    pub(crate) user_id: i64,
    pub(crate) signature: String,
    #[serde(default)]
    pub(crate) deputy_ids: Vec<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct DeputyCandidatesResponse {
    pub(crate) items: Vec<SettingsDeputySummary>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct DeputyUpdateResponse {
    pub(crate) ok: bool,
    pub(crate) deputies: Vec<SettingsDeputySummary>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeputyOwnerAccess {
    pub(crate) chat_id: i64,
    pub(crate) user_id: i64,
    pub(crate) is_global_admin: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ChatListItem {
    pub(crate) id: i64,
    pub(crate) title: String,
    #[serde(rename = "type")]
    pub(crate) chat_type: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct SettingsMemoryResponse {
    pub(crate) chat_id: i64,
    pub(crate) user_id: i64,
    pub(crate) count: usize,
    pub(crate) cards: Vec<openplotva_memory::Card>,
}

pub(crate) async fn settings_get_response(
    routes: &SettingsHttpState,
    raw_query: Option<&str>,
    init_data: Option<&str>,
) -> Response {
    let Some(settings_store) = &routes.settings_store else {
        return settings_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
        );
    };
    let Some(member_store) = &routes.member_store else {
        return settings_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
        );
    };

    let values = query_values(raw_query);
    let claimed_user_id = values
        .get("user_id")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    if authenticate_settings_init_data(routes, init_data, claimed_user_id).is_err() {
        return settings_error_response(StatusCode::UNAUTHORIZED, "Unauthorized");
    }
    let access = match parse_settings_get_access(&values, &routes.admin_ids) {
        Ok(access) => access,
        Err(error) => return settings_access_error_response(error),
    };

    if let Err(error) = ensure_settings_chat_available(routes, access.chat_id, access.user_id).await
    {
        return error;
    }
    if let Err(error) = authorize_settings_user(routes, access).await {
        return error;
    }

    let mut settings = match settings_store.get_chat_settings(access.chat_id).await {
        Ok(Some(settings)) => settings,
        Ok(None) | Err(_) => openplotva_core::ChatSettings::defaults(access.chat_id),
    };
    settings.custom_persona =
        openplotva_web::truncate_custom_persona(settings.custom_persona.take());

    let (chat_title, chat_type) =
        settings_chat_display_for_read(routes, access.chat_id, access.user_id).await;
    let mut response = new_settings_response(&settings, chat_title, chat_type.clone());
    apply_private_chat_response_defaults(&mut response, access.chat_id, access.user_id);
    apply_user_settings_response(routes, &mut response, access.user_id).await;
    apply_deputy_settings_response(
        member_store,
        &mut response,
        access.chat_id,
        access.user_id,
        &chat_type,
        access.is_global_admin,
    )
    .await;
    settings_json_response(StatusCode::OK, &response)
}

pub(crate) async fn settings_update_response(
    routes: &SettingsHttpState,
    body: &[u8],
    init_data: Option<&str>,
) -> Response {
    let Some(settings_store) = &routes.settings_store else {
        return settings_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
        );
    };
    let req = match parse_settings_update_request(body) {
        Ok(req) => req,
        Err(response) => return *response,
    };
    if authenticate_settings_init_data(routes, init_data, req.user_id).is_err() {
        return settings_error_response(StatusCode::UNAUTHORIZED, "Unauthorized");
    }
    if let Err(error) = ensure_settings_chat_available(routes, req.chat_id, req.user_id).await {
        return error;
    }
    let chat = match settings_update_chat(routes, req.chat_id, req.user_id).await {
        Ok(chat) => chat,
        Err(response) => return response,
    };
    let access = SettingsAccess {
        chat_id: req.chat_id,
        user_id: req.user_id,
        is_global_admin: routes.admin_ids.contains(&req.user_id),
    };
    if let Err(error) = authorize_settings_user(routes, access).await {
        return error;
    }

    let update = settings_update_from_request(settings_store, &req, &chat).await;
    if let Err(error) = settings_store.upsert_chat_settings(&update).await {
        tracing::error!(%error, chat_id = req.chat_id, "failed to update chat settings");
        return settings_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to update settings",
        );
    }
    update_user_settings_from_request(routes, &req).await;
    settings_json_response(StatusCode::OK, &serde_json::json!({ "status": "success" }))
}

pub(crate) async fn settings_chats_response(
    routes: &SettingsHttpState,
    raw_query: Option<&str>,
    init_data: Option<&str>,
) -> Response {
    let values = query_values(raw_query);
    let Some(user_id_raw) = values.get("user_id") else {
        return settings_json_raw_response(
            StatusCode::BAD_REQUEST,
            r#"{"error":"missing user_id"}"#,
        );
    };
    let Ok(user_id) = user_id_raw.parse::<i64>() else {
        return settings_json_raw_response(
            StatusCode::BAD_REQUEST,
            r#"{"error":"invalid user_id"}"#,
        );
    };
    if authenticate_settings_init_data(routes, init_data, user_id).is_err() {
        return settings_json_raw_response(StatusCode::UNAUTHORIZED, r#"{"error":"unauthorized"}"#);
    }
    let Some(signature) = values.get("signature").filter(|value| !value.is_empty()) else {
        return settings_json_raw_response(
            StatusCode::BAD_REQUEST,
            r#"{"error":"missing signature"}"#,
        );
    };
    if !openplotva_web::validate_settings_access_signature(user_id, 0, signature) {
        return settings_json_raw_response(
            StatusCode::FORBIDDEN,
            r#"{"error":"invalid signature"}"#,
        );
    }
    let Some(settings_store) = &routes.settings_store else {
        return settings_json_response(StatusCode::OK, &Vec::<ChatListItem>::new());
    };
    let Some(member_store) = &routes.member_store else {
        return settings_json_response(StatusCode::OK, &Vec::<ChatListItem>::new());
    };
    let chats = managed_chat_list_items(routes, settings_store, member_store, user_id).await;
    settings_json_response(StatusCode::OK, &chats)
}

pub(crate) async fn settings_deputy_candidates_response(
    routes: &SettingsHttpState,
    raw_query: Option<&str>,
    init_data: Option<&str>,
) -> Response {
    let access = match parse_deputy_owner_access(routes, raw_query, init_data).await {
        Ok(access) => access,
        Err(response) => return response,
    };
    if !access.is_global_admin
        && let Err(response) =
            ensure_deputy_management_permission(routes, access.chat_id, access.user_id).await
    {
        return response;
    }
    let query_values = query_values(raw_query);
    let query = query_values
        .get("query")
        .or_else(|| query_values.get("q"))
        .map(|value| value.trim())
        .unwrap_or("");
    let limit = parse_deputy_candidates_limit(query_values.get("limit").map(String::as_str));
    let Some(member_store) = &routes.member_store else {
        return settings_side_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
            "GET, PUT, OPTIONS",
        );
    };
    let rows = match member_store
        .search_chat_member_candidates(access.chat_id, query, limit as i32)
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(%error, chat_id = access.chat_id, user_id = access.user_id, "failed to search deputy candidates");
            return settings_side_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to search deputy candidates",
                "GET, PUT, OPTIONS",
            );
        }
    };
    let items = rows
        .into_iter()
        .filter(|row| row.id != 0 && row.id != access.user_id)
        .map(|row| SettingsDeputySummary {
            id: row.id,
            display_name: build_deputy_display_name(
                row.id,
                &row.first_name,
                row.last_name.as_deref(),
                row.username.as_deref(),
            ),
            first_name: row.first_name,
            last_name: row.last_name,
            username: row.username,
            status: row.status,
        })
        .collect::<Vec<_>>();
    settings_side_json_response(
        StatusCode::OK,
        &DeputyCandidatesResponse { items },
        "GET, PUT, OPTIONS",
    )
}

pub(crate) async fn settings_deputies_update_response(
    routes: &SettingsHttpState,
    body: &[u8],
    init_data: Option<&str>,
) -> Response {
    let req = match parse_deputy_update_request(body) {
        Ok(req) => req,
        Err(response) => return *response,
    };
    if authenticate_settings_init_data(routes, init_data, req.user_id).is_err() {
        return settings_side_error_response(
            StatusCode::UNAUTHORIZED,
            "Unauthorized",
            "GET, PUT, OPTIONS",
        );
    }
    if let Err(error) = ensure_settings_chat_available(routes, req.chat_id, req.user_id).await {
        return error;
    }
    if !routes.admin_ids.contains(&req.user_id)
        && let Err(response) =
            ensure_deputy_management_permission(routes, req.chat_id, req.user_id).await
    {
        return response;
    }
    let deputy_ids = normalize_deputy_ids(&req.deputy_ids, req.user_id);
    if let Err(message) = validate_deputy_ids(routes, req.chat_id, &deputy_ids).await {
        return settings_side_json_raw_response(
            StatusCode::BAD_REQUEST,
            &serde_json::json!({ "error": message }).to_string(),
            "GET, PUT, OPTIONS",
        );
    }
    let Some(member_store) = &routes.member_store else {
        return settings_side_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
            "GET, PUT, OPTIONS",
        );
    };
    if let Err(error) = member_store
        .replace_chat_deputies(req.chat_id, &deputy_ids)
        .await
    {
        tracing::warn!(%error, chat_id = req.chat_id, user_id = req.user_id, "failed to replace chat deputies");
        return settings_side_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to update deputies",
            "GET, PUT, OPTIONS",
        );
    }
    let deputies = list_deputy_summaries(routes, req.chat_id)
        .await
        .unwrap_or_default();
    settings_side_json_response(
        StatusCode::OK,
        &DeputyUpdateResponse { ok: true, deputies },
        "GET, PUT, OPTIONS",
    )
}

pub(crate) async fn settings_memory_get_response(
    routes: &SettingsHttpState,
    raw_query: Option<&str>,
    init_data: Option<&str>,
) -> Response {
    let (chat_id, user_id, scope) =
        match parse_settings_memory_access(routes, raw_query, init_data).await {
            Ok(access) => access,
            Err(response) => return response,
        };
    let limit = parse_memory_limit(query_values(raw_query).get("limit").map(String::as_str));
    let cards = match &routes.memory_store {
        Some(store) => match store.list_visible_cards(&scope, limit as i32).await {
            Ok(cards) => cards,
            Err(error) => {
                tracing::error!(%error, chat_id, user_id, "failed to list settings memory");
                return settings_side_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to list memory",
                    "GET, DELETE, OPTIONS",
                );
            }
        },
        None => Vec::new(),
    };
    settings_side_json_response(
        StatusCode::OK,
        &SettingsMemoryResponse {
            chat_id,
            user_id,
            count: cards.len(),
            cards,
        },
        "GET, DELETE, OPTIONS",
    )
}

pub(crate) async fn settings_memory_delete_response(
    routes: &SettingsHttpState,
    raw_query: Option<&str>,
    init_data: Option<&str>,
) -> Response {
    let (_, user_id, scope) = match parse_settings_memory_access(routes, raw_query, init_data).await
    {
        Ok(access) => access,
        Err(response) => return response,
    };
    let id = query_values(raw_query)
        .get("id")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|id| *id != 0);
    let Some(id) = id else {
        return settings_side_error_response(
            StatusCode::BAD_REQUEST,
            "id required",
            "GET, DELETE, OPTIONS",
        );
    };
    let deleted = match &routes.memory_store {
        Some(store) => match store.soft_delete_visible_card(id, user_id, &scope).await {
            Ok(deleted) => deleted,
            Err(error) => {
                tracing::error!(%error, id, user_id, "failed to soft-delete settings memory card");
                return settings_side_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to delete memory",
                    "GET, DELETE, OPTIONS",
                );
            }
        },
        None => {
            return settings_side_json_response(
                StatusCode::OK,
                &serde_json::json!({ "ok": true }),
                "GET, DELETE, OPTIONS",
            );
        }
    };
    if !deleted {
        return settings_side_error_response(
            StatusCode::NOT_FOUND,
            "memory card not found",
            "GET, DELETE, OPTIONS",
        );
    }
    settings_side_json_response(
        StatusCode::OK,
        &serde_json::json!({ "ok": true }),
        "GET, DELETE, OPTIONS",
    )
}

/// Telegram WebApp `initData` freshness window (1 hour).
const SETTINGS_INIT_DATA_MAX_AGE_SECONDS: u64 = 3_600;

const SETTINGS_INIT_DATA_HEADER: &str = "x-telegram-init-data";

pub(crate) fn settings_init_data_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(SETTINGS_INIT_DATA_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

/// Authenticate a settings WebApp request via Telegram `initData`.
///
/// `initData` is the authority for *who* the caller is; the existing per-target authorization
/// (`authorize_settings_user`, admin/deputy membership) still governs *what* they may do.
///
/// `Err(())` signals the gate should answer `401 Unauthorized`; the gate owns the response shape.
pub(crate) fn authenticate_settings_init_data(
    routes: &SettingsHttpState,
    init_data: Option<&str>,
    claimed_user_id: i64,
) -> Result<(), ()> {
    if claimed_user_id <= 0 {
        tracing::warn!(
            claimed_user_id,
            "settings request has no authenticated user ID"
        );
        return Err(());
    }
    let now_unix = current_unix_timestamp();
    let Some(init_data) = init_data else {
        tracing::warn!(
            claimed_user_id,
            "settings request rejected: initData absent"
        );
        return Err(());
    };
    let Some(user) = openplotva_web::validate_webapp_init_data(
        init_data,
        &routes.bot_token,
        SETTINGS_INIT_DATA_MAX_AGE_SECONDS,
        now_unix,
    ) else {
        tracing::warn!(claimed_user_id, "settings initData failed validation");
        return Err(());
    };
    if user.id <= 0 || user.id != claimed_user_id {
        tracing::warn!(
            caller_user_id = user.id,
            claimed_user_id,
            "settings initData caller does not match requested user_id"
        );
        return Err(());
    }
    Ok(())
}

pub(crate) fn current_unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

pub(crate) fn parse_settings_get_access(
    values: &BTreeMap<String, String>,
    admin_ids: &[i64],
) -> Result<SettingsAccess, SettingsAccessError> {
    let chat_id = values
        .get("chat_id")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|chat_id| *chat_id != 0)
        .ok_or(SettingsAccessError::InvalidChatId)?;
    let signature = values
        .get("signature")
        .filter(|value| !value.is_empty())
        .ok_or(SettingsAccessError::MissingSignature)?;
    let user_id = values
        .get("user_id")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    if !openplotva_web::validate_settings_access_signature(chat_id, user_id, signature) {
        return Err(SettingsAccessError::InvalidSignature);
    }
    Ok(SettingsAccess {
        chat_id,
        user_id,
        is_global_admin: admin_ids.contains(&user_id),
    })
}

pub(crate) fn settings_access_error_response(error: SettingsAccessError) -> Response {
    match error {
        SettingsAccessError::InvalidChatId => {
            settings_error_response(StatusCode::BAD_REQUEST, "Invalid chat_id")
        }
        SettingsAccessError::MissingSignature => {
            settings_error_response(StatusCode::BAD_REQUEST, "Missing signature")
        }
        SettingsAccessError::InvalidSignature => {
            settings_error_response(StatusCode::FORBIDDEN, "Invalid signature")
        }
    }
}

pub(crate) fn parse_deputy_update_request(
    body: &[u8],
) -> Result<DeputyUpdateRequest, Box<Response>> {
    let req = serde_json::from_slice::<DeputyUpdateRequest>(body).map_err(|_| {
        Box::new(settings_side_error_response(
            StatusCode::BAD_REQUEST,
            "Invalid JSON in request body",
            "GET, PUT, OPTIONS",
        ))
    })?;
    if req.chat_id == 0 || req.user_id == 0 {
        return Err(Box::new(settings_side_error_response(
            StatusCode::BAD_REQUEST,
            "chat_id and user_id are required",
            "GET, PUT, OPTIONS",
        )));
    }
    if req.signature.trim().is_empty() {
        return Err(Box::new(settings_side_error_response(
            StatusCode::BAD_REQUEST,
            "Missing signature",
            "GET, PUT, OPTIONS",
        )));
    }
    if !openplotva_web::validate_settings_access_signature(req.chat_id, req.user_id, &req.signature)
    {
        return Err(Box::new(settings_side_error_response(
            StatusCode::FORBIDDEN,
            "Invalid signature",
            "GET, PUT, OPTIONS",
        )));
    }
    Ok(req)
}

pub(crate) async fn parse_deputy_owner_access(
    routes: &SettingsHttpState,
    raw_query: Option<&str>,
    init_data: Option<&str>,
) -> Result<DeputyOwnerAccess, Response> {
    let values = query_values(raw_query);
    let chat_id = values
        .get("chat_id")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value != 0)
        .ok_or_else(|| {
            settings_side_error_response(
                StatusCode::BAD_REQUEST,
                "Invalid chat_id",
                "GET, PUT, OPTIONS",
            )
        })?;
    let user_id = values
        .get("user_id")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value != 0)
        .ok_or_else(|| {
            settings_side_error_response(
                StatusCode::BAD_REQUEST,
                "Invalid user_id",
                "GET, PUT, OPTIONS",
            )
        })?;
    if authenticate_settings_init_data(routes, init_data, user_id).is_err() {
        return Err(settings_side_error_response(
            StatusCode::UNAUTHORIZED,
            "Unauthorized",
            "GET, PUT, OPTIONS",
        ));
    }
    let signature = values
        .get("signature")
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            settings_side_error_response(
                StatusCode::BAD_REQUEST,
                "Missing signature",
                "GET, PUT, OPTIONS",
            )
        })?;
    if !openplotva_web::validate_settings_access_signature(chat_id, user_id, signature) {
        return Err(settings_side_error_response(
            StatusCode::FORBIDDEN,
            "Invalid signature",
            "GET, PUT, OPTIONS",
        ));
    }
    if chat_id == user_id {
        return Err(settings_side_error_response(
            StatusCode::BAD_REQUEST,
            "Deputies are available only for chats",
            "GET, PUT, OPTIONS",
        ));
    }
    ensure_settings_chat_available(routes, chat_id, user_id).await?;
    let Some(settings_store) = &routes.settings_store else {
        return Err(settings_side_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
            "GET, PUT, OPTIONS",
        ));
    };
    let chat = settings_store
        .get_chat_state(chat_id)
        .await
        .map_err(|_| {
            settings_side_error_response(
                StatusCode::NOT_FOUND,
                "Chat not found",
                "GET, PUT, OPTIONS",
            )
        })?
        .ok_or_else(|| {
            settings_side_error_response(
                StatusCode::NOT_FOUND,
                "Chat not found",
                "GET, PUT, OPTIONS",
            )
        })?;
    if !chat_allows_deputies(&chat) {
        return Err(settings_side_error_response(
            StatusCode::BAD_REQUEST,
            "Deputies are available only for chats",
            "GET, PUT, OPTIONS",
        ));
    }
    Ok(DeputyOwnerAccess {
        chat_id,
        user_id,
        is_global_admin: routes.admin_ids.contains(&user_id),
    })
}

pub(crate) async fn ensure_deputy_management_permission(
    routes: &SettingsHttpState,
    chat_id: i64,
    user_id: i64,
) -> Result<(), Response> {
    let Some(member_store) = &routes.member_store else {
        return Err(settings_side_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
            "GET, PUT, OPTIONS",
        ));
    };
    match member_store.get_chat_member(chat_id, user_id).await {
        Ok(Some(member)) if member.status == openplotva_storage::CHAT_MEMBER_STATUS_CREATOR => {
            return Ok(());
        }
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(%error, chat_id, user_id, "deputy permission check failed");
            return Err(settings_side_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Permission check failed",
                "GET, PUT, OPTIONS",
            ));
        }
    }
    if routes.telegram.is_some()
        && let Ok(Some(member)) = refresh_chat_member_for_web(routes, chat_id, user_id).await
        && member.status == openplotva_storage::CHAT_MEMBER_STATUS_CREATOR
    {
        return Ok(());
    }
    Err(settings_side_error_response(
        StatusCode::FORBIDDEN,
        "Unauthorized access",
        "GET, PUT, OPTIONS",
    ))
}

pub(crate) fn normalize_deputy_ids(values: &[i64], current_user_id: i64) -> Vec<i64> {
    let mut seen = HashSet::with_capacity(values.len());
    let mut result = Vec::with_capacity(values.len());
    for value in values {
        if *value <= 0 || *value == current_user_id || !seen.insert(*value) {
            continue;
        }
        result.push(*value);
    }
    result.sort_unstable();
    result
}

pub(crate) async fn validate_deputy_ids(
    routes: &SettingsHttpState,
    chat_id: i64,
    deputy_ids: &[i64],
) -> Result<(), String> {
    if deputy_ids.is_empty() {
        return Ok(());
    }
    let Some(member_store) = &routes.member_store else {
        return Err(format!("invalid deputy {}", deputy_ids[0]));
    };
    let members = member_store
        .list_chat_members_by_user_ids(chat_id, deputy_ids)
        .await
        .map_err(|_| format!("invalid deputy {}", deputy_ids[0]))?;
    let mut by_id = members
        .into_iter()
        .map(|member| (member.user_id, member))
        .collect::<HashMap<_, _>>();
    for deputy_id in deputy_ids {
        if routes.telegram.is_some()
            && !by_id.get(deputy_id).is_some_and(|member| {
                openplotva_storage::is_active_chat_member_status(&member.status)
            })
            && let Ok(Some(member)) = refresh_chat_member_for_web(routes, chat_id, *deputy_id).await
        {
            by_id.insert(*deputy_id, member);
        }
        let Some(member) = by_id.get(deputy_id) else {
            return Err(format!("invalid deputy {deputy_id}"));
        };
        if !openplotva_storage::is_active_chat_member_status(&member.status) {
            return Err(format!("invalid deputy {deputy_id}"));
        }
    }
    Ok(())
}

pub(crate) async fn list_deputy_summaries(
    routes: &SettingsHttpState,
    chat_id: i64,
) -> Result<Vec<SettingsDeputySummary>, String> {
    let member_store = routes
        .member_store
        .as_ref()
        .ok_or_else(|| "settings store not configured".to_owned())?;
    list_deputy_summaries_from_store(member_store, chat_id).await
}

pub(crate) async fn list_deputy_summaries_from_store(
    member_store: &PostgresChatMemberStore,
    chat_id: i64,
) -> Result<Vec<SettingsDeputySummary>, String> {
    let deputy_ids = member_store
        .list_chat_deputy_ids(chat_id)
        .await
        .map_err(|error| error.to_string())?;
    if deputy_ids.is_empty() {
        return Ok(Vec::new());
    }
    let members = member_store
        .list_chat_members_by_user_ids(chat_id, &deputy_ids)
        .await
        .map_err(|error| error.to_string())?;
    let users = member_store
        .list_user_states_by_ids(&deputy_ids)
        .await
        .map_err(|error| error.to_string())?;
    let members_by_id = members
        .into_iter()
        .map(|member| (member.user_id, member))
        .collect::<HashMap<_, _>>();
    let users_by_id = users
        .into_iter()
        .map(|user| (user.id, user))
        .collect::<HashMap<_, _>>();
    let mut deputies = Vec::with_capacity(deputy_ids.len());
    for deputy_id in deputy_ids {
        let Some(member) = members_by_id.get(&deputy_id) else {
            continue;
        };
        if !openplotva_storage::is_active_chat_member_status(&member.status) {
            continue;
        }
        let Some(user) = users_by_id.get(&deputy_id) else {
            continue;
        };
        deputies.push(SettingsDeputySummary {
            id: user.id,
            first_name: user.first_name.clone(),
            last_name: user.last_name.clone(),
            username: user.username.clone(),
            status: member.status.clone(),
            display_name: build_deputy_display_name(
                user.id,
                &user.first_name,
                user.last_name.as_deref(),
                user.username.as_deref(),
            ),
        });
    }
    deputies.sort_by(|left, right| {
        left.display_name
            .cmp(&right.display_name)
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(deputies)
}

pub(crate) fn build_deputy_display_name(
    user_id: i64,
    first_name: &str,
    last_name: Option<&str>,
    username: Option<&str>,
) -> String {
    let mut name = first_name.trim().to_owned();
    if let Some(last_name) = last_name.map(str::trim).filter(|value| !value.is_empty()) {
        if !name.is_empty() {
            name.push(' ');
        }
        name.push_str(last_name);
    }
    if !name.is_empty() {
        return name;
    }
    if let Some(username) = username.map(str::trim).filter(|value| !value.is_empty()) {
        return format!("@{username}");
    }
    format!("User {user_id}")
}

pub(crate) fn parse_deputy_candidates_limit(value: Option<&str>) -> usize {
    match value.and_then(|value| value.trim().parse::<usize>().ok()) {
        Some(limit) if limit > 100 => 100,
        Some(limit) if limit > 0 => limit,
        _ => 50,
    }
}

pub(crate) fn chat_allows_deputies(chat: &openplotva_core::ChatState) -> bool {
    !chat.chat_type.trim().is_empty() && chat.chat_type != "private"
}

pub(crate) fn parse_settings_update_request(
    body: &[u8],
) -> Result<SettingsUpdateRequest, Box<Response>> {
    let mut req = serde_json::from_slice::<SettingsUpdateRequest>(body).map_err(|error| {
        tracing::error!(%error, "invalid JSON in settings update request");
        Box::new(settings_error_response(
            StatusCode::BAD_REQUEST,
            "Invalid JSON in request body",
        ))
    })?;
    if req.chat_id == 0 {
        return Err(Box::new(settings_error_response(
            StatusCode::BAD_REQUEST,
            "Chat ID is required",
        )));
    }
    if req.signature.is_empty() {
        return Err(Box::new(settings_error_response(
            StatusCode::BAD_REQUEST,
            "Missing signature",
        )));
    }
    if !openplotva_web::validate_settings_access_signature(req.chat_id, req.user_id, &req.signature)
    {
        return Err(Box::new(settings_error_response(
            StatusCode::FORBIDDEN,
            "Invalid signature",
        )));
    }
    req.custom_persona = openplotva_web::truncate_custom_persona(req.custom_persona);
    Ok(req)
}

pub(crate) async fn ensure_settings_chat_available(
    routes: &SettingsHttpState,
    chat_id: i64,
    user_id: i64,
) -> Result<(), Response> {
    if chat_id == user_id {
        return Ok(());
    }
    let Some(settings_store) = &routes.settings_store else {
        return Err(settings_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
        ));
    };
    match settings_store.get_chat_state(chat_id).await {
        Ok(Some(_)) => ensure_settings_bot_membership(routes, chat_id).await,
        Ok(None) => {
            refresh_settings_chat_from_telegram(routes, chat_id).await?;
            ensure_settings_bot_membership(routes, chat_id).await
        }
        Err(error) => {
            tracing::warn!(%error, chat_id, "failed to validate chat availability");
            Err(settings_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to validate chat availability",
            ))
        }
    }
}

pub(crate) async fn refresh_settings_chat_from_telegram(
    routes: &SettingsHttpState,
    chat_id: i64,
) -> Result<(), Response> {
    let Some(telegram) = &routes.telegram else {
        return Err(settings_error_response(
            StatusCode::NOT_FOUND,
            "Chat not found",
        ));
    };
    let Some(state_store) = &routes.state_store else {
        return Err(settings_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to validate chat availability",
        ));
    };
    let chat = telegram
        .execute(openplotva_telegram::build_get_chat_method(chat_id))
        .await
        .map_err(|error| {
            tracing::debug!(%error, chat_id, "Telegram getChat failed for settings availability");
            settings_error_response(StatusCode::NOT_FOUND, "Chat not found")
        })?;
    let chat_state = settings_chat_state_from_full_info(chat);
    state_store
        .upsert_chat_state(&chat_state)
        .await
        .map_err(|error| {
            tracing::warn!(%error, chat_id, "failed to persist Telegram chat freshness for settings availability");
            settings_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to validate chat availability",
            )
        })
}

pub(crate) async fn ensure_settings_bot_membership(
    routes: &SettingsHttpState,
    chat_id: i64,
) -> Result<(), Response> {
    let (Some(telegram), Some(bot_id)) = (&routes.telegram, routes.bot_id) else {
        return Ok(());
    };
    let Some(member_store) = &routes.member_store else {
        return Err(settings_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to validate chat availability",
        ));
    };
    match member_store.get_chat_member(chat_id, bot_id).await {
        Ok(Some(member)) if openplotva_storage::is_active_chat_member_status(&member.status) => {
            return Ok(());
        }
        Ok(_) => {}
        Err(error) => {
            tracing::debug!(%error, chat_id, bot_id, "failed to load cached bot membership for settings availability");
        }
    }
    let member = settings::GroupSettingsMemberApi::get_chat_member(telegram, chat_id, bot_id)
        .await
        .map_err(|error| {
            tracing::warn!(%error, chat_id, bot_id, "Telegram bot membership check failed for settings availability");
            settings_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to validate chat availability",
            )
        })?;
    let upsert = settings::chat_member_upsert_from_telegram(chat_id, bot_id, &member);
    if let Err(error) = member_store.upsert_chat_member(&upsert).await {
        tracing::debug!(%error, chat_id, bot_id, "failed to persist Telegram bot membership freshness for settings availability");
    }
    let user_state = settings::user_state_from_telegram_user(member.get_user());
    if let Err(error) = member_store.upsert_user_state(&user_state).await {
        tracing::debug!(%error, chat_id, bot_id, "failed to persist Telegram bot user freshness for settings availability");
    }
    if openplotva_storage::is_active_chat_member_status(&upsert.status) {
        Ok(())
    } else {
        Err(settings_error_response(
            StatusCode::NOT_FOUND,
            "Chat not found",
        ))
    }
}

pub(crate) fn settings_chat_state_from_full_info(
    chat: openplotva_telegram::ChatFullInfo,
) -> openplotva_core::ChatState {
    openplotva_core::ChatState::new(
        chat.id,
        settings_chat_full_info_type_name(chat.chat_type),
        chat.title,
        chat.username,
        chat.first_name,
        chat.last_name,
        chat.is_forum,
    )
}

pub(crate) fn settings_chat_full_info_type_name(
    chat_type: carapax::types::ChatFullInfoType,
) -> &'static str {
    match chat_type {
        carapax::types::ChatFullInfoType::Channel => "channel",
        carapax::types::ChatFullInfoType::Group => "group",
        carapax::types::ChatFullInfoType::Private => "private",
        carapax::types::ChatFullInfoType::Supergroup => "supergroup",
    }
}

pub(crate) async fn authorize_settings_user(
    routes: &SettingsHttpState,
    access: SettingsAccess,
) -> Result<(), Response> {
    if access.user_id <= 0 {
        return Err(settings_error_response(
            StatusCode::UNAUTHORIZED,
            "Unauthorized access",
        ));
    }
    if access.is_global_admin {
        return Ok(());
    }
    match settings_user_can_edit(routes, access.chat_id, access.user_id).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(settings_error_response(
            StatusCode::FORBIDDEN,
            "Unauthorized access",
        )),
        Err(error) => {
            tracing::warn!(%error, chat_id = access.chat_id, user_id = access.user_id, "settings permission check failed");
            Err(settings_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Permission check failed",
            ))
        }
    }
}

pub(crate) async fn authorize_settings_memory_user(
    routes: &SettingsHttpState,
    access: SettingsAccess,
) -> Result<(), Response> {
    if access.user_id <= 0 {
        return Err(settings_error_response(
            StatusCode::UNAUTHORIZED,
            "Unauthorized access",
        ));
    }
    match settings_user_can_edit(routes, access.chat_id, access.user_id).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(settings_error_response(
            StatusCode::FORBIDDEN,
            "Unauthorized access",
        )),
        Err(error) => {
            tracing::warn!(%error, chat_id = access.chat_id, user_id = access.user_id, "settings memory permission check failed");
            Err(settings_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Permission check failed",
            ))
        }
    }
}

pub(crate) async fn settings_user_can_edit(
    routes: &SettingsHttpState,
    chat_id: i64,
    user_id: i64,
) -> Result<bool, String> {
    if user_id == 0 {
        return Err("user ID is required".to_owned());
    }
    if chat_id == user_id {
        return Ok(true);
    }
    let settings_store = routes
        .settings_store
        .as_ref()
        .ok_or_else(|| "settings store not configured".to_owned())?;
    let member_store = routes
        .member_store
        .as_ref()
        .ok_or_else(|| "settings store not configured".to_owned())?;
    let Some(chat) = settings_store
        .get_chat_state(chat_id)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Err("failed to get chat info".to_owned());
    };
    if chat.chat_type == "private" {
        return Ok(false);
    }
    let mut member = member_store
        .get_chat_member(chat_id, user_id)
        .await
        .map_err(|error| error.to_string())?;
    if !stored_chat_member_is_fresh(member.as_ref(), OffsetDateTime::now_utc()) {
        member = refresh_chat_member_for_web(routes, chat_id, user_id).await?;
    }
    if openplotva_storage::stored_member_can_open_group_settings(member.as_ref()) {
        return Ok(true);
    }
    let Some(member) = member.as_ref() else {
        return Ok(false);
    };
    if !openplotva_storage::is_active_chat_member_status(&member.status) {
        return Ok(false);
    }
    let deputy_ids = member_store
        .list_chat_deputy_ids(chat_id)
        .await
        .map_err(|error| error.to_string())?;
    Ok(deputy_ids.contains(&user_id))
}

pub(crate) fn stored_chat_member_is_fresh(
    member: Option<&openplotva_storage::ChatMemberRecord>,
    now: OffsetDateTime,
) -> bool {
    const MAX_AGE_SECONDS: i64 = 5 * 60;

    member
        .and_then(|member| member.telegram_observed_at)
        .is_some_and(|observed_at| {
            observed_at <= now
                && now
                    .unix_timestamp()
                    .saturating_sub(observed_at.unix_timestamp())
                    <= MAX_AGE_SECONDS
        })
}

pub(crate) async fn refresh_chat_member_for_web(
    routes: &SettingsHttpState,
    chat_id: i64,
    user_id: i64,
) -> Result<Option<openplotva_storage::ChatMemberRecord>, String> {
    let member_store = routes
        .member_store
        .as_ref()
        .ok_or_else(|| "settings store not configured".to_owned())?;
    let telegram = routes
        .telegram
        .as_ref()
        .ok_or_else(|| "Telegram membership check not configured".to_owned())?;
    if chat_id == 0 || user_id == 0 {
        return Ok(None);
    }

    match settings::GroupSettingsMemberApi::get_chat_member(telegram, chat_id, user_id).await {
        Ok(member) => {
            let upsert = settings::chat_member_upsert_from_telegram(chat_id, user_id, &member);
            if let Err(error) = member_store.upsert_chat_member(&upsert).await {
                tracing::debug!(%error, chat_id, user_id, "failed to persist Telegram member freshness for web route");
                return Ok(Some(chat_member_record_from_upsert(&upsert)));
            }
            let user_state = settings::user_state_from_telegram_user(member.get_user());
            if let Err(error) = member_store.upsert_user_state(&user_state).await {
                tracing::debug!(%error, chat_id, user_id, "failed to persist Telegram user freshness for web route");
            }
            member_store
                .get_chat_member(chat_id, user_id)
                .await
                .map(|stored| stored.or_else(|| Some(chat_member_record_from_upsert(&upsert))))
                .map_err(|error| error.to_string())
        }
        Err(error) => {
            tracing::warn!(%error, chat_id, user_id, "Telegram member freshness failed for web route");
            Err("Telegram membership check failed".to_owned())
        }
    }
}

pub(crate) fn chat_member_record_from_upsert(
    upsert: &openplotva_storage::ChatMemberUpsert,
) -> openplotva_storage::ChatMemberRecord {
    openplotva_storage::ChatMemberRecord {
        chat_id: upsert.chat_id,
        user_id: upsert.user_id,
        status: upsert.status.clone(),
        is_member: upsert.is_member,
        is_anonymous: upsert.is_anonymous,
        custom_title: upsert.custom_title.clone(),
        can_be_edited: upsert.can_be_edited,
        can_manage_chat: upsert.can_manage_chat,
        can_delete_messages: upsert.can_delete_messages,
        can_manage_video_chats: upsert.can_manage_video_chats,
        can_restrict_members: upsert.can_restrict_members,
        can_promote_members: upsert.can_promote_members,
        can_change_info: upsert.can_change_info,
        can_invite_users: upsert.can_invite_users,
        can_post_messages: upsert.can_post_messages,
        can_edit_messages: upsert.can_edit_messages,
        can_pin_messages: upsert.can_pin_messages,
        can_manage_topics: upsert.can_manage_topics,
        can_send_messages: upsert.can_send_messages,
        can_send_media_messages: upsert.can_send_media_messages,
        can_send_polls: upsert.can_send_polls,
        can_send_other_messages: upsert.can_send_other_messages,
        can_add_web_page_previews: upsert.can_add_web_page_previews,
        until_date: upsert.until_date,
        telegram_observed_at: Some(OffsetDateTime::now_utc()),
    }
}

pub(crate) async fn settings_update_chat(
    routes: &SettingsHttpState,
    chat_id: i64,
    user_id: i64,
) -> Result<openplotva_core::ChatState, Response> {
    let Some(settings_store) = &routes.settings_store else {
        return Err(settings_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
        ));
    };
    match settings_store.get_chat_state(chat_id).await {
        Ok(Some(chat)) => Ok(chat),
        Ok(None) if chat_id == user_id && user_id != 0 => {
            create_private_settings_chat(routes, chat_id).await
        }
        Ok(None) => Err(settings_error_response(
            StatusCode::NOT_FOUND,
            "Chat not found",
        )),
        Err(error) => {
            tracing::error!(%error, chat_id, "chat not found for settings update");
            Err(settings_error_response(
                StatusCode::NOT_FOUND,
                "Chat not found",
            ))
        }
    }
}

pub(crate) async fn create_private_settings_chat(
    routes: &SettingsHttpState,
    chat_id: i64,
) -> Result<openplotva_core::ChatState, Response> {
    let chat = openplotva_core::ChatState::new(chat_id, "private", None, None, None, None, None);
    if let Some(store) = &routes.state_store
        && let Err(error) = store.upsert_chat_state(&chat).await
    {
        tracing::error!(%error, chat_id, "failed to create private chat record");
        return Err(settings_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to initialize chat",
        ));
    }
    Ok(chat)
}

pub(crate) async fn settings_chat_display_for_read(
    routes: &SettingsHttpState,
    chat_id: i64,
    user_id: i64,
) -> (String, String) {
    let Some(settings_store) = &routes.settings_store else {
        return (String::new(), String::new());
    };
    match settings_store.get_chat_state(chat_id).await {
        Ok(Some(chat)) => settings_chat_display(&chat),
        Ok(None) | Err(_) if chat_id == user_id && user_id != 0 => {
            let _ = create_private_settings_chat(routes, chat_id).await;
            (String::new(), "private".to_owned())
        }
        Ok(None) | Err(_) => (String::new(), String::new()),
    }
}

pub(crate) fn settings_chat_display(chat: &openplotva_core::ChatState) -> (String, String) {
    if let Some(title) = chat.title.as_ref().filter(|value| !value.is_empty()) {
        return (title.clone(), chat.chat_type.clone());
    }
    let private_name = settings_chat_name(chat.first_name.as_deref(), chat.last_name.as_deref());
    if !private_name.is_empty() {
        return (private_name, chat.chat_type.clone());
    }
    if let Some(username) = chat.username.as_ref().filter(|value| !value.is_empty()) {
        return (format!("@{username}"), chat.chat_type.clone());
    }
    (String::new(), chat.chat_type.clone())
}

pub(crate) fn settings_chat_name(first_name: Option<&str>, last_name: Option<&str>) -> String {
    let Some(first_name) = first_name else {
        return String::new();
    };
    let mut title = first_name.to_owned();
    if let Some(last_name) = last_name.filter(|value| !value.is_empty()) {
        title.push(' ');
        title.push_str(last_name);
    }
    title
}

pub(crate) fn new_settings_response(
    settings: &openplotva_core::ChatSettings,
    chat_title: String,
    chat_type: String,
) -> SettingsResponseBody {
    SettingsResponseBody {
        chat_id: settings.chat_id,
        chat_title,
        chat_type,
        mood_alignment: settings.mood_alignment.clone(),
        custom_persona: settings.custom_persona.clone(),
        reactivity_percentage: settings.reactivity_percentage,
        proactivity_percentage: settings.proactivity_percentage,
        enable_obscenifier: settings.enable_obscenifier,
        enable_profanity: settings.enable_profanity,
        enable_greet_joiners: settings.enable_greet_joiners,
        enable_global_text_reply: settings.enable_global_text_reply,
        enable_global_draw_reply: settings.enable_global_draw_reply,
        enable_daily_game: settings.enable_daily_game.unwrap_or(true),
        daily_game_theme: chat_setting_daily_game_theme(settings.daily_game_theme.as_deref()),
        greeting_html: settings.greeting_html.clone(),
        is_vip: false,
        disable_random_reactivity: false,
        hide_original_draw_prompt: false,
        is_deputy: false,
        can_manage_deputies: false,
        deputies: Vec::new(),
    }
}

pub(crate) fn chat_setting_daily_game_theme(theme: Option<&str>) -> String {
    theme
        .filter(|value| !value.is_empty())
        .unwrap_or("auto")
        .to_owned()
}

pub(crate) fn apply_private_chat_response_defaults(
    response: &mut SettingsResponseBody,
    chat_id: i64,
    user_id: i64,
) {
    if chat_id == user_id && user_id != 0 {
        response.enable_global_text_reply = true;
        response.enable_global_draw_reply = true;
    }
}

pub(crate) async fn apply_user_settings_response(
    routes: &SettingsHttpState,
    response: &mut SettingsResponseBody,
    user_id: i64,
) {
    if user_id != 0 {
        response.is_vip = settings_user_is_vip(routes, user_id).await;
        if let Some(settings_store) = &routes.settings_store
            && let Ok(Some(user_settings)) = settings_store.get_user_settings(user_id).await
        {
            response.disable_random_reactivity = user_settings.disable_random_reactivity;
            if response.is_vip {
                response.hide_original_draw_prompt = user_settings.hide_original_draw_prompt;
            }
        }
    }
    if !response.is_vip {
        response.hide_original_draw_prompt = false;
    }
}

pub(crate) async fn settings_user_is_vip(routes: &SettingsHttpState, user_id: i64) -> bool {
    if user_id <= 0 {
        return false;
    }
    if let Some(vip_status) = &routes.vip_status {
        return vip_status
            .is_vip_at(user_id, OffsetDateTime::now_utc())
            .await;
    }
    let Some(vip_store) = &routes.vip_store else {
        return false;
    };
    if let Ok(Some(summary)) = vip_store.get_vip_summary_by_user(user_id).await
        && summary.is_active
        && OffsetDateTime::now_utc() < summary.effective_expires_at
    {
        return true;
    }
    vip_store
        .get_vip_cache(user_id)
        .await
        .ok()
        .flatten()
        .is_some_and(|cache| cache.is_vip && OffsetDateTime::now_utc() < cache.expires_at)
}

pub(crate) async fn apply_deputy_settings_response(
    member_store: &PostgresChatMemberStore,
    response: &mut SettingsResponseBody,
    chat_id: i64,
    user_id: i64,
    chat_type: &str,
    is_global_admin: bool,
) {
    if chat_id == user_id || chat_type == "private" {
        return;
    }
    let deputy_ids = member_store
        .list_chat_deputy_ids(chat_id)
        .await
        .unwrap_or_default();
    let member = member_store
        .get_chat_member(chat_id, user_id)
        .await
        .ok()
        .flatten();
    response.is_deputy = member
        .as_ref()
        .is_some_and(|member| openplotva_storage::is_active_chat_member_status(&member.status))
        && deputy_ids.contains(&user_id);
    response.can_manage_deputies = is_global_admin
        || member
            .as_ref()
            .is_some_and(|member| member.status == openplotva_storage::CHAT_MEMBER_STATUS_CREATOR);
    if response.can_manage_deputies {
        response.deputies = list_deputy_summaries_from_store(member_store, chat_id)
            .await
            .unwrap_or_default();
    }
}

pub(crate) async fn settings_update_from_request(
    settings_store: &PostgresChatSettingsStore,
    req: &SettingsUpdateRequest,
    chat: &openplotva_core::ChatState,
) -> openplotva_core::ChatSettingsUpdate {
    let (enable_global_text_reply, enable_global_draw_reply) =
        settings_reply_flags(&chat.chat_type, req);
    let (enable_daily_game, daily_game_theme) =
        daily_game_update(settings_store, req, is_group_chat_type(&chat.chat_type)).await;
    openplotva_core::ChatSettingsUpdate {
        chat_id: req.chat_id,
        chat_type: chat.chat_type.clone(),
        mood_alignment: req.mood_alignment.clone(),
        custom_persona: req.custom_persona.clone(),
        reactivity_percentage: req.reactivity_percentage.unwrap_or(50),
        proactivity_percentage: req.proactivity_percentage.unwrap_or(50),
        enable_global_text_reply,
        enable_global_draw_reply,
        enable_obscenifier: req.enable_obscenifier.unwrap_or(true),
        enable_profanity: req.enable_profanity.unwrap_or(true),
        enable_greet_joiners: req.enable_greet_joiners.unwrap_or(false),
        enable_daily_game,
        daily_game_theme: chat_setting_daily_game_theme(daily_game_theme.as_deref()),
        greeting_html: req.greeting_html.clone(),
    }
}

pub(crate) fn settings_reply_flags(chat_type: &str, req: &SettingsUpdateRequest) -> (bool, bool) {
    let mut enable_global_text_reply = req.enable_global_text_reply.unwrap_or(true);
    let mut enable_global_draw_reply = req.enable_global_draw_reply.unwrap_or(true);
    if chat_type == "private" {
        enable_global_text_reply = true;
        enable_global_draw_reply = true;
    }
    (enable_global_text_reply, enable_global_draw_reply)
}

pub(crate) async fn daily_game_update(
    settings_store: &PostgresChatSettingsStore,
    req: &SettingsUpdateRequest,
    is_group: bool,
) -> (bool, Option<String>) {
    let current = settings_store
        .get_chat_settings(req.chat_id)
        .await
        .ok()
        .flatten();
    let mut daily_enabled = current
        .as_ref()
        .and_then(|settings| settings.enable_daily_game)
        .unwrap_or(true);
    let mut daily_theme = current.and_then(|settings| settings.daily_game_theme);
    if is_group {
        if let Some(enabled) = req.enable_daily_game {
            daily_enabled = enabled;
        }
        if req.daily_game_theme.is_some() {
            daily_theme = req.daily_game_theme.clone();
        }
    }
    (daily_enabled, daily_theme)
}

pub(crate) fn is_group_chat_type(chat_type: &str) -> bool {
    chat_type == "group" || chat_type == "supergroup"
}

pub(crate) async fn update_user_settings_from_request(
    routes: &SettingsHttpState,
    req: &SettingsUpdateRequest,
) {
    if req.user_id == 0
        || (req.disable_random_reactivity.is_none() && req.hide_original_draw_prompt.is_none())
    {
        return;
    }
    let Some(settings_store) = &routes.settings_store else {
        return;
    };
    let current = settings_store
        .get_user_settings(req.user_id)
        .await
        .ok()
        .flatten();
    let disable_random_reactivity = req.disable_random_reactivity.unwrap_or_else(|| {
        current
            .as_ref()
            .is_some_and(|settings| settings.disable_random_reactivity)
    });
    let mut hide_original_draw_prompt = current
        .as_ref()
        .is_some_and(|settings| settings.hide_original_draw_prompt);
    if settings_user_is_vip(routes, req.user_id).await {
        if let Some(hide) = req.hide_original_draw_prompt {
            hide_original_draw_prompt = hide;
        }
    } else {
        hide_original_draw_prompt = false;
    }
    if let Err(error) = settings_store
        .upsert_user_settings(
            req.user_id,
            disable_random_reactivity,
            hide_original_draw_prompt,
        )
        .await
    {
        tracing::warn!(%error, user_id = req.user_id, "failed to update user settings");
    }
}

pub(crate) async fn managed_chat_list_items(
    routes: &SettingsHttpState,
    settings_store: &PostgresChatSettingsStore,
    member_store: &PostgresChatMemberStore,
    user_id: i64,
) -> Vec<ChatListItem> {
    let chats = match settings_store.list_user_chats(user_id).await {
        Ok(chats) => chats,
        Err(error) => {
            tracing::debug!(%error, user_id, "failed to list user chats");
            return Vec::new();
        }
    };
    let memberships = member_store.list_user_chat_memberships(user_id).await;
    let membership_fallback = memberships.is_err();
    let members_by_chat_id = memberships
        .unwrap_or_default()
        .into_iter()
        .map(|member| (member.chat_id, member))
        .collect::<HashMap<_, _>>();
    let deputy_chats = member_store.list_user_deputy_chat_ids(user_id).await;
    let deputy_fallback = deputy_chats.is_err();
    let deputy_chat_ids = deputy_chats
        .unwrap_or_default()
        .into_iter()
        .collect::<HashSet<_>>();

    let mut result = Vec::with_capacity(chats.len());
    for chat in chats {
        let mut member = if membership_fallback {
            member_store
                .get_chat_member(chat.id, user_id)
                .await
                .ok()
                .flatten()
        } else {
            members_by_chat_id.get(&chat.id).cloned()
        };
        if routes.telegram.is_some()
            && (!stored_chat_member_is_fresh(member.as_ref(), OffsetDateTime::now_utc())
                || !member
                    .as_ref()
                    .is_some_and(|member| chat_member_can_manage_settings(member, false)))
            && let Ok(fresh) = refresh_chat_member_for_web(routes, chat.id, user_id).await
        {
            member = fresh;
        }
        let Some(member) = member else {
            continue;
        };
        let is_deputy = user_is_deputy_for_managed_chat(
            member_store,
            user_id,
            chat.id,
            &member,
            &deputy_chat_ids,
            deputy_fallback,
        )
        .await;
        if !chat_member_can_manage_settings(&member, is_deputy) {
            continue;
        }
        result.push(ChatListItem {
            id: chat.id,
            title: chat_list_title(&chat),
            chat_type: chat.chat_type,
        });
    }
    result
}

pub(crate) async fn user_is_deputy_for_managed_chat(
    member_store: &PostgresChatMemberStore,
    user_id: i64,
    chat_id: i64,
    member: &openplotva_storage::ChatMemberRecord,
    deputy_chat_ids: &HashSet<i64>,
    fallback: bool,
) -> bool {
    if chat_member_can_manage_settings(member, false)
        || !openplotva_storage::is_active_chat_member_status(&member.status)
    {
        return false;
    }
    if !fallback {
        return deputy_chat_ids.contains(&chat_id);
    }
    member_store
        .list_chat_deputy_ids(chat_id)
        .await
        .is_ok_and(|ids| ids.contains(&user_id))
}

pub(crate) fn chat_member_can_manage_settings(
    member: &openplotva_storage::ChatMemberRecord,
    is_deputy: bool,
) -> bool {
    member.status == openplotva_storage::CHAT_MEMBER_STATUS_CREATOR
        || is_deputy
        || member.status == openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR
            && member.can_promote_members == Some(true)
}

pub(crate) fn chat_list_title(chat: &openplotva_core::ChatState) -> String {
    let (title, _) = settings_chat_display(chat);
    if title.is_empty() {
        format!("Chat {}", chat.id)
    } else {
        title
    }
}

pub(crate) async fn parse_settings_memory_access(
    routes: &SettingsHttpState,
    raw_query: Option<&str>,
    init_data: Option<&str>,
) -> Result<(i64, i64, openplotva_memory::RetrievalScope), Response> {
    let values = query_values(raw_query);
    let chat_id = values
        .get("chat_id")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value != 0)
        .ok_or_else(|| {
            settings_side_error_response(
                StatusCode::BAD_REQUEST,
                "invalid chat_id",
                "GET, DELETE, OPTIONS",
            )
        })?;
    let signature = values
        .get("signature")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            settings_side_error_response(
                StatusCode::BAD_REQUEST,
                "missing signature",
                "GET, DELETE, OPTIONS",
            )
        })?;
    let user_id = values
        .get("user_id")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    if authenticate_settings_init_data(routes, init_data, user_id).is_err() {
        return Err(settings_side_error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "GET, DELETE, OPTIONS",
        ));
    }
    if !openplotva_web::validate_settings_access_signature(chat_id, user_id, signature) {
        return Err(settings_side_error_response(
            StatusCode::FORBIDDEN,
            "invalid signature",
            "GET, DELETE, OPTIONS",
        ));
    }
    if let Err(error) = authorize_settings_memory_user(
        routes,
        SettingsAccess {
            chat_id,
            user_id,
            is_global_admin: false,
        },
    )
    .await
    {
        return Err(match error.status() {
            StatusCode::FORBIDDEN => settings_side_error_response(
                StatusCode::FORBIDDEN,
                "unauthorized access",
                "GET, DELETE, OPTIONS",
            ),
            _ => settings_side_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "permission check failed",
                "GET, DELETE, OPTIONS",
            ),
        });
    }
    let thread_id = values
        .get("thread_id")
        .map(String::as_str)
        .map(parse_optional_i32)
        .unwrap_or(0);
    let mut scope = openplotva_memory::RetrievalScope {
        chat_id,
        thread_id,
        user_id,
        ..openplotva_memory::RetrievalScope::default()
    };
    if let Some(settings_store) = &routes.settings_store
        && let Ok(Some(meta)) = settings_store.get_dialog_memory_chat_meta(chat_id).await
    {
        scope.chat_type = meta.chat_type;
        scope.username = meta.username;
        scope.active_usernames = meta.active_usernames;
    }
    Ok((chat_id, user_id, scope))
}

pub(crate) fn parse_optional_i32(value: &str) -> i32 {
    value
        .trim()
        .parse::<i32>()
        .ok()
        .filter(|value| *value >= 0)
        .unwrap_or(0)
}

pub(crate) fn settings_options_response() -> Response {
    let mut response = StatusCode::OK.into_response();
    add_settings_api_headers(response.headers_mut());
    response
}

pub(crate) fn settings_error_response(status: StatusCode, error: &str) -> Response {
    settings_json_raw_response(status, &format!(r#"{{"error": "{error}"}}"#))
}

pub(crate) fn settings_json_response<T: Serialize>(status: StatusCode, value: &T) -> Response {
    let body = match serde_json::to_string(value) {
        Ok(body) => body,
        Err(error) => {
            tracing::error!(%error, "failed to encode settings response");
            r#"{"error": "Failed to encode response"}"#.to_owned()
        }
    };
    settings_json_raw_response(status, &body)
}

pub(crate) fn settings_json_raw_response(status: StatusCode, body: &str) -> Response {
    let mut response = (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        format!("{body}\n"),
    )
        .into_response();
    add_settings_api_headers(response.headers_mut());
    response
}

pub(crate) fn settings_side_options_response(methods: &'static str) -> Response {
    let mut response = StatusCode::OK.into_response();
    add_settings_side_api_headers(response.headers_mut(), methods);
    response
}

pub(crate) fn settings_side_error_response(
    status: StatusCode,
    error: &str,
    methods: &'static str,
) -> Response {
    settings_side_json_raw_response(
        status,
        &serde_json::json!({ "error": error }).to_string(),
        methods,
    )
}

pub(crate) fn settings_side_json_response<T: Serialize>(
    status: StatusCode,
    value: &T,
    methods: &'static str,
) -> Response {
    let body = serde_json::to_string(value).unwrap_or_else(|error| {
        tracing::error!(%error, "failed to encode settings side response");
        r#"{"error":"Failed to encode response"}"#.to_owned()
    });
    settings_side_json_raw_response(status, &body, methods)
}

pub(crate) fn settings_side_json_raw_response(
    status: StatusCode,
    body: &str,
    methods: &'static str,
) -> Response {
    let mut response = (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        format!("{body}\n"),
    )
        .into_response();
    add_settings_side_api_headers(response.headers_mut(), methods);
    response
}

pub(crate) fn add_settings_api_headers(headers: &mut HeaderMap) {
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, PUT, OPTIONS"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Content-Type, Authorization"),
    );
}

pub(crate) fn add_settings_side_api_headers(headers: &mut HeaderMap, methods: &'static str) {
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static(methods),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Content-Type, Authorization"),
    );
}
