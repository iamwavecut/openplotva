//! HTTP server surfaces for OpenPlotva.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use axum::{Json, Router, extract::State, http::StatusCode, routing::get};
pub use openplotva_core::{
    ChatSettings, ChatSettingsUpdate, MessageIdMapping, PendingEditPayload, PendingOp,
    ReadyPendingOp, pending_edit_payload,
};
use serde::Serialize;

/// Health response returned by `/api/health`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HealthResponse {
    /// Service status.
    pub status: &'static str,
    /// Service name.
    pub service: &'static str,
}

/// Readiness response returned by `/api/ready`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReadinessResponse {
    /// Overall readiness status.
    pub status: &'static str,
    /// Service name.
    pub service: &'static str,
    /// Dependency checks captured during startup.
    pub checks: Vec<ReadinessCheck>,
}

/// Individual readiness check.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReadinessCheck {
    /// Dependency or invariant name.
    pub name: String,
    /// Check status.
    pub status: &'static str,
    /// Human-readable detail.
    pub detail: String,
}

/// Go Redis key prefix for persisted rate-limited chat expiry timestamps.
pub const RATE_LIMITED_CHAT_KEY_PREFIX: &str = "plotva:rate_limited_chat:";

/// Go permission action for sending text messages.
pub const ACTION_SEND_TEXT: &str = "send_text";
/// Go permission action for sending images.
pub const ACTION_SEND_IMAGE: &str = "send_image";
/// Go permission action for sending stickers.
pub const ACTION_SEND_STICKER: &str = "send_sticker";
/// Go permission action for sending videos.
pub const ACTION_SEND_VIDEO: &str = "send_video";
/// Go permission action for sending audio.
pub const ACTION_SEND_AUDIO: &str = "send_audio";
/// Go permission action for sending files.
pub const ACTION_SEND_FILE: &str = "send_file";
/// Go permission action for sending polls.
pub const ACTION_SEND_POLL: &str = "send_poll";
/// Go permission action for generic message sends.
pub const ACTION_SEND_MESSAGE: &str = "send_message";
/// Go permission action for pinning messages.
pub const ACTION_PIN_MESSAGE: &str = "pin_message";
/// Go permission action for editing messages.
pub const ACTION_EDIT_MESSAGE: &str = "edit_message";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingOpMappingError {
    /// Batch lookup failed.
    BatchLookup,
    /// Single virtual-message lookup failed.
    SingleLookup,
}

/// Store boundary used by Go pending-op mapping recovery.
pub trait PendingOpMappingStore {
    /// Batch-load virtual-message mappings.
    fn list_mappings_by_virtual_ids(
        &mut self,
        vmsg_ids: &[String],
    ) -> Result<Vec<MessageIdMapping>, PendingOpMappingError>;

    /// Load one virtual-message mapping.
    fn get_mapping_by_virtual(
        &mut self,
        vmsg_id: &str,
    ) -> Result<Option<MessageIdMapping>, PendingOpMappingError>;
}

/// Return unique non-empty virtual IDs from pending ops, preserving Go row order.
pub fn pending_op_virtual_ids(rows: &[PendingOp]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut ids = Vec::new();
    for row in rows {
        if row.vmsg_id.is_empty() || !seen.insert(row.vmsg_id.clone()) {
            continue;
        }
        ids.push(row.vmsg_id.clone());
    }
    ids
}

/// Index mappings by virtual ID, skipping empty IDs like Go.
pub fn index_mappings_by_virtual_id(
    mappings: &[MessageIdMapping],
) -> HashMap<String, MessageIdMapping> {
    let mut index = HashMap::with_capacity(mappings.len());
    for mapping in mappings {
        if mapping.vmsg_id.is_empty() {
            continue;
        }
        index.insert(mapping.vmsg_id.clone(), mapping.clone());
    }
    index
}

/// Load mappings for pending ops using Go's batch-first, single-lookup fallback.
pub fn load_pending_op_mappings(
    store: &mut impl PendingOpMappingStore,
    rows: &[PendingOp],
) -> HashMap<String, MessageIdMapping> {
    let ids = pending_op_virtual_ids(rows);
    if ids.is_empty() {
        return HashMap::new();
    }

    match store.list_mappings_by_virtual_ids(&ids) {
        Ok(mappings) => index_mappings_by_virtual_id(&mappings),
        Err(_) => {
            let mut index = HashMap::with_capacity(ids.len());
            for id in ids {
                if let Ok(Some(mapping)) = store.get_mapping_by_virtual(&id)
                    && !mapping.vmsg_id.is_empty()
                {
                    index.insert(mapping.vmsg_id.clone(), mapping);
                }
            }
            index
        }
    }
}

/// Return pending ops whose virtual message has a resolved real Telegram message ID.
pub fn pending_ops_ready_for_execution(
    rows: &[PendingOp],
    mappings: &HashMap<String, MessageIdMapping>,
) -> Vec<ReadyPendingOp> {
    rows.iter()
        .filter_map(|row| {
            let mapping = mappings.get(&row.vmsg_id)?;
            let real_message_id = mapping.real_message_id?;
            Some(ReadyPendingOp {
                id: row.id,
                vmsg_id: row.vmsg_id.clone(),
                chat_id: row.chat_id,
                op: row.op.clone(),
                payload: row.payload.clone(),
                real_message_id,
            })
        })
        .collect()
}

#[derive(Clone, Debug)]
struct AppState {
    readiness: ReadinessResponse,
}

/// Build the HTTP router for the current app shell.
pub fn router() -> Router {
    router_with_readiness(ReadinessResponse::ready(Vec::new()))
}

/// Build the HTTP router with a startup readiness snapshot.
pub fn router_with_readiness(readiness: ReadinessResponse) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/ready", get(ready))
        .with_state(Arc::new(AppState { readiness }))
}

/// Build the health payload without transport details.
pub fn health_response() -> HealthResponse {
    HealthResponse {
        status: "ok",
        service: openplotva_core::PROJECT_NAME,
    }
}

/// Build the in-memory Go rate-limit cache key for a chat.
pub fn rate_limit_key(chat_id: i64) -> String {
    format!("rate_limit:{chat_id}")
}

/// Build the persisted Go rate-limited-chat key for a chat.
pub fn rate_limited_chat_key(chat_id: i64) -> String {
    format!("{RATE_LIMITED_CHAT_KEY_PREFIX}{chat_id}")
}

/// Build the Go permission cache key for a chat/action pair.
pub fn permission_cache_key(chat_id: i64, action: &str) -> String {
    format!("perm_check:{chat_id}:{action}")
}

/// Build the Go VIP cache key for a user.
pub fn vip_cache_key(user_id: i64) -> String {
    format!("vip:{user_id}")
}

/// Decide whether a Telegram action is allowed by Go's chat settings policy.
pub fn can_perform_action(chat_type: &str, settings: Option<&ChatSettings>, action: &str) -> bool {
    if chat_type == "private" {
        return true;
    }
    let Some(settings) = settings else {
        return true;
    };

    match action {
        ACTION_SEND_TEXT | ACTION_EDIT_MESSAGE | ACTION_SEND_MESSAGE => {
            settings.enable_global_text_reply
        }
        ACTION_SEND_IMAGE | ACTION_SEND_STICKER | ACTION_SEND_AUDIO => {
            settings.enable_global_draw_reply
        }
        _ => false,
    }
}

/// Return the chat type Go records when a permission error is classified.
pub fn permission_error_chat_type(chat_type: Option<&str>) -> String {
    match chat_type
        .map(str::trim)
        .filter(|chat_type| !chat_type.is_empty())
    {
        Some(chat_type) => chat_type.to_owned(),
        None => "private".to_owned(),
    }
}

/// Build Go's chat settings update after a Telegram permission send error.
pub fn permission_error_settings_update(
    settings: &ChatSettings,
    chat_type: &str,
    is_media_permission_error: bool,
) -> ChatSettingsUpdate {
    let enable_daily_game = settings.enable_daily_game.unwrap_or(true);
    let daily_game_theme = settings
        .daily_game_theme
        .as_deref()
        .filter(|theme| !theme.is_empty())
        .unwrap_or("auto")
        .to_owned();
    let mut update = ChatSettingsUpdate {
        chat_id: settings.chat_id,
        chat_type: chat_type.to_owned(),
        mood_alignment: settings.mood_alignment.clone(),
        custom_persona: settings.custom_persona.clone(),
        reactivity_percentage: settings.reactivity_percentage,
        proactivity_percentage: settings.proactivity_percentage,
        enable_global_text_reply: settings.enable_global_text_reply,
        enable_global_draw_reply: settings.enable_global_draw_reply,
        enable_obscenifier: settings.enable_obscenifier,
        enable_profanity: settings.enable_profanity,
        enable_greet_joiners: settings.enable_greet_joiners,
        enable_daily_game,
        daily_game_theme,
        greeting_html: None,
    };

    if is_media_permission_error {
        update.enable_global_draw_reply = false;
    } else {
        update.enable_global_text_reply = false;
    }
    update
}

impl ReadinessResponse {
    /// Build a ready response from startup checks.
    pub fn ready(checks: Vec<ReadinessCheck>) -> Self {
        Self {
            status: "ok",
            service: openplotva_core::PROJECT_NAME,
            checks,
        }
    }
}

impl ReadinessCheck {
    /// Build an OK readiness check.
    pub fn ok(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: "ok",
            detail: detail.into(),
        }
    }

    /// Build a skipped readiness check.
    pub fn skipped(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: "skipped",
            detail: detail.into(),
        }
    }
}

async fn health() -> Json<HealthResponse> {
    Json(health_response())
}

async fn ready(State(state): State<Arc<AppState>>) -> (StatusCode, Json<ReadinessResponse>) {
    (StatusCode::OK, Json(state.readiness.clone()))
}

#[cfg(test)]
mod tests {
    use super::{
        ACTION_EDIT_MESSAGE, ACTION_SEND_STICKER, ACTION_SEND_TEXT, ACTION_SEND_VIDEO,
        HealthResponse, MessageIdMapping, PendingEditPayload, PendingOp, PendingOpMappingError,
        PendingOpMappingStore, ReadinessCheck, ReadinessResponse, can_perform_action,
        health_response, index_mappings_by_virtual_id, load_pending_op_mappings,
        pending_edit_payload, pending_op_virtual_ids, pending_ops_ready_for_execution,
        permission_cache_key, permission_error_chat_type, permission_error_settings_update,
        rate_limit_key, rate_limited_chat_key, vip_cache_key,
    };

    #[test]
    fn health_payload_names_the_service() {
        assert_eq!(
            health_response(),
            HealthResponse {
                status: "ok",
                service: "openplotva"
            }
        );
    }

    #[test]
    fn readiness_payload_names_startup_checks() {
        let response = ReadinessResponse::ready(vec![
            ReadinessCheck::ok("reference_snapshot", "verified"),
            ReadinessCheck::skipped("postgres", "disabled"),
        ]);

        assert_eq!(response.status, "ok");
        assert_eq!(response.service, "openplotva");
        assert_eq!(response.checks.len(), 2);
    }

    #[test]
    fn rate_limit_keys_match_go_policy_cache_keys() {
        assert_eq!(rate_limit_key(42), "rate_limit:42");
        assert_eq!(rate_limited_chat_key(42), "plotva:rate_limited_chat:42");
    }

    #[test]
    fn permission_and_vip_keys_match_go_policy_cache_keys() {
        assert_eq!(
            permission_cache_key(42, ACTION_SEND_TEXT),
            "perm_check:42:send_text"
        );
        assert_eq!(vip_cache_key(42), "vip:42");
    }

    #[test]
    fn can_perform_action_matches_go_chat_settings_policy() {
        let disabled = openplotva_core::ChatSettings {
            chat_id: 42,
            enable_global_text_reply: false,
            enable_global_draw_reply: false,
            ..openplotva_core::ChatSettings::defaults(42)
        };
        let draw_only = openplotva_core::ChatSettings {
            chat_id: 42,
            enable_global_text_reply: false,
            enable_global_draw_reply: true,
            ..openplotva_core::ChatSettings::defaults(42)
        };

        assert!(can_perform_action(
            "private",
            Some(&disabled),
            ACTION_SEND_TEXT
        ));
        assert!(can_perform_action("supergroup", None, ACTION_SEND_TEXT));
        assert!(!can_perform_action(
            "supergroup",
            Some(&disabled),
            ACTION_SEND_TEXT
        ));
        assert!(!can_perform_action(
            "supergroup",
            Some(&draw_only),
            ACTION_EDIT_MESSAGE
        ));
        assert!(can_perform_action(
            "supergroup",
            Some(&draw_only),
            ACTION_SEND_STICKER
        ));
        assert!(!can_perform_action(
            "supergroup",
            Some(&draw_only),
            ACTION_SEND_VIDEO
        ));
    }

    #[test]
    fn permission_error_settings_update_disables_expected_reply_mode_like_go() {
        let mut settings = openplotva_core::ChatSettings::defaults(42);
        settings.enable_daily_game = None;
        settings.daily_game_theme = None;

        let text_update = permission_error_settings_update(&settings, "supergroup", false);
        let media_update = permission_error_settings_update(&settings, "supergroup", true);

        assert!(!text_update.enable_global_text_reply);
        assert!(text_update.enable_global_draw_reply);
        assert!(media_update.enable_global_text_reply);
        assert!(!media_update.enable_global_draw_reply);
        assert!(text_update.enable_daily_game);
        assert_eq!(text_update.daily_game_theme, "auto");
        assert_eq!(text_update.chat_type, "supergroup");
    }

    #[test]
    fn permission_error_chat_type_uses_known_chat_info_like_go() {
        assert_eq!(permission_error_chat_type(None), "private");
        assert_eq!(permission_error_chat_type(Some("  ")), "private");
        assert_eq!(
            permission_error_chat_type(Some(" supergroup ")),
            "supergroup"
        );
    }

    #[derive(Default)]
    struct PendingOpMappingStoreStub {
        batch_calls: usize,
        batch_ids: Vec<String>,
        batch_rows: Vec<MessageIdMapping>,
        batch_error: bool,
        single_calls: Vec<String>,
        single_rows: Vec<MessageIdMapping>,
    }

    impl PendingOpMappingStore for PendingOpMappingStoreStub {
        fn list_mappings_by_virtual_ids(
            &mut self,
            vmsg_ids: &[String],
        ) -> Result<Vec<MessageIdMapping>, PendingOpMappingError> {
            self.batch_calls += 1;
            self.batch_ids = vmsg_ids.to_vec();
            if self.batch_error {
                return Err(PendingOpMappingError::BatchLookup);
            }
            Ok(self.batch_rows.clone())
        }

        fn get_mapping_by_virtual(
            &mut self,
            vmsg_id: &str,
        ) -> Result<Option<MessageIdMapping>, PendingOpMappingError> {
            self.single_calls.push(vmsg_id.to_owned());
            Ok(self
                .single_rows
                .iter()
                .find(|mapping| mapping.vmsg_id == vmsg_id)
                .cloned())
        }
    }

    #[test]
    fn pending_op_virtual_ids_batch_unique_non_empty_values_like_go() {
        let rows = vec![
            PendingOp::new(1, "v1", 42, "edit"),
            PendingOp::new(2, "v2", 42, "delete"),
            PendingOp::new(3, "v1", 42, "edit"),
            PendingOp::new(4, "", 42, "delete"),
        ];

        assert_eq!(pending_op_virtual_ids(&rows), vec!["v1", "v2"]);
    }

    #[test]
    fn load_pending_op_mappings_batches_unique_virtual_ids() {
        let mut store = PendingOpMappingStoreStub {
            batch_rows: vec![
                MessageIdMapping::unresolved("v1", 42),
                MessageIdMapping::unresolved("v2", 42),
            ],
            ..PendingOpMappingStoreStub::default()
        };
        let rows = vec![
            PendingOp::new(1, "v1", 42, "edit"),
            PendingOp::new(2, "v2", 42, "delete"),
            PendingOp::new(3, "v1", 42, "edit"),
            PendingOp::new(4, "", 42, "delete"),
        ];

        let index = load_pending_op_mappings(&mut store, &rows);

        assert_eq!(store.batch_calls, 1);
        assert_eq!(store.batch_ids, vec!["v1", "v2"]);
        assert!(store.single_calls.is_empty());
        assert!(index.contains_key("v1"));
        assert!(index.contains_key("v2"));
    }

    #[test]
    fn load_pending_op_mappings_falls_back_to_single_lookups() {
        let mut store = PendingOpMappingStoreStub {
            batch_error: true,
            single_rows: vec![MessageIdMapping::unresolved("v1", 42)],
            ..PendingOpMappingStoreStub::default()
        };
        let rows = vec![
            PendingOp::new(1, "v1", 42, "edit"),
            PendingOp::new(2, "v2", 42, "delete"),
        ];

        let index = load_pending_op_mappings(&mut store, &rows);

        assert_eq!(store.batch_calls, 1);
        assert_eq!(store.single_calls, vec!["v1", "v2"]);
        assert!(index.contains_key("v1"));
        assert!(!index.contains_key("v2"));
    }

    #[test]
    fn pending_ops_ready_for_execution_skips_unresolved_mappings_like_go() {
        let rows = vec![
            PendingOp::new(1, "v1", 42, "edit"),
            PendingOp::new(2, "v2", 42, "delete"),
            PendingOp::new(3, "missing", 42, "delete"),
        ];
        let mappings = index_mappings_by_virtual_id(&[
            MessageIdMapping::resolved("v1", 42, 77),
            MessageIdMapping::unresolved("v2", 42),
            MessageIdMapping::unresolved("", 42),
        ]);

        let ready = pending_ops_ready_for_execution(&rows, &mappings);

        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].id, 1);
        assert_eq!(ready[0].real_message_id, 77);
    }

    #[test]
    fn pending_edit_payload_decodes_text_and_parse_mode_like_go() {
        let payload = br#"{"text":"<b>edited</b>","parse_mode":"HTML"}"#;

        assert_eq!(
            pending_edit_payload(payload),
            PendingEditPayload {
                text: "<b>edited</b>".to_owned(),
                parse_mode: "HTML".to_owned(),
            }
        );
        assert_eq!(
            pending_edit_payload(b"not-json"),
            PendingEditPayload::default()
        );
    }
}
