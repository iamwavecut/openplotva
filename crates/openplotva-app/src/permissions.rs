//! App-level Telegram chat permission policy matching Go server behavior.

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Mutex, MutexGuard},
};

use openplotva_core::{ChatSettings, ChatSettingsUpdate};
use openplotva_server::{
    ACTION_EDIT_MESSAGE, ACTION_SEND_AUDIO, ACTION_SEND_IMAGE, ACTION_SEND_STICKER,
    ACTION_SEND_TEXT, can_perform_action, permission_cache_key, permission_error_chat_type,
    permission_error_settings_update,
};
use openplotva_telegram::TelegramOutboundMethodKind;
use time::OffsetDateTime;

/// Go server in-memory permission cache TTL.
pub const GO_PERMISSION_CACHE_TTL: time::Duration = time::Duration::minutes(30);

/// Boxed future returned by permission persistence stores.
pub type ChatPermissionStoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Chat context needed by Go permission checks and permission-error updates.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChatPermissionContext {
    pub chat_type: Option<String>,
    /// Stored Go `chat_settings` row.
    pub settings: Option<ChatSettings>,
}

/// Persistent store boundary for chat permission decisions.
pub trait ChatPermissionStore {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Load one chat's type and settings.
    fn load_context<'a>(
        &'a self,
        chat_id: i64,
    ) -> ChatPermissionStoreFuture<'a, Result<ChatPermissionContext, Self::Error>>;

    /// Save a Go-shaped chat settings update.
    fn save_settings<'a>(
        &'a self,
        update: ChatSettingsUpdate,
    ) -> ChatPermissionStoreFuture<'a, Result<(), Self::Error>>;
}

impl ChatPermissionStore for openplotva_storage::PostgresChatSettingsStore {
    type Error = openplotva_storage::StorageError;

    fn load_context<'a>(
        &'a self,
        chat_id: i64,
    ) -> ChatPermissionStoreFuture<'a, Result<ChatPermissionContext, Self::Error>> {
        Box::pin(async move {
            Ok(ChatPermissionContext {
                chat_type: self.get_chat_type(chat_id).await?,
                settings: self.get_chat_settings(chat_id).await?,
            })
        })
    }

    fn save_settings<'a>(
        &'a self,
        update: ChatSettingsUpdate,
    ) -> ChatPermissionStoreFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move { self.upsert_chat_settings(&update).await })
    }
}

/// Source used to decide one permission check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PermissionCheckSource {
    /// Go allows all private-chat actions before touching storage.
    PrivateChat,
    /// The in-memory Go policy cache had an entry.
    Cache,
    /// Stored chat settings decided the action.
    Store,
    /// Stored settings were missing, which Go treats as allowed.
    MissingSettings,
    /// Storage returned an error, which Go treats as allowed for `CanPerformAction`.
    LoadError,
}

/// Result of checking one chat/action permission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionCheckReport {
    /// Whether the action may proceed.
    pub allowed: bool,
    /// Source used by the check.
    pub source: PermissionCheckSource,
    pub load_error: Option<String>,
}

/// Result of applying a Telegram permission-send-error side effect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionErrorUpdateReport {
    /// Whether the method kind maps to a Go permission auto-disable path.
    pub applicable: bool,
    /// Whether an update was attempted and saved without error.
    pub updated: bool,
    /// Chat type written to Go `UpsertChatSettingsParams`.
    pub chat_type: Option<String>,
    /// Context load error that prevented an update.
    pub load_error: Option<String>,
    /// Settings save error ignored after the send itself has already failed.
    pub save_error: Option<String>,
}

#[derive(Debug)]
pub struct ChatPermissionPolicy<S> {
    store: S,
    cache: Mutex<HashMap<String, CachedPermission>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CachedPermission {
    allowed: bool,
    cached_until: OffsetDateTime,
}

impl<S> ChatPermissionPolicy<S> {
    /// Build an empty policy cache over a persistent store.
    pub fn new(store: S) -> Self {
        Self {
            store,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Invalidate the same action keys as Go `InvalidatePermissionCache`.
    pub fn invalidate_chat(&self, chat_id: i64) {
        let mut cache = self.cache();
        for action in [
            ACTION_SEND_TEXT,
            ACTION_SEND_IMAGE,
            ACTION_SEND_STICKER,
            ACTION_SEND_AUDIO,
        ] {
            cache.remove(&permission_cache_key(chat_id, action));
        }
    }
}

impl<S> ChatPermissionPolicy<S>
where
    S: ChatPermissionStore + Send + Sync,
{
    /// Return whether one Go permission action is allowed at `now`.
    pub async fn can_perform_action_at(
        &self,
        chat_id: i64,
        chat_type: Option<&str>,
        action: &str,
        now: OffsetDateTime,
    ) -> PermissionCheckReport {
        if chat_type == Some("private") {
            return PermissionCheckReport {
                allowed: true,
                source: PermissionCheckSource::PrivateChat,
                load_error: None,
            };
        }

        let key = permission_cache_key(chat_id, action);
        if let Some(cached) = self.cached_permission(&key, now) {
            return PermissionCheckReport {
                allowed: cached,
                source: PermissionCheckSource::Cache,
                load_error: None,
            };
        }

        let context = match self.store.load_context(chat_id).await {
            Ok(context) => context,
            Err(error) => {
                self.cache_permission(key, true, now);
                return PermissionCheckReport {
                    allowed: true,
                    source: PermissionCheckSource::LoadError,
                    load_error: Some(error.to_string()),
                };
            }
        };
        let Some(settings) = context.settings else {
            self.cache_permission(key, true, now);
            return PermissionCheckReport {
                allowed: true,
                source: PermissionCheckSource::MissingSettings,
                load_error: None,
            };
        };
        let effective_chat_type = chat_type
            .or(context.chat_type.as_deref())
            .unwrap_or_default();
        let allowed = can_perform_action(effective_chat_type, Some(&settings), action);
        self.cache_permission(key, allowed, now);
        PermissionCheckReport {
            allowed,
            source: PermissionCheckSource::Store,
            load_error: None,
        }
    }

    /// Apply Go's auto-disable side effect after a Telegram permission send error.
    pub async fn record_send_permission_error(
        &self,
        chat_id: i64,
        method_kind: TelegramOutboundMethodKind,
    ) -> PermissionErrorUpdateReport {
        let Some(is_media_permission_error) = permission_error_media_flag(method_kind) else {
            return PermissionErrorUpdateReport {
                applicable: false,
                updated: false,
                chat_type: None,
                load_error: None,
                save_error: None,
            };
        };
        let context = match self.store.load_context(chat_id).await {
            Ok(context) => context,
            Err(error) => {
                return PermissionErrorUpdateReport {
                    applicable: true,
                    updated: false,
                    chat_type: None,
                    load_error: Some(error.to_string()),
                    save_error: None,
                };
            }
        };

        let chat_type = permission_error_chat_type(context.chat_type.as_deref());
        let settings = context
            .settings
            .unwrap_or_else(|| ChatSettings::defaults(chat_id));
        let update =
            permission_error_settings_update(&settings, &chat_type, is_media_permission_error);
        let save_error = self
            .store
            .save_settings(update)
            .await
            .err()
            .map(|error| error.to_string());
        if save_error.is_none() {
            self.invalidate_chat(chat_id);
        }

        PermissionErrorUpdateReport {
            applicable: true,
            updated: save_error.is_none(),
            chat_type: Some(chat_type),
            load_error: None,
            save_error,
        }
    }

    fn cached_permission(&self, key: &str, now: OffsetDateTime) -> Option<bool> {
        let mut cache = self.cache();
        let cached = cache.get(key).copied()?;
        if now > cached.cached_until {
            cache.remove(key);
            return None;
        }
        Some(cached.allowed)
    }

    fn cache_permission(&self, key: String, allowed: bool, now: OffsetDateTime) {
        self.cache().insert(
            key,
            CachedPermission {
                allowed,
                cached_until: now + GO_PERMISSION_CACHE_TTL,
            },
        );
    }
}

impl<S> ChatPermissionPolicy<S> {
    fn cache(&self) -> MutexGuard<'_, HashMap<String, CachedPermission>> {
        match self.cache.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

/// Return the Go permission actions checked before dispatching a queued method.
pub fn dispatcher_required_actions(
    method_kind: TelegramOutboundMethodKind,
) -> &'static [&'static str] {
    match method_kind {
        TelegramOutboundMethodKind::SendMessage | TelegramOutboundMethodKind::SendChatAction => {
            &[ACTION_SEND_TEXT]
        }
        TelegramOutboundMethodKind::SendSticker => &[ACTION_SEND_STICKER, ACTION_SEND_TEXT],
        TelegramOutboundMethodKind::EditMessageText => &[ACTION_EDIT_MESSAGE],
        _ => &[],
    }
}

/// Return whether a `carapax` execute error is a Go permission send error.
pub fn telegram_execute_error_is_permission_error(error: &carapax::api::ExecuteError) -> bool {
    let carapax::api::ExecuteError::Response(response) = error else {
        return false;
    };
    match response.error_code() {
        Some(403) => true,
        Some(400) => is_permission_send_error(response.description()),
        _ => false,
    }
}

fn is_permission_send_error(message: &str) -> bool {
    message.contains("not enough rights")
        || message.contains("CHAT_WRITE_FORBIDDEN")
        || message.contains("have no rights to send a message")
}

fn permission_error_media_flag(method_kind: TelegramOutboundMethodKind) -> Option<bool> {
    match method_kind {
        TelegramOutboundMethodKind::SendMessage
        | TelegramOutboundMethodKind::SendChatAction
        | TelegramOutboundMethodKind::EditMessageText => Some(false),
        TelegramOutboundMethodKind::SendSticker
        | TelegramOutboundMethodKind::SendPhoto
        | TelegramOutboundMethodKind::SendAudio
        | TelegramOutboundMethodKind::SendMediaGroup => Some(true),
        TelegramOutboundMethodKind::EditMessageCaption
        | TelegramOutboundMethodKind::EditMessageReplyMarkup
        | TelegramOutboundMethodKind::EditMessageMedia
        | TelegramOutboundMethodKind::DeleteMessage => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        fmt,
        sync::{Arc, Mutex},
    };

    use openplotva_core::{ChatSettings, ChatSettingsUpdate};
    use openplotva_server::{ACTION_EDIT_MESSAGE, ACTION_SEND_STICKER, ACTION_SEND_TEXT};
    use openplotva_telegram::TelegramOutboundMethodKind;
    use time::OffsetDateTime;

    use super::{
        ChatPermissionContext, ChatPermissionPolicy, ChatPermissionStore,
        ChatPermissionStoreFuture, PermissionCheckSource, dispatcher_required_actions,
        telegram_execute_error_is_permission_error,
    };

    #[tokio::test]
    async fn policy_allows_private_chats_without_loading_settings() -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let store = PermissionStoreStub::default();
        let policy = ChatPermissionPolicy::new(store.clone());

        let report = policy
            .can_perform_action_at(42, Some("private"), ACTION_SEND_TEXT, now)
            .await;

        assert!(report.allowed);
        assert_eq!(report.source, PermissionCheckSource::PrivateChat);
        assert_eq!(store.load_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn policy_defaults_to_allow_and_caches_when_settings_load_fails()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let store = PermissionStoreStub::default();
        store.set_load_error(Some("database unavailable"));
        let policy = ChatPermissionPolicy::new(store.clone());

        let first = policy
            .can_perform_action_at(42, Some("supergroup"), ACTION_SEND_TEXT, now)
            .await;
        let cached = policy
            .can_perform_action_at(42, Some("supergroup"), ACTION_SEND_TEXT, now)
            .await;

        assert!(first.allowed);
        assert_eq!(first.source, PermissionCheckSource::LoadError);
        assert_eq!(first.load_error.as_deref(), Some("database unavailable"));
        assert!(cached.allowed);
        assert_eq!(cached.source, PermissionCheckSource::Cache);
        assert_eq!(store.load_count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn policy_caches_per_go_permission_key_and_can_invalidate() -> Result<(), Box<dyn Error>>
    {
        let now = OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let store = PermissionStoreStub::with_context(ChatPermissionContext {
            chat_type: Some("supergroup".to_owned()),
            settings: Some(ChatSettings {
                enable_global_text_reply: false,
                enable_global_draw_reply: true,
                ..ChatSettings::defaults(42)
            }),
        });
        let policy = ChatPermissionPolicy::new(store.clone());

        let text = policy
            .can_perform_action_at(42, Some("supergroup"), ACTION_SEND_TEXT, now)
            .await;
        let sticker = policy
            .can_perform_action_at(42, Some("supergroup"), ACTION_SEND_STICKER, now)
            .await;
        let cached_text = policy
            .can_perform_action_at(42, Some("supergroup"), ACTION_SEND_TEXT, now)
            .await;
        policy.invalidate_chat(42);
        let reloaded_text = policy
            .can_perform_action_at(42, Some("supergroup"), ACTION_SEND_TEXT, now)
            .await;

        assert!(!text.allowed);
        assert_eq!(text.source, PermissionCheckSource::Store);
        assert!(sticker.allowed);
        assert_eq!(sticker.source, PermissionCheckSource::Store);
        assert!(!cached_text.allowed);
        assert_eq!(cached_text.source, PermissionCheckSource::Cache);
        assert!(!reloaded_text.allowed);
        assert_eq!(reloaded_text.source, PermissionCheckSource::Store);
        assert_eq!(store.load_count(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn policy_records_permission_error_update_and_invalidates_like_go()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let store = PermissionStoreStub::with_context(ChatPermissionContext {
            chat_type: Some("supergroup".to_owned()),
            settings: Some(ChatSettings::defaults(42)),
        });
        let policy = ChatPermissionPolicy::new(store.clone());

        let allowed = policy
            .can_perform_action_at(42, Some("supergroup"), ACTION_SEND_TEXT, now)
            .await;
        let update = policy
            .record_send_permission_error(42, TelegramOutboundMethodKind::SendPhoto)
            .await;
        store.set_context(ChatPermissionContext {
            chat_type: Some("supergroup".to_owned()),
            settings: Some(ChatSettings {
                enable_global_text_reply: true,
                enable_global_draw_reply: false,
                ..ChatSettings::defaults(42)
            }),
        });
        let reloaded = policy
            .can_perform_action_at(42, Some("supergroup"), ACTION_SEND_STICKER, now)
            .await;

        assert!(allowed.allowed);
        assert!(update.updated);
        assert_eq!(update.chat_type.as_deref(), Some("supergroup"));
        assert_eq!(update.save_error, None);
        assert_eq!(
            store.saved_updates(),
            vec![ChatSettingsUpdate {
                chat_id: 42,
                chat_type: "supergroup".to_owned(),
                enable_global_draw_reply: false,
                ..ChatSettingsUpdate::from_defaults_for_test(42)
            }]
        );
        assert!(!reloaded.allowed);
        assert_eq!(reloaded.source, PermissionCheckSource::Store);
        Ok(())
    }

    #[test]
    fn dispatcher_required_actions_match_go_send_paths() {
        assert_eq!(
            dispatcher_required_actions(TelegramOutboundMethodKind::SendMessage),
            &[ACTION_SEND_TEXT]
        );
        assert_eq!(
            dispatcher_required_actions(TelegramOutboundMethodKind::SendSticker),
            &[ACTION_SEND_STICKER, ACTION_SEND_TEXT]
        );
        assert_eq!(
            dispatcher_required_actions(TelegramOutboundMethodKind::SendChatAction),
            &[ACTION_SEND_TEXT]
        );
        assert_eq!(
            dispatcher_required_actions(TelegramOutboundMethodKind::EditMessageText),
            &[ACTION_EDIT_MESSAGE]
        );
        assert!(dispatcher_required_actions(TelegramOutboundMethodKind::SendPhoto).is_empty());
    }

    #[test]
    fn telegram_permission_error_classifier_matches_go_send_errors() -> Result<(), Box<dyn Error>> {
        let blocked = response_error(
            r#"{"ok":false,"error_code":403,"description":"Forbidden: bot was kicked from the group chat"}"#,
        )?;
        assert!(telegram_execute_error_is_permission_error(&blocked));

        let forbidden = response_error(
            r#"{"ok":false,"error_code":400,"description":"Bad Request: CHAT_WRITE_FORBIDDEN"}"#,
        )?;
        assert!(telegram_execute_error_is_permission_error(&forbidden));

        let empty_text = response_error(
            r#"{"ok":false,"error_code":400,"description":"Bad Request: message text is empty"}"#,
        )?;
        assert!(!telegram_execute_error_is_permission_error(&empty_text));

        Ok(())
    }

    fn response_error(json: &str) -> Result<carapax::api::ExecuteError, Box<dyn Error>> {
        let response: carapax::types::Response<serde_json::Value> = serde_json::from_str(json)?;
        match response.into_result() {
            Ok(_) => Err("test response unexpectedly succeeded".into()),
            Err(error) => Ok(carapax::api::ExecuteError::Response(error)),
        }
    }

    trait TestChatSettingsUpdate {
        fn from_defaults_for_test(chat_id: i64) -> Self;
    }

    impl TestChatSettingsUpdate for ChatSettingsUpdate {
        fn from_defaults_for_test(chat_id: i64) -> Self {
            ChatSettingsUpdate {
                chat_id,
                chat_type: "supergroup".to_owned(),
                mood_alignment: Some("neutral".to_owned()),
                custom_persona: None,
                reactivity_percentage: 3,
                proactivity_percentage: 0,
                enable_global_text_reply: true,
                enable_global_draw_reply: true,
                enable_obscenifier: true,
                enable_profanity: true,
                enable_greet_joiners: false,
                enable_daily_game: true,
                daily_game_theme: "auto".to_owned(),
                greeting_html: None,
            }
        }
    }

    #[derive(Clone, Default)]
    struct PermissionStoreStub {
        state: Arc<Mutex<PermissionStoreState>>,
    }

    #[derive(Default)]
    struct PermissionStoreState {
        context: ChatPermissionContext,
        load_error: Option<&'static str>,
        save_error: Option<&'static str>,
        load_count: usize,
        saved: Vec<ChatSettingsUpdate>,
    }

    #[derive(Debug)]
    struct PermissionStoreStubError(&'static str);

    impl fmt::Display for PermissionStoreStubError {
        fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
            out.write_str(self.0)
        }
    }

    impl Error for PermissionStoreStubError {}

    impl PermissionStoreStub {
        fn with_context(context: ChatPermissionContext) -> Self {
            let store = Self::default();
            store.set_context(context);
            store
        }

        fn set_context(&self, context: ChatPermissionContext) {
            self.state().context = context;
        }

        fn set_load_error(&self, error: Option<&'static str>) {
            self.state().load_error = error;
        }

        fn load_count(&self) -> usize {
            self.state().load_count
        }

        fn saved_updates(&self) -> Vec<ChatSettingsUpdate> {
            self.state().saved.clone()
        }

        fn state(&self) -> std::sync::MutexGuard<'_, PermissionStoreState> {
            match self.state.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            }
        }
    }

    impl ChatPermissionStore for PermissionStoreStub {
        type Error = PermissionStoreStubError;

        fn load_context<'a>(
            &'a self,
            _chat_id: i64,
        ) -> ChatPermissionStoreFuture<'a, Result<ChatPermissionContext, Self::Error>> {
            Box::pin(async move {
                let mut state = self.state();
                state.load_count += 1;
                if let Some(error) = state.load_error {
                    return Err(PermissionStoreStubError(error));
                }
                Ok(state.context.clone())
            })
        }

        fn save_settings<'a>(
            &'a self,
            update: ChatSettingsUpdate,
        ) -> ChatPermissionStoreFuture<'a, Result<(), Self::Error>> {
            Box::pin(async move {
                let mut state = self.state();
                state.saved.push(update);
                if let Some(error) = state.save_error {
                    return Err(PermissionStoreStubError(error));
                }
                Ok(())
            })
        }
    }
}
