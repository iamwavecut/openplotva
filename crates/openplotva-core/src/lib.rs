
use serde::{Deserialize, Serialize};

/// Public project name used in diagnostics and health responses.
pub const PROJECT_NAME: &str = "openplotva";

pub const REFERENCE_SOURCE_REPOSITORY: &str = "/Users/Shared/src/github.com/iamwavecut/reference-app";

pub const INITIAL_REFERENCE_SOURCE_COMMIT: &str = "56506a95a749629235ecf1ea35c54d5a4172fdbd";

/// Chat identity/state extracted by the Go update consumer before handling side effects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChatState {
    /// Telegram chat ID.
    pub id: i64,
    pub chat_type: String,
    /// Chat title, when non-blank.
    pub title: Option<String>,
    /// Chat username, when non-blank.
    pub username: Option<String>,
    /// Private chat first name, when non-blank.
    pub first_name: Option<String>,
    /// Private chat last name, when non-blank.
    pub last_name: Option<String>,
    /// Forum flag. Go only writes this when Telegram reports it as true.
    pub is_forum: Option<bool>,
}

impl ChatState {
    /// Build chat state while preserving Go's blank-field normalization.
    pub fn new(
        id: i64,
        chat_type: impl Into<String>,
        title: Option<String>,
        username: Option<String>,
        first_name: Option<String>,
        last_name: Option<String>,
        is_forum: Option<bool>,
    ) -> Self {
        let chat_type = chat_type.into();
        Self {
            id,
            chat_type: if chat_type.trim().is_empty() {
                "private".to_owned()
            } else {
                chat_type
            },
            title: non_blank_string(title),
            username: non_blank_string(username),
            first_name: non_blank_string(first_name),
            last_name: non_blank_string(last_name),
            is_forum: is_forum.filter(|value| *value),
        }
    }
}

/// User identity/state extracted by the Go update consumer before handling side effects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserState {
    /// Telegram user ID.
    pub id: i64,
    /// Telegram first name.
    pub first_name: String,
    /// Telegram last name. Go preserves present empty strings here.
    pub last_name: Option<String>,
    /// Telegram username. Go preserves present empty strings here.
    pub username: Option<String>,
    /// Telegram language code, when non-blank.
    pub language_code: Option<String>,
    /// Telegram Premium flag.
    pub is_premium: Option<bool>,
}

impl UserState {
    /// Build user state while preserving Go's language-code blank normalization.
    pub fn new(
        id: i64,
        first_name: impl Into<String>,
        last_name: Option<String>,
        username: Option<String>,
        language_code: Option<String>,
        is_premium: Option<bool>,
    ) -> Self {
        Self {
            id,
            first_name: first_name.into(),
            last_name,
            username,
            language_code: non_blank_string(language_code),
            is_premium,
        }
    }
}

/// Chat/user pair extracted from one update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateState {
    /// Chat state to persist.
    pub chat: Option<ChatState>,
    /// User state to persist.
    pub user: Option<UserState>,
}

impl UpdateState {
    /// Return `None` when both chat and user state are absent, matching Go no-op state updates.
    pub fn new(chat: Option<ChatState>, user: Option<UserState>) -> Option<Self> {
        if chat.is_none() && user.is_none() {
            None
        } else {
            Some(Self { chat, user })
        }
    }
}

/// Go `sharedtypes.SenderTypeUser`.
pub const SENDER_TYPE_USER: &str = "user";
/// Go `sharedtypes.SenderTypeChannel`.
pub const SENDER_TYPE_CHANNEL: &str = "channel";
/// Go `sharedtypes.SenderTypeSameChat`.
pub const SENDER_TYPE_SAME_CHAT: &str = "same_chat";
/// Go `sharedtypes.SenderTypeSystem`.
pub const SENDER_TYPE_SYSTEM: &str = "system";

/// Telegram sender identity after Go `utils.ResolveMessageSender` normalization.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MessageSender {
    /// Sender kind, one of the Go sender type constants.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sender_type: String,
    /// Sender Telegram ID.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub id: i64,
    /// Full display name before final fallback formatting.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub full_name: String,
    /// Telegram username without surrounding whitespace.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub username: String,
    /// Whether the sender user is a bot.
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_bot: bool,
}

impl MessageSender {
    /// Build the Go system-sender zero path.
    #[must_use]
    pub fn system() -> Self {
        Self {
            sender_type: SENDER_TYPE_SYSTEM.to_owned(),
            id: 0,
            full_name: String::new(),
            username: String::new(),
            is_bot: false,
        }
    }

    /// Return Go `MessageSender.DisplayName`.
    #[must_use]
    pub fn display_name(&self) -> String {
        let full_name = self.full_name.trim();
        if !full_name.is_empty() {
            return full_name.to_owned();
        }

        let username = self.username.trim();
        if !username.is_empty() {
            if username.starts_with('@') {
                return username.to_owned();
            }
            return format!("@{username}");
        }

        if self.id != 0 {
            return self.id.to_string();
        }

        "Telegram".to_owned()
    }
}

impl Default for MessageSender {
    fn default() -> Self {
        Self::system()
    }
}

/// Go `sharedtypes.ToolCall` metadata stored with chat history entries.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ToolCall {
    /// Tool name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// Tool reference.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub r#ref: String,
    /// Tool input JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
    /// Tool output JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    /// Tool timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
}

/// Go `sharedtypes.ChatMessageMeta` stored with chat history and dialog jobs.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ChatMessageMeta {
    /// Message type, serialized as Go `type`.
    #[serde(default, rename = "type", skip_serializing_if = "String::is_empty")]
    pub message_type: String,
    /// Optional annotation.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub annotation: String,
    /// Vision description.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub vision_description: String,
    /// Sender kind.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sender_type: String,
    /// Sender ID.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub sender_id: i64,
    /// Sender display name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sender_name: String,
    /// Sender Telegram username.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sender_username: String,
    /// Message attachments.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<ChatAttachment>,
    /// Tool calls associated with the message.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
}

/// Go `sharedtypes.ChatAttachment` metadata stored with chat history entries.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ChatAttachment {
    /// Attachment kind, such as `image`, `audio`, or `contact`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    /// Attachment source, defaulting to `message` for Telegram messages.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source: String,
    /// Textual content for stickers and venues.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub content: String,
    /// Telegram stable file unique ID.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub file_unique_id: String,
    /// Original file name, when Telegram provides it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub file_name: String,
    /// MIME type, when Telegram provides it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub mime_type: String,
    /// Media caption.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub caption: String,
    /// Location latitude.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latitude: Option<f64>,
    /// Location longitude.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub longitude: Option<f64>,
    /// Audio/video/voice duration in seconds.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub duration_seconds: i64,
    /// Audio performer.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub performer: String,
    /// Audio title.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub title: String,
    /// Contact phone number.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub phone: String,
    /// Contact first name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub first_name: String,
    /// Contact last name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub last_name: String,
    /// Contact Telegram user ID.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub user_id: i64,
}

/// Go `chat_settings` fields used by permission and settings behavior.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChatSettings {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Chat persona mood alignment.
    pub mood_alignment: Option<String>,
    /// Chat custom persona text.
    pub custom_persona: Option<String>,
    /// Text reactivity percentage.
    pub reactivity_percentage: i32,
    /// Proactivity percentage.
    pub proactivity_percentage: i32,
    /// Whether global text replies are enabled.
    pub enable_global_text_reply: bool,
    /// Whether global draw/media replies are enabled.
    pub enable_global_draw_reply: bool,
    /// Whether the obscenifier feature is enabled.
    pub enable_obscenifier: bool,
    /// Whether profanity is enabled.
    pub enable_profanity: bool,
    /// Whether join greetings are enabled.
    pub enable_greet_joiners: bool,
    /// Whether the daily game is enabled. Go keeps this nullable in stored rows.
    pub enable_daily_game: Option<bool>,
    /// Daily game theme. Go defaults blank or missing values to `auto` on permission updates.
    pub daily_game_theme: Option<String>,
    /// Greeting HTML. Go permission updates leave this unset.
    pub greeting_html: Option<String>,
}

impl ChatSettings {
    /// Build Go default chat settings from `internal/db/defaults.Settings`.
    pub fn defaults(chat_id: i64) -> Self {
        Self {
            chat_id,
            mood_alignment: Some("neutral".to_owned()),
            custom_persona: None,
            reactivity_percentage: 3,
            proactivity_percentage: 0,
            enable_global_text_reply: true,
            enable_global_draw_reply: true,
            enable_obscenifier: true,
            enable_profanity: true,
            enable_greet_joiners: false,
            enable_daily_game: Some(true),
            daily_game_theme: Some("auto".to_owned()),
            greeting_html: None,
        }
    }
}

/// Go `UpsertChatSettingsParams` shape used when permission errors auto-disable replies.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChatSettingsUpdate {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Chat type to upsert into `chats` before writing settings.
    pub chat_type: String,
    /// Chat persona mood alignment.
    pub mood_alignment: Option<String>,
    /// Chat custom persona text.
    pub custom_persona: Option<String>,
    /// Text reactivity percentage.
    pub reactivity_percentage: i32,
    /// Proactivity percentage.
    pub proactivity_percentage: i32,
    /// Whether global text replies are enabled.
    pub enable_global_text_reply: bool,
    /// Whether global draw/media replies are enabled.
    pub enable_global_draw_reply: bool,
    /// Whether the obscenifier feature is enabled.
    pub enable_obscenifier: bool,
    /// Whether profanity is enabled.
    pub enable_profanity: bool,
    /// Whether join greetings are enabled.
    pub enable_greet_joiners: bool,
    /// Whether the daily game is enabled.
    pub enable_daily_game: bool,
    /// Daily game theme.
    pub daily_game_theme: String,
    /// Greeting HTML. Go permission updates leave this unset.
    pub greeting_html: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingOp {
    /// Pending operation ID.
    pub id: i64,
    /// Virtual message ID the operation targets.
    pub vmsg_id: String,
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Operation kind, currently `edit` or `delete` in Go.
    pub op: String,
    /// Operation payload, if any.
    pub payload: Vec<u8>,
    /// Number of attempts recorded by Go storage.
    pub attempts: i32,
}

impl PendingOp {
    /// Build a pending op with the fields used by the Go mapping helpers.
    pub fn new(id: i64, vmsg_id: impl Into<String>, chat_id: i64, op: impl Into<String>) -> Self {
        Self {
            id,
            vmsg_id: vmsg_id.into(),
            chat_id,
            op: op.into(),
            payload: Vec::new(),
            attempts: 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageIdMapping {
    /// Virtual message ID.
    pub vmsg_id: String,
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram forum thread ID, when present.
    pub thread_id: Option<i32>,
    /// Resolved real Telegram message ID, when the send finished.
    pub real_message_id: Option<i32>,
}

impl MessageIdMapping {
    /// Build an unresolved virtual-message mapping.
    pub fn unresolved(vmsg_id: impl Into<String>, chat_id: i64) -> Self {
        Self {
            vmsg_id: vmsg_id.into(),
            chat_id,
            thread_id: None,
            real_message_id: None,
        }
    }

    /// Build a resolved virtual-message mapping.
    pub fn resolved(vmsg_id: impl Into<String>, chat_id: i64, real_message_id: i32) -> Self {
        Self {
            vmsg_id: vmsg_id.into(),
            chat_id,
            thread_id: None,
            real_message_id: Some(real_message_id),
        }
    }
}

/// Pending operation that has a resolved real Telegram message ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadyPendingOp {
    /// Pending operation ID.
    pub id: i64,
    /// Virtual message ID the operation targets.
    pub vmsg_id: String,
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Operation kind, currently `edit` or `delete` in Go.
    pub op: String,
    /// Operation payload, if any.
    pub payload: Vec<u8>,
    /// Real Telegram message ID from `message_id_map`.
    pub real_message_id: i32,
}

/// Decoded Go pending edit payload.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct PendingEditPayload {
    /// Edited text.
    #[serde(default)]
    pub text: String,
    /// Telegram parse mode, such as `HTML`.
    #[serde(default)]
    pub parse_mode: String,
}

/// Decode Go's pending edit payload, returning zero-values on malformed JSON.
pub fn pending_edit_payload(payload: &[u8]) -> PendingEditPayload {
    serde_json::from_slice(payload).unwrap_or_default()
}

fn non_blank_string(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
mod tests {
    use super::{
        ChatSettings, ChatState, MessageSender, PendingEditPayload, UserState, pending_edit_payload,
    };

    #[test]
    fn message_sender_display_name_matches_go_fallback_order() {
        assert_eq!(
            MessageSender {
                full_name: " Ada Lovelace ".to_owned(),
                username: "ada".to_owned(),
                id: 99,
                ..MessageSender::system()
            }
            .display_name(),
            "Ada Lovelace"
        );
        assert_eq!(
            MessageSender {
                username: "ada".to_owned(),
                id: 99,
                ..MessageSender::system()
            }
            .display_name(),
            "@ada"
        );
        assert_eq!(
            MessageSender {
                username: "@ada".to_owned(),
                id: 99,
                ..MessageSender::system()
            }
            .display_name(),
            "@ada"
        );
        assert_eq!(
            MessageSender {
                id: 99,
                ..MessageSender::system()
            }
            .display_name(),
            "99"
        );
        assert_eq!(MessageSender::system().display_name(), "Telegram");
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

    #[test]
    fn chat_state_normalizes_blank_fields_like_go_consumer() {
        assert_eq!(
            ChatState::new(
                -300,
                " ",
                Some(" Team ".to_owned()),
                Some(" ".to_owned()),
                Some("Alice".to_owned()),
                Some(String::new()),
                Some(true),
            ),
            ChatState {
                id: -300,
                chat_type: "private".to_owned(),
                title: Some(" Team ".to_owned()),
                username: None,
                first_name: Some("Alice".to_owned()),
                last_name: None,
                is_forum: Some(true),
            }
        );
    }

    #[test]
    fn user_state_preserves_present_empty_names_like_go_consumer() {
        assert_eq!(
            UserState::new(
                500,
                "Carol",
                Some(String::new()),
                Some("carol".to_owned()),
                Some(" ru ".to_owned()),
                Some(true),
            ),
            UserState {
                id: 500,
                first_name: "Carol".to_owned(),
                last_name: Some(String::new()),
                username: Some("carol".to_owned()),
                language_code: Some(" ru ".to_owned()),
                is_premium: Some(true),
            }
        );
    }

    #[test]
    fn chat_settings_defaults_match_go_defaults() {
        let settings = ChatSettings::defaults(42);

        assert_eq!(settings.chat_id, 42);
        assert_eq!(settings.mood_alignment.as_deref(), Some("neutral"));
        assert_eq!(settings.reactivity_percentage, 3);
        assert_eq!(settings.proactivity_percentage, 0);
        assert!(settings.enable_global_text_reply);
        assert!(settings.enable_global_draw_reply);
        assert!(settings.enable_obscenifier);
        assert!(settings.enable_profanity);
        assert!(!settings.enable_greet_joiners);
        assert_eq!(settings.enable_daily_game, Some(true));
        assert_eq!(settings.daily_game_theme.as_deref(), Some("auto"));
    }
}
