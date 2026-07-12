use serde::{Deserialize, Serialize};

/// Public project name used in diagnostics and health responses.
pub const PROJECT_NAME: &str = "openplotva";

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
    pub is_forum: Option<bool>,
}

impl ChatState {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserState {
    /// Telegram user ID.
    pub id: i64,
    /// Telegram first name.
    pub first_name: String,
    pub last_name: Option<String>,
    pub username: Option<String>,
    /// Telegram language code, when non-blank.
    pub language_code: Option<String>,
    /// Telegram Premium flag.
    pub is_premium: Option<bool>,
}

impl UserState {
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
    pub fn new(chat: Option<ChatState>, user: Option<UserState>) -> Option<Self> {
        if chat.is_none() && user.is_none() {
            None
        } else {
            Some(Self { chat, user })
        }
    }
}

pub const SENDER_TYPE_USER: &str = "user";
pub const SENDER_TYPE_CHANNEL: &str = "channel";
pub const SENDER_TYPE_SAME_CHAT: &str = "same_chat";
pub const SENDER_TYPE_SYSTEM: &str = "system";

pub const VIP_EVENT_TYPE_PAYMENT: &str = "payment";
pub const VIP_EVENT_TYPE_ADMIN_ADJUSTMENT: &str = "admin_adjustment";
pub const VIP_EVENT_TYPE_ADMIN_REVOKE: &str = "admin_revoke";
pub const VIP_EVENT_TYPE_REFUND_REVERSAL: &str = "refund_reversal";
pub const VIP_EVENT_TYPE_LEGACY_SUBSCRIPTION_BACKFILL: &str = "legacy_subscription_backfill";
pub const VIP_EVENT_TYPE_LEGACY_VIP_CACHE_BACKFILL: &str = "legacy_vip_cache_backfill";
pub const VIP_SECONDS_PER_DAY: i64 = 24 * 60 * 60;

#[must_use]
pub fn vip_days_to_seconds(days: i64) -> i64 {
    days * VIP_SECONDS_PER_DAY
}

#[must_use]
pub fn is_synthetic_subscription_charge_id(charge_id: &str) -> bool {
    charge_id.trim().starts_with("admin_grant_")
}

#[must_use]
pub fn subscription_delta_seconds(
    charge_id: &str,
    created_at: time::OffsetDateTime,
    expires_at: time::OffsetDateTime,
    default_days: i64,
) -> i64 {
    if !is_synthetic_subscription_charge_id(charge_id) {
        return vip_days_to_seconds(default_days);
    }
    if expires_at < created_at {
        return 0;
    }
    (expires_at - created_at).whole_seconds()
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MessageSender {
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

#[must_use]
pub fn is_terminator_tool_call_name(name: &str) -> bool {
    let name = name.trim();
    name.eq_ignore_ascii_case("final_response")
        || name.eq_ignore_ascii_case("optimize_prompt_terminator")
        || name.eq_ignore_ascii_case("optimize_edit_prompt_terminator")
}

#[must_use]
pub fn filter_non_terminator_tool_calls(calls: &[ToolCall]) -> Vec<ToolCall> {
    calls
        .iter()
        .filter(|call| !is_terminator_tool_call_name(&call.name))
        .cloned()
        .collect()
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ChatMessageMeta {
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
    /// Media width in pixels when Telegram provides it.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub width: i64,
    /// Media height in pixels when Telegram provides it.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub height: i64,
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
    pub enable_daily_game: Option<bool>,
    pub daily_game_theme: Option<String>,
    pub greeting_html: Option<String>,
}

impl ChatSettings {
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
    pub greeting_html: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserSettings {
    /// Telegram user ID.
    pub user_id: i64,
    /// Whether random group reactivity is disabled for this user.
    pub disable_random_reactivity: bool,
    /// Whether original draw prompts are hidden for this user.
    pub hide_original_draw_prompt: bool,
    /// Last update timestamp.
    pub updated: time::OffsetDateTime,
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
        ChatSettings, ChatState, MessageSender, ToolCall, UserState,
        VIP_EVENT_TYPE_ADMIN_ADJUSTMENT, VIP_EVENT_TYPE_ADMIN_REVOKE,
        VIP_EVENT_TYPE_LEGACY_SUBSCRIPTION_BACKFILL, VIP_EVENT_TYPE_LEGACY_VIP_CACHE_BACKFILL,
        VIP_EVENT_TYPE_PAYMENT, VIP_EVENT_TYPE_REFUND_REVERSAL, filter_non_terminator_tool_calls,
        is_synthetic_subscription_charge_id, is_terminator_tool_call_name,
        subscription_delta_seconds, vip_days_to_seconds,
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
    fn tool_call_terminator_filter_matches_go_names() {
        assert!(is_terminator_tool_call_name("final_response"));
        assert!(is_terminator_tool_call_name(" optimize_prompt_terminator "));
        assert!(is_terminator_tool_call_name(
            "OPTIMIZE_EDIT_PROMPT_TERMINATOR"
        ));
        assert!(!is_terminator_tool_call_name("draw_image"));
        assert!(!is_terminator_tool_call_name(""));

        let filtered = filter_non_terminator_tool_calls(&[
            ToolCall {
                name: "web_search".to_owned(),
                ..ToolCall::default()
            },
            ToolCall {
                name: "final_response".to_owned(),
                ..ToolCall::default()
            },
            ToolCall {
                name: "draw_image".to_owned(),
                ..ToolCall::default()
            },
        ]);

        assert_eq!(
            filtered
                .iter()
                .map(|call| call.name.as_str())
                .collect::<Vec<_>>(),
            vec!["web_search", "draw_image"]
        );
        assert!(
            filter_non_terminator_tool_calls(&[ToolCall {
                name: "final_response".to_owned(),
                ..ToolCall::default()
            }])
            .is_empty()
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

    #[test]
    fn vip_domain_helpers_match_go_vip_package() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(VIP_EVENT_TYPE_PAYMENT, "payment");
        assert_eq!(VIP_EVENT_TYPE_ADMIN_ADJUSTMENT, "admin_adjustment");
        assert_eq!(VIP_EVENT_TYPE_ADMIN_REVOKE, "admin_revoke");
        assert_eq!(VIP_EVENT_TYPE_REFUND_REVERSAL, "refund_reversal");
        assert_eq!(
            VIP_EVENT_TYPE_LEGACY_SUBSCRIPTION_BACKFILL,
            "legacy_subscription_backfill"
        );
        assert_eq!(
            VIP_EVENT_TYPE_LEGACY_VIP_CACHE_BACKFILL,
            "legacy_vip_cache_backfill"
        );
        assert_eq!(vip_days_to_seconds(30), 2_592_000);
        assert!(is_synthetic_subscription_charge_id(" admin_grant_42 "));
        assert!(!is_synthetic_subscription_charge_id("subscription_42"));

        let created_at = time::OffsetDateTime::from_unix_timestamp(1_800_000_000)?;
        let expires_at = created_at + time::Duration::seconds(1234);

        assert_eq!(
            subscription_delta_seconds("telegram-charge", created_at, expires_at, 30),
            2_592_000,
            "Paid Telegram charges use the configured default duration"
        );
        assert_eq!(
            subscription_delta_seconds("admin_grant_42", created_at, expires_at, 30),
            1234,
            "synthetic admin grants preserve their exact created/expires delta"
        );
        assert_eq!(
            subscription_delta_seconds("admin_grant_42", expires_at, created_at, 30),
            0,
            "Inverted synthetic admin grants clamp to zero seconds"
        );

        Ok(())
    }
}
