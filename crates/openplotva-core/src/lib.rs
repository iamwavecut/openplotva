
use serde::Deserialize;

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

#[cfg(test)]
mod tests {
    use super::{ChatState, PendingEditPayload, UserState, pending_edit_payload};

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
}
