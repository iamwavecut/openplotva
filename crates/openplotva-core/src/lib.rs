
/// Public project name used in diagnostics and health responses.
pub const PROJECT_NAME: &str = "openplotva";

pub const REFERENCE_SOURCE_REPOSITORY: &str = "/Users/Shared/src/github.com/iamwavecut/reference-app";

pub const INITIAL_REFERENCE_SOURCE_COMMIT: &str = "56506a95a749629235ecf1ea35c54d5a4172fdbd";

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
