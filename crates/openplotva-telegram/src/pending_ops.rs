//! Pending virtual-message operation builders for Telegram methods.

use carapax::types::EditMessageText;
use openplotva_core::{ReadyPendingOp, pending_edit_payload};
use thiserror::Error;

use crate::{
    DeleteMessageRequest, OutboundBuildError, TelegramOutboundMethod, build_delete_message_method,
    parse_mode_from_go,
};

pub const PENDING_OP_DELETE: &str = "delete";

pub const PENDING_OP_EDIT: &str = "edit";

/// Pending operation could not be converted into a Telegram method.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum PendingOpBuildError {
    /// Pending operation name is unknown.
    #[error("unknown pending operation: {0}")]
    UnknownOp(String),
    /// Telegram method construction failed.
    #[error("failed to build pending operation Telegram method: {0}")]
    Outbound(#[from] OutboundBuildError),
}

/// Build the concrete Telegram method for a resolved pending virtual-message op.
pub fn build_pending_op_method(
    op: &ReadyPendingOp,
) -> Result<TelegramOutboundMethod, PendingOpBuildError> {
    match op.op.as_str() {
        PENDING_OP_DELETE => Ok(TelegramOutboundMethod::from(build_delete_message_method(
            &DeleteMessageRequest {
                chat_id: op.chat_id,
                message_id: i64::from(op.real_message_id),
            },
        )?)),
        PENDING_OP_EDIT => build_pending_edit_method(op),
        other => Err(PendingOpBuildError::UnknownOp(other.to_owned())),
    }
}

fn build_pending_edit_method(
    op: &ReadyPendingOp,
) -> Result<TelegramOutboundMethod, PendingOpBuildError> {
    let payload = pending_edit_payload(&op.payload);
    let mut method =
        EditMessageText::for_chat_message(op.chat_id, i64::from(op.real_message_id), payload.text);
    if let Some(parse_mode) = parse_mode_from_go(&payload.parse_mode)? {
        method = method.with_parse_mode(parse_mode);
    }
    Ok(TelegramOutboundMethod::from(method))
}

#[cfg(test)]
mod tests {
    use openplotva_core::ReadyPendingOp;
    use serde_json::json;

    use super::{PENDING_OP_DELETE, PENDING_OP_EDIT, PendingOpBuildError, build_pending_op_method};
    use crate::{OutboundBuildError, TelegramOutboundMethod};

    fn ready_op(op: &str, payload: Vec<u8>) -> ReadyPendingOp {
        ReadyPendingOp {
            id: 1,
            vmsg_id: "v1".to_owned(),
            chat_id: 42,
            op: op.to_owned(),
            payload,
            real_message_id: 77,
        }
    }

    #[test]
    fn pending_delete_builds_delete_message_method() -> Result<(), Box<dyn std::error::Error>> {
        let method = build_pending_op_method(&ready_op(PENDING_OP_DELETE, Vec::new()))?;
        assert_eq!(method.method_name(), "deleteMessage");
        let TelegramOutboundMethod::DeleteMessage(method) = method else {
            panic!("expected deleteMessage method");
        };
        let payload = serde_json::to_value(method.as_ref())?;

        assert_eq!(payload["chat_id"], json!(42));
        assert_eq!(payload["message_id"], json!(77));

        Ok(())
    }

    #[test]
    fn pending_edit_builds_edit_message_text_method() -> Result<(), Box<dyn std::error::Error>> {
        let method = build_pending_op_method(&ready_op(
            PENDING_OP_EDIT,
            br#"{"text":"<b>edited</b>","parse_mode":"HTML"}"#.to_vec(),
        ))?;
        assert_eq!(method.method_name(), "editMessageText");
        let TelegramOutboundMethod::EditMessageText(method) = method else {
            panic!("expected editMessageText method");
        };
        let payload = serde_json::to_value(method.as_ref())?;

        assert_eq!(payload["chat_id"], json!(42));
        assert_eq!(payload["message_id"], json!(77));
        assert_eq!(payload["text"], json!("<b>edited</b>"));
        assert_eq!(payload["parse_mode"], json!("HTML"));

        Ok(())
    }

    #[test]
    fn malformed_pending_edit_payload_keeps_go_zero_value_text()
    -> Result<(), Box<dyn std::error::Error>> {
        let method = build_pending_op_method(&ready_op(PENDING_OP_EDIT, b"not-json".to_vec()))?;
        let TelegramOutboundMethod::EditMessageText(method) = method else {
            panic!("expected editMessageText method");
        };
        let payload = serde_json::to_value(method.as_ref())?;

        assert_eq!(payload["text"], json!(""));
        assert!(payload.get("parse_mode").is_none());

        Ok(())
    }

    #[test]
    fn pending_op_builder_rejects_unknown_ops() {
        assert_eq!(
            build_pending_op_method(&ready_op("pin", Vec::new())).err(),
            Some(PendingOpBuildError::UnknownOp("pin".to_owned()))
        );
    }

    #[test]
    fn pending_op_builder_rejects_zero_delete_message_id() {
        let op = ReadyPendingOp {
            real_message_id: 0,
            ..ready_op(PENDING_OP_DELETE, Vec::new())
        };

        assert_eq!(
            build_pending_op_method(&op).err(),
            Some(PendingOpBuildError::Outbound(
                OutboundBuildError::MessageIdRequired
            ))
        );
    }
}
