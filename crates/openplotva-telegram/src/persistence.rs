use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    DispatcherDrain, DispatcherWorkItem, TelegramOutboundMethod, TelegramOutboundMethodKind,
};

/// Go Redis key used for dispatcher shutdown queue persistence.
pub const DEFAULT_DISPATCHER_QUEUE_KEY: &str = "plotva:message_queue";

/// Go dispatcher shutdown timeout before queue persistence is abandoned.
pub const DEFAULT_DISPATCHER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Error returned while converting queued dispatcher work into persistent items.
#[derive(Debug, Error)]
pub enum DispatcherPersistenceError {
    /// A concrete Telegram method failed to serialize to JSON.
    #[error("failed to serialize outbound Telegram method: {0}")]
    SerializeMethod(#[from] serde_json::Error),
    #[error("failed to format dispatcher enqueue timestamp: {0}")]
    FormatEnqueuedAt(#[from] time::error::Format),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PersistentDispatcherItem {
    /// JSON-encoded Telegram message/method payload, serialized as base64 like Go `[]byte`.
    #[serde(with = "go_byte_slice")]
    pub message: Vec<u8>,
    /// Go message config type string used by the queue loader.
    pub message_type: String,
    /// Whether this item came from the immediate queue.
    pub immediate: bool,
    /// Wall-clock enqueue time in RFC3339 form, matching Go `time.Time` JSON.
    pub enqueued_at: String,
    /// Dedupe fingerprint string.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub fingerprint: String,
    /// Telegram chat ID used by rate limiting and diagnostics.
    pub chat_id: i64,
    /// Virtual message ID, when the send path created one.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub virtual_id: String,
}

impl PersistentDispatcherItem {
    /// Return the Rust outbound method kind implied by the Go message type string.
    pub fn method_kind(&self) -> Option<TelegramOutboundMethodKind> {
        message_type_method_kind(&self.message_type)
    }

    fn from_work_item(
        item: DispatcherWorkItem,
    ) -> Result<Option<Self>, DispatcherPersistenceError> {
        let (metadata, method, persistence_payload) = item.into_persistence_parts();
        let Some(method) = method else {
            return Ok(None);
        };
        let method_kind = method.kind();
        let (message_type, message) = if let Some(payload) = persistence_payload {
            (payload.message_type, payload.message)
        } else {
            let Some(message) = serialize_outbound_method(&method)? else {
                return Ok(None);
            };
            (go_message_type(method_kind).to_owned(), message)
        };

        Ok(Some(Self {
            message,
            message_type,
            immediate: metadata.immediate,
            enqueued_at: format_system_time(metadata.enqueued_at)?,
            fingerprint: metadata.fingerprint_key,
            chat_id: metadata.chat_id,
            virtual_id: metadata.virtual_id,
        }))
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PersistentDispatcherQueue {
    /// Items ready to serialize and store under `DEFAULT_DISPATCHER_QUEUE_KEY`.
    pub items: Vec<PersistentDispatcherItem>,
    /// Number of drained items skipped because they had no serializable payload.
    pub skipped: usize,
}

pub fn persistent_queue_from_drain(
    drain: DispatcherDrain,
    max_messages: usize,
) -> Result<PersistentDispatcherQueue, DispatcherPersistenceError> {
    let DispatcherDrain { immediate, regular } = drain;
    let mut queue = PersistentDispatcherQueue::default();

    for item in immediate.into_iter().chain(regular) {
        match PersistentDispatcherItem::from_work_item(item)? {
            Some(item) => queue.items.push(item),
            None => queue.skipped += 1,
        }
    }

    if queue.items.len() > max_messages {
        queue.items.truncate(max_messages);
    }

    Ok(queue)
}

fn serialize_outbound_method(
    method: &TelegramOutboundMethod,
) -> Result<Option<Vec<u8>>, serde_json::Error> {
    match method {
        TelegramOutboundMethod::SendMessage(method) => {
            serde_json::to_vec(method.as_ref()).map(Some)
        }
        TelegramOutboundMethod::EditMessageText(method) => {
            serde_json::to_vec(method.as_ref()).map(Some)
        }
        TelegramOutboundMethod::SendSticker(_)
        | TelegramOutboundMethod::SendPhoto(_)
        | TelegramOutboundMethod::SendAudio(_)
        | TelegramOutboundMethod::SendMediaGroup(_)
        | TelegramOutboundMethod::EditMessageMedia(_) => Ok(None),
    }
}

fn go_message_type(kind: TelegramOutboundMethodKind) -> &'static str {
    match kind {
        TelegramOutboundMethodKind::SendMessage => "*api.MessageConfig",
        TelegramOutboundMethodKind::SendSticker => "*api.StickerConfig",
        TelegramOutboundMethodKind::SendPhoto => "*api.PhotoConfig",
        TelegramOutboundMethodKind::SendAudio => "*api.AudioConfig",
        TelegramOutboundMethodKind::SendMediaGroup => "*api.MediaGroupConfig",
        TelegramOutboundMethodKind::EditMessageText => "*api.EditMessageTextConfig",
        TelegramOutboundMethodKind::EditMessageMedia => "*api.EditMessageMediaConfig",
    }
}

fn message_type_method_kind(message_type: &str) -> Option<TelegramOutboundMethodKind> {
    match message_type {
        "*tgbotapi.MessageConfig"
        | "*api.MessageConfig"
        | "tgbotapi.MessageConfig"
        | "api.MessageConfig" => Some(TelegramOutboundMethodKind::SendMessage),
        "*tgbotapi.StickerConfig"
        | "*api.StickerConfig"
        | "tgbotapi.StickerConfig"
        | "api.StickerConfig" => Some(TelegramOutboundMethodKind::SendSticker),
        "*tgbotapi.PhotoConfig"
        | "*api.PhotoConfig"
        | "tgbotapi.PhotoConfig"
        | "api.PhotoConfig" => Some(TelegramOutboundMethodKind::SendPhoto),
        "*tgbotapi.AudioConfig"
        | "*api.AudioConfig"
        | "tgbotapi.AudioConfig"
        | "api.AudioConfig" => Some(TelegramOutboundMethodKind::SendAudio),
        "*tgbotapi.MediaGroupConfig"
        | "*api.MediaGroupConfig"
        | "tgbotapi.MediaGroupConfig"
        | "api.MediaGroupConfig" => Some(TelegramOutboundMethodKind::SendMediaGroup),
        "*api.EditMessageTextConfig" | "api.EditMessageTextConfig" => {
            Some(TelegramOutboundMethodKind::EditMessageText)
        }
        "*api.EditMessageMediaConfig" | "api.EditMessageMediaConfig" => {
            Some(TelegramOutboundMethodKind::EditMessageMedia)
        }
        _ => None,
    }
}

fn format_system_time(value: SystemTime) -> Result<String, time::error::Format> {
    OffsetDateTime::from(value).format(&Rfc3339)
}

mod go_byte_slice {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    pub fn serialize<S>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&STANDARD.encode(value))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        STANDARD.decode(encoded).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use crate::{
        DEFAULT_DISPATCHER_QUEUE_KEY, DEFAULT_DISPATCHER_SHUTDOWN_TIMEOUT, DispatcherConfig,
        DispatcherMessage, DispatcherQueue, MESSAGE_TYPE_TEXT, MessageFingerprint,
        PersistentDispatcherItem, ReplyParametersPlan, StickerMessagePlan, TelegramOutboundMethod,
        TelegramOutboundMethodKind, hash_content, persistent_queue_from_drain,
    };

    fn text_message(chat_id: i64, text: &str, virtual_id: &str) -> DispatcherMessage {
        DispatcherMessage::new(
            MessageFingerprint {
                chat_id,
                message_type: MESSAGE_TYPE_TEXT.to_owned(),
                content_hash: hash_content(text),
                debounce_key: None,
            },
            virtual_id,
        )
    }

    fn text_method(chat_id: i64, text: &str) -> TelegramOutboundMethod {
        TelegramOutboundMethod::from(carapax::types::SendMessage::new(chat_id, text))
    }

    fn sticker_method(chat_id: i64, file_id: &str) -> TelegramOutboundMethod {
        TelegramOutboundMethod::from(carapax::types::SendSticker::new(
            chat_id,
            carapax::types::InputFile::file_id(file_id),
        ))
    }

    #[test]
    fn persistent_item_json_matches_go_byte_slice_contract() -> Result<(), serde_json::Error> {
        let raw_message = br#"{"chat_id":42,"text":"hello"}"#.to_vec();
        let item = PersistentDispatcherItem {
            message: raw_message.clone(),
            message_type: "*api.MessageConfig".to_owned(),
            immediate: true,
            enqueued_at: "2026-05-19T17:00:00Z".to_owned(),
            fingerprint: "42:text:abcd".to_owned(),
            chat_id: 42,
            virtual_id: "vmsg-1".to_owned(),
        };

        let value = serde_json::to_value(vec![item])?;

        assert_eq!(
            value,
            json!([{
                "message": "eyJjaGF0X2lkIjo0MiwidGV4dCI6ImhlbGxvIn0=",
                "message_type": "*api.MessageConfig",
                "immediate": true,
                "enqueued_at": "2026-05-19T17:00:00Z",
                "fingerprint": "42:text:abcd",
                "chat_id": 42,
                "virtual_id": "vmsg-1"
            }])
        );

        let decoded: Vec<PersistentDispatcherItem> = serde_json::from_value(value)?;
        assert_eq!(decoded[0].message, raw_message);
        Ok(())
    }

    #[test]
    fn drain_persistence_uses_go_key_defaults_and_truncates_after_immediate_first_order()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DEFAULT_DISPATCHER_QUEUE_KEY, "plotva:message_queue");
        assert_eq!(
            DEFAULT_DISPATCHER_SHUTDOWN_TIMEOUT,
            std::time::Duration::from_secs(10)
        );

        let queue = DispatcherQueue::new(DispatcherConfig::default());
        queue.enqueue(
            text_message(42, "regular first", "regular-1")
                .with_method(text_method(42, "regular first")),
            false,
        );
        queue.enqueue(
            text_message(43, "immediate first", "immediate-1")
                .with_method(text_method(43, "immediate first")),
            true,
        );
        queue.enqueue(
            text_message(44, "regular second", "regular-2")
                .with_method(text_method(44, "regular second")),
            false,
        );

        let persisted = persistent_queue_from_drain(queue.drain_for_shutdown(), 2)?;

        assert_eq!(persisted.skipped, 0);
        assert_eq!(persisted.items.len(), 2);
        assert_eq!(persisted.items[0].virtual_id, "immediate-1");
        assert!(persisted.items[0].immediate);
        assert_eq!(persisted.items[0].chat_id, 43);
        assert_eq!(persisted.items[0].message_type, "*api.MessageConfig");
        assert_eq!(persisted.items[1].virtual_id, "regular-1");
        assert!(!persisted.items[1].immediate);
        assert_eq!(persisted.items[1].chat_id, 42);

        let payload: Value = serde_json::from_slice(&persisted.items[0].message)?;
        assert_eq!(payload["chat_id"], json!(43));
        assert_eq!(payload["text"], json!("immediate first"));
        assert!(!persisted.items[0].enqueued_at.is_empty());
        Ok(())
    }

    #[test]
    fn drain_persistence_skips_items_without_payloads_like_go_save_queue_skips_encode_failures()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        queue.enqueue(text_message(42, "missing", "missing-method"), true);
        queue.enqueue(
            text_message(43, "kept", "kept").with_method(text_method(43, "kept")),
            false,
        );

        let persisted = persistent_queue_from_drain(queue.drain_for_shutdown(), 100)?;

        assert_eq!(persisted.skipped, 1);
        assert_eq!(persisted.items.len(), 1);
        assert_eq!(persisted.items[0].virtual_id, "kept");
        assert_eq!(
            persisted.items[0].method_kind(),
            Some(TelegramOutboundMethodKind::SendMessage)
        );
        Ok(())
    }

    #[test]
    fn explicit_persistence_payload_keeps_form_backed_methods()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        queue.enqueue(
            text_message(42, "sticker", "sticker-1")
                .with_method(sticker_method(42, "sticker-file-id"))
                .with_persistence_payload(
                    StickerMessagePlan {
                        chat_id: 42,
                        file_id: "sticker-file-id".to_owned(),
                        message_thread_id: None,
                        disable_notification: true,
                        reply_parameters: Some(ReplyParametersPlan {
                            message_id: 7,
                            chat_id: 42,
                            allow_sending_without_reply: true,
                        }),
                    }
                    .to_persistence_payload()?,
                ),
            false,
        );

        let persisted = persistent_queue_from_drain(queue.drain_for_shutdown(), 100)?;

        assert_eq!(persisted.skipped, 0);
        assert_eq!(persisted.items.len(), 1);
        assert_eq!(persisted.items[0].message_type, "*api.StickerConfig");
        assert_eq!(
            persisted.items[0].method_kind(),
            Some(TelegramOutboundMethodKind::SendSticker)
        );
        let payload: Value = serde_json::from_slice(&persisted.items[0].message)?;
        assert_eq!(payload["ChatID"], json!(42));
        assert_eq!(payload["File"], json!("sticker-file-id"));
        assert_eq!(payload["DisableNotification"], json!(true));
        assert_eq!(
            payload["ReplyParameters"]["allow_sending_without_reply"],
            json!(true)
        );
        Ok(())
    }
}
