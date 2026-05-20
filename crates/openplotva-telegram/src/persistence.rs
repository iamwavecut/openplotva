use std::time::{Duration, SystemTime};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use carapax::types::{DeleteMessage, EditMessageText, ReplyMarkup, ReplyParameters, SendMessage};
use redis::Client as RedisClient;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    AudioMessagePlan, AudioSource, DispatcherDrain, DispatcherPersistencePayload, DispatcherQueue,
    DispatcherRestoredMessage, DispatcherWorkItem, EditMediaMessagePlan, EnqueueOutcome,
    MediaGroupMessagePlan, MediaGroupPhotoItem, MessageFingerprint, PhotoMessagePlan, PhotoSource,
    ReplyParametersPlan, StickerMessagePlan, TelegramOutboundMethod, TelegramOutboundMethodKind,
    hash_content, parse_mode_from_go,
};

/// Go Redis key used for dispatcher shutdown queue persistence.
pub const DEFAULT_DISPATCHER_QUEUE_KEY: &str = "plotva:message_queue";

/// Go dispatcher shutdown timeout before queue persistence is abandoned.
pub const DEFAULT_DISPATCHER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Error returned while saving, loading, or converting dispatcher queue persistence.
#[derive(Debug, Error)]
pub enum DispatcherPersistenceError {
    /// A persistence payload failed to serialize to JSON.
    #[error("failed to serialize dispatcher queue JSON: {0}")]
    SerializeJson(#[from] serde_json::Error),
    /// Persistent queue JSON could not be decoded.
    #[error("failed to decode persistent dispatcher queue: {0}")]
    DeserializeQueue(serde_json::Error),
    /// Redis command failed.
    #[error("dispatcher queue Redis operation failed: {0}")]
    Redis(#[from] redis::RedisError),
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
    /// Rust-native preservation of Go `BypassChatRestrictions` after enqueue-time checks.
    #[serde(default, skip_serializing_if = "is_false")]
    pub bypass_chat_restrictions: bool,
}

impl PersistentDispatcherItem {
    /// Return the Rust outbound method kind implied by the Go message type string.
    pub fn method_kind(&self) -> Option<TelegramOutboundMethodKind> {
        message_type_method_kind(&self.message_type)
    }

    fn from_work_item(
        item: DispatcherWorkItem,
    ) -> Result<Option<Self>, DispatcherPersistenceError> {
        let bypass_chat_restrictions = item.bypasses_chat_restrictions();
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
            bypass_chat_restrictions,
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

/// Result of decoding persistent queue storage into dispatcher replay items.
#[derive(Debug, Default)]
pub struct PersistentDispatcherReplay {
    /// Items ready to restore into the dispatcher queue.
    pub items: Vec<DispatcherRestoredMessage>,
    /// Number of persistent items skipped because Go would fail to decode them.
    pub skipped: usize,
}

/// Result of applying a persistent replay to a dispatcher queue.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PersistentDispatcherRestoreReport {
    /// Number of decoded replay items considered for restore.
    pub loaded: usize,
    /// Number of replay items inserted into the dispatcher queue.
    pub restored: usize,
    pub deduped: usize,
    /// Number of stored items skipped during replay decode.
    pub skipped: usize,
}

/// Redis-backed store for the Go dispatcher queue persistence key.
#[derive(Clone, Debug)]
pub struct RedisDispatcherQueueStore {
    client: RedisClient,
    key: String,
    max_messages: usize,
}

impl RedisDispatcherQueueStore {
    /// Create a store using the Go dispatcher queue key and max-message cap.
    pub fn new(client: RedisClient, key: impl Into<String>, max_messages: usize) -> Self {
        Self {
            client,
            key: key.into(),
            max_messages,
        }
    }

    /// Return the Redis key this store reads and writes.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Load Redis replay data into the dispatcher queue using Go startup semantics.
    pub async fn load_into_queue(
        &self,
        queue: &DispatcherQueue,
    ) -> Result<PersistentDispatcherRestoreReport, DispatcherPersistenceError> {
        let Some(replay) = self.load_replay().await? else {
            return Ok(PersistentDispatcherRestoreReport::default());
        };
        Ok(restore_persistent_queue_replay(queue, replay))
    }

    pub async fn save_drain(
        &self,
        drain: DispatcherDrain,
    ) -> Result<PersistentDispatcherQueue, DispatcherPersistenceError> {
        if drain.is_empty() {
            return Ok(PersistentDispatcherQueue::default());
        }
        let queue = persistent_queue_from_drain(drain, self.max_messages)?;
        self.save_queue(&queue).await?;
        Ok(queue)
    }

    /// Drain the dispatcher queue and persist it with Go graceful-shutdown semantics.
    pub async fn save_queue_on_shutdown(
        &self,
        queue: &DispatcherQueue,
    ) -> Result<PersistentDispatcherQueue, DispatcherPersistenceError> {
        self.save_drain(queue.drain_for_shutdown()).await
    }

    /// Persist already converted queue items to Redis using the Rust-native JSON wire value.
    pub async fn save_queue(
        &self,
        queue: &PersistentDispatcherQueue,
    ) -> Result<(), DispatcherPersistenceError> {
        let value = persistent_queue_redis_value_from_items(&queue.items)?;
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let _: () = redis::cmd("SET")
            .arg(&self.key)
            .arg(value)
            .query_async(&mut connection)
            .await?;
        Ok(())
    }

    /// Load replayable queue items from Redis and delete the key like Go `LoadQueue`.
    pub async fn load_replay(
        &self,
    ) -> Result<Option<PersistentDispatcherReplay>, DispatcherPersistenceError> {
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(&self.key)
            .query_async(&mut connection)
            .await?;
        let Some(value) = value else {
            return Ok(None);
        };

        let replay = persistent_queue_replay_from_redis_value(&value)?;
        let _ignored_delete_result: redis::RedisResult<i64> = redis::cmd("DEL")
            .arg(&self.key)
            .query_async(&mut connection)
            .await;
        Ok(Some(replay))
    }
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

pub fn persistent_queue_replay_from_json(
    data: &[u8],
) -> Result<PersistentDispatcherReplay, DispatcherPersistenceError> {
    let items: Vec<PersistentDispatcherItem> =
        serde_json::from_slice(data).map_err(DispatcherPersistenceError::DeserializeQueue)?;
    Ok(persistent_queue_replay_from_items(items))
}

/// Encode converted persistent items as the Rust-native Redis JSON value.
pub fn persistent_queue_redis_value_from_items(
    items: &[PersistentDispatcherItem],
) -> Result<Vec<u8>, DispatcherPersistenceError> {
    Ok(serde_json::to_vec(items)?)
}

/// Decode a Rust-native Redis JSON value and replay it as persistent dispatcher items.
pub fn persistent_queue_replay_from_redis_value(
    value: &[u8],
) -> Result<PersistentDispatcherReplay, DispatcherPersistenceError> {
    persistent_queue_replay_from_json(value)
}

/// Apply decoded replay items to a dispatcher queue like Go `Dispatcher.Start`.
pub fn restore_persistent_queue_replay(
    queue: &DispatcherQueue,
    replay: PersistentDispatcherReplay,
) -> PersistentDispatcherRestoreReport {
    let loaded = replay.items.len();
    let mut report = PersistentDispatcherRestoreReport {
        loaded,
        skipped: replay.skipped,
        ..PersistentDispatcherRestoreReport::default()
    };

    for item in replay.items {
        match queue.restore(item) {
            EnqueueOutcome::Enqueued => report.restored += 1,
            EnqueueOutcome::Deduped => report.deduped += 1,
        }
    }

    report
}

/// Convert decoded persistent items into replayable dispatcher items.
pub fn persistent_queue_replay_from_items(
    items: Vec<PersistentDispatcherItem>,
) -> PersistentDispatcherReplay {
    let mut replay = PersistentDispatcherReplay::default();
    for item in items {
        match replay_item_from_persistent(item) {
            Some(item) => replay.items.push(item),
            None => replay.skipped += 1,
        }
    }
    replay
}

fn replay_item_from_persistent(
    item: PersistentDispatcherItem,
) -> Option<DispatcherRestoredMessage> {
    let method_kind = item.method_kind()?;
    let value: Value = serde_json::from_slice(&item.message).ok()?;
    let method = replay_method_from_value(method_kind, &value)?;
    let fingerprint = parse_fingerprint_key(&item.fingerprint)
        .unwrap_or_else(|| fingerprint_from_value(method_kind, &value, item.chat_id));
    let enqueued_at = parse_system_time(&item.enqueued_at)?;
    let persistence_payload =
        DispatcherPersistencePayload::new(item.message_type.clone(), item.message.clone());

    Some(DispatcherRestoredMessage {
        fingerprint,
        fingerprint_key: item.fingerprint,
        virtual_id: item.virtual_id,
        immediate: item.immediate,
        enqueued_at,
        method,
        persistence_payload: Some(persistence_payload),
        bypass_chat_restrictions: item.bypass_chat_restrictions,
    })
}

fn replay_method_from_value(
    method_kind: TelegramOutboundMethodKind,
    value: &Value,
) -> Option<TelegramOutboundMethod> {
    match method_kind {
        TelegramOutboundMethodKind::SendMessage => replay_text_method(value),
        TelegramOutboundMethodKind::SendSticker => replay_sticker_method(value),
        TelegramOutboundMethodKind::SendPhoto => replay_photo_method(value),
        TelegramOutboundMethodKind::SendAudio => replay_audio_method(value),
        TelegramOutboundMethodKind::SendMediaGroup => replay_media_group_method(value),
        TelegramOutboundMethodKind::SendChatAction => None,
        TelegramOutboundMethodKind::AnswerCallbackQuery => None,
        TelegramOutboundMethodKind::AnswerInlineQuery => None,
        TelegramOutboundMethodKind::AnswerGuestQuery => None,
        TelegramOutboundMethodKind::AnswerPreCheckoutQuery => None,
        TelegramOutboundMethodKind::CreateInvoiceLink => None,
        TelegramOutboundMethodKind::RefundStarPayment => None,
        TelegramOutboundMethodKind::EditUserStarSubscription => None,
        TelegramOutboundMethodKind::EditMessageText => replay_edit_text_method(value),
        TelegramOutboundMethodKind::EditMessageCaption
        | TelegramOutboundMethodKind::EditMessageReplyMarkup => None,
        TelegramOutboundMethodKind::EditMessageMedia => replay_edit_media_method(value),
        TelegramOutboundMethodKind::DeleteMessage => replay_delete_method(value),
    }
}

fn replay_text_method(value: &Value) -> Option<TelegramOutboundMethod> {
    let chat_id = field_i64(value, &["ChatID", "chat_id"])?;
    let text = field_string(value, &["Text", "text"])?;
    let mut method = SendMessage::new(chat_id, text);
    if field_bool(value, &["DisableNotification", "disable_notification"]).unwrap_or(false) {
        method = method.with_disable_notification(true);
    }
    if let Some(thread_id) = field_i64(value, &["MessageThreadID", "message_thread_id"])
        .filter(|thread_id| *thread_id != 0)
    {
        method = method.with_message_thread_id(thread_id);
    }
    if let Some(parse_mode) = field_string(value, &["ParseMode", "parse_mode"])
        .and_then(|mode| parse_mode_from_go(&mode).ok())
        .flatten()
    {
        method = method.with_parse_mode(parse_mode);
    }
    if let Some(reply) = reply_parameters_plan(value, chat_id) {
        method = method.with_reply_parameters(reply_parameters(reply));
    }
    if let Some(markup) = reply_markup(value) {
        method = method.with_reply_markup(markup);
    }
    Some(TelegramOutboundMethod::from(method))
}

fn replay_edit_text_method(value: &Value) -> Option<TelegramOutboundMethod> {
    let chat_id = field_i64(value, &["ChatID", "chat_id"])?;
    let message_id = field_i64(value, &["MessageID", "message_id"])?;
    let text = field_string(value, &["Text", "text"])?;
    let mut method = EditMessageText::for_chat_message(chat_id, message_id, text);
    if let Some(parse_mode) = field_string(value, &["ParseMode", "parse_mode"])
        .and_then(|mode| parse_mode_from_go(&mode).ok())
        .flatten()
    {
        method = method.with_parse_mode(parse_mode);
    }
    Some(TelegramOutboundMethod::from(method))
}

fn replay_delete_method(value: &Value) -> Option<TelegramOutboundMethod> {
    let chat_id = field_i64(value, &["ChatID", "chat_id"])?;
    let message_id = field_i64(value, &["MessageID", "message_id"])?;
    Some(TelegramOutboundMethod::from(DeleteMessage::new(
        chat_id, message_id,
    )))
}

fn replay_sticker_method(value: &Value) -> Option<TelegramOutboundMethod> {
    let chat_id = field_i64(value, &["ChatID", "chat_id"])?;
    let plan = StickerMessagePlan {
        chat_id,
        file_id: string_file_value(field_value(value, &["File", "sticker"])?)?,
        message_thread_id: non_zero_field_i64(value, &["MessageThreadID", "message_thread_id"]),
        disable_notification: field_bool(value, &["DisableNotification", "disable_notification"])
            .unwrap_or(false),
        reply_parameters: reply_parameters_plan(value, chat_id),
    };
    plan.to_carapax().ok().map(TelegramOutboundMethod::from)
}

fn replay_photo_method(value: &Value) -> Option<TelegramOutboundMethod> {
    let chat_id = field_i64(value, &["ChatID", "chat_id"])?;
    let plan = PhotoMessagePlan {
        chat_id,
        photo: photo_source_value(field_value(value, &["File", "photo"])?)?,
        message_thread_id: non_zero_field_i64(value, &["MessageThreadID", "message_thread_id"]),
        disable_notification: field_bool(value, &["DisableNotification", "disable_notification"])
            .unwrap_or(false),
        caption: field_string(value, &["Caption", "caption"]).unwrap_or_default(),
        render_as: field_string(value, &["ParseMode", "parse_mode"]).unwrap_or_default(),
        has_spoiler: field_bool(value, &["HasSpoiler", "has_spoiler"]).unwrap_or(false),
        reply_parameters: reply_parameters_plan(value, chat_id),
    };
    plan.to_carapax().ok().map(TelegramOutboundMethod::from)
}

fn replay_audio_method(value: &Value) -> Option<TelegramOutboundMethod> {
    let chat_id = field_i64(value, &["ChatID", "chat_id"])?;
    let plan = AudioMessagePlan {
        chat_id,
        audio: audio_source_value(field_value(value, &["File", "audio"])?)?,
        message_thread_id: non_zero_field_i64(value, &["MessageThreadID", "message_thread_id"]),
        disable_notification: field_bool(value, &["DisableNotification", "disable_notification"])
            .unwrap_or(false),
        caption: field_string(value, &["Caption", "caption"]).unwrap_or_default(),
        render_as: field_string(value, &["ParseMode", "parse_mode"]).unwrap_or_default(),
        reply_parameters: reply_parameters_plan(value, chat_id),
    };
    plan.to_carapax().ok().map(TelegramOutboundMethod::from)
}

fn replay_media_group_method(value: &Value) -> Option<TelegramOutboundMethod> {
    let chat_id = field_i64(value, &["ChatID", "chat_id"])?;
    let media = field_value(value, &["Media", "media"])?.as_array()?;
    let plan = MediaGroupMessagePlan {
        chat_id,
        message_thread_id: non_zero_field_i64(value, &["MessageThreadID", "message_thread_id"]),
        disable_notification: field_bool(value, &["DisableNotification", "disable_notification"])
            .unwrap_or(false),
        items: media
            .iter()
            .map(media_group_photo_item)
            .collect::<Option<Vec<_>>>()?,
        reply_parameters: reply_parameters_plan(value, chat_id),
    };
    plan.to_carapax().ok().map(TelegramOutboundMethod::from)
}

fn replay_edit_media_method(value: &Value) -> Option<TelegramOutboundMethod> {
    let chat_id = field_i64(value, &["ChatID", "chat_id"])?;
    let message_id = field_i64(value, &["MessageID", "message_id"])?;
    let plan = EditMediaMessagePlan {
        chat_id,
        message_id,
        media: media_group_photo_item(field_value(value, &["Media", "media"])?)?,
    };
    plan.to_carapax().ok().map(TelegramOutboundMethod::from)
}

fn media_group_photo_item(value: &Value) -> Option<MediaGroupPhotoItem> {
    if field_string(value, &["type"]).as_deref() != Some("photo") {
        return None;
    }
    Some(MediaGroupPhotoItem {
        photo: photo_source_value(field_value(value, &["media"])?)?,
        caption: field_string(value, &["caption"]).unwrap_or_default(),
        render_as: field_string(value, &["parse_mode"]).unwrap_or_default(),
        has_spoiler: field_bool(value, &["has_spoiler"]).unwrap_or(false),
    })
}

fn fingerprint_from_value(
    method_kind: TelegramOutboundMethodKind,
    value: &Value,
    fallback_chat_id: i64,
) -> MessageFingerprint {
    let chat_id = field_i64(value, &["ChatID", "chat_id"]).unwrap_or(fallback_chat_id);
    match method_kind {
        TelegramOutboundMethodKind::SendMessage => MessageFingerprint {
            chat_id,
            message_type: "text".to_owned(),
            content_hash: hash_content(&field_string(value, &["Text", "text"]).unwrap_or_default()),
            debounce_key: None,
        },
        TelegramOutboundMethodKind::SendSticker => {
            file_fingerprint(chat_id, "sticker", field_value(value, &["File", "sticker"]))
        }
        TelegramOutboundMethodKind::SendPhoto => {
            file_fingerprint(chat_id, "photo", field_value(value, &["File", "photo"]))
        }
        TelegramOutboundMethodKind::SendAudio => {
            file_fingerprint(chat_id, "audio", field_value(value, &["File", "audio"]))
        }
        TelegramOutboundMethodKind::SendMediaGroup => MessageFingerprint {
            chat_id,
            message_type: "media_group".to_owned(),
            content_hash: hash_content(&value.to_string()),
            debounce_key: None,
        },
        TelegramOutboundMethodKind::SendChatAction
        | TelegramOutboundMethodKind::AnswerCallbackQuery
        | TelegramOutboundMethodKind::AnswerInlineQuery
        | TelegramOutboundMethodKind::AnswerGuestQuery
        | TelegramOutboundMethodKind::AnswerPreCheckoutQuery
        | TelegramOutboundMethodKind::CreateInvoiceLink
        | TelegramOutboundMethodKind::RefundStarPayment
        | TelegramOutboundMethodKind::EditUserStarSubscription
        | TelegramOutboundMethodKind::EditMessageText
        | TelegramOutboundMethodKind::EditMessageCaption
        | TelegramOutboundMethodKind::EditMessageReplyMarkup
        | TelegramOutboundMethodKind::EditMessageMedia
        | TelegramOutboundMethodKind::DeleteMessage => MessageFingerprint {
            chat_id,
            message_type: "unknown".to_owned(),
            content_hash: hash_content(&value.to_string()),
            debounce_key: None,
        },
    }
}

fn file_fingerprint(chat_id: i64, message_type: &str, value: Option<&Value>) -> MessageFingerprint {
    MessageFingerprint {
        chat_id,
        message_type: message_type.to_owned(),
        content_hash: hash_content(
            &value
                .and_then(file_fingerprint_content)
                .unwrap_or_else(|| "<nil>".to_owned()),
        ),
        debounce_key: None,
    }
}

fn parse_fingerprint_key(value: &str) -> Option<MessageFingerprint> {
    let mut parts = value.splitn(4, ':');
    let chat_id = parts.next()?.parse().ok()?;
    let message_type = parts.next()?.to_owned();
    let content_hash = u32::from_str_radix(parts.next()?, 16).ok()?;
    let debounce_key = parts
        .next()
        .map(str::to_owned)
        .filter(|part| !part.is_empty());
    Some(MessageFingerprint {
        chat_id,
        message_type,
        content_hash,
        debounce_key,
    })
}

fn parse_system_time(value: &str) -> Option<SystemTime> {
    OffsetDateTime::parse(value, &Rfc3339)
        .ok()
        .map(SystemTime::from)
}

fn reply_parameters_plan(value: &Value, default_chat_id: i64) -> Option<ReplyParametersPlan> {
    let reply = field_value(value, &["ReplyParameters", "reply_parameters"])?;
    let message_id = field_i64(reply, &["message_id"])?;
    if message_id == 0 {
        return None;
    }
    Some(ReplyParametersPlan {
        message_id,
        chat_id: field_i64(reply, &["chat_id"]).unwrap_or(default_chat_id),
        allow_sending_without_reply: field_bool(reply, &["allow_sending_without_reply"])
            .unwrap_or(false),
    })
}

fn reply_parameters(plan: ReplyParametersPlan) -> ReplyParameters {
    let mut params = ReplyParameters::new(plan.message_id).with_chat_id(plan.chat_id);
    if plan.allow_sending_without_reply {
        params = params.with_allow_sending_without_reply(true);
    }
    params
}

fn reply_markup(value: &Value) -> Option<ReplyMarkup> {
    let value = field_value(value, &["ReplyMarkup", "reply_markup"])?;
    if value.is_null() {
        return None;
    }
    serde_json::from_value(value.clone()).ok()
}

fn photo_source_value(value: &Value) -> Option<PhotoSource> {
    match file_source_value(value)? {
        ParsedFileSource::String(value) if is_url(&value) => Some(PhotoSource::Url(value)),
        ParsedFileSource::String(value) => Some(PhotoSource::FileId(value)),
        ParsedFileSource::Bytes { file_name, bytes } => {
            Some(PhotoSource::Bytes { file_name, bytes })
        }
    }
}

fn audio_source_value(value: &Value) -> Option<AudioSource> {
    match file_source_value(value)? {
        ParsedFileSource::String(value) if is_url(&value) => Some(AudioSource::Url(value)),
        ParsedFileSource::String(value) => Some(AudioSource::FileId(value)),
        ParsedFileSource::Bytes { file_name, bytes } => {
            Some(AudioSource::Bytes { file_name, bytes })
        }
    }
}

fn string_file_value(value: &Value) -> Option<String> {
    match file_source_value(value)? {
        ParsedFileSource::String(value) => Some(value),
        ParsedFileSource::Bytes { .. } => None,
    }
}

enum ParsedFileSource {
    String(String),
    Bytes { file_name: String, bytes: Vec<u8> },
}

fn file_source_value(value: &Value) -> Option<ParsedFileSource> {
    if let Some(value) = value.as_str() {
        return Some(ParsedFileSource::String(value.to_owned()));
    }
    let file_name = field_string(value, &["Name", "name"])?;
    let bytes = field_string(value, &["Bytes", "bytes"])
        .and_then(|encoded| BASE64_STANDARD.decode(encoded).ok())?;
    Some(ParsedFileSource::Bytes { file_name, bytes })
}

fn file_fingerprint_content(value: &Value) -> Option<String> {
    match file_source_value(value)? {
        ParsedFileSource::String(value) => Some(value),
        ParsedFileSource::Bytes { file_name, bytes } => {
            Some(format_go_file_bytes(&file_name, &bytes))
        }
    }
}

fn field_value<'a>(value: &'a Value, names: &[&str]) -> Option<&'a Value> {
    names.iter().find_map(|name| value.get(*name))
}

fn field_string(value: &Value, names: &[&str]) -> Option<String> {
    field_value(value, names).and_then(|value| value.as_str().map(str::to_owned))
}

fn field_i64(value: &Value, names: &[&str]) -> Option<i64> {
    field_value(value, names).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
    })
}

fn non_zero_field_i64(value: &Value, names: &[&str]) -> Option<i64> {
    field_i64(value, names).filter(|value| *value != 0)
}

fn field_bool(value: &Value, names: &[&str]) -> Option<bool> {
    field_value(value, names).and_then(|value| {
        value
            .as_bool()
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
    })
}

fn is_url(value: &str) -> bool {
    value.starts_with("http://") || value.starts_with("https://")
}

fn format_go_file_bytes(file_name: &str, bytes: &[u8]) -> String {
    let mut formatted = String::new();
    formatted.push('{');
    formatted.push_str(file_name);
    formatted.push_str(" [");
    for (idx, byte) in bytes.iter().enumerate() {
        if idx > 0 {
            formatted.push(' ');
        }
        formatted.push_str(&byte.to_string());
    }
    formatted.push_str("]}");
    formatted
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
        | TelegramOutboundMethod::SendChatAction(_)
        | TelegramOutboundMethod::AnswerCallbackQuery(_)
        | TelegramOutboundMethod::AnswerInlineQuery(_)
        | TelegramOutboundMethod::AnswerGuestQuery(_)
        | TelegramOutboundMethod::AnswerPreCheckoutQuery(_)
        | TelegramOutboundMethod::CreateInvoiceLink(_)
        | TelegramOutboundMethod::RefundStarPayment(_)
        | TelegramOutboundMethod::EditUserStarSubscription(_)
        | TelegramOutboundMethod::EditMessageCaption(_)
        | TelegramOutboundMethod::EditMessageReplyMarkup(_)
        | TelegramOutboundMethod::EditMessageMedia(_)
        | TelegramOutboundMethod::DeleteMessage(_) => Ok(None),
    }
}

fn go_message_type(kind: TelegramOutboundMethodKind) -> &'static str {
    match kind {
        TelegramOutboundMethodKind::SendMessage => "*api.MessageConfig",
        TelegramOutboundMethodKind::SendSticker => "*api.StickerConfig",
        TelegramOutboundMethodKind::SendPhoto => "*api.PhotoConfig",
        TelegramOutboundMethodKind::SendAudio => "*api.AudioConfig",
        TelegramOutboundMethodKind::SendMediaGroup => "*api.MediaGroupConfig",
        TelegramOutboundMethodKind::SendChatAction => "*api.ChatActionConfig",
        TelegramOutboundMethodKind::AnswerCallbackQuery => "*api.CallbackConfig",
        TelegramOutboundMethodKind::AnswerInlineQuery => "api.InlineConfig",
        TelegramOutboundMethodKind::AnswerGuestQuery => "api.AnswerGuestQueryConfig",
        TelegramOutboundMethodKind::AnswerPreCheckoutQuery => "api.PreCheckoutConfig",
        TelegramOutboundMethodKind::CreateInvoiceLink => "api.CreateInvoiceLinkConfig",
        TelegramOutboundMethodKind::RefundStarPayment => "api.RefundStarPaymentConfig",
        TelegramOutboundMethodKind::EditUserStarSubscription => {
            "api.EditUserStarSubscriptionConfig"
        }
        TelegramOutboundMethodKind::EditMessageText => "*api.EditMessageTextConfig",
        TelegramOutboundMethodKind::EditMessageCaption => "*api.EditMessageCaptionConfig",
        TelegramOutboundMethodKind::EditMessageReplyMarkup => "*api.EditMessageReplyMarkupConfig",
        TelegramOutboundMethodKind::EditMessageMedia => "*api.EditMessageMediaConfig",
        TelegramOutboundMethodKind::DeleteMessage => "*api.DeleteMessageConfig",
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
        "*tgbotapi.ChatActionConfig"
        | "*api.ChatActionConfig"
        | "tgbotapi.ChatActionConfig"
        | "api.ChatActionConfig" => Some(TelegramOutboundMethodKind::SendChatAction),
        "*tgbotapi.CallbackConfig"
        | "*api.CallbackConfig"
        | "tgbotapi.CallbackConfig"
        | "api.CallbackConfig" => Some(TelegramOutboundMethodKind::AnswerCallbackQuery),
        "api.InlineConfig" | "tgbotapi.InlineConfig" => {
            Some(TelegramOutboundMethodKind::AnswerInlineQuery)
        }
        "api.AnswerGuestQueryConfig" | "tgbotapi.AnswerGuestQueryConfig" => {
            Some(TelegramOutboundMethodKind::AnswerGuestQuery)
        }
        "*api.EditMessageTextConfig" | "api.EditMessageTextConfig" => {
            Some(TelegramOutboundMethodKind::EditMessageText)
        }
        "*api.EditMessageCaptionConfig" | "api.EditMessageCaptionConfig" => {
            Some(TelegramOutboundMethodKind::EditMessageCaption)
        }
        "*api.EditMessageReplyMarkupConfig" | "api.EditMessageReplyMarkupConfig" => {
            Some(TelegramOutboundMethodKind::EditMessageReplyMarkup)
        }
        "*api.EditMessageMediaConfig" | "api.EditMessageMediaConfig" => {
            Some(TelegramOutboundMethodKind::EditMessageMedia)
        }
        "*tgbotapi.DeleteMessageConfig"
        | "*api.DeleteMessageConfig"
        | "tgbotapi.DeleteMessageConfig"
        | "api.DeleteMessageConfig" => Some(TelegramOutboundMethodKind::DeleteMessage),
        _ => None,
    }
}

fn format_system_time(value: SystemTime) -> Result<String, time::error::Format> {
    OffsetDateTime::from(value).format(&Rfc3339)
}

fn is_false(value: &bool) -> bool {
    !*value
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
        AudioMessagePlan, AudioSource, DEFAULT_DISPATCHER_QUEUE_KEY,
        DEFAULT_DISPATCHER_SHUTDOWN_TIMEOUT, DebouncerConfig, DispatcherConfig, DispatcherMessage,
        DispatcherPersistenceError, DispatcherQueue, DispatcherRestoredMessage,
        EditMediaMessagePlan, EnqueueOutcome, MESSAGE_TYPE_TEXT, MediaGroupMessagePlan,
        MediaGroupPhotoItem, MessageFingerprint, PersistentDispatcherItem,
        PersistentDispatcherReplay, PhotoMessagePlan, PhotoSource, ReplyParametersPlan,
        StickerMessagePlan, TELEGRAM_PARSE_MODE_HTML, TelegramOutboundMethod,
        TelegramOutboundMethodKind, hash_content, persistent_queue_from_drain,
        persistent_queue_redis_value_from_items, persistent_queue_replay_from_json,
        persistent_queue_replay_from_redis_value, restore_persistent_queue_replay,
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

    fn restored_text(
        chat_id: i64,
        text: &str,
        virtual_id: &str,
        immediate: bool,
    ) -> DispatcherRestoredMessage {
        let fingerprint = MessageFingerprint {
            chat_id,
            message_type: MESSAGE_TYPE_TEXT.to_owned(),
            content_hash: hash_content(text),
            debounce_key: None,
        };
        DispatcherRestoredMessage {
            fingerprint: fingerprint.clone(),
            fingerprint_key: fingerprint.to_string(),
            virtual_id: virtual_id.to_owned(),
            immediate,
            enqueued_at: std::time::SystemTime::now(),
            method: text_method(chat_id, text),
            persistence_payload: None,
            bypass_chat_restrictions: false,
        }
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
            bypass_chat_restrictions: false,
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

    #[test]
    fn bypass_chat_restrictions_survives_drain_and_replay() -> Result<(), Box<dyn std::error::Error>>
    {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        queue.enqueue(
            text_message(42, "bypass", "bypass-1")
                .with_method(text_method(42, "bypass"))
                .with_bypass_chat_restrictions(true),
            false,
        );

        let persisted = persistent_queue_from_drain(queue.drain_for_shutdown(), 100)?;

        assert_eq!(persisted.skipped, 0);
        assert_eq!(persisted.items.len(), 1);
        assert!(persisted.items[0].bypass_chat_restrictions);

        let raw = serde_json::to_vec(&persisted.items)?;
        let replay = persistent_queue_replay_from_json(&raw)?;
        let restored_queue = DispatcherQueue::new(DispatcherConfig::default());
        for item in replay.items {
            restored_queue.restore(item);
        }

        let restored = restored_queue
            .dequeue_regular()
            .expect("bypass item restored");
        assert!(restored.bypasses_chat_restrictions());
        Ok(())
    }

    #[test]
    fn replay_from_persisted_json_reconstructs_go_text_and_sticker_items()
    -> Result<(), Box<dyn std::error::Error>> {
        let persisted = vec![
            PersistentDispatcherItem {
                message: br#"{"ChatID":42,"MessageThreadID":77,"DisableNotification":true,"ReplyParameters":{"message_id":9,"chat_id":42,"allow_sending_without_reply":true},"Text":"<b>hello</b>","ParseMode":"HTML"}"#.to_vec(),
                message_type: "*api.MessageConfig".to_owned(),
                immediate: false,
                enqueued_at: "2026-05-19T17:00:00Z".to_owned(),
                fingerprint: "42:text:2ebc7fa6".to_owned(),
                chat_id: 42,
                virtual_id: "text-vmsg".to_owned(),
                bypass_chat_restrictions: false,
            },
            PersistentDispatcherItem {
                message: br#"{"ChatID":43,"DisableNotification":true,"ReplyParameters":{"message_id":7,"chat_id":43,"allow_sending_without_reply":true},"File":"sticker-file-id"}"#.to_vec(),
                message_type: "*api.StickerConfig".to_owned(),
                immediate: true,
                enqueued_at: "2026-05-19T17:00:01Z".to_owned(),
                fingerprint: "43:sticker:3783d445".to_owned(),
                chat_id: 43,
                virtual_id: "sticker-vmsg".to_owned(),
                bypass_chat_restrictions: false,
            },
        ];
        let raw = serde_json::to_vec(&persisted)?;

        let replay = persistent_queue_replay_from_json(&raw)?;

        assert_eq!(replay.skipped, 0);
        assert_eq!(replay.items.len(), 2);

        let queue = DispatcherQueue::new(DispatcherConfig::default());
        for item in replay.items {
            queue.restore(item);
        }

        let snapshot = queue.snapshot();
        assert_eq!(snapshot.regular[0].virtual_id, "text-vmsg");
        assert_eq!(snapshot.regular[0].fingerprint_key, "42:text:2ebc7fa6");
        assert_eq!(snapshot.immediate[0].virtual_id, "sticker-vmsg");
        assert_eq!(snapshot.immediate[0].fingerprint_key, "43:sticker:3783d445");

        let text = queue.dequeue_regular().expect("text item restored");
        assert_eq!(
            text.method_kind(),
            Some(TelegramOutboundMethodKind::SendMessage)
        );
        let (_, method, payload) = text.into_persistence_parts();
        let Some(TelegramOutboundMethod::SendMessage(method)) = method else {
            panic!("expected sendMessage method");
        };
        let method_payload = serde_json::to_value(method.as_ref())?;
        assert_eq!(method_payload["chat_id"], json!(42));
        assert_eq!(method_payload["message_thread_id"], json!(77));
        assert_eq!(method_payload["text"], json!("<b>hello</b>"));
        assert_eq!(method_payload["parse_mode"], json!("HTML"));
        assert_eq!(
            method_payload["reply_parameters"]["allow_sending_without_reply"],
            json!(true)
        );
        assert_eq!(
            payload.expect("text payload preserved").message_type,
            "*api.MessageConfig"
        );

        let sticker = queue.dequeue_immediate().expect("sticker item restored");
        assert_eq!(
            sticker.method_kind(),
            Some(TelegramOutboundMethodKind::SendSticker)
        );
        let (_, _, payload) = sticker.into_persistence_parts();
        assert_eq!(
            payload.expect("sticker payload preserved").message_type,
            "*api.StickerConfig"
        );

        Ok(())
    }

    #[test]
    fn replay_reconstructs_delete_message_configs_when_present()
    -> Result<(), Box<dyn std::error::Error>> {
        let persisted = vec![PersistentDispatcherItem {
            message: br#"{"ChatID":42,"MessageID":77}"#.to_vec(),
            message_type: "*api.DeleteMessageConfig".to_owned(),
            immediate: true,
            enqueued_at: "2026-05-19T17:00:01Z".to_owned(),
            fingerprint: "delete".to_owned(),
            chat_id: 42,
            virtual_id: "delete-vmsg".to_owned(),
            bypass_chat_restrictions: false,
        }];
        let raw = serde_json::to_vec(&persisted)?;

        let replay = persistent_queue_replay_from_json(&raw)?;

        assert_eq!(replay.skipped, 0);
        assert_eq!(replay.items.len(), 1);
        assert_eq!(
            replay.items[0].method.kind(),
            TelegramOutboundMethodKind::DeleteMessage
        );
        let TelegramOutboundMethod::DeleteMessage(method) = &replay.items[0].method else {
            panic!("expected deleteMessage method");
        };
        let payload = serde_json::to_value(method.as_ref())?;

        assert_eq!(payload["chat_id"], json!(42));
        assert_eq!(payload["message_id"], json!(77));

        Ok(())
    }

    #[test]
    fn replay_skips_unsupported_or_malformed_items_like_go_load_queue()
    -> Result<(), Box<dyn std::error::Error>> {
        let persisted = vec![
            PersistentDispatcherItem {
                message: br#"{"ChatID":42,"Text":"kept"}"#.to_vec(),
                message_type: "*api.MessageConfig".to_owned(),
                immediate: false,
                enqueued_at: "2026-05-19T17:00:00Z".to_owned(),
                fingerprint: String::new(),
                chat_id: 42,
                virtual_id: "kept".to_owned(),
                bypass_chat_restrictions: false,
            },
            PersistentDispatcherItem {
                message: br#"{"ChatID":42}"#.to_vec(),
                message_type: "*api.UnknownConfig".to_owned(),
                immediate: false,
                enqueued_at: "2026-05-19T17:00:00Z".to_owned(),
                fingerprint: String::new(),
                chat_id: 42,
                virtual_id: "unknown".to_owned(),
                bypass_chat_restrictions: false,
            },
            PersistentDispatcherItem {
                message: b"not-json".to_vec(),
                message_type: "*api.StickerConfig".to_owned(),
                immediate: true,
                enqueued_at: "2026-05-19T17:00:00Z".to_owned(),
                fingerprint: String::new(),
                chat_id: 43,
                virtual_id: "malformed".to_owned(),
                bypass_chat_restrictions: false,
            },
        ];
        let raw = serde_json::to_vec(&persisted)?;

        let replay = persistent_queue_replay_from_json(&raw)?;

        assert_eq!(replay.items.len(), 1);
        assert_eq!(replay.skipped, 2);
        assert_eq!(replay.items[0].virtual_id, "kept");
        assert_eq!(
            replay.items[0].method.kind(),
            TelegramOutboundMethodKind::SendMessage
        );

        Ok(())
    }

    #[test]
    fn replay_reconstructs_current_form_backed_media_methods()
    -> Result<(), Box<dyn std::error::Error>> {
        let photo = PhotoMessagePlan {
            chat_id: 42,
            photo: PhotoSource::Url("https://example.test/image.png".to_owned()),
            message_thread_id: Some(77),
            disable_notification: true,
            caption: "<b>photo</b>".to_owned(),
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            has_spoiler: true,
            reply_parameters: None,
        }
        .to_persistence_payload()?;
        let audio = AudioMessagePlan {
            chat_id: 42,
            audio: AudioSource::Bytes {
                file_name: "song.mp3".to_owned(),
                bytes: vec![1, 2, 3],
            },
            message_thread_id: None,
            disable_notification: false,
            caption: "song".to_owned(),
            render_as: String::new(),
            reply_parameters: None,
        }
        .to_persistence_payload()?;
        let media_group = MediaGroupMessagePlan {
            chat_id: 42,
            message_thread_id: None,
            disable_notification: false,
            items: vec![
                MediaGroupPhotoItem {
                    photo: PhotoSource::FileId("first-photo".to_owned()),
                    caption: "<b>album</b>".to_owned(),
                    render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
                    has_spoiler: false,
                },
                MediaGroupPhotoItem {
                    photo: PhotoSource::FileId("second-photo".to_owned()),
                    caption: String::new(),
                    render_as: String::new(),
                    has_spoiler: true,
                },
            ],
            reply_parameters: None,
        }
        .to_persistence_payload()?;
        let edit_media = EditMediaMessagePlan {
            chat_id: 42,
            message_id: 7,
            media: MediaGroupPhotoItem {
                photo: PhotoSource::FileId("replacement-photo".to_owned()),
                caption: "<b>done</b>".to_owned(),
                render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
                has_spoiler: false,
            },
        }
        .to_persistence_payload()?;

        let persisted = vec![
            persistent_item_from_payload(photo, false, "photo"),
            persistent_item_from_payload(audio, false, "audio"),
            persistent_item_from_payload(media_group, false, "media-group"),
            persistent_item_from_payload(edit_media, true, "edit-media"),
        ];
        let raw = serde_json::to_vec(&persisted)?;

        let replay = persistent_queue_replay_from_json(&raw)?;

        assert_eq!(replay.skipped, 0);
        assert_eq!(
            replay
                .items
                .iter()
                .map(|item| item.method.kind())
                .collect::<Vec<_>>(),
            vec![
                TelegramOutboundMethodKind::SendPhoto,
                TelegramOutboundMethodKind::SendAudio,
                TelegramOutboundMethodKind::SendMediaGroup,
                TelegramOutboundMethodKind::EditMessageMedia,
            ]
        );

        Ok(())
    }

    fn persistent_item_from_payload(
        payload: crate::DispatcherPersistencePayload,
        immediate: bool,
        virtual_id: &str,
    ) -> PersistentDispatcherItem {
        PersistentDispatcherItem {
            message: payload.message,
            message_type: payload.message_type,
            immediate,
            enqueued_at: "2026-05-19T17:00:00Z".to_owned(),
            fingerprint: String::new(),
            chat_id: 42,
            virtual_id: virtual_id.to_owned(),
            bypass_chat_restrictions: false,
        }
    }

    #[test]
    fn redis_value_codec_uses_rust_native_json_without_go_gob_wrapper()
    -> Result<(), Box<dyn std::error::Error>> {
        let persisted = vec![PersistentDispatcherItem {
            message: br#"{"ChatID":42,"Text":"kept"}"#.to_vec(),
            message_type: "*api.MessageConfig".to_owned(),
            immediate: false,
            enqueued_at: "2026-05-19T17:00:00Z".to_owned(),
            fingerprint: String::new(),
            chat_id: 42,
            virtual_id: "kept".to_owned(),
            bypass_chat_restrictions: false,
        }];
        let expected = serde_json::to_vec(&persisted)?;

        let encoded = persistent_queue_redis_value_from_items(&persisted)?;

        assert_eq!(encoded, expected);
        Ok(())
    }

    #[test]
    fn redis_value_replay_decodes_rust_native_json_and_skips_bad_items()
    -> Result<(), Box<dyn std::error::Error>> {
        let persisted = vec![
            PersistentDispatcherItem {
                message: br#"{"ChatID":42,"Text":"kept"}"#.to_vec(),
                message_type: "*api.MessageConfig".to_owned(),
                immediate: false,
                enqueued_at: "2026-05-19T17:00:00Z".to_owned(),
                fingerprint: String::new(),
                chat_id: 42,
                virtual_id: "kept".to_owned(),
                bypass_chat_restrictions: false,
            },
            PersistentDispatcherItem {
                message: b"not-json".to_vec(),
                message_type: "*api.StickerConfig".to_owned(),
                immediate: true,
                enqueued_at: "2026-05-19T17:00:00Z".to_owned(),
                fingerprint: String::new(),
                chat_id: 43,
                virtual_id: "bad".to_owned(),
                bypass_chat_restrictions: false,
            },
        ];

        let redis_value = persistent_queue_redis_value_from_items(&persisted)?;
        let replay = persistent_queue_replay_from_redis_value(&redis_value)?;

        assert_eq!(replay.items.len(), 1);
        assert_eq!(replay.skipped, 1);
        assert_eq!(replay.items[0].virtual_id, "kept");
        Ok(())
    }

    #[test]
    fn redis_value_replay_rejects_legacy_go_gob_wrapped_payload() {
        let persisted = vec![PersistentDispatcherItem {
            message: br#"{"ChatID":42,"Text":"kept"}"#.to_vec(),
            message_type: "*api.MessageConfig".to_owned(),
            immediate: false,
            enqueued_at: "2026-05-19T17:00:00Z".to_owned(),
            fingerprint: String::new(),
            chat_id: 42,
            virtual_id: "kept".to_owned(),
            bypass_chat_restrictions: false,
        }];
        let legacy_json = serde_json::to_vec(&persisted).expect("fixture should serialize");
        assert_eq!(legacy_json.len(), 176);

        let mut legacy_gob_wrapped = Vec::from([0xff, 0xb4, 0x0a, 0x00, 0xff, 0xb0]);
        legacy_gob_wrapped.extend_from_slice(&legacy_json);

        let error = persistent_queue_replay_from_redis_value(&legacy_gob_wrapped).expect_err(
            "legacy gob-wrapped values must not be accepted after the approved cutover",
        );

        assert!(matches!(
            error,
            DispatcherPersistenceError::DeserializeQueue(_)
        ));
    }

    #[test]
    fn restore_replay_into_queue_reports_go_startup_counts() {
        let queue = DispatcherQueue::new(DispatcherConfig {
            max_queue_size: 0,
            dedupe_config: DebouncerConfig {
                enabled: true,
                default_window: std::time::Duration::from_secs(30),
                max_cache_size: 1000,
                per_chat_settings: Default::default(),
            },
        });
        assert_eq!(
            queue.enqueue(
                text_message(42, "already-sent", "existing")
                    .with_method(text_method(42, "already-sent")),
                false,
            ),
            EnqueueOutcome::Enqueued
        );
        let replay = PersistentDispatcherReplay {
            items: vec![
                restored_text(42, "already-sent", "deduped-immediate", true),
                restored_text(42, "new", "restored-regular", false),
            ],
            skipped: 2,
        };

        let report = restore_persistent_queue_replay(&queue, replay);

        assert_eq!(report.loaded, 2);
        assert_eq!(report.restored, 1);
        assert_eq!(report.deduped, 1);
        assert_eq!(report.skipped, 2);
        let snapshot = queue.snapshot();
        assert_eq!(snapshot.regular.len(), 2);
        assert_eq!(snapshot.regular[1].virtual_id, "restored-regular");
        assert!(snapshot.immediate.is_empty());
    }
}
