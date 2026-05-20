//! Telegram update ingestion, classification, and replay.

use std::{
    collections::HashSet,
    fmt,
    future::Future,
    io,
    pin::Pin,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use carapax::types::{
    AllowedUpdate as TelegramAllowedUpdate, Chat as TelegramChat, MaybeInaccessibleMessage,
    Message as TelegramMessage, MessageData as TelegramMessageData, PollAnswerVoter,
    ReplyTo as TelegramReplyTo, Text as TelegramText, TextEntity as TelegramTextEntity,
    TextEntityPosition as TelegramTextEntityPosition, Update as TelegramUpdate,
    UpdateType as TelegramUpdateType, User as TelegramUser,
};
use openplotva_core::{
    ChatAttachment, ChatMessageMeta, ChatState, MessageSender, SENDER_TYPE_CHANNEL,
    SENDER_TYPE_SAME_CHAT, SENDER_TYPE_SYSTEM, SENDER_TYPE_USER, UpdateState, UserState,
};
use redis::Client as RedisClient;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// Human-readable crate purpose used by scaffold tests and docs.
pub const PURPOSE: &str = "updates";

/// Go `internal/updates.QueueKey` Redis list used for Telegram update ingestion.
pub const DEFAULT_UPDATE_QUEUE_KEY: &str = "plotva:updates:queue";

/// Rust-native update payload format stored inside the Redis update queue.
pub const NATIVE_UPDATE_CODEC: &str = "openplotva.update.v1+carapax-json.zstd";

pub const NATIVE_UPDATE_FORMAT_VERSION: u16 = 1;

/// Go `internal/processor` dequeue timeout for the update consumer loop.
pub const DEFAULT_UPDATE_DEQUEUE_TIMEOUT: Duration = Duration::from_secs(5);

/// Go `internal/fetcher.allowedUpdates` names passed to Telegram startup.
pub const GO_ALLOWED_UPDATE_NAMES: &[&str] = &[
    "message",
    "edited_message",
    "guest_message",
    "inline_query",
    "chosen_inline_result",
    "callback_query",
    "my_chat_member",
    "chat_member",
    "chat_join_request",
    "pre_checkout_query",
];

/// Go `internal/fetcher.allowedUpdates` represented as native `carapax` values.
pub const GO_ALLOWED_UPDATES: &[TelegramAllowedUpdate] = &[
    TelegramAllowedUpdate::Message,
    TelegramAllowedUpdate::EditedMessage,
    TelegramAllowedUpdate::GuestMessage,
    TelegramAllowedUpdate::InlineQuery,
    TelegramAllowedUpdate::ChosenInlineResult,
    TelegramAllowedUpdate::CallbackQuery,
    TelegramAllowedUpdate::BotStatus,
    TelegramAllowedUpdate::UserStatus,
    TelegramAllowedUpdate::ChatJoinRequest,
    TelegramAllowedUpdate::PreCheckoutQuery,
];

/// Go `internal/processor.updateStateTimeout`.
pub const UPDATE_STATE_TIMEOUT: Duration = Duration::from_secs(10);

/// Go `internal/processor.updateHandleTimeout`.
pub const UPDATE_HANDLE_TIMEOUT: Duration = Duration::from_secs(45);

/// Go `shouldSkipSideEffects` max age.
pub const UPDATE_SIDE_EFFECT_MAX_AGE: Duration = Duration::from_secs(60);

/// Go `internal/processor.updateStallAge`.
pub const UPDATE_STALL_AGE: Duration = Duration::from_secs(120);

/// Go update consumer worker limit multiplier over available CPUs.
pub const UPDATE_WORKER_LIMIT_PER_CPU: usize = 4;

/// Go update classifier names used by the fetcher before enqueueing updates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GoUpdateType {
    /// No Go-known update payload is present.
    Unknown,
    /// `message`.
    Message,
    /// `edited_message`.
    EditedMessage,
    /// `guest_message`.
    GuestMessage,
    /// `channel_post`.
    ChannelPost,
    /// `edited_channel_post`.
    EditedChannelPost,
    /// `inline_query`.
    InlineQuery,
    /// `chosen_inline_result`.
    ChosenInlineResult,
    /// `callback_query`.
    CallbackQuery,
    /// `shipping_query`.
    ShippingQuery,
    /// `pre_checkout_query`.
    PreCheckoutQuery,
    /// `poll`.
    Poll,
    /// `poll_answer`.
    PollAnswer,
    /// `my_chat_member`.
    MyChatMember,
    /// `chat_member`.
    ChatMember,
    /// `chat_join_request`.
    ChatJoinRequest,
    /// `message_reaction`.
    MessageReaction,
    /// `message_reaction_count`.
    MessageReactionCount,
}

impl GoUpdateType {
    /// Return the Go string form for this update classifier value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Message => "message",
            Self::EditedMessage => "edited_message",
            Self::GuestMessage => "guest_message",
            Self::ChannelPost => "channel_post",
            Self::EditedChannelPost => "edited_channel_post",
            Self::InlineQuery => "inline_query",
            Self::ChosenInlineResult => "chosen_inline_result",
            Self::CallbackQuery => "callback_query",
            Self::ShippingQuery => "shipping_query",
            Self::PreCheckoutQuery => "pre_checkout_query",
            Self::Poll => "poll",
            Self::PollAnswer => "poll_answer",
            Self::MyChatMember => "my_chat_member",
            Self::ChatMember => "chat_member",
            Self::ChatJoinRequest => "chat_join_request",
            Self::MessageReaction => "message_reaction",
            Self::MessageReactionCount => "message_reaction_count",
        }
    }

    /// Return whether Go's fetcher enqueues this update type.
    #[must_use]
    pub const fn is_allowed(self) -> bool {
        matches!(
            self,
            Self::Message
                | Self::EditedMessage
                | Self::GuestMessage
                | Self::InlineQuery
                | Self::ChosenInlineResult
                | Self::CallbackQuery
                | Self::PreCheckoutQuery
                | Self::MyChatMember
                | Self::ChatMember
                | Self::ChatJoinRequest
        )
    }
}

///
/// The Go runtime stored each update as zstd-compressed `encoding/gob`
/// envelope around `carapax::types::Update`, then compresses that envelope
/// with zstd before pushing it as a binary-safe Redis string.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedUpdate {
    compressed_payload: Vec<u8>,
}

impl EncodedUpdate {
    /// Build an encoded update from the raw Redis list value.
    pub fn from_queue_value(value: impl Into<Vec<u8>>) -> Self {
        Self {
            compressed_payload: value.into(),
        }
    }

    /// Build an encoded update from an uncompressed native JSON envelope.
    pub fn from_native_json_bytes(json_bytes: &[u8]) -> Result<Self, UpdateCodecError> {
        Ok(Self {
            compressed_payload: zstd::encode_all(json_bytes, 1)?,
        })
    }

    /// Build an encoded update from a typed `carapax` update.
    pub fn from_update(update: &TelegramUpdate) -> Result<Self, UpdateCodecError> {
        let envelope = NativeUpdateEnvelopeRef {
            version: NATIVE_UPDATE_FORMAT_VERSION,
            codec: NATIVE_UPDATE_CODEC,
            update,
        };
        let payload = serde_json::to_vec(&envelope)?;
        Self::from_native_json_bytes(&payload)
    }

    /// Return the binary Redis value stored in the update queue.
    pub fn as_queue_value(&self) -> &[u8] {
        &self.compressed_payload
    }

    /// Consume this wrapper and return the binary Redis value.
    pub fn into_queue_value(self) -> Vec<u8> {
        self.compressed_payload
    }

    /// Decompress this queued update into the native JSON envelope bytes.
    pub fn decompress_native_json(&self) -> Result<Vec<u8>, UpdateCodecError> {
        Ok(zstd::decode_all(self.compressed_payload.as_slice())?)
    }

    /// Decode this queued update into a typed `carapax` update.
    pub fn decode_update(&self) -> Result<TelegramUpdate, UpdateCodecError> {
        let payload = self.decompress_native_json()?;
        let envelope: NativeUpdateEnvelope = serde_json::from_slice(&payload)?;

        if envelope.version != NATIVE_UPDATE_FORMAT_VERSION || envelope.codec != NATIVE_UPDATE_CODEC
        {
            return Err(UpdateCodecError::UnsupportedFormat {
                version: envelope.version,
                codec: envelope.codec,
            });
        }

        Ok(serde_json::from_value(envelope.update)?)
    }
}

#[derive(Debug, Deserialize)]
struct NativeUpdateEnvelope {
    version: u16,
    codec: String,
    update: serde_json::Value,
}

#[derive(Serialize)]
struct NativeUpdateEnvelopeRef<'a> {
    version: u16,
    codec: &'static str,
    update: &'a TelegramUpdate,
}

/// Redis-backed Telegram update queue.
#[derive(Clone, Debug)]
pub struct RedisUpdateQueue {
    client: RedisClient,
    key: String,
}

impl RedisUpdateQueue {
    /// Create a queue using the Go update queue key.
    pub fn new(client: RedisClient) -> Self {
        Self::with_key(client, DEFAULT_UPDATE_QUEUE_KEY)
    }

    /// Create a queue using an explicit Redis key, useful for isolated tests.
    pub fn with_key(client: RedisClient, key: impl Into<String>) -> Self {
        Self {
            client,
            key: key.into(),
        }
    }

    /// Return the Redis key this queue reads and writes.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Enqueue an already-encoded Telegram update with Go `RPUSH` semantics.
    pub async fn enqueue_encoded(&self, update: &EncodedUpdate) -> Result<(), UpdateQueueError> {
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("RPUSH")
            .arg(&self.key)
            .arg(update.as_queue_value())
            .query_async(&mut connection)
            .await?;
        Ok(())
    }

    /// Encode and enqueue a typed `carapax` update with `RPUSH` semantics.
    pub async fn enqueue_update(&self, update: &TelegramUpdate) -> Result<(), UpdateQueueError> {
        let update = EncodedUpdate::from_update(update)?;
        self.enqueue_encoded(&update).await
    }

    /// Encode and enqueue only updates Go's fetcher would pass through.
    ///
    /// Returns `Ok(true)` when the update was pushed and `Ok(false)` when the
    /// update type is intentionally ignored.
    pub async fn enqueue_allowed_update(
        &self,
        update: &TelegramUpdate,
    ) -> Result<bool, UpdateQueueError> {
        if !is_allowed_producer_update(update) {
            return Ok(false);
        }
        self.enqueue_update(update).await?;
        Ok(true)
    }

    /// Dequeue one encoded update with Go `BLPOP` semantics.
    ///
    /// `Ok(None)` corresponds to Go `cache.ErrNotFound`, including a timeout.
    pub async fn dequeue_encoded(
        &self,
        timeout: Duration,
    ) -> Result<Option<EncodedUpdate>, UpdateQueueError> {
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let result: Option<(String, Vec<u8>)> = redis::cmd("BLPOP")
            .arg(&self.key)
            .arg(blpop_timeout_arg(timeout))
            .query_async(&mut connection)
            .await?;
        Ok(result.map(|(_, value)| EncodedUpdate::from_queue_value(value)))
    }

    /// Dequeue and decode one typed Telegram update.
    pub async fn dequeue_update(
        &self,
        timeout: Duration,
    ) -> Result<Option<TelegramUpdate>, UpdateQueueError> {
        let Some(update) = self.dequeue_encoded(timeout).await? else {
            return Ok(None);
        };
        Ok(Some(update.decode_update()?))
    }

    /// Dequeue and process one update using the Rust-native consumer primitive.
    pub async fn process_next_update<
        StateFn,
        StateFuture,
        StateError,
        HandleFn,
        HandleFuture,
        HandleError,
    >(
        &self,
        config: UpdateConsumerConfig,
        state: StateFn,
        handle: HandleFn,
    ) -> Result<Option<UpdateProcessReport>, UpdateQueueError>
    where
        StateFn: FnOnce(TelegramUpdate) -> StateFuture,
        StateFuture: Future<Output = Result<(), StateError>>,
        StateError: fmt::Display,
        HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
        HandleFuture: Future<Output = Result<(), HandleError>>,
        HandleError: fmt::Display,
    {
        let Some(update) = self.dequeue_update(config.dequeue_timeout).await? else {
            return Ok(None);
        };
        Ok(Some(process_update(update, config, state, handle).await))
    }

    /// Return the Redis list length using Go `LLEN` semantics.
    pub async fn len(&self) -> Result<i64, UpdateQueueError> {
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let len: i64 = redis::cmd("LLEN")
            .arg(&self.key)
            .query_async(&mut connection)
            .await?;
        Ok(len)
    }

    /// Return whether the Redis list is empty.
    pub async fn is_empty(&self) -> Result<bool, UpdateQueueError> {
        Ok(self.len().await? == 0)
    }
}

/// Boxed future returned by update producer sources.
pub type UpdateProducerSourceFuture<'a> =
    Pin<Box<dyn Future<Output = Option<TelegramUpdate>> + Send + 'a>>;

/// Boxed future returned by update producer queue sinks.
pub type UpdateProducerQueueFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

pub trait UpdateProducerSource {
    /// Receive the next update, or `None` when the source is closed.
    fn next_update<'a>(&'a self) -> UpdateProducerSourceFuture<'a>;
}

/// Queue sink used by the update producer after filtering allowed updates.
pub trait UpdateProducerQueue {
    /// Error returned by the concrete queue.
    type Error: fmt::Display;

    /// Enqueue one already-approved Telegram update.
    fn enqueue_update<'a>(
        &'a self,
        update: &'a TelegramUpdate,
    ) -> UpdateProducerQueueFuture<'a, Self::Error>;
}

impl UpdateProducerQueue for RedisUpdateQueue {
    type Error = UpdateQueueError;

    fn enqueue_update<'a>(
        &'a self,
        update: &'a TelegramUpdate,
    ) -> UpdateProducerQueueFuture<'a, Self::Error> {
        Box::pin(RedisUpdateQueue::enqueue_update(self, update))
    }
}

/// Summary for one Go-style update producer run.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UpdateProducerRunReport {
    /// Updates received from the source.
    pub received: usize,
    /// Updates accepted by the Go producer filter and successfully enqueued.
    pub enqueued: usize,
    /// Updates rejected by the Go producer filter.
    pub skipped: usize,
    /// Enqueue errors observed; the producer continues after these like Go logs and continues.
    pub enqueue_errors: Vec<String>,
    /// Whether the source closed before shutdown was requested.
    pub source_closed: bool,
}

/// Options for extracting Go-shaped attachment metadata from a Telegram message.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TelegramMessageAttachmentOptions {
    /// Attachment source; blanks default to Go's `message`.
    pub source: String,
    /// Caption supplied by the caller, trimmed before use.
    pub caption: String,
    /// Whether Go should promote the first image-like ref for inbound history.
    pub promote_first_image_ref: bool,
}

/// Run Go's update producer loop over an abstract source and queue.
pub async fn run_update_producer_until<S, Q, Stop>(
    source: &S,
    queue: &Q,
    stop: Stop,
) -> UpdateProducerRunReport
where
    S: UpdateProducerSource + Sync,
    Q: UpdateProducerQueue + Sync,
    Stop: Future<Output = ()>,
{
    let mut report = UpdateProducerRunReport::default();
    tokio::pin!(stop);

    loop {
        tokio::select! {
            biased;

            _ = &mut stop => break,
            update = source.next_update() => {
                let Some(update) = update else {
                    report.source_closed = true;
                    break;
                };

                report.received += 1;
                if !is_allowed_producer_update(&update) {
                    report.skipped += 1;
                    continue;
                }

                let queued = queue.enqueue_update(&update);
                tokio::pin!(queued);
                tokio::select! {
                    biased;

                    _ = &mut stop => break,
                    result = &mut queued => match result {
                        Ok(()) => report.enqueued += 1,
                        Err(error) => report.enqueue_errors.push(error.to_string()),
                    },
                }
            }
        }
    }

    report
}

/// Runtime knobs matching the Go update consumer defaults.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UpdateConsumerConfig {
    /// Blocking pop timeout for one queue read.
    pub dequeue_timeout: Duration,
    /// Timeout for chat/user state updates.
    pub state_timeout: Duration,
    /// Timeout for user-visible update handling.
    pub handle_timeout: Duration,
    /// Maximum update age before skipping side effects.
    pub side_effect_max_age: Duration,
    /// Maximum number of concurrently active tasks.
    pub worker_limit: usize,
}

impl Default for UpdateConsumerConfig {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1);
        Self {
            dequeue_timeout: DEFAULT_UPDATE_DEQUEUE_TIMEOUT,
            state_timeout: UPDATE_STATE_TIMEOUT,
            handle_timeout: UPDATE_HANDLE_TIMEOUT,
            side_effect_max_age: UPDATE_SIDE_EFFECT_MAX_AGE,
            worker_limit: UPDATE_WORKER_LIMIT_PER_CPU * cpus,
        }
    }
}

/// Update consumer task stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateStage {
    /// Chat/user state persistence stage.
    State,
    /// User-visible update handler stage.
    Handle,
}

/// Outcome of one update consumer stage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UpdateStageOutcome {
    /// Stage completed without returning an error.
    Completed,
    /// Stage returned an error. Go logs these and keeps the consumer alive.
    Failed(String),
    /// Stage exceeded its configured timeout.
    TimedOut,
}

/// Report for one update consumer stage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateStageReport {
    /// Stage that ran.
    pub stage: UpdateStage,
    /// Stage result.
    pub outcome: UpdateStageOutcome,
    /// Wall-clock time spent in the stage.
    pub elapsed: Duration,
}

/// Report for one decoded update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateProcessReport {
    /// Telegram update id.
    pub update_id: i64,
    pub update_name: &'static str,
    /// Chat/user state stage report.
    pub state: UpdateStageReport,
    /// User-visible handle stage report, absent when skipped as stale.
    pub handle: Option<UpdateStageReport>,
    /// Whether user-visible side effects were skipped because the update is stale.
    pub skipped_handle: bool,
}

/// Process one update using Go consumer stage ordering and timeouts.
pub async fn process_update<StateFn, StateFuture, StateError, HandleFn, HandleFuture, HandleError>(
    update: TelegramUpdate,
    config: UpdateConsumerConfig,
    state: StateFn,
    handle: HandleFn,
) -> UpdateProcessReport
where
    StateFn: FnOnce(TelegramUpdate) -> StateFuture,
    StateFuture: Future<Output = Result<(), StateError>>,
    StateError: fmt::Display,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    process_update_at(update, config, SystemTime::now(), state, handle).await
}

pub async fn process_update_at<
    StateFn,
    StateFuture,
    StateError,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    update: TelegramUpdate,
    config: UpdateConsumerConfig,
    now: SystemTime,
    state: StateFn,
    handle: HandleFn,
) -> UpdateProcessReport
where
    StateFn: FnOnce(TelegramUpdate) -> StateFuture,
    StateFuture: Future<Output = Result<(), StateError>>,
    StateError: fmt::Display,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let update_id = update.id;
    let name = update_name(&update);
    let skip_handle = should_skip_side_effects_at(&update, config.side_effect_max_age, now);
    let state_task = run_stage(
        UpdateStage::State,
        config.state_timeout,
        state(update.clone()),
    );

    if skip_handle {
        return UpdateProcessReport {
            update_id,
            update_name: name,
            state: state_task.await,
            handle: None,
            skipped_handle: true,
        };
    }

    let handle_task = run_stage(UpdateStage::Handle, config.handle_timeout, handle(update));
    let (state, handle) = tokio::join!(state_task, handle_task);

    UpdateProcessReport {
        update_id,
        update_name: name,
        state,
        handle: Some(handle),
        skipped_handle: false,
    }
}

/// Return the Go consumer stats name for an update.
pub fn update_name(update: &TelegramUpdate) -> &'static str {
    match &update.update_type {
        TelegramUpdateType::Message(_) => "message",
        TelegramUpdateType::EditedMessage(_) => "edited_message",
        TelegramUpdateType::GuestMessage(_) => "guest_message",
        TelegramUpdateType::CallbackQuery(_) => "callback_query",
        TelegramUpdateType::PreCheckoutQuery(_) => "pre_checkout_query",
        TelegramUpdateType::BotStatus(_) => "my_chat_member",
        TelegramUpdateType::UserStatus(_) => "chat_member",
        TelegramUpdateType::ChatJoinRequest(_) => "chat_join_request",
        _ => "unknown",
    }
}

/// Return the Go fetcher classification for an update before enqueueing.
#[must_use]
pub fn producer_update_type(update: &TelegramUpdate) -> GoUpdateType {
    match &update.update_type {
        TelegramUpdateType::Message(_) => GoUpdateType::Message,
        TelegramUpdateType::EditedMessage(_) => GoUpdateType::EditedMessage,
        TelegramUpdateType::GuestMessage(_) => GoUpdateType::GuestMessage,
        TelegramUpdateType::ChannelPost(_) => GoUpdateType::ChannelPost,
        TelegramUpdateType::EditedChannelPost(_) => GoUpdateType::EditedChannelPost,
        TelegramUpdateType::InlineQuery(_) => GoUpdateType::InlineQuery,
        TelegramUpdateType::ChosenInlineResult(_) => GoUpdateType::ChosenInlineResult,
        TelegramUpdateType::CallbackQuery(_) => GoUpdateType::CallbackQuery,
        TelegramUpdateType::ShippingQuery(_) => GoUpdateType::ShippingQuery,
        TelegramUpdateType::PreCheckoutQuery(_) => GoUpdateType::PreCheckoutQuery,
        TelegramUpdateType::Poll(_) => GoUpdateType::Poll,
        TelegramUpdateType::PollAnswer(_) => GoUpdateType::PollAnswer,
        TelegramUpdateType::BotStatus(_) => GoUpdateType::MyChatMember,
        TelegramUpdateType::UserStatus(_) => GoUpdateType::ChatMember,
        TelegramUpdateType::ChatJoinRequest(_) => GoUpdateType::ChatJoinRequest,
        TelegramUpdateType::MessageReaction(_) => GoUpdateType::MessageReaction,
        TelegramUpdateType::MessageReactionCount(_) => GoUpdateType::MessageReactionCount,
        _ => GoUpdateType::Unknown,
    }
}

/// Return the Go fetcher classification name for an update before enqueueing.
#[must_use]
pub fn producer_update_name(update: &TelegramUpdate) -> &'static str {
    producer_update_type(update).as_str()
}

/// Return whether Go's fetcher would enqueue this update.
#[must_use]
pub fn is_allowed_producer_update(update: &TelegramUpdate) -> bool {
    producer_update_type(update).is_allowed()
}

/// Extract the chat/user state Go persists before update side effects.
pub fn extract_update_state(update: &TelegramUpdate) -> Option<UpdateState> {
    if matches!(update.update_type, TelegramUpdateType::GuestMessage(_)) {
        return None;
    }

    let chat = extract_update_chat(update).map(chat_state);
    let user = update.get_user().map(user_state);
    UpdateState::new(chat, user)
}

/// Extract Go `utils.TelegramMessageAttachments` metadata from a Telegram message.
#[must_use]
pub fn telegram_message_attachments(
    message: &TelegramMessage,
    opts: TelegramMessageAttachmentOptions,
) -> Vec<ChatAttachment> {
    let source = telegram_attachment_source(&opts.source);
    let caption = opts.caption.trim();
    let mut out = Vec::with_capacity(2);

    append_telegram_image_attachment(
        &mut out,
        &message.data,
        &source,
        caption,
        opts.promote_first_image_ref,
    );
    append_telegram_file_attachments(&mut out, &message.data, &source, caption);
    append_telegram_contact_attachments(&mut out, &message.data, &source);

    out
}

/// Return Go `fetcher.detectMessageType` for a Telegram message plus known attachments.
#[must_use]
pub fn detect_message_type(
    message: Option<&TelegramMessage>,
    attachments: &[ChatAttachment],
) -> String {
    first_message_attachment_kind(attachments)
        .or_else(|| telegram_message_type(message))
        .unwrap_or("text")
        .to_owned()
}

/// Collect Go fetcher media attachments that are not already present in metadata.
#[must_use]
pub fn collect_media_attachments(
    message: Option<&TelegramMessage>,
    existing: &[ChatAttachment],
) -> Vec<ChatAttachment> {
    let Some(message) = message else {
        return Vec::new();
    };

    let mut seen = attachment_key_set(existing);
    let mut out = Vec::new();
    for attachment in media_attachments_from_message(message) {
        add_unique_attachment(&mut out, &mut seen, attachment);
    }
    out
}

/// Resolve Go `utils.ResolveMessageSender` metadata from a Telegram message.
#[must_use]
pub fn resolve_message_sender(message: Option<&TelegramMessage>) -> MessageSender {
    let Some(message) = message else {
        return MessageSender::system();
    };

    match &message.sender {
        carapax::types::MessageSender::Chat(chat) => {
            let sender_type = if chat.get_id() == message.chat.get_id() {
                SENDER_TYPE_SAME_CHAT
            } else {
                SENDER_TYPE_CHANNEL
            };
            let full_name = telegram_chat_full_name(chat);
            MessageSender {
                sender_type: sender_type.to_owned(),
                id: chat.get_id().into(),
                full_name: if full_name.is_empty() {
                    String::new()
                } else {
                    format!("📣 {full_name}")
                },
                username: chat
                    .get_username()
                    .map(ToString::to_string)
                    .unwrap_or_default()
                    .trim()
                    .to_owned(),
                is_bot: false,
            }
        }
        carapax::types::MessageSender::User(user) => MessageSender {
            sender_type: SENDER_TYPE_USER.to_owned(),
            id: user.id.into(),
            full_name: telegram_user_full_name(user),
            username: user
                .username
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default()
                .trim()
                .to_owned(),
            is_bot: user.is_bot,
        },
        carapax::types::MessageSender::Unknown => MessageSender {
            sender_type: SENDER_TYPE_SYSTEM.to_owned(),
            ..MessageSender::system()
        },
    }
}

/// Build Go `fetcher.buildMessageMeta` output for a message.
#[must_use]
pub fn build_message_meta(
    message: Option<&TelegramMessage>,
    sender: MessageSender,
    attachments: &[ChatAttachment],
    vision_description: &str,
) -> ChatMessageMeta {
    let mut meta = ChatMessageMeta {
        vision_description: vision_description.trim().to_owned(),
        sender_type: sender.sender_type.clone(),
        sender_id: sender.id,
        sender_name: sender.display_name(),
        sender_username: sender.username.trim().to_owned(),
        attachments: attachments.to_vec(),
        ..ChatMessageMeta::default()
    };

    meta.attachments
        .extend(collect_media_attachments(message, &meta.attachments));
    meta.message_type = detect_message_type(message, &meta.attachments);
    meta
}

/// Go fetcher message context fields needed before higher-level routing is ported.
#[derive(Clone, Debug, PartialEq)]
pub struct FetcherMessageContext {
    /// Original `Message.Text` before Go fills fallback content from captions or media.
    pub original_text: String,
    /// Go fetcher message text after fallback extraction.
    pub text: String,
    /// Go `buildMessageMeta` output.
    pub meta: ChatMessageMeta,
}

/// Build the history-relevant part of Go `Fetcher.newMessageContext`.
#[must_use]
pub fn build_fetcher_message_context(message: &TelegramMessage) -> FetcherMessageContext {
    let sender = resolve_message_sender(Some(message));
    FetcherMessageContext {
        original_text: message_text_before_fetcher_fallback(message),
        text: fetcher_message_text(message),
        meta: build_message_meta(Some(message), sender, &[], ""),
    }
}

/// Result of Go `parseIfAdressed` over a Telegram message.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ParsedAddressedMessage {
    /// Text used as the full message text for downstream handling.
    pub message_text: String,
    /// First parsed word after Go strips direct bot addressing where applicable.
    pub first_word: String,
    /// Remaining parsed text after the first word.
    pub rest_text: String,
    /// Whether the message is addressed to the bot.
    pub is_addressed: bool,
}

/// Parse Go fetcher addressing semantics for a Telegram message.
#[must_use]
pub fn parse_if_addressed(message: &TelegramMessage, bot: &TelegramUser) -> ParsedAddressedMessage {
    let (message_text, text_for_parsing) = addressable_message_text(message);
    let (mut first_word, mut rest_text) = cut_first_word(&text_for_parsing);

    if addressed_by_bot_name(bot, &first_word) {
        (first_word, rest_text) = cut_first_word(&rest_text);
        return ParsedAddressedMessage {
            message_text,
            first_word,
            rest_text,
            is_addressed: true,
        };
    }

    if telegram_chat_is_private(&message.chat) {
        return ParsedAddressedMessage {
            message_text,
            first_word,
            rest_text,
            is_addressed: true,
        };
    }

    if addressed_by_bot_mention(bot, &first_word) {
        (first_word, rest_text) = cut_first_word(&rest_text);
        return ParsedAddressedMessage {
            message_text,
            first_word,
            rest_text,
            is_addressed: true,
        };
    }

    let is_addressed = addressed_by_bot_reply(message, bot);
    ParsedAddressedMessage {
        message_text,
        first_word,
        rest_text,
        is_addressed,
    }
}

/// Return whether this message is Go's `/settings` command for the current bot.
#[must_use]
pub fn is_settings_command_message(message: &TelegramMessage, bot_username: &str) -> bool {
    let Some(command) = leading_bot_command(message) else {
        return false;
    };
    if !command.command.eq_ignore_ascii_case("settings") {
        return false;
    }
    if telegram_chat_is_private(&message.chat) || bot_username.is_empty() {
        return true;
    }
    command
        .target
        .as_deref()
        .is_none_or(|target| target.eq_ignore_ascii_case(bot_username))
}

/// Parse Go `parseEditCommand` semantics for image-edit intent text.
#[must_use]
pub fn parse_edit_command(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }

    let (first_word, rest_text) = cut_first_word(trimmed);
    is_edit_verb(&first_word).then(|| rest_text.trim().to_owned())
}

/// Parse Go `resolveDrawPromptFromMessage` semantics for draw command text.
#[must_use]
pub fn resolve_draw_prompt_from_message(
    message: &TelegramMessage,
    first_word_lower: &str,
    rest_text: &str,
) -> Option<String> {
    DRAW_VERB_ALIASES
        .contains(&first_word_lower)
        .then(|| draw_prompt_with_reply_context(rest_text, Some(message)))
}

/// Compose Go `drawPromptWithReplyContext` prompt text for reply-based draws.
#[must_use]
pub fn draw_prompt_with_reply_context(text: &str, message: Option<&TelegramMessage>) -> String {
    let trimmed = text.trim();
    let prefix = first_draw_reply_prefix(trimmed);
    let Some(TelegramReplyTo::Message(reply)) =
        message.and_then(|message| message.reply_to.as_ref())
    else {
        return trimmed.to_owned();
    };
    if !trimmed.is_empty() && prefix.is_none() {
        return trimmed.to_owned();
    }

    let lower_text = prefix
        .map(|prefix| {
            let lower = trimmed.to_lowercase();
            lower
                .strip_prefix(prefix)
                .unwrap_or(lower.as_str())
                .trim()
                .to_owned()
        })
        .unwrap_or_default();
    format!(
        "{} {}",
        lower_text,
        message_text_before_fetcher_fallback(reply)
    )
    .trim()
    .to_owned()
}

/// Result of Go `editedImagePromptUpdate` for pending image job updates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EditedImagePromptUpdate {
    /// Prompt with message metadata context composed for image generation.
    pub prompt: String,
    /// Trimmed prompt before metadata context is appended.
    pub original_prompt: String,
}

/// Compose Go `editedImagePromptUpdate` semantics for edited draw messages.
#[must_use]
pub fn edited_image_prompt_update(
    message: &TelegramMessage,
    first_word: &str,
    rest_text: &str,
    meta: &ChatMessageMeta,
) -> Option<EditedImagePromptUpdate> {
    let first_word_lower = first_word.to_lowercase();
    let base_prompt = resolve_draw_prompt_from_message(message, &first_word_lower, rest_text)?;
    let original_prompt = base_prompt.trim().to_owned();
    let mut prompt = compose_image_prompt(&original_prompt, meta);
    if prompt.is_empty() {
        prompt.clone_from(&original_prompt);
    }

    Some(EditedImagePromptUpdate {
        prompt,
        original_prompt,
    })
}

/// Compose Go image prompt context from prompt, vision description, and attachments.
#[must_use]
pub fn compose_image_prompt(prompt: &str, meta: &ChatMessageMeta) -> String {
    let mut parts = Vec::with_capacity(1 + meta.attachments.len());
    let mut seen = HashSet::new();

    add_image_prompt_part(&mut parts, &mut seen, prompt);
    add_image_prompt_part(&mut parts, &mut seen, &meta.vision_description);
    for attachment in &meta.attachments {
        if !attachment.content.is_empty() {
            add_image_prompt_part(&mut parts, &mut seen, &attachment.content);
            continue;
        }
        if !attachment.caption.is_empty() {
            add_image_prompt_part(&mut parts, &mut seen, &attachment.caption);
        }
    }

    parts.join("\n\n")
}

/// Go `dialog.MessageKindText` history kind.
pub const HISTORY_MESSAGE_KIND_TEXT: &str = "text";

/// Go `dialog.RoleUser` history role.
pub const HISTORY_ROLE_USER: &str = "user";

/// Go `dialog.RoleModel` history role.
pub const HISTORY_ROLE_MODEL: &str = "model";

/// Storage-ready Go text history entry extracted from a Telegram message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryTextEntry {
    /// Go `MessageEntry.EntryID`, currently `msg:<message_id>` for text entries.
    pub entry_id: String,
    /// Go history kind.
    pub kind: String,
    /// Go dialog role.
    pub role: String,
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram forum/private thread ID, or zero when absent.
    pub thread_id: i32,
    /// Telegram message ID.
    pub message_id: i32,
    /// Resolved sender ID used by Go history indexes.
    pub sender_id: i64,
    /// UTC occurrence timestamp.
    pub occurred_at: OffsetDateTime,
    /// Go-shaped `history.MessageEntry` JSON payload.
    pub payload: Vec<u8>,
}

/// Build the Go `history.messageEntryFromTelegramMessage` text-entry payload.
#[must_use]
pub fn build_history_text_entry(
    message: &TelegramMessage,
    original_text: &str,
    meta: ChatMessageMeta,
    bot_id: i64,
) -> Option<HistoryTextEntry> {
    if message.chat.get_id() == 0 {
        return None;
    }

    let sender = resolve_message_sender(Some(message));
    if sender.id == 0 {
        return None;
    }

    let text = fetcher_message_text(message);
    let (text, original_text, meta) = normalize_history_text_payload(&text, original_text, meta);
    if !history_text_entry_has_content(&text, &original_text, &meta) {
        return None;
    }

    let meta = fill_history_sender_meta(meta, &sender);
    let occurred_at = OffsetDateTime::from_unix_timestamp(message.date).ok()?;
    let timestamp = occurred_at.format(&Rfc3339).ok()?;
    let entry_id = format!("msg:{}", message.id);
    let role = if bot_id != 0 && sender.id == bot_id {
        HISTORY_ROLE_MODEL
    } else {
        HISTORY_ROLE_USER
    };

    let mut payload = Map::new();
    payload.insert("entry_id".to_owned(), Value::String(entry_id.clone()));
    payload.insert("role".to_owned(), Value::String(role.to_owned()));
    payload.insert(
        "kind".to_owned(),
        Value::String(HISTORY_MESSAGE_KIND_TEXT.to_owned()),
    );
    payload.insert("timestamp".to_owned(), Value::String(timestamp));
    merge_json_object(&mut payload, history_message_payload(message, &text)?);
    if !original_text.is_empty() {
        payload.insert("original_text".to_owned(), Value::String(original_text));
    }
    payload.insert("meta".to_owned(), serde_json::to_value(&meta).ok()?);

    Some(HistoryTextEntry {
        entry_id,
        kind: HISTORY_MESSAGE_KIND_TEXT.to_owned(),
        role: role.to_owned(),
        chat_id: message.chat.get_id().into(),
        thread_id: message
            .message_thread_id
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or_default(),
        message_id: i32::try_from(message.id).ok()?,
        sender_id: history_entry_sender_id(&meta, message),
        occurred_at,
        payload: serde_json::to_vec(&Value::Object(payload)).ok()?,
    })
}

/// Apply Go fetcher MIME-derived attachment-kind normalization.
#[must_use]
pub fn normalize_attachment_kind(mut attachment: ChatAttachment) -> ChatAttachment {
    if !attachment.kind.is_empty() || attachment.mime_type.is_empty() {
        return attachment;
    }

    if has_mime_prefix(&attachment.mime_type, "image/") {
        attachment.kind = "image".to_owned();
    } else if has_mime_prefix(&attachment.mime_type, "video/") {
        attachment.kind = "video".to_owned();
    } else if has_mime_prefix(&attachment.mime_type, "audio/") {
        attachment.kind = "audio".to_owned();
    }

    attachment
}

/// Return whether Go would skip user-visible side effects for this update age.
pub fn should_skip_side_effects_at(
    update: &TelegramUpdate,
    max_age: Duration,
    now: SystemTime,
) -> bool {
    let Some(update_date) = side_effect_message_unix_date(update) else {
        return false;
    };
    let now_secs = unix_timestamp_seconds(now);
    i128::from(update_date) + i128::from(max_age.as_secs()) <= now_secs
}

fn extract_update_chat(update: &TelegramUpdate) -> Option<&TelegramChat> {
    update.get_chat().or(match &update.update_type {
        TelegramUpdateType::PollAnswer(answer) => match &answer.voter {
            PollAnswerVoter::Chat(chat) => Some(chat),
            PollAnswerVoter::User(_) => None,
        },
        _ => None,
    })
}

fn chat_state(chat: &TelegramChat) -> ChatState {
    match chat {
        TelegramChat::Channel(chat) => ChatState::new(
            chat.id.into(),
            "channel",
            Some(chat.title.clone()),
            chat.username.clone().map(String::from),
            None,
            None,
            None,
        ),
        TelegramChat::Group(chat) => ChatState::new(
            chat.id.into(),
            "group",
            Some(chat.title.clone()),
            None,
            None,
            None,
            None,
        ),
        TelegramChat::Private(chat) => ChatState::new(
            chat.id.into(),
            "private",
            None,
            chat.username.clone().map(String::from),
            Some(chat.first_name.clone()),
            chat.last_name.clone(),
            None,
        ),
        TelegramChat::Supergroup(chat) => ChatState::new(
            chat.id.into(),
            "supergroup",
            Some(chat.title.clone()),
            chat.username.clone().map(String::from),
            None,
            None,
            chat.is_forum,
        ),
    }
}

fn user_state(user: &TelegramUser) -> UserState {
    UserState::new(
        user.id.into(),
        user.first_name.clone(),
        user.last_name.clone(),
        user.username.clone().map(String::from),
        user.language_code.clone(),
        Some(user.is_premium.unwrap_or(false)),
    )
}

fn telegram_attachment_source(source: &str) -> String {
    let source = source.trim();
    if source.is_empty() {
        "message".to_owned()
    } else {
        source.to_owned()
    }
}

fn first_message_attachment_kind(attachments: &[ChatAttachment]) -> Option<&str> {
    attachments.iter().find_map(|attachment| {
        if attachment.source.trim() != "message" {
            return None;
        }
        let kind = attachment.kind.trim();
        (!kind.is_empty()).then_some(kind)
    })
}

fn telegram_message_type(message: Option<&TelegramMessage>) -> Option<&'static str> {
    let message = message?;
    match &message.data {
        TelegramMessageData::Voice(_) => Some("voice"),
        TelegramMessageData::Video(_) => Some("video"),
        TelegramMessageData::Audio(_) => Some("audio"),
        TelegramMessageData::Document(_) => Some("document"),
        TelegramMessageData::Location(_) | TelegramMessageData::Venue(_) => Some("location"),
        TelegramMessageData::Contact(_) => Some("contact"),
        TelegramMessageData::Photo(_) | TelegramMessageData::Sticker(_) => Some("image"),
        TelegramMessageData::Text(text) if !text.data.trim().is_empty() => Some("text"),
        _ => None,
    }
}

fn telegram_user_full_name(user: &TelegramUser) -> String {
    let name = format!(
        "{} {}",
        user.first_name,
        user.last_name.as_deref().unwrap_or_default()
    )
    .trim()
    .to_owned();
    if name.is_empty() {
        user.username
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default()
    } else {
        name
    }
}

fn telegram_chat_full_name(chat: &TelegramChat) -> String {
    match chat {
        TelegramChat::Channel(chat) => chat.title.clone(),
        TelegramChat::Group(chat) => chat.title.clone(),
        TelegramChat::Supergroup(chat) => chat.title.clone(),
        TelegramChat::Private(chat) => {
            let name = format!(
                "{} {}",
                chat.first_name,
                chat.last_name.as_deref().unwrap_or_default()
            )
            .trim()
            .to_owned();
            if name.is_empty() {
                chat.username
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default()
            } else {
                name
            }
        }
    }
}

fn attachment_key_set(existing: &[ChatAttachment]) -> HashSet<String> {
    existing.iter().filter_map(attachment_key).collect()
}

fn add_unique_attachment(
    out: &mut Vec<ChatAttachment>,
    seen: &mut HashSet<String>,
    attachment: ChatAttachment,
) {
    let attachment = normalize_attachment_kind(attachment);
    if let Some(key) = attachment_key(&attachment)
        && !seen.insert(key)
    {
        return;
    }
    out.push(attachment);
}

fn media_attachments_from_message(message: &TelegramMessage) -> Vec<ChatAttachment> {
    telegram_message_attachments(
        message,
        TelegramMessageAttachmentOptions {
            caption: attachment_caption(message),
            promote_first_image_ref: true,
            ..TelegramMessageAttachmentOptions::default()
        },
    )
}

fn attachment_caption(message: &TelegramMessage) -> String {
    if matches!(
        &message.data,
        TelegramMessageData::Text(text) if !text.data.trim().is_empty()
    ) {
        return String::new();
    }

    message
        .get_text()
        .map(|text| text.as_ref().trim().to_owned())
        .unwrap_or_default()
}

fn attachment_key(attachment: &ChatAttachment) -> Option<String> {
    if !attachment.file_unique_id.is_empty() {
        return Some(format!("{}:{}", attachment.kind, attachment.file_unique_id));
    }
    if !attachment.kind.is_empty()
        && !attachment.source.is_empty()
        && !attachment.content.is_empty()
    {
        return Some(format!(
            "{}:{}:{}",
            attachment.kind, attachment.source, attachment.content
        ));
    }
    None
}

fn append_telegram_image_attachment(
    out: &mut Vec<ChatAttachment>,
    data: &TelegramMessageData,
    source: &str,
    caption: &str,
    promote: bool,
) {
    if promote {
        if let Some(attachment) = telegram_first_image_attachment(data, source, caption) {
            out.push(attachment);
        }
        return;
    }

    if let TelegramMessageData::Photo(photo) = data
        && let Some(ref_data) = photo.data.last()
    {
        out.push(ChatAttachment {
            kind: "image".to_owned(),
            source: source.to_owned(),
            file_unique_id: ref_data.file_unique_id.clone(),
            caption: caption.to_owned(),
            ..ChatAttachment::default()
        });
    }
}

fn append_telegram_file_attachments(
    out: &mut Vec<ChatAttachment>,
    data: &TelegramMessageData,
    source: &str,
    caption: &str,
) {
    match data {
        TelegramMessageData::Video(video) => out.push(ChatAttachment {
            kind: "video".to_owned(),
            source: source.to_owned(),
            file_unique_id: video.data.file_unique_id.clone(),
            file_name: video.data.file_name.clone().unwrap_or_default(),
            mime_type: video.data.mime_type.clone().unwrap_or_default(),
            caption: caption.to_owned(),
            duration_seconds: video.data.duration,
            ..ChatAttachment::default()
        }),
        TelegramMessageData::Audio(audio) => out.push(ChatAttachment {
            kind: "audio".to_owned(),
            source: source.to_owned(),
            file_unique_id: audio.data.file_unique_id.clone(),
            file_name: audio.data.file_name.clone().unwrap_or_default(),
            mime_type: audio.data.mime_type.clone().unwrap_or_default(),
            caption: caption.to_owned(),
            duration_seconds: audio.data.duration,
            performer: audio.data.performer.clone().unwrap_or_default(),
            title: audio.data.title.clone().unwrap_or_default(),
            ..ChatAttachment::default()
        }),
        TelegramMessageData::Voice(voice) => out.push(ChatAttachment {
            kind: "voice".to_owned(),
            source: source.to_owned(),
            file_unique_id: voice.data.file_unique_id.clone(),
            duration_seconds: voice.data.duration,
            ..ChatAttachment::default()
        }),
        TelegramMessageData::Document(document) => out.push(ChatAttachment {
            kind: "document".to_owned(),
            source: source.to_owned(),
            file_unique_id: document.data.file_unique_id.clone(),
            file_name: document.data.file_name.clone().unwrap_or_default(),
            mime_type: document.data.mime_type.clone().unwrap_or_default(),
            caption: caption.to_owned(),
            ..ChatAttachment::default()
        }),
        TelegramMessageData::Sticker(sticker) => out.push(ChatAttachment {
            kind: "sticker".to_owned(),
            source: source.to_owned(),
            file_unique_id: sticker.file_unique_id.clone(),
            content: sticker.emoji.clone().unwrap_or_default(),
            ..ChatAttachment::default()
        }),
        _ => {}
    }
}

fn append_telegram_contact_attachments(
    out: &mut Vec<ChatAttachment>,
    data: &TelegramMessageData,
    source: &str,
) {
    match data {
        TelegramMessageData::Location(location) => out.push(ChatAttachment {
            kind: "location".to_owned(),
            source: source.to_owned(),
            latitude: Some(f64::from(location.latitude)),
            longitude: Some(f64::from(location.longitude)),
            ..ChatAttachment::default()
        }),
        TelegramMessageData::Venue(venue) => out.push(ChatAttachment {
            kind: "venue".to_owned(),
            source: source.to_owned(),
            content: format!("{} {}", venue.title, venue.address)
                .trim()
                .to_owned(),
            latitude: Some(f64::from(venue.location.latitude)),
            longitude: Some(f64::from(venue.location.longitude)),
            ..ChatAttachment::default()
        }),
        TelegramMessageData::Contact(contact) => out.push(ChatAttachment {
            kind: "contact".to_owned(),
            source: source.to_owned(),
            phone: contact.phone_number.clone(),
            first_name: contact.first_name.clone(),
            last_name: contact.last_name.clone().unwrap_or_default(),
            user_id: contact.user_id.unwrap_or_default(),
            ..ChatAttachment::default()
        }),
        _ => {}
    }
}

fn telegram_first_image_attachment(
    data: &TelegramMessageData,
    source: &str,
    caption: &str,
) -> Option<ChatAttachment> {
    match data {
        TelegramMessageData::Photo(photo) => photo.data.last().map(|ref_data| ChatAttachment {
            kind: "image".to_owned(),
            source: source.to_owned(),
            file_unique_id: ref_data.file_unique_id.clone(),
            caption: caption.to_owned(),
            ..ChatAttachment::default()
        }),
        TelegramMessageData::Document(document)
            if has_mime_prefix(
                document.data.mime_type.as_deref().unwrap_or_default(),
                "image/",
            ) =>
        {
            Some(ChatAttachment {
                kind: "image".to_owned(),
                source: source.to_owned(),
                file_unique_id: document.data.file_unique_id.clone(),
                mime_type: document.data.mime_type.clone().unwrap_or_default(),
                caption: caption.to_owned(),
                ..ChatAttachment::default()
            })
        }
        TelegramMessageData::Sticker(sticker) => Some(ChatAttachment {
            kind: "image".to_owned(),
            source: source.to_owned(),
            file_unique_id: sticker.file_unique_id.clone(),
            caption: caption.to_owned(),
            ..ChatAttachment::default()
        }),
        _ => None,
    }
}

fn has_mime_prefix(mime_type: &str, prefix: &str) -> bool {
    let mime_type = mime_type.trim();
    mime_type
        .get(..prefix.len())
        .is_some_and(|value| value.eq_ignore_ascii_case(prefix))
}

/// Return the text Go fetcher would place into `Message.Text` before processing.
#[must_use]
pub fn fetcher_message_text(message: &TelegramMessage) -> String {
    if let Some(text) = message.get_text() {
        return text.as_ref().to_owned();
    }

    match &message.data {
        TelegramMessageData::Sticker(sticker) => sticker.emoji.clone().unwrap_or_default(),
        TelegramMessageData::Audio(audio) => audio_fallback_text(&audio.data),
        TelegramMessageData::Video(video) => video.data.file_name.clone().unwrap_or_default(),
        TelegramMessageData::Document(document) => {
            document.data.file_name.clone().unwrap_or_default()
        }
        TelegramMessageData::Contact(contact) => format!(
            "{} {}",
            contact.first_name,
            contact.last_name.as_deref().unwrap_or_default()
        )
        .trim()
        .to_owned(),
        _ => String::new(),
    }
}

fn message_text_before_fetcher_fallback(message: &TelegramMessage) -> String {
    match &message.data {
        TelegramMessageData::Text(text) => text.as_ref().to_owned(),
        _ => String::new(),
    }
}

fn addressable_message_text(message: &TelegramMessage) -> (String, String) {
    let message_text = fetcher_message_text(message);
    (message_text.clone(), message_text)
}

fn cut_first_word(text: &str) -> (String, String) {
    let Some((first, rest)) = text.split_once(' ') else {
        return (first_word_trim_no_space(text).to_owned(), String::new());
    };

    (
        first_word_trim(first).to_owned(),
        rest.trim_start_matches(word_separator).to_owned(),
    )
}

fn first_word_trim_no_space(value: &str) -> &str {
    value.trim_end_matches([' ', ',', '.', '!', '?', ':'])
}

fn first_word_trim(value: &str) -> &str {
    value.trim_end_matches(word_separator)
}

fn word_separator(ch: char) -> bool {
    matches!(ch, ' ' | ',' | '.' | '!' | '?' | ':' | '\r' | '\n' | '\t')
}

fn is_edit_verb(word: &str) -> bool {
    let trimmed = word.trim();
    if trimmed.is_empty() {
        return false;
    }

    let lower = trimmed.to_lowercase();
    EDIT_VERB_ALIASES.contains(&lower.as_str())
}

fn first_draw_reply_prefix(text: &str) -> Option<&'static str> {
    DRAW_REPLY_PREFIXES.iter().copied().find(|prefix| {
        text.get(..prefix.len())
            .is_some_and(|head| head.to_lowercase() == *prefix)
    })
}

fn add_image_prompt_part(parts: &mut Vec<String>, seen: &mut HashSet<String>, value: &str) {
    let trimmed = value.trim();
    if trimmed.is_empty() || !seen.insert(trimmed.to_owned()) {
        return;
    }
    parts.push(trimmed.to_owned());
}

const DRAW_VERB_ALIASES: &[&str] = &["нарисуй", "draw", "рисуй"];

const DRAW_REPLY_PREFIXES: &[&str] = &["это", "this", "that", "these", "those"];

const EDIT_VERB_ALIASES: &[&str] = &[
    "изменить",
    "измени",
    "измените",
    "отредактируй",
    "отредактируйте",
    "редактируй",
    "редактируйте",
    "поправь",
    "поправьте",
    "исправь",
    "исправьте",
    "переделай",
    "переделайте",
    "перерисуй",
    "перерисуйте",
    "замени",
    "замените",
    "убери",
    "уберите",
    "добавь",
    "добавьте",
    "убавь",
    "убавьте",
    "усиль",
    "усильте",
    "edit",
    "modify",
    "change",
    "alter",
    "retouch",
    "fix",
    "replace",
    "remove",
    "delete",
    "add",
    "adjust",
    "tweak",
];

fn telegram_chat_is_private(chat: &TelegramChat) -> bool {
    matches!(chat, TelegramChat::Private(_))
}

fn addressed_by_bot_name(bot: &TelegramUser, first_word: &str) -> bool {
    is_equal_transliterated(&bot.first_name, first_word)
}

fn addressed_by_bot_mention(bot: &TelegramUser, first_word: &str) -> bool {
    let Some(username) = bot.username.as_ref() else {
        return false;
    };
    first_word.eq_ignore_ascii_case(&format!("@{username}"))
}

fn addressed_by_bot_reply(message: &TelegramMessage, bot: &TelegramUser) -> bool {
    let Some(TelegramReplyTo::Message(reply)) = message.reply_to.as_ref() else {
        return false;
    };
    let Some(reply_user) = reply.sender.get_user() else {
        return false;
    };
    if reply_user.id != bot.id {
        return false;
    }

    let thread_id = message.message_thread_id.unwrap_or_default();
    reply.id != thread_id
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BotCommandInMessage {
    command: String,
    target: Option<String>,
}

fn leading_bot_command(message: &TelegramMessage) -> Option<BotCommandInMessage> {
    let TelegramMessageData::Text(text) = &message.data else {
        return None;
    };
    leading_bot_command_from_text(text)
}

fn leading_bot_command_from_text(text: &TelegramText) -> Option<BotCommandInMessage> {
    let first = text.entities.as_ref()?.into_iter().next()?;
    let TelegramTextEntity::BotCommand(position) = first else {
        return None;
    };
    if position.offset != 0 {
        return None;
    }

    let command_with_slash = text_entity_content(&text.data, *position);
    let command_with_target = command_with_slash.strip_prefix('/')?;
    let (command, target) = match command_with_target.split_once('@') {
        Some((command, target)) => (command, Some(target.to_owned())),
        None => (command_with_target, None),
    };

    Some(BotCommandInMessage {
        command: command.to_owned(),
        target,
    })
}

fn text_entity_content(text: &str, position: TelegramTextEntityPosition) -> String {
    String::from_utf16_lossy(
        &text
            .encode_utf16()
            .skip(position.offset as usize)
            .take(position.length as usize)
            .collect::<Vec<u16>>(),
    )
}

fn is_equal_transliterated(name: &str, given: &str) -> bool {
    if name.eq_ignore_ascii_case(given) {
        return true;
    }

    let name = name.to_lowercase();
    let given = given.to_lowercase();
    transliterate1(&name) == given || transliterate2(&name) == given
}

fn transliterate1(value: &str) -> String {
    transliterate(value, TransliterationStyle::One)
}

fn transliterate2(value: &str) -> String {
    transliterate(value, TransliterationStyle::Two)
}

#[derive(Clone, Copy)]
enum TransliterationStyle {
    One,
    Two,
}

fn transliterate(value: &str, style: TransliterationStyle) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match transliterated_char(ch, style) {
            Some(replacement) => out.push_str(replacement),
            None => out.push(ch),
        }
    }
    out
}

fn transliterated_char(ch: char, style: TransliterationStyle) -> Option<&'static str> {
    let value = match ch {
        'а' => "a",
        'б' => "b",
        'в' => match style {
            TransliterationStyle::One => "v",
            TransliterationStyle::Two => "w",
        },
        'г' => "g",
        'д' => "d",
        'е' | 'ё' => "e",
        'ж' => "j",
        'з' => "z",
        'и' => "i",
        'й' => "y",
        'к' => "k",
        'л' => "l",
        'м' => "m",
        'н' => "n",
        'о' => "o",
        'п' => "p",
        'р' => "r",
        'с' => "s",
        'т' => "t",
        'у' => "u",
        'ф' => "f",
        'х' => "h",
        'ц' => "c",
        'ч' => "ch",
        'ш' => "sh",
        'щ' => "shch",
        'ы' => "y",
        'э' => "e",
        'ю' => match style {
            TransliterationStyle::One => "yu",
            TransliterationStyle::Two => "ju",
        },
        'я' => match style {
            TransliterationStyle::One => "ya",
            TransliterationStyle::Two => "ja",
        },
        'ъ' | 'ь' => "",
        _ => return None,
    };
    Some(value)
}

fn audio_fallback_text(audio: &carapax::types::Audio) -> String {
    let performer = audio.performer.as_deref().unwrap_or_default();
    let title = audio.title.as_deref().unwrap_or_default();
    if !performer.is_empty() || !title.is_empty() {
        return format!("{performer} {title}").trim().to_owned();
    }
    audio.file_name.clone().unwrap_or_default()
}

fn normalize_history_text_payload(
    text: &str,
    original_text: &str,
    mut meta: ChatMessageMeta,
) -> (String, String, ChatMessageMeta) {
    let trimmed_text = text.trim().to_owned();
    let mut trimmed_original = original_text.trim().to_owned();
    if trimmed_original == trimmed_text {
        trimmed_original.clear();
    }

    if !trimmed_text.is_empty() {
        for attachment in &mut meta.attachments {
            if attachment.source.trim() != "message" {
                continue;
            }
            if !attachment.caption.trim().is_empty() {
                attachment.caption.clear();
            }
            if attachment.content.trim() == trimmed_text {
                attachment.content.clear();
            }
        }
    }

    (trimmed_text, trimmed_original, meta)
}

fn history_text_entry_has_content(text: &str, original_text: &str, meta: &ChatMessageMeta) -> bool {
    !text.is_empty()
        || !original_text.is_empty()
        || !meta.message_type.trim().is_empty()
        || !meta.annotation.trim().is_empty()
        || !meta.vision_description.trim().is_empty()
        || !meta.attachments.is_empty()
}

fn fill_history_sender_meta(mut meta: ChatMessageMeta, sender: &MessageSender) -> ChatMessageMeta {
    if meta.sender_id == 0 {
        meta.sender_id = sender.id;
    }
    if meta.sender_type.is_empty() {
        meta.sender_type = sender.sender_type.clone();
    }
    if meta.sender_name.trim().is_empty() {
        meta.sender_name = sender.display_name();
    }
    if meta.sender_username.trim().is_empty() {
        meta.sender_username = sender.username.trim().to_owned();
    }
    meta
}

fn history_message_payload(message: &TelegramMessage, text: &str) -> Option<Value> {
    let mut payload = Map::new();
    payload.insert(
        "message_id".to_owned(),
        serde_json::to_value(message.id).ok()?,
    );
    payload.insert("chat".to_owned(), serde_json::to_value(&message.chat).ok()?);
    payload.insert("date".to_owned(), serde_json::to_value(message.date).ok()?);
    if !text.is_empty() {
        payload.insert("text".to_owned(), Value::String(text.to_owned()));
    }

    merge_json_object(&mut payload, serde_json::to_value(&message.sender).ok()?);

    if let Some(thread_id) = message.message_thread_id {
        payload.insert(
            "message_thread_id".to_owned(),
            serde_json::to_value(thread_id).ok()?,
        );
    }
    if let Some(origin) = message.forward_origin.as_ref() {
        payload.insert(
            "forward_origin".to_owned(),
            serde_json::to_value(origin).ok()?,
        );
    }
    if message.is_automatic_forward {
        payload.insert("is_automatic_forward".to_owned(), Value::Bool(true));
    }
    if let Some(via_bot) = message.via_bot.as_ref() {
        payload.insert("via_bot".to_owned(), serde_json::to_value(via_bot).ok()?);
    }
    if let Some(reply) = reply_message_stub_value(message) {
        payload.insert("reply_to_message".to_owned(), reply);
    }

    Some(Value::Object(payload))
}

fn reply_message_stub_value(message: &TelegramMessage) -> Option<Value> {
    let reply = message.reply_to.as_ref()?;
    let carapax::types::ReplyTo::Message(reply) = reply else {
        return None;
    };

    let mut stub = Map::new();
    stub.insert(
        "message_id".to_owned(),
        serde_json::to_value(reply.id).ok()?,
    );
    merge_json_object(&mut stub, serde_json::to_value(&reply.sender).ok()?);
    Some(Value::Object(stub))
}

fn history_entry_sender_id(meta: &ChatMessageMeta, message: &TelegramMessage) -> i64 {
    if meta.sender_id != 0 {
        return meta.sender_id;
    }

    match &message.sender {
        carapax::types::MessageSender::Chat(chat) => chat.get_id().into(),
        carapax::types::MessageSender::User(user) => user.id.into(),
        carapax::types::MessageSender::Unknown => 0,
    }
}

fn merge_json_object(target: &mut Map<String, Value>, source: Value) {
    if let Value::Object(source) = source {
        target.extend(source);
    }
}

async fn run_stage<Fut, E>(stage: UpdateStage, timeout: Duration, task: Fut) -> UpdateStageReport
where
    Fut: Future<Output = Result<(), E>>,
    E: fmt::Display,
{
    let started = Instant::now();
    let outcome = if timeout.is_zero() {
        stage_outcome(task.await)
    } else {
        match tokio::time::timeout(timeout, task).await {
            Ok(result) => stage_outcome(result),
            Err(_) => UpdateStageOutcome::TimedOut,
        }
    };

    UpdateStageReport {
        stage,
        outcome,
        elapsed: started.elapsed(),
    }
}

fn stage_outcome<E>(result: Result<(), E>) -> UpdateStageOutcome
where
    E: fmt::Display,
{
    match result {
        Ok(()) => UpdateStageOutcome::Completed,
        Err(error) => UpdateStageOutcome::Failed(error.to_string()),
    }
}

fn side_effect_message_unix_date(update: &TelegramUpdate) -> Option<i64> {
    match &update.update_type {
        TelegramUpdateType::Message(message) | TelegramUpdateType::GuestMessage(message) => {
            Some(message.date)
        }
        TelegramUpdateType::CallbackQuery(query) => {
            query.message.as_ref().map(|message| match message {
                MaybeInaccessibleMessage::Message(message) => message.date,
                MaybeInaccessibleMessage::InaccessibleMessage(_) => 0,
            })
        }
        _ => None,
    }
}

fn unix_timestamp_seconds(time: SystemTime) -> i128 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i128::from(duration.as_secs()),
        Err(error) => -i128::from(error.duration().as_secs()),
    }
}

/// Errors returned while compressing or decompressing queued update payloads.
#[derive(Debug, Error)]
pub enum UpdateCodecError {
    /// zstd compression or decompression failed.
    #[error("failed to process zstd update payload: {0}")]
    Zstd(#[from] io::Error),
    /// JSON serialization or deserialization failed.
    #[error("failed to process native update JSON payload: {0}")]
    Json(#[from] serde_json::Error),
    /// The decoded envelope is not a supported Rust update queue format.
    #[error("unsupported native update frame {codec} version {version}")]
    UnsupportedFormat {
        /// Decoded format version.
        version: u16,
        /// Decoded codec string.
        codec: String,
    },
}

/// Errors returned by the Redis-backed update queue.
#[derive(Debug, Error)]
pub enum UpdateQueueError {
    /// The zstd/native JSON payload could not be prepared.
    #[error(transparent)]
    Codec(#[from] UpdateCodecError),
    /// Redis command failed.
    #[error("update queue Redis operation failed: {0}")]
    Redis(#[from] redis::RedisError),
}

fn blpop_timeout_arg(timeout: Duration) -> String {
    if timeout.is_zero() {
        return "0".to_owned();
    }
    if timeout.subsec_nanos() == 0 {
        return timeout.as_secs().to_string();
    }

    let mut seconds = format!("{:.9}", timeout.as_secs_f64());
    while seconds.ends_with('0') {
        seconds.pop();
    }
    if seconds.ends_with('.') {
        seconds.pop();
    }
    seconds
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        env,
        error::Error,
        io,
        sync::{Arc, Mutex},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use openplotva_core::{ChatAttachment, ChatMessageMeta, SENDER_TYPE_USER};
    use serde_json::{Value, json};

    use super::{
        DEFAULT_UPDATE_QUEUE_KEY, EncodedUpdate, GO_ALLOWED_UPDATE_NAMES, GO_ALLOWED_UPDATES,
        GoUpdateType, RedisUpdateQueue, TelegramMessageAttachmentOptions, UpdateCodecError,
        UpdateConsumerConfig, UpdateProducerQueue, UpdateProducerQueueFuture, UpdateProducerSource,
        UpdateProducerSourceFuture, UpdateStageOutcome, blpop_timeout_arg, compose_image_prompt,
        edited_image_prompt_update, extract_update_state, fetcher_message_text,
        is_allowed_producer_update, is_settings_command_message, parse_edit_command,
        parse_if_addressed, process_update_at, producer_update_name, producer_update_type,
        resolve_draw_prompt_from_message, run_update_producer_until, telegram_message_attachments,
        update_name,
    };
    use carapax::types::{
        Message as TelegramMessage, Update as TelegramUpdate, UpdateType as TelegramUpdateType,
    };

    #[test]
    fn queue_key_matches_go_update_queue() {
        assert_eq!(DEFAULT_UPDATE_QUEUE_KEY, "plotva:updates:queue");
    }

    #[test]
    fn encoded_update_preserves_queue_value_bytes() {
        let value = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00];
        let update = EncodedUpdate::from_queue_value(value.clone());

        assert_eq!(update.as_queue_value(), value.as_slice());
        assert_eq!(update.into_queue_value(), value);
    }

    #[test]
    fn zstd_update_frame_round_trips_native_json_bytes() -> Result<(), Box<dyn Error>> {
        let json_bytes = br#"{"version":1,"codec":"openplotva.update.v1+carapax-json.zstd"}"#;
        let update = EncodedUpdate::from_native_json_bytes(json_bytes)?;

        assert_ne!(update.as_queue_value(), json_bytes);
        assert_eq!(update.decompress_native_json()?, json_bytes);

        Ok(())
    }

    #[test]
    fn native_update_frame_round_trips_carapax_update() -> Result<(), Box<dyn Error>> {
        let update = sample_message_update()?;
        let encoded = EncodedUpdate::from_update(&update)?;
        let decoded = encoded.decode_update()?;

        assert_eq!(decoded.id, update.id);
        assert_eq!(
            serde_json::to_value(&decoded)?,
            serde_json::to_value(&update)?
        );
        let text = decoded
            .get_message()
            .and_then(|message| message.get_text())
            .ok_or_else(|| io::Error::other("expected decoded message text"))?;
        assert_eq!(text.as_ref(), "/start hello");

        Ok(())
    }

    #[test]
    fn unsupported_native_update_frame_is_rejected() -> Result<(), Box<dyn Error>> {
        let payload = serde_json::to_vec(&json!({
            "version": 2,
            "codec": "unsupported",
            "update": {
                "future_update_shape": true
            }
        }))?;
        let encoded = EncodedUpdate::from_native_json_bytes(&payload)?;

        assert!(matches!(
            encoded.decode_update(),
            Err(UpdateCodecError::UnsupportedFormat {
                version: 2,
                ref codec,
            }) if codec == "unsupported"
        ));

        Ok(())
    }

    #[test]
    fn invalid_zstd_update_frame_is_rejected() {
        let update = EncodedUpdate::from_queue_value(b"not zstd".to_vec());

        assert!(update.decompress_native_json().is_err());
    }

    #[test]
    fn blpop_timeout_argument_matches_go_second_values() {
        assert_eq!(blpop_timeout_arg(Duration::ZERO), "0");
        assert_eq!(blpop_timeout_arg(Duration::from_secs(5)), "5");
        assert_eq!(blpop_timeout_arg(Duration::from_millis(1500)), "1.5");
        assert_eq!(blpop_timeout_arg(Duration::from_millis(1)), "0.001");
    }

    #[test]
    fn allowed_update_names_match_go_fetcher_startup_contract() -> Result<(), Box<dyn Error>> {
        assert_eq!(
            GO_ALLOWED_UPDATE_NAMES,
            &[
                "message",
                "edited_message",
                "guest_message",
                "inline_query",
                "chosen_inline_result",
                "callback_query",
                "my_chat_member",
                "chat_member",
                "chat_join_request",
                "pre_checkout_query",
            ]
        );

        let serialized_updates: Vec<String> = GO_ALLOWED_UPDATES
            .iter()
            .map(|update| serde_json::from_value(serde_json::to_value(update)?))
            .collect::<Result<_, _>>()?;

        assert_eq!(serialized_updates, GO_ALLOWED_UPDATE_NAMES);
        Ok(())
    }

    #[test]
    fn producer_update_type_matches_go_fetcher_classification() -> Result<(), Box<dyn Error>> {
        let cases = [
            (
                "message",
                sample_message_update()?,
                GoUpdateType::Message,
                true,
            ),
            (
                "edited_message",
                sample_update_json(json!({
                    "update_id": 1,
                    "edited_message": sample_message_json(2, 1_710_000_000, "edited")
                }))?,
                GoUpdateType::EditedMessage,
                true,
            ),
            (
                "guest_message",
                sample_update_json(json!({
                    "update_id": 2,
                    "guest_message": {
                        "message_id": 3,
                        "date": 1_710_000_000,
                        "chat": {
                            "id": -100,
                            "type": "supergroup",
                            "title": "Team",
                            "is_forum": true
                        },
                        "guest_query_id": "guest-query",
                        "text": "hello"
                    }
                }))?,
                GoUpdateType::GuestMessage,
                true,
            ),
            (
                "channel_post",
                sample_update_json(json!({
                    "update_id": 3,
                    "channel_post": sample_channel_message_json(4, 1_710_000_000, "post")
                }))?,
                GoUpdateType::ChannelPost,
                false,
            ),
            (
                "edited_channel_post",
                sample_update_json(json!({
                    "update_id": 4,
                    "edited_channel_post": sample_channel_message_json(5, 1_710_000_000, "post")
                }))?,
                GoUpdateType::EditedChannelPost,
                false,
            ),
            (
                "inline_query",
                sample_update_json(json!({
                    "update_id": 5,
                    "inline_query": {
                        "from": sample_user_json(),
                        "id": "query-id",
                        "offset": "query offset",
                        "query": "query query"
                    }
                }))?,
                GoUpdateType::InlineQuery,
                true,
            ),
            (
                "chosen_inline_result",
                sample_update_json(json!({
                    "update_id": 6,
                    "chosen_inline_result": {
                        "from": sample_user_json(),
                        "query": "q",
                        "result_id": "chosen-inline-result-id"
                    }
                }))?,
                GoUpdateType::ChosenInlineResult,
                true,
            ),
            (
                "callback_query",
                sample_update_json(json!({
                    "update_id": 7,
                    "callback_query": {
                        "id": "callback-id",
                        "from": sample_user_json(),
                        "message": sample_message_json(8, 1_710_000_000, "callback")
                    }
                }))?,
                GoUpdateType::CallbackQuery,
                true,
            ),
            (
                "shipping_query",
                sample_update_json(json!({
                    "update_id": 8,
                    "shipping_query": {
                        "id": "query-id",
                        "from": sample_user_json(),
                        "invoice_payload": "payload",
                        "shipping_address": {
                            "city": "Gudermes",
                            "country_code": "RU",
                            "post_code": "366200",
                            "state": "Chechen Republic",
                            "street_line1": "Nuradilov st., 12",
                            "street_line2": ""
                        }
                    }
                }))?,
                GoUpdateType::ShippingQuery,
                false,
            ),
            (
                "pre_checkout_query",
                sample_update_json(json!({
                    "update_id": 9,
                    "pre_checkout_query": {
                        "currency": "GEL",
                        "from": sample_user_json(),
                        "id": "query-id",
                        "invoice_payload": "invoice payload",
                        "total_amount": 100
                    }
                }))?,
                GoUpdateType::PreCheckoutQuery,
                true,
            ),
            (
                "poll",
                sample_update_json(json!({
                    "update_id": 10,
                    "poll": {
                        "type": "regular",
                        "allows_multiple_answers": false,
                        "allows_revoting": false,
                        "id": "poll-id",
                        "is_anonymous": true,
                        "is_closed": true,
                        "members_only": false,
                        "options": [
                            {
                                "persistent_id": "1",
                                "text": "Yes",
                                "voter_count": 1000
                            },
                            {
                                "persistent_id": "2",
                                "text": "No",
                                "voter_count": 0
                            }
                        ],
                        "question": "Rust?",
                        "total_voter_count": 1000
                    }
                }))?,
                GoUpdateType::Poll,
                false,
            ),
            (
                "poll_answer",
                sample_update_json(json!({
                    "update_id": 11,
                    "poll_answer": {
                        "option_ids": [0],
                        "option_persistent_ids": [],
                        "poll_id": "poll-id",
                        "user": sample_user_json()
                    }
                }))?,
                GoUpdateType::PollAnswer,
                false,
            ),
            (
                "my_chat_member",
                sample_update_json(json!({
                    "update_id": 12,
                    "my_chat_member": sample_chat_member_updated_json(true)
                }))?,
                GoUpdateType::MyChatMember,
                true,
            ),
            (
                "chat_member",
                sample_update_json(json!({
                    "update_id": 13,
                    "chat_member": sample_chat_member_updated_json(false)
                }))?,
                GoUpdateType::ChatMember,
                true,
            ),
            (
                "chat_join_request",
                sample_update_json(json!({
                    "update_id": 14,
                    "chat_join_request": {
                        "chat": {
                            "type": "group",
                            "id": 1,
                            "title": "Group"
                        },
                        "date": 0,
                        "from": sample_user_json()
                    }
                }))?,
                GoUpdateType::ChatJoinRequest,
                true,
            ),
            (
                "message_reaction",
                sample_update_json(json!({
                    "update_id": 15,
                    "message_reaction": {
                        "chat": {
                            "type": "private",
                            "id": 1,
                            "first_name": "Ada"
                        },
                        "date": 0,
                        "message_id": 1,
                        "new_reaction": [
                            {
                                "type": "emoji",
                                "emoji": "\u{1f44d}"
                            }
                        ],
                        "old_reaction": [
                            {
                                "type": "emoji",
                                "emoji": "\u{1f44e}"
                            }
                        ]
                    }
                }))?,
                GoUpdateType::MessageReaction,
                false,
            ),
            (
                "message_reaction_count",
                sample_update_json(json!({
                    "update_id": 16,
                    "message_reaction_count": {
                        "chat": {
                            "type": "private",
                            "id": 1,
                            "first_name": "Ada"
                        },
                        "date": 0,
                        "message_id": 1,
                        "reactions": [
                            {
                                "type": {
                                    "type": "emoji",
                                    "emoji": "\u{1f44d}"
                                },
                                "total_count": 1
                            }
                        ]
                    }
                }))?,
                GoUpdateType::MessageReactionCount,
                false,
            ),
            (
                "unknown",
                sample_update_json(json!({
                    "update_id": 17,
                    "future_update_shape": {
                        "value": true
                    }
                }))?,
                GoUpdateType::Unknown,
                false,
            ),
        ];

        for (name, update, want_type, want_allowed) in cases {
            assert_eq!(producer_update_type(&update), want_type, "{name}");
            assert_eq!(producer_update_name(&update), want_type.as_str(), "{name}");
            assert_eq!(is_allowed_producer_update(&update), want_allowed, "{name}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn update_producer_enqueues_allowed_updates_and_skips_disallowed()
    -> Result<(), Box<dyn Error>> {
        let source = ProducerSourceStub::new(vec![
            sample_message_update_with_id(100)?,
            sample_poll_update_with_id(101)?,
            sample_callback_update_with_id(102)?,
        ]);
        let queue = ProducerQueueStub::default();

        let report = run_update_producer_until(&source, &queue, std::future::pending()).await;

        assert_eq!(report.received, 3);
        assert_eq!(report.enqueued, 2);
        assert_eq!(report.skipped, 1);
        assert!(report.source_closed);
        assert!(report.enqueue_errors.is_empty());
        assert_eq!(queue.enqueued_ids(), vec![100, 102]);
        Ok(())
    }

    #[tokio::test]
    async fn update_producer_continues_after_enqueue_errors_like_go() -> Result<(), Box<dyn Error>>
    {
        let source = ProducerSourceStub::new(vec![
            sample_message_update_with_id(100)?,
            sample_callback_update_with_id(101)?,
        ]);
        let queue = ProducerQueueStub::default().with_failures(vec!["redis unavailable"]);

        let report = run_update_producer_until(&source, &queue, std::future::pending()).await;

        assert_eq!(report.received, 2);
        assert_eq!(report.enqueued, 1);
        assert_eq!(report.skipped, 0);
        assert!(report.source_closed);
        assert_eq!(report.enqueue_errors, vec!["redis unavailable".to_owned()]);
        assert_eq!(queue.enqueued_ids(), vec![101]);
        Ok(())
    }

    #[tokio::test]
    async fn update_producer_stop_signal_exits_without_draining_source()
    -> Result<(), Box<dyn Error>> {
        let source = ProducerSourceStub::new(vec![sample_message_update_with_id(100)?]);
        let queue = ProducerQueueStub::default();

        let report = run_update_producer_until(&source, &queue, async {}).await;

        assert_eq!(report.received, 0);
        assert_eq!(report.enqueued, 0);
        assert_eq!(report.skipped, 0);
        assert!(!report.source_closed);
        assert!(queue.enqueued_ids().is_empty());
        Ok(())
    }

    #[test]
    fn update_name_matches_go_consumer_stats_names() -> Result<(), Box<dyn Error>> {
        assert_eq!(update_name(&sample_message_update()?), "message");
        assert_eq!(
            update_name(&serde_json::from_value(json!({
                "update_id": 1,
                "edited_message": sample_message_json(2, 1_710_000_000, "edited")
            }))?),
            "edited_message"
        );
        assert_eq!(
            update_name(&serde_json::from_value(json!({
                "update_id": 2,
                "callback_query": {
                    "id": "callback-id",
                    "from": sample_user_json(),
                    "message": sample_message_json(3, 1_710_000_000, "callback")
                }
            }))?),
            "callback_query"
        );
        assert_eq!(
            update_name(&serde_json::from_value(json!({
                "update_id": 3,
                "message_reaction": {
                    "chat": {
                        "id": 42,
                        "type": "private",
                        "first_name": "Ada"
                    },
                    "message_id": 99,
                    "date": 1_710_000_000,
                    "old_reaction": [],
                    "new_reaction": []
                }
            }))?),
            "unknown"
        );

        Ok(())
    }

    #[test]
    fn extract_update_state_matches_go_message_chat_user_params() -> Result<(), Box<dyn Error>> {
        let state = extract_update_state(&sample_message_update()?)
            .ok_or_else(|| io::Error::other("message update should produce chat and user state"))?;

        assert_eq!(
            state.chat,
            Some(openplotva_core::ChatState {
                id: 42,
                chat_type: "private".to_owned(),
                title: None,
                username: Some("ada_l".to_owned()),
                first_name: Some("Ada".to_owned()),
                last_name: None,
                is_forum: None,
            })
        );
        assert_eq!(
            state.user,
            Some(openplotva_core::UserState {
                id: 99,
                first_name: "Ada".to_owned(),
                last_name: None,
                username: Some("ada_l".to_owned()),
                language_code: None,
                is_premium: Some(false),
            })
        );

        Ok(())
    }

    #[test]
    fn extract_update_state_keeps_go_guest_message_identity_empty() -> Result<(), Box<dyn Error>> {
        let update = serde_json::from_value(json!({
            "update_id": 123,
            "guest_message": {
                "message_id": 55,
                "date": 1_710_000_000,
                "chat": {
                    "id": -100,
                    "type": "supergroup",
                    "title": "Team",
                    "is_forum": true
                },
                "guest_query_id": "guest-query",
                "text": "hello"
            }
        }))?;

        assert_eq!(update_name(&update), "guest_message");
        assert_eq!(extract_update_state(&update), None);

        Ok(())
    }

    #[test]
    fn extract_update_state_preserves_go_supergroup_chat_fields() -> Result<(), Box<dyn Error>> {
        let update = serde_json::from_value(json!({
            "update_id": 124,
            "message": {
                "message_id": 56,
                "date": 1_710_000_000,
                "chat": {
                    "id": -100,
                    "type": "supergroup",
                    "title": " Team ",
                    "username": "team_chat",
                    "is_forum": true
                },
                "from": sample_user_json(),
                "text": "hello"
            }
        }))?;

        let state = extract_update_state(&update)
            .ok_or_else(|| io::Error::other("supergroup message should produce state"))?;

        assert_eq!(
            state.chat,
            Some(openplotva_core::ChatState {
                id: -100,
                chat_type: "supergroup".to_owned(),
                title: Some(" Team ".to_owned()),
                username: Some("team_chat".to_owned()),
                first_name: None,
                last_name: None,
                is_forum: Some(true),
            })
        );

        Ok(())
    }

    #[test]
    fn telegram_message_attachments_promotes_image_document_like_go_utils()
    -> Result<(), Box<dyn Error>> {
        let update = serde_json::from_value(json!({
            "update_id": 125,
            "message": {
                "message_id": 57,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "caption": " diagram ",
                "document": {
                    "file_id": "doc-file",
                    "file_unique_id": "doc-image",
                    "file_name": "diagram.png",
                    "mime_type": "image/png"
                }
            }
        }))?;
        let message = update_message(&update)?;

        let got = telegram_message_attachments(
            message,
            TelegramMessageAttachmentOptions {
                caption: "diagram".to_owned(),
                promote_first_image_ref: true,
                ..TelegramMessageAttachmentOptions::default()
            },
        );

        assert_eq!(
            got,
            vec![
                openplotva_core::ChatAttachment {
                    kind: "image".to_owned(),
                    source: "message".to_owned(),
                    file_unique_id: "doc-image".to_owned(),
                    mime_type: "image/png".to_owned(),
                    caption: "diagram".to_owned(),
                    ..openplotva_core::ChatAttachment::default()
                },
                openplotva_core::ChatAttachment {
                    kind: "document".to_owned(),
                    source: "message".to_owned(),
                    file_unique_id: "doc-image".to_owned(),
                    file_name: "diagram.png".to_owned(),
                    mime_type: "image/png".to_owned(),
                    caption: "diagram".to_owned(),
                    ..openplotva_core::ChatAttachment::default()
                },
            ]
        );

        Ok(())
    }

    #[test]
    fn telegram_message_attachments_promotes_uppercase_image_mime_like_go_utils()
    -> Result<(), Box<dyn Error>> {
        let update = serde_json::from_value(json!({
            "update_id": 126,
            "message": {
                "message_id": 58,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "document": {
                    "file_id": "doc-file",
                    "file_unique_id": "doc-image",
                    "mime_type": " IMAGE/PNG "
                }
            }
        }))?;
        let message = update_message(&update)?;

        let got = telegram_message_attachments(
            message,
            TelegramMessageAttachmentOptions {
                source: " history ".to_owned(),
                caption: " caption ".to_owned(),
                promote_first_image_ref: true,
            },
        );

        assert_eq!(got[0].kind, "image");
        assert_eq!(got[0].source, "history");
        assert_eq!(got[0].file_unique_id, "doc-image");
        assert_eq!(got[0].mime_type, " IMAGE/PNG ");
        assert_eq!(got[0].caption, "caption");

        Ok(())
    }

    #[test]
    fn telegram_message_attachments_keeps_sticker_unpromoted_by_default_like_go_utils()
    -> Result<(), Box<dyn Error>> {
        let update = serde_json::from_value(json!({
            "update_id": 127,
            "message": {
                "message_id": 59,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "sticker": {
                    "file_id": "sticker-file",
                    "file_unique_id": "sticker-1",
                    "type": "regular",
                    "height": 512,
                    "width": 512,
                    "is_animated": false,
                    "is_video": false,
                    "emoji": "ok"
                }
            }
        }))?;
        let message = update_message(&update)?;

        let got =
            telegram_message_attachments(message, TelegramMessageAttachmentOptions::default());

        assert_eq!(
            got,
            vec![openplotva_core::ChatAttachment {
                kind: "sticker".to_owned(),
                source: "message".to_owned(),
                file_unique_id: "sticker-1".to_owned(),
                content: "ok".to_owned(),
                ..openplotva_core::ChatAttachment::default()
            }]
        );

        Ok(())
    }

    #[test]
    fn telegram_message_attachments_uses_last_photo_size_like_go_utils()
    -> Result<(), Box<dyn Error>> {
        let update = serde_json::from_value(json!({
            "update_id": 128,
            "message": {
                "message_id": 60,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "photo": [
                    {
                        "file_id": "small-file",
                        "file_unique_id": "small-photo",
                        "height": 90,
                        "width": 90
                    },
                    {
                        "file_id": "large-file",
                        "file_unique_id": "large-photo",
                        "height": 1280,
                        "width": 1280
                    }
                ],
                "caption": " photo caption "
            }
        }))?;
        let message = update_message(&update)?;

        let got = telegram_message_attachments(
            message,
            TelegramMessageAttachmentOptions {
                caption: " photo caption ".to_owned(),
                ..TelegramMessageAttachmentOptions::default()
            },
        );

        assert_eq!(got.len(), 1);
        assert_eq!(got[0].kind, "image");
        assert_eq!(got[0].file_unique_id, "large-photo");
        assert_eq!(got[0].caption, "photo caption");

        Ok(())
    }

    #[test]
    fn telegram_message_attachments_keeps_voice_caption_empty_like_go_utils()
    -> Result<(), Box<dyn Error>> {
        let update = serde_json::from_value(json!({
            "update_id": 129,
            "message": {
                "message_id": 61,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "voice": {
                    "file_id": "voice-file",
                    "file_unique_id": "voice-1",
                    "duration": 7
                },
                "caption": " voice caption "
            }
        }))?;
        let message = update_message(&update)?;

        let got = telegram_message_attachments(
            message,
            TelegramMessageAttachmentOptions {
                caption: "voice caption".to_owned(),
                ..TelegramMessageAttachmentOptions::default()
            },
        );

        assert_eq!(got.len(), 1);
        assert_eq!(got[0].kind, "voice");
        assert_eq!(got[0].file_unique_id, "voice-1");
        assert_eq!(got[0].duration_seconds, 7);
        assert_eq!(got[0].caption, "");

        Ok(())
    }

    #[test]
    fn telegram_message_attachments_preserves_contact_and_location_like_go_utils()
    -> Result<(), Box<dyn Error>> {
        let contact_update = serde_json::from_value(json!({
            "update_id": 130,
            "message": {
                "message_id": 62,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "contact": {
                    "phone_number": "+100",
                    "first_name": "Grace",
                    "last_name": "Hopper",
                    "user_id": 123
                }
            }
        }))?;
        let location_update = serde_json::from_value(json!({
            "update_id": 131,
            "message": {
                "message_id": 63,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "location": {
                    "latitude": 52.2297,
                    "longitude": 21.0122
                }
            }
        }))?;

        let contact = telegram_message_attachments(
            update_message(&contact_update)?,
            TelegramMessageAttachmentOptions::default(),
        );
        let location = telegram_message_attachments(
            update_message(&location_update)?,
            TelegramMessageAttachmentOptions::default(),
        );

        assert_eq!(
            contact,
            vec![openplotva_core::ChatAttachment {
                kind: "contact".to_owned(),
                source: "message".to_owned(),
                phone: "+100".to_owned(),
                first_name: "Grace".to_owned(),
                last_name: "Hopper".to_owned(),
                user_id: 123,
                ..openplotva_core::ChatAttachment::default()
            }]
        );
        assert_eq!(location.len(), 1);
        assert_eq!(location[0].kind, "location");
        assert_float_eq(location[0].latitude, 52.2297);
        assert_float_eq(location[0].longitude, 21.0122);

        Ok(())
    }

    #[test]
    fn message_type_prefers_message_attachment_kind_like_go_fetcher() -> Result<(), Box<dyn Error>>
    {
        let update = serde_json::from_value(json!({
            "update_id": 132,
            "message": {
                "message_id": 64,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "voice": {
                    "file_id": "voice-file",
                    "file_unique_id": "voice-1",
                    "duration": 7
                }
            }
        }))?;
        let attachments = vec![openplotva_core::ChatAttachment {
            kind: "audio".to_owned(),
            source: "message".to_owned(),
            ..openplotva_core::ChatAttachment::default()
        }];

        assert_eq!(
            super::detect_message_type(Some(update_message(&update)?), &attachments),
            "audio"
        );

        Ok(())
    }

    #[test]
    fn message_type_falls_back_to_telegram_fields_like_go_fetcher() -> Result<(), Box<dyn Error>> {
        let voice_update = serde_json::from_value(json!({
            "update_id": 133,
            "message": {
                "message_id": 65,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "voice": {
                    "file_id": "voice-file",
                    "file_unique_id": "voice-1",
                    "duration": 7
                }
            }
        }))?;
        let photo_update = serde_json::from_value(json!({
            "update_id": 134,
            "message": {
                "message_id": 66,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "photo": [
                    {
                        "file_id": "photo-file",
                        "file_unique_id": "photo-1",
                        "height": 90,
                        "width": 90
                    }
                ]
            }
        }))?;
        let text_update = sample_message_update()?;

        assert_eq!(
            super::detect_message_type(Some(update_message(&voice_update)?), &[]),
            "voice"
        );
        assert_eq!(
            super::detect_message_type(Some(update_message(&photo_update)?), &[]),
            "image"
        );
        assert_eq!(
            super::detect_message_type(Some(update_message(&text_update)?), &[]),
            "text"
        );
        assert_eq!(super::detect_message_type(None, &[]), "text");

        Ok(())
    }

    #[test]
    fn normalize_attachment_kind_uses_case_insensitive_mime_prefix_like_go_fetcher() {
        let got = super::normalize_attachment_kind(openplotva_core::ChatAttachment {
            mime_type: " IMAGE/PNG ".to_owned(),
            ..openplotva_core::ChatAttachment::default()
        });

        assert_eq!(got.kind, "image");
    }

    #[test]
    fn collect_media_attachments_skips_existing_attachment_like_go_fetcher()
    -> Result<(), Box<dyn Error>> {
        let update = serde_json::from_value(json!({
            "update_id": 135,
            "message": {
                "message_id": 67,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "photo": [
                    {
                        "file_id": "photo-file",
                        "file_unique_id": "photo-1",
                        "height": 90,
                        "width": 90
                    }
                ]
            }
        }))?;
        let existing = vec![openplotva_core::ChatAttachment {
            kind: "image".to_owned(),
            source: "message".to_owned(),
            file_unique_id: "photo-1".to_owned(),
            ..openplotva_core::ChatAttachment::default()
        }];

        let got = super::collect_media_attachments(Some(update_message(&update)?), &existing);

        assert!(
            got.is_empty(),
            "duplicate attachment should be skipped: {got:?}"
        );
        Ok(())
    }

    #[test]
    fn collect_media_attachments_keeps_voice_caption_empty_like_go_fetcher()
    -> Result<(), Box<dyn Error>> {
        let update = serde_json::from_value(json!({
            "update_id": 136,
            "message": {
                "message_id": 68,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "caption": " voice caption ",
                "voice": {
                    "file_id": "voice-file",
                    "file_unique_id": "voice-1",
                    "duration": 7
                }
            }
        }))?;

        let got = super::collect_media_attachments(Some(update_message(&update)?), &[]);

        assert_eq!(got.len(), 1);
        assert_eq!(got[0].kind, "voice");
        assert_eq!(got[0].file_unique_id, "voice-1");
        assert_eq!(got[0].duration_seconds, 7);
        assert_eq!(got[0].caption, "");
        Ok(())
    }

    #[test]
    fn fetcher_message_text_matches_go_fallback_order() -> Result<(), Box<dyn Error>> {
        let caption_update = serde_json::from_value(json!({
            "update_id": 1,
            "message": {
                "message_id": 77,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "caption": " photo caption ",
                "photo": [{"file_id": "small", "file_unique_id": "small-u", "width": 1, "height": 1}]
            }
        }))?;
        assert_eq!(
            fetcher_message_text(update_message(&caption_update)?),
            " photo caption "
        );

        let audio_update = serde_json::from_value(json!({
            "update_id": 2,
            "message": {
                "message_id": 78,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "audio": {
                    "file_id": "audio-file",
                    "file_unique_id": "audio-unique",
                    "duration": 5,
                    "performer": " Ada ",
                    "title": " Theme "
                }
            }
        }))?;
        assert_eq!(
            fetcher_message_text(update_message(&audio_update)?),
            "Ada   Theme"
        );

        let contact_update = serde_json::from_value(json!({
            "update_id": 3,
            "message": {
                "message_id": 79,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "contact": {
                    "phone_number": "+123",
                    "first_name": "Ada",
                    "last_name": "Lovelace"
                }
            }
        }))?;
        assert_eq!(
            fetcher_message_text(update_message(&contact_update)?),
            "Ada Lovelace"
        );
        Ok(())
    }

    #[test]
    fn resolve_message_sender_matches_go_user_display_fields() -> Result<(), Box<dyn Error>> {
        let update = serde_json::from_value(json!({
            "update_id": 137,
            "message": {
                "message_id": 69,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": {
                    "id": 99,
                    "is_bot": true,
                    "first_name": " Ada ",
                    "last_name": " Lovelace ",
                    "username": " ada_l "
                },
                "text": "hello"
            }
        }))?;

        assert_eq!(
            super::resolve_message_sender(Some(update_message(&update)?)),
            openplotva_core::MessageSender {
                sender_type: openplotva_core::SENDER_TYPE_USER.to_owned(),
                id: 99,
                full_name: "Ada   Lovelace".to_owned(),
                username: "ada_l".to_owned(),
                is_bot: true,
            }
        );

        Ok(())
    }

    #[test]
    fn resolve_message_sender_matches_go_channel_and_system_paths() -> Result<(), Box<dyn Error>> {
        let channel_update = serde_json::from_value(json!({
            "update_id": 138,
            "message": {
                "message_id": 70,
                "date": 1_710_000_000,
                "chat": {
                    "id": -100,
                    "type": "supergroup",
                    "title": "Discussion"
                },
                "sender_chat": {
                    "id": -200,
                    "type": "channel",
                    "title": "News",
                    "username": " news "
                },
                "text": "post"
            }
        }))?;
        let same_chat_update = serde_json::from_value(json!({
            "update_id": 139,
            "message": {
                "message_id": 71,
                "date": 1_710_000_000,
                "chat": {
                    "id": -100,
                    "type": "supergroup",
                    "title": "Discussion",
                    "username": "discussion"
                },
                "sender_chat": {
                    "id": -100,
                    "type": "supergroup",
                    "title": "Discussion",
                    "username": "discussion"
                },
                "text": "anonymous"
            }
        }))?;

        assert_eq!(
            super::resolve_message_sender(Some(update_message(&channel_update)?)),
            openplotva_core::MessageSender {
                sender_type: openplotva_core::SENDER_TYPE_CHANNEL.to_owned(),
                id: -200,
                full_name: "📣 News".to_owned(),
                username: "news".to_owned(),
                is_bot: false,
            }
        );
        assert_eq!(
            super::resolve_message_sender(Some(update_message(&same_chat_update)?)).sender_type,
            openplotva_core::SENDER_TYPE_SAME_CHAT
        );
        assert_eq!(
            super::resolve_message_sender(None),
            openplotva_core::MessageSender::system()
        );

        Ok(())
    }

    #[test]
    fn build_message_meta_matches_go_fetcher_shape() -> Result<(), Box<dyn Error>> {
        let update = serde_json::from_value(json!({
            "update_id": 140,
            "message": {
                "message_id": 72,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "photo": [
                    {
                        "file_id": "photo-file",
                        "file_unique_id": "photo-1",
                        "height": 90,
                        "width": 90
                    }
                ],
                "caption": " photo caption "
            }
        }))?;
        let message = update_message(&update)?;
        let sender = super::resolve_message_sender(Some(message));
        let existing = vec![openplotva_core::ChatAttachment {
            kind: "audio".to_owned(),
            source: "history".to_owned(),
            file_unique_id: "audio-1".to_owned(),
            ..openplotva_core::ChatAttachment::default()
        }];

        let got = super::build_message_meta(Some(message), sender, &existing, " vision ");

        assert_eq!(got.message_type, "image");
        assert_eq!(got.vision_description, "vision");
        assert_eq!(got.sender_type, openplotva_core::SENDER_TYPE_USER);
        assert_eq!(got.sender_id, 99);
        assert_eq!(got.sender_name, "Ada");
        assert_eq!(got.sender_username, "ada_l");
        assert_eq!(got.attachments.len(), 2);
        assert_eq!(got.attachments[0], existing[0]);
        assert_eq!(got.attachments[1].kind, "image");
        assert_eq!(got.attachments[1].source, "message");
        assert_eq!(got.attachments[1].file_unique_id, "photo-1");
        assert_eq!(got.attachments[1].caption, "photo caption");

        Ok(())
    }

    #[test]
    fn build_history_text_entry_preserves_go_message_entry_payload() -> Result<(), Box<dyn Error>> {
        let update = sample_update_json(json!({
            "update_id": 1,
            "message": {
                "message_id": 10,
                "date": 1_710_000_000,
                "message_thread_id": 77,
                "chat": {
                    "id": -100,
                    "type": "supergroup",
                    "title": "Team",
                    "is_forum": true
                },
                "from": {
                    "id": 42,
                    "is_bot": false,
                    "first_name": "Alice"
                },
                "text": " forwarded text ",
                "forward_origin": {
                    "type": "channel",
                    "date": 1_709_999_900,
                    "message_id": 99,
                    "chat": {
                        "id": -200,
                        "type": "channel",
                        "title": "News"
                    }
                },
                "is_automatic_forward": true,
                "via_bot": {
                    "id": 77,
                    "is_bot": true,
                    "first_name": "Inline",
                    "username": "inline_bot"
                }
            }
        }))?;
        let message = update_message(&update)?;

        let entry = super::build_history_text_entry(
            message,
            " forwarded text ",
            ChatMessageMeta::default(),
            0,
        )
        .ok_or_else(|| io::Error::other("expected a history entry"))?;

        assert_eq!(entry.entry_id, "msg:10");
        assert_eq!(entry.kind, "text");
        assert_eq!(entry.role, "user");
        assert_eq!(entry.chat_id, -100);
        assert_eq!(entry.thread_id, 77);
        assert_eq!(entry.message_id, 10);
        assert_eq!(entry.sender_id, 42);
        assert_eq!(entry.occurred_at.unix_timestamp(), 1_710_000_000);

        let payload: Value = serde_json::from_slice(&entry.payload)?;
        assert_eq!(payload["entry_id"], "msg:10");
        assert_eq!(payload["role"], "user");
        assert_eq!(payload["kind"], "text");
        assert_eq!(payload["timestamp"], "2024-03-09T16:00:00Z");
        assert_eq!(payload["message_id"], 10);
        assert_eq!(payload["message_thread_id"], 77);
        assert_eq!(payload["chat"]["id"], -100);
        assert_eq!(payload["from"]["id"], 42);
        assert_eq!(payload["text"], "forwarded text");
        assert!(payload.get("original_text").is_none());
        assert_eq!(payload["meta"]["sender_id"], 42);
        assert_eq!(payload["meta"]["sender_type"], SENDER_TYPE_USER);
        assert_eq!(payload["meta"]["sender_name"], "Alice");
        assert_eq!(payload["forward_origin"]["type"], "channel");
        assert_eq!(payload["forward_origin"]["message_id"], 99);
        assert_eq!(payload["is_automatic_forward"], true);
        assert_eq!(payload["via_bot"]["username"], "inline_bot");

        Ok(())
    }

    #[test]
    fn parse_if_addressed_matches_go_group_and_private_addressing() -> Result<(), Box<dyn Error>> {
        let bot = sample_bot_user();
        let group = sample_message_from_value(json!({
            "message_id": 1,
            "date": 1_710_000_000,
            "chat": {
                "id": -42,
                "type": "group",
                "title": "Group"
            },
            "from": sample_user_json(),
            "text": "@PlotvaBot измени фон"
        }))?;
        let private = sample_message_from_value(json!({
            "message_id": 2,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "text": "@PlotvaBot измени фон"
        }))?;

        let parsed = parse_if_addressed(&group, &bot);
        assert!(parsed.is_addressed);
        assert_eq!(parsed.message_text, "@PlotvaBot измени фон");
        assert_eq!(parsed.first_word, "измени");
        assert_eq!(parsed.rest_text, "фон");

        let parsed = parse_if_addressed(&private, &bot);
        assert!(parsed.is_addressed);
        assert_eq!(parsed.first_word, "@PlotvaBot");
        assert_eq!(parsed.rest_text, "измени фон");
        Ok(())
    }

    #[test]
    fn parse_if_addressed_matches_go_name_transliteration_and_reply_rules()
    -> Result<(), Box<dyn Error>> {
        let bot = sample_bot_user();
        let name_address = sample_message_from_value(json!({
            "message_id": 3,
            "date": 1_710_000_000,
            "chat": {
                "id": -42,
                "type": "group",
                "title": "Group"
            },
            "from": sample_user_json(),
            "text": "plotva, измени фон"
        }))?;
        let reply_address = sample_message_from_value(json!({
            "message_id": 4,
            "date": 1_710_000_000,
            "chat": {
                "id": -42,
                "type": "group",
                "title": "Group"
            },
            "from": sample_user_json(),
            "text": "а так?",
            "reply_to_message": {
                "message_id": 99,
                "date": 1_710_000_000,
                "chat": {
                    "id": -42,
                    "type": "group",
                    "title": "Group"
                },
                "from": {
                    "id": 777,
                    "is_bot": true,
                    "first_name": "Плотва",
                    "username": "PlotvaBot"
                },
                "text": "old answer"
            }
        }))?;
        let topic_reply = sample_message_from_value(json!({
            "message_id": 5,
            "message_thread_id": 99,
            "date": 1_710_000_000,
            "chat": {
                "id": -42,
                "type": "supergroup",
                "title": "Forum",
                "is_forum": true
            },
            "from": sample_user_json(),
            "text": "topic root reply",
            "reply_to_message": {
                "message_id": 99,
                "date": 1_710_000_000,
                "chat": {
                    "id": -42,
                    "type": "supergroup",
                    "title": "Forum",
                    "is_forum": true
                },
                "from": {
                    "id": 777,
                    "is_bot": true,
                    "first_name": "Плотва",
                    "username": "PlotvaBot"
                },
                "text": "topic starter"
            }
        }))?;

        let parsed = parse_if_addressed(&name_address, &bot);
        assert!(parsed.is_addressed);
        assert_eq!(parsed.first_word, "измени");
        assert_eq!(parsed.rest_text, "фон");

        let parsed = parse_if_addressed(&reply_address, &bot);
        assert!(parsed.is_addressed);
        assert_eq!(parsed.first_word, "а");
        assert_eq!(parsed.rest_text, "так?");

        let parsed = parse_if_addressed(&topic_reply, &bot);
        assert!(!parsed.is_addressed);
        Ok(())
    }

    #[test]
    fn settings_command_message_matches_go_bot_target_rules() -> Result<(), Box<dyn Error>> {
        let group_settings = sample_message_from_value(json!({
            "message_id": 6,
            "date": 1_710_000_000,
            "chat": {
                "id": -42,
                "type": "group",
                "title": "Group"
            },
            "from": sample_user_json(),
            "text": "/settings@PlotvaBot",
            "entities": [
                {
                    "type": "bot_command",
                    "offset": 0,
                    "length": 19
                }
            ]
        }))?;
        let wrong_bot = sample_message_from_value(json!({
            "message_id": 7,
            "date": 1_710_000_000,
            "chat": {
                "id": -42,
                "type": "group",
                "title": "Group"
            },
            "from": sample_user_json(),
            "text": "/settings@OtherBot",
            "entities": [
                {
                    "type": "bot_command",
                    "offset": 0,
                    "length": 18
                }
            ]
        }))?;
        let embedded_command = sample_message_from_value(json!({
            "message_id": 8,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "text": "see /settings",
            "entities": [
                {
                    "type": "bot_command",
                    "offset": 4,
                    "length": 9
                }
            ]
        }))?;

        assert!(is_settings_command_message(&group_settings, "plotvabot"));
        assert!(!is_settings_command_message(&wrong_bot, "plotvabot"));
        assert!(is_settings_command_message(&group_settings, ""));
        assert!(!is_settings_command_message(&embedded_command, "plotvabot"));
        Ok(())
    }

    #[test]
    fn parse_edit_command_matches_go_edit_verb_semantics() {
        assert_eq!(parse_edit_command("измени фон"), Some("фон".to_owned()));
        assert_eq!(parse_edit_command("поправь: цвет"), Some("цвет".to_owned()));
        assert_eq!(
            parse_edit_command("  FIX   contrast  "),
            Some("contrast".to_owned())
        );
        assert_eq!(parse_edit_command("ИЗМЕНИ"), Some(String::new()));

        assert_eq!(parse_edit_command("нарисуй кота"), None);
        assert_eq!(parse_edit_command("  "), None);
    }

    #[test]
    fn resolve_draw_prompt_from_message_matches_go_reply_context() -> Result<(), Box<dyn Error>> {
        let reply_text = sample_message_from_value(json!({
            "message_id": 9,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "text": "reply text"
        }))?;
        let reply_caption = sample_message_from_value(json!({
            "message_id": 10,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "photo": [
                {
                    "file_id": "photo-file",
                    "file_unique_id": "photo-unique",
                    "width": 10,
                    "height": 10
                }
            ],
            "caption": "caption fallback"
        }))?;
        let text_reply = sample_message_from_value(json!({
            "message_id": 11,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "text": "draw",
            "reply_to_message": reply_text
        }))?;
        let caption_reply = sample_message_from_value(json!({
            "message_id": 12,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "text": "draw",
            "reply_to_message": reply_caption
        }))?;

        assert_eq!(
            resolve_draw_prompt_from_message(&text_reply, "draw", ""),
            Some("reply text".to_owned())
        );
        assert_eq!(
            resolve_draw_prompt_from_message(&text_reply, "нарисуй", "this style"),
            Some("style reply text".to_owned())
        );
        assert_eq!(
            resolve_draw_prompt_from_message(&text_reply, "рисуй", "cat"),
            Some("cat".to_owned())
        );
        assert_eq!(
            resolve_draw_prompt_from_message(&caption_reply, "draw", "this style"),
            Some("style".to_owned())
        );
        assert_eq!(
            resolve_draw_prompt_from_message(&text_reply, "song", "cat"),
            None
        );

        Ok(())
    }

    #[test]
    fn compose_image_prompt_matches_go_context_dedupe() {
        let meta = ChatMessageMeta {
            vision_description: " рыжий ".to_owned(),
            attachments: vec![
                ChatAttachment {
                    content: " рыжий ".to_owned(),
                    caption: "ignored caption".to_owned(),
                    ..ChatAttachment::default()
                },
                ChatAttachment {
                    caption: " на столе ".to_owned(),
                    ..ChatAttachment::default()
                },
                ChatAttachment {
                    content: "кота".to_owned(),
                    ..ChatAttachment::default()
                },
            ],
            ..ChatMessageMeta::default()
        };

        assert_eq!(
            compose_image_prompt(" кота ", &meta),
            "кота\n\nрыжий\n\nна столе"
        );
    }

    #[test]
    fn edited_image_prompt_update_matches_go_pending_image_prompt() -> Result<(), Box<dyn Error>> {
        let message = sample_message_from_value(json!({
            "message_id": 13,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "text": "нарисуй кота"
        }))?;

        let update = edited_image_prompt_update(
            &message,
            "нарисуй",
            "кота",
            &ChatMessageMeta {
                vision_description: "рыжий".to_owned(),
                ..ChatMessageMeta::default()
            },
        )
        .expect("draw command should produce image prompt update");

        assert_eq!(update.original_prompt, "кота");
        assert_eq!(update.prompt, "кота\n\nрыжий");
        assert!(
            edited_image_prompt_update(&message, "song", "кота", &ChatMessageMeta::default())
                .is_none()
        );

        Ok(())
    }

    #[tokio::test]
    async fn update_consumer_runs_state_and_handle_for_fresh_update() -> Result<(), Box<dyn Error>>
    {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let state_calls = calls.clone();
        let handle_calls = calls.clone();
        let update = sample_message_update_with_date(1_710_000_000)?;
        let now = UNIX_EPOCH + Duration::from_secs(1_710_000_030);

        let report = process_update_at(
            update,
            UpdateConsumerConfig::default(),
            now,
            move |_| async move {
                push_call(&state_calls, "state")?;
                Ok::<_, io::Error>(())
            },
            move |_| async move {
                push_call(&handle_calls, "handle")?;
                Ok::<_, io::Error>(())
            },
        )
        .await;

        assert_eq!(
            calls
                .lock()
                .map_err(|err| io::Error::other(err.to_string()))?
                .as_slice(),
            ["state", "handle"]
        );
        assert_eq!(report.update_name, "message");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert!(!report.skipped_handle);

        Ok(())
    }

    #[tokio::test]
    async fn update_consumer_skips_handle_at_go_stale_boundary() -> Result<(), Box<dyn Error>> {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let state_calls = calls.clone();
        let handle_calls = calls.clone();
        let update = sample_message_update_with_date(1_710_000_000)?;
        let now = UNIX_EPOCH + Duration::from_secs(1_710_000_060);

        let report = process_update_at(
            update,
            UpdateConsumerConfig::default(),
            now,
            move |_| async move {
                push_call(&state_calls, "state")?;
                Ok::<_, io::Error>(())
            },
            move |_| async move {
                push_call(&handle_calls, "handle")?;
                Ok::<_, io::Error>(())
            },
        )
        .await;

        assert_eq!(
            calls
                .lock()
                .map_err(|err| io::Error::other(err.to_string()))?
                .as_slice(),
            ["state"]
        );
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert!(report.handle.is_none());
        assert!(report.skipped_handle);

        Ok(())
    }

    #[tokio::test]
    async fn update_consumer_reports_stage_timeouts() -> Result<(), Box<dyn Error>> {
        let update = sample_message_update()?;
        let config = UpdateConsumerConfig {
            handle_timeout: Duration::from_millis(1),
            ..UpdateConsumerConfig::default()
        };

        let report = process_update_at(
            update,
            config,
            UNIX_EPOCH + Duration::from_secs(1_710_000_030),
            |_| async { Ok::<_, io::Error>(()) },
            |_| async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<_, io::Error>(())
            },
        )
        .await;

        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::TimedOut)
        );

        Ok(())
    }

    #[tokio::test]
    async fn live_redis_queue_round_trips_encoded_updates_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };

        let client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:updates:{suffix}");
        let queue = RedisUpdateQueue::with_key(client.clone(), key.clone());
        let mut connection = client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut connection)
            .await?;

        let first = sample_message_update()?;
        let second = serde_json::from_value(json!({
            "update_id": 12346,
            "message": {
                "message_id": 78,
                "date": 1710000001,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "text": "second update"
            }
        }))?;
        queue.enqueue_update(&first).await?;
        queue.enqueue_update(&second).await?;
        assert_eq!(queue.len().await?, 2);
        assert!(!queue.is_empty().await?);

        let dequeued = queue
            .dequeue_encoded(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected queued update"))?;
        assert_eq!(dequeued.decode_update()?.id, 12345);
        assert_eq!(queue.len().await?, 1);

        let dequeued = queue
            .dequeue_encoded(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected queued update"))?;
        assert_eq!(dequeued.decode_update()?.id, 12346);
        assert!(queue.is_empty().await?);

        queue.enqueue_update(&first).await?;
        let dequeued = queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected queued update"))?;
        assert_eq!(dequeued.id, 12345);

        let fresh_date = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let fresh = sample_message_update_with_date(fresh_date)?;
        queue.enqueue_update(&fresh).await?;
        let processed = queue
            .process_next_update(
                UpdateConsumerConfig::default(),
                |_| async { Ok::<_, io::Error>(()) },
                |_| async { Ok::<_, io::Error>(()) },
            )
            .await?
            .ok_or_else(|| io::Error::other("expected processed update"))?;
        assert_eq!(processed.update_id, 12345);
        assert_eq!(processed.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            processed.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );

        let _: i64 = redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut connection)
            .await?;
        Ok(())
    }

    fn push_call(calls: &Mutex<Vec<&'static str>>, name: &'static str) -> Result<(), io::Error> {
        calls
            .lock()
            .map_err(|err| io::Error::other(err.to_string()))?
            .push(name);
        Ok(())
    }

    #[derive(Clone)]
    struct ProducerSourceStub {
        updates: Arc<Mutex<VecDeque<TelegramUpdate>>>,
    }

    impl ProducerSourceStub {
        fn new(updates: Vec<TelegramUpdate>) -> Self {
            Self {
                updates: Arc::new(Mutex::new(updates.into())),
            }
        }
    }

    impl UpdateProducerSource for ProducerSourceStub {
        fn next_update<'a>(&'a self) -> UpdateProducerSourceFuture<'a> {
            Box::pin(async move {
                self.updates
                    .lock()
                    .expect("producer source updates")
                    .pop_front()
            })
        }
    }

    #[derive(Clone, Default)]
    struct ProducerQueueStub {
        enqueued: Arc<Mutex<Vec<i64>>>,
        failures: Arc<Mutex<VecDeque<&'static str>>>,
    }

    impl ProducerQueueStub {
        fn with_failures(self, failures: Vec<&'static str>) -> Self {
            *self.failures.lock().expect("producer queue failures") = failures.into();
            self
        }

        fn enqueued_ids(&self) -> Vec<i64> {
            self.enqueued.lock().expect("producer queue ids").clone()
        }
    }

    impl UpdateProducerQueue for ProducerQueueStub {
        type Error = io::Error;

        fn enqueue_update<'a>(
            &'a self,
            update: &'a TelegramUpdate,
        ) -> UpdateProducerQueueFuture<'a, Self::Error> {
            Box::pin(async move {
                if let Some(message) = self
                    .failures
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .pop_front()
                {
                    return Err(io::Error::other(message));
                }

                self.enqueued
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(update.id);
                Ok(())
            })
        }
    }

    fn sample_message_update() -> Result<TelegramUpdate, serde_json::Error> {
        sample_message_update_with_date(1_710_000_000)
    }

    fn sample_message_update_with_id(update_id: i64) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": update_id,
            "message": sample_message_json(77, 1_710_000_000, "/start hello")
        }))
    }

    fn sample_callback_update_with_id(update_id: i64) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": update_id,
            "callback_query": {
                "id": "callback-id",
                "from": sample_user_json(),
                "message": sample_message_json(8, 1_710_000_000, "callback")
            }
        }))
    }

    fn sample_poll_update_with_id(update_id: i64) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": update_id,
            "poll": {
                "type": "regular",
                "allows_multiple_answers": false,
                "allows_revoting": false,
                "id": "poll-id",
                "is_anonymous": true,
                "is_closed": true,
                "members_only": false,
                "options": [
                    {
                        "persistent_id": "1",
                        "text": "Yes",
                        "voter_count": 1000
                    },
                    {
                        "persistent_id": "2",
                        "text": "No",
                        "voter_count": 0
                    }
                ],
                "question": "Rust?",
                "total_voter_count": 1000
            }
        }))
    }

    fn sample_message_update_with_date(date: i64) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12345,
            "message": sample_message_json(77, date, "/start hello")
        }))
    }

    fn sample_update_json(value: serde_json::Value) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(value)
    }

    fn sample_message_json(message_id: i64, date: i64, text: &str) -> serde_json::Value {
        json!({
            "message_id": message_id,
            "date": date,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "text": text
        })
    }

    fn sample_message_from_value(
        value: serde_json::Value,
    ) -> Result<TelegramMessage, serde_json::Error> {
        serde_json::from_value(value)
    }

    fn sample_bot_user() -> carapax::types::User {
        serde_json::from_value(json!({
            "id": 777,
            "is_bot": true,
            "first_name": "Плотва",
            "username": "PlotvaBot"
        }))
        .expect("sample bot user")
    }

    fn sample_private_chat_json() -> serde_json::Value {
        json!({
            "id": 42,
            "type": "private",
            "first_name": "Ada",
            "username": "ada_l"
        })
    }

    fn update_message(update: &TelegramUpdate) -> Result<&TelegramMessage, io::Error> {
        match &update.update_type {
            TelegramUpdateType::Message(message) => Ok(message),
            other => Err(io::Error::other(format!(
                "expected message update, got {other:?}"
            ))),
        }
    }

    fn assert_float_eq(got: Option<f64>, want: f64) {
        let Some(got) = got else {
            panic!("missing float value, want {want}");
        };
        assert!(
            (got - want).abs() < 0.00001,
            "float value {got} differs from {want}"
        );
    }

    fn sample_channel_message_json(message_id: i64, date: i64, text: &str) -> serde_json::Value {
        json!({
            "message_id": message_id,
            "date": date,
            "chat": {
                "id": -100,
                "type": "channel",
                "title": "Channel",
                "username": "channel_name"
            },
            "sender_chat": {
                "id": -100,
                "type": "channel",
                "title": "Channel",
                "username": "channel_name"
            },
            "text": text
        })
    }

    fn sample_chat_member_updated_json(is_bot_member: bool) -> serde_json::Value {
        json!({
            "chat": {
                "type": "group",
                "id": 1,
                "title": "Group"
            },
            "date": 0,
            "from": sample_user_json(),
            "new_chat_member": {
                "status": "kicked",
                "until_date": 0,
                "user": {
                    "first_name": "Bot",
                    "id": 2,
                    "is_bot": is_bot_member
                }
            },
            "old_chat_member": {
                "status": "member",
                "user": {
                    "first_name": "Bot",
                    "id": 2,
                    "is_bot": is_bot_member
                }
            }
        })
    }

    fn sample_user_json() -> serde_json::Value {
        json!({
            "id": 99,
            "is_bot": false,
            "first_name": "Ada",
            "username": "ada_l"
        })
    }
}
