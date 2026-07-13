//! Telegram update ingestion, classification, and replay.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use carapax::types::{
    Chat as TelegramChat, MaybeInaccessibleMessage, Message as TelegramMessage,
    MessageData as TelegramMessageData, PollAnswerVoter, ReplyTo as TelegramReplyTo,
    Text as TelegramText, TextEntity as TelegramTextEntity,
    TextEntityPosition as TelegramTextEntityPosition, Update as TelegramUpdate,
    UpdateType as TelegramUpdateType, User as TelegramUser,
};
use openplotva_core::{
    ChatAttachment, ChatMessageMeta, ChatState, MessageSender, SENDER_TYPE_CHANNEL,
    SENDER_TYPE_SAME_CHAT, SENDER_TYPE_SYSTEM, SENDER_TYPE_USER, UpdateState, UserState,
};
use redis::{
    AsyncConnectionConfig, Client as RedisClient,
    aio::{ConnectionManager, ConnectionManagerConfig, MultiplexedConnection},
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::OnceCell;

mod redis_stream;

pub use redis_stream::*;

/// Human-readable crate purpose used by scaffold tests and docs.
pub const PURPOSE: &str = "updates";

pub const DEFAULT_UPDATE_QUEUE_KEY: &str = "plotva:updates:queue";

/// Rust-native update payload format stored inside the Redis update queue.
pub const NATIVE_UPDATE_CODEC: &str = "openplotva.update.v1+carapax-json.zstd";

pub const NATIVE_UPDATE_FORMAT_VERSION: u16 = 1;

pub const DEFAULT_UPDATE_DEQUEUE_TIMEOUT: Duration = Duration::from_secs(5);

pub const TELEGRAM_FILE_MEDIA_KIND_PHOTO: &str = "photo";
pub const TELEGRAM_FILE_MEDIA_KIND_DOCUMENT: &str = "document";
pub const TELEGRAM_FILE_MEDIA_KIND_AUDIO: &str = "audio";
pub const TELEGRAM_FILE_MEDIA_KIND_VOICE: &str = "voice";
pub const TELEGRAM_FILE_MEDIA_KIND_STICKER: &str = "sticker";
pub const TELEGRAM_FILE_MEDIA_KIND_VIDEO: &str = "video";
pub const TELEGRAM_FILE_MEDIA_KIND_ANIMATION: &str = "animation";
pub const TELEGRAM_FILE_MEDIA_KIND_VIDEO_NOTE: &str = "video_note";

pub const UPDATE_STATE_TIMEOUT: Duration = Duration::from_secs(10);

pub const UPDATE_HANDLE_TIMEOUT: Duration = Duration::from_secs(45);

// Go parity: the consumer skips side effects for updates older than this. go-plotva
// calls shouldSkipSideEffects(update, time.Minute) (internal/processor/consumer.go), so the
// boundary is 60s — not 5 minutes. A wider window would re-handle stale backlog updates.
pub const UPDATE_SIDE_EFFECT_MAX_AGE: Duration = Duration::from_secs(60);

pub const UPDATE_STALL_AGE: Duration = Duration::from_secs(120);

pub const UPDATE_WORKER_LIMIT_PER_CPU: usize = 4;

pub const GUEST_CHAIN_MAX_MESSAGES: usize = 15;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TelegramFileMetadataRef {
    /// Telegram downloadable file ID.
    pub file_id: String,
    /// Telegram stable unique file ID.
    pub file_unique_id: String,
    pub media_kind: String,
    pub mime_type: String,
    /// Image/sticker width.
    pub width: i32,
    /// Image/sticker height.
    pub height: i32,
    /// File size in bytes.
    pub file_size: i64,
    /// Source chat ID.
    pub chat_id: i64,
    /// Source message ID.
    pub message_id: i64,
    /// Forum topic/thread ID, or zero when absent.
    pub thread_id: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GoUpdateType {
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
}

/// Serialized and compressed Telegram update for Redis queue storage.
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

        Ok(decode_telegram_update_value(envelope.update)?)
    }
}

#[derive(Debug, Deserialize)]
struct NativeUpdateEnvelope {
    version: u16,
    codec: String,
    update: serde_json::Value,
}

/// Decode one Telegram update JSON payload with compatibility fixes for
/// Bot API fields not represented by the current `carapax` wire shape.
pub fn decode_telegram_update_json_slice(
    bytes: &[u8],
) -> Result<TelegramUpdate, serde_json::Error> {
    decode_telegram_update_value(serde_json::from_slice(bytes)?)
}

/// Decode one Telegram update JSON value with compatibility fixes for
/// Bot API fields not represented by the current `carapax` wire shape.
pub fn decode_telegram_update_value(
    mut value: serde_json::Value,
) -> Result<TelegramUpdate, serde_json::Error> {
    normalize_poll_answer_voter_chat(&mut value);
    serde_json::from_value(value)
}

fn normalize_poll_answer_voter_chat(value: &mut serde_json::Value) {
    let Some(update) = value.as_object_mut() else {
        return;
    };
    let Some(poll_answer) = update
        .get_mut("poll_answer")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };
    if poll_answer.contains_key("chat") {
        return;
    }
    if let Some(voter_chat) = poll_answer.get("voter_chat").cloned() {
        poll_answer.insert("chat".to_owned(), voter_chat);
    }
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
    connections: RedisUpdateConnections,
    key: String,
}

#[derive(Clone, Debug)]
struct RedisUpdateConnections {
    client: RedisClient,
    commands: Arc<OnceCell<ConnectionManager>>,
}

/// Margin added to the client-side response timeout of a blocking read so the
/// server-side `BLPOP` timeout always fires first.
const BLOCKING_RESPONSE_TIMEOUT_GRACE: Duration = Duration::from_secs(2);
const COMMAND_RESPONSE_TIMEOUT: Duration = Duration::from_secs(3);
const REDIS_UPDATE_COMMAND_CLIENT_NAME: &str = "openplotva:updates:commands";
const REDIS_UPDATE_BLOCKING_CLIENT_NAME: &str = "openplotva:updates:blocking";
/// Cap on retained enqueue-error strings per producer run so a sustained queue
/// outage cannot grow the report without bound.
const MAX_ENQUEUE_ERRORS: usize = 64;

fn command_connection_config() -> ConnectionManagerConfig {
    ConnectionManagerConfig::new().set_response_timeout(Some(COMMAND_RESPONSE_TIMEOUT))
}

async fn set_redis_client_name(
    connection: &mut impl redis::aio::ConnectionLike,
    name: &str,
) -> redis::RedisResult<()> {
    let _: String = redis::cmd("CLIENT")
        .arg("SETNAME")
        .arg(name)
        .query_async(connection)
        .await?;
    Ok(())
}

fn blocking_response_timeout(timeout: Duration) -> Option<Duration> {
    (!timeout.is_zero()).then(|| timeout + BLOCKING_RESPONSE_TIMEOUT_GRACE)
}

impl RedisUpdateConnections {
    fn new(client: RedisClient) -> Self {
        Self {
            client,
            commands: Arc::new(OnceCell::new()),
        }
    }

    async fn command_connection(&self) -> redis::RedisResult<ConnectionManager> {
        self.commands
            .get_or_try_init(|| async {
                let mut connection = self
                    .client
                    .get_connection_manager_with_config(command_connection_config())
                    .await?;
                set_redis_client_name(&mut connection, REDIS_UPDATE_COMMAND_CLIENT_NAME).await?;
                Ok(connection)
            })
            .await
            .cloned()
    }

    /// Blocking reads use a dedicated connection per call: dropping it on
    /// caller cancellation or timeout closes the socket, which cancels the
    /// server-side `BLPOP` or `XREADGROUP`. A shared multiplexed connection
    /// must never carry blocking commands: an abandoned read keeps blocking
    /// server-side and can deliver an update to a dropped receiver. The
    /// client-side response timeout stays above the Redis block timeout so
    /// the server always answers first.
    async fn blocking_connection(
        &self,
        timeout: Duration,
    ) -> redis::RedisResult<MultiplexedConnection> {
        let config =
            AsyncConnectionConfig::new().set_response_timeout(blocking_response_timeout(timeout));
        let mut connection = self
            .client
            .get_multiplexed_async_connection_with_config(&config)
            .await?;
        set_redis_client_name(&mut connection, REDIS_UPDATE_BLOCKING_CLIENT_NAME).await?;
        Ok(connection)
    }

    #[cfg(test)]
    fn shares_command_manager_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.commands, &other.commands)
    }
}

impl RedisUpdateQueue {
    pub fn new(client: RedisClient) -> Self {
        Self::with_key(client, DEFAULT_UPDATE_QUEUE_KEY)
    }

    /// Create a queue using an explicit Redis key, useful for isolated tests.
    pub fn with_key(client: RedisClient, key: impl Into<String>) -> Self {
        Self {
            connections: RedisUpdateConnections::new(client),
            key: key.into(),
        }
    }

    /// Return the Redis key this queue reads and writes.
    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn enqueue_encoded(&self, update: &EncodedUpdate) -> Result<(), UpdateQueueError> {
        let mut connection = self.connections.command_connection().await?;
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

    /// Dequeue one encoded update with Redis `BLPOP` semantics.
    pub async fn dequeue_encoded(
        &self,
        timeout: Duration,
    ) -> Result<Option<EncodedUpdate>, UpdateQueueError> {
        let mut connection = self.connections.blocking_connection(timeout).await?;
        let result: Option<(String, Vec<u8>)> = redis::cmd("BLPOP")
            .arg(&self.key)
            .arg(blpop_timeout_arg(timeout))
            .query_async(&mut connection)
            .await?;
        Ok(result.map(|(_, value)| EncodedUpdate::from_queue_value(value)))
    }

    pub async fn dequeue_encoded_batch(
        &self,
        timeout: Duration,
        max_count: usize,
    ) -> Result<Vec<EncodedUpdate>, UpdateQueueError> {
        if max_count == 0 {
            return Ok(Vec::new());
        }
        let Some(first) = self.dequeue_encoded(timeout).await? else {
            return Ok(Vec::new());
        };
        let mut updates = Vec::with_capacity(max_count);
        updates.push(first);
        let remaining = max_count.saturating_sub(1);
        if remaining == 0 {
            return Ok(updates);
        }

        let mut connection = self.connections.command_connection().await?;
        let values: Option<Vec<Vec<u8>>> = redis::cmd("LPOP")
            .arg(&self.key)
            .arg(remaining)
            .query_async(&mut connection)
            .await?;
        if let Some(values) = values {
            updates.extend(values.into_iter().map(EncodedUpdate::from_queue_value));
        }
        Ok(updates)
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

    pub async fn dequeue_updates(
        &self,
        timeout: Duration,
        max_count: usize,
    ) -> Result<Vec<TelegramUpdate>, UpdateQueueError> {
        let updates = self.dequeue_encoded_batch(timeout, max_count).await?;
        updates
            .into_iter()
            .map(|update| update.decode_update().map_err(UpdateQueueError::from))
            .collect()
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

    pub async fn len(&self) -> Result<i64, UpdateQueueError> {
        let mut connection = self.connections.command_connection().await?;
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

/// Queue sink used by the update producer.
pub trait UpdateProducerQueue {
    /// Error returned by the concrete queue.
    type Error: fmt::Display;

    /// Enqueue one Telegram update without ingress policy filtering.
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UpdateProducerRunReport {
    /// Updates received from the source.
    pub received: usize,
    pub enqueued: usize,
    pub enqueue_errors: Vec<String>,
    /// Count of enqueue errors that occurred beyond `MAX_ENQUEUE_ERRORS` and were not
    /// retained in `enqueue_errors`.
    pub dropped_enqueue_errors: usize,
    /// Whether the source closed before shutdown was requested.
    pub source_closed: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TelegramMessageAttachmentOptions {
    pub source: String,
    /// Caption supplied by the caller, trimmed before use.
    pub caption: String,
    pub promote_first_image_ref: bool,
}

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
                let queued = queue.enqueue_update(&update);
                tokio::pin!(queued);
                tokio::select! {
                    biased;

                    _ = &mut stop => break,
                    result = &mut queued => match result {
                        Ok(()) => report.enqueued += 1,
                        Err(error) => {
                            let error = error.to_string();
                            tracing::warn!(%error, "failed to enqueue Telegram update");
                            if report.enqueue_errors.len() < MAX_ENQUEUE_ERRORS {
                                report.enqueue_errors.push(error);
                            } else {
                                report.dropped_enqueue_errors += 1;
                            }
                        }
                    },
                }
            }
        }
    }

    report
}

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

/// Observer for live update consumer stage diagnostics.
pub trait UpdateStageTracker {
    /// Register one stage start and return a token passed back on finish.
    fn stage_started(
        &self,
        update: &TelegramUpdate,
        stage: UpdateStage,
        started_at: SystemTime,
    ) -> u64;

    /// Register one stage finish.
    fn stage_finished(&self, token: u64, report: &UpdateStageReport, finished_at: SystemTime);
}

/// No-op update stage tracker.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopUpdateStageTracker;

impl UpdateStageTracker for NoopUpdateStageTracker {
    fn stage_started(
        &self,
        _update: &TelegramUpdate,
        _stage: UpdateStage,
        _started_at: SystemTime,
    ) -> u64 {
        0
    }

    fn stage_finished(&self, _token: u64, _report: &UpdateStageReport, _finished_at: SystemTime) {}
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
    process_update_with_stage_tracker_at(
        update,
        config,
        now,
        state,
        handle,
        &NoopUpdateStageTracker,
    )
    .await
}

/// Process one update with live stage tracking hooks.
pub async fn process_update_with_stage_tracker_at<
    StateFn,
    StateFuture,
    StateError,
    HandleFn,
    HandleFuture,
    HandleError,
    Tracker,
>(
    update: TelegramUpdate,
    config: UpdateConsumerConfig,
    now: SystemTime,
    state: StateFn,
    handle: HandleFn,
    tracker: &Tracker,
) -> UpdateProcessReport
where
    StateFn: FnOnce(TelegramUpdate) -> StateFuture,
    StateFuture: Future<Output = Result<(), StateError>>,
    StateError: fmt::Display,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
    Tracker: UpdateStageTracker + ?Sized,
{
    let update_id = update.id;
    let name = update_name(&update);
    let skip_handle = should_skip_side_effects_at(&update, config.side_effect_max_age, now)
        && !stale_update_requires_handle(&update);
    let state_task = run_tracked_stage(
        &update,
        UpdateStage::State,
        config.state_timeout,
        state(update.clone()),
        tracker,
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

    let handle_update = update.clone();
    let handle_task = run_tracked_stage(
        &update,
        UpdateStage::Handle,
        config.handle_timeout,
        handle(handle_update),
        tracker,
    );
    let (state, handle) = tokio::join!(state_task, handle_task);

    UpdateProcessReport {
        update_id,
        update_name: name,
        state,
        handle: Some(handle),
        skipped_handle: false,
    }
}

pub fn update_name(update: &TelegramUpdate) -> &'static str {
    match &update.update_type {
        TelegramUpdateType::Message(_) => GoUpdateType::Message.as_str(),
        TelegramUpdateType::EditedMessage(_) => GoUpdateType::EditedMessage.as_str(),
        TelegramUpdateType::GuestMessage(_) => GoUpdateType::GuestMessage.as_str(),
        TelegramUpdateType::ChannelPost(_) => GoUpdateType::ChannelPost.as_str(),
        TelegramUpdateType::EditedChannelPost(_) => GoUpdateType::EditedChannelPost.as_str(),
        TelegramUpdateType::InlineQuery(_) => GoUpdateType::InlineQuery.as_str(),
        TelegramUpdateType::ChosenInlineResult(_) => GoUpdateType::ChosenInlineResult.as_str(),
        TelegramUpdateType::CallbackQuery(_) => GoUpdateType::CallbackQuery.as_str(),
        TelegramUpdateType::ShippingQuery(_) => GoUpdateType::ShippingQuery.as_str(),
        TelegramUpdateType::PreCheckoutQuery(_) => GoUpdateType::PreCheckoutQuery.as_str(),
        TelegramUpdateType::Poll(_) => GoUpdateType::Poll.as_str(),
        TelegramUpdateType::PollAnswer(_) => GoUpdateType::PollAnswer.as_str(),
        TelegramUpdateType::BotStatus(_) => GoUpdateType::MyChatMember.as_str(),
        TelegramUpdateType::UserStatus(_) => GoUpdateType::ChatMember.as_str(),
        TelegramUpdateType::ChatJoinRequest(_) => GoUpdateType::ChatJoinRequest.as_str(),
        TelegramUpdateType::MessageReaction(_) => GoUpdateType::MessageReaction.as_str(),
        TelegramUpdateType::MessageReactionCount(_) => GoUpdateType::MessageReactionCount.as_str(),
        _ => "unknown",
    }
}

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

#[must_use]
pub fn producer_update_name(update: &TelegramUpdate) -> &'static str {
    producer_update_type(update).as_str()
}

#[must_use]
pub fn is_payment_update(update: &TelegramUpdate) -> bool {
    match &update.update_type {
        TelegramUpdateType::ShippingQuery(_)
        | TelegramUpdateType::PreCheckoutQuery(_)
        | TelegramUpdateType::PurchasedPaidMedia(_) => true,
        TelegramUpdateType::Message(message)
        | TelegramUpdateType::EditedMessage(message)
        | TelegramUpdateType::GuestMessage(message)
        | TelegramUpdateType::ChannelPost(message)
        | TelegramUpdateType::EditedChannelPost(message)
        | TelegramUpdateType::BusinessMessage(message)
        | TelegramUpdateType::EditedBusinessMessage(message) => {
            matches!(
                message.data,
                TelegramMessageData::DirectMessagePriceChanged(_)
                    | TelegramMessageData::Invoice(_)
                    | TelegramMessageData::PaidMedia(_)
                    | TelegramMessageData::PaidMessagePriceChanged(_)
                    | TelegramMessageData::RefundedPayment(_)
                    | TelegramMessageData::SuggestedPostPaid(_)
                    | TelegramMessageData::SuggestedPostRefunded(_)
                    | TelegramMessageData::SuccessfulPayment(_)
            )
        }
        _ => false,
    }
}

#[must_use]
pub fn is_passive_update(update: &TelegramUpdate) -> bool {
    !is_payment_update(update)
        && matches!(
            &update.update_type,
            TelegramUpdateType::ChannelPost(_)
                | TelegramUpdateType::EditedChannelPost(_)
                | TelegramUpdateType::ChosenInlineResult(_)
                | TelegramUpdateType::Poll(_)
                | TelegramUpdateType::PollAnswer(_)
                | TelegramUpdateType::MessageReaction(_)
                | TelegramUpdateType::MessageReactionCount(_)
                | TelegramUpdateType::BusinessConnection(_)
                | TelegramUpdateType::BusinessMessage(_)
                | TelegramUpdateType::ChatBoostRemoved(_)
                | TelegramUpdateType::ChatBoostUpdated(_)
                | TelegramUpdateType::DeletedBusinessMessages(_)
                | TelegramUpdateType::EditedBusinessMessage(_)
                | TelegramUpdateType::ManagedBot(_)
                | TelegramUpdateType::Unknown(_)
        )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UpdateIngressGuardConfig {
    pub short_window: Duration,
    pub short_limit: usize,
    pub long_window: Duration,
    pub long_limit: usize,
    pub block_duration: Duration,
}

impl Default for UpdateIngressGuardConfig {
    fn default() -> Self {
        Self {
            short_window: Duration::from_secs(10),
            short_limit: 200,
            long_window: Duration::from_secs(60),
            long_limit: 600,
            block_duration: Duration::from_secs(5 * 60),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateIngressDecision {
    Allowed {
        chat_id: Option<i64>,
        payment: bool,
    },
    DroppedBlocked {
        chat_id: i64,
        blocked_until: SystemTime,
    },
    DroppedFlood {
        chat_id: i64,
        blocked_until: SystemTime,
    },
}

impl UpdateIngressDecision {
    #[must_use]
    pub const fn is_allowed(self) -> bool {
        matches!(self, Self::Allowed { .. })
    }

    #[must_use]
    pub const fn is_dropped(self) -> bool {
        !self.is_allowed()
    }

    #[must_use]
    pub const fn chat_id(self) -> Option<i64> {
        match self {
            Self::Allowed { chat_id, .. } => chat_id,
            Self::DroppedBlocked { chat_id, .. } | Self::DroppedFlood { chat_id, .. } => {
                Some(chat_id)
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct UpdateIngressGuard {
    config: UpdateIngressGuardConfig,
    chats: Mutex<HashMap<i64, ChatIngressState>>,
}

#[derive(Debug, Default)]
struct ChatIngressState {
    samples: VecDeque<SystemTime>,
    blocked_until: Option<SystemTime>,
}

impl UpdateIngressGuard {
    #[must_use]
    pub fn new(config: UpdateIngressGuardConfig) -> Self {
        Self {
            config,
            chats: Mutex::new(HashMap::new()),
        }
    }

    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(UpdateIngressGuardConfig::default())
    }

    #[must_use]
    pub fn check_update(&self, update: &TelegramUpdate) -> UpdateIngressDecision {
        self.check_update_at(update, SystemTime::now())
    }

    #[must_use]
    pub fn check_update_at(
        &self,
        update: &TelegramUpdate,
        now: SystemTime,
    ) -> UpdateIngressDecision {
        if is_payment_update(update) {
            return UpdateIngressDecision::Allowed {
                chat_id: update_chat_id(update),
                payment: true,
            };
        }
        let Some(chat_id) = flood_guard_chat_id(update).filter(|chat_id| *chat_id != 0) else {
            return UpdateIngressDecision::Allowed {
                chat_id: None,
                payment: false,
            };
        };
        let mut chats = self
            .chats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if chats.len() >= 4_096 && chats.len().is_power_of_two() && !chats.contains_key(&chat_id) {
            chats.retain(|_, state| {
                state
                    .blocked_until
                    .is_some_and(|blocked_until| blocked_until > now)
                    || state.samples.back().is_some_and(|sample| {
                        sample
                            .checked_add(self.config.long_window)
                            .is_some_and(|expiry| expiry > now)
                    })
            });
        }
        let state = chats.entry(chat_id).or_default();
        if let Some(blocked_until) = state.blocked_until {
            if now < blocked_until {
                return UpdateIngressDecision::DroppedBlocked {
                    chat_id,
                    blocked_until,
                };
            }
            state.blocked_until = None;
        }

        retain_recent_samples(&mut state.samples, now, self.config.long_window);
        let short_count = recent_sample_count(&state.samples, now, self.config.short_window) + 1;
        let long_count = state.samples.len() + 1;
        if short_count >= self.config.short_limit || long_count >= self.config.long_limit {
            let blocked_until = now + self.config.block_duration;
            state.blocked_until = Some(blocked_until);
            state.samples.clear();
            return UpdateIngressDecision::DroppedFlood {
                chat_id,
                blocked_until,
            };
        }

        state.samples.push_back(now);
        UpdateIngressDecision::Allowed {
            chat_id: Some(chat_id),
            payment: false,
        }
    }
}

fn flood_guard_chat_id(update: &TelegramUpdate) -> Option<i64> {
    let guarded = matches!(
        &update.update_type,
        TelegramUpdateType::Message(_)
            | TelegramUpdateType::EditedMessage(_)
            | TelegramUpdateType::GuestMessage(_)
            | TelegramUpdateType::BusinessMessage(_)
            | TelegramUpdateType::EditedBusinessMessage(_)
            | TelegramUpdateType::DeletedBusinessMessages(_)
            | TelegramUpdateType::CallbackQuery(_)
            | TelegramUpdateType::BotStatus(_)
            | TelegramUpdateType::UserStatus(_)
            | TelegramUpdateType::ChatJoinRequest(_)
            | TelegramUpdateType::ChatBoostRemoved(_)
            | TelegramUpdateType::ChatBoostUpdated(_)
            | TelegramUpdateType::MessageReaction(_)
            | TelegramUpdateType::MessageReactionCount(_)
            | TelegramUpdateType::PollAnswer(_)
    );
    if !guarded || update.get_user().is_some_and(|user| user.is_bot) {
        return None;
    }
    update_chat_id(update)
}

fn retain_recent_samples(samples: &mut VecDeque<SystemTime>, now: SystemTime, window: Duration) {
    while samples.front().is_some_and(|sample| {
        sample
            .checked_add(window)
            .is_some_and(|expiry| expiry <= now)
    }) {
        samples.pop_front();
    }
}

fn recent_sample_count(samples: &VecDeque<SystemTime>, now: SystemTime, window: Duration) -> usize {
    samples
        .iter()
        .rev()
        .take_while(|sample| {
            sample
                .checked_add(window)
                .is_some_and(|expiry| expiry > now)
        })
        .count()
}

pub fn extract_update_state(update: &TelegramUpdate) -> Option<UpdateState> {
    if matches!(update.update_type, TelegramUpdateType::GuestMessage(_)) {
        return None;
    }

    let chat = extract_update_chat(update).map(chat_state);
    let user = update.get_user().map(user_state);
    UpdateState::new(chat, user)
}

/// Collect Telegram file metadata references attached to an update.
/// that can use media references for vision/audio context.
#[must_use]
pub fn update_file_metadata_refs(update: &TelegramUpdate) -> Vec<TelegramFileMetadataRef> {
    match &update.update_type {
        TelegramUpdateType::Message(message) | TelegramUpdateType::EditedMessage(message) => {
            telegram_message_and_reply_file_metadata_refs(message)
        }
        _ => Vec::new(),
    }
}

#[must_use]
pub fn telegram_message_and_reply_file_metadata_refs(
    message: &TelegramMessage,
) -> Vec<TelegramFileMetadataRef> {
    let mut refs = telegram_message_file_metadata_refs(message);
    if let Some(reply) = reply_message(message) {
        refs.extend(telegram_message_file_metadata_refs(reply));
    }
    refs
}

#[must_use]
pub fn telegram_message_file_metadata_refs(
    message: &TelegramMessage,
) -> Vec<TelegramFileMetadataRef> {
    let Some(base) = telegram_file_metadata_base(message) else {
        return Vec::new();
    };

    let mut refs = Vec::new();
    if let TelegramMessageData::Photo(photo) = &message.data
        && let Some(ref_data) = photo.data.last()
    {
        refs.push(
            base.clone()
                .with_file(
                    &ref_data.file_id,
                    &ref_data.file_unique_id,
                    TELEGRAM_FILE_MEDIA_KIND_PHOTO,
                    "",
                    ref_data.file_size.unwrap_or_default(),
                )
                .with_dimensions(ref_data.width, ref_data.height),
        );
    }
    if let Some(ref_data) = document_file_metadata_ref(&base, &message.data) {
        refs.push(ref_data);
    }
    if let TelegramMessageData::Audio(audio) = &message.data {
        refs.push(base.clone().with_file(
            &audio.data.file_id,
            &audio.data.file_unique_id,
            TELEGRAM_FILE_MEDIA_KIND_AUDIO,
            audio.data.mime_type.as_deref().unwrap_or_default(),
            audio.data.file_size.unwrap_or_default(),
        ));
    }
    if let TelegramMessageData::Voice(voice) = &message.data {
        refs.push(base.clone().with_file(
            &voice.data.file_id,
            &voice.data.file_unique_id,
            TELEGRAM_FILE_MEDIA_KIND_VOICE,
            "audio/ogg",
            voice.data.file_size.unwrap_or_default(),
        ));
    }
    if let TelegramMessageData::Video(video) = &message.data {
        refs.push(
            base.clone()
                .with_file(
                    &video.data.file_id,
                    &video.data.file_unique_id,
                    TELEGRAM_FILE_MEDIA_KIND_VIDEO,
                    video.data.mime_type.as_deref().unwrap_or("video/mp4"),
                    video.data.file_size.unwrap_or_default(),
                )
                .with_dimensions(video.data.width, video.data.height),
        );
    }
    if let TelegramMessageData::Animation(animation) = &message.data {
        refs.push(
            base.clone()
                .with_file(
                    &animation.file_id,
                    &animation.file_unique_id,
                    TELEGRAM_FILE_MEDIA_KIND_ANIMATION,
                    animation.mime_type.as_deref().unwrap_or("video/mp4"),
                    animation.file_size.unwrap_or_default(),
                )
                .with_dimensions(animation.width, animation.height),
        );
    }
    if let TelegramMessageData::VideoNote(video_note) = &message.data {
        refs.push(
            base.clone()
                .with_file(
                    &video_note.file_id,
                    &video_note.file_unique_id,
                    TELEGRAM_FILE_MEDIA_KIND_VIDEO_NOTE,
                    "video/mp4",
                    video_note.file_size.unwrap_or_default(),
                )
                .with_dimensions(video_note.length, video_note.length),
        );
    }
    if let Some(ref_data) = sticker_file_metadata_ref(&base, &message.data) {
        refs.push(ref_data);
    }
    refs
}

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
    append_telegram_payload_attachments(&mut out, &message.data, &source);

    out
}

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

fn collect_dialog_attachments(message: &TelegramMessage) -> Vec<ChatAttachment> {
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(2);

    if let Some(reply) = reply_message(message) {
        add_dialog_attachment(
            &mut out,
            &mut seen,
            "quoted",
            "image",
            first_dialog_image_unique_id(reply),
        );
        add_dialog_attachment(
            &mut out,
            &mut seen,
            "quoted",
            "audio",
            first_dialog_audio_unique_id(reply),
        );
    }

    out
}

fn add_dialog_attachment(
    out: &mut Vec<ChatAttachment>,
    seen: &mut HashSet<String>,
    source: &str,
    kind: &str,
    file_unique_id: Option<String>,
) {
    let Some(file_unique_id) = file_unique_id else {
        return;
    };
    let file_unique_id = file_unique_id.trim();
    if file_unique_id.is_empty() || !seen.insert(file_unique_id.to_owned()) {
        return;
    }
    out.push(ChatAttachment {
        kind: kind.to_owned(),
        source: source.to_owned(),
        file_unique_id: file_unique_id.to_owned(),
        ..ChatAttachment::default()
    });
}

fn first_dialog_image_unique_id(message: &TelegramMessage) -> Option<String> {
    match &message.data {
        TelegramMessageData::Photo(photo) => {
            photo.data.last().map(|photo| photo.file_unique_id.clone())
        }
        TelegramMessageData::Document(document)
            if has_mime_prefix(
                document.data.mime_type.as_deref().unwrap_or_default(),
                "image/",
            ) =>
        {
            Some(document.data.file_unique_id.clone())
        }
        TelegramMessageData::Sticker(sticker) if !sticker.is_animated && !sticker.is_video => {
            Some(sticker.file_unique_id.clone())
        }
        _ => None,
    }
}

fn first_dialog_audio_unique_id(message: &TelegramMessage) -> Option<String> {
    match &message.data {
        TelegramMessageData::Audio(audio) => Some(audio.data.file_unique_id.clone()),
        TelegramMessageData::Voice(voice) => Some(voice.data.file_unique_id.clone()),
        TelegramMessageData::Document(document)
            if has_mime_prefix(
                document.data.mime_type.as_deref().unwrap_or_default(),
                "audio/",
            ) =>
        {
            Some(document.data.file_unique_id.clone())
        }
        _ => None,
    }
}

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

#[derive(Clone, Debug, PartialEq)]
pub struct FetcherMessageContext {
    pub original_text: String,
    pub text: String,
    pub meta: ChatMessageMeta,
}

#[must_use]
pub fn build_fetcher_message_context(message: &TelegramMessage) -> FetcherMessageContext {
    let sender = resolve_message_sender(Some(message));
    let attachments = collect_dialog_attachments(message);
    FetcherMessageContext {
        original_text: message_text_before_fetcher_fallback(message),
        text: fetcher_message_text(message),
        meta: build_message_meta(Some(message), sender, &attachments, ""),
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ParsedAddressedMessage {
    /// Text used as the full message text for downstream handling.
    pub message_text: String,
    pub first_word: String,
    /// Remaining parsed text after the first word.
    pub rest_text: String,
    /// Whether the message is addressed to the bot.
    pub is_addressed: bool,
}

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

#[must_use]
pub fn parse_edit_command(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }

    let (first_word, rest_text) = cut_first_word(trimmed);
    is_edit_verb(&first_word).then(|| rest_text.trim().to_owned())
}

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EditedImagePromptUpdate {
    /// Prompt with message metadata context composed for image generation.
    pub prompt: String,
    /// Trimmed prompt before metadata context is appended.
    pub original_prompt: String,
}

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

#[must_use]
pub fn should_handle_addressed_message(
    message: Option<&TelegramMessage>,
    sender: &MessageSender,
    bot_id: i64,
) -> bool {
    if !sender.is_bot {
        return true;
    }

    let Some(message) = message else {
        return false;
    };
    if message.id == 0 || bot_id == 0 {
        return false;
    }

    let Some(TelegramReplyTo::Message(reply)) = message.reply_to.as_ref() else {
        return false;
    };
    let Some(reply_user) = reply.sender.get_user() else {
        return false;
    };
    if reply_user.id != bot_id {
        return false;
    }

    addressed_bot_response_bucket(message.chat.get_id().into(), message.id) == 0
}

#[must_use]
pub fn should_handle_random_response(
    _message: Option<&TelegramMessage>,
    _original_text: &str,
    sender: &MessageSender,
) -> bool {
    !sender.is_bot
}

#[must_use]
pub fn react_message_words(first_word_lower: &str, text: &str) -> Vec<String> {
    let text = text.trim();
    if !text.is_empty() {
        return text.split(' ').map(ToOwned::to_owned).collect();
    }
    if !first_word_lower.is_empty() {
        return vec![first_word_lower.to_owned()];
    }
    Vec::new()
}

pub const HISTORY_MESSAGE_KIND_TEXT: &str = "text";

pub const HISTORY_ROLE_USER: &str = "user";

pub const HISTORY_ROLE_MODEL: &str = "model";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryTextEntry {
    pub entry_id: String,
    pub kind: String,
    pub role: String,
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram forum/private thread ID, or zero when absent.
    pub thread_id: i32,
    /// Telegram message ID.
    pub message_id: i32,
    pub sender_id: i64,
    /// UTC occurrence timestamp.
    pub occurred_at: OffsetDateTime,
    pub payload: Vec<u8>,
}

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

#[must_use]
pub fn stale_update_requires_handle(update: &TelegramUpdate) -> bool {
    matches!(
        &update.update_type,
        TelegramUpdateType::Message(message)
            if matches!(message.data, TelegramMessageData::SuccessfulPayment(_))
    )
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

#[must_use]
pub fn update_chat_id(update: &TelegramUpdate) -> Option<i64> {
    extract_update_chat(update).map(|chat| chat.get_id().into())
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
        TelegramMessageData::Animation(_) => Some("animation"),
        TelegramMessageData::VideoNote(_) => Some("video_note"),
        TelegramMessageData::Audio(_) => Some("audio"),
        TelegramMessageData::Document(_) => Some("document"),
        TelegramMessageData::Location(_) | TelegramMessageData::Venue(_) => Some("location"),
        TelegramMessageData::Contact(_) => Some("contact"),
        TelegramMessageData::Photo(_) | TelegramMessageData::Sticker(_) => Some("image"),
        TelegramMessageData::Dice(_) => Some("dice"),
        TelegramMessageData::Checklist(_) => Some("checklist"),
        TelegramMessageData::Story(_) => Some("story"),
        TelegramMessageData::PaidMedia(_) => Some("paid_media"),
        TelegramMessageData::Poll(_) => Some("poll"),
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
            file_id: ref_data.file_id.clone(),
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
            file_id: video.data.file_id.clone(),
            file_name: video.data.file_name.clone().unwrap_or_default(),
            mime_type: video.data.mime_type.clone().unwrap_or_default(),
            caption: caption.to_owned(),
            duration_seconds: video.data.duration,
            width: video.data.width,
            height: video.data.height,
            ..ChatAttachment::default()
        }),
        TelegramMessageData::Animation(animation) => out.push(ChatAttachment {
            kind: "animation".to_owned(),
            source: source.to_owned(),
            file_unique_id: animation.file_unique_id.clone(),
            file_id: animation.file_id.clone(),
            file_name: animation.file_name.clone().unwrap_or_default(),
            mime_type: animation
                .mime_type
                .clone()
                .unwrap_or_else(|| "video/mp4".to_owned()),
            caption: caption.to_owned(),
            duration_seconds: animation.duration,
            width: animation.width,
            height: animation.height,
            ..ChatAttachment::default()
        }),
        TelegramMessageData::VideoNote(video_note) => out.push(ChatAttachment {
            kind: "video_note".to_owned(),
            source: source.to_owned(),
            file_unique_id: video_note.file_unique_id.clone(),
            file_id: video_note.file_id.clone(),
            mime_type: "video/mp4".to_owned(),
            duration_seconds: video_note.duration,
            width: video_note.length,
            height: video_note.length,
            ..ChatAttachment::default()
        }),
        TelegramMessageData::Audio(audio) => out.push(ChatAttachment {
            kind: "audio".to_owned(),
            source: source.to_owned(),
            file_unique_id: audio.data.file_unique_id.clone(),
            file_id: audio.data.file_id.clone(),
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
            file_id: voice.data.file_id.clone(),
            duration_seconds: voice.data.duration,
            ..ChatAttachment::default()
        }),
        TelegramMessageData::Document(document) => out.push(ChatAttachment {
            kind: if has_mime_prefix(
                document.data.mime_type.as_deref().unwrap_or_default(),
                "video/",
            ) {
                "video"
            } else {
                "document"
            }
            .to_owned(),
            source: source.to_owned(),
            file_unique_id: document.data.file_unique_id.clone(),
            file_id: document.data.file_id.clone(),
            file_name: document.data.file_name.clone().unwrap_or_default(),
            mime_type: document.data.mime_type.clone().unwrap_or_default(),
            caption: caption.to_owned(),
            ..ChatAttachment::default()
        }),
        TelegramMessageData::Sticker(sticker) => out.push(ChatAttachment {
            kind: "sticker".to_owned(),
            source: source.to_owned(),
            file_unique_id: sticker.file_unique_id.clone(),
            file_id: sticker.file_id.clone(),
            content: sticker.emoji.clone().unwrap_or_default(),
            ..ChatAttachment::default()
        }),
        _ => {}
    }
}

fn append_telegram_payload_attachments(
    out: &mut Vec<ChatAttachment>,
    data: &TelegramMessageData,
    source: &str,
) {
    let (kind, content) = match data {
        TelegramMessageData::Dice(dice) => {
            let emoji = char::from(dice.dice_type());
            ("dice", format!("{emoji} result: {}", dice.value()))
        }
        TelegramMessageData::Checklist(checklist) => {
            let tasks = checklist
                .tasks
                .iter()
                .map(|task| task.text.trim())
                .filter(|text| !text.is_empty())
                .collect::<Vec<_>>()
                .join("; ");
            (
                "checklist",
                format!("{}: {}", checklist.title.trim(), tasks)
                    .trim_matches([' ', ':'])
                    .to_owned(),
            )
        }
        TelegramMessageData::Story(story) => ("story", format!("story id {}", story.id)),
        TelegramMessageData::PaidMedia(media) => (
            "paid_media",
            format!(
                "{} paid media item(s), {} Telegram Stars",
                media.paid_media.len(),
                media.star_count
            ),
        ),
        TelegramMessageData::Poll(poll) => {
            let question = match poll {
                carapax::types::Poll::Regular(poll) => poll.question.data.trim(),
                carapax::types::Poll::Quiz(poll) => poll.question.data.trim(),
            };
            ("poll", question.to_owned())
        }
        _ => return,
    };
    out.push(ChatAttachment {
        kind: kind.to_owned(),
        source: source.to_owned(),
        content,
        ..ChatAttachment::default()
    });
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
            file_id: ref_data.file_id.clone(),
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
                file_id: document.data.file_id.clone(),
                mime_type: document.data.mime_type.clone().unwrap_or_default(),
                caption: caption.to_owned(),
                ..ChatAttachment::default()
            })
        }
        TelegramMessageData::Sticker(sticker) => Some(ChatAttachment {
            kind: "image".to_owned(),
            source: source.to_owned(),
            file_unique_id: sticker.file_unique_id.clone(),
            file_id: sticker.file_id.clone(),
            caption: caption.to_owned(),
            ..ChatAttachment::default()
        }),
        _ => None,
    }
}

impl TelegramFileMetadataRef {
    fn with_file(
        mut self,
        file_id: &str,
        file_unique_id: &str,
        media_kind: &str,
        mime_type: &str,
        file_size: i64,
    ) -> Self {
        self.file_id = file_id.to_owned();
        self.file_unique_id = file_unique_id.to_owned();
        self.media_kind = media_kind.to_owned();
        self.mime_type = mime_type.to_owned();
        self.file_size = file_size;
        self
    }

    fn with_dimensions(mut self, width: i64, height: i64) -> Self {
        self.width = width as i32;
        self.height = height as i32;
        self
    }
}

fn telegram_file_metadata_base(message: &TelegramMessage) -> Option<TelegramFileMetadataRef> {
    if message.chat.get_id() == 0 {
        return None;
    }

    Some(TelegramFileMetadataRef {
        chat_id: message.chat.get_id().into(),
        message_id: message.id,
        thread_id: message
            .message_thread_id
            .map(|value| value as i32)
            .unwrap_or_default(),
        ..TelegramFileMetadataRef::default()
    })
}

fn document_file_metadata_ref(
    base: &TelegramFileMetadataRef,
    data: &TelegramMessageData,
) -> Option<TelegramFileMetadataRef> {
    let TelegramMessageData::Document(document) = data else {
        return None;
    };
    let mime_type = document.data.mime_type.as_deref().unwrap_or_default();
    if has_mime_prefix(mime_type, "image/") || has_mime_prefix(mime_type, "video/") {
        let mut file = base.clone().with_file(
            &document.data.file_id,
            &document.data.file_unique_id,
            TELEGRAM_FILE_MEDIA_KIND_DOCUMENT,
            mime_type,
            document.data.file_size.unwrap_or_default(),
        );
        if let Some(thumbnail) = document.data.thumbnail.as_ref() {
            file = file.with_dimensions(thumbnail.width, thumbnail.height);
        }
        return Some(file);
    }
    if has_mime_prefix(mime_type, "audio/") {
        return Some(base.clone().with_file(
            &document.data.file_id,
            &document.data.file_unique_id,
            TELEGRAM_FILE_MEDIA_KIND_DOCUMENT,
            mime_type,
            document.data.file_size.unwrap_or_default(),
        ));
    }
    None
}

fn sticker_file_metadata_ref(
    base: &TelegramFileMetadataRef,
    data: &TelegramMessageData,
) -> Option<TelegramFileMetadataRef> {
    let TelegramMessageData::Sticker(sticker) = data else {
        return None;
    };
    if sticker.is_animated || sticker.is_video {
        return None;
    }
    Some(
        base.clone()
            .with_file(
                &sticker.file_id,
                &sticker.file_unique_id,
                TELEGRAM_FILE_MEDIA_KIND_STICKER,
                "",
                sticker.file_size.unwrap_or_default(),
            )
            .with_dimensions(sticker.width, sticker.height),
    )
}

fn has_mime_prefix(mime_type: &str, prefix: &str) -> bool {
    let mime_type = mime_type.trim();
    mime_type
        .get(..prefix.len())
        .is_some_and(|value| value.eq_ignore_ascii_case(prefix))
}

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
        TelegramMessageData::Contact(contact) => {
            let name = format!(
                "{} {}",
                contact.first_name,
                contact.last_name.as_deref().unwrap_or_default()
            )
            .trim()
            .to_owned();
            if name.is_empty() {
                contact.phone_number.clone()
            } else {
                name
            }
        }
        _ => String::new(),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuestMessageRejectReason {
    NilMessage,
    MissingGuestQueryId,
    BotCaller,
    MissingHumanSender,
    BotSender,
    OtherBotMention,
}

impl GuestMessageRejectReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NilMessage => "nil_message",
            Self::MissingGuestQueryId => "missing_guest_query_id",
            Self::BotCaller => "bot_caller",
            Self::MissingHumanSender => "missing_human_sender",
            Self::BotSender => "bot_sender",
            Self::OtherBotMention => "other_bot_mention",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuestChainRole {
    User,
    Assistant,
}

impl GuestChainRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuestChainMessage {
    pub role: GuestChainRole,
    pub name: String,
    pub text: String,
    pub at: Option<OffsetDateTime>,
}

#[must_use]
pub fn guest_visible_text(message: Option<&TelegramMessage>) -> String {
    let Some(message) = message else {
        return String::new();
    };

    if let Some(text) = message.get_text() {
        let text = text.as_ref().trim();
        if !text.is_empty() {
            return text.to_owned();
        }
    }

    if let TelegramMessageData::Sticker(sticker) = &message.data
        && let Some(emoji) = sticker.emoji.as_deref()
    {
        let emoji = emoji.trim();
        if !emoji.is_empty() {
            return emoji.to_owned();
        }
    }

    String::new()
}

#[must_use]
pub fn strip_guest_address_prefix(text: &str, bot_username: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() || bot_username.is_empty() {
        return trimmed.to_owned();
    }

    let username = format!(
        "@{}",
        bot_username.trim().trim_start_matches('@').to_lowercase()
    );
    let (first_word, rest) = cut_first_word(trimmed);
    if first_word.eq_ignore_ascii_case(&username) {
        return rest.trim().to_owned();
    }

    trimmed.to_owned()
}

#[must_use]
pub fn guest_current_request_text(message: Option<&TelegramMessage>, bot_username: &str) -> String {
    strip_guest_address_prefix(&guest_visible_text(message), bot_username)
        .trim()
        .to_owned()
}

#[must_use]
pub fn guest_request_has_visible_text(
    message: Option<&TelegramMessage>,
    bot_username: &str,
) -> bool {
    let current_text = guest_current_request_text(message, bot_username);
    let reply_text = message
        .and_then(reply_message)
        .map_or_else(String::new, |reply| guest_visible_text(Some(reply)));

    !current_text.trim().is_empty() || !reply_text.trim().is_empty()
}

#[must_use]
pub fn guest_has_other_bot_mention(
    message: Option<&TelegramMessage>,
    bot_user: Option<&TelegramUser>,
) -> bool {
    let Some(message) = message else {
        return false;
    };

    let own_bot_id = bot_user.map(user_id_i64).unwrap_or_default();
    let own_username = bot_user
        .and_then(user_username)
        .map(|username| normalize_username(&username))
        .unwrap_or_default();

    if guest_text_mentions_other_bot(message.get_text(), own_bot_id) {
        return true;
    }

    guest_ascii_mentions(&guest_visible_text(Some(message)))
        .into_iter()
        .any(|mention| {
            let username = normalize_username(&mention);
            !username.is_empty() && username != own_username && username.ends_with("bot")
        })
}

#[must_use]
pub fn guest_message_reject_reason(
    message: Option<&TelegramMessage>,
    bot_user: Option<&TelegramUser>,
) -> Option<GuestMessageRejectReason> {
    let Some(message) = message else {
        return Some(GuestMessageRejectReason::NilMessage);
    };

    if message
        .guest_query_id
        .as_deref()
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        return Some(GuestMessageRejectReason::MissingGuestQueryId);
    }
    if guest_bot_caller_user_is_bot(message) {
        return Some(GuestMessageRejectReason::BotCaller);
    }

    let Some(sender) = message.sender.get_user() else {
        return Some(GuestMessageRejectReason::MissingHumanSender);
    };
    if sender.is_bot {
        return Some(GuestMessageRejectReason::BotSender);
    }
    if bot_user.is_some() && guest_has_other_bot_mention(Some(message), bot_user) {
        return Some(GuestMessageRejectReason::OtherBotMention);
    }

    None
}

#[must_use]
pub fn is_guest_unsupported_feature_request(
    message: Option<&TelegramMessage>,
    bot_username: &str,
) -> bool {
    let text = strip_guest_address_prefix(&guest_visible_text(message), bot_username)
        .trim()
        .to_lowercase();
    if text.is_empty() {
        return false;
    }

    let (first_word, rest_text) = cut_first_word(&text);
    let first_word = normalize_guest_command_word(&first_word, bot_username);
    let first_word = first_word.as_str();

    if first_word == "%"
        || DRAW_VERB_ALIASES.contains(&first_word)
        || GUEST_DRAW_BANG_ALIASES.contains(&first_word)
        || SONG_ALIASES.contains(&first_word)
        || is_edit_verb(first_word)
        || GUEST_UNSUPPORTED_COMMANDS.contains(&first_word)
        || first_word.starts_with("admin")
    {
        return true;
    }

    looks_like_guest_history_summary_request(first_word, &rest_text)
}

#[must_use]
pub fn looks_like_guest_history_summary_request(first_word: &str, rest_text: &str) -> bool {
    let text = format!("{first_word} {rest_text}").trim().to_owned();
    if text.is_empty() {
        return false;
    }

    let has_summary_verb = text.contains("перескаж")
        || text.contains("summary")
        || text.contains("recap")
        || text.contains("о чем говорили")
        || text.contains("о чём говорили");
    has_summary_verb && (text.contains("чат") || text.contains("тред") || text.contains("сообщен"))
}

#[must_use]
pub fn normalize_guest_command_word(word: &str, bot_username: &str) -> String {
    let word = word.trim().strip_prefix('/').unwrap_or(word.trim());
    if word.is_empty() {
        return String::new();
    }

    let Some((command, username)) = word.split_once('@') else {
        return word.to_owned();
    };

    if bot_username.is_empty()
        || username.eq_ignore_ascii_case(bot_username.trim().trim_start_matches('@'))
    {
        return command.to_owned();
    }

    word.to_owned()
}

#[must_use]
pub fn format_guest_chain_for_prompt(messages: &[GuestChainMessage]) -> String {
    let messages = trim_guest_chain_messages(messages, GUEST_CHAIN_MAX_MESSAGES);
    if messages.is_empty() {
        return String::new();
    }

    messages
        .iter()
        .map(|message| format!("{}: {}", message.name, message.text))
        .collect::<Vec<_>>()
        .join("\n")
}

#[must_use]
pub fn build_guest_dialog_text(
    message: Option<&TelegramMessage>,
    bot_username: &str,
    chain: &[GuestChainMessage],
) -> String {
    let current = guest_current_request_text(message, bot_username);
    let reply = message
        .and_then(reply_message)
        .map_or_else(String::new, |reply| guest_visible_text(Some(reply)));
    let current = current.trim();
    let reply = reply.trim();

    let text = match (current.is_empty(), reply.is_empty()) {
        (false, false) => format!(
            "Гостевой запрос пользователя:\n{current}\n\nКонтекст сообщения, на которое ответили:\n{reply}"
        ),
        (false, true) => current.to_owned(),
        (true, false) => {
            format!("Ответь на сообщение, на которое ссылается гостевой вызов:\n{reply}")
        }
        (true, true) => String::new(),
    };

    let chain_text = format_guest_chain_for_prompt(chain);
    if chain_text.is_empty() {
        return text;
    }
    if text.is_empty() {
        return format!("Гостевая цепочка за последние сутки:\n{chain_text}");
    }
    format!("Гостевая цепочка за последние сутки:\n{chain_text}\n\n{text}")
}

#[must_use]
pub fn build_guest_shield_query_text(
    message: Option<&TelegramMessage>,
    max_chars: usize,
    chain: &[GuestChainMessage],
) -> String {
    let mut parts = Vec::new();
    append_shield_query_part(
        &mut parts,
        "current",
        &guest_current_request_text(message, ""),
    );
    append_shield_query_part(&mut parts, "chain", &format_guest_chain_for_prompt(chain));
    let reply = message
        .and_then(reply_message)
        .map_or_else(String::new, |reply| guest_visible_text(Some(reply)));
    append_shield_query_part(&mut parts, "reply", &reply);

    let query = parts.join("\n").trim().to_owned();
    if max_chars == 0 {
        return query;
    }

    query.chars().take(max_chars).collect()
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

fn reply_message(message: &TelegramMessage) -> Option<&TelegramMessage> {
    match message.reply_to.as_ref()? {
        TelegramReplyTo::Message(reply) => Some(reply),
        TelegramReplyTo::Story(_) => None,
    }
}

fn user_id_i64(user: &TelegramUser) -> i64 {
    user.id.into()
}

fn user_username(user: &TelegramUser) -> Option<String> {
    user.username.as_ref().map(ToString::to_string)
}

fn normalize_username(username: &str) -> String {
    username.trim().trim_start_matches('@').to_lowercase()
}

fn guest_text_mentions_other_bot(text: Option<&TelegramText>, own_bot_id: i64) -> bool {
    let Some(entities) = text.and_then(|text| text.entities.as_ref()) else {
        return false;
    };

    entities.into_iter().any(|entity| {
        let TelegramTextEntity::TextMention { user, .. } = entity else {
            return false;
        };
        if !user.is_bot {
            return false;
        }
        own_bot_id == 0 || user_id_i64(user) != own_bot_id
    })
}

fn guest_ascii_mentions(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut mentions = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'@' {
            i += 1;
            continue;
        }

        let start = i;
        i += 1;
        let name_start = i;
        while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
            i += 1;
        }
        if i - name_start >= 3 {
            mentions.push(text[start..i].to_owned());
        }
    }

    mentions
}

fn guest_bot_caller_user_is_bot(message: &TelegramMessage) -> bool {
    let Some(guest_bot) = &message.guest_bot else {
        return false;
    };

    serde_json::to_value(guest_bot)
        .ok()
        .and_then(|value| {
            value
                .get("guest_bot_caller_user")
                .and_then(|user| user.get("is_bot"))
                .and_then(Value::as_bool)
        })
        .unwrap_or(false)
}

fn trim_guest_chain_messages(
    messages: &[GuestChainMessage],
    max_messages: usize,
) -> Vec<GuestChainMessage> {
    let max_messages = if max_messages == 0 {
        GUEST_CHAIN_MAX_MESSAGES
    } else {
        max_messages
    };
    let mut out = Vec::with_capacity(messages.len().min(max_messages));
    for message in messages {
        if let Some(normalized) = normalize_guest_chain_message(message) {
            out.push(normalized);
        }
    }
    if out.len() <= max_messages {
        return out;
    }

    out[out.len() - max_messages..].to_vec()
}

fn normalize_guest_chain_message(message: &GuestChainMessage) -> Option<GuestChainMessage> {
    let text = message.text.trim();
    if text.is_empty() {
        return None;
    }

    let name = message.name.trim();
    Some(GuestChainMessage {
        role: message.role,
        name: if name.is_empty() {
            guest_chain_default_name(message.role).to_owned()
        } else {
            name.to_owned()
        },
        text: text.to_owned(),
        at: message.at,
    })
}

fn guest_chain_default_name(role: GuestChainRole) -> &'static str {
    match role {
        GuestChainRole::Assistant => "Plotva",
        GuestChainRole::User => "Telegram",
    }
}

fn append_shield_query_part(parts: &mut Vec<String>, label: &str, text: &str) {
    let text = text.trim();
    if !text.is_empty() {
        parts.push(format!("{label}: {text}"));
    }
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

fn addressed_bot_response_bucket(chat_id: i64, message_id: i64) -> u64 {
    if message_id <= 0 {
        return 1;
    }

    let abs_chat_id = chat_id.unsigned_abs();
    (abs_chat_id + message_id as u64) % BOT_ADDRESSED_RESPONSE_SAMPLING_DIVISOR
}

const BOT_ADDRESSED_RESPONSE_SAMPLING_DIVISOR: u64 = 3;

const DRAW_VERB_ALIASES: &[&str] = &["нарисуй", "draw", "рисуй"];

const GUEST_DRAW_BANG_ALIASES: &[&str] = &["!рис", "!draw"];

const DRAW_REPLY_PREFIXES: &[&str] = &["это", "this", "that", "these", "those"];

const SONG_ALIASES: &[&str] = &["song", "песня", "!song", "!песня"];

const GUEST_UNSUPPORTED_COMMANDS: &[&str] = &[
    "settings",
    "admin_settings",
    "delete_drawing",
    "queue_status",
    "cancel_drawing",
    "очередь",
];

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
    name == given || transliterate1(&name) == given || transliterate2(&name) == given
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

async fn run_tracked_stage<Fut, E, Tracker>(
    update: &TelegramUpdate,
    stage: UpdateStage,
    timeout: Duration,
    task: Fut,
    tracker: &Tracker,
) -> UpdateStageReport
where
    Fut: Future<Output = Result<(), E>>,
    E: fmt::Display,
    Tracker: UpdateStageTracker + ?Sized,
{
    let token = tracker.stage_started(update, stage, SystemTime::now());
    let report = run_stage(stage, timeout, task).await;
    tracker.stage_finished(token, &report, SystemTime::now());
    report
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

    use openplotva_core::{ChatAttachment, ChatMessageMeta, MessageSender, SENDER_TYPE_USER};
    use serde_json::{Value, json};

    use super::{
        DEFAULT_UPDATE_QUEUE_KEY, EncodedUpdate, GoUpdateType, GuestChainMessage, GuestChainRole,
        MAX_ENQUEUE_ERRORS, RedisUpdateQueue, TelegramMessageAttachmentOptions, UpdateCodecError,
        UpdateConsumerConfig, UpdateIngressDecision, UpdateIngressGuard, UpdateIngressGuardConfig,
        UpdateProducerQueue, UpdateProducerQueueFuture, UpdateProducerSource,
        UpdateProducerSourceFuture, UpdateStage, UpdateStageOutcome, UpdateStageReport,
        UpdateStageTracker, blocking_response_timeout, blpop_timeout_arg, build_guest_dialog_text,
        build_guest_shield_query_text, command_connection_config, compose_image_prompt,
        edited_image_prompt_update, extract_update_state, fetcher_message_text,
        format_guest_chain_for_prompt, guest_current_request_text, guest_has_other_bot_mention,
        guest_message_reject_reason, guest_request_has_visible_text, guest_visible_text,
        is_guest_unsupported_feature_request, is_settings_command_message,
        looks_like_guest_history_summary_request, normalize_guest_command_word, parse_edit_command,
        parse_if_addressed, process_update_at, process_update_with_stage_tracker_at,
        producer_update_name, producer_update_type, react_message_words,
        resolve_draw_prompt_from_message, run_update_producer_until,
        should_handle_addressed_message, should_handle_random_response, strip_guest_address_prefix,
        telegram_message_attachments, update_name,
    };
    use carapax::types::{
        Message as TelegramMessage, MessageData as TelegramMessageData, Update as TelegramUpdate,
        UpdateType as TelegramUpdateType,
    };

    #[test]
    fn queue_key_matches_go_update_queue() {
        assert_eq!(DEFAULT_UPDATE_QUEUE_KEY, "plotva:updates:queue");
    }

    #[test]
    fn redis_update_queue_clones_share_command_connection_pool() -> Result<(), Box<dyn Error>> {
        let client = redis::Client::open("redis://127.0.0.1/0")?;
        let queue = RedisUpdateQueue::with_key(client, "openplotva:test:updates:pool");
        let clone = queue.clone();

        assert!(
            queue
                .connections
                .shares_command_manager_with(&clone.connections)
        );
        Ok(())
    }

    #[test]
    fn blocking_response_timeout_outlives_server_side_blpop_timeout() {
        assert_eq!(blocking_response_timeout(Duration::ZERO), None);
        assert_eq!(
            blocking_response_timeout(Duration::from_secs(5)),
            Some(Duration::from_secs(7))
        );
    }

    #[test]
    fn command_connection_timeout_outlives_redis_default_response_timeout() {
        let config = command_connection_config();

        assert_eq!(config.response_timeout(), Some(Duration::from_secs(3)));
        assert!(config.response_timeout() > Some(Duration::from_millis(500)));
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
    fn telegram_update_decoder_accepts_bot_api_poll_answer_voter_chat() -> Result<(), Box<dyn Error>>
    {
        let update = super::decode_telegram_update_value(json!({
            "update_id": 17,
            "poll_answer": {
                "poll_id": "poll-id",
                "voter_chat": {
                    "id": -10043,
                    "type": "supergroup",
                    "title": "Poll Team"
                },
                "option_ids": [1],
                "option_persistent_ids": []
            }
        }))?;

        assert_eq!(producer_update_name(&update), "poll_answer");
        let state = extract_update_state(&update)
            .ok_or_else(|| io::Error::other("expected poll-answer state"))?;
        let chat = state
            .chat
            .ok_or_else(|| io::Error::other("expected voter chat state"))?;
        assert_eq!(chat.id, -10043);
        assert_eq!(chat.chat_type, "supergroup");
        assert!(state.user.is_none());

        Ok(())
    }

    #[test]
    fn native_update_frame_decodes_bot_api_poll_answer_voter_chat() -> Result<(), Box<dyn Error>> {
        let payload = serde_json::to_vec(&json!({
            "version": 1,
            "codec": super::NATIVE_UPDATE_CODEC,
            "update": {
                "update_id": 18,
                "poll_answer": {
                    "poll_id": "poll-id",
                    "voter_chat": {
                        "id": -10044,
                        "type": "supergroup",
                        "title": "Poll Team"
                    },
                    "option_ids": [1],
                    "option_persistent_ids": []
                }
            }
        }))?;
        let update = EncodedUpdate::from_native_json_bytes(&payload)?.decode_update()?;

        assert_eq!(producer_update_name(&update), "poll_answer");
        let state = extract_update_state(&update)
            .ok_or_else(|| io::Error::other("expected poll-answer state"))?;
        assert_eq!(state.chat.expect("voter chat").id, -10044);

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
    fn producer_update_type_classifies_without_ingress_filtering() -> Result<(), Box<dyn Error>> {
        let message = sample_message_update()?;
        let poll = sample_poll_update_with_id(101)?;
        let unknown = sample_update_json(json!({
            "update_id": 102,
            "future_update_shape": { "value": true }
        }))?;

        assert_eq!(producer_update_type(&message), GoUpdateType::Message);
        assert_eq!(producer_update_name(&message), "message");
        assert_eq!(producer_update_type(&poll), GoUpdateType::Poll);
        assert_eq!(producer_update_name(&poll), "poll");
        assert_eq!(producer_update_type(&unknown), GoUpdateType::Unknown);
        assert_eq!(producer_update_name(&unknown), "unknown");
        Ok(())
    }

    #[test]
    fn ingress_guard_is_per_chat_covers_user_events_and_never_blocks_payments()
    -> Result<(), Box<dyn Error>> {
        let guard = UpdateIngressGuard::new(UpdateIngressGuardConfig {
            short_window: Duration::from_secs(10),
            short_limit: 3,
            long_window: Duration::from_secs(60),
            long_limit: 100,
            block_duration: Duration::from_secs(300),
        });
        let now = UNIX_EPOCH + Duration::from_secs(1_710_000_000);
        let message = sample_message_update_with_id(100)?;
        let callback = sample_callback_update_with_id(101)?;
        let reaction = sample_update_json(json!({
            "update_id": 102,
            "message_reaction": {
                "chat": sample_private_chat_json(),
                "message_id": 77,
                "date": 1_710_000_000,
                "user": sample_user_json(),
                "old_reaction": [],
                "new_reaction": [{"type": "emoji", "emoji": "👍"}]
            }
        }))?;
        let other_chat = sample_update_json(json!({
            "update_id": 103,
            "message": {
                "message_id": 78,
                "date": 1_710_000_000,
                "chat": {"id": 9001, "type": "private", "first_name": "Other"},
                "from": {"id": 9001, "is_bot": false, "first_name": "Other"},
                "text": "hello"
            }
        }))?;
        let payment = sample_successful_payment_update_with_date(1_710_000_000)?;

        assert!(guard.check_update_at(&message, now).is_allowed());
        assert!(guard.check_update_at(&callback, now).is_allowed());
        assert!(guard.check_update_at(&reaction, now).is_dropped());
        assert!(guard.check_update_at(&other_chat, now).is_allowed());
        assert_eq!(
            guard.check_update_at(&payment, now),
            UpdateIngressDecision::Allowed {
                chat_id: Some(42),
                payment: true,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn update_producer_enqueues_every_update_type_without_ingress_drops()
    -> Result<(), Box<dyn Error>> {
        let source = ProducerSourceStub::new(vec![
            sample_message_update_with_id(100)?,
            sample_poll_update_with_id(101)?,
            sample_callback_update_with_id(102)?,
        ]);
        let queue = ProducerQueueStub::default();

        let report = run_update_producer_until(&source, &queue, std::future::pending()).await;

        assert_eq!(report.received, 3);
        assert_eq!(report.enqueued, 3);
        assert!(report.source_closed);
        assert!(report.enqueue_errors.is_empty());
        assert_eq!(queue.enqueued_ids(), vec![100, 101, 102]);
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
        assert!(report.source_closed);
        assert_eq!(report.enqueue_errors, vec!["redis unavailable".to_owned()]);
        assert_eq!(queue.enqueued_ids(), vec![101]);
        Ok(())
    }

    #[tokio::test]
    async fn update_producer_enqueue_error_vec_is_bounded() -> Result<(), Box<dyn Error>> {
        // Drive more than MAX_ENQUEUE_ERRORS failures so the cap and the dropped
        // counter are both exercised.
        let total_failures = MAX_ENQUEUE_ERRORS + 10;
        let updates: Vec<TelegramUpdate> = (0..total_failures as i64)
            .map(sample_message_update_with_id)
            .collect::<Result<_, _>>()?;
        let failures: Vec<&'static str> = vec!["queue full"; total_failures];
        let source = ProducerSourceStub::new(updates);
        let queue = ProducerQueueStub::default().with_failures(failures);

        let report = run_update_producer_until(&source, &queue, std::future::pending()).await;

        assert_eq!(report.enqueue_errors.len(), MAX_ENQUEUE_ERRORS);
        assert_eq!(
            report.dropped_enqueue_errors,
            total_failures - MAX_ENQUEUE_ERRORS
        );
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
            "message_reaction"
        );
        assert_eq!(
            update_name(&serde_json::from_value(json!({
                "update_id": 4,
                "inline_query": {
                    "id": "inline-id",
                    "from": sample_user_json(),
                    "query": "plotva",
                    "offset": ""
                }
            }))?),
            "inline_query"
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
    fn guest_visible_text_and_address_prefix_match_go_helpers() -> Result<(), Box<dyn Error>> {
        let text_message = sample_guest_message_from_value(sample_guest_message_json(
            501,
            "  @PlotvaBot   привет  ",
        ))?;
        assert_eq!(
            guest_visible_text(Some(&text_message)),
            "@PlotvaBot   привет"
        );
        assert_eq!(
            guest_current_request_text(Some(&text_message), "plotvabot"),
            "привет"
        );
        assert_eq!(
            strip_guest_address_prefix("  @OtherBot привет  ", "PlotvaBot"),
            "@OtherBot привет"
        );
        assert_eq!(guest_visible_text(None), "");

        let caption_message = sample_guest_message_from_value(json!({
            "message_id": 502,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "guest_query_id": "guest-query",
            "photo": [
                {
                    "file_id": "photo-file",
                    "file_unique_id": "photo-1",
                    "height": 90,
                    "width": 90
                }
            ],
            "caption": "  caption request  "
        }))?;
        assert_eq!(
            guest_visible_text(Some(&caption_message)),
            "caption request"
        );

        let sticker_message = sample_guest_message_from_value(json!({
            "message_id": 503,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "guest_query_id": "guest-query",
            "sticker": {
                "file_id": "sticker-file",
                "file_unique_id": "sticker-1",
                "type": "regular",
                "width": 64,
                "height": 64,
                "is_animated": false,
                "is_video": false,
                "emoji": " 🙂 "
            }
        }))?;
        assert_eq!(guest_visible_text(Some(&sticker_message)), "🙂");

        Ok(())
    }

    #[test]
    fn guest_request_has_visible_text_uses_current_message_or_reply() -> Result<(), Box<dyn Error>>
    {
        let current_message = sample_guest_message_from_value(sample_guest_message_json(
            504,
            "  @PlotvaBot   расскажи  ",
        ))?;
        assert!(guest_request_has_visible_text(
            Some(&current_message),
            "PlotvaBot"
        ));

        let reply_only_message = sample_guest_message_from_value(json!({
            "message_id": 505,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "guest_query_id": "guest-query",
            "text": " @PlotvaBot ",
            "reply_to_message": sample_message_json(404, 1_709_999_900, "  original topic  ")
        }))?;
        assert_eq!(
            guest_current_request_text(Some(&reply_only_message), "PlotvaBot"),
            ""
        );
        assert!(guest_request_has_visible_text(
            Some(&reply_only_message),
            "PlotvaBot"
        ));

        let empty_message =
            sample_guest_message_from_value(sample_guest_message_json(506, " @PlotvaBot "))?;
        assert!(!guest_request_has_visible_text(
            Some(&empty_message),
            "PlotvaBot"
        ));
        assert!(!guest_request_has_visible_text(None, "PlotvaBot"));

        Ok(())
    }

    #[test]
    fn guest_message_reject_reason_matches_go_guard_order() -> Result<(), Box<dyn Error>> {
        let bot = sample_bot_user();
        assert_eq!(
            guest_message_reject_reason(None, Some(&bot)).map(|reason| reason.as_str()),
            Some("nil_message")
        );

        let missing_query = sample_guest_message_from_value(json!({
            "message_id": 507,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "text": "hello"
        }))?;
        assert_eq!(
            guest_message_reject_reason(Some(&missing_query), Some(&bot))
                .map(|reason| reason.as_str()),
            Some("missing_guest_query_id")
        );

        let bot_caller = sample_guest_message_from_value(json!({
            "message_id": 508,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "guest_query_id": "guest-query",
            "guest_bot_caller_user": {
                "id": 88,
                "is_bot": true,
                "first_name": "CallerBot"
            },
            "text": "hello"
        }))?;
        assert_eq!(
            guest_message_reject_reason(Some(&bot_caller), Some(&bot))
                .map(|reason| reason.as_str()),
            Some("bot_caller")
        );

        let missing_sender = sample_guest_message_from_value(json!({
            "message_id": 509,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "guest_query_id": "guest-query",
            "text": "hello"
        }))?;
        assert_eq!(
            guest_message_reject_reason(Some(&missing_sender), Some(&bot))
                .map(|reason| reason.as_str()),
            Some("missing_human_sender")
        );

        let bot_sender = sample_guest_message_from_value(json!({
            "message_id": 510,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "guest_query_id": "guest-query",
            "from": {
                "id": 100,
                "is_bot": true,
                "first_name": "SenderBot"
            },
            "text": "hello"
        }))?;
        assert_eq!(
            guest_message_reject_reason(Some(&bot_sender), Some(&bot))
                .map(|reason| reason.as_str()),
            Some("bot_sender")
        );

        let other_bot_mention =
            sample_guest_message_from_value(sample_guest_message_json(511, "hello @OtherBot"))?;
        assert_eq!(
            guest_message_reject_reason(Some(&other_bot_mention), Some(&bot))
                .map(|reason| reason.as_str()),
            Some("other_bot_mention")
        );

        let accepted =
            sample_guest_message_from_value(sample_guest_message_json(512, "hello Plotva"))?;
        assert_eq!(
            guest_message_reject_reason(Some(&accepted), Some(&bot)),
            None
        );

        Ok(())
    }

    #[test]
    fn guest_other_bot_mention_matches_go_text_and_entity_rules() -> Result<(), Box<dyn Error>> {
        let bot = sample_bot_user();
        let own_mention =
            sample_guest_message_from_value(sample_guest_message_json(513, "hello @PlotvaBot"))?;
        assert!(!guest_has_other_bot_mention(Some(&own_mention), Some(&bot)));

        let not_bot =
            sample_guest_message_from_value(sample_guest_message_json(514, "hello @PlotvaTeam"))?;
        assert!(!guest_has_other_bot_mention(Some(&not_bot), Some(&bot)));

        let bot_suffix =
            sample_guest_message_from_value(sample_guest_message_json(515, "hello @OtherBot"))?;
        assert!(guest_has_other_bot_mention(Some(&bot_suffix), Some(&bot)));

        let entity_bot = sample_guest_message_from_value(json!({
            "message_id": 516,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "guest_query_id": "guest-query",
            "text": "other",
            "entities": [
                {
                    "type": "text_mention",
                    "offset": 0,
                    "length": 5,
                    "user": {
                        "id": 888,
                        "is_bot": true,
                        "first_name": "Other"
                    }
                }
            ]
        }))?;
        assert!(guest_has_other_bot_mention(Some(&entity_bot), Some(&bot)));
        assert!(!guest_has_other_bot_mention(None, Some(&bot)));

        Ok(())
    }

    #[test]
    fn guest_unsupported_feature_request_matches_go_command_blocks() -> Result<(), Box<dyn Error>> {
        let cases = [
            ("@PlotvaBot draw a cat", true),
            ("/draw@PlotvaBot cat", true),
            ("!рис кота", true),
            ("песня про море", true),
            ("измени картинку", true),
            ("/settings@PlotvaBot", true),
            ("queue_status", true),
            ("cancel_drawing", true),
            ("admin_chat_settings 42", true),
            ("%", true),
            ("перескажи о чём говорили в чате", true),
            ("@PlotvaBot", false),
            ("/draw@OtherBot cat", false),
            ("summary only", false),
            ("просто поговорим", false),
        ];

        for (text, want) in cases {
            let message = sample_guest_message_from_value(sample_guest_message_json(520, text))?;
            assert_eq!(
                is_guest_unsupported_feature_request(Some(&message), "PlotvaBot"),
                want,
                "guest unsupported feature detection for {text:?}"
            );
        }
        assert!(!is_guest_unsupported_feature_request(None, "PlotvaBot"));

        Ok(())
    }

    #[test]
    fn guest_command_normalization_and_history_summary_match_go_helpers() {
        assert_eq!(
            normalize_guest_command_word("/draw@PlotvaBot", "plotvabot"),
            "draw"
        );
        assert_eq!(
            normalize_guest_command_word("/draw@OtherBot", "plotvabot"),
            "draw@OtherBot"
        );
        assert_eq!(normalize_guest_command_word("/settings", ""), "settings");

        assert!(looks_like_guest_history_summary_request(
            "перескажи",
            "о чем говорили в чате"
        ));
        assert!(looks_like_guest_history_summary_request(
            "summary",
            "чат recap"
        ));
        assert!(!looks_like_guest_history_summary_request("summary", "only"));
        assert!(!looks_like_guest_history_summary_request(
            "перескажи",
            "личные новости"
        ));
    }

    #[test]
    fn guest_chain_prompt_formatting_matches_go_normalization_and_limit() {
        let mut chain = vec![
            GuestChainMessage {
                role: GuestChainRole::User,
                name: "  ".to_owned(),
                text: "  first  ".to_owned(),
                at: None,
            },
            GuestChainMessage {
                role: GuestChainRole::Assistant,
                name: "".to_owned(),
                text: " second ".to_owned(),
                at: None,
            },
            GuestChainMessage {
                role: GuestChainRole::User,
                name: "Ignored".to_owned(),
                text: "   ".to_owned(),
                at: None,
            },
        ];
        for idx in 0..16 {
            chain.push(GuestChainMessage {
                role: GuestChainRole::User,
                name: format!("User{idx}"),
                text: format!("message {idx}"),
                at: None,
            });
        }

        let formatted = format_guest_chain_for_prompt(&chain);
        assert!(!formatted.contains("first"));
        assert!(!formatted.contains("second"));
        assert!(!formatted.contains("Ignored"));
        assert!(formatted.starts_with("User1: message 1"));
        assert!(formatted.ends_with("User15: message 15"));
        assert_eq!(formatted.lines().count(), 15);

        assert_eq!(
            format_guest_chain_for_prompt(&[
                GuestChainMessage {
                    role: GuestChainRole::User,
                    name: " ".to_owned(),
                    text: " hi ".to_owned(),
                    at: None,
                },
                GuestChainMessage {
                    role: GuestChainRole::Assistant,
                    name: " ".to_owned(),
                    text: " hello ".to_owned(),
                    at: None,
                },
            ]),
            "Telegram: hi\nPlotva: hello"
        );
    }

    #[test]
    fn guest_dialog_text_matches_go_current_reply_and_chain_shapes() -> Result<(), Box<dyn Error>> {
        let chain = vec![
            GuestChainMessage {
                role: GuestChainRole::User,
                name: "Alice".to_owned(),
                text: "старый вопрос".to_owned(),
                at: None,
            },
            GuestChainMessage {
                role: GuestChainRole::Assistant,
                name: "Plotva".to_owned(),
                text: "старый ответ".to_owned(),
                at: None,
            },
        ];
        let current_and_reply = sample_guest_message_from_value(json!({
            "message_id": 530,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "guest_query_id": "guest-query",
            "text": " @PlotvaBot текущий вопрос ",
            "reply_to_message": sample_message_json(420, 1_709_999_900, "  исходный контекст  ")
        }))?;

        assert_eq!(
            build_guest_dialog_text(Some(&current_and_reply), "PlotvaBot", &[]),
            "Гостевой запрос пользователя:\nтекущий вопрос\n\nКонтекст сообщения, на которое ответили:\nисходный контекст"
        );
        assert_eq!(
            build_guest_dialog_text(Some(&current_and_reply), "PlotvaBot", &chain),
            "Гостевая цепочка за последние сутки:\nAlice: старый вопрос\nPlotva: старый ответ\n\nГостевой запрос пользователя:\nтекущий вопрос\n\nКонтекст сообщения, на которое ответили:\nисходный контекст"
        );

        let reply_only = sample_guest_message_from_value(json!({
            "message_id": 531,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "guest_query_id": "guest-query",
            "text": " @PlotvaBot ",
            "reply_to_message": sample_message_json(421, 1_709_999_900, "  ответь на это  ")
        }))?;
        assert_eq!(
            build_guest_dialog_text(Some(&reply_only), "PlotvaBot", &[]),
            "Ответь на сообщение, на которое ссылается гостевой вызов:\nответь на это"
        );

        let empty =
            sample_guest_message_from_value(sample_guest_message_json(532, " @PlotvaBot "))?;
        assert_eq!(
            build_guest_dialog_text(Some(&empty), "PlotvaBot", &chain),
            "Гостевая цепочка за последние сутки:\nAlice: старый вопрос\nPlotva: старый ответ"
        );
        assert_eq!(build_guest_dialog_text(None, "PlotvaBot", &[]), "");

        Ok(())
    }

    #[test]
    fn guest_shield_query_text_matches_go_parts_and_rune_truncation() -> Result<(), Box<dyn Error>>
    {
        let message = sample_guest_message_from_value(json!({
            "message_id": 533,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "guest_query_id": "guest-query",
            "text": " @PlotvaBot текущий риск ",
            "reply_to_message": sample_message_json(422, 1_709_999_900, "  ответный риск  ")
        }))?;
        let chain = vec![GuestChainMessage {
            role: GuestChainRole::User,
            name: "Alice".to_owned(),
            text: "предыдущий риск".to_owned(),
            at: None,
        }];

        assert_eq!(
            build_guest_shield_query_text(Some(&message), 0, &chain),
            "current: @PlotvaBot текущий риск\nchain: Alice: предыдущий риск\nreply: ответный риск"
        );
        assert_eq!(
            build_guest_shield_query_text(Some(&message), 18, &chain),
            "current: @PlotvaBo"
        );
        assert_eq!(
            build_guest_shield_query_text(None, 0, &chain),
            "chain: Alice: предыдущий риск"
        );

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
                    file_id: "doc-file".to_owned(),
                    mime_type: "image/png".to_owned(),
                    caption: "diagram".to_owned(),
                    ..openplotva_core::ChatAttachment::default()
                },
                openplotva_core::ChatAttachment {
                    kind: "document".to_owned(),
                    source: "message".to_owned(),
                    file_unique_id: "doc-image".to_owned(),
                    file_id: "doc-file".to_owned(),
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
                file_id: "sticker-file".to_owned(),
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
    fn telegram_file_metadata_refs_match_go_photo_audio_voice_and_static_sticker()
    -> Result<(), Box<dyn Error>> {
        let photo_update = serde_json::from_value(json!({
            "update_id": 137,
            "message": {
                "message_id": 69,
                "message_thread_id": 7,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "photo": [
                    {
                        "file_id": "photo-small",
                        "file_unique_id": "photo-small-u",
                        "height": 10,
                        "width": 10,
                        "file_size": 1
                    },
                    {
                        "file_id": "photo-large",
                        "file_unique_id": "photo-large-u",
                        "height": 800,
                        "width": 600,
                        "file_size": 20
                    }
                ],
                "reply_to_message": {
                    "message_id": 68,
                    "date": 1_709_999_900,
                    "chat": sample_private_chat_json(),
                    "from": sample_user_json(),
                    "voice": {
                        "file_id": "voice-file",
                        "file_unique_id": "voice-u",
                        "duration": 5,
                        "file_size": 30
                    }
                }
            }
        }))?;

        let refs = super::update_file_metadata_refs(&photo_update);

        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].media_kind, super::TELEGRAM_FILE_MEDIA_KIND_PHOTO);
        assert_eq!(refs[0].file_id, "photo-large");
        assert_eq!(refs[0].file_unique_id, "photo-large-u");
        assert_eq!(refs[0].width, 600);
        assert_eq!(refs[0].height, 800);
        assert_eq!(refs[0].file_size, 20);
        assert_eq!(refs[0].chat_id, 42);
        assert_eq!(refs[0].message_id, 69);
        assert_eq!(refs[0].thread_id, 7);
        assert_eq!(refs[1].media_kind, super::TELEGRAM_FILE_MEDIA_KIND_VOICE);
        assert_eq!(refs[1].mime_type, "audio/ogg");
        assert_eq!(refs[1].file_unique_id, "voice-u");

        let audio_update = serde_json::from_value(json!({
            "update_id": 138,
            "message": {
                "message_id": 70,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "audio": {
                    "file_id": "audio-file",
                    "file_unique_id": "audio-u",
                    "duration": 7,
                    "mime_type": "audio/mpeg",
                    "file_size": 40
                }
            }
        }))?;
        let audio_refs = super::update_file_metadata_refs(&audio_update);
        assert_eq!(audio_refs.len(), 1);
        assert_eq!(
            audio_refs[0].media_kind,
            super::TELEGRAM_FILE_MEDIA_KIND_AUDIO
        );
        assert_eq!(audio_refs[0].mime_type, "audio/mpeg");

        let sticker_update = serde_json::from_value(json!({
            "update_id": 139,
            "message": {
                "message_id": 71,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "sticker": {
                    "file_id": "sticker-file",
                    "file_unique_id": "sticker-u",
                    "height": 512,
                    "width": 512,
                    "is_animated": false,
                    "is_video": false,
                    "type": "regular",
                    "file_size": 50
                }
            }
        }))?;
        let sticker_refs = super::update_file_metadata_refs(&sticker_update);
        assert_eq!(sticker_refs.len(), 1);
        assert_eq!(
            sticker_refs[0].media_kind,
            super::TELEGRAM_FILE_MEDIA_KIND_STICKER
        );
        assert_eq!(sticker_refs[0].width, 512);
        assert_eq!(sticker_refs[0].height, 512);

        Ok(())
    }

    #[test]
    fn telegram_file_metadata_refs_match_go_document_mime_filters() -> Result<(), Box<dyn Error>> {
        let image_update = serde_json::from_value(json!({
            "update_id": 140,
            "message": {
                "message_id": 72,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "document": {
                    "file_id": "image-file",
                    "file_unique_id": "image-u",
                    "mime_type": " IMAGE/PNG ",
                    "file_size": 60,
                    "thumbnail": {
                        "file_id": "thumb",
                        "file_unique_id": "thumb-u",
                        "height": 90,
                        "width": 120
                    }
                }
            }
        }))?;
        let audio_update = serde_json::from_value(json!({
            "update_id": 141,
            "message": {
                "message_id": 73,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "document": {
                    "file_id": "audio-doc",
                    "file_unique_id": "audio-doc-u",
                    "mime_type": " AUDIO/MPEG ",
                    "file_size": 70
                }
            }
        }))?;
        let pdf_update = serde_json::from_value(json!({
            "update_id": 142,
            "message": {
                "message_id": 74,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "document": {
                    "file_id": "pdf-file",
                    "file_unique_id": "pdf-u",
                    "mime_type": "application/pdf"
                }
            }
        }))?;

        let image_refs = super::update_file_metadata_refs(&image_update);
        assert_eq!(image_refs.len(), 1);
        assert_eq!(
            image_refs[0].media_kind,
            super::TELEGRAM_FILE_MEDIA_KIND_DOCUMENT
        );
        assert_eq!(image_refs[0].mime_type, " IMAGE/PNG ");
        assert_eq!(image_refs[0].width, 120);
        assert_eq!(image_refs[0].height, 90);

        let audio_refs = super::update_file_metadata_refs(&audio_update);
        assert_eq!(audio_refs.len(), 1);
        assert_eq!(audio_refs[0].file_unique_id, "audio-doc-u");
        assert_eq!(audio_refs[0].mime_type, " AUDIO/MPEG ");

        assert!(super::update_file_metadata_refs(&pdf_update).is_empty());
        Ok(())
    }

    #[test]
    fn telegram_video_payloads_preserve_download_and_dialog_metadata() -> Result<(), Box<dyn Error>>
    {
        let cases = [
            (
                "video",
                json!({
                    "file_id": "video-file",
                    "file_unique_id": "video-u",
                    "duration": 12,
                    "width": 1280,
                    "height": 720,
                    "mime_type": "video/mp4",
                    "file_size": 1000
                }),
                super::TELEGRAM_FILE_MEDIA_KIND_VIDEO,
            ),
            (
                "animation",
                json!({
                    "file_id": "animation-file",
                    "file_unique_id": "animation-u",
                    "duration": 4,
                    "width": 320,
                    "height": 240,
                    "mime_type": "video/mp4",
                    "file_size": 500
                }),
                super::TELEGRAM_FILE_MEDIA_KIND_ANIMATION,
            ),
            (
                "video_note",
                json!({
                    "file_id": "note-file",
                    "file_unique_id": "note-u",
                    "duration": 9,
                    "length": 384,
                    "file_size": 700
                }),
                super::TELEGRAM_FILE_MEDIA_KIND_VIDEO_NOTE,
            ),
        ];

        for (field, media, expected_kind) in cases {
            let mut message = json!({
                "message_id": 80,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json()
            });
            message[field] = media;
            let update: TelegramUpdate = serde_json::from_value(json!({
                "update_id": 180,
                "message": message
            }))?;
            let message = update_message(&update)?;
            let refs = super::update_file_metadata_refs(&update);
            let attachments = super::telegram_message_attachments(
                message,
                TelegramMessageAttachmentOptions::default(),
            );

            assert_eq!(refs.len(), 1, "{field}");
            assert_eq!(refs[0].media_kind, expected_kind, "{field}");
            assert_eq!(refs[0].mime_type, "video/mp4", "{field}");
            assert_eq!(attachments.len(), 1, "{field}");
            assert_eq!(attachments[0].kind, field, "{field}");
            assert!(!attachments[0].file_unique_id.is_empty(), "{field}");
            assert!(attachments[0].duration_seconds > 0, "{field}");
        }

        Ok(())
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

        let audio_file_update = serde_json::from_value(json!({
            "update_id": 21,
            "message": {
                "message_id": 81,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "audio": {
                    "file_id": "audio-file",
                    "file_unique_id": "audio-unique",
                    "duration": 5,
                    "file_name": "song.mp3"
                }
            }
        }))?;
        assert_eq!(
            fetcher_message_text(update_message(&audio_file_update)?),
            "song.mp3"
        );

        let video_update = serde_json::from_value(json!({
            "update_id": 22,
            "message": {
                "message_id": 82,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "video": {
                    "file_id": "video-file",
                    "file_unique_id": "video-unique",
                    "duration": 5,
                    "width": 640,
                    "height": 360,
                    "file_name": "clip.mp4"
                }
            }
        }))?;
        assert_eq!(
            fetcher_message_text(update_message(&video_update)?),
            "clip.mp4"
        );

        let document_update = serde_json::from_value(json!({
            "update_id": 23,
            "message": {
                "message_id": 83,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "document": {
                    "file_id": "doc-file",
                    "file_unique_id": "doc-unique",
                    "file_name": "doc.pdf"
                }
            }
        }))?;
        assert_eq!(
            fetcher_message_text(update_message(&document_update)?),
            "doc.pdf"
        );

        let sticker_update = serde_json::from_value(json!({
            "update_id": 24,
            "message": {
                "message_id": 84,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "sticker": {
                    "file_id": "sticker-file",
                    "file_unique_id": "sticker-unique",
                    "type": "regular",
                    "width": 512,
                    "height": 512,
                    "is_animated": false,
                    "is_video": false,
                    "emoji": "🙂"
                }
            }
        }))?;
        assert_eq!(fetcher_message_text(update_message(&sticker_update)?), "🙂");

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

        let phone_contact_update = serde_json::from_value(json!({
            "update_id": 4,
            "message": {
                "message_id": 80,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "contact": {
                    "phone_number": "+123",
                    "first_name": "",
                    "last_name": ""
                }
            }
        }))?;
        assert_eq!(
            fetcher_message_text(update_message(&phone_contact_update)?),
            "+123"
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
        assert_eq!(got.attachments[1].file_id, "photo-file");
        assert_eq!(got.attachments[1].caption, "photo caption");

        Ok(())
    }

    #[test]
    fn build_fetcher_message_context_includes_quoted_reply_audio_like_go()
    -> Result<(), Box<dyn Error>> {
        let update = sample_update_json(json!({
            "update_id": 141,
            "message": {
                "message_id": 73,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "text": "!song night",
                "reply_to_message": {
                    "message_id": 72,
                    "date": 1_709_999_999,
                    "chat": sample_private_chat_json(),
                    "from": sample_user_json(),
                    "audio": {
                        "file_id": "reply-audio-file",
                        "file_unique_id": "reply-audio-unique",
                        "duration": 5
                    }
                }
            }
        }))?;

        let got = super::build_fetcher_message_context(update_message(&update)?);

        assert_eq!(got.meta.attachments.len(), 1);
        assert_eq!(got.meta.attachments[0].kind, "audio");
        assert_eq!(got.meta.attachments[0].source, "quoted");
        assert_eq!(got.meta.attachments[0].file_unique_id, "reply-audio-unique");
        Ok(())
    }

    #[test]
    fn build_fetcher_message_context_keeps_quoted_image_before_current_media()
    -> Result<(), Box<dyn Error>> {
        let update = sample_update_json(json!({
            "update_id": 142,
            "message": {
                "message_id": 74,
                "date": 1_710_000_000,
                "chat": sample_private_chat_json(),
                "from": sample_user_json(),
                "photo": [
                    {
                        "file_id": "current-photo-file",
                        "file_unique_id": "current-photo-unique",
                        "height": 90,
                        "width": 90
                    }
                ],
                "caption": "fix contrast",
                "reply_to_message": {
                    "message_id": 71,
                    "date": 1_709_999_998,
                    "chat": sample_private_chat_json(),
                    "from": sample_user_json(),
                    "photo": [
                        {
                            "file_id": "reply-photo-file",
                            "file_unique_id": "reply-photo-unique",
                            "height": 90,
                            "width": 90
                        }
                    ]
                }
            }
        }))?;

        let got = super::build_fetcher_message_context(update_message(&update)?);

        assert_eq!(got.meta.attachments.len(), 2);
        assert_eq!(got.meta.attachments[0].kind, "image");
        assert_eq!(got.meta.attachments[0].source, "quoted");
        assert_eq!(got.meta.attachments[0].file_unique_id, "reply-photo-unique");
        assert_eq!(got.meta.attachments[1].kind, "image");
        assert_eq!(got.meta.attachments[1].source, "message");
        assert_eq!(
            got.meta.attachments[1].file_unique_id,
            "current-photo-unique"
        );
        assert_eq!(got.meta.attachments[1].caption, "fix contrast");
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
    fn parse_if_addressed_accepts_lowercase_cyrillic_bot_name_like_go_equalfold()
    -> Result<(), Box<dyn Error>> {
        let bot = sample_bot_user();
        let name_address = sample_message_from_value(json!({
            "message_id": 9,
            "date": 1_710_000_000,
            "chat": {
                "id": -42,
                "type": "group",
                "title": "Group"
            },
            "from": sample_user_json(),
            "text": "плотва, измени фон"
        }))?;

        let parsed = parse_if_addressed(&name_address, &bot);

        assert!(parsed.is_addressed);
        assert_eq!(parsed.first_word, "измени");
        assert_eq!(parsed.rest_text, "фон");
        Ok(())
    }

    #[test]
    fn parse_if_addressed_accepts_uppercase_name_and_mention_like_go_equalfold()
    -> Result<(), Box<dyn Error>> {
        let bot = sample_bot_user();
        let group_chat = json!({
            "id": -42,
            "type": "group",
            "title": "Group"
        });
        let uppercase_name = sample_message_from_value(json!({
            "message_id": 10,
            "date": 1_710_000_000,
            "chat": group_chat.clone(),
            "from": sample_user_json(),
            "text": "ПЛОТВА, нарисуй кота"
        }))?;
        let mixed_case_translit = sample_message_from_value(json!({
            "message_id": 11,
            "date": 1_710_000_000,
            "chat": group_chat.clone(),
            "from": sample_user_json(),
            "text": "PLOTVA нарисуй кота"
        }))?;
        let uppercase_mention = sample_message_from_value(json!({
            "message_id": 12,
            "date": 1_710_000_000,
            "chat": group_chat,
            "from": sample_user_json(),
            "text": "@PLOTVABOT нарисуй кота"
        }))?;

        for message in [uppercase_name, mixed_case_translit, uppercase_mention] {
            let parsed = parse_if_addressed(&message, &bot);
            assert!(parsed.is_addressed, "{}", parsed.message_text);
            assert_eq!(parsed.first_word, "нарисуй");
            assert_eq!(parsed.rest_text, "кота");
        }
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

    #[test]
    fn should_handle_addressed_message_matches_go_bot_gate() -> Result<(), Box<dyn Error>> {
        let human_sender = MessageSender {
            id: 42,
            is_bot: false,
            ..MessageSender::system()
        };
        let bot_sender = MessageSender {
            id: 777,
            is_bot: true,
            ..MessageSender::system()
        };
        let human_message = sample_message_from_value(json!({
            "message_id": 1,
            "date": 1_710_000_000,
            "chat": {
                "id": -1001,
                "type": "group",
                "title": "Group"
            },
            "from": sample_user_json(),
            "text": "hello"
        }))?;
        let bot_reply_allowed = sample_message_from_value(json!({
            "message_id": 2,
            "date": 1_710_000_000,
            "chat": {
                "id": -100,
                "type": "group",
                "title": "Group"
            },
            "from": {
                "id": 777,
                "is_bot": true,
                "first_name": "RelayBot"
            },
            "reply_to_message": {
                "message_id": 99,
                "date": 1_710_000_000,
                "chat": {
                    "id": -100,
                    "type": "group",
                    "title": "Group"
                },
                "from": {
                    "id": 12345,
                    "is_bot": true,
                    "first_name": "Plotva",
                    "username": "PlotvaBot"
                },
                "text": "old answer"
            },
            "text": "bot reply"
        }))?;
        let bot_reply_denied = sample_message_from_value(json!({
            "message_id": 1,
            "date": 1_710_000_000,
            "chat": {
                "id": -100,
                "type": "group",
                "title": "Group"
            },
            "from": {
                "id": 777,
                "is_bot": true,
                "first_name": "RelayBot"
            },
            "reply_to_message": {
                "message_id": 99,
                "date": 1_710_000_000,
                "chat": {
                    "id": -100,
                    "type": "group",
                    "title": "Group"
                },
                "from": {
                    "id": 12345,
                    "is_bot": true,
                    "first_name": "Plotva",
                    "username": "PlotvaBot"
                },
                "text": "old answer"
            },
            "text": "bot reply"
        }))?;
        let wrong_bot_reply = sample_message_from_value(json!({
            "message_id": 2,
            "date": 1_710_000_000,
            "chat": {
                "id": -100,
                "type": "group",
                "title": "Group"
            },
            "from": {
                "id": 777,
                "is_bot": true,
                "first_name": "RelayBot"
            },
            "reply_to_message": {
                "message_id": 99,
                "date": 1_710_000_000,
                "chat": {
                    "id": -100,
                    "type": "group",
                    "title": "Group"
                },
                "from": {
                    "id": 99999,
                    "is_bot": true,
                    "first_name": "AnotherBot"
                },
                "text": "old answer"
            },
            "text": "bot reply"
        }))?;

        assert!(should_handle_addressed_message(
            Some(&human_message),
            &human_sender,
            12345
        ));
        assert!(should_handle_addressed_message(
            Some(&bot_reply_allowed),
            &bot_sender,
            12345
        ));
        assert!(!should_handle_addressed_message(
            Some(&bot_reply_denied),
            &bot_sender,
            12345
        ));
        assert!(!should_handle_addressed_message(
            Some(&wrong_bot_reply),
            &bot_sender,
            12345
        ));
        assert_eq!(
            super::addressed_bot_response_bucket(100, 2),
            super::addressed_bot_response_bucket(-100, 2)
        );

        Ok(())
    }

    #[test]
    fn should_handle_random_response_matches_go_media_gate() -> Result<(), Box<dyn Error>> {
        let human_sender = MessageSender {
            id: 42,
            is_bot: false,
            ..MessageSender::system()
        };
        let bot_sender = MessageSender {
            id: 777,
            is_bot: true,
            ..MessageSender::system()
        };
        let text_message = sample_message_from_value(json!({
            "message_id": 14,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "text": "привет"
        }))?;
        let document_message = sample_message_from_value(json!({
            "message_id": 15,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "document": {
                "file_id": "doc-file",
                "file_unique_id": "doc-unique",
                "file_name": "IMG_4253.MP4"
            }
        }))?;
        let captioned_video_message = sample_message_from_value(json!({
            "message_id": 16,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "video": {
                "file_id": "video-file",
                "file_unique_id": "video-unique",
                "width": 640,
                "height": 480,
                "duration": 7
            },
            "caption": "смотри что тут"
        }))?;
        let animation_message = sample_message_from_value(json!({
            "message_id": 17,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "animation": {
                "file_id": "animation-file",
                "file_unique_id": "animation-unique",
                "width": 320,
                "height": 240,
                "duration": 4
            }
        }))?;
        let premium_animation_message = sample_message_from_value(json!({
            "message_id": 18,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "premium_animation": {
                "file_id": "premium-animation-file",
                "file_unique_id": "premium-animation-unique",
                "width": 320,
                "height": 240,
                "duration": 4
            }
        }))?;

        assert!(should_handle_random_response(
            Some(&text_message),
            "привет",
            &human_sender
        ));
        assert!(!should_handle_random_response(None, "", &bot_sender));
        assert!(should_handle_random_response(
            Some(&document_message),
            "",
            &human_sender
        ));
        assert!(should_handle_random_response(
            Some(&captioned_video_message),
            "",
            &human_sender
        ));
        assert!(should_handle_random_response(
            Some(&animation_message),
            "",
            &human_sender
        ));
        assert!(matches!(
            &premium_animation_message.data,
            TelegramMessageData::Unknown(value)
                if value
                    .as_object()
                    .is_some_and(|object| object.contains_key("premium_animation"))
        ));
        assert!(should_handle_random_response(
            Some(&premium_animation_message),
            "",
            &human_sender
        ));

        Ok(())
    }

    #[test]
    fn react_message_words_preserve_go_fallback_order() {
        assert_eq!(
            react_message_words("draw", "draw cat"),
            vec!["draw".to_owned(), "cat".to_owned()]
        );
        assert_eq!(react_message_words("draw", " "), vec!["draw".to_owned()]);
        assert!(react_message_words("", " ").is_empty());
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
    async fn update_consumer_stage_tracker_observes_go_state_and_handle_tasks()
    -> Result<(), Box<dyn Error>> {
        #[derive(Clone, Default)]
        struct Tracker {
            events: Arc<Mutex<Vec<String>>>,
        }

        impl UpdateStageTracker for Tracker {
            fn stage_started(
                &self,
                update: &carapax::types::Update,
                stage: UpdateStage,
                _started_at: SystemTime,
            ) -> u64 {
                self.events
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(format!("start:{stage:?}:{}", update_name(update)));
                match stage {
                    UpdateStage::State => 1,
                    UpdateStage::Handle => 2,
                }
            }

            fn stage_finished(
                &self,
                token: u64,
                report: &UpdateStageReport,
                _finished_at: SystemTime,
            ) {
                self.events
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(format!("finish:{token}:{:?}", report.outcome));
            }
        }

        let tracker = Tracker::default();
        let update = sample_message_update_with_date(1_710_000_000)?;
        let report = process_update_with_stage_tracker_at(
            update,
            UpdateConsumerConfig::default(),
            UNIX_EPOCH + Duration::from_secs(1_710_000_030),
            |_| async { Ok::<_, io::Error>(()) },
            |_| async { Ok::<_, io::Error>(()) },
            &tracker,
        )
        .await;

        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        let events = tracker
            .events
            .lock()
            .map_err(|err| io::Error::other(err.to_string()))?
            .clone();
        assert!(events.contains(&"start:State:message".to_owned()));
        assert!(events.contains(&"start:Handle:message".to_owned()));
        assert!(events.contains(&"finish:1:Completed".to_owned()));
        assert!(events.contains(&"finish:2:Completed".to_owned()));

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
    async fn update_consumer_handles_stale_successful_payment() -> Result<(), Box<dyn Error>> {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let state_calls = calls.clone();
        let handle_calls = calls.clone();
        let update = sample_successful_payment_update_with_date(1_710_000_000)?;
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
            ["state", "handle"]
        );
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert!(!report.skipped_handle);

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

    #[tokio::test]
    async fn live_redis_cancelled_blocking_dequeues_do_not_swallow_updates_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };

        let client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:updates:cancel:{suffix}");
        let queue = RedisUpdateQueue::with_key(client.clone(), key.clone());
        let mut connection = client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut connection)
            .await?;

        // Abandon in-flight blocking reads the way the consumer select loop
        // does. Each abandoned read must cancel its server-side BLPOP instead
        // of leaving it to swallow the next enqueued update.
        for _ in 0..5 {
            let pending = queue.dequeue_encoded(Duration::from_secs(5));
            tokio::pin!(pending);
            let raced = tokio::time::timeout(Duration::from_millis(200), &mut pending).await;
            assert!(
                raced.is_err(),
                "BLPOP should still be blocked when abandoned"
            );
        }

        queue.enqueue_update(&sample_message_update()?).await?;
        let dequeued = queue
            .dequeue_update(Duration::from_secs(5))
            .await?
            .ok_or_else(|| io::Error::other("update swallowed by an abandoned blocking read"))?;
        assert_eq!(dequeued.id, 12345);

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

    fn sample_successful_payment_update_with_date(
        date: i64,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12346,
            "message": {
                "message_id": 78,
                "date": date,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Alice",
                    "username": "alice"
                },
                "from": sample_user_json(),
                "successful_payment": {
                    "currency": "XTR",
                    "total_amount": 300,
                    "invoice_payload": "subscription_42",
                    "telegram_payment_charge_id": "telegram-charge",
                    "provider_payment_charge_id": "provider-charge"
                }
            }
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

    fn sample_guest_message_from_value(
        value: serde_json::Value,
    ) -> Result<TelegramMessage, serde_json::Error> {
        serde_json::from_value(value)
    }

    fn sample_guest_message_json(message_id: i64, text: &str) -> serde_json::Value {
        json!({
            "message_id": message_id,
            "date": 1_710_000_000,
            "chat": sample_private_chat_json(),
            "from": sample_user_json(),
            "guest_query_id": "guest-query",
            "text": text
        })
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

    fn sample_user_json() -> serde_json::Value {
        json!({
            "id": 99,
            "is_bot": false,
            "first_name": "Ada",
            "username": "ada_l"
        })
    }
}
