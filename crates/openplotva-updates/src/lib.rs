//! Telegram update ingestion, classification, and replay.

use std::{
    fmt,
    future::Future,
    io,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use carapax::types::{
    MaybeInaccessibleMessage, Update as TelegramUpdate, UpdateType as TelegramUpdateType,
};
use redis::Client as RedisClient;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Human-readable crate purpose used by scaffold tests and docs.
pub const PURPOSE: &str = "updates";

/// Go `internal/updates.QueueKey` Redis list used for Telegram update ingestion.
pub const DEFAULT_UPDATE_QUEUE_KEY: &str = "plotva:updates:queue";

/// Rust-native update payload format stored inside the Redis update queue.
pub const NATIVE_UPDATE_CODEC: &str = "openplotva.update.v1+carapax-json.zstd";

pub const NATIVE_UPDATE_FORMAT_VERSION: u16 = 1;

/// Go `internal/processor` dequeue timeout for the update consumer loop.
pub const DEFAULT_UPDATE_DEQUEUE_TIMEOUT: Duration = Duration::from_secs(5);

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
        env,
        error::Error,
        io,
        sync::{Arc, Mutex},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use serde_json::json;

    use super::{
        DEFAULT_UPDATE_QUEUE_KEY, EncodedUpdate, RedisUpdateQueue, UpdateCodecError,
        UpdateConsumerConfig, UpdateStageOutcome, blpop_timeout_arg, process_update_at,
        update_name,
    };
    use carapax::types::Update as TelegramUpdate;

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

    fn sample_message_update() -> Result<TelegramUpdate, serde_json::Error> {
        sample_message_update_with_date(1_710_000_000)
    }

    fn sample_message_update_with_date(date: i64) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12345,
            "message": sample_message_json(77, date, "/start hello")
        }))
    }

    fn sample_message_json(message_id: i64, date: i64, text: &str) -> serde_json::Value {
        json!({
            "message_id": message_id,
            "date": date,
            "chat": {
                "id": 42,
                "type": "private",
                "first_name": "Ada",
                "username": "ada_l"
            },
            "from": sample_user_json(),
            "text": text
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
