//! Telegram update ingestion, classification, and replay.

use std::{io, time::Duration};

use carapax::types::Update as TelegramUpdate;
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
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use serde_json::json;

    use super::{
        DEFAULT_UPDATE_QUEUE_KEY, EncodedUpdate, RedisUpdateQueue, UpdateCodecError,
        blpop_timeout_arg,
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

        let _: i64 = redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut connection)
            .await?;
        Ok(())
    }

    fn sample_message_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12345,
            "message": {
                "message_id": 77,
                "date": 1710000000,
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
                "text": "/start hello"
            }
        }))
    }
}
