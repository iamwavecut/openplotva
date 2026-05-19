//! Telegram update ingestion, classification, and replay.

use std::{io, time::Duration};

use redis::Client as RedisClient;
use thiserror::Error;

/// Human-readable crate purpose used by scaffold tests and docs.
pub const PURPOSE: &str = "updates";

/// Go `internal/updates.QueueKey` Redis list used for Telegram update ingestion.
pub const DEFAULT_UPDATE_QUEUE_KEY: &str = "plotva:updates:queue";

/// A queued Telegram update in the Go wire format.
///
/// The Go runtime stores each update as zstd-compressed `encoding/gob`
/// bytes and pushes the bytes as a binary-safe Redis string. The typed
/// `api.Update` gob schema is still Go-owned; this wrapper keeps the
/// queue representation byte-compatible while the Rust update processor
/// is being ported.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedUpdate {
    compressed_gob: Vec<u8>,
}

impl EncodedUpdate {
    /// Build an encoded update from the raw Redis list value written by Go.
    pub fn from_go_queue_value(value: impl Into<Vec<u8>>) -> Self {
        Self {
            compressed_gob: value.into(),
        }
    }

    /// Build an encoded update from already-gob-encoded `api.Update` bytes.
    pub fn from_gob_bytes(gob_bytes: &[u8]) -> Result<Self, UpdateCodecError> {
        Ok(Self {
            compressed_gob: zstd::encode_all(gob_bytes, 1)?,
        })
    }

    /// Return the binary Redis value expected by Go `RPushStringContext`.
    pub fn as_go_queue_value(&self) -> &[u8] {
        &self.compressed_gob
    }

    /// Consume this wrapper and return the binary Redis value.
    pub fn into_go_queue_value(self) -> Vec<u8> {
        self.compressed_gob
    }

    /// Decompress this queued update into Go gob bytes.
    pub fn decompress_gob(&self) -> Result<Vec<u8>, UpdateCodecError> {
        Ok(zstd::decode_all(self.compressed_gob.as_slice())?)
    }
}

/// Redis-backed Telegram update queue matching Go `internal/updates.Queue`.
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
            .arg(update.as_go_queue_value())
            .query_async(&mut connection)
            .await?;
        Ok(())
    }

    /// Compress gob bytes and enqueue them with Go `RPUSH` semantics.
    pub async fn enqueue_gob_bytes(&self, gob_bytes: &[u8]) -> Result<(), UpdateQueueError> {
        let update = EncodedUpdate::from_gob_bytes(gob_bytes)?;
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
        Ok(result.map(|(_, value)| EncodedUpdate::from_go_queue_value(value)))
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
}

/// Errors returned by the Redis-backed update queue.
#[derive(Debug, Error)]
pub enum UpdateQueueError {
    /// The zstd/gob payload could not be prepared.
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

    use super::{DEFAULT_UPDATE_QUEUE_KEY, EncodedUpdate, RedisUpdateQueue, blpop_timeout_arg};

    #[test]
    fn queue_key_matches_go_update_queue() {
        assert_eq!(DEFAULT_UPDATE_QUEUE_KEY, "plotva:updates:queue");
    }

    #[test]
    fn encoded_update_preserves_go_queue_value_bytes() {
        let value = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00];
        let update = EncodedUpdate::from_go_queue_value(value.clone());

        assert_eq!(update.as_go_queue_value(), value.as_slice());
        assert_eq!(update.into_go_queue_value(), value);
    }

    #[test]
    fn zstd_update_frame_round_trips_gob_bytes() -> Result<(), Box<dyn Error>> {
        let gob_bytes = b"go gob api.Update bytes stay opaque for now";
        let update = EncodedUpdate::from_gob_bytes(gob_bytes)?;

        assert_ne!(update.as_go_queue_value(), gob_bytes);
        assert_eq!(update.decompress_gob()?, gob_bytes);

        Ok(())
    }

    #[test]
    fn invalid_zstd_update_frame_is_rejected() {
        let update = EncodedUpdate::from_go_queue_value(b"not zstd".to_vec());

        assert!(update.decompress_gob().is_err());
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

        let first = EncodedUpdate::from_gob_bytes(b"first gob update")?;
        let second = EncodedUpdate::from_gob_bytes(b"second gob update")?;
        queue.enqueue_encoded(&first).await?;
        queue.enqueue_encoded(&second).await?;
        assert_eq!(queue.len().await?, 2);
        assert!(!queue.is_empty().await?);

        let dequeued = queue
            .dequeue_encoded(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected queued update"))?;
        assert_eq!(dequeued.decompress_gob()?, b"first gob update");
        assert_eq!(queue.len().await?, 1);

        let dequeued = queue
            .dequeue_encoded(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected queued update"))?;
        assert_eq!(dequeued.decompress_gob()?, b"second gob update");
        assert!(queue.is_empty().await?);

        let _: i64 = redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut connection)
            .await?;
        Ok(())
    }
}
