use std::{collections::BTreeMap, fmt, str::FromStr, time::Duration};

use redis::streams::{
    StreamAutoClaimReply, StreamInfoGroupsReply, StreamRangeReply, StreamReadReply,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::RedisUpdateConnections;

/// Prefix used for per-bot durable ingress streams.
pub const DEFAULT_UPDATE_STREAM_KEY_PREFIX: &str = "plotva:updates:stream:v1";
/// Consumer group used by update materializers.
pub const DEFAULT_UPDATE_STREAM_CONSUMER_GROUP: &str = "openplotva-materializers-v1";
/// Version of the fields stored in one Redis Stream entry.
pub const UPDATE_STREAM_SCHEMA_VERSION: u16 = 1;

pub const DEFAULT_UPDATE_MATERIALIZER_BATCH_MAX_ROWS: usize = 512;
pub const DEFAULT_UPDATE_MATERIALIZER_BATCH_MAX_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_UPDATE_MATERIALIZER_BATCH_MAX_WAIT: Duration = Duration::from_millis(50);
pub const DEFAULT_UPDATE_MATERIALIZER_DB_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_UPDATE_STREAM_READ_BLOCK_TIMEOUT: Duration = Duration::from_secs(1);
pub const DEFAULT_UPDATE_STREAM_RECLAIM_IDLE: Duration = Duration::from_secs(60);

pub const MIN_UPDATE_MATERIALIZER_BATCH_ROWS: usize = 1;
pub const MAX_UPDATE_MATERIALIZER_BATCH_ROWS: usize = 1_000;
pub const MIN_UPDATE_MATERIALIZER_BATCH_BYTES: usize = 1024 * 1024;
pub const MAX_UPDATE_MATERIALIZER_BATCH_BYTES: usize = 32 * 1024 * 1024;
pub const MIN_UPDATE_MATERIALIZER_BATCH_WAIT: Duration = Duration::from_millis(1);
pub const MAX_UPDATE_MATERIALIZER_BATCH_WAIT: Duration = Duration::from_secs(1);
/// Reserve headroom below PostgreSQL's bind-parameter limit.
pub const UPDATE_MATERIALIZER_MAX_BIND_PARAMETERS: usize = 60_000;

const STREAM_FIELD_SCHEMA_VERSION: &str = "schema_version";
const STREAM_FIELD_BOT_ID: &str = "bot_id";
const STREAM_FIELD_UPDATE_ID: &str = "update_id";
const STREAM_FIELD_SOURCE: &str = "source";
/// Unix timestamp in milliseconds.
const STREAM_FIELD_RECEIVED_AT: &str = "received_at";
const STREAM_FIELD_PAYLOAD: &str = "payload";
const STREAM_FIELD_PAYLOAD_SHA256: &str = "payload_sha256";

const ACK_AND_DELETE_SCRIPT: &str = r#"
local acked = redis.call('XACK', KEYS[1], ARGV[1], unpack(ARGV, 2))
local deleted = redis.call('XDEL', KEYS[1], unpack(ARGV, 2))
return {acked, deleted}
"#;

const RENEW_MATERIALIZER_LEASE_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"#;

const RELEASE_MATERIALIZER_LEASE_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"#;

/// Redis Stream source for one Telegram update.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateStreamSource {
    Webhook,
    LongPoll,
    /// Crash-safe import from the retired Redis List queue.
    Legacy,
}

impl UpdateStreamSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Webhook => "webhook",
            Self::LongPoll => "long_poll",
            Self::Legacy => "legacy",
        }
    }
}

impl FromStr for UpdateStreamSource {
    type Err = UpdateStreamSourceParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "webhook" => Ok(Self::Webhook),
            "long_poll" => Ok(Self::LongPoll),
            "legacy" => Ok(Self::Legacy),
            _ => Err(UpdateStreamSourceParseError),
        }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("unsupported update stream source")]
pub struct UpdateStreamSourceParseError;

/// The two numeric components of a Redis Stream entry ID.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct UpdateStreamId {
    pub milliseconds: u64,
    pub sequence: u64,
}

impl UpdateStreamId {
    pub const MIN: Self = Self {
        milliseconds: 0,
        sequence: 0,
    };
}

impl fmt::Display for UpdateStreamId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}-{}", self.milliseconds, self.sequence)
    }
}

impl FromStr for UpdateStreamId {
    type Err = UpdateStreamIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some((milliseconds, sequence)) = value.split_once('-') else {
            return Err(UpdateStreamIdParseError::InvalidFormat);
        };
        if sequence.contains('-') {
            return Err(UpdateStreamIdParseError::InvalidFormat);
        }
        Ok(Self {
            milliseconds: milliseconds
                .parse()
                .map_err(|_| UpdateStreamIdParseError::InvalidMilliseconds)?,
            sequence: sequence
                .parse()
                .map_err(|_| UpdateStreamIdParseError::InvalidSequence)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum UpdateStreamIdParseError {
    #[error("Redis Stream ID must contain exactly one dash")]
    InvalidFormat,
    #[error("Redis Stream ID has invalid milliseconds")]
    InvalidMilliseconds,
    #[error("Redis Stream ID has invalid sequence")]
    InvalidSequence,
}

/// Raw update accepted into the Redis fan-in stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateStreamAppend {
    pub bot_id: i64,
    pub update_id: Option<i64>,
    pub source: UpdateStreamSource,
    /// Server receive time as Unix milliseconds.
    pub received_at_unix_ms: i64,
    /// Original Telegram JSON body.
    pub raw_payload: Vec<u8>,
}

impl UpdateStreamAppend {
    #[must_use]
    pub fn payload_sha256(&self) -> [u8; 32] {
        Sha256::digest(&self.raw_payload).into()
    }
}

/// Redis Stream entry retaining the raw fields needed to quarantine malformed envelopes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawUpdateStreamEntry {
    pub stream_id: UpdateStreamId,
    pub fields: BTreeMap<String, Vec<u8>>,
}

impl RawUpdateStreamEntry {
    /// Validate and decode the Redis envelope without parsing the Telegram JSON body.
    pub fn decode(&self) -> Result<UpdateStreamEntry, UpdateStreamEntryError> {
        let schema_version =
            parse_required_field::<u16>(&self.fields, STREAM_FIELD_SCHEMA_VERSION)?;
        if schema_version != UPDATE_STREAM_SCHEMA_VERSION {
            return Err(UpdateStreamEntryError::UnsupportedSchemaVersion {
                version: schema_version,
            });
        }

        let bot_id = parse_required_field::<i64>(&self.fields, STREAM_FIELD_BOT_ID)?;
        let update_id = parse_optional_field::<i64>(&self.fields, STREAM_FIELD_UPDATE_ID)?;
        let source = required_utf8_field(&self.fields, STREAM_FIELD_SOURCE)?
            .parse()
            .map_err(|_| UpdateStreamEntryError::InvalidField {
                field: STREAM_FIELD_SOURCE,
            })?;
        let received_at_unix_ms =
            parse_required_field::<i64>(&self.fields, STREAM_FIELD_RECEIVED_AT)?;
        let raw_payload = required_field(&self.fields, STREAM_FIELD_PAYLOAD)?.to_vec();
        let hash = required_field(&self.fields, STREAM_FIELD_PAYLOAD_SHA256)?;
        let payload_sha256: [u8; 32] = hash
            .try_into()
            .map_err(|_| UpdateStreamEntryError::InvalidPayloadHashLength { actual: hash.len() })?;
        let actual_payload_sha256: [u8; 32] = Sha256::digest(&raw_payload).into();
        if payload_sha256 != actual_payload_sha256 {
            return Err(UpdateStreamEntryError::PayloadHashMismatch);
        }

        Ok(UpdateStreamEntry {
            stream_id: self.stream_id,
            schema_version,
            bot_id,
            update_id,
            source,
            received_at_unix_ms,
            raw_payload,
            payload_sha256,
        })
    }

    #[must_use]
    pub fn raw_payload(&self) -> Option<&[u8]> {
        self.fields.get(STREAM_FIELD_PAYLOAD).map(Vec::as_slice)
    }
}

/// Validated Redis Stream envelope ready for bulk materialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateStreamEntry {
    pub stream_id: UpdateStreamId,
    pub schema_version: u16,
    pub bot_id: i64,
    pub update_id: Option<i64>,
    pub source: UpdateStreamSource,
    pub received_at_unix_ms: i64,
    pub raw_payload: Vec<u8>,
    pub payload_sha256: [u8; 32],
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum UpdateStreamEntryError {
    #[error("update stream entry is missing field {field}")]
    MissingField { field: &'static str },
    #[error("update stream entry has invalid field {field}")]
    InvalidField { field: &'static str },
    #[error("unsupported update stream schema version {version}")]
    UnsupportedSchemaVersion { version: u16 },
    #[error("update stream payload hash has invalid length {actual}")]
    InvalidPayloadHashLength { actual: usize },
    #[error("update stream payload hash does not match its payload")]
    PayloadHashMismatch,
}

/// Runtime limits shared by the Redis reader and Postgres bulk materializer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UpdateStreamMaterializerConfig {
    pub batch_max_rows: usize,
    pub batch_max_bytes: usize,
    pub batch_max_wait: Duration,
    pub db_timeout: Duration,
    pub read_block_timeout: Duration,
    pub reclaim_idle: Duration,
}

impl Default for UpdateStreamMaterializerConfig {
    fn default() -> Self {
        Self {
            batch_max_rows: DEFAULT_UPDATE_MATERIALIZER_BATCH_MAX_ROWS,
            batch_max_bytes: DEFAULT_UPDATE_MATERIALIZER_BATCH_MAX_BYTES,
            batch_max_wait: DEFAULT_UPDATE_MATERIALIZER_BATCH_MAX_WAIT,
            db_timeout: DEFAULT_UPDATE_MATERIALIZER_DB_TIMEOUT,
            read_block_timeout: DEFAULT_UPDATE_STREAM_READ_BLOCK_TIMEOUT,
            reclaim_idle: DEFAULT_UPDATE_STREAM_RECLAIM_IDLE,
        }
    }
}

impl UpdateStreamMaterializerConfig {
    pub fn validate(self) -> Result<Self, UpdateStreamConfigError> {
        validate_range(
            "batch_max_rows",
            self.batch_max_rows as u128,
            MIN_UPDATE_MATERIALIZER_BATCH_ROWS as u128,
            MAX_UPDATE_MATERIALIZER_BATCH_ROWS as u128,
        )?;
        validate_range(
            "batch_max_bytes",
            self.batch_max_bytes as u128,
            MIN_UPDATE_MATERIALIZER_BATCH_BYTES as u128,
            MAX_UPDATE_MATERIALIZER_BATCH_BYTES as u128,
        )?;
        validate_range(
            "batch_max_wait_ms",
            self.batch_max_wait.as_millis(),
            MIN_UPDATE_MATERIALIZER_BATCH_WAIT.as_millis(),
            MAX_UPDATE_MATERIALIZER_BATCH_WAIT.as_millis(),
        )?;
        if self.db_timeout.is_zero() {
            return Err(UpdateStreamConfigError::ZeroDuration {
                field: "db_timeout",
            });
        }
        if self.read_block_timeout.is_zero() {
            return Err(UpdateStreamConfigError::ZeroDuration {
                field: "read_block_timeout",
            });
        }
        if self.reclaim_idle.is_zero() {
            return Err(UpdateStreamConfigError::ZeroDuration {
                field: "reclaim_idle",
            });
        }
        Ok(self)
    }

    /// Limit rows so one multi-value statement retains PostgreSQL bind headroom.
    pub fn effective_max_rows(
        self,
        bind_parameters_per_row: usize,
    ) -> Result<usize, UpdateStreamConfigError> {
        let config = self.validate()?;
        if bind_parameters_per_row == 0 {
            return Err(UpdateStreamConfigError::ZeroBindParametersPerRow);
        }
        let bind_limited = UPDATE_MATERIALIZER_MAX_BIND_PARAMETERS / bind_parameters_per_row;
        if bind_limited == 0 {
            return Err(UpdateStreamConfigError::TooManyBindParametersPerRow {
                actual: bind_parameters_per_row,
            });
        }
        Ok(config.batch_max_rows.min(bind_limited))
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum UpdateStreamConfigError {
    #[error("{field} must be between {minimum} and {maximum}, got {actual}")]
    OutOfRange {
        field: &'static str,
        minimum: u128,
        maximum: u128,
        actual: u128,
    },
    #[error("{field} must be greater than zero")]
    ZeroDuration { field: &'static str },
    #[error("bind_parameters_per_row must be greater than zero")]
    ZeroBindParametersPerRow,
    #[error("bind_parameters_per_row {actual} exceeds the materializer bind-parameter budget")]
    TooManyBindParametersPerRow { actual: usize },
}

/// Result of one `XAUTOCLAIM` scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateStreamClaimBatch {
    pub next_start: UpdateStreamId,
    pub entries: Vec<RawUpdateStreamEntry>,
    pub deleted_ids: Vec<UpdateStreamId>,
    pub invalid_entries: bool,
}

/// Counts returned by the atomic `XACK` + `XDEL` script.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UpdateStreamAckReport {
    pub requested: usize,
    pub acknowledged: usize,
    pub deleted: usize,
}

/// Consumer-group details returned by `XINFO GROUPS`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateStreamGroupStats {
    pub consumers: usize,
    pub pending: usize,
    pub lag: Option<usize>,
    pub last_delivered_id: UpdateStreamId,
}

/// Redis-side ingress backlog stats.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateStreamStats {
    pub length: usize,
    pub oldest_entry_id: Option<UpdateStreamId>,
    pub group: Option<UpdateStreamGroupStats>,
}

/// Physical durability and memory pressure reported by the ingress Redis.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UpdateStreamDurabilityStats {
    pub used_memory_bytes: u64,
    pub maxmemory_bytes: u64,
    pub maxmemory_policy: String,
    pub aof_enabled: bool,
    pub aof_current_size_bytes: u64,
    pub aof_rewrite_in_progress: bool,
    pub aof_last_write_status: String,
    pub aof_last_rewrite_status: String,
}

impl UpdateStreamStats {
    /// Approximate age based on the Redis-generated entry ID.
    #[must_use]
    pub fn oldest_entry_age_at(&self, now_unix_ms: u64) -> Option<Duration> {
        self.oldest_entry_id
            .map(|id| Duration::from_millis(now_unix_ms.saturating_sub(id.milliseconds)))
    }
}

/// Durable Redis Stream ingress for one bot identity.
#[derive(Clone, Debug)]
pub struct RedisUpdateStream {
    connections: RedisUpdateConnections,
    key: String,
    group: String,
    long_poll_cursor_key: String,
    materializer_lease_key: String,
}

impl RedisUpdateStream {
    #[must_use]
    pub fn new(client: redis::Client, bot_id: i64) -> Self {
        Self::with_key_and_group(
            client,
            update_stream_key(bot_id),
            DEFAULT_UPDATE_STREAM_CONSUMER_GROUP,
        )
    }

    #[must_use]
    pub fn with_key(client: redis::Client, key: impl Into<String>) -> Self {
        Self::with_key_and_group(client, key, DEFAULT_UPDATE_STREAM_CONSUMER_GROUP)
    }

    #[must_use]
    pub fn with_key_and_group(
        client: redis::Client,
        key: impl Into<String>,
        group: impl Into<String>,
    ) -> Self {
        let key = key.into();
        Self {
            connections: RedisUpdateConnections::new(client),
            long_poll_cursor_key: format!("{key}:long-poll-cursor"),
            materializer_lease_key: format!("{key}:materializer-lease"),
            key,
            group: group.into(),
        }
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[must_use]
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Create the materializer consumer group at the beginning of the stream.
    /// Returns `true` when this call created it and `false` when it already existed.
    pub async fn ensure_consumer_group(&self) -> Result<bool, UpdateStreamError> {
        let mut connection = self.connections.command_connection().await?;
        let result: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.key)
            .arg(&self.group)
            .arg("0-0")
            .arg("MKSTREAM")
            .query_async(&mut connection)
            .await;
        match result {
            Ok(()) => Ok(true),
            Err(error) if error.code() == Some("BUSYGROUP") => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// Append one untrimmed entry. The command intentionally contains neither
    /// `MAXLEN` nor any TTL/admission policy.
    pub async fn append(
        &self,
        update: &UpdateStreamAppend,
    ) -> Result<UpdateStreamId, UpdateStreamError> {
        let mut connection = self.connections.command_connection().await?;
        let id: String = build_append_command(&self.key, update)
            .query_async(&mut connection)
            .await?;
        Ok(id.parse()?)
    }

    /// Read the last long-poll offset committed with a Stream append batch.
    pub async fn long_poll_cursor(&self) -> Result<i64, UpdateStreamError> {
        let mut connection = self.connections.command_connection().await?;
        let cursor: Option<i64> = redis::cmd("GET")
            .arg(&self.long_poll_cursor_key)
            .query_async(&mut connection)
            .await?;
        Ok(cursor.unwrap_or_default())
    }

    /// Append one Telegram long-poll response and advance its offset in the
    /// same Redis transaction. A crash therefore replays either the whole
    /// response or none of it.
    pub async fn append_long_poll_batch(
        &self,
        updates: &[UpdateStreamAppend],
        next_cursor: i64,
    ) -> Result<Vec<UpdateStreamId>, UpdateStreamError> {
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        for update in updates {
            pipeline.add_command(build_append_command(&self.key, update));
        }
        pipeline
            .cmd("SET")
            .arg(&self.long_poll_cursor_key)
            .arg(next_cursor)
            .ignore();
        let mut connection = self.connections.command_connection().await?;
        let ids: Vec<String> = pipeline.query_async(&mut connection).await?;
        ids.into_iter()
            .map(|id| id.parse().map_err(UpdateStreamError::from))
            .collect()
    }

    /// Acquire the single-active-materializer lease for this bot Stream.
    pub async fn acquire_materializer_lease(
        &self,
        owner: &str,
        ttl: Duration,
    ) -> Result<bool, UpdateStreamError> {
        let mut connection = self.connections.command_connection().await?;
        let result: Option<String> = redis::cmd("SET")
            .arg(&self.materializer_lease_key)
            .arg(owner)
            .arg("NX")
            .arg("PX")
            .arg(duration_millis(ttl))
            .query_async(&mut connection)
            .await?;
        Ok(result.is_some())
    }

    /// Renew a lease only while it is still owned by this worker.
    pub async fn renew_materializer_lease(
        &self,
        owner: &str,
        ttl: Duration,
    ) -> Result<bool, UpdateStreamError> {
        let mut connection = self.connections.command_connection().await?;
        let renewed: i64 = redis::cmd("EVAL")
            .arg(RENEW_MATERIALIZER_LEASE_SCRIPT)
            .arg(1)
            .arg(&self.materializer_lease_key)
            .arg(owner)
            .arg(duration_millis(ttl))
            .query_async(&mut connection)
            .await?;
        Ok(renewed == 1)
    }

    /// Best-effort compare-and-delete on orderly shutdown.
    pub async fn release_materializer_lease(&self, owner: &str) -> Result<bool, UpdateStreamError> {
        let mut connection = self.connections.command_connection().await?;
        let released: i64 = redis::cmd("EVAL")
            .arg(RELEASE_MATERIALIZER_LEASE_SCRIPT)
            .arg(1)
            .arg(&self.materializer_lease_key)
            .arg(owner)
            .query_async(&mut connection)
            .await?;
        Ok(released == 1)
    }

    /// Read new entries into the consumer group's PEL. A zero block duration
    /// performs a non-blocking read.
    pub async fn read_group(
        &self,
        consumer: &str,
        block: Duration,
        max_count: usize,
    ) -> Result<Vec<RawUpdateStreamEntry>, UpdateStreamError> {
        if max_count == 0 {
            return Ok(Vec::new());
        }

        let reply: Option<StreamReadReply> = if block.is_zero() {
            let mut connection = self.connections.command_connection().await?;
            build_read_group_command(&self.key, &self.group, consumer, block, max_count)
                .query_async(&mut connection)
                .await?
        } else {
            let mut connection = self.connections.blocking_connection(block).await?;
            build_read_group_command(&self.key, &self.group, consumer, block, max_count)
                .query_async(&mut connection)
                .await?
        };
        raw_entries_from_read_reply(reply)
    }

    /// Reassign idle PEL entries to this consumer. Call repeatedly with
    /// `next_start` until Redis returns `0-0`.
    pub async fn reclaim_pending(
        &self,
        consumer: &str,
        min_idle: Duration,
        start: UpdateStreamId,
        max_count: usize,
    ) -> Result<UpdateStreamClaimBatch, UpdateStreamError> {
        if max_count == 0 {
            return Ok(UpdateStreamClaimBatch {
                next_start: start,
                entries: Vec::new(),
                deleted_ids: Vec::new(),
                invalid_entries: false,
            });
        }

        let mut connection = self.connections.command_connection().await?;
        let reply: StreamAutoClaimReply = redis::cmd("XAUTOCLAIM")
            .arg(&self.key)
            .arg(&self.group)
            .arg(consumer)
            .arg(duration_millis(min_idle))
            .arg(start.to_string())
            .arg("COUNT")
            .arg(max_count)
            .query_async(&mut connection)
            .await?;

        Ok(UpdateStreamClaimBatch {
            next_start: reply.next_stream_id.parse()?,
            entries: raw_entries_from_stream_ids(reply.claimed)?,
            deleted_ids: reply
                .deleted_ids
                .into_iter()
                .map(|id| id.parse().map_err(UpdateStreamError::from))
                .collect::<Result<Vec<_>, _>>()?,
            invalid_entries: reply.invalid_entries,
        })
    }

    /// Atomically acknowledge and delete a materialized batch.
    pub async fn acknowledge_and_delete(
        &self,
        ids: &[UpdateStreamId],
    ) -> Result<UpdateStreamAckReport, UpdateStreamError> {
        if ids.is_empty() {
            return Ok(UpdateStreamAckReport::default());
        }
        let ids = ids.iter().map(ToString::to_string).collect::<Vec<_>>();
        let mut connection = self.connections.command_connection().await?;
        let (acknowledged, deleted): (usize, usize) = redis::cmd("EVAL")
            .arg(ACK_AND_DELETE_SCRIPT)
            .arg(1)
            .arg(&self.key)
            .arg(&self.group)
            .arg(&ids)
            .query_async(&mut connection)
            .await?;
        Ok(UpdateStreamAckReport {
            requested: ids.len(),
            acknowledged,
            deleted,
        })
    }

    pub async fn stats(&self) -> Result<UpdateStreamStats, UpdateStreamError> {
        let mut connection = self.connections.command_connection().await?;
        let length: usize = redis::cmd("XLEN")
            .arg(&self.key)
            .query_async(&mut connection)
            .await?;
        let groups: StreamInfoGroupsReply = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(&self.key)
            .query_async(&mut connection)
            .await?;
        let oldest: StreamRangeReply = redis::cmd("XRANGE")
            .arg(&self.key)
            .arg("-")
            .arg("+")
            .arg("COUNT")
            .arg(1)
            .query_async(&mut connection)
            .await?;

        let group = match groups
            .groups
            .into_iter()
            .find(|group| group.name == self.group)
        {
            Some(group) => Some(UpdateStreamGroupStats {
                consumers: group.consumers,
                pending: group.pending,
                lag: group.lag,
                last_delivered_id: group.last_delivered_id.parse()?,
            }),
            None => None,
        };
        let oldest_entry_id = oldest
            .ids
            .first()
            .map(|entry| entry.id.parse())
            .transpose()?;

        Ok(UpdateStreamStats {
            length,
            oldest_entry_id,
            group,
        })
    }

    /// Read INFO fields needed to alert on memory pressure and AOF health.
    pub async fn durability_stats(&self) -> Result<UpdateStreamDurabilityStats, UpdateStreamError> {
        let mut connection = self.connections.command_connection().await?;
        let memory: String = redis::cmd("INFO")
            .arg("memory")
            .query_async(&mut connection)
            .await?;
        let persistence: String = redis::cmd("INFO")
            .arg("persistence")
            .query_async(&mut connection)
            .await?;
        Ok(UpdateStreamDurabilityStats {
            used_memory_bytes: info_u64(&memory, "used_memory"),
            maxmemory_bytes: info_u64(&memory, "maxmemory"),
            maxmemory_policy: info_value(&memory, "maxmemory_policy")
                .unwrap_or_default()
                .to_owned(),
            aof_enabled: info_value(&persistence, "aof_enabled") == Some("1"),
            aof_current_size_bytes: info_u64(&persistence, "aof_current_size"),
            aof_rewrite_in_progress: info_value(&persistence, "aof_rewrite_in_progress")
                == Some("1"),
            aof_last_write_status: info_value(&persistence, "aof_last_write_status")
                .unwrap_or("unknown")
                .to_owned(),
            aof_last_rewrite_status: info_value(&persistence, "aof_last_bgrewrite_status")
                .unwrap_or("unknown")
                .to_owned(),
        })
    }
}

#[derive(Debug, Error)]
pub enum UpdateStreamError {
    #[error("update stream Redis operation failed: {0}")]
    Redis(#[from] redis::RedisError),
    #[error(transparent)]
    InvalidStreamId(#[from] UpdateStreamIdParseError),
    #[error("Redis returned non-binary field {field} for stream entry {stream_id}")]
    InvalidRedisField { stream_id: String, field: String },
}

#[must_use]
pub fn update_stream_key(bot_id: i64) -> String {
    format!("{DEFAULT_UPDATE_STREAM_KEY_PREFIX}:{bot_id}")
}

fn build_append_command(key: &str, update: &UpdateStreamAppend) -> redis::Cmd {
    let mut command = redis::cmd("XADD");
    let payload_sha256 = update.payload_sha256();
    command
        .arg(key)
        .arg("*")
        .arg(STREAM_FIELD_SCHEMA_VERSION)
        .arg(UPDATE_STREAM_SCHEMA_VERSION)
        .arg(STREAM_FIELD_BOT_ID)
        .arg(update.bot_id);
    if let Some(update_id) = update.update_id {
        command.arg(STREAM_FIELD_UPDATE_ID).arg(update_id);
    }
    command
        .arg(STREAM_FIELD_SOURCE)
        .arg(update.source.as_str())
        .arg(STREAM_FIELD_RECEIVED_AT)
        .arg(update.received_at_unix_ms)
        .arg(STREAM_FIELD_PAYLOAD)
        .arg(&update.raw_payload)
        .arg(STREAM_FIELD_PAYLOAD_SHA256)
        .arg(payload_sha256.as_slice());
    command
}

fn build_read_group_command(
    key: &str,
    group: &str,
    consumer: &str,
    block: Duration,
    max_count: usize,
) -> redis::Cmd {
    let mut command = redis::cmd("XREADGROUP");
    command
        .arg("GROUP")
        .arg(group)
        .arg(consumer)
        .arg("COUNT")
        .arg(max_count);
    if !block.is_zero() {
        command.arg("BLOCK").arg(duration_millis(block));
    }
    command.arg("STREAMS").arg(key).arg(">");
    command
}

fn raw_entries_from_read_reply(
    reply: Option<StreamReadReply>,
) -> Result<Vec<RawUpdateStreamEntry>, UpdateStreamError> {
    let stream_ids = reply
        .into_iter()
        .flat_map(|reply| reply.keys)
        .flat_map(|key| key.ids)
        .collect();
    raw_entries_from_stream_ids(stream_ids)
}

fn raw_entries_from_stream_ids(
    entries: Vec<redis::streams::StreamId>,
) -> Result<Vec<RawUpdateStreamEntry>, UpdateStreamError> {
    entries
        .into_iter()
        .map(|entry| {
            let stream_id = entry.id.parse()?;
            let fields = entry
                .map
                .into_iter()
                .map(|(field, value)| {
                    redis::from_redis_value::<Vec<u8>>(value)
                        .map(|value| (field.clone(), value))
                        .map_err(|_| UpdateStreamError::InvalidRedisField {
                            stream_id: entry.id.clone(),
                            field,
                        })
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            Ok(RawUpdateStreamEntry { stream_id, fields })
        })
        .collect()
}

fn required_field<'a>(
    fields: &'a BTreeMap<String, Vec<u8>>,
    field: &'static str,
) -> Result<&'a [u8], UpdateStreamEntryError> {
    fields
        .get(field)
        .map(Vec::as_slice)
        .ok_or(UpdateStreamEntryError::MissingField { field })
}

fn required_utf8_field<'a>(
    fields: &'a BTreeMap<String, Vec<u8>>,
    field: &'static str,
) -> Result<&'a str, UpdateStreamEntryError> {
    std::str::from_utf8(required_field(fields, field)?)
        .map_err(|_| UpdateStreamEntryError::InvalidField { field })
}

fn parse_required_field<T>(
    fields: &BTreeMap<String, Vec<u8>>,
    field: &'static str,
) -> Result<T, UpdateStreamEntryError>
where
    T: FromStr,
{
    required_utf8_field(fields, field)?
        .parse()
        .map_err(|_| UpdateStreamEntryError::InvalidField { field })
}

fn parse_optional_field<T>(
    fields: &BTreeMap<String, Vec<u8>>,
    field: &'static str,
) -> Result<Option<T>, UpdateStreamEntryError>
where
    T: FromStr,
{
    fields
        .get(field)
        .map(|value| {
            std::str::from_utf8(value)
                .map_err(|_| UpdateStreamEntryError::InvalidField { field })?
                .parse()
                .map_err(|_| UpdateStreamEntryError::InvalidField { field })
        })
        .transpose()
}

fn validate_range(
    field: &'static str,
    actual: u128,
    minimum: u128,
    maximum: u128,
) -> Result<(), UpdateStreamConfigError> {
    if (minimum..=maximum).contains(&actual) {
        return Ok(());
    }
    Err(UpdateStreamConfigError::OutOfRange {
        field,
        minimum,
        maximum,
        actual,
    })
}

fn duration_millis(duration: Duration) -> usize {
    let millis = duration.as_millis().max(1);
    match usize::try_from(millis) {
        Ok(millis) => millis,
        Err(_) => usize::MAX,
    }
}

fn info_value<'a>(info: &'a str, field: &str) -> Option<&'a str> {
    info.lines().find_map(|line| {
        let line = line.trim_end_matches('\r');
        let (name, value) = line.split_once(':')?;
        (name == field).then_some(value)
    })
}

fn info_u64(info: &str, field: &str) -> u64 {
    info_value(info, field)
        .and_then(|value| value.parse().ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        env,
        error::Error,
        io,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use super::{
        DEFAULT_UPDATE_MATERIALIZER_BATCH_MAX_BYTES, DEFAULT_UPDATE_MATERIALIZER_BATCH_MAX_ROWS,
        DEFAULT_UPDATE_MATERIALIZER_BATCH_MAX_WAIT, DEFAULT_UPDATE_MATERIALIZER_DB_TIMEOUT,
        MAX_UPDATE_MATERIALIZER_BATCH_ROWS, RawUpdateStreamEntry, RedisUpdateStream,
        STREAM_FIELD_BOT_ID, STREAM_FIELD_PAYLOAD, STREAM_FIELD_PAYLOAD_SHA256,
        STREAM_FIELD_RECEIVED_AT, STREAM_FIELD_SCHEMA_VERSION, STREAM_FIELD_SOURCE,
        STREAM_FIELD_UPDATE_ID, UPDATE_STREAM_SCHEMA_VERSION, UpdateStreamAppend,
        UpdateStreamConfigError, UpdateStreamId, UpdateStreamMaterializerConfig,
        UpdateStreamSource, build_append_command, info_u64, info_value, update_stream_key,
    };

    #[test]
    fn stream_key_is_partitioned_by_bot_identity() {
        assert_eq!(update_stream_key(12345), "plotva:updates:stream:v1:12345");
    }

    #[test]
    fn redis_info_parser_reads_memory_and_aof_fields() {
        let info = "# Memory\r\nused_memory:123\r\nmaxmemory:456\r\n\
                    maxmemory_policy:noeviction\r\naof_enabled:1\r\n";

        assert_eq!(info_u64(info, "used_memory"), 123);
        assert_eq!(info_u64(info, "maxmemory"), 456);
        assert_eq!(info_value(info, "maxmemory_policy"), Some("noeviction"));
        assert_eq!(info_value(info, "aof_enabled"), Some("1"));
    }

    #[test]
    fn materializer_config_has_bulk_defaults_and_bind_guard() -> Result<(), Box<dyn Error>> {
        let config = UpdateStreamMaterializerConfig::default().validate()?;

        assert_eq!(
            config.batch_max_rows,
            DEFAULT_UPDATE_MATERIALIZER_BATCH_MAX_ROWS
        );
        assert_eq!(
            config.batch_max_bytes,
            DEFAULT_UPDATE_MATERIALIZER_BATCH_MAX_BYTES
        );
        assert_eq!(
            config.batch_max_wait,
            DEFAULT_UPDATE_MATERIALIZER_BATCH_MAX_WAIT
        );
        assert_eq!(config.db_timeout, DEFAULT_UPDATE_MATERIALIZER_DB_TIMEOUT);
        assert_eq!(config.effective_max_rows(20)?, 512);
        assert_eq!(config.effective_max_rows(120)?, 500);
        assert_eq!(
            config.effective_max_rows(0),
            Err(UpdateStreamConfigError::ZeroBindParametersPerRow)
        );

        let invalid = UpdateStreamMaterializerConfig {
            batch_max_rows: MAX_UPDATE_MATERIALIZER_BATCH_ROWS + 1,
            ..config
        };
        assert!(invalid.validate().is_err());
        Ok(())
    }

    #[test]
    fn append_command_has_raw_envelope_and_no_trimming_policy() {
        let append = UpdateStreamAppend {
            bot_id: 42,
            update_id: Some(99),
            source: UpdateStreamSource::Webhook,
            received_at_unix_ms: 1_710_000_000_123,
            raw_payload: br#"{"update_id":99}"#.to_vec(),
        };

        let command = build_append_command("updates", &append).get_packed_command();
        let command = String::from_utf8_lossy(&command);

        assert!(command.contains("XADD"));
        assert!(command.contains("schema_version"));
        assert!(command.contains("payload_sha256"));
        assert!(command.contains("update_id"));
        assert!(!command.contains("MAXLEN"));
        assert!(!command.contains("MINID"));
    }

    #[test]
    fn raw_stream_entry_validates_hash_and_preserves_payload() -> Result<(), Box<dyn Error>> {
        let payload = br#"{"update_id":99}"#.to_vec();
        let append = UpdateStreamAppend {
            bot_id: 42,
            update_id: Some(99),
            source: UpdateStreamSource::LongPoll,
            received_at_unix_ms: 1_710_000_000_123,
            raw_payload: payload.clone(),
        };
        let fields = BTreeMap::from([
            (
                STREAM_FIELD_SCHEMA_VERSION.to_owned(),
                UPDATE_STREAM_SCHEMA_VERSION.to_string().into_bytes(),
            ),
            (STREAM_FIELD_BOT_ID.to_owned(), b"42".to_vec()),
            (STREAM_FIELD_UPDATE_ID.to_owned(), b"99".to_vec()),
            (STREAM_FIELD_SOURCE.to_owned(), b"long_poll".to_vec()),
            (
                STREAM_FIELD_RECEIVED_AT.to_owned(),
                b"1710000000123".to_vec(),
            ),
            (STREAM_FIELD_PAYLOAD.to_owned(), payload.clone()),
            (
                STREAM_FIELD_PAYLOAD_SHA256.to_owned(),
                append.payload_sha256().to_vec(),
            ),
        ]);
        let raw = RawUpdateStreamEntry {
            stream_id: UpdateStreamId {
                milliseconds: 1_710_000_000_124,
                sequence: 2,
            },
            fields,
        };

        let decoded = raw.decode()?;

        assert_eq!(decoded.bot_id, 42);
        assert_eq!(decoded.update_id, Some(99));
        assert_eq!(decoded.source, UpdateStreamSource::LongPoll);
        assert_eq!(decoded.raw_payload, payload);
        Ok(())
    }

    #[test]
    fn raw_stream_entry_reports_payload_hash_mismatch() {
        let fields = BTreeMap::from([
            (
                STREAM_FIELD_SCHEMA_VERSION.to_owned(),
                UPDATE_STREAM_SCHEMA_VERSION.to_string().into_bytes(),
            ),
            (STREAM_FIELD_BOT_ID.to_owned(), b"42".to_vec()),
            (STREAM_FIELD_SOURCE.to_owned(), b"webhook".to_vec()),
            (
                STREAM_FIELD_RECEIVED_AT.to_owned(),
                b"1710000000123".to_vec(),
            ),
            (STREAM_FIELD_PAYLOAD.to_owned(), b"payload".to_vec()),
            (STREAM_FIELD_PAYLOAD_SHA256.to_owned(), vec![0; 32]),
        ]);
        let raw = RawUpdateStreamEntry {
            stream_id: UpdateStreamId::MIN,
            fields,
        };

        assert!(raw.decode().is_err());
    }

    #[tokio::test]
    async fn live_redis_stream_round_trip_reclaim_and_atomic_ack_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };

        let client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:updates:stream:{suffix}");
        let group = format!("openplotva-test-materializer-{suffix}");
        let stream = RedisUpdateStream::with_key_and_group(client.clone(), key.clone(), group);
        let mut connection = client.get_multiplexed_async_connection().await?;
        let _: usize = redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut connection)
            .await?;

        assert!(stream.ensure_consumer_group().await?);
        assert!(!stream.ensure_consumer_group().await?);

        for update_id in [100_i64, 101_i64] {
            stream
                .append(&UpdateStreamAppend {
                    bot_id: 42,
                    update_id: Some(update_id),
                    source: UpdateStreamSource::Webhook,
                    received_at_unix_ms: 1_710_000_000_000 + update_id,
                    raw_payload: format!(r#"{{"update_id":{update_id}}}"#).into_bytes(),
                })
                .await?;
        }

        let read = stream.read_group("consumer-1", Duration::ZERO, 512).await?;
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].decode()?.update_id, Some(100));
        assert_eq!(read[1].decode()?.update_id, Some(101));

        let before = stream.stats().await?;
        assert_eq!(before.length, 2);
        let group = before
            .group
            .ok_or_else(|| io::Error::other("expected consumer group stats"))?;
        assert_eq!(group.pending, 2);

        let claimed = stream
            .reclaim_pending(
                "consumer-2",
                Duration::from_millis(1),
                UpdateStreamId::MIN,
                512,
            )
            .await?;
        assert_eq!(claimed.entries.len(), 2);
        let ids = claimed
            .entries
            .iter()
            .map(|entry| entry.stream_id)
            .collect::<Vec<_>>();
        let report = stream.acknowledge_and_delete(&ids).await?;
        assert_eq!(report.requested, 2);
        assert_eq!(report.acknowledged, 2);
        assert_eq!(report.deleted, 2);

        let after = stream.stats().await?;
        assert_eq!(after.length, 0);
        let group = after
            .group
            .ok_or_else(|| io::Error::other("expected consumer group stats"))?;
        assert_eq!(group.pending, 0);

        let _: usize = redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut connection)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_long_poll_batch_commits_stream_entries_and_cursor_together_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:updates:long-poll:{suffix}");
        let stream = RedisUpdateStream::with_key(client.clone(), key.clone());
        let mut connection = client.get_multiplexed_async_connection().await?;
        let _: usize = redis::cmd("DEL")
            .arg(&key)
            .arg(format!("{key}:long-poll-cursor"))
            .query_async(&mut connection)
            .await?;

        assert_eq!(stream.long_poll_cursor().await?, 0);
        let appends = [200_i64, 201_i64].map(|update_id| UpdateStreamAppend {
            bot_id: 42,
            update_id: Some(update_id),
            source: UpdateStreamSource::LongPoll,
            received_at_unix_ms: 1_710_000_000_000 + update_id,
            raw_payload: format!(r#"{{"update_id":{update_id}}}"#).into_bytes(),
        });
        let ids = stream.append_long_poll_batch(&appends, 202).await?;

        assert_eq!(ids.len(), 2);
        assert!(ids[0] < ids[1]);
        assert_eq!(stream.long_poll_cursor().await?, 202);
        assert_eq!(stream.stats().await?.length, 2);

        let dropped = stream.append_long_poll_batch(&[], 250).await?;
        assert!(dropped.is_empty());
        assert_eq!(stream.long_poll_cursor().await?, 250);
        assert_eq!(stream.stats().await?.length, 2);

        let _: usize = redis::cmd("DEL")
            .arg(&key)
            .arg(format!("{key}:long-poll-cursor"))
            .query_async(&mut connection)
            .await?;
        Ok(())
    }
}
