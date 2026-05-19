//! Storage boundary for Postgres, pgvector, SQLx, Redis, and Dragonfly.

use std::time::Duration;

use openplotva_config::{PostgresConfig, RedisConfig};
use openplotva_core::{ChatState, MessageIdMapping, PendingOp, UserState};
use redis::{
    Client as RedisClient, ConnectionAddr, ConnectionInfo, IntoConnectionInfo, RedisConnectionInfo,
};
use serde::{Deserialize, Serialize};
use sqlx::{
    PgPool, Row,
    migrate::{MigrateError, Migrator},
    postgres::{PgPoolOptions, PgRow},
};
use thiserror::Error;
use time::OffsetDateTime;

/// Human-readable crate purpose used by scaffold tests and docs.
pub const PURPOSE: &str = "storage";

const POSTGRES_MAX_CONNECTIONS: u32 = 50;
const POSTGRES_MIN_CONNECTIONS: u32 = 10;
const POSTGRES_MAX_CONNECTION_LIFETIME: Duration = Duration::from_secs(45 * 60);

/// Go `InsertVirtualMessage` SQL with SQLC name/comment removed.
pub const SQL_INSERT_VIRTUAL_MESSAGE: &str =
    "INSERT INTO message_id_map (vmsg_id, chat_id, thread_id) VALUES ($1, $2, $3)";

/// Go `UpsertUser` SQL narrowed to the state fields written by the update consumer.
pub const SQL_UPSERT_USER: &str = "INSERT INTO users (id, first_name, last_name, username, language_code, is_premium, settings) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb) ON CONFLICT (id) DO UPDATE SET first_name = COALESCE(EXCLUDED.first_name, users.first_name), last_name = COALESCE(EXCLUDED.last_name, users.last_name), username = COALESCE(EXCLUDED.username, users.username), language_code = COALESCE(EXCLUDED.language_code, users.language_code), is_premium = COALESCE(EXCLUDED.is_premium, users.is_premium), settings = COALESCE(EXCLUDED.settings, users.settings), updated = CURRENT_TIMESTAMP";

/// Go `UpsertChat` SQL for the full chat row shape, with non-state fields bound as null.
pub const SQL_UPSERT_CHAT: &str = "INSERT INTO chats (id, type, title, username, first_name, last_name, is_forum, active_usernames, available_reactions, bio, has_private_forwards, has_restricted_voice_and_video_messages, join_to_send_messages, join_by_request, description, invite_link, pinned_message, permissions, slow_mode_delay, message_auto_delete_time, has_aggressive_anti_spam_enabled, has_hidden_members, has_protected_content, has_visible_history, sticker_set_name, can_set_sticker_set, linked_chat_id, location) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10, $11, $12, $13, $14, $15, $16, $17::jsonb, $18::jsonb, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28::jsonb) ON CONFLICT (id) DO UPDATE SET type = COALESCE(EXCLUDED.type, chats.type), title = COALESCE(EXCLUDED.title, chats.title), username = COALESCE(EXCLUDED.username, chats.username), first_name = COALESCE(EXCLUDED.first_name, chats.first_name), last_name = COALESCE(EXCLUDED.last_name, chats.last_name), is_forum = COALESCE(EXCLUDED.is_forum, chats.is_forum), active_usernames = COALESCE(EXCLUDED.active_usernames, chats.active_usernames), available_reactions = COALESCE(EXCLUDED.available_reactions, chats.available_reactions), bio = COALESCE(EXCLUDED.bio, chats.bio), has_private_forwards = COALESCE(EXCLUDED.has_private_forwards, chats.has_private_forwards), has_restricted_voice_and_video_messages = COALESCE(EXCLUDED.has_restricted_voice_and_video_messages, chats.has_restricted_voice_and_video_messages), join_to_send_messages = COALESCE(EXCLUDED.join_to_send_messages, chats.join_to_send_messages), join_by_request = COALESCE(EXCLUDED.join_by_request, chats.join_by_request), description = COALESCE(EXCLUDED.description, chats.description), invite_link = COALESCE(EXCLUDED.invite_link, chats.invite_link), pinned_message = COALESCE(EXCLUDED.pinned_message, chats.pinned_message), permissions = COALESCE(EXCLUDED.permissions, chats.permissions), slow_mode_delay = COALESCE(EXCLUDED.slow_mode_delay, chats.slow_mode_delay), message_auto_delete_time = COALESCE(EXCLUDED.message_auto_delete_time, chats.message_auto_delete_time), has_aggressive_anti_spam_enabled = COALESCE(EXCLUDED.has_aggressive_anti_spam_enabled, chats.has_aggressive_anti_spam_enabled), has_hidden_members = COALESCE(EXCLUDED.has_hidden_members, chats.has_hidden_members), has_protected_content = COALESCE(EXCLUDED.has_protected_content, chats.has_protected_content), has_visible_history = COALESCE(EXCLUDED.has_visible_history, chats.has_visible_history), sticker_set_name = COALESCE(EXCLUDED.sticker_set_name, chats.sticker_set_name), can_set_sticker_set = COALESCE(EXCLUDED.can_set_sticker_set, chats.can_set_sticker_set), linked_chat_id = COALESCE(EXCLUDED.linked_chat_id, chats.linked_chat_id), location = COALESCE(EXCLUDED.location, chats.location), updated = CURRENT_TIMESTAMP";

/// Go `ResolveVirtualMessage` SQL with SQLC name/comment removed.
pub const SQL_RESOLVE_VIRTUAL_MESSAGE: &str = "UPDATE message_id_map SET real_message_id = $1, resolved_at = COALESCE($2, NOW()) WHERE vmsg_id = $3";

/// Go `GetMappingByVirtual` SQL narrowed to fields currently ported.
pub const SQL_GET_MAPPING_BY_VIRTUAL: &str =
    "SELECT vmsg_id, chat_id, thread_id, real_message_id FROM message_id_map WHERE vmsg_id = $1";

/// Go `ListMappingsByVirtualIDs` SQL narrowed to fields currently ported.
pub const SQL_LIST_MAPPINGS_BY_VIRTUAL_IDS: &str = "SELECT vmsg_id, chat_id, thread_id, real_message_id FROM message_id_map WHERE vmsg_id = ANY($1::text[])";

/// Go `GetMappingByReal` SQL narrowed to fields currently ported.
pub const SQL_GET_MAPPING_BY_REAL: &str = "SELECT vmsg_id, chat_id, thread_id, real_message_id FROM message_id_map WHERE chat_id = $1 AND real_message_id = $2";

/// Go `DeleteMappingByVirtual` SQL.
pub const SQL_DELETE_MAPPING_BY_VIRTUAL: &str = "DELETE FROM message_id_map WHERE vmsg_id = $1";

/// Go `EnqueueMessageOp` SQL with explicit JSONB cast for runtime binding.
pub const SQL_ENQUEUE_MESSAGE_OP: &str = "INSERT INTO message_ops_queue (vmsg_id, chat_id, op, payload) VALUES ($1, $2, $3, $4::jsonb) RETURNING id";

/// Go `ListPendingOps` SQL narrowed to fields currently ported.
pub const SQL_LIST_PENDING_OPS: &str = "SELECT id, vmsg_id, chat_id, op, COALESCE(payload::text, '') AS payload, attempts FROM message_ops_queue WHERE status = 'pending' ORDER BY created_at ASC LIMIT $1";

/// Go `MarkOpDone` SQL.
pub const SQL_MARK_OP_DONE: &str =
    "UPDATE message_ops_queue SET status = 'done', executed_at = NOW() WHERE id = $1";

/// Go `MarkOpFailed` SQL.
pub const SQL_MARK_OP_FAILED: &str =
    "UPDATE message_ops_queue SET attempts = attempts + 1, last_error = $2 WHERE id = $1";

/// Go Redis key prefix for persisted rate-limited chat expiry timestamps.
pub const RATE_LIMITED_CHAT_KEY_PREFIX: &str = "plotva:rate_limited_chat:";

/// SQLx migrator for the converted Go schema migrations.
pub static MIGRATOR: Migrator = sqlx::migrate!("../../migrations");

/// Connected service clients kept alive by the application shell.
#[derive(Clone, Debug)]
pub struct ServiceClients {
    /// SQLx Postgres pool.
    pub postgres: PgPool,
    /// Redis client that has passed a startup ping.
    pub redis: RedisStore,
    /// Whether SQLx migrations were applied during startup.
    pub migrations_applied: bool,
}

/// Redis client wrapper.
#[derive(Clone, Debug)]
pub struct RedisStore {
    client: RedisClient,
}

impl RedisStore {
    /// Access the underlying Redis client.
    pub fn client(&self) -> &RedisClient {
        &self.client
    }

    /// Build the Redis-backed rate-limit store over this client.
    pub fn rate_limit_store(&self) -> RedisRateLimitStore {
        RedisRateLimitStore::new(self.client.clone())
    }
}

/// Redis-backed store for Go server persisted chat rate-limit expiries.
#[derive(Clone, Debug)]
pub struct RedisRateLimitStore {
    client: RedisClient,
    key_prefix: String,
}

impl RedisRateLimitStore {
    /// Build a rate-limit store using Go's persisted key prefix.
    pub fn new(client: RedisClient) -> Self {
        Self::with_key_prefix(client, RATE_LIMITED_CHAT_KEY_PREFIX)
    }

    /// Build a rate-limit store with an explicit key prefix, useful for isolated tests.
    pub fn with_key_prefix(client: RedisClient, key_prefix: impl Into<String>) -> Self {
        Self {
            client,
            key_prefix: key_prefix.into(),
        }
    }

    /// Return the Redis key this store uses for one chat.
    pub fn key_for_chat(&self, chat_id: i64) -> String {
        format!("{}{chat_id}", self.key_prefix)
    }

    /// Persist one chat's rate-limit expiry with the Go retry duration as Redis TTL.
    pub async fn set_chat_rate_limit(
        &self,
        chat_id: i64,
        expiry: OffsetDateTime,
        ttl: Duration,
    ) -> Result<(), StorageError> {
        let value = rate_limit_expiry_redis_value(expiry)?;
        let mut connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let mut command = redis::cmd("SET");
        command.arg(self.key_for_chat(chat_id)).arg(value);
        if !ttl.is_zero() {
            command.arg("PX").arg(redis_ttl_millis(ttl));
        }
        let _: String = command
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(())
    }

    /// Load one chat's persisted rate-limit expiry.
    pub async fn chat_rate_limit_expiry(
        &self,
        chat_id: i64,
    ) -> Result<Option<OffsetDateTime>, StorageError> {
        let mut connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(self.key_for_chat(chat_id))
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        value
            .as_deref()
            .map(rate_limit_expiry_from_redis_value)
            .transpose()
    }

    /// Return whether a chat is still rate-limited at `now`.
    pub async fn chat_is_rate_limited_at(
        &self,
        chat_id: i64,
        now: OffsetDateTime,
    ) -> Result<bool, StorageError> {
        let expiry = self.chat_rate_limit_expiry(chat_id).await?;
        Ok(rate_limit_is_active_at(expiry, now))
    }
}

/// SQLx-backed storage for Go virtual message mappings and pending message ops.
#[derive(Clone, Debug)]
pub struct PostgresVirtualMessageStore {
    pool: PgPool,
}

impl PostgresVirtualMessageStore {
    /// Build a virtual-message store on an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Access the underlying Postgres pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Upsert user state using Go `UpsertUser` semantics.
    pub async fn upsert_user_state(&self, user: &UserState) -> Result<(), StorageError> {
        sqlx::query(SQL_UPSERT_USER)
            .bind(user.id)
            .bind(&user.first_name)
            .bind(user.last_name.as_deref())
            .bind(user.username.as_deref())
            .bind(user.language_code.as_deref())
            .bind(user.is_premium)
            .bind(Option::<&str>::None)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Upsert chat state using Go `UpsertChat` semantics.
    pub async fn upsert_chat_state(&self, chat: &ChatState) -> Result<(), StorageError> {
        sqlx::query(SQL_UPSERT_CHAT)
            .bind(chat.id)
            .bind(&chat.chat_type)
            .bind(chat.title.as_deref())
            .bind(chat.username.as_deref())
            .bind(chat.first_name.as_deref())
            .bind(chat.last_name.as_deref())
            .bind(chat.is_forum)
            .bind(Option::<&str>::None)
            .bind(Option::<&str>::None)
            .bind(Option::<&str>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<&str>::None)
            .bind(Option::<&str>::None)
            .bind(Option::<&str>::None)
            .bind(Option::<&str>::None)
            .bind(Option::<i64>::None)
            .bind(Option::<i64>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<&str>::None)
            .bind(Option::<bool>::None)
            .bind(Option::<i64>::None)
            .bind(Option::<&str>::None)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Insert a Go `message_id_map` virtual message row.
    pub async fn insert_virtual_message(
        &self,
        vmsg_id: &str,
        chat_id: i64,
        thread_id: Option<i32>,
    ) -> Result<(), StorageError> {
        sqlx::query(SQL_INSERT_VIRTUAL_MESSAGE)
            .bind(vmsg_id)
            .bind(chat_id)
            .bind(thread_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Resolve a virtual message to its real Telegram message ID.
    pub async fn resolve_virtual_message(
        &self,
        vmsg_id: &str,
        real_message_id: i32,
        resolved_at: Option<OffsetDateTime>,
    ) -> Result<(), StorageError> {
        sqlx::query(SQL_RESOLVE_VIRTUAL_MESSAGE)
            .bind(real_message_id)
            .bind(resolved_at)
            .bind(vmsg_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Get a virtual-message mapping by virtual ID.
    pub async fn get_mapping_by_virtual(
        &self,
        vmsg_id: &str,
    ) -> Result<Option<MessageIdMapping>, StorageError> {
        let row = sqlx::query(SQL_GET_MAPPING_BY_VIRTUAL)
            .bind(vmsg_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(message_id_mapping_from_row).transpose()?)
    }

    /// List virtual-message mappings by virtual IDs.
    pub async fn list_mappings_by_virtual_ids(
        &self,
        vmsg_ids: &[String],
    ) -> Result<Vec<MessageIdMapping>, StorageError> {
        let rows = sqlx::query(SQL_LIST_MAPPINGS_BY_VIRTUAL_IDS)
            .bind(vmsg_ids)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(message_id_mapping_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    /// Get a virtual-message mapping by real Telegram message ID.
    pub async fn get_mapping_by_real(
        &self,
        chat_id: i64,
        real_message_id: i32,
    ) -> Result<Option<MessageIdMapping>, StorageError> {
        let row = sqlx::query(SQL_GET_MAPPING_BY_REAL)
            .bind(chat_id)
            .bind(real_message_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(message_id_mapping_from_row).transpose()?)
    }

    /// Delete a virtual-message mapping by virtual ID.
    pub async fn delete_mapping_by_virtual(&self, vmsg_id: &str) -> Result<(), StorageError> {
        sqlx::query(SQL_DELETE_MAPPING_BY_VIRTUAL)
            .bind(vmsg_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Enqueue a pending operation for a virtual message.
    pub async fn enqueue_message_op(
        &self,
        vmsg_id: &str,
        chat_id: i64,
        op: &str,
        payload_json: Option<&str>,
    ) -> Result<i64, StorageError> {
        let row = sqlx::query(SQL_ENQUEUE_MESSAGE_OP)
            .bind(vmsg_id)
            .bind(chat_id)
            .bind(op)
            .bind(payload_json)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.try_get::<i64, _>("id")?)
    }

    /// List pending virtual-message operations in Go execution order.
    pub async fn list_pending_ops(&self, limit: i32) -> Result<Vec<PendingOp>, StorageError> {
        let rows = sqlx::query(SQL_LIST_PENDING_OPS)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(pending_op_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    /// Mark a pending operation as done.
    pub async fn mark_op_done(&self, id: i64) -> Result<(), StorageError> {
        sqlx::query(SQL_MARK_OP_DONE)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Record a failed pending operation attempt.
    pub async fn mark_op_failed(&self, id: i64, message: &str) -> Result<(), StorageError> {
        sqlx::query(SQL_MARK_OP_FAILED)
            .bind(id)
            .bind(message)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

/// Storage connection failures.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Postgres connection or ping failed.
    #[error("failed to connect to Postgres: {source}")]
    Postgres {
        /// SQLx error.
        #[from]
        source: sqlx::Error,
    },
    /// Redis client configuration failed.
    #[error("invalid Redis configuration: {source}")]
    RedisConfig {
        /// Redis configuration error.
        source: redis::RedisError,
    },
    /// Redis connection or ping failed.
    #[error("failed to connect to Redis: {source}")]
    Redis {
        /// Redis connection error.
        source: redis::RedisError,
    },
    /// SQLx migration execution failed.
    #[error("failed to apply SQL migrations: {source}")]
    Migrate {
        /// SQLx migration error.
        #[from]
        source: MigrateError,
    },
    /// Rate-limit expiry JSON codec failed.
    #[error("decode rate limit expiry: {source}")]
    RateLimitCodec {
        /// JSON codec error.
        source: serde_json::Error,
    },
    /// Rate-limit expiry timestamp could not be represented.
    #[error("invalid rate limit expiry timestamp: {source}")]
    RateLimitTimestamp {
        /// Timestamp range error.
        source: time::error::ComponentRange,
    },
}

/// Build the persisted Go rate-limited-chat key for a chat.
pub fn rate_limited_chat_key(chat_id: i64) -> String {
    format!("{RATE_LIMITED_CHAT_KEY_PREFIX}{chat_id}")
}

/// Encode a rate-limit expiry as the approved Rust-native Redis JSON value.
pub fn rate_limit_expiry_redis_value(expiry: OffsetDateTime) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(&RateLimitExpiryValue {
        unix_timestamp_nanos: expiry.unix_timestamp_nanos(),
    })
    .map_err(|source| StorageError::RateLimitCodec { source })
}

/// Decode a Rust-native rate-limit expiry Redis value.
pub fn rate_limit_expiry_from_redis_value(value: &[u8]) -> Result<OffsetDateTime, StorageError> {
    let value: RateLimitExpiryValue =
        serde_json::from_slice(value).map_err(|source| StorageError::RateLimitCodec { source })?;
    OffsetDateTime::from_unix_timestamp_nanos(value.unix_timestamp_nanos)
        .map_err(|source| StorageError::RateLimitTimestamp { source })
}

/// Return whether the loaded expiry is still active using Go's strict `time.Before` boundary.
pub fn rate_limit_is_active_at(expiry: Option<OffsetDateTime>, now: OffsetDateTime) -> bool {
    expiry.is_some_and(|expiry| now < expiry)
}

#[derive(Debug, Deserialize, Serialize)]
struct RateLimitExpiryValue {
    unix_timestamp_nanos: i128,
}

fn redis_ttl_millis(ttl: Duration) -> u64 {
    let millis = ttl.as_millis();
    if millis == 0 {
        return 1;
    }
    u64::try_from(millis).unwrap_or(u64::MAX)
}

/// Connect to Postgres and Redis using the current service-spine settings.
pub async fn connect_service_clients(
    postgres: &PostgresConfig,
    redis: &RedisConfig,
    run_migrations: bool,
) -> Result<ServiceClients, StorageError> {
    let postgres = connect_postgres(postgres).await?;
    if run_migrations {
        run_migrations_on(&postgres).await?;
    }
    let redis = connect_redis(redis).await?;

    Ok(ServiceClients {
        postgres,
        redis,
        migrations_applied: run_migrations,
    })
}

pub async fn connect_postgres(config: &PostgresConfig) -> Result<PgPool, StorageError> {
    PgPoolOptions::new()
        .max_connections(POSTGRES_MAX_CONNECTIONS)
        .min_connections(POSTGRES_MIN_CONNECTIONS)
        .max_lifetime(POSTGRES_MAX_CONNECTION_LIFETIME)
        .connect(&config.go_startup_dsn())
        .await
        .map_err(StorageError::from)
}

/// Apply all pending SQLx migrations to an already connected Postgres pool.
pub async fn run_migrations_on(pool: &PgPool) -> Result<(), StorageError> {
    MIGRATOR.run(pool).await.map_err(StorageError::from)
}

/// Connect to Redis/Dragonfly and verify the selected DB with `PING`.
pub async fn connect_redis(config: &RedisConfig) -> Result<RedisStore, StorageError> {
    let client = RedisClient::open(
        redis_connection_info(config).map_err(|source| StorageError::RedisConfig { source })?,
    )
    .map_err(|source| StorageError::RedisConfig { source })?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|source| StorageError::Redis { source })?;
    let _: String = redis::cmd("PING")
        .query_async(&mut connection)
        .await
        .map_err(|source| StorageError::Redis { source })?;

    Ok(RedisStore { client })
}

fn redis_connection_info(config: &RedisConfig) -> redis::RedisResult<ConnectionInfo> {
    let redis_settings = if config.password.is_empty() {
        RedisConnectionInfo::default().set_db(config.db)
    } else {
        RedisConnectionInfo::default()
            .set_db(config.db)
            .set_password(&config.password)
    };

    ConnectionAddr::Tcp(config.host.clone(), config.port)
        .into_connection_info()
        .map(|info| info.set_redis_settings(redis_settings))
}

fn message_id_mapping_from_row(row: PgRow) -> Result<MessageIdMapping, sqlx::Error> {
    Ok(MessageIdMapping {
        vmsg_id: row.try_get("vmsg_id")?,
        chat_id: row.try_get("chat_id")?,
        thread_id: row.try_get("thread_id")?,
        real_message_id: row.try_get("real_message_id")?,
    })
}

fn pending_op_from_row(row: PgRow) -> Result<PendingOp, sqlx::Error> {
    let payload: String = row.try_get("payload")?;
    Ok(PendingOp {
        id: row.try_get("id")?,
        vmsg_id: row.try_get("vmsg_id")?,
        chat_id: row.try_get("chat_id")?,
        op: row.try_get("op")?,
        payload: payload.into_bytes(),
        attempts: row.try_get("attempts")?,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        error::Error,
        fs,
        path::Path,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use openplotva_config::{DEFAULT_REDIS_DB, RedisConfig};
    use redis::ConnectionAddr;
    use serde::Deserialize;
    use sqlx::postgres::PgPoolOptions;

    #[derive(Deserialize)]
    struct MigrationInventoryEntry {
        path: String,
        sha256: String,
    }

    #[test]
    fn converted_migration_corpus_is_embedded() {
        assert_eq!(super::MIGRATOR.iter().count(), 96);
    }

    #[test]
    fn converted_migrations_reference_frozen_go_inventory() -> Result<(), Box<dyn Error>> {
        let inventory = include_str!("../../../docs/contract/generated/migrations.json");
        let entries: Vec<MigrationInventoryEntry> = serde_json::from_str(inventory)?;
        assert_eq!(entries.len(), 48);

        for entry in entries {
            let source_file = Path::new(&entry.path)
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or("migration inventory path has no file name")?;
            let stem = source_file
                .strip_suffix(".sql")
                .ok_or("migration inventory path is not a SQL file")?;
            let up_path = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join(format!("../../migrations/{stem}.up.sql"));
            let down_path = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join(format!("../../migrations/{stem}.down.sql"));

            let up = fs::read_to_string(up_path)?;
            let down = fs::read_to_string(down_path)?;
            let source_header =
                format!("-- Converted from reference-app/internal/db/sql/migrations/{source_file}");
            let hash_header = format!("-- Source SHA-256: {}", entry.sha256);

            assert!(up.contains(&source_header));
            assert!(up.contains(&hash_header));
            assert!(down.contains(&source_header));
            assert!(down.contains(&hash_header));
            assert!(!up.contains("+migrate"));
            assert!(!down.contains("+migrate"));
        }

        Ok(())
    }

    #[test]
    fn redis_connection_info_matches_go_cache_defaults() -> Result<(), redis::RedisError> {
        let config = RedisConfig {
            host: "127.0.0.1".to_owned(),
            port: 6379,
            password: String::new(),
            db: DEFAULT_REDIS_DB,
        };

        let info = super::redis_connection_info(&config)?;

        assert_eq!(
            info.addr(),
            &ConnectionAddr::Tcp("127.0.0.1".to_owned(), 6379)
        );
        assert_eq!(info.redis_settings().db(), 0);
        assert_eq!(info.redis_settings().password(), None);

        Ok(())
    }

    #[test]
    fn rate_limit_storage_keys_and_codec_preserve_go_policy_contract() -> Result<(), Box<dyn Error>>
    {
        let expiry = time::OffsetDateTime::from_unix_timestamp_nanos(1_710_000_000_123_456_789)?;

        assert_eq!(
            super::rate_limited_chat_key(42),
            "plotva:rate_limited_chat:42"
        );
        assert_eq!(
            super::rate_limit_expiry_redis_value(expiry)?,
            br#"{"unix_timestamp_nanos":1710000000123456789}"#
        );
        assert_eq!(
            super::rate_limit_expiry_from_redis_value(
                br#"{"unix_timestamp_nanos":1710000000123456789}"#
            )?,
            expiry
        );

        Ok(())
    }

    #[test]
    fn rate_limit_active_check_uses_go_strict_before_boundary() -> Result<(), Box<dyn Error>> {
        let now = time::OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let future = now + time::Duration::seconds(1);
        let past = now - time::Duration::seconds(1);

        assert!(super::rate_limit_is_active_at(Some(future), now));
        assert!(!super::rate_limit_is_active_at(Some(now), now));
        assert!(!super::rate_limit_is_active_at(Some(past), now));
        assert!(!super::rate_limit_is_active_at(None, now));

        Ok(())
    }

    #[test]
    fn rate_limit_codec_rejects_legacy_gob_wrapped_values() {
        let error =
            super::rate_limit_expiry_from_redis_value(&[0xff, 0xb4, 0x0a, 0x00, 0xff, 0xb0])
                .expect_err("legacy gob values should be rejected after the approved cutover");

        assert!(error.to_string().contains("decode rate limit expiry"));
    }

    #[test]
    fn virtual_message_sql_matches_go_query_contracts() {
        let _mapping = openplotva_core::MessageIdMapping::resolved("v1", 42, 77);

        assert_eq!(
            super::SQL_INSERT_VIRTUAL_MESSAGE,
            "INSERT INTO message_id_map (vmsg_id, chat_id, thread_id) VALUES ($1, $2, $3)"
        );
        assert_eq!(
            super::SQL_RESOLVE_VIRTUAL_MESSAGE,
            "UPDATE message_id_map SET real_message_id = $1, resolved_at = COALESCE($2, NOW()) WHERE vmsg_id = $3"
        );
        assert_eq!(
            super::SQL_GET_MAPPING_BY_VIRTUAL,
            "SELECT vmsg_id, chat_id, thread_id, real_message_id FROM message_id_map WHERE vmsg_id = $1"
        );
        assert_eq!(
            super::SQL_LIST_MAPPINGS_BY_VIRTUAL_IDS,
            "SELECT vmsg_id, chat_id, thread_id, real_message_id FROM message_id_map WHERE vmsg_id = ANY($1::text[])"
        );
        assert_eq!(
            super::SQL_GET_MAPPING_BY_REAL,
            "SELECT vmsg_id, chat_id, thread_id, real_message_id FROM message_id_map WHERE chat_id = $1 AND real_message_id = $2"
        );
        assert_eq!(
            super::SQL_DELETE_MAPPING_BY_VIRTUAL,
            "DELETE FROM message_id_map WHERE vmsg_id = $1"
        );
    }

    #[test]
    fn pending_message_op_sql_matches_go_query_contracts() {
        let _op = openplotva_core::PendingOp::new(1, "v1", 42, "edit");

        assert_eq!(
            super::SQL_ENQUEUE_MESSAGE_OP,
            "INSERT INTO message_ops_queue (vmsg_id, chat_id, op, payload) VALUES ($1, $2, $3, $4::jsonb) RETURNING id"
        );
        assert_eq!(
            super::SQL_LIST_PENDING_OPS,
            "SELECT id, vmsg_id, chat_id, op, COALESCE(payload::text, '') AS payload, attempts FROM message_ops_queue WHERE status = 'pending' ORDER BY created_at ASC LIMIT $1"
        );
        assert_eq!(
            super::SQL_MARK_OP_DONE,
            "UPDATE message_ops_queue SET status = 'done', executed_at = NOW() WHERE id = $1"
        );
        assert_eq!(
            super::SQL_MARK_OP_FAILED,
            "UPDATE message_ops_queue SET attempts = attempts + 1, last_error = $2 WHERE id = $1"
        );
    }

    #[test]
    fn user_and_chat_state_sql_matches_go_query_contracts() {
        let _user = openplotva_core::UserState {
            id: 500,
            first_name: "Carol".to_owned(),
            last_name: Some(String::new()),
            username: Some("carol".to_owned()),
            language_code: Some(" ru ".to_owned()),
            is_premium: Some(true),
        };
        let _chat = openplotva_core::ChatState {
            id: -300,
            chat_type: "private".to_owned(),
            title: Some(" Team ".to_owned()),
            username: None,
            first_name: Some("Alice".to_owned()),
            last_name: None,
            is_forum: Some(true),
        };

        assert_eq!(
            super::SQL_UPSERT_USER,
            "INSERT INTO users (id, first_name, last_name, username, language_code, is_premium, settings) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb) ON CONFLICT (id) DO UPDATE SET first_name = COALESCE(EXCLUDED.first_name, users.first_name), last_name = COALESCE(EXCLUDED.last_name, users.last_name), username = COALESCE(EXCLUDED.username, users.username), language_code = COALESCE(EXCLUDED.language_code, users.language_code), is_premium = COALESCE(EXCLUDED.is_premium, users.is_premium), settings = COALESCE(EXCLUDED.settings, users.settings), updated = CURRENT_TIMESTAMP"
        );
        assert_eq!(
            super::SQL_UPSERT_CHAT,
            "INSERT INTO chats (id, type, title, username, first_name, last_name, is_forum, active_usernames, available_reactions, bio, has_private_forwards, has_restricted_voice_and_video_messages, join_to_send_messages, join_by_request, description, invite_link, pinned_message, permissions, slow_mode_delay, message_auto_delete_time, has_aggressive_anti_spam_enabled, has_hidden_members, has_protected_content, has_visible_history, sticker_set_name, can_set_sticker_set, linked_chat_id, location) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10, $11, $12, $13, $14, $15, $16, $17::jsonb, $18::jsonb, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28::jsonb) ON CONFLICT (id) DO UPDATE SET type = COALESCE(EXCLUDED.type, chats.type), title = COALESCE(EXCLUDED.title, chats.title), username = COALESCE(EXCLUDED.username, chats.username), first_name = COALESCE(EXCLUDED.first_name, chats.first_name), last_name = COALESCE(EXCLUDED.last_name, chats.last_name), is_forum = COALESCE(EXCLUDED.is_forum, chats.is_forum), active_usernames = COALESCE(EXCLUDED.active_usernames, chats.active_usernames), available_reactions = COALESCE(EXCLUDED.available_reactions, chats.available_reactions), bio = COALESCE(EXCLUDED.bio, chats.bio), has_private_forwards = COALESCE(EXCLUDED.has_private_forwards, chats.has_private_forwards), has_restricted_voice_and_video_messages = COALESCE(EXCLUDED.has_restricted_voice_and_video_messages, chats.has_restricted_voice_and_video_messages), join_to_send_messages = COALESCE(EXCLUDED.join_to_send_messages, chats.join_to_send_messages), join_by_request = COALESCE(EXCLUDED.join_by_request, chats.join_by_request), description = COALESCE(EXCLUDED.description, chats.description), invite_link = COALESCE(EXCLUDED.invite_link, chats.invite_link), pinned_message = COALESCE(EXCLUDED.pinned_message, chats.pinned_message), permissions = COALESCE(EXCLUDED.permissions, chats.permissions), slow_mode_delay = COALESCE(EXCLUDED.slow_mode_delay, chats.slow_mode_delay), message_auto_delete_time = COALESCE(EXCLUDED.message_auto_delete_time, chats.message_auto_delete_time), has_aggressive_anti_spam_enabled = COALESCE(EXCLUDED.has_aggressive_anti_spam_enabled, chats.has_aggressive_anti_spam_enabled), has_hidden_members = COALESCE(EXCLUDED.has_hidden_members, chats.has_hidden_members), has_protected_content = COALESCE(EXCLUDED.has_protected_content, chats.has_protected_content), has_visible_history = COALESCE(EXCLUDED.has_visible_history, chats.has_visible_history), sticker_set_name = COALESCE(EXCLUDED.sticker_set_name, chats.sticker_set_name), can_set_sticker_set = COALESCE(EXCLUDED.can_set_sticker_set, chats.can_set_sticker_set), linked_chat_id = COALESCE(EXCLUDED.linked_chat_id, chats.linked_chat_id), location = COALESCE(EXCLUDED.location, chats.location), updated = CURRENT_TIMESTAMP"
        );
    }

    #[tokio::test]
    async fn live_virtual_message_store_round_trips_when_postgres_dsn_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        let store = super::PostgresVirtualMessageStore::new(pool);
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let vmsg_id = format!("t{suffix}");
        let chat_id = -9_001_234_567_890_i64;
        let real_message_id = 321_987_i32;
        let _ = store.delete_mapping_by_virtual(&vmsg_id).await;

        let result: Result<(), Box<dyn Error>> = async {
            store
                .insert_virtual_message(&vmsg_id, chat_id, Some(77))
                .await?;
            let mapping = store
                .get_mapping_by_virtual(&vmsg_id)
                .await?
                .ok_or_else(|| std::io::Error::other("inserted mapping was not readable"))?;
            assert_eq!(mapping.vmsg_id, vmsg_id);
            assert_eq!(mapping.chat_id, chat_id);
            assert_eq!(mapping.thread_id, Some(77));
            assert_eq!(mapping.real_message_id, None);

            let op_id = store
                .enqueue_message_op(
                    &vmsg_id,
                    chat_id,
                    "edit",
                    Some(r#"{"text":"edited","parse_mode":"HTML"}"#),
                )
                .await?;
            let pending = store.list_pending_ops(1_000).await?;
            let op = pending
                .iter()
                .find(|row| row.id == op_id)
                .ok_or_else(|| std::io::Error::other("enqueued op was not listed as pending"))?;
            assert_eq!(op.vmsg_id, vmsg_id);
            assert_eq!(op.chat_id, chat_id);
            assert_eq!(op.op, "edit");
            let payload: serde_json::Value = serde_json::from_slice(&op.payload)?;
            assert_eq!(
                payload,
                serde_json::json!({"text": "edited", "parse_mode": "HTML"})
            );
            assert_eq!(op.attempts, 0);

            store.mark_op_failed(op_id, "temporary failure").await?;
            let pending = store.list_pending_ops(1_000).await?;
            let op = pending
                .iter()
                .find(|row| row.id == op_id)
                .ok_or_else(|| std::io::Error::other("failed op was not still pending"))?;
            assert_eq!(op.attempts, 1);

            store
                .resolve_virtual_message(&vmsg_id, real_message_id, None)
                .await?;
            let mapping = store
                .get_mapping_by_real(chat_id, real_message_id)
                .await?
                .ok_or_else(|| std::io::Error::other("resolved mapping was not readable"))?;
            assert_eq!(mapping.vmsg_id, vmsg_id);
            assert_eq!(mapping.real_message_id, Some(real_message_id));

            store.mark_op_done(op_id).await?;
            let pending = store.list_pending_ops(1_000).await?;
            assert!(pending.iter().all(|row| row.id != op_id));

            Ok(())
        }
        .await;

        let cleanup = store.delete_mapping_by_virtual(&vmsg_id).await;
        result?;
        cleanup?;
        assert!(store.get_mapping_by_virtual(&vmsg_id).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn live_redis_rate_limit_store_round_trips_when_url_is_set() -> Result<(), Box<dyn Error>>
    {
        let Ok(url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let client = redis::Client::open(url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let prefix = format!("openplotva:test:rate_limited_chat:{suffix}:");
        let store = super::RedisRateLimitStore::with_key_prefix(client.clone(), prefix);
        let chat_id = -900_123_i64;
        let now = time::OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let expiry = now + time::Duration::seconds(30);
        let mut connection = client.get_multiplexed_async_connection().await?;

        let result: Result<(), Box<dyn Error>> = async {
            store
                .set_chat_rate_limit(chat_id, expiry, Duration::from_secs(30))
                .await?;

            assert_eq!(store.chat_rate_limit_expiry(chat_id).await?, Some(expiry));
            assert!(store.chat_is_rate_limited_at(chat_id, now).await?);
            assert!(!store.chat_is_rate_limited_at(chat_id, expiry).await?);

            Ok(())
        }
        .await;

        let _: i64 = redis::cmd("DEL")
            .arg(store.key_for_chat(chat_id))
            .query_async(&mut connection)
            .await?;
        result
    }
}
