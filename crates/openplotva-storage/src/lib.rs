//! Storage boundary for Postgres, pgvector, SQLx, Redis, and Dragonfly.

use std::time::Duration;

use openplotva_config::{PostgresConfig, RedisConfig};
use openplotva_core::{
    ChatMessageMeta, ChatSettings, ChatSettingsUpdate, ChatState, MessageIdMapping, PendingOp,
    UserState,
};
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

/// Go `GetUserByUsername` narrowed to the ID needed by admin VIP commands.
pub const SQL_GET_USER_ID_BY_USERNAME: &str = "SELECT id FROM users WHERE username = $1 LIMIT 1";

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

/// Go `GetChatInfo` narrowed to the chat type needed by permission policy.
pub const SQL_GET_CHAT_TYPE: &str = "SELECT type FROM chats WHERE id = $1";

/// Go `GetChatMember` SQL narrowed to the full row shape used by Rust helpers.
pub const SQL_GET_CHAT_MEMBER: &str =
    "SELECT * FROM chat_members WHERE chat_id = $1 AND user_id = $2";

/// Go `ListChatMembers` SQL narrowed to one chat.
pub const SQL_LIST_CHAT_MEMBERS: &str = "SELECT * FROM chat_members WHERE chat_id = $1";

/// Go `UpsertChatMember` SQL with SQLC bindings converted to positional bindings.
pub const SQL_UPSERT_CHAT_MEMBER: &str = "INSERT INTO chat_members (chat_id, user_id, status, is_anonymous, custom_title, can_be_edited, can_manage_chat, can_delete_messages, can_manage_video_chats, can_restrict_members, can_promote_members, can_change_info, can_invite_users, can_post_messages, can_edit_messages, can_pin_messages, can_manage_topics, can_send_messages, can_send_media_messages, can_send_polls, can_send_other_messages, can_add_web_page_previews, until_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23) ON CONFLICT (chat_id, user_id) DO UPDATE SET status = COALESCE(EXCLUDED.status, chat_members.status), is_anonymous = COALESCE(EXCLUDED.is_anonymous, chat_members.is_anonymous), custom_title = COALESCE(EXCLUDED.custom_title, chat_members.custom_title), can_be_edited = COALESCE(EXCLUDED.can_be_edited, chat_members.can_be_edited), can_manage_chat = COALESCE(EXCLUDED.can_manage_chat, chat_members.can_manage_chat), can_delete_messages = COALESCE(EXCLUDED.can_delete_messages, chat_members.can_delete_messages), can_manage_video_chats = COALESCE(EXCLUDED.can_manage_video_chats, chat_members.can_manage_video_chats), can_restrict_members = COALESCE(EXCLUDED.can_restrict_members, chat_members.can_restrict_members), can_promote_members = COALESCE(EXCLUDED.can_promote_members, chat_members.can_promote_members), can_change_info = COALESCE(EXCLUDED.can_change_info, chat_members.can_change_info), can_invite_users = COALESCE(EXCLUDED.can_invite_users, chat_members.can_invite_users), can_post_messages = COALESCE(EXCLUDED.can_post_messages, chat_members.can_post_messages), can_edit_messages = COALESCE(EXCLUDED.can_edit_messages, chat_members.can_edit_messages), can_pin_messages = COALESCE(EXCLUDED.can_pin_messages, chat_members.can_pin_messages), can_manage_topics = COALESCE(EXCLUDED.can_manage_topics, chat_members.can_manage_topics), can_send_messages = COALESCE(EXCLUDED.can_send_messages, chat_members.can_send_messages), can_send_media_messages = COALESCE(EXCLUDED.can_send_media_messages, chat_members.can_send_media_messages), can_send_polls = COALESCE(EXCLUDED.can_send_polls, chat_members.can_send_polls), can_send_other_messages = COALESCE(EXCLUDED.can_send_other_messages, chat_members.can_send_other_messages), can_add_web_page_previews = COALESCE(EXCLUDED.can_add_web_page_previews, chat_members.can_add_web_page_previews), until_date = COALESCE(EXCLUDED.until_date, chat_members.until_date), updated_at = CURRENT_TIMESTAMP";

/// Go `GetChatSettings` SQL narrowed to fields currently needed by permission policy.
pub const SQL_GET_CHAT_SETTINGS: &str = "SELECT chat_id, mood_alignment, custom_persona, reactivity_percentage, proactivity_percentage, enable_global_text_reply, enable_global_draw_reply, enable_obscenifier, enable_profanity, enable_greet_joiners, enable_daily_game, daily_game_theme, greeting_html FROM chat_settings WHERE chat_id = $1";

/// Go `UpsertChatSettings` SQL with positional bindings for the permission update shape.
pub const SQL_UPSERT_CHAT_SETTINGS: &str = "WITH ensure_chat AS (INSERT INTO chats (id, type) VALUES ($1, COALESCE(NULLIF($14::text, ''), 'private')) ON CONFLICT (id) DO NOTHING) INSERT INTO chat_settings (chat_id, mood_alignment, custom_persona, reactivity_percentage, proactivity_percentage, enable_obscenifier, enable_profanity, enable_greet_joiners, enable_global_text_reply, enable_global_draw_reply, enable_daily_game, daily_game_theme, updated, greeting_html) VALUES ($1, $2, $3, $4, $5, COALESCE($6, TRUE)::boolean, COALESCE($7, TRUE)::boolean, COALESCE($8, FALSE)::boolean, COALESCE($9, TRUE)::boolean, COALESCE($10, TRUE)::boolean, COALESCE($11, TRUE)::boolean, COALESCE($12, 'auto')::text, CURRENT_TIMESTAMP, $13) ON CONFLICT (chat_id) DO UPDATE SET mood_alignment = EXCLUDED.mood_alignment, custom_persona = EXCLUDED.custom_persona, reactivity_percentage = COALESCE(EXCLUDED.reactivity_percentage, chat_settings.reactivity_percentage), proactivity_percentage = COALESCE(EXCLUDED.proactivity_percentage, chat_settings.proactivity_percentage), enable_obscenifier = COALESCE(EXCLUDED.enable_obscenifier, chat_settings.enable_obscenifier), enable_profanity = COALESCE(EXCLUDED.enable_profanity, chat_settings.enable_profanity), enable_greet_joiners = COALESCE(EXCLUDED.enable_greet_joiners, chat_settings.enable_greet_joiners), enable_global_text_reply = COALESCE(EXCLUDED.enable_global_text_reply, chat_settings.enable_global_text_reply), enable_global_draw_reply = COALESCE(EXCLUDED.enable_global_draw_reply, chat_settings.enable_global_draw_reply), enable_daily_game = COALESCE(EXCLUDED.enable_daily_game, chat_settings.enable_daily_game), daily_game_theme = EXCLUDED.daily_game_theme, greeting_html = EXCLUDED.greeting_html, updated = CURRENT_TIMESTAMP";

/// Go `SelectTextHistoryEntryPayload` plus the conflict keys needed for Rust-native updates.
pub const SQL_SELECT_TEXT_HISTORY_ENTRY: &str = "SELECT bucket_day, entry_id, payload::text AS payload FROM chat_history_entries WHERE chat_id = $1 AND message_id = $2 AND kind = 'text' ORDER BY occurred_at DESC LIMIT 1";

/// Rust-native history payload update that preserves Go's existing row identity.
pub const SQL_UPDATE_HISTORY_ENTRY_PAYLOAD: &str = "UPDATE chat_history_entries SET payload = $4::jsonb, updated_at = CURRENT_TIMESTAMP WHERE bucket_day = $1 AND chat_id = $2 AND entry_id = $3";

/// Go `DeleteHistoryMessageEntries` SQL.
pub const SQL_DELETE_HISTORY_MESSAGE_ENTRIES: &str =
    "DELETE FROM chat_history_entries WHERE chat_id = $1 AND message_id = $2";

/// Go `ensure_chat_history_partition` call used before history entry upserts.
pub const SQL_ENSURE_CHAT_HISTORY_PARTITION: &str =
    "SELECT ensure_chat_history_partition($1::date)";

/// Go `UpsertHistoryEntry` SQL with SQLC name/comment removed.
pub const SQL_UPSERT_HISTORY_ENTRY: &str = "INSERT INTO chat_history_entries (bucket_day, chat_id, thread_id, message_id, entry_id, kind, role, occurred_at, sender_id, payload) VALUES ($1::date, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb) ON CONFLICT (bucket_day, chat_id, entry_id) DO UPDATE SET thread_id = EXCLUDED.thread_id, message_id = EXCLUDED.message_id, kind = EXCLUDED.kind, role = EXCLUDED.role, occurred_at = EXCLUDED.occurred_at, sender_id = EXCLUDED.sender_id, payload = EXCLUDED.payload, updated_at = CURRENT_TIMESTAMP";

/// Go `CreateSubscription` SQL with SQLC name/comment removed.
pub const SQL_CREATE_SUBSCRIPTION: &str = "INSERT INTO subscriptions (user_id, telegram_payment_charge_id, provider_payment_charge_id, expires_at) VALUES ($1, $2, $3, $4) ON CONFLICT (telegram_payment_charge_id) DO UPDATE SET expires_at = COALESCE(EXCLUDED.expires_at, subscriptions.expires_at), updated_at = CURRENT_TIMESTAMP RETURNING *";

/// Go `GetActiveSubscription` SQL with SQLC name/comment removed.
pub const SQL_GET_ACTIVE_SUBSCRIPTION: &str = "SELECT * FROM subscriptions WHERE user_id = $1 AND expires_at > NOW() AND canceled_at IS NULL AND refunded_at IS NULL AND telegram_payment_charge_id NOT LIKE 'admin_grant_%' ORDER BY created_at DESC, id DESC LIMIT 1";

/// Go `ListSubscriptionsByUser` SQL with SQLC name/comment removed.
pub const SQL_LIST_SUBSCRIPTIONS_BY_USER: &str =
    "SELECT * FROM subscriptions WHERE user_id = $1 ORDER BY created_at DESC, id DESC";

/// Go `GetSubscriptionByTelegramPaymentChargeID` SQL with SQLC name/comment removed.
pub const SQL_GET_SUBSCRIPTION_BY_TELEGRAM_PAYMENT_CHARGE_ID: &str =
    "SELECT * FROM subscriptions WHERE telegram_payment_charge_id = $1 LIMIT 1";

/// Go `DeleteSubscription` SQL with SQLC name/comment removed.
pub const SQL_DELETE_SUBSCRIPTION: &str = "DELETE FROM subscriptions WHERE id = $1 RETURNING *";

/// Go `ExpireSubscription` SQL with SQLC name/comment removed.
pub const SQL_EXPIRE_SUBSCRIPTION: &str = "UPDATE subscriptions SET expires_at = $2, updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *";

/// Go `MarkSubscriptionCanceled` SQL with SQLC name/comment removed.
pub const SQL_MARK_SUBSCRIPTION_CANCELED: &str = "UPDATE subscriptions SET canceled_at = COALESCE(canceled_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *";

/// Go `MarkSubscriptionRefunded` SQL with SQLC name/comment removed.
pub const SQL_MARK_SUBSCRIPTION_REFUNDED: &str = "UPDATE subscriptions SET refunded_at = COALESCE(refunded_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *";

/// Go `CreateDonation` SQL with SQLC name/comment removed.
pub const SQL_CREATE_DONATION: &str = "INSERT INTO donations (user_id, telegram_payment_charge_id, provider_payment_charge_id, amount_stars) VALUES ($1, $2, $3, $4) ON CONFLICT (telegram_payment_charge_id) DO NOTHING RETURNING *";

/// Go `GetDonationByTelegramPaymentChargeID` SQL with SQLC name/comment removed.
pub const SQL_GET_DONATION_BY_TELEGRAM_PAYMENT_CHARGE_ID: &str =
    "SELECT * FROM donations WHERE telegram_payment_charge_id = $1 LIMIT 1";

/// Go `DeleteDonation` SQL with SQLC name/comment removed.
pub const SQL_DELETE_DONATION: &str = "DELETE FROM donations WHERE id = $1 RETURNING *";

/// Go `UpsertVIPCache` SQL with SQLC name/comment removed.
pub const SQL_UPSERT_VIP_CACHE: &str = "INSERT INTO vip_cache (user_id, is_vip, expires_at) VALUES ($1, $2, $3) ON CONFLICT (user_id) DO UPDATE SET is_vip = COALESCE(EXCLUDED.is_vip, vip_cache.is_vip), expires_at = COALESCE(EXCLUDED.expires_at, vip_cache.expires_at), updated_at = CURRENT_TIMESTAMP";

/// Go `CreateVIPEvent` SQL with SQLC name/comment removed.
pub const SQL_CREATE_VIP_EVENT: &str = "SELECT id, user_id, event_type, delta_seconds, effective_expires_at, subscription_id, actor_user_id, reason, created_at FROM vip_create_event($1, $2, $3, $4, $5, $6)";

/// Go `GetVIPSummaryByUser` SQL with SQLC name/comment removed.
pub const SQL_GET_VIP_SUMMARY_BY_USER: &str = "SELECT id AS latest_event_id, user_id, event_type AS latest_event_type, delta_seconds AS latest_delta_seconds, effective_expires_at, effective_expires_at > CURRENT_TIMESTAMP AS is_active, CASE WHEN effective_expires_at > CURRENT_TIMESTAMP THEN FLOOR(EXTRACT(EPOCH FROM (effective_expires_at - CURRENT_TIMESTAMP)))::bigint ELSE 0::bigint END AS remaining_seconds, subscription_id AS latest_subscription_id, actor_user_id AS latest_actor_user_id, reason AS latest_reason, created_at AS latest_created_at FROM vip_events WHERE user_id = $1 ORDER BY id DESC LIMIT 1";

/// Go `ListVIPEventsByUser` SQL with SQLC name/comment removed.
pub const SQL_LIST_VIP_EVENTS_BY_USER: &str = "SELECT ve.id, ve.user_id, ve.event_type, ve.delta_seconds, ve.effective_expires_at, ve.subscription_id, ve.actor_user_id, actor.username AS actor_username, actor.first_name AS actor_first_name, ve.reason, ve.created_at, s.telegram_payment_charge_id, s.provider_payment_charge_id, s.expires_at AS subscription_expires_at, s.canceled_at AS subscription_canceled_at, s.refunded_at AS subscription_refunded_at FROM vip_events ve LEFT JOIN users actor ON actor.id = ve.actor_user_id LEFT JOIN subscriptions s ON s.id = ve.subscription_id WHERE ve.user_id = $1 ORDER BY ve.id DESC";

/// Go `GetVIPCache` SQL with SQLC name/comment removed.
pub const SQL_GET_VIP_CACHE: &str =
    "SELECT * FROM vip_cache WHERE user_id = $1 AND expires_at > CURRENT_TIMESTAMP LIMIT 1";

/// Go `DeleteVIPCache` SQL with SQLC name/comment removed.
pub const SQL_DELETE_VIP_CACHE: &str = "DELETE FROM vip_cache WHERE user_id = $1";

/// Go `CleanupExpiredVIPCache` SQL with SQLC name/comment removed.
pub const SQL_CLEANUP_EXPIRED_VIP_CACHE: &str =
    "DELETE FROM vip_cache WHERE expires_at <= CURRENT_TIMESTAMP RETURNING user_id";

/// Go Redis key prefix for persisted rate-limited chat expiry timestamps.
pub const RATE_LIMITED_CHAT_KEY_PREFIX: &str = "plotva:rate_limited_chat:";

/// Go Redis key prefix for cached chat administrator IDs.
pub const CHAT_ADMINS_KEY_PREFIX: &str = "chat:";

/// Go Redis key suffix for cached chat administrator IDs.
pub const CHAT_ADMINS_KEY_SUFFIX: &str = ":admins";

/// Go Redis key prefix for tracked ephemeral Telegram messages.
pub const EPHEMERAL_MESSAGE_KEY_PREFIX: &str = "ephemeral_messages:";

/// Go Redis SCAN pattern for tracked ephemeral Telegram messages.
pub const EPHEMERAL_MESSAGE_PATTERN: &str = "ephemeral_messages:*";

/// Go cleanup batch size for deleting expired ephemeral Telegram messages.
pub const EPHEMERAL_CLEANUP_BATCH_SIZE: usize = 10;

/// Go default cleanup interval for ephemeral Telegram messages.
pub const EPHEMERAL_DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(15);

/// Go Redis key prefix for chat history read-through cache.
pub const CHAT_HISTORY_CACHE_KEY_PREFIX: &str = "plotva:chat_history_cache:v2:";

/// Go chat-member status string for regular members.
pub const CHAT_MEMBER_STATUS_MEMBER: &str = "member";

/// Go chat-member status string for administrators.
pub const CHAT_MEMBER_STATUS_ADMINISTRATOR: &str = "administrator";

/// Go chat-member status string for chat creators.
pub const CHAT_MEMBER_STATUS_CREATOR: &str = "creator";

/// Go chat-member status string for users who left.
pub const CHAT_MEMBER_STATUS_LEFT: &str = "left";

/// Go chat-member status string for kicked users.
pub const CHAT_MEMBER_STATUS_KICKED: &str = "kicked";

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

/// Persisted ephemeral Telegram message lifecycle record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EphemeralMessage {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram message ID.
    pub message_id: i64,
    /// Instant after which cleanup should try deleting the message.
    pub expires_at: OffsetDateTime,
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

    /// Build the Redis-backed chat-admin cache store over this client.
    pub fn chat_admin_cache_store(&self) -> RedisChatAdminCacheStore {
        RedisChatAdminCacheStore::new(self.client.clone())
    }

    /// Build the Redis-backed ephemeral-message store over this client.
    pub fn ephemeral_message_store(&self) -> RedisEphemeralMessageStore {
        RedisEphemeralMessageStore::new(self.client.clone())
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

/// Redis-backed store for Go `chat:{id}:admins` admin-ID cache values.
#[derive(Clone, Debug)]
pub struct RedisChatAdminCacheStore {
    client: RedisClient,
    key_prefix: String,
    key_suffix: String,
}

impl RedisChatAdminCacheStore {
    /// Build a chat-admin cache store using Go's persisted key shape.
    pub fn new(client: RedisClient) -> Self {
        Self::with_key_prefix(client, CHAT_ADMINS_KEY_PREFIX)
    }

    /// Build a chat-admin cache store with an explicit prefix, useful for isolated tests.
    pub fn with_key_prefix(client: RedisClient, key_prefix: impl Into<String>) -> Self {
        Self {
            client,
            key_prefix: key_prefix.into(),
            key_suffix: CHAT_ADMINS_KEY_SUFFIX.to_owned(),
        }
    }

    /// Return the Redis key this store uses for one chat.
    pub fn key_for_chat(&self, chat_id: i64) -> String {
        format!("{}{chat_id}{}", self.key_prefix, self.key_suffix)
    }

    /// Persist the latest successful Telegram admin ID list with the Go TTL.
    pub async fn set_chat_admin_ids(
        &self,
        chat_id: i64,
        admin_ids: &[i64],
        ttl: Duration,
    ) -> Result<(), StorageError> {
        let value = chat_admin_ids_redis_value(admin_ids)?;
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

    /// Load cached Telegram admin IDs for one chat.
    pub async fn chat_admin_ids(&self, chat_id: i64) -> Result<Option<Vec<i64>>, StorageError> {
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
            .map(chat_admin_ids_from_redis_value)
            .transpose()
    }
}

/// Redis-backed store for Go `ephemeral_messages:{chat_id}:{message_id}` values.
#[derive(Clone, Debug)]
pub struct RedisEphemeralMessageStore {
    client: RedisClient,
    key_prefix: String,
}

impl RedisEphemeralMessageStore {
    /// Build an ephemeral-message store using Go's persisted key prefix.
    pub fn new(client: RedisClient) -> Self {
        Self::with_key_prefix(client, EPHEMERAL_MESSAGE_KEY_PREFIX)
    }

    /// Build an ephemeral-message store with an explicit prefix, useful for isolated tests.
    pub fn with_key_prefix(client: RedisClient, key_prefix: impl Into<String>) -> Self {
        Self {
            client,
            key_prefix: key_prefix.into(),
        }
    }

    /// Return the Redis key this store uses for one Telegram message.
    pub fn key_for_message(&self, chat_id: i64, message_id: i64) -> String {
        format!("{}{chat_id}:{message_id}", self.key_prefix)
    }

    /// Persist one ephemeral message with the Go cleanup-cushioned Redis TTL.
    pub async fn set_ephemeral_message(
        &self,
        message: &EphemeralMessage,
        ttl: Duration,
    ) -> Result<(), StorageError> {
        let value = ephemeral_message_redis_value(message)?;
        let mut connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let mut command = redis::cmd("SET");
        command
            .arg(self.key_for_message(message.chat_id, message.message_id))
            .arg(value);
        if !ttl.is_zero() {
            command.arg("PX").arg(redis_ttl_millis(ttl));
        }
        let _: String = command
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(())
    }

    /// Load one persisted ephemeral message.
    pub async fn ephemeral_message(
        &self,
        chat_id: i64,
        message_id: i64,
    ) -> Result<Option<EphemeralMessage>, StorageError> {
        let mut connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(self.key_for_message(chat_id, message_id))
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        value
            .as_deref()
            .map(ephemeral_message_from_redis_value)
            .transpose()
    }

    /// Delete persisted ephemeral-message records after Telegram delete attempts.
    pub async fn delete_ephemeral_messages(
        &self,
        messages: &[EphemeralMessage],
    ) -> Result<(), StorageError> {
        if messages.is_empty() {
            return Ok(());
        }
        let mut connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let keys = messages
            .iter()
            .map(|message| self.key_for_message(message.chat_id, message.message_id));
        let _: i64 = redis::cmd("DEL")
            .arg(keys.collect::<Vec<_>>())
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(())
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

    /// Resolve a stored Telegram username to a user ID using Go `GetUserByUsername` semantics.
    pub async fn get_user_id_by_username(
        &self,
        username: &str,
    ) -> Result<Option<i64>, StorageError> {
        let user_id = sqlx::query_scalar::<_, i64>(SQL_GET_USER_ID_BY_USERNAME)
            .bind(username)
            .fetch_optional(&self.pool)
            .await?;
        Ok(user_id)
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

/// SQLx-backed storage for Go chat settings used by permission policy.
#[derive(Clone, Debug)]
pub struct PostgresChatSettingsStore {
    pool: PgPool,
}

impl PostgresChatSettingsStore {
    /// Build a chat-settings store on an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Access the underlying Postgres pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Load the Go `chats.type` value for permission decisions.
    pub async fn get_chat_type(&self, chat_id: i64) -> Result<Option<String>, StorageError> {
        let chat_type = sqlx::query_scalar::<_, String>(SQL_GET_CHAT_TYPE)
            .bind(chat_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(chat_type)
    }

    /// Load Go `chat_settings` for one chat.
    pub async fn get_chat_settings(
        &self,
        chat_id: i64,
    ) -> Result<Option<ChatSettings>, StorageError> {
        let row = sqlx::query(SQL_GET_CHAT_SETTINGS)
            .bind(chat_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(chat_settings_from_row).transpose()?)
    }

    /// Upsert Go `chat_settings` using the permission-update parameter shape.
    pub async fn upsert_chat_settings(
        &self,
        update: &ChatSettingsUpdate,
    ) -> Result<(), StorageError> {
        sqlx::query(SQL_UPSERT_CHAT_SETTINGS)
            .bind(update.chat_id)
            .bind(update.mood_alignment.as_deref())
            .bind(update.custom_persona.as_deref())
            .bind(update.reactivity_percentage)
            .bind(update.proactivity_percentage)
            .bind(update.enable_obscenifier)
            .bind(update.enable_profanity)
            .bind(update.enable_greet_joiners)
            .bind(update.enable_global_text_reply)
            .bind(update.enable_global_draw_reply)
            .bind(update.enable_daily_game)
            .bind(update.daily_game_theme.as_str())
            .bind(update.greeting_html.as_deref())
            .bind(update.chat_type.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

/// SQLx-backed storage for Go `chat_members`.
#[derive(Clone, Debug)]
pub struct PostgresChatMemberStore {
    pool: PgPool,
}

/// Stored Go `chat_members` row fields needed by membership and permission flows.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChatMemberRecord {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram user ID.
    pub user_id: i64,
    /// Telegram chat-member status string.
    pub status: String,
    /// Whether the member is anonymous.
    pub is_anonymous: Option<bool>,
    /// Custom admin/creator title.
    pub custom_title: Option<String>,
    /// Whether the bot can edit this member.
    pub can_be_edited: Option<bool>,
    /// Whether the member can manage chat metadata.
    pub can_manage_chat: Option<bool>,
    /// Whether the member can delete messages.
    pub can_delete_messages: Option<bool>,
    /// Whether the member can manage video chats.
    pub can_manage_video_chats: Option<bool>,
    /// Whether the member can restrict users.
    pub can_restrict_members: Option<bool>,
    /// Whether the member can promote users.
    pub can_promote_members: Option<bool>,
    /// Whether the member can change chat info.
    pub can_change_info: Option<bool>,
    /// Whether the member can invite users.
    pub can_invite_users: Option<bool>,
    /// Whether the member can post messages.
    pub can_post_messages: Option<bool>,
    /// Whether the member can edit messages.
    pub can_edit_messages: Option<bool>,
    /// Whether the member can pin messages.
    pub can_pin_messages: Option<bool>,
    /// Whether the member can manage forum topics.
    pub can_manage_topics: Option<bool>,
    /// Whether the member can send text messages.
    pub can_send_messages: Option<bool>,
    /// Go legacy aggregate media-send permission.
    pub can_send_media_messages: Option<bool>,
    /// Whether the member can send polls.
    pub can_send_polls: Option<bool>,
    /// Whether the member can send other messages.
    pub can_send_other_messages: Option<bool>,
    /// Whether the member can add web page previews.
    pub can_add_web_page_previews: Option<bool>,
    /// Optional restricted/kicked expiration.
    pub until_date: Option<OffsetDateTime>,
}

/// Go `UpsertChatMemberParams` equivalent.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChatMemberUpsert {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram user ID.
    pub user_id: i64,
    /// Telegram chat-member status string.
    pub status: String,
    /// Whether the member is anonymous.
    pub is_anonymous: Option<bool>,
    /// Custom admin/creator title.
    pub custom_title: Option<String>,
    /// Whether the bot can edit this member.
    pub can_be_edited: Option<bool>,
    /// Whether the member can manage chat metadata.
    pub can_manage_chat: Option<bool>,
    /// Whether the member can delete messages.
    pub can_delete_messages: Option<bool>,
    /// Whether the member can manage video chats.
    pub can_manage_video_chats: Option<bool>,
    /// Whether the member can restrict users.
    pub can_restrict_members: Option<bool>,
    /// Whether the member can promote users.
    pub can_promote_members: Option<bool>,
    /// Whether the member can change chat info.
    pub can_change_info: Option<bool>,
    /// Whether the member can invite users.
    pub can_invite_users: Option<bool>,
    /// Whether the member can post messages.
    pub can_post_messages: Option<bool>,
    /// Whether the member can edit messages.
    pub can_edit_messages: Option<bool>,
    /// Whether the member can pin messages.
    pub can_pin_messages: Option<bool>,
    /// Whether the member can manage forum topics.
    pub can_manage_topics: Option<bool>,
    /// Whether the member can send text messages.
    pub can_send_messages: Option<bool>,
    /// Go legacy aggregate media-send permission.
    pub can_send_media_messages: Option<bool>,
    /// Whether the member can send polls.
    pub can_send_polls: Option<bool>,
    /// Whether the member can send other messages.
    pub can_send_other_messages: Option<bool>,
    /// Whether the member can add web page previews.
    pub can_add_web_page_previews: Option<bool>,
    /// Optional restricted/kicked expiration.
    pub until_date: Option<OffsetDateTime>,
}

/// Telegram-free equivalent of Go `storedAdminChatMember`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StoredAdminChatMember {
    /// Telegram user ID.
    pub user_id: i64,
    /// Stored admin/creator status.
    pub status: String,
    /// Whether the admin is anonymous.
    pub is_anonymous: Option<bool>,
    /// Custom admin title.
    pub custom_title: Option<String>,
    /// Whether the admin can delete messages.
    pub can_delete_messages: Option<bool>,
    /// Whether the admin can manage video chats.
    pub can_manage_video_chats: Option<bool>,
    /// Whether the admin can restrict members.
    pub can_restrict_members: Option<bool>,
    /// Whether the admin can promote members.
    pub can_promote_members: Option<bool>,
    /// Whether the admin can change chat info.
    pub can_change_info: Option<bool>,
    /// Whether the admin can invite users.
    pub can_invite_users: Option<bool>,
    /// Whether the admin can post messages.
    pub can_post_messages: Option<bool>,
    /// Whether the admin can edit messages.
    pub can_edit_messages: Option<bool>,
    /// Whether the admin can pin messages.
    pub can_pin_messages: Option<bool>,
}

impl PostgresChatMemberStore {
    /// Build a chat-member store on an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Access the underlying Postgres pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Load one Go `chat_members` row.
    pub async fn get_chat_member(
        &self,
        chat_id: i64,
        user_id: i64,
    ) -> Result<Option<ChatMemberRecord>, StorageError> {
        let row = sqlx::query(SQL_GET_CHAT_MEMBER)
            .bind(chat_id)
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(chat_member_from_row).transpose()?)
    }

    /// List Go `chat_members` rows for one chat.
    pub async fn list_chat_members(
        &self,
        chat_id: i64,
    ) -> Result<Vec<ChatMemberRecord>, StorageError> {
        let rows = sqlx::query(SQL_LIST_CHAT_MEMBERS)
            .bind(chat_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(chat_member_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    /// Upsert one Go `chat_members` row, preserving nullable permission semantics.
    pub async fn upsert_chat_member(&self, member: &ChatMemberUpsert) -> Result<(), StorageError> {
        sqlx::query(SQL_UPSERT_CHAT_MEMBER)
            .bind(member.chat_id)
            .bind(member.user_id)
            .bind(&member.status)
            .bind(member.is_anonymous)
            .bind(member.custom_title.as_deref())
            .bind(member.can_be_edited)
            .bind(member.can_manage_chat)
            .bind(member.can_delete_messages)
            .bind(member.can_manage_video_chats)
            .bind(member.can_restrict_members)
            .bind(member.can_promote_members)
            .bind(member.can_change_info)
            .bind(member.can_invite_users)
            .bind(member.can_post_messages)
            .bind(member.can_edit_messages)
            .bind(member.can_pin_messages)
            .bind(member.can_manage_topics)
            .bind(member.can_send_messages)
            .bind(member.can_send_media_messages)
            .bind(member.can_send_polls)
            .bind(member.can_send_other_messages)
            .bind(member.can_add_web_page_previews)
            .bind(member.until_date)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Upsert user state for admin-sync user persistence.
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
}

/// Go `storedMemberCanOpenGroupSettings`.
#[must_use]
pub fn stored_member_can_open_group_settings(member: Option<&ChatMemberRecord>) -> bool {
    member.is_some_and(|member| {
        member.status == CHAT_MEMBER_STATUS_CREATOR
            || member.status == CHAT_MEMBER_STATUS_ADMINISTRATOR
                && member.can_promote_members == Some(true)
    })
}

/// Go `storedAdminChatMember` without Telegram SDK types.
#[must_use]
pub fn stored_admin_chat_member(member: &ChatMemberRecord) -> Option<StoredAdminChatMember> {
    if member.status != CHAT_MEMBER_STATUS_ADMINISTRATOR
        && member.status != CHAT_MEMBER_STATUS_CREATOR
    {
        return None;
    }

    Some(StoredAdminChatMember {
        user_id: member.user_id,
        status: member.status.clone(),
        is_anonymous: member.is_anonymous,
        custom_title: member.custom_title.clone(),
        can_delete_messages: member.can_delete_messages,
        can_manage_video_chats: member.can_manage_video_chats,
        can_restrict_members: member.can_restrict_members,
        can_promote_members: member.can_promote_members,
        can_change_info: member.can_change_info,
        can_invite_users: member.can_invite_users,
        can_post_messages: member.can_post_messages,
        can_edit_messages: member.can_edit_messages,
        can_pin_messages: member.can_pin_messages,
    })
}

/// SQLx-backed storage for Go chat-history edit/delete side effects.
#[derive(Clone, Debug)]
pub struct PostgresHistoryStore {
    pool: PgPool,
    redis: Option<RedisClient>,
}

/// Go `UpsertHistoryEntryParams` row shape for chat-history persistence.
#[derive(Clone, Copy, Debug)]
pub struct HistoryEntryUpsert<'payload> {
    /// UTC bucket day partition.
    pub bucket_day: time::Date,
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram thread/topic ID.
    pub thread_id: i32,
    /// Telegram message ID.
    pub message_id: i32,
    /// Stable history entry ID, such as `msg:123`.
    pub entry_id: &'payload str,
    /// History message kind.
    pub kind: &'payload str,
    /// Dialog role.
    pub role: &'payload str,
    /// Message timestamp.
    pub occurred_at: OffsetDateTime,
    /// Sender ID.
    pub sender_id: i64,
    /// Serialized Go-shaped `MessageEntry` JSON payload.
    pub payload: &'payload [u8],
}

impl PostgresHistoryStore {
    /// Build a history store on an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool, redis: None }
    }

    /// Attach Redis cache invalidation using Go's chat-history cache key.
    pub fn with_redis_client(mut self, redis: RedisClient) -> Self {
        self.redis = Some(redis);
        self
    }

    /// Access the underlying Postgres pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Ensure the daily partition and upsert one Go-shaped history entry row.
    pub async fn upsert_history_entry(
        &self,
        entry: HistoryEntryUpsert<'_>,
    ) -> Result<(), StorageError> {
        sqlx::query(SQL_ENSURE_CHAT_HISTORY_PARTITION)
            .bind(entry.bucket_day)
            .execute(&self.pool)
            .await?;

        sqlx::query(SQL_UPSERT_HISTORY_ENTRY)
            .bind(entry.bucket_day)
            .bind(entry.chat_id)
            .bind(entry.thread_id)
            .bind(entry.message_id)
            .bind(entry.entry_id)
            .bind(entry.kind)
            .bind(entry.role)
            .bind(entry.occurred_at)
            .bind(entry.sender_id)
            .bind(entry.payload)
            .execute(&self.pool)
            .await?;

        self.invalidate_history_cache(entry.chat_id).await?;
        Ok(())
    }

    /// Update the stored text payload for one Go chat-history message entry.
    ///
    /// Returns `false` when Go would silently no-op because the service is missing
    /// required IDs or no text history row exists.
    pub async fn update_text_entry(
        &self,
        chat_id: i64,
        message_id: i32,
        new_text: &str,
    ) -> Result<bool, StorageError> {
        if chat_id == 0 || message_id == 0 {
            return Ok(false);
        }

        let row = sqlx::query(SQL_SELECT_TEXT_HISTORY_ENTRY)
            .bind(chat_id)
            .bind(message_id)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Ok(false);
        };

        let bucket_day: time::Date = row.try_get("bucket_day")?;
        let entry_id: String = row.try_get("entry_id")?;
        let payload: String = row.try_get("payload")?;
        let updated_payload = history_text_payload_with_text(&payload, new_text)?;

        sqlx::query(SQL_UPDATE_HISTORY_ENTRY_PAYLOAD)
            .bind(bucket_day)
            .bind(chat_id)
            .bind(&entry_id)
            .bind(&updated_payload)
            .execute(&self.pool)
            .await?;
        self.invalidate_history_cache(chat_id).await?;
        Ok(true)
    }

    /// Update the stored message text, original text, and metadata for an edited inbound message.
    ///
    /// Returns `false` when Go would silently no-op because the service is missing
    /// required IDs or no text history row exists.
    pub async fn update_message_entry(
        &self,
        chat_id: i64,
        message_id: i32,
        new_text: &str,
        original_text: &str,
        meta: &ChatMessageMeta,
    ) -> Result<bool, StorageError> {
        if chat_id == 0 || message_id == 0 {
            return Ok(false);
        }

        let row = sqlx::query(SQL_SELECT_TEXT_HISTORY_ENTRY)
            .bind(chat_id)
            .bind(message_id)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Ok(false);
        };

        let bucket_day: time::Date = row.try_get("bucket_day")?;
        let entry_id: String = row.try_get("entry_id")?;
        let payload: String = row.try_get("payload")?;
        let updated_payload = history_text_payload_with_message_update(
            &payload,
            new_text,
            original_text,
            meta.clone(),
        )?;

        sqlx::query(SQL_UPDATE_HISTORY_ENTRY_PAYLOAD)
            .bind(bucket_day)
            .bind(chat_id)
            .bind(&entry_id)
            .bind(&updated_payload)
            .execute(&self.pool)
            .await?;
        self.invalidate_history_cache(chat_id).await?;
        Ok(true)
    }

    /// Delete stored history entries for one Telegram message ID.
    pub async fn delete_message_entries(
        &self,
        chat_id: i64,
        message_id: i32,
    ) -> Result<u64, StorageError> {
        if chat_id == 0 || message_id == 0 {
            return Ok(0);
        }

        let result = sqlx::query(SQL_DELETE_HISTORY_MESSAGE_ENTRIES)
            .bind(chat_id)
            .bind(message_id)
            .execute(&self.pool)
            .await?;
        self.invalidate_history_cache(chat_id).await?;
        Ok(result.rows_affected())
    }

    async fn invalidate_history_cache(&self, chat_id: i64) -> Result<(), StorageError> {
        let Some(redis) = &self.redis else {
            return Ok(());
        };
        let mut connection = redis
            .get_multiplexed_async_connection()
            .await
            .map_err(|source| StorageError::Redis { source })?;
        let _: i64 = redis::cmd("DEL")
            .arg(history_cache_key(chat_id))
            .query_async(&mut connection)
            .await
            .map_err(|source| StorageError::Redis { source })?;
        Ok(())
    }
}

/// SQLx-backed storage for Go Telegram Stars subscriptions and donations.
#[derive(Clone, Debug)]
pub struct PostgresPaymentStore {
    pool: PgPool,
}

/// Go `CreateSubscriptionParams` row shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriptionCreate<'value> {
    /// Telegram user ID.
    pub user_id: i64,
    /// Telegram Stars payment charge ID.
    pub telegram_payment_charge_id: &'value str,
    /// Provider-side payment charge ID, often empty for Stars.
    pub provider_payment_charge_id: &'value str,
    /// Subscription expiry recorded at payment processing time.
    pub expires_at: OffsetDateTime,
}

/// Go `subscriptions` row shape used by payments and VIP ledger code.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionRecord {
    /// Database primary key.
    pub id: i64,
    /// Telegram user ID.
    pub user_id: i64,
    /// Telegram Stars payment charge ID.
    pub telegram_payment_charge_id: String,
    /// Provider-side payment charge ID, often empty for Stars.
    pub provider_payment_charge_id: String,
    /// Subscription expiry.
    pub expires_at: OffsetDateTime,
    /// Row creation time.
    pub created_at: OffsetDateTime,
    /// Row update time.
    pub updated_at: OffsetDateTime,
    /// Telegram-side cancellation timestamp, when recorded.
    pub canceled_at: Option<OffsetDateTime>,
    /// Refund timestamp, when recorded.
    pub refunded_at: Option<OffsetDateTime>,
}

/// Go `CreateDonationParams` row shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DonationCreate<'value> {
    /// Telegram user ID.
    pub user_id: i64,
    /// Telegram Stars payment charge ID.
    pub telegram_payment_charge_id: &'value str,
    /// Provider-side payment charge ID, often empty for Stars.
    pub provider_payment_charge_id: &'value str,
    /// Donation amount in Telegram Stars.
    pub amount_stars: i64,
}

/// Go `donations` row shape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DonationRecord {
    /// Database primary key.
    pub id: i64,
    /// Telegram user ID.
    pub user_id: i64,
    /// Telegram Stars payment charge ID.
    pub telegram_payment_charge_id: String,
    /// Provider-side payment charge ID, often empty for Stars.
    pub provider_payment_charge_id: String,
    /// Donation amount in Telegram Stars.
    pub amount_stars: i64,
    /// Row creation time.
    pub created_at: OffsetDateTime,
}

impl PostgresPaymentStore {
    /// Build a payment store on an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Access the underlying Postgres pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Create or refresh a Go subscription payment row.
    pub async fn create_subscription(
        &self,
        subscription: SubscriptionCreate<'_>,
    ) -> Result<SubscriptionRecord, StorageError> {
        let row = sqlx::query(SQL_CREATE_SUBSCRIPTION)
            .bind(subscription.user_id)
            .bind(subscription.telegram_payment_charge_id)
            .bind(subscription.provider_payment_charge_id)
            .bind(subscription.expires_at)
            .fetch_one(&self.pool)
            .await?;
        subscription_from_row(row).map_err(StorageError::from)
    }

    /// Load the current active non-admin, non-canceled, non-refunded subscription for a user.
    pub async fn get_active_subscription(
        &self,
        user_id: i64,
    ) -> Result<Option<SubscriptionRecord>, StorageError> {
        let row = sqlx::query(SQL_GET_ACTIVE_SUBSCRIPTION)
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(subscription_from_row).transpose()?)
    }

    /// List all subscription rows for a user in Go display order.
    pub async fn list_subscriptions_by_user(
        &self,
        user_id: i64,
    ) -> Result<Vec<SubscriptionRecord>, StorageError> {
        let rows = sqlx::query(SQL_LIST_SUBSCRIPTIONS_BY_USER)
            .bind(user_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(subscription_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
    }

    /// Load a subscription by Telegram Stars payment charge ID.
    pub async fn get_subscription_by_telegram_payment_charge_id(
        &self,
        telegram_payment_charge_id: &str,
    ) -> Result<Option<SubscriptionRecord>, StorageError> {
        let row = sqlx::query(SQL_GET_SUBSCRIPTION_BY_TELEGRAM_PAYMENT_CHARGE_ID)
            .bind(telegram_payment_charge_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(subscription_from_row).transpose()?)
    }

    /// Delete a subscription row and return the removed row.
    pub async fn delete_subscription(&self, id: i64) -> Result<SubscriptionRecord, StorageError> {
        let row = sqlx::query(SQL_DELETE_SUBSCRIPTION)
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        subscription_from_row(row).map_err(StorageError::from)
    }

    /// Set a subscription expiry and return the updated row.
    pub async fn expire_subscription(
        &self,
        id: i64,
        expires_at: OffsetDateTime,
    ) -> Result<SubscriptionRecord, StorageError> {
        let row = sqlx::query(SQL_EXPIRE_SUBSCRIPTION)
            .bind(id)
            .bind(expires_at)
            .fetch_one(&self.pool)
            .await?;
        subscription_from_row(row).map_err(StorageError::from)
    }

    /// Mark a subscription canceled using Go's first-write-wins timestamp behavior.
    pub async fn mark_subscription_canceled(
        &self,
        id: i64,
    ) -> Result<SubscriptionRecord, StorageError> {
        let row = sqlx::query(SQL_MARK_SUBSCRIPTION_CANCELED)
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        subscription_from_row(row).map_err(StorageError::from)
    }

    /// Mark a subscription refunded using Go's first-write-wins timestamp behavior.
    pub async fn mark_subscription_refunded(
        &self,
        id: i64,
    ) -> Result<SubscriptionRecord, StorageError> {
        let row = sqlx::query(SQL_MARK_SUBSCRIPTION_REFUNDED)
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        subscription_from_row(row).map_err(StorageError::from)
    }

    /// Insert a donation payment row.
    ///
    /// Duplicate Telegram charge IDs return `sqlx::Error::RowNotFound`, matching
    /// SQLC's no-row result for Go `ON CONFLICT DO NOTHING RETURNING *`.
    pub async fn create_donation(
        &self,
        donation: DonationCreate<'_>,
    ) -> Result<DonationRecord, StorageError> {
        let row = sqlx::query(SQL_CREATE_DONATION)
            .bind(donation.user_id)
            .bind(donation.telegram_payment_charge_id)
            .bind(donation.provider_payment_charge_id)
            .bind(donation.amount_stars)
            .fetch_one(&self.pool)
            .await?;
        donation_from_row(row).map_err(StorageError::from)
    }

    /// Load a donation by Telegram Stars payment charge ID.
    pub async fn get_donation_by_telegram_payment_charge_id(
        &self,
        telegram_payment_charge_id: &str,
    ) -> Result<Option<DonationRecord>, StorageError> {
        let row = sqlx::query(SQL_GET_DONATION_BY_TELEGRAM_PAYMENT_CHARGE_ID)
            .bind(telegram_payment_charge_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(donation_from_row).transpose()?)
    }

    /// Delete a donation row and return the removed row.
    pub async fn delete_donation(&self, id: i64) -> Result<DonationRecord, StorageError> {
        let row = sqlx::query(SQL_DELETE_DONATION)
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        donation_from_row(row).map_err(StorageError::from)
    }
}

/// SQLx-backed storage for Go VIP cache and event-sourced VIP ledger rows.
#[derive(Clone, Debug)]
pub struct PostgresVipStore {
    pool: PgPool,
}

/// Go `UpsertVIPCacheParams` row shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VipCacheUpsert {
    /// Telegram user ID.
    pub user_id: i64,
    /// Whether the user has VIP according to the external Telegram check cache.
    pub is_vip: bool,
    /// Cached VIP expiry timestamp.
    pub expires_at: OffsetDateTime,
}

/// Go `vip_cache` row shape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VipCacheRecord {
    /// Telegram user ID.
    pub user_id: i64,
    /// Whether the user has VIP according to the external Telegram check cache.
    pub is_vip: bool,
    /// Cached VIP expiry timestamp.
    pub expires_at: OffsetDateTime,
    /// Row creation time.
    pub created_at: OffsetDateTime,
    /// Row update time.
    pub updated_at: OffsetDateTime,
}

/// Go `CreateVIPEventParams` row shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VipEventCreate<'value> {
    /// Telegram user ID.
    pub user_id: i64,
    /// VIP event type.
    pub event_type: &'value str,
    /// Delta in VIP seconds.
    pub delta_seconds: i64,
    /// Related subscription row, when applicable.
    pub subscription_id: Option<i64>,
    /// Admin or actor user ID, when applicable.
    pub actor_user_id: Option<i64>,
    /// Human-readable reason. `None` is stored as an empty string by Go SQL.
    pub reason: Option<&'value str>,
}

/// Go `vip_events` row shape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VipEventRecord {
    /// Database primary key.
    pub id: i64,
    /// Telegram user ID.
    pub user_id: i64,
    /// VIP event type.
    pub event_type: String,
    /// Delta in VIP seconds.
    pub delta_seconds: i64,
    /// Effective expiry after applying this event.
    pub effective_expires_at: OffsetDateTime,
    /// Related subscription row, when applicable.
    pub subscription_id: Option<i64>,
    /// Admin or actor user ID, when applicable.
    pub actor_user_id: Option<i64>,
    /// Human-readable reason.
    pub reason: String,
    /// Row creation time.
    pub created_at: OffsetDateTime,
}

/// Go `GetVIPSummaryByUserRow` shape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VipSummaryRecord {
    /// Latest VIP event ID.
    pub latest_event_id: i64,
    /// Telegram user ID.
    pub user_id: i64,
    /// Latest VIP event type.
    pub latest_event_type: String,
    /// Latest VIP delta in seconds.
    pub latest_delta_seconds: i64,
    /// Current effective expiry.
    pub effective_expires_at: OffsetDateTime,
    /// Whether the effective expiry is still in the future at query time.
    pub is_active: bool,
    /// Query-time remaining seconds, clamped to zero by Go SQL.
    pub remaining_seconds: i64,
    /// Related latest subscription row, when applicable.
    pub latest_subscription_id: Option<i64>,
    /// Related latest actor user, when applicable.
    pub latest_actor_user_id: Option<i64>,
    /// Latest event reason.
    pub latest_reason: String,
    /// Latest event creation time.
    pub latest_created_at: OffsetDateTime,
}

/// Go `ListVIPEventsByUserRow` shape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VipEventListRecord {
    /// Database primary key.
    pub id: i64,
    /// Telegram user ID.
    pub user_id: i64,
    /// VIP event type.
    pub event_type: String,
    /// Delta in VIP seconds.
    pub delta_seconds: i64,
    /// Effective expiry after applying this event.
    pub effective_expires_at: OffsetDateTime,
    /// Related subscription row, when applicable.
    pub subscription_id: Option<i64>,
    /// Admin or actor user ID, when applicable.
    pub actor_user_id: Option<i64>,
    /// Joined actor username.
    pub actor_username: Option<String>,
    /// Joined actor first name.
    pub actor_first_name: Option<String>,
    /// Human-readable reason.
    pub reason: String,
    /// Row creation time.
    pub created_at: OffsetDateTime,
    /// Joined subscription Telegram payment charge ID.
    pub telegram_payment_charge_id: Option<String>,
    /// Joined subscription provider payment charge ID.
    pub provider_payment_charge_id: Option<String>,
    /// Joined subscription expiry.
    pub subscription_expires_at: Option<OffsetDateTime>,
    /// Joined subscription cancellation timestamp.
    pub subscription_canceled_at: Option<OffsetDateTime>,
    /// Joined subscription refund timestamp.
    pub subscription_refunded_at: Option<OffsetDateTime>,
}

impl PostgresVipStore {
    /// Build a VIP store on an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Access the underlying Postgres pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Upsert the legacy external VIP cache row.
    pub async fn upsert_vip_cache(&self, cache: VipCacheUpsert) -> Result<(), StorageError> {
        sqlx::query(SQL_UPSERT_VIP_CACHE)
            .bind(cache.user_id)
            .bind(cache.is_vip)
            .bind(cache.expires_at)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Load a non-expired VIP cache row by user ID.
    pub async fn get_vip_cache(
        &self,
        user_id: i64,
    ) -> Result<Option<VipCacheRecord>, StorageError> {
        let row = sqlx::query(SQL_GET_VIP_CACHE)
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(vip_cache_from_row).transpose()?)
    }

    /// Delete a VIP cache row by user ID.
    pub async fn delete_vip_cache(&self, user_id: i64) -> Result<(), StorageError> {
        sqlx::query(SQL_DELETE_VIP_CACHE)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Delete expired VIP cache rows and return affected user IDs.
    pub async fn cleanup_expired_vip_cache(&self) -> Result<Vec<i64>, StorageError> {
        let rows = sqlx::query_scalar::<_, i64>(SQL_CLEANUP_EXPIRED_VIP_CACHE)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }

    /// Create an event-sourced VIP ledger entry through Go's `vip_create_event` SQL function.
    pub async fn create_vip_event(
        &self,
        event: VipEventCreate<'_>,
    ) -> Result<VipEventRecord, StorageError> {
        let row = sqlx::query(SQL_CREATE_VIP_EVENT)
            .bind(event.user_id)
            .bind(event.event_type)
            .bind(event.delta_seconds)
            .bind(event.subscription_id)
            .bind(event.actor_user_id)
            .bind(event.reason)
            .fetch_one(&self.pool)
            .await?;
        vip_event_from_row(row).map_err(StorageError::from)
    }

    /// Load the latest VIP ledger summary for a user.
    pub async fn get_vip_summary_by_user(
        &self,
        user_id: i64,
    ) -> Result<Option<VipSummaryRecord>, StorageError> {
        let row = sqlx::query(SQL_GET_VIP_SUMMARY_BY_USER)
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(vip_summary_from_row).transpose()?)
    }

    /// List VIP ledger events for a user in Go display order.
    pub async fn list_vip_events_by_user(
        &self,
        user_id: i64,
    ) -> Result<Vec<VipEventListRecord>, StorageError> {
        let rows = sqlx::query(SQL_LIST_VIP_EVENTS_BY_USER)
            .bind(user_id)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(vip_event_list_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StorageError::from)
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
    /// Chat-admin ID list JSON codec failed.
    #[error("decode chat admin ids: {source}")]
    ChatAdminIdsCodec {
        /// JSON codec error.
        source: serde_json::Error,
    },
    /// Ephemeral message JSON codec failed.
    #[error("decode ephemeral message: {source}")]
    EphemeralMessageCodec {
        /// JSON codec error.
        source: serde_json::Error,
    },
    /// Rate-limit expiry timestamp could not be represented.
    #[error("invalid rate limit expiry timestamp: {source}")]
    RateLimitTimestamp {
        /// Timestamp range error.
        source: time::error::ComponentRange,
    },
    /// Ephemeral message expiry timestamp could not be represented.
    #[error("invalid ephemeral message expiry timestamp: {source}")]
    EphemeralMessageTimestamp {
        /// Timestamp range error.
        source: time::error::ComponentRange,
    },
    /// Chat history payload JSON codec failed.
    #[error("decode chat history payload: {source}")]
    HistoryPayloadCodec {
        /// JSON codec error.
        source: serde_json::Error,
    },
    /// Chat history payload was not an object shaped like Go `MessageEntry`.
    #[error("chat history payload is not a JSON object")]
    HistoryPayloadShape,
}

impl StorageError {
    /// Whether this error is SQLx's no-row result, matching Go/SQLC duplicate no-row fallbacks.
    #[must_use]
    pub fn is_row_not_found(&self) -> bool {
        matches!(
            self,
            Self::Postgres {
                source: sqlx::Error::RowNotFound
            }
        )
    }
}

/// Build the persisted Go rate-limited-chat key for a chat.
pub fn rate_limited_chat_key(chat_id: i64) -> String {
    format!("{RATE_LIMITED_CHAT_KEY_PREFIX}{chat_id}")
}

/// Build the Go cached-admin Redis key for a chat.
pub fn chat_admins_key(chat_id: i64) -> String {
    format!("{CHAT_ADMINS_KEY_PREFIX}{chat_id}{CHAT_ADMINS_KEY_SUFFIX}")
}

/// Build the Go tracked-ephemeral-message Redis key for a Telegram message.
pub fn ephemeral_message_key(chat_id: i64, message_id: i64) -> String {
    format!("{EPHEMERAL_MESSAGE_KEY_PREFIX}{chat_id}:{message_id}")
}

/// Build Go tracked-ephemeral-message Redis keys for a cleanup batch.
pub fn ephemeral_message_keys(messages: &[EphemeralMessage]) -> Vec<String> {
    messages
        .iter()
        .map(|message| ephemeral_message_key(message.chat_id, message.message_id))
        .collect()
}

/// Build the persisted Go chat-history cache key for a chat.
pub fn history_cache_key(chat_id: i64) -> String {
    format!("{CHAT_HISTORY_CACHE_KEY_PREFIX}{chat_id}")
}

/// Mutate a stored Go history payload the same way `Service.Update` changes `api.Message.Text`.
pub fn history_text_payload_with_text(
    payload: impl AsRef<[u8]>,
    new_text: &str,
) -> Result<String, StorageError> {
    let mut payload: serde_json::Value = serde_json::from_slice(payload.as_ref())
        .map_err(|source| StorageError::HistoryPayloadCodec { source })?;
    let object = payload
        .as_object_mut()
        .ok_or(StorageError::HistoryPayloadShape)?;
    if new_text.is_empty() {
        object.remove("text");
    } else {
        object.insert(
            "text".to_owned(),
            serde_json::Value::String(new_text.to_owned()),
        );
    }
    serde_json::to_string(&payload).map_err(|source| StorageError::HistoryPayloadCodec { source })
}

/// Mutate a stored Go history payload the same way `Service.UpdateMessage` changes a text entry.
pub fn history_text_payload_with_message_update(
    payload: impl AsRef<[u8]>,
    new_text: &str,
    original_text: &str,
    meta: ChatMessageMeta,
) -> Result<String, StorageError> {
    let mut payload: serde_json::Value = serde_json::from_slice(payload.as_ref())
        .map_err(|source| StorageError::HistoryPayloadCodec { source })?;
    let object = payload
        .as_object_mut()
        .ok_or(StorageError::HistoryPayloadShape)?;
    let (text, original_text, meta) =
        normalize_history_message_update(new_text, original_text, meta);

    if text.is_empty() {
        object.remove("text");
    } else {
        object.insert("text".to_owned(), serde_json::Value::String(text));
    }
    if original_text.is_empty() {
        object.remove("original_text");
    } else {
        object.insert(
            "original_text".to_owned(),
            serde_json::Value::String(original_text),
        );
    }
    object.insert(
        "meta".to_owned(),
        serde_json::to_value(meta)
            .map_err(|source| StorageError::HistoryPayloadCodec { source })?,
    );

    serde_json::to_string(&payload).map_err(|source| StorageError::HistoryPayloadCodec { source })
}

fn normalize_history_message_update(
    text: &str,
    original_text: &str,
    mut meta: ChatMessageMeta,
) -> (String, String, ChatMessageMeta) {
    let text = text.trim().to_owned();
    let mut original_text = original_text.trim().to_owned();
    if original_text == text {
        original_text.clear();
    }

    if !text.is_empty() {
        for attachment in &mut meta.attachments {
            if attachment.source.trim() != "message" {
                continue;
            }
            if !attachment.caption.trim().is_empty() {
                attachment.caption.clear();
            }
            if attachment.content.trim() == text {
                attachment.content.clear();
            }
        }
    }

    (text, original_text, meta)
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

/// Encode cached chat administrator IDs as the approved Rust-native Redis JSON value.
pub fn chat_admin_ids_redis_value(admin_ids: &[i64]) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(admin_ids).map_err(|source| StorageError::ChatAdminIdsCodec { source })
}

/// Decode cached chat administrator IDs from the Rust-native Redis JSON value.
pub fn chat_admin_ids_from_redis_value(value: &[u8]) -> Result<Vec<i64>, StorageError> {
    serde_json::from_slice(value).map_err(|source| StorageError::ChatAdminIdsCodec { source })
}

/// Encode an ephemeral message as the approved Rust-native Redis JSON value.
pub fn ephemeral_message_redis_value(message: &EphemeralMessage) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(&EphemeralMessageValue {
        chat_id: message.chat_id,
        message_id: message.message_id,
        expires_at_unix_timestamp_nanos: message.expires_at.unix_timestamp_nanos(),
    })
    .map_err(|source| StorageError::EphemeralMessageCodec { source })
}

/// Decode an ephemeral message from the Rust-native Redis JSON value.
pub fn ephemeral_message_from_redis_value(value: &[u8]) -> Result<EphemeralMessage, StorageError> {
    let value: EphemeralMessageValue = serde_json::from_slice(value)
        .map_err(|source| StorageError::EphemeralMessageCodec { source })?;
    let expires_at =
        OffsetDateTime::from_unix_timestamp_nanos(value.expires_at_unix_timestamp_nanos)
            .map_err(|source| StorageError::EphemeralMessageTimestamp { source })?;
    Ok(EphemeralMessage {
        chat_id: value.chat_id,
        message_id: value.message_id,
        expires_at,
    })
}

/// Return the Redis TTL Go uses for tracked ephemeral messages.
pub fn ephemeral_redis_ttl(duration: Duration, cleanup_interval: Duration) -> Duration {
    duration
        .saturating_add(cleanup_interval)
        .saturating_add(Duration::from_secs(1))
}

/// Filter ephemeral messages whose expiry is strictly before `now`, matching Go `time.After`.
pub fn expired_ephemeral_messages_at(
    messages: &[EphemeralMessage],
    now: OffsetDateTime,
) -> Vec<EphemeralMessage> {
    messages
        .iter()
        .filter(|message| now > message.expires_at)
        .cloned()
        .collect()
}

/// Return whether the loaded expiry is still active using Go's strict `time.Before` boundary.
pub fn rate_limit_is_active_at(expiry: Option<OffsetDateTime>, now: OffsetDateTime) -> bool {
    expiry.is_some_and(|expiry| now < expiry)
}

#[derive(Debug, Deserialize, Serialize)]
struct RateLimitExpiryValue {
    unix_timestamp_nanos: i128,
}

#[derive(Debug, Deserialize, Serialize)]
struct EphemeralMessageValue {
    chat_id: i64,
    message_id: i64,
    expires_at_unix_timestamp_nanos: i128,
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

fn chat_settings_from_row(row: PgRow) -> Result<ChatSettings, sqlx::Error> {
    Ok(ChatSettings {
        chat_id: row.try_get("chat_id")?,
        mood_alignment: row.try_get("mood_alignment")?,
        custom_persona: row.try_get("custom_persona")?,
        reactivity_percentage: row.try_get("reactivity_percentage")?,
        proactivity_percentage: row.try_get("proactivity_percentage")?,
        enable_global_text_reply: row.try_get("enable_global_text_reply")?,
        enable_global_draw_reply: row.try_get("enable_global_draw_reply")?,
        enable_obscenifier: row.try_get("enable_obscenifier")?,
        enable_profanity: row.try_get("enable_profanity")?,
        enable_greet_joiners: row.try_get("enable_greet_joiners")?,
        enable_daily_game: row.try_get("enable_daily_game")?,
        daily_game_theme: row.try_get("daily_game_theme")?,
        greeting_html: row.try_get("greeting_html")?,
    })
}

fn chat_member_from_row(row: PgRow) -> Result<ChatMemberRecord, sqlx::Error> {
    Ok(ChatMemberRecord {
        chat_id: row.try_get("chat_id")?,
        user_id: row.try_get("user_id")?,
        status: row.try_get("status")?,
        is_anonymous: row.try_get("is_anonymous")?,
        custom_title: row.try_get("custom_title")?,
        can_be_edited: row.try_get("can_be_edited")?,
        can_manage_chat: row.try_get("can_manage_chat")?,
        can_delete_messages: row.try_get("can_delete_messages")?,
        can_manage_video_chats: row.try_get("can_manage_video_chats")?,
        can_restrict_members: row.try_get("can_restrict_members")?,
        can_promote_members: row.try_get("can_promote_members")?,
        can_change_info: row.try_get("can_change_info")?,
        can_invite_users: row.try_get("can_invite_users")?,
        can_post_messages: row.try_get("can_post_messages")?,
        can_edit_messages: row.try_get("can_edit_messages")?,
        can_pin_messages: row.try_get("can_pin_messages")?,
        can_manage_topics: row.try_get("can_manage_topics")?,
        can_send_messages: row.try_get("can_send_messages")?,
        can_send_media_messages: row.try_get("can_send_media_messages")?,
        can_send_polls: row.try_get("can_send_polls")?,
        can_send_other_messages: row.try_get("can_send_other_messages")?,
        can_add_web_page_previews: row.try_get("can_add_web_page_previews")?,
        until_date: row.try_get("until_date")?,
    })
}

fn subscription_from_row(row: PgRow) -> Result<SubscriptionRecord, sqlx::Error> {
    Ok(SubscriptionRecord {
        id: row.try_get("id")?,
        user_id: row.try_get("user_id")?,
        telegram_payment_charge_id: row.try_get("telegram_payment_charge_id")?,
        provider_payment_charge_id: row.try_get("provider_payment_charge_id")?,
        expires_at: row.try_get("expires_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        canceled_at: row.try_get("canceled_at")?,
        refunded_at: row.try_get("refunded_at")?,
    })
}

fn donation_from_row(row: PgRow) -> Result<DonationRecord, sqlx::Error> {
    Ok(DonationRecord {
        id: row.try_get("id")?,
        user_id: row.try_get("user_id")?,
        telegram_payment_charge_id: row.try_get("telegram_payment_charge_id")?,
        provider_payment_charge_id: row.try_get("provider_payment_charge_id")?,
        amount_stars: row.try_get("amount_stars")?,
        created_at: row.try_get("created_at")?,
    })
}

fn vip_cache_from_row(row: PgRow) -> Result<VipCacheRecord, sqlx::Error> {
    Ok(VipCacheRecord {
        user_id: row.try_get("user_id")?,
        is_vip: row.try_get("is_vip")?,
        expires_at: row.try_get("expires_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn vip_event_from_row(row: PgRow) -> Result<VipEventRecord, sqlx::Error> {
    Ok(VipEventRecord {
        id: row.try_get("id")?,
        user_id: row.try_get("user_id")?,
        event_type: row.try_get("event_type")?,
        delta_seconds: row.try_get("delta_seconds")?,
        effective_expires_at: row.try_get("effective_expires_at")?,
        subscription_id: row.try_get("subscription_id")?,
        actor_user_id: row.try_get("actor_user_id")?,
        reason: row.try_get("reason")?,
        created_at: row.try_get("created_at")?,
    })
}

fn vip_summary_from_row(row: PgRow) -> Result<VipSummaryRecord, sqlx::Error> {
    Ok(VipSummaryRecord {
        latest_event_id: row.try_get("latest_event_id")?,
        user_id: row.try_get("user_id")?,
        latest_event_type: row.try_get("latest_event_type")?,
        latest_delta_seconds: row.try_get("latest_delta_seconds")?,
        effective_expires_at: row.try_get("effective_expires_at")?,
        is_active: row.try_get("is_active")?,
        remaining_seconds: row.try_get("remaining_seconds")?,
        latest_subscription_id: row.try_get("latest_subscription_id")?,
        latest_actor_user_id: row.try_get("latest_actor_user_id")?,
        latest_reason: row.try_get("latest_reason")?,
        latest_created_at: row.try_get("latest_created_at")?,
    })
}

fn vip_event_list_from_row(row: PgRow) -> Result<VipEventListRecord, sqlx::Error> {
    Ok(VipEventListRecord {
        id: row.try_get("id")?,
        user_id: row.try_get("user_id")?,
        event_type: row.try_get("event_type")?,
        delta_seconds: row.try_get("delta_seconds")?,
        effective_expires_at: row.try_get("effective_expires_at")?,
        subscription_id: row.try_get("subscription_id")?,
        actor_user_id: row.try_get("actor_user_id")?,
        actor_username: row.try_get("actor_username")?,
        actor_first_name: row.try_get("actor_first_name")?,
        reason: row.try_get("reason")?,
        created_at: row.try_get("created_at")?,
        telegram_payment_charge_id: row.try_get("telegram_payment_charge_id")?,
        provider_payment_charge_id: row.try_get("provider_payment_charge_id")?,
        subscription_expires_at: row.try_get("subscription_expires_at")?,
        subscription_canceled_at: row.try_get("subscription_canceled_at")?,
        subscription_refunded_at: row.try_get("subscription_refunded_at")?,
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
    fn chat_admin_cache_key_and_codec_use_rust_native_json() -> Result<(), Box<dyn Error>> {
        assert_eq!(super::chat_admins_key(-10042), "chat:-10042:admins");

        let value = super::chat_admin_ids_redis_value(&[42, 43])?;
        assert_eq!(serde_json::from_slice::<Vec<i64>>(&value)?, vec![42, 43]);
        assert_eq!(
            super::chat_admin_ids_from_redis_value(&value)?,
            vec![42, 43]
        );

        let error = super::chat_admin_ids_from_redis_value(&[0xff, 0xb4, 0x0a, 0x00, 0xff, 0xb0])
            .expect_err("legacy gob values should be rejected after the approved cutover");
        assert!(error.to_string().contains("decode chat admin ids"));
        Ok(())
    }

    #[test]
    fn ephemeral_message_keys_ttl_and_codec_preserve_go_lifecycle_contract()
    -> Result<(), Box<dyn Error>> {
        let expires_at =
            time::OffsetDateTime::from_unix_timestamp_nanos(1_710_000_000_123_456_789)?;
        let message = super::EphemeralMessage {
            chat_id: -10042,
            message_id: 77,
            expires_at,
        };

        assert_eq!(
            super::ephemeral_message_key(-10042, 77),
            "ephemeral_messages:-10042:77"
        );
        assert_eq!(
            super::ephemeral_message_keys(std::slice::from_ref(&message)),
            vec!["ephemeral_messages:-10042:77"]
        );
        assert_eq!(super::EPHEMERAL_CLEANUP_BATCH_SIZE, 10);
        assert_eq!(
            super::ephemeral_redis_ttl(Duration::from_secs(60), Duration::from_secs(15)),
            Duration::from_secs(76)
        );

        let value = super::ephemeral_message_redis_value(&message)?;
        assert_eq!(
            value,
            br#"{"chat_id":-10042,"message_id":77,"expires_at_unix_timestamp_nanos":1710000000123456789}"#
        );
        assert_eq!(super::ephemeral_message_from_redis_value(&value)?, message);

        let error =
            super::ephemeral_message_from_redis_value(&[0xff, 0xb4, 0x0a, 0x00, 0xff, 0xb0])
                .expect_err("legacy gob values should be rejected after the approved cutover");
        assert!(error.to_string().contains("decode ephemeral message"));
        Ok(())
    }

    #[test]
    fn expired_ephemeral_messages_use_go_strict_after_boundary() -> Result<(), Box<dyn Error>> {
        let now = time::OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let messages = vec![
            super::EphemeralMessage {
                chat_id: 1,
                message_id: 10,
                expires_at: now - time::Duration::seconds(1),
            },
            super::EphemeralMessage {
                chat_id: 2,
                message_id: 20,
                expires_at: now,
            },
            super::EphemeralMessage {
                chat_id: 3,
                message_id: 30,
                expires_at: now + time::Duration::seconds(1),
            },
        ];

        assert_eq!(
            super::expired_ephemeral_messages_at(&messages, now),
            vec![messages[0].clone()]
        );
        Ok(())
    }

    #[test]
    fn history_text_payload_with_message_update_matches_go_normalization()
    -> Result<(), Box<dyn Error>> {
        let payload = serde_json::json!({
            "entry_id": "msg:77",
            "role": "user",
            "kind": "text",
            "timestamp": "2026-05-20T00:00:00Z",
            "message_id": 77,
            "date": 1_710_000_000,
            "chat": {"id": 42, "type": "private"},
            "text": "old text",
            "original_text": "old original",
            "meta": {}
        });
        let meta = openplotva_core::ChatMessageMeta {
            sender_id: 99,
            attachments: vec![
                openplotva_core::ChatAttachment {
                    kind: "image".to_owned(),
                    source: "message".to_owned(),
                    caption: " edited text ".to_owned(),
                    content: "edited text".to_owned(),
                    ..openplotva_core::ChatAttachment::default()
                },
                openplotva_core::ChatAttachment {
                    kind: "image".to_owned(),
                    source: "upload".to_owned(),
                    caption: " keep ".to_owned(),
                    content: "edited text".to_owned(),
                    ..openplotva_core::ChatAttachment::default()
                },
            ],
            ..openplotva_core::ChatMessageMeta::default()
        };

        let updated = super::history_text_payload_with_message_update(
            payload.to_string(),
            " edited text ",
            " edited text ",
            meta,
        )?;
        let updated: serde_json::Value = serde_json::from_str(&updated)?;

        assert_eq!(updated["text"], "edited text");
        assert!(updated.get("original_text").is_none());
        assert_eq!(updated["meta"]["sender_id"], 99);
        assert_eq!(updated["meta"]["attachments"][0]["source"], "message");
        assert!(updated["meta"]["attachments"][0].get("caption").is_none());
        assert!(updated["meta"]["attachments"][0].get("content").is_none());
        assert_eq!(updated["meta"]["attachments"][1]["source"], "upload");
        assert_eq!(updated["meta"]["attachments"][1]["caption"], " keep ");
        assert_eq!(updated["meta"]["attachments"][1]["content"], "edited text");
        Ok(())
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
    fn chat_member_sql_matches_go_query_contracts() {
        assert_eq!(
            super::SQL_GET_CHAT_MEMBER,
            "SELECT * FROM chat_members WHERE chat_id = $1 AND user_id = $2"
        );
        assert_eq!(
            super::SQL_LIST_CHAT_MEMBERS,
            "SELECT * FROM chat_members WHERE chat_id = $1"
        );
        assert_eq!(
            super::SQL_UPSERT_CHAT_MEMBER,
            "INSERT INTO chat_members (chat_id, user_id, status, is_anonymous, custom_title, can_be_edited, can_manage_chat, can_delete_messages, can_manage_video_chats, can_restrict_members, can_promote_members, can_change_info, can_invite_users, can_post_messages, can_edit_messages, can_pin_messages, can_manage_topics, can_send_messages, can_send_media_messages, can_send_polls, can_send_other_messages, can_add_web_page_previews, until_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23) ON CONFLICT (chat_id, user_id) DO UPDATE SET status = COALESCE(EXCLUDED.status, chat_members.status), is_anonymous = COALESCE(EXCLUDED.is_anonymous, chat_members.is_anonymous), custom_title = COALESCE(EXCLUDED.custom_title, chat_members.custom_title), can_be_edited = COALESCE(EXCLUDED.can_be_edited, chat_members.can_be_edited), can_manage_chat = COALESCE(EXCLUDED.can_manage_chat, chat_members.can_manage_chat), can_delete_messages = COALESCE(EXCLUDED.can_delete_messages, chat_members.can_delete_messages), can_manage_video_chats = COALESCE(EXCLUDED.can_manage_video_chats, chat_members.can_manage_video_chats), can_restrict_members = COALESCE(EXCLUDED.can_restrict_members, chat_members.can_restrict_members), can_promote_members = COALESCE(EXCLUDED.can_promote_members, chat_members.can_promote_members), can_change_info = COALESCE(EXCLUDED.can_change_info, chat_members.can_change_info), can_invite_users = COALESCE(EXCLUDED.can_invite_users, chat_members.can_invite_users), can_post_messages = COALESCE(EXCLUDED.can_post_messages, chat_members.can_post_messages), can_edit_messages = COALESCE(EXCLUDED.can_edit_messages, chat_members.can_edit_messages), can_pin_messages = COALESCE(EXCLUDED.can_pin_messages, chat_members.can_pin_messages), can_manage_topics = COALESCE(EXCLUDED.can_manage_topics, chat_members.can_manage_topics), can_send_messages = COALESCE(EXCLUDED.can_send_messages, chat_members.can_send_messages), can_send_media_messages = COALESCE(EXCLUDED.can_send_media_messages, chat_members.can_send_media_messages), can_send_polls = COALESCE(EXCLUDED.can_send_polls, chat_members.can_send_polls), can_send_other_messages = COALESCE(EXCLUDED.can_send_other_messages, chat_members.can_send_other_messages), can_add_web_page_previews = COALESCE(EXCLUDED.can_add_web_page_previews, chat_members.can_add_web_page_previews), until_date = COALESCE(EXCLUDED.until_date, chat_members.until_date), updated_at = CURRENT_TIMESTAMP"
        );
    }

    #[test]
    fn stored_chat_member_permission_matches_go_group_settings_rule() {
        let creator = super::ChatMemberRecord {
            chat_id: -10042,
            user_id: 42,
            status: super::CHAT_MEMBER_STATUS_CREATOR.to_owned(),
            ..super::ChatMemberRecord::default()
        };
        let promoting_admin = super::ChatMemberRecord {
            chat_id: -10042,
            user_id: 43,
            status: super::CHAT_MEMBER_STATUS_ADMINISTRATOR.to_owned(),
            can_promote_members: Some(true),
            ..super::ChatMemberRecord::default()
        };
        let non_promoting_admin = super::ChatMemberRecord {
            chat_id: -10042,
            user_id: 44,
            status: super::CHAT_MEMBER_STATUS_ADMINISTRATOR.to_owned(),
            can_promote_members: Some(false),
            ..super::ChatMemberRecord::default()
        };

        assert!(super::stored_member_can_open_group_settings(Some(&creator)));
        assert!(super::stored_member_can_open_group_settings(Some(
            &promoting_admin
        )));
        assert!(!super::stored_member_can_open_group_settings(Some(
            &non_promoting_admin
        )));
        assert!(!super::stored_member_can_open_group_settings(None));
    }

    #[test]
    fn stored_admin_chat_member_maps_go_admin_permissions() {
        let admin = super::ChatMemberRecord {
            chat_id: -10042,
            user_id: 42,
            status: super::CHAT_MEMBER_STATUS_ADMINISTRATOR.to_owned(),
            is_anonymous: Some(true),
            custom_title: Some("Boss".to_owned()),
            can_delete_messages: Some(true),
            can_manage_video_chats: Some(true),
            can_restrict_members: Some(true),
            can_promote_members: Some(true),
            can_change_info: Some(true),
            can_invite_users: Some(true),
            can_post_messages: Some(true),
            can_edit_messages: Some(true),
            can_pin_messages: Some(true),
            ..super::ChatMemberRecord::default()
        };
        let regular_member = super::ChatMemberRecord {
            chat_id: -10042,
            user_id: 43,
            status: super::CHAT_MEMBER_STATUS_MEMBER.to_owned(),
            ..super::ChatMemberRecord::default()
        };

        let stored_admin =
            super::stored_admin_chat_member(&admin).expect("admin should map from stored row");

        assert_eq!(stored_admin.user_id, 42);
        assert_eq!(stored_admin.status, super::CHAT_MEMBER_STATUS_ADMINISTRATOR);
        assert_eq!(stored_admin.is_anonymous, Some(true));
        assert_eq!(stored_admin.custom_title.as_deref(), Some("Boss"));
        assert_eq!(stored_admin.can_delete_messages, Some(true));
        assert_eq!(stored_admin.can_manage_video_chats, Some(true));
        assert_eq!(stored_admin.can_promote_members, Some(true));
        assert!(super::stored_admin_chat_member(&regular_member).is_none());
    }

    #[test]
    fn history_edit_delete_storage_contract_matches_go_side_effects() -> Result<(), Box<dyn Error>>
    {
        let updated_payload = super::history_text_payload_with_text(
            br#"{"entry_id":"msg:77","message_id":77,"text":"old","meta":{}}"#,
            "new text",
        )?;
        let empty_text_payload = super::history_text_payload_with_text(
            br#"{"entry_id":"msg:77","message_id":77,"text":"old","meta":{}}"#,
            "",
        )?;

        assert_eq!(
            super::SQL_SELECT_TEXT_HISTORY_ENTRY,
            "SELECT bucket_day, entry_id, payload::text AS payload FROM chat_history_entries WHERE chat_id = $1 AND message_id = $2 AND kind = 'text' ORDER BY occurred_at DESC LIMIT 1"
        );
        assert_eq!(
            super::SQL_UPDATE_HISTORY_ENTRY_PAYLOAD,
            "UPDATE chat_history_entries SET payload = $4::jsonb, updated_at = CURRENT_TIMESTAMP WHERE bucket_day = $1 AND chat_id = $2 AND entry_id = $3"
        );
        assert_eq!(
            super::SQL_DELETE_HISTORY_MESSAGE_ENTRIES,
            "DELETE FROM chat_history_entries WHERE chat_id = $1 AND message_id = $2"
        );
        assert_eq!(
            super::history_cache_key(42),
            "plotva:chat_history_cache:v2:42"
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&updated_payload)?["text"],
            "new text"
        );
        assert!(
            serde_json::from_str::<serde_json::Value>(&empty_text_payload)?
                .get("text")
                .is_none(),
            "Go marshals api.Message.Text with omitempty, so empty edits remove the JSON text field"
        );

        Ok(())
    }

    #[test]
    fn history_upsert_storage_contract_matches_go_service() {
        assert_eq!(
            super::SQL_UPSERT_HISTORY_ENTRY,
            "INSERT INTO chat_history_entries (bucket_day, chat_id, thread_id, message_id, entry_id, kind, role, occurred_at, sender_id, payload) VALUES ($1::date, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb) ON CONFLICT (bucket_day, chat_id, entry_id) DO UPDATE SET thread_id = EXCLUDED.thread_id, message_id = EXCLUDED.message_id, kind = EXCLUDED.kind, role = EXCLUDED.role, occurred_at = EXCLUDED.occurred_at, sender_id = EXCLUDED.sender_id, payload = EXCLUDED.payload, updated_at = CURRENT_TIMESTAMP"
        );
        assert_eq!(
            super::SQL_ENSURE_CHAT_HISTORY_PARTITION,
            "SELECT ensure_chat_history_partition($1::date)"
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
            super::SQL_GET_USER_ID_BY_USERNAME,
            "SELECT id FROM users WHERE username = $1 LIMIT 1"
        );
        assert_eq!(
            super::SQL_UPSERT_CHAT,
            "INSERT INTO chats (id, type, title, username, first_name, last_name, is_forum, active_usernames, available_reactions, bio, has_private_forwards, has_restricted_voice_and_video_messages, join_to_send_messages, join_by_request, description, invite_link, pinned_message, permissions, slow_mode_delay, message_auto_delete_time, has_aggressive_anti_spam_enabled, has_hidden_members, has_protected_content, has_visible_history, sticker_set_name, can_set_sticker_set, linked_chat_id, location) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10, $11, $12, $13, $14, $15, $16, $17::jsonb, $18::jsonb, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28::jsonb) ON CONFLICT (id) DO UPDATE SET type = COALESCE(EXCLUDED.type, chats.type), title = COALESCE(EXCLUDED.title, chats.title), username = COALESCE(EXCLUDED.username, chats.username), first_name = COALESCE(EXCLUDED.first_name, chats.first_name), last_name = COALESCE(EXCLUDED.last_name, chats.last_name), is_forum = COALESCE(EXCLUDED.is_forum, chats.is_forum), active_usernames = COALESCE(EXCLUDED.active_usernames, chats.active_usernames), available_reactions = COALESCE(EXCLUDED.available_reactions, chats.available_reactions), bio = COALESCE(EXCLUDED.bio, chats.bio), has_private_forwards = COALESCE(EXCLUDED.has_private_forwards, chats.has_private_forwards), has_restricted_voice_and_video_messages = COALESCE(EXCLUDED.has_restricted_voice_and_video_messages, chats.has_restricted_voice_and_video_messages), join_to_send_messages = COALESCE(EXCLUDED.join_to_send_messages, chats.join_to_send_messages), join_by_request = COALESCE(EXCLUDED.join_by_request, chats.join_by_request), description = COALESCE(EXCLUDED.description, chats.description), invite_link = COALESCE(EXCLUDED.invite_link, chats.invite_link), pinned_message = COALESCE(EXCLUDED.pinned_message, chats.pinned_message), permissions = COALESCE(EXCLUDED.permissions, chats.permissions), slow_mode_delay = COALESCE(EXCLUDED.slow_mode_delay, chats.slow_mode_delay), message_auto_delete_time = COALESCE(EXCLUDED.message_auto_delete_time, chats.message_auto_delete_time), has_aggressive_anti_spam_enabled = COALESCE(EXCLUDED.has_aggressive_anti_spam_enabled, chats.has_aggressive_anti_spam_enabled), has_hidden_members = COALESCE(EXCLUDED.has_hidden_members, chats.has_hidden_members), has_protected_content = COALESCE(EXCLUDED.has_protected_content, chats.has_protected_content), has_visible_history = COALESCE(EXCLUDED.has_visible_history, chats.has_visible_history), sticker_set_name = COALESCE(EXCLUDED.sticker_set_name, chats.sticker_set_name), can_set_sticker_set = COALESCE(EXCLUDED.can_set_sticker_set, chats.can_set_sticker_set), linked_chat_id = COALESCE(EXCLUDED.linked_chat_id, chats.linked_chat_id), location = COALESCE(EXCLUDED.location, chats.location), updated = CURRENT_TIMESTAMP"
        );
    }

    #[test]
    fn chat_settings_sql_matches_go_permission_contracts() {
        let _settings = openplotva_core::ChatSettings::defaults(42);
        assert_eq!(
            super::SQL_GET_CHAT_TYPE,
            "SELECT type FROM chats WHERE id = $1"
        );
        assert_eq!(
            super::SQL_GET_CHAT_SETTINGS,
            "SELECT chat_id, mood_alignment, custom_persona, reactivity_percentage, proactivity_percentage, enable_global_text_reply, enable_global_draw_reply, enable_obscenifier, enable_profanity, enable_greet_joiners, enable_daily_game, daily_game_theme, greeting_html FROM chat_settings WHERE chat_id = $1"
        );
        assert_eq!(
            super::SQL_UPSERT_CHAT_SETTINGS,
            "WITH ensure_chat AS (INSERT INTO chats (id, type) VALUES ($1, COALESCE(NULLIF($14::text, ''), 'private')) ON CONFLICT (id) DO NOTHING) INSERT INTO chat_settings (chat_id, mood_alignment, custom_persona, reactivity_percentage, proactivity_percentage, enable_obscenifier, enable_profanity, enable_greet_joiners, enable_global_text_reply, enable_global_draw_reply, enable_daily_game, daily_game_theme, updated, greeting_html) VALUES ($1, $2, $3, $4, $5, COALESCE($6, TRUE)::boolean, COALESCE($7, TRUE)::boolean, COALESCE($8, FALSE)::boolean, COALESCE($9, TRUE)::boolean, COALESCE($10, TRUE)::boolean, COALESCE($11, TRUE)::boolean, COALESCE($12, 'auto')::text, CURRENT_TIMESTAMP, $13) ON CONFLICT (chat_id) DO UPDATE SET mood_alignment = EXCLUDED.mood_alignment, custom_persona = EXCLUDED.custom_persona, reactivity_percentage = COALESCE(EXCLUDED.reactivity_percentage, chat_settings.reactivity_percentage), proactivity_percentage = COALESCE(EXCLUDED.proactivity_percentage, chat_settings.proactivity_percentage), enable_obscenifier = COALESCE(EXCLUDED.enable_obscenifier, chat_settings.enable_obscenifier), enable_profanity = COALESCE(EXCLUDED.enable_profanity, chat_settings.enable_profanity), enable_greet_joiners = COALESCE(EXCLUDED.enable_greet_joiners, chat_settings.enable_greet_joiners), enable_global_text_reply = COALESCE(EXCLUDED.enable_global_text_reply, chat_settings.enable_global_text_reply), enable_global_draw_reply = COALESCE(EXCLUDED.enable_global_draw_reply, chat_settings.enable_global_draw_reply), enable_daily_game = COALESCE(EXCLUDED.enable_daily_game, chat_settings.enable_daily_game), daily_game_theme = EXCLUDED.daily_game_theme, greeting_html = EXCLUDED.greeting_html, updated = CURRENT_TIMESTAMP"
        );
    }

    #[test]
    fn payment_storage_sql_matches_go_query_contracts() {
        assert_eq!(
            super::SQL_CREATE_SUBSCRIPTION,
            "INSERT INTO subscriptions (user_id, telegram_payment_charge_id, provider_payment_charge_id, expires_at) VALUES ($1, $2, $3, $4) ON CONFLICT (telegram_payment_charge_id) DO UPDATE SET expires_at = COALESCE(EXCLUDED.expires_at, subscriptions.expires_at), updated_at = CURRENT_TIMESTAMP RETURNING *"
        );
        assert_eq!(
            super::SQL_GET_ACTIVE_SUBSCRIPTION,
            "SELECT * FROM subscriptions WHERE user_id = $1 AND expires_at > NOW() AND canceled_at IS NULL AND refunded_at IS NULL AND telegram_payment_charge_id NOT LIKE 'admin_grant_%' ORDER BY created_at DESC, id DESC LIMIT 1"
        );
        assert_eq!(
            super::SQL_LIST_SUBSCRIPTIONS_BY_USER,
            "SELECT * FROM subscriptions WHERE user_id = $1 ORDER BY created_at DESC, id DESC"
        );
        assert_eq!(
            super::SQL_GET_SUBSCRIPTION_BY_TELEGRAM_PAYMENT_CHARGE_ID,
            "SELECT * FROM subscriptions WHERE telegram_payment_charge_id = $1 LIMIT 1"
        );
        assert_eq!(
            super::SQL_DELETE_SUBSCRIPTION,
            "DELETE FROM subscriptions WHERE id = $1 RETURNING *"
        );
        assert_eq!(
            super::SQL_EXPIRE_SUBSCRIPTION,
            "UPDATE subscriptions SET expires_at = $2, updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *"
        );
        assert_eq!(
            super::SQL_MARK_SUBSCRIPTION_CANCELED,
            "UPDATE subscriptions SET canceled_at = COALESCE(canceled_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *"
        );
        assert_eq!(
            super::SQL_MARK_SUBSCRIPTION_REFUNDED,
            "UPDATE subscriptions SET refunded_at = COALESCE(refunded_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *"
        );
        assert_eq!(
            super::SQL_CREATE_DONATION,
            "INSERT INTO donations (user_id, telegram_payment_charge_id, provider_payment_charge_id, amount_stars) VALUES ($1, $2, $3, $4) ON CONFLICT (telegram_payment_charge_id) DO NOTHING RETURNING *"
        );
        assert_eq!(
            super::SQL_GET_DONATION_BY_TELEGRAM_PAYMENT_CHARGE_ID,
            "SELECT * FROM donations WHERE telegram_payment_charge_id = $1 LIMIT 1"
        );
        assert_eq!(
            super::SQL_DELETE_DONATION,
            "DELETE FROM donations WHERE id = $1 RETURNING *"
        );
    }

    #[tokio::test]
    async fn live_payment_store_round_trips_when_postgres_dsn_is_set() -> Result<(), Box<dyn Error>>
    {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let user_id = 9_002_000_000_000_i64 + i64::try_from(suffix % 1_000_000)?;
        let charge_id = format!("test_subscription_{suffix}");
        let duplicate_provider_id = format!("provider_updated_{suffix}");
        let donation_charge_id = format!("test_donation_{suffix}");
        let admin_charge_id = format!("admin_grant_test_{suffix}");
        let store = super::PostgresPaymentStore::new(pool.clone());
        let identity_store = super::PostgresVirtualMessageStore::new(pool.clone());

        identity_store
            .upsert_user_state(&openplotva_core::UserState::new(
                user_id,
                "Payment Tester",
                None,
                None,
                None,
                None,
            ))
            .await?;

        let first_expiry = time::OffsetDateTime::from_unix_timestamp(1_800_000_000)?;
        let second_expiry = first_expiry + time::Duration::days(30);

        let result: Result<(), Box<dyn Error>> = async {
            let subscription = store
                .create_subscription(super::SubscriptionCreate {
                    user_id,
                    telegram_payment_charge_id: &charge_id,
                    provider_payment_charge_id: "provider-original",
                    expires_at: first_expiry,
                })
                .await?;
            assert_eq!(subscription.user_id, user_id);
            assert_eq!(subscription.telegram_payment_charge_id, charge_id);
            assert_eq!(subscription.provider_payment_charge_id, "provider-original");
            assert_eq!(subscription.expires_at, first_expiry);

            let duplicate = store
                .create_subscription(super::SubscriptionCreate {
                    user_id,
                    telegram_payment_charge_id: &charge_id,
                    provider_payment_charge_id: &duplicate_provider_id,
                    expires_at: second_expiry,
                })
                .await?;
            assert_eq!(duplicate.id, subscription.id);
            assert_eq!(
                duplicate.provider_payment_charge_id, "provider-original",
                "Go only refreshes expires_at on duplicate Telegram charge IDs"
            );
            assert_eq!(duplicate.expires_at, second_expiry);

            let loaded_subscription = store
                .get_subscription_by_telegram_payment_charge_id(&charge_id)
                .await?
                .ok_or_else(|| std::io::Error::other("subscription should be readable"))?;
            assert_eq!(loaded_subscription.id, subscription.id);

            let expired = store
                .expire_subscription(subscription.id, first_expiry)
                .await?;
            assert_eq!(expired.expires_at, first_expiry);

            let active = store
                .get_active_subscription(user_id)
                .await?
                .ok_or_else(|| std::io::Error::other("subscription should be active"))?;
            assert_eq!(active.id, subscription.id);

            let canceled = store.mark_subscription_canceled(subscription.id).await?;
            assert!(canceled.canceled_at.is_some());
            assert!(store.get_active_subscription(user_id).await?.is_none());

            let admin_grant = store
                .create_subscription(super::SubscriptionCreate {
                    user_id,
                    telegram_payment_charge_id: &admin_charge_id,
                    provider_payment_charge_id: "",
                    expires_at: second_expiry,
                })
                .await?;
            assert!(store.get_active_subscription(user_id).await?.is_none());

            let refunded = store.mark_subscription_refunded(admin_grant.id).await?;
            assert!(refunded.refunded_at.is_some());

            let subscriptions = store.list_subscriptions_by_user(user_id).await?;
            assert_eq!(subscriptions.len(), 2);
            assert_eq!(subscriptions[0].id, admin_grant.id);
            assert_eq!(subscriptions[1].id, subscription.id);

            let donation = store
                .create_donation(super::DonationCreate {
                    user_id,
                    telegram_payment_charge_id: &donation_charge_id,
                    provider_payment_charge_id: "provider-donation",
                    amount_stars: 123,
                })
                .await?;
            assert_eq!(donation.user_id, user_id);
            assert_eq!(donation.amount_stars, 123);
            assert!(matches!(
                store
                    .create_donation(super::DonationCreate {
                        user_id,
                        telegram_payment_charge_id: &donation_charge_id,
                        provider_payment_charge_id: "provider-donation-duplicate",
                        amount_stars: 456,
                    })
                    .await,
                Err(super::StorageError::Postgres {
                    source: sqlx::Error::RowNotFound
                })
            ));

            let loaded_donation = store
                .get_donation_by_telegram_payment_charge_id(&donation_charge_id)
                .await?
                .ok_or_else(|| std::io::Error::other("donation should be readable"))?;
            assert_eq!(loaded_donation.id, donation.id);

            let deleted = store.delete_donation(donation.id).await?;
            assert_eq!(deleted.id, donation.id);
            assert!(
                store
                    .get_donation_by_telegram_payment_charge_id(&donation_charge_id)
                    .await?
                    .is_none()
            );

            let deleted_admin_grant = store.delete_subscription(admin_grant.id).await?;
            assert_eq!(deleted_admin_grant.id, admin_grant.id);
            let deleted_subscription = store.delete_subscription(subscription.id).await?;
            assert_eq!(deleted_subscription.id, subscription.id);

            Ok(())
        }
        .await;

        let _ = sqlx::query("DELETE FROM donations WHERE telegram_payment_charge_id = $1")
            .bind(&donation_charge_id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM subscriptions WHERE telegram_payment_charge_id = ANY($1)")
            .bind([charge_id.as_str(), admin_charge_id.as_str()])
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await;
        result
    }

    #[test]
    fn vip_storage_sql_matches_go_query_contracts() {
        assert_eq!(
            super::SQL_UPSERT_VIP_CACHE,
            "INSERT INTO vip_cache (user_id, is_vip, expires_at) VALUES ($1, $2, $3) ON CONFLICT (user_id) DO UPDATE SET is_vip = COALESCE(EXCLUDED.is_vip, vip_cache.is_vip), expires_at = COALESCE(EXCLUDED.expires_at, vip_cache.expires_at), updated_at = CURRENT_TIMESTAMP"
        );
        assert_eq!(
            super::SQL_CREATE_VIP_EVENT,
            "SELECT id, user_id, event_type, delta_seconds, effective_expires_at, subscription_id, actor_user_id, reason, created_at FROM vip_create_event($1, $2, $3, $4, $5, $6)"
        );
        assert_eq!(
            super::SQL_GET_VIP_SUMMARY_BY_USER,
            "SELECT id AS latest_event_id, user_id, event_type AS latest_event_type, delta_seconds AS latest_delta_seconds, effective_expires_at, effective_expires_at > CURRENT_TIMESTAMP AS is_active, CASE WHEN effective_expires_at > CURRENT_TIMESTAMP THEN FLOOR(EXTRACT(EPOCH FROM (effective_expires_at - CURRENT_TIMESTAMP)))::bigint ELSE 0::bigint END AS remaining_seconds, subscription_id AS latest_subscription_id, actor_user_id AS latest_actor_user_id, reason AS latest_reason, created_at AS latest_created_at FROM vip_events WHERE user_id = $1 ORDER BY id DESC LIMIT 1"
        );
        assert_eq!(
            super::SQL_LIST_VIP_EVENTS_BY_USER,
            "SELECT ve.id, ve.user_id, ve.event_type, ve.delta_seconds, ve.effective_expires_at, ve.subscription_id, ve.actor_user_id, actor.username AS actor_username, actor.first_name AS actor_first_name, ve.reason, ve.created_at, s.telegram_payment_charge_id, s.provider_payment_charge_id, s.expires_at AS subscription_expires_at, s.canceled_at AS subscription_canceled_at, s.refunded_at AS subscription_refunded_at FROM vip_events ve LEFT JOIN users actor ON actor.id = ve.actor_user_id LEFT JOIN subscriptions s ON s.id = ve.subscription_id WHERE ve.user_id = $1 ORDER BY ve.id DESC"
        );
        assert_eq!(
            super::SQL_GET_VIP_CACHE,
            "SELECT * FROM vip_cache WHERE user_id = $1 AND expires_at > CURRENT_TIMESTAMP LIMIT 1"
        );
        assert_eq!(
            super::SQL_DELETE_VIP_CACHE,
            "DELETE FROM vip_cache WHERE user_id = $1"
        );
        assert_eq!(
            super::SQL_CLEANUP_EXPIRED_VIP_CACHE,
            "DELETE FROM vip_cache WHERE expires_at <= CURRENT_TIMESTAMP RETURNING user_id"
        );
    }

    #[tokio::test]
    async fn live_vip_store_round_trips_when_postgres_dsn_is_set() -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let user_id = 9_003_000_000_000_i64 + i64::try_from(suffix % 1_000_000)?;
        let actor_id = user_id + 1;
        let charge_id = format!("test_vip_subscription_{suffix}");
        let identity_store = super::PostgresVirtualMessageStore::new(pool.clone());
        let payment_store = super::PostgresPaymentStore::new(pool.clone());
        let vip_store = super::PostgresVipStore::new(pool.clone());
        let future_expiry = time::OffsetDateTime::from_unix_timestamp(1_900_000_000)?;
        let past_expiry = time::OffsetDateTime::from_unix_timestamp(1_600_000_000)?;

        identity_store
            .upsert_user_state(&openplotva_core::UserState::new(
                user_id,
                "VIP Tester",
                None,
                None,
                None,
                None,
            ))
            .await?;
        identity_store
            .upsert_user_state(&openplotva_core::UserState::new(
                actor_id,
                "Admin Actor",
                None,
                Some("admin_actor".to_owned()),
                None,
                None,
            ))
            .await?;

        let result: Result<(), Box<dyn Error>> = async {
            vip_store
                .upsert_vip_cache(super::VipCacheUpsert {
                    user_id,
                    is_vip: true,
                    expires_at: future_expiry,
                })
                .await?;
            let cache = vip_store
                .get_vip_cache(user_id)
                .await?
                .ok_or_else(|| std::io::Error::other("future VIP cache should be readable"))?;
            assert_eq!(cache.user_id, user_id);
            assert!(cache.is_vip);
            assert_eq!(cache.expires_at, future_expiry);

            vip_store.delete_vip_cache(user_id).await?;
            assert!(vip_store.get_vip_cache(user_id).await?.is_none());
            vip_store
                .upsert_vip_cache(super::VipCacheUpsert {
                    user_id: actor_id,
                    is_vip: true,
                    expires_at: past_expiry,
                })
                .await?;
            assert!(vip_store.get_vip_cache(actor_id).await?.is_none());
            assert!(
                vip_store
                    .cleanup_expired_vip_cache()
                    .await?
                    .contains(&actor_id)
            );

            let subscription = payment_store
                .create_subscription(super::SubscriptionCreate {
                    user_id,
                    telegram_payment_charge_id: &charge_id,
                    provider_payment_charge_id: "provider-vip",
                    expires_at: future_expiry,
                })
                .await?;
            let payment_event = vip_store
                .create_vip_event(super::VipEventCreate {
                    user_id,
                    event_type: openplotva_core::VIP_EVENT_TYPE_PAYMENT,
                    delta_seconds: openplotva_core::vip_days_to_seconds(30),
                    subscription_id: Some(subscription.id),
                    actor_user_id: None,
                    reason: Some("payment charge"),
                })
                .await?;
            let duplicate_payment_event = vip_store
                .create_vip_event(super::VipEventCreate {
                    user_id,
                    event_type: openplotva_core::VIP_EVENT_TYPE_PAYMENT,
                    delta_seconds: openplotva_core::vip_days_to_seconds(30),
                    subscription_id: Some(subscription.id),
                    actor_user_id: None,
                    reason: Some("payment duplicate"),
                })
                .await?;
            assert_eq!(
                duplicate_payment_event.id, payment_event.id,
                "Go vip_create_event returns the existing subscription-scoped event on conflicts"
            );

            let adjustment = vip_store
                .create_vip_event(super::VipEventCreate {
                    user_id,
                    event_type: openplotva_core::VIP_EVENT_TYPE_ADMIN_ADJUSTMENT,
                    delta_seconds: -3_600,
                    subscription_id: None,
                    actor_user_id: Some(actor_id),
                    reason: Some("admin correction"),
                })
                .await?;

            let summary = vip_store
                .get_vip_summary_by_user(user_id)
                .await?
                .ok_or_else(|| std::io::Error::other("VIP summary should be readable"))?;
            assert_eq!(summary.latest_event_id, adjustment.id);
            assert_eq!(
                summary.latest_event_type,
                openplotva_core::VIP_EVENT_TYPE_ADMIN_ADJUSTMENT
            );
            assert_eq!(summary.latest_actor_user_id, Some(actor_id));
            assert_eq!(summary.latest_reason, "admin correction");

            let events = vip_store.list_vip_events_by_user(user_id).await?;
            assert_eq!(events.len(), 2);
            assert_eq!(events[0].id, adjustment.id);
            assert_eq!(events[0].actor_username.as_deref(), Some("admin_actor"));
            assert_eq!(events[0].actor_first_name.as_deref(), Some("Admin Actor"));
            assert_eq!(events[1].id, payment_event.id);
            assert_eq!(events[1].subscription_id, Some(subscription.id));
            assert_eq!(
                events[1].telegram_payment_charge_id.as_deref(),
                Some(charge_id.as_str())
            );
            assert_eq!(
                events[1].provider_payment_charge_id.as_deref(),
                Some("provider-vip")
            );
            assert_eq!(events[1].subscription_expires_at, Some(future_expiry));

            Ok(())
        }
        .await;

        let _ = sqlx::query("DELETE FROM vip_events WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM vip_cache WHERE user_id = ANY($1)")
            .bind([user_id, actor_id])
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM subscriptions WHERE telegram_payment_charge_id = $1")
            .bind(&charge_id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM users WHERE id = ANY($1)")
            .bind([user_id, actor_id])
            .execute(&pool)
            .await;
        result
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
    async fn live_history_store_updates_and_deletes_when_postgres_dsn_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        let store = super::PostgresHistoryStore::new(pool.clone());
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let chat_id = -9_001_222_333_444_i64;
        let message_id = i32::try_from(suffix % 1_000_000_000)?;
        let entry_id = format!("msg:{message_id}");
        let occurred_at = time::OffsetDateTime::now_utc();
        let bucket_day = occurred_at.date();
        let payload = serde_json::json!({
            "entry_id": entry_id,
            "role": "user",
            "kind": "text",
            "timestamp": "2026-05-20T00:00:00Z",
            "message_id": message_id,
            "date": occurred_at.unix_timestamp(),
            "chat": {"id": chat_id, "type": "private"},
            "text": "old text",
            "meta": {}
        })
        .to_string();

        sqlx::query("SELECT ensure_chat_history_partition($1::date)")
            .bind(bucket_day)
            .execute(&pool)
            .await?;
        let _ = store.delete_message_entries(chat_id, message_id).await;

        let result: Result<(), Box<dyn Error>> = async {
            store
                .upsert_history_entry(super::HistoryEntryUpsert {
                    bucket_day,
                    chat_id,
                    thread_id: 0,
                    message_id,
                    entry_id: &entry_id,
                    kind: "text",
                    role: "user",
                    occurred_at,
                    sender_id: 100,
                    payload: payload.as_bytes(),
                })
                .await?;

            assert!(store
                .update_text_entry(chat_id, message_id, "new text")
                .await?);
            let updated_payload: String = sqlx::query_scalar(
                "SELECT payload::text FROM chat_history_entries WHERE chat_id = $1 AND message_id = $2 AND kind = 'text'",
            )
            .bind(chat_id)
            .bind(message_id)
            .fetch_one(&pool)
            .await?;
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&updated_payload)?["text"],
                "new text"
            );

            assert_eq!(store.delete_message_entries(chat_id, message_id).await?, 1);
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM chat_history_entries WHERE chat_id = $1 AND message_id = $2",
            )
            .bind(chat_id)
            .bind(message_id)
            .fetch_one(&pool)
            .await?;
            assert_eq!(count, 0);

            Ok(())
        }
        .await;

        let _ = store.delete_message_entries(chat_id, message_id).await;
        result
    }

    #[tokio::test]
    async fn live_chat_settings_store_round_trips_when_postgres_dsn_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        let store = super::PostgresChatSettingsStore::new(pool.clone());
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let chat_id = -9_001_444_555_666_i64 - i64::try_from(suffix % 1_000_000)?;
        let mut settings = openplotva_core::ChatSettings::defaults(chat_id);
        settings.enable_global_draw_reply = false;
        let update = openplotva_core::ChatSettingsUpdate {
            chat_id,
            chat_type: "supergroup".to_owned(),
            mood_alignment: settings.mood_alignment.clone(),
            custom_persona: settings.custom_persona.clone(),
            reactivity_percentage: settings.reactivity_percentage,
            proactivity_percentage: settings.proactivity_percentage,
            enable_global_text_reply: settings.enable_global_text_reply,
            enable_global_draw_reply: settings.enable_global_draw_reply,
            enable_obscenifier: settings.enable_obscenifier,
            enable_profanity: settings.enable_profanity,
            enable_greet_joiners: settings.enable_greet_joiners,
            enable_daily_game: settings.enable_daily_game.unwrap_or(true),
            daily_game_theme: settings.daily_game_theme.clone().unwrap_or_default(),
            greeting_html: None,
        };

        let result: Result<(), Box<dyn Error>> = async {
            store.upsert_chat_settings(&update).await?;
            let loaded = store
                .get_chat_settings(chat_id)
                .await?
                .ok_or_else(|| std::io::Error::other("inserted settings were not readable"))?;

            assert_eq!(loaded.chat_id, chat_id);
            assert!(loaded.enable_global_text_reply);
            assert!(!loaded.enable_global_draw_reply);
            assert_eq!(loaded.daily_game_theme.as_deref(), Some("auto"));

            Ok(())
        }
        .await;

        let _ = sqlx::query("DELETE FROM chat_settings WHERE chat_id = $1")
            .bind(chat_id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM chats WHERE id = $1")
            .bind(chat_id)
            .execute(&pool)
            .await;
        result
    }

    #[tokio::test]
    async fn live_chat_member_store_round_trips_when_postgres_dsn_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        let identity_store = super::PostgresVirtualMessageStore::new(pool.clone());
        let store = super::PostgresChatMemberStore::new(pool.clone());
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let chat_id = -9_001_555_666_777_i64 - i64::try_from(suffix % 1_000_000)?;
        let user_id = 9_001_000_000_i64 + i64::try_from(suffix % 1_000_000)?;

        let result: Result<(), Box<dyn Error>> = async {
            identity_store
                .upsert_chat_state(&openplotva_core::ChatState::new(
                    chat_id,
                    "supergroup",
                    Some("member test".to_owned()),
                    None,
                    None,
                    None,
                    None,
                ))
                .await?;
            identity_store
                .upsert_user_state(&openplotva_core::UserState::new(
                    user_id, "Ada", None, None, None, None,
                ))
                .await?;
            store
                .upsert_chat_member(&super::ChatMemberUpsert {
                    chat_id,
                    user_id,
                    status: super::CHAT_MEMBER_STATUS_ADMINISTRATOR.to_owned(),
                    can_promote_members: Some(true),
                    can_delete_messages: Some(true),
                    ..super::ChatMemberUpsert::default()
                })
                .await?;
            store
                .upsert_chat_member(&super::ChatMemberUpsert {
                    chat_id,
                    user_id,
                    status: super::CHAT_MEMBER_STATUS_ADMINISTRATOR.to_owned(),
                    can_delete_messages: Some(false),
                    ..super::ChatMemberUpsert::default()
                })
                .await?;

            let member = store
                .get_chat_member(chat_id, user_id)
                .await?
                .ok_or_else(|| std::io::Error::other("inserted member was not readable"))?;

            assert_eq!(member.chat_id, chat_id);
            assert_eq!(member.user_id, user_id);
            assert_eq!(member.status, super::CHAT_MEMBER_STATUS_ADMINISTRATOR);
            assert_eq!(
                member.can_promote_members,
                Some(true),
                "Go COALESCE upsert preserves nullable permissions when later writes omit them"
            );
            assert_eq!(member.can_delete_messages, Some(false));
            let members = store.list_chat_members(chat_id).await?;
            assert_eq!(members.len(), 1);
            assert_eq!(members[0].user_id, user_id);

            Ok(())
        }
        .await;

        let _ = sqlx::query("DELETE FROM chats WHERE id = $1")
            .bind(chat_id)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await;
        result
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

    #[tokio::test]
    async fn live_redis_chat_admin_cache_store_round_trips_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let client = redis::Client::open(url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let store = super::RedisChatAdminCacheStore::with_key_prefix(
            client.clone(),
            format!("openplotva:test:chat_admins:{suffix}:"),
        );
        let chat_id = -900_124_i64;
        let mut connection = client.get_multiplexed_async_connection().await?;

        let result: Result<(), Box<dyn Error>> = async {
            store
                .set_chat_admin_ids(chat_id, &[42, 43], Duration::from_secs(30 * 60))
                .await?;

            assert_eq!(store.chat_admin_ids(chat_id).await?, Some(vec![42, 43]));
            Ok(())
        }
        .await;

        let _: i64 = redis::cmd("DEL")
            .arg(store.key_for_chat(chat_id))
            .query_async(&mut connection)
            .await?;
        result
    }

    #[tokio::test]
    async fn live_redis_ephemeral_message_store_round_trips_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let client = redis::Client::open(url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let store = super::RedisEphemeralMessageStore::with_key_prefix(
            client.clone(),
            format!("openplotva:test:ephemeral_messages:{suffix}:"),
        );
        let expires_at =
            time::OffsetDateTime::from_unix_timestamp_nanos(1_710_000_000_123_456_789)?;
        let message = super::EphemeralMessage {
            chat_id: -900_125,
            message_id: 77,
            expires_at,
        };
        let mut connection = client.get_multiplexed_async_connection().await?;

        let result: Result<(), Box<dyn Error>> = async {
            store
                .set_ephemeral_message(
                    &message,
                    super::ephemeral_redis_ttl(
                        Duration::from_secs(60),
                        super::EPHEMERAL_DEFAULT_CLEANUP_INTERVAL,
                    ),
                )
                .await?;

            assert_eq!(
                store
                    .ephemeral_message(message.chat_id, message.message_id)
                    .await?,
                Some(message.clone())
            );
            store
                .delete_ephemeral_messages(std::slice::from_ref(&message))
                .await?;
            assert_eq!(
                store
                    .ephemeral_message(message.chat_id, message.message_id)
                    .await?,
                None
            );
            Ok(())
        }
        .await;

        let _: i64 = redis::cmd("DEL")
            .arg(store.key_for_message(message.chat_id, message.message_id))
            .query_async(&mut connection)
            .await?;
        result
    }
}
