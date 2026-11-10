//! Stored Telegram entity profiles shared by HTTP and runtime API adapters.

use crate::StorageError;
use crate::database as sqlx;
use sqlx::{FromRow, PgPool, Row};
use time::OffsetDateTime;

#[derive(Clone, Debug, serde::Deserialize)]
pub struct TelegramUserProfile {
    pub id: i64,
    pub is_premium: Option<bool>,
    pub first_name: String,
    pub last_name: Option<String>,
    pub username: Option<String>,
    pub language_code: Option<String>,
    pub is_vip: Option<bool>,
    pub settings: Option<serde_json::Value>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub discovered: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub updated: Option<OffsetDateTime>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for TelegramUserProfile {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            is_premium: row.try_get("is_premium")?,
            first_name: row.try_get("first_name")?,
            last_name: row.try_get("last_name")?,
            username: row.try_get("username")?,
            language_code: row.try_get("language_code")?,
            is_vip: row.try_get("is_vip")?,
            settings: row.try_get("settings")?,
            discovered: row.try_get("discovered")?,
            updated: row.try_get("updated")?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct TelegramChatProfile {
    pub id: i64,
    pub chat_type: String,
    pub title: Option<String>,
    pub username: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub is_forum: Option<bool>,
    pub active_usernames: Option<serde_json::Value>,
    pub available_reactions: Option<serde_json::Value>,
    pub bio: Option<String>,
    pub has_private_forwards: Option<bool>,
    pub has_restricted_voice_and_video_messages: Option<bool>,
    pub join_to_send_messages: Option<bool>,
    pub join_by_request: Option<bool>,
    pub description: Option<String>,
    pub invite_link: Option<String>,
    pub pinned_message: Option<serde_json::Value>,
    pub permissions: Option<serde_json::Value>,
    pub slow_mode_delay: Option<i64>,
    pub message_auto_delete_time: Option<i64>,
    pub has_aggressive_anti_spam_enabled: Option<bool>,
    pub has_hidden_members: Option<bool>,
    pub has_protected_content: Option<bool>,
    pub has_visible_history: Option<bool>,
    pub sticker_set_name: Option<String>,
    pub can_set_sticker_set: Option<bool>,
    pub linked_chat_id: Option<i64>,
    pub location: Option<serde_json::Value>,
    pub discovered: Option<OffsetDateTime>,
    pub updated: Option<OffsetDateTime>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for TelegramChatProfile {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            chat_type: row.try_get("type")?,
            title: row.try_get("title")?,
            username: row.try_get("username")?,
            first_name: row.try_get("first_name")?,
            last_name: row.try_get("last_name")?,
            is_forum: row.try_get("is_forum")?,
            active_usernames: row.try_get("active_usernames")?,
            available_reactions: row.try_get("available_reactions")?,
            bio: row.try_get("bio")?,
            has_private_forwards: row.try_get("has_private_forwards")?,
            has_restricted_voice_and_video_messages: row
                .try_get("has_restricted_voice_and_video_messages")?,
            join_to_send_messages: row.try_get("join_to_send_messages")?,
            join_by_request: row.try_get("join_by_request")?,
            description: row.try_get("description")?,
            invite_link: row.try_get("invite_link")?,
            pinned_message: row.try_get("pinned_message")?,
            permissions: row.try_get("permissions")?,
            slow_mode_delay: row.try_get("slow_mode_delay")?,
            message_auto_delete_time: row.try_get("message_auto_delete_time")?,
            has_aggressive_anti_spam_enabled: row.try_get("has_aggressive_anti_spam_enabled")?,
            has_hidden_members: row.try_get("has_hidden_members")?,
            has_protected_content: row.try_get("has_protected_content")?,
            has_visible_history: row.try_get("has_visible_history")?,
            sticker_set_name: row.try_get("sticker_set_name")?,
            can_set_sticker_set: row.try_get("can_set_sticker_set")?,
            linked_chat_id: row.try_get("linked_chat_id")?,
            location: row.try_get("location")?,
            discovered: row.try_get("discovered")?,
            updated: row.try_get("updated")?,
        })
    }
}

const SQL_COUNT_USERS_FILTERED: &str = r#"
SELECT COUNT(*)::int
FROM telegram_users_effective
WHERE (
    $1::text IS NULL
    OR username ILIKE '%' || $1::text || '%'
    OR first_name ILIKE '%' || $1::text || '%'
    OR last_name ILIKE '%' || $1::text || '%'
)"#;

const SQL_LIST_USERS_FILTERED: &str = r#"
SELECT *
FROM telegram_users_effective
WHERE (
    $1::text IS NULL
    OR username ILIKE '%' || $1::text || '%'
    OR first_name ILIKE '%' || $1::text || '%'
    OR last_name ILIKE '%' || $1::text || '%'
)
ORDER BY id
LIMIT $2
OFFSET $3"#;

const SQL_GET_RUNTIME_USER: &str = "SELECT * FROM telegram_users_effective WHERE id = $1";
const SQL_GET_RUNTIME_USER_BY_USERNAME: &str =
    "SELECT * FROM telegram_users_effective WHERE username = $1 LIMIT 1";
const SQL_GET_RUNTIME_USER_ID_BY_USERNAME: &str =
    "SELECT id FROM telegram_users_effective WHERE lower(username) = lower($1) LIMIT 1";

const SQL_COUNT_CHATS_FILTERED: &str = r#"
SELECT COUNT(*)::int
FROM telegram_chats_effective
WHERE (
    $1::text IS NULL
    OR CAST(id AS text) LIKE '%' || $1::text || '%'
    OR title ILIKE '%' || $1::text || '%'
    OR username ILIKE '%' || $1::text || '%'
    OR first_name ILIKE '%' || $1::text || '%'
    OR last_name ILIKE '%' || $1::text || '%'
)"#;

const SQL_LIST_CHATS_FILTERED: &str = r#"
SELECT *
FROM telegram_chats_effective
WHERE (
    $1::text IS NULL
    OR CAST(id AS text) LIKE '%' || $1::text || '%'
    OR title ILIKE '%' || $1::text || '%'
    OR username ILIKE '%' || $1::text || '%'
    OR first_name ILIKE '%' || $1::text || '%'
    OR last_name ILIKE '%' || $1::text || '%'
)
ORDER BY id
LIMIT $2
OFFSET $3"#;

const SQL_COUNT_CHATS_BY_MEMBER: &str = r#"
SELECT COUNT(DISTINCT c.id)::int
FROM telegram_chats_effective c
JOIN telegram_chat_members_effective cm ON c.id = cm.chat_id
WHERE cm.user_id = $1"#;

const SQL_LIST_CHATS_BY_MEMBER: &str = r#"
SELECT DISTINCT c.*
FROM telegram_chats_effective c
JOIN telegram_chat_members_effective cm ON c.id = cm.chat_id
WHERE cm.user_id = $1
ORDER BY c.id
LIMIT $2
OFFSET $3"#;

const SQL_GET_RUNTIME_CHAT: &str = "SELECT * FROM telegram_chats_effective WHERE id = $1";
#[derive(Clone, Debug)]
pub struct PostgresEntityStore {
    pool: PgPool,
}

impl PostgresEntityStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn user(&self, id: i64) -> Result<Option<TelegramUserProfile>, StorageError> {
        Ok(sqlx::query_as(SQL_GET_RUNTIME_USER)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?)
    }

    pub async fn user_by_username(
        &self,
        username: &str,
    ) -> Result<Option<TelegramUserProfile>, StorageError> {
        Ok(sqlx::query_as(SQL_GET_RUNTIME_USER_BY_USERNAME)
            .bind(username)
            .fetch_optional(&self.pool)
            .await?)
    }

    pub async fn user_id_by_username(&self, username: &str) -> Result<Option<i64>, StorageError> {
        Ok(sqlx::query_scalar(SQL_GET_RUNTIME_USER_ID_BY_USERNAME)
            .bind(username)
            .fetch_optional(&self.pool)
            .await?)
    }

    pub async fn users(
        &self,
        search: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TelegramUserProfile>, StorageError> {
        Ok(sqlx::query_as(SQL_LIST_USERS_FILTERED)
            .bind(search)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?)
    }

    pub async fn count_users(&self, search: Option<&str>) -> Result<i32, StorageError> {
        Ok(sqlx::query_scalar(SQL_COUNT_USERS_FILTERED)
            .bind(search)
            .fetch_one(&self.pool)
            .await?)
    }

    pub async fn chat(&self, id: i64) -> Result<Option<TelegramChatProfile>, StorageError> {
        Ok(sqlx::query_as(SQL_GET_RUNTIME_CHAT)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?)
    }

    pub async fn chats(
        &self,
        search: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TelegramChatProfile>, StorageError> {
        Ok(sqlx::query_as(SQL_LIST_CHATS_FILTERED)
            .bind(search)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?)
    }

    pub async fn count_chats(&self, search: Option<&str>) -> Result<i32, StorageError> {
        Ok(sqlx::query_scalar(SQL_COUNT_CHATS_FILTERED)
            .bind(search)
            .fetch_one(&self.pool)
            .await?)
    }

    pub async fn chats_by_member(
        &self,
        user_id: i64,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TelegramChatProfile>, StorageError> {
        Ok(sqlx::query_as(SQL_LIST_CHATS_BY_MEMBER)
            .bind(user_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?)
    }

    pub async fn count_chats_by_member(&self, user_id: i64) -> Result<i32, StorageError> {
        Ok(sqlx::query_scalar(SQL_COUNT_CHATS_BY_MEMBER)
            .bind(user_id)
            .fetch_one(&self.pool)
            .await?)
    }
}

const SQL_ADMIN_LIST_VIP_USERS: &str = "WITH latest_vip AS (SELECT DISTINCT ON (user_id) id, user_id, event_type, delta_seconds, effective_expires_at, reason, created_at FROM vip_events ORDER BY user_id, id DESC), subscription_stats AS (SELECT user_id, COUNT(*)::bigint AS subscriptions_count, MAX(expires_at) AS latest_subscription_expires_at FROM subscriptions GROUP BY user_id) SELECT u.*, latest_vip.id AS latest_event_id, latest_vip.event_type AS latest_event_type, latest_vip.delta_seconds AS latest_delta_seconds, latest_vip.effective_expires_at AS vip_expires_at, latest_vip.reason AS latest_reason, latest_vip.created_at AS latest_created_at, (latest_vip.effective_expires_at > CURRENT_TIMESTAMP) AS vip_active, CASE WHEN latest_vip.effective_expires_at > CURRENT_TIMESTAMP THEN FLOOR(EXTRACT(EPOCH FROM (latest_vip.effective_expires_at - CURRENT_TIMESTAMP)))::bigint ELSE 0::bigint END AS remaining_seconds, COALESCE(subscription_stats.subscriptions_count, 0)::bigint AS subscriptions_count, subscription_stats.latest_subscription_expires_at AS latest_subscription_expires_at FROM latest_vip JOIN telegram_users_effective u ON u.id = latest_vip.user_id LEFT JOIN subscription_stats ON subscription_stats.user_id = u.id WHERE ($1::text = 'all' OR ($1::text = 'active' AND latest_vip.effective_expires_at > CURRENT_TIMESTAMP) OR ($1::text = 'expired' AND latest_vip.effective_expires_at <= CURRENT_TIMESTAMP)) AND ($2::text IS NULL OR CAST(u.id AS text) LIKE '%' || $2::text || '%' OR u.username ILIKE '%' || $2::text || '%' OR u.first_name ILIKE '%' || $2::text || '%' OR u.last_name ILIKE '%' || $2::text || '%') ORDER BY latest_vip.effective_expires_at DESC, latest_vip.id DESC LIMIT $3 OFFSET $4";

const SQL_ADMIN_SEARCH_CHATS_BY_MEMBER: &str = "SELECT DISTINCT c.* FROM telegram_chats_effective c JOIN telegram_chat_members_effective cm ON c.id = cm.chat_id JOIN telegram_users_effective u ON cm.user_id = u.id WHERE ($1::text IS NULL OR LOWER(u.username) = LOWER($1::text)) AND ($2::bigint IS NULL OR u.id = $2::bigint) ORDER BY c.id LIMIT $3";

#[derive(Clone, Debug)]
pub struct VipUserProfile {
    pub user: TelegramUserProfile,
    pub remaining_seconds: Option<i64>,
    pub vip_active: Option<bool>,
    pub vip_expires_at: Option<OffsetDateTime>,
    pub latest_event_id: Option<i64>,
    pub latest_event_type: Option<String>,
    pub latest_delta_seconds: Option<i64>,
    pub latest_reason: Option<String>,
    pub latest_created_at: Option<OffsetDateTime>,
    pub subscriptions_count: Option<i64>,
    pub latest_subscription_expires_at: Option<OffsetDateTime>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for VipUserProfile {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            user: <TelegramUserProfile as sqlx::FromRow<sqlx::postgres::PgRow>>::from_row(row)?,
            remaining_seconds: row.try_get("remaining_seconds")?,
            vip_active: row.try_get("vip_active")?,
            vip_expires_at: row.try_get("vip_expires_at")?,
            latest_event_id: row.try_get("latest_event_id")?,
            latest_event_type: row.try_get("latest_event_type")?,
            latest_delta_seconds: row.try_get("latest_delta_seconds")?,
            latest_reason: row.try_get("latest_reason")?,
            latest_created_at: row.try_get("latest_created_at")?,
            subscriptions_count: row.try_get("subscriptions_count")?,
            latest_subscription_expires_at: row.try_get("latest_subscription_expires_at")?,
        })
    }
}

impl PostgresEntityStore {
    pub async fn vip_users(
        &self,
        status: &str,
        search: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<VipUserProfile>, StorageError> {
        Ok(sqlx::query_as(SQL_ADMIN_LIST_VIP_USERS)
            .bind(status)
            .bind(search)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?)
    }
    pub async fn search_chats_by_member(
        &self,
        username: Option<&str>,
        user_id: Option<i64>,
        limit: i64,
    ) -> Result<Vec<TelegramChatProfile>, StorageError> {
        Ok(sqlx::query_as(SQL_ADMIN_SEARCH_CHATS_BY_MEMBER)
            .bind(username)
            .bind(user_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?)
    }
}

#[derive(Clone, Debug)]
pub struct ChatMemberProfile {
    pub membership: crate::ChatMemberRecord,
    pub created_at: Option<OffsetDateTime>,
    pub updated_at: Option<OffsetDateTime>,
    pub last_message_at: Option<OffsetDateTime>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for ChatMemberProfile {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            membership:
                <crate::ChatMemberRecord as sqlx::FromRow<sqlx::postgres::PgRow>>::from_row(row)?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
            last_message_at: row.try_get("last_message_at")?,
        })
    }
}

impl PostgresEntityStore {
    pub async fn members_with_users(
        &self,
        chat_id: i64,
    ) -> Result<Vec<(ChatMemberProfile, Option<TelegramUserProfile>)>, StorageError> {
        let rows = sqlx::query("SELECT member.*, CASE WHEN profile.id IS NOT NULL THEN to_jsonb(profile) END AS user_profile FROM telegram_chat_members_effective AS member LEFT JOIN telegram_users_effective AS profile ON profile.id = member.user_id WHERE member.chat_id = $1")
            .bind(chat_id).fetch_all(&self.pool).await?;
        rows.iter()
            .map(|row| {
                let member = ChatMemberProfile::from_row(row)?;
                let user = row
                    .try_get::<Option<sqlx::types::Json<TelegramUserProfile>>, _>("user_profile")?
                    .map(|user| user.0);
                Ok((member, user))
            })
            .collect()
    }
}

const SQL_ADMIN_ENSURE_USER: &str = "INSERT INTO users (id, first_name, is_premium, is_vip) VALUES ($1, $2, FALSE, FALSE) ON CONFLICT (id) DO NOTHING";

const SQL_ADMIN_SAFE_DELETE_USER: &str = "WITH deleted_user_stage AS (DELETE FROM telegram_users_stage WHERE user_id = $1 RETURNING user_id), deleted_member_stage AS (DELETE FROM telegram_chat_members_stage WHERE user_id = $1 RETURNING user_id), deleted_activity_stage AS (DELETE FROM telegram_activity_stage WHERE user_id = $1 RETURNING user_id), deleted_memberships AS (DELETE FROM chat_members WHERE user_id = $1 RETURNING user_id), deleted_vip AS (DELETE FROM vip_cache WHERE user_id = $1 RETURNING user_id) DELETE FROM users WHERE id = $1";

const SQL_ADMIN_GET_CHAT_TYPE: &str = "SELECT type FROM telegram_chats_effective WHERE id = $1";

impl PostgresEntityStore {
    pub async fn ensure_user(&self, user_id: i64, first_name: &str) -> Result<(), StorageError> {
        sqlx::query(SQL_ADMIN_ENSURE_USER)
            .bind(user_id)
            .bind(first_name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    pub async fn delete_user(&self, user_id: i64) -> Result<u64, StorageError> {
        Ok(sqlx::query(SQL_ADMIN_SAFE_DELETE_USER)
            .bind(user_id)
            .execute(&self.pool)
            .await?
            .rows_affected())
    }
    pub async fn chat_type(&self, chat_id: i64) -> Result<Option<String>, StorageError> {
        Ok(sqlx::query_scalar(SQL_ADMIN_GET_CHAT_TYPE)
            .bind(chat_id)
            .fetch_optional(&self.pool)
            .await?)
    }
}

#[derive(Clone, Debug)]
pub struct StoredChatSettings {
    pub chat_id: i64,
    pub mood_alignment: Option<String>,
    pub custom_persona: Option<String>,
    pub updated: Option<OffsetDateTime>,
    pub reactivity_percentage: i32,
    pub proactivity_percentage: i32,
    pub enable_global_text_reply: bool,
    pub enable_global_draw_reply: bool,
    pub enable_obscenifier: bool,
    pub enable_profanity: bool,
    pub enable_greet_joiners: bool,
    pub enable_daily_game: Option<bool>,
    pub daily_game_theme: Option<String>,
    pub greeting_html: Option<String>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for StoredChatSettings {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            chat_id: row.try_get("chat_id")?,
            mood_alignment: row.try_get("mood_alignment")?,
            custom_persona: row.try_get("custom_persona")?,
            updated: row.try_get("updated")?,
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
}

#[derive(Clone, Debug)]
pub struct StoredChatPermissions {
    pub chat_id: i64,
    pub status: Option<String>,
    pub can_manage_chat: Option<bool>,
    pub can_delete_messages: Option<bool>,
    pub can_manage_video_chats: Option<bool>,
    pub can_restrict_members: Option<bool>,
    pub can_promote_members: Option<bool>,
    pub can_change_info: Option<bool>,
    pub can_invite_users: Option<bool>,
    pub can_post_messages: Option<bool>,
    pub can_edit_messages: Option<bool>,
    pub can_pin_messages: Option<bool>,
    pub can_manage_topics: Option<bool>,
    pub can_send_messages: Option<bool>,
    pub can_send_media_messages: Option<bool>,
    pub can_send_polls: Option<bool>,
    pub can_send_other_messages: Option<bool>,
    pub can_add_web_page_previews: Option<bool>,
    pub last_checked_at: Option<OffsetDateTime>,
    pub last_error_at: Option<OffsetDateTime>,
    pub error_message: Option<String>,
    pub created_at: Option<OffsetDateTime>,
    pub updated_at: Option<OffsetDateTime>,
    pub error_count: Option<i32>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for StoredChatPermissions {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            chat_id: row.try_get("chat_id")?,
            status: row.try_get("status")?,
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
            last_checked_at: row.try_get("last_checked_at")?,
            last_error_at: row.try_get("last_error_at")?,
            error_message: row.try_get("error_message")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
            error_count: row.try_get("error_count")?,
        })
    }
}

const SQL_ADMIN_GET_CHAT_SETTINGS: &str = "SELECT * FROM chat_settings WHERE chat_id = $1";

const SQL_ADMIN_GET_CHAT_PERMISSIONS: &str =
    "SELECT * FROM chat_permissions WHERE chat_id = $1 LIMIT 1";

const SQL_ADMIN_COUNT_CHAT_MEMBERS: &str =
    "SELECT COUNT(*)::bigint AS count FROM telegram_chat_members_effective WHERE chat_id = $1";

impl PostgresEntityStore {
    pub async fn chat_settings(
        &self,
        chat_id: i64,
    ) -> Result<Option<StoredChatSettings>, StorageError> {
        Ok(sqlx::query_as(SQL_ADMIN_GET_CHAT_SETTINGS)
            .bind(chat_id)
            .fetch_optional(&self.pool)
            .await?)
    }
    pub async fn chat_permissions(
        &self,
        chat_id: i64,
    ) -> Result<Option<StoredChatPermissions>, StorageError> {
        Ok(sqlx::query_as(SQL_ADMIN_GET_CHAT_PERMISSIONS)
            .bind(chat_id)
            .fetch_optional(&self.pool)
            .await?)
    }
    pub async fn member_count(&self, chat_id: i64) -> Result<i64, StorageError> {
        Ok(sqlx::query_scalar(SQL_ADMIN_COUNT_CHAT_MEMBERS)
            .bind(chat_id)
            .fetch_one(&self.pool)
            .await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database as sqlx;

    #[tokio::test]
    async fn postgres_entity_rows_preserve_field_names_flattening_and_defaults()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(dsn) = std::env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&dsn)
            .await?;
        crate::run_migrations_on(&pool).await?;
        let user_json = serde_json::json!({"id": 42, "first_name": "Row test", "is_vip": false, "settings": {"language": "ru"}});
        let user: TelegramUserProfile = sqlx::query_as(
            "SELECT * FROM jsonb_populate_record(NULL::telegram_users_effective, $1)",
        )
        .bind(&user_json)
        .fetch_one(&pool)
        .await?;
        assert_eq!(user.id, 42);
        assert_eq!(user.first_name, "Row test");
        assert_eq!(user.is_vip, Some(false));
        assert_eq!(user.settings, Some(serde_json::json!({"language": "ru"})));
        assert_eq!(user.updated, None);

        let chat: TelegramChatProfile = sqlx::query_as(
            "SELECT * FROM jsonb_populate_record(NULL::telegram_chats_effective, $1)",
        ).bind(serde_json::json!({"id": -10042, "type": "supergroup", "title": "Row test", "is_forum": true})).fetch_one(&pool).await?;
        assert_eq!(chat.chat_type, "supergroup");
        assert_eq!(chat.id, -10042);
        assert_eq!(chat.is_forum, Some(true));

        let vip: VipUserProfile = sqlx::query_as(
            "SELECT profile.*, 60::bigint AS remaining_seconds, true AS vip_active, NULL::timestamptz AS vip_expires_at, 7::bigint AS latest_event_id, 'purchase'::text AS latest_event_type, 60::bigint AS latest_delta_seconds, NULL::text AS latest_reason, NULL::timestamptz AS latest_created_at, 1::bigint AS subscriptions_count, NULL::timestamptz AS latest_subscription_expires_at FROM jsonb_populate_record(NULL::telegram_users_effective, $1) AS profile",
        ).bind(&user_json).fetch_one(&pool).await?;
        assert_eq!(vip.user.id, 42);
        assert_eq!(vip.user.first_name, user.first_name);
        assert_eq!(vip.remaining_seconds, Some(60));
        assert_eq!(vip.latest_event_id, Some(7));

        let membership = serde_json::json!({"chat_id": -10042, "user_id": 42, "status": "member", "can_delete_messages": false});
        let member: ChatMemberProfile = sqlx::query_as(
            "SELECT * FROM jsonb_populate_record(NULL::telegram_chat_members_effective, $1)",
        )
        .bind(&membership)
        .fetch_one(&pool)
        .await?;
        assert_eq!(member.membership.user_id, 42);
        assert_eq!(member.membership.status, "member");
        assert_eq!(member.membership.can_delete_messages, Some(false));
        assert_eq!(member.membership.telegram_observed_at, None);

        let settings: StoredChatSettings = sqlx::query_as(
            "SELECT * FROM jsonb_populate_record(NULL::chat_settings, $1)",
        ).bind(serde_json::json!({"chat_id": -10042, "reactivity_percentage": 75, "proactivity_percentage": 20, "enable_global_text_reply": true, "enable_global_draw_reply": false, "enable_obscenifier": false, "enable_profanity": true, "enable_greet_joiners": false})).fetch_one(&pool).await?;
        assert_eq!(settings.reactivity_percentage, 75);
        assert!(settings.enable_global_text_reply);
        assert_eq!(settings.daily_game_theme, None);

        let permissions: StoredChatPermissions = sqlx::query_as(
            "SELECT * FROM jsonb_populate_record(NULL::chat_permissions, $1)",
        )
        .bind(serde_json::json!({"chat_id": -10042, "can_delete_messages": true, "error_count": 0}))
        .fetch_one(&pool)
        .await?;
        assert_eq!(permissions.can_delete_messages, Some(true));
        assert_eq!(permissions.error_count, Some(0));

        let missing = sqlx::query_as::<_, TelegramUserProfile>("SELECT 42::bigint AS other_id")
            .fetch_one(&pool)
            .await;
        assert!(matches!(missing, Err(sqlx::Error::ColumnNotFound(_))));
        let invalid = sqlx::query_as::<_, TelegramUserProfile>("SELECT 'invalid'::text AS id")
            .fetch_one(&pool)
            .await;
        assert!(matches!(invalid, Err(sqlx::Error::ColumnDecode { .. })));
        let invalid_default = sqlx::query_as::<_, crate::ChatMemberRecord>(
            "SELECT member.*, 'invalid'::text AS telegram_observed_at FROM jsonb_populate_record(NULL::telegram_chat_members_effective, $1) AS member",
        ).bind(&membership).fetch_one(&pool).await;
        assert!(matches!(
            invalid_default,
            Err(sqlx::Error::ColumnDecode { .. })
        ));
        Ok(())
    }
}
