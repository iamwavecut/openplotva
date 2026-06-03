//! Runtime API entity readers backed by Postgres.

use std::collections::HashMap;

use openplotva_server::{
    RuntimeChatConnectionData, RuntimeChatData, RuntimeChatMemberData,
    RuntimeChatMemberWithUserData, RuntimeChatsFilter, RuntimeEntityReader,
    RuntimeEntityReaderFuture, RuntimeSubscriptionData, RuntimeUserConnectionData, RuntimeUserData,
    RuntimeUserDetailsData, RuntimeUserLookup, RuntimeUsersFilter, RuntimeVipCacheData,
    RuntimeVipEventData, RuntimeVipSummaryData,
};
use openplotva_storage::{
    PostgresPaymentStore, PostgresVipStore, SubscriptionRecord, VipCacheRecord, VipEventListRecord,
    VipSummaryRecord,
};
use sqlx::{PgPool, Row};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const SQL_COUNT_USERS_FILTERED: &str = r#"
SELECT COUNT(*)::int
FROM users
WHERE (
    $1::text IS NULL
    OR username ILIKE '%' || $1::text || '%'
    OR first_name ILIKE '%' || $1::text || '%'
    OR last_name ILIKE '%' || $1::text || '%'
)"#;

const SQL_LIST_USERS_FILTERED: &str = r#"
SELECT *
FROM users
WHERE (
    $1::text IS NULL
    OR username ILIKE '%' || $1::text || '%'
    OR first_name ILIKE '%' || $1::text || '%'
    OR last_name ILIKE '%' || $1::text || '%'
)
ORDER BY id
LIMIT $2
OFFSET $3"#;

const SQL_GET_RUNTIME_USER: &str = "SELECT * FROM users WHERE id = $1";
const SQL_GET_RUNTIME_USER_BY_USERNAME: &str = "SELECT * FROM users WHERE username = $1 LIMIT 1";
const SQL_LIST_RUNTIME_USERS_BY_IDS: &str = "SELECT * FROM users WHERE id = ANY($1::bigint[])";

const SQL_COUNT_CHATS_FILTERED: &str = r#"
SELECT COUNT(*)::int
FROM chats
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
FROM chats
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
FROM chats c
JOIN chat_members cm ON c.id = cm.chat_id
JOIN users u ON cm.user_id = u.id
WHERE ($1::text IS NULL OR LOWER(u.username) = LOWER($1::text))
  AND ($2::bigint IS NULL OR u.id = $2::bigint)"#;

const SQL_LIST_CHATS_BY_MEMBER: &str = r#"
SELECT DISTINCT c.*
FROM chats c
JOIN chat_members cm ON c.id = cm.chat_id
JOIN users u ON cm.user_id = u.id
WHERE ($1::text IS NULL OR LOWER(u.username) = LOWER($1::text))
  AND ($2::bigint IS NULL OR u.id = $2::bigint)
ORDER BY c.id
LIMIT $3
OFFSET $4"#;

const SQL_GET_RUNTIME_CHAT: &str = "SELECT * FROM chats WHERE id = $1";
const SQL_LIST_RUNTIME_CHAT_MEMBERS: &str = "SELECT * FROM chat_members WHERE chat_id = $1";

/// SQLx-backed runtime API core entity reader.
#[derive(Clone, Debug)]
pub struct PostgresRuntimeEntityReader {
    pool: PgPool,
    payments: PostgresPaymentStore,
    vip: PostgresVipStore,
}

impl PostgresRuntimeEntityReader {
    /// Build a reader over an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self {
            payments: PostgresPaymentStore::new(pool.clone()),
            vip: PostgresVipStore::new(pool.clone()),
            pool,
        }
    }
}

impl RuntimeEntityReader for PostgresRuntimeEntityReader {
    fn users<'a>(
        &'a self,
        filter: RuntimeUsersFilter,
    ) -> RuntimeEntityReaderFuture<'a, RuntimeUserConnectionData> {
        Box::pin(async move { self.list_users(filter).await.map_err(error_text) })
    }

    fn user<'a>(
        &'a self,
        lookup: RuntimeUserLookup,
    ) -> RuntimeEntityReaderFuture<'a, Option<RuntimeUserDetailsData>> {
        Box::pin(async move { self.get_user_details(lookup).await.map_err(error_text) })
    }

    fn chats<'a>(
        &'a self,
        filter: RuntimeChatsFilter,
    ) -> RuntimeEntityReaderFuture<'a, RuntimeChatConnectionData> {
        Box::pin(async move { self.list_chats(filter).await.map_err(error_text) })
    }

    fn chat<'a>(&'a self, id: i64) -> RuntimeEntityReaderFuture<'a, Option<RuntimeChatData>> {
        Box::pin(async move { self.get_chat(id).await.map_err(error_text) })
    }

    fn chat_members<'a>(
        &'a self,
        chat_id: i64,
    ) -> RuntimeEntityReaderFuture<'a, Vec<RuntimeChatMemberWithUserData>> {
        Box::pin(async move { self.list_chat_members(chat_id).await.map_err(error_text) })
    }
}

impl PostgresRuntimeEntityReader {
    async fn list_users(
        &self,
        filter: RuntimeUsersFilter,
    ) -> Result<RuntimeUserConnectionData, sqlx::Error> {
        let search = optional_search(&filter.q);
        let count = sqlx::query_scalar::<_, i32>(SQL_COUNT_USERS_FILTERED)
            .bind(search.as_deref())
            .fetch_one(&self.pool)
            .await?;
        let rows = sqlx::query(SQL_LIST_USERS_FILTERED)
            .bind(search.as_deref())
            .bind(filter.limit)
            .bind(filter.offset)
            .fetch_all(&self.pool)
            .await?;
        Ok(RuntimeUserConnectionData {
            count,
            offset: filter.offset,
            limit: filter.limit,
            items: rows
                .into_iter()
                .map(runtime_user_from_row)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    async fn get_user_details(
        &self,
        lookup: RuntimeUserLookup,
    ) -> Result<Option<RuntimeUserDetailsData>, sqlx::Error> {
        let row = match lookup {
            RuntimeUserLookup::Id(id) => {
                sqlx::query(SQL_GET_RUNTIME_USER)
                    .bind(id)
                    .fetch_optional(&self.pool)
                    .await?
            }
            RuntimeUserLookup::Username(username) => {
                sqlx::query(SQL_GET_RUNTIME_USER_BY_USERNAME)
                    .bind(username)
                    .fetch_optional(&self.pool)
                    .await?
            }
        };
        let Some(row) = row else {
            return Ok(None);
        };
        let user = runtime_user_from_row(row)?;
        let subscription = self
            .payments
            .get_active_subscription(user.id)
            .await
            .ok()
            .flatten()
            .map(runtime_subscription_from_record);
        let vip = self
            .vip
            .get_vip_cache(user.id)
            .await
            .ok()
            .flatten()
            .map(runtime_vip_cache_from_record);
        let vip_summary = self
            .vip
            .get_vip_summary_by_user(user.id)
            .await
            .ok()
            .flatten()
            .and_then(runtime_vip_summary_from_record);
        let vip_events = self
            .vip
            .list_vip_events_by_user(user.id)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(runtime_vip_event_from_record)
            .collect();
        let subscriptions = self
            .payments
            .list_subscriptions_by_user(user.id)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(runtime_subscription_from_record)
            .collect();
        Ok(Some(RuntimeUserDetailsData {
            user,
            subscription,
            vip,
            vip_summary,
            vip_events,
            subscriptions,
        }))
    }

    async fn list_chats(
        &self,
        filter: RuntimeChatsFilter,
    ) -> Result<RuntimeChatConnectionData, sqlx::Error> {
        if filter.member_username.is_some() || filter.member_user_id.is_some() {
            return self.list_chats_by_member(filter).await;
        }
        let search = optional_search(&filter.q);
        let count = sqlx::query_scalar::<_, i32>(SQL_COUNT_CHATS_FILTERED)
            .bind(search.as_deref())
            .fetch_one(&self.pool)
            .await?;
        let rows = sqlx::query(SQL_LIST_CHATS_FILTERED)
            .bind(search.as_deref())
            .bind(filter.limit)
            .bind(filter.offset)
            .fetch_all(&self.pool)
            .await?;
        Ok(RuntimeChatConnectionData {
            count,
            offset: filter.offset,
            limit: filter.limit,
            items: rows
                .into_iter()
                .map(runtime_chat_from_row)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    async fn list_chats_by_member(
        &self,
        filter: RuntimeChatsFilter,
    ) -> Result<RuntimeChatConnectionData, sqlx::Error> {
        let count = sqlx::query_scalar::<_, i32>(SQL_COUNT_CHATS_BY_MEMBER)
            .bind(filter.member_username.as_deref())
            .bind(filter.member_user_id)
            .fetch_one(&self.pool)
            .await?;
        let rows = sqlx::query(SQL_LIST_CHATS_BY_MEMBER)
            .bind(filter.member_username.as_deref())
            .bind(filter.member_user_id)
            .bind(filter.limit)
            .bind(filter.offset)
            .fetch_all(&self.pool)
            .await?;
        Ok(RuntimeChatConnectionData {
            count,
            offset: filter.offset,
            limit: filter.limit,
            items: rows
                .into_iter()
                .map(runtime_chat_from_row)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    async fn get_chat(&self, id: i64) -> Result<Option<RuntimeChatData>, sqlx::Error> {
        let row = sqlx::query(SQL_GET_RUNTIME_CHAT)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(runtime_chat_from_row).transpose()
    }

    async fn list_chat_members(
        &self,
        chat_id: i64,
    ) -> Result<Vec<RuntimeChatMemberWithUserData>, sqlx::Error> {
        let rows = sqlx::query(SQL_LIST_RUNTIME_CHAT_MEMBERS)
            .bind(chat_id)
            .fetch_all(&self.pool)
            .await?;
        let members = rows
            .into_iter()
            .map(runtime_chat_member_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let users = self.users_by_id(&members).await?;
        Ok(members
            .into_iter()
            .map(|member| {
                let user = users.get(&member.user_id).cloned();
                RuntimeChatMemberWithUserData { member, user }
            })
            .collect())
    }

    async fn users_by_id(
        &self,
        members: &[RuntimeChatMemberData],
    ) -> Result<HashMap<i64, RuntimeUserData>, sqlx::Error> {
        let mut ids = members
            .iter()
            .map(|member| member.user_id)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let rows = sqlx::query(SQL_LIST_RUNTIME_USERS_BY_IDS)
            .bind(&ids)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(runtime_user_from_row)
            .map(|result| result.map(|user| (user.id, user)))
            .collect()
    }
}

fn runtime_user_from_row(row: sqlx::postgres::PgRow) -> Result<RuntimeUserData, sqlx::Error> {
    Ok(RuntimeUserData {
        id: row.try_get("id")?,
        is_premium: row.try_get("is_premium")?,
        first_name: row.try_get("first_name")?,
        last_name: row.try_get("last_name")?,
        username: row.try_get("username")?,
        language_code: row.try_get("language_code")?,
        is_vip: row.try_get("is_vip")?,
        discovered_at: format_ts(row.try_get("discovered")?),
        updated_at: format_ts(row.try_get("updated")?),
    })
}

fn runtime_chat_from_row(row: sqlx::postgres::PgRow) -> Result<RuntimeChatData, sqlx::Error> {
    Ok(RuntimeChatData {
        id: row.try_get("id")?,
        chat_type: row.try_get("type")?,
        title: row.try_get("title")?,
        username: row.try_get("username")?,
        first_name: row.try_get("first_name")?,
        last_name: row.try_get("last_name")?,
        is_forum: row.try_get("is_forum")?,
        description: row.try_get("description")?,
        invite_link: row.try_get("invite_link")?,
        discovered_at: format_ts(row.try_get("discovered")?),
        updated_at: format_ts(row.try_get("updated")?),
    })
}

fn runtime_chat_member_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<RuntimeChatMemberData, sqlx::Error> {
    Ok(RuntimeChatMemberData {
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
        created_at: format_ts(row.try_get("created_at")?),
        updated_at: format_ts(row.try_get("updated_at")?),
        last_message_at: format_ts(row.try_get("last_message_at")?),
    })
}

fn runtime_subscription_from_record(subscription: SubscriptionRecord) -> RuntimeSubscriptionData {
    let status = subscription_status(&subscription, OffsetDateTime::now_utc());
    RuntimeSubscriptionData {
        id: subscription.id,
        user_id: subscription.user_id,
        telegram_payment_charge_id: subscription.telegram_payment_charge_id,
        provider_payment_charge_id: subscription.provider_payment_charge_id,
        expires_at: format_ts(Some(subscription.expires_at)),
        created_at: format_ts(Some(subscription.created_at)),
        updated_at: format_ts(Some(subscription.updated_at)),
        canceled_at: format_ts(subscription.canceled_at),
        refunded_at: format_ts(subscription.refunded_at),
        status,
    }
}

fn runtime_vip_cache_from_record(vip: VipCacheRecord) -> RuntimeVipCacheData {
    RuntimeVipCacheData {
        user_id: vip.user_id,
        is_vip: vip.is_vip,
        expires_at: format_ts(Some(vip.expires_at)),
        created_at: format_ts(Some(vip.created_at)),
        updated_at: format_ts(Some(vip.updated_at)),
    }
}

fn runtime_vip_summary_from_record(summary: VipSummaryRecord) -> Option<RuntimeVipSummaryData> {
    let now = OffsetDateTime::now_utc();
    Some(RuntimeVipSummaryData {
        active: summary.is_active && now < summary.effective_expires_at,
        has_history: true,
        expires_at: format_ts(Some(summary.effective_expires_at)),
        remaining_seconds: summary.remaining_seconds.to_string(),
        remaining_days: vip_display_days_left_at(summary.effective_expires_at, now),
        latest_event_id: Some(summary.latest_event_id),
        latest_event_type: non_empty(summary.latest_event_type),
        latest_reason: non_empty(summary.latest_reason),
        latest_created_at: format_ts(Some(summary.latest_created_at)),
    })
}

fn runtime_vip_event_from_record(event: VipEventListRecord) -> RuntimeVipEventData {
    let actor_label = runtime_actor_label(&event);
    let subscription_status = event.subscription_id.map(|_| {
        subscription_status_from_parts(
            event.subscription_expires_at,
            event.subscription_canceled_at,
            event.subscription_refunded_at,
            OffsetDateTime::now_utc(),
        )
    });
    RuntimeVipEventData {
        id: event.id,
        event_type: event.event_type,
        delta_seconds: event.delta_seconds.to_string(),
        delta_days: event.delta_seconds as f64 / openplotva_core::VIP_SECONDS_PER_DAY as f64,
        effective_expires_at: format_ts(Some(event.effective_expires_at)),
        actor_user_id: event.actor_user_id,
        actor_label,
        reason: non_empty(event.reason),
        created_at: format_ts(Some(event.created_at)),
        subscription_id: event.subscription_id,
        telegram_payment_charge_id: event.telegram_payment_charge_id,
        provider_payment_charge_id: event.provider_payment_charge_id,
        subscription_status,
    }
}

fn subscription_status(subscription: &SubscriptionRecord, now: OffsetDateTime) -> String {
    subscription_status_from_parts(
        Some(subscription.expires_at),
        subscription.canceled_at,
        subscription.refunded_at,
        now,
    )
}

fn subscription_status_from_parts(
    expires_at: Option<OffsetDateTime>,
    canceled_at: Option<OffsetDateTime>,
    refunded_at: Option<OffsetDateTime>,
    now: OffsetDateTime,
) -> String {
    if refunded_at.is_some() {
        "refunded".to_owned()
    } else if canceled_at.is_some() {
        "canceled".to_owned()
    } else if expires_at.is_some_and(|expires_at| now < expires_at) {
        "active".to_owned()
    } else {
        "expired".to_owned()
    }
}

fn runtime_actor_label(event: &VipEventListRecord) -> Option<String> {
    if let Some(username) = event
        .actor_username
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        return Some(format!("@{username}"));
    }
    if let Some(first_name) = event
        .actor_first_name
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        return Some(first_name.to_owned());
    }
    event.actor_user_id.map(|id| id.to_string())
}

fn optional_search(value: &str) -> Option<String> {
    non_empty(value.trim().to_owned())
}

fn non_empty(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
}

fn format_ts(value: Option<OffsetDateTime>) -> Option<String> {
    value.and_then(|value| value.format(&Rfc3339).ok())
}

fn vip_display_days_left_at(expires_at: OffsetDateTime, now: OffsetDateTime) -> i32 {
    let remaining = expires_at - now;
    if remaining <= time::Duration::ZERO {
        return 0;
    }
    ((remaining.whole_seconds() as f64) / openplotva_core::VIP_SECONDS_PER_DAY as f64)
        .round()
        .max(0.0) as i32
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_status_matches_go_priority_order() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let future = now + time::Duration::days(1);
        assert_eq!(
            subscription_status_from_parts(Some(future), None, None, now),
            "active"
        );
        assert_eq!(
            subscription_status_from_parts(Some(future), Some(now), None, now),
            "canceled"
        );
        assert_eq!(
            subscription_status_from_parts(Some(future), Some(now), Some(now), now),
            "refunded"
        );
        assert_eq!(
            subscription_status_from_parts(Some(now), None, None, now),
            "expired"
        );
    }

    #[test]
    fn vip_actor_label_matches_go_fallback_order() {
        let mut event = VipEventListRecord {
            id: 1,
            user_id: 10,
            event_type: "admin_adjustment".to_owned(),
            delta_seconds: 0,
            effective_expires_at: OffsetDateTime::UNIX_EPOCH,
            subscription_id: None,
            actor_user_id: Some(42),
            actor_username: Some(" wave ".to_owned()),
            actor_first_name: Some("Alice".to_owned()),
            reason: String::new(),
            created_at: OffsetDateTime::UNIX_EPOCH,
            telegram_payment_charge_id: None,
            provider_payment_charge_id: None,
            subscription_expires_at: None,
            subscription_canceled_at: None,
            subscription_refunded_at: None,
        };
        assert_eq!(runtime_actor_label(&event).as_deref(), Some("@ wave "));
        event.actor_username = None;
        assert_eq!(runtime_actor_label(&event).as_deref(), Some("Alice"));
        event.actor_first_name = None;
        assert_eq!(runtime_actor_label(&event).as_deref(), Some("42"));
    }
}
