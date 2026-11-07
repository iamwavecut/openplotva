//! Runtime API entity readers backed by Postgres.

use openplotva_server::{
    RuntimeChatConnectionData, RuntimeChatData, RuntimeChatMemberData,
    RuntimeChatMemberWithUserData, RuntimeChatsFilter, RuntimeEntityReader,
    RuntimeEntityReaderFuture, RuntimeSubscriptionData, RuntimeUserConnectionData, RuntimeUserData,
    RuntimeUserDetailsData, RuntimeUserLookup, RuntimeUsersFilter, RuntimeVipCacheData,
    RuntimeVipEventData, RuntimeVipSummaryData,
};
use openplotva_storage::entities::{
    ChatMemberProfile, PostgresEntityStore, TelegramChatProfile, TelegramUserProfile,
};
use openplotva_storage::{
    PostgresPaymentStore, PostgresVipStore, SubscriptionRecord, VipCacheRecord, VipEventListRecord,
    VipSummaryRecord,
};
use sqlx::PgPool;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// SQLx-backed runtime API core entity reader.
#[derive(Clone, Debug)]
pub struct PostgresRuntimeEntityReader {
    entities: PostgresEntityStore,
    payments: PostgresPaymentStore,
    vip: PostgresVipStore,
}

impl PostgresRuntimeEntityReader {
    /// Build a reader over an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self {
            entities: PostgresEntityStore::new(pool.clone()),
            payments: PostgresPaymentStore::new(pool.clone()),
            vip: PostgresVipStore::new(pool.clone()),
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
    ) -> Result<RuntimeUserConnectionData, openplotva_storage::StorageError> {
        let search = optional_search(&filter.q);
        let count = self.entities.count_users(search.as_deref()).await?;
        let rows = self
            .entities
            .users(
                search.as_deref(),
                i64::from(filter.limit),
                i64::from(filter.offset),
            )
            .await?;
        Ok(RuntimeUserConnectionData {
            count,
            offset: filter.offset,
            limit: filter.limit,
            items: rows.into_iter().map(runtime_user_from_record).collect(),
        })
    }

    async fn get_user_details(
        &self,
        lookup: RuntimeUserLookup,
    ) -> Result<Option<RuntimeUserDetailsData>, openplotva_storage::StorageError> {
        let row = match lookup {
            RuntimeUserLookup::Id(id) => self.entities.user(id).await?,
            RuntimeUserLookup::Username(username) => {
                self.entities.user_by_username(&username).await?
            }
        };
        let Some(row) = row else {
            return Ok(None);
        };
        let user = runtime_user_from_record(row);
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
    ) -> Result<RuntimeChatConnectionData, openplotva_storage::StorageError> {
        if filter.member_username.is_some() || filter.member_user_id.is_some() {
            return self.list_chats_by_member(filter).await;
        }
        let search = optional_search(&filter.q);
        let count = self.entities.count_chats(search.as_deref()).await?;
        let rows = self
            .entities
            .chats(
                search.as_deref(),
                i64::from(filter.limit),
                i64::from(filter.offset),
            )
            .await?;
        Ok(RuntimeChatConnectionData {
            count,
            offset: filter.offset,
            limit: filter.limit,
            items: rows.into_iter().map(runtime_chat_from_record).collect(),
        })
    }

    async fn list_chats_by_member(
        &self,
        filter: RuntimeChatsFilter,
    ) -> Result<RuntimeChatConnectionData, openplotva_storage::StorageError> {
        let Some(member_user_id) = self.chat_member_filter_user_id(&filter).await? else {
            return Ok(RuntimeChatConnectionData {
                count: 0,
                offset: filter.offset,
                limit: filter.limit,
                items: Vec::new(),
            });
        };
        let count = self.entities.count_chats_by_member(member_user_id).await?;
        let rows = self
            .entities
            .chats_by_member(
                member_user_id,
                i64::from(filter.limit),
                i64::from(filter.offset),
            )
            .await?;
        Ok(RuntimeChatConnectionData {
            count,
            offset: filter.offset,
            limit: filter.limit,
            items: rows.into_iter().map(runtime_chat_from_record).collect(),
        })
    }

    async fn chat_member_filter_user_id(
        &self,
        filter: &RuntimeChatsFilter,
    ) -> Result<Option<i64>, openplotva_storage::StorageError> {
        let username = filter
            .member_username
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let username_was_requested = username.is_some();
        let username_user_id = match username {
            Some(username) => self.entities.user_id_by_username(username).await?,
            None => None,
        };
        match (filter.member_user_id, username_user_id) {
            (Some(member_user_id), Some(username_user_id))
                if member_user_id == username_user_id =>
            {
                Ok(Some(member_user_id))
            }
            (Some(_), Some(_)) => Ok(None),
            (Some(_), None) if username_was_requested => Ok(None),
            (Some(member_user_id), None) => Ok(Some(member_user_id)),
            (None, Some(username_user_id)) => Ok(Some(username_user_id)),
            (None, None) => Ok(None),
        }
    }

    async fn get_chat(
        &self,
        id: i64,
    ) -> Result<Option<RuntimeChatData>, openplotva_storage::StorageError> {
        Ok(self.entities.chat(id).await?.map(runtime_chat_from_record))
    }

    async fn list_chat_members(
        &self,
        chat_id: i64,
    ) -> Result<Vec<RuntimeChatMemberWithUserData>, openplotva_storage::StorageError> {
        Ok(self
            .entities
            .members_with_users(chat_id)
            .await?
            .into_iter()
            .map(|(member, user)| RuntimeChatMemberWithUserData {
                member: runtime_chat_member_from_record(member),
                user: user.map(runtime_user_from_record),
            })
            .collect())
    }
}

fn runtime_user_from_record(record: TelegramUserProfile) -> RuntimeUserData {
    RuntimeUserData {
        id: record.id,
        is_premium: record.is_premium,
        first_name: record.first_name,
        last_name: record.last_name,
        username: record.username,
        language_code: record.language_code,
        is_vip: record.is_vip,
        discovered_at: format_ts(record.discovered),
        updated_at: format_ts(record.updated),
    }
}

fn runtime_chat_from_record(record: TelegramChatProfile) -> RuntimeChatData {
    RuntimeChatData {
        id: record.id,
        chat_type: record.chat_type,
        title: record.title,
        username: record.username,
        first_name: record.first_name,
        last_name: record.last_name,
        is_forum: record.is_forum,
        description: record.description,
        invite_link: record.invite_link,
        discovered_at: format_ts(record.discovered),
        updated_at: format_ts(record.updated),
    }
}

fn runtime_chat_member_from_record(record: ChatMemberProfile) -> RuntimeChatMemberData {
    RuntimeChatMemberData {
        chat_id: record.membership.chat_id,
        user_id: record.membership.user_id,
        status: record.membership.status,
        is_anonymous: record.membership.is_anonymous,
        custom_title: record.membership.custom_title,
        can_be_edited: record.membership.can_be_edited,
        can_manage_chat: record.membership.can_manage_chat,
        can_delete_messages: record.membership.can_delete_messages,
        can_manage_video_chats: record.membership.can_manage_video_chats,
        can_restrict_members: record.membership.can_restrict_members,
        can_promote_members: record.membership.can_promote_members,
        can_change_info: record.membership.can_change_info,
        can_invite_users: record.membership.can_invite_users,
        can_post_messages: record.membership.can_post_messages,
        can_edit_messages: record.membership.can_edit_messages,
        can_pin_messages: record.membership.can_pin_messages,
        can_manage_topics: record.membership.can_manage_topics,
        created_at: format_ts(record.created_at),
        updated_at: format_ts(record.updated_at),
        last_message_at: format_ts(record.last_message_at),
    }
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

fn error_text(error: openplotva_storage::StorageError) -> String {
    match error {
        openplotva_storage::StorageError::Postgres { source } => source.to_string(),
        error => error.to_string(),
    }
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

    #[tokio::test]
    async fn postgres_entity_profiles_preserve_member_filters_and_username_lookup()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(dsn) = std::env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&dsn)
            .await?;
        openplotva_storage::run_migrations_on(&pool).await?;
        let first = OffsetDateTime::now_utc().unix_timestamp_nanos() as i64;
        let second = first + 1;
        let username = format!("Lookup{first}");
        sqlx::query("INSERT INTO users (id,first_name,username,is_premium) VALUES ($1,'Alice',$2,TRUE),($3,'Bob',NULL,FALSE)")
            .bind(first).bind(&username).bind(second).execute(&pool).await?;
        sqlx::query("INSERT INTO chats (id,type,title,is_forum) VALUES ($1,'supergroup','A',TRUE),($2,'group','B',FALSE)")
            .bind(-first).bind(-second).execute(&pool).await?;
        sqlx::query("INSERT INTO chat_members (chat_id,user_id,status) VALUES ($1,$2,'member'),($3,$4,'member')")
            .bind(-first).bind(first).bind(-second).bind(second).execute(&pool).await?;
        let reader = PostgresRuntimeEntityReader::new(pool.clone());
        let by_id = reader
            .chats(RuntimeChatsFilter {
                member_user_id: Some(second),
                limit: 20,
                ..Default::default()
            })
            .await?;
        assert_eq!(by_id.count, 1);
        assert_eq!(by_id.items[0].id, -second);
        let by_name = reader
            .chats(RuntimeChatsFilter {
                member_username: Some(username.to_uppercase()),
                limit: 20,
                ..Default::default()
            })
            .await?;
        assert_eq!(by_name.count, 1);
        assert_eq!(by_name.items[0].id, -first);
        let mismatch = reader
            .chats(RuntimeChatsFilter {
                member_user_id: Some(second),
                member_username: Some(username.clone()),
                limit: 20,
                ..Default::default()
            })
            .await?;
        assert_eq!(mismatch.count, 0);
        assert!(mismatch.items.is_empty());
        let exact = reader
            .user(RuntimeUserLookup::Username(username.clone()))
            .await?
            .expect("exact username");
        assert_eq!(exact.user.id, first);
        assert_eq!(exact.user.is_premium, Some(true));
        assert!(
            reader
                .user(RuntimeUserLookup::Username(username.to_uppercase()))
                .await?
                .is_none()
        );
        let users = reader
            .users(RuntimeUsersFilter {
                q: username.to_lowercase(),
                limit: 20,
                ..Default::default()
            })
            .await?;
        assert_eq!(users.count, 1);
        assert_eq!(users.items[0].id, first);
        assert_eq!(
            reader.chat(-first).await?.expect("chat").is_forum,
            Some(true)
        );
        let members = reader.chat_members(-first).await?;
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].member.user_id, first);
        assert_eq!(members[0].user.as_ref().expect("member profile").id, first);
        sqlx::query("DELETE FROM chat_members WHERE user_id = ANY($1)")
            .bind(vec![first, second])
            .execute(&pool)
            .await?;
        sqlx::query("DELETE FROM users WHERE id = ANY($1)")
            .bind(vec![first, second])
            .execute(&pool)
            .await?;
        sqlx::query("DELETE FROM chats WHERE id = ANY($1)")
            .bind(vec![-first, -second])
            .execute(&pool)
            .await?;
        Ok(())
    }
}
