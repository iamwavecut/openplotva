use serde::Serialize;
use sqlx::{PgPool, Row};
use time::OffsetDateTime;

use crate::StorageError;

pub const NOTIFICATION_PREFIX: &str = "maintenance-notification:";

#[derive(Clone, Debug, Serialize)]
pub struct MaintenanceNotification {
    pub key: String,
    pub state: String,
    pub telegram_message_id: Option<i64>,
    #[serde(skip)]
    pub recipient_id: i64,
    #[serde(skip)]
    pub body: String,
}

#[derive(Clone)]
pub struct MaintenanceNotificationStore {
    pool: PgPool,
}

impl MaintenanceNotificationStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create(
        &self,
        key: &str,
        recipient_id: i64,
        body: &str,
    ) -> Result<Option<MaintenanceNotification>, StorageError> {
        sqlx::query(
            "INSERT INTO maintenance_notifications (notification_key, recipient_id, body)
             VALUES ($1, $2, $3) ON CONFLICT (notification_key) DO NOTHING",
        )
        .bind(key)
        .bind(recipient_id)
        .bind(body)
        .execute(&self.pool)
        .await?;
        Ok(self
            .get(key)
            .await?
            .filter(|saved| saved.recipient_id == recipient_id && saved.body == body))
    }

    pub async fn get(&self, key: &str) -> Result<Option<MaintenanceNotification>, StorageError> {
        sqlx::query("SELECT * FROM maintenance_notifications WHERE notification_key = $1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await?
            .map(notification_from_row)
            .transpose()
    }

    pub async fn claim_due(
        &self,
        now: OffsetDateTime,
    ) -> Result<Vec<MaintenanceNotification>, StorageError> {
        sqlx::query(
            "UPDATE maintenance_notifications SET state = 'ambiguous', updated_at = $1
             WHERE state = 'sending' AND updated_at < $1 - interval '10 minutes'",
        )
        .bind(now)
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "WITH due AS (
                SELECT notification_key FROM maintenance_notifications
                WHERE state IN ('pending', 'queued') AND available_at <= $1
                ORDER BY available_at LIMIT 20 FOR UPDATE SKIP LOCKED
             ) UPDATE maintenance_notifications n SET state = 'queued',
                updated_at = $1, available_at = $1 + interval '60 seconds'
             FROM due WHERE n.notification_key = due.notification_key RETURNING n.*",
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(notification_from_row)
        .collect()
    }

    pub async fn begin_send(&self, key: &str) -> Result<bool, StorageError> {
        Ok(sqlx::query(
            "UPDATE maintenance_notifications SET state = 'sending', attempts = attempts + 1,
             updated_at = now() WHERE notification_key = $1 AND state = 'queued'",
        )
        .bind(key)
        .execute(&self.pool)
        .await?
        .rows_affected()
            == 1)
    }

    pub async fn finish_send(
        &self,
        key: &str,
        message_id: Option<i64>,
        proven_not_sent: bool,
        terminal: bool,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE maintenance_notifications SET
                state = CASE WHEN $2::BIGINT IS NOT NULL THEN 'sent'
                    WHEN $3 AND NOT $4 THEN 'pending'
                    WHEN $3 THEN 'failed' ELSE 'ambiguous' END,
                telegram_message_id = $2,
                available_at = now() + LEAST(21600, 300 * GREATEST(attempts, 1)) * interval '1 second',
                updated_at = now()
             WHERE notification_key = $1 AND state = 'sending'",
        ).bind(key).bind(message_id).bind(proven_not_sent).bind(terminal).execute(&self.pool).await?;
        Ok(())
    }
}

fn notification_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<MaintenanceNotification, StorageError> {
    Ok(MaintenanceNotification {
        key: row.try_get("notification_key")?,
        state: row.try_get("state")?,
        telegram_message_id: row.try_get("telegram_message_id")?,
        recipient_id: row.try_get("recipient_id")?,
        body: row.try_get("body")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn live_maintenance_notification_receipts_fence_replay_and_ambiguous_delivery()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(dsn) = std::env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            include_str!("../../../migrations/186_maintenance_notifications.up.sql").replace(
                "CREATE TABLE maintenance_notifications",
                "CREATE TEMP TABLE maintenance_notifications",
            ),
        ))
        .execute(&pool)
        .await?;
        let store = MaintenanceNotificationStore::new(pool);
        let key = "a".repeat(64);
        for _ in 0..100 {
            assert!(store.create(&key, 42, "PR created").await?.is_some());
        }
        assert!(store.create(&key, 42, "different effect").await?.is_none());
        assert_eq!(store.claim_due(OffsetDateTime::now_utc()).await?.len(), 1);
        assert!(store.begin_send(&key).await?);
        assert!(!store.begin_send(&key).await?);
        store.finish_send(&key, Some(123), false, false).await?;
        let receipt = store.get(&key).await?.expect("receipt");
        assert_eq!(receipt.state, "sent");
        assert_eq!(receipt.telegram_message_id, Some(123));
        assert!(
            store
                .claim_due(OffsetDateTime::now_utc() + time::Duration::hours(1))
                .await?
                .is_empty()
        );
        let other = "b".repeat(64);
        store.create(&other, 42, "Ready").await?;
        store
            .claim_due(OffsetDateTime::now_utc() + time::Duration::seconds(1))
            .await?;
        assert!(store.begin_send(&other).await?);
        assert!(
            store
                .claim_due(OffsetDateTime::now_utc() + time::Duration::minutes(11))
                .await?
                .is_empty()
        );
        assert_eq!(
            store.get(&other).await?.expect("receipt").state,
            "ambiguous"
        );
        assert!(!store.begin_send(&other).await?);
        let offline = "c".repeat(64);
        store
            .create(&offline, 42, "Bot temporarily offline")
            .await?;
        for _ in 0..7 {
            store
                .claim_due(OffsetDateTime::now_utc() + time::Duration::hours(7))
                .await?;
            assert!(store.begin_send(&offline).await?);
            store.finish_send(&offline, None, true, false).await?;
            assert_eq!(
                store.get(&offline).await?.expect("receipt").state,
                "pending"
            );
        }
        store
            .claim_due(OffsetDateTime::now_utc() + time::Duration::hours(7))
            .await?;
        assert!(store.begin_send(&offline).await?);
        store.finish_send(&offline, Some(124), false, false).await?;
        assert_eq!(
            store
                .get(&offline)
                .await?
                .expect("receipt")
                .telegram_message_id,
            Some(124)
        );
        Ok(())
    }
}
