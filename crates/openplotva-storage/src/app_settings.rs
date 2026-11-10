//! Persisted application settings, usable in an existing transaction.

use crate::StorageError;
use crate::database as sqlx;
use sqlx::{Executor, Postgres};

pub async fn app_setting<'a>(
    executor: impl Executor<'a, Database = Postgres>,
    key: &'a str,
) -> Result<Option<String>, StorageError> {
    Ok(
        sqlx::query_scalar("SELECT value FROM app_settings WHERE key = $1")
            .bind(key)
            .fetch_optional(executor)
            .await?,
    )
}

pub async fn upsert_app_setting<'a>(
    executor: impl Executor<'a, Database = Postgres>,
    key: &'a str,
    value: &'a str,
) -> Result<(), StorageError> {
    sqlx::query("INSERT INTO app_settings (key, value, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()")
        .bind(key).bind(value).execute(executor).await?;
    Ok(())
}

pub async fn app_setting_present<'a>(
    executor: impl Executor<'a, Database = Postgres>,
    key: &'a str,
) -> Result<bool, StorageError> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM app_settings WHERE key = $1)")
            .bind(key)
            .fetch_one(executor)
            .await?,
    )
}
