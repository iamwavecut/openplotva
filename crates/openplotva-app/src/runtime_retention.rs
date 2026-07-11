//! Daily data-retention workers: drop expired `chat_history_entries` partitions
//! and batch-delete aged Telegram update artifacts, `telegram_files`, and
//! `whitecircle_checks` rows. Configurable workers are no-ops when their
//! `retention_days` is `<= 0` (the knob's "disabled" value).
//!
//! Mirrors the proven `runtime_llm` cleanup worker: a drain-then-sleep loop that
//! cancels promptly on the runtime stop signal and never holds a lock across the
//! sleep. The drain inner loop clears a large initial backlog gradually (a small
//! pause between batches) so it does not spike replication lag or autovacuum.

use std::time::Duration;

use openplotva_storage::PostgresTelegramDeliveryStore;
use sqlx::PgPool;
use time::OffsetDateTime;

/// Once per day. Long enough that DDL / bulk deletes stay infrequent.
pub const RETENTION_CLEANUP_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
pub const RETENTION_DELETE_BATCH_SIZE: i64 = 10_000;
/// Small pause between batches during a drain so a large initial backlog does
/// not spike replication lag or autovacuum pressure.
pub const RETENTION_INTER_BATCH_PAUSE: Duration = Duration::from_millis(200);

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PartitionRetentionReport {
    pub enabled: bool,
    pub dropped_partitions: u64,
    pub ticks: u64,
    pub errors: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BatchedRetentionReport {
    pub enabled: bool,
    pub deleted: u64,
    pub ticks: u64,
    pub errors: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TelegramUpdateRetentionReport {
    pub inbox_deleted: u64,
    pub quarantine_deleted: u64,
    pub ticks: u64,
    pub errors: u64,
}

fn current_utc_day_start(now: OffsetDateTime) -> OffsetDateTime {
    now.date().midnight().assume_utc()
}

/// Drop `chat_history_entries` partitions older than `retention_days`, once per
/// `interval`, until `stop` resolves.
pub async fn run_chat_history_partition_retention_worker_until<Stop>(
    pool: PgPool,
    interval: Duration,
    retention_days: i32,
    stop: Stop,
) -> PartitionRetentionReport
where
    Stop: std::future::Future<Output = ()>,
{
    let mut report = PartitionRetentionReport {
        enabled: retention_days > 0,
        ..PartitionRetentionReport::default()
    };
    if !report.enabled {
        return report;
    }
    tokio::pin!(stop);
    loop {
        match openplotva_storage::drop_expired_chat_history_partitions(&pool, retention_days).await
        {
            Ok(dropped) => {
                report.dropped_partitions += dropped.len() as u64;
                if !dropped.is_empty() {
                    tracing::info!(
                        ?dropped,
                        retention_days,
                        "dropped expired chat_history partitions"
                    );
                }
            }
            Err(error) => {
                report.errors += 1;
                tracing::warn!(%error, retention_days, "failed to drop expired chat_history partitions");
            }
        }
        report.ticks += 1;
        let sleep = tokio::time::sleep(interval);
        tokio::pin!(sleep);
        tokio::select! {
            () = &mut stop => break,
            () = &mut sleep => {}
        }
    }
    report
}

async fn run_batched_retention_worker_until<Stop, F, Fut>(
    label: &'static str,
    interval: Duration,
    retention_days: i32,
    batch_size: i64,
    inter_batch: Duration,
    mut delete_batch: F,
    stop: Stop,
) -> BatchedRetentionReport
where
    Stop: std::future::Future<Output = ()>,
    F: FnMut(i64) -> Fut,
    Fut: std::future::Future<Output = Result<u64, sqlx::Error>>,
{
    let mut report = BatchedRetentionReport {
        enabled: retention_days > 0,
        ..BatchedRetentionReport::default()
    };
    if !report.enabled {
        return report;
    }
    tokio::pin!(stop);
    'outer: loop {
        // Drain: keep deleting batches until one returns less than a full batch
        // (caught up) or errors, pausing briefly between batches.
        loop {
            match delete_batch(batch_size).await {
                Ok(deleted) => {
                    report.deleted += deleted;
                    if (deleted as i64) < batch_size {
                        break;
                    }
                }
                Err(error) => {
                    report.errors += 1;
                    tracing::warn!(%error, label, retention_days, "retention batch delete failed");
                    break;
                }
            }
            let pause = tokio::time::sleep(inter_batch);
            tokio::pin!(pause);
            tokio::select! {
                () = &mut stop => break 'outer,
                () = &mut pause => {}
            }
        }
        report.ticks += 1;
        let sleep = tokio::time::sleep(interval);
        tokio::pin!(sleep);
        tokio::select! {
            () = &mut stop => break,
            () = &mut sleep => {}
        }
    }
    report
}

/// Batch-delete `telegram_files` rows older than `retention_days` (by
/// `last_seen_at`), once per `interval`, until `stop` resolves.
pub async fn run_telegram_files_retention_worker_until<Stop>(
    pool: PgPool,
    interval: Duration,
    retention_days: i32,
    batch_size: i64,
    inter_batch: Duration,
    stop: Stop,
) -> BatchedRetentionReport
where
    Stop: std::future::Future<Output = ()>,
{
    run_batched_retention_worker_until(
        "telegram_files",
        interval,
        retention_days,
        batch_size,
        inter_batch,
        |batch| {
            let pool = pool.clone();
            async move {
                openplotva_storage::delete_old_telegram_files_batch(&pool, retention_days, batch)
                    .await
            }
        },
        stop,
    )
    .await
}

/// Keep only the current UTC day's terminal inbox and quarantine rows. The
/// worker drains yesterday and earlier immediately at startup, then repeats
/// once per `interval` in bounded transactions.
pub async fn run_telegram_update_retention_worker_until<Stop>(
    store: PostgresTelegramDeliveryStore,
    interval: Duration,
    batch_size: i64,
    inter_batch: Duration,
    stop: Stop,
) -> TelegramUpdateRetentionReport
where
    Stop: std::future::Future<Output = ()>,
{
    let mut report = TelegramUpdateRetentionReport::default();
    let batch_size = batch_size.max(1);
    let full_batch = u64::try_from(batch_size).unwrap_or(u64::MAX);
    tokio::pin!(stop);

    'outer: loop {
        let cutoff = current_utc_day_start(OffsetDateTime::now_utc());
        let mut tick_inbox_deleted = 0_u64;
        let mut tick_quarantine_deleted = 0_u64;

        loop {
            match store
                .delete_update_artifacts_before(cutoff, batch_size)
                .await
            {
                Ok(batch) => {
                    report.inbox_deleted = report.inbox_deleted.saturating_add(batch.inbox_deleted);
                    report.quarantine_deleted = report
                        .quarantine_deleted
                        .saturating_add(batch.quarantine_deleted);
                    tick_inbox_deleted = tick_inbox_deleted.saturating_add(batch.inbox_deleted);
                    tick_quarantine_deleted =
                        tick_quarantine_deleted.saturating_add(batch.quarantine_deleted);

                    if batch.inbox_deleted < full_batch && batch.quarantine_deleted < full_batch {
                        break;
                    }
                }
                Err(error) => {
                    report.errors = report.errors.saturating_add(1);
                    tracing::warn!(%error, %cutoff, "Telegram update retention batch delete failed");
                    break;
                }
            }

            let pause = tokio::time::sleep(inter_batch);
            tokio::pin!(pause);
            tokio::select! {
                () = &mut stop => break 'outer,
                () = &mut pause => {}
            }
        }

        report.ticks = report.ticks.saturating_add(1);
        if tick_inbox_deleted > 0 || tick_quarantine_deleted > 0 {
            tracing::info!(
                inbox_deleted = tick_inbox_deleted,
                quarantine_deleted = tick_quarantine_deleted,
                %cutoff,
                "deleted expired Telegram update artifacts"
            );
        }

        let sleep = tokio::time::sleep(interval);
        tokio::pin!(sleep);
        tokio::select! {
            () = &mut stop => break,
            () = &mut sleep => {}
        }
    }

    report
}

/// Batch-delete `whitecircle_checks` rows older than `retention_days` (by
/// `created_at`), once per `interval`, until `stop` resolves.
pub async fn run_whitecircle_checks_retention_worker_until<Stop>(
    pool: PgPool,
    interval: Duration,
    retention_days: i32,
    batch_size: i64,
    inter_batch: Duration,
    stop: Stop,
) -> BatchedRetentionReport
where
    Stop: std::future::Future<Output = ()>,
{
    run_batched_retention_worker_until(
        "whitecircle_checks",
        interval,
        retention_days,
        batch_size,
        inter_batch,
        |batch| {
            let pool = pool.clone();
            async move {
                openplotva_storage::delete_old_whitecircle_checks_batch(
                    &pool,
                    retention_days,
                    batch,
                )
                .await
            }
        },
        stop,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn partition_worker_disabled_returns_immediately() {
        // A never-completing stop future proves the disabled branch returns
        // without awaiting the pool.
        let pool = PgPool::connect_lazy("postgres://invalid/invalid").expect("lazy pool");
        let report = run_chat_history_partition_retention_worker_until(
            pool,
            Duration::from_secs(1),
            0,
            std::future::pending::<()>(),
        )
        .await;
        assert!(!report.enabled);
        assert_eq!(report.ticks, 0);
        assert_eq!(report.dropped_partitions, 0);
    }

    #[tokio::test]
    async fn batched_worker_disabled_returns_immediately() {
        let pool = PgPool::connect_lazy("postgres://invalid/invalid").expect("lazy pool");
        let report = run_telegram_files_retention_worker_until(
            pool,
            Duration::from_secs(1),
            0,
            RETENTION_DELETE_BATCH_SIZE,
            RETENTION_INTER_BATCH_PAUSE,
            std::future::pending::<()>(),
        )
        .await;
        assert!(!report.enabled);
        assert_eq!(report.deleted, 0);
    }

    #[test]
    fn telegram_update_cutoff_is_start_of_current_utc_day() {
        let now = OffsetDateTime::from_unix_timestamp(1_752_232_496).expect("valid timestamp");
        let expected = OffsetDateTime::from_unix_timestamp(1_752_192_000).expect("valid timestamp");

        assert_eq!(current_utc_day_start(now), expected);
    }
}
