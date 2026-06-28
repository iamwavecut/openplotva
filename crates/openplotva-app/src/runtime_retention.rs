//! Daily data-retention workers: drop expired `chat_history_entries` partitions
//! and batch-delete aged `telegram_files` / `whitecircle_checks` rows. Each
//! worker is a no-op when its `retention_days` is `<= 0` (the knob's "disabled"
//! value).
//!
//! Mirrors the proven `runtime_llm` cleanup worker: a drain-then-sleep loop that
//! cancels promptly on the runtime stop signal and never holds a lock across the
//! sleep. The drain inner loop clears a large initial backlog gradually (a small
//! pause between batches) so it does not spike replication lag or autovacuum.

use std::time::Duration;

use sqlx::PgPool;

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
}
