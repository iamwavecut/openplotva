//! App-owned durable task queue runtime for non-control taskman work.

use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use openplotva_config::PersistentQueueConfig;
use openplotva_taskman::{
    InMemoryTaskQueue, TaskQueueIdAllocator, TaskQueueSnapshot, TaskQueueWalRecord,
    TaskQueueWalSink,
};
use openplotva_telegram::{
    DeleteMessageRequest, TelegramOutboundMethod, build_delete_message_method,
};
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::sync::Notify;

/// Shutdown dirty Postgres sync timeout for the runtime shared queue.
pub const SHARED_TASK_QUEUE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
pub const SHARED_TASK_QUEUE_STUCK_SCAN_INTERVAL: Duration = Duration::from_secs(2 * 60);
pub const SHARED_TASK_QUEUE_STUCK_DURATION: Duration = Duration::from_secs(4 * 60 * 60);
pub const TASK_QUEUE_DB_SYNC_INTERVAL: Duration = Duration::from_secs(1);
pub const TASK_QUEUE_DB_SYNC_MUTATION_THRESHOLD: usize = 1_000;

pub type TaskQueueStoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait SharedTaskQueueDurableStore: Clone + Send + Sync + 'static {
    type Error: fmt::Display + Send + Sync + 'static;

    fn load_snapshot<'a>(
        &'a self,
    ) -> TaskQueueStoreFuture<'a, Result<TaskQueueSnapshot, Self::Error>>;

    fn apply_wal_batch<'a>(
        &'a self,
        batch: Vec<TaskQueueWalRecord>,
    ) -> TaskQueueStoreFuture<'a, Result<(), Self::Error>>;
}

impl SharedTaskQueueDurableStore for openplotva_storage::PostgresTaskQueueStore {
    type Error = openplotva_storage::StorageError;

    fn load_snapshot<'a>(
        &'a self,
    ) -> TaskQueueStoreFuture<'a, Result<TaskQueueSnapshot, Self::Error>> {
        Box::pin(async move { self.load_task_queue_snapshot().await })
    }

    fn apply_wal_batch<'a>(
        &'a self,
        batch: Vec<TaskQueueWalRecord>,
    ) -> TaskQueueStoreFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move { self.apply_task_queue_wal_batch(batch).await })
    }
}

#[derive(Debug, Default)]
struct BufferedTaskQueueJournalState {
    dirty: BTreeMap<i64, TaskQueueWalRecord>,
    dirty_since: Option<Instant>,
    mutation_count: usize,
}

#[derive(Clone, Debug)]
pub struct BufferedTaskQueueJournal<S> {
    store: S,
    state: Arc<Mutex<BufferedTaskQueueJournalState>>,
    notify: Arc<Notify>,
    // Serializes flushes so the background worker and synchronous (payment) flushes
    // never run apply_wal_batch concurrently — that would race the mutation_count
    // accounting and duplicate DB writes.
    flush_lock: Arc<tokio::sync::Mutex<()>>,
}

impl<S> BufferedTaskQueueJournal<S> {
    #[must_use]
    pub fn new(store: S) -> Self {
        Self {
            store,
            state: Arc::new(Mutex::new(BufferedTaskQueueJournalState::default())),
            notify: Arc::new(Notify::new()),
            flush_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    #[must_use]
    pub fn should_flush_now(&self) -> bool {
        self.lock().mutation_count >= TASK_QUEUE_DB_SYNC_MUTATION_THRESHOLD
    }

    #[must_use]
    pub fn dirty_len(&self) -> usize {
        self.lock().dirty.len()
    }

    async fn wait_for_dirty_or_stop(&self, stop: &mut (impl Future<Output = ()> + Unpin)) -> bool {
        loop {
            if self.dirty_len() > 0 {
                return true;
            }
            tokio::select! {
                () = &mut *stop => return false,
                () = self.notify.notified() => {}
            }
        }
    }

    fn flush_delay(&self) -> Option<Duration> {
        let state = self.lock();
        if state.dirty.is_empty() {
            return None;
        }
        if state.mutation_count >= TASK_QUEUE_DB_SYNC_MUTATION_THRESHOLD {
            return Some(Duration::ZERO);
        }
        let Some(dirty_since) = state.dirty_since else {
            return Some(TASK_QUEUE_DB_SYNC_INTERVAL);
        };
        Some(
            TASK_QUEUE_DB_SYNC_INTERVAL
                .checked_sub(dirty_since.elapsed())
                .unwrap_or(Duration::ZERO),
        )
    }

    pub async fn flush_dirty(&self) -> Result<usize, S::Error>
    where
        S: SharedTaskQueueDurableStore,
    {
        let _flush_guard = self.flush_lock.lock().await;
        let (batch, flushing_mutation_count) = {
            let state = self.lock();
            if state.dirty.is_empty() {
                return Ok(0);
            }
            (
                state.dirty.values().cloned().collect::<Vec<_>>(),
                state.mutation_count,
            )
        };
        if let Err(error) = self.store.apply_wal_batch(batch.clone()).await {
            self.mark_flush_failed(flushing_mutation_count);
            return Err(error);
        }
        self.mark_flushed(&batch, flushing_mutation_count);
        Ok(batch.len())
    }

    fn mark_flush_failed(&self, _flushing_mutation_count: usize) {
        // Nothing was persisted, so keep dirty_since and mutation_count intact: both
        // triggers must keep reflecting the true unpersisted backlog. The worker
        // applies its own retry backoff (run_task_queue_db_sync_worker_until).
        self.notify.notify_one();
    }

    fn mark_flushed(&self, batch: &[TaskQueueWalRecord], flushing_mutation_count: usize) {
        let mut state = self.lock();
        for flushed in batch {
            if state
                .dirty
                .get(&flushed.job_id)
                .is_some_and(|current| current == flushed)
            {
                state.dirty.remove(&flushed.job_id);
            }
        }
        if state.dirty.is_empty() {
            state.dirty_since = None;
            state.mutation_count = 0;
        } else {
            state.dirty_since = Some(Instant::now());
            state.mutation_count = state.mutation_count.saturating_sub(flushing_mutation_count);
            self.notify.notify_one();
        }
    }

    fn lock(&self) -> MutexGuard<'_, BufferedTaskQueueJournalState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl<S> TaskQueueWalSink for BufferedTaskQueueJournal<S>
where
    S: Send + Sync,
{
    fn append_task_queue_wal_record(&self, record: &TaskQueueWalRecord) {
        let mut state = self.lock();
        if state.dirty_since.is_none() {
            state.dirty_since = Some(Instant::now());
        }
        state.mutation_count = state.mutation_count.saturating_add(1);
        state.dirty.insert(record.job_id, record.clone());
        let notify = state.mutation_count == 1
            || state.mutation_count >= TASK_QUEUE_DB_SYNC_MUTATION_THRESHOLD;
        drop(state);
        if notify {
            self.notify.notify_one();
        }
    }
}

/// Cap and shaping for the worker's exponential retry backoff on flush failure.
const TASK_QUEUE_DB_SYNC_BACKOFF_CAP: Duration = Duration::from_secs(30);
const TASK_QUEUE_DB_SYNC_BACKOFF_MAX_SHIFT: u32 = 5;
/// After the first failure log, only re-log every Nth consecutive failure so a
/// multi-minute Postgres outage does not spam the warn stream.
const TASK_QUEUE_DB_SYNC_FAILURE_LOG_EVERY: u32 = 30;

fn task_queue_db_sync_backoff(consecutive_failures: u32) -> Duration {
    let shift = consecutive_failures
        .saturating_sub(1)
        .min(TASK_QUEUE_DB_SYNC_BACKOFF_MAX_SHIFT);
    let capped =
        (TASK_QUEUE_DB_SYNC_INTERVAL * (1u32 << shift)).min(TASK_QUEUE_DB_SYNC_BACKOFF_CAP);
    // Full jitter in [capped/2, capped] decorrelates retries if several instances ever
    // share the database, and avoids a synchronized retry pulse.
    let half = (capped.as_millis() as u64) / 2;
    Duration::from_millis(half + rand::random::<u64>() % (half + 1))
}

pub async fn run_task_queue_db_sync_worker_until<S>(
    journal: BufferedTaskQueueJournal<S>,
    stop: impl Future<Output = ()>,
) -> SharedTaskQueueDbSyncWorkerReport
where
    S: SharedTaskQueueDurableStore,
{
    let mut report = SharedTaskQueueDbSyncWorkerReport::default();
    let mut consecutive_failures: u32 = 0;
    tokio::pin!(stop);

    loop {
        if !journal.wait_for_dirty_or_stop(&mut stop).await {
            break;
        }

        while let Some(delay) = journal.flush_delay() {
            if delay.is_zero() {
                break;
            }
            tokio::select! {
                () = &mut stop => {
                    match journal.flush_dirty().await {
                        Ok(flushed) => report.flushed += flushed,
                        Err(error) => {
                            report.errors += 1;
                            report.last_error = Some(error.to_string());
                            tracing::warn!(%error, "failed to flush shared taskman queue during shutdown");
                        }
                    }
                    return report;
                }
                () = journal.notify.notified() => {}
                () = tokio::time::sleep(delay) => break,
            }
        }

        report.ticks += 1;
        match journal.flush_dirty().await {
            Ok(flushed) => {
                report.flushed += flushed;
                consecutive_failures = 0;
            }
            Err(error) => {
                report.errors += 1;
                consecutive_failures = consecutive_failures.saturating_add(1);
                report.last_error = Some(error.to_string());
                if consecutive_failures == 1
                    || consecutive_failures.is_multiple_of(TASK_QUEUE_DB_SYNC_FAILURE_LOG_EVERY)
                {
                    tracing::warn!(
                        %error,
                        consecutive_failures,
                        "failed to flush shared taskman queue to Postgres"
                    );
                }
                tokio::select! {
                    () = &mut stop => break,
                    () = tokio::time::sleep(task_queue_db_sync_backoff(consecutive_failures)) => {}
                }
            }
        }
    }

    report
}

/// Runtime-owned shared task queue plus its buffered PostgreSQL journal.
#[derive(Clone, Debug)]
pub struct SharedTaskQueueRuntime {
    queue: Arc<InMemoryTaskQueue>,
    db_journal: Option<BufferedTaskQueueJournal<openplotva_storage::PostgresTaskQueueStore>>,
}

/// Startup restore report for the shared task queue.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTaskQueueRestoreReport {
    /// Number of records loaded from PostgreSQL.
    pub restored: usize,
    /// Number of processing jobs reset to pending during startup.
    pub requeued: usize,
}

/// Buffered PostgreSQL sync worker report.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTaskQueueDbSyncWorkerReport {
    /// Number of dirty flush attempts observed.
    pub ticks: usize,
    /// Number of coalesced dirty records flushed to PostgreSQL.
    pub flushed: usize,
    /// Number of failed flush attempts.
    pub errors: usize,
    /// Last PostgreSQL flush error, if any.
    pub last_error: Option<String>,
}

/// Periodic stale-processing recovery worker report.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTaskQueueRecoveryWorkerReport {
    /// Number of recovery ticks observed.
    pub ticks: usize,
    /// Number of processing jobs moved back to pending.
    pub requeued: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTaskQueueTerminalCleanupWorkerReport {
    /// Number of cleanup ticks observed.
    pub ticks: usize,
    /// Number of terminal jobs deleted.
    pub deleted: usize,
}

/// Periodic Postgres hard-purge worker report.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTaskQueueDbPurgeWorkerReport {
    /// Number of purge ticks observed.
    pub ticks: usize,
    /// Number of soft-deleted jobs and history rows physically removed.
    pub purged: usize,
    /// Number of failed purge attempts.
    pub errors: usize,
    /// Last Postgres purge error, if any.
    pub last_error: Option<String>,
}

/// Periodic worker-heartbeat report.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTaskQueueHeartbeatWorkerReport {
    /// Number of heartbeat ticks observed.
    pub ticks: usize,
    /// Number of worker heartbeat writes.
    pub heartbeats: usize,
}

/// Periodic stuck-processing cleanup worker report.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTaskQueueStuckCleanupWorkerReport {
    /// Number of cleanup ticks observed.
    pub ticks: usize,
    /// Number of processing jobs marked failed.
    pub failed: usize,
}

/// One placeholder cleanup pass report.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTaskQueuePlaceholderCleanupReport {
    /// Number of stale placeholder rows found.
    pub found: usize,
    /// Number of Telegram delete requests attempted.
    pub attempted: usize,
    /// Number of taskman message rows removed from the in-memory queue.
    pub cleaned: usize,
    /// Number of Telegram delete requests that failed.
    pub delete_errors: usize,
    /// Last Telegram delete error, if any.
    pub last_error: Option<String>,
}

/// Periodic placeholder cleanup worker report.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SharedTaskQueuePlaceholderCleanupWorkerReport {
    /// Number of cleanup ticks observed.
    pub ticks: usize,
    /// Number of stale placeholder rows found.
    pub found: usize,
    /// Number of Telegram delete requests attempted.
    pub attempted: usize,
    /// Number of taskman message rows removed from the in-memory queue.
    pub cleaned: usize,
    /// Number of Telegram delete requests that failed.
    pub delete_errors: usize,
    /// Last Telegram delete error, if any.
    pub last_error: Option<String>,
}

/// Future returned by placeholder cleanup delete effects.
pub type PlaceholderDeleteFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Telegram boundary used by the shared taskman placeholder cleanup worker.
pub trait SharedTaskQueuePlaceholderDeleteEffects {
    /// Effect error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Delete one placeholder message from Telegram.
    fn delete_placeholder_message<'a>(
        &'a self,
        chat_id: i64,
        message_id: i64,
    ) -> PlaceholderDeleteFuture<'a, Self::Error>;
}

impl SharedTaskQueuePlaceholderDeleteEffects for openplotva_telegram::TelegramClient {
    type Error = String;

    fn delete_placeholder_message<'a>(
        &'a self,
        chat_id: i64,
        message_id: i64,
    ) -> PlaceholderDeleteFuture<'a, Self::Error> {
        Box::pin(async move {
            let method = build_delete_message_method(&DeleteMessageRequest {
                chat_id,
                message_id,
            })
            .map_err(|error| error.to_string())?;
            TelegramOutboundMethod::from(method)
                .execute_with(self)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    }
}

impl SharedTaskQueueRuntime {
    pub async fn load_from_postgres_with_id_allocator(
        store: openplotva_storage::PostgresTaskQueueStore,
        ids: TaskQueueIdAllocator,
    ) -> Result<(Self, SharedTaskQueueRestoreReport), openplotva_storage::StorageError> {
        // Seed the allocator from the durable id sequences (reconciled with any rows)
        // so issued ids never regress below a value handed out before this restart.
        let (next_job_id, next_message_id) = store.reserve_id_high_water().await?;
        ids.seed_high_water(next_job_id, next_message_id);
        let snapshot = store.load_task_queue_snapshot().await?;
        let restored_before_requeue = snapshot.records.len();
        let journal = BufferedTaskQueueJournal::new(store);
        let journal_sink: Arc<dyn TaskQueueWalSink> = Arc::new(journal.clone());
        let queue = Arc::new(
            InMemoryTaskQueue::from_snapshot_with_id_allocator_and_journal(
                snapshot,
                ids,
                journal_sink,
            ),
        );
        // Agent runs keep their Processing status + durable checkpoint and are
        // re-adopted by a live worker; only non-agent jobs are requeued.
        let release = queue.release_orphaned_processing_for_startup();
        let requeued = release.requeued;
        if release.requeued > 0 || release.agent_kept > 0 {
            journal.flush_dirty().await?;
        }
        let report = SharedTaskQueueRestoreReport {
            restored: queue.records().len().max(restored_before_requeue),
            requeued,
        };
        Ok((
            Self {
                queue,
                db_journal: Some(journal),
            },
            report,
        ))
    }

    #[cfg(test)]
    #[must_use]
    fn new_for_test(queue: InMemoryTaskQueue) -> Self {
        Self {
            queue: Arc::new(queue),
            db_journal: None,
        }
    }

    /// Return the shared task queue.
    #[must_use]
    pub fn queue(&self) -> Arc<InMemoryTaskQueue> {
        Arc::clone(&self.queue)
    }

    pub fn requeue_expired_processing(&self, now: OffsetDateTime) -> Vec<i64> {
        self.queue.requeue_expired_processing(now)
    }

    /// Update one worker heartbeat timestamp.
    pub fn update_worker_heartbeat(&self, worker_id: &str, at: OffsetDateTime) {
        self.queue.update_worker_heartbeat(worker_id, at);
    }

    pub fn fail_stuck_processing(&self, now: OffsetDateTime, stuck_duration: Duration) -> Vec<i64> {
        self.queue.fail_stuck_processing(now, stuck_duration)
    }

    pub fn prune_terminal_before(&self, cutoff: OffsetDateTime) -> Vec<i64> {
        self.queue.prune_terminal_before(cutoff)
    }

    #[must_use]
    pub fn db_journal(
        &self,
    ) -> Option<BufferedTaskQueueJournal<openplotva_storage::PostgresTaskQueueStore>> {
        self.db_journal.clone()
    }

    pub async fn flush_dirty(&self) -> Result<usize, String> {
        match &self.db_journal {
            Some(journal) => journal
                .flush_dirty()
                .await
                .map_err(|error| error.to_string()),
            None => Ok(0),
        }
    }
}

#[must_use]
pub fn shared_task_queue_recovery_interval_from_config(config: &PersistentQueueConfig) -> Duration {
    Duration::from_secs(config.recovery_interval_seconds.max(1) as u64)
}

#[must_use]
pub fn shared_task_queue_cleanup_interval_from_config(config: &PersistentQueueConfig) -> Duration {
    Duration::from_secs(config.cleanup_interval_seconds.max(1) as u64)
}

#[must_use]
pub fn shared_task_queue_completed_retention_from_config(
    config: &PersistentQueueConfig,
) -> TimeDuration {
    TimeDuration::days(i64::from(config.completed_job_retention_days.max(0)))
}

#[must_use]
pub fn shared_task_queue_heartbeat_interval_from_config(
    config: &PersistentQueueConfig,
) -> Duration {
    Duration::from_secs(config.heartbeat_interval_seconds.max(1) as u64)
}

#[must_use]
pub fn shared_task_queue_placeholder_cleanup_interval_from_config(
    config: &PersistentQueueConfig,
) -> Duration {
    Duration::from_secs(config.placeholder_cleanup_interval_seconds.max(1) as u64)
}

#[must_use]
pub fn shared_task_queue_placeholder_max_age_from_config(
    config: &PersistentQueueConfig,
) -> Duration {
    Duration::from_secs(config.placeholder_max_age_seconds.max(1) as u64)
}

#[must_use]
pub fn shared_task_queue_worker_ids(worker_counts: &BTreeMap<String, i32>) -> Vec<String> {
    worker_counts
        .iter()
        .flat_map(|(queue_name, count)| {
            (0..(*count).max(0)).map(move |index| format!("{queue_name}-worker-{index}"))
        })
        .collect()
}

/// Update shared taskman worker heartbeats periodically until shutdown is requested.
pub async fn run_shared_task_queue_heartbeat_worker_until(
    runtime: SharedTaskQueueRuntime,
    worker_ids: Vec<String>,
    interval: Duration,
    stop: impl std::future::Future<Output = ()>,
) -> SharedTaskQueueHeartbeatWorkerReport {
    let mut report = SharedTaskQueueHeartbeatWorkerReport::default();
    tokio::pin!(stop);
    if worker_ids.is_empty() {
        let _ = stop.await;
        return report;
    }

    loop {
        tokio::select! {
            () = &mut stop => break,
            () = tokio::time::sleep(interval) => {
                report.ticks += 1;
                let now = OffsetDateTime::now_utc();
                for worker_id in &worker_ids {
                    runtime.update_worker_heartbeat(worker_id, now);
                }
                report.heartbeats += worker_ids.len();
            }
        }
    }

    report
}

/// Requeue expired processing jobs periodically until shutdown is requested.
pub async fn run_shared_task_queue_recovery_worker_until(
    runtime: SharedTaskQueueRuntime,
    interval: Duration,
    stop: impl std::future::Future<Output = ()>,
) -> SharedTaskQueueRecoveryWorkerReport {
    let mut report = SharedTaskQueueRecoveryWorkerReport::default();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            _ = ticker.tick() => {
                report.ticks += 1;
                let requeued = runtime.requeue_expired_processing(OffsetDateTime::now_utc());
                if requeued.is_empty() {
                    continue;
                }
                report.requeued += requeued.len();
                tracing::warn!(?requeued, "requeued expired shared taskman processing jobs");
            }
        }
    }

    report
}

pub async fn run_shared_task_queue_terminal_cleanup_worker_until(
    runtime: SharedTaskQueueRuntime,
    interval: Duration,
    retention: TimeDuration,
    stop: impl std::future::Future<Output = ()>,
) -> SharedTaskQueueTerminalCleanupWorkerReport {
    let mut report = SharedTaskQueueTerminalCleanupWorkerReport::default();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            _ = ticker.tick() => {
                report.ticks += 1;
                let cutoff = OffsetDateTime::now_utc() - retention;
                let deleted = runtime.prune_terminal_before(cutoff);
                if deleted.is_empty() {
                    continue;
                }
                report.deleted += deleted.len();
                tracing::warn!(deleted = deleted.len(), "deleted old shared taskman terminal jobs");
            }
        }
    }

    report
}

/// Hard-delete soft-deleted jobs and old history from Postgres periodically. The
/// in-memory prune only soft-deletes rows (via delete WAL); without this purge the
/// `taskman_jobs` and `taskman_job_history` tables grow without bound.
pub async fn run_shared_task_queue_db_purge_worker_until(
    store: openplotva_storage::PostgresTaskQueueStore,
    interval: Duration,
    retention: TimeDuration,
    stop: impl std::future::Future<Output = ()>,
) -> SharedTaskQueueDbPurgeWorkerReport {
    let mut report = SharedTaskQueueDbPurgeWorkerReport::default();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            _ = ticker.tick() => {
                report.ticks += 1;
                let cutoff = OffsetDateTime::now_utc() - retention;
                match store.purge_task_queue_terminal(cutoff).await {
                    Ok(0) => {}
                    Ok(purged) => {
                        report.purged += purged as usize;
                        tracing::info!(purged, "purged old shared taskman rows from Postgres");
                    }
                    Err(error) => {
                        report.errors += 1;
                        report.last_error = Some(error.to_string());
                        tracing::warn!(%error, "failed to purge shared taskman rows from Postgres");
                    }
                }
            }
        }
    }

    report
}

/// Mark stuck processing jobs failed periodically until shutdown is requested.
pub async fn run_shared_task_queue_stuck_cleanup_worker_until(
    runtime: SharedTaskQueueRuntime,
    interval: Duration,
    stuck_duration: Duration,
    stop: impl std::future::Future<Output = ()>,
) -> SharedTaskQueueStuckCleanupWorkerReport {
    let mut report = SharedTaskQueueStuckCleanupWorkerReport::default();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            _ = ticker.tick() => {
                report.ticks += 1;
                let failed = runtime.fail_stuck_processing(OffsetDateTime::now_utc(), stuck_duration);
                if failed.is_empty() {
                    continue;
                }
                report.failed += failed.len();
                tracing::warn!(?failed, "marked stuck shared taskman processing jobs failed");
            }
        }
    }

    report
}

/// Clean up stale placeholder rows once, deleting Telegram messages best-effort first.
pub async fn cleanup_shared_task_queue_placeholders_once<Effects>(
    runtime: &SharedTaskQueueRuntime,
    effects: &Effects,
    max_age: Duration,
    now: OffsetDateTime,
    per_delete_delay: Duration,
) -> SharedTaskQueuePlaceholderCleanupReport
where
    Effects: SharedTaskQueuePlaceholderDeleteEffects + Sync,
{
    let mut report = SharedTaskQueuePlaceholderCleanupReport::default();
    let stale = runtime.queue.stale_placeholder_messages(now, max_age);
    report.found = stale.len();

    for placeholder in stale {
        if !per_delete_delay.is_zero() {
            tokio::time::sleep(per_delete_delay).await;
        }
        report.attempted += 1;
        if let Err(error) = effects
            .delete_placeholder_message(placeholder.chat_id, i64::from(placeholder.message_id))
            .await
        {
            report.delete_errors += 1;
            report.last_error = Some(error.to_string());
            tracing::warn!(
                %error,
                job_id = placeholder.job_id,
                chat_id = placeholder.chat_id,
                message_id = placeholder.message_id,
                "failed to delete stale taskman placeholder from Telegram"
            );
        }
        if runtime.queue.delete_job_message(placeholder.id) {
            report.cleaned += 1;
        }
    }

    report
}

/// Clean up stale placeholder rows periodically until shutdown is requested.
pub async fn run_shared_task_queue_placeholder_cleanup_worker_until<Effects>(
    runtime: SharedTaskQueueRuntime,
    effects: Effects,
    interval: Duration,
    max_age: Duration,
    per_delete_delay: Duration,
    stop: impl std::future::Future<Output = ()>,
) -> SharedTaskQueuePlaceholderCleanupWorkerReport
where
    Effects: SharedTaskQueuePlaceholderDeleteEffects + Send + Sync,
{
    let mut report = SharedTaskQueuePlaceholderCleanupWorkerReport::default();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            _ = ticker.tick() => {
                report.ticks += 1;
                let pass = cleanup_shared_task_queue_placeholders_once(
                    &runtime,
                    &effects,
                    max_age,
                    OffsetDateTime::now_utc(),
                    per_delete_delay,
                ).await;
                report.found += pass.found;
                report.attempted += pass.attempted;
                report.cleaned += pass.cleaned;
                report.delete_errors += pass.delete_errors;
                if pass.last_error.is_some() {
                    report.last_error = pass.last_error;
                }
            }
        }
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use openplotva_taskman::{
        DialogJobParams, MESSAGE_STATUS_COMPLETED, MESSAGE_STATUS_PLACEHOLDER, MESSAGE_TYPE_RESULT,
        TEXT_QUEUE_NAME, TaskQueueJobMessageParams, TaskQueueRecord, empty_task_queue_snapshot,
        new_dialog_job_at,
    };
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };
    use time::OffsetDateTime;

    #[test]
    fn shared_task_queue_recovery_interval_uses_go_config_seconds() {
        let mut config = persistent_queue_config();
        config.recovery_interval_seconds = 9;

        assert_eq!(
            shared_task_queue_recovery_interval_from_config(&config),
            Duration::from_secs(9)
        );
    }

    #[test]
    fn shared_task_queue_heartbeat_config_and_worker_ids_match_go_shape() {
        let mut config = persistent_queue_config();
        config.heartbeat_interval_seconds = 17;
        let worker_counts = BTreeMap::from([
            ("image-regular".to_owned(), 1),
            ("image-vip".to_owned(), 2),
            ("music-vip".to_owned(), 0),
        ]);

        assert_eq!(
            shared_task_queue_heartbeat_interval_from_config(&config),
            Duration::from_secs(17)
        );
        assert_eq!(
            shared_task_queue_worker_ids(&worker_counts),
            vec![
                "image-regular-worker-0".to_owned(),
                "image-vip-worker-0".to_owned(),
                "image-vip-worker-1".to_owned(),
            ]
        );
    }

    #[test]
    fn shared_task_queue_runtime_updates_worker_heartbeat() {
        let runtime = SharedTaskQueueRuntime::new_for_test(InMemoryTaskQueue::new());
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("timestamp");

        runtime.update_worker_heartbeat("text-worker-0", now);

        assert_eq!(
            runtime.queue().worker_heartbeat_at("text-worker-0"),
            Some(now)
        );
    }

    #[test]
    fn shared_task_queue_placeholder_cleanup_config_uses_go_seconds() {
        let mut config = persistent_queue_config();
        config.placeholder_cleanup_interval_seconds = 11;
        config.placeholder_max_age_seconds = 22;

        assert_eq!(
            shared_task_queue_placeholder_cleanup_interval_from_config(&config),
            Duration::from_secs(11)
        );
        assert_eq!(
            shared_task_queue_placeholder_max_age_from_config(&config),
            Duration::from_secs(22)
        );
    }

    #[tokio::test]
    async fn shared_task_queue_placeholder_cleanup_deletes_stale_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = SharedTaskQueueRuntime::new_for_test(InMemoryTaskQueue::new());
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let old = now - time::Duration::seconds(121);
        let boundary = now - time::Duration::seconds(120);
        let job_id = runtime.queue().assign(
            TEXT_QUEUE_NAME,
            new_dialog_job_at(dialog_params("stale"), now),
        );
        let stale = runtime
            .queue()
            .create_job_message(TaskQueueJobMessageParams {
                job_id,
                message_type: MESSAGE_TYPE_RESULT.to_owned(),
                chat_id: 10,
                message_id: 103,
                created_at: old,
                status: MESSAGE_STATUS_PLACEHOLDER.to_owned(),
            })?;
        let boundary_placeholder =
            runtime
                .queue()
                .create_job_message(TaskQueueJobMessageParams {
                    job_id,
                    message_type: MESSAGE_TYPE_RESULT.to_owned(),
                    chat_id: 10,
                    message_id: 104,
                    created_at: boundary,
                    status: MESSAGE_STATUS_PLACEHOLDER.to_owned(),
                })?;
        let completed = runtime
            .queue()
            .create_job_message(TaskQueueJobMessageParams {
                job_id,
                message_type: MESSAGE_TYPE_RESULT.to_owned(),
                chat_id: 10,
                message_id: 105,
                created_at: old,
                status: MESSAGE_STATUS_COMPLETED.to_owned(),
            })?;
        let effects = PlaceholderEffectsStub::default();

        let report = cleanup_shared_task_queue_placeholders_once(
            &runtime,
            &effects,
            Duration::from_secs(120),
            now,
            Duration::ZERO,
        )
        .await;

        assert_eq!(
            report,
            SharedTaskQueuePlaceholderCleanupReport {
                found: 1,
                attempted: 1,
                cleaned: 1,
                ..SharedTaskQueuePlaceholderCleanupReport::default()
            }
        );
        assert_eq!(effects.deleted(), vec![(10, 103)]);
        let remaining = runtime
            .queue()
            .job_messages(job_id)
            .into_iter()
            .map(|message| message.id)
            .collect::<Vec<_>>();
        assert_eq!(remaining, vec![boundary_placeholder, completed]);
        assert!(!remaining.contains(&stale));

        Ok(())
    }

    #[tokio::test]
    async fn shared_task_queue_placeholder_cleanup_removes_row_after_delete_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = SharedTaskQueueRuntime::new_for_test(InMemoryTaskQueue::new());
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let old = now - time::Duration::seconds(121);
        let job_id = runtime.queue().assign(
            TEXT_QUEUE_NAME,
            new_dialog_job_at(dialog_params("stale"), now),
        );
        runtime
            .queue()
            .create_job_message(TaskQueueJobMessageParams {
                job_id,
                message_type: MESSAGE_TYPE_RESULT.to_owned(),
                chat_id: 10,
                message_id: 103,
                created_at: old,
                status: MESSAGE_STATUS_PLACEHOLDER.to_owned(),
            })?;
        let effects = PlaceholderEffectsStub::failing("telegram unavailable");

        let report = cleanup_shared_task_queue_placeholders_once(
            &runtime,
            &effects,
            Duration::from_secs(120),
            now,
            Duration::ZERO,
        )
        .await;

        assert_eq!(report.found, 1);
        assert_eq!(report.attempted, 1);
        assert_eq!(report.cleaned, 1);
        assert_eq!(report.delete_errors, 1);
        assert!(runtime.queue().job_messages(job_id).is_empty());

        Ok(())
    }

    #[derive(Clone, Default)]
    struct PlaceholderEffectsStub {
        deleted: Arc<Mutex<Vec<(i64, i64)>>>,
        error: Option<String>,
    }

    impl PlaceholderEffectsStub {
        fn failing(error: impl Into<String>) -> Self {
            Self {
                deleted: Arc::new(Mutex::new(Vec::new())),
                error: Some(error.into()),
            }
        }

        fn deleted(&self) -> Vec<(i64, i64)> {
            self.deleted
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl SharedTaskQueuePlaceholderDeleteEffects for PlaceholderEffectsStub {
        type Error = String;

        fn delete_placeholder_message<'a>(
            &'a self,
            chat_id: i64,
            message_id: i64,
        ) -> PlaceholderDeleteFuture<'a, Self::Error> {
            Box::pin(async move {
                self.deleted
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push((chat_id, message_id));
                match &self.error {
                    Some(error) => Err(error.clone()),
                    None => Ok(()),
                }
            })
        }
    }

    #[derive(Clone, Default)]
    struct TaskQueueDurableStoreStub {
        batches: Arc<Mutex<Vec<Vec<TaskQueueWalRecord>>>>,
        fail_next: Arc<Mutex<bool>>,
    }

    impl TaskQueueDurableStoreStub {
        fn batches(&self) -> Vec<Vec<TaskQueueWalRecord>> {
            self.batches
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }

        fn fail_next(&self) {
            *self
                .fail_next
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        }
    }

    impl SharedTaskQueueDurableStore for TaskQueueDurableStoreStub {
        type Error = String;

        fn load_snapshot<'a>(
            &'a self,
        ) -> TaskQueueStoreFuture<'a, Result<TaskQueueSnapshot, Self::Error>> {
            Box::pin(async { Ok(empty_task_queue_snapshot()) })
        }

        fn apply_wal_batch<'a>(
            &'a self,
            batch: Vec<TaskQueueWalRecord>,
        ) -> TaskQueueStoreFuture<'a, Result<(), Self::Error>> {
            Box::pin(async move {
                let mut fail_next = self
                    .fail_next
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if *fail_next {
                    *fail_next = false;
                    return Err("db down".to_owned());
                }
                drop(fail_next);
                self.batches
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(batch);
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn buffered_task_queue_journal_flushes_on_thousandth_mutation()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = TaskQueueDurableStoreStub::default();
        let journal = BufferedTaskQueueJournal::new(store.clone());
        let now = OffsetDateTime::now_utc();

        for job_id in 1..TASK_QUEUE_DB_SYNC_MUTATION_THRESHOLD {
            let record = task_queue_record(job_id as i64, now, "queued");
            journal.append_task_queue_wal_record(&task_queue_wal_upsert(record));
        }

        assert_eq!(store.batches().len(), 0);
        assert!(!journal.should_flush_now());

        let last = task_queue_record(TASK_QUEUE_DB_SYNC_MUTATION_THRESHOLD as i64, now, "last");
        journal.append_task_queue_wal_record(&task_queue_wal_upsert(last.clone()));

        assert!(journal.should_flush_now());
        journal.flush_dirty().await?;

        assert_eq!(store.batches().len(), 1);
        assert_eq!(journal.dirty_len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn buffered_task_queue_journal_coalesces_repeated_job_changes()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = TaskQueueDurableStoreStub::default();
        let journal = BufferedTaskQueueJournal::new(store.clone());
        let now = OffsetDateTime::now_utc();

        journal
            .append_task_queue_wal_record(&task_queue_wal_upsert(task_queue_record(1, now, "old")));
        let latest = task_queue_record(1, now, "new");
        journal.append_task_queue_wal_record(&task_queue_wal_upsert(latest.clone()));
        journal.flush_dirty().await?;

        let batches = store.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(batches[0][0].record, Some(latest));
        Ok(())
    }

    #[test]
    fn buffered_task_queue_journal_keeps_new_mutation_count_after_overlapping_flush() {
        let journal = BufferedTaskQueueJournal::new(TaskQueueDurableStoreStub::default());
        let now = OffsetDateTime::now_utc();
        let flushing = task_queue_wal_upsert(task_queue_record(1, now, "old"));
        journal.append_task_queue_wal_record(&flushing);

        for index in 0..TASK_QUEUE_DB_SYNC_MUTATION_THRESHOLD {
            journal.append_task_queue_wal_record(&task_queue_wal_upsert(task_queue_record(
                1,
                now,
                &format!("new-{index}"),
            )));
        }
        journal.mark_flushed(&[flushing], 1);

        assert_eq!(journal.dirty_len(), 1);
        assert!(journal.should_flush_now());
    }

    #[tokio::test]
    async fn buffered_task_queue_journal_keeps_dirty_records_after_failed_flush() {
        let store = TaskQueueDurableStoreStub::default();
        let journal = BufferedTaskQueueJournal::new(store.clone());
        let now = OffsetDateTime::now_utc();

        store.fail_next();
        journal
            .append_task_queue_wal_record(&task_queue_wal_upsert(task_queue_record(1, now, "x")));

        assert!(journal.flush_dirty().await.is_err());
        assert_eq!(journal.dirty_len(), 1);
        assert!(!journal.should_flush_now());
        assert_eq!(store.batches().len(), 0);
    }

    fn dialog_params(message_text: &str) -> DialogJobParams {
        DialogJobParams {
            chat_id: 100,
            message_id: 20,
            user_id: 7,
            user_full_name: "Wave Cut".to_owned(),
            message_text: message_text.to_owned(),
            original_text: message_text.to_owned(),
            meta: serde_json::json!({}),
            max_output_tokens: 0,
            thread_id: None,
        }
    }

    fn task_queue_record(id: i64, created: OffsetDateTime, title: &str) -> TaskQueueRecord {
        TaskQueueRecord {
            id,
            queue_name: TEXT_QUEUE_NAME.to_owned(),
            status: openplotva_taskman::JobStatus::Pending,
            job: new_dialog_job_at(dialog_params(title), created).with_name(title),
            worker_id: None,
            started_at: None,
            execution_started_at: None,
            completed_at: None,
            error: None,
            progress_message_id: None,
            queue_position_message_id: None,
            queue_position_message_pending: false,
            result_message_id: None,
            messages: Vec::new(),
            events: Vec::new(),
            agent_state: None,
        }
    }

    fn task_queue_wal_upsert(record: TaskQueueRecord) -> TaskQueueWalRecord {
        TaskQueueWalRecord {
            format: openplotva_taskman::TASK_QUEUE_WAL_FORMAT.to_owned(),
            op: openplotva_taskman::TASK_QUEUE_WAL_UPSERT_JOB.to_owned(),
            job_id: record.id,
            record: Some(record),
        }
    }

    fn persistent_queue_config() -> PersistentQueueConfig {
        PersistentQueueConfig {
            enabled: true,
            heartbeat_interval_seconds: 30,
            recovery_interval_seconds: 60,
            cleanup_interval_seconds: 300,
            default_processing_timeout_seconds: 300,
            max_retries: 3,
            completed_job_retention_days: 1,
            message_cleanup_interval_seconds: 300,
            job_message_cleanup_minutes: 30,
            control_workers: 2,
            text_workers: 4,
            dialog_aifarm_workers: 2,
            dialog_workers_cap: 24,
            dialog_unpooled_share: 2,
            dialog_aifarm_fallback_workers: 1,
            dialog_aifarm_fallback_high_watermark: 30,
            dialog_aifarm_fallback_low_watermark: 20,
            dialog_aifarm_fallback_poll_interval_seconds: 1,
            image_regular_workers: 1,
            image_vip_workers: 1,
            music_vip_workers: 1,
            memory_consolidation_workers: 1,
            placeholder_cleanup_interval_seconds: 3600,
            placeholder_max_age_seconds: 7200,
            llm_job_max_attempts: 5,
        }
    }
}
