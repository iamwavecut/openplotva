//! Durable Redis Stream to PostgreSQL update materialization worker.

use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use carapax::types::{Update as TelegramUpdate, UpdateType as TelegramUpdateType};
use openplotva_storage::{
    MATERIALIZED_UPDATE_BINDS_PER_ROW, MaterializationReport, MaterializedUpdateDisposition,
    MaterializedUpdateInput, PostgresTelegramDeliveryStore, QuarantinedUpdateInput,
};
use openplotva_updates::{
    RawUpdateStreamEntry, RedisUpdateStream, UpdateStreamConfigError, UpdateStreamEntry,
    UpdateStreamId, UpdateStreamMaterializerConfig, decode_telegram_update_json_slice,
    is_passive_update, update_name,
};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use tokio::sync::watch;

const MATERIALIZER_RETRY_BACKOFF_MIN: Duration = Duration::from_millis(100);
const MATERIALIZER_RETRY_BACKOFF_CAP: Duration = Duration::from_secs(30);
const MATERIALIZER_RECLAIM_RECHECK_MAX: Duration = Duration::from_secs(1);
const MATERIALIZER_EMPTY_FILL_POLL_INTERVAL: Duration = Duration::from_millis(1);
const MATERIALIZER_LEASE_TTL: Duration = Duration::from_secs(60);
const MATERIALIZER_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(15);
const MATERIALIZER_LEASE_ACQUIRE_RECHECK: Duration = Duration::from_secs(1);
const MATERIALIZER_SUPERVISOR_RESTART_DELAY: Duration =
    Duration::from_secs(MATERIALIZER_LEASE_TTL.as_secs() / 60);

static MATERIALIZER_OWNER_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn materializer_owner_consumer_id(bot_id: i64) -> String {
    let sequence = MATERIALIZER_OWNER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let nonce = rand::random::<u64>();
    format!(
        "openplotva-materializer-{bot_id}-{}-{sequence:016x}-{nonce:016x}",
        std::process::id()
    )
}

/// Point-in-time counters and last-batch gauges exposed to runtime diagnostics.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct UpdateMaterializerMetricsSnapshot {
    pub supervisor_running: bool,
    pub lease_held: bool,
    pub supervisor_restarts: u64,
    pub batch_rows: usize,
    pub batch_bytes: usize,
    /// Utilization of whichever configured row/byte limit is closer to full.
    pub batch_fill_ratio: f64,
    pub last_transaction_latency: Option<Duration>,
    pub materialized_batches: u64,
    pub inbox_insert_statements: u64,
    pub quarantine_insert_statements: u64,
    pub inserted: u64,
    pub duplicates: u64,
    pub conflicted: u64,
    pub quarantined: u64,
    pub reclaims: u64,
    pub reclaimed_entries: u64,
    pub ack_delete_mismatches: u64,
    pub db_failures: u64,
    pub redis_failures: u64,
}

/// Cloneable live metrics handle shared by the materializer and runtime API.
#[derive(Clone, Debug, Default)]
pub struct UpdateMaterializerMetrics {
    inner: Arc<Mutex<UpdateMaterializerMetricsSnapshot>>,
}

impl UpdateMaterializerMetrics {
    #[must_use]
    pub fn snapshot(&self) -> UpdateMaterializerMetricsSnapshot {
        *self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn update(&self, update: impl FnOnce(&mut UpdateMaterializerMetricsSnapshot)) {
        let mut snapshot = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        update(&mut snapshot);
    }

    fn set_lease_held(&self, lease_held: bool) {
        self.update(|snapshot| snapshot.lease_held = lease_held);
    }

    fn set_supervisor_running(&self, supervisor_running: bool) {
        self.update(|snapshot| snapshot.supervisor_running = supervisor_running);
    }

    fn record_supervisor_restart(&self) {
        self.update(|snapshot| {
            snapshot.supervisor_restarts = snapshot.supervisor_restarts.saturating_add(1);
        });
    }

    fn record_batch(&self, rows: usize, bytes: usize, max_rows: usize, max_bytes: usize) {
        let row_fill = rows as f64 / max_rows.max(1) as f64;
        let byte_fill = bytes as f64 / max_bytes.max(1) as f64;
        self.update(|snapshot| {
            snapshot.batch_rows = rows;
            snapshot.batch_bytes = bytes;
            snapshot.batch_fill_ratio = row_fill.max(byte_fill).min(1.0);
        });
    }

    fn record_reclaimed(&self, entries: usize) {
        self.update(|snapshot| {
            snapshot.reclaims = snapshot.reclaims.saturating_add(1);
            snapshot.reclaimed_entries = snapshot.reclaimed_entries.saturating_add(entries as u64);
        });
    }

    fn record_db_failure(&self) {
        self.update(|snapshot| {
            snapshot.db_failures = snapshot.db_failures.saturating_add(1);
        });
    }

    fn record_redis_failure(&self) {
        self.update(|snapshot| {
            snapshot.redis_failures = snapshot.redis_failures.saturating_add(1);
        });
    }

    fn record_ack_delete_mismatch(&self) {
        self.update(|snapshot| {
            snapshot.ack_delete_mismatches = snapshot.ack_delete_mismatches.saturating_add(1);
        });
    }

    fn record_materialization(
        &self,
        batch: &PreprocessedBatch,
        materialization: MaterializationReport,
        transaction_latency: Duration,
    ) {
        self.update(|snapshot| {
            snapshot.last_transaction_latency = Some(transaction_latency);
            snapshot.materialized_batches = snapshot.materialized_batches.saturating_add(1);
            snapshot.inserted = snapshot.inserted.saturating_add(materialization.inserted);
            snapshot.duplicates = snapshot
                .duplicates
                .saturating_add(materialization.duplicates);
            snapshot.conflicted = snapshot
                .conflicted
                .saturating_add(materialization.conflicted);
            snapshot.quarantined = snapshot
                .quarantined
                .saturating_add(materialization.quarantined);
            if !batch.updates.is_empty() {
                snapshot.inbox_insert_statements =
                    snapshot.inbox_insert_statements.saturating_add(1);
            }
            if !batch.quarantine.is_empty() {
                snapshot.quarantine_insert_statements =
                    snapshot.quarantine_insert_statements.saturating_add(1);
            }
        });
    }
}

/// Aggregate counters returned when the materializer stops.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UpdateMaterializerWorkerReport {
    pub read_entries: usize,
    pub reclaimed_entries: usize,
    pub redis_deleted_pending_entries: usize,
    pub materialized_batches: usize,
    pub materialized_deliveries: usize,
    pub inserted: u64,
    pub duplicates: u64,
    pub conflicted: u64,
    pub quarantined: u64,
    pub inbox_insert_statements: usize,
    pub quarantine_insert_statements: usize,
    pub db_failures: usize,
    pub redis_failures: usize,
    pub ack_delete_mismatches: usize,
    pub last_error: Option<String>,
}

#[derive(Debug)]
struct PreprocessedBatch {
    stream_ids: Vec<UpdateStreamId>,
    updates: Vec<MaterializedUpdateInput>,
    quarantine: Vec<QuarantinedUpdateInput>,
}

/// Run one active materializer for a bot Stream until shutdown.
///
/// Replicas contend for one Redis lease scoped to the Stream. The winner uses
/// the same unique identity for the lease and consumer-group reads; standbys
/// wait for failover. A failed renewal stops all further work before cleanup.
///
/// A failed PostgreSQL transaction keeps the current PEL batch pinned and is
/// retried before any newer entry is read. A successful commit is followed by
/// one atomic Redis `XACK` + `XDEL` command for every Stream ID in the batch.
pub async fn run_update_materializer_until<Stop>(
    stream: RedisUpdateStream,
    store: PostgresTelegramDeliveryStore,
    bot_id: i64,
    config: UpdateStreamMaterializerConfig,
    stop: Stop,
) -> Result<UpdateMaterializerWorkerReport, UpdateStreamConfigError>
where
    Stop: Future<Output = ()> + Send,
{
    run_update_materializer_with_metrics_until(
        stream,
        store,
        bot_id,
        config,
        UpdateMaterializerMetrics::default(),
        stop,
    )
    .await
}

/// Keep the materializer available across transient lease-heartbeat failures.
///
/// The inner worker stops before its lease can expire whenever renewal becomes
/// ambiguous. The supervisor then starts a fresh owner which must acquire the
/// Redis lease before it can resume materialization.
pub async fn run_supervised_update_materializer_until(
    stream: RedisUpdateStream,
    store: PostgresTelegramDeliveryStore,
    bot_id: i64,
    config: UpdateStreamMaterializerConfig,
    metrics: UpdateMaterializerMetrics,
    stop: watch::Receiver<bool>,
) -> Result<(), UpdateStreamConfigError> {
    config.validate()?;
    metrics.set_supervisor_running(true);
    let _running = MaterializerSupervisorRunning(metrics.clone());
    loop {
        let report = run_update_materializer_with_metrics_until(
            stream.clone(),
            store.clone(),
            bot_id,
            config,
            metrics.clone(),
            wait_for_stop(stop.clone()),
        )
        .await?;
        if *stop.borrow() || report.last_error.is_none() {
            return Ok(());
        }

        metrics.record_supervisor_restart();
        tracing::warn!(
            error = report.last_error.as_deref().unwrap_or_default(),
            restart_delay_ms = MATERIALIZER_SUPERVISOR_RESTART_DELAY.as_millis(),
            "restarting Telegram update materializer after transient lease failure"
        );
        if sleep_or_watch_stop(stop.clone(), MATERIALIZER_SUPERVISOR_RESTART_DELAY).await {
            return Ok(());
        }
    }
}

struct MaterializerSupervisorRunning(UpdateMaterializerMetrics);

impl Drop for MaterializerSupervisorRunning {
    fn drop(&mut self) {
        self.0.set_supervisor_running(false);
        self.0.set_lease_held(false);
    }
}

async fn wait_for_stop(mut stop: watch::Receiver<bool>) {
    loop {
        if *stop.borrow() || stop.changed().await.is_err() {
            return;
        }
    }
}

async fn sleep_or_watch_stop(stop: watch::Receiver<bool>, delay: Duration) -> bool {
    tokio::select! {
        () = tokio::time::sleep(delay) => false,
        () = wait_for_stop(stop.clone()) => true,
    }
}

/// Run one active materializer while publishing live runtime metrics.
pub async fn run_update_materializer_with_metrics_until<Stop>(
    stream: RedisUpdateStream,
    store: PostgresTelegramDeliveryStore,
    bot_id: i64,
    config: UpdateStreamMaterializerConfig,
    metrics: UpdateMaterializerMetrics,
    stop: Stop,
) -> Result<UpdateMaterializerWorkerReport, UpdateStreamConfigError>
where
    Stop: Future<Output = ()> + Send,
{
    let config = config.validate()?;
    let max_rows = config.effective_max_rows(MATERIALIZED_UPDATE_BINDS_PER_ROW)?;
    let mut report = UpdateMaterializerWorkerReport::default();
    let mut external_stop = std::pin::pin!(stop);
    let mut consecutive_redis_failures = 0_u32;

    while let Err(error) = stream.ensure_consumer_group().await {
        record_redis_failure(&mut report, &metrics, &error);
        consecutive_redis_failures = consecutive_redis_failures.saturating_add(1);
        if sleep_or_stop(
            &mut external_stop,
            retry_backoff(consecutive_redis_failures),
        )
        .await
        {
            return Ok(report);
        }
    }

    let owner_consumer_id = materializer_owner_consumer_id(bot_id);
    let mut waiting_for_lease = false;
    loop {
        if stop_requested_now(&mut external_stop).await {
            return Ok(report);
        }
        // Acquiring the lease mutates Redis. Await it to completion so a
        // concurrent shutdown cannot discard a successful acquisition and
        // strand the next materializer behind the full lease TTL.
        let acquired = stream
            .acquire_materializer_lease(&owner_consumer_id, MATERIALIZER_LEASE_TTL)
            .await;
        match acquired {
            Ok(true) => break,
            Ok(false) => {
                consecutive_redis_failures = 0;
                if !waiting_for_lease {
                    waiting_for_lease = true;
                    tracing::info!(
                        stream_key = stream.key(),
                        %owner_consumer_id,
                        "Telegram update materializer is standing by for the active Redis lease"
                    );
                }
                if sleep_or_stop(&mut external_stop, MATERIALIZER_LEASE_ACQUIRE_RECHECK).await {
                    return Ok(report);
                }
            }
            Err(error) => {
                record_redis_failure(&mut report, &metrics, &error);
                consecutive_redis_failures = consecutive_redis_failures.saturating_add(1);
                if sleep_or_stop(
                    &mut external_stop,
                    retry_backoff(consecutive_redis_failures),
                )
                .await
                {
                    return Ok(report);
                }
            }
        }
    }
    tracing::info!(
        stream_key = stream.key(),
        %owner_consumer_id,
        lease_ttl_seconds = MATERIALIZER_LEASE_TTL.as_secs(),
        lease_renew_seconds = MATERIALIZER_LEASE_RENEW_INTERVAL.as_secs(),
        "acquired the active Telegram update materializer lease"
    );
    metrics.set_lease_held(true);

    let (lease_lost_tx, lease_lost_observer) = watch::channel(false);
    let lease_heartbeat = tokio::spawn(run_materializer_lease_heartbeat(
        stream.clone(),
        owner_consumer_id.clone(),
        MATERIALIZER_LEASE_TTL,
        MATERIALIZER_LEASE_RENEW_INTERVAL,
        lease_lost_tx,
    ));
    let mut lease_lost_stop = lease_lost_observer.clone();
    let combined_stop = async {
        tokio::select! {
            () = external_stop.as_mut() => {}
            () = wait_for_materializer_lease_loss(&mut lease_lost_stop) => {}
        }
    };
    let mut stop = std::pin::pin!(combined_stop);
    consecutive_redis_failures = 0;

    let mut pending = VecDeque::with_capacity(max_rows);
    let mut reclaim_cursor = UpdateStreamId::MIN;
    // The lease is exclusive, so every pre-existing PEL owner is abandoned at
    // this point. Adopt the whole PEL without an extra idle wait once; later
    // reclaim scans retain the configured crash-recovery threshold.
    let mut startup_reclaim = true;

    loop {
        if pending.is_empty() {
            if stop_requested_now(&mut stop).await {
                break;
            }
            // XAUTOCLAIM changes PEL ownership. Once started, its response must
            // be observed so a graceful stop can drain every claimed entry.
            let reclaim_idle = reclaim_idle_for_pass(startup_reclaim, config.reclaim_idle);
            let reclaimed = stream
                .reclaim_pending(&owner_consumer_id, reclaim_idle, reclaim_cursor, max_rows)
                .await;
            match reclaimed {
                Ok(claim) => {
                    consecutive_redis_failures = 0;
                    reclaim_cursor = claim.next_start;
                    if startup_reclaim && reclaim_cursor == UpdateStreamId::MIN {
                        startup_reclaim = false;
                    }
                    if claim.invalid_entries {
                        tracing::warn!(
                            stream_key = stream.key(),
                            "Redis reported invalid entries while reclaiming the update PEL"
                        );
                    }
                    if !claim.deleted_ids.is_empty() {
                        report.redis_deleted_pending_entries = report
                            .redis_deleted_pending_entries
                            .saturating_add(claim.deleted_ids.len());
                        tracing::error!(
                            stream_key = stream.key(),
                            deleted_pending_entries = claim.deleted_ids.len(),
                            "update Stream entries disappeared before materialization"
                        );
                    }
                    if !claim.entries.is_empty() {
                        report.reclaimed_entries =
                            report.reclaimed_entries.saturating_add(claim.entries.len());
                        metrics.record_reclaimed(claim.entries.len());
                        pending.extend(claim.entries);
                    }
                }
                Err(error) => {
                    record_redis_failure(&mut report, &metrics, &error);
                    consecutive_redis_failures = consecutive_redis_failures.saturating_add(1);
                    if sleep_or_stop(&mut stop, retry_backoff(consecutive_redis_failures)).await {
                        break;
                    }
                    continue;
                }
            }

            if pending.is_empty() && reclaim_cursor != UpdateStreamId::MIN {
                continue;
            }

            if pending.is_empty() {
                let stats = tokio::select! {
                    () = stop.as_mut() => break,
                    result = stream.stats() => result,
                };
                let pending_count = match stats {
                    Ok(stats) => {
                        consecutive_redis_failures = 0;
                        stats.group.map_or(0, |group| group.pending)
                    }
                    Err(error) => {
                        record_redis_failure(&mut report, &metrics, &error);
                        consecutive_redis_failures = consecutive_redis_failures.saturating_add(1);
                        if sleep_or_stop(&mut stop, retry_backoff(consecutive_redis_failures)).await
                        {
                            break;
                        }
                        continue;
                    }
                };

                // Do not materialize newer entries ahead of a not-yet-idle PEL
                // batch left by a previous consumer.
                if pending_count > 0 {
                    if sleep_or_stop(
                        &mut stop,
                        config.reclaim_idle.min(MATERIALIZER_RECLAIM_RECHECK_MAX),
                    )
                    .await
                    {
                        break;
                    }
                    continue;
                }

                if stop_requested_now(&mut stop).await {
                    break;
                }
                // XREADGROUP commits entries to the PEL. Never race this future
                // with shutdown: a winning stop branch can otherwise discard
                // the claimed rows and force the next process to wait for
                // XAUTOCLAIM's idle threshold.
                let entries = stream
                    .read_group(&owner_consumer_id, config.read_block_timeout, max_rows)
                    .await;
                match entries {
                    Ok(entries) => {
                        consecutive_redis_failures = 0;
                        if entries.is_empty() {
                            continue;
                        }
                        report.read_entries = report.read_entries.saturating_add(entries.len());
                        pending.extend(entries);
                    }
                    Err(error) => {
                        record_redis_failure(&mut report, &metrics, &error);
                        consecutive_redis_failures = consecutive_redis_failures.saturating_add(1);
                        if sleep_or_stop(&mut stop, retry_backoff(consecutive_redis_failures)).await
                        {
                            break;
                        }
                        continue;
                    }
                }

                let mut batch_lease_lost = lease_lost_observer.clone();
                let batch_stop = wait_for_materializer_lease_loss(&mut batch_lease_lost);
                tokio::pin!(batch_stop);
                if fill_new_batch_until_deadline(
                    &stream,
                    &owner_consumer_id,
                    &mut pending,
                    max_rows,
                    config.batch_max_bytes,
                    config.batch_max_wait,
                    &mut batch_stop,
                    &mut report,
                    &metrics,
                )
                .await
                {
                    break;
                }
            }
        }

        let batch = take_batch(&mut pending, max_rows, config.batch_max_bytes);
        if batch.is_empty() {
            continue;
        }
        metrics.record_batch(
            batch.len(),
            batch_payload_bytes(&batch),
            max_rows,
            config.batch_max_bytes,
        );
        let mut batch_lease_lost = lease_lost_observer.clone();
        let batch_stop = wait_for_materializer_lease_loss(&mut batch_lease_lost);
        tokio::pin!(batch_stop);
        if !materialize_and_ack_batch(
            &stream,
            &store,
            bot_id,
            &batch,
            config.db_timeout,
            &mut batch_stop,
            &mut report,
            &metrics,
        )
        .await
        {
            break;
        }
    }

    let lease_was_lost = *lease_lost_observer.borrow();
    if !lease_was_lost {
        lease_heartbeat.abort();
    }
    match lease_heartbeat.await {
        Ok(reason) => {
            report.last_error = Some(reason.clone());
            tracing::error!(
                stream_key = stream.key(),
                %owner_consumer_id,
                %reason,
                "Telegram update materializer stopped after losing its Redis lease"
            );
        }
        Err(error) if error.is_cancelled() => {}
        Err(error) => {
            let reason = format!("materializer lease heartbeat stopped unexpectedly: {error}");
            report.last_error = Some(reason.clone());
            tracing::error!(
                stream_key = stream.key(),
                %owner_consumer_id,
                %reason,
                "Telegram update materializer lease heartbeat failed"
            );
        }
    }
    metrics.set_lease_held(false);
    match stream.release_materializer_lease(&owner_consumer_id).await {
        Ok(true) => tracing::debug!(
            stream_key = stream.key(),
            %owner_consumer_id,
            "released the Telegram update materializer lease"
        ),
        Ok(false) => tracing::debug!(
            stream_key = stream.key(),
            %owner_consumer_id,
            "Telegram update materializer lease was no longer owned at shutdown"
        ),
        Err(error) => tracing::warn!(
            stream_key = stream.key(),
            %owner_consumer_id,
            %error,
            "failed to release the Telegram update materializer lease; TTL will expire it"
        ),
    }

    Ok(report)
}

async fn run_materializer_lease_heartbeat(
    stream: RedisUpdateStream,
    owner_consumer_id: String,
    lease_ttl: Duration,
    renew_interval: Duration,
    lease_lost: watch::Sender<bool>,
) -> String {
    loop {
        tokio::time::sleep(renew_interval).await;
        let reason = match stream
            .renew_materializer_lease(&owner_consumer_id, lease_ttl)
            .await
        {
            Ok(true) => continue,
            Ok(false) => "Redis materializer lease ownership was lost".to_owned(),
            Err(error) => format!("Redis materializer lease renewal failed: {error}"),
        };
        lease_lost.send_replace(true);
        return reason;
    }
}

async fn wait_for_materializer_lease_loss(lease_lost: &mut watch::Receiver<bool>) {
    loop {
        if *lease_lost.borrow() {
            return;
        }
        if lease_lost.changed().await.is_err() {
            return;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn fill_new_batch_until_deadline<Stop>(
    stream: &RedisUpdateStream,
    owner_consumer_id: &str,
    pending: &mut VecDeque<RawUpdateStreamEntry>,
    max_rows: usize,
    max_bytes: usize,
    max_wait: Duration,
    stop: &mut Pin<&mut Stop>,
    report: &mut UpdateMaterializerWorkerReport,
    metrics: &UpdateMaterializerMetrics,
) -> bool
where
    Stop: Future<Output = ()>,
{
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        let (batch_rows, batch_bytes) = batch_prefix(pending, max_rows, max_bytes);
        if batch_rows == max_rows
            || batch_bytes >= max_bytes
            || batch_rows < pending.len()
            || tokio::time::Instant::now() >= deadline
        {
            return false;
        }

        let remaining_rows = max_rows.saturating_sub(batch_rows);
        let entries = tokio::select! {
            () = stop.as_mut() => return true,
            result = stream.read_group(owner_consumer_id, Duration::ZERO, remaining_rows) => result,
        };
        match entries {
            Ok(entries) if entries.is_empty() => {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return false;
                }
                if sleep_or_stop(stop, remaining.min(MATERIALIZER_EMPTY_FILL_POLL_INTERVAL)).await {
                    return true;
                }
            }
            Ok(entries) => {
                report.read_entries = report.read_entries.saturating_add(entries.len());
                pending.extend(entries);
            }
            Err(error) => {
                record_redis_failure(report, metrics, &error);
                // The entries already held in the PEL still form a valid batch.
                return false;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn materialize_and_ack_batch<Stop>(
    stream: &RedisUpdateStream,
    store: &PostgresTelegramDeliveryStore,
    bot_id: i64,
    entries: &[RawUpdateStreamEntry],
    db_timeout: Duration,
    stop: &mut Pin<&mut Stop>,
    report: &mut UpdateMaterializerWorkerReport,
    metrics: &UpdateMaterializerMetrics,
) -> bool
where
    Stop: Future<Output = ()>,
{
    let batch = preprocess_batch(entries, bot_id);
    let mut consecutive_db_failures = 0_u32;

    let (materialization, transaction_latency) = loop {
        let transaction_started = Instant::now();
        let result = tokio::select! {
            () = stop.as_mut() => return false,
            result = tokio::time::timeout(
                db_timeout,
                store.materialize_update_batch(&batch.updates, &batch.quarantine),
            ) => result,
        };
        match result {
            Ok(Ok(materialization)) => break (materialization, transaction_started.elapsed()),
            Ok(Err(error)) => {
                report.db_failures = report.db_failures.saturating_add(1);
                metrics.record_db_failure();
                report.last_error = Some(error.to_string());
                consecutive_db_failures = consecutive_db_failures.saturating_add(1);
                tracing::warn!(
                    %error,
                    consecutive_db_failures,
                    batch_entries = batch.stream_ids.len(),
                    "failed to bulk-materialize Telegram update batch"
                );
            }
            Err(error) => {
                report.db_failures = report.db_failures.saturating_add(1);
                metrics.record_db_failure();
                report.last_error = Some(error.to_string());
                consecutive_db_failures = consecutive_db_failures.saturating_add(1);
                tracing::warn!(
                    %error,
                    consecutive_db_failures,
                    batch_entries = batch.stream_ids.len(),
                    "Telegram update bulk-materialization timed out"
                );
            }
        }

        if sleep_or_stop(stop, retry_backoff(consecutive_db_failures)).await {
            return false;
        }
    };

    record_materialization(report, &batch, materialization);
    metrics.record_materialization(&batch, materialization, transaction_latency);

    let mut consecutive_ack_failures = 0_u32;
    loop {
        let acknowledged = tokio::select! {
            () = stop.as_mut() => return false,
            result = stream.acknowledge_and_delete(&batch.stream_ids) => result,
        };
        match acknowledged {
            Ok(acknowledged) => {
                if acknowledged.acknowledged != acknowledged.requested
                    || acknowledged.deleted != acknowledged.requested
                {
                    report.ack_delete_mismatches = report.ack_delete_mismatches.saturating_add(1);
                    metrics.record_ack_delete_mismatch();
                    tracing::warn!(
                        requested = acknowledged.requested,
                        acknowledged = acknowledged.acknowledged,
                        deleted = acknowledged.deleted,
                        "Redis update batch ACK/Delete counts differ"
                    );
                }
                return true;
            }
            Err(error) => {
                record_redis_failure(report, metrics, &error);
                consecutive_ack_failures = consecutive_ack_failures.saturating_add(1);
                tracing::warn!(
                    %error,
                    consecutive_ack_failures,
                    batch_entries = batch.stream_ids.len(),
                    "Postgres commit succeeded but Redis update ACK/Delete failed"
                );
                if sleep_or_stop(stop, retry_backoff(consecutive_ack_failures)).await {
                    return false;
                }
            }
        }
    }
}

fn preprocess_batch(entries: &[RawUpdateStreamEntry], expected_bot_id: i64) -> PreprocessedBatch {
    let mut ordered = entries.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|entry| entry.stream_id);

    let mut stream_ids = Vec::with_capacity(entries.len());
    let mut updates = Vec::with_capacity(entries.len());
    let mut quarantine = Vec::with_capacity(entries.len());
    let mut update_indexes = HashMap::with_capacity(entries.len());

    for raw in ordered {
        stream_ids.push(raw.stream_id);
        match materialized_input(raw, expected_bot_id) {
            Ok(update) => {
                let key = (update.bot_id, update.update_id);
                if let Some(index) = update_indexes.get(&key).copied() {
                    aggregate_duplicate(&mut updates[index], &update);
                } else {
                    update_indexes.insert(key, updates.len());
                    updates.push(update);
                }
            }
            Err(conversion) => {
                let (error_class, error, decoded) = *conversion;
                quarantine.push(quarantine_input(
                    raw,
                    expected_bot_id,
                    decoded.as_ref(),
                    error_class,
                    error,
                ));
            }
        }
    }

    PreprocessedBatch {
        stream_ids,
        updates,
        quarantine,
    }
}

type InputConversionError = Box<(&'static str, String, Option<UpdateStreamEntry>)>;

fn materialized_input(
    raw: &RawUpdateStreamEntry,
    expected_bot_id: i64,
) -> Result<MaterializedUpdateInput, InputConversionError> {
    let entry = raw
        .decode()
        .map_err(|error| Box::new(("stream_envelope", error.to_string(), None)))?;
    if entry.bot_id != expected_bot_id {
        return Err(Box::new((
            "bot_id_mismatch",
            format!(
                "stream belongs to bot {expected_bot_id}, entry belongs to bot {}",
                entry.bot_id
            ),
            Some(entry),
        )));
    }

    let stream_ms = i64::try_from(entry.stream_id.milliseconds).map_err(|_| {
        Box::new((
            "stream_id_out_of_range",
            "Redis Stream millisecond component exceeds PostgreSQL BIGINT".to_owned(),
            Some(entry.clone()),
        ))
    })?;
    let stream_seq = i64::try_from(entry.stream_id.sequence).map_err(|_| {
        Box::new((
            "stream_id_out_of_range",
            "Redis Stream sequence component exceeds PostgreSQL BIGINT".to_owned(),
            Some(entry.clone()),
        ))
    })?;
    let received_at = offset_from_unix_millis(entry.received_at_unix_ms)
        .map_err(|error| Box::new(("received_at_out_of_range", error, Some(entry.clone()))))?;
    let update = decode_telegram_update_json_slice(&entry.raw_payload)
        .map_err(|error| Box::new(("telegram_payload", error.to_string(), Some(entry.clone()))))?;
    if entry
        .update_id
        .is_some_and(|update_id| update_id != update.id)
    {
        return Err(Box::new((
            "update_id_mismatch",
            format!(
                "stream envelope update ID {:?} differs from payload update ID {}",
                entry.update_id, update.id
            ),
            Some(entry),
        )));
    }

    let chat_id = update.get_chat_id().map(i64::from);
    let user_id = update.get_user_id().map(i64::from);
    let thread_id = update
        .get_message()
        .and_then(|message| message.message_thread_id)
        .and_then(|thread_id| i32::try_from(thread_id).ok());
    let ordering_key = ordering_key(expected_bot_id, update.id, chat_id, thread_id, user_id);
    let disposition = materialized_disposition(&update);

    Ok(MaterializedUpdateInput {
        bot_id: entry.bot_id,
        update_id: update.id,
        schema_version: i16::try_from(entry.schema_version).unwrap_or(i16::MAX),
        source: entry.source.as_str().to_owned(),
        stream_ms,
        stream_seq,
        last_stream_ms: stream_ms,
        last_stream_seq: stream_seq,
        raw_payload: entry.raw_payload,
        payload_sha256: entry.payload_sha256.to_vec(),
        payload_conflict: false,
        update_type: Some(update_name(&update).to_owned()),
        telegram_event_at: telegram_event_at(&update),
        first_received_at: received_at,
        last_received_at: received_at,
        delivery_count: 1,
        ordering_key,
        priority: 0,
        chat_id,
        thread_id,
        user_id,
        disposition,
    })
}

fn materialized_disposition(update: &TelegramUpdate) -> MaterializedUpdateDisposition {
    if is_passive_update(update) {
        MaterializedUpdateDisposition::Ignored {
            reason: if matches!(&update.update_type, TelegramUpdateType::Unknown(_)) {
                "unsupported_type".to_owned()
            } else {
                format!("passive_type:{}", update_name(update))
            },
        }
    } else {
        MaterializedUpdateDisposition::Pending
    }
}

fn aggregate_duplicate(
    canonical: &mut MaterializedUpdateInput,
    duplicate: &MaterializedUpdateInput,
) {
    canonical.delivery_count = canonical
        .delivery_count
        .saturating_add(duplicate.delivery_count.max(1));
    canonical.first_received_at = canonical.first_received_at.min(duplicate.first_received_at);
    canonical.last_received_at = canonical.last_received_at.max(duplicate.last_received_at);
    canonical.payload_conflict |=
        canonical.payload_sha256 != duplicate.payload_sha256 || duplicate.payload_conflict;
    if (duplicate.last_stream_ms, duplicate.last_stream_seq)
        > (canonical.last_stream_ms, canonical.last_stream_seq)
    {
        canonical.last_stream_ms = duplicate.last_stream_ms;
        canonical.last_stream_seq = duplicate.last_stream_seq;
    }
}

fn quarantine_input(
    raw: &RawUpdateStreamEntry,
    expected_bot_id: i64,
    decoded: Option<&UpdateStreamEntry>,
    error_class: &str,
    error: String,
) -> QuarantinedUpdateInput {
    let raw_payload = decoded
        .map(|entry| entry.raw_payload.clone())
        .or_else(|| raw.raw_payload().map(<[u8]>::to_vec))
        .unwrap_or_default();
    let payload_sha256 = Sha256::digest(&raw_payload).to_vec();
    let received_at = decoded
        .and_then(|entry| offset_from_unix_millis(entry.received_at_unix_ms).ok())
        .or_else(|| offset_from_stream_id(raw.stream_id))
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    QuarantinedUpdateInput {
        bot_id: decoded.map_or(expected_bot_id, |entry| entry.bot_id),
        stream_ms: i64::try_from(raw.stream_id.milliseconds).unwrap_or(i64::MAX),
        stream_seq: i64::try_from(raw.stream_id.sequence).unwrap_or(i64::MAX),
        schema_version: decoded.map_or_else(
            || best_effort_i16_field(raw, "schema_version").unwrap_or_default(),
            |entry| i16::try_from(entry.schema_version).unwrap_or(i16::MAX),
        ),
        source: decoded.map_or_else(
            || best_effort_quarantine_source(raw),
            |entry| entry.source.as_str().to_owned(),
        ),
        raw_payload,
        payload_sha256,
        first_received_at: received_at,
        last_received_at: received_at,
        delivery_count: 1,
        error_class: error_class.to_owned(),
        error,
    }
}

fn ordering_key(
    bot_id: i64,
    update_id: i64,
    chat_id: Option<i64>,
    thread_id: Option<i32>,
    user_id: Option<i64>,
) -> String {
    if let Some(chat_id) = chat_id {
        return format!(
            "dialog:{bot_id}:{chat_id}:{}",
            thread_id.unwrap_or_default()
        );
    }
    if let Some(user_id) = user_id {
        return format!("user:{bot_id}:{user_id}");
    }
    format!("update:{bot_id}:{update_id}")
}

fn telegram_event_at(update: &TelegramUpdate) -> Option<OffsetDateTime> {
    let unix_timestamp = match &update.update_type {
        TelegramUpdateType::Message(message)
        | TelegramUpdateType::BusinessMessage(message)
        | TelegramUpdateType::ChannelPost(message)
        | TelegramUpdateType::GuestMessage(message) => Some(message.date),
        TelegramUpdateType::EditedBusinessMessage(message)
        | TelegramUpdateType::EditedChannelPost(message)
        | TelegramUpdateType::EditedMessage(message) => {
            Some(message.edit_date.unwrap_or(message.date))
        }
        TelegramUpdateType::BotStatus(status) | TelegramUpdateType::UserStatus(status) => {
            Some(status.date)
        }
        TelegramUpdateType::BusinessConnection(connection) => Some(connection.date),
        TelegramUpdateType::ChatJoinRequest(request) => Some(request.date),
        TelegramUpdateType::MessageReaction(reaction) => Some(reaction.date),
        TelegramUpdateType::MessageReactionCount(reaction) => Some(reaction.date),
        _ => None,
    }?;
    OffsetDateTime::from_unix_timestamp(unix_timestamp).ok()
}

fn take_batch(
    pending: &mut VecDeque<RawUpdateStreamEntry>,
    max_rows: usize,
    max_bytes: usize,
) -> Vec<RawUpdateStreamEntry> {
    let (rows, _) = batch_prefix(pending, max_rows, max_bytes);
    pending.drain(..rows).collect()
}

fn batch_prefix(
    pending: &VecDeque<RawUpdateStreamEntry>,
    max_rows: usize,
    max_bytes: usize,
) -> (usize, usize) {
    let mut rows = 0;
    let mut bytes = 0_usize;
    for entry in pending.iter().take(max_rows) {
        let entry_bytes = entry.raw_payload().map_or(0, <[u8]>::len);
        let next_bytes = bytes.saturating_add(entry_bytes);
        if rows > 0 && next_bytes > max_bytes {
            break;
        }
        rows += 1;
        bytes = next_bytes;
        if bytes >= max_bytes {
            break;
        }
    }
    (rows, bytes)
}

fn batch_payload_bytes(entries: &[RawUpdateStreamEntry]) -> usize {
    entries.iter().fold(0_usize, |total, entry| {
        total.saturating_add(entry.raw_payload().map_or(0, <[u8]>::len))
    })
}

fn offset_from_unix_millis(unix_millis: i64) -> Result<OffsetDateTime, String> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(unix_millis) * 1_000_000)
        .map_err(|error| error.to_string())
}

fn offset_from_stream_id(stream_id: UpdateStreamId) -> Option<OffsetDateTime> {
    i64::try_from(stream_id.milliseconds)
        .ok()
        .and_then(|millis| offset_from_unix_millis(millis).ok())
}

fn best_effort_i16_field(entry: &RawUpdateStreamEntry, field: &str) -> Option<i16> {
    best_effort_string_field(entry, field)?.parse().ok()
}

fn best_effort_string_field(entry: &RawUpdateStreamEntry, field: &str) -> Option<String> {
    std::str::from_utf8(entry.fields.get(field)?)
        .ok()
        .map(str::to_owned)
}

fn best_effort_quarantine_source(entry: &RawUpdateStreamEntry) -> String {
    match best_effort_string_field(entry, "source").as_deref() {
        Some(source @ ("webhook" | "long_poll" | "legacy")) => source.to_owned(),
        _ => "legacy".to_owned(),
    }
}

fn record_materialization(
    report: &mut UpdateMaterializerWorkerReport,
    batch: &PreprocessedBatch,
    materialization: MaterializationReport,
) {
    report.materialized_batches = report.materialized_batches.saturating_add(1);
    report.materialized_deliveries = report
        .materialized_deliveries
        .saturating_add(batch.stream_ids.len());
    report.inserted = report.inserted.saturating_add(materialization.inserted);
    report.duplicates = report.duplicates.saturating_add(materialization.duplicates);
    report.conflicted = report.conflicted.saturating_add(materialization.conflicted);
    report.quarantined = report
        .quarantined
        .saturating_add(materialization.quarantined);
    if !batch.updates.is_empty() {
        report.inbox_insert_statements = report.inbox_insert_statements.saturating_add(1);
    }
    if !batch.quarantine.is_empty() {
        report.quarantine_insert_statements = report.quarantine_insert_statements.saturating_add(1);
    }
}

fn record_redis_failure(
    report: &mut UpdateMaterializerWorkerReport,
    metrics: &UpdateMaterializerMetrics,
    error: &openplotva_updates::UpdateStreamError,
) {
    report.redis_failures = report.redis_failures.saturating_add(1);
    metrics.record_redis_failure();
    report.last_error = Some(error.to_string());
    tracing::warn!(%error, "Redis update Stream operation failed");
}

fn retry_backoff(consecutive_failures: u32) -> Duration {
    let shift = consecutive_failures.saturating_sub(1).min(8);
    let capped =
        (MATERIALIZER_RETRY_BACKOFF_MIN * (1_u32 << shift)).min(MATERIALIZER_RETRY_BACKOFF_CAP);
    let half_millis = u64::try_from(capped.as_millis()).unwrap_or(u64::MAX) / 2;
    Duration::from_millis(half_millis + rand::random::<u64>() % half_millis.saturating_add(1))
}

fn reclaim_idle_for_pass(startup_reclaim: bool, configured: Duration) -> Duration {
    if startup_reclaim {
        Duration::ZERO
    } else {
        configured
    }
}

async fn stop_requested_now<Stop>(stop: &mut Pin<&mut Stop>) -> bool
where
    Stop: Future<Output = ()>,
{
    tokio::select! {
        biased;
        () = stop.as_mut() => true,
        () = std::future::ready(()) => false,
    }
}

async fn sleep_or_stop<Stop>(stop: &mut Pin<&mut Stop>, duration: Duration) -> bool
where
    Stop: Future<Output = ()>,
{
    tokio::select! {
        () = stop.as_mut() => true,
        () = tokio::time::sleep(duration) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        env,
        error::Error,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use openplotva_storage::{MATERIALIZED_UPDATE_BINDS_PER_ROW, MaterializationReport};
    use openplotva_updates::{
        RawUpdateStreamEntry, RedisUpdateStream, UPDATE_STREAM_SCHEMA_VERSION, UpdateStreamId,
        UpdateStreamMaterializerConfig,
    };
    use sha2::{Digest, Sha256};

    use super::{
        MATERIALIZER_LEASE_RENEW_INTERVAL, MATERIALIZER_LEASE_TTL, UpdateMaterializerMetrics,
        batch_prefix, materializer_owner_consumer_id, preprocess_batch, reclaim_idle_for_pass,
        run_materializer_lease_heartbeat, stop_requested_now, take_batch,
        wait_for_materializer_lease_loss,
    };

    #[tokio::test]
    async fn stop_probe_is_nonblocking_and_observes_shutdown() {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let stop = async move {
            let _ = stop_rx.await;
        };
        tokio::pin!(stop);

        assert!(!stop_requested_now(&mut stop).await);
        stop_tx.send(()).expect("send shutdown");
        assert!(stop_requested_now(&mut stop).await);
    }

    #[test]
    fn first_reclaim_pass_immediately_adopts_abandoned_pel_entries() {
        let configured = Duration::from_secs(60);

        assert_eq!(reclaim_idle_for_pass(true, configured), Duration::ZERO);
        assert_eq!(reclaim_idle_for_pass(false, configured), configured);
    }

    #[test]
    fn materializer_owner_consumer_ids_are_unique_and_renew_before_expiry() {
        let first = materializer_owner_consumer_id(42);
        let second = materializer_owner_consumer_id(42);

        assert_ne!(first, second);
        assert!(first.starts_with("openplotva-materializer-42-"));
        assert!(MATERIALIZER_LEASE_RENEW_INTERVAL < MATERIALIZER_LEASE_TTL / 2);
    }

    #[tokio::test]
    async fn live_materializer_heartbeat_renews_and_stops_after_lease_loss_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:updates:materializer-lease:{suffix}");
        let lease_key = format!("{key}:materializer-lease");
        let stream = RedisUpdateStream::with_key(client.clone(), key.clone());
        let mut connection = client.get_multiplexed_async_connection().await?;
        let _: usize = redis::cmd("DEL")
            .arg(&key)
            .arg(&lease_key)
            .query_async(&mut connection)
            .await?;

        let lease_ttl = Duration::from_millis(200);
        let renew_interval = Duration::from_millis(20);
        assert!(
            stream
                .acquire_materializer_lease("owner-a", lease_ttl)
                .await?
        );
        let (lease_lost_tx, mut lease_lost) = tokio::sync::watch::channel(false);
        let heartbeat = tokio::spawn(run_materializer_lease_heartbeat(
            stream.clone(),
            "owner-a".to_owned(),
            lease_ttl,
            renew_interval,
            lease_lost_tx,
        ));

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !stream
                .acquire_materializer_lease("owner-b", lease_ttl)
                .await?,
            "the heartbeat must retain the lease beyond its original TTL"
        );

        assert!(stream.release_materializer_lease("owner-a").await?);
        assert!(
            stream
                .acquire_materializer_lease("owner-b", lease_ttl)
                .await?
        );
        tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_materializer_lease_loss(&mut lease_lost),
        )
        .await?;
        let reason = heartbeat.await?;
        assert!(reason.contains("ownership was lost"));
        assert!(!stream.release_materializer_lease("owner-a").await?);
        assert!(stream.release_materializer_lease("owner-b").await?);

        let _: usize = redis::cmd("DEL")
            .arg(&key)
            .arg(&lease_key)
            .query_async(&mut connection)
            .await?;
        Ok(())
    }

    #[test]
    fn live_metrics_are_shared_and_record_bulk_invariants() {
        let metrics = UpdateMaterializerMetrics::default();
        let observer = metrics.clone();
        let payload = br#"{"update_id":41,"message":{"message_id":1,"date":1700000000,"chat":{"id":7,"type":"private","first_name":"Ada"},"from":{"id":9,"is_bot":false,"first_name":"Ada"},"text":"ok"}}"#;
        let batch = preprocess_batch(
            &[stream_entry(
                1_700_000_000_001,
                0,
                41,
                1_700_000_000_010,
                payload,
            )],
            123,
        );

        metrics.record_batch(256, 4 * 1024 * 1024, 512, 8 * 1024 * 1024);
        metrics.record_reclaimed(3);
        metrics.record_materialization(
            &batch,
            MaterializationReport {
                inserted: 1,
                duplicates: 2,
                conflicted: 1,
                quarantined: 0,
            },
            Duration::from_millis(12),
        );

        let snapshot = observer.snapshot();
        assert_eq!(snapshot.batch_rows, 256);
        assert_eq!(snapshot.batch_bytes, 4 * 1024 * 1024);
        assert_eq!(snapshot.batch_fill_ratio, 0.5);
        assert_eq!(
            snapshot.last_transaction_latency,
            Some(Duration::from_millis(12))
        );
        assert_eq!(snapshot.materialized_batches, 1);
        assert_eq!(snapshot.inbox_insert_statements, 1);
        assert_eq!(snapshot.quarantine_insert_statements, 0);
        assert_eq!(snapshot.inserted, 1);
        assert_eq!(snapshot.duplicates, 2);
        assert_eq!(snapshot.conflicted, 1);
        assert_eq!(snapshot.reclaims, 1);
        assert_eq!(snapshot.reclaimed_entries, 3);
    }

    #[test]
    fn duplicate_updates_are_aggregated_before_storage() {
        let first = br#"{"update_id":41,"message":{"message_id":1,"date":1700000000,"chat":{"id":7,"type":"private","first_name":"Ada"},"from":{"id":9,"is_bot":false,"first_name":"Ada"},"text":"first"}}"#;
        let same = first;
        let conflict = br#"{"update_id":41,"message":{"message_id":1,"date":1700000000,"chat":{"id":7,"type":"private","first_name":"Ada"},"from":{"id":9,"is_bot":false,"first_name":"Ada"},"text":"changed"}}"#;
        let batch = vec![
            stream_entry(1_700_000_000_003, 0, 41, 1_700_000_000_030, conflict),
            stream_entry(1_700_000_000_001, 0, 41, 1_700_000_000_010, first),
            stream_entry(1_700_000_000_002, 0, 41, 1_700_000_000_020, same),
        ];

        let prepared = preprocess_batch(&batch, 123);

        assert_eq!(prepared.stream_ids.len(), 3);
        assert!(prepared.quarantine.is_empty());
        assert_eq!(prepared.updates.len(), 1);
        let update = &prepared.updates[0];
        assert_eq!(update.delivery_count, 3);
        assert_eq!(update.raw_payload, first);
        assert!(update.payload_conflict);
        assert_eq!(
            (update.stream_ms, update.stream_seq),
            (1_700_000_000_001, 0)
        );
        assert_eq!(
            (update.last_stream_ms, update.last_stream_seq),
            (1_700_000_000_003, 0)
        );
        assert_eq!(
            update.first_received_at.unix_timestamp_nanos(),
            1_700_000_000_010_000_000
        );
        assert_eq!(
            update.last_received_at.unix_timestamp_nanos(),
            1_700_000_000_030_000_000
        );
        assert_eq!(update.ordering_key, "dialog:123:7:0");
    }

    #[test]
    fn exact_byte_limit_is_one_batch_and_next_entry_stays_pending() {
        const MIB: usize = 1024 * 1024;
        let mut pending = VecDeque::from([
            raw_entry_with_payload_size(1, 3 * MIB),
            raw_entry_with_payload_size(2, 5 * MIB),
            raw_entry_with_payload(3, &[0]),
        ]);

        assert_eq!(batch_prefix(&pending, 512, 8 * MIB), (2, 8 * MIB));
        let batch = take_batch(&mut pending, 512, 8 * MIB);

        assert_eq!(batch.len(), 2);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].stream_id.milliseconds, 3);
    }

    #[test]
    fn configured_rows_are_capped_by_postgres_bind_budget() {
        let configured = UpdateStreamMaterializerConfig {
            batch_max_rows: 1_000,
            ..UpdateStreamMaterializerConfig::default()
        };

        assert_eq!(
            configured
                .effective_max_rows(MATERIALIZED_UPDATE_BINDS_PER_ROW)
                .expect("valid materializer config"),
            1_000
        );
        assert_eq!(
            configured
                .effective_max_rows(100)
                .expect("valid materializer config"),
            600
        );
    }

    #[test]
    fn malformed_payload_is_quarantined_without_blocking_valid_update() {
        let malformed = stream_entry(1_700_000_000_001, 0, 40, 1_700_000_000_010, b"not-json");
        let valid_payload = br#"{"update_id":41,"message":{"message_id":1,"date":1700000000,"chat":{"id":7,"type":"private","first_name":"Ada"},"from":{"id":9,"is_bot":false,"first_name":"Ada"},"text":"ok"}}"#;
        let valid = stream_entry(1_700_000_000_002, 0, 41, 1_700_000_000_020, valid_payload);

        let prepared = preprocess_batch(&[malformed, valid], 123);

        assert_eq!(prepared.stream_ids.len(), 2);
        assert_eq!(prepared.updates.len(), 1);
        assert_eq!(prepared.quarantine.len(), 1);
        assert_eq!(prepared.quarantine[0].error_class, "telegram_payload");
    }

    #[test]
    fn malformed_envelope_uses_a_database_valid_quarantine_source() {
        let malformed = raw_entry_with_payload(1_700_000_000_001, b"not-json");

        let prepared = preprocess_batch(&[malformed], 123);

        assert!(prepared.updates.is_empty());
        assert_eq!(prepared.quarantine.len(), 1);
        assert_eq!(prepared.quarantine[0].source, "legacy");
        assert_eq!(prepared.quarantine[0].error_class, "stream_envelope");
    }

    #[test]
    fn unsupported_update_type_is_materialized_for_explicit_ignore() {
        let payload = br#"{"update_id":42,"brand_new_update":{"value":1}}"#;
        let unknown = stream_entry(1_700_000_000_001, 0, 42, 1_700_000_000_010, payload);

        let prepared = preprocess_batch(&[unknown], 123);

        assert!(prepared.quarantine.is_empty());
        assert_eq!(prepared.updates.len(), 1);
        assert_eq!(prepared.updates[0].update_type.as_deref(), Some("unknown"));
        assert!(matches!(
            prepared.updates[0].disposition,
            openplotva_storage::MaterializedUpdateDisposition::Ignored { .. }
        ));
    }

    #[test]
    fn payment_update_is_never_terminalized_during_materialization() {
        let payload = br#"{"update_id":43,"pre_checkout_query":{"id":"checkout","from":{"id":9,"is_bot":false,"first_name":"Ada"},"currency":"XTR","total_amount":10,"invoice_payload":"vip"}}"#;
        let payment = stream_entry(1_700_000_000_001, 0, 43, 1_700_000_000_010, payload);

        let prepared = preprocess_batch(&[payment], 123);

        assert!(prepared.quarantine.is_empty());
        assert_eq!(prepared.updates.len(), 1);
        assert_eq!(
            prepared.updates[0].disposition,
            openplotva_storage::MaterializedUpdateDisposition::Pending
        );
    }

    #[test]
    fn channel_posts_remain_pending_until_canonical_history_is_persisted() {
        let channel_post = br#"{"update_id":44,"channel_post":{"message_id":7,"date":1700000000,"chat":{"id":-1007,"type":"channel","title":"News"},"sender_chat":{"id":-1007,"type":"channel","title":"News"},"text":"first"}}"#;
        let edited_channel_post = br#"{"update_id":45,"edited_channel_post":{"message_id":7,"date":1700000000,"edit_date":1700000010,"chat":{"id":-1007,"type":"channel","title":"News"},"sender_chat":{"id":-1007,"type":"channel","title":"News"},"text":"edited"}}"#;

        for (index, payload) in [channel_post.as_slice(), edited_channel_post.as_slice()]
            .into_iter()
            .enumerate()
        {
            let prepared = preprocess_batch(
                &[stream_entry(
                    1_700_000_000_001 + index as u64,
                    0,
                    44 + index as i64,
                    1_700_000_000_010 + index as i64,
                    payload,
                )],
                123,
            );

            assert!(prepared.quarantine.is_empty());
            assert_eq!(prepared.updates.len(), 1);
            assert_eq!(
                prepared.updates[0].disposition,
                openplotva_storage::MaterializedUpdateDisposition::Pending
            );
        }
    }

    fn stream_entry(
        stream_ms: u64,
        stream_seq: u64,
        update_id: i64,
        received_at: i64,
        payload: &[u8],
    ) -> RawUpdateStreamEntry {
        let mut fields = BTreeMap::new();
        fields.insert(
            "schema_version".to_owned(),
            UPDATE_STREAM_SCHEMA_VERSION.to_string().into_bytes(),
        );
        fields.insert("bot_id".to_owned(), b"123".to_vec());
        fields.insert("update_id".to_owned(), update_id.to_string().into_bytes());
        fields.insert("source".to_owned(), b"webhook".to_vec());
        fields.insert(
            "received_at".to_owned(),
            received_at.to_string().into_bytes(),
        );
        fields.insert("payload".to_owned(), payload.to_vec());
        fields.insert(
            "payload_sha256".to_owned(),
            Sha256::digest(payload).to_vec(),
        );
        RawUpdateStreamEntry {
            stream_id: UpdateStreamId {
                milliseconds: stream_ms,
                sequence: stream_seq,
            },
            fields,
        }
    }

    fn raw_entry_with_payload(stream_ms: u64, payload: &[u8]) -> RawUpdateStreamEntry {
        RawUpdateStreamEntry {
            stream_id: UpdateStreamId {
                milliseconds: stream_ms,
                sequence: 0,
            },
            fields: BTreeMap::from([("payload".to_owned(), payload.to_vec())]),
        }
    }

    fn raw_entry_with_payload_size(stream_ms: u64, payload_size: usize) -> RawUpdateStreamEntry {
        RawUpdateStreamEntry {
            stream_id: UpdateStreamId {
                milliseconds: stream_ms,
                sequence: 0,
            },
            fields: BTreeMap::from([("payload".to_owned(), vec![0; payload_size])]),
        }
    }
}
