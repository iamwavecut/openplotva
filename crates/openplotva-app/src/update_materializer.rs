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

use carapax::types::{
    ChatMember as TelegramChatMember, MessageData as TelegramMessageData, Update as TelegramUpdate,
    UpdateType as TelegramUpdateType,
};
use openplotva_config::UpdateMaterializationMode;
use openplotva_storage::{
    ChatMemberUpsert, MATERIALIZED_UPDATE_BINDS_PER_ROW, MaterializationReport,
    MaterializedUpdateDisposition, MaterializedUpdateInput, PostgresTelegramDeliveryStore,
    PostgresTelegramProjectionStore, QuarantinedUpdateInput, StorageError,
    TelegramActivityProjection, TelegramChatMemberProjection, TelegramChatProjection,
    TelegramFileProjection, TelegramProjectionBatch, TelegramProjectionVersion,
    TelegramUserProjection,
};
use openplotva_updates::{
    RawUpdateStreamEntry, RedisUpdateStream, UpdateStreamConfigError, UpdateStreamEntry,
    UpdateStreamId, UpdateStreamMaterializerConfig, decode_telegram_update_json_slice,
    extract_update_state, is_passive_update, update_file_metadata_refs, update_name,
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
const PROJECTION_STAGE_DEGRADED_AGE: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UpdateProjectionMaterializerConfig {
    pub mode: UpdateMaterializationMode,
    pub flush_interval: Duration,
    pub flush_max_mutations: usize,
    pub stage_hard_limit_rows: usize,
}

impl Default for UpdateProjectionMaterializerConfig {
    fn default() -> Self {
        Self {
            mode: UpdateMaterializationMode::Legacy,
            flush_interval: Duration::from_secs(10),
            flush_max_mutations: 10_000,
            stage_hard_limit_rows: 1_000_000,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateMaterializationPlan {
    OnlineEvent,
    ImmediateProjection,
    DeferredProjection,
    Ignored,
    Quarantine,
}

struct ProjectionFlushState {
    last_flush: Instant,
    staged_mutations_since_flush: usize,
}

impl ProjectionFlushState {
    fn new(flush_interval: Duration) -> Self {
        Self {
            last_flush: Instant::now()
                .checked_sub(flush_interval)
                .unwrap_or_else(Instant::now),
            staged_mutations_since_flush: 0,
        }
    }

    fn flush_due(&self, config: UpdateProjectionMaterializerConfig) -> bool {
        self.last_flush.elapsed() >= config.flush_interval
            || self.staged_mutations_since_flush >= config.flush_max_mutations
    }
}

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
    pub projection_stage_rows: i64,
    pub projection_oldest_stage_age: Option<Duration>,
    pub projection_last_flush_latency: Option<Duration>,
    pub projection_staged_mutations: u64,
    pub projection_flushed_rows: u64,
    pub projection_flush_errors: u64,
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

    fn record_projection_stage(
        &self,
        staged_mutations: usize,
        rows: i64,
        oldest_observed_at: Option<OffsetDateTime>,
    ) {
        let oldest_stage_age = oldest_observed_at
            .and_then(|oldest| (OffsetDateTime::now_utc() - oldest).try_into().ok());
        self.update(|snapshot| {
            snapshot.projection_stage_rows = rows;
            snapshot.projection_oldest_stage_age = oldest_stage_age;
            snapshot.projection_staged_mutations = snapshot
                .projection_staged_mutations
                .saturating_add(staged_mutations as u64);
        });
    }

    fn record_projection_flush(&self, latency: Duration, flushed_rows: u64, remaining_rows: i64) {
        self.update(|snapshot| {
            snapshot.projection_last_flush_latency = Some(latency);
            snapshot.projection_flushed_rows = snapshot
                .projection_flushed_rows
                .saturating_add(flushed_rows);
            snapshot.projection_stage_rows = remaining_rows;
            if remaining_rows == 0 {
                snapshot.projection_oldest_stage_age = None;
            }
        });
    }

    fn record_projection_flush_error(&self) {
        self.update(|snapshot| {
            snapshot.projection_flush_errors = snapshot.projection_flush_errors.saturating_add(1);
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
    immediate_projections: TelegramProjectionBatch,
    deferred_projections: TelegramProjectionBatch,
}

#[derive(Default)]
struct ProjectionBatchReducer {
    users: HashMap<i64, TelegramUserProjection>,
    chats: HashMap<i64, TelegramChatProjection>,
    members: HashMap<(i64, i64), TelegramChatMemberProjection>,
    activity: HashMap<(i64, i64), TelegramActivityProjection>,
    files: HashMap<String, TelegramFileProjection>,
}

impl ProjectionBatchReducer {
    fn extend(&mut self, batch: TelegramProjectionBatch) {
        for row in batch.users {
            merge_user_projection(&mut self.users, row);
        }
        for row in batch.chats {
            merge_chat_projection(&mut self.chats, row);
        }
        for row in batch.members {
            merge_member_projection(&mut self.members, row);
        }
        for row in batch.activity {
            merge_activity_projection(&mut self.activity, row);
        }
        for row in batch.files {
            merge_file_projection(&mut self.files, row);
        }
    }

    fn finish(self) -> TelegramProjectionBatch {
        let mut users = self.users.into_values().collect::<Vec<_>>();
        users.sort_by_key(|row| row.state.id);
        let mut chats = self.chats.into_values().collect::<Vec<_>>();
        chats.sort_by_key(|row| row.state.id);
        let mut members = self.members.into_values().collect::<Vec<_>>();
        members.sort_by_key(|row| (row.state.chat_id, row.state.user_id));
        let mut activity = self.activity.into_values().collect::<Vec<_>>();
        activity.sort_by_key(|row| (row.chat_id, row.user_id));
        let mut files = self.files.into_values().collect::<Vec<_>>();
        files.sort_by(|left, right| left.state.file_unique_id.cmp(&right.state.file_unique_id));
        TelegramProjectionBatch {
            users,
            chats,
            members,
            activity,
            files,
        }
    }
}

fn projection_is_newer(
    candidate: TelegramProjectionVersion,
    current: TelegramProjectionVersion,
) -> bool {
    (candidate.stream_ms, candidate.stream_seq) > (current.stream_ms, current.stream_seq)
}

fn merge_user_projection(
    rows: &mut HashMap<i64, TelegramUserProjection>,
    mut candidate: TelegramUserProjection,
) {
    let key = candidate.state.id;
    match rows.remove(&key) {
        Some(current) if projection_is_newer(candidate.version, current.version) => {
            candidate.state.last_name = candidate.state.last_name.or(current.state.last_name);
            candidate.state.username = candidate.state.username.or(current.state.username);
            candidate.state.language_code = candidate
                .state
                .language_code
                .or(current.state.language_code);
            candidate.state.is_premium = candidate.state.is_premium.or(current.state.is_premium);
            rows.insert(key, candidate);
        }
        Some(current) => {
            rows.insert(key, current);
        }
        None => {
            rows.insert(key, candidate);
        }
    }
}

fn merge_chat_projection(
    rows: &mut HashMap<i64, TelegramChatProjection>,
    mut candidate: TelegramChatProjection,
) {
    let key = candidate.state.id;
    match rows.remove(&key) {
        Some(current) if projection_is_newer(candidate.version, current.version) => {
            candidate.state.title = candidate.state.title.or(current.state.title);
            candidate.state.username = candidate.state.username.or(current.state.username);
            candidate.state.first_name = candidate.state.first_name.or(current.state.first_name);
            candidate.state.last_name = candidate.state.last_name.or(current.state.last_name);
            candidate.state.is_forum = candidate.state.is_forum.or(current.state.is_forum);
            rows.insert(key, candidate);
        }
        Some(current) => {
            rows.insert(key, current);
        }
        None => {
            rows.insert(key, candidate);
        }
    }
}

fn merge_member_projection(
    rows: &mut HashMap<(i64, i64), TelegramChatMemberProjection>,
    mut candidate: TelegramChatMemberProjection,
) {
    let key = (candidate.state.chat_id, candidate.state.user_id);
    match rows.remove(&key) {
        Some(current) if projection_is_newer(candidate.version, current.version) => {
            merge_member_permissions(&mut candidate.state, current.state);
            rows.insert(key, candidate);
        }
        Some(current) => {
            rows.insert(key, current);
        }
        None => {
            rows.insert(key, candidate);
        }
    }
}

fn merge_member_permissions(candidate: &mut ChatMemberUpsert, current: ChatMemberUpsert) {
    candidate.is_member = candidate.is_member.or(current.is_member);
    candidate.is_anonymous = candidate.is_anonymous.or(current.is_anonymous);
    candidate.custom_title = candidate.custom_title.take().or(current.custom_title);
    candidate.can_be_edited = candidate.can_be_edited.or(current.can_be_edited);
    candidate.can_manage_chat = candidate.can_manage_chat.or(current.can_manage_chat);
    candidate.can_delete_messages = candidate
        .can_delete_messages
        .or(current.can_delete_messages);
    candidate.can_manage_video_chats = candidate
        .can_manage_video_chats
        .or(current.can_manage_video_chats);
    candidate.can_restrict_members = candidate
        .can_restrict_members
        .or(current.can_restrict_members);
    candidate.can_promote_members = candidate
        .can_promote_members
        .or(current.can_promote_members);
    candidate.can_change_info = candidate.can_change_info.or(current.can_change_info);
    candidate.can_invite_users = candidate.can_invite_users.or(current.can_invite_users);
    candidate.can_post_messages = candidate.can_post_messages.or(current.can_post_messages);
    candidate.can_edit_messages = candidate.can_edit_messages.or(current.can_edit_messages);
    candidate.can_pin_messages = candidate.can_pin_messages.or(current.can_pin_messages);
    candidate.can_manage_topics = candidate.can_manage_topics.or(current.can_manage_topics);
    candidate.can_send_messages = candidate.can_send_messages.or(current.can_send_messages);
    candidate.can_send_media_messages = candidate
        .can_send_media_messages
        .or(current.can_send_media_messages);
    candidate.can_send_polls = candidate.can_send_polls.or(current.can_send_polls);
    candidate.can_send_other_messages = candidate
        .can_send_other_messages
        .or(current.can_send_other_messages);
    candidate.can_add_web_page_previews = candidate
        .can_add_web_page_previews
        .or(current.can_add_web_page_previews);
    candidate.until_date = candidate.until_date.or(current.until_date);
}

fn merge_activity_projection(
    rows: &mut HashMap<(i64, i64), TelegramActivityProjection>,
    mut candidate: TelegramActivityProjection,
) {
    let key = (candidate.chat_id, candidate.user_id);
    match rows.remove(&key) {
        Some(current) => {
            candidate.last_message_at =
                max_optional_timestamp(candidate.last_message_at, current.last_message_at);
            candidate.last_active_at =
                max_optional_timestamp(candidate.last_active_at, current.last_active_at);
            if !projection_is_newer(candidate.version, current.version) {
                candidate.version = current.version;
            }
            rows.insert(key, candidate);
        }
        None => {
            rows.insert(key, candidate);
        }
    }
}

fn max_optional_timestamp(
    left: Option<OffsetDateTime>,
    right: Option<OffsetDateTime>,
) -> Option<OffsetDateTime> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    }
}

fn merge_file_projection(
    rows: &mut HashMap<String, TelegramFileProjection>,
    mut candidate: TelegramFileProjection,
) {
    let key = candidate.state.file_unique_id.clone();
    match rows.remove(&key) {
        Some(current) if projection_is_newer(candidate.version, current.version) => {
            candidate.state.mime_type = candidate.state.mime_type.or(current.state.mime_type);
            candidate.state.width = candidate.state.width.or(current.state.width);
            candidate.state.height = candidate.state.height.or(current.state.height);
            candidate.state.file_size = candidate.state.file_size.or(current.state.file_size);
            candidate.state.first_seen_chat_id = current
                .state
                .first_seen_chat_id
                .or(candidate.state.first_seen_chat_id);
            candidate.state.first_seen_message_id = current
                .state
                .first_seen_message_id
                .or(candidate.state.first_seen_message_id);
            rows.insert(key, candidate);
        }
        Some(current) => {
            rows.insert(key, current);
        }
        None => {
            rows.insert(key, candidate);
        }
    }
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
        UpdateProjectionMaterializerConfig::default(),
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
    projection_config: UpdateProjectionMaterializerConfig,
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
            projection_config,
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
    projection_config: UpdateProjectionMaterializerConfig,
    metrics: UpdateMaterializerMetrics,
    stop: Stop,
) -> Result<UpdateMaterializerWorkerReport, UpdateStreamConfigError>
where
    Stop: Future<Output = ()> + Send,
{
    let config = config.validate()?;
    let projection_config = UpdateProjectionMaterializerConfig {
        flush_interval: projection_config
            .flush_interval
            .max(Duration::from_millis(1)),
        flush_max_mutations: projection_config.flush_max_mutations.max(1),
        stage_hard_limit_rows: projection_config.stage_hard_limit_rows.max(1),
        ..projection_config
    };
    let projection_store = PostgresTelegramProjectionStore::new(store.pool().clone());
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

    let mut projection_flush_state = ProjectionFlushState::new(projection_config.flush_interval);
    let mut consecutive_flush_failures = 0_u32;
    loop {
        let flush = tokio::select! {
            () = stop.as_mut() => None,
            result = tokio::time::timeout(
                config.db_timeout,
                flush_staged_projections(
                    &projection_store,
                    bot_id,
                    &metrics,
                    &mut projection_flush_state,
                ),
            ) => Some(result),
        };
        match flush {
            None => break,
            Some(Ok(Ok(()))) => break,
            Some(Ok(Err(error))) => {
                report.db_failures = report.db_failures.saturating_add(1);
                metrics.record_db_failure();
                metrics.record_projection_flush_error();
                report.last_error = Some(error.to_string());
                consecutive_flush_failures = consecutive_flush_failures.saturating_add(1);
                tracing::warn!(
                    %error,
                    consecutive_flush_failures,
                    "failed to flush staged Telegram projections at materializer startup"
                );
            }
            Some(Err(error)) => {
                report.db_failures = report.db_failures.saturating_add(1);
                metrics.record_db_failure();
                metrics.record_projection_flush_error();
                report.last_error = Some(error.to_string());
                consecutive_flush_failures = consecutive_flush_failures.saturating_add(1);
                tracing::warn!(
                    %error,
                    consecutive_flush_failures,
                    "staged Telegram projection startup flush timed out"
                );
            }
        }
        if sleep_or_stop(&mut stop, retry_backoff(consecutive_flush_failures)).await {
            break;
        }
    }

    let mut pending = VecDeque::with_capacity(max_rows);
    let mut reclaim_cursor = UpdateStreamId::MIN;
    // The lease is exclusive, so every pre-existing PEL owner is abandoned at
    // this point. Adopt the whole PEL without an extra idle wait once; later
    // reclaim scans retain the configured crash-recovery threshold.
    let mut startup_reclaim = true;

    loop {
        if projection_config.mode.stages_projections()
            && projection_flush_state.flush_due(projection_config)
            && let Err(error) = tokio::time::timeout(
                config.db_timeout,
                flush_staged_projections(
                    &projection_store,
                    bot_id,
                    &metrics,
                    &mut projection_flush_state,
                ),
            )
            .await
            .map_err(|error| error.to_string())
            .and_then(|result| result.map_err(|error| error.to_string()))
        {
            report.db_failures = report.db_failures.saturating_add(1);
            metrics.record_db_failure();
            metrics.record_projection_flush_error();
            report.last_error = Some(error.clone());
            tracing::warn!(%error, "failed to flush staged Telegram projections");
        }

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
            &projection_store,
            bot_id,
            &batch,
            config.db_timeout,
            projection_config,
            &mut projection_flush_state,
            &mut batch_stop,
            &mut report,
            &metrics,
        )
        .await
        {
            break;
        }
    }

    if projection_config.mode.stages_projections() {
        match tokio::time::timeout(
            config.db_timeout,
            flush_staged_projections(
                &projection_store,
                bot_id,
                &metrics,
                &mut projection_flush_state,
            ),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                metrics.record_projection_flush_error();
                tracing::warn!(%error, "failed to flush staged Telegram projections at shutdown");
            }
            Err(error) => {
                metrics.record_projection_flush_error();
                tracing::warn!(%error, "staged Telegram projection shutdown flush timed out");
            }
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
    projection_store: &PostgresTelegramProjectionStore,
    bot_id: i64,
    entries: &[RawUpdateStreamEntry],
    db_timeout: Duration,
    projection_config: UpdateProjectionMaterializerConfig,
    projection_flush_state: &mut ProjectionFlushState,
    stop: &mut Pin<&mut Stop>,
    report: &mut UpdateMaterializerWorkerReport,
    metrics: &UpdateMaterializerMetrics,
) -> bool
where
    Stop: Future<Output = ()>,
{
    let batch = preprocess_batch_for_mode(entries, bot_id, projection_config.mode);
    let mut consecutive_db_failures = 0_u32;

    let (materialization, transaction_latency) = loop {
        let transaction_started = Instant::now();
        let result = tokio::select! {
            () = stop.as_mut() => return false,
            result = tokio::time::timeout(
                db_timeout,
                store.materialize_online_batch(
                    &batch.updates,
                    &batch.quarantine,
                    &batch.immediate_projections,
                ),
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

    if !batch.deferred_projections.is_empty() {
        let deferred_mutations = batch.deferred_projections.mutation_count();
        if !ensure_projection_stage_capacity(
            projection_store,
            bot_id,
            deferred_mutations,
            db_timeout,
            projection_config,
            projection_flush_state,
            stop,
            report,
            metrics,
        )
        .await
        {
            return false;
        }

        consecutive_db_failures = 0;
        loop {
            let staged = tokio::select! {
                () = stop.as_mut() => return false,
                result = tokio::time::timeout(
                    db_timeout,
                    projection_store.stage_projection_batch(&batch.deferred_projections),
                ) => result,
            };
            match staged {
                Ok(Ok(_)) => {
                    projection_flush_state.staged_mutations_since_flush = projection_flush_state
                        .staged_mutations_since_flush
                        .saturating_add(deferred_mutations);
                    match projection_store.stage_stats(bot_id).await {
                        Ok(stats) => {
                            metrics.record_projection_stage(
                                deferred_mutations,
                                stats.rows,
                                stats.oldest_observed_at,
                            );
                            if stats.oldest_observed_at.is_some_and(|oldest| {
                                OffsetDateTime::now_utc() - oldest
                                    > time::Duration::seconds(
                                        i64::try_from(PROJECTION_STAGE_DEGRADED_AGE.as_secs())
                                            .unwrap_or(30),
                                    )
                            }) {
                                tracing::warn!(
                                    stage_rows = stats.rows,
                                    "Telegram projection staging is older than the degraded threshold"
                                );
                            }
                        }
                        Err(error) => {
                            tracing::warn!(%error, "failed to read Telegram projection stage stats");
                        }
                    }
                    break;
                }
                Ok(Err(error)) => {
                    report.db_failures = report.db_failures.saturating_add(1);
                    metrics.record_db_failure();
                    report.last_error = Some(error.to_string());
                    consecutive_db_failures = consecutive_db_failures.saturating_add(1);
                    tracing::warn!(
                        %error,
                        consecutive_db_failures,
                        deferred_mutations,
                        "failed to stage deferred Telegram projections"
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
                        deferred_mutations,
                        "deferred Telegram projection staging timed out"
                    );
                }
            }
            if sleep_or_stop(stop, retry_backoff(consecutive_db_failures)).await {
                return false;
            }
        }
    }

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

#[allow(clippy::too_many_arguments)]
async fn ensure_projection_stage_capacity<Stop>(
    store: &PostgresTelegramProjectionStore,
    bot_id: i64,
    pending_mutations: usize,
    db_timeout: Duration,
    config: UpdateProjectionMaterializerConfig,
    flush_state: &mut ProjectionFlushState,
    stop: &mut Pin<&mut Stop>,
    report: &mut UpdateMaterializerWorkerReport,
    metrics: &UpdateMaterializerMetrics,
) -> bool
where
    Stop: Future<Output = ()>,
{
    let mut consecutive_failures = 0_u32;
    loop {
        let stats = tokio::select! {
            () = stop.as_mut() => return false,
            result = tokio::time::timeout(db_timeout, store.stage_stats(bot_id)) => result,
        };
        match stats {
            Ok(Ok(stats)) => {
                metrics.record_projection_stage(0, stats.rows, stats.oldest_observed_at);
                let current_rows = usize::try_from(stats.rows.max(0)).unwrap_or(usize::MAX);
                if current_rows.saturating_add(pending_mutations) <= config.stage_hard_limit_rows {
                    return true;
                }
                tracing::warn!(
                    stage_rows = stats.rows,
                    pending_mutations,
                    hard_limit_rows = config.stage_hard_limit_rows,
                    "Telegram projection stage reached the hard limit; holding Redis entries"
                );
                match tokio::time::timeout(
                    db_timeout,
                    flush_staged_projections(store, bot_id, metrics, flush_state),
                )
                .await
                {
                    Ok(Ok(())) => {
                        consecutive_failures = 0;
                        continue;
                    }
                    Ok(Err(error)) => {
                        report.last_error = Some(error.to_string());
                        tracing::warn!(%error, "failed to flush full Telegram projection stage");
                    }
                    Err(error) => {
                        report.last_error = Some(error.to_string());
                        tracing::warn!(%error, "full Telegram projection stage flush timed out");
                    }
                }
                metrics.record_projection_flush_error();
            }
            Ok(Err(error)) => {
                report.last_error = Some(error.to_string());
                tracing::warn!(%error, "failed to inspect Telegram projection stage capacity");
            }
            Err(error) => {
                report.last_error = Some(error.to_string());
                tracing::warn!(%error, "Telegram projection stage capacity check timed out");
            }
        }

        report.db_failures = report.db_failures.saturating_add(1);
        metrics.record_db_failure();
        consecutive_failures = consecutive_failures.saturating_add(1);
        if sleep_or_stop(stop, retry_backoff(consecutive_failures)).await {
            return false;
        }
    }
}

async fn flush_staged_projections(
    store: &PostgresTelegramProjectionStore,
    bot_id: i64,
    metrics: &UpdateMaterializerMetrics,
    state: &mut ProjectionFlushState,
) -> Result<(), StorageError> {
    let started = Instant::now();
    let report = store.flush_staged_projections(bot_id).await?;
    let stats = store.stage_stats(bot_id).await?;
    state.last_flush = Instant::now();
    state.staged_mutations_since_flush = 0;
    metrics.record_projection_flush(started.elapsed(), report.deleted_stage_rows, stats.rows);
    Ok(())
}

#[cfg(test)]
fn preprocess_batch(entries: &[RawUpdateStreamEntry], expected_bot_id: i64) -> PreprocessedBatch {
    preprocess_batch_for_mode(entries, expected_bot_id, UpdateMaterializationMode::Legacy)
}

fn preprocess_batch_for_mode(
    entries: &[RawUpdateStreamEntry],
    expected_bot_id: i64,
    mode: UpdateMaterializationMode,
) -> PreprocessedBatch {
    let mut ordered = entries.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|entry| entry.stream_id);

    let mut stream_ids = Vec::with_capacity(entries.len());
    let mut updates = Vec::with_capacity(entries.len());
    let mut quarantine = Vec::with_capacity(entries.len());
    let mut update_indexes = HashMap::with_capacity(entries.len());
    let mut immediate_projections = ProjectionBatchReducer::default();
    let mut deferred_projections = ProjectionBatchReducer::default();

    for raw in ordered {
        stream_ids.push(raw.stream_id);
        match decoded_materialized_input(raw, expected_bot_id) {
            Ok(decoded) => {
                let plan = update_materialization_plan(&decoded.update, expected_bot_id);
                if mode == UpdateMaterializationMode::Active
                    && plan == UpdateMaterializationPlan::Quarantine
                {
                    quarantine.push(quarantine_input(
                        raw,
                        expected_bot_id,
                        Some(&decoded.envelope),
                        "unsupported_type",
                        format!(
                            "unsupported Telegram update type {}",
                            update_name(&decoded.update)
                        ),
                    ));
                    continue;
                }

                if mode.stages_projections() {
                    let (immediate, deferred) = projection_batches_from_update(
                        &decoded.update,
                        projection_version(&decoded.input),
                        plan,
                    );
                    if mode.projection_is_authoritative() {
                        immediate_projections.extend(immediate);
                        deferred_projections.extend(deferred);
                    } else {
                        deferred_projections.extend(immediate);
                        deferred_projections.extend(deferred);
                    }
                }

                let include_inbox = match mode {
                    UpdateMaterializationMode::Legacy | UpdateMaterializationMode::Shadow => true,
                    UpdateMaterializationMode::Active => {
                        plan == UpdateMaterializationPlan::OnlineEvent
                    }
                };
                if !include_inbox {
                    continue;
                }

                let update = decoded.input;
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
        immediate_projections: immediate_projections.finish(),
        deferred_projections: deferred_projections.finish(),
    }
}

type InputConversionError = Box<(&'static str, String, Option<UpdateStreamEntry>)>;

struct DecodedMaterializedInput {
    input: MaterializedUpdateInput,
    update: TelegramUpdate,
    envelope: UpdateStreamEntry,
}

fn decoded_materialized_input(
    raw: &RawUpdateStreamEntry,
    expected_bot_id: i64,
) -> Result<DecodedMaterializedInput, InputConversionError> {
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

    let input = MaterializedUpdateInput {
        bot_id: entry.bot_id,
        update_id: update.id,
        schema_version: i16::try_from(entry.schema_version).unwrap_or(i16::MAX),
        source: entry.source.as_str().to_owned(),
        stream_ms,
        stream_seq,
        last_stream_ms: stream_ms,
        last_stream_seq: stream_seq,
        raw_payload: entry.raw_payload.clone(),
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
    };
    Ok(DecodedMaterializedInput {
        input,
        update,
        envelope: entry,
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

fn update_materialization_plan(update: &TelegramUpdate, bot_id: i64) -> UpdateMaterializationPlan {
    match &update.update_type {
        TelegramUpdateType::Unknown(_) => UpdateMaterializationPlan::Quarantine,
        TelegramUpdateType::Message(_)
        | TelegramUpdateType::EditedMessage(_)
        | TelegramUpdateType::GuestMessage(_)
        | TelegramUpdateType::ChannelPost(_)
        | TelegramUpdateType::EditedChannelPost(_)
        | TelegramUpdateType::BusinessMessage(_)
        | TelegramUpdateType::EditedBusinessMessage(_)
        | TelegramUpdateType::BotStatus(_) => UpdateMaterializationPlan::OnlineEvent,
        TelegramUpdateType::UserStatus(status) => {
            let member = &status.new_chat_member;
            let user = member.get_user();
            if user.is_bot
                || i64::from(user.id) == bot_id
                || sensitive_membership(&status.old_chat_member)
                || sensitive_membership(member)
            {
                UpdateMaterializationPlan::OnlineEvent
            } else {
                UpdateMaterializationPlan::DeferredProjection
            }
        }
        _ if is_passive_update(update) => {
            if extract_update_state(update).is_some() {
                UpdateMaterializationPlan::DeferredProjection
            } else {
                UpdateMaterializationPlan::Ignored
            }
        }
        _ => UpdateMaterializationPlan::OnlineEvent,
    }
}

fn sensitive_membership(member: &TelegramChatMember) -> bool {
    matches!(
        member,
        TelegramChatMember::Administrator(_)
            | TelegramChatMember::Creator(_)
            | TelegramChatMember::Restricted(_)
    )
}

fn projection_version(input: &MaterializedUpdateInput) -> TelegramProjectionVersion {
    TelegramProjectionVersion {
        bot_id: input.bot_id,
        observed_at: input.last_received_at,
        stream_ms: input.last_stream_ms,
        stream_seq: input.last_stream_seq,
    }
}

fn projection_batches_from_update(
    update: &TelegramUpdate,
    version: TelegramProjectionVersion,
    plan: UpdateMaterializationPlan,
) -> (TelegramProjectionBatch, TelegramProjectionBatch) {
    let mut state = TelegramProjectionBatch::default();
    if let Some(common) = extract_update_state(update) {
        if let Some(user) = common.user {
            state.users.push(TelegramUserProjection {
                version,
                state: user,
            });
        }
        if let Some(chat) = common.chat {
            state.chats.push(TelegramChatProjection {
                version,
                state: chat,
            });
        }
    }
    for file_ref in update_file_metadata_refs(update) {
        if let Some(file) = crate::updates::telegram_file_metadata_upsert_from_ref(&file_ref) {
            state.files.push(TelegramFileProjection {
                version,
                state: file,
            });
        }
    }
    append_membership_projections(update, version, &mut state);

    let mut activity = TelegramProjectionBatch::default();
    append_activity_projection(update, version, &mut activity);

    match plan {
        UpdateMaterializationPlan::OnlineEvent => (state, activity),
        UpdateMaterializationPlan::ImmediateProjection => {
            state.activity.append(&mut activity.activity);
            (state, TelegramProjectionBatch::default())
        }
        UpdateMaterializationPlan::DeferredProjection => {
            state.activity.append(&mut activity.activity);
            (TelegramProjectionBatch::default(), state)
        }
        UpdateMaterializationPlan::Ignored | UpdateMaterializationPlan::Quarantine => (
            TelegramProjectionBatch::default(),
            TelegramProjectionBatch::default(),
        ),
    }
}

fn append_membership_projections(
    update: &TelegramUpdate,
    version: TelegramProjectionVersion,
    batch: &mut TelegramProjectionBatch,
) {
    if let TelegramUpdateType::BotStatus(status) | TelegramUpdateType::UserStatus(status) =
        &update.update_type
    {
        let chat_id = i64::from(status.chat.get_id());
        let user = status.new_chat_member.get_user();
        let user_id = i64::from(user.id);
        if chat_id != 0 && user_id != 0 {
            batch.users.push(TelegramUserProjection {
                version,
                state: crate::members::user_state_from_telegram_user(user),
            });
            batch.members.push(TelegramChatMemberProjection {
                version,
                state: crate::members::chat_member_state_upsert_from_telegram(
                    chat_id,
                    user_id,
                    &status.new_chat_member,
                ),
            });
        }
        return;
    }

    let Some(message) = update.get_message() else {
        return;
    };
    let chat_id = i64::from(message.chat.get_id());
    if chat_id == 0 {
        return;
    }
    match &message.data {
        TelegramMessageData::NewChatMembers(members) => {
            for user in members {
                let user_id = i64::from(user.id);
                if user_id == 0 {
                    continue;
                }
                batch.users.push(TelegramUserProjection {
                    version,
                    state: crate::members::user_state_from_telegram_user(user),
                });
                batch.members.push(TelegramChatMemberProjection {
                    version,
                    state: ChatMemberUpsert {
                        chat_id,
                        user_id,
                        status: openplotva_storage::CHAT_MEMBER_STATUS_MEMBER.to_owned(),
                        is_member: Some(true),
                        is_anonymous: Some(false),
                        can_be_edited: Some(false),
                        ..ChatMemberUpsert::default()
                    },
                });
            }
        }
        TelegramMessageData::LeftChatMember(user) => {
            let user_id = i64::from(user.id);
            if user_id != 0 {
                batch.users.push(TelegramUserProjection {
                    version,
                    state: crate::members::user_state_from_telegram_user(user),
                });
                batch.members.push(TelegramChatMemberProjection {
                    version,
                    state: ChatMemberUpsert {
                        chat_id,
                        user_id,
                        status: openplotva_storage::CHAT_MEMBER_STATUS_LEFT.to_owned(),
                        is_member: Some(false),
                        is_anonymous: Some(false),
                        can_be_edited: Some(false),
                        ..ChatMemberUpsert::default()
                    },
                });
            }
        }
        _ => {}
    }
}

fn append_activity_projection(
    update: &TelegramUpdate,
    version: TelegramProjectionVersion,
    batch: &mut TelegramProjectionBatch,
) {
    let Some(message) = update.get_message() else {
        return;
    };
    let Some(user) = message.sender.get_user() else {
        return;
    };
    let chat_id = i64::from(message.chat.get_id());
    let user_id = i64::from(user.id);
    if chat_id == 0 || user_id == 0 || user.is_bot {
        return;
    }
    let active_at = telegram_event_at(update).unwrap_or(version.observed_at);
    batch.activity.push(TelegramActivityProjection {
        version,
        chat_id,
        user_id,
        last_message_at: Some(active_at),
        last_active_at: Some(active_at),
    });
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
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    use openplotva_config::UpdateMaterializationMode;
    use openplotva_storage::{
        MATERIALIZED_UPDATE_BINDS_PER_ROW, MaterializationReport, PostgresTelegramDeliveryStore,
        PostgresTelegramProjectionStore,
    };
    use openplotva_updates::{
        RawUpdateStreamEntry, RedisUpdateStream, UPDATE_STREAM_SCHEMA_VERSION, UpdateStreamAppend,
        UpdateStreamId, UpdateStreamMaterializerConfig, UpdateStreamSource,
    };
    use sha2::{Digest, Sha256};
    use sqlx::postgres::PgPoolOptions;
    use time::OffsetDateTime;

    use super::{
        MATERIALIZER_LEASE_RENEW_INTERVAL, MATERIALIZER_LEASE_TTL, UpdateMaterializerMetrics,
        UpdateMaterializerWorkerReport, UpdateProjectionMaterializerConfig, batch_prefix,
        materialize_and_ack_batch, materializer_owner_consumer_id, preprocess_batch,
        preprocess_batch_for_mode, reclaim_idle_for_pass, run_materializer_lease_heartbeat,
        stop_requested_now, take_batch, wait_for_materializer_lease_loss,
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

    #[tokio::test]
    async fn offline_entry_stays_in_stream_until_stage_commit_then_exposes_the_accepted_loss_window()
    -> Result<(), Box<dyn Error>> {
        let (Ok(redis_url), Ok(postgres_dsn)) = (
            env::var("OPENPLOTVA_TEST_REDIS_URL"),
            env::var("OPENPLOTVA_TEST_POSTGRES_DSN"),
        ) else {
            return Ok(());
        };
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let bot_id = i64::try_from(suffix % 1_000_000_000)? + 40_000;
        let actor_id = bot_id + 1_000_000_000;
        let user_id = bot_id + 2_000_000_000;
        let chat_id = -(bot_id + 3_000_000_000);
        let key = format!("openplotva:test:updates:projection-crash:{suffix}");
        let client = redis::Client::open(redis_url)?;
        let stream = RedisUpdateStream::with_key(client.clone(), key.clone());
        stream.ensure_consumer_group().await?;
        let received_at_unix_ms = i64::try_from(
            OffsetDateTime::now_utc()
                .unix_timestamp_nanos()
                .div_euclid(1_000_000),
        )?;
        let payload = format!(
            r#"{{"update_id":80,"chat_member":{{"chat":{{"id":{chat_id},"type":"supergroup","title":"Crash window"}},"from":{{"id":{actor_id},"is_bot":false,"first_name":"Admin"}},"date":1700000000,"old_chat_member":{{"status":"left","user":{{"id":{user_id},"is_bot":false,"first_name":"Tracked"}}}},"new_chat_member":{{"status":"member","user":{{"id":{user_id},"is_bot":false,"first_name":"Tracked"}}}}}}}}"#
        );
        stream
            .append(&UpdateStreamAppend {
                bot_id,
                update_id: Some(80),
                source: UpdateStreamSource::Webhook,
                received_at_unix_ms,
                raw_payload: payload.into_bytes(),
            })
            .await?;
        let entries = stream
            .read_group("projection-crash-test", Duration::ZERO, 1)
            .await?;
        assert_eq!(entries.len(), 1);

        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&postgres_dsn)
            .await?;
        openplotva_storage::run_migrations_on(&pool).await?;
        let delivery = PostgresTelegramDeliveryStore::new(pool.clone());
        let unavailable_pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(20))
            .connect_lazy("postgres://plotva:plotva@127.0.0.1:1/plotva")?;
        let unavailable_projection = PostgresTelegramProjectionStore::new(unavailable_pool);
        let projection_config = UpdateProjectionMaterializerConfig {
            mode: UpdateMaterializationMode::Active,
            ..UpdateProjectionMaterializerConfig::default()
        };
        let mut failed_flush_state =
            super::ProjectionFlushState::new(projection_config.flush_interval);
        let mut failed_report = UpdateMaterializerWorkerReport::default();
        let failed_metrics = UpdateMaterializerMetrics::default();
        let failed_stop = tokio::time::sleep(Duration::from_millis(150));
        tokio::pin!(failed_stop);

        assert!(
            !materialize_and_ack_batch(
                &stream,
                &delivery,
                &unavailable_projection,
                bot_id,
                &entries,
                Duration::from_millis(50),
                projection_config,
                &mut failed_flush_state,
                &mut failed_stop,
                &mut failed_report,
                &failed_metrics,
            )
            .await
        );
        let before_commit = stream.stats().await?;
        assert_eq!(before_commit.length, 1);
        assert_eq!(before_commit.group.expect("consumer group").pending, 1);

        let projection = PostgresTelegramProjectionStore::new(pool.clone());
        let mut committed_flush_state =
            super::ProjectionFlushState::new(projection_config.flush_interval);
        let mut committed_report = UpdateMaterializerWorkerReport::default();
        let committed_metrics = UpdateMaterializerMetrics::default();
        let committed_stop = std::future::pending::<()>();
        tokio::pin!(committed_stop);
        assert!(
            materialize_and_ack_batch(
                &stream,
                &delivery,
                &projection,
                bot_id,
                &entries,
                Duration::from_secs(2),
                projection_config,
                &mut committed_flush_state,
                &mut committed_stop,
                &mut committed_report,
                &committed_metrics,
            )
            .await
        );
        assert_eq!(stream.stats().await?.length, 0);
        assert!(projection.stage_stats(bot_id).await?.rows > 0);
        assert!(
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM telegram_chat_members_effective \
                 WHERE chat_id = $1 AND user_id = $2 AND status = 'member')",
            )
            .bind(chat_id)
            .bind(user_id)
            .fetch_one(&pool)
            .await?
        );

        let mut tx = pool.begin().await?;
        for statement in [
            "DELETE FROM telegram_users_stage WHERE bot_id = $1",
            "DELETE FROM telegram_chats_stage WHERE bot_id = $1",
            "DELETE FROM telegram_chat_members_stage WHERE bot_id = $1",
            "DELETE FROM telegram_activity_stage WHERE bot_id = $1",
            "DELETE FROM telegram_files_stage WHERE bot_id = $1",
        ] {
            sqlx::query(statement)
                .bind(bot_id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        assert!(
            !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM telegram_chat_members_effective \
                 WHERE chat_id = $1 AND user_id = $2)",
            )
            .bind(chat_id)
            .bind(user_id)
            .fetch_one(&pool)
            .await?
        );

        let mut connection = client.get_multiplexed_async_connection().await?;
        let _: usize = redis::cmd("DEL")
            .arg(&key)
            .arg(format!("{key}:materializer-lease"))
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

    #[test]
    fn active_mode_routes_ordinary_human_membership_only_to_deferred_projection() {
        let payload = br#"{"update_id":50,"chat_member":{"chat":{"id":-10042,"type":"supergroup","title":"Plotva Lab"},"from":{"id":42,"is_bot":false,"first_name":"Admin"},"date":1700000000,"old_chat_member":{"status":"left","user":{"id":77,"is_bot":false,"first_name":"Tracked"}},"new_chat_member":{"status":"member","user":{"id":77,"is_bot":false,"first_name":"Tracked"}}}}"#;
        let prepared = preprocess_batch_for_mode(
            &[stream_entry(
                1_700_000_000_001,
                0,
                50,
                1_700_000_000_010,
                payload,
            )],
            123,
            UpdateMaterializationMode::Active,
        );

        assert!(prepared.updates.is_empty());
        assert!(prepared.quarantine.is_empty());
        assert!(prepared.immediate_projections.is_empty());
        assert_eq!(prepared.deferred_projections.users.len(), 2);
        assert_eq!(prepared.deferred_projections.chats.len(), 1);
        assert_eq!(prepared.deferred_projections.members.len(), 1);
        assert_eq!(
            prepared.deferred_projections.members[0].state.status,
            "member"
        );
    }

    #[test]
    fn active_mode_keeps_admin_membership_online_with_immediate_dependencies() {
        let payload = br#"{"update_id":51,"chat_member":{"chat":{"id":-10042,"type":"supergroup","title":"Plotva Lab"},"from":{"id":42,"is_bot":false,"first_name":"Admin"},"date":1700000000,"old_chat_member":{"status":"member","user":{"id":77,"is_bot":false,"first_name":"Tracked"}},"new_chat_member":{"status":"administrator","user":{"id":77,"is_bot":false,"first_name":"Tracked"},"can_be_edited":false,"is_anonymous":false,"can_manage_chat":true,"can_delete_messages":true,"can_manage_video_chats":true,"can_restrict_members":true,"can_promote_members":true,"can_change_info":true,"can_invite_users":true}}}"#;
        let prepared = preprocess_batch_for_mode(
            &[stream_entry(
                1_700_000_000_001,
                0,
                51,
                1_700_000_000_010,
                payload,
            )],
            123,
            UpdateMaterializationMode::Active,
        );

        assert_eq!(prepared.updates.len(), 1);
        assert_eq!(prepared.immediate_projections.users.len(), 2);
        assert_eq!(prepared.immediate_projections.chats.len(), 1);
        assert_eq!(prepared.immediate_projections.members.len(), 1);
        assert_eq!(
            prepared.immediate_projections.members[0].state.status,
            "administrator"
        );
    }

    #[test]
    fn active_message_stays_online_while_activity_is_deferred() {
        let payload = br#"{"update_id":52,"message":{"message_id":1,"date":1700000000,"chat":{"id":7,"type":"private","first_name":"Ada"},"from":{"id":9,"is_bot":false,"first_name":"Ada"},"text":"/checkin"}}"#;
        let prepared = preprocess_batch_for_mode(
            &[stream_entry(
                1_700_000_000_001,
                0,
                52,
                1_700_000_000_010,
                payload,
            )],
            123,
            UpdateMaterializationMode::Active,
        );

        assert_eq!(prepared.updates.len(), 1);
        assert_eq!(prepared.immediate_projections.users.len(), 1);
        assert_eq!(prepared.immediate_projections.chats.len(), 1);
        assert_eq!(prepared.deferred_projections.activity.len(), 1);
    }

    #[test]
    fn active_new_members_service_message_materializes_members_without_control_projection_job() {
        let payload = br#"{"update_id":56,"message":{"message_id":3,"date":1700000000,"chat":{"id":-10042,"type":"supergroup","title":"Plotva Lab"},"from":{"id":42,"is_bot":false,"first_name":"Admin"},"new_chat_members":[{"id":77,"is_bot":false,"first_name":"Tracked"}]}}"#;
        let prepared = preprocess_batch_for_mode(
            &[stream_entry(
                1_700_000_000_001,
                0,
                56,
                1_700_000_000_010,
                payload,
            )],
            123,
            UpdateMaterializationMode::Active,
        );

        assert_eq!(prepared.updates.len(), 1);
        assert_eq!(prepared.immediate_projections.users.len(), 2);
        assert_eq!(prepared.immediate_projections.chats.len(), 1);
        assert_eq!(prepared.immediate_projections.members.len(), 1);
        assert_eq!(
            prepared.immediate_projections.members[0].state.status,
            openplotva_storage::CHAT_MEMBER_STATUS_MEMBER
        );
    }

    #[test]
    fn active_unknown_payload_is_durably_quarantined_without_inbox_row() {
        let payload = br#"{"update_id":53,"brand_new_update":{"value":1}}"#;
        let prepared = preprocess_batch_for_mode(
            &[stream_entry(
                1_700_000_000_001,
                0,
                53,
                1_700_000_000_010,
                payload,
            )],
            123,
            UpdateMaterializationMode::Active,
        );

        assert!(prepared.updates.is_empty());
        assert_eq!(prepared.quarantine.len(), 1);
        assert_eq!(prepared.quarantine[0].error_class, "unsupported_type");
    }

    #[test]
    fn projection_reducer_keeps_one_latest_partial_state_per_key() {
        let first = br#"{"update_id":54,"message":{"message_id":1,"date":1700000000,"chat":{"id":7,"type":"private","first_name":"Ada"},"from":{"id":9,"is_bot":false,"first_name":"Ada","username":"ada"},"text":"one"}}"#;
        let latest = br#"{"update_id":55,"message":{"message_id":2,"date":1700000001,"chat":{"id":7,"type":"private","first_name":"Ada"},"from":{"id":9,"is_bot":false,"first_name":"Ada Lovelace"},"text":"two"}}"#;
        let prepared = preprocess_batch_for_mode(
            &[
                stream_entry(1_700_000_000_001, 0, 54, 1_700_000_000_010, first),
                stream_entry(1_700_000_000_002, 0, 55, 1_700_000_000_020, latest),
            ],
            123,
            UpdateMaterializationMode::Active,
        );

        assert_eq!(prepared.immediate_projections.users.len(), 1);
        let user = &prepared.immediate_projections.users[0];
        assert_eq!(user.state.first_name, "Ada Lovelace");
        assert_eq!(user.state.username.as_deref(), Some("ada"));
        assert_eq!(user.version.stream_ms, 1_700_000_000_002);
        assert_eq!(prepared.deferred_projections.activity.len(), 1);
    }

    #[tokio::test]
    #[ignore = "20k live PostgreSQL load scenario"]
    async fn twenty_thousand_membership_updates_do_not_delay_midstream_checkin()
    -> Result<(), Box<dyn Error>> {
        let Ok(postgres_dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };
        const MEMBERS: usize = 20_000;
        const BATCH_ROWS: usize = 512;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let bot_id = i64::try_from(suffix % 1_000_000_000)? + 50_000;
        let actor_id = bot_id + 1_000_000_000;
        let first_member_id = bot_id + 2_000_000_000;
        let membership_chat_id = -(bot_id + 3_000_000_000);
        let checkin_chat_id = actor_id;
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&postgres_dsn)
            .await?;
        openplotva_storage::run_migrations_on(&pool).await?;
        let projection_store = PostgresTelegramProjectionStore::new(pool.clone());
        let delivery_store = PostgresTelegramDeliveryStore::new(pool.clone());
        let started = Instant::now();
        let mut checkin_elapsed = None;

        for chunk_start in (0..MEMBERS).step_by(BATCH_ROWS) {
            let chunk_end = (chunk_start + BATCH_ROWS).min(MEMBERS);
            let mut entries = Vec::with_capacity(chunk_end - chunk_start);
            for index in chunk_start..chunk_end {
                let user_id = first_member_id + i64::try_from(index)?;
                let update_id = 1_000 + i64::try_from(index)?;
                let payload = format!(
                    r#"{{"update_id":{update_id},"chat_member":{{"chat":{{"id":{membership_chat_id},"type":"supergroup","title":"Load"}},"from":{{"id":{actor_id},"is_bot":false,"first_name":"Admin"}},"date":1700000000,"old_chat_member":{{"status":"left","user":{{"id":{user_id},"is_bot":false,"first_name":"Member"}}}},"new_chat_member":{{"status":"member","user":{{"id":{user_id},"is_bot":false,"first_name":"Member"}}}}}}}}"#
                );
                entries.push(stream_entry_for_bot(
                    bot_id,
                    1_800_000_000_000 + u64::try_from(index)?,
                    0,
                    update_id,
                    1_800_000_000_000 + i64::try_from(index)?,
                    payload.as_bytes(),
                ));
            }
            let prepared =
                preprocess_batch_for_mode(&entries, bot_id, UpdateMaterializationMode::Active);
            assert!(prepared.updates.is_empty());
            projection_store
                .stage_projection_batch(&prepared.deferred_projections)
                .await?;

            if checkin_elapsed.is_none() && chunk_end >= MEMBERS / 2 {
                let payload = format!(
                    r#"{{"update_id":900000,"message":{{"message_id":1,"date":1700000001,"chat":{{"id":{checkin_chat_id},"type":"private","first_name":"Admin"}},"from":{{"id":{actor_id},"is_bot":false,"first_name":"Admin"}},"text":"/checkin"}}}}"#
                );
                let checkin = preprocess_batch_for_mode(
                    &[stream_entry_for_bot(
                        bot_id,
                        1_900_000_000_000,
                        0,
                        900_000,
                        1_900_000_000_000,
                        payload.as_bytes(),
                    )],
                    bot_id,
                    UpdateMaterializationMode::Active,
                );
                let report = delivery_store
                    .materialize_online_batch(
                        &checkin.updates,
                        &checkin.quarantine,
                        &checkin.immediate_projections,
                    )
                    .await?;
                projection_store
                    .stage_projection_batch(&checkin.deferred_projections)
                    .await?;
                assert_eq!(report.inserted, 1);
                checkin_elapsed = Some(started.elapsed());
            }
        }

        let checkin_elapsed = checkin_elapsed.expect("midstream checkin materialized");
        println!("midstream /checkin wait: {checkin_elapsed:?}");
        assert!(
            checkin_elapsed < Duration::from_secs(2),
            "midstream /checkin waited {checkin_elapsed:?}"
        );
        let flushed = projection_store.flush_staged_projections(bot_id).await?;
        assert_eq!(flushed.members, MEMBERS as u64);
        let durable_members: i64 =
            sqlx::query_scalar("SELECT count(*) FROM chat_members WHERE chat_id = $1")
                .bind(membership_chat_id)
                .fetch_one(&pool)
                .await?;
        assert_eq!(durable_members, MEMBERS as i64);
        let inbox_rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM telegram_update_inbox WHERE bot_id = $1")
                .bind(bot_id)
                .fetch_one(&pool)
                .await?;
        assert_eq!(inbox_rows, 1);

        let repeated_payload = format!(
            r#"{{"update_id":990000,"chat_member":{{"chat":{{"id":{membership_chat_id},"type":"supergroup","title":"Load"}},"from":{{"id":{actor_id},"is_bot":false,"first_name":"Admin"}},"date":1700000002,"old_chat_member":{{"status":"member","user":{{"id":{first_member_id},"is_bot":false,"first_name":"Member"}}}},"new_chat_member":{{"status":"member","user":{{"id":{first_member_id},"is_bot":false,"first_name":"Member"}}}}}}}}"#
        );
        let repeated = preprocess_batch_for_mode(
            &[stream_entry_for_bot(
                bot_id,
                2_000_000_000_000,
                0,
                990_000,
                2_000_000_000_000,
                repeated_payload.as_bytes(),
            )],
            bot_id,
            UpdateMaterializationMode::Active,
        );
        projection_store
            .stage_projection_batch(&repeated.deferred_projections)
            .await?;
        let repeated_flush = projection_store.flush_staged_projections(bot_id).await?;
        assert_eq!(repeated_flush.users, 0);
        assert_eq!(repeated_flush.chats, 0);
        assert_eq!(repeated_flush.members, 0);

        let mut tx = pool.begin().await?;
        sqlx::query("DELETE FROM telegram_update_lanes WHERE bot_id = $1")
            .bind(bot_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM telegram_update_inbox WHERE bot_id = $1")
            .bind(bot_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM chat_active_users WHERE chat_id = $1")
            .bind(checkin_chat_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM chat_members WHERE chat_id = $1")
            .bind(membership_chat_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM chats WHERE id = ANY($1::bigint[])")
            .bind(vec![membership_chat_id, checkin_chat_id])
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM users WHERE id = $1 OR id BETWEEN $2 AND $3")
            .bind(actor_id)
            .bind(first_member_id)
            .bind(first_member_id + i64::try_from(MEMBERS)? - 1)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    fn stream_entry(
        stream_ms: u64,
        stream_seq: u64,
        update_id: i64,
        received_at: i64,
        payload: &[u8],
    ) -> RawUpdateStreamEntry {
        stream_entry_for_bot(123, stream_ms, stream_seq, update_id, received_at, payload)
    }

    fn stream_entry_for_bot(
        bot_id: i64,
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
        fields.insert("bot_id".to_owned(), bot_id.to_string().into_bytes());
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
