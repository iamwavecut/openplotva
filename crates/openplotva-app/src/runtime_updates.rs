use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use openplotva_server::{
    RuntimeUpdatesInspector, RuntimeUpdatesInspectorFuture, RuntimeUpdatesRuntimeData,
    RuntimeUpdatesTaskData,
};
use openplotva_updates::{
    UPDATE_CLAIM_TIMEOUT, UPDATE_STALL_AGE, UpdateStage, UpdateStageOutcome, UpdateStageReport,
    UpdateStageTracker, extract_update_state, update_name,
};
use sqlx::{PgPool, Row};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const UPDATE_STALL_LOG_INTERVAL: Duration = UPDATE_CLAIM_TIMEOUT;
const POSTGRES_RUNTIME_STATS_TIMEOUT: Duration = Duration::from_secs(2);
const POSTGRES_RUNTIME_STATS_RECENT_ROWS: i64 = 5_000;
const POSTGRES_RUNTIME_STATS_TASK_ROWS: i64 = 500;

#[derive(Clone)]
pub(crate) struct RuntimeUpdatesInspectorHandle {
    queue: openplotva_updates::RedisUpdateQueue,
    tracker: RuntimeUpdateTracker,
    gate_counters: Arc<Mutex<Option<Arc<crate::ingestion_telemetry::IngestionGateCounters>>>>,
    stream_plane: Arc<Mutex<Option<RuntimeUpdateStreamPlane>>>,
}

#[derive(Clone)]
struct RuntimeUpdateStreamPlane {
    stream: openplotva_updates::RedisUpdateStream,
    postgres: PgPool,
    materializer: crate::update_materializer::UpdateMaterializerMetrics,
}

impl RuntimeUpdatesInspectorHandle {
    pub(crate) fn new(queue: openplotva_updates::RedisUpdateQueue) -> Self {
        Self {
            queue,
            tracker: RuntimeUpdateTracker::with_worker_limit(
                openplotva_updates::UpdateConsumerConfig::default().worker_limit,
            ),
            gate_counters: Arc::new(Mutex::new(None)),
            stream_plane: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn stage_tracker(&self) -> RuntimeUpdateTracker {
        self.tracker.clone()
    }

    pub(crate) fn set_gate_counters(
        &self,
        counters: Arc<crate::ingestion_telemetry::IngestionGateCounters>,
    ) {
        *self
            .gate_counters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(counters);
    }

    pub(crate) fn set_stream_plane(
        &self,
        stream: openplotva_updates::RedisUpdateStream,
        postgres: PgPool,
        materializer: crate::update_materializer::UpdateMaterializerMetrics,
    ) {
        *self
            .stream_plane
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(RuntimeUpdateStreamPlane {
            stream,
            postgres,
            materializer,
        });
    }

    fn gate_counters_snapshot(&self) -> Option<openplotva_server::RuntimeIngestionGatesData> {
        self.gate_counters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|counters| counters.snapshot())
    }
}

impl RuntimeUpdatesInspector for RuntimeUpdatesInspectorHandle {
    fn snapshot<'a>(&'a self) -> RuntimeUpdatesInspectorFuture<'a> {
        Box::pin(async move {
            let mut snapshot = self.tracker.snapshot(SystemTime::now());
            let stream_plane = self
                .stream_plane
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            if let Some(stream_plane) = stream_plane {
                match stream_plane.stream.stats().await {
                    Ok(stats) => {
                        snapshot.stream_len = usize_to_i64(stats.length);
                        snapshot.queue_len = stats.length.min(i32::MAX as usize) as i32;
                        if let Some(group) = &stats.group {
                            snapshot.stream_pending = usize_to_i64(group.pending);
                            snapshot.stream_group_lag = group.lag.map_or(-1, usize_to_i64);
                        } else {
                            snapshot.stream_group_lag = -1;
                        }
                        let now_unix_ms = OffsetDateTime::now_utc()
                            .unix_timestamp_nanos()
                            .div_euclid(1_000_000)
                            .max(0) as u128;
                        snapshot.oldest_unmaterialized_ms = stats
                            .oldest_entry_age_at(u64::try_from(now_unix_ms).unwrap_or(u64::MAX))
                            .map_or(0, duration_millis_i64);
                    }
                    Err(error) => {
                        snapshot.queue_len = -1;
                        snapshot.queue_error = Some(error.to_string());
                    }
                }
                match stream_plane.stream.durability_stats().await {
                    Ok(durability) => {
                        snapshot.ingress_used_memory_bytes =
                            u64_to_i64(durability.used_memory_bytes);
                        snapshot.ingress_maxmemory_bytes = u64_to_i64(durability.maxmemory_bytes);
                        snapshot.ingress_maxmemory_policy = durability.maxmemory_policy;
                        snapshot.ingress_aof_enabled = durability.aof_enabled;
                        snapshot.ingress_aof_current_size_bytes =
                            u64_to_i64(durability.aof_current_size_bytes);
                        snapshot.ingress_aof_rewrite_in_progress =
                            durability.aof_rewrite_in_progress;
                        snapshot.ingress_aof_last_write_status = durability.aof_last_write_status;
                        snapshot.ingress_aof_last_rewrite_status =
                            durability.aof_last_rewrite_status;
                    }
                    Err(error) => {
                        let error = format!("Redis ingress durability stats: {error}");
                        snapshot.queue_error = Some(match snapshot.queue_error.take() {
                            Some(existing) => format!("{existing}; {error}"),
                            None => error,
                        });
                    }
                }
                let materializer = stream_plane.materializer.snapshot();
                snapshot.materializer_supervisor_running = materializer.supervisor_running;
                snapshot.materializer_lease_held = materializer.lease_held;
                snapshot.materializer_supervisor_restarts =
                    u64_to_i64(materializer.supervisor_restarts);
                snapshot.materializer_batch_rows =
                    materializer.batch_rows.min(i32::MAX as usize) as i32;
                snapshot.materializer_batch_bytes = usize_to_i64(materializer.batch_bytes);
                snapshot.materializer_batch_fill_ratio = materializer.batch_fill_ratio;
                snapshot.bulk_transaction_latency_ms = materializer
                    .last_transaction_latency
                    .map_or(0, duration_millis_i64);
                snapshot.materialized_batches = u64_to_i64(materializer.materialized_batches);
                snapshot.inbox_insert_statements = u64_to_i64(materializer.inbox_insert_statements);
                snapshot.quarantine_insert_statements =
                    u64_to_i64(materializer.quarantine_insert_statements);
                snapshot.materialized_inserted = u64_to_i64(materializer.inserted);
                snapshot.materialized_duplicates = u64_to_i64(materializer.duplicates);
                snapshot.materialized_conflicted = u64_to_i64(materializer.conflicted);
                snapshot.materialized_quarantined = u64_to_i64(materializer.quarantined);
                snapshot.materializer_reclaims = u64_to_i64(materializer.reclaims);
                snapshot.ack_delete_mismatches = u64_to_i64(materializer.ack_delete_mismatches);
                snapshot.materializer_db_failures = u64_to_i64(materializer.db_failures);
                snapshot.materializer_redis_failures = u64_to_i64(materializer.redis_failures);
                match postgres_update_runtime_stats(&stream_plane.postgres).await {
                    Ok(postgres) => postgres.apply_to(&mut snapshot),
                    Err(error) => {
                        let error = format!("Postgres update inbox stats: {error}");
                        snapshot.queue_error = Some(match snapshot.queue_error.take() {
                            Some(existing) => format!("{existing}; {error}"),
                            None => error,
                        });
                    }
                }
            } else {
                match self.queue.len().await {
                    Ok(queue_len) => {
                        snapshot.queue_len = queue_len.clamp(0, i32::MAX as i64) as i32;
                    }
                    Err(error) => {
                        snapshot.queue_len = -1;
                        snapshot.queue_error = Some(error.to_string());
                    }
                }
            }
            snapshot.gates = self.gate_counters_snapshot();
            Ok(snapshot)
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct PostgresUpdateRuntimeStats {
    pending: i64,
    retry_wait: i64,
    dead_letter: i64,
    event_to_redis_avg_ms: i64,
    redis_to_postgres_avg_ms: i64,
    materialization_to_claim_avg_ms: i64,
    claim_to_taskman_avg_ms: i64,
}

impl PostgresUpdateRuntimeStats {
    fn apply_to(self, snapshot: &mut RuntimeUpdatesRuntimeData) {
        snapshot.postgres_pending = self.pending;
        snapshot.postgres_retry_wait = self.retry_wait;
        snapshot.postgres_dead_letter = self.dead_letter;
        snapshot.event_to_redis_avg_ms = self.event_to_redis_avg_ms;
        snapshot.redis_to_postgres_avg_ms = self.redis_to_postgres_avg_ms;
        snapshot.materialization_to_claim_avg_ms = self.materialization_to_claim_avg_ms;
        snapshot.claim_to_taskman_avg_ms = self.claim_to_taskman_avg_ms;
    }
}

async fn postgres_update_runtime_stats(
    pool: &PgPool,
) -> Result<PostgresUpdateRuntimeStats, sqlx::Error> {
    let timeout_ms = i64::try_from(POSTGRES_RUNTIME_STATS_TIMEOUT.as_millis()).unwrap_or(i64::MAX);
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT set_config('statement_timeout', $1, true)")
        .bind(format!("{timeout_ms}ms"))
        .execute(&mut *tx)
        .await?;
    let row = sqlx::query(
        r#"
        WITH recent AS MATERIALIZED (
            SELECT id, update_id, telegram_event_at, first_received_at,
                   materialized_at, processing_started_at
            FROM telegram_update_inbox
            ORDER BY id DESC
            LIMIT $1
        ), recent_claimed AS MATERIALIZED (
            SELECT update_id, processing_started_at
            FROM recent
            WHERE processing_started_at IS NOT NULL
            ORDER BY id DESC
            LIMIT $2
        )
        SELECT
            (SELECT count(*) FROM telegram_update_inbox
                WHERE status = 'pending')::bigint AS pending,
            (SELECT count(*) FROM telegram_update_inbox
                WHERE status = 'retry_wait')::bigint AS retry_wait,
            (SELECT count(*) FROM telegram_update_inbox
                WHERE status = 'dead_letter')::bigint AS dead_letter,
            COALESCE(avg(GREATEST(0, EXTRACT(EPOCH FROM
                (first_received_at - telegram_event_at)) * 1000))
                FILTER (WHERE telegram_event_at IS NOT NULL), 0)::bigint AS event_to_redis_avg_ms,
            COALESCE(avg(GREATEST(0, EXTRACT(EPOCH FROM
                (materialized_at - first_received_at)) * 1000)), 0)::bigint AS redis_to_postgres_avg_ms,
            COALESCE(avg(GREATEST(0, EXTRACT(EPOCH FROM
                (processing_started_at - materialized_at)) * 1000))
                FILTER (WHERE processing_started_at IS NOT NULL), 0)::bigint
                AS materialization_to_claim_avg_ms,
            COALESCE((
                SELECT avg(GREATEST(0, EXTRACT(EPOCH FROM
                    (task.created_at - inbox.processing_started_at)) * 1000))::bigint
                FROM recent_claimed AS inbox
                JOIN LATERAL (
                    SELECT created_at
                    FROM taskman_jobs
                    WHERE deleted_at IS NULL
                      AND cardinality(source_update_ids) > 0
                      AND source_update_ids @> ARRAY[inbox.update_id]
                    ORDER BY created_at
                    LIMIT 1
                ) AS task ON true
            ), 0)::bigint AS claim_to_taskman_avg_ms
        FROM recent
        "#,
    )
    .bind(POSTGRES_RUNTIME_STATS_RECENT_ROWS)
    .bind(POSTGRES_RUNTIME_STATS_TASK_ROWS)
    .fetch_one(&mut *tx)
    .await?;
    let stats = PostgresUpdateRuntimeStats {
        pending: row.try_get("pending")?,
        retry_wait: row.try_get("retry_wait")?,
        dead_letter: row.try_get("dead_letter")?,
        event_to_redis_avg_ms: row.try_get("event_to_redis_avg_ms")?,
        redis_to_postgres_avg_ms: row.try_get("redis_to_postgres_avg_ms")?,
        materialization_to_claim_avg_ms: row.try_get("materialization_to_claim_avg_ms")?,
        claim_to_taskman_avg_ms: row.try_get("claim_to_taskman_avg_ms")?,
    };
    tx.commit().await?;
    Ok(stats)
}

fn usize_to_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn duration_millis_i64(value: Duration) -> i64 {
    i64::try_from(value.as_millis()).unwrap_or(i64::MAX)
}

#[derive(Clone)]
pub(crate) struct RuntimeUpdateTracker {
    inner: Arc<RuntimeUpdateTrackerInner>,
    worker_limit: usize,
}

#[derive(Default)]
struct RuntimeUpdateTrackerInner {
    next_task_id: AtomicU64,
    tasks: Mutex<HashMap<u64, ActiveUpdateTask>>,
    started: Mutex<VecDeque<SystemTime>>,
    completed: Mutex<VecDeque<SystemTime>>,
    timeouts: Mutex<VecDeque<SystemTime>>,
    last_stall_at: Mutex<Option<SystemTime>>,
}

#[derive(Clone, Debug)]
struct ActiveUpdateTask {
    stage: &'static str,
    started_at: SystemTime,
    chat_id: Option<i64>,
    user_id: Option<i64>,
    update: String,
}

impl RuntimeUpdateTracker {
    fn with_worker_limit(worker_limit: usize) -> Self {
        Self {
            inner: Arc::new(RuntimeUpdateTrackerInner::default()),
            worker_limit: worker_limit.max(1),
        }
    }

    fn snapshot(&self, now: SystemTime) -> RuntimeUpdatesRuntimeData {
        let tasks = self
            .inner
            .tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut state_active = 0;
        let mut handle_active = 0;
        let mut oldest_active_ms = 0;
        let task_rows = tasks
            .into_iter()
            .map(|task| {
                match task.stage {
                    "state" => state_active += 1,
                    "handle" => handle_active += 1,
                    _ => {}
                }
                let age_ms = duration_since_ms(now, task.started_at);
                oldest_active_ms = oldest_active_ms.max(age_ms);
                RuntimeUpdatesTaskData {
                    stage: task.stage.to_owned(),
                    started_at: format_system_time(task.started_at),
                    age_ms,
                    chat_id: task.chat_id,
                    user_id: task.user_id,
                    update: task.update,
                }
            })
            .collect::<Vec<_>>();
        let active = task_rows.len().min(i32::MAX as usize) as i32;
        let started1m = recent_count(&self.inner.started, now);
        let completed1m = recent_count(&self.inner.completed, now);
        let timeouts1m = recent_count(&self.inner.timeouts, now);

        let last_stall_at = {
            let mut last_stall_at = self
                .inner
                .last_stall_at
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if should_mark_stall(active, self.worker_limit, started1m, oldest_active_ms)
                && mark_stall_logged(*last_stall_at, now)
            {
                *last_stall_at = Some(now);
            }
            last_stall_at.map(format_system_time)
        };

        RuntimeUpdatesRuntimeData {
            active,
            state_active,
            handle_active,
            started1m,
            completed1m,
            timeouts1m,
            oldest_active_ms,
            last_stall_at,
            tasks: task_rows,
            ..RuntimeUpdatesRuntimeData::default()
        }
    }
}

impl Default for RuntimeUpdateTracker {
    fn default() -> Self {
        Self::with_worker_limit(openplotva_updates::UpdateConsumerConfig::default().worker_limit)
    }
}

impl UpdateStageTracker for RuntimeUpdateTracker {
    fn stage_started(
        &self,
        update: &carapax::types::Update,
        stage: UpdateStage,
        started_at: SystemTime,
    ) -> u64 {
        push_recent(&self.inner.started, started_at);
        let task_id = self.inner.next_task_id.fetch_add(1, Ordering::Relaxed) + 1;
        let state = extract_update_state(update);
        let task = ActiveUpdateTask {
            stage: stage_name(stage),
            started_at,
            chat_id: state
                .as_ref()
                .and_then(|state| state.chat.as_ref().map(|chat| chat.id)),
            user_id: state
                .as_ref()
                .and_then(|state| state.user.as_ref().map(|user| user.id)),
            update: update_name(update).to_owned(),
        };
        self.inner
            .tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(task_id, task);
        task_id
    }

    fn stage_finished(&self, token: u64, report: &UpdateStageReport, finished_at: SystemTime) {
        self.inner
            .tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&token);
        push_recent(&self.inner.completed, finished_at);
        if matches!(report.outcome, UpdateStageOutcome::TimedOut) {
            push_recent(&self.inner.timeouts, finished_at);
        }
    }
}

fn stage_name(stage: UpdateStage) -> &'static str {
    match stage {
        UpdateStage::State => "state",
        UpdateStage::Handle => "handle",
    }
}

fn push_recent(events: &Mutex<VecDeque<SystemTime>>, at: SystemTime) {
    events
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push_back(at);
}

fn recent_count(events: &Mutex<VecDeque<SystemTime>>, now: SystemTime) -> i32 {
    let mut events = events
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    while events
        .front()
        .is_some_and(|event| is_older_than(*event, now, Duration::from_secs(60)))
    {
        events.pop_front();
    }
    events.len().min(i32::MAX as usize) as i32
}

fn is_older_than(event: SystemTime, now: SystemTime, max_age: Duration) -> bool {
    now.duration_since(event).is_ok_and(|age| age > max_age)
}

fn should_mark_stall(
    active: i32,
    worker_limit: usize,
    started1m: i32,
    oldest_active_ms: i32,
) -> bool {
    active as usize == worker_limit.max(1)
        && started1m == 0
        && Duration::from_millis(oldest_active_ms.max(0) as u64) > UPDATE_STALL_AGE
}

fn mark_stall_logged(previous: Option<SystemTime>, now: SystemTime) -> bool {
    !previous.is_some_and(|previous| {
        now.duration_since(previous)
            .is_ok_and(|elapsed| elapsed <= UPDATE_STALL_LOG_INTERVAL)
    })
}

fn duration_since_ms(now: SystemTime, started_at: SystemTime) -> i32 {
    now.duration_since(started_at)
        .unwrap_or_default()
        .as_millis()
        .min(i32::MAX as u128) as i32
}

fn format_system_time(value: SystemTime) -> String {
    OffsetDateTime::from(value)
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use openplotva_updates::{UpdateStage, UpdateStageOutcome, UpdateStageReport};
    use serde_json::json;

    use super::{RuntimeUpdateTracker, mark_stall_logged, should_mark_stall};

    #[test]
    fn runtime_update_tracker_reports_live_go_shaped_stage_stats() {
        let tracker = RuntimeUpdateTracker::default();
        let update = serde_json::from_value(json!({
            "update_id": 11,
            "message": {
                "message_id": 22,
                "date": 1710000000,
                "chat": { "id": 42, "type": "private", "first_name": "Ada" },
                "from": { "id": 7, "is_bot": false, "first_name": "Ada" },
                "text": "/ping"
            }
        }))
        .expect("message update JSON should decode");
        let start = UNIX_EPOCH + Duration::from_secs(1_710_000_000);

        let state_id = openplotva_updates::UpdateStageTracker::stage_started(
            &tracker,
            &update,
            UpdateStage::State,
            start,
        );
        let handle_id = openplotva_updates::UpdateStageTracker::stage_started(
            &tracker,
            &update,
            UpdateStage::Handle,
            start + Duration::from_millis(10),
        );
        let snapshot = tracker.snapshot(start + Duration::from_millis(250));

        assert_eq!(snapshot.active, 2);
        assert_eq!(snapshot.state_active, 1);
        assert_eq!(snapshot.handle_active, 1);
        assert_eq!(snapshot.started1m, 2);
        assert_eq!(snapshot.oldest_active_ms, 250);
        assert_eq!(snapshot.tasks[0].chat_id, Some(42));
        assert_eq!(snapshot.tasks[0].user_id, Some(7));
        assert_eq!(snapshot.tasks[0].update, "message");

        openplotva_updates::UpdateStageTracker::stage_finished(
            &tracker,
            state_id,
            &UpdateStageReport {
                stage: UpdateStage::State,
                outcome: UpdateStageOutcome::Completed,
                elapsed: Duration::from_millis(20),
            },
            start + Duration::from_millis(20),
        );
        openplotva_updates::UpdateStageTracker::stage_finished(
            &tracker,
            handle_id,
            &UpdateStageReport {
                stage: UpdateStage::Handle,
                outcome: UpdateStageOutcome::TimedOut,
                elapsed: Duration::from_secs(45),
            },
            start + Duration::from_secs(45),
        );
        let snapshot = tracker.snapshot(start + Duration::from_secs(46));
        assert_eq!(snapshot.active, 0);
        assert_eq!(snapshot.completed1m, 2);
        assert_eq!(snapshot.timeouts1m, 1);
    }

    #[test]
    fn runtime_update_tracker_marks_stalls_with_go_gate_and_throttle() {
        let tracker = RuntimeUpdateTracker::with_worker_limit(2);
        let update = serde_json::from_value(json!({
            "update_id": 11,
            "message": {
                "message_id": 22,
                "date": 1710000000,
                "chat": { "id": 42, "type": "private", "first_name": "Ada" },
                "from": { "id": 7, "is_bot": false, "first_name": "Ada" },
                "text": "/ping"
            }
        }))
        .expect("message update JSON should decode");
        let start = UNIX_EPOCH + Duration::from_secs(1_710_000_000);

        openplotva_updates::UpdateStageTracker::stage_started(
            &tracker,
            &update,
            UpdateStage::State,
            start,
        );
        openplotva_updates::UpdateStageTracker::stage_started(
            &tracker,
            &update,
            UpdateStage::Handle,
            start + Duration::from_millis(10),
        );

        let boundary = tracker.snapshot(start + Duration::from_secs(120));
        assert_eq!(boundary.last_stall_at, None);

        let stalled = tracker.snapshot(start + Duration::from_secs(121));
        assert_eq!(stalled.active, 2);
        assert_eq!(stalled.started1m, 0);
        assert_eq!(
            stalled.last_stall_at.as_deref(),
            Some("2024-03-09T16:02:01Z")
        );

        let throttled = tracker.snapshot(start + Duration::from_secs(150));
        assert_eq!(
            throttled.last_stall_at.as_deref(),
            Some("2024-03-09T16:02:01Z")
        );

        let repeated = tracker.snapshot(start + Duration::from_secs(182));
        assert_eq!(
            repeated.last_stall_at.as_deref(),
            Some("2024-03-09T16:03:02Z")
        );
    }

    #[test]
    fn runtime_update_stall_gate_requires_full_pool_no_progress_and_old_task() {
        assert!(should_mark_stall(8, 8, 0, 120_001));
        assert!(!should_mark_stall(7, 8, 0, 120_001));
        assert!(!should_mark_stall(8, 8, 1, 120_001));
        assert!(!should_mark_stall(8, 8, 0, 120_000));

        let now = UNIX_EPOCH + Duration::from_secs(1_000);
        assert!(mark_stall_logged(None, now));
        assert!(!mark_stall_logged(Some(now - Duration::from_secs(60)), now));
        assert!(mark_stall_logged(Some(now - Duration::from_secs(61)), now));
    }
}
