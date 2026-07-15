use std::{future::Future, pin::Pin, sync::Arc};

use async_graphql::{EmptySubscription, Enum, ID, InputObject, Json, Object, Schema, SimpleObject};
use serde_json::{Value, json};

/// Runtime API GraphQL schema type.
pub type RuntimeApiSchema = Schema<RuntimeQuery, RuntimeMutation, EmptySubscription>;

const RUNTIME_TASKMAN_LOWEST_PRIORITY: i32 = -4;
const RUNTIME_VIRTUAL_DIALOG_SESSION_ID_MAX_CHARS: usize = 128;
const RUNTIME_TELEGRAM_DELIVERY_LIST_DEFAULT: i32 = 100;
const RUNTIME_TELEGRAM_DELIVERY_LIST_MAX: i32 = 500;
const RUNTIME_TELEGRAM_OPERATION_ID_MAX_CHARS: usize = 512;
const RUNTIME_TELEGRAM_DIAGNOSTIC_MAX_CHARS: usize = 2_048;
const RUNTIME_TELEGRAM_LABEL_MAX_CHARS: usize = 512;

/// Boxed future returned by runtime Redis diagnostic inspectors.
pub type RuntimeRedisInspectorFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// Boxed future returned by runtime SQL diagnostic readers.
pub type RuntimeSqlReaderFuture<'a> =
    Pin<Box<dyn Future<Output = Result<RuntimeSqlReadResult, String>> + Send + 'a>>;

/// Boxed future returned by runtime entity readers.
pub type RuntimeEntityReaderFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// Boxed future returned by runtime safety-check readers.
pub type RuntimeSafetyCheckReaderFuture<'a> =
    Pin<Box<dyn Future<Output = Result<RuntimeSafetyCheckConnectionData, String>> + Send + 'a>>;

/// Boxed future returned by runtime update diagnostic inspectors.
pub type RuntimeUpdatesInspectorFuture<'a> =
    Pin<Box<dyn Future<Output = Result<RuntimeUpdatesRuntimeData, String>> + Send + 'a>>;

/// Boxed future returned by durable Telegram ingress/outbox inspectors.
pub type RuntimeTelegramDeliveryFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// Boxed future returned by runtime memory restart mutations.
pub type RuntimeMemoryRestartFuture<'a> =
    Pin<Box<dyn Future<Output = Result<RuntimeMemoryRestartResultData, String>> + Send + 'a>>;

/// Boxed future returned by runtime Gemini cache purge mutations.
pub type RuntimeGeminiCachePurgerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<RuntimeGeminiCachePurgeResultData, String>> + Send + 'a>>;

/// Boxed future returned by runtime LLM analytics readers.
pub type RuntimeLlmAnalyticsReaderFuture<'a> =
    Pin<Box<dyn Future<Output = Result<RuntimeLlmAnalyticsData, String>> + Send + 'a>>;

/// Boxed future returned by runtime virtual-dialog managers.
pub type RuntimeVirtualDialogFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// Boxed future returned by the taskman job-detail lookup, which may fall back
/// to persisted storage when the in-memory queue misses.
pub type RuntimeTaskmanJobFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<RuntimeTaskmanJobDetailsData>, String>> + Send + 'a>>;

/// Runtime API Redis diagnostics boundary.
pub trait RuntimeRedisInspector: Send + Sync {
    fn prefix_groups<'a>(
        &'a self,
        prefix: &'a str,
        limit: usize,
    ) -> RuntimeRedisInspectorFuture<'a, Vec<RuntimeRedisPrefixGroup>>;

    fn keys<'a>(
        &'a self,
        pattern: &'a str,
        limit: usize,
    ) -> RuntimeRedisInspectorFuture<'a, Vec<String>>;

    fn value<'a>(
        &'a self,
        key: &'a str,
        max_bytes: usize,
    ) -> RuntimeRedisInspectorFuture<'a, Option<RuntimeRedisValue>>;
}

/// Runtime API read-only SQL boundary.
pub trait RuntimeSqlReader: Send + Sync {
    /// Execute a guarded read-only SQL diagnostic query.
    fn read<'a>(&'a self, request: RuntimeSqlReadRequest) -> RuntimeSqlReaderFuture<'a>;
}

/// Runtime API core entity read boundary.
pub trait RuntimeEntityReader: Send + Sync {
    fn users<'a>(
        &'a self,
        filter: RuntimeUsersFilter,
    ) -> RuntimeEntityReaderFuture<'a, RuntimeUserConnectionData>;

    /// Load one user by ID or username.
    fn user<'a>(
        &'a self,
        lookup: RuntimeUserLookup,
    ) -> RuntimeEntityReaderFuture<'a, Option<RuntimeUserDetailsData>>;

    fn chats<'a>(
        &'a self,
        filter: RuntimeChatsFilter,
    ) -> RuntimeEntityReaderFuture<'a, RuntimeChatConnectionData>;

    /// Load one chat by ID.
    fn chat<'a>(&'a self, id: i64) -> RuntimeEntityReaderFuture<'a, Option<RuntimeChatData>>;

    /// List one chat's members and optional user rows.
    fn chat_members<'a>(
        &'a self,
        chat_id: i64,
    ) -> RuntimeEntityReaderFuture<'a, Vec<RuntimeChatMemberWithUserData>>;
}

/// Runtime API safety-check read boundary.
pub trait RuntimeSafetyCheckReader: Send + Sync {
    fn safety_checks<'a>(
        &'a self,
        filter: RuntimeSafetyChecksFilter,
    ) -> RuntimeSafetyCheckReaderFuture<'a>;
}

/// Runtime API LLM trace read boundary.
pub trait RuntimeLlmTraceInspector: Send + Sync {
    fn llm_requests(
        &self,
        filter: RuntimeLlmRequestsFilter,
    ) -> Result<Vec<RuntimeLlmRequestData>, String>;
}

/// Runtime API agent-run read boundary (admin LLM Dialogs skeletons).
#[derive(Clone, Debug, Default)]
pub struct RuntimeLlmRunsFilter {
    pub kind: String,
    pub chat_id: Option<i64>,
    pub errors_only: bool,
    pub q: String,
    pub limit: i32,
}

/// Serves in-memory agent-run skeletons (same JSON shape as the admin REST
/// list) for operator diagnostics.
pub trait RuntimeLlmRunInspector: Send + Sync {
    fn llm_runs(&self, filter: RuntimeLlmRunsFilter) -> Result<Vec<Value>, String>;
}

/// Runtime API dialog turn outcome read boundary.
pub trait RuntimeTurnOutcomeInspector: Send + Sync {
    fn turn_outcomes(
        &self,
        filter: RuntimeTurnOutcomesFilter,
    ) -> Result<Vec<RuntimeTurnOutcomeData>, String>;
}

/// Runtime API LLM routing event read boundary.
pub trait RuntimeRoutingEventInspector: Send + Sync {
    fn routing_events(
        &self,
        filter: RuntimeRoutingEventsFilter,
    ) -> Result<Vec<RuntimeRoutingEventData>, String>;
}

/// Runtime API LLM analytics read boundary.
pub trait RuntimeLlmAnalyticsReader: Send + Sync {
    fn llm_analytics<'a>(&'a self, range: &'a str) -> RuntimeLlmAnalyticsReaderFuture<'a>;
}

/// Runtime API log replay boundary.
pub trait RuntimeLogInspector: Send + Sync {
    fn logs(&self, query: RuntimeLogQuery) -> Vec<RuntimeLogEntry>;
}

/// Runtime API dispatcher diagnostics boundary.
pub trait RuntimeDispatcherInspector: Send + Sync {
    fn stats(&self) -> RuntimeDispatcherStatsData;
}

/// Runtime API dispatcher terminal-send-failure read boundary.
pub trait RuntimeDispatcherFailureInspector: Send + Sync {
    /// Most recent terminal outbound send failures, newest first.
    fn send_failures(&self, limit: i32) -> Vec<RuntimeDispatchFailureData>;
}

/// Runtime API cache diagnostics boundary.
pub trait RuntimeCacheInspector: Send + Sync {
    fn stats(&self) -> RuntimeCacheSnapshotData;
}

/// Runtime API updates diagnostics boundary.
pub trait RuntimeUpdatesInspector: Send + Sync {
    /// Return a live decoded-update runtime snapshot.
    fn snapshot<'a>(&'a self) -> RuntimeUpdatesInspectorFuture<'a>;
}

/// Runtime API boundary for durable Telegram inbox/outbox diagnostics and
/// explicit operator recovery actions.
pub trait RuntimeTelegramDeliveryInspector: Send + Sync {
    fn update_inbox_stats<'a>(
        &'a self,
    ) -> RuntimeTelegramDeliveryFuture<'a, RuntimeTelegramUpdateInboxStatsData>;

    fn update_inbox_item<'a>(
        &'a self,
        id: i64,
    ) -> RuntimeTelegramDeliveryFuture<'a, Option<RuntimeTelegramUpdateInboxItemData>>;

    fn update_inbox_items<'a>(
        &'a self,
        filter: RuntimeTelegramDeliveryListFilter,
    ) -> RuntimeTelegramDeliveryFuture<'a, Vec<RuntimeTelegramUpdateInboxItemData>>;

    fn update_inbox_attempts<'a>(
        &'a self,
        inbox_id: i64,
        limit: i32,
    ) -> RuntimeTelegramDeliveryFuture<'a, Vec<RuntimeTelegramUpdateAttemptData>>;

    fn outbox_stats<'a>(
        &'a self,
    ) -> RuntimeTelegramDeliveryFuture<'a, RuntimeTelegramOutboxStatsData>;

    fn outbox_item<'a>(
        &'a self,
        operation_id: &'a str,
    ) -> RuntimeTelegramDeliveryFuture<'a, Option<RuntimeTelegramOutboxItemData>>;

    fn outbox_items<'a>(
        &'a self,
        filter: RuntimeTelegramDeliveryListFilter,
    ) -> RuntimeTelegramDeliveryFuture<'a, Vec<RuntimeTelegramOutboxItemData>>;

    fn outbox_attempts<'a>(
        &'a self,
        outbox_id: i64,
        limit: i32,
    ) -> RuntimeTelegramDeliveryFuture<'a, Vec<RuntimeTelegramOutboxAttemptData>>;

    fn retry_outbox<'a>(
        &'a self,
        request: RuntimeTelegramOutboxRetryRequest,
    ) -> RuntimeTelegramDeliveryFuture<'a, RuntimeTelegramOutboxMutationResultData>;

    fn cancel_outbox<'a>(
        &'a self,
        operation_id: String,
    ) -> RuntimeTelegramDeliveryFuture<'a, RuntimeTelegramOutboxMutationResultData>;
}

/// Runtime API memory restart mutation boundary.
pub trait RuntimeMemoryRestarter: Send + Sync {
    /// Retry failed memory runs and trigger the memory worker.
    fn restart<'a>(&'a self, run_id: Option<i64>) -> RuntimeMemoryRestartFuture<'a>;
}

/// Runtime API Gemini explicit-cache purge mutation boundary.
pub trait RuntimeGeminiCachePurger: Send + Sync {
    /// Purge remotely tracked Gemini explicit caches.
    fn purge<'a>(&'a self) -> RuntimeGeminiCachePurgerFuture<'a>;
}

/// Runtime API taskman diagnostics boundary.
pub trait RuntimeTaskmanInspector: Send + Sync {
    fn list_jobs(
        &self,
        filter: RuntimeTaskmanJobsFilter,
    ) -> Result<RuntimeTaskmanJobListResultData, String>;

    /// Inspect one taskman job by ID, falling back to persisted storage when
    /// the in-memory queue no longer holds the row.
    fn job<'a>(&'a self, id: i64) -> RuntimeTaskmanJobFuture<'a>;

    /// Return a live taskman queue diagnostic snapshot.
    fn queue_diagnostics(
        &self,
        queues: Vec<String>,
        priority: i32,
    ) -> Result<RuntimeTaskmanDiagnosticsData, String>;
}

/// Runtime API virtual-dialog mutation/read boundary.
pub trait RuntimeVirtualDialogManager: Send + Sync {
    /// Read one virtual dialog by caller-provided session ID.
    fn virtual_dialog<'a>(
        &'a self,
        session_id: &'a str,
    ) -> RuntimeVirtualDialogFuture<'a, Option<RuntimeVirtualDialogData>>;

    /// Start a virtual dialog for a caller-provided session ID.
    fn start_virtual_dialog<'a>(
        &'a self,
        request: RuntimeVirtualDialogStartRequest,
    ) -> RuntimeVirtualDialogFuture<'a, RuntimeVirtualDialogData>;

    /// Send one user message into a virtual dialog.
    fn send_virtual_dialog_message<'a>(
        &'a self,
        request: RuntimeVirtualDialogSendRequest,
    ) -> RuntimeVirtualDialogFuture<'a, RuntimeVirtualDialogMessageData>;

    /// Delete one virtual dialog and its cleanup-friendly artifacts.
    fn delete_virtual_dialog<'a>(
        &'a self,
        session_id: &'a str,
    ) -> RuntimeVirtualDialogFuture<'a, RuntimeVirtualDialogDeleteResultData>;
}

/// Optional live runtime diagnostic providers used by the GraphQL route shell.
#[derive(Clone, Default)]
pub struct RuntimeApiLiveDiagnostics {
    pub redis_inspector: Option<Arc<dyn RuntimeRedisInspector>>,
    pub sql_reader: Option<Arc<dyn RuntimeSqlReader>>,
    pub entity_reader: Option<Arc<dyn RuntimeEntityReader>>,
    pub safety_check_reader: Option<Arc<dyn RuntimeSafetyCheckReader>>,
    pub llm_trace_inspector: Option<Arc<dyn RuntimeLlmTraceInspector>>,
    pub llm_run_inspector: Option<Arc<dyn RuntimeLlmRunInspector>>,
    pub turn_outcome_inspector: Option<Arc<dyn RuntimeTurnOutcomeInspector>>,
    pub routing_event_inspector: Option<Arc<dyn RuntimeRoutingEventInspector>>,
    pub llm_analytics_reader: Option<Arc<dyn RuntimeLlmAnalyticsReader>>,
    pub log_inspector: Option<Arc<dyn RuntimeLogInspector>>,
    pub taskman_inspector: Option<Arc<dyn RuntimeTaskmanInspector>>,
    pub updates_inspector: Option<Arc<dyn RuntimeUpdatesInspector>>,
    pub telegram_delivery_inspector: Option<Arc<dyn RuntimeTelegramDeliveryInspector>>,
    pub dispatcher_inspector: Option<Arc<dyn RuntimeDispatcherInspector>>,
    pub dispatcher_failure_inspector: Option<Arc<dyn RuntimeDispatcherFailureInspector>>,
    pub cache_inspector: Option<Arc<dyn RuntimeCacheInspector>>,
    pub memory_restarter: Option<Arc<dyn RuntimeMemoryRestarter>>,
    pub gemini_cache_purger: Option<Arc<dyn RuntimeGeminiCachePurger>>,
    pub virtual_dialog_manager: Option<Arc<dyn RuntimeVirtualDialogManager>>,
}

/// Runtime API log replay query after GraphQL/default shaping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeLogQuery {
    pub after_seq: u64,
    pub limit: i32,
    pub level: String,
    pub search: String,
}

/// Runtime API log entry.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeLogEntry {
    pub seq: u64,
    pub time: Option<String>,
    pub level: String,
    pub message: String,
    pub attrs: Option<Value>,
}

/// Runtime API outbound dispatcher stats from a live inspector.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeDispatcherStatsData {
    pub regular_queue_size: i32,
    pub immediate_queue_size: i32,
    pub processed_total: i64,
    pub deduped_total: i64,
    pub oldest_regular_age_ms: i32,
    pub oldest_immediate_age_ms: i32,
}

/// Runtime API terminal outbound dispatcher send failure from a live inspector.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeDispatchFailureData {
    pub at: String,
    pub virtual_id: String,
    pub chat_id: i64,
    pub method_kind: String,
    pub error: String,
    pub class: String,
    pub protected: bool,
    /// Trigger message id recovered from a reply-scoped debounce key, if any.
    pub reply_to_message_id: Option<i64>,
}

/// Runtime API cache stats from a live inspector.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeCacheStatsData {
    pub size: i32,
    pub capacity: i32,
    pub hits: u64,
    pub misses: u64,
    pub mem_size: u64,
}

/// Runtime API main/planner cache stats from a live inspector.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeCacheSnapshotData {
    pub cache: RuntimeCacheStatsData,
    pub planner_cache: RuntimeCacheStatsData,
}

/// Runtime API memory restart mutation result.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeMemoryRestartResultData {
    pub ok: bool,
    pub run_id: Option<i64>,
    pub retried_failed_runs: i32,
    pub queued_runs: i32,
    pub started: bool,
    pub override_: bool,
    pub provider: Option<String>,
    pub model: Option<String>,
}

/// Runtime API Gemini explicit-cache purge mutation result.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeGeminiCachePurgeResultData {
    pub scanned: i32,
    pub matched: i32,
    pub deleted: i32,
    pub failed: i32,
}

/// Runtime virtual-dialog tool mode.
#[derive(Clone, Copy, Debug, Default, Enum, Eq, PartialEq)]
pub enum RuntimeVirtualDialogToolMode {
    /// Use a cleanup-friendly toolbox that does not run side-effect tools.
    #[default]
    Safe,
    /// Use the normal toolbox; external side effects may start.
    Real,
}

/// Runtime virtual-dialog start request after GraphQL/default shaping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeVirtualDialogStartRequest {
    pub session_id: String,
    pub replace_existing: bool,
}

/// Runtime virtual-dialog send request after GraphQL/default shaping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeVirtualDialogSendRequest {
    pub session_id: String,
    pub text: String,
    pub tool_mode: RuntimeVirtualDialogToolMode,
}

/// Runtime virtual-dialog row returned by a live manager.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeVirtualDialogData {
    pub session_id: String,
    pub chat_id: i64,
    pub user_id: i64,
    pub next_message_id: i32,
    pub message_count: i32,
    pub last_activity_at: Option<String>,
    pub expires_at: Option<String>,
    pub messages: Vec<RuntimeVirtualDialogMessageData>,
}

/// Runtime virtual-dialog message returned by a live manager.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeVirtualDialogMessageData {
    pub message_id: i32,
    pub role: String,
    pub text: String,
    pub at: String,
    pub provider: Option<String>,
    pub tool_mode: Option<RuntimeVirtualDialogToolMode>,
    pub tool_calls: Option<Value>,
}

/// Runtime virtual-dialog delete result returned by a live manager.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeVirtualDialogDeleteResultData {
    pub found: bool,
    pub deleted: bool,
    pub history_deleted: i32,
    pub taskman_deleted: i32,
    pub llm_traces_deleted: i32,
}

/// Runtime API taskman job list filter after GraphQL/default shaping.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeTaskmanJobsFilter {
    pub q: String,
    pub status: Vec<String>,
    pub queue: Vec<String>,
    pub user_id: Option<i64>,
    pub chat_id: Option<i64>,
    pub time_field: String,
    pub from: String,
    pub to: String,
    pub sort_by: String,
    pub sort_dir: String,
    pub offset: i32,
    pub limit: i32,
}

/// Runtime API taskman list result from a live inspector.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeTaskmanJobListResultData {
    pub total: i32,
    pub offset: i32,
    pub limit: i32,
    pub summary: RuntimeTaskmanJobSummaryData,
    pub items: Vec<RuntimeTaskmanJobListEntryData>,
}

/// Runtime API taskman summary maps.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeTaskmanJobSummaryData {
    pub by_status: Value,
    pub by_queue: Value,
}

/// Runtime API taskman job list row from a live inspector.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeTaskmanJobListEntryData {
    pub id: i64,
    pub queue_name: String,
    pub priority: i32,
    pub title: String,
    pub job_type: String,
    pub status: String,
    pub user_id: i64,
    pub chat_id: i64,
    pub trigger_message_id: i32,
    pub thread_message_id: Option<i32>,
    pub progress_message_id: Option<i32>,
    pub result_message_id: Option<i32>,
    pub worker_id: Option<String>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub error_message: Option<String>,
    pub processing_timeout_seconds: i32,
    pub prompt_hash: Option<String>,
    pub estimated_processing_time: Option<i32>,
    pub actual_processing_time: Option<i32>,
    pub preview: Option<String>,
}

/// Runtime API taskman job details from a live inspector.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeTaskmanJobDetailsData {
    pub job: RuntimeTaskmanJobData,
    pub messages: Vec<RuntimeTaskmanJobMessageData>,
    pub events: Option<Value>,
}

/// Runtime API taskman full job row from a live inspector.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeTaskmanJobData {
    pub id: i64,
    pub queue_name: String,
    pub priority: i32,
    pub title: String,
    pub payload: Option<Value>,
    pub status: String,
    pub user_id: i64,
    pub chat_id: i64,
    pub trigger_message_id: i32,
    pub thread_message_id: Option<i32>,
    pub progress_message_id: Option<i32>,
    pub result_message_id: Option<i32>,
    pub worker_id: Option<String>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub error_message: Option<String>,
    pub processing_timeout_seconds: i32,
    pub prompt_hash: Option<String>,
    pub estimated_processing_time: Option<i32>,
    pub actual_processing_time: Option<i32>,
}

/// Runtime API taskman message row from a live inspector.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeTaskmanJobMessageData {
    pub id: i64,
    pub job_id: i64,
    pub message_type: String,
    pub chat_id: i64,
    pub message_id: i32,
    pub created_at: String,
    pub status: String,
}

/// Runtime API taskman diagnostics from a live inspector.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeTaskmanDiagnosticsData {
    pub running: bool,
    pub active: i32,
    pub started1m: i32,
    pub completed1m: i32,
    pub worker_count: i32,
    pub queue_signal_count: i32,
    pub slow_job_count: i32,
    pub queues: Vec<RuntimeTaskmanQueueDiagnosticsData>,
}

/// Runtime API taskman queue diagnostics from a live inspector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeTaskmanQueueDiagnosticsData {
    pub queue_name: String,
    pub priority: i32,
    pub pending: i32,
    pub pending_or_higher: i32,
    pub active: i32,
    pub worker_count: i32,
    pub eta_seconds: i32,
}

/// Runtime API decoded-update runtime snapshot from a live inspector.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeUpdatesRuntimeData {
    pub active: i32,
    pub state_active: i32,
    pub handle_active: i32,
    pub queue_len: i32,
    pub queue_error: Option<String>,
    pub started1m: i32,
    pub completed1m: i32,
    pub timeouts1m: i32,
    pub oldest_active_ms: i32,
    pub last_stall_at: Option<String>,
    pub tasks: Vec<RuntimeUpdatesTaskData>,
    pub gates: Option<RuntimeIngestionGatesData>,
    pub stream_len: i64,
    pub stream_group_lag: i64,
    pub stream_pending: i64,
    pub oldest_unmaterialized_ms: i64,
    pub ingress_used_memory_bytes: i64,
    pub ingress_maxmemory_bytes: i64,
    pub ingress_maxmemory_policy: String,
    pub ingress_aof_enabled: bool,
    pub ingress_aof_current_size_bytes: i64,
    pub ingress_aof_rewrite_in_progress: bool,
    pub ingress_aof_last_write_status: String,
    pub ingress_aof_last_rewrite_status: String,
    pub materializer_supervisor_running: bool,
    pub materializer_lease_held: bool,
    pub materializer_supervisor_restarts: i64,
    pub materializer_batch_rows: i32,
    pub materializer_batch_bytes: i64,
    pub materializer_batch_fill_ratio: f64,
    pub bulk_transaction_latency_ms: i64,
    pub materialized_batches: i64,
    pub inbox_insert_statements: i64,
    pub quarantine_insert_statements: i64,
    pub materialized_inserted: i64,
    pub materialized_duplicates: i64,
    pub materialized_conflicted: i64,
    pub materialized_quarantined: i64,
    pub materializer_reclaims: i64,
    pub ack_delete_mismatches: i64,
    pub materializer_db_failures: i64,
    pub materializer_redis_failures: i64,
    pub postgres_pending: i64,
    pub postgres_retry_wait: i64,
    pub postgres_dead_letter: i64,
    pub event_to_redis_avg_ms: i64,
    pub redis_to_postgres_avg_ms: i64,
    pub materialization_to_claim_avg_ms: i64,
    pub claim_to_taskman_avg_ms: i64,
}

/// Process-lifetime counters for the ingestion gates that consume a message
/// before any dialog job exists (rate limits, debounce coalescing, sampling).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeIngestionGatesData {
    pub task_rate_limited: i64,
    pub debounce_coalesced: i64,
    pub bot_sampling_skipped: i64,
    pub empty_trigger_skipped: i64,
    pub invalid_message: i64,
    pub random_skipped_roll: i64,
    pub random_skipped_reactivity_off: i64,
    pub random_skipped_gate: i64,
    pub random_skipped_user_disabled: i64,
}

/// Runtime API decoded-update task row from a live inspector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeUpdatesTaskData {
    pub stage: String,
    pub started_at: String,
    pub age_ms: i32,
    pub chat_id: Option<i64>,
    pub user_id: Option<i64>,
    pub update: String,
}

/// Shared cursor filter for durable Telegram inbox/outbox lists.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeTelegramDeliveryListFilter {
    pub before_id: Option<i64>,
    pub state: Option<String>,
    pub limit: i32,
}

/// Aggregate durable update-inbox diagnostics.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeTelegramUpdateInboxStatsData {
    pub pending: i64,
    pub processing: i64,
    pub retry_wait: i64,
    pub completed: i64,
    pub ignored: i64,
    pub dead_letter: i64,
    pub payload_conflicts: i64,
    pub quarantined: i64,
    pub total_deliveries: i64,
    pub oldest_pending_at: Option<String>,
    pub oldest_retry_at: Option<String>,
    pub oldest_lease_expiry: Option<String>,
}

/// One operator-visible durable update row. Raw update bytes are deliberately
/// excluded from this API; payload identity and size are sufficient for
/// correlation without exposing user content or credentials.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeTelegramUpdateInboxItemData {
    pub id: i64,
    pub bot_id: i64,
    pub update_id: i64,
    pub schema_version: i32,
    pub source: String,
    pub stream_ms: i64,
    pub stream_seq: i64,
    pub last_stream_ms: i64,
    pub last_stream_seq: i64,
    pub payload_size_bytes: i64,
    pub payload_sha256: String,
    pub payload_conflict: bool,
    pub update_type: Option<String>,
    pub telegram_event_at: Option<String>,
    pub first_received_at: String,
    pub last_received_at: String,
    pub materialized_at: String,
    pub delivery_count: i64,
    pub ordering_key: String,
    pub priority: i32,
    pub chat_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub user_id: Option<i64>,
    pub status: String,
    pub available_at: String,
    pub attempt_count: i32,
    pub lease_owner: Option<String>,
    pub leased_until: Option<String>,
    pub processing_started_at: Option<String>,
    pub state_applied_at: Option<String>,
    pub handler_completed_at: Option<String>,
    pub completed_at: Option<String>,
    pub outcome: Option<String>,
    pub ignored_reason: Option<String>,
    pub last_error_class: Option<String>,
    pub last_error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// One durable update handler attempt.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeTelegramUpdateAttemptData {
    pub attempt: i32,
    pub lease_token: i64,
    pub worker_id: String,
    pub claimed_at: String,
    pub state_started_at: Option<String>,
    pub state_completed_at: Option<String>,
    pub handler_started_at: Option<String>,
    pub handler_completed_at: Option<String>,
    pub finished_at: Option<String>,
    pub outcome: Option<String>,
    pub error_class: Option<String>,
    pub error: Option<String>,
}

/// Aggregate durable Telegram outbox diagnostics.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeTelegramOutboxStatsData {
    pub pending: i64,
    pub leased: i64,
    pub retry_wait: i64,
    pub delivered: i64,
    pub ambiguous: i64,
    pub dead_letter: i64,
    pub expired: i64,
    pub cancelled: i64,
    pub protected_unresolved: i64,
    pub oldest_pending_at: Option<String>,
    pub oldest_lease_expiry: Option<String>,
}

/// One operator-visible durable outbound operation. Telegram request payloads,
/// media bytes, and raw receipts are deliberately excluded from GraphQL.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeTelegramOutboxItemData {
    pub id: i64,
    pub operation_id: String,
    pub batch_id: String,
    pub part_index: i32,
    pub bot_id: i64,
    pub chat_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub ordering_key: String,
    pub causation_update_id: Option<i64>,
    pub dialog_job_id: Option<i64>,
    pub trigger_message_id: Option<i64>,
    pub method_kind: String,
    pub delivery_policy: String,
    pub protected: bool,
    pub priority: i32,
    pub state: String,
    pub available_at: String,
    pub expires_at: Option<String>,
    pub attempt_count: i32,
    pub lease_owner: Option<String>,
    pub leased_until: Option<String>,
    pub last_error_class: Option<String>,
    pub last_error: Option<String>,
    pub response_kind: Option<String>,
    pub telegram_message_ids: Vec<i64>,
    pub has_receipt: bool,
    pub confirmed_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// One durable outbound network attempt.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeTelegramOutboxAttemptData {
    pub attempt: i32,
    pub lease_token: i64,
    pub worker_id: String,
    pub claimed_at: String,
    pub request_started_at: Option<String>,
    pub response_received_at: Option<String>,
    pub finished_at: Option<String>,
    pub outcome: Option<String>,
    pub http_status: Option<i32>,
    pub latency_ms: Option<i64>,
    pub error_class: Option<String>,
    pub error: Option<String>,
}

/// Operator-confirmed manual outbox retry request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeTelegramOutboxRetryRequest {
    pub operation_id: String,
    pub accept_duplicate_risk: bool,
}

/// Result of an explicit outbox operator action.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeTelegramOutboxMutationResultData {
    pub operation_id: String,
    pub changed: bool,
    pub state: Option<String>,
}

/// Runtime API SQL read request after GraphQL/default shaping.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeSqlReadRequest {
    pub sql: String,
    pub args: Vec<Value>,
    pub timeout_ms: i32,
    pub row_limit: i32,
    pub result_bytes_limit: i32,
}

/// Runtime API SQL read result.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeSqlReadResult {
    pub columns: Vec<String>,
    pub rows: Vec<Value>,
    pub row_count: i32,
    pub elapsed_ms: i32,
    pub truncated: bool,
}

/// Runtime API users list filter after GraphQL/default shaping.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeUsersFilter {
    pub q: String,
    pub offset: i32,
    pub limit: i32,
}

/// Runtime API user lookup after GraphQL/default shaping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeUserLookup {
    Id(i64),
    Username(String),
}

/// Runtime API chats list filter after GraphQL/default shaping.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeChatsFilter {
    pub q: String,
    pub offset: i32,
    pub limit: i32,
    pub member_username: Option<String>,
    pub member_user_id: Option<i64>,
}

/// Runtime API user row from a live entity reader.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeUserData {
    pub id: i64,
    pub is_premium: Option<bool>,
    pub first_name: String,
    pub last_name: Option<String>,
    pub username: Option<String>,
    pub language_code: Option<String>,
    pub is_vip: Option<bool>,
    pub discovered_at: Option<String>,
    pub updated_at: Option<String>,
}

/// Runtime API users connection from a live entity reader.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeUserConnectionData {
    pub count: i32,
    pub offset: i32,
    pub limit: i32,
    pub items: Vec<RuntimeUserData>,
}

/// Runtime API user details from a live entity reader.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeUserDetailsData {
    pub user: RuntimeUserData,
    pub subscription: Option<RuntimeSubscriptionData>,
    pub vip: Option<RuntimeVipCacheData>,
    pub vip_summary: Option<RuntimeVipSummaryData>,
    pub vip_events: Vec<RuntimeVipEventData>,
    pub subscriptions: Vec<RuntimeSubscriptionData>,
}

/// Runtime API subscription row from a live entity reader.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeSubscriptionData {
    pub id: i64,
    pub user_id: i64,
    pub telegram_payment_charge_id: String,
    pub provider_payment_charge_id: String,
    pub expires_at: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub canceled_at: Option<String>,
    pub refunded_at: Option<String>,
    pub status: String,
}

/// Runtime API VIP cache row from a live entity reader.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeVipCacheData {
    pub user_id: i64,
    pub is_vip: bool,
    pub expires_at: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

/// Runtime API VIP summary from a live entity reader.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeVipSummaryData {
    pub active: bool,
    pub has_history: bool,
    pub expires_at: Option<String>,
    pub remaining_seconds: String,
    pub remaining_days: i32,
    pub latest_event_id: Option<i64>,
    pub latest_event_type: Option<String>,
    pub latest_reason: Option<String>,
    pub latest_created_at: Option<String>,
}

/// Runtime API VIP event row from a live entity reader.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeVipEventData {
    pub id: i64,
    pub event_type: String,
    pub delta_seconds: String,
    pub delta_days: f64,
    pub effective_expires_at: Option<String>,
    pub actor_user_id: Option<i64>,
    pub actor_label: Option<String>,
    pub reason: Option<String>,
    pub created_at: Option<String>,
    pub subscription_id: Option<i64>,
    pub telegram_payment_charge_id: Option<String>,
    pub provider_payment_charge_id: Option<String>,
    pub subscription_status: Option<String>,
}

/// Runtime API chat row from a live entity reader.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeChatData {
    pub id: i64,
    pub chat_type: String,
    pub title: Option<String>,
    pub username: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub is_forum: Option<bool>,
    pub description: Option<String>,
    pub invite_link: Option<String>,
    pub discovered_at: Option<String>,
    pub updated_at: Option<String>,
}

/// Runtime API chats connection from a live entity reader.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeChatConnectionData {
    pub count: i32,
    pub offset: i32,
    pub limit: i32,
    pub items: Vec<RuntimeChatData>,
}

/// Runtime API chat-member row from a live entity reader.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeChatMemberData {
    pub chat_id: i64,
    pub user_id: i64,
    pub status: String,
    pub is_anonymous: Option<bool>,
    pub custom_title: Option<String>,
    pub can_be_edited: Option<bool>,
    pub can_manage_chat: Option<bool>,
    pub can_delete_messages: Option<bool>,
    pub can_manage_video_chats: Option<bool>,
    pub can_restrict_members: Option<bool>,
    pub can_promote_members: Option<bool>,
    pub can_change_info: Option<bool>,
    pub can_invite_users: Option<bool>,
    pub can_post_messages: Option<bool>,
    pub can_edit_messages: Option<bool>,
    pub can_pin_messages: Option<bool>,
    pub can_manage_topics: Option<bool>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub last_message_at: Option<String>,
}

/// Runtime API chat-member plus optional user row from a live entity reader.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeChatMemberWithUserData {
    pub member: RuntimeChatMemberData,
    pub user: Option<RuntimeUserData>,
}

/// Runtime API safety-check list filter after GraphQL/default shaping.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeSafetyChecksFilter {
    pub q: String,
    pub flagged: Option<bool>,
    pub offset: i32,
    pub limit: i32,
}

/// Runtime API safety-check connection from a live reader.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeSafetyCheckConnectionData {
    pub count: i32,
    pub offset: i32,
    pub limit: i32,
    pub items: Vec<RuntimeSafetyCheckData>,
}

/// Runtime API safety-check row from a live reader.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeSafetyCheckData {
    pub id: i64,
    pub created_at: String,
    pub source: String,
    pub flow: Option<String>,
    pub mode: Option<String>,
    pub chat_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub message_id: Option<i32>,
    pub user_id: Option<i64>,
    pub deployment_id: String,
    pub external_session_id: Option<String>,
    pub request_messages: Option<Value>,
    pub flagged: Option<bool>,
    pub internal_session_id: Option<String>,
    pub policies: Option<Value>,
    pub response_json: Option<Value>,
    pub duration_ms: i32,
    pub error: Option<String>,
}

/// Runtime API LLM request filter after GraphQL/default shaping.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeLlmRequestsFilter {
    pub q: String,
    pub source: String,
    pub model: String,
    pub chat_id: Option<i64>,
    pub user_id: Option<i64>,
    pub message_id: Option<i32>,
    /// Only traces whose result carries a non-empty error.
    pub error_only: bool,
    /// Only traces with neither a response preview nor an error — the
    /// silent-completion fingerprint.
    pub empty_only: bool,
    pub limit: i32,
}

/// Runtime API LLM routing event filter after GraphQL/default shaping.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeRoutingEventsFilter {
    pub q: String,
    pub workflow_key: String,
    pub event_type: String,
    pub limit: i32,
}

/// Runtime API LLM routing event.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeRoutingEventData {
    pub id: i64,
    pub at: String,
    pub severity: String,
    pub event_type: String,
    pub workflow_key: String,
    pub provider_id: Option<i64>,
    pub model_id: Option<i64>,
    pub queue_name: Option<String>,
    pub job_id: Option<i64>,
    pub chat_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub message_id: Option<i32>,
    pub dedupe_key: String,
    pub summary: String,
    pub detail: Value,
}

/// Runtime API LLM request trace.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeLlmRequestData {
    pub id: i64,
    pub at: String,
    /// Agent-run correlation key (`job-…`, `song-…`, `console-…`).
    pub run_id: Option<String>,
    /// 1-based round ordinal within the run.
    pub run_seq: Option<i32>,
    pub provider: Option<String>,
    pub request_kind: Option<String>,
    pub source: String,
    pub mode: Option<String>,
    pub flow: Option<String>,
    pub iteration: i32,
    pub model: Option<String>,
    pub chat: RuntimeLlmRequestChatData,
    pub user: RuntimeLlmRequestUserData,
    pub message: RuntimeLlmRequestMessageData,
    pub gen_config: RuntimeLlmGenConfigData,
    pub docs: Option<Value>,
    pub messages: Option<Value>,
    pub raw_request: Option<Value>,
    pub resolved_cache_content: Option<Value>,
    pub raw_response: Option<Value>,
    pub transport: Option<Value>,
    pub inference_params: Option<Value>,
    pub usage: Option<Value>,
    pub timings: Option<Value>,
    pub prompt_chars: i32,
    pub prompt_messages: i32,
    pub docs_chars: i32,
    pub duration_ms: i32,
    pub result: RuntimeLlmRequestResultData,
}

/// Runtime API LLM request chat metadata.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeLlmRequestChatData {
    pub chat_id: i64,
    pub thread_id: Option<i32>,
    pub chat_title: Option<String>,
}

/// Runtime API LLM request user metadata.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeLlmRequestUserData {
    pub user_id: i64,
    pub full_name: Option<String>,
}

/// Runtime API LLM request message metadata.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeLlmRequestMessageData {
    pub message_id: i32,
}

/// Runtime API LLM generation config.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeLlmGenConfigData {
    pub max_output_tokens: i32,
    pub temperature: f64,
    pub top_p: f64,
    pub top_k: i32,
    pub safety_settings: Option<Value>,
}

/// Runtime API LLM request result.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeLlmRequestResultData {
    pub duration_ms: i32,
    pub error: Option<String>,
    pub response_text_preview: Option<String>,
}

/// Runtime API dialog turn outcome filter after GraphQL/default shaping.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeTurnOutcomesFilter {
    pub chat_id: Option<i64>,
    pub job_id: Option<i64>,
    pub outcome: String,
    pub limit: i32,
}

/// Runtime API dialog turn outcome row.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeTurnOutcomeData {
    pub id: i64,
    pub at: String,
    pub job_id: i64,
    pub queue_name: String,
    pub chat_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub user_id: Option<i64>,
    pub trigger_message_id: Option<i32>,
    pub attempt: i32,
    pub outcome: String,
    pub reason: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub elapsed_ms: Option<i32>,
    pub budget_ms: Option<i32>,
    pub user_signal: Option<String>,
    pub sent_message_parts: Option<i32>,
    pub side_effect_ticket_id: Option<i64>,
    pub detail: Value,
}

/// Runtime API LLM analytics summary.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeLlmAnalyticsData {
    pub range: String,
    pub bucket: String,
    pub since: String,
    pub totals: RuntimeLlmAnalyticsTotalsData,
    pub series: Vec<RuntimeLlmAnalyticsSeriesPointData>,
    pub model_series: Vec<RuntimeLlmAnalyticsModelSeriesPointData>,
    pub top_chats: Vec<RuntimeLlmAnalyticsTopChatData>,
    pub models: Vec<RuntimeLlmAnalyticsModelStatData>,
    pub providers: Vec<RuntimeLlmAnalyticsProviderStatData>,
    pub inference_params: Vec<RuntimeLlmAnalyticsInferenceParamStatData>,
    pub stage_metrics: Vec<RuntimeLlmAnalyticsStageMetricData>,
    pub runtime_jobs: Vec<RuntimeJobAnalyticsStatData>,
    pub runtime_jobs_error: Option<String>,
    pub ai_farm_capacity: Option<RuntimeAifarmCapacitySnapshotData>,
}

/// Runtime API LLM analytics model time-series point.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeLlmAnalyticsModelSeriesPointData {
    pub ts: String,
    pub model: String,
    pub request_count: i32,
    pub error_count: i32,
    pub avg_duration_ms: i32,
    pub avg_generation_tps: f64,
    pub avg_effective_output_tps: f64,
    pub output_tokens: i64,
}

/// Runtime API LLM analytics totals.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeLlmAnalyticsTotalsData {
    pub total_count: i32,
    pub error_count: i32,
    pub avg_duration_ms: i32,
}

/// Runtime API LLM analytics series point.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeLlmAnalyticsSeriesPointData {
    pub ts: String,
    pub total_count: i32,
    pub error_count: i32,
    pub avg_duration_ms: i32,
}

/// Runtime API LLM analytics top chat.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeLlmAnalyticsTopChatData {
    pub chat_id: i64,
    pub title: Option<String>,
    pub username: Option<String>,
    pub request_count: i32,
}

/// Runtime API LLM analytics model stat.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeLlmAnalyticsModelStatData {
    pub model: String,
    pub request_count: i32,
    pub error_count: i32,
    pub avg_duration_ms: i32,
    pub p50_duration_ms: i32,
    pub p95_duration_ms: i32,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_tokens: i64,
    pub avg_generation_tps: f64,
    pub avg_effective_output_tps: f64,
    pub p50_effective_output_tps: f64,
    pub p95_effective_output_tps: f64,
}

/// Runtime API LLM analytics provider stat.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeLlmAnalyticsProviderStatData {
    pub provider: String,
    pub source: String,
    pub request_count: i32,
    pub error_count: i32,
    pub avg_duration_ms: i32,
    pub p50_duration_ms: i32,
    pub p95_duration_ms: i32,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_tokens: i64,
    pub avg_generation_tps: f64,
    pub avg_effective_output_tps: f64,
}

/// Runtime API LLM analytics inference-parameter stat.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeLlmAnalyticsInferenceParamStatData {
    pub provider: String,
    pub source: String,
    pub model: String,
    pub max_tokens: Option<i32>,
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    pub top_k: Option<f64>,
    pub candidate_count: Option<i32>,
    pub tool_mode: String,
    pub response_format: String,
    pub request_count: i32,
    pub error_count: i32,
    pub avg_duration_ms: i32,
    pub avg_effective_output_tps: f64,
}

/// Runtime API LLM analytics stage metric.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeLlmAnalyticsStageMetricData {
    pub stage: String,
    pub source: String,
    pub request_count: i32,
    pub error_count: i32,
    pub avg_duration_ms: i32,
    pub p50_duration_ms: i32,
    pub p95_duration_ms: i32,
    pub avg_iteration: i32,
    pub max_iteration: i32,
}

/// Runtime API task/job analytics stat.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeJobAnalyticsStatData {
    pub job_type: String,
    pub queue_name: String,
    pub provider: String,
    pub created_count: i32,
    pub completed_count: i32,
    pub failed_count: i32,
    pub avg_wait_ms: i32,
    pub p95_wait_ms: i32,
    pub avg_processing_ms: i32,
    pub p95_processing_ms: i32,
}

/// Runtime API AI Farm capacity snapshot.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeAifarmCapacitySnapshotData {
    pub service: String,
    pub max_concurrent_jobs: i32,
    pub running: i32,
    pub queued: i32,
    pub available: i32,
    pub locked: bool,
    pub ready: bool,
    pub observed_at: String,
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RuntimeApiGraphqlSnapshot {
    pub log_level: String,
    pub web_host: String,
    pub web_port: i32,
    pub runtime_api_enabled: bool,
    pub runtime_api_host: String,
    pub runtime_api_port: i32,
    pub discovery_base_url: Option<String>,
    pub embedder_enabled: bool,
    pub embedder_url: Option<String>,
    pub shield_enabled: bool,
    pub shield_embedder_url: Option<String>,
    pub shield_max_matches: i32,
    pub shield_vector_min_score: f64,
    pub shield_lexical_min_score: f64,
    pub shield_retrieval_timeout_seconds: i32,
    pub shield_history_tail_messages: i32,
    pub vision_discovery_service_name: String,
    pub vision_discovery_endpoint_name: String,
    pub vision_model: String,
    pub vision_max_tokens: i32,
    pub vision_temperature: f64,
    pub vision_direct_image_limit: i32,
    pub vision_request_timeout_seconds: i32,
    pub white_circle_enabled: bool,
    pub ace_step_enabled: bool,
    pub ace_step_base_url: Option<String>,
    pub dialog_provider: String,
    pub dialog_fallback_provider: Option<String>,
    pub persistent_queue_enabled: bool,
    pub active_draw_providers: Vec<String>,
    pub sql_timeout_ms: i32,
    pub sql_row_limit: i32,
    pub sql_result_bytes_limit: i32,
    pub db_status: String,
    pub redis_status: String,
}

impl Default for RuntimeApiGraphqlSnapshot {
    fn default() -> Self {
        Self {
            log_level: "info".to_owned(),
            web_host: "127.0.0.1".to_owned(),
            web_port: 0,
            runtime_api_enabled: false,
            runtime_api_host: "127.0.0.1".to_owned(),
            runtime_api_port: 0,
            discovery_base_url: None,
            embedder_enabled: false,
            embedder_url: None,
            shield_enabled: false,
            shield_embedder_url: None,
            shield_max_matches: 0,
            shield_vector_min_score: 0.0,
            shield_lexical_min_score: 0.0,
            shield_retrieval_timeout_seconds: 0,
            shield_history_tail_messages: 0,
            vision_discovery_service_name: String::new(),
            vision_discovery_endpoint_name: String::new(),
            vision_model: String::new(),
            vision_max_tokens: 0,
            vision_temperature: 0.0,
            vision_direct_image_limit: 0,
            vision_request_timeout_seconds: 0,
            white_circle_enabled: false,
            ace_step_enabled: false,
            ace_step_base_url: None,
            dialog_provider: String::new(),
            dialog_fallback_provider: None,
            persistent_queue_enabled: false,
            active_draw_providers: Vec::new(),
            sql_timeout_ms: 0,
            sql_row_limit: 0,
            sql_result_bytes_limit: 0,
            db_status: "disabled".to_owned(),
            redis_status: "disabled".to_owned(),
        }
    }
}

pub fn runtime_api_graphql_schema(snapshot: RuntimeApiGraphqlSnapshot) -> RuntimeApiSchema {
    runtime_api_graphql_schema_with_redis(snapshot, None)
}

/// Build the runtime GraphQL schema with optional live Redis diagnostics.
pub fn runtime_api_graphql_schema_with_redis(
    snapshot: RuntimeApiGraphqlSnapshot,
    redis_inspector: Option<Arc<dyn RuntimeRedisInspector>>,
) -> RuntimeApiSchema {
    runtime_api_graphql_schema_with_diagnostics(snapshot, redis_inspector, None)
}

/// Build the runtime GraphQL schema with optional live diagnostics.
pub fn runtime_api_graphql_schema_with_diagnostics(
    snapshot: RuntimeApiGraphqlSnapshot,
    redis_inspector: Option<Arc<dyn RuntimeRedisInspector>>,
    sql_reader: Option<Arc<dyn RuntimeSqlReader>>,
) -> RuntimeApiSchema {
    runtime_api_graphql_schema_with_live_diagnostics(
        snapshot,
        RuntimeApiLiveDiagnostics {
            redis_inspector,
            sql_reader,
            ..RuntimeApiLiveDiagnostics::default()
        },
    )
}

/// Build the runtime GraphQL schema with optional live diagnostics.
pub fn runtime_api_graphql_schema_with_live_diagnostics(
    snapshot: RuntimeApiGraphqlSnapshot,
    diagnostics: RuntimeApiLiveDiagnostics,
) -> RuntimeApiSchema {
    Schema::build(
        RuntimeQuery {
            snapshot,
            redis_inspector: diagnostics.redis_inspector,
            sql_reader: diagnostics.sql_reader,
            entity_reader: diagnostics.entity_reader,
            safety_check_reader: diagnostics.safety_check_reader,
            llm_trace_inspector: diagnostics.llm_trace_inspector,
            llm_run_inspector: diagnostics.llm_run_inspector,
            turn_outcome_inspector: diagnostics.turn_outcome_inspector,
            routing_event_inspector: diagnostics.routing_event_inspector,
            llm_analytics_reader: diagnostics.llm_analytics_reader,
            log_inspector: diagnostics.log_inspector,
            taskman_inspector: diagnostics.taskman_inspector,
            updates_inspector: diagnostics.updates_inspector,
            telegram_delivery_inspector: diagnostics.telegram_delivery_inspector.clone(),
            dispatcher_inspector: diagnostics.dispatcher_inspector,
            dispatcher_failure_inspector: diagnostics.dispatcher_failure_inspector,
            cache_inspector: diagnostics.cache_inspector,
            virtual_dialog_manager: diagnostics.virtual_dialog_manager.clone(),
        },
        RuntimeMutation {
            memory_restarter: diagnostics.memory_restarter,
            gemini_cache_purger: diagnostics.gemini_cache_purger,
            virtual_dialog_manager: diagnostics.virtual_dialog_manager,
            telegram_delivery_inspector: diagnostics.telegram_delivery_inspector,
        },
        EmptySubscription,
    )
    .finish()
}

pub struct RuntimeQuery {
    snapshot: RuntimeApiGraphqlSnapshot,
    redis_inspector: Option<Arc<dyn RuntimeRedisInspector>>,
    sql_reader: Option<Arc<dyn RuntimeSqlReader>>,
    entity_reader: Option<Arc<dyn RuntimeEntityReader>>,
    safety_check_reader: Option<Arc<dyn RuntimeSafetyCheckReader>>,
    llm_trace_inspector: Option<Arc<dyn RuntimeLlmTraceInspector>>,
    llm_run_inspector: Option<Arc<dyn RuntimeLlmRunInspector>>,
    turn_outcome_inspector: Option<Arc<dyn RuntimeTurnOutcomeInspector>>,
    routing_event_inspector: Option<Arc<dyn RuntimeRoutingEventInspector>>,
    llm_analytics_reader: Option<Arc<dyn RuntimeLlmAnalyticsReader>>,
    log_inspector: Option<Arc<dyn RuntimeLogInspector>>,
    taskman_inspector: Option<Arc<dyn RuntimeTaskmanInspector>>,
    updates_inspector: Option<Arc<dyn RuntimeUpdatesInspector>>,
    telegram_delivery_inspector: Option<Arc<dyn RuntimeTelegramDeliveryInspector>>,
    dispatcher_inspector: Option<Arc<dyn RuntimeDispatcherInspector>>,
    dispatcher_failure_inspector: Option<Arc<dyn RuntimeDispatcherFailureInspector>>,
    cache_inspector: Option<Arc<dyn RuntimeCacheInspector>>,
    virtual_dialog_manager: Option<Arc<dyn RuntimeVirtualDialogManager>>,
}

pub struct RuntimeMutation {
    memory_restarter: Option<Arc<dyn RuntimeMemoryRestarter>>,
    gemini_cache_purger: Option<Arc<dyn RuntimeGeminiCachePurger>>,
    virtual_dialog_manager: Option<Arc<dyn RuntimeVirtualDialogManager>>,
    telegram_delivery_inspector: Option<Arc<dyn RuntimeTelegramDeliveryInspector>>,
}

#[Object]
impl RuntimeQuery {
    async fn runtime_state(&self) -> RuntimeState {
        let cache_snapshot = self
            .cache_inspector
            .as_deref()
            .map(RuntimeCacheInspector::stats)
            .unwrap_or_default();
        RuntimeState {
            log_level: self.snapshot.log_level.clone(),
            dispatcher: self
                .dispatcher_inspector
                .as_deref()
                .map(RuntimeDispatcherInspector::stats)
                .map(DispatcherStats::from)
                .unwrap_or_default(),
            cache: CacheStats::from(cache_snapshot.cache),
            planner_cache: CacheStats::from(cache_snapshot.planner_cache),
        }
    }

    async fn health_snapshot(&self) -> HealthSnapshot {
        let updates_queue_length = match self.updates_inspector.as_deref() {
            Some(inspector) => inspector
                .snapshot()
                .await
                .map(|runtime| runtime.queue_len)
                .unwrap_or(-1),
            None => 0,
        };

        HealthSnapshot {
            db: DependencyHealth::from_status(&self.snapshot.db_status),
            redis: DependencyHealth::from_status(&self.snapshot.redis_status),
            dispatcher: match self.dispatcher_inspector.as_deref() {
                None => DependencyHealth::from_status("disabled"),
                Some(inspector) => {
                    let stats = inspector.stats();
                    DependencyHealth::from_status(if stats.oldest_regular_age_ms > 60_000 {
                        "degraded"
                    } else {
                        "ok"
                    })
                }
            },
            ai_handler: DependencyHealth::from_status("disabled"),
            shield: DependencyHealth::from_status("disabled"),
            rag: DependencyHealth::from_status("disabled"),
            ace_step: DependencyHealth::from_status("disabled"),
            updates_queue_length,
        }
    }

    async fn config_snapshot(&self) -> ConfigSnapshot {
        ConfigSnapshot::from(self.snapshot.clone())
    }

    async fn users(
        &self,
        filter: Option<UsersFilterInput>,
    ) -> async_graphql::Result<UserConnection> {
        let Some(reader) = self.entity_reader.as_deref() else {
            return Err("runtime entity reader is not configured".into());
        };
        reader
            .users(users_filter_from_input(filter))
            .await
            .map(UserConnection::from)
            .map_err(async_graphql::Error::new)
    }

    async fn user(
        &self,
        id: Option<ID>,
        username: Option<String>,
    ) -> async_graphql::Result<Option<UserDetails>> {
        let Some(reader) = self.entity_reader.as_deref() else {
            return Err("runtime entity reader is not configured".into());
        };
        let lookup = user_lookup_from_input(id, username)?;
        reader
            .user(lookup)
            .await
            .map(|user| user.map(UserDetails::from))
            .map_err(async_graphql::Error::new)
    }

    async fn chats(
        &self,
        filter: Option<ChatsFilterInput>,
    ) -> async_graphql::Result<ChatConnection> {
        let Some(reader) = self.entity_reader.as_deref() else {
            return Err("runtime entity reader is not configured".into());
        };
        reader
            .chats(chats_filter_from_input(filter)?)
            .await
            .map(ChatConnection::from)
            .map_err(async_graphql::Error::new)
    }

    async fn chat(&self, id: ID) -> async_graphql::Result<Option<Chat>> {
        let Some(reader) = self.entity_reader.as_deref() else {
            return Err("runtime entity reader is not configured".into());
        };
        let id = parse_id(id, "id")?;
        reader
            .chat(id)
            .await
            .map(|chat| chat.map(Chat::from))
            .map_err(async_graphql::Error::new)
    }

    async fn chat_members(
        &self,
        #[graphql(name = "chatID")] chat_id: ID,
    ) -> async_graphql::Result<Vec<ChatMemberWithUser>> {
        let Some(reader) = self.entity_reader.as_deref() else {
            return Err("runtime entity reader is not configured".into());
        };
        let chat_id = parse_id(chat_id, "chatID")?;
        reader
            .chat_members(chat_id)
            .await
            .map(|members| members.into_iter().map(ChatMemberWithUser::from).collect())
            .map_err(async_graphql::Error::new)
    }

    async fn taskman_jobs(
        &self,
        filter: Option<TaskmanJobsFilterInput>,
    ) -> async_graphql::Result<TaskmanJobListResult> {
        let Some(inspector) = self.taskman_inspector.as_deref() else {
            return Ok(TaskmanJobListResult::empty());
        };
        let filter = taskman_jobs_filter_from_input(filter)?;
        let result = inspector
            .list_jobs(filter)
            .map_err(async_graphql::Error::new)?;
        Ok(TaskmanJobListResult::from(result))
    }

    async fn taskman_job(&self, id: ID) -> async_graphql::Result<Option<TaskmanJobDetails>> {
        let Some(inspector) = self.taskman_inspector.as_deref() else {
            return Ok(None);
        };
        let id = id
            .as_str()
            .parse::<i64>()
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        inspector
            .job(id)
            .await
            .map(|job| job.map(TaskmanJobDetails::from))
            .map_err(async_graphql::Error::new)
    }

    async fn taskman_queue_diagnostics(
        &self,
        queues: Option<Vec<String>>,
        priority: Option<i32>,
    ) -> async_graphql::Result<TaskmanDiagnostics> {
        let Some(inspector) = self.taskman_inspector.as_deref() else {
            return Ok(TaskmanDiagnostics::default());
        };
        inspector
            .queue_diagnostics(
                queues.unwrap_or_default(),
                priority.unwrap_or(RUNTIME_TASKMAN_LOWEST_PRIORITY),
            )
            .map(TaskmanDiagnostics::from)
            .map_err(async_graphql::Error::new)
    }

    async fn safety_checks(
        &self,
        filter: Option<SafetyChecksFilterInput>,
    ) -> async_graphql::Result<SafetyCheckConnection> {
        let Some(reader) = self.safety_check_reader.as_deref() else {
            return Err("runtime safety-check reader is not configured".into());
        };
        reader
            .safety_checks(safety_checks_filter_from_input(filter))
            .await
            .map(SafetyCheckConnection::from)
            .map_err(async_graphql::Error::new)
    }

    async fn llm_requests(
        &self,
        filter: Option<LlmRequestsFilterInput>,
    ) -> async_graphql::Result<Vec<LlmRequest>> {
        let Some(inspector) = self.llm_trace_inspector.as_deref() else {
            return Ok(Vec::new());
        };
        inspector
            .llm_requests(llm_requests_filter_from_input(filter)?)
            .map(|items| items.into_iter().map(LlmRequest::from).collect())
            .map_err(async_graphql::Error::new)
    }

    async fn llm_runs(
        &self,
        filter: Option<LlmRunsFilterInput>,
    ) -> async_graphql::Result<Vec<Json<Value>>> {
        let Some(inspector) = self.llm_run_inspector.as_deref() else {
            return Ok(Vec::new());
        };
        inspector
            .llm_runs(llm_runs_filter_from_input(filter)?)
            .map(|items| items.into_iter().map(Json).collect())
            .map_err(async_graphql::Error::new)
    }

    async fn dialog_turn_outcomes(
        &self,
        filter: Option<DialogTurnOutcomesFilterInput>,
    ) -> async_graphql::Result<Vec<DialogTurnOutcome>> {
        let Some(inspector) = self.turn_outcome_inspector.as_deref() else {
            return Ok(Vec::new());
        };
        inspector
            .turn_outcomes(turn_outcomes_filter_from_input(filter)?)
            .map(|items| items.into_iter().map(DialogTurnOutcome::from).collect())
            .map_err(async_graphql::Error::new)
    }

    async fn dispatcher_send_failures(
        &self,
        limit: Option<i32>,
    ) -> async_graphql::Result<Vec<DispatcherSendFailure>> {
        let Some(inspector) = self.dispatcher_failure_inspector.as_deref() else {
            return Ok(Vec::new());
        };
        let limit = clamp_positive_range_i32(limit.unwrap_or(0), 100, 1024);
        Ok(inspector
            .send_failures(limit)
            .into_iter()
            .map(DispatcherSendFailure::from)
            .collect())
    }

    async fn llm_routing_events(
        &self,
        filter: Option<RoutingEventsFilterInput>,
    ) -> async_graphql::Result<Vec<RoutingEvent>> {
        let Some(inspector) = self.routing_event_inspector.as_deref() else {
            return Ok(Vec::new());
        };
        inspector
            .routing_events(routing_events_filter_from_input(filter))
            .map(|items| items.into_iter().map(RoutingEvent::from).collect())
            .map_err(async_graphql::Error::new)
    }

    async fn llm_analytics(&self, range: Option<String>) -> async_graphql::Result<LlmAnalytics> {
        let Some(reader) = self.llm_analytics_reader.as_deref() else {
            return Err("runtime LLM analytics reader is not configured".into());
        };
        reader
            .llm_analytics(range.as_deref().unwrap_or_default())
            .await
            .map(LlmAnalytics::from)
            .map_err(async_graphql::Error::new)
    }

    async fn virtual_dialog(
        &self,
        #[graphql(name = "sessionID")] session_id: String,
    ) -> async_graphql::Result<Option<VirtualDialog>> {
        let Some(manager) = self.virtual_dialog_manager.as_deref() else {
            return Err("runtime virtual dialog manager is not configured".into());
        };
        let session_id = normalize_virtual_dialog_session_id(session_id)?;
        manager
            .virtual_dialog(&session_id)
            .await
            .map(|dialog| dialog.map(VirtualDialog::from))
            .map_err(async_graphql::Error::new)
    }

    async fn updates_runtime(&self) -> async_graphql::Result<UpdatesRuntime> {
        let Some(inspector) = self.updates_inspector.as_deref() else {
            return Ok(UpdatesRuntime::default());
        };
        inspector
            .snapshot()
            .await
            .map(UpdatesRuntime::from)
            .map_err(async_graphql::Error::new)
    }

    async fn telegram_update_inbox_stats(&self) -> async_graphql::Result<TelegramUpdateInboxStats> {
        let inspector = self.telegram_delivery_inspector()?;
        inspector
            .update_inbox_stats()
            .await
            .map(TelegramUpdateInboxStats::from)
            .map_err(async_graphql::Error::new)
    }

    async fn telegram_update_inbox(
        &self,
        id: ID,
    ) -> async_graphql::Result<Option<TelegramUpdateInboxItem>> {
        let inspector = self.telegram_delivery_inspector()?;
        inspector
            .update_inbox_item(parse_positive_id(id, "id")?)
            .await
            .map(|item| item.map(TelegramUpdateInboxItem::from))
            .map_err(async_graphql::Error::new)
    }

    async fn telegram_update_inbox_items(
        &self,
        #[graphql(name = "beforeID")] before_id: Option<ID>,
        state: Option<String>,
        limit: Option<i32>,
    ) -> async_graphql::Result<Vec<TelegramUpdateInboxItem>> {
        let inspector = self.telegram_delivery_inspector()?;
        inspector
            .update_inbox_items(telegram_delivery_list_filter(before_id, state, limit)?)
            .await
            .map(|items| {
                items
                    .into_iter()
                    .map(TelegramUpdateInboxItem::from)
                    .collect()
            })
            .map_err(async_graphql::Error::new)
    }

    async fn telegram_update_inbox_attempts(
        &self,
        #[graphql(name = "inboxID")] inbox_id: ID,
        limit: Option<i32>,
    ) -> async_graphql::Result<Vec<TelegramUpdateAttempt>> {
        let inspector = self.telegram_delivery_inspector()?;
        inspector
            .update_inbox_attempts(
                parse_positive_id(inbox_id, "inboxID")?,
                telegram_delivery_limit(limit),
            )
            .await
            .map(|items| items.into_iter().map(TelegramUpdateAttempt::from).collect())
            .map_err(async_graphql::Error::new)
    }

    async fn telegram_outbox_stats(&self) -> async_graphql::Result<TelegramOutboxStats> {
        let inspector = self.telegram_delivery_inspector()?;
        inspector
            .outbox_stats()
            .await
            .map(TelegramOutboxStats::from)
            .map_err(async_graphql::Error::new)
    }

    async fn telegram_outbox(
        &self,
        #[graphql(name = "operationID")] operation_id: ID,
    ) -> async_graphql::Result<Option<TelegramOutboxItem>> {
        let inspector = self.telegram_delivery_inspector()?;
        let operation_id = normalize_telegram_operation_id(operation_id)?;
        inspector
            .outbox_item(&operation_id)
            .await
            .map(|item| item.map(TelegramOutboxItem::from))
            .map_err(async_graphql::Error::new)
    }

    async fn telegram_outbox_items(
        &self,
        #[graphql(name = "beforeID")] before_id: Option<ID>,
        state: Option<String>,
        limit: Option<i32>,
    ) -> async_graphql::Result<Vec<TelegramOutboxItem>> {
        let inspector = self.telegram_delivery_inspector()?;
        inspector
            .outbox_items(telegram_delivery_list_filter(before_id, state, limit)?)
            .await
            .map(|items| items.into_iter().map(TelegramOutboxItem::from).collect())
            .map_err(async_graphql::Error::new)
    }

    async fn telegram_outbox_attempts(
        &self,
        #[graphql(name = "outboxID")] outbox_id: ID,
        limit: Option<i32>,
    ) -> async_graphql::Result<Vec<TelegramOutboxAttempt>> {
        let inspector = self.telegram_delivery_inspector()?;
        inspector
            .outbox_attempts(
                parse_positive_id(outbox_id, "outboxID")?,
                telegram_delivery_limit(limit),
            )
            .await
            .map(|items| items.into_iter().map(TelegramOutboxAttempt::from).collect())
            .map_err(async_graphql::Error::new)
    }

    async fn logs(
        &self,
        after_seq: Option<ID>,
        limit: Option<i32>,
        level: Option<String>,
        search: Option<String>,
    ) -> async_graphql::Result<LogsResult> {
        let after_seq = match after_seq {
            Some(after_seq) => after_seq
                .as_str()
                .parse::<u64>()
                .map_err(|error| async_graphql::Error::new(error.to_string()))?,
            None => 0,
        };
        let limit = clamp_positive_range_i32(limit.unwrap_or(0), 50, 200);
        let items = self
            .log_inspector
            .as_deref()
            .map(|inspector| {
                inspector.logs(RuntimeLogQuery {
                    after_seq,
                    limit,
                    level: level.unwrap_or_default().trim().to_owned(),
                    search: search.unwrap_or_default().trim().to_owned(),
                })
            })
            .unwrap_or_default();
        let last_seq = items.last().map(|entry| ID(entry.seq.to_string()));
        Ok(LogsResult {
            count: items.len() as i32,
            last_seq,
            items: items.into_iter().map(LogEntry::from).collect(),
        })
    }

    async fn redis_prefixes(
        &self,
        prefix: Option<String>,
    ) -> async_graphql::Result<Vec<RuntimeRedisPrefixGroup>> {
        let Some(inspector) = self.redis_inspector.as_deref() else {
            return Err("redis client is not configured".into());
        };
        inspector
            .prefix_groups(prefix.as_deref().unwrap_or("").trim(), 1000)
            .await
            .map_err(Into::into)
    }

    async fn redis_keys(&self, pattern: String, limit: i32) -> async_graphql::Result<Vec<String>> {
        let Some(inspector) = self.redis_inspector.as_deref() else {
            return Err("redis client is not configured".into());
        };
        inspector
            .keys(
                &pattern,
                clamp_positive_range_i32(limit, 100, 1000) as usize,
            )
            .await
            .map_err(Into::into)
    }

    async fn redis_value(&self, key: String) -> async_graphql::Result<Option<RuntimeRedisValue>> {
        let Some(inspector) = self.redis_inspector.as_deref() else {
            return Err("redis client is not configured".into());
        };
        Ok(inspector.value(&key, 64 * 1024).await.unwrap_or(None))
    }

    async fn sql_read(&self, input: SqlReadInput) -> async_graphql::Result<SqlReadResult> {
        let Some(reader) = self.sql_reader.as_deref() else {
            return Err("database pool not initialized".into());
        };
        let timeout_ms = match input.timeout_ms {
            Some(timeout_ms) if timeout_ms > 0 => timeout_ms.min(self.snapshot.sql_timeout_ms),
            _ => self.snapshot.sql_timeout_ms,
        };
        let result = reader
            .read(RuntimeSqlReadRequest {
                sql: input.sql,
                args: input
                    .args
                    .unwrap_or_default()
                    .into_iter()
                    .map(|arg| arg.0)
                    .collect(),
                timeout_ms,
                row_limit: self.snapshot.sql_row_limit,
                result_bytes_limit: self.snapshot.sql_result_bytes_limit,
            })
            .await
            .map_err(async_graphql::Error::new)?;
        Ok(SqlReadResult {
            columns: result.columns,
            rows: result.rows.into_iter().map(Json).collect(),
            row_count: result.row_count,
            elapsed_ms: result.elapsed_ms,
            truncated: result.truncated,
        })
    }
}

impl RuntimeQuery {
    fn telegram_delivery_inspector(
        &self,
    ) -> async_graphql::Result<&dyn RuntimeTelegramDeliveryInspector> {
        self.telegram_delivery_inspector
            .as_deref()
            .ok_or_else(|| "telegram delivery inspector is not configured".into())
    }
}

fn users_filter_from_input(input: Option<UsersFilterInput>) -> RuntimeUsersFilter {
    let Some(input) = input else {
        return RuntimeUsersFilter {
            limit: 100,
            ..RuntimeUsersFilter::default()
        };
    };
    RuntimeUsersFilter {
        q: trim_optional(input.q),
        offset: clamp_non_negative_i32(input.offset.unwrap_or(0)),
        limit: clamp_positive_range_i32(input.limit.unwrap_or(0), 100, 500),
    }
}

fn user_lookup_from_input(
    id: Option<ID>,
    username: Option<String>,
) -> async_graphql::Result<RuntimeUserLookup> {
    if let Some(id) = id {
        return parse_id(id, "id").map(RuntimeUserLookup::Id);
    }
    let username = username.unwrap_or_default().trim().to_owned();
    if !username.is_empty() {
        return Ok(RuntimeUserLookup::Username(username));
    }
    Err("id or username required".into())
}

fn chats_filter_from_input(
    input: Option<ChatsFilterInput>,
) -> async_graphql::Result<RuntimeChatsFilter> {
    let Some(input) = input else {
        return Ok(RuntimeChatsFilter {
            limit: 100,
            ..RuntimeChatsFilter::default()
        });
    };
    Ok(RuntimeChatsFilter {
        q: trim_optional(input.q),
        offset: clamp_non_negative_i32(input.offset.unwrap_or(0)),
        limit: clamp_positive_range_i32(input.limit.unwrap_or(0), 100, 500),
        member_username: trim_nonempty(input.member_username),
        member_user_id: parse_optional_id(input.member_user_id, "memberUserID")?,
    })
}

fn safety_checks_filter_from_input(
    input: Option<SafetyChecksFilterInput>,
) -> RuntimeSafetyChecksFilter {
    let Some(input) = input else {
        return RuntimeSafetyChecksFilter {
            limit: 100,
            ..RuntimeSafetyChecksFilter::default()
        };
    };
    RuntimeSafetyChecksFilter {
        q: trim_optional(input.q),
        flagged: input.flagged,
        offset: clamp_non_negative_i32(input.offset.unwrap_or(0)),
        limit: clamp_positive_range_i32(input.limit.unwrap_or(0), 100, 1000),
    }
}

fn llm_runs_filter_from_input(
    filter: Option<LlmRunsFilterInput>,
) -> async_graphql::Result<RuntimeLlmRunsFilter> {
    let Some(filter) = filter else {
        return Ok(RuntimeLlmRunsFilter::default());
    };
    let chat_id = filter
        .chat_id
        .map(|id| {
            id.0.trim()
                .parse::<i64>()
                .map_err(|_| async_graphql::Error::new("chatID must be an integer id"))
        })
        .transpose()?;
    Ok(RuntimeLlmRunsFilter {
        kind: filter.kind.unwrap_or_default().trim().to_owned(),
        chat_id,
        errors_only: filter.errors_only.unwrap_or(false),
        q: filter.q.unwrap_or_default().trim().to_owned(),
        limit: filter.limit.unwrap_or(0),
    })
}

fn llm_requests_filter_from_input(
    input: Option<LlmRequestsFilterInput>,
) -> async_graphql::Result<RuntimeLlmRequestsFilter> {
    let Some(input) = input else {
        return Ok(RuntimeLlmRequestsFilter {
            limit: 100,
            ..RuntimeLlmRequestsFilter::default()
        });
    };
    Ok(RuntimeLlmRequestsFilter {
        q: trim_optional(input.q),
        source: trim_optional(input.source),
        model: trim_optional(input.model),
        chat_id: parse_optional_id(input.chat_id, "chatID")?,
        user_id: parse_optional_id(input.user_id, "userID")?,
        message_id: input.message_id,
        error_only: input.error_only.unwrap_or(false),
        empty_only: input.empty_only.unwrap_or(false),
        limit: clamp_positive_range_i32(input.limit.unwrap_or(0), 100, 1000),
    })
}

fn turn_outcomes_filter_from_input(
    input: Option<DialogTurnOutcomesFilterInput>,
) -> async_graphql::Result<RuntimeTurnOutcomesFilter> {
    let Some(input) = input else {
        return Ok(RuntimeTurnOutcomesFilter {
            limit: 100,
            ..RuntimeTurnOutcomesFilter::default()
        });
    };
    Ok(RuntimeTurnOutcomesFilter {
        chat_id: parse_optional_id(input.chat_id, "chatID")?,
        job_id: parse_optional_id(input.job_id, "jobID")?,
        outcome: trim_optional(input.outcome),
        limit: clamp_positive_range_i32(input.limit.unwrap_or(0), 100, 1000),
    })
}

fn routing_events_filter_from_input(
    input: Option<RoutingEventsFilterInput>,
) -> RuntimeRoutingEventsFilter {
    let Some(input) = input else {
        return RuntimeRoutingEventsFilter {
            limit: 100,
            ..RuntimeRoutingEventsFilter::default()
        };
    };
    RuntimeRoutingEventsFilter {
        q: trim_optional(input.q),
        workflow_key: trim_optional(input.workflow_key),
        event_type: trim_optional(input.event_type),
        limit: clamp_positive_range_i32(input.limit.unwrap_or(0), 100, 1000),
    }
}

fn clamp_non_negative_i32(value: i32) -> i32 {
    value.max(0)
}

fn clamp_positive_range_i32(value: i32, default: i32, max_value: i32) -> i32 {
    let mut result = if value == 0 { default } else { value };
    if result < 1 {
        result = 1;
    }
    if result > max_value {
        result = max_value;
    }
    result
}

fn taskman_jobs_filter_from_input(
    input: Option<TaskmanJobsFilterInput>,
) -> async_graphql::Result<RuntimeTaskmanJobsFilter> {
    let Some(input) = input else {
        return Ok(RuntimeTaskmanJobsFilter::default());
    };
    Ok(RuntimeTaskmanJobsFilter {
        q: trim_optional(input.q),
        status: trim_vec(input.status),
        queue: trim_vec(input.queue),
        user_id: parse_optional_id(input.user_id, "userID")?,
        chat_id: parse_optional_id(input.chat_id, "chatID")?,
        time_field: trim_optional(input.time_field),
        from: trim_optional(input.from),
        to: trim_optional(input.to),
        sort_by: trim_optional(input.sort_by),
        sort_dir: trim_optional(input.sort_dir),
        offset: input.offset.unwrap_or(0),
        limit: input.limit.unwrap_or(0),
    })
}

fn parse_optional_id(value: Option<ID>, name: &str) -> async_graphql::Result<Option<i64>> {
    value.map(|value| parse_id(value, name)).transpose()
}

fn parse_id(value: ID, name: &str) -> async_graphql::Result<i64> {
    value
        .as_str()
        .parse::<i64>()
        .map_err(|error| async_graphql::Error::new(format!("invalid {name}: {error}")))
}

fn parse_positive_id(value: ID, name: &str) -> async_graphql::Result<i64> {
    let value = parse_id(value, name)?;
    if value <= 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(value)
}

fn telegram_delivery_limit(limit: Option<i32>) -> i32 {
    clamp_positive_range_i32(
        limit.unwrap_or_default(),
        RUNTIME_TELEGRAM_DELIVERY_LIST_DEFAULT,
        RUNTIME_TELEGRAM_DELIVERY_LIST_MAX,
    )
}

fn telegram_delivery_list_filter(
    before_id: Option<ID>,
    state: Option<String>,
    limit: Option<i32>,
) -> async_graphql::Result<RuntimeTelegramDeliveryListFilter> {
    Ok(RuntimeTelegramDeliveryListFilter {
        before_id: before_id
            .map(|id| parse_positive_id(id, "beforeID"))
            .transpose()?,
        state: trim_nonempty(state).map(|state| bounded_text(&state, 64)),
        limit: telegram_delivery_limit(limit),
    })
}

fn normalize_telegram_operation_id(value: ID) -> async_graphql::Result<String> {
    let value = value.as_str().trim();
    if value.is_empty() {
        return Err("operationID is required".into());
    }
    if value.chars().count() > RUNTIME_TELEGRAM_OPERATION_ID_MAX_CHARS {
        return Err(format!(
            "operationID must be at most {RUNTIME_TELEGRAM_OPERATION_ID_MAX_CHARS} characters"
        )
        .into());
    }
    Ok(value.to_owned())
}

fn trim_optional(value: Option<String>) -> String {
    value.unwrap_or_default().trim().to_owned()
}

fn trim_vec(value: Option<Vec<String>>) -> Vec<String> {
    value
        .unwrap_or_default()
        .into_iter()
        .map(|item| item.trim().to_owned())
        .filter(|item| !item.is_empty())
        .collect()
}

fn normalize_virtual_dialog_session_id(value: String) -> async_graphql::Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("sessionID is required".into());
    }
    if value.chars().count() > RUNTIME_VIRTUAL_DIALOG_SESSION_ID_MAX_CHARS {
        return Err(format!(
            "sessionID must be at most {RUNTIME_VIRTUAL_DIALOG_SESSION_ID_MAX_CHARS} characters"
        )
        .into());
    }
    Ok(value.to_owned())
}

fn normalize_virtual_dialog_text(value: String) -> async_graphql::Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("text is required".into());
    }
    Ok(value.to_owned())
}

#[Object]
impl RuntimeMutation {
    async fn retry_telegram_outbox(
        &self,
        #[graphql(name = "operationID")] operation_id: ID,
        #[graphql(name = "acceptDuplicateRisk")] accept_duplicate_risk: Option<bool>,
    ) -> async_graphql::Result<TelegramOutboxMutationResult> {
        let inspector = self.telegram_delivery_inspector.as_deref().ok_or_else(|| {
            async_graphql::Error::new("telegram delivery inspector is not configured")
        })?;
        inspector
            .retry_outbox(RuntimeTelegramOutboxRetryRequest {
                operation_id: normalize_telegram_operation_id(operation_id)?,
                accept_duplicate_risk: accept_duplicate_risk.unwrap_or(false),
            })
            .await
            .map(TelegramOutboxMutationResult::from)
            .map_err(async_graphql::Error::new)
    }

    async fn cancel_telegram_outbox(
        &self,
        #[graphql(name = "operationID")] operation_id: ID,
    ) -> async_graphql::Result<TelegramOutboxMutationResult> {
        let inspector = self.telegram_delivery_inspector.as_deref().ok_or_else(|| {
            async_graphql::Error::new("telegram delivery inspector is not configured")
        })?;
        inspector
            .cancel_outbox(normalize_telegram_operation_id(operation_id)?)
            .await
            .map(TelegramOutboxMutationResult::from)
            .map_err(async_graphql::Error::new)
    }

    async fn restart_memory(
        &self,
        #[graphql(name = "runID")] run_id: Option<ID>,
    ) -> async_graphql::Result<MemoryRestartResult> {
        let Some(restarter) = self.memory_restarter.as_deref() else {
            return Err("memory service is not configured".into());
        };
        let run_id = parse_optional_id(run_id, "runID")?;
        if run_id.is_some_and(|id| id <= 0) {
            return Err("runID must be positive".into());
        }
        restarter
            .restart(run_id)
            .await
            .map(MemoryRestartResult::from)
            .map_err(async_graphql::Error::new)
    }

    async fn purge_gemini_explicit_caches(
        &self,
    ) -> async_graphql::Result<GeminiExplicitCachePurgeResult> {
        let Some(purger) = self.gemini_cache_purger.as_deref() else {
            return Err("gemini explicit cache purger is not configured".into());
        };
        purger
            .purge()
            .await
            .map(GeminiExplicitCachePurgeResult::from)
            .map_err(async_graphql::Error::new)
    }

    async fn start_virtual_dialog(
        &self,
        input: StartVirtualDialogInput,
    ) -> async_graphql::Result<VirtualDialog> {
        let Some(manager) = self.virtual_dialog_manager.as_deref() else {
            return Err("runtime virtual dialog manager is not configured".into());
        };
        let request = RuntimeVirtualDialogStartRequest {
            session_id: normalize_virtual_dialog_session_id(input.session_id)?,
            replace_existing: input.replace_existing.unwrap_or(false),
        };
        manager
            .start_virtual_dialog(request)
            .await
            .map(VirtualDialog::from)
            .map_err(async_graphql::Error::new)
    }

    async fn send_virtual_dialog_message(
        &self,
        input: SendVirtualDialogMessageInput,
    ) -> async_graphql::Result<VirtualDialogMessage> {
        let Some(manager) = self.virtual_dialog_manager.as_deref() else {
            return Err("runtime virtual dialog manager is not configured".into());
        };
        let request = RuntimeVirtualDialogSendRequest {
            session_id: normalize_virtual_dialog_session_id(input.session_id)?,
            text: normalize_virtual_dialog_text(input.text)?,
            tool_mode: input.tool_mode.unwrap_or_default(),
        };
        manager
            .send_virtual_dialog_message(request)
            .await
            .map(VirtualDialogMessage::from)
            .map_err(async_graphql::Error::new)
    }

    async fn delete_virtual_dialog(
        &self,
        #[graphql(name = "sessionID")] session_id: String,
    ) -> async_graphql::Result<VirtualDialogDeleteResult> {
        let Some(manager) = self.virtual_dialog_manager.as_deref() else {
            return Err("runtime virtual dialog manager is not configured".into());
        };
        let session_id = normalize_virtual_dialog_session_id(session_id)?;
        manager
            .delete_virtual_dialog(&session_id)
            .await
            .map(VirtualDialogDeleteResult::from)
            .map_err(async_graphql::Error::new)
    }
}

#[derive(Clone, SimpleObject)]
struct RuntimeState {
    log_level: String,
    dispatcher: DispatcherStats,
    cache: CacheStats,
    planner_cache: CacheStats,
}

#[derive(Clone, Default, SimpleObject)]
struct CacheStats {
    size: i32,
    capacity: i32,
    hits: String,
    misses: String,
    mem_size: String,
}

impl From<RuntimeCacheStatsData> for CacheStats {
    fn from(stats: RuntimeCacheStatsData) -> Self {
        Self {
            size: stats.size,
            capacity: stats.capacity,
            hits: stats.hits.to_string(),
            misses: stats.misses.to_string(),
            mem_size: stats.mem_size.to_string(),
        }
    }
}

#[derive(Clone, Default, SimpleObject)]
struct DispatcherStats {
    regular_queue_size: i32,
    immediate_queue_size: i32,
    processed_total: String,
    deduped_total: String,
    oldest_regular_age_ms: i32,
    oldest_immediate_age_ms: i32,
}

impl From<RuntimeDispatcherStatsData> for DispatcherStats {
    fn from(stats: RuntimeDispatcherStatsData) -> Self {
        Self {
            regular_queue_size: stats.regular_queue_size,
            immediate_queue_size: stats.immediate_queue_size,
            processed_total: stats.processed_total.to_string(),
            deduped_total: stats.deduped_total.to_string(),
            oldest_regular_age_ms: stats.oldest_regular_age_ms,
            oldest_immediate_age_ms: stats.oldest_immediate_age_ms,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct DispatcherSendFailure {
    at: String,
    #[graphql(name = "virtualID")]
    virtual_id: String,
    #[graphql(name = "chatID")]
    chat_id: ID,
    method_kind: String,
    error: String,
    class: String,
    protected: bool,
    #[graphql(name = "replyToMessageID")]
    reply_to_message_id: Option<i64>,
}

impl From<RuntimeDispatchFailureData> for DispatcherSendFailure {
    fn from(failure: RuntimeDispatchFailureData) -> Self {
        Self {
            at: failure.at,
            virtual_id: failure.virtual_id,
            chat_id: ID(failure.chat_id.to_string()),
            method_kind: failure.method_kind,
            error: failure.error,
            class: failure.class,
            protected: failure.protected,
            reply_to_message_id: failure.reply_to_message_id,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct DependencyHealth {
    status: String,
    error: Option<String>,
    latency_ms: Option<i32>,
    details: Option<Json<Value>>,
}

impl DependencyHealth {
    fn from_status(status: &str) -> Self {
        Self {
            status: status.to_owned(),
            error: None,
            latency_ms: None,
            details: None,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct HealthSnapshot {
    db: DependencyHealth,
    redis: DependencyHealth,
    dispatcher: DependencyHealth,
    ai_handler: DependencyHealth,
    shield: DependencyHealth,
    rag: DependencyHealth,
    ace_step: DependencyHealth,
    updates_queue_length: i32,
}

#[derive(Clone, SimpleObject)]
#[graphql(name = "ConfigSnapshot")]
struct ConfigSnapshot {
    log_level: String,
    web_host: String,
    web_port: i32,
    runtime_api_enabled: bool,
    runtime_api_host: String,
    runtime_api_port: i32,
    #[graphql(name = "discoveryBaseURL")]
    discovery_base_url: Option<String>,
    embedder_enabled: bool,
    #[graphql(name = "embedderURL")]
    embedder_url: Option<String>,
    shield_enabled: bool,
    #[graphql(name = "shieldEmbedderURL")]
    shield_embedder_url: Option<String>,
    shield_max_matches: i32,
    shield_vector_min_score: f64,
    shield_lexical_min_score: f64,
    shield_retrieval_timeout_seconds: i32,
    shield_history_tail_messages: i32,
    white_circle_enabled: bool,
    ace_step_enabled: bool,
    #[graphql(name = "aceStepBaseURL")]
    ace_step_base_url: Option<String>,
    dialog_provider: String,
    dialog_fallback_provider: Option<String>,
    persistent_queue_enabled: bool,
    active_draw_providers: Vec<String>,
    sql_timeout_ms: i32,
    sql_row_limit: i32,
    sql_result_bytes_limit: i32,
}

impl From<RuntimeApiGraphqlSnapshot> for ConfigSnapshot {
    fn from(snapshot: RuntimeApiGraphqlSnapshot) -> Self {
        Self {
            log_level: snapshot.log_level,
            web_host: snapshot.web_host,
            web_port: snapshot.web_port,
            runtime_api_enabled: snapshot.runtime_api_enabled,
            runtime_api_host: snapshot.runtime_api_host,
            runtime_api_port: snapshot.runtime_api_port,
            discovery_base_url: snapshot.discovery_base_url,
            embedder_enabled: snapshot.embedder_enabled,
            embedder_url: snapshot.embedder_url,
            shield_enabled: snapshot.shield_enabled,
            shield_embedder_url: snapshot.shield_embedder_url,
            shield_max_matches: snapshot.shield_max_matches,
            shield_vector_min_score: snapshot.shield_vector_min_score,
            shield_lexical_min_score: snapshot.shield_lexical_min_score,
            shield_retrieval_timeout_seconds: snapshot.shield_retrieval_timeout_seconds,
            shield_history_tail_messages: snapshot.shield_history_tail_messages,
            white_circle_enabled: snapshot.white_circle_enabled,
            ace_step_enabled: snapshot.ace_step_enabled,
            ace_step_base_url: snapshot.ace_step_base_url,
            dialog_provider: snapshot.dialog_provider,
            dialog_fallback_provider: snapshot.dialog_fallback_provider,
            persistent_queue_enabled: snapshot.persistent_queue_enabled,
            active_draw_providers: snapshot.active_draw_providers,
            sql_timeout_ms: snapshot.sql_timeout_ms,
            sql_row_limit: snapshot.sql_row_limit,
            sql_result_bytes_limit: snapshot.sql_result_bytes_limit,
        }
    }
}

#[derive(InputObject)]
#[allow(dead_code)]
struct UsersFilterInput {
    q: Option<String>,
    offset: Option<i32>,
    limit: Option<i32>,
}

#[derive(InputObject)]
#[allow(dead_code)]
struct ChatsFilterInput {
    q: Option<String>,
    offset: Option<i32>,
    limit: Option<i32>,
    member_username: Option<String>,
    #[graphql(name = "memberUserID")]
    member_user_id: Option<ID>,
}

#[derive(InputObject)]
#[allow(dead_code)]
struct SafetyChecksFilterInput {
    q: Option<String>,
    flagged: Option<bool>,
    offset: Option<i32>,
    limit: Option<i32>,
}

#[derive(InputObject)]
#[allow(dead_code)]
struct LlmRequestsFilterInput {
    q: Option<String>,
    source: Option<String>,
    model: Option<String>,
    #[graphql(name = "chatID")]
    chat_id: Option<ID>,
    #[graphql(name = "userID")]
    user_id: Option<ID>,
    #[graphql(name = "messageID")]
    message_id: Option<i32>,
    error_only: Option<bool>,
    empty_only: Option<bool>,
    limit: Option<i32>,
}

#[derive(InputObject)]
#[allow(dead_code)]
struct DialogTurnOutcomesFilterInput {
    #[graphql(name = "chatID")]
    chat_id: Option<ID>,
    #[graphql(name = "jobID")]
    job_id: Option<ID>,
    outcome: Option<String>,
    limit: Option<i32>,
}

#[derive(InputObject)]
#[allow(dead_code)]
struct LlmRunsFilterInput {
    kind: Option<String>,
    #[graphql(name = "chatID")]
    chat_id: Option<ID>,
    #[graphql(name = "errorsOnly")]
    errors_only: Option<bool>,
    q: Option<String>,
    limit: Option<i32>,
}

#[derive(InputObject)]
#[allow(dead_code)]
struct RoutingEventsFilterInput {
    q: Option<String>,
    #[graphql(name = "workflowKey")]
    workflow_key: Option<String>,
    #[graphql(name = "eventType")]
    event_type: Option<String>,
    limit: Option<i32>,
}

#[derive(InputObject)]
#[allow(dead_code)]
struct TaskmanJobsFilterInput {
    q: Option<String>,
    status: Option<Vec<String>>,
    queue: Option<Vec<String>>,
    #[graphql(name = "userID")]
    user_id: Option<ID>,
    #[graphql(name = "chatID")]
    chat_id: Option<ID>,
    time_field: Option<String>,
    from: Option<String>,
    to: Option<String>,
    sort_by: Option<String>,
    sort_dir: Option<String>,
    offset: Option<i32>,
    limit: Option<i32>,
}

#[derive(InputObject)]
#[allow(dead_code)]
struct StartVirtualDialogInput {
    #[graphql(name = "sessionID")]
    session_id: String,
    #[graphql(name = "replaceExisting")]
    replace_existing: Option<bool>,
}

#[derive(InputObject)]
#[allow(dead_code)]
struct SendVirtualDialogMessageInput {
    #[graphql(name = "sessionID")]
    session_id: String,
    text: String,
    #[graphql(name = "toolMode")]
    tool_mode: Option<RuntimeVirtualDialogToolMode>,
}

#[derive(Clone, SimpleObject)]
struct VirtualDialog {
    #[graphql(name = "sessionID")]
    session_id: String,
    #[graphql(name = "chatID")]
    chat_id: ID,
    #[graphql(name = "userID")]
    user_id: ID,
    #[graphql(name = "nextMessageID")]
    next_message_id: i32,
    message_count: i32,
    last_activity_at: Option<String>,
    expires_at: Option<String>,
    messages: Vec<VirtualDialogMessage>,
}

impl From<RuntimeVirtualDialogData> for VirtualDialog {
    fn from(data: RuntimeVirtualDialogData) -> Self {
        Self {
            session_id: data.session_id,
            chat_id: ID(data.chat_id.to_string()),
            user_id: ID(data.user_id.to_string()),
            next_message_id: data.next_message_id,
            message_count: data.message_count,
            last_activity_at: data.last_activity_at,
            expires_at: data.expires_at,
            messages: data
                .messages
                .into_iter()
                .map(VirtualDialogMessage::from)
                .collect(),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct VirtualDialogMessage {
    #[graphql(name = "messageID")]
    message_id: i32,
    role: String,
    text: String,
    at: String,
    provider: Option<String>,
    tool_mode: Option<RuntimeVirtualDialogToolMode>,
    tool_calls: Option<Json<Value>>,
}

impl From<RuntimeVirtualDialogMessageData> for VirtualDialogMessage {
    fn from(data: RuntimeVirtualDialogMessageData) -> Self {
        Self {
            message_id: data.message_id,
            role: data.role,
            text: data.text,
            at: data.at,
            provider: data.provider,
            tool_mode: data.tool_mode,
            tool_calls: data.tool_calls.map(Json),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct VirtualDialogDeleteResult {
    found: bool,
    deleted: bool,
    history_deleted: i32,
    taskman_deleted: i32,
    llm_traces_deleted: i32,
}

impl From<RuntimeVirtualDialogDeleteResultData> for VirtualDialogDeleteResult {
    fn from(data: RuntimeVirtualDialogDeleteResultData) -> Self {
        Self {
            found: data.found,
            deleted: data.deleted,
            history_deleted: data.history_deleted,
            taskman_deleted: data.taskman_deleted,
            llm_traces_deleted: data.llm_traces_deleted,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct User {
    id: ID,
    is_premium: Option<bool>,
    first_name: String,
    last_name: Option<String>,
    username: Option<String>,
    language_code: Option<String>,
    is_vip: Option<bool>,
    discovered_at: Option<String>,
    updated_at: Option<String>,
}

impl From<RuntimeUserData> for User {
    fn from(user: RuntimeUserData) -> Self {
        Self {
            id: ID(user.id.to_string()),
            is_premium: user.is_premium,
            first_name: user.first_name,
            last_name: user.last_name,
            username: user.username,
            language_code: user.language_code,
            is_vip: user.is_vip,
            discovered_at: user.discovered_at,
            updated_at: user.updated_at,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct UserConnection {
    count: i32,
    offset: i32,
    limit: i32,
    items: Vec<User>,
}

impl From<RuntimeUserConnectionData> for UserConnection {
    fn from(connection: RuntimeUserConnectionData) -> Self {
        Self {
            count: connection.count,
            offset: connection.offset,
            limit: connection.limit,
            items: connection.items.into_iter().map(User::from).collect(),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct UserDetails {
    id: ID,
    is_premium: Option<bool>,
    first_name: String,
    last_name: Option<String>,
    username: Option<String>,
    language_code: Option<String>,
    is_vip: Option<bool>,
    discovered_at: Option<String>,
    updated_at: Option<String>,
    subscription: Option<Subscription>,
    vip: Option<VipCache>,
    vip_summary: Option<VipSummary>,
    vip_events: Vec<VipEvent>,
    subscriptions: Vec<Subscription>,
}

impl From<RuntimeUserDetailsData> for UserDetails {
    fn from(details: RuntimeUserDetailsData) -> Self {
        let user = details.user;
        Self {
            id: ID(user.id.to_string()),
            is_premium: user.is_premium,
            first_name: user.first_name,
            last_name: user.last_name,
            username: user.username,
            language_code: user.language_code,
            is_vip: user.is_vip,
            discovered_at: user.discovered_at,
            updated_at: user.updated_at,
            subscription: details.subscription.map(Subscription::from),
            vip: details.vip.map(VipCache::from),
            vip_summary: details.vip_summary.map(VipSummary::from),
            vip_events: details.vip_events.into_iter().map(VipEvent::from).collect(),
            subscriptions: details
                .subscriptions
                .into_iter()
                .map(Subscription::from)
                .collect(),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct Subscription {
    id: ID,
    #[graphql(name = "userID")]
    user_id: ID,
    #[graphql(name = "telegramPaymentChargeID")]
    telegram_payment_charge_id: String,
    #[graphql(name = "providerPaymentChargeID")]
    provider_payment_charge_id: String,
    expires_at: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
    canceled_at: Option<String>,
    refunded_at: Option<String>,
    status: String,
}

impl From<RuntimeSubscriptionData> for Subscription {
    fn from(subscription: RuntimeSubscriptionData) -> Self {
        Self {
            id: ID(subscription.id.to_string()),
            user_id: ID(subscription.user_id.to_string()),
            telegram_payment_charge_id: subscription.telegram_payment_charge_id,
            provider_payment_charge_id: subscription.provider_payment_charge_id,
            expires_at: subscription.expires_at,
            created_at: subscription.created_at,
            updated_at: subscription.updated_at,
            canceled_at: subscription.canceled_at,
            refunded_at: subscription.refunded_at,
            status: subscription.status,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct VipCache {
    #[graphql(name = "userID")]
    user_id: ID,
    is_vip: bool,
    expires_at: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

impl From<RuntimeVipCacheData> for VipCache {
    fn from(vip: RuntimeVipCacheData) -> Self {
        Self {
            user_id: ID(vip.user_id.to_string()),
            is_vip: vip.is_vip,
            expires_at: vip.expires_at,
            created_at: vip.created_at,
            updated_at: vip.updated_at,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct VipSummary {
    active: bool,
    has_history: bool,
    expires_at: Option<String>,
    remaining_seconds: String,
    remaining_days: i32,
    #[graphql(name = "latestEventID")]
    latest_event_id: Option<ID>,
    latest_event_type: Option<String>,
    latest_reason: Option<String>,
    latest_created_at: Option<String>,
}

impl From<RuntimeVipSummaryData> for VipSummary {
    fn from(summary: RuntimeVipSummaryData) -> Self {
        Self {
            active: summary.active,
            has_history: summary.has_history,
            expires_at: summary.expires_at,
            remaining_seconds: summary.remaining_seconds,
            remaining_days: summary.remaining_days,
            latest_event_id: summary.latest_event_id.map(|id| ID(id.to_string())),
            latest_event_type: summary.latest_event_type,
            latest_reason: summary.latest_reason,
            latest_created_at: summary.latest_created_at,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct VipEvent {
    id: ID,
    event_type: String,
    delta_seconds: String,
    delta_days: f64,
    effective_expires_at: Option<String>,
    #[graphql(name = "actorUserID")]
    actor_user_id: Option<ID>,
    actor_label: Option<String>,
    reason: Option<String>,
    created_at: Option<String>,
    #[graphql(name = "subscriptionID")]
    subscription_id: Option<ID>,
    #[graphql(name = "telegramPaymentChargeID")]
    telegram_payment_charge_id: Option<String>,
    #[graphql(name = "providerPaymentChargeID")]
    provider_payment_charge_id: Option<String>,
    subscription_status: Option<String>,
}

impl From<RuntimeVipEventData> for VipEvent {
    fn from(event: RuntimeVipEventData) -> Self {
        Self {
            id: ID(event.id.to_string()),
            event_type: event.event_type,
            delta_seconds: event.delta_seconds,
            delta_days: event.delta_days,
            effective_expires_at: event.effective_expires_at,
            actor_user_id: event.actor_user_id.map(|id| ID(id.to_string())),
            actor_label: event.actor_label,
            reason: event.reason,
            created_at: event.created_at,
            subscription_id: event.subscription_id.map(|id| ID(id.to_string())),
            telegram_payment_charge_id: event.telegram_payment_charge_id,
            provider_payment_charge_id: event.provider_payment_charge_id,
            subscription_status: event.subscription_status,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct Chat {
    id: ID,
    #[graphql(name = "type")]
    chat_type: String,
    title: Option<String>,
    username: Option<String>,
    first_name: Option<String>,
    last_name: Option<String>,
    is_forum: Option<bool>,
    description: Option<String>,
    invite_link: Option<String>,
    discovered_at: Option<String>,
    updated_at: Option<String>,
}

impl From<RuntimeChatData> for Chat {
    fn from(chat: RuntimeChatData) -> Self {
        Self {
            id: ID(chat.id.to_string()),
            chat_type: chat.chat_type,
            title: chat.title,
            username: chat.username,
            first_name: chat.first_name,
            last_name: chat.last_name,
            is_forum: chat.is_forum,
            description: chat.description,
            invite_link: chat.invite_link,
            discovered_at: chat.discovered_at,
            updated_at: chat.updated_at,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct ChatConnection {
    count: i32,
    offset: i32,
    limit: i32,
    items: Vec<Chat>,
}

impl From<RuntimeChatConnectionData> for ChatConnection {
    fn from(connection: RuntimeChatConnectionData) -> Self {
        Self {
            count: connection.count,
            offset: connection.offset,
            limit: connection.limit,
            items: connection.items.into_iter().map(Chat::from).collect(),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct ChatMember {
    #[graphql(name = "chatID")]
    chat_id: ID,
    #[graphql(name = "userID")]
    user_id: ID,
    status: String,
    is_anonymous: Option<bool>,
    custom_title: Option<String>,
    can_be_edited: Option<bool>,
    can_manage_chat: Option<bool>,
    can_delete_messages: Option<bool>,
    can_manage_video_chats: Option<bool>,
    can_restrict_members: Option<bool>,
    can_promote_members: Option<bool>,
    can_change_info: Option<bool>,
    can_invite_users: Option<bool>,
    can_post_messages: Option<bool>,
    can_edit_messages: Option<bool>,
    can_pin_messages: Option<bool>,
    can_manage_topics: Option<bool>,
    created_at: Option<String>,
    updated_at: Option<String>,
    last_message_at: Option<String>,
}

impl From<RuntimeChatMemberData> for ChatMember {
    fn from(member: RuntimeChatMemberData) -> Self {
        Self {
            chat_id: ID(member.chat_id.to_string()),
            user_id: ID(member.user_id.to_string()),
            status: member.status,
            is_anonymous: member.is_anonymous,
            custom_title: member.custom_title,
            can_be_edited: member.can_be_edited,
            can_manage_chat: member.can_manage_chat,
            can_delete_messages: member.can_delete_messages,
            can_manage_video_chats: member.can_manage_video_chats,
            can_restrict_members: member.can_restrict_members,
            can_promote_members: member.can_promote_members,
            can_change_info: member.can_change_info,
            can_invite_users: member.can_invite_users,
            can_post_messages: member.can_post_messages,
            can_edit_messages: member.can_edit_messages,
            can_pin_messages: member.can_pin_messages,
            can_manage_topics: member.can_manage_topics,
            created_at: member.created_at,
            updated_at: member.updated_at,
            last_message_at: member.last_message_at,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct ChatMemberWithUser {
    member: ChatMember,
    user: Option<User>,
}

impl From<RuntimeChatMemberWithUserData> for ChatMemberWithUser {
    fn from(value: RuntimeChatMemberWithUserData) -> Self {
        Self {
            member: ChatMember::from(value.member),
            user: value.user.map(User::from),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct TaskmanJobListResult {
    total: i32,
    offset: i32,
    limit: i32,
    summary: TaskmanJobSummary,
    items: Vec<TaskmanJobListEntry>,
}

impl TaskmanJobListResult {
    fn empty() -> Self {
        Self {
            total: 0,
            offset: 0,
            limit: 0,
            summary: TaskmanJobSummary::default(),
            items: Vec::new(),
        }
    }
}

impl From<RuntimeTaskmanJobListResultData> for TaskmanJobListResult {
    fn from(result: RuntimeTaskmanJobListResultData) -> Self {
        Self {
            total: result.total,
            offset: result.offset,
            limit: result.limit,
            summary: TaskmanJobSummary {
                by_status: Json(result.summary.by_status),
                by_queue: Json(result.summary.by_queue),
            },
            items: result
                .items
                .into_iter()
                .map(TaskmanJobListEntry::from)
                .collect(),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct TaskmanJobSummary {
    by_status: Json<Value>,
    by_queue: Json<Value>,
}

impl Default for TaskmanJobSummary {
    fn default() -> Self {
        Self {
            by_status: Json(json!({})),
            by_queue: Json(json!({})),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct TaskmanJobListEntry {
    id: ID,
    queue_name: String,
    priority: i32,
    title: String,
    job_type: String,
    status: String,
    #[graphql(name = "userID")]
    user_id: ID,
    #[graphql(name = "chatID")]
    chat_id: ID,
    #[graphql(name = "triggerMessageID")]
    trigger_message_id: i32,
    #[graphql(name = "threadMessageID")]
    thread_message_id: Option<i32>,
    #[graphql(name = "progressMessageID")]
    progress_message_id: Option<i32>,
    #[graphql(name = "resultMessageID")]
    result_message_id: Option<i32>,
    #[graphql(name = "workerID")]
    worker_id: Option<String>,
    created_at: String,
    started_at: Option<String>,
    completed_at: Option<String>,
    error_message: Option<String>,
    processing_timeout_seconds: i32,
    prompt_hash: Option<String>,
    estimated_processing_time: Option<i32>,
    actual_processing_time: Option<i32>,
    preview: Option<String>,
}

impl From<RuntimeTaskmanJobListEntryData> for TaskmanJobListEntry {
    fn from(entry: RuntimeTaskmanJobListEntryData) -> Self {
        Self {
            id: ID(entry.id.to_string()),
            queue_name: entry.queue_name,
            priority: entry.priority,
            title: entry.title,
            job_type: entry.job_type,
            status: entry.status,
            user_id: ID(entry.user_id.to_string()),
            chat_id: ID(entry.chat_id.to_string()),
            trigger_message_id: entry.trigger_message_id,
            thread_message_id: entry.thread_message_id,
            progress_message_id: entry.progress_message_id,
            result_message_id: entry.result_message_id,
            worker_id: entry.worker_id,
            created_at: entry.created_at,
            started_at: entry.started_at,
            completed_at: entry.completed_at,
            error_message: entry.error_message,
            processing_timeout_seconds: entry.processing_timeout_seconds,
            prompt_hash: entry.prompt_hash,
            estimated_processing_time: entry.estimated_processing_time,
            actual_processing_time: entry.actual_processing_time,
            preview: entry.preview,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct TaskmanJobDetails {
    job: TaskmanJob,
    messages: Vec<TaskmanJobMessage>,
    events: Option<Json<Value>>,
}

impl From<RuntimeTaskmanJobDetailsData> for TaskmanJobDetails {
    fn from(details: RuntimeTaskmanJobDetailsData) -> Self {
        Self {
            job: TaskmanJob::from(details.job),
            messages: details
                .messages
                .into_iter()
                .map(TaskmanJobMessage::from)
                .collect(),
            events: details.events.map(Json),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct TaskmanJob {
    id: ID,
    queue_name: String,
    priority: i32,
    title: String,
    payload: Option<Json<Value>>,
    status: String,
    #[graphql(name = "userID")]
    user_id: ID,
    #[graphql(name = "chatID")]
    chat_id: ID,
    #[graphql(name = "triggerMessageID")]
    trigger_message_id: i32,
    #[graphql(name = "threadMessageID")]
    thread_message_id: Option<i32>,
    #[graphql(name = "progressMessageID")]
    progress_message_id: Option<i32>,
    #[graphql(name = "resultMessageID")]
    result_message_id: Option<i32>,
    #[graphql(name = "workerID")]
    worker_id: Option<String>,
    created_at: String,
    started_at: Option<String>,
    completed_at: Option<String>,
    error_message: Option<String>,
    processing_timeout_seconds: i32,
    prompt_hash: Option<String>,
    estimated_processing_time: Option<i32>,
    actual_processing_time: Option<i32>,
}

impl From<RuntimeTaskmanJobData> for TaskmanJob {
    fn from(job: RuntimeTaskmanJobData) -> Self {
        Self {
            id: ID(job.id.to_string()),
            queue_name: job.queue_name,
            priority: job.priority,
            title: job.title,
            payload: job.payload.map(Json),
            status: job.status,
            user_id: ID(job.user_id.to_string()),
            chat_id: ID(job.chat_id.to_string()),
            trigger_message_id: job.trigger_message_id,
            thread_message_id: job.thread_message_id,
            progress_message_id: job.progress_message_id,
            result_message_id: job.result_message_id,
            worker_id: job.worker_id,
            created_at: job.created_at,
            started_at: job.started_at,
            completed_at: job.completed_at,
            error_message: job.error_message,
            processing_timeout_seconds: job.processing_timeout_seconds,
            prompt_hash: job.prompt_hash,
            estimated_processing_time: job.estimated_processing_time,
            actual_processing_time: job.actual_processing_time,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct TaskmanJobMessage {
    id: ID,
    #[graphql(name = "jobID")]
    job_id: ID,
    message_type: String,
    #[graphql(name = "chatID")]
    chat_id: ID,
    #[graphql(name = "messageID")]
    message_id: i32,
    created_at: String,
    status: String,
}

impl From<RuntimeTaskmanJobMessageData> for TaskmanJobMessage {
    fn from(message: RuntimeTaskmanJobMessageData) -> Self {
        Self {
            id: ID(message.id.to_string()),
            job_id: ID(message.job_id.to_string()),
            message_type: message.message_type,
            chat_id: ID(message.chat_id.to_string()),
            message_id: message.message_id,
            created_at: message.created_at,
            status: message.status,
        }
    }
}

#[derive(Clone, Default, SimpleObject)]
struct TaskmanDiagnostics {
    running: bool,
    active: i32,
    started1m: i32,
    completed1m: i32,
    worker_count: i32,
    queue_signal_count: i32,
    slow_job_count: i32,
    queues: Vec<TaskmanQueueDiagnostics>,
}

impl From<RuntimeTaskmanDiagnosticsData> for TaskmanDiagnostics {
    fn from(diagnostics: RuntimeTaskmanDiagnosticsData) -> Self {
        Self {
            running: diagnostics.running,
            active: diagnostics.active,
            started1m: diagnostics.started1m,
            completed1m: diagnostics.completed1m,
            worker_count: diagnostics.worker_count,
            queue_signal_count: diagnostics.queue_signal_count,
            slow_job_count: diagnostics.slow_job_count,
            queues: diagnostics
                .queues
                .into_iter()
                .map(TaskmanQueueDiagnostics::from)
                .collect(),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct TaskmanQueueDiagnostics {
    queue_name: String,
    priority: i32,
    pending: i32,
    pending_or_higher: i32,
    active: i32,
    worker_count: i32,
    #[graphql(name = "etaSeconds")]
    eta_seconds: i32,
}

impl From<RuntimeTaskmanQueueDiagnosticsData> for TaskmanQueueDiagnostics {
    fn from(queue: RuntimeTaskmanQueueDiagnosticsData) -> Self {
        Self {
            queue_name: queue.queue_name,
            priority: queue.priority,
            pending: queue.pending,
            pending_or_higher: queue.pending_or_higher,
            active: queue.active,
            worker_count: queue.worker_count,
            eta_seconds: queue.eta_seconds,
        }
    }
}

#[derive(Clone, Default, SimpleObject)]
struct SafetyCheckConnection {
    count: i32,
    offset: i32,
    limit: i32,
    items: Vec<SafetyCheck>,
}

impl From<RuntimeSafetyCheckConnectionData> for SafetyCheckConnection {
    fn from(connection: RuntimeSafetyCheckConnectionData) -> Self {
        Self {
            count: connection.count,
            offset: connection.offset,
            limit: connection.limit,
            items: connection
                .items
                .into_iter()
                .map(SafetyCheck::from)
                .collect(),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct SafetyCheck {
    id: ID,
    created_at: String,
    source: String,
    flow: Option<String>,
    mode: Option<String>,
    #[graphql(name = "chatID")]
    chat_id: Option<ID>,
    #[graphql(name = "threadID")]
    thread_id: Option<i32>,
    #[graphql(name = "messageID")]
    message_id: Option<i32>,
    #[graphql(name = "userID")]
    user_id: Option<ID>,
    #[graphql(name = "deploymentID")]
    deployment_id: String,
    #[graphql(name = "externalSessionID")]
    external_session_id: Option<String>,
    request_messages: Option<Json<Value>>,
    flagged: Option<bool>,
    #[graphql(name = "internalSessionID")]
    internal_session_id: Option<String>,
    policies: Option<Json<Value>>,
    #[graphql(name = "responseJSON")]
    response_json: Option<Json<Value>>,
    duration_ms: i32,
    error: Option<String>,
}

impl From<RuntimeSafetyCheckData> for SafetyCheck {
    fn from(check: RuntimeSafetyCheckData) -> Self {
        Self {
            id: ID(check.id.to_string()),
            created_at: check.created_at,
            source: check.source,
            flow: check.flow,
            mode: check.mode,
            chat_id: check.chat_id.map(|id| ID(id.to_string())),
            thread_id: check.thread_id,
            message_id: check.message_id,
            user_id: check.user_id.map(|id| ID(id.to_string())),
            deployment_id: check.deployment_id,
            external_session_id: check.external_session_id,
            request_messages: check.request_messages.map(Json),
            flagged: check.flagged,
            internal_session_id: check.internal_session_id,
            policies: check.policies.map(Json),
            response_json: check.response_json.map(Json),
            duration_ms: check.duration_ms,
            error: check.error,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct LlmRequest {
    id: ID,
    at: String,
    provider: Option<String>,
    request_kind: Option<String>,
    source: String,
    mode: Option<String>,
    flow: Option<String>,
    iteration: i32,
    model: Option<String>,
    chat: LlmRequestChat,
    user: LlmRequestUser,
    message: LlmRequestMessage,
    gen_config: LlmGenConfig,
    docs: Option<Json<Value>>,
    messages: Option<Json<Value>>,
    raw_request: Option<Json<Value>>,
    resolved_cache_content: Option<Json<Value>>,
    raw_response: Option<Json<Value>>,
    transport: Option<Json<Value>>,
    inference_params: Option<Json<Value>>,
    usage: Option<Json<Value>>,
    timings: Option<Json<Value>>,
    prompt_chars: i32,
    prompt_messages: i32,
    docs_chars: i32,
    duration_ms: i32,
    result: LlmRequestResult,
}

impl From<RuntimeLlmRequestData> for LlmRequest {
    fn from(request: RuntimeLlmRequestData) -> Self {
        Self {
            id: ID(request.id.to_string()),
            at: request.at,
            provider: request.provider,
            request_kind: request.request_kind,
            source: request.source,
            mode: request.mode,
            flow: request.flow,
            iteration: request.iteration,
            model: request.model,
            chat: LlmRequestChat::from(request.chat),
            user: LlmRequestUser::from(request.user),
            message: LlmRequestMessage::from(request.message),
            gen_config: LlmGenConfig::from(request.gen_config),
            docs: request.docs.map(Json),
            messages: request.messages.map(Json),
            raw_request: request.raw_request.map(Json),
            resolved_cache_content: request.resolved_cache_content.map(Json),
            raw_response: request.raw_response.map(Json),
            transport: request.transport.map(Json),
            inference_params: request.inference_params.map(Json),
            usage: request.usage.map(Json),
            timings: request.timings.map(Json),
            prompt_chars: request.prompt_chars,
            prompt_messages: request.prompt_messages,
            docs_chars: request.docs_chars,
            duration_ms: request.duration_ms,
            result: LlmRequestResult::from(request.result),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct DialogTurnOutcome {
    id: ID,
    at: String,
    #[graphql(name = "jobID")]
    job_id: ID,
    queue_name: String,
    #[graphql(name = "chatID")]
    chat_id: Option<ID>,
    #[graphql(name = "threadID")]
    thread_id: Option<i32>,
    #[graphql(name = "userID")]
    user_id: Option<ID>,
    #[graphql(name = "triggerMessageID")]
    trigger_message_id: Option<i32>,
    attempt: i32,
    outcome: String,
    reason: Option<String>,
    provider: Option<String>,
    model: Option<String>,
    elapsed_ms: Option<i32>,
    budget_ms: Option<i32>,
    user_signal: Option<String>,
    sent_message_parts: Option<i32>,
    #[graphql(name = "sideEffectTicketID")]
    side_effect_ticket_id: Option<ID>,
    detail: Json<Value>,
}

impl From<RuntimeTurnOutcomeData> for DialogTurnOutcome {
    fn from(outcome: RuntimeTurnOutcomeData) -> Self {
        Self {
            id: ID(outcome.id.to_string()),
            at: outcome.at,
            job_id: ID(outcome.job_id.to_string()),
            queue_name: outcome.queue_name,
            chat_id: outcome.chat_id.map(|id| ID(id.to_string())),
            thread_id: outcome.thread_id,
            user_id: outcome.user_id.map(|id| ID(id.to_string())),
            trigger_message_id: outcome.trigger_message_id,
            attempt: outcome.attempt,
            outcome: outcome.outcome,
            reason: outcome.reason,
            provider: outcome.provider,
            model: outcome.model,
            elapsed_ms: outcome.elapsed_ms,
            budget_ms: outcome.budget_ms,
            user_signal: outcome.user_signal,
            sent_message_parts: outcome.sent_message_parts,
            side_effect_ticket_id: outcome.side_effect_ticket_id.map(|id| ID(id.to_string())),
            detail: Json(outcome.detail),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct RoutingEvent {
    id: ID,
    at: String,
    severity: String,
    event_type: String,
    workflow_key: String,
    #[graphql(name = "providerID")]
    provider_id: Option<ID>,
    #[graphql(name = "modelID")]
    model_id: Option<ID>,
    queue_name: Option<String>,
    #[graphql(name = "jobID")]
    job_id: Option<ID>,
    #[graphql(name = "chatID")]
    chat_id: Option<ID>,
    #[graphql(name = "threadID")]
    thread_id: Option<i32>,
    #[graphql(name = "messageID")]
    message_id: Option<i32>,
    dedupe_key: String,
    summary: String,
    detail: Json<Value>,
}

impl From<RuntimeRoutingEventData> for RoutingEvent {
    fn from(event: RuntimeRoutingEventData) -> Self {
        Self {
            id: ID(event.id.to_string()),
            at: event.at,
            severity: event.severity,
            event_type: event.event_type,
            workflow_key: event.workflow_key,
            provider_id: event.provider_id.map(|id| ID(id.to_string())),
            model_id: event.model_id.map(|id| ID(id.to_string())),
            queue_name: event.queue_name,
            job_id: event.job_id.map(|id| ID(id.to_string())),
            chat_id: event.chat_id.map(|id| ID(id.to_string())),
            thread_id: event.thread_id,
            message_id: event.message_id,
            dedupe_key: event.dedupe_key,
            summary: event.summary,
            detail: Json(event.detail),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct LlmRequestChat {
    #[graphql(name = "chatID")]
    chat_id: ID,
    #[graphql(name = "threadID")]
    thread_id: Option<i32>,
    chat_title: Option<String>,
}

impl From<RuntimeLlmRequestChatData> for LlmRequestChat {
    fn from(chat: RuntimeLlmRequestChatData) -> Self {
        Self {
            chat_id: ID(chat.chat_id.to_string()),
            thread_id: chat.thread_id,
            chat_title: chat.chat_title,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct LlmRequestUser {
    #[graphql(name = "userID")]
    user_id: ID,
    full_name: Option<String>,
}

impl From<RuntimeLlmRequestUserData> for LlmRequestUser {
    fn from(user: RuntimeLlmRequestUserData) -> Self {
        Self {
            user_id: ID(user.user_id.to_string()),
            full_name: user.full_name,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct LlmRequestMessage {
    #[graphql(name = "messageID")]
    message_id: i32,
}

impl From<RuntimeLlmRequestMessageData> for LlmRequestMessage {
    fn from(message: RuntimeLlmRequestMessageData) -> Self {
        Self {
            message_id: message.message_id,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct LlmGenConfig {
    max_output_tokens: i32,
    temperature: f64,
    top_p: f64,
    top_k: i32,
    safety_settings: Option<Json<Value>>,
}

impl From<RuntimeLlmGenConfigData> for LlmGenConfig {
    fn from(config: RuntimeLlmGenConfigData) -> Self {
        Self {
            max_output_tokens: config.max_output_tokens,
            temperature: config.temperature,
            top_p: config.top_p,
            top_k: config.top_k,
            safety_settings: config.safety_settings.map(Json),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct LlmRequestResult {
    duration_ms: i32,
    error: Option<String>,
    response_text_preview: Option<String>,
}

impl From<RuntimeLlmRequestResultData> for LlmRequestResult {
    fn from(result: RuntimeLlmRequestResultData) -> Self {
        Self {
            duration_ms: result.duration_ms,
            error: result.error,
            response_text_preview: result.response_text_preview,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct LlmAnalytics {
    range: String,
    bucket: String,
    since: String,
    totals: LlmAnalyticsTotals,
    series: Vec<LlmAnalyticsSeriesPoint>,
    top_chats: Vec<LlmAnalyticsTopChat>,
    models: Vec<LlmAnalyticsModelStat>,
    providers: Vec<LlmAnalyticsProviderStat>,
    stage_metrics: Vec<LlmAnalyticsStageMetric>,
    runtime_jobs: Vec<RuntimeJobAnalyticsStat>,
    runtime_jobs_error: Option<String>,
    ai_farm_capacity: Option<AifarmCapacitySnapshot>,
}

impl From<RuntimeLlmAnalyticsData> for LlmAnalytics {
    fn from(summary: RuntimeLlmAnalyticsData) -> Self {
        Self {
            range: summary.range,
            bucket: summary.bucket,
            since: summary.since,
            totals: LlmAnalyticsTotals::from(summary.totals),
            series: summary
                .series
                .into_iter()
                .map(LlmAnalyticsSeriesPoint::from)
                .collect(),
            top_chats: summary
                .top_chats
                .into_iter()
                .map(LlmAnalyticsTopChat::from)
                .collect(),
            models: summary
                .models
                .into_iter()
                .map(LlmAnalyticsModelStat::from)
                .collect(),
            providers: summary
                .providers
                .into_iter()
                .map(LlmAnalyticsProviderStat::from)
                .collect(),
            stage_metrics: summary
                .stage_metrics
                .into_iter()
                .map(LlmAnalyticsStageMetric::from)
                .collect(),
            runtime_jobs: summary
                .runtime_jobs
                .into_iter()
                .map(RuntimeJobAnalyticsStat::from)
                .collect(),
            runtime_jobs_error: summary.runtime_jobs_error,
            ai_farm_capacity: summary.ai_farm_capacity.map(AifarmCapacitySnapshot::from),
        }
    }
}

#[derive(Clone, Default, SimpleObject)]
struct LlmAnalyticsTotals {
    total_count: i32,
    error_count: i32,
    avg_duration_ms: i32,
}

impl From<RuntimeLlmAnalyticsTotalsData> for LlmAnalyticsTotals {
    fn from(totals: RuntimeLlmAnalyticsTotalsData) -> Self {
        Self {
            total_count: totals.total_count,
            error_count: totals.error_count,
            avg_duration_ms: totals.avg_duration_ms,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct LlmAnalyticsSeriesPoint {
    ts: String,
    total_count: i32,
    error_count: i32,
    avg_duration_ms: i32,
}

impl From<RuntimeLlmAnalyticsSeriesPointData> for LlmAnalyticsSeriesPoint {
    fn from(point: RuntimeLlmAnalyticsSeriesPointData) -> Self {
        Self {
            ts: point.ts,
            total_count: point.total_count,
            error_count: point.error_count,
            avg_duration_ms: point.avg_duration_ms,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct LlmAnalyticsTopChat {
    #[graphql(name = "chatID")]
    chat_id: ID,
    title: Option<String>,
    username: Option<String>,
    request_count: i32,
}

impl From<RuntimeLlmAnalyticsTopChatData> for LlmAnalyticsTopChat {
    fn from(chat: RuntimeLlmAnalyticsTopChatData) -> Self {
        Self {
            chat_id: ID(chat.chat_id.to_string()),
            title: chat.title,
            username: chat.username,
            request_count: chat.request_count,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct LlmAnalyticsModelStat {
    model: String,
    request_count: i32,
    error_count: i32,
    avg_duration_ms: i32,
    p50_duration_ms: i32,
    p95_duration_ms: i32,
}

impl From<RuntimeLlmAnalyticsModelStatData> for LlmAnalyticsModelStat {
    fn from(stat: RuntimeLlmAnalyticsModelStatData) -> Self {
        Self {
            model: stat.model,
            request_count: stat.request_count,
            error_count: stat.error_count,
            avg_duration_ms: stat.avg_duration_ms,
            p50_duration_ms: stat.p50_duration_ms,
            p95_duration_ms: stat.p95_duration_ms,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct LlmAnalyticsProviderStat {
    provider: String,
    source: String,
    request_count: i32,
    error_count: i32,
    avg_duration_ms: i32,
    p50_duration_ms: i32,
    p95_duration_ms: i32,
}

impl From<RuntimeLlmAnalyticsProviderStatData> for LlmAnalyticsProviderStat {
    fn from(stat: RuntimeLlmAnalyticsProviderStatData) -> Self {
        Self {
            provider: stat.provider,
            source: stat.source,
            request_count: stat.request_count,
            error_count: stat.error_count,
            avg_duration_ms: stat.avg_duration_ms,
            p50_duration_ms: stat.p50_duration_ms,
            p95_duration_ms: stat.p95_duration_ms,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct LlmAnalyticsStageMetric {
    stage: String,
    source: String,
    request_count: i32,
    error_count: i32,
    avg_duration_ms: i32,
    p50_duration_ms: i32,
    p95_duration_ms: i32,
    avg_iteration: i32,
    max_iteration: i32,
}

impl From<RuntimeLlmAnalyticsStageMetricData> for LlmAnalyticsStageMetric {
    fn from(metric: RuntimeLlmAnalyticsStageMetricData) -> Self {
        Self {
            stage: metric.stage,
            source: metric.source,
            request_count: metric.request_count,
            error_count: metric.error_count,
            avg_duration_ms: metric.avg_duration_ms,
            p50_duration_ms: metric.p50_duration_ms,
            p95_duration_ms: metric.p95_duration_ms,
            avg_iteration: metric.avg_iteration,
            max_iteration: metric.max_iteration,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct RuntimeJobAnalyticsStat {
    job_type: String,
    queue_name: String,
    provider: String,
    created_count: i32,
    completed_count: i32,
    failed_count: i32,
    avg_wait_ms: i32,
    p95_wait_ms: i32,
    avg_processing_ms: i32,
    p95_processing_ms: i32,
}

impl From<RuntimeJobAnalyticsStatData> for RuntimeJobAnalyticsStat {
    fn from(stat: RuntimeJobAnalyticsStatData) -> Self {
        Self {
            job_type: stat.job_type,
            queue_name: stat.queue_name,
            provider: stat.provider,
            created_count: stat.created_count,
            completed_count: stat.completed_count,
            failed_count: stat.failed_count,
            avg_wait_ms: stat.avg_wait_ms,
            p95_wait_ms: stat.p95_wait_ms,
            avg_processing_ms: stat.avg_processing_ms,
            p95_processing_ms: stat.p95_processing_ms,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct AifarmCapacitySnapshot {
    service: String,
    max_concurrent_jobs: i32,
    running: i32,
    queued: i32,
    available: i32,
    locked: bool,
    ready: bool,
    observed_at: String,
    error: Option<String>,
}

impl From<RuntimeAifarmCapacitySnapshotData> for AifarmCapacitySnapshot {
    fn from(snapshot: RuntimeAifarmCapacitySnapshotData) -> Self {
        Self {
            service: snapshot.service,
            max_concurrent_jobs: snapshot.max_concurrent_jobs,
            running: snapshot.running,
            queued: snapshot.queued,
            available: snapshot.available,
            locked: snapshot.locked,
            ready: snapshot.ready,
            observed_at: snapshot.observed_at,
            error: snapshot.error,
        }
    }
}

#[derive(Clone, Default, SimpleObject)]
struct UpdatesRuntime {
    active: i32,
    state_active: i32,
    handle_active: i32,
    queue_len: i32,
    queue_error: Option<String>,
    started1m: i32,
    completed1m: i32,
    timeouts1m: i32,
    oldest_active_ms: i32,
    last_stall_at: Option<String>,
    tasks: Vec<UpdatesTask>,
    gates: Option<IngestionGates>,
    stream_len: i64,
    stream_group_lag: i64,
    stream_pending: i64,
    oldest_unmaterialized_ms: i64,
    ingress_used_memory_bytes: i64,
    ingress_maxmemory_bytes: i64,
    ingress_maxmemory_policy: String,
    ingress_aof_enabled: bool,
    ingress_aof_current_size_bytes: i64,
    ingress_aof_rewrite_in_progress: bool,
    ingress_aof_last_write_status: String,
    ingress_aof_last_rewrite_status: String,
    materializer_supervisor_running: bool,
    materializer_lease_held: bool,
    materializer_supervisor_restarts: i64,
    materializer_batch_rows: i32,
    materializer_batch_bytes: i64,
    materializer_batch_fill_ratio: f64,
    bulk_transaction_latency_ms: i64,
    materialized_batches: i64,
    inbox_insert_statements: i64,
    quarantine_insert_statements: i64,
    materialized_inserted: i64,
    materialized_duplicates: i64,
    materialized_conflicted: i64,
    materialized_quarantined: i64,
    materializer_reclaims: i64,
    ack_delete_mismatches: i64,
    materializer_db_failures: i64,
    materializer_redis_failures: i64,
    postgres_pending: i64,
    postgres_retry_wait: i64,
    postgres_dead_letter: i64,
    event_to_redis_avg_ms: i64,
    redis_to_postgres_avg_ms: i64,
    materialization_to_claim_avg_ms: i64,
    claim_to_taskman_avg_ms: i64,
}

impl From<RuntimeUpdatesRuntimeData> for UpdatesRuntime {
    fn from(runtime: RuntimeUpdatesRuntimeData) -> Self {
        Self {
            active: runtime.active,
            state_active: runtime.state_active,
            handle_active: runtime.handle_active,
            queue_len: runtime.queue_len,
            queue_error: runtime.queue_error,
            started1m: runtime.started1m,
            completed1m: runtime.completed1m,
            timeouts1m: runtime.timeouts1m,
            oldest_active_ms: runtime.oldest_active_ms,
            last_stall_at: runtime.last_stall_at,
            tasks: runtime.tasks.into_iter().map(UpdatesTask::from).collect(),
            gates: runtime.gates.map(IngestionGates::from),
            stream_len: runtime.stream_len,
            stream_group_lag: runtime.stream_group_lag,
            stream_pending: runtime.stream_pending,
            oldest_unmaterialized_ms: runtime.oldest_unmaterialized_ms,
            ingress_used_memory_bytes: runtime.ingress_used_memory_bytes,
            ingress_maxmemory_bytes: runtime.ingress_maxmemory_bytes,
            ingress_maxmemory_policy: runtime.ingress_maxmemory_policy,
            ingress_aof_enabled: runtime.ingress_aof_enabled,
            ingress_aof_current_size_bytes: runtime.ingress_aof_current_size_bytes,
            ingress_aof_rewrite_in_progress: runtime.ingress_aof_rewrite_in_progress,
            ingress_aof_last_write_status: runtime.ingress_aof_last_write_status,
            ingress_aof_last_rewrite_status: runtime.ingress_aof_last_rewrite_status,
            materializer_supervisor_running: runtime.materializer_supervisor_running,
            materializer_lease_held: runtime.materializer_lease_held,
            materializer_supervisor_restarts: runtime.materializer_supervisor_restarts,
            materializer_batch_rows: runtime.materializer_batch_rows,
            materializer_batch_bytes: runtime.materializer_batch_bytes,
            materializer_batch_fill_ratio: runtime.materializer_batch_fill_ratio,
            bulk_transaction_latency_ms: runtime.bulk_transaction_latency_ms,
            materialized_batches: runtime.materialized_batches,
            inbox_insert_statements: runtime.inbox_insert_statements,
            quarantine_insert_statements: runtime.quarantine_insert_statements,
            materialized_inserted: runtime.materialized_inserted,
            materialized_duplicates: runtime.materialized_duplicates,
            materialized_conflicted: runtime.materialized_conflicted,
            materialized_quarantined: runtime.materialized_quarantined,
            materializer_reclaims: runtime.materializer_reclaims,
            ack_delete_mismatches: runtime.ack_delete_mismatches,
            materializer_db_failures: runtime.materializer_db_failures,
            materializer_redis_failures: runtime.materializer_redis_failures,
            postgres_pending: runtime.postgres_pending,
            postgres_retry_wait: runtime.postgres_retry_wait,
            postgres_dead_letter: runtime.postgres_dead_letter,
            event_to_redis_avg_ms: runtime.event_to_redis_avg_ms,
            redis_to_postgres_avg_ms: runtime.redis_to_postgres_avg_ms,
            materialization_to_claim_avg_ms: runtime.materialization_to_claim_avg_ms,
            claim_to_taskman_avg_ms: runtime.claim_to_taskman_avg_ms,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct IngestionGates {
    task_rate_limited: i64,
    debounce_coalesced: i64,
    bot_sampling_skipped: i64,
    empty_trigger_skipped: i64,
    invalid_message: i64,
    random_skipped_roll: i64,
    random_skipped_reactivity_off: i64,
    random_skipped_gate: i64,
    random_skipped_user_disabled: i64,
}

impl From<RuntimeIngestionGatesData> for IngestionGates {
    fn from(gates: RuntimeIngestionGatesData) -> Self {
        Self {
            task_rate_limited: gates.task_rate_limited,
            debounce_coalesced: gates.debounce_coalesced,
            bot_sampling_skipped: gates.bot_sampling_skipped,
            empty_trigger_skipped: gates.empty_trigger_skipped,
            invalid_message: gates.invalid_message,
            random_skipped_roll: gates.random_skipped_roll,
            random_skipped_reactivity_off: gates.random_skipped_reactivity_off,
            random_skipped_gate: gates.random_skipped_gate,
            random_skipped_user_disabled: gates.random_skipped_user_disabled,
        }
    }
}

#[derive(Clone, SimpleObject)]
struct UpdatesTask {
    stage: String,
    started_at: String,
    age_ms: i32,
    #[graphql(name = "chatID")]
    chat_id: Option<ID>,
    #[graphql(name = "userID")]
    user_id: Option<ID>,
    update: String,
}

impl From<RuntimeUpdatesTaskData> for UpdatesTask {
    fn from(task: RuntimeUpdatesTaskData) -> Self {
        Self {
            stage: task.stage,
            started_at: task.started_at,
            age_ms: task.age_ms,
            chat_id: task.chat_id.map(|id| ID(id.to_string())),
            user_id: task.user_id.map(|id| ID(id.to_string())),
            update: task.update,
        }
    }
}

#[derive(Clone, Default, SimpleObject)]
struct TelegramUpdateInboxStats {
    pending: i64,
    processing: i64,
    retry_wait: i64,
    completed: i64,
    ignored: i64,
    dead_letter: i64,
    payload_conflicts: i64,
    quarantined: i64,
    total_deliveries: i64,
    oldest_pending_at: Option<String>,
    oldest_retry_at: Option<String>,
    oldest_lease_expiry: Option<String>,
}

impl From<RuntimeTelegramUpdateInboxStatsData> for TelegramUpdateInboxStats {
    fn from(stats: RuntimeTelegramUpdateInboxStatsData) -> Self {
        Self {
            pending: stats.pending,
            processing: stats.processing,
            retry_wait: stats.retry_wait,
            completed: stats.completed,
            ignored: stats.ignored,
            dead_letter: stats.dead_letter,
            payload_conflicts: stats.payload_conflicts,
            quarantined: stats.quarantined,
            total_deliveries: stats.total_deliveries,
            oldest_pending_at: bounded_optional_label(stats.oldest_pending_at),
            oldest_retry_at: bounded_optional_label(stats.oldest_retry_at),
            oldest_lease_expiry: bounded_optional_label(stats.oldest_lease_expiry),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct TelegramUpdateInboxItem {
    id: ID,
    #[graphql(name = "botID")]
    bot_id: ID,
    #[graphql(name = "updateID")]
    update_id: ID,
    schema_version: i32,
    source: String,
    stream_ms: ID,
    stream_seq: ID,
    last_stream_ms: ID,
    last_stream_seq: ID,
    payload_size_bytes: i64,
    payload_sha256: String,
    payload_conflict: bool,
    update_type: Option<String>,
    telegram_event_at: Option<String>,
    first_received_at: String,
    last_received_at: String,
    materialized_at: String,
    delivery_count: i64,
    ordering_key: String,
    priority: i32,
    #[graphql(name = "chatID")]
    chat_id: Option<ID>,
    #[graphql(name = "threadID")]
    thread_id: Option<i32>,
    #[graphql(name = "userID")]
    user_id: Option<ID>,
    status: String,
    available_at: String,
    attempt_count: i32,
    lease_owner: Option<String>,
    leased_until: Option<String>,
    processing_started_at: Option<String>,
    state_applied_at: Option<String>,
    handler_completed_at: Option<String>,
    completed_at: Option<String>,
    outcome: Option<String>,
    ignored_reason: Option<String>,
    last_error_class: Option<String>,
    last_error: Option<String>,
    created_at: String,
    updated_at: String,
}

impl From<RuntimeTelegramUpdateInboxItemData> for TelegramUpdateInboxItem {
    fn from(item: RuntimeTelegramUpdateInboxItemData) -> Self {
        Self {
            id: ID(item.id.to_string()),
            bot_id: ID(item.bot_id.to_string()),
            update_id: ID(item.update_id.to_string()),
            schema_version: item.schema_version,
            source: bounded_text(&item.source, 64),
            stream_ms: ID(item.stream_ms.to_string()),
            stream_seq: ID(item.stream_seq.to_string()),
            last_stream_ms: ID(item.last_stream_ms.to_string()),
            last_stream_seq: ID(item.last_stream_seq.to_string()),
            payload_size_bytes: item.payload_size_bytes.max(0),
            payload_sha256: bounded_text(&item.payload_sha256, 128),
            payload_conflict: item.payload_conflict,
            update_type: bounded_optional_label(item.update_type),
            telegram_event_at: bounded_optional_label(item.telegram_event_at),
            first_received_at: bounded_text(&item.first_received_at, 128),
            last_received_at: bounded_text(&item.last_received_at, 128),
            materialized_at: bounded_text(&item.materialized_at, 128),
            delivery_count: item.delivery_count.max(0),
            ordering_key: bounded_text(&item.ordering_key, RUNTIME_TELEGRAM_LABEL_MAX_CHARS),
            priority: item.priority,
            chat_id: item.chat_id.map(|id| ID(id.to_string())),
            thread_id: item.thread_id,
            user_id: item.user_id.map(|id| ID(id.to_string())),
            status: bounded_text(&item.status, 64),
            available_at: bounded_text(&item.available_at, 128),
            attempt_count: item.attempt_count.max(0),
            lease_owner: bounded_optional_label(item.lease_owner),
            leased_until: bounded_optional_label(item.leased_until),
            processing_started_at: bounded_optional_label(item.processing_started_at),
            state_applied_at: bounded_optional_label(item.state_applied_at),
            handler_completed_at: bounded_optional_label(item.handler_completed_at),
            completed_at: bounded_optional_label(item.completed_at),
            outcome: bounded_optional_label(item.outcome),
            ignored_reason: bounded_optional_diagnostic(item.ignored_reason),
            last_error_class: bounded_optional_label(item.last_error_class),
            last_error: bounded_optional_diagnostic(item.last_error),
            created_at: bounded_text(&item.created_at, 128),
            updated_at: bounded_text(&item.updated_at, 128),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct TelegramUpdateAttempt {
    attempt: i32,
    lease_token: ID,
    #[graphql(name = "workerID")]
    worker_id: String,
    claimed_at: String,
    state_started_at: Option<String>,
    state_completed_at: Option<String>,
    handler_started_at: Option<String>,
    handler_completed_at: Option<String>,
    finished_at: Option<String>,
    outcome: Option<String>,
    error_class: Option<String>,
    error: Option<String>,
}

impl From<RuntimeTelegramUpdateAttemptData> for TelegramUpdateAttempt {
    fn from(attempt: RuntimeTelegramUpdateAttemptData) -> Self {
        Self {
            attempt: attempt.attempt.max(0),
            lease_token: ID(attempt.lease_token.to_string()),
            worker_id: bounded_text(&attempt.worker_id, RUNTIME_TELEGRAM_LABEL_MAX_CHARS),
            claimed_at: bounded_text(&attempt.claimed_at, 128),
            state_started_at: bounded_optional_label(attempt.state_started_at),
            state_completed_at: bounded_optional_label(attempt.state_completed_at),
            handler_started_at: bounded_optional_label(attempt.handler_started_at),
            handler_completed_at: bounded_optional_label(attempt.handler_completed_at),
            finished_at: bounded_optional_label(attempt.finished_at),
            outcome: bounded_optional_label(attempt.outcome),
            error_class: bounded_optional_label(attempt.error_class),
            error: bounded_optional_diagnostic(attempt.error),
        }
    }
}

#[derive(Clone, Default, SimpleObject)]
struct TelegramOutboxStats {
    pending: i64,
    leased: i64,
    retry_wait: i64,
    delivered: i64,
    ambiguous: i64,
    dead_letter: i64,
    expired: i64,
    cancelled: i64,
    protected_unresolved: i64,
    oldest_pending_at: Option<String>,
    oldest_lease_expiry: Option<String>,
}

impl From<RuntimeTelegramOutboxStatsData> for TelegramOutboxStats {
    fn from(stats: RuntimeTelegramOutboxStatsData) -> Self {
        Self {
            pending: stats.pending,
            leased: stats.leased,
            retry_wait: stats.retry_wait,
            delivered: stats.delivered,
            ambiguous: stats.ambiguous,
            dead_letter: stats.dead_letter,
            expired: stats.expired,
            cancelled: stats.cancelled,
            protected_unresolved: stats.protected_unresolved,
            oldest_pending_at: bounded_optional_label(stats.oldest_pending_at),
            oldest_lease_expiry: bounded_optional_label(stats.oldest_lease_expiry),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct TelegramOutboxItem {
    id: ID,
    #[graphql(name = "operationID")]
    operation_id: ID,
    #[graphql(name = "batchID")]
    batch_id: ID,
    part_index: i32,
    #[graphql(name = "botID")]
    bot_id: ID,
    #[graphql(name = "chatID")]
    chat_id: Option<ID>,
    #[graphql(name = "threadID")]
    thread_id: Option<i32>,
    ordering_key: String,
    #[graphql(name = "causationUpdateID")]
    causation_update_id: Option<ID>,
    #[graphql(name = "dialogJobID")]
    dialog_job_id: Option<ID>,
    #[graphql(name = "triggerMessageID")]
    trigger_message_id: Option<ID>,
    method_kind: String,
    delivery_policy: String,
    protected: bool,
    priority: i32,
    state: String,
    available_at: String,
    expires_at: Option<String>,
    attempt_count: i32,
    lease_owner: Option<String>,
    leased_until: Option<String>,
    last_error_class: Option<String>,
    last_error: Option<String>,
    response_kind: Option<String>,
    #[graphql(name = "telegramMessageIDs")]
    telegram_message_ids: Vec<ID>,
    has_receipt: bool,
    confirmed_at: Option<String>,
    created_at: String,
    updated_at: String,
}

impl From<RuntimeTelegramOutboxItemData> for TelegramOutboxItem {
    fn from(item: RuntimeTelegramOutboxItemData) -> Self {
        Self {
            id: ID(item.id.to_string()),
            operation_id: ID(bounded_text(
                &item.operation_id,
                RUNTIME_TELEGRAM_OPERATION_ID_MAX_CHARS,
            )),
            batch_id: ID(bounded_text(
                &item.batch_id,
                RUNTIME_TELEGRAM_OPERATION_ID_MAX_CHARS,
            )),
            part_index: item.part_index.max(0),
            bot_id: ID(item.bot_id.to_string()),
            chat_id: item.chat_id.map(|id| ID(id.to_string())),
            thread_id: item.thread_id,
            ordering_key: bounded_text(&item.ordering_key, RUNTIME_TELEGRAM_LABEL_MAX_CHARS),
            causation_update_id: item.causation_update_id.map(|id| ID(id.to_string())),
            dialog_job_id: item.dialog_job_id.map(|id| ID(id.to_string())),
            trigger_message_id: item.trigger_message_id.map(|id| ID(id.to_string())),
            method_kind: bounded_text(&item.method_kind, 128),
            delivery_policy: bounded_text(&item.delivery_policy, 64),
            protected: item.protected,
            priority: item.priority,
            state: bounded_text(&item.state, 64),
            available_at: bounded_text(&item.available_at, 128),
            expires_at: bounded_optional_label(item.expires_at),
            attempt_count: item.attempt_count.max(0),
            lease_owner: bounded_optional_label(item.lease_owner),
            leased_until: bounded_optional_label(item.leased_until),
            last_error_class: bounded_optional_label(item.last_error_class),
            last_error: bounded_optional_diagnostic(item.last_error),
            response_kind: bounded_optional_label(item.response_kind),
            telegram_message_ids: item
                .telegram_message_ids
                .into_iter()
                .map(|id| ID(id.to_string()))
                .collect(),
            has_receipt: item.has_receipt,
            confirmed_at: bounded_optional_label(item.confirmed_at),
            created_at: bounded_text(&item.created_at, 128),
            updated_at: bounded_text(&item.updated_at, 128),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct TelegramOutboxAttempt {
    attempt: i32,
    lease_token: ID,
    #[graphql(name = "workerID")]
    worker_id: String,
    claimed_at: String,
    request_started_at: Option<String>,
    response_received_at: Option<String>,
    finished_at: Option<String>,
    outcome: Option<String>,
    #[graphql(name = "httpStatus")]
    http_status: Option<i32>,
    latency_ms: Option<i64>,
    error_class: Option<String>,
    error: Option<String>,
}

impl From<RuntimeTelegramOutboxAttemptData> for TelegramOutboxAttempt {
    fn from(attempt: RuntimeTelegramOutboxAttemptData) -> Self {
        Self {
            attempt: attempt.attempt.max(0),
            lease_token: ID(attempt.lease_token.to_string()),
            worker_id: bounded_text(&attempt.worker_id, RUNTIME_TELEGRAM_LABEL_MAX_CHARS),
            claimed_at: bounded_text(&attempt.claimed_at, 128),
            request_started_at: bounded_optional_label(attempt.request_started_at),
            response_received_at: bounded_optional_label(attempt.response_received_at),
            finished_at: bounded_optional_label(attempt.finished_at),
            outcome: bounded_optional_label(attempt.outcome),
            http_status: attempt.http_status,
            latency_ms: attempt.latency_ms.map(|value| value.max(0)),
            error_class: bounded_optional_label(attempt.error_class),
            error: bounded_optional_diagnostic(attempt.error),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct TelegramOutboxMutationResult {
    #[graphql(name = "operationID")]
    operation_id: ID,
    changed: bool,
    state: Option<String>,
}

impl From<RuntimeTelegramOutboxMutationResultData> for TelegramOutboxMutationResult {
    fn from(result: RuntimeTelegramOutboxMutationResultData) -> Self {
        Self {
            operation_id: ID(bounded_text(
                &result.operation_id,
                RUNTIME_TELEGRAM_OPERATION_ID_MAX_CHARS,
            )),
            changed: result.changed,
            state: bounded_optional_label(result.state),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct LogEntry {
    seq: ID,
    time: Option<String>,
    level: String,
    message: String,
    attrs: Option<Json<Value>>,
}

impl From<RuntimeLogEntry> for LogEntry {
    fn from(entry: RuntimeLogEntry) -> Self {
        Self {
            seq: ID(entry.seq.to_string()),
            time: entry.time,
            level: entry.level,
            message: entry.message,
            attrs: entry.attrs.map(Json),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct LogsResult {
    count: i32,
    last_seq: Option<ID>,
    items: Vec<LogEntry>,
}

/// Redis prefix group returned by runtime API diagnostics.
#[derive(Clone, Debug, Eq, PartialEq, SimpleObject)]
pub struct RuntimeRedisPrefixGroup {
    pub prefix: String,
    pub count: i32,
}

/// Redis value returned by runtime API diagnostics.
#[derive(Clone, Debug, Eq, PartialEq, SimpleObject)]
pub struct RuntimeRedisValue {
    pub key: String,
    pub value: String,
    pub truncated: bool,
}

#[derive(InputObject)]
#[allow(dead_code)]
struct SqlReadInput {
    sql: String,
    args: Option<Vec<Json<Value>>>,
    timeout_ms: Option<i32>,
}

#[derive(Clone, SimpleObject)]
struct SqlReadResult {
    columns: Vec<String>,
    rows: Vec<Json<Value>>,
    row_count: i32,
    elapsed_ms: i32,
    truncated: bool,
}

#[derive(Clone, SimpleObject)]
struct MemoryRestartResult {
    ok: bool,
    #[graphql(name = "runID")]
    run_id: Option<ID>,
    retried_failed_runs: i32,
    queued_runs: i32,
    started: bool,
    #[graphql(name = "override")]
    override_: bool,
    provider: Option<String>,
    model: Option<String>,
}

impl From<RuntimeMemoryRestartResultData> for MemoryRestartResult {
    fn from(result: RuntimeMemoryRestartResultData) -> Self {
        Self {
            ok: result.ok,
            run_id: result.run_id.map(|id| ID(id.to_string())),
            retried_failed_runs: result.retried_failed_runs,
            queued_runs: result.queued_runs,
            started: result.started,
            override_: result.override_,
            provider: trim_nonempty(result.provider),
            model: trim_nonempty(result.model),
        }
    }
}

#[derive(Clone, SimpleObject)]
struct GeminiExplicitCachePurgeResult {
    ok: bool,
    scanned: i32,
    matched: i32,
    deleted: i32,
    failed: i32,
}

impl From<RuntimeGeminiCachePurgeResultData> for GeminiExplicitCachePurgeResult {
    fn from(result: RuntimeGeminiCachePurgeResultData) -> Self {
        Self {
            ok: result.failed == 0,
            scanned: result.scanned,
            matched: result.matched,
            deleted: result.deleted,
            failed: result.failed,
        }
    }
}

fn trim_nonempty(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn bounded_text(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn bounded_optional_label(value: Option<String>) -> Option<String> {
    value.map(|value| bounded_text(&value, RUNTIME_TELEGRAM_LABEL_MAX_CHARS))
}

fn bounded_optional_diagnostic(value: Option<String>) -> Option<String> {
    value.map(|value| {
        bounded_text(
            &redact_telegram_bot_api_token(&value),
            RUNTIME_TELEGRAM_DIAGNOSTIC_MAX_CHARS,
        )
    })
}

fn redact_telegram_bot_api_token(value: &str) -> String {
    const MARKER: &str = "api.telegram.org/bot";
    let mut redacted = value.to_owned();
    let mut search_from = 0;
    while let Some(relative) = redacted[search_from..].find(MARKER) {
        let token_start = search_from + relative + MARKER.len();
        let token_end = redacted[token_start..]
            .find(|character: char| {
                character == '/'
                    || character == '?'
                    || character == '#'
                    || character.is_whitespace()
                    || matches!(character, '"' | '\'' | ')' | ']')
            })
            .map_or(redacted.len(), |end| token_start + end);
        let candidate = &redacted[token_start..token_end];
        let looks_like_token = candidate.split_once(':').is_some_and(|(bot_id, secret)| {
            !bot_id.is_empty()
                && bot_id.bytes().all(|byte| byte.is_ascii_digit())
                && secret.len() >= 16
        });
        if looks_like_token {
            redacted.replace_range(token_start..token_end, "<redacted>");
            search_from = token_start + "<redacted>".len();
        } else {
            search_from = token_start;
        }
        if search_from >= redacted.len() {
            break;
        }
    }
    redacted
}
