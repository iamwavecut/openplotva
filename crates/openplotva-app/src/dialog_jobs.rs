use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashSet},
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use openplotva_core::{
    ChatMessageMeta, ChatSettings, SENDER_TYPE_CHANNEL, SENDER_TYPE_SAME_CHAT, SENDER_TYPE_USER,
    ToolCall, filter_non_terminator_tool_calls,
};
use openplotva_dialog::{
    DialogContext, DialogInput, DialogMessage, DialogOutput, DialogUser, HistoryMessage,
    MESSAGE_KIND_TEXT, MESSAGE_KIND_TOOL_REQUEST, MESSAGE_KIND_TOOL_RESPONSE, Persona, ROLE_MODEL,
    ROLE_TOOL, ROLE_USER, conversation_projection, daily_persona_for_unix_timestamp,
    filter_dialog_tool_calls_for_history, is_dialog_history_noise_tool_call_name,
};
use openplotva_llm::ChatProvider;
use openplotva_memory::format_context as format_memory_context;
use openplotva_shield::{Options as ShieldOptions, SearchRequest as ShieldSearchRequest};
use openplotva_storage::{
    DialogMemoryChatMeta, PostgresChatSettingsStore, PostgresHistoryStore, PostgresMemoryStore,
    PostgresShieldStore, PostgresVirtualMessageStore,
};
use openplotva_taskman::{
    DEFAULT_LLM_JOB_MAX_ATTEMPTS, DIALOG_AIFARM_QUEUE_NAME, DialogJobParams, InMemoryTaskQueue,
    JobType, LLM_JOB_RETRY_EXHAUSTED_STAGE, LLM_JOB_RETRY_STAGE, LOWEST_PRIORITY, Priority,
    StatelessJobItem, TEXT_QUEUE_NAME, TaskQueueError, TaskQueueJobEvent, TaskQueueWorkItem,
    dialog_job_params_from_stateless_job,
};
use openplotva_telegram::{
    ChatRef, DispatcherQueue, ReplyMessageRef, RichMessageRequest, TELEGRAM_PARSE_MODE_HTML,
    TextMessageRequest, clean_unicode_non_printables, decode_html_entities,
    ensure_telegram_safe_text, sanitize_rich_html, sanitize_telegram_html,
};
use thiserror::Error;
use time::{Duration as TimeDuration, OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    dialog_context::{
        build_dialog_shield_query_text, dialog_memory_retrieval_request,
        dialog_reference_context_from_memory,
    },
    memory_runtime::{EmbeddingProvider, memory_retrieval_query_task},
    virtual_messages::{
        QueueRichRequest, QueueTextRequest, VirtualIdFactory, monotonic_virtual_id_factory,
        queue_rich_message, queue_text_message_parts,
    },
    vision::DialogVisionInputMaterializer,
};

const DIALOG_JOB_WORKER_ID: &str = "dialog-job";
pub const DIALOG_JOB_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DIALOG_HISTORY_FETCH_LIMIT: i32 = 100;
const DIALOG_HISTORY_TTL: TimeDuration = TimeDuration::days(7);

/// Default per-turn wall-clock budget (`DIALOG_TURN_BUDGET_SECS`).
pub const DEFAULT_DIALOG_TURN_BUDGET_SECS: i32 = 120;
/// Default pending age beyond which never-processed dialog jobs are dropped
/// (`DIALOG_TURN_MAX_QUEUE_AGE_SECS`).
pub const DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS: i32 = 600;
/// Default in-process duplicate-answer regenerations per turn
/// (`DIALOG_TURN_MAX_REGENERATIONS`).
pub const DEFAULT_DIALOG_TURN_MAX_REGENERATIONS: i32 = 2;

pub const DIALOG_JOB_WORKER_QUEUES: [&str; 2] = [DIALOG_AIFARM_QUEUE_NAME, TEXT_QUEUE_NAME];

/// Boxed future returned by dialog taskman queue calls.
pub type DialogJobWorkerFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by dialog side effects.
pub type DialogJobEffectFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by dialog tool-call history storage.
pub type DialogToolCallHistoryFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<bool, E>> + Send + 'a>>;

/// Boxed future returned by dialog input materializers.
pub type DialogInputMaterializerFuture<'a> = Pin<Box<dyn Future<Output = DialogInput> + Send + 'a>>;

/// Concrete taskman row ready for the dialog worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DialogJobWorkItem {
    /// Taskman job ID used for completion/failure writes.
    pub id: i64,
    pub job: StatelessJobItem,
    pub events: Vec<TaskQueueJobEvent>,
}

/// Result of one dialog taskman worker tick.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DialogJobWorkerReport {
    /// Queue checked by this tick.
    pub queue_name: String,
    /// Whether a job was dequeued.
    pub dequeued: bool,
    /// Dequeued taskman job ID.
    pub job_id: Option<i64>,
    /// Provider chosen for execution.
    pub provider: Option<String>,
    pub skipped_empty_payload: bool,
    /// Never-processed job outlived the queue-age gate and was dropped.
    pub skipped_queue_backlog: bool,
    pub content_blocked: bool,
    pub sent_answer: bool,
    /// Dialog answer matched the latest comparable bot reply and was suppressed.
    pub suppressed_duplicate_message_id: Option<i32>,
    /// Job was finalized as completed.
    pub completed: bool,
    /// Job was finalized as failed.
    pub failed: bool,
    /// Queue dequeue failed.
    pub dequeue_error: Option<String>,
    pub decode_error: Option<String>,
    /// Provider failure reason classified as retryable by the LLM layer.
    pub retryable_provider_error: Option<String>,
    /// Retryable LLM failure attempt number.
    pub retry_attempt: Option<i32>,
    /// Retryable LLM failure max attempt count.
    pub retry_max_attempts: Option<i32>,
    /// Target queue selected for retryable LLM requeue.
    pub retry_target_queue: Option<String>,
    /// Retryable LLM failure was requeued without failing the job.
    pub retry_requeued: bool,
    /// Retryable LLM failure exhausted attempts and failed the job.
    pub retry_exhausted: bool,
    /// Provider execution failed.
    pub provider_error: Option<String>,
    /// Answer side effect failed.
    pub send_error: Option<String>,
    /// Provider returned content that became empty after outbound sanitization.
    pub empty_answer_error: Option<String>,
    /// Provider tool calls were persisted to chat history.
    pub persisted_tool_call_history: bool,
    /// Tool-call history persistence failed non-fatally.
    pub tool_call_history_error: Option<String>,
    pub recorded_dialog_fallback_event: bool,
    pub dialog_fallback_event_error: Option<String>,
    /// Completion/failure status write failed.
    pub status_error: Option<String>,
    /// Chat the job answered (from decoded params).
    pub chat_id: Option<i64>,
    /// Forum thread of the trigger message, when present.
    pub thread_id: Option<i32>,
    /// User whose message triggered the job.
    pub user_id: Option<i64>,
    /// Trigger message ID.
    pub message_id: Option<i32>,
    /// answer, response, and queued tool material were all empty; the turn
    /// engine retries this class instead of completing silently.
    pub answer_empty_all_sources: bool,
    /// In-process duplicate-answer regenerations performed this tick.
    pub regenerations: i32,
    /// Terminal user signal failed after reaction and fallback attempts.
    pub user_signal_error: Option<String>,
}

/// Aggregate report for a long-running dialog taskman worker.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DialogJobWorkerRunReport {
    /// Number of poll ticks.
    pub ticks: u64,
    /// Number of ticks that dequeued a job.
    pub dequeued: u64,
    /// Number of jobs completed.
    pub completed: u64,
    /// Number of jobs failed.
    pub failed: u64,
    /// Number of empty-payload jobs skipped.
    pub skipped_empty_payload: u64,
    /// Number of expired-backlog jobs dropped without provider execution.
    pub skipped_queue_backlog: u64,
    /// Number of content-blocked provider results treated as completed.
    pub content_blocked: u64,
    /// Number of answers queued.
    pub sent_answers: u64,
    /// Number of duplicate answers suppressed before send.
    pub suppressed_duplicate_answers: u64,
    /// Number of retryable provider errors requeued.
    pub retry_requeued: u64,
    /// Number of retryable provider errors exhausted.
    pub retry_exhausted: u64,
    /// Number of queue dequeue errors.
    pub dequeue_errors: u64,
    /// Number of completion/failure write errors.
    pub status_errors: u64,
}

impl DialogJobWorkerRunReport {
    fn record_tick(&mut self, tick: &DialogJobWorkerReport) {
        self.ticks += 1;
        if tick.dequeued {
            self.dequeued += 1;
        }
        if tick.completed {
            self.completed += 1;
        }
        if tick.failed {
            self.failed += 1;
        }
        if tick.skipped_empty_payload {
            self.skipped_empty_payload += 1;
        }
        if tick.skipped_queue_backlog {
            self.skipped_queue_backlog += 1;
        }
        if tick.content_blocked {
            self.content_blocked += 1;
        }
        if tick.sent_answer {
            self.sent_answers += 1;
        }
        if tick.suppressed_duplicate_message_id.is_some() {
            self.suppressed_duplicate_answers += 1;
        }
        if tick.retry_requeued {
            self.retry_requeued += 1;
        }
        if tick.retry_exhausted {
            self.retry_exhausted += 1;
        }
        if tick.dequeue_error.is_some() {
            self.dequeue_errors += 1;
        }
        if tick.status_error.is_some() {
            self.status_errors += 1;
        }
    }
}

/// Shared loop controls for dialog taskman workers.
#[derive(Clone, Copy)]
pub struct DialogJobWorkerLoopOptions<'a, Materializer: ?Sized, ToolHistory: ?Sized> {
    pub materializer: &'a Materializer,
    pub tool_history: &'a ToolHistory,
    pub routing_events: Option<&'a crate::runtime_routing::RoutingEventReporter>,
    /// Reply-outcome ledger observer; every handled job records one outcome.
    pub turn_outcomes: Option<&'a crate::dialog_turn::DialogTurnObserver>,
    /// Queue names to poll in order.
    pub queue_names: &'static [&'static str],
    /// Poll interval.
    pub interval: Duration,
    pub max_llm_job_attempts: i32,
    /// Per-turn wall-clock budget in seconds, anchored at first processing start.
    pub turn_budget_secs: i32,
    /// Pending age in seconds beyond which never-processed jobs are dropped.
    pub turn_max_queue_age_secs: i32,
    /// In-process duplicate-answer regenerations per turn.
    pub max_regenerations: i32,
    /// Terminal-failure user signal wiring (reaction with text fallback).
    pub terminal_signal: crate::dialog_turn::TurnSignalPolicy<'a>,
    /// Delivery-obligation annotator: finalize backfills the dialog job id on
    /// obligations recorded by the schedulers (annotation only, never creates).
    pub obligations: Option<&'a dyn crate::dialog_turn::DeliveryObligationAnnotator>,
}

/// True when the current UTC time is inside the daily `[start, end)` minute window,
/// supporting windows that wrap past midnight (`start > end`).
fn in_utc_minute_window(start_minute: u32, end_minute: u32) -> bool {
    let now = OffsetDateTime::now_utc();
    let minute = u32::from(now.hour()) * 60 + u32::from(now.minute());
    if start_minute <= end_minute {
        minute >= start_minute && minute < end_minute
    } else {
        minute >= start_minute || minute < end_minute
    }
}

/// Evaluate every workflow's triggers once and publish their engagement state.
/// queue_depth applies hysteresis (engage at `high`, disengage below `low`);
/// error_rate compares the breaker's windowed ratio; time_of_day checks the UTC
/// window. Only the known dialog queue is polled for depth in this version.
async fn evaluate_router_triggers_once<Queue>(
    handle: &openplotva_llm::router::RouterHandle,
    triggers: &openplotva_llm::router::TriggerState,
    breakers: &openplotva_llm::router::BreakerSet,
    queue: &Queue,
) where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let table = handle.snapshot();
    let keys: Vec<String> = table.workflow_keys().map(str::to_owned).collect();
    for key in keys {
        let Some(route) = table.resolve(&key, false) else {
            continue;
        };
        for spec in &route.triggers {
            let engaged = match &spec.condition {
                openplotva_llm::router::TriggerCondition::QueueDepth {
                    queue: queue_name,
                    high,
                    low,
                } => {
                    if queue_name != DIALOG_AIFARM_QUEUE_NAME {
                        continue;
                    }
                    let Ok(depth) = queue
                        .pending_dialog_job_depth(DIALOG_AIFARM_QUEUE_NAME, LOWEST_PRIORITY)
                        .await
                    else {
                        continue;
                    };
                    if triggers.is_engaged(spec.id) {
                        depth >= *low
                    } else {
                        depth >= *high
                    }
                }
                openplotva_llm::router::TriggerCondition::ErrorRate {
                    provider,
                    model,
                    threshold,
                    window,
                } => {
                    breakers.error_rate(*provider, *model, *window, std::time::Instant::now())
                        >= *threshold
                }
                openplotva_llm::router::TriggerCondition::TimeOfDay {
                    start_minute,
                    end_minute,
                } => in_utc_minute_window(*start_minute, *end_minute),
                openplotva_llm::router::TriggerCondition::ProviderCapacity {
                    provider,
                    model,
                    ..
                } => triggers.provider_capacity_unavailable(*provider, *model),
            };
            triggers.set_engaged(spec.id, engaged);
        }
    }
}

/// Background task that keeps the router's trigger-engagement state current,
/// generalizing the dialog-aifarm watermark gate to all DB-configured triggers.
pub async fn run_router_trigger_poller<Queue, Stop>(
    handle: Arc<openplotva_llm::router::RouterHandle>,
    triggers: Arc<openplotva_llm::router::TriggerState>,
    breakers: Arc<openplotva_llm::router::BreakerSet>,
    queue: Arc<Queue>,
    interval: Duration,
    stop: Stop,
) where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Stop: Future<Output = ()>,
{
    let interval = if interval.is_zero() {
        Duration::from_secs(1)
    } else {
        interval
    };
    let mut stop = std::pin::pin!(stop);
    loop {
        tokio::select! {
            () = &mut stop => break,
            () = tokio::time::sleep(interval) => {
                evaluate_router_triggers_once(&handle, &triggers, &breakers, queue.as_ref()).await;
            }
        }
    }
}

/// Queue/status boundary for the dialog-owned taskman worker.
pub trait DialogJobWorkerQueue {
    /// Error returned by the concrete queue implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Dequeue the next pending dialog job from a named taskman queue.
    fn dequeue_dialog_job<'a>(
        &'a self,
        queue_name: &'static str,
    ) -> DialogJobWorkerFuture<'a, Option<DialogJobWorkItem>, Self::Error>;

    /// Count pending dialog jobs at this priority or higher.
    fn pending_dialog_job_depth<'a>(
        &'a self,
        queue_name: &'static str,
        priority: Priority,
    ) -> DialogJobWorkerFuture<'a, usize, Self::Error>;

    /// Mark one dialog job completed.
    fn complete_dialog_job<'a>(&'a self, job_id: i64)
    -> DialogJobWorkerFuture<'a, (), Self::Error>;

    /// Mark one dialog job failed.
    fn fail_dialog_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error>;

    fn append_dialog_job_event<'a>(
        &'a self,
        job_id: i64,
        event: TaskQueueJobEvent,
        at: OffsetDateTime,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error>;

    /// Move one retryable dialog job back to pending in the chosen queue.
    fn requeue_retryable_dialog_job<'a>(
        &'a self,
        job_id: i64,
        target_queue: &'a str,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error>;
}

/// Side effects performed after a provider returns a dialog answer.
pub trait DialogJobEffects {
    /// Error returned by concrete side effects.
    type Error: fmt::Display + Send + Sync + 'static;

    fn send_dialog_answer<'a>(
        &'a self,
        params: &'a DialogJobParams,
        answer: &'a str,
    ) -> DialogJobEffectFuture<'a, Self::Error>;
}

pub trait DialogToolCallHistoryStore {
    /// Error returned by concrete storage.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Upsert filtered dialog tool calls for one base text message.
    fn upsert_dialog_tool_calls<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        tool_calls: &'a [ToolCall],
    ) -> DialogToolCallHistoryFuture<'a, Self::Error>;
}

/// No-op tool history store used by narrow tests and unconnected wrappers.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopDialogToolCallHistoryStore;

pub trait DialogInputMaterializer {
    /// Materialize one provider input. Errors should be fail-open inside the implementation,
    fn materialize_dialog_input<'a>(
        &'a self,
        params: &'a DialogJobParams,
        now: OffsetDateTime,
    ) -> DialogInputMaterializerFuture<'a>;
}

/// Current-message-only fallback used by narrow tests and unconnected slices.
#[derive(Clone, Copy, Debug, Default)]
pub struct BasicDialogInputMaterializer;

impl DialogInputMaterializer for BasicDialogInputMaterializer {
    fn materialize_dialog_input<'a>(
        &'a self,
        params: &'a DialogJobParams,
        now: OffsetDateTime,
    ) -> DialogInputMaterializerFuture<'a> {
        Box::pin(async move { dialog_input_from_job_params_at(params, now) })
    }
}

/// Telegram bot identity used in dialog context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DialogBotIdentity {
    /// Bot user ID.
    pub id: i64,
    /// Bot first/display name.
    pub name: String,
}

impl DialogBotIdentity {
    #[must_use]
    pub fn new(id: i64, name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            id,
            name: non_blank_or(name.trim(), "Plotva"),
        }
    }
}

impl Default for DialogBotIdentity {
    fn default() -> Self {
        Self {
            id: 0,
            name: "Plotva".to_owned(),
        }
    }
}

/// Storage-backed dialog input materializer for the runtime worker.
#[derive(Clone, Debug)]
pub struct PostgresDialogInputMaterializer {
    settings: PostgresChatSettingsStore,
    identity: PostgresVirtualMessageStore,
    history: PostgresHistoryStore,
    memory: Option<PostgresMemoryStore>,
    memory_embedder: Option<Arc<dyn EmbeddingProvider>>,
    memory_embedding_dim: i32,
    shield: Option<PostgresShieldStore>,
    shield_embedder: Option<Arc<dyn EmbeddingProvider>>,
    shield_options: ShieldOptions,
    shield_history_tail_messages: usize,
    vision: Option<Arc<dyn DialogVisionInputMaterializer>>,
    bot: DialogBotIdentity,
}

impl PostgresDialogInputMaterializer {
    /// Build the concrete runtime materializer.
    #[must_use]
    pub fn new(
        settings: PostgresChatSettingsStore,
        identity: PostgresVirtualMessageStore,
        history: PostgresHistoryStore,
        bot: DialogBotIdentity,
    ) -> Self {
        Self {
            settings,
            identity,
            history,
            memory: None,
            memory_embedder: None,
            memory_embedding_dim: 0,
            shield: None,
            shield_embedder: None,
            shield_options: ShieldOptions::default(),
            shield_history_tail_messages: 0,
            vision: None,
            bot,
        }
    }

    #[must_use]
    pub fn with_memory_store(mut self, memory: PostgresMemoryStore) -> Self {
        self.memory = Some(memory);
        self
    }

    #[must_use]
    pub fn with_memory_embedder(
        mut self,
        embedder: Arc<dyn EmbeddingProvider>,
        embedding_dim: i32,
    ) -> Self {
        self.memory_embedder = Some(embedder);
        self.memory_embedding_dim = embedding_dim;
        self
    }

    #[must_use]
    pub fn with_shield_store(
        mut self,
        shield: PostgresShieldStore,
        options: ShieldOptions,
        history_tail_messages: usize,
    ) -> Self {
        self.shield = Some(shield);
        self.shield_options = options.with_defaults();
        self.shield_history_tail_messages = history_tail_messages;
        self
    }

    #[must_use]
    pub fn with_shield_embedder(mut self, embedder: Arc<dyn EmbeddingProvider>) -> Self {
        self.shield_embedder = Some(embedder);
        self
    }

    #[must_use]
    pub fn with_vision_materializer(
        mut self,
        vision: Arc<dyn DialogVisionInputMaterializer>,
    ) -> Self {
        self.vision = Some(vision);
        self
    }
}

impl DialogInputMaterializer for PostgresDialogInputMaterializer {
    fn materialize_dialog_input<'a>(
        &'a self,
        params: &'a DialogJobParams,
        now: OffsetDateTime,
    ) -> DialogInputMaterializerFuture<'a> {
        Box::pin(async move { self.materialize(params, now).await })
    }
}

#[derive(Clone)]
pub struct DialogDispatcherEffects {
    queue: Arc<DispatcherQueue>,
    next_virtual_id: VirtualIdFactory,
}

impl DialogDispatcherEffects {
    #[must_use]
    pub fn new(queue: Arc<DispatcherQueue>) -> Self {
        Self {
            queue,
            next_virtual_id: monotonic_virtual_id_factory("dialog-vmsg"),
        }
    }

    #[must_use]
    pub fn with_virtual_id_factory(mut self, next_virtual_id: VirtualIdFactory) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }
}

/// Concrete dispatch failure.
#[derive(Debug, Error)]
pub enum DialogDispatchEffectError {
    #[error("failed to queue dialog answer: {0}")]
    Queue(#[from] openplotva_telegram::OutboundBuildError),
}

impl DialogJobWorkerQueue for InMemoryTaskQueue {
    type Error = TaskQueueError;

    fn dequeue_dialog_job<'a>(
        &'a self,
        queue_name: &'static str,
    ) -> DialogJobWorkerFuture<'a, Option<DialogJobWorkItem>, Self::Error> {
        Box::pin(async move {
            Ok(self
                .dequeue_matching(
                    queue_name,
                    DIALOG_JOB_WORKER_ID,
                    OffsetDateTime::now_utc(),
                    is_dialog_job,
                )
                .map(dialog_work_item_from_taskman))
        })
    }

    fn pending_dialog_job_depth<'a>(
        &'a self,
        queue_name: &'static str,
        priority: Priority,
    ) -> DialogJobWorkerFuture<'a, usize, Self::Error> {
        Box::pin(async move { Ok(self.queue_depth_for_priority_or_higher(queue_name, priority)) })
    }

    fn complete_dialog_job<'a>(
        &'a self,
        job_id: i64,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async move { self.complete(job_id, OffsetDateTime::now_utc()) })
    }

    fn fail_dialog_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async move { self.fail(job_id, error, OffsetDateTime::now_utc()) })
    }

    fn append_dialog_job_event<'a>(
        &'a self,
        job_id: i64,
        event: TaskQueueJobEvent,
        at: OffsetDateTime,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async move { self.append_job_event(job_id, event, at) })
    }

    fn requeue_retryable_dialog_job<'a>(
        &'a self,
        job_id: i64,
        target_queue: &'a str,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async move { self.requeue_job_to_queue(job_id, target_queue) })
    }
}

impl DialogJobEffects for DialogDispatcherEffects {
    type Error = DialogDispatchEffectError;

    fn send_dialog_answer<'a>(
        &'a self,
        params: &'a DialogJobParams,
        answer: &'a str,
    ) -> DialogJobEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            let chat = ChatRef {
                id: params.chat_id,
                is_forum: params.thread_id.is_some(),
            };
            let reply_to = ReplyMessageRef {
                message_id: i64::from(params.message_id),
                chat,
                is_topic_message: params.thread_id.is_some(),
                message_thread_id: params.thread_id.map(i64::from).unwrap_or_default(),
            };
            // Dialog answers must survive queue trims, and the reply-scoped
            // debounce key stops identical answers to different trigger
            // messages from deduping each other (a duplicate reply to the
            // same message is still suppressed).
            let debounce_key = format!("r{}", params.message_id);
            if dialog_response_requires_rich(answer) {
                let request = RichMessageRequest {
                    chat: Some(chat),
                    message_thread_id: params.thread_id.map(i64::from).unwrap_or_default(),
                    disable_notification: false,
                    allow_sending_without_reply: None,
                    html: answer.to_owned(),
                    reply_markup: None,
                };
                queue_rich_message(
                    &self.queue,
                    QueueRichRequest {
                        message: &request,
                        reply_to: Some(&reply_to),
                        immediate: true,
                        bypass_chat_restrictions: false,
                        ephemeral_delete_after: None,
                        protected: true,
                        debounce_key: Some(&debounce_key),
                    },
                    || (self.next_virtual_id)(),
                )
                .await?;
            } else {
                let request = TextMessageRequest {
                    chat: Some(chat),
                    message_thread_id: params.thread_id.map(i64::from).unwrap_or_default(),
                    disable_notification: false,
                    allow_sending_without_reply: None,
                    text: answer.to_owned(),
                    render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
                    reply_markup: None,
                };
                queue_text_message_parts(
                    &self.queue,
                    QueueTextRequest {
                        protected: true,
                        debounce_key: Some(&debounce_key),
                        message: &request,
                        reply_to: Some(&reply_to),
                        immediate_first: true,
                        bypass_chat_restrictions: false,
                        ephemeral_delete_after: None,
                    },
                    || (self.next_virtual_id)(),
                )
                .await?;
            }
            Ok(())
        })
    }
}

impl DialogToolCallHistoryStore for NoopDialogToolCallHistoryStore {
    type Error = std::convert::Infallible;

    fn upsert_dialog_tool_calls<'a>(
        &'a self,
        _chat_id: i64,
        _message_id: i32,
        _tool_calls: &'a [ToolCall],
    ) -> DialogToolCallHistoryFuture<'a, Self::Error> {
        Box::pin(async { Ok(false) })
    }
}

impl DialogToolCallHistoryStore for PostgresHistoryStore {
    type Error = openplotva_storage::StorageError;

    fn upsert_dialog_tool_calls<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        tool_calls: &'a [ToolCall],
    ) -> DialogToolCallHistoryFuture<'a, Self::Error> {
        Box::pin(async move {
            self.upsert_tool_call_history(chat_id, message_id, tool_calls)
                .await
        })
    }
}

pub async fn process_dialog_job_once_at<Queue, Provider, Effects>(
    queue: &Queue,
    provider: &Provider,
    effects: &Effects,
    now: OffsetDateTime,
) -> DialogJobWorkerReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
{
    process_dialog_job_once_in_queue_at(queue, DIALOG_AIFARM_QUEUE_NAME, provider, effects, now)
        .await
}

/// Process one dialog taskman job from a specific queue.
pub async fn process_dialog_job_once_in_queue_at<Queue, Provider, Effects>(
    queue: &Queue,
    queue_name: &'static str,
    provider: &Provider,
    effects: &Effects,
    now: OffsetDateTime,
) -> DialogJobWorkerReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
{
    process_dialog_job_once_in_queue_with_materializer_at(
        queue,
        queue_name,
        provider,
        effects,
        &BasicDialogInputMaterializer,
        now,
    )
    .await
}

/// Process one dialog taskman job from a specific queue with a custom input materializer.
pub async fn process_dialog_job_once_in_queue_with_materializer_at<
    Queue,
    Provider,
    Effects,
    Materializer,
>(
    queue: &Queue,
    queue_name: &'static str,
    provider: &Provider,
    effects: &Effects,
    materializer: &Materializer,
    now: OffsetDateTime,
) -> DialogJobWorkerReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
    Materializer: DialogInputMaterializer + Sync + ?Sized,
{
    process_dialog_job_once_in_queue_with_materializer_and_history_at(
        queue,
        queue_name,
        provider,
        effects,
        materializer,
        &NoopDialogToolCallHistoryStore,
        now,
    )
    .await
}

/// Process one dialog taskman job with custom input materialization and tool-call history storage.
pub async fn process_dialog_job_once_in_queue_with_materializer_and_history_at<
    Queue,
    Provider,
    Effects,
    Materializer,
    ToolHistory,
>(
    queue: &Queue,
    queue_name: &'static str,
    provider: &Provider,
    effects: &Effects,
    materializer: &Materializer,
    tool_history: &ToolHistory,
    now: OffsetDateTime,
) -> DialogJobWorkerReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
    Materializer: DialogInputMaterializer + Sync + ?Sized,
    ToolHistory: DialogToolCallHistoryStore + Sync + ?Sized,
{
    process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        queue,
        provider,
        effects,
        materializer,
        tool_history,
        DialogJobProcessOptions {
            queue_name,
            max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            now,
            routing_events: None,
            turn_outcomes: None,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
        },
    )
    .await
}

#[derive(Clone, Copy)]
struct DialogJobProcessOptions<'a> {
    queue_name: &'static str,
    max_llm_job_attempts: i32,
    turn_budget_secs: i32,
    turn_max_queue_age_secs: i32,
    max_regenerations: i32,
    now: OffsetDateTime,
    routing_events: Option<&'a crate::runtime_routing::RoutingEventReporter>,
    turn_outcomes: Option<&'a crate::dialog_turn::DialogTurnObserver>,
    terminal_signal: crate::dialog_turn::TurnSignalPolicy<'a>,
    obligations: Option<&'a dyn crate::dialog_turn::DeliveryObligationAnnotator>,
}

async fn process_dialog_job_once_in_queue_with_materializer_history_and_retry_at<
    Queue,
    Provider,
    Effects,
    Materializer,
    ToolHistory,
>(
    queue: &Queue,
    provider: &Provider,
    effects: &Effects,
    materializer: &Materializer,
    tool_history: &ToolHistory,
    options: DialogJobProcessOptions<'_>,
) -> DialogJobWorkerReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
    Materializer: DialogInputMaterializer + Sync + ?Sized,
    ToolHistory: DialogToolCallHistoryStore + Sync + ?Sized,
{
    let mut report = DialogJobWorkerReport {
        queue_name: options.queue_name.to_owned(),
        ..DialogJobWorkerReport::default()
    };
    let item = match queue.dequeue_dialog_job(options.queue_name).await {
        Ok(item) => item,
        Err(error) => {
            report.dequeue_error = Some(error.to_string());
            return report;
        }
    };

    let Some(item) = item else {
        return report;
    };
    report.dequeued = true;
    report.job_id = Some(item.id);
    report.provider = Some(provider.provider_name().to_owned());

    let budget = crate::dialog_turn::TurnBudget::from_events(
        &item.events,
        options.turn_budget_secs,
        options.now,
    );

    let params = match dialog_job_params_from_stateless_job(&item.job) {
        Ok(params) => params,
        Err(error) => {
            let error = error.to_string();
            report.decode_error = Some(error.clone());
            let resolution = crate::dialog_turn::TurnResolution {
                outcome: crate::dialog_turn::TurnOutcome::SkippedDecodeError {
                    error: error.clone(),
                },
                disposition: crate::dialog_turn::JobDisposition::Fail(error),
            };
            crate::dialog_turn::finalize_turn(
                queue,
                &item,
                resolution,
                &budget,
                options.now,
                options.turn_outcomes,
                options.terminal_signal,
                options.obligations,
                &mut report,
            )
            .await;
            return report;
        }
    };
    report.chat_id = (params.chat_id != 0).then_some(params.chat_id);
    report.thread_id = params.thread_id;
    report.user_id = (params.user_id != 0).then_some(params.user_id);
    report.message_id = (params.message_id != 0).then_some(params.message_id);

    let resolution = crate::dialog_turn::execute_dialog_turn(
        crate::dialog_turn::TurnContext {
            item: &item,
            params: &params,
            queue_name: options.queue_name,
            max_llm_job_attempts: options.max_llm_job_attempts,
            max_queue_age: TimeDuration::seconds(i64::from(options.turn_max_queue_age_secs.max(1))),
            max_regenerations: options.max_regenerations,
            budget,
            now: options.now,
            routing_events: options.routing_events,
        },
        queue,
        provider,
        effects,
        materializer,
        tool_history,
        &mut report,
    )
    .await;

    crate::dialog_turn::finalize_turn(
        queue,
        &item,
        resolution,
        &budget,
        options.now,
        options.turn_outcomes,
        options.terminal_signal,
        options.obligations,
        &mut report,
    )
    .await;
    report
}

pub async fn run_dialog_job_worker_every_until<Queue, Provider, Effects, Stop>(
    queue: &Queue,
    provider: &Provider,
    effects: &Effects,
    queue_names: &'static [&'static str],
    interval: Duration,
    stop: Stop,
) -> DialogJobWorkerRunReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
    Stop: Future<Output = ()>,
{
    run_dialog_job_worker_with_materializer_every_until(
        queue,
        provider,
        effects,
        &BasicDialogInputMaterializer,
        queue_names,
        interval,
        stop,
    )
    .await
}

pub async fn run_dialog_job_worker_with_materializer_every_until<
    Queue,
    Provider,
    Effects,
    Materializer,
    Stop,
>(
    queue: &Queue,
    provider: &Provider,
    effects: &Effects,
    materializer: &Materializer,
    queue_names: &'static [&'static str],
    interval: Duration,
    stop: Stop,
) -> DialogJobWorkerRunReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
    Materializer: DialogInputMaterializer + Sync + ?Sized,
    Stop: Future<Output = ()>,
{
    let noop = NoopDialogToolCallHistoryStore;
    run_dialog_job_worker_with_materializer_and_history_every_until(
        queue,
        provider,
        effects,
        DialogJobWorkerLoopOptions {
            materializer,
            tool_history: &noop,
            routing_events: None,
            turn_outcomes: None,
            queue_names,
            interval,
            max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
        },
        stop,
    )
    .await
}

/// Run dialog taskman workers with custom input materialization and tool-call history storage.
pub async fn run_dialog_job_worker_with_materializer_and_history_every_until<
    Queue,
    Provider,
    Effects,
    Materializer,
    ToolHistory,
    Stop,
>(
    queue: &Queue,
    provider: &Provider,
    effects: &Effects,
    options: DialogJobWorkerLoopOptions<'_, Materializer, ToolHistory>,
    stop: Stop,
) -> DialogJobWorkerRunReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
    Materializer: DialogInputMaterializer + Sync + ?Sized,
    ToolHistory: DialogToolCallHistoryStore + Sync + ?Sized,
    Stop: Future<Output = ()>,
{
    let mut report = DialogJobWorkerRunReport::default();
    let mut stop = std::pin::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            () = tokio::time::sleep(options.interval) => {
                for queue_name in options.queue_names {
                    let tick = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
                        queue,
                        provider,
                        effects,
                        options.materializer,
                        options.tool_history,
                        DialogJobProcessOptions {
                            queue_name,
                            max_llm_job_attempts: options.max_llm_job_attempts,
                            turn_budget_secs: options.turn_budget_secs,
                            turn_max_queue_age_secs: options.turn_max_queue_age_secs,
                            max_regenerations: options.max_regenerations,
                            now: OffsetDateTime::now_utc(),
                            routing_events: options.routing_events,
                            turn_outcomes: options.turn_outcomes,
                            terminal_signal: options.terminal_signal,
                            obligations: options.obligations,
                        },
                    ).await;
                    trace_dialog_job_tick(&tick);
                    report.record_tick(&tick);
                }
            }
        }
    }

    report
}

pub async fn run_dialog_job_worker_until<Queue, Provider, Effects, Stop>(
    queue: &Queue,
    provider: &Provider,
    effects: &Effects,
    stop: Stop,
) -> DialogJobWorkerRunReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
    Stop: Future<Output = ()>,
{
    run_dialog_job_worker_every_until(
        queue,
        provider,
        effects,
        &DIALOG_JOB_WORKER_QUEUES,
        DIALOG_JOB_POLL_INTERVAL,
        stop,
    )
    .await
}

pub async fn run_dialog_job_worker_with_materializer_until<
    Queue,
    Provider,
    Effects,
    Materializer,
    Stop,
>(
    queue: &Queue,
    provider: &Provider,
    effects: &Effects,
    materializer: &Materializer,
    stop: Stop,
) -> DialogJobWorkerRunReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
    Materializer: DialogInputMaterializer + Sync + ?Sized,
    Stop: Future<Output = ()>,
{
    run_dialog_job_worker_with_materializer_every_until(
        queue,
        provider,
        effects,
        materializer,
        &DIALOG_JOB_WORKER_QUEUES,
        DIALOG_JOB_POLL_INTERVAL,
        stop,
    )
    .await
}

pub async fn run_dialog_job_worker_with_materializer_and_history_until<
    Queue,
    Provider,
    Effects,
    Materializer,
    ToolHistory,
    Stop,
>(
    queue: &Queue,
    provider: &Provider,
    effects: &Effects,
    materializer: &Materializer,
    tool_history: &ToolHistory,
    stop: Stop,
) -> DialogJobWorkerRunReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
    Materializer: DialogInputMaterializer + Sync + ?Sized,
    ToolHistory: DialogToolCallHistoryStore + Sync + ?Sized,
    Stop: Future<Output = ()>,
{
    run_dialog_job_worker_with_materializer_and_history_until_with_max_attempts(
        queue,
        provider,
        effects,
        materializer,
        tool_history,
        DEFAULT_LLM_JOB_MAX_ATTEMPTS,
        stop,
    )
    .await
}

/// Run dialog taskman workers with configured retryable LLM attempt limit.
pub async fn run_dialog_job_worker_with_materializer_and_history_until_with_max_attempts<
    Queue,
    Provider,
    Effects,
    Materializer,
    ToolHistory,
    Stop,
>(
    queue: &Queue,
    provider: &Provider,
    effects: &Effects,
    materializer: &Materializer,
    tool_history: &ToolHistory,
    max_llm_job_attempts: i32,
    stop: Stop,
) -> DialogJobWorkerRunReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
    Materializer: DialogInputMaterializer + Sync + ?Sized,
    ToolHistory: DialogToolCallHistoryStore + Sync + ?Sized,
    Stop: Future<Output = ()>,
{
    run_dialog_job_worker_with_materializer_and_history_every_until(
        queue,
        provider,
        effects,
        DialogJobWorkerLoopOptions {
            materializer,
            tool_history,
            routing_events: None,
            turn_outcomes: None,
            queue_names: &DIALOG_JOB_WORKER_QUEUES,
            interval: DIALOG_JOB_POLL_INTERVAL,
            max_llm_job_attempts,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
        },
        stop,
    )
    .await
}

#[must_use]
pub fn dialog_job_has_payload(params: &DialogJobParams) -> bool {
    let meta = dialog_meta_from_value(&params.meta);
    !params.message_text.trim().is_empty()
        || !params.original_text.trim().is_empty()
        || !meta.message_type.trim().is_empty()
        || !meta.annotation.trim().is_empty()
        || !meta.vision_description.trim().is_empty()
        || !meta.attachments.is_empty()
}

#[must_use]
pub fn dialog_input_from_job_params_at(
    params: &DialogJobParams,
    now: OffsetDateTime,
) -> DialogInput {
    DialogInput {
        context: DialogContext {
            chat_id: params.chat_id,
            thread_id: params.thread_id,
            bot_name: "Plotva".to_owned(),
            ..DialogContext::default()
        },
        user: DialogUser {
            id: params.user_id,
            full_name: params.user_full_name.clone(),
        },
        message: DialogMessage {
            id: params.message_id,
            text: params.message_text.clone(),
            normalized: params.message_text.clone(),
            original_text: params.original_text.clone(),
            timestamp: Some(now),
            meta: dialog_meta_from_value(&params.meta),
            ..DialogMessage::default()
        },
        timestamp: Some(now),
        max_output_tokens: params.max_output_tokens,
        ..DialogInput::default()
    }
}

impl PostgresDialogInputMaterializer {
    async fn materialize(&self, params: &DialogJobParams, now: OffsetDateTime) -> DialogInput {
        let settings = self.load_settings(params.chat_id).await;
        let chat = self.load_chat(params.chat_id).await;
        let user = self.load_user(params.user_id).await;
        let history = self.load_history(params, now).await;
        let reference_context = self.load_reference_context(params).await;
        let shield_context = self.load_shield_context(params, &history).await;
        let mut input = dialog_input_from_materialized_context(
            params,
            now,
            &self.bot,
            Some(&settings),
            chat,
            user,
            history,
        );
        input.reference_context = reference_context;
        input.shield_context = shield_context;
        if let Some(vision) = self.vision.as_ref() {
            input = vision.materialize_dialog_vision_input(input, now).await;
        }
        input
    }

    async fn load_settings(&self, chat_id: i64) -> ChatSettings {
        match self.settings.get_chat_settings(chat_id).await {
            Ok(Some(settings)) => settings,
            Ok(None) => ChatSettings::defaults(chat_id),
            Err(error) => {
                tracing::debug!(
                    %error,
                    chat_id,
                    "failed to load chat settings for dialog input; using defaults"
                );
                ChatSettings::defaults(chat_id)
            }
        }
    }

    async fn load_chat(&self, chat_id: i64) -> Option<openplotva_core::ChatState> {
        match self.settings.get_chat_state(chat_id).await {
            Ok(chat) => chat,
            Err(error) => {
                tracing::debug!(
                    %error,
                    chat_id,
                    "failed to load chat state for dialog input"
                );
                None
            }
        }
    }

    async fn load_memory_chat_meta(&self, chat_id: i64) -> DialogMemoryChatMeta {
        if chat_id == 0 {
            return DialogMemoryChatMeta::default();
        }
        match self.settings.get_dialog_memory_chat_meta(chat_id).await {
            Ok(Some(meta)) => meta,
            Ok(None) => DialogMemoryChatMeta::default(),
            Err(error) => {
                tracing::debug!(
                    %error,
                    chat_id,
                    "failed to load chat memory metadata for dialog input"
                );
                DialogMemoryChatMeta::default()
            }
        }
    }

    async fn load_user(&self, user_id: i64) -> Option<openplotva_core::UserState> {
        if user_id == 0 {
            return None;
        }
        match self.identity.get_user_state(user_id).await {
            Ok(user) => user,
            Err(error) => {
                tracing::debug!(
                    %error,
                    user_id,
                    "failed to load user state for dialog input"
                );
                None
            }
        }
    }

    async fn load_history(
        &self,
        params: &DialogJobParams,
        now: OffsetDateTime,
    ) -> Vec<HistoryMessage> {
        if params.chat_id == 0 {
            return Vec::new();
        }

        let thread_id = params.thread_id.unwrap_or_default();
        let chat_reset_at = self
            .history_reset_at(params.chat_id, 0)
            .await
            .unwrap_or(None)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);
        let thread_reset_at = if thread_id == 0 {
            OffsetDateTime::UNIX_EPOCH
        } else {
            self.history_reset_at(params.chat_id, thread_id)
                .await
                .unwrap_or(None)
                .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        };
        let ttl_cutoff = now - DIALOG_HISTORY_TTL;
        let chat_cutoff = max_offset_datetime(ttl_cutoff, chat_reset_at);
        let thread_cutoff = max_offset_datetime(ttl_cutoff, thread_reset_at);

        let chat_payloads = match self
            .history
            .recent_chat_history_payloads(
                params.chat_id,
                chat_cutoff,
                thread_id,
                thread_reset_at,
                DIALOG_HISTORY_FETCH_LIMIT,
            )
            .await
        {
            Ok(payloads) => payloads,
            Err(error) => {
                tracing::debug!(
                    %error,
                    chat_id = params.chat_id,
                    "failed to load chat history payloads for dialog input"
                );
                Vec::new()
            }
        };
        let thread_payloads = if thread_id == 0 {
            Vec::new()
        } else {
            match self
                .history
                .recent_thread_history_payloads(
                    params.chat_id,
                    thread_id,
                    thread_cutoff,
                    DIALOG_HISTORY_FETCH_LIMIT,
                )
                .await
            {
                Ok(payloads) => payloads,
                Err(error) => {
                    tracing::debug!(
                        %error,
                        chat_id = params.chat_id,
                        thread_id,
                        "failed to load thread history payloads for dialog input"
                    );
                    Vec::new()
                }
            }
        };

        merge_dialog_history_payloads(&chat_payloads, &thread_payloads, self.bot.id)
    }

    async fn load_reference_context(&self, params: &DialogJobParams) -> Vec<String> {
        let Some(memory) = self.memory.as_ref() else {
            return Vec::new();
        };
        if params.message_text.trim().is_empty() {
            return Vec::new();
        }

        let meta = self.load_memory_chat_meta(params.chat_id).await;
        let Some(request) = dialog_memory_retrieval_request(
            params,
            meta.chat_type,
            meta.username,
            meta.active_usernames,
        ) else {
            return Vec::new();
        };

        let query_embedding = self.memory_query_embedding(&request.query, params).await;
        match memory
            .retrieve_with_vector(&request, query_embedding.as_ref())
            .await
        {
            Ok(memory) => dialog_reference_context_from_memory(&format_memory_context(&memory)),
            Err(error) => {
                tracing::warn!(
                    %error,
                    chat_id = params.chat_id,
                    user_id = params.user_id,
                    "memory retrieval failed for dialog input"
                );
                Vec::new()
            }
        }
    }

    async fn memory_query_embedding(
        &self,
        query: &str,
        params: &DialogJobParams,
    ) -> Option<openplotva_storage::PgEmbeddingVector> {
        let embedder = self.memory_embedder.as_ref()?;
        match embedder
            .embed_one(
                query,
                self.memory_embedding_dim,
                memory_retrieval_query_task(),
            )
            .await
        {
            Ok(embedding) => embedding,
            Err(error) => {
                tracing::warn!(
                    %error,
                    chat_id = params.chat_id,
                    user_id = params.user_id,
                    "memory query embedding failed; using lexical-only retrieval"
                );
                None
            }
        }
    }

    async fn load_shield_context(
        &self,
        params: &DialogJobParams,
        history: &[HistoryMessage],
    ) -> String {
        let Some(shield) = self.shield.as_ref() else {
            return String::new();
        };
        let query = build_dialog_shield_query_text(
            params,
            history,
            self.shield_options.query_max_chars,
            self.shield_history_tail_messages,
        );
        if query.trim().is_empty() {
            return String::new();
        }

        let query_embedding = self.shield_query_embedding(&query, params).await;
        match shield
            .search_with_vector(
                &ShieldSearchRequest {
                    query,
                    max_matches: self.shield_options.max_matches,
                    include_candidates: false,
                },
                &self.shield_options,
                query_embedding.as_ref(),
            )
            .await
        {
            Ok(result) => {
                if !result.matches.is_empty() {
                    tracing::info!(
                        chat_id = params.chat_id,
                        user_id = params.user_id,
                        message_id = params.message_id,
                        lexical_only = result.lexical_only,
                        matches = result.matches.len(),
                        "shield retrieval matched"
                    );
                }
                result.context
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    chat_id = params.chat_id,
                    user_id = params.user_id,
                    "shield retrieval failed for dialog input"
                );
                String::new()
            }
        }
    }

    async fn shield_query_embedding(
        &self,
        query: &str,
        params: &DialogJobParams,
    ) -> Option<openplotva_storage::PgEmbeddingVector> {
        let embedder = self.shield_embedder.as_ref()?;
        match embedder
            .embed_one(
                query,
                self.shield_options.embedding_dim,
                openplotva_shield::QUERY_EMBEDDING_TASK,
            )
            .await
        {
            Ok(embedding) => embedding,
            Err(error) => {
                tracing::warn!(
                    %error,
                    chat_id = params.chat_id,
                    user_id = params.user_id,
                    "shield query embedding failed; using lexical-only retrieval"
                );
                None
            }
        }
    }

    async fn history_reset_at(
        &self,
        chat_id: i64,
        thread_id: i32,
    ) -> Result<Option<OffsetDateTime>, openplotva_storage::StorageError> {
        self.history.history_reset_at(chat_id, thread_id).await
    }
}

#[must_use]
pub fn dialog_input_from_materialized_context(
    params: &DialogJobParams,
    now: OffsetDateTime,
    bot: &DialogBotIdentity,
    settings: Option<&ChatSettings>,
    chat: Option<openplotva_core::ChatState>,
    user: Option<openplotva_core::UserState>,
    history: Vec<HistoryMessage>,
) -> DialogInput {
    let mut input = dialog_input_from_job_params_at(params, now);
    input.context.bot_name = bot.name.clone();
    input.context.locale = dialog_locale(user.as_ref());
    input.context.chat_title = chat
        .and_then(|chat| chat.title)
        .map(|title| title.trim().to_owned())
        .filter(|title| !title.is_empty())
        .unwrap_or_default();
    input.persona = dialog_persona(params.chat_id, now, settings, &bot.name);
    input.history = history;
    let (reply_to_id, reply_to_name) =
        current_message_reply_context(&input.history, params.message_id);
    input.message.reply_to_id = reply_to_id;
    input.message.reply_to_name = reply_to_name;
    input.message.meta = dialog_message_meta(params, input.message.meta);
    input
}

/// Answer text of one provider round. Queued generation results deliberately
/// contribute nothing here: the artifact is the reply, and
/// `classify_reply_material` resolves such turns as `Delegated`.
#[must_use]
pub fn dialog_job_answer(output: &DialogOutput) -> String {
    let answer = output.answer.trim();
    if !answer.is_empty() {
        return answer.to_owned();
    }
    output.response.trim().to_owned()
}

#[must_use]
pub fn prepare_dialog_chat_response(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let decoded = decode_html_entities(&strip_code_fences(trimmed));
    if dialog_response_requires_rich(&decoded) {
        sanitize_rich_html(&decoded).trim().to_owned()
    } else {
        sanitize_telegram_html(&decoded).trim().to_owned()
    }
}

/// Mirror of the outbound boundary checks: will `queue_text_message_parts` /
/// `queue_rich_message` accept this answer verbatim? The turn engine verifies
/// deliverability before queueing so rejected answers regenerate instead of
/// failing terminally at send time.
pub fn validate_dialog_answer_deliverable(
    answer: &str,
) -> Result<(), openplotva_telegram::OutboundBuildError> {
    if dialog_response_requires_rich(answer) {
        let html = openplotva_telegram::format_rich_html(answer);
        if html.is_empty() {
            return Err(openplotva_telegram::OutboundBuildError::EmptyText);
        }
        if !openplotva_telegram::rich_message_within_char_limit(&html) {
            return Err(openplotva_telegram::OutboundBuildError::RichMessageTooLong(
                html.chars().count(),
                openplotva_telegram::RICH_MESSAGE_MAX_CHARS,
            ));
        }
        Ok(())
    } else {
        openplotva_telegram::validate_text_message_text(answer, TELEGRAM_PARSE_MODE_HTML)
    }
}

#[must_use]
pub fn dialog_response_requires_rich(value: &str) -> bool {
    const RICH_ONLY_TAGS: &[&str] = &[
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "p",
        "footer",
        "hr",
        "ul",
        "ol",
        "li",
        "table",
        "tr",
        "td",
        "th",
        "details",
        "summary",
        "figure",
        "figcaption",
        "img",
        "video",
        "audio",
        "tg-math",
        "tg-reference",
    ];
    let lower = value.to_ascii_lowercase();
    RICH_ONLY_TAGS
        .iter()
        .any(|tag| html_contains_tag(&lower, tag))
}

fn html_contains_tag(lower_html: &str, tag: &str) -> bool {
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    if lower_html.contains(&close) {
        return true;
    }
    let mut search_from = 0;
    while let Some(offset) = lower_html[search_from..].find(&open) {
        let after_tag = search_from + offset + open.len();
        match lower_html.as_bytes().get(after_tag) {
            None | Some(b'>') | Some(b'/') | Some(b' ') | Some(b'\t' | b'\n' | b'\r') => {
                return true;
            }
            Some(_) => {
                search_from = after_tag;
            }
        }
    }
    false
}

fn strip_code_fences(value: &str) -> String {
    let trimmed = value.trim();
    if let Some(body) = strip_triple_code_fence(trimmed) {
        return body;
    }
    if trimmed.starts_with('`') && trimmed.ends_with('`') && trimmed.len() >= 2 {
        return trimmed.trim().trim_matches('`').to_owned();
    }
    trimmed.to_owned()
}

fn strip_triple_code_fence(value: &str) -> Option<String> {
    if !value.starts_with("```") {
        return None;
    }
    let end = value.rfind("```")?;
    if end <= 2 {
        return None;
    }

    let mut body = value[3..end].trim();
    if let Some((head, rest)) = body.split_once('\n')
        && head.len() < 16
        && is_code_fence_language(head)
    {
        body = rest.trim();
    }
    Some(body.trim().to_owned())
}

fn is_code_fence_language(head: &str) -> bool {
    head.chars().all(
        |ch| matches!(ch, ' ' | '\t' | '-' | '_' | '.' | '+' | '0'..='9' | 'A'..='Z' | 'a'..='z'),
    )
}

#[must_use]
pub fn should_suppress_duplicate_bot_reply(
    history: &[HistoryMessage],
    candidate: &str,
) -> (i32, bool) {
    let normalized_candidate = normalize_comparable_chat_text(candidate);
    if normalized_candidate.is_empty() {
        return (0, false);
    }

    for entry in conversation_projection(history) {
        if entry.role != ROLE_MODEL {
            continue;
        }
        let previous = comparable_history_entry_text(&entry);
        if previous.is_empty() {
            continue;
        }
        if previous == normalized_candidate {
            return (entry.message_id, true);
        }
        return (0, false);
    }

    (0, false)
}

#[must_use]
pub fn normalize_comparable_chat_text(text: &str) -> String {
    let text = text.trim();
    if text.is_empty() {
        return String::new();
    }

    collapse_comparable_whitespace(&clean_unicode_non_printables(&decode_html_entities(
        &strip_html_tags_to_spaces(&ensure_telegram_safe_text(text)),
    )))
}

fn comparable_history_entry_text(entry: &HistoryMessage) -> String {
    let original = normalize_comparable_chat_text(&entry.original_text);
    if !original.is_empty() {
        return original;
    }
    normalize_comparable_chat_text(&entry.text)
}

fn strip_html_tags_to_spaces(input: &str) -> String {
    if input.is_empty() {
        return String::new();
    }
    let mut out = String::with_capacity(input.len());
    let mut idx = 0;
    while idx < input.len() {
        let ch = input[idx..].chars().next().expect("char boundary");
        if ch == '<'
            && let Some(end) = input[idx + 1..].find('>')
            && end > 0
        {
            out.push(' ');
            idx += end + 2;
            continue;
        }
        out.push(ch);
        idx += ch.len_utf8();
    }
    out
}

fn collapse_comparable_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(crate) async fn persist_dialog_tool_calls<Store>(
    store: &Store,
    params: &DialogJobParams,
    tool_calls: &[ToolCall],
) -> Result<bool, Store::Error>
where
    Store: DialogToolCallHistoryStore + Sync + ?Sized,
{
    if params.chat_id == 0 || params.message_id == 0 || tool_calls.is_empty() {
        return Ok(false);
    }
    let filtered = filter_dialog_tool_calls_for_history(tool_calls);
    if filtered.is_empty() {
        return Ok(false);
    }
    store
        .upsert_dialog_tool_calls(params.chat_id, params.message_id, &filtered)
        .await
}

fn dialog_meta_from_value(value: &serde_json::Value) -> ChatMessageMeta {
    if value.is_null() {
        return ChatMessageMeta::default();
    }
    serde_json::from_value(value.clone()).unwrap_or_default()
}

fn dialog_locale(user: Option<&openplotva_core::UserState>) -> String {
    user.and_then(|user| user.language_code.as_ref())
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or("ru")
        .to_owned()
}

fn dialog_persona(
    chat_id: i64,
    now: OffsetDateTime,
    settings: Option<&ChatSettings>,
    bot_name: &str,
) -> Persona {
    let daily_persona = || {
        let mut persona = daily_persona_for_unix_timestamp(chat_id, now.unix_timestamp());
        if persona.name.trim().is_empty() {
            persona.name = non_blank_or(bot_name.trim(), "Plotva");
        }
        persona
    };
    let Some(settings) = settings else {
        return Persona {
            mood: "neutral".to_owned(),
            persona: Some(daily_persona()),
            ..Persona::default()
        };
    };
    let custom_persona = settings
        .custom_persona
        .as_ref()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_default();
    Persona {
        mood: settings
            .mood_alignment
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or("neutral")
            .to_owned(),
        persona: custom_persona.is_empty().then(daily_persona),
        custom_persona,
        profanity: settings.enable_profanity,
        obscenifier: settings.enable_obscenifier,
        reactivity: settings.reactivity_percentage,
        proactivity: settings.proactivity_percentage,
    }
}

fn dialog_message_meta(params: &DialogJobParams, mut meta: ChatMessageMeta) -> ChatMessageMeta {
    meta.tool_calls = filter_non_terminator_tool_calls(&meta.tool_calls);
    if meta.message_type.trim().is_empty() {
        meta.message_type = detect_dialog_message_type(&params.message_text, &meta);
    }
    if meta.sender_type.trim().is_empty() {
        meta.sender_type = SENDER_TYPE_USER.to_owned();
    }
    if meta.sender_id == 0 {
        meta.sender_id = params.user_id;
    }
    if meta.sender_name.trim().is_empty() {
        meta.sender_name = params.user_full_name.trim().to_owned();
    }
    meta
}

fn detect_dialog_message_type(message_text: &str, meta: &ChatMessageMeta) -> String {
    for attachment in &meta.attachments {
        if attachment.source.trim() != "message" {
            continue;
        }
        if !attachment.kind.trim().is_empty() {
            return attachment.kind.trim().to_owned();
        }
    }
    if !message_text.trim().is_empty() {
        return "text".to_owned();
    }
    "text".to_owned()
}

fn merge_dialog_history_payloads(
    chat_payloads: &[Vec<u8>],
    thread_payloads: &[Vec<u8>],
    bot_id: i64,
) -> Vec<HistoryMessage> {
    let mut merged = Vec::with_capacity(chat_payloads.len() + thread_payloads.len());
    let mut seen = HashSet::<String>::with_capacity(merged.capacity());
    append_unique_history_payloads(&mut merged, &mut seen, chat_payloads, true, bot_id);
    append_unique_history_payloads(&mut merged, &mut seen, thread_payloads, false, bot_id);
    merged.sort_by(history_message_newer);
    merged
}

fn append_unique_history_payloads(
    out: &mut Vec<HistoryMessage>,
    seen: &mut HashSet<String>,
    payloads: &[Vec<u8>],
    include_empty_key: bool,
    bot_id: i64,
) {
    for payload in payloads {
        let Some(message) = history_message_from_payload(payload, bot_id) else {
            continue;
        };
        let key = history_message_key(&message);
        if key.is_empty() && !include_empty_key {
            continue;
        }
        if !seen.insert(key) {
            continue;
        }
        out.push(message);
    }
}

fn history_message_from_payload(payload: &[u8], bot_id: i64) -> Option<HistoryMessage> {
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let object = value.as_object()?;
    let mut meta = object
        .get("meta")
        .and_then(|value| serde_json::from_value::<ChatMessageMeta>(value.clone()).ok())
        .unwrap_or_default();
    meta.tool_calls = filter_non_terminator_tool_calls(&meta.tool_calls);

    let tool_call = object
        .get("tool_call")
        .and_then(|value| serde_json::from_value::<ToolCall>(value.clone()).ok());
    if tool_call
        .as_ref()
        .is_some_and(|call| is_dialog_history_noise_tool_call_name(&call.name))
    {
        return None;
    }

    let kind = normalized_history_kind(
        string_field(object, "kind"),
        &tool_call,
        string_field(object, "role"),
    );
    let sender_id = non_zero_i64(meta.sender_id)
        .or_else(|| nested_i64(object, "from", "id"))
        .or_else(|| nested_i64(object, "sender_chat", "id"))
        .unwrap_or_default();
    if sender_id == 0 && tool_call.is_none() {
        return None;
    }
    if meta.sender_id == 0 {
        meta.sender_id = sender_id;
    }
    if meta.sender_type.trim().is_empty() {
        meta.sender_type = if object.contains_key("sender_chat") {
            history_payload_sender_chat_type(object)
        } else {
            SENDER_TYPE_USER.to_owned()
        };
    }
    if meta.sender_name.trim().is_empty() {
        meta.sender_name = history_payload_sender_name(object);
    }
    if meta.sender_username.trim().is_empty() {
        meta.sender_username = nested_string(object, "from", "username")
            .or_else(|| nested_string(object, "sender_chat", "username"))
            .unwrap_or_default();
    }

    let role = history_payload_role(&kind, string_field(object, "role"), sender_id, bot_id);
    let (reply_to_id, reply_to_name) = history_payload_reply_context(object);
    let name = if meta.sender_name.trim().is_empty() && role == ROLE_TOOL {
        tool_call
            .as_ref()
            .map(|call| call.name.trim().to_owned())
            .unwrap_or_default()
    } else {
        meta.sender_name.trim().to_owned()
    };

    Some(HistoryMessage {
        entry_id: string_field(object, "entry_id").trim().to_owned(),
        role,
        kind,
        name,
        text: string_field(object, "text"),
        original_text: string_field(object, "original_text"),
        timestamp: history_payload_timestamp(object),
        message_id: i32_field(object, "message_id"),
        thread_id: i32_field(object, "message_thread_id"),
        user_id: sender_id,
        reply_to_id,
        reply_to_name,
        meta,
        tool_call,
    })
}

fn normalized_history_kind(kind: String, tool_call: &Option<ToolCall>, role: String) -> String {
    if !kind.trim().is_empty() {
        return kind;
    }
    if tool_call.is_some() {
        if role == ROLE_TOOL {
            return MESSAGE_KIND_TOOL_RESPONSE.to_owned();
        }
        return MESSAGE_KIND_TOOL_REQUEST.to_owned();
    }
    MESSAGE_KIND_TEXT.to_owned()
}

fn history_payload_role(kind: &str, stored_role: String, sender_id: i64, bot_id: i64) -> String {
    match kind {
        MESSAGE_KIND_TOOL_RESPONSE => ROLE_TOOL.to_owned(),
        MESSAGE_KIND_TOOL_REQUEST => ROLE_MODEL.to_owned(),
        MESSAGE_KIND_TEXT => {
            if bot_id != 0 && sender_id == bot_id {
                ROLE_MODEL.to_owned()
            } else {
                ROLE_USER.to_owned()
            }
        }
        _ if stored_role == ROLE_TOOL => ROLE_TOOL.to_owned(),
        _ if bot_id != 0 && sender_id == bot_id => ROLE_MODEL.to_owned(),
        _ => ROLE_USER.to_owned(),
    }
}

fn history_payload_timestamp(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Option<OffsetDateTime> {
    object
        .get("timestamp")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
        .or_else(|| {
            object
                .get("date")
                .and_then(serde_json::Value::as_i64)
                .and_then(|value| OffsetDateTime::from_unix_timestamp(value).ok())
        })
}

fn current_message_reply_context(history: &[HistoryMessage], message_id: i32) -> (i32, String) {
    if message_id == 0 {
        return (0, String::new());
    }
    history
        .iter()
        .find(|item| item.message_id == message_id && item.kind == MESSAGE_KIND_TEXT)
        .map(|item| (item.reply_to_id, item.reply_to_name.clone()))
        .unwrap_or_default()
}

fn history_payload_reply_context(
    object: &serde_json::Map<String, serde_json::Value>,
) -> (i32, String) {
    let Some(reply) = object
        .get("reply_to_message")
        .and_then(serde_json::Value::as_object)
    else {
        return (0, String::new());
    };
    (
        i32_field(reply, "message_id"),
        history_payload_sender_name(reply),
    )
}

fn history_payload_sender_name(object: &serde_json::Map<String, serde_json::Value>) -> String {
    nested_display_name(object, "from")
        .or_else(|| nested_string(object, "sender_chat", "title"))
        .unwrap_or_default()
}

fn history_payload_sender_chat_type(object: &serde_json::Map<String, serde_json::Value>) -> String {
    let sender_chat_id = nested_i64(object, "sender_chat", "id");
    let chat_id = nested_i64(object, "chat", "id");
    if sender_chat_id.is_some() && sender_chat_id == chat_id {
        SENDER_TYPE_SAME_CHAT.to_owned()
    } else {
        SENDER_TYPE_CHANNEL.to_owned()
    }
}

fn nested_display_name(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<String> {
    let first = nested_string(object, key, "first_name").unwrap_or_default();
    let last = nested_string(object, key, "last_name").unwrap_or_default();
    let name = format!("{} {}", first.trim(), last.trim())
        .trim()
        .to_owned();
    (!name.is_empty()).then_some(name)
}

fn history_message_newer(left: &HistoryMessage, right: &HistoryMessage) -> Ordering {
    match (left.timestamp, right.timestamp) {
        (Some(left), Some(right)) if left != right => return right.cmp(&left),
        (Some(_), None) => return Ordering::Less,
        (None, Some(_)) => return Ordering::Greater,
        _ => {}
    }
    right
        .message_id
        .cmp(&left.message_id)
        .then_with(|| history_kind_order(right).cmp(&history_kind_order(left)))
}

fn history_kind_order(message: &HistoryMessage) -> i32 {
    match message.kind.as_str() {
        MESSAGE_KIND_TOOL_RESPONSE => 3,
        MESSAGE_KIND_TOOL_REQUEST => 2,
        MESSAGE_KIND_TEXT => 1,
        _ => 0,
    }
}

fn history_message_key(message: &HistoryMessage) -> String {
    if !message.entry_id.trim().is_empty() {
        return message.entry_id.trim().to_owned();
    }
    if message.message_id != 0 {
        return message.message_id.to_string();
    }
    String::new()
}

fn max_offset_datetime(left: OffsetDateTime, right: OffsetDateTime) -> OffsetDateTime {
    left.max(right)
}

fn non_blank_or(value: &str, fallback: &str) -> String {
    if value.trim().is_empty() {
        fallback.to_owned()
    } else {
        value.trim().to_owned()
    }
}

fn string_field(object: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    object
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn i32_field(object: &serde_json::Map<String, serde_json::Value>, key: &str) -> i32 {
    object
        .get(key)
        .and_then(serde_json::Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or_default()
}

fn nested_string(
    object: &serde_json::Map<String, serde_json::Value>,
    parent: &str,
    key: &str,
) -> Option<String> {
    object
        .get(parent)
        .and_then(serde_json::Value::as_object)
        .and_then(|nested| nested.get(key))
        .and_then(serde_json::Value::as_str)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn nested_i64(
    object: &serde_json::Map<String, serde_json::Value>,
    parent: &str,
    key: &str,
) -> Option<i64> {
    object
        .get(parent)
        .and_then(serde_json::Value::as_object)
        .and_then(|nested| nested.get(key))
        .and_then(serde_json::Value::as_i64)
}

fn non_zero_i64(value: i64) -> Option<i64> {
    (value != 0).then_some(value)
}

fn dialog_work_item_from_taskman(item: TaskQueueWorkItem) -> DialogJobWorkItem {
    DialogJobWorkItem {
        id: item.id,
        job: item.job,
        events: item.events,
    }
}

fn is_dialog_job(job: &StatelessJobItem) -> bool {
    job.data.job_type == JobType::Dialog
}

/// Ledger reason codes for one retryable failure class: the string recorded
/// while attempts remain, and the string recorded on exhaustion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RetryReasonCodes {
    pub scheduled: &'static str,
    pub exhausted: &'static str,
}

/// Retryable provider error that escaped the routed walker.
pub(crate) const PROVIDER_ERROR_RETRY_CODES: RetryReasonCodes = RetryReasonCodes {
    scheduled: "provider_retryable",
    exhausted: "retry_exhausted",
};
/// Raw text existed but collapsed during outbound sanitization.
pub(crate) const SANITIZED_EMPTY_RETRY_CODES: RetryReasonCodes = RetryReasonCodes {
    scheduled: "sanitized_empty",
    exhausted: "sanitized_empty_exhausted",
};
/// Answer, response, and tool material were all empty.
pub(crate) const PROVIDER_EMPTY_RETRY_CODES: RetryReasonCodes = RetryReasonCodes {
    scheduled: "provider_empty",
    exhausted: "provider_empty_exhausted",
};
/// The outbound boundary would reject the answer verbatim.
pub(crate) const UNDELIVERABLE_RETRY_CODES: RetryReasonCodes = RetryReasonCodes {
    scheduled: "undeliverable_answer",
    exhausted: "undeliverable_answer_exhausted",
};

pub(crate) struct RetryableDialogProviderFailure<'a> {
    pub queue_name: &'a str,
    pub provider_name: &'a str,
    pub reason: openplotva_llm::retry::FailureReason,
    pub codes: RetryReasonCodes,
    pub error: &'a str,
    pub max_attempts: i32,
    pub now: OffsetDateTime,
    pub budget_deadline: OffsetDateTime,
}

/// Decide retry vs terminal for one retryable failure. Appends the retry job
/// event (append failure never blocks the resolution — the job must not stick
/// in `Processing`) and returns the resolution; `finalize_turn` applies it.
pub(crate) async fn handle_retryable_dialog_provider_error<Queue>(
    queue: &Queue,
    item: &DialogJobWorkItem,
    params: &DialogJobParams,
    routing_events: Option<&crate::runtime_routing::RoutingEventReporter>,
    failure: RetryableDialogProviderFailure<'_>,
    report: &mut DialogJobWorkerReport,
) -> crate::dialog_turn::TurnResolution
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let attempt = next_dialog_llm_job_attempt(&item.events);
    let max_attempts = failure.max_attempts.max(1);
    let provider = infer_dialog_retry_provider(failure.provider_name, failure.queue_name);
    let target_queue = retryable_dialog_job_target_queue(failure.queue_name);
    let attempts_exhausted = attempt >= max_attempts;
    let budget_exhausted = failure.now >= failure.budget_deadline;
    let exhausted = attempts_exhausted || budget_exhausted;
    let stage = if exhausted {
        LLM_JOB_RETRY_EXHAUSTED_STAGE
    } else {
        LLM_JOB_RETRY_STAGE
    };
    let mut event = dialog_retry_job_event(
        stage,
        attempt,
        max_attempts,
        &provider,
        failure.reason,
        &target_queue,
        failure.error,
    );
    if exhausted {
        event.message = if budget_exhausted && !attempts_exhausted {
            "retryable LLM provider error exhausted the turn budget".to_owned()
        } else {
            "retryable LLM provider error exhausted job attempts".to_owned()
        };
    }

    report.retryable_provider_error = Some(failure.reason.to_string());
    report.retry_attempt = Some(attempt);
    report.retry_max_attempts = Some(max_attempts);
    report.retry_target_queue = Some(target_queue.clone());

    if let Err(error) = queue
        .append_dialog_job_event(item.id, event, failure.now)
        .await
    {
        report.status_error = Some(error.to_string());
    }

    if exhausted {
        report.retry_exhausted = true;
        if let Some(reporter) = routing_events {
            reporter.record(dialog_retry_exhausted_routing_event(
                item,
                params,
                &provider,
                &target_queue,
                &failure,
                attempt,
                max_attempts,
            ));
        }
        let reason = if budget_exhausted && !attempts_exhausted {
            "budget_exhausted"
        } else {
            failure.codes.exhausted
        };
        return crate::dialog_turn::TurnResolution {
            outcome: crate::dialog_turn::TurnOutcome::TerminalFailed {
                reason,
                error: failure.error.to_owned(),
                user_signal: crate::dialog_turn::UserSignalPlan::React,
            },
            disposition: crate::dialog_turn::JobDisposition::Fail(failure.error.to_owned()),
        };
    }

    crate::dialog_turn::TurnResolution {
        outcome: crate::dialog_turn::TurnOutcome::RetryScheduled {
            reason: failure.codes.scheduled,
            attempt,
            max_attempts,
            target_queue: target_queue.clone(),
        },
        disposition: crate::dialog_turn::JobDisposition::Requeue(target_queue),
    }
}

pub(crate) async fn record_dialog_fallback_event<Queue>(
    queue: &Queue,
    job_id: i64,
    output: &DialogOutput,
    now: OffsetDateTime,
    report: &mut DialogJobWorkerReport,
) where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let Some(event) = dialog_fallback_job_event(output) else {
        return;
    };
    match queue.append_dialog_job_event(job_id, event, now).await {
        Ok(()) => report.recorded_dialog_fallback_event = true,
        Err(error) => {
            let error = error.to_string();
            tracing::debug!(%error, job_id, "failed to append dialog fallback event");
            report.dialog_fallback_event_error = Some(error);
        }
    }
}

fn dialog_fallback_job_event(output: &DialogOutput) -> Option<TaskQueueJobEvent> {
    let primary_provider = output.fallback_from.trim();
    let primary_error = output.fallback_error.trim();
    if primary_provider.is_empty() || primary_error.is_empty() {
        return None;
    }
    let fallback_provider = output.provider.trim();
    let fallback_reason = openplotva_llm::retry::retryable_reason_from_message(primary_error)
        .map(|reason| reason.to_string())
        .unwrap_or_default();
    let mut data = BTreeMap::new();
    data.insert("fallback_provider".to_owned(), fallback_provider.to_owned());
    data.insert("fallback_reason".to_owned(), fallback_reason);

    Some(TaskQueueJobEvent {
        level: "warn".to_owned(),
        stage: "dialog_fallback".to_owned(),
        provider: primary_provider.to_owned(),
        message: "primary dialog backend failed, trying fallback".to_owned(),
        error: primary_error.to_owned(),
        data,
        ..TaskQueueJobEvent::default()
    })
}

pub(crate) fn next_dialog_llm_job_attempt(events: &[TaskQueueJobEvent]) -> i32 {
    events
        .iter()
        .filter(|event| {
            event.stage == LLM_JOB_RETRY_STAGE || event.stage == LLM_JOB_RETRY_EXHAUSTED_STAGE
        })
        .map(|event| event.attempt)
        .max()
        .unwrap_or(0)
        + 1
}

fn retryable_dialog_job_target_queue(queue_name: &str) -> String {
    if queue_name.trim().is_empty() {
        DIALOG_AIFARM_QUEUE_NAME.to_owned()
    } else {
        queue_name.to_owned()
    }
}

fn infer_dialog_retry_provider(provider_name: &str, queue_name: &str) -> String {
    let provider_name = provider_name.trim();
    if !provider_name.is_empty() {
        return provider_name.to_owned();
    }
    if queue_name == DIALOG_AIFARM_QUEUE_NAME {
        "aifarm".to_owned()
    } else {
        "llm".to_owned()
    }
}

fn dialog_retry_job_event(
    stage: &str,
    attempt: i32,
    max_attempts: i32,
    provider: &str,
    reason: openplotva_llm::retry::FailureReason,
    target_queue: &str,
    error: &str,
) -> TaskQueueJobEvent {
    let mut data = BTreeMap::new();
    data.insert("fallback_reason".to_owned(), reason.to_string());
    data.insert("max_attempts".to_owned(), max_attempts.to_string());
    data.insert("target_queue".to_owned(), target_queue.to_owned());

    TaskQueueJobEvent {
        level: "warn".to_owned(),
        stage: stage.to_owned(),
        attempt,
        provider: provider.to_owned(),
        message: "retryable LLM provider error, requeueing job".to_owned(),
        error: error.to_owned(),
        data,
        ..TaskQueueJobEvent::default()
    }
}

fn dialog_retry_exhausted_routing_event(
    item: &DialogJobWorkItem,
    params: &DialogJobParams,
    provider: &str,
    target_queue: &str,
    failure: &RetryableDialogProviderFailure<'_>,
    attempt: i32,
    max_attempts: i32,
) -> crate::runtime_routing::RoutingEvent {
    crate::runtime_routing::RoutingEvent {
        severity: "error".to_owned(),
        event_type: "all_attempts_exhausted".to_owned(),
        workflow_key: "dialog".to_owned(),
        provider_id: None,
        model_id: None,
        queue_name: Some(failure.queue_name.to_owned()),
        job_id: Some(item.id),
        chat_id: (params.chat_id != 0).then_some(params.chat_id),
        thread_id: params.thread_id,
        message_id: (params.message_id != 0).then_some(params.message_id),
        dedupe_key: format!("all_attempts_exhausted:dialog:job_retry_exhausted:{provider}"),
        summary: "dialog job retry budget exhausted".to_owned(),
        detail: serde_json::json!({
            "job_attempts": attempt,
            "max_attempts": max_attempts,
            "last_retryable_reason": failure.reason.as_str(),
            "provider": provider,
            "target_queue": target_queue,
        }),
    }
}

fn trace_dialog_job_tick(tick: &DialogJobWorkerReport) {
    if !tick.dequeued
        && tick.dequeue_error.is_none()
        && tick.decode_error.is_none()
        && tick.provider_error.is_none()
        && tick.send_error.is_none()
        && tick.empty_answer_error.is_none()
        && tick.dialog_fallback_event_error.is_none()
        && tick.status_error.is_none()
        && tick.user_signal_error.is_none()
    {
        return;
    }

    tracing::debug!(
        queue_name = tick.queue_name,
        job_id = tick.job_id,
        provider = tick.provider.as_deref(),
        dequeued = tick.dequeued,
        completed = tick.completed,
        failed = tick.failed,
        skipped_empty_payload = tick.skipped_empty_payload,
        skipped_queue_backlog = tick.skipped_queue_backlog,
        content_blocked = tick.content_blocked,
        sent_answer = tick.sent_answer,
        suppressed_duplicate_message_id = tick.suppressed_duplicate_message_id,
        persisted_tool_call_history = tick.persisted_tool_call_history,
        recorded_dialog_fallback_event = tick.recorded_dialog_fallback_event,
        retry_requeued = tick.retry_requeued,
        retry_exhausted = tick.retry_exhausted,
        retry_attempt = tick.retry_attempt,
        retry_max_attempts = tick.retry_max_attempts,
        retry_target_queue = tick.retry_target_queue.as_deref(),
        dequeue_error = tick.dequeue_error.as_deref(),
        decode_error = tick.decode_error.as_deref(),
        retryable_provider_error = tick.retryable_provider_error.as_deref(),
        provider_error = tick.provider_error.as_deref(),
        send_error = tick.send_error.as_deref(),
        empty_answer_error = tick.empty_answer_error.as_deref(),
        tool_call_history_error = tick.tool_call_history_error.as_deref(),
        dialog_fallback_event_error = tick.dialog_fallback_event_error.as_deref(),
        status_error = tick.status_error.as_deref(),
        regenerations = tick.regenerations,
        user_signal_error = tick.user_signal_error.as_deref(),
        "processed dialog taskman worker tick"
    );

    if tick.empty_answer_error.is_some() && tick.retry_exhausted {
        tracing::warn!(
            queue_name = tick.queue_name,
            job_id = tick.job_id,
            provider = tick.provider.as_deref(),
            retry_attempt = tick.retry_attempt,
            empty_answer_error = tick.empty_answer_error.as_deref(),
            "dialog turn produced no sendable answer after retries; no reply sent"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        io,
        sync::{Arc, Mutex},
    };

    use openplotva_core::ChatAttachment;
    use openplotva_dialog::DialogOutput;
    use openplotva_llm::{
        ChatProviderFuture, ContentBlockedError,
        retry::{FailureReason, ProviderError},
    };
    use openplotva_taskman::{DialogJobParams, HIGH_PRIORITY, JobStatus, new_dialog_job_at};

    use super::*;

    #[tokio::test]
    async fn dialog_worker_runs_provider_sends_answer_and_completes_job()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            TEXT_QUEUE_NAME,
            new_dialog_job_at(dialog_params("hello"), now).with_priority(HIGH_PRIORITY),
        );
        let provider = ProviderStub::returning(DialogOutput {
            provider: "stub".to_owned(),
            answer: "  pong  ".to_owned(),
            ..DialogOutput::default()
        });
        let effects = EffectsStub::default();

        let report =
            process_dialog_job_once_in_queue_at(&queue, TEXT_QUEUE_NAME, &provider, &effects, now)
                .await;

        assert_eq!(report.job_id, Some(job_id));
        assert!(report.sent_answer);
        assert!(report.completed);
        assert!(!report.failed);
        assert_eq!(
            effects.sent(),
            vec![("hello".to_owned(), "pong".to_owned())]
        );
        let input = provider.inputs().pop().expect("provider input");
        assert_eq!(input.context.chat_id, 42);
        assert_eq!(input.context.thread_id, Some(9));
        assert_eq!(input.message.text, "hello");
        assert_eq!(input.message.normalized, "hello");
        assert_eq!(input.max_output_tokens, 512);
        assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
        Ok(())
    }

    #[tokio::test]
    async fn dialog_dispatcher_effects_allow_sending_without_deleted_reply_target()
    -> Result<(), Box<dyn Error>> {
        let queue = Arc::new(DispatcherQueue::new(
            openplotva_telegram::DispatcherConfig::default(),
        ));
        let effects = DialogDispatcherEffects::new(Arc::clone(&queue))
            .with_virtual_id_factory(Arc::new(|| "dialog-v1".to_owned()));

        effects
            .send_dialog_answer(&dialog_params("hello"), "pong")
            .await?;

        let item = queue.dequeue_immediate().expect("queued dialog answer");
        assert_eq!(
            item.method_kind(),
            Some(openplotva_telegram::TelegramOutboundMethodKind::SendMessage)
        );
        assert!(
            item.metadata().protected,
            "dialog answers must survive queue trims"
        );
        assert!(
            item.metadata().fingerprint_key.ends_with(":r100"),
            "debounce key is reply-scoped, got {}",
            item.metadata().fingerprint_key
        );
        let (_, method) = item.into_parts();
        let Some(openplotva_telegram::TelegramOutboundMethod::SendMessage(method)) = method else {
            return Err("expected sendMessage".into());
        };
        let value = serde_json::to_value(method.as_ref())?;
        assert_eq!(value["chat_id"], 42);
        assert_eq!(value["text"], "pong");
        assert_eq!(value["reply_parameters"]["message_id"], 100);
        assert_eq!(
            value["reply_parameters"]["allow_sending_without_reply"],
            true
        );
        Ok(())
    }

    #[tokio::test]
    async fn dialog_dispatcher_effects_routes_rich_only_html_to_rich_queue()
    -> Result<(), Box<dyn Error>> {
        let queue = Arc::new(DispatcherQueue::new(
            openplotva_telegram::DispatcherConfig::default(),
        ));
        let effects = DialogDispatcherEffects::new(Arc::clone(&queue))
            .with_virtual_id_factory(Arc::new(|| "dialog-v1".to_owned()));

        effects
            .send_dialog_answer(&dialog_params("hello"), "<h2>pong</h2>")
            .await?;

        let item = queue.dequeue_immediate().expect("queued dialog answer");
        assert_eq!(
            item.method_kind(),
            Some(openplotva_telegram::TelegramOutboundMethodKind::SendRichMessage)
        );
        assert!(
            item.metadata().protected,
            "rich dialog answers must survive queue trims"
        );
        assert!(
            item.metadata().fingerprint_key.ends_with(":r100"),
            "debounce key is reply-scoped, got {}",
            item.metadata().fingerprint_key
        );
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_records_go_fallback_event_before_completion()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("hello"), now),
        );
        let provider = ProviderStub::returning(DialogOutput {
            provider: "genkit".to_owned(),
            fallback_from: "aifarm".to_owned(),
            fallback_error: "aifarm provider provider_unavailable: status 503".to_owned(),
            answer: "pong".to_owned(),
            ..DialogOutput::default()
        });
        let effects = EffectsStub::default();

        let report = process_dialog_job_once_in_queue_at(
            &queue,
            DIALOG_AIFARM_QUEUE_NAME,
            &provider,
            &effects,
            now,
        )
        .await;

        assert!(report.recorded_dialog_fallback_event);
        assert_eq!(report.dialog_fallback_event_error, None);
        assert!(report.completed);
        let record = queue.record(job_id).expect("job record");
        let event = record
            .events
            .iter()
            .find(|event| event.stage == "dialog_fallback")
            .expect("dialog fallback event");
        assert_eq!(event.at, "2026-05-19T12:30:00Z");
        assert_eq!(event.level, "warn");
        assert_eq!(event.stage, "dialog_fallback");
        assert_eq!(event.provider, "aifarm");
        assert_eq!(
            event.message,
            "primary dialog backend failed, trying fallback"
        );
        assert_eq!(
            event.error,
            "aifarm provider provider_unavailable: status 503"
        );
        assert_eq!(event.data["fallback_provider"], "genkit");
        assert_eq!(event.data["fallback_reason"], "provider_unavailable");
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_persists_filtered_tool_call_history_before_answer()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("нарисуй кота"), now),
        );
        let provider = ProviderStub::returning(DialogOutput {
            provider: "stub".to_owned(),
            answer: "готово".to_owned(),
            tool_calls: vec![
                ToolCall {
                    name: "draw_image".to_owned(),
                    r#ref: "req-1".to_owned(),
                    input: Some(serde_json::json!({"prompt":"cat"})),
                    output: Some(serde_json::json!({"status":"queued"})),
                    ..ToolCall::default()
                },
                ToolCall {
                    name: "chat_history_summary".to_owned(),
                    ..ToolCall::default()
                },
                ToolCall {
                    name: "final_response".to_owned(),
                    ..ToolCall::default()
                },
            ],
            ..DialogOutput::default()
        });
        let effects = EffectsStub::default();
        let history = ToolHistoryStub::default();

        let report = process_dialog_job_once_in_queue_with_materializer_and_history_at(
            &queue,
            DIALOG_AIFARM_QUEUE_NAME,
            &provider,
            &effects,
            &BasicDialogInputMaterializer,
            &history,
            now,
        )
        .await;

        assert_eq!(report.job_id, Some(job_id));
        assert!(report.persisted_tool_call_history);
        assert_eq!(history.calls().len(), 1);
        assert_eq!(history.calls()[0].0, 42);
        assert_eq!(history.calls()[0].1, 100);
        assert_eq!(history.calls()[0].2.len(), 1);
        assert_eq!(history.calls()[0].2[0].name, "draw_image");
        assert!(report.sent_answer);
        assert!(report.completed);
        assert!(!report.failed);
        assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_completes_delegated_queued_song_without_sending_text()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("напиши песню"), now),
        );
        let provider = ProviderStub::returning(DialogOutput {
            provider: "stub".to_owned(),
            tool_calls: vec![ToolCall {
                name: "generate_song".to_owned(),
                r#ref: "generate_song-1".to_owned(),
                output: Some(serde_json::json!({
                    "status": "queued",
                    "side_effect": {
                        "kind": "music_generation_job",
                        "ticket_id": "9001",
                        "state": "queued"
                    }
                })),
                ..ToolCall::default()
            }],
            ..DialogOutput::default()
        });
        let effects = EffectsStub::default();
        let history = ToolHistoryStub::default();
        let outcomes = crate::dialog_turn::DialogTurnObserver::new(
            crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
            None,
        );

        let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &BasicDialogInputMaterializer,
            &history,
            DialogJobProcessOptions {
                queue_name: DIALOG_AIFARM_QUEUE_NAME,
                max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
                turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
                turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
                max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
                now,
                routing_events: None,
                turn_outcomes: Some(&outcomes),
                terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
                obligations: None,
            },
        )
        .await;

        assert_eq!(report.job_id, Some(job_id));
        assert!(report.persisted_tool_call_history);
        assert!(
            !report.sent_answer,
            "queued schedule is silent: the artifact is the reply"
        );
        assert!(report.completed);
        assert!(!report.failed);
        assert!(effects.sent().is_empty(), "no confirmation text is sent");
        assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
        use openplotva_server::{RuntimeTurnOutcomeInspector, RuntimeTurnOutcomesFilter};
        let recorded = outcomes
            .buffer()
            .turn_outcomes(RuntimeTurnOutcomesFilter {
                limit: 10,
                ..RuntimeTurnOutcomesFilter::default()
            })
            .expect("turn outcomes");
        assert_eq!(recorded.len(), 1);
        assert_eq!(
            recorded[0].outcome,
            crate::dialog_turn::TURN_OUTCOME_SIDE_EFFECT_DELEGATED
        );
        assert_eq!(
            recorded[0].side_effect_ticket_id,
            Some(9001),
            "delegated turn records the generation ticket"
        );
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_keeps_tool_call_history_failures_nonfatal() -> Result<(), Box<dyn Error>>
    {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("нарисуй кота"), now),
        );
        let provider = ProviderStub::returning(DialogOutput {
            answer: "готово".to_owned(),
            tool_calls: vec![ToolCall {
                name: "draw_image".to_owned(),
                ..ToolCall::default()
            }],
            ..DialogOutput::default()
        });
        let effects = EffectsStub::default();
        let history = ToolHistoryStub::failing();

        let report = process_dialog_job_once_in_queue_with_materializer_and_history_at(
            &queue,
            DIALOG_AIFARM_QUEUE_NAME,
            &provider,
            &effects,
            &BasicDialogInputMaterializer,
            &history,
            now,
        )
        .await;

        assert_eq!(
            report.tool_call_history_error,
            Some("history down".to_owned())
        );
        assert!(report.sent_answer);
        assert!(report.completed);
        assert!(!report.failed);
        assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
        Ok(())
    }

    fn duplicate_guard_history_fixture() -> Vec<HistoryMessage> {
        vec![
            HistoryMessage {
                message_id: 30,
                role: ROLE_USER.to_owned(),
                kind: MESSAGE_KIND_TEXT.to_owned(),
                text: "latest user message".to_owned(),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                message_id: 29,
                role: ROLE_MODEL.to_owned(),
                kind: MESSAGE_KIND_TEXT.to_owned(),
                text: "fallback".to_owned(),
                original_text: "<b>Привет, мир &amp; день!</b>".to_owned(),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                message_id: 28,
                role: ROLE_MODEL.to_owned(),
                kind: MESSAGE_KIND_TEXT.to_owned(),
                text: "older bot reply".to_owned(),
                ..HistoryMessage::default()
            },
        ]
    }

    fn duplicate_answer_output() -> DialogOutput {
        DialogOutput {
            answer: "  <i>Привет,   мир &amp; день!</i>  ".to_owned(),
            ..DialogOutput::default()
        }
    }

    #[tokio::test]
    async fn dialog_worker_regenerates_duplicate_answer_with_anti_loop_hint()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("что нового"), now),
        );
        let provider = SequenceProviderStub::returning(vec![
            duplicate_answer_output(),
            DialogOutput {
                answer: "Свежий ответ".to_owned(),
                ..DialogOutput::default()
            },
        ]);
        let effects = EffectsStub::default();
        let materializer = MaterializerStub::with_history(duplicate_guard_history_fixture());
        let outcomes = crate::dialog_turn::DialogTurnObserver::new(
            crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
            None,
        );

        let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &materializer,
            &NoopDialogToolCallHistoryStore,
            DialogJobProcessOptions {
                queue_name: DIALOG_AIFARM_QUEUE_NAME,
                max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
                turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
                turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
                max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
                terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
                obligations: None,
                now,
                routing_events: None,
                turn_outcomes: Some(&outcomes),
            },
        )
        .await;

        assert_eq!(report.job_id, Some(job_id));
        assert_eq!(report.regenerations, 1);
        assert_eq!(report.suppressed_duplicate_message_id, None);
        assert!(report.sent_answer);
        assert!(report.completed);
        assert!(!report.failed);
        assert_eq!(
            effects.sent(),
            vec![("что нового".to_owned(), "Свежий ответ".to_owned())]
        );
        let inputs = provider.inputs();
        assert_eq!(inputs.len(), 2);
        assert!(inputs[0].reference_context.is_empty());
        assert_eq!(
            inputs[1].reference_context,
            vec![openplotva_dialog::turn::ANTI_LOOP_HINT.to_owned()]
        );
        assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
        let record = queue.record(job_id).expect("job record");
        let regen_events: Vec<_> = record
            .events
            .iter()
            .filter(|event| event.stage == crate::dialog_turn::DIALOG_TURN_REGENERATE_STAGE)
            .collect();
        assert_eq!(regen_events.len(), 1);
        assert_eq!(regen_events[0].attempt, 1);
        assert_eq!(regen_events[0].data["reason"], "dedup_regenerate");
        assert_eq!(regen_events[0].data["duplicate_message_id"], "29");
        let rows = ledger_rows(&outcomes);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, "sent");
        assert_eq!(rows[0].detail["regenerations"], 1);
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_exhausts_regenerations_on_permanent_duplicate_and_signals()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("что нового"), now),
        );
        let provider = SequenceProviderStub::returning(vec![duplicate_answer_output()]);
        let effects = EffectsStub::default();
        let materializer = MaterializerStub::with_history(duplicate_guard_history_fixture());
        let signal = FakeSignal::default();
        let outcomes = crate::dialog_turn::DialogTurnObserver::new(
            crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
            None,
        );

        let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &materializer,
            &NoopDialogToolCallHistoryStore,
            DialogJobProcessOptions {
                queue_name: DIALOG_AIFARM_QUEUE_NAME,
                max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
                turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
                turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
                max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
                terminal_signal: crate::dialog_turn::TurnSignalPolicy::new(
                    Some(&signal),
                    "🤔",
                    600,
                ),
                obligations: None,
                now,
                routing_events: None,
                turn_outcomes: Some(&outcomes),
            },
        )
        .await;

        assert_eq!(report.regenerations, 2);
        assert_eq!(report.suppressed_duplicate_message_id, Some(29));
        assert!(report.failed);
        assert!(!report.completed);
        assert!(!report.sent_answer);
        assert!(effects.sent().is_empty());
        assert_eq!(
            provider.inputs().len(),
            3,
            "initial round + 2 regenerations"
        );
        assert_eq!(record_status(&queue, job_id), JobStatus::Failed);
        let calls = signal.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].emoji, "🤔");
        assert!(calls[0].text_fallback_allowed);
        let record = queue.record(job_id).expect("job record");
        let regen_events: Vec<_> = record
            .events
            .iter()
            .filter(|event| event.stage == crate::dialog_turn::DIALOG_TURN_REGENERATE_STAGE)
            .collect();
        assert_eq!(regen_events.len(), 2);
        assert!(
            record
                .events
                .iter()
                .any(|event| event.stage == crate::dialog_turn::TERMINAL_USER_SIGNAL_STAGE)
        );
        let rows = ledger_rows(&outcomes);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, "terminal_failed");
        assert_eq!(rows[0].reason.as_deref(), Some("duplicate_exhausted"));
        assert_eq!(rows[0].user_signal.as_deref(), Some("reaction_sent"));
        assert_eq!(rows[0].detail["regenerations"], 2);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn dialog_worker_skips_regeneration_when_budget_nearly_exhausted()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("что нового"), now),
        );
        // Generation eats all but ~9s of the 120s budget, below the 10s
        // regeneration floor: the duplicate goes terminal without a retry.
        let provider = SlowProviderStub::new(duplicate_answer_output(), Duration::from_secs(111));
        let effects = EffectsStub::default();
        let materializer = MaterializerStub::with_history(duplicate_guard_history_fixture());
        let signal = FakeSignal::default();
        let outcomes = crate::dialog_turn::DialogTurnObserver::new(
            crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
            None,
        );

        let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &materializer,
            &NoopDialogToolCallHistoryStore,
            DialogJobProcessOptions {
                queue_name: DIALOG_AIFARM_QUEUE_NAME,
                max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
                turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
                turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
                max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
                terminal_signal: crate::dialog_turn::TurnSignalPolicy::new(
                    Some(&signal),
                    "🤔",
                    600,
                ),
                obligations: None,
                now,
                routing_events: None,
                turn_outcomes: Some(&outcomes),
            },
        )
        .await;

        assert_eq!(report.regenerations, 0);
        assert!(report.failed);
        assert_eq!(provider.call_count(), 1);
        assert!(effects.sent().is_empty());
        assert_eq!(record_status(&queue, job_id), JobStatus::Failed);
        assert_eq!(signal.calls().len(), 1);
        let record = queue.record(job_id).expect("job record");
        assert!(
            record
                .events
                .iter()
                .all(|event| event.stage != crate::dialog_turn::DIALOG_TURN_REGENERATE_STAGE)
        );
        let rows = ledger_rows(&outcomes);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].reason.as_deref(), Some("duplicate_exhausted"));
        assert_eq!(rows[0].user_signal.as_deref(), Some("reaction_sent"));
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_signals_terminal_failure_with_configured_emoji()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("hello"), now),
        );
        let provider = ProviderStub::failing(io::Error::other("provider down"));
        let effects = EffectsStub::default();
        let signal = FakeSignal::default();
        let outcomes = crate::dialog_turn::DialogTurnObserver::new(
            crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
            None,
        );

        let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &BasicDialogInputMaterializer,
            &NoopDialogToolCallHistoryStore,
            DialogJobProcessOptions {
                queue_name: DIALOG_AIFARM_QUEUE_NAME,
                max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
                turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
                turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
                max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
                terminal_signal: crate::dialog_turn::TurnSignalPolicy::new(
                    Some(&signal),
                    "🤔",
                    600,
                ),
                obligations: None,
                now,
                routing_events: None,
                turn_outcomes: Some(&outcomes),
            },
        )
        .await;

        assert!(report.failed);
        let calls = signal.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].chat_id, 42);
        assert_eq!(calls[0].thread_id, Some(9));
        assert_eq!(calls[0].message_id, 100);
        assert_eq!(calls[0].emoji, "🤔");
        assert!(calls[0].text_fallback_allowed);
        let record = queue.record(job_id).expect("job record");
        let marker = record
            .events
            .iter()
            .find(|event| event.stage == crate::dialog_turn::TERMINAL_USER_SIGNAL_STAGE)
            .expect("terminal user signal marker");
        assert_eq!(marker.data["result"], "reaction_sent");
        assert_eq!(marker.data["emoji"], "🤔");
        let outcome_event = record
            .events
            .iter()
            .find(|event| event.stage == crate::dialog_turn::TURN_OUTCOME_STAGE)
            .expect("turn outcome event");
        assert_eq!(outcome_event.data["user_signal"], "reaction_sent");
        let rows = ledger_rows(&outcomes);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, "terminal_failed");
        assert_eq!(rows[0].user_signal.as_deref(), Some("reaction_sent"));
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_skips_signal_for_job_older_than_max_signal_age()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        // Old enough to be past the signal age gate, with a recent processing
        // anchor so the turn itself still runs (post-downtime backlog shape).
        let created = now - TimeDuration::seconds(700);
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("hello"), created),
        );
        queue.append_job_event(
            job_id,
            TaskQueueJobEvent {
                stage: crate::dialog_turn::TURN_STARTED_STAGE.to_owned(),
                ..TaskQueueJobEvent::default()
            },
            now - TimeDuration::seconds(30),
        )?;
        let provider = ProviderStub::failing(io::Error::other("provider down"));
        let effects = EffectsStub::default();
        let signal = FakeSignal::default();
        let outcomes = crate::dialog_turn::DialogTurnObserver::new(
            crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
            None,
        );

        let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &BasicDialogInputMaterializer,
            &NoopDialogToolCallHistoryStore,
            DialogJobProcessOptions {
                queue_name: DIALOG_AIFARM_QUEUE_NAME,
                max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
                turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
                turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
                max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
                terminal_signal: crate::dialog_turn::TurnSignalPolicy::new(
                    Some(&signal),
                    "🤔",
                    600,
                ),
                obligations: None,
                now,
                routing_events: None,
                turn_outcomes: Some(&outcomes),
            },
        )
        .await;

        assert!(report.failed);
        assert!(signal.calls().is_empty(), "no reaction on stale backlog");
        let record = queue.record(job_id).expect("job record");
        assert!(
            record
                .events
                .iter()
                .all(|event| event.stage != crate::dialog_turn::TERMINAL_USER_SIGNAL_STAGE)
        );
        let rows = ledger_rows(&outcomes);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].user_signal.as_deref(), Some("skipped_too_old"));
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_still_reacts_but_gates_text_fallback_after_prior_signal()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("hello"), now),
        );
        // A previous run delivered the signal before its status write failed.
        queue.append_job_event(
            job_id,
            TaskQueueJobEvent {
                stage: crate::dialog_turn::TERMINAL_USER_SIGNAL_STAGE.to_owned(),
                ..TaskQueueJobEvent::default()
            },
            now - TimeDuration::seconds(5),
        )?;
        let provider = ProviderStub::failing(io::Error::other("provider down"));
        let effects = EffectsStub::default();
        let signal = FakeSignal::default();

        let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &BasicDialogInputMaterializer,
            &NoopDialogToolCallHistoryStore,
            DialogJobProcessOptions {
                queue_name: DIALOG_AIFARM_QUEUE_NAME,
                max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
                turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
                turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
                max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
                terminal_signal: crate::dialog_turn::TurnSignalPolicy::new(
                    Some(&signal),
                    "🤔",
                    600,
                ),
                obligations: None,
                now,
                routing_events: None,
                turn_outcomes: None,
            },
        )
        .await;

        assert!(report.failed);
        let calls = signal.calls();
        assert_eq!(calls.len(), 1, "reaction is re-attempted (idempotent)");
        assert!(
            !calls[0].text_fallback_allowed,
            "marker gates the non-idempotent text fallback"
        );
        let record = queue.record(job_id).expect("job record");
        let markers = record
            .events
            .iter()
            .filter(|event| event.stage == crate::dialog_turn::TERMINAL_USER_SIGNAL_STAGE)
            .count();
        assert_eq!(markers, 1, "marker is not duplicated");
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_fails_when_answer_sanitizes_to_empty_after_attempts_exhausted()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("найди сообщения"), now),
        );
        let provider = ProviderStub::returning(DialogOutput {
            answer: "<tool_calls><tool_call name=\"history_search\"></tool_call></tool_calls>"
                .to_owned(),
            ..DialogOutput::default()
        });
        let effects = EffectsStub::default();

        let outcomes = crate::dialog_turn::DialogTurnObserver::new(
            crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
            None,
        );
        let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &BasicDialogInputMaterializer,
            &NoopDialogToolCallHistoryStore,
            DialogJobProcessOptions {
                queue_name: DIALOG_AIFARM_QUEUE_NAME,
                max_llm_job_attempts: 1,
                turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
                turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
                max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
                terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
                obligations: None,
                now,
                routing_events: None,
                turn_outcomes: Some(&outcomes),
            },
        )
        .await;

        assert!(report.failed);
        assert!(report.retry_exhausted);
        assert!(!report.completed);
        assert!(!report.sent_answer);
        assert!(effects.sent().is_empty());
        assert_eq!(
            report.empty_answer_error,
            Some("dialog answer became empty after sanitization".to_owned())
        );
        assert_eq!(record_status(&queue, job_id), JobStatus::Failed);
        let rows = ledger_rows(&outcomes);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, "terminal_failed");
        assert_eq!(rows[0].reason.as_deref(), Some("sanitized_empty_exhausted"));
        assert_eq!(rows[0].user_signal.as_deref(), Some("none"));
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_requeues_when_provider_answer_sanitizes_to_empty()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("найди сообщения"), now),
        );
        let provider = ProviderStub::returning(DialogOutput {
            answer: "<tool_calls><tool_call name=\"history_search\"></tool_call></tool_calls>"
                .to_owned(),
            ..DialogOutput::default()
        });
        let effects = EffectsStub::default();

        let report = process_dialog_job_once_at(&queue, &provider, &effects, now).await;

        assert!(report.retry_requeued);
        assert!(!report.failed);
        assert!(!report.sent_answer);
        assert!(effects.sent().is_empty());
        assert_eq!(report.retry_attempt, Some(1));
        assert_eq!(record_status(&queue, job_id), JobStatus::Pending);
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_skips_empty_payload_without_provider_call() -> Result<(), Box<dyn Error>>
    {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(empty_params(), now),
        );
        let provider = ProviderStub::returning(DialogOutput {
            answer: "unused".to_owned(),
            ..DialogOutput::default()
        });
        let effects = EffectsStub::default();

        let report = process_dialog_job_once_at(&queue, &provider, &effects, now).await;

        assert_eq!(report.job_id, Some(job_id));
        assert!(report.skipped_empty_payload);
        assert!(report.completed);
        assert!(provider.inputs().is_empty());
        assert!(effects.sent().is_empty());
        assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_fails_expired_queue_backlog_job_without_provider_call()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let created = now - TimeDuration::seconds(11 * 60);
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("old hello"), created),
        );
        let provider = ProviderStub::returning(DialogOutput {
            answer: "unused".to_owned(),
            ..DialogOutput::default()
        });
        let effects = EffectsStub::default();
        let outcomes = crate::dialog_turn::DialogTurnObserver::new(
            crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
            None,
        );

        let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &BasicDialogInputMaterializer,
            &NoopDialogToolCallHistoryStore,
            DialogJobProcessOptions {
                queue_name: DIALOG_AIFARM_QUEUE_NAME,
                max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
                turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
                turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
                max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
                terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
                obligations: None,
                now,
                routing_events: None,
                turn_outcomes: Some(&outcomes),
            },
        )
        .await;

        assert_eq!(report.job_id, Some(job_id));
        assert!(report.skipped_queue_backlog);
        assert!(report.failed);
        assert!(!report.completed);
        assert!(provider.inputs().is_empty());
        assert!(effects.sent().is_empty());
        let record = queue.record(job_id).expect("job record");
        assert_eq!(record.status, JobStatus::Failed);
        // The turn never started: no anchor event, only the outcome event.
        assert!(
            record
                .events
                .iter()
                .all(|event| event.stage != crate::dialog_turn::TURN_STARTED_STAGE)
        );
        let outcome_event = record
            .events
            .iter()
            .find(|event| event.stage == crate::dialog_turn::TURN_OUTCOME_STAGE)
            .expect("turn outcome event");
        assert_eq!(outcome_event.data["outcome"], "skipped");
        assert_eq!(outcome_event.data["reason"], "queue_backlog_expired");
        let rows = ledger_rows(&outcomes);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, "skipped");
        assert_eq!(rows[0].reason.as_deref(), Some("queue_backlog_expired"));
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_processes_old_job_that_already_started_processing()
    -> Result<(), Box<dyn Error>> {
        // A requeued retry older than the old 180s stale gate must keep answering:
        // the budget is anchored at the recorded turn_started event, not job.created.
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let created = now - TimeDuration::seconds(11 * 60);
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("late retry"), created),
        );
        queue.append_job_event(
            job_id,
            TaskQueueJobEvent {
                stage: crate::dialog_turn::TURN_STARTED_STAGE.to_owned(),
                ..TaskQueueJobEvent::default()
            },
            now - TimeDuration::seconds(30),
        )?;
        let provider = ProviderStub::returning(DialogOutput {
            answer: "pong".to_owned(),
            ..DialogOutput::default()
        });
        let effects = EffectsStub::default();

        let report = process_dialog_job_once_at(&queue, &provider, &effects, now).await;

        assert_eq!(report.job_id, Some(job_id));
        assert!(!report.skipped_queue_backlog);
        assert!(report.sent_answer);
        assert!(report.completed);
        assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_fails_turn_budget_exhausted_before_provider_call()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("hello"), now - TimeDuration::seconds(200)),
        );
        queue.append_job_event(
            job_id,
            TaskQueueJobEvent {
                stage: crate::dialog_turn::TURN_STARTED_STAGE.to_owned(),
                ..TaskQueueJobEvent::default()
            },
            now - TimeDuration::seconds(115),
        )?;
        let provider = ProviderStub::returning(DialogOutput {
            answer: "unused".to_owned(),
            ..DialogOutput::default()
        });
        let effects = EffectsStub::default();
        let outcomes = crate::dialog_turn::DialogTurnObserver::new(
            crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
            None,
        );

        let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &BasicDialogInputMaterializer,
            &NoopDialogToolCallHistoryStore,
            DialogJobProcessOptions {
                queue_name: DIALOG_AIFARM_QUEUE_NAME,
                max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
                turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
                turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
                max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
                terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
                obligations: None,
                now,
                routing_events: None,
                turn_outcomes: Some(&outcomes),
            },
        )
        .await;

        assert!(report.failed);
        assert!(provider.inputs().is_empty());
        assert!(effects.sent().is_empty());
        let record = queue.record(job_id).expect("job record");
        assert_eq!(record.status, JobStatus::Failed);
        assert!(
            record
                .error
                .as_deref()
                .is_some_and(|error| error.contains("budget exhausted"))
        );
        let rows = ledger_rows(&outcomes);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, "terminal_failed");
        assert_eq!(rows[0].reason.as_deref(), Some("budget_exhausted"));
        assert_eq!(rows[0].user_signal.as_deref(), Some("none"));
        assert_eq!(rows[0].budget_ms, Some(120_000));
        assert_eq!(rows[0].elapsed_ms, Some(115_000));
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_retries_provider_empty_output_and_fails_on_exhaustion()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("hello"), now),
        );
        let provider = ProviderStub::returning(DialogOutput::default());
        let effects = EffectsStub::default();
        let outcomes = crate::dialog_turn::DialogTurnObserver::new(
            crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
            None,
        );
        let options = DialogJobProcessOptions {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            max_llm_job_attempts: 2,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
            now,
            routing_events: None,
            turn_outcomes: Some(&outcomes),
        };

        let first = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &BasicDialogInputMaterializer,
            &NoopDialogToolCallHistoryStore,
            options,
        )
        .await;

        assert!(first.retry_requeued);
        assert!(first.answer_empty_all_sources);
        assert!(!first.completed);
        assert!(!first.failed);
        assert_eq!(first.retry_attempt, Some(1));
        assert_eq!(record_status(&queue, job_id), JobStatus::Pending);

        let second = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &BasicDialogInputMaterializer,
            &NoopDialogToolCallHistoryStore,
            options,
        )
        .await;

        assert!(second.retry_exhausted);
        assert!(second.failed);
        assert!(effects.sent().is_empty());
        assert_eq!(record_status(&queue, job_id), JobStatus::Failed);
        let rows = ledger_rows(&outcomes);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].outcome, "retry_scheduled");
        assert_eq!(rows[1].reason.as_deref(), Some("provider_empty"));
        assert_eq!(rows[1].attempt, 1);
        assert_eq!(rows[0].outcome, "terminal_failed");
        assert_eq!(rows[0].reason.as_deref(), Some("provider_empty_exhausted"));
        assert_eq!(rows[0].attempt, 2);
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_requeues_undeliverable_answer_instead_of_failing_at_send()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("напиши статью"), now),
        );
        let oversized_rich = format!("<p>{}</p>", "a".repeat(40_000));
        let provider = ProviderStub::returning(DialogOutput {
            answer: oversized_rich,
            ..DialogOutput::default()
        });
        let effects = EffectsStub::default();
        let outcomes = crate::dialog_turn::DialogTurnObserver::new(
            crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
            None,
        );
        let options = DialogJobProcessOptions {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            max_llm_job_attempts: 2,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
            now,
            routing_events: None,
            turn_outcomes: Some(&outcomes),
        };

        let first = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &BasicDialogInputMaterializer,
            &NoopDialogToolCallHistoryStore,
            options,
        )
        .await;

        assert!(first.retry_requeued);
        assert!(!first.failed);
        assert!(!first.sent_answer);
        assert!(effects.sent().is_empty());
        assert!(
            first
                .empty_answer_error
                .as_deref()
                .is_some_and(|error| error.contains("rejected by outbound validation"))
        );
        assert_eq!(record_status(&queue, job_id), JobStatus::Pending);

        let second = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &BasicDialogInputMaterializer,
            &NoopDialogToolCallHistoryStore,
            options,
        )
        .await;

        assert!(second.retry_exhausted);
        assert!(second.failed);
        assert_eq!(record_status(&queue, job_id), JobStatus::Failed);
        let rows = ledger_rows(&outcomes);
        assert_eq!(rows[1].reason.as_deref(), Some("undeliverable_answer"));
        assert_eq!(
            rows[0].reason.as_deref(),
            Some("undeliverable_answer_exhausted")
        );
        Ok(())
    }

    #[tokio::test]
    async fn retry_handler_exhausts_on_budget_deadline_despite_remaining_attempts()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("hello"), now),
        );
        let item = queue
            .dequeue_dialog_job(DIALOG_AIFARM_QUEUE_NAME)
            .await?
            .expect("work item");
        let params = dialog_params("hello");
        let mut report = DialogJobWorkerReport::default();

        let resolution = handle_retryable_dialog_provider_error(
            &queue,
            &item,
            &params,
            None,
            RetryableDialogProviderFailure {
                queue_name: DIALOG_AIFARM_QUEUE_NAME,
                provider_name: "aifarm",
                reason: FailureReason::ProviderUnavailable,
                codes: PROVIDER_ERROR_RETRY_CODES,
                error: "status 503",
                max_attempts: 5,
                now: now + TimeDuration::seconds(130),
                budget_deadline: now + TimeDuration::seconds(120),
            },
            &mut report,
        )
        .await;

        assert!(report.retry_exhausted);
        assert_eq!(report.retry_attempt, Some(1));
        assert_eq!(
            resolution.outcome,
            crate::dialog_turn::TurnOutcome::TerminalFailed {
                reason: "budget_exhausted",
                error: "status 503".to_owned(),
                user_signal: crate::dialog_turn::UserSignalPlan::React,
            }
        );
        assert_eq!(
            resolution.disposition,
            crate::dialog_turn::JobDisposition::Fail("status 503".to_owned())
        );
        let record = queue.record(job_id).expect("job record");
        let event = record.events.last().expect("retry event");
        assert_eq!(event.stage, LLM_JOB_RETRY_EXHAUSTED_STAGE);
        assert_eq!(
            event.message,
            "retryable LLM provider error exhausted the turn budget"
        );
        Ok(())
    }

    #[tokio::test]
    async fn event_append_failure_does_not_leave_job_processing() -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = FailingEventQueue::new();
        let job_id = queue.inner.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("x"), now),
        );
        let provider =
            RetryableProviderStub::new("aifarm", FailureReason::ProviderUnavailable, "status 503");
        let effects = EffectsStub::default();

        let report = process_dialog_job_once_in_queue_at(
            &queue,
            DIALOG_AIFARM_QUEUE_NAME,
            &provider,
            &effects,
            now,
        )
        .await;

        assert!(report.retry_requeued, "status write must win over events");
        assert!(report.status_error.is_some(), "append failure is recorded");
        assert!(!report.failed);
        let record = queue
            .inner
            .records()
            .into_iter()
            .find(|record| record.id == job_id)
            .expect("record");
        assert_eq!(record.status, JobStatus::Pending);
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_records_turn_started_anchor_on_first_attempt()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("hello"), now - TimeDuration::seconds(30)),
        );
        let provider = ProviderStub::returning(DialogOutput {
            answer: "pong".to_owned(),
            ..DialogOutput::default()
        });
        let effects = EffectsStub::default();

        let report = process_dialog_job_once_at(&queue, &provider, &effects, now).await;

        assert!(report.sent_answer);
        let record = queue.record(job_id).expect("job record");
        let anchor = record
            .events
            .iter()
            .find(|event| event.stage == crate::dialog_turn::TURN_STARTED_STAGE)
            .expect("turn_started event");
        assert_eq!(anchor.at, "2026-05-19T12:30:00Z");
        let outcome_event = record
            .events
            .iter()
            .find(|event| event.stage == crate::dialog_turn::TURN_OUTCOME_STAGE)
            .expect("turn outcome event");
        assert_eq!(outcome_event.data["outcome"], "sent");
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_completes_content_blocked_provider_error() -> Result<(), Box<dyn Error>>
    {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("x"), now),
        );
        let provider = ProviderStub::failing(ContentBlockedError::new("safety"));
        let effects = EffectsStub::default();

        let report = process_dialog_job_once_at(&queue, &provider, &effects, now).await;

        assert!(report.content_blocked);
        assert!(report.completed);
        assert!(!report.failed);
        assert!(effects.sent().is_empty());
        assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_marks_provider_and_send_errors_failed() -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let provider_queue = InMemoryTaskQueue::new();
        let provider_job = provider_queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("x"), now),
        );
        let provider = ProviderStub::failing(io::Error::other("provider down"));
        let effects = EffectsStub::default();

        let provider_report =
            process_dialog_job_once_at(&provider_queue, &provider, &effects, now).await;

        assert!(provider_report.failed);
        assert_eq!(
            provider_report.provider_error,
            Some("provider down".to_owned())
        );
        assert_eq!(
            record_status(&provider_queue, provider_job),
            JobStatus::Failed
        );

        let send_queue = InMemoryTaskQueue::new();
        let send_job = send_queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("x"), now),
        );
        let provider = ProviderStub::returning(DialogOutput {
            answer: "answer".to_owned(),
            ..DialogOutput::default()
        });
        let effects = EffectsStub::failing();

        let send_report = process_dialog_job_once_at(&send_queue, &provider, &effects, now).await;

        assert!(send_report.failed);
        assert_eq!(send_report.send_error, Some("send down".to_owned()));
        assert_eq!(record_status(&send_queue, send_job), JobStatus::Failed);
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_requeues_retryable_provider_error_like_go() -> Result<(), Box<dyn Error>>
    {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("x"), now),
        );
        let provider =
            RetryableProviderStub::new("gemini", FailureReason::ProviderOverloaded, "high demand");
        let effects = EffectsStub::default();

        let report = process_dialog_job_once_in_queue_at(
            &queue,
            DIALOG_AIFARM_QUEUE_NAME,
            &provider,
            &effects,
            now,
        )
        .await;

        assert!(report.retry_requeued);
        assert!(!report.failed);
        assert_eq!(
            report.retryable_provider_error,
            Some("provider_overloaded".to_owned())
        );
        assert_eq!(report.retry_attempt, Some(1));
        assert_eq!(
            report.retry_target_queue,
            Some(DIALOG_AIFARM_QUEUE_NAME.to_owned())
        );
        let record = queue.record(job_id).expect("job");
        assert_eq!(record.status, JobStatus::Pending);
        assert_eq!(record.queue_name, DIALOG_AIFARM_QUEUE_NAME);
        assert_eq!(record.error, None);
        assert_eq!(record.worker_id, None);
        assert_eq!(record.started_at, None);
        assert_eq!(record.completed_at, None);
        let event = record
            .events
            .iter()
            .find(|event| event.stage == LLM_JOB_RETRY_STAGE)
            .expect("retry event");
        // `at` carries the real generation wall time on top of the tick time.
        assert!(
            event.at.starts_with("2026-05-19T12:30:00"),
            "unexpected retry event time: {}",
            event.at
        );
        assert_eq!(event.level, "warn");
        assert_eq!(event.stage, LLM_JOB_RETRY_STAGE);
        assert_eq!(event.attempt, 1);
        assert_eq!(event.provider, "gemini");
        assert_eq!(
            event.message,
            "retryable LLM provider error, requeueing job"
        );
        assert_eq!(
            event.error,
            "gemini provider provider_overloaded: high demand"
        );
        assert_eq!(event.data["fallback_reason"], "provider_overloaded");
        assert_eq!(event.data["max_attempts"], "5");
        assert_eq!(event.data["target_queue"], DIALOG_AIFARM_QUEUE_NAME);
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_exhausts_retryable_provider_error_at_default_limit()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("x"), now),
        );
        for attempt in 1..DEFAULT_LLM_JOB_MAX_ATTEMPTS {
            queue.append_job_event(
                job_id,
                TaskQueueJobEvent {
                    stage: LLM_JOB_RETRY_STAGE.to_owned(),
                    attempt,
                    ..TaskQueueJobEvent::default()
                },
                now,
            )?;
        }
        let provider =
            RetryableProviderStub::new("aifarm", FailureReason::ProviderUnavailable, "status 503");
        let effects = EffectsStub::default();
        let reporter = crate::runtime_routing::RoutingEventReporter::new(
            crate::runtime_routing::RoutingEventBuffer::new(8),
            None,
            None,
            Duration::from_secs(600),
        );

        let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &BasicDialogInputMaterializer,
            &NoopDialogToolCallHistoryStore,
            DialogJobProcessOptions {
                queue_name: DIALOG_AIFARM_QUEUE_NAME,
                max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
                turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
                turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
                max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
                terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
                obligations: None,
                now,
                routing_events: Some(&reporter),
                turn_outcomes: None,
            },
        )
        .await;

        assert!(report.retry_exhausted);
        assert!(report.failed);
        assert_eq!(report.retry_attempt, Some(DEFAULT_LLM_JOB_MAX_ATTEMPTS));
        let record = queue.record(job_id).expect("job");
        assert_eq!(record.status, JobStatus::Failed);
        assert_eq!(
            record.error,
            Some("aifarm provider provider_unavailable: status 503".to_owned())
        );
        let retry_events: Vec<_> = record
            .events
            .iter()
            .filter(|event| {
                event.stage == LLM_JOB_RETRY_STAGE || event.stage == LLM_JOB_RETRY_EXHAUSTED_STAGE
            })
            .collect();
        assert_eq!(retry_events.len(), DEFAULT_LLM_JOB_MAX_ATTEMPTS as usize);
        let event = retry_events.last().expect("exhausted event");
        assert_eq!(event.stage, LLM_JOB_RETRY_EXHAUSTED_STAGE);
        assert_eq!(event.attempt, DEFAULT_LLM_JOB_MAX_ATTEMPTS);
        assert_eq!(
            event.message,
            "retryable LLM provider error exhausted job attempts"
        );
        assert_eq!(event.data["fallback_reason"], "provider_unavailable");
        let routing_events = reporter.buffer().routing_events(10);
        assert_eq!(routing_events.len(), 1);
        assert_eq!(routing_events[0].event_type, "all_attempts_exhausted");
        assert_eq!(routing_events[0].workflow_key, "dialog");
        assert_eq!(
            routing_events[0].queue_name.as_deref(),
            Some(DIALOG_AIFARM_QUEUE_NAME)
        );
        assert_eq!(routing_events[0].job_id, Some(job_id));
        assert_eq!(routing_events[0].chat_id, Some(42));
        assert_eq!(routing_events[0].message_id, Some(100));
        assert_eq!(
            routing_events[0].detail["last_retryable_reason"],
            "provider_unavailable"
        );
        Ok(())
    }

    #[tokio::test]
    async fn dialog_worker_uses_configured_retry_attempt_limit() -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = InMemoryTaskQueue::new();
        let job_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(dialog_params("x"), now),
        );
        let provider =
            RetryableProviderStub::new("aifarm", FailureReason::ProviderUnavailable, "status 503");
        let effects = EffectsStub::default();

        let first = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &BasicDialogInputMaterializer,
            &NoopDialogToolCallHistoryStore,
            DialogJobProcessOptions {
                queue_name: DIALOG_AIFARM_QUEUE_NAME,
                max_llm_job_attempts: 2,
                turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
                turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
                max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
                terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
                obligations: None,
                now,
                routing_events: None,
                turn_outcomes: None,
            },
        )
        .await;
        assert!(first.retry_requeued);
        assert_eq!(first.retry_max_attempts, Some(2));

        let second = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &BasicDialogInputMaterializer,
            &NoopDialogToolCallHistoryStore,
            DialogJobProcessOptions {
                queue_name: DIALOG_AIFARM_QUEUE_NAME,
                max_llm_job_attempts: 2,
                turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
                turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
                max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
                terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
                obligations: None,
                now,
                routing_events: None,
                turn_outcomes: None,
            },
        )
        .await;
        assert!(second.retry_exhausted);
        assert!(second.failed);
        assert_eq!(second.retry_attempt, Some(2));

        let record = queue.record(job_id).expect("job");
        assert_eq!(record.status, JobStatus::Failed);
        let retry_events: Vec<_> = record
            .events
            .iter()
            .filter(|event| {
                event.stage == LLM_JOB_RETRY_STAGE || event.stage == LLM_JOB_RETRY_EXHAUSTED_STAGE
            })
            .collect();
        assert_eq!(retry_events.len(), 2);
        assert_eq!(retry_events[1].stage, LLM_JOB_RETRY_EXHAUSTED_STAGE);
        assert_eq!(retry_events[1].data["max_attempts"], "2");
        Ok(())
    }

    #[test]
    fn dialog_job_has_payload_matches_go_text_meta_and_attachments() {
        assert!(!dialog_job_has_payload(&empty_params()));
        let mut original = empty_params();
        original.original_text = " original ".to_owned();
        assert!(dialog_job_has_payload(&original));

        let mut meta = empty_params();
        meta.meta = serde_json::json!({"vision_description":" image "});
        assert!(dialog_job_has_payload(&meta));

        let mut attachment = empty_params();
        attachment.meta = serde_json::to_value(ChatMessageMeta {
            attachments: vec![ChatAttachment {
                kind: "image".to_owned(),
                ..ChatAttachment::default()
            }],
            ..ChatMessageMeta::default()
        })
        .expect("meta json");
        assert!(dialog_job_has_payload(&attachment));
    }

    #[test]
    fn dialog_job_answer_prefers_answer_then_response() {
        assert_eq!(
            dialog_job_answer(&DialogOutput {
                answer: " answer ".to_owned(),
                response: "response".to_owned(),
                ..DialogOutput::default()
            }),
            "answer"
        );
        assert_eq!(
            dialog_job_answer(&DialogOutput {
                response: " response ".to_owned(),
                ..DialogOutput::default()
            }),
            "response"
        );
    }

    #[test]
    fn prepare_dialog_chat_response_matches_go_dialog_send_sanitizer() {
        assert_eq!(
            prepare_dialog_chat_response(r#"Он сказал "Привет" и ушел"#),
            r#"Он сказал "Привет" и ушел"#
        );
        assert_eq!(
            prepare_dialog_chat_response(r#"<b>Он сказал "Привет"</b><div> и ушел</div>"#),
            r#"<b>Он сказал "Привет"</b> и ушел"#
        );
        assert_eq!(prepare_dialog_chat_response("```\nhello\n```"), "hello");
        assert_eq!(
            prepare_dialog_chat_response("```json\n{\"ok\":true}\n```"),
            "{\"ok\":true}"
        );
        assert_eq!(
            prepare_dialog_chat_response("```go!\nprintln()\n```"),
            "go!\nprintln()"
        );
        assert_eq!(
            prepare_dialog_chat_response("```very-long-language-name\nbody\n```"),
            "very-long-language-name\nbody"
        );
        assert_eq!(prepare_dialog_chat_response("`hello`"), "hello");
    }

    #[test]
    fn dialog_rich_router_keeps_classic_telegram_html_classic() {
        assert!(!dialog_response_requires_rich("<pre>code</pre>"));
        assert!(!dialog_response_requires_rich(
            "<a href='https://example.test'>link</a>"
        ));
        assert!(dialog_response_requires_rich("<p>paragraph</p>"));
        assert!(dialog_response_requires_rich("<h2>Status</h2>"));
        assert!(dialog_response_requires_rich(
            "<table><tr><td>1</td></tr></table>"
        ));
    }

    #[test]
    fn duplicate_guard_matches_go_normalization_and_latest_model_policy() {
        let history = vec![
            HistoryMessage {
                message_id: 40,
                role: ROLE_USER.to_owned(),
                kind: MESSAGE_KIND_TEXT.to_owned(),
                text: "latest user message".to_owned(),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                message_id: 39,
                role: ROLE_MODEL.to_owned(),
                kind: MESSAGE_KIND_TEXT.to_owned(),
                text: "fresh answer".to_owned(),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                message_id: 38,
                role: ROLE_MODEL.to_owned(),
                kind: MESSAGE_KIND_TEXT.to_owned(),
                text: "same as candidate".to_owned(),
                ..HistoryMessage::default()
            },
        ];

        assert_eq!(
            should_suppress_duplicate_bot_reply(&history, "same as candidate"),
            (0, false)
        );

        let latest = vec![HistoryMessage {
            message_id: 29,
            role: ROLE_MODEL.to_owned(),
            kind: MESSAGE_KIND_TEXT.to_owned(),
            text: "Привет, мир & день!".to_owned(),
            original_text: "<b>Привет, мир &amp; день!</b>".to_owned(),
            ..HistoryMessage::default()
        }];

        assert_eq!(
            should_suppress_duplicate_bot_reply(&latest, "  <i>Привет,   мир &amp; день!</i>  "),
            (29, true)
        );
    }

    #[test]
    fn duplicate_guard_ignores_foreign_bot_user_role() {
        let history = vec![
            HistoryMessage {
                message_id: 11,
                role: ROLE_USER.to_owned(),
                kind: MESSAGE_KIND_TEXT.to_owned(),
                text: "чужой бот".to_owned(),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                message_id: 10,
                role: ROLE_MODEL.to_owned(),
                kind: MESSAGE_KIND_TEXT.to_owned(),
                text: "наша реплика".to_owned(),
                ..HistoryMessage::default()
            },
        ];

        assert_eq!(
            should_suppress_duplicate_bot_reply(&history, "чужой бот"),
            (0, false)
        );
    }

    #[test]
    fn materialized_dialog_input_fills_go_context_settings_history_and_meta()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let mut params = dialog_params(" hello ");
        params.meta = serde_json::to_value(ChatMessageMeta {
            attachments: vec![ChatAttachment {
                kind: "image".to_owned(),
                source: "message".to_owned(),
                ..ChatAttachment::default()
            }],
            tool_calls: vec![openplotva_core::ToolCall {
                name: "final_response".to_owned(),
                ..openplotva_core::ToolCall::default()
            }],
            ..ChatMessageMeta::default()
        })?;
        let settings = ChatSettings {
            mood_alignment: Some(" warm ".to_owned()),
            custom_persona: Some("  custom voice  ".to_owned()),
            reactivity_percentage: 9,
            proactivity_percentage: 2,
            enable_obscenifier: false,
            enable_profanity: false,
            ..ChatSettings::defaults(params.chat_id)
        };
        let history = vec![HistoryMessage {
            message_id: params.message_id,
            kind: MESSAGE_KIND_TEXT.to_owned(),
            reply_to_id: 55,
            reply_to_name: "Bob".to_owned(),
            ..HistoryMessage::default()
        }];

        let input = dialog_input_from_materialized_context(
            &params,
            now,
            &DialogBotIdentity::new(99, "Plotvabot"),
            Some(&settings),
            Some(openplotva_core::ChatState::new(
                params.chat_id,
                "supergroup",
                Some(" Team ".to_owned()),
                None,
                None,
                None,
                Some(true),
            )),
            Some(openplotva_core::UserState::new(
                params.user_id,
                "Ada",
                None,
                Some("ada".to_owned()),
                Some(" uk ".to_owned()),
                Some(true),
            )),
            history,
        );

        assert_eq!(input.context.bot_name, "Plotvabot");
        assert_eq!(input.context.chat_title, "Team");
        assert_eq!(input.context.locale, "uk");
        assert_eq!(input.persona.mood, "warm");
        assert_eq!(input.persona.custom_persona, "custom voice");
        assert_eq!(input.persona.persona, None);
        assert!(!input.persona.profanity);
        assert!(!input.persona.obscenifier);
        assert_eq!(input.persona.reactivity, 9);
        assert_eq!(input.persona.proactivity, 2);
        assert_eq!(input.message.reply_to_id, 55);
        assert_eq!(input.message.reply_to_name, "Bob");
        assert_eq!(input.message.meta.message_type, "image");
        assert_eq!(input.message.meta.sender_id, params.user_id);
        assert_eq!(input.message.meta.sender_name, "Ada");
        assert!(input.message.meta.tool_calls.is_empty());
        Ok(())
    }

    #[test]
    fn dialog_history_payload_merge_matches_go_dedupe_order_and_reply_context()
    -> Result<(), Box<dyn Error>> {
        let older = serde_json::json!({
            "entry_id": "msg:100",
            "kind": "text",
            "timestamp": "2024-03-09T16:00:00Z",
            "message_id": 100,
            "message_thread_id": 9,
            "text": "current",
            "from": {"id": 7, "first_name": "Ada", "username": "ada"},
            "reply_to_message": {
                "message_id": 55,
                "from": {"id": 8, "first_name": "Bob"}
            },
            "meta": {"sender_id": 7}
        });
        let duplicate_thread = serde_json::json!({
            "entry_id": "msg:100",
            "kind": "text",
            "timestamp": "2024-03-09T16:00:01Z",
            "message_id": 100,
            "text": "duplicate"
        });
        let newer_bot = serde_json::json!({
            "entry_id": "msg:101",
            "kind": "text",
            "timestamp": "2024-03-09T16:00:02Z",
            "message_id": 101,
            "text": "model",
            "from": {"id": 99, "first_name": "Plotva"},
            "meta": {"sender_id": 99}
        });

        let history = merge_dialog_history_payloads(
            &[serde_json::to_vec(&older)?, serde_json::to_vec(&newer_bot)?],
            &[serde_json::to_vec(&duplicate_thread)?],
            99,
        );

        assert_eq!(history.len(), 2);
        assert_eq!(history[0].entry_id, "msg:101");
        assert_eq!(history[0].role, ROLE_MODEL);
        assert_eq!(history[1].entry_id, "msg:100");
        assert_eq!(history[1].role, ROLE_USER);
        assert_eq!(history[1].reply_to_id, 55);
        assert_eq!(history[1].reply_to_name, "Bob");
        assert_eq!(history[1].meta.sender_name, "Ada");
        Ok(())
    }

    #[test]
    fn dialog_persona_uses_go_daily_catalogue_without_custom_persona() -> Result<(), Box<dyn Error>>
    {
        let now = OffsetDateTime::from_unix_timestamp(12_345 * 86_400)?;
        let settings = ChatSettings {
            custom_persona: None,
            ..ChatSettings::defaults(42)
        };

        let persona = dialog_persona(42, now, Some(&settings), "Plotvabot");

        assert_eq!(
            persona.persona,
            Some(openplotva_dialog::daily_persona_for_day(42, 12_345))
        );
        assert!(persona.custom_persona.is_empty());
        Ok(())
    }

    #[derive(Clone)]
    struct ProviderStub {
        state: Arc<Mutex<ProviderState>>,
    }

    struct ProviderState {
        result: Result<DialogOutput, Box<dyn Error + Send + Sync + 'static>>,
        inputs: Vec<DialogInput>,
    }

    impl ProviderStub {
        fn returning(output: DialogOutput) -> Self {
            Self {
                state: Arc::new(Mutex::new(ProviderState {
                    result: Ok(output),
                    inputs: Vec::new(),
                })),
            }
        }

        fn failing<E>(error: E) -> Self
        where
            E: Error + Send + Sync + 'static,
        {
            Self {
                state: Arc::new(Mutex::new(ProviderState {
                    result: Err(Box::new(error)),
                    inputs: Vec::new(),
                })),
            }
        }

        fn inputs(&self) -> Vec<DialogInput> {
            self.state.lock().expect("provider state").inputs.clone()
        }
    }

    impl ChatProvider for ProviderStub {
        fn provider_name(&self) -> &str {
            "stub"
        }

        fn run_dialog<'a>(&'a self, input: DialogInput) -> ChatProviderFuture<'a> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("provider state");
                state.inputs.push(input);
                match &state.result {
                    Ok(output) => Ok(output.clone()),
                    Err(error) => {
                        let error: Box<dyn Error + Send + Sync + 'static> =
                            Box::new(io::Error::other(error.to_string()));
                        Err(error)
                    }
                }
            })
        }
    }

    /// Provider returning scripted outputs in order, repeating the last one.
    #[derive(Clone)]
    struct SequenceProviderStub {
        state: Arc<Mutex<SequenceProviderState>>,
    }

    struct SequenceProviderState {
        outputs: Vec<DialogOutput>,
        inputs: Vec<DialogInput>,
    }

    impl SequenceProviderStub {
        fn returning(outputs: Vec<DialogOutput>) -> Self {
            assert!(!outputs.is_empty(), "sequence provider needs outputs");
            Self {
                state: Arc::new(Mutex::new(SequenceProviderState {
                    outputs,
                    inputs: Vec::new(),
                })),
            }
        }

        fn inputs(&self) -> Vec<DialogInput> {
            self.state.lock().expect("provider state").inputs.clone()
        }
    }

    impl ChatProvider for SequenceProviderStub {
        fn provider_name(&self) -> &str {
            "stub"
        }

        fn run_dialog<'a>(&'a self, input: DialogInput) -> ChatProviderFuture<'a> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("provider state");
                let round = state.inputs.len();
                state.inputs.push(input);
                let index = round.min(state.outputs.len() - 1);
                Ok(state.outputs[index].clone())
            })
        }
    }

    /// Provider that consumes (paused) tokio time before answering, so tests
    /// can drive the budget without real waiting.
    struct SlowProviderStub {
        output: DialogOutput,
        delay: Duration,
        calls: Arc<Mutex<usize>>,
    }

    impl SlowProviderStub {
        fn new(output: DialogOutput, delay: Duration) -> Self {
            Self {
                output,
                delay,
                calls: Arc::new(Mutex::new(0)),
            }
        }

        fn call_count(&self) -> usize {
            *self.calls.lock().expect("slow provider calls")
        }
    }

    impl ChatProvider for SlowProviderStub {
        fn provider_name(&self) -> &str {
            "stub"
        }

        fn run_dialog<'a>(&'a self, _input: DialogInput) -> ChatProviderFuture<'a> {
            Box::pin(async move {
                *self.calls.lock().expect("slow provider calls") += 1;
                tokio::time::sleep(self.delay).await;
                Ok(self.output.clone())
            })
        }
    }

    /// Recording terminal user signal with a scripted result.
    #[derive(Clone)]
    struct FakeSignal {
        state: Arc<Mutex<FakeSignalState>>,
    }

    struct FakeSignalState {
        calls: Vec<crate::dialog_turn::SignalTarget>,
        result: crate::dialog_turn::UserSignalResult,
    }

    impl Default for FakeSignal {
        fn default() -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeSignalState {
                    calls: Vec::new(),
                    result: crate::dialog_turn::UserSignalResult::ReactionSent,
                })),
            }
        }
    }

    impl FakeSignal {
        fn calls(&self) -> Vec<crate::dialog_turn::SignalTarget> {
            self.state.lock().expect("signal state").calls.clone()
        }
    }

    impl crate::dialog_turn::TerminalUserSignal for FakeSignal {
        fn signal_turn_failure<'a>(
            &'a self,
            target: crate::dialog_turn::SignalTarget,
        ) -> crate::dialog_turn::UserSignalFuture<'a> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("signal state");
                state.calls.push(target);
                state.result.clone()
            })
        }
    }

    struct RetryableProviderStub {
        provider: &'static str,
        reason: FailureReason,
        message: &'static str,
    }

    impl RetryableProviderStub {
        fn new(provider: &'static str, reason: FailureReason, message: &'static str) -> Self {
            Self {
                provider,
                reason,
                message,
            }
        }
    }

    impl ChatProvider for RetryableProviderStub {
        fn provider_name(&self) -> &str {
            self.provider
        }

        fn run_dialog<'a>(&'a self, _input: DialogInput) -> ChatProviderFuture<'a> {
            Box::pin(async move {
                let error: Box<dyn Error + Send + Sync + 'static> =
                    Box::new(ProviderError::new(self.provider, self.reason, self.message));
                Err(error)
            })
        }
    }

    #[derive(Clone, Default)]
    struct EffectsStub {
        state: Arc<Mutex<EffectsState>>,
    }

    #[derive(Default)]
    struct EffectsState {
        sent: Vec<(String, String)>,
        fail: bool,
    }

    impl EffectsStub {
        fn failing() -> Self {
            let this = Self::default();
            this.state.lock().expect("effects state").fail = true;
            this
        }

        fn sent(&self) -> Vec<(String, String)> {
            self.state.lock().expect("effects state").sent.clone()
        }
    }

    impl DialogJobEffects for EffectsStub {
        type Error = io::Error;

        fn send_dialog_answer<'a>(
            &'a self,
            params: &'a DialogJobParams,
            answer: &'a str,
        ) -> DialogJobEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("effects state");
                if state.fail {
                    return Err(io::Error::other("send down"));
                }
                state
                    .sent
                    .push((params.message_text.clone(), answer.to_owned()));
                Ok(())
            })
        }
    }

    #[derive(Clone, Default)]
    struct ToolHistoryStub {
        state: Arc<Mutex<ToolHistoryState>>,
    }

    #[derive(Default)]
    struct ToolHistoryState {
        calls: Vec<(i64, i32, Vec<ToolCall>)>,
        fail: bool,
    }

    impl ToolHistoryStub {
        fn failing() -> Self {
            let this = Self::default();
            this.state.lock().expect("tool history state").fail = true;
            this
        }

        fn calls(&self) -> Vec<(i64, i32, Vec<ToolCall>)> {
            self.state.lock().expect("tool history state").calls.clone()
        }
    }

    impl DialogToolCallHistoryStore for ToolHistoryStub {
        type Error = io::Error;

        fn upsert_dialog_tool_calls<'a>(
            &'a self,
            chat_id: i64,
            message_id: i32,
            tool_calls: &'a [ToolCall],
        ) -> DialogToolCallHistoryFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("tool history state");
                if state.fail {
                    return Err(io::Error::other("history down"));
                }
                state.calls.push((chat_id, message_id, tool_calls.to_vec()));
                Ok(true)
            })
        }
    }

    #[derive(Clone, Default)]
    struct MaterializerStub {
        history: Vec<HistoryMessage>,
    }

    impl MaterializerStub {
        fn with_history(history: Vec<HistoryMessage>) -> Self {
            Self { history }
        }
    }

    impl DialogInputMaterializer for MaterializerStub {
        fn materialize_dialog_input<'a>(
            &'a self,
            params: &'a DialogJobParams,
            now: OffsetDateTime,
        ) -> DialogInputMaterializerFuture<'a> {
            Box::pin(async move {
                let mut input = dialog_input_from_job_params_at(params, now);
                input.history = self.history.clone();
                input
            })
        }
    }

    #[tokio::test]
    async fn deliverable_validation_matches_outbound_acceptance() -> Result<(), Box<dyn Error>> {
        let queue = Arc::new(DispatcherQueue::new(
            openplotva_telegram::DispatcherConfig::default(),
        ));
        let effects = DialogDispatcherEffects::new(Arc::clone(&queue))
            .with_virtual_id_factory(Arc::new(|| "dialog-v1".to_owned()));
        let params = dialog_params("hello");
        let corpus: Vec<String> = vec![
            "hello".to_owned(),
            "<b>hello</b>".to_owned(),
            "<p>rich paragraph</p>".to_owned(),
            "<b></b>".to_owned(),
            "<b> </b>".to_owned(),
            "\u{200b}\u{200c}".to_owned(),
            "&#x200b;".to_owned(),
            format!("<p>{}</p>", "a".repeat(40_000)),
            format!("<h2>{}</h2>", "б".repeat(100)),
        ];

        for answer in corpus {
            let validated = validate_dialog_answer_deliverable(&answer).is_ok();
            let accepted = effects.send_dialog_answer(&params, &answer).await.is_ok();
            assert_eq!(
                validated,
                accepted,
                "validator and outbound boundary disagree on {:?}...",
                answer.chars().take(40).collect::<String>()
            );
        }
        Ok(())
    }

    fn ledger_rows(
        outcomes: &crate::dialog_turn::DialogTurnObserver,
    ) -> Vec<openplotva_server::RuntimeTurnOutcomeData> {
        use openplotva_server::{RuntimeTurnOutcomeInspector, RuntimeTurnOutcomesFilter};
        outcomes
            .buffer()
            .turn_outcomes(RuntimeTurnOutcomesFilter {
                limit: 32,
                ..RuntimeTurnOutcomesFilter::default()
            })
            .expect("turn outcomes")
    }

    /// Queue whose event appends always fail while status writes succeed,
    /// probing the S13 invariant.
    struct FailingEventQueue {
        inner: InMemoryTaskQueue,
    }

    impl FailingEventQueue {
        fn new() -> Self {
            Self {
                inner: InMemoryTaskQueue::new(),
            }
        }
    }

    impl DialogJobWorkerQueue for FailingEventQueue {
        type Error = TaskQueueError;

        fn dequeue_dialog_job<'a>(
            &'a self,
            queue_name: &'static str,
        ) -> DialogJobWorkerFuture<'a, Option<DialogJobWorkItem>, Self::Error> {
            self.inner.dequeue_dialog_job(queue_name)
        }

        fn pending_dialog_job_depth<'a>(
            &'a self,
            queue_name: &'static str,
            priority: Priority,
        ) -> DialogJobWorkerFuture<'a, usize, Self::Error> {
            self.inner.pending_dialog_job_depth(queue_name, priority)
        }

        fn complete_dialog_job<'a>(
            &'a self,
            job_id: i64,
        ) -> DialogJobWorkerFuture<'a, (), Self::Error> {
            self.inner.complete_dialog_job(job_id)
        }

        fn fail_dialog_job<'a>(
            &'a self,
            job_id: i64,
            error: &'a str,
        ) -> DialogJobWorkerFuture<'a, (), Self::Error> {
            self.inner.fail_dialog_job(job_id, error)
        }

        fn append_dialog_job_event<'a>(
            &'a self,
            _job_id: i64,
            _event: TaskQueueJobEvent,
            _at: OffsetDateTime,
        ) -> DialogJobWorkerFuture<'a, (), Self::Error> {
            Box::pin(async { Err(TaskQueueError::QueueNameRequired) })
        }

        fn requeue_retryable_dialog_job<'a>(
            &'a self,
            job_id: i64,
            target_queue: &'a str,
        ) -> DialogJobWorkerFuture<'a, (), Self::Error> {
            self.inner
                .requeue_retryable_dialog_job(job_id, target_queue)
        }
    }

    fn dialog_params(text: &str) -> DialogJobParams {
        DialogJobParams {
            chat_id: 42,
            message_id: 100,
            user_id: 7,
            user_full_name: "Ada".to_owned(),
            message_text: text.to_owned(),
            original_text: String::new(),
            meta: serde_json::Value::Null,
            max_output_tokens: 512,
            thread_id: Some(9),
        }
    }

    fn empty_params() -> DialogJobParams {
        DialogJobParams {
            message_text: String::new(),
            max_output_tokens: 0,
            thread_id: None,
            ..dialog_params("")
        }
    }

    fn record_status(queue: &InMemoryTaskQueue, job_id: i64) -> JobStatus {
        queue
            .records()
            .into_iter()
            .find(|record| record.id == job_id)
            .expect("record")
            .status
    }
}
