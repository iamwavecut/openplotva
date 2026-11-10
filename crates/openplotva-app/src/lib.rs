//! Composition root for the OpenPlotva application shell.

use openplotva_storage::database as sqlx;
pub mod admin;
pub mod agent_runtime;
mod agent_tools;
pub mod asr;
pub mod callbacks;
pub mod checkin;
pub mod control_jobs;
pub mod deepseek;
pub mod delete_drawing;
pub mod delete_lyrics;
pub mod delete_message;
pub mod diagnostics;
pub mod dialog_context;
pub mod dialog_debounce;
pub mod dialog_jobs;
pub mod dialog_messages;
pub mod dialog_runtime;
pub mod dialog_tools;
pub mod dialog_turn;
pub mod dialog_workers;
pub mod edited;
pub mod embedder;
pub mod gradius_ads;
pub mod gradius_utility_outbox;
pub mod guest;
pub mod help;
mod http;
use http::HttpRouterState;
use http::ingress::TelegramWebhookRoute;
pub use http::ingress::telegram_webhook_response;
use http::memory::AdminMemoryOverrideRuntime;
use runtime_dispatcher::{
    DISPATCHER_MAX_QUEUE_SIZE, DispatcherWorkerGroupInputs, build_dispatcher_worker_group,
    dispatcher_config,
};
use task_queue::{SharedTaskQueueMaintenanceConfig, build_shared_task_queue_maintenance_group};
pub mod history_summary;
pub mod image_edit_references;
pub mod image_jobs;
pub mod ingestion_telemetry;
pub mod inline;
mod lane_checks;
pub mod llm_translate;
mod maintenance_api;
pub mod media;
pub mod members;
pub mod memory_card_usage;
pub mod memory_runtime;
mod memory_workers;
pub mod message_gate;
pub mod model_routing;
pub mod music_jobs;
pub mod openrouter_free_pool;
pub mod payments;
pub mod permissions;
mod provider_transport;
pub mod python_sandbox;
pub mod rate_limits;
pub mod rates;
pub mod reactions;
pub mod reset;
pub mod rich;
mod routed_attempts;
mod routing_admin_reports;
mod runtime_analytics_overview;
pub mod runtime_api;
mod runtime_cache;
mod runtime_dispatcher;
mod runtime_entities;
mod runtime_gemini_cache;
mod runtime_gpu_arbiter;
mod runtime_gradius;
mod runtime_llm;
mod runtime_llm_analytics;
mod runtime_llm_runs;
mod runtime_redis;
mod runtime_retention;
use runtime_redis::RedisRuntimeInspector;
mod runtime_routing;
mod runtime_safety;
mod runtime_sql;
mod runtime_taskman;
mod runtime_telegram_delivery;
mod runtime_updates;
mod runtime_virtual_dialog;
pub mod serper;
pub mod settings;
pub mod skipped;
pub mod subscription_sync;
pub mod task_queue;
pub mod telegram_activity;
pub mod telegram_outbox;
pub mod translate;
pub mod update_importer;
pub mod update_materializer;
mod update_reconciler;
pub mod updates;
pub mod virtual_messages;
pub mod vision;
pub mod youtube;

pub use runtime_dispatcher::{
    DISPATCH_FAILURE_CLASS_CHAT_RATE_LIMITED, DISPATCH_FAILURE_CLASS_MISSING_METHOD,
    DispatchFailureRecord, DispatchFailureRing, reply_message_id_from_fingerprint_key,
};

use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context as _;
use axum::{
    extract::{DefaultBodyLimit, Extension},
    routing::{any, get},
};
use openplotva_config::AppConfig;
use openplotva_server::{ReadinessCheck, ReadinessProbe, ReadinessResponse};
use openplotva_storage::{
    PostgresChatMemberStore, PostgresChatSettingsStore, PostgresGeneratedSongStore,
    PostgresHistoryStore, PostgresMemoryStore, PostgresPaymentStore, PostgresRuntimeTokenStore,
    PostgresRuntimeVirtualDialogStore, PostgresShieldStore, PostgresTelegramFileStore,
    PostgresVipStore, PostgresVirtualMessageStore, ServiceClients,
};
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;
use thiserror::Error;
use time::OffsetDateTime;
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant, timeout, timeout_at},
};

const WEBHOOK_DELETE_ON_STOP_TIMEOUT: Duration = Duration::from_secs(10);
const RUNTIME_WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(50);
const RUNTIME_PROCESSOR_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const RUNTIME_PERSISTENCE_RESERVE: Duration = Duration::from_secs(8);
const RUNTIME_FINALIZATION_RESERVE: Duration = Duration::from_secs(2);
const TELEGRAM_WEBHOOK_BODY_LIMIT_BYTES: usize = 16 * 1024 * 1024;
// Keep I/O overlap without restoring the 12:1 backlog fan-out that exhausted
// the critical pool before update leases and Telegram receipts could advance.
const DURABLE_UPDATE_CONSUMER_MAX_WORKERS: usize =
    openplotva_storage::POSTGRES_CRITICAL_MAX_CONNECTIONS as usize * 3;
const REGULAR_IMAGE_GENERATION_WORKFLOW_KEY: &str =
    image_jobs::IMAGE_GENERATION_BOOGU_TURBO_WORKFLOW_KEY;
const VIP_IMAGE_GENERATION_WORKFLOW_KEYS: [&str; 3] = [
    image_jobs::IMAGE_GENERATION_FLUX_WORKFLOW_KEY,
    image_jobs::IMAGE_GENERATION_MOSS_WORKFLOW_KEY,
    image_jobs::IMAGE_GENERATION_FIBO_WORKFLOW_KEY,
];
const VIP_IMAGE_EDITOR_WORKFLOW_KEY: &str = image_jobs::IMAGE_EDIT_FLUX_WORKFLOW_KEY;

fn durable_update_consumer_worker_limit(configured: usize) -> usize {
    configured.clamp(1, DURABLE_UPDATE_CONSUMER_MAX_WORKERS)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeWorkerPhase {
    Ingress,
    Processor,
    Outbound,
    Ancillary,
    Server,
}

struct NamedRuntimeWorker {
    name: String,
    phase: RuntimeWorkerPhase,
    handle: JoinHandle<()>,
}

struct RuntimeWorkerGroup {
    phase: RuntimeWorkerPhase,
    workers: Vec<NamedRuntimeWorker>,
}

impl RuntimeWorkerGroup {
    fn new(phase: RuntimeWorkerPhase) -> Self {
        Self {
            phase,
            workers: Vec::new(),
        }
    }

    fn register(&mut self, name: impl Into<String>, handle: JoinHandle<()>) {
        self.workers.push(NamedRuntimeWorker {
            name: name.into(),
            phase: self.phase,
            handle,
        });
    }
}

impl Drop for RuntimeWorkerGroup {
    fn drop(&mut self) {
        for worker in self.workers.drain(..) {
            worker.handle.abort();
        }
    }
}

#[derive(Default)]
struct RuntimeWorkerRegistry {
    workers: Mutex<Vec<NamedRuntimeWorker>>,
}

impl RuntimeWorkerRegistry {
    fn install(&self, mut group: RuntimeWorkerGroup) {
        self.workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(std::mem::take(&mut group.workers));
    }

    fn register(&self, name: impl Into<String>, phase: RuntimeWorkerPhase, handle: JoinHandle<()>) {
        self.workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(NamedRuntimeWorker {
                name: name.into(),
                phase,
                handle,
            });
    }

    fn readiness_check(&self) -> ReadinessCheck {
        let finished = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|worker| worker.handle.is_finished())
            .map(|worker| worker.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        if finished.is_empty() {
            ReadinessCheck::ok(
                "runtime_workers",
                "all registered runtime workers are running",
            )
        } else {
            ReadinessCheck::error(
                "runtime_workers",
                format!("runtime workers exited before shutdown: {finished}"),
            )
        }
    }

    fn take_phase(&self, phase: RuntimeWorkerPhase) -> Vec<NamedRuntimeWorker> {
        let mut workers = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (selected, remaining) = std::mem::take(&mut *workers)
            .into_iter()
            .partition(|worker| worker.phase == phase);
        *workers = remaining;
        selected
    }

    fn take_all(&self) -> Vec<NamedRuntimeWorker> {
        std::mem::take(
            &mut *self
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

impl Drop for RuntimeWorkerRegistry {
    fn drop(&mut self) {
        for worker in self
            .workers
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
        {
            worker.handle.abort();
        }
    }
}

struct RuntimeWorkerStops {
    ingress: watch::Sender<bool>,
    processor: watch::Sender<bool>,
    outbound: watch::Sender<bool>,
    ancillary: watch::Sender<bool>,
    server: watch::Sender<bool>,
}

impl RuntimeWorkerStops {
    fn new() -> Self {
        Self {
            ingress: watch::channel(false).0,
            processor: watch::channel(false).0,
            outbound: watch::channel(false).0,
            ancillary: watch::channel(false).0,
            server: watch::channel(false).0,
        }
    }

    fn subscribe(&self, phase: RuntimeWorkerPhase) -> watch::Receiver<bool> {
        self.sender(phase).subscribe()
    }

    fn signal(&self, phase: RuntimeWorkerPhase) {
        let _ = self.sender(phase).send(true);
    }

    fn sender(&self, phase: RuntimeWorkerPhase) -> &watch::Sender<bool> {
        match phase {
            RuntimeWorkerPhase::Ingress => &self.ingress,
            RuntimeWorkerPhase::Processor => &self.processor,
            RuntimeWorkerPhase::Outbound => &self.outbound,
            RuntimeWorkerPhase::Ancillary => &self.ancillary,
            RuntimeWorkerPhase::Server => &self.server,
        }
    }
}

impl Default for RuntimeWorkerStops {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct RuntimeWorkers {
    worker_registry: Arc<RuntimeWorkerRegistry>,
    worker_stops: Arc<RuntimeWorkerStops>,
    dispatcher: Option<DispatcherRuntime>,
    shared_task_queue: Option<task_queue::SharedTaskQueueRuntime>,
    dialog_debounce: Option<Arc<dialog_debounce::InMemoryDialogDebounce>>,
    webhook_route: Option<TelegramWebhookRoute>,
    telegram: Option<openplotva_telegram::TelegramClient>,
    delete_webhook_on_shutdown: bool,
    bot_username: Option<String>,
    bot_id: Option<i64>,
    dispatcher_inspector: runtime_dispatcher::RuntimeDispatcherInspectorHandle,
    cache_inspector: runtime_cache::RuntimeCacheInspectorHandle,
    taskman_inspector: runtime_taskman::RuntimeTaskmanInspectorHandle,
    memory_restart_trigger: Option<Arc<tokio::sync::Notify>>,
    llm_trace_buffer: Option<runtime_llm::RuntimeLlmTraceBuffer>,
    llm_run_buffer: Option<runtime_llm_runs::RuntimeLlmRunBuffer>,
    routing_event_buffer: Option<runtime_routing::RoutingEventBuffer>,
    routing_event_reporter: Option<runtime_routing::RoutingEventReporter>,
    runtime_api_tls_public_key_pin: Option<String>,
    attempt_walker: Option<routed_attempts::RoutedAttemptWalker>,
    router_handle: Option<Arc<openplotva_llm::router::RouterHandle>>,
    router_breakers: Option<Arc<openplotva_llm::router::BreakerSet>>,
    router_triggers: Option<Arc<openplotva_llm::router::TriggerState>>,
    router_pools: Option<Arc<openplotva_llm::router::PoolRegistry>>,
    openrouter_free_gate: Option<Arc<openrouter_free_pool::OpenRouterFreeQuotaGate>>,
    router_runtime: Option<Arc<model_routing::RouterRuntime>>,
    dialog_worker_gauge: Option<Arc<dialog_workers::WorkerGauge>>,
    memory_card_cache: Option<Arc<openplotva_storage::MemoryCardCache>>,
    readiness_probes: Vec<ReadinessProbe>,
}

struct DispatcherRuntime {
    queue: Arc<openplotva_telegram::DispatcherQueue>,
    persistence: openplotva_telegram::RedisDispatcherQueueStore,
}

/// Boxed future returned by Telegram startup method executors.
pub type DeleteWebhookFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by Telegram webhook setup executors.
pub type SetWebhookFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by Telegram bot command setup executors.
pub type BotCommandSetupFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Minimal Telegram startup capability needed before long polling.
pub trait DeleteWebhookExecutor {
    /// Error returned by the concrete Telegram client.
    type Error: fmt::Display + Send;

    fn delete_webhook<'a>(&'a self) -> DeleteWebhookFuture<'a, Self::Error>;
}

impl DeleteWebhookExecutor for openplotva_telegram::TelegramClient {
    type Error = carapax::api::ExecuteError;

    fn delete_webhook<'a>(&'a self) -> DeleteWebhookFuture<'a, Self::Error> {
        Box::pin(async move {
            self.execute(openplotva_telegram::build_delete_webhook_method())
                .await
                .map(|_: bool| ())
        })
    }
}

/// Minimal Telegram startup capability needed before webhook update intake.
pub trait SetWebhookExecutor {
    /// Error returned by the concrete Telegram client.
    type Error: fmt::Display + Send;

    fn set_webhook<'a>(
        &'a self,
        setup: &'a openplotva_telegram::WebhookSetup,
    ) -> SetWebhookFuture<'a, Self::Error>;
}

/// Telegram webhook setup client that uses raw multipart only for custom certificate uploads.
#[derive(Clone)]
struct TelegramWebhookSetupClient {
    json: openplotva_telegram::TelegramClient,
    multipart: TelegramWebhookMultipartClient,
}

impl TelegramWebhookSetupClient {
    fn new(token: impl Into<String>, json: openplotva_telegram::TelegramClient) -> Self {
        Self {
            json,
            multipart: TelegramWebhookMultipartClient::new(token),
        }
    }
}

impl SetWebhookExecutor for TelegramWebhookSetupClient {
    type Error = TelegramWebhookSetupError;

    fn set_webhook<'a>(
        &'a self,
        setup: &'a openplotva_telegram::WebhookSetup,
    ) -> SetWebhookFuture<'a, Self::Error> {
        Box::pin(async move {
            if setup.certificate.is_some() {
                self.multipart.set_webhook(setup).await
            } else {
                self.json
                    .execute(openplotva_telegram::build_set_webhook_method(setup))
                    .await
                    .map(|_: bool| ())
                    .map_err(TelegramWebhookSetupError::Carapax)
            }
        })
    }
}

#[derive(Clone)]
struct TelegramWebhookMultipartClient {
    http: reqwest::Client,
    token: String,
    base_url: String,
}

impl TelegramWebhookMultipartClient {
    fn new(token: impl Into<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            token: token.into(),
            base_url: "https://api.telegram.org".to_owned(),
        }
    }

    async fn set_webhook(
        &self,
        setup: &openplotva_telegram::WebhookSetup,
    ) -> Result<(), TelegramWebhookSetupError> {
        let plan = telegram_webhook_multipart_plan(setup)?;
        let mut form = reqwest::multipart::Form::new();
        for (name, value) in plan.fields {
            form = form.text(name, value);
        }
        form = form.part(
            "certificate",
            reqwest::multipart::Part::bytes(plan.certificate_bytes)
                .file_name(plan.certificate_name),
        );

        let response = self
            .http
            .post(format!("{}/bot{}/setWebhook", self.base_url, self.token))
            .multipart(form)
            .send()
            .await?
            .error_for_status()?
            .json::<TelegramApiResponse>()
            .await?;

        if response.ok && response.result.unwrap_or(false) {
            return Ok(());
        }

        Err(TelegramWebhookSetupError::Telegram(
            response
                .description
                .unwrap_or_else(|| "Telegram setWebhook returned ok=false".to_owned()),
        ))
    }
}

#[derive(Debug, Error)]
pub enum TelegramWebhookSetupError {
    #[error("set webhook through carapax: {0}")]
    Carapax(#[source] carapax::api::ExecuteError),
    #[error("build webhook multipart payload: {0}")]
    MultipartPlan(#[from] TelegramWebhookMultipartPlanError),
    #[error("send webhook multipart request: {0}")]
    Request(#[from] reqwest::Error),
    #[error("Telegram setWebhook failed: {0}")]
    Telegram(String),
}

#[derive(Debug, Deserialize)]
struct TelegramApiResponse {
    ok: bool,
    result: Option<bool>,
    description: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramWebhookMultipartPlan {
    pub fields: Vec<(String, String)>,
    pub certificate_name: String,
    pub certificate_bytes: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum TelegramWebhookMultipartPlanError {
    #[error("webhook certificate is missing")]
    MissingCertificate,
    #[error("serialize allowed updates: {0}")]
    AllowedUpdatesJson(#[from] serde_json::Error),
}

/// Minimal Telegram command setup capability used during app startup.
pub trait BotCommandSetupExecutor {
    /// Error returned by the concrete Telegram client.
    type Error: fmt::Display + Send;

    fn delete_my_commands<'a>(
        &'a self,
        method: openplotva_telegram::DeleteBotCommands,
    ) -> BotCommandSetupFuture<'a, Self::Error>;

    fn set_my_commands<'a>(
        &'a self,
        scope: &'static str,
        method: openplotva_telegram::SetBotCommands,
    ) -> BotCommandSetupFuture<'a, Self::Error>;
}

impl BotCommandSetupExecutor for openplotva_telegram::TelegramClient {
    type Error = carapax::api::ExecuteError;

    fn delete_my_commands<'a>(
        &'a self,
        method: openplotva_telegram::DeleteBotCommands,
    ) -> BotCommandSetupFuture<'a, Self::Error> {
        Box::pin(async move { self.execute(method).await.map(|_: bool| ()) })
    }

    fn set_my_commands<'a>(
        &'a self,
        _scope: &'static str,
        method: openplotva_telegram::SetBotCommands,
    ) -> BotCommandSetupFuture<'a, Self::Error> {
        Box::pin(async move { self.execute(method).await.map(|_: bool| ()) })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BotCommandSetupReport {
    /// Whether the global command list was deleted first.
    pub deleted_existing: bool,
    pub set_scopes: Vec<&'static str>,
}

/// Error returned while configuring Telegram bot commands.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum BotCommandSetupError {
    /// Static command definitions failed to build into Telegram command objects.
    #[error("build bot commands: {message}")]
    Build {
        /// Display form of the command-build error.
        message: String,
    },
    /// The initial `deleteMyCommands` request failed.
    #[error("delete bot commands: {message}")]
    Delete {
        /// Display form of the Telegram client error.
        message: String,
    },
    /// A scoped `setMyCommands` request failed.
    #[error("set {scope} bot commands: {message}")]
    Set {
        scope: &'static str,
        /// Display form of the Telegram client error.
        message: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WebhookShutdownCleanupReport {
    SkippedDisabled,
    /// Telegram `deleteWebhook` completed successfully.
    Deleted,
    Failed {
        error: String,
    },
    TimedOut,
}

fn static_web_routes(
    admin_ids: Vec<i64>,
    bot_token: impl Into<String>,
    webapp_url: impl Into<String>,
    bot_username: impl Into<String>,
    state_store: Option<PostgresVirtualMessageStore>,
) -> HttpRouterState {
    HttpRouterState {
        admin_ids: Arc::from(admin_ids),
        bot_token: Arc::from(bot_token.into()),
        webapp_url: Arc::from(webapp_url.into()),
        bot_username: Arc::from(bot_username.into()),
        bot_id: None,
        default_log_level: Arc::from("info"),
        postgres: None,
        redis: None,
        log_buffer: None,
        telegram: None,
        dispatcher_inspector: runtime_dispatcher::RuntimeDispatcherInspectorHandle::default(),
        cache_inspector: runtime_cache::RuntimeCacheInspectorHandle::default(),
        taskman_inspector: runtime_taskman::RuntimeTaskmanInspectorHandle::default(),
        llm_trace_buffer: None,
        llm_run_buffer: None,
        gradius_audit_reader: None,
        routing_event_buffer: None,
        routing_event_reporter: None,
        llm_discovery_base_url: Arc::from(""),
        llm_discovery_service_name: Arc::from(""),
        runtime_sql_timeout_ms: openplotva_config::DEFAULT_RUNTIME_API_SQL_TIMEOUT_MS,
        state_store,
        settings_store: None,
        member_store: None,
        vip_store: None,
        vip_status: None,
        memory_store: None,
        memory_admin_enabled: false,
        memory_retention: Duration::from_secs(7 * 24 * 60 * 60),
        memory_enqueue_policy: openplotva_memory::MemoryRunEnqueuePolicy::default(),
        memory_consolidation_model: Arc::from(
            openplotva_memory::DEFAULT_MEMORY_CONSOLIDATION_MODEL,
        ),
        memory_max_input_tokens: 0,
        memory_max_messages_per_run: 0,
        memory_token_estimator_source: Arc::from(openplotva_memory::MEMORY_TOKEN_ESTIMATOR_SOURCE),
        memory_restart_trigger: None,
        memory_override_runtime: None,
        shield_store: None,
        shield_options: openplotva_shield::Options::default(),
        shield_embedder: None,
        router_handle: None,
        router_breakers: None,
        router_triggers: None,
        router_pools: None,
        openrouter_free_gate: None,
        router_runtime: None,
        dialog_worker_gauge: None,
    }
}

fn static_web_routes_from_config(
    config: &AppConfig,
    service_clients: Option<&ServiceClients>,
    bot_username: impl Into<String>,
    log_buffer: Arc<openplotva_observability::RuntimeLogBuffer>,
    runtime_workers: &RuntimeWorkers,
) -> HttpRouterState {
    let mut routes = static_web_routes(
        config.admins.admin_ids.clone(),
        config.bot.key.clone().unwrap_or_default(),
        config.server.url.clone(),
        bot_username,
        service_clients.map(|clients| PostgresVirtualMessageStore::new(clients.postgres.clone())),
    );
    routes.default_log_level = Arc::from(config.observability.log_level.clone());
    routes.log_buffer = Some(log_buffer);
    routes.telegram = runtime_workers.telegram.clone();
    routes.bot_id = runtime_workers.bot_id;
    routes.dispatcher_inspector = runtime_workers.dispatcher_inspector.clone();
    routes.cache_inspector = runtime_workers.cache_inspector.clone();
    routes.taskman_inspector = runtime_workers.taskman_inspector.clone();
    routes.llm_trace_buffer = runtime_workers.llm_trace_buffer.clone();
    routes.llm_run_buffer = runtime_workers.llm_run_buffer.clone();
    routes.routing_event_buffer = runtime_workers.routing_event_buffer.clone();
    routes.routing_event_reporter = runtime_workers.routing_event_reporter.clone();
    routes.llm_discovery_base_url = Arc::from(config.llm.discovery.base_url.clone());
    routes.llm_discovery_service_name = Arc::from(config.llm.dialog.discovery_service_name.clone());
    routes.runtime_sql_timeout_ms = config.runtime_api.sql_timeout_ms;
    routes.memory_admin_enabled = config.memory.enabled;
    let memory_worker_config =
        memory_runtime::memory_service_worker_config_from_memory_config(&config.memory);
    routes.memory_retention = memory_worker_config.retention;
    routes.memory_enqueue_policy = memory_worker_config.enqueue_policy;
    routes.memory_consolidation_model = Arc::from(config.memory.consolidation_model.clone());
    routes.memory_max_input_tokens = memory_worker_config.process.max_input_tokens;
    routes.memory_max_messages_per_run = memory_worker_config.process.max_messages_per_run;
    routes.memory_token_estimator_source =
        Arc::from(openplotva_memory::MEMORY_TOKEN_ESTIMATOR_SOURCE);
    routes.memory_restart_trigger = runtime_workers.memory_restart_trigger.clone();
    routes.router_handle = runtime_workers.router_handle.clone();
    routes.router_breakers = runtime_workers.router_breakers.clone();
    routes.router_triggers = runtime_workers.router_triggers.clone();
    routes.router_pools = runtime_workers.router_pools.clone();
    routes.openrouter_free_gate = runtime_workers.openrouter_free_gate.clone();
    routes.router_runtime = runtime_workers.router_runtime.clone();
    routes.dialog_worker_gauge = runtime_workers.dialog_worker_gauge.clone();
    routes.shield_options = shield_options_from_config(&config.shield);
    if let Some(clients) = service_clients {
        routes.postgres = Some(clients.postgres.clone());
        routes.gradius_audit_reader = Some(Arc::new(
            runtime_gradius::PostgresRuntimeGradiusAuditReader::new(clients.postgres.clone()),
        ));
        routes.redis = Some(clients.redis.client().clone());
        routes.settings_store = Some(PostgresChatSettingsStore::new(clients.postgres.clone()));
        routes.member_store = Some(PostgresChatMemberStore::new(clients.postgres.clone()));
        routes.vip_store = Some(PostgresVipStore::new(clients.postgres.clone()));
        if let Some(telegram) = runtime_workers.telegram.as_ref() {
            let payment_store = payments::PostgresSuccessfulPaymentStore::new(
                PostgresVirtualMessageStore::new(clients.postgres.clone()),
                PostgresPaymentStore::new(clients.postgres.clone()),
                PostgresVipStore::new(clients.postgres.clone()),
            );
            routes.vip_status = Some(Arc::new(payments::VipStatusWithExternalMembership::new(
                payment_store,
                payments::TelegramExternalVipMembershipChecker::new(telegram.clone()),
                config.vip.chat_id,
            )));
        }
        let mut memory_store = PostgresMemoryStore::new(clients.postgres.clone());
        if let Some(cache) = &runtime_workers.memory_card_cache {
            memory_store = memory_store.with_card_cache(Arc::clone(cache));
        }
        routes.memory_store = Some(memory_store.clone());
        if config.memory.enabled
            && let Some(attempt_walker) = runtime_workers.attempt_walker.clone()
        {
            routes.memory_override_runtime = Some(AdminMemoryOverrideRuntime {
                config: Arc::new(config.clone()),
                store: memory_store,
                lock: Arc::new(tokio::sync::Mutex::new(())),
                attempt_walker,
            });
        }
        if config.shield.enabled {
            routes.shield_store = Some(PostgresShieldStore::new(clients.postgres.clone()));
            match memory_runtime::shield_embedder_from_config(
                &config.shield,
                &config.llm.discovery.base_url,
            ) {
                Ok(Some(client)) => {
                    routes.shield_embedder =
                        Some(Arc::new(client) as Arc<dyn memory_runtime::EmbeddingProvider>);
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(%error, "failed to configure admin shield embedder");
                }
            }
        }
    }
    routes
}

/// Build the HTTP router without binding a socket.
pub fn router() -> axum::Router {
    router_with_readiness_and_static_web(
        ReadinessResponse::ready(Vec::new()),
        Vec::new(),
        static_web_routes(Vec::new(), "", "", "", None),
    )
}

/// Build the HTTP router with a Telegram webhook intake route attached.
pub fn router_with_readiness_and_telegram_webhook(
    readiness: ReadinessResponse,
    sender: openplotva_telegram::WebhookUpdateSender,
    secret_token: impl Into<String>,
) -> axum::Router {
    let route = TelegramWebhookRoute {
        sender,
        secret_token: Arc::from(secret_token.into()),
    };

    router_with_readiness_static_web_and_telegram_webhook_route(
        readiness,
        Vec::new(),
        static_web_routes(Vec::new(), "", "", "", None),
        route,
    )
}

fn router_with_readiness_and_static_web(
    readiness: ReadinessResponse,
    readiness_probes: Vec<ReadinessProbe>,
    static_web: HttpRouterState,
) -> axum::Router {
    install_static_web_routes(
        openplotva_server::router_with_readiness_probes(readiness, readiness_probes),
        static_web,
    )
}

fn router_with_readiness_static_web_and_telegram_webhook_route(
    readiness: ReadinessResponse,
    readiness_probes: Vec<ReadinessProbe>,
    static_web: HttpRouterState,
    route: TelegramWebhookRoute,
) -> axum::Router {
    router_with_readiness_and_static_web(readiness, readiness_probes, static_web)
        .route(
            openplotva_telegram::TELEGRAM_WEBHOOK_PATH,
            any(http::ingress::telegram_webhook)
                .layer(DefaultBodyLimit::max(TELEGRAM_WEBHOOK_BODY_LIMIT_BYTES)),
        )
        .layer(Extension(route))
}

fn install_static_web_routes(router: axum::Router, static_web: HttpRouterState) -> axum::Router {
    let state = Arc::new(static_web);
    let web = axum::Router::<Arc<HttpRouterState>>::new()
        .route("/settings", get(http::settings::settings_redirect))
        .route("/settings/", get(http::settings::settings_index))
        .route("/settings/{*path}", get(http::settings::settings_asset))
        .route("/api/settings", any(http::settings::settings_api))
        .route("/api/settings/", any(http::settings::settings_api))
        .route(
            "/api/settings/deputies",
            any(http::settings::settings_deputies_api),
        )
        .route(
            "/api/settings/deputies/",
            any(http::settings::settings_deputies_api),
        )
        .route(
            "/api/settings/deputies/candidates",
            any(http::settings::settings_deputy_candidates_api),
        )
        .route(
            "/api/settings/deputies/candidates/",
            any(http::settings::settings_deputy_candidates_api),
        )
        .route(
            "/api/settings/memory",
            any(http::settings::settings_memory_api),
        )
        .route(
            "/api/settings/memory/",
            any(http::settings::settings_memory_api),
        )
        .route("/api/chats", any(http::settings::settings_chats_api))
        .route("/admin/api/auth", any(http::auth::admin_auth))
        .route("/admin/api/auth_check", get(http::auth::admin_auth_check))
        .route("/admin/api/state", any(http::observability::admin_state))
        .route(
            "/admin/api/loglevel",
            any(http::observability::admin_loglevel),
        )
        .route("/admin/api/routing", any(http::routing::admin_routing))
        .route(
            "/admin/api/routing/status",
            any(http::routing::admin_routing_status),
        )
        .route(
            "/admin/api/routing/events",
            any(http::routing::admin_routing_events),
        )
        .route(
            "/admin/api/logs/stream",
            any(http::observability::admin_logs_stream),
        )
        .route("/admin/api/bootstrap", get(http::auth::admin_bootstrap))
        .route("/admin/api/metrics", any(http::observability::admin_state))
        .route(
            "/admin/api/safety/checks",
            any(http::observability::admin_safety_checks),
        )
        .route(
            "/admin/api/gradius/opportunities",
            any(http::observability::admin_gradius_opportunities),
        )
        .route(
            "/admin/api/gradius/summary",
            any(http::observability::admin_gradius_summary),
        )
        .route(
            "/admin/api/analytics/llm/summary",
            any(http::observability::admin_llm_analytics_summary),
        )
        .route(
            "/admin/api/analytics/overview",
            any(http::observability::admin_analytics_overview),
        )
        .route(
            "/admin/api/gpu1/status",
            any(http::observability::admin_gpu1_status),
        )
        .route(
            "/admin/api/llm/dialogs",
            any(http::observability::admin_llm_dialogs),
        )
        .route(
            "/admin/api/llm/dialogs/detail",
            any(http::observability::admin_llm_dialogs_detail),
        )
        .route(
            "/admin/api/llm/dialogs/clear",
            any(http::observability::admin_llm_dialogs_clear),
        )
        .route(
            "/admin/api/memory/cards",
            any(http::memory::admin_memory_cards),
        )
        .route(
            "/admin/api/memory/runs",
            any(http::memory::admin_memory_runs),
        )
        .route(
            "/admin/api/memory/restart",
            any(http::memory::admin_memory_restart),
        )
        .route(
            "/admin/api/memory/card",
            any(http::memory::admin_memory_card),
        )
        .route(
            "/admin/api/memory/overview",
            any(http::memory::admin_memory_overview),
        )
        .route(
            "/admin/api/shield/documents",
            any(http::shield::admin_shield_documents),
        )
        .route(
            "/admin/api/shield/embeddings/rebuild",
            any(http::shield::admin_shield_embeddings_rebuild),
        )
        .route(
            "/admin/api/shield/test",
            any(http::shield::admin_shield_test),
        )
        .route("/admin/api/redis/list", any(http::redis::admin_redis_list))
        .route("/admin/api/redis/get", any(http::redis::admin_redis_get))
        .route(
            "/admin/api/redis/delete_prefix",
            any(http::redis::admin_redis_delete_prefix),
        )
        .route(
            "/admin/api/redis/prefixes",
            any(http::redis::admin_redis_prefixes),
        )
        .route(
            "/admin/api/redis/delete_key",
            any(http::redis::admin_redis_delete_key),
        )
        .route(
            "/admin/api/redis/flushdb",
            any(http::redis::admin_redis_flushdb),
        )
        .route("/admin/api/chat", any(http::entities::admin_chat_get))
        .route(
            "/admin/api/chat/settings",
            any(http::entities::admin_chat_settings),
        )
        .route(
            "/admin/api/chat/block",
            any(http::entities::admin_chat_block),
        )
        .route(
            "/admin/api/chat/unblock",
            any(http::entities::admin_chat_unblock),
        )
        .route(
            "/admin/api/chat/members",
            any(http::entities::admin_chat_members),
        )
        .route("/admin/api/chats", any(http::entities::admin_chats_list))
        .route(
            "/admin/api/chats/search_by_member",
            any(http::entities::admin_chats_search_by_member),
        )
        .route("/admin/api/users", any(http::entities::admin_users_list))
        .route(
            "/admin/api/vip/users",
            any(http::entities::admin_vip_users_list),
        )
        .route("/admin/api/user", any(http::entities::admin_user_get))
        .route(
            "/admin/api/user/grant_vip",
            any(http::entities::admin_user_grant_vip),
        )
        .route(
            "/admin/api/user/revoke_vip",
            any(http::entities::admin_user_revoke_vip),
        )
        .route(
            "/admin/api/user/delete",
            any(http::entities::admin_user_delete),
        )
        .route(
            "/admin/api/taskman/jobs",
            any(http::taskman::admin_taskman_jobs),
        )
        .route(
            "/admin/api/taskman/jobs/clear",
            any(http::taskman::admin_taskman_jobs_clear),
        )
        .route(
            "/admin/api/taskman/job",
            any(http::taskman::admin_taskman_job),
        )
        .route(
            "/admin/api/taskman/job/cancel",
            any(http::taskman::admin_taskman_job_cancel),
        )
        .route(
            "/admin/api/taskman/job/restart",
            any(http::taskman::admin_taskman_job_restart),
        )
        .route("/admin", get(http::auth::admin_redirect))
        .route("/admin/", get(http::auth::admin_index))
        .route("/admin/{*path}", get(http::auth::admin_asset))
        .with_state(state);
    router.merge(web)
}

#[cfg(test)]
const ADMIN_API_ROUTE_PATTERNS: &[&str] = &[
    "/admin/api/auth",
    "/admin/api/auth_check",
    "/admin/api/state",
    "/admin/api/loglevel",
    "/admin/api/logs/stream",
    "/admin/api/bootstrap",
    "/admin/api/metrics",
    "/admin/api/safety/checks",
    "/admin/api/gradius/opportunities",
    "/admin/api/gradius/summary",
    "/admin/api/analytics/llm/summary",
    "/admin/api/analytics/overview",
    "/admin/api/llm/dialogs",
    "/admin/api/llm/dialogs/detail",
    "/admin/api/llm/dialogs/clear",
    "/admin/api/memory/cards",
    "/admin/api/memory/runs",
    "/admin/api/memory/restart",
    "/admin/api/memory/card",
    "/admin/api/memory/overview",
    "/admin/api/shield/documents",
    "/admin/api/shield/embeddings/rebuild",
    "/admin/api/shield/test",
    "/admin/api/redis/list",
    "/admin/api/redis/get",
    "/admin/api/redis/delete_prefix",
    "/admin/api/redis/prefixes",
    "/admin/api/redis/delete_key",
    "/admin/api/redis/flushdb",
    "/admin/api/chat",
    "/admin/api/chat/settings",
    "/admin/api/chat/block",
    "/admin/api/chat/unblock",
    "/admin/api/chat/members",
    "/admin/api/chats",
    "/admin/api/chats/search_by_member",
    "/admin/api/users",
    "/admin/api/vip/users",
    "/admin/api/user",
    "/admin/api/user/grant_vip",
    "/admin/api/user/revoke_vip",
    "/admin/api/user/delete",
    "/admin/api/taskman/jobs",
    "/admin/api/taskman/jobs/clear",
    "/admin/api/taskman/job",
    "/admin/api/taskman/job/cancel",
    "/admin/api/taskman/job/restart",
];

/// Render a fatal error like the report of a `Result` returned from `main`, with
/// secrets masked. That report goes straight to stderr, past the redacting log
/// writer, and Telegram client errors carry the bot token in the request URL.
#[must_use]
pub fn fatal_error_report(error: &anyhow::Error) -> String {
    format!(
        "Error: {}",
        openplotva_observability::secrets::redact(&format!("{error:?}"))
    )
}

/// Run the current OpenPlotva app shell.
pub async fn run() -> anyhow::Result<()> {
    let config = AppConfig::from_env().context("load configuration")?;
    let prompt_store =
        Arc::new(openplotva_prompts::PromptStore::load().context("compile prompt registry")?);
    let log_buffer = openplotva_observability::init_with_log_buffer_capacity(
        &config.observability,
        config.runtime_api.log_buffer_size,
    );
    if let Some(token) = config.bot.key.as_deref() {
        openplotva_observability::secrets::register_secret(token);
    }
    openplotva_observability::secrets::register_secret(&config.google_ai.key);

    let mut readiness_checks = Vec::new();
    let service_clients = connect_services(&config, &mut readiness_checks).await?;
    record_dialog_tool_mode_readiness(&config, &mut readiness_checks);
    let runtime_workers = start_runtime_workers(
        &config,
        service_clients.as_ref(),
        &mut readiness_checks,
        Arc::clone(&log_buffer),
        prompt_store,
    )
    .await?;

    let listener = tokio::net::TcpListener::bind(&config.server.bind_addr)
        .await
        .with_context(|| format!("bind HTTP listener to {}", config.server.bind_addr))?;
    let local_addr = listener
        .local_addr()
        .context("read HTTP listener address")?;

    tracing::info!(address = %local_addr, "openplotva listening");

    let readiness = ReadinessResponse::ready(readiness_checks);
    let bot_username = runtime_workers
        .bot_username
        .clone()
        .unwrap_or_else(|| std::env::var("BOT_USERNAME").unwrap_or_default());
    let static_web = static_web_routes_from_config(
        &config,
        service_clients.as_ref(),
        bot_username,
        log_buffer,
        &runtime_workers,
    );
    let app = if let Some(webhook_route) = runtime_workers.webhook_route.clone() {
        router_with_readiness_static_web_and_telegram_webhook_route(
            readiness,
            runtime_workers.readiness_probes.clone(),
            static_web,
            webhook_route,
        )
    } else {
        router_with_readiness_and_static_web(
            readiness,
            runtime_workers.readiness_probes.clone(),
            static_web,
        )
    };

    let serve_result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve HTTP app");

    shutdown_runtime_workers(runtime_workers).await;
    serve_result?;
    drop(service_clients);
    Ok(())
}

async fn connect_services(
    config: &AppConfig,
    readiness_checks: &mut Vec<ReadinessCheck>,
) -> anyhow::Result<Option<openplotva_storage::ServiceClients>> {
    if !config.service_probe.connect_services {
        readiness_checks.push(ReadinessCheck::skipped(
            "postgres",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "redis",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        return Ok(None);
    }

    let clients = openplotva_storage::connect_service_clients(
        &config.database.postgres,
        &config.redis,
        config.service_probe.run_migrations,
    )
    .await
    .context("connect Postgres and Redis")?;
    readiness_checks.push(ReadinessCheck::ok(
        "postgres",
        format!(
            "startup connections established; {} general and {} critical slots",
            openplotva_storage::POSTGRES_GENERAL_MAX_CONNECTIONS,
            openplotva_storage::POSTGRES_CRITICAL_MAX_CONNECTIONS,
        ),
    ));
    if clients.migrations_applied {
        readiness_checks.push(ReadinessCheck::ok(
            "migrations",
            "pending SQLx migrations applied",
        ));
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "migrations",
            "OPENPLOTVA_RUN_MIGRATIONS=false",
        ));
    }
    readiness_checks.push(ReadinessCheck::ok("redis", "startup ping succeeded"));
    Ok(Some(clients))
}

fn postgres_capacity_is_exhausted(size: u32, max_connections: u32, idle: usize) -> bool {
    size >= max_connections && idle == 0
}

fn postgres_pool_is_exhausted(pool: &PgPool) -> bool {
    postgres_capacity_is_exhausted(
        pool.size(),
        pool.options().get_max_connections(),
        pool.num_idle(),
    )
}

fn postgres_pool_readiness(general: &PgPool, critical: &PgPool) -> ReadinessCheck {
    if general.is_closed() || critical.is_closed() {
        return ReadinessCheck::error("postgres_pools_live", "a Postgres pool is closed");
    }
    let general_exhausted = postgres_pool_is_exhausted(general);
    let critical_exhausted = postgres_pool_is_exhausted(critical);
    let detail = format!(
        "general size={}/{} idle={}; critical size={}/{} idle={}",
        general.size(),
        general.options().get_max_connections(),
        general.num_idle(),
        critical.size(),
        critical.options().get_max_connections(),
        critical.num_idle(),
    );
    if general_exhausted || critical_exhausted {
        ReadinessCheck::error("postgres_pools_live", format!("pool exhausted; {detail}"))
    } else {
        ReadinessCheck::ok("postgres_pools_live", detail)
    }
}

fn telegram_webhook_ingress_readiness(
    sender: &openplotva_telegram::WebhookUpdateSender,
) -> ReadinessCheck {
    const RECENT_FAILURE_WINDOW_MS: u64 = 30_000;

    let snapshot = sender.ingress_snapshot();
    let now_unix_ms = u64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000)
        .unwrap_or_default();
    let recent_failure = snapshot
        .last_failure_unix_ms
        .is_some_and(|failed_at| now_unix_ms.saturating_sub(failed_at) <= RECENT_FAILURE_WINDOW_MS);
    let detail = format!(
        "{} succeeded, {} failed ({} timeouts), {} in flight, last latency {} ms",
        snapshot.append_succeeded,
        snapshot.append_failed,
        snapshot.append_timed_out,
        snapshot.append_in_flight,
        snapshot.last_append_latency_ms,
    );
    if recent_failure {
        ReadinessCheck::error(
            "telegram_webhook_ingress_live",
            format!("recent durable append failure; {detail}"),
        )
    } else {
        ReadinessCheck::ok("telegram_webhook_ingress_live", detail)
    }
}

fn telegram_outbox_readiness(
    metrics: &telegram_outbox::TelegramOutboxWorkerMetrics,
) -> ReadinessCheck {
    const RECENT_FAILURE_WINDOW_MS: u64 = 30_000;

    let snapshot = metrics.snapshot();
    if !snapshot.running {
        return ReadinessCheck::error(
            "telegram_outbox_live",
            "durable Telegram outbox worker is not running",
        );
    }
    let now_unix_ms = u64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000)
        .unwrap_or_default();
    let recent_error = snapshot
        .last_error_unix_ms
        .is_some_and(|failed_at| now_unix_ms.saturating_sub(failed_at) <= RECENT_FAILURE_WINDOW_MS);
    let report = snapshot.report;
    let detail = format!(
        "{} claimed, {} delivered, {} retried, {} ambiguous, {} dead-lettered, {} errors",
        report.claimed,
        report.delivered,
        report.retried,
        report.ambiguous,
        report.dead_lettered,
        report.errors,
    );
    if recent_error {
        ReadinessCheck::error(
            "telegram_outbox_live",
            format!("recent worker error; {detail}"),
        )
    } else {
        ReadinessCheck::ok("telegram_outbox_live", detail)
    }
}

fn record_dialog_tool_mode_readiness(
    config: &AppConfig,
    readiness_checks: &mut Vec<ReadinessCheck>,
) {
    let dialog = &config.llm.dialog;
    if !dialog
        .provider
        .eq_ignore_ascii_case(openplotva_dialog::PROVIDER_AIFARM)
    {
        return;
    }
    readiness_checks.push(ReadinessCheck::ok(
        "dialog_tool_mode",
        "AIFarm native dialog tool calls enabled",
    ));
}

pub async fn delete_webhook_on_shutdown_if_enabled<Startup>(
    enabled: bool,
    startup: &Startup,
) -> WebhookShutdownCleanupReport
where
    Startup: DeleteWebhookExecutor + Sync,
    Startup::Error: fmt::Display,
{
    if !enabled {
        return WebhookShutdownCleanupReport::SkippedDisabled;
    }

    match timeout(WEBHOOK_DELETE_ON_STOP_TIMEOUT, startup.delete_webhook()).await {
        Ok(Ok(())) => WebhookShutdownCleanupReport::Deleted,
        Ok(Err(error)) => WebhookShutdownCleanupReport::Failed {
            error: error.to_string(),
        },
        Err(_) => WebhookShutdownCleanupReport::TimedOut,
    }
}

pub async fn configure_telegram_bot_commands<C>(
    client: &C,
) -> Result<BotCommandSetupReport, BotCommandSetupError>
where
    C: BotCommandSetupExecutor + Sync,
{
    client
        .delete_my_commands(openplotva_telegram::delete_my_commands_method())
        .await
        .map_err(|error| BotCommandSetupError::Delete {
            message: error.to_string(),
        })?;

    let methods = openplotva_telegram::set_my_commands_methods().map_err(|error| {
        BotCommandSetupError::Build {
            message: error.to_string(),
        }
    })?;

    let mut report = BotCommandSetupReport {
        deleted_existing: true,
        set_scopes: Vec::with_capacity(methods.len()),
    };

    for (set, method) in openplotva_telegram::COMMAND_SETS.iter().zip(methods) {
        let scope = set.scope.inventory_name();
        client
            .set_my_commands(scope, method)
            .await
            .map_err(|error| BotCommandSetupError::Set {
                scope,
                message: error.to_string(),
            })?;
        report.set_scopes.push(scope);
    }

    Ok(report)
}

pub fn webhook_setup_from_config(
    config: &openplotva_config::BotWebhookConfig,
) -> std::io::Result<openplotva_telegram::WebhookSetup> {
    if config.enabled && config.secret_token.trim().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "BOT_WEBHOOK_SECRET_TOKEN is required when webhook updates are enabled",
        ));
    }
    let mut setup = openplotva_telegram::WebhookSetup::new(
        config.url.clone(),
        Some(config.secret_token.clone()).filter(|secret_token| !secret_token.trim().is_empty()),
    );

    if !config.cert_file.is_empty() && !config.key_file.is_empty() {
        let bytes = std::fs::read(&config.cert_file)?;
        setup = setup.with_certificate(openplotva_telegram::WebhookCertificate::new(
            "cert.pem", bytes,
        ));
    }

    Ok(setup)
}

pub fn telegram_webhook_multipart_plan(
    setup: &openplotva_telegram::WebhookSetup,
) -> Result<TelegramWebhookMultipartPlan, TelegramWebhookMultipartPlanError> {
    let certificate = setup
        .certificate
        .clone()
        .ok_or(TelegramWebhookMultipartPlanError::MissingCertificate)?;
    let mut allowed_updates = openplotva_telegram::product_allowed_update_set()
        .into_iter()
        .map(|update| serde_json::to_value(update).and_then(serde_json::from_value::<String>))
        .collect::<Result<Vec<_>, _>>()?;
    allowed_updates.sort_unstable();
    let mut fields = vec![
        (
            "allowed_updates".to_owned(),
            serde_json::to_string(&allowed_updates)?,
        ),
        ("url".to_owned(), setup.url.clone()),
    ];
    if let Some(secret_token) = setup
        .secret_token
        .as_deref()
        .filter(|secret_token| !secret_token.is_empty())
    {
        fields.push(("secret_token".to_owned(), secret_token.to_owned()));
    }
    fields.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(TelegramWebhookMultipartPlan {
        fields,
        certificate_name: certificate.name,
        certificate_bytes: certificate.bytes,
    })
}

fn runtime_api_bind_addr(config: &openplotva_config::RuntimeApiConfig) -> String {
    format!("{}:{}", config.host, config.port)
}

fn shield_options_from_config(
    config: &openplotva_config::ShieldConfig,
) -> openplotva_shield::Options {
    openplotva_shield::Options {
        enabled: config.enabled,
        embedding_dim: config.embedding_dim,
        max_matches: config.max_matches,
        vector_min_score: config.vector_min_score,
        lexical_min_score: config.lexical_min_score,
        query_max_chars: usize::try_from(config.query_max_chars.max(0)).unwrap_or_default(),
        retrieval_timeout_seconds: config.retrieval_timeout_seconds,
        rebuild_batch_size: openplotva_shield::DEFAULT_REBUILD_BATCH_SIZE,
    }
    .with_defaults()
}

fn shield_history_tail_messages_from_config(config: &openplotva_config::ShieldConfig) -> usize {
    usize::try_from(config.history_tail_messages.max(0)).unwrap_or_default()
}

fn dialog_memory_context_enabled(config: &AppConfig) -> bool {
    config.memory.enabled
}

fn admin_queue_config_from_app_config(config: &AppConfig) -> admin::AdminQueueCommandConfig {
    let queue = &config.persistent_queue;
    admin::AdminQueueCommandConfig {
        persistent_queue_enabled: queue.enabled,
        default_processing_timeout: Duration::from_secs(
            queue.default_processing_timeout_seconds.max(0) as u64,
        ),
        workers: vec![
            admin::AdminQueueWorkerConfig {
                queue_name: openplotva_taskman::CONTROL_QUEUE_NAME.to_owned(),
                worker_count: queue.control_workers,
            },
            admin::AdminQueueWorkerConfig {
                queue_name: openplotva_taskman::TEXT_QUEUE_NAME.to_owned(),
                worker_count: queue.text_workers,
            },
            admin::AdminQueueWorkerConfig {
                queue_name: openplotva_taskman::DIALOG_AIFARM_QUEUE_NAME.to_owned(),
                worker_count: queue.dialog_aifarm_workers,
            },
            admin::AdminQueueWorkerConfig {
                queue_name: openplotva_taskman::IMAGE_REGULAR_QUEUE_NAME.to_owned(),
                worker_count: queue.image_regular_workers,
            },
            admin::AdminQueueWorkerConfig {
                queue_name: openplotva_taskman::IMAGE_VIP_QUEUE_NAME.to_owned(),
                worker_count: queue.image_vip_workers,
            },
            admin::AdminQueueWorkerConfig {
                queue_name: openplotva_taskman::MUSIC_VIP_QUEUE_NAME.to_owned(),
                worker_count: queue.music_vip_workers,
            },
            admin::AdminQueueWorkerConfig {
                queue_name: openplotva_taskman::MEMORY_CONSOLIDATION_QUEUE_NAME.to_owned(),
                worker_count: queue.memory_consolidation_workers,
            },
        ],
    }
}

fn subscription_sync_config_from_app_config(
    config: &AppConfig,
) -> subscription_sync::SubscriptionSyncConfig {
    subscription_sync::SubscriptionSyncConfig {
        enabled: config.subscription_sync.enabled,
        interval: Duration::from_secs(config.subscription_sync.interval_seconds.max(1) as u64),
        dry_run: config.subscription_sync.dry_run,
        page_limit: i64::from(config.subscription_sync.page_limit.max(1)),
    }
}

fn runtime_api_graphql_snapshot(
    config: &AppConfig,
) -> openplotva_server::RuntimeApiGraphqlSnapshot {
    openplotva_server::RuntimeApiGraphqlSnapshot {
        log_level: config.observability.log_level.clone(),
        web_host: config.server.host.clone(),
        web_port: i32::from(config.server.port),
        runtime_api_enabled: config.runtime_api.enabled,
        runtime_api_host: config.runtime_api.host.clone(),
        runtime_api_port: i32::from(config.runtime_api.port),
        discovery_base_url: Some(config.llm.discovery.base_url.clone())
            .filter(|value| !value.trim().is_empty()),
        embedder_enabled: !config.llm.discovery.base_url.trim().is_empty()
            && !config.memory.embedder_service_name.trim().is_empty(),
        embedder_url: Some(config.memory.embedder_service_name.clone())
            .filter(|value| !value.trim().is_empty()),
        shield_enabled: config.shield.enabled,
        shield_embedder_url: Some(config.shield.embedder_service_name.clone())
            .filter(|value| !value.trim().is_empty()),
        shield_max_matches: config.shield.max_matches,
        shield_vector_min_score: config.shield.vector_min_score,
        shield_lexical_min_score: config.shield.lexical_min_score,
        shield_retrieval_timeout_seconds: config.shield.retrieval_timeout_seconds,
        shield_history_tail_messages: config.shield.history_tail_messages,
        vision_discovery_service_name: config.vision.discovery_service_name.clone(),
        vision_discovery_endpoint_name: config.vision.discovery_endpoint_name.clone(),
        vision_model: config.vision.model.clone(),
        vision_max_tokens: config.vision.max_tokens,
        vision_temperature: config.vision.temperature,
        vision_direct_image_limit: config.vision.direct_image_limit,
        vision_request_timeout_seconds: config.vision.request_timeout_seconds,
        white_circle_enabled: dialog_runtime::white_circle_effective_enabled(config),
        ace_step_enabled: config.music.acestep.enabled,
        ace_step_base_url: Some(config.music.acestep.base_url.trim().to_owned())
            .filter(|value| !value.is_empty()),
        dialog_provider: config.llm.dialog.provider.clone(),
        dialog_fallback_provider: Some(config.llm.dialog.fallback_provider.clone())
            .filter(|value| !value.trim().is_empty()),
        persistent_queue_enabled: config.persistent_queue.enabled,
        active_draw_providers: runtime_active_draw_providers(config),
        sql_timeout_ms: config.runtime_api.sql_timeout_ms,
        sql_row_limit: config.runtime_api.sql_row_limit,
        sql_result_bytes_limit: config.runtime_api.sql_result_bytes_limit,
        db_status: "ok".to_owned(),
        redis_status: "ok".to_owned(),
    }
}

fn runtime_active_draw_providers(config: &AppConfig) -> Vec<String> {
    let mut providers = Vec::new();
    if !config.llm.discovery.base_url.trim().is_empty() {
        providers.push("drawapi".to_owned());
    }
    providers.sort();
    providers
}

#[derive(Clone)]
struct RuntimeLogInspector {
    buffer: Arc<openplotva_observability::RuntimeLogBuffer>,
}

impl RuntimeLogInspector {
    fn new(buffer: Arc<openplotva_observability::RuntimeLogBuffer>) -> Self {
        Self { buffer }
    }
}

impl openplotva_server::RuntimeLogInspector for RuntimeLogInspector {
    fn logs(
        &self,
        query: openplotva_server::RuntimeLogQuery,
    ) -> Vec<openplotva_server::RuntimeLogEntry> {
        self.buffer
            .logs(query.after_seq, query.limit, &query.level, &query.search)
            .into_iter()
            .map(|entry| openplotva_server::RuntimeLogEntry {
                seq: entry.seq,
                time: entry.time,
                level: entry.level,
                message: entry.message,
                attrs: entry.attrs,
            })
            .collect()
    }
}

struct RuntimeApiTlsMaterial {
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
    tls_public_key_pin: String,
    source: &'static str,
}

fn runtime_api_tls_material(
    config: &openplotva_config::RuntimeApiConfig,
) -> anyhow::Result<RuntimeApiTlsMaterial> {
    let cert_file_present = !config.cert_file.trim().is_empty();
    let key_file_present = !config.key_file.trim().is_empty();
    if cert_file_present && key_file_present {
        let cert_path = std::path::Path::new(&config.cert_file);
        let key_path = std::path::Path::new(&config.key_file);
        let cert_exists = cert_path
            .try_exists()
            .with_context(|| format!("inspect runtime API certificate {}", config.cert_file))?;
        let key_exists = key_path
            .try_exists()
            .with_context(|| format!("inspect runtime API private key {}", config.key_file))?;
        if cert_exists != key_exists {
            anyhow::bail!(
                "runtime API TLS certificate and private key must both exist or both be absent"
            );
        }
        if !cert_exists {
            let generated = openplotva_server::generate_runtime_api_tls_material(
                runtime_api_tls_subject_alt_names(config),
            )
            .context("generate persistent runtime API TLS material")?;
            persist_runtime_api_tls_file(key_path, &generated.key_pem, true)
                .with_context(|| format!("persist runtime API private key {}", config.key_file))?;
            if let Err(error) = persist_runtime_api_tls_file(cert_path, &generated.cert_pem, false)
            {
                let _ = std::fs::remove_file(key_path);
                return Err(error).with_context(|| {
                    format!("persist runtime API certificate {}", config.cert_file)
                });
            }
            return Ok(RuntimeApiTlsMaterial {
                cert_pem: generated.cert_pem,
                key_pem: generated.key_pem,
                tls_public_key_pin: generated.tls_public_key_pin,
                source: "generated-persisted",
            });
        }
        let cert_pem = std::fs::read(cert_path)
            .with_context(|| format!("read runtime API certificate {}", config.cert_file))?;
        let key_pem = std::fs::read(key_path)
            .with_context(|| format!("read runtime API private key {}", config.key_file))?;
        let tls_public_key_pin =
            openplotva_server::runtime_api_tls_public_key_pin_from_pem(&cert_pem)
                .context("compute runtime API TLS public key pin")?;
        return Ok(RuntimeApiTlsMaterial {
            cert_pem,
            key_pem,
            tls_public_key_pin,
            source: "configured",
        });
    }

    let generated = openplotva_server::generate_runtime_api_tls_material(
        runtime_api_tls_subject_alt_names(config),
    )
    .context("generate runtime API TLS material")?;
    Ok(RuntimeApiTlsMaterial {
        cert_pem: generated.cert_pem,
        key_pem: generated.key_pem,
        tls_public_key_pin: generated.tls_public_key_pin,
        source: "generated",
    })
}

fn persist_runtime_api_tls_file(
    path: &std::path::Path,
    contents: &[u8],
    private: bool,
) -> std::io::Result<()> {
    use std::io::Write;

    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("runtime-api-tls");
    let temp_path = path.with_file_name(format!(
        ".{file_name}.tmp-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(if private { 0o600 } else { 0o644 });
    }
    let result = (|| {
        let mut file = options.open(&temp_path)?;
        file.write_all(contents)?;
        file.sync_all()?;
        std::fs::rename(&temp_path, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp_path);
    }
    result
}

fn runtime_api_tls_subject_alt_names(config: &openplotva_config::RuntimeApiConfig) -> Vec<String> {
    let mut names = Vec::new();
    push_unique_tls_name(&mut names, "localhost");
    push_unique_tls_name(&mut names, "127.0.0.1");
    push_unique_tls_name(&mut names, "::1");
    let host = config.host.trim();
    if !host.is_empty() && host != "0.0.0.0" && host != "::" {
        push_unique_tls_name(&mut names, host);
    }
    names
}

fn push_unique_tls_name(names: &mut Vec<String>, value: &str) {
    if !names.iter().any(|name| name == value) {
        names.push(value.to_owned());
    }
}

async fn start_runtime_api_worker(
    app_config: &AppConfig,
    config: &openplotva_config::RuntimeApiConfig,
    service_clients: &ServiceClients,
    diagnostics: openplotva_server::RuntimeApiLiveDiagnostics,
    stop: watch::Receiver<bool>,
) -> anyhow::Result<(JoinHandle<()>, std::net::SocketAddr, String)> {
    let bind_addr = runtime_api_bind_addr(config);
    let tcp_listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("bind runtime API TLS listener to {bind_addr}"))?;
    let local_addr = tcp_listener
        .local_addr()
        .context("read runtime API listener address")?;
    let tls_material = runtime_api_tls_material(config)?;
    let tls_acceptor = openplotva_server::runtime_api_tls_acceptor_from_pem(
        &tls_material.cert_pem,
        &tls_material.key_pem,
    )
    .context("load runtime API TLS material")?;
    let token_store = PostgresRuntimeTokenStore::new(service_clients.postgres.clone());
    let token_manager = runtime_api::RuntimeTokenManager::new(token_store);
    let app = openplotva_server::runtime_api_router_with_graphql_live_diagnostics(
        token_manager,
        runtime_api_graphql_snapshot(app_config),
        diagnostics,
    );
    let tls_listener = openplotva_server::RuntimeApiTlsListener::new(tcp_listener, tls_acceptor);
    let tls_public_key_pin = tls_material.tls_public_key_pin;

    tracing::info!(
        address = %local_addr,
        tls_public_key_pin = %tls_public_key_pin,
        tls_material = tls_material.source,
        "runtime API listening"
    );

    let worker = tokio::spawn(async move {
        let result = axum::serve(tls_listener, app)
            .with_graceful_shutdown(wait_for_runtime_stop(stop))
            .await;
        if let Err(error) = result {
            tracing::warn!(%error, "runtime API server stopped with error");
        }
    });

    Ok((worker, local_addr, tls_public_key_pin))
}

async fn start_runtime_workers(
    config: &AppConfig,
    service_clients: Option<&ServiceClients>,
    readiness_checks: &mut Vec<ReadinessCheck>,
    log_buffer: Arc<openplotva_observability::RuntimeLogBuffer>,
    prompt_store: Arc<openplotva_prompts::PromptStore>,
) -> anyhow::Result<RuntimeWorkers> {
    let Some(service_clients) = service_clients else {
        if config.runtime_api.enabled {
            anyhow::bail!(
                "RUNTIME_API_ENABLED=true requires OPENPLOTVA_CONNECT_SERVICES=true for runtime token validation"
            );
        }
        readiness_checks.push(ReadinessCheck::skipped(
            "runtime_api",
            "RUNTIME_API_ENABLED=false",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "pending_ops",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "outbound_dispatcher_restore",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "outbound_dispatcher",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_update_producer",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_update_consumer",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_outbox",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_commands",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_get_me",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "control_jobs",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "shared_task_queue_restore",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "shared_task_queue_snapshot",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "dialog_jobs",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "image_jobs",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "music_jobs",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        return Ok(RuntimeWorkers::default());
    };

    let worker_registry = Arc::new(RuntimeWorkerRegistry::default());
    let worker_stops = Arc::new(RuntimeWorkerStops::new());
    let taskman_inspector = runtime_taskman::RuntimeTaskmanInspectorHandle::default();
    let virtual_dialog_manager =
        runtime_virtual_dialog::RuntimeVirtualDialogManagerHandle::default();
    let dispatcher_inspector = runtime_dispatcher::RuntimeDispatcherInspectorHandle::default();
    let dispatch_failure_ring = Arc::new(DispatchFailureRing::default());
    let cache_inspector = runtime_cache::RuntimeCacheInspectorHandle::default();
    let mut workers = RuntimeWorkers {
        worker_registry: Arc::clone(&worker_registry),
        worker_stops: Arc::clone(&worker_stops),
        dispatcher: None,
        shared_task_queue: None,
        dialog_debounce: None,
        webhook_route: None,
        telegram: None,
        delete_webhook_on_shutdown: false,
        bot_username: None,
        bot_id: None,
        dispatcher_inspector: dispatcher_inspector.clone(),
        cache_inspector: cache_inspector.clone(),
        taskman_inspector: taskman_inspector.clone(),
        memory_restart_trigger: None,
        llm_trace_buffer: None,
        llm_run_buffer: None,
        routing_event_buffer: None,
        routing_event_reporter: None,
        runtime_api_tls_public_key_pin: None,
        attempt_walker: None,
        router_handle: None,
        router_breakers: None,
        router_triggers: None,
        router_pools: None,
        openrouter_free_gate: None,
        router_runtime: None,
        dialog_worker_gauge: None,
        memory_card_cache: None,
        readiness_probes: Vec::new(),
    };
    let readiness_general_postgres = service_clients.postgres.clone();
    let readiness_critical_postgres = service_clients.critical_postgres.clone();
    workers.readiness_probes.push(Arc::new(move || {
        postgres_pool_readiness(&readiness_general_postgres, &readiness_critical_postgres)
    }));
    let readiness_worker_registry = Arc::clone(&worker_registry);
    workers.readiness_probes.push(Arc::new(move || {
        readiness_worker_registry.readiness_check()
    }));
    let routing_event_buffer = runtime_routing::RoutingEventBuffer::default();
    let (routing_event_recorder, routing_event_recorder_worker) =
        runtime_routing::PostgresRoutingEventRecorder::spawn(
            service_clients.postgres.clone(),
            worker_stops.subscribe(RuntimeWorkerPhase::Ancillary),
        );
    worker_registry.register(
        "routing-event-recorder",
        RuntimeWorkerPhase::Ancillary,
        routing_event_recorder_worker,
    );
    let routing_admin_report_trigger = Arc::new(tokio::sync::Notify::new());
    let routing_event_reporter = runtime_routing::RoutingEventReporter::new(
        routing_event_buffer.clone(),
        Some(routing_event_recorder.clone()),
        Some(Arc::clone(&routing_admin_report_trigger)),
    );
    workers.routing_event_buffer = Some(routing_event_buffer.clone());
    workers.routing_event_reporter = Some(routing_event_reporter.clone());
    // Seed and correct the routing tables, then publish the config-only flow resolver
    // BEFORE the per-flow workers are built, so memory / history / agentic-reasoner pick
    // up the DB-selected model. Every step is idempotent (flag/existence guarded).
    if let Err(error) =
        model_routing::seed_routing_from_env(&service_clients.postgres, config).await
    {
        routing_event_reporter.record(runtime_routing::routing_backfill_failed_event(
            "seed_routing_from_env",
            &error.to_string(),
        ));
        tracing::warn!(%error, "failed to seed LLM routing tables; continuing with existing rows");
    }
    if let Err(error) =
        model_routing::backfill_vram_cloud_from_env(&service_clients.postgres, config).await
    {
        routing_event_reporter.record(runtime_routing::routing_backfill_failed_event(
            "backfill_vram_cloud_from_env",
            &error.to_string(),
        ));
        tracing::warn!(%error, "failed to backfill AI Farm pool into routing tables");
    }
    if let Err(error) =
        model_routing::backfill_vram_cloud_vision_fallback(&service_clients.postgres).await
    {
        routing_event_reporter.record(runtime_routing::routing_backfill_failed_event(
            "backfill_vram_cloud_vision_fallback",
            &error.to_string(),
        ));
        tracing::warn!(%error, "failed to backfill VRAM Cloud vision fallbacks");
    }
    if let Err(error) = model_routing::backfill_gpu_models(&service_clients.postgres, config).await
    {
        routing_event_reporter.record(runtime_routing::routing_backfill_failed_event(
            "backfill_gpu_models",
            &error.to_string(),
        ));
        tracing::warn!(%error, "failed to backfill GPU llama.cpp models into routing tables");
    }
    if let Err(error) =
        model_routing::backfill_dialog_local_reasoner_fallback(&service_clients.postgres, config)
            .await
    {
        routing_event_reporter.record(runtime_routing::routing_backfill_failed_event(
            "backfill_dialog_local_reasoner_fallback",
            &error.to_string(),
        ));
        tracing::warn!(%error, "failed to backfill dialog GPU local-reasoner fallback");
    }
    if let Err(error) =
        model_routing::backfill_genkit_flash_model(&service_clients.postgres, config).await
    {
        routing_event_reporter.record(runtime_routing::routing_backfill_failed_event(
            "backfill_genkit_flash_model",
            &error.to_string(),
        ));
        tracing::warn!(%error, "failed to correct genkit dialog fallback model");
    }
    if let Err(error) =
        model_routing::backfill_declarative_v2(&service_clients.postgres, config).await
    {
        routing_event_reporter.record(runtime_routing::routing_backfill_failed_event(
            "backfill_declarative_v2",
            &error.to_string(),
        ));
        tracing::warn!(%error, "failed to backfill declarative routing v2 rows");
    }
    if let Err(error) =
        model_routing::backfill_image_generation_draw_api_primary(&service_clients.postgres, config)
            .await
    {
        routing_event_reporter.record(runtime_routing::routing_backfill_failed_event(
            "backfill_image_generation_draw_api_primary",
            &error.to_string(),
        ));
        tracing::warn!(%error, "failed to backfill image generation draw-api primary");
    }
    if let Err(error) =
        model_routing::backfill_boogu_image_slots(&service_clients.postgres, config).await
    {
        routing_event_reporter.record(runtime_routing::routing_backfill_failed_event(
            "backfill_boogu_image_slots",
            &error.to_string(),
        ));
        tracing::warn!(%error, "failed to backfill Boogu image slot workflows");
    }
    if let Err(error) = model_routing::backfill_asr_routing(&service_clients.postgres, config).await
    {
        routing_event_reporter.record(runtime_routing::routing_backfill_failed_event(
            "backfill_asr_routing",
            &error.to_string(),
        ));
        tracing::warn!(%error, "failed to backfill ASR routing rows");
    }
    if let Err(error) = model_routing::backfill_provider_protocols(&service_clients.postgres).await
    {
        routing_event_reporter.record(runtime_routing::routing_backfill_failed_event(
            "backfill_provider_protocols",
            &error.to_string(),
        ));
        tracing::warn!(%error, "failed to backfill provider protocols");
    }
    // Runs after every provider/model backfill above so the pools can attach
    // to whatever those steps created.
    if let Err(error) =
        model_routing::backfill_capacity_pools(&service_clients.postgres, config).await
    {
        routing_event_reporter.record(runtime_routing::routing_backfill_failed_event(
            "backfill_capacity_pools",
            &error.to_string(),
        ));
        tracing::warn!(%error, "failed to backfill capacity pools");
    }
    if let Err(error) =
        model_routing::backfill_memory_routing_cascades(&service_clients.postgres).await
    {
        routing_event_reporter.record(runtime_routing::routing_backfill_failed_event(
            "backfill_memory_routing_cascades",
            &error.to_string(),
        ));
        tracing::warn!(%error, "failed to backfill independent memory routing cascades");
    }
    match openrouter_free_pool::load_pool_config(&service_clients.postgres).await {
        Ok(pool_config) if pool_config.refresh_on_startup => {
            let http = reqwest::Client::new();
            match openrouter_free_pool::refresh_openrouter_free_pool(
                &service_clients.postgres,
                &http,
            )
            .await
            {
                Ok(report) => {
                    routing_event_reporter.record(runtime_routing::RoutingEvent {
                        severity: "info".to_owned(),
                        event_type: "openrouter_free_refresh_succeeded".to_owned(),
                        workflow_key: "routing".to_owned(),
                        provider_id: None,
                        model_id: None,
                        queue_name: None,
                        job_id: None,
                        chat_id: None,
                        user_id: None,
                        thread_id: None,
                        message_id: None,
                        dedupe_key: "openrouter_free_refresh_succeeded:startup".to_owned(),
                        summary: "OpenRouter Free startup refresh succeeded".to_owned(),
                        detail: json!({
                            "active_models": report.active_models,
                            "disabled_models": report.disabled_models,
                            "assignments": report.assignments,
                            "ranking_version": report.ranking_version,
                            "source_updated_at": report.source_updated_at,
                            "key_status_error": report.key_status_error.clone(),
                        }),
                    });
                    if let Some(error) = report.key_status_error.as_deref() {
                        runtime_routing::record_openrouter_free_key_status_failed(
                            &routing_event_reporter,
                            "startup",
                            error,
                        );
                    }
                }
                Err(error) => {
                    routing_event_reporter.record(runtime_routing::RoutingEvent {
                        severity: "warn".to_owned(),
                        event_type: "openrouter_free_refresh_failed".to_owned(),
                        workflow_key: "routing".to_owned(),
                        provider_id: None,
                        model_id: None,
                        queue_name: None,
                        job_id: None,
                        chat_id: None,
                        user_id: None,
                        thread_id: None,
                        message_id: None,
                        dedupe_key: "openrouter_free_refresh_failed:startup".to_owned(),
                        summary: "OpenRouter Free startup refresh failed".to_owned(),
                        detail: json!({ "error": error.to_string(), "source": "startup" }),
                    });
                    tracing::warn!(%error, "failed to refresh OpenRouter Free pool on startup");
                }
            }
        }
        Ok(_) => {}
        Err(error) => {
            tracing::debug!(%error, "OpenRouter Free pool config unavailable; startup refresh skipped");
        }
    }
    let router_breakers = Arc::new(openplotva_llm::router::BreakerSet::new());
    let router_triggers = Arc::new(openplotva_llm::router::TriggerState::new());
    let router_pools = Arc::new(openplotva_llm::router::PoolRegistry::new());
    let openrouter_free_gate = Arc::new(openrouter_free_pool::OpenRouterFreeQuotaGate::new(
        service_clients.redis.client().clone(),
        service_clients.postgres.clone(),
    ));
    let router_handle = match model_routing::load_routing_table(&service_clients.postgres).await {
        Ok(table) => openplotva_llm::router::RouterHandle::new(table),
        Err(error) => {
            routing_event_reporter.record(runtime_routing::router_reload_failed_event(
                "load_routing_table",
                &error.to_string(),
            ));
            tracing::warn!(%error, "failed to load LLM routing table; starting with an empty table");
            openplotva_llm::router::RouterHandle::new(
                openplotva_llm::router::RoutingTable::default(),
            )
        }
    };
    let attempt_walker = routed_attempts::RoutedAttemptWalker::new(
        Arc::clone(&router_handle),
        Arc::clone(&router_breakers),
        Arc::clone(&router_triggers),
        Arc::clone(&router_pools),
    )
    .with_openrouter_free_gate(Arc::clone(&openrouter_free_gate))
    .with_reporter(routing_event_reporter.clone());
    workers.attempt_walker = Some(attempt_walker.clone());
    workers.router_handle = Some(Arc::clone(&router_handle));
    workers.router_breakers = Some(Arc::clone(&router_breakers));
    workers.router_triggers = Some(Arc::clone(&router_triggers));
    workers.router_pools = Some(Arc::clone(&router_pools));
    workers.openrouter_free_gate = Some(Arc::clone(&openrouter_free_gate));
    router_pools.apply(&router_handle.snapshot().pool_specs());
    let lane_check = lane_checks::LaneCheck {
        discovery_base_url: config.llm.discovery.base_url.clone(),
        reporter: Some(routing_event_reporter.clone()),
    };
    readiness_checks.push(ReadinessCheck::ok(
        "lane_config",
        lane_check.run(&router_handle.snapshot()).await.summary(),
    ));
    {
        let pool = service_clients.postgres.clone();
        let handle = Arc::clone(&router_handle);
        let pools = Arc::clone(&router_pools);
        let reporter = routing_event_reporter.clone();
        let mut stop_rx = worker_stops.subscribe(RuntimeWorkerPhase::Ancillary);
        worker_registry.register(
            "openrouter-free-refresh",
            RuntimeWorkerPhase::Ancillary,
            tokio::spawn(async move {
            loop {
                let pool_config = match openrouter_free_pool::load_pool_config(&pool).await {
                    Ok(config) => config,
                    Err(error) => {
                        tracing::debug!(%error, "OpenRouter Free daily refresh skipped; managed pool config unavailable");
                        let interval = Duration::from_secs(86_400);
                        tokio::select! {
                            changed = stop_rx.changed() => {
                                if changed.is_err() || *stop_rx.borrow() {
                                    break;
                                }
                            }
                            () = tokio::time::sleep(interval) => {}
                        }
                        continue;
                    }
                };
                let interval = Duration::from_secs(pool_config.refresh_interval_seconds.max(60));
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    () = tokio::time::sleep(interval) => {
                        if !pool_config.auto_refresh_enabled {
                            continue;
                        }
                        let http = reqwest::Client::new();
                        match openrouter_free_pool::refresh_openrouter_free_pool(&pool, &http).await {
                            Ok(report) => {
                                if let Err(error) = model_routing::reload_router(&handle, &pool).await {
                                    reporter.record(runtime_routing::router_reload_failed_event(
                                        "openrouter_free_daily_refresh",
                                        &error.to_string(),
                                    ));
                                } else {
                                    pools.apply(&handle.snapshot().pool_specs());
                                }
                                reporter.record(runtime_routing::RoutingEvent {
                                    severity: "info".to_owned(),
                                    event_type: "openrouter_free_refresh_succeeded".to_owned(),
                                    workflow_key: "routing".to_owned(),
                                    provider_id: None,
                                    model_id: None,
                                    queue_name: None,
                                    job_id: None,
                                    chat_id: None,
                                    user_id: None,
                                    thread_id: None,
                                    message_id: None,
                                    dedupe_key: "openrouter_free_refresh_succeeded:daily".to_owned(),
                                    summary: "OpenRouter Free daily refresh succeeded".to_owned(),
                                    detail: json!({
                                        "active_models": report.active_models,
                                        "disabled_models": report.disabled_models,
                                        "assignments": report.assignments,
                                        "ranking_version": report.ranking_version,
                                        "source_updated_at": report.source_updated_at,
                                        "key_status_error": report.key_status_error.clone(),
                                    }),
                                });
                                if let Some(error) = report.key_status_error.as_deref() {
                                    runtime_routing::record_openrouter_free_key_status_failed(
                                        &reporter,
                                        "daily",
                                        error,
                                    );
                                }
                            }
                            Err(error) => {
                                reporter.record(runtime_routing::RoutingEvent {
                                    severity: "warn".to_owned(),
                                    event_type: "openrouter_free_refresh_failed".to_owned(),
                                    workflow_key: "routing".to_owned(),
                                    provider_id: None,
                                    model_id: None,
                                    queue_name: None,
                                    job_id: None,
                                    chat_id: None,
                                    user_id: None,
                                    thread_id: None,
                                    message_id: None,
                                    dedupe_key: "openrouter_free_refresh_failed:daily".to_owned(),
                                    summary: "OpenRouter Free daily refresh failed".to_owned(),
                                    detail: json!({ "error": error.to_string(), "source": "daily" }),
                                });
                            }
                        }
                    }
                }
            }
            }),
        );
    }
    let update_queue =
        openplotva_updates::RedisUpdateQueue::new(service_clients.redis.client().clone());
    let mut update_consumer_config = openplotva_updates::UpdateConsumerConfig::default();
    update_consumer_config.worker_limit =
        durable_update_consumer_worker_limit(update_consumer_config.worker_limit);
    let update_stream_redis_client = if config.update_queue.stream_redis_url.is_empty() {
        service_clients.redis.client().clone()
    } else {
        redis::Client::open(config.update_queue.stream_redis_url.as_str())
            .context("create dedicated Telegram ingress Redis client")?
    };
    let updates_inspector = runtime_updates::RuntimeUpdatesInspectorHandle::new(
        update_queue.clone(),
        update_consumer_config.worker_limit,
    );
    let ingestion_gate_counters = Arc::new(ingestion_telemetry::IngestionGateCounters::default());
    updates_inspector.set_gate_counters(Arc::clone(&ingestion_gate_counters));
    let llm_trace_buffer = runtime_llm::RuntimeLlmTraceBuffer::default();
    let (llm_event_recorder, llm_event_recorder_worker) =
        runtime_llm::PostgresRuntimeLlmEventRecorder::spawn(
            service_clients.postgres.clone(),
            worker_stops.subscribe(RuntimeWorkerPhase::Ancillary),
            runtime_llm::RawBodyPolicy {
                enabled: config.runtime_api.llm_raw_body_persist_enabled,
                max_bytes: usize::try_from(config.runtime_api.llm_raw_body_max_bytes)
                    .unwrap_or(65_536),
            },
        );
    worker_registry.register(
        "llm-event-recorder",
        RuntimeWorkerPhase::Ancillary,
        llm_event_recorder_worker,
    );
    let turn_outcome_buffer = dialog_turn::RuntimeTurnOutcomeBuffer::default();
    let (turn_outcome_recorder, turn_outcome_recorder_worker) =
        dialog_turn::PostgresDialogTurnOutcomeRecorder::spawn(
            service_clients.postgres.clone(),
            worker_stops.subscribe(RuntimeWorkerPhase::Ancillary),
        );
    worker_registry.register(
        "turn-outcome-recorder",
        RuntimeWorkerPhase::Ancillary,
        turn_outcome_recorder_worker,
    );
    let llm_run_buffer = runtime_llm_runs::RuntimeLlmRunBuffer::new(
        usize::try_from(config.runtime_api.llm_run_buffer_capacity)
            .unwrap_or(runtime_llm_runs::DEFAULT_LLM_RUN_BUFFER_CAPACITY)
            .max(1),
    );
    workers.llm_run_buffer = Some(llm_run_buffer.clone());
    let turn_outcome_observer = dialog_turn::DialogTurnObserver::new(
        turn_outcome_buffer.clone(),
        Some(turn_outcome_recorder),
    )
    .with_run_buffer(llm_run_buffer.clone());
    let turn_outcome_cleanup_pool = service_clients.postgres.clone();
    let turn_outcome_cleanup_stop = worker_stops.subscribe(RuntimeWorkerPhase::Ancillary);
    let turn_outcome_cleanup_worker = tokio::spawn(async move {
        let obligation_store = openplotva_storage::PostgresDeliveryObligationStore::new(
            turn_outcome_cleanup_pool.clone(),
        );
        let mut stop = std::pin::pin!(wait_for_runtime_stop(turn_outcome_cleanup_stop));
        loop {
            tokio::select! {
                () = &mut stop => break,
                () = tokio::time::sleep(std::time::Duration::from_secs(6 * 60 * 60)) => {
                    match dialog_turn::delete_old_turn_outcomes_batch(
                        &turn_outcome_cleanup_pool,
                        30,
                        5_000,
                    )
                    .await
                    {
                        Ok(deleted) if deleted > 0 => {
                            tracing::info!(deleted, "deleted old dialog turn outcomes");
                        }
                        Ok(_) => {}
                        Err(error) => {
                            tracing::warn!(%error, "dialog turn outcome cleanup failed");
                        }
                    }
                    match obligation_store
                        .delete_resolved_delivery_obligations_batch(30, 5_000)
                        .await
                    {
                        Ok(deleted) if deleted > 0 => {
                            tracing::info!(
                                deleted,
                                "deleted old resolved dialog delivery obligations"
                            );
                        }
                        Ok(_) => {}
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                "dialog delivery obligation cleanup failed"
                            );
                        }
                    }
                }
            }
        }
    });
    worker_registry.register(
        "turn-outcome-cleanup",
        RuntimeWorkerPhase::Ancillary,
        turn_outcome_cleanup_worker,
    );
    let llm_request_events_retention_days = config.runtime_api.llm_request_events_retention_days;
    if llm_request_events_retention_days > 0 {
        let llm_event_cleanup_pool = service_clients.postgres.clone();
        let llm_event_cleanup_stop = worker_stops.subscribe(RuntimeWorkerPhase::Ancillary);
        let llm_event_cleanup_interval = runtime_llm::LLM_REQUEST_EVENTS_CLEANUP_INTERVAL;
        let llm_event_cleanup_worker = tokio::spawn(async move {
            let report = runtime_llm::run_llm_request_event_cleanup_worker_until(
                llm_event_cleanup_pool,
                runtime_llm::LLM_REQUEST_EVENTS_CLEANUP_FIRST_PASS_DELAY,
                llm_event_cleanup_interval,
                llm_request_events_retention_days,
                wait_for_runtime_stop(llm_event_cleanup_stop),
            )
            .await;

            tracing::info!(?report, "llm_request_events cleanup worker stopped");
        });
        readiness_checks.push(ReadinessCheck::ok(
            "llm_request_events_cleanup",
            format!(
                "LLM analytics rollup and raw request event partition drop every {}s after a {}s start delay, retention {}d",
                llm_event_cleanup_interval.as_secs(),
                runtime_llm::LLM_REQUEST_EVENTS_CLEANUP_FIRST_PASS_DELAY.as_secs(),
                llm_request_events_retention_days
            ),
        ));
        worker_registry.register(
            "llm-event-cleanup",
            RuntimeWorkerPhase::Ancillary,
            llm_event_cleanup_worker,
        );
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "llm_request_events_cleanup",
            "LLM raw request events cleanup disabled",
        ));
    }
    let llm_raw_body_retention_hours = config.runtime_api.llm_raw_body_retention_hours;
    if config.runtime_api.llm_raw_body_persist_enabled && llm_raw_body_retention_hours > 0 {
        let llm_raw_scrub_pool = service_clients.postgres.clone();
        let llm_raw_scrub_stop = worker_stops.subscribe(RuntimeWorkerPhase::Ancillary);
        let llm_raw_scrub_worker = tokio::spawn(async move {
            let report = runtime_llm::run_llm_raw_body_scrub_worker_until(
                llm_raw_scrub_pool,
                std::time::Duration::from_secs(60 * 60),
                llm_raw_body_retention_hours,
                runtime_llm::LLM_REQUEST_EVENTS_CLEANUP_BATCH_SIZE,
                wait_for_runtime_stop(llm_raw_scrub_stop),
            )
            .await;

            tracing::info!(?report, "llm raw body scrub worker stopped");
        });
        readiness_checks.push(ReadinessCheck::ok(
            "llm_raw_body_scrub",
            format!("LLM raw bodies scrubbed hourly, retention {llm_raw_body_retention_hours}h"),
        ));
        worker_registry.register(
            "llm-raw-body-scrub",
            RuntimeWorkerPhase::Ancillary,
            llm_raw_scrub_worker,
        );
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "llm_raw_body_scrub",
            "LLM raw body persistence disabled",
        ));
    }
    let chat_history_retention_days = config.llm.history_summary.chat_history_retention_days;
    if chat_history_retention_days > 0 {
        let pool = service_clients.postgres.clone();
        let stop_rx = worker_stops.subscribe(RuntimeWorkerPhase::Ancillary);
        let worker = tokio::spawn(async move {
            let report = runtime_retention::run_chat_history_partition_retention_worker_until(
                pool,
                runtime_retention::RETENTION_CLEANUP_INTERVAL,
                chat_history_retention_days,
                wait_for_runtime_stop(stop_rx),
            )
            .await;
            tracing::info!(?report, "chat_history partition retention worker stopped");
        });
        readiness_checks.push(ReadinessCheck::ok(
            "chat_history_retention",
            format!(
                "chat_history partitions dropped daily, retention {chat_history_retention_days}d"
            ),
        ));
        worker_registry.register(
            "chat-history-retention",
            RuntimeWorkerPhase::Ancillary,
            worker,
        );
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "chat_history_retention",
            "chat_history partition retention disabled",
        ));
    }
    let telegram_update_retention_store =
        openplotva_storage::PostgresTelegramDeliveryStore::new(service_clients.postgres.clone());
    let telegram_update_retention_stop = worker_stops.subscribe(RuntimeWorkerPhase::Ancillary);
    let telegram_update_retention_worker = tokio::spawn(async move {
        let report = runtime_retention::run_telegram_update_retention_worker_until(
            telegram_update_retention_store,
            runtime_retention::RETENTION_CLEANUP_INTERVAL,
            runtime_retention::RETENTION_DELETE_BATCH_SIZE,
            runtime_retention::RETENTION_INTER_BATCH_PAUSE,
            wait_for_runtime_stop(telegram_update_retention_stop),
        )
        .await;
        tracing::info!(?report, "Telegram update retention worker stopped");
    });
    readiness_checks.push(ReadinessCheck::ok(
        "telegram_update_retention",
        format!(
            "terminal inbox and quarantine rows before the current UTC day deleted every {}s in batches of {}",
            runtime_retention::RETENTION_CLEANUP_INTERVAL.as_secs(),
            runtime_retention::RETENTION_DELETE_BATCH_SIZE
        ),
    ));
    worker_registry.register(
        "telegram-update-retention",
        RuntimeWorkerPhase::Ancillary,
        telegram_update_retention_worker,
    );
    let partition_pool = service_clients.postgres.clone();
    let partition_stop = worker_stops.subscribe(RuntimeWorkerPhase::Ancillary);
    let outbox_retention_days = config.update_queue.outbox_retention_days;
    let partition_worker = tokio::spawn(async move {
        let report = runtime_retention::run_daily_partition_maintenance_worker_until(
            partition_pool,
            runtime_retention::PARTITION_MAINTENANCE_INTERVAL,
            outbox_retention_days,
            wait_for_runtime_stop(partition_stop),
        )
        .await;
        tracing::info!(?report, "daily partition maintenance worker stopped");
    });
    readiness_checks.push(ReadinessCheck::ok(
        "daily_partition_maintenance",
        format!(
            "daily partitions kept {} days ahead every {}s; Telegram outbox retention {}d",
            openplotva_storage::DAILY_PARTITIONS_AHEAD,
            runtime_retention::PARTITION_MAINTENANCE_INTERVAL.as_secs(),
            outbox_retention_days
        ),
    ));
    worker_registry.register(
        "daily-partition-maintenance",
        RuntimeWorkerPhase::Ancillary,
        partition_worker,
    );
    let telegram_files_retention_days = config.vision.telegram_files_retention_days;
    if telegram_files_retention_days > 0 {
        let pool = service_clients.postgres.clone();
        let stop_rx = worker_stops.subscribe(RuntimeWorkerPhase::Ancillary);
        let worker = tokio::spawn(async move {
            let report = runtime_retention::run_telegram_files_retention_worker_until(
                pool,
                runtime_retention::RETENTION_CLEANUP_INTERVAL,
                telegram_files_retention_days,
                runtime_retention::RETENTION_DELETE_BATCH_SIZE,
                runtime_retention::RETENTION_INTER_BATCH_PAUSE,
                wait_for_runtime_stop(stop_rx),
            )
            .await;
            tracing::info!(?report, "telegram_files retention worker stopped");
        });
        readiness_checks.push(ReadinessCheck::ok(
            "telegram_files_retention",
            format!(
                "telegram_files deleted by last_seen_at daily, retention {telegram_files_retention_days}d"
            ),
        ));
        worker_registry.register(
            "telegram-files-retention",
            RuntimeWorkerPhase::Ancillary,
            worker,
        );
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_files_retention",
            "telegram_files retention disabled",
        ));
    }
    let whitecircle_checks_retention_days = config.white_circle.whitecircle_checks_retention_days;
    if whitecircle_checks_retention_days > 0 {
        let pool = service_clients.postgres.clone();
        let stop_rx = worker_stops.subscribe(RuntimeWorkerPhase::Ancillary);
        let worker = tokio::spawn(async move {
            let report = runtime_retention::run_whitecircle_checks_retention_worker_until(
                pool,
                runtime_retention::RETENTION_CLEANUP_INTERVAL,
                whitecircle_checks_retention_days,
                runtime_retention::RETENTION_DELETE_BATCH_SIZE,
                runtime_retention::RETENTION_INTER_BATCH_PAUSE,
                wait_for_runtime_stop(stop_rx),
            )
            .await;
            tracing::info!(?report, "whitecircle_checks retention worker stopped");
        });
        readiness_checks.push(ReadinessCheck::ok(
            "whitecircle_checks_retention",
            format!(
                "whitecircle_checks deleted by created_at daily, retention {whitecircle_checks_retention_days}d"
            ),
        ));
        worker_registry.register(
            "whitecircle-checks-retention",
            RuntimeWorkerPhase::Ancillary,
            worker,
        );
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "whitecircle_checks_retention",
            "whitecircle_checks retention disabled",
        ));
    }
    if llm_request_events_retention_days > 0 {
        let routing_event_cleanup_pool = service_clients.postgres.clone();
        let routing_event_cleanup_stop = worker_stops.subscribe(RuntimeWorkerPhase::Ancillary);
        let routing_event_cleanup_interval = runtime_routing::LLM_ROUTING_EVENTS_CLEANUP_INTERVAL;
        let routing_event_cleanup_worker = tokio::spawn(async move {
            let report = runtime_routing::run_llm_routing_event_cleanup_worker_until(
                routing_event_cleanup_pool,
                routing_event_cleanup_interval,
                llm_request_events_retention_days,
                runtime_routing::LLM_ROUTING_EVENTS_CLEANUP_BATCH_SIZE,
                wait_for_runtime_stop(routing_event_cleanup_stop),
            )
            .await;

            tracing::info!(?report, "llm_routing_events cleanup worker stopped");
        });
        readiness_checks.push(ReadinessCheck::ok(
            "llm_routing_events_cleanup",
            format!(
                "LLM routing events cleanup every {}s, retention {}d",
                routing_event_cleanup_interval.as_secs(),
                llm_request_events_retention_days
            ),
        ));
        worker_registry.register(
            "routing-event-cleanup",
            RuntimeWorkerPhase::Ancillary,
            routing_event_cleanup_worker,
        );
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "llm_routing_events_cleanup",
            "LLM routing events cleanup disabled",
        ));
    }
    workers.llm_trace_buffer = Some(llm_trace_buffer.clone());
    let llm_observer: Arc<dyn openplotva_llm::LlmCallObserver> = Arc::new(
        runtime_llm::RuntimeLlmObserver::new(
            llm_trace_buffer.clone(),
            Some(llm_event_recorder.clone()),
        )
        .with_run_buffer(llm_run_buffer.clone()),
    );
    openplotva_llm::trace::set_observer(llm_observer);
    let memory_card_cache = usize::try_from(config.memory.card_cache_entries)
        .ok()
        .filter(|entries| *entries > 0)
        .map(|entries| {
            Arc::new(openplotva_storage::MemoryCardCache::new(
                entries,
                openplotva_storage::MEMORY_CARD_CACHE_TTL,
            ))
        });
    workers.memory_card_cache = memory_card_cache.clone();
    let memory_store = match &memory_card_cache {
        Some(cache) => PostgresMemoryStore::new(service_clients.postgres.clone())
            .with_card_cache(Arc::clone(cache)),
        None => PostgresMemoryStore::new(service_clients.postgres.clone()),
    };
    let memory_card_usage = memory_card_usage::MemoryCardUsageJournal::new();
    {
        let journal = memory_card_usage.clone();
        let store = memory_store.clone();
        let cache = memory_card_cache.clone();
        let stop = worker_stops.subscribe(RuntimeWorkerPhase::Ancillary);
        let worker = tokio::spawn(async move {
            let report = memory_card_usage::run_memory_card_usage_flush_worker_until(
                journal,
                store,
                cache,
                memory_card_usage::MEMORY_CARD_USAGE_FLUSH_INTERVAL,
                wait_for_runtime_stop(stop),
            )
            .await;
            tracing::info!(?report, "memory card usage flush worker stopped");
        });
        readiness_checks.push(ReadinessCheck::ok(
            "memory_card_usage_flush",
            format!(
                "memory card uses flushed every {}s; visible-card cache {} entries",
                memory_card_usage::MEMORY_CARD_USAGE_FLUSH_INTERVAL.as_secs(),
                config.memory.card_cache_entries
            ),
        ));
        worker_registry.register(
            "memory-card-usage-flush",
            RuntimeWorkerPhase::Ancillary,
            worker,
        );
    }
    let memory_restart_trigger = Arc::new(tokio::sync::Notify::new());
    let virtual_dialog_store =
        PostgresRuntimeVirtualDialogStore::new(service_clients.postgres.clone());

    if config.runtime_api.enabled {
        let runtime_taskman_reader: Arc<dyn openplotva_server::RuntimeTaskmanInspector> =
            Arc::new(taskman_inspector.clone());
        let gemini_cache_purger = runtime_gemini_cache::GeminiExplicitCachePurger::from_config(
            &config.google_ai,
        )
        .map(|purger| Arc::new(purger) as Arc<dyn openplotva_server::RuntimeGeminiCachePurger>);
        let diagnostics = openplotva_server::RuntimeApiLiveDiagnostics {
            redis_inspector: Some(Arc::new(RedisRuntimeInspector::new(
                service_clients.redis.client().clone(),
            ))),
            sql_reader: Some(Arc::new(runtime_sql::PostgresRuntimeSqlReader::new(
                service_clients.postgres.clone(),
            ))),
            entity_reader: Some(Arc::new(
                runtime_entities::PostgresRuntimeEntityReader::new(
                    service_clients.postgres.clone(),
                ),
            )),
            safety_check_reader: Some(Arc::new(
                runtime_safety::PostgresRuntimeSafetyCheckReader::new(
                    service_clients.postgres.clone(),
                ),
            )),
            llm_trace_inspector: Some(Arc::new(llm_trace_buffer.clone())),
            llm_run_inspector: Some(Arc::new(llm_run_buffer.clone())),
            turn_outcome_inspector: Some(Arc::new(turn_outcome_buffer.clone())),
            routing_event_inspector: Some(Arc::new(routing_event_buffer.clone())),
            lane_inspector: Some(Arc::new(lane_checks::RuntimeLanes {
                handle: Arc::clone(&router_handle),
                pools: Arc::clone(&router_pools),
            })),
            llm_analytics_reader: Some(Arc::new(
                runtime_llm_analytics::PostgresRuntimeLlmAnalyticsReader::new(
                    service_clients.postgres.clone(),
                )
                .with_discovery_capacity(
                    config.llm.discovery.base_url.clone(),
                    config.llm.dialog.discovery_service_name.clone(),
                )
                .with_sql_timeout_ms(config.runtime_api.sql_timeout_ms)
                .with_taskman(Arc::clone(&runtime_taskman_reader)),
            )),
            gradius_audit_reader: Some(Arc::new(
                runtime_gradius::PostgresRuntimeGradiusAuditReader::new(
                    service_clients.postgres.clone(),
                ),
            )),
            log_inspector: Some(Arc::new(RuntimeLogInspector::new(Arc::clone(&log_buffer)))),
            taskman_inspector: Some(runtime_taskman_reader),
            updates_inspector: Some(Arc::new(updates_inspector.clone())),
            telegram_delivery_inspector: Some(Arc::new(
                runtime_telegram_delivery::PostgresRuntimeTelegramDeliveryInspector::new(
                    service_clients.postgres.clone(),
                ),
            )),
            dispatcher_inspector: Some(Arc::new(dispatcher_inspector.clone())),
            dispatcher_failure_inspector: Some(Arc::clone(&dispatch_failure_ring)
                as Arc<dyn openplotva_server::RuntimeDispatcherFailureInspector>),
            cache_inspector: Some(Arc::new(cache_inspector.clone())),
            memory_restarter: config.memory.enabled.then(|| {
                Arc::new(memory_runtime::RuntimeMemoryRestarter::new(
                    memory_store.clone(),
                    Arc::clone(&memory_restart_trigger),
                    config.memory.consolidation_model.clone(),
                )) as Arc<dyn openplotva_server::RuntimeMemoryRestarter>
            }),
            gemini_cache_purger,
            virtual_dialog_manager: Some(Arc::new(virtual_dialog_manager.clone())),
        };
        let (worker, local_addr, tls_public_key_pin) = start_runtime_api_worker(
            config,
            &config.runtime_api,
            service_clients,
            diagnostics,
            worker_stops.subscribe(RuntimeWorkerPhase::Server),
        )
        .await
        .context("start runtime API worker")?;
        readiness_checks.push(ReadinessCheck::ok(
            "runtime_api",
            format!(
                "TLS runtime API listening on {local_addr}; public key pin {}",
                tls_public_key_pin
            ),
        ));
        workers.runtime_api_tls_public_key_pin = Some(tls_public_key_pin);
        worker_registry.register("runtime-api", RuntimeWorkerPhase::Server, worker);
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "runtime_api",
            "RUNTIME_API_ENABLED=false",
        ));
    }

    if config.memory.enabled && config.bot.key.is_none() {
        let memory_write_embedder = Some(
            embedder::RoutedDiscoveryEmbedder::new(
                attempt_walker.clone(),
                embedder::DiscoveryEmbedderConfig {
                    base_url: config.llm.discovery.base_url.clone(),
                    service_name: config.memory.embedder_service_name.clone(),
                    endpoint_name: config.memory.embedder_endpoint_name.clone(),
                    request_timeout: memory_runtime::EMBEDDER_DEFAULT_TIMEOUT,
                    task_timeout: memory_runtime::EMBEDDER_DEFAULT_TIMEOUT,
                    poll_interval: Duration::from_millis(100),
                    capacity_wait: Duration::from_secs(2),
                },
            )
            .with_lane_priority(openplotva_llm::router::LanePriority::Background),
        );
        let memory_extractor =
            memory_runtime::RoutedMemoryExtractor::new(attempt_walker.clone(), config);
        let memory_worker_store = memory_store.clone();
        let memory_worker_config =
            memory_runtime::memory_service_worker_config_from_memory_config(&config.memory);
        let memory_worker_stop = worker_stops.subscribe(RuntimeWorkerPhase::Processor);
        let memory_worker_trigger = Arc::clone(&memory_restart_trigger);
        let memory_worker = tokio::spawn(async move {
            let report = memory_runtime::run_memory_service_worker_with_trigger_until(
                &memory_extractor,
                &memory_worker_store,
                memory_write_embedder.as_ref(),
                memory_worker_config,
                memory_worker_trigger,
                wait_for_runtime_stop(memory_worker_stop),
            )
            .await;

            tracing::info!(?report, "memory service worker stopped");
        });
        readiness_checks.push(ReadinessCheck::ok(
            "memory_service",
            "Memory service worker started with daily-run ensure, queue drain, routed extraction, routed embeddings, and SQLx persistence",
        ));
        workers.memory_restart_trigger = Some(Arc::clone(&memory_restart_trigger));
        worker_registry.register(
            "memory-service",
            RuntimeWorkerPhase::Processor,
            memory_worker,
        );
    } else if !config.memory.enabled {
        readiness_checks.push(ReadinessCheck::skipped(
            "memory_service",
            "MEMORY_ENABLED=false",
        ));
    }

    if config.memory.enabled {
        let (group, checks) =
            memory_runtime::build_memory_retention_worker_group(&memory_store, &worker_stops);
        readiness_checks.extend(checks);
        worker_registry.install(group);
    }

    let dispatcher_queue = Arc::new(openplotva_telegram::DispatcherQueue::new(
        dispatcher_config(),
    ));
    if config.maintenance.enabled {
        let (server, capture) = maintenance_api::start(
            &config.maintenance,
            &config.runtime_api,
            service_clients.postgres.clone(),
            Arc::clone(&dispatcher_queue),
            worker_stops.subscribe(RuntimeWorkerPhase::Server),
            worker_stops.subscribe(RuntimeWorkerPhase::Processor),
        )
        .await
        .context("start incident maintenance API and capture worker")?;
        worker_registry.register("maintenance-api", RuntimeWorkerPhase::Server, server);
        worker_registry.register(
            "maintenance-capture",
            RuntimeWorkerPhase::Processor,
            capture,
        );
        readiness_checks.push(ReadinessCheck::ok(
            "maintenance",
            "sanitized incident capture and dedicated authenticated API enabled",
        ));
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "maintenance",
            "MAINTENANCE_ENABLED=false",
        ));
    }

    let Some(bot_key) = config.bot.key.as_deref() else {
        readiness_checks.push(ReadinessCheck::skipped("pending_ops", "BOT_KEY is not set"));
        readiness_checks.push(ReadinessCheck::skipped(
            "outbound_dispatcher_restore",
            "BOT_KEY is not set",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "outbound_dispatcher",
            "BOT_KEY is not set",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_update_producer",
            "BOT_KEY is not set",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_update_consumer",
            "BOT_KEY is not set",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_outbox",
            "BOT_KEY is not set",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_commands",
            "BOT_KEY is not set",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_get_me",
            "BOT_KEY is not set",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "control_jobs",
            "BOT_KEY is not set",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "shared_task_queue_restore",
            "BOT_KEY is not set",
        ));
        readiness_checks.push(ReadinessCheck::skipped(
            "shared_task_queue_snapshot",
            "BOT_KEY is not set",
        ));
        readiness_checks.push(ReadinessCheck::skipped("dialog_jobs", "BOT_KEY is not set"));
        readiness_checks.push(ReadinessCheck::skipped("image_jobs", "BOT_KEY is not set"));
        readiness_checks.push(ReadinessCheck::skipped("music_jobs", "BOT_KEY is not set"));
        return Ok(workers);
    };

    let telegram = openplotva_telegram::telegram_client_with_base_url(
        bot_key.to_owned(),
        &config.bot.api_base_url,
    )
    .context("create Telegram Bot API client")?;
    let rich_api = openplotva_telegram::RichApiClient::with_base_url(
        bot_key.to_owned(),
        &config.bot.api_base_url,
    )
    .context("create rich-message API client")?;
    let media_uploader = openplotva_media::uploader::UploaderClient::new(
        openplotva_media::uploader::UploaderConfig {
            base_url: config.uploader.base_url.clone(),
            secret: config.uploader.secret.clone(),
            timeout: std::time::Duration::from_secs(config.uploader.timeout_seconds.max(0) as u64),
        },
    )
    .context("create media uploader client")?;
    let bot_identity = telegram
        .execute(openplotva_telegram::build_get_bot_method())
        .await
        .context("get Telegram bot identity")?;
    readiness_checks.push(ReadinessCheck::ok(
        "telegram_get_me",
        format!(
            "loaded bot identity @{} ({})",
            bot_identity.username, bot_identity.id
        ),
    ));
    workers.bot_username = Some(bot_identity.username.clone());
    workers.bot_id = Some(bot_identity.id);
    workers.telegram = Some(telegram.clone());
    workers.delete_webhook_on_shutdown = config.bot.webhook.enabled;
    let update_stream = {
        let schema_ready =
            openplotva_storage::telegram_delivery::delivery_schema_ready(&service_clients.postgres)
                .await
                .context("check Telegram delivery durability schema")?;
        if !schema_ready {
            anyhow::bail!(
                "UPDATE_QUEUE_BACKEND=stream requires Telegram inbox, quarantine, attempts, and outbox migrations"
            );
        }
        readiness_checks.push(ReadinessCheck::ok(
            "telegram_update_schema",
            "durable Telegram update inbox schema is available",
        ));
        let reconcile_report = update_reconciler::reconcile_update_backlog_once(
            &openplotva_storage::PostgresTelegramDeliveryStore::new(
                service_clients.postgres.clone(),
            ),
            bot_identity.id,
            &bot_user_from_get_me(&bot_identity),
        )
        .await
        .context("reconcile Telegram update inbox backlog")?;
        tracing::info!(
            ?reconcile_report,
            "Telegram update startup backlog reconciliation finished"
        );
        readiness_checks.push(ReadinessCheck::ok(
            "telegram_update_startup_reconcile",
            format!(
                "startup bulk reconciliation {} ({} scanned, {} ignored, {} decode failures, {} attempts repaired, {} lanes)",
                if reconcile_report.storage.already_completed {
                    "already completed"
                } else {
                    "completed"
                },
                reconcile_report.scanned_rows,
                reconcile_report.storage.ignored_rows,
                reconcile_report.decode_failures,
                reconcile_report.storage.repaired_attempt_rows,
                reconcile_report.storage.lane_rows,
            ),
        ));
        let stream = openplotva_updates::RedisUpdateStream::with_append_connection_count(
            update_stream_redis_client.clone(),
            bot_identity.id,
            config.update_queue.stream_append_connections,
        );
        let created = stream
            .ensure_consumer_group()
            .await
            .context("ensure Telegram update materializer Redis consumer group")?;
        readiness_checks.push(ReadinessCheck::ok(
            "telegram_update_stream",
            if created {
                "durable Telegram ingress Stream and consumer group created"
            } else {
                "durable Telegram ingress Stream and consumer group are available"
            },
        ));
        let import_report = update_importer::import_legacy_update_list(
            service_clients.redis.client().clone(),
            &stream,
            bot_identity.id,
        )
        .await
        .context("import legacy Redis List updates into the durable Stream")?;
        readiness_checks.push(ReadinessCheck::ok(
            "telegram_update_legacy_import",
            format!(
                "legacy Redis List drained through crash-safe staging ({} appended, {} quarantinable)",
                import_report.appended, import_report.malformed
            ),
        ));
        stream
    };
    let command_report = configure_telegram_bot_commands(&telegram)
        .await
        .context("configure Telegram bot commands")?;
    readiness_checks.push(ReadinessCheck::ok(
        "telegram_commands",
        format!(
            "deleted existing commands and set {} scoped command lists",
            command_report.set_scopes.len()
        ),
    ));
    let store = PostgresVirtualMessageStore::new(service_clients.postgres.clone());
    let history_store = PostgresHistoryStore::new(service_clients.postgres.clone())
        .with_redis_client(service_clients.redis.client().clone());
    {
        let cleanup_store = virtual_dialog_store.clone();
        let cleanup_history = history_store.clone();
        let cleanup_taskman = taskman_inspector.clone();
        let cleanup_llm_traces = llm_trace_buffer.clone();
        let cleanup_llm_runs = llm_run_buffer.clone();
        let cleanup_stop = worker_stops.subscribe(RuntimeWorkerPhase::Processor);
        let cleanup_interval = runtime_virtual_dialog::RUNTIME_VIRTUAL_DIALOG_CLEANUP_INTERVAL;
        let cleanup_worker = tokio::spawn(async move {
            let deleted = runtime_virtual_dialog::run_runtime_virtual_dialog_cleanup_worker_until(
                cleanup_store,
                cleanup_history,
                cleanup_taskman,
                cleanup_llm_traces,
                Some(cleanup_llm_runs),
                cleanup_interval,
                cleanup_stop,
            )
            .await;

            tracing::info!(deleted, "runtime virtual dialog cleanup worker stopped");
        });
        readiness_checks.push(ReadinessCheck::ok(
            "runtime_virtual_dialog_cleanup",
            format!(
                "Runtime virtual dialogs expire after 24h inactivity; cleanup every {}s",
                cleanup_interval.as_secs()
            ),
        ));
        worker_registry.register(
            "runtime-virtual-dialog-cleanup",
            RuntimeWorkerPhase::Processor,
            cleanup_worker,
        );
    }
    let ephemeral_store = service_clients.redis.ephemeral_message_store();

    let dispatcher_persistence = openplotva_telegram::RedisDispatcherQueueStore::new(
        service_clients.redis.client().clone(),
        openplotva_telegram::DEFAULT_DISPATCHER_QUEUE_KEY,
        DISPATCHER_MAX_QUEUE_SIZE,
    );
    let routing_admin_report_store =
        openplotva_storage::llm_routing::PostgresRoutingAdminReportStore::new(
            service_clients.postgres.clone(),
        );
    if config.admins.admin_ids.is_empty() {
        readiness_checks.push(ReadinessCheck::skipped(
            "llm_admin_reports",
            "ADMINS_ADMIN_IDS is empty",
        ));
    } else {
        let report_store = routing_admin_report_store.clone();
        let report_queue = Arc::clone(&dispatcher_queue);
        let report_admin_ids = Arc::<[i64]>::from(config.admins.admin_ids.clone());
        let report_trigger = Arc::clone(&routing_admin_report_trigger);
        let report_stop = worker_stops.subscribe(RuntimeWorkerPhase::Processor);
        worker_registry.register(
            "routing-admin-reports",
            RuntimeWorkerPhase::Processor,
            tokio::spawn(async move {
                routing_admin_reports::run_routing_admin_report_worker_until(
                    report_store,
                    report_queue,
                    report_admin_ids,
                    report_trigger,
                    report_stop,
                )
                .await;
                tracing::info!("LLM admin report worker stopped");
            }),
        );
        readiness_checks.push(ReadinessCheck::ok(
            "llm_admin_reports",
            "rolling 60-minute digest; new messages hourly, intermediate updates edited",
        ));
    }
    dispatcher_inspector.set_queue(Arc::clone(&dispatcher_queue));
    let restore_report = dispatcher_persistence
        .load_into_queue(&dispatcher_queue)
        .await
        .context("load persisted Telegram dispatcher queue")?;
    readiness_checks.push(ReadinessCheck::ok(
        "outbound_dispatcher_restore",
        format!(
            "loaded {}, restored {}, deduped {}, skipped {} persisted Telegram messages",
            restore_report.loaded,
            restore_report.restored,
            restore_report.deduped,
            restore_report.skipped
        ),
    ));

    let dispatcher_limiters = Arc::new(openplotva_telegram::ChatLimiters::new(
        openplotva_telegram::DEFAULT_DISPATCH_INTERVAL,
    ));
    let rate_limit_policy = Arc::new(rate_limits::ChatRateLimitPolicy::new(
        service_clients.redis.rate_limit_store(),
    ));
    let permission_policy = Arc::new(permissions::ChatPermissionPolicy::new(
        PostgresChatSettingsStore::new(service_clients.postgres.clone()),
    ));
    let telegram_activity_effects =
        Arc::new(telegram_activity::TelegramActivityRuntimeEffects::new(
            telegram.clone(),
            Arc::clone(&permission_policy),
        ));
    let telegram_activity_pulse = Arc::new(telegram_activity::TelegramActivityPulse::new(
        (&config.bot.activity_pulse).into(),
        telegram_activity_effects,
    ));
    cache_inspector.set_policy_caches(
        Arc::clone(&rate_limit_policy),
        Arc::clone(&permission_policy),
    );
    let chat_settings_store = PostgresChatSettingsStore::new(service_clients.postgres.clone());
    let chat_member_store = PostgresChatMemberStore::new(service_clients.postgres.clone());
    let stale_inactive_member_cleanup_store = chat_member_store.clone();
    let stale_inactive_member_cleanup_stop = worker_stops.subscribe(RuntimeWorkerPhase::Ancillary);
    let stale_inactive_member_cleanup_worker = tokio::spawn(async move {
        let report = members::run_stale_inactive_member_cleanup_worker_until(
            &stale_inactive_member_cleanup_store,
            members::STALE_INACTIVE_MEMBER_CLEANUP_INTERVAL,
            members::STALE_INACTIVE_MEMBER_MAX_AGE,
            wait_for_runtime_stop(stale_inactive_member_cleanup_stop),
        )
        .await;

        tracing::info!(?report, "stale inactive chat-member cleanup worker stopped");
    });
    readiness_checks.push(ReadinessCheck::ok(
        "stale_inactive_chat_members",
        format!(
            "cleanup every {}s, max age {}s",
            members::STALE_INACTIVE_MEMBER_CLEANUP_INTERVAL.as_secs(),
            members::STALE_INACTIVE_MEMBER_MAX_AGE.as_secs()
        ),
    ));
    worker_registry.register(
        "stale-inactive-member-cleanup",
        RuntimeWorkerPhase::Ancillary,
        stale_inactive_member_cleanup_worker,
    );

    let payment_store = payments::PostgresSuccessfulPaymentStore::new(
        store.clone(),
        PostgresPaymentStore::new(service_clients.postgres.clone()),
        PostgresVipStore::new(service_clients.postgres.clone()),
    );
    let payment_successful_effects = payments::SuccessfulPaymentDispatcherEffects::new(
        Arc::clone(&dispatcher_queue),
        payments::NoopVipCacheInvalidator,
    )
    .with_support_url(config.payments.support_url.clone());
    let payment_effects =
        payments::PaymentRuntimeEffects::new(telegram.clone(), payment_successful_effects)
            .with_subscription_test_price_users(config.admins.admin_ids.clone());
    let subscription_sync_config = subscription_sync_config_from_app_config(config);
    if subscription_sync_config.enabled {
        let subscription_sync_source = subscription_sync::TelegramStarSubscriptionSyncSource::new(
            openplotva_telegram::StarTransactionsClient::new(
                bot_key.to_owned(),
                &config.bot.api_base_url,
            ),
        );
        let subscription_sync_store = subscription_sync::PostgresSubscriptionSyncStore::new(
            payments::PostgresSuccessfulPaymentStore::new(
                store.clone(),
                PostgresPaymentStore::new(service_clients.postgres.clone()),
                PostgresVipStore::new(service_clients.postgres.clone()),
            ),
            PostgresVipStore::new(service_clients.postgres.clone()),
        );
        let subscription_sync_stop = worker_stops.subscribe(RuntimeWorkerPhase::Ancillary);
        let subscription_sync_worker = tokio::spawn(async move {
            let report = subscription_sync::run_subscription_sync_worker_until(
                &subscription_sync_source,
                &subscription_sync_store,
                subscription_sync_config,
                wait_for_runtime_stop(subscription_sync_stop),
            )
            .await;
            tracing::info!(?report, "subscription sync worker stopped");
        });
        readiness_checks.push(ReadinessCheck::ok(
            "subscription_sync",
            format!(
                "enabled every {}s, page_limit {}, dry_run {}",
                subscription_sync_config.interval.as_secs(),
                subscription_sync_config.page_limit,
                subscription_sync_config.dry_run
            ),
        ));
        worker_registry.register(
            "subscription-sync",
            RuntimeWorkerPhase::Ancillary,
            subscription_sync_worker,
        );
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "subscription_sync",
            "SUBSCRIPTION_SYNC_ENABLED=false",
        ));
    }
    let taskman_ids = openplotva_taskman::TaskQueueIdAllocator::new();
    let translation_cache = service_clients.redis.translation_cache_store();
    let translator =
        llm_translate::runtime_llm_translator(config, translation_cache, attempt_walker.clone());
    let translate_effects =
        translate::TranslateDispatcherEffects::new(Arc::clone(&dispatcher_queue));
    let admin_cache_store = service_clients.redis.chat_admin_cache_store();
    let group_settings_effects = settings::GroupSettingsRuntimeEffects::new(
        chat_member_store.clone(),
        telegram.clone(),
        chat_member_store.clone(),
        telegram.clone(),
        admin_cache_store.clone(),
    );
    let join_greeting_sender = settings::TelegramJoinGreetingSender::new(
        ephemeral_store.clone(),
        Arc::clone(&permission_policy),
        telegram.clone(),
        rich_api.clone(),
    );
    let join_greeting_runtime = settings::NewMembersJoinGreetingRuntime::new(
        service_clients.redis.join_greeting_store(),
        chat_settings_store.clone(),
        chat_member_store.clone(),
        join_greeting_sender,
    );
    let new_members_effects = settings::NewMembersFollowupRuntimeEffects::new(
        members::ChatSettingsCommunicationEffects::new(chat_settings_store.clone()),
        service_clients.redis.blocked_chat_store(),
        join_greeting_runtime.clone(),
    );
    let member_effects = members::MemberStateRuntimeEffects::new(
        chat_member_store.clone(),
        telegram.clone(),
        admin_cache_store,
    );
    let checkin_game_store = checkin::PostgresCheckinGameStore::new(
        chat_settings_store.clone(),
        chat_member_store.clone(),
    );
    let vip_for_ads: Arc<dyn gradius_ads::GradiusVipChecker> =
        Arc::new(payments::VipStatusWithExternalMembership::new(
            payment_store.clone(),
            payments::TelegramExternalVipMembershipChecker::new(telegram.clone()),
            config.vip.chat_id,
        ));
    let mut initialize_placement = |requested: bool, api_key: &str, check: &str| {
        if !requested {
            return None;
        }
        if !config.gradius.enabled || api_key.trim().is_empty() {
            readiness_checks.push(ReadinessCheck::skipped(
                check,
                "Gradius master switch or placement API key is missing",
            ));
            return None;
        }
        match gradius_ads::initialize_placement_service(
            config,
            api_key,
            service_clients.postgres.clone(),
            Arc::clone(&vip_for_ads),
        ) {
            Ok(ads) => {
                let outbox = Arc::new(gradius_utility_outbox::GradiusUtilityOutbox::new(
                    openplotva_storage::PostgresTelegramOutboxStore::new(
                        service_clients.postgres.clone(),
                    ),
                    bot_identity.id,
                ));
                readiness_checks.push(ReadinessCheck::ok(
                    check,
                    "Gradius placement initialized with fail-closed privacy redaction",
                ));
                Some((Arc::new(ads), outbox))
            }
            Err(error) => {
                tracing::warn!(%error, "Gradius placement privacy redactor unavailable");
                readiness_checks.push(ReadinessCheck::skipped(
                    check,
                    "Gradius placement privacy redactor unavailable",
                ));
                None
            }
        }
    };
    let gradius_utility = initialize_placement(
        config.gradius.utility_rates_enabled || config.gradius.utility_checkin_enabled,
        &config.gradius.api_key,
        "gradius_utility",
    );
    let gradius_generation = initialize_placement(
        config.gradius.utility_image_enabled,
        &config.gradius.generation_api_key,
        "gradius_generation",
    );
    let telegram_outbox_store = openplotva_storage::PostgresTelegramOutboxStore::new(
        service_clients.critical_postgres.clone(),
    );
    let rich_outbox = Arc::new(rich::RichOutbox::new(
        telegram_outbox_store.clone(),
        bot_identity.id,
    ));
    let mut checkin_effects = checkin::CheckinGameRuntimeEffects::new(
        checkin_game_store.clone(),
        checkin::TelegramCheckinGameSender::new(
            ephemeral_store.clone(),
            telegram.clone(),
            rich_api.clone(),
            rich_outbox.clone(),
        ),
        Arc::clone(&permission_policy),
        bot_identity.id,
    );
    if config.gradius.utility_checkin_enabled
        && let Some((ads, outbox)) = &gradius_utility
    {
        checkin_effects = checkin_effects.with_utility_ads(Arc::clone(ads), Arc::clone(outbox));
    }
    let bot_username = bot_identity.username.clone();
    let telegram_effects = Arc::new(telegram.clone());
    let payment_rich_effects = Arc::new(payments::RichPaymentEffects::new(
        telegram.clone(),
        Arc::clone(&rich_outbox),
    ));
    let payment_runtime_effects = Arc::new(payment_effects.clone());
    let shared_task_queue_recovery_interval =
        task_queue::shared_task_queue_recovery_interval_from_config(&config.persistent_queue);
    let shared_task_queue_cleanup_interval =
        task_queue::shared_task_queue_cleanup_interval_from_config(&config.persistent_queue);
    let shared_task_queue_completed_retention =
        task_queue::shared_task_queue_completed_retention_from_config(&config.persistent_queue);
    let shared_task_queue_heartbeat_interval =
        task_queue::shared_task_queue_heartbeat_interval_from_config(&config.persistent_queue);
    let shared_task_queue_placeholder_cleanup_interval =
        task_queue::shared_task_queue_placeholder_cleanup_interval_from_config(
            &config.persistent_queue,
        );
    let shared_task_queue_placeholder_max_age =
        task_queue::shared_task_queue_placeholder_max_age_from_config(&config.persistent_queue);
    let shared_task_queue_store =
        openplotva_storage::PostgresTaskQueueStore::new(service_clients.postgres.clone());
    // A load failure is fatal: starting empty would hide every durable in-flight job
    // and let freshly issued ids overwrite still-pending rows. Fail startup so the
    // process is restarted and retries the load instead of silently dropping work.
    let (shared_task_queue, shared_task_queue_restore_report) =
        task_queue::SharedTaskQueueRuntime::load_from_postgres_with_id_allocator(
            shared_task_queue_store,
            taskman_ids.clone(),
        )
        .await
        .context("load shared task queue from Postgres taskman v2")?;
    let shared_task_queue_readiness = format!(
        "restored {} shared taskman jobs and requeued {} processing jobs from Postgres taskman v2",
        shared_task_queue_restore_report.restored, shared_task_queue_restore_report.requeued
    );
    let task_queue_for_updates = shared_task_queue.queue();
    let task_enqueue_rate_limit = Arc::new(rate_limits::TaskEnqueueRateLimitPolicy::new(
        service_clients.redis.task_enqueue_rate_limit_store(),
    ));
    let control_queue = control_jobs::PersistentControlJobQueue::from_task_queue(
        task_queue_for_updates.as_ref().clone(),
    )
    .with_task_enqueue_rate_limit(task_enqueue_rate_limit.clone())
    .with_db_journal(
        shared_task_queue
            .db_journal()
            .expect("Postgres taskman runtime should have a DB journal"),
    );
    let control_queue_readiness =
        "unified control-job worker uses the shared Postgres-backed taskman queue".to_owned();
    let control_queue_for_updates = Arc::new(control_queue.clone());
    readiness_checks.push(ReadinessCheck::ok(
        "shared_task_queue_restore",
        shared_task_queue_readiness,
    ));
    let (shared_task_queue_workers, shared_task_queue_checks) =
        build_shared_task_queue_maintenance_group(
            &shared_task_queue,
            openplotva_storage::PostgresTaskQueueStore::new(service_clients.postgres.clone()),
            telegram.clone(),
            SharedTaskQueueMaintenanceConfig {
                recovery_interval: shared_task_queue_recovery_interval,
                cleanup_interval: shared_task_queue_cleanup_interval,
                completed_retention: shared_task_queue_completed_retention,
                placeholder_cleanup_interval: shared_task_queue_placeholder_cleanup_interval,
                placeholder_max_age: shared_task_queue_placeholder_max_age,
            },
            worker_stops.as_ref(),
        );
    worker_registry.install(shared_task_queue_workers);
    readiness_checks.extend(shared_task_queue_checks);
    workers.shared_task_queue = Some(shared_task_queue.clone());
    let telegram_outbox_transport =
        telegram_outbox::TelegramApiOutboxTransport::new(telegram.clone(), rich_api.clone());
    // One shared reaction signaler feeds the tool adapter, image/music
    // workers, and the obligations watcher.
    let generation_reactions: reactions::GenerationReactions =
        Arc::new(reactions::GenerationReactionSignaler::new(telegram.clone()));
    let telegram_outbox_jobs = telegram_outbox::RunAwareTelegramOutboxJobResolver::new(
        shared_task_queue.clone(),
        llm_run_buffer.clone(),
        openplotva_storage::gradius_ads::PostgresGradiusAdStore::new(
            service_clients.postgres.clone(),
        ),
        telegram_outbox_store.clone(),
        Some(Arc::clone(&generation_reactions)),
    );
    let telegram_outbox_worker_id =
        format!("telegram-outbox-{}-{}", bot_identity.id, std::process::id());
    let telegram_outbox_metrics = telegram_outbox::TelegramOutboxWorkerMetrics::default();
    let readiness_telegram_outbox_metrics = telegram_outbox_metrics.clone();
    workers.readiness_probes.push(Arc::new(move || {
        telegram_outbox_readiness(&readiness_telegram_outbox_metrics)
    }));
    let telegram_outbox_bot_id = bot_identity.id;
    let telegram_outbox_history = history_store.clone();
    let telegram_outbox_stop = worker_stops.subscribe(RuntimeWorkerPhase::Outbound);
    let telegram_outbox_worker = tokio::spawn(async move {
        let report = telegram_outbox::run_telegram_outbox_worker_until(
            telegram_outbox_store,
            telegram_outbox_history,
            telegram_outbox_transport,
            telegram_outbox_jobs,
            &telegram_outbox_worker_id,
            telegram_outbox_bot_id,
            telegram_outbox::TelegramOutboxWorkerConfig::default(),
            telegram_outbox_metrics,
            wait_for_runtime_stop(telegram_outbox_stop),
        )
        .await;
        tracing::info!(?report, "durable Telegram outbox worker stopped");
    });
    worker_registry.register(
        "telegram-outbox",
        RuntimeWorkerPhase::Outbound,
        telegram_outbox_worker,
    );
    readiness_checks.push(ReadinessCheck::ok(
        "telegram_outbox",
        "durable Telegram outbox worker started with fenced leases and real receipt tracking",
    ));
    let mut shared_taskman_worker_counts = BTreeMap::new();
    shared_taskman_worker_counts.insert(openplotva_taskman::CONTROL_QUEUE_NAME.to_owned(), 1);
    let mut memory_worker_scale = None;
    if config.memory.enabled {
        let memory_write_embedder = Some(
            embedder::RoutedDiscoveryEmbedder::new(
                attempt_walker.clone(),
                embedder::DiscoveryEmbedderConfig {
                    base_url: config.llm.discovery.base_url.clone(),
                    service_name: config.memory.embedder_service_name.clone(),
                    endpoint_name: config.memory.embedder_endpoint_name.clone(),
                    request_timeout: memory_runtime::EMBEDDER_DEFAULT_TIMEOUT,
                    task_timeout: memory_runtime::EMBEDDER_DEFAULT_TIMEOUT,
                    poll_interval: Duration::from_millis(100),
                    capacity_wait: Duration::from_secs(2),
                },
            )
            .with_lane_priority(openplotva_llm::router::LanePriority::Background),
        );
        let merger = config.memory.subject_merge_enabled.then(|| {
            (
                memory_runtime::RoutedSubjectMerger::new(attempt_walker.clone(), config),
                memory_runtime::MemorySubjectMergeConfig {
                    interval: std::time::Duration::from_secs(
                        u64::try_from(config.memory.subject_merge_interval_seconds.max(1))
                            .unwrap_or(45),
                    ),
                    cooldown_hours: config.memory.subject_merge_cooldown_hours,
                    min_cards: i64::from(config.memory.subject_merge_min_cards),
                    embedding_dimension: config.memory.embedding_dim,
                    ..memory_runtime::MemorySubjectMergeConfig::default()
                },
            )
        });
        let group = memory_workers::build_taskman_workers(
            memory_workers::MemoryTaskmanWorkerInputs {
                store: memory_store.clone(),
                queue: Arc::clone(&task_queue_for_updates),
                config: memory_runtime::memory_service_worker_config_from_memory_config(
                    &config.memory,
                ),
                trigger: Arc::clone(&memory_restart_trigger),
                extractor: memory_runtime::RoutedMemoryExtractor::new(
                    attempt_walker.clone(),
                    config,
                ),
                embedder: memory_write_embedder,
                merger,
                worker_floor: config.persistent_queue.memory_consolidation_workers,
                workers_per_lane: config.persistent_queue.memory_workers_per_lane,
                worker_cap: config.persistent_queue.memory_workers_cap,
            },
            &worker_stops,
            &router_handle.snapshot(),
        );
        let memory_worker_count = group.count;
        memory_worker_scale = group.scale;
        readiness_checks.extend(group.readiness);
        worker_registry.install(group.ingress);
        worker_registry.install(group.processors);
        workers.memory_restart_trigger = Some(Arc::clone(&memory_restart_trigger));
        shared_taskman_worker_counts.insert(
            openplotva_taskman::MEMORY_CONSOLIDATION_QUEUE_NAME.to_owned(),
            i32::try_from(memory_worker_count).unwrap_or(i32::MAX),
        );
    }
    let payment_store_for_updates = Arc::new(payment_store.clone());
    let vip_status_for_updates = Arc::new(payments::VipStatusWithExternalMembership::new(
        payment_store.clone(),
        payments::TelegramExternalVipMembershipChecker::new(telegram.clone()),
        config.vip.chat_id,
    ));
    let history_store_for_updates = Arc::new(history_store.clone());
    let checkin_game_store_for_updates = Arc::new(checkin_game_store.clone());
    let rate_limits_for_updates = Arc::clone(&rate_limit_policy);
    let permission_policy_for_updates = Arc::clone(&permission_policy);
    let dispatcher_queue_for_updates = Arc::clone(&dispatcher_queue);
    let control_dispatcher_queue = Arc::clone(&dispatcher_queue);
    let dialog_translator = Arc::new(translator.clone());
    let control_stop = worker_stops.subscribe(RuntimeWorkerPhase::Processor);
    let control_worker = tokio::spawn(async move {
        let control_next_sequence = Arc::new(std::sync::atomic::AtomicU64::new(1));
        let control_next_virtual_id = {
            let control_next_sequence = Arc::clone(&control_next_sequence);
            move || {
                let id = control_next_sequence.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                format!("control-vmsg-{id}")
            }
        };
        let registry = control_jobs::AppControlJobExecutors {
            payment_store: &payment_store,
            payment_effects: &payment_effects,
            dispatcher_queue: &control_dispatcher_queue,
            group_settings_effects: &group_settings_effects,
            new_members_effects: &new_members_effects,
            bot_username: &bot_username,
            next_virtual_id: &control_next_virtual_id,
            translator: &translator,
            translation_effects: &translate_effects,
            member_effects: &member_effects,
            checkin_effects: &checkin_effects,
        };
        let report = control_jobs::run_control_job_worker_until(
            &control_queue,
            &registry,
            wait_for_runtime_stop(control_stop),
        )
        .await;

        tracing::info!(?report, "unified control-job worker stopped");
    });
    readiness_checks.push(ReadinessCheck::ok("control_jobs", control_queue_readiness));
    worker_registry.register(
        "control-job-worker",
        RuntimeWorkerPhase::Processor,
        control_worker,
    );

    let music_service_available = config.music.acestep.enabled;
    let media_failure_explainer = Arc::new(std::sync::OnceLock::new());
    let delivery_obligation_store = Arc::new(
        openplotva_storage::PostgresDeliveryObligationStore::new(service_clients.postgres.clone()),
    );
    let dialog_tool_adapter =
        dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&task_queue_for_updates))
            .with_durability_barrier(Arc::new(shared_task_queue.clone()))
            .with_draw_image_vip_status(vip_status_for_updates.clone())
            .with_draw_image_rate_limit(Arc::new(dialog_tools::DrawImageRateLimitPolicy::new(
                service_clients.redis.draw_rate_limit_store(),
            )))
            .with_task_enqueue_rate_limit(task_enqueue_rate_limit.clone())
            .with_draw_image_permission(permission_policy.clone())
            .with_song_service_available(music_service_available)
            .with_song_audio_permission(permission_policy.clone())
            .with_delivery_obligations(
                Arc::clone(&delivery_obligation_store)
                    as Arc<dyn dialog_turn::DeliveryObligationRecorder>,
                config.llm.dialog.image_delivery_timeout_secs,
                config.llm.dialog.music_delivery_timeout_secs,
            )
            .with_image_edit_file_resolver(Arc::new(
                dialog_tools::TelegramImageEditFileResolver::new(
                    PostgresTelegramFileStore::new(service_clients.postgres.clone()),
                    dialog_tools::TelegramBotFileProbe::new(telegram.clone()),
                )
                .with_pending_albums(
                    bot_identity.id,
                    openplotva_storage::PostgresTelegramDeliveryStore::new(
                        service_clients.postgres.clone(),
                    ),
                ),
            ));
    let dialog_tool_adapter = Arc::new(
        dialog_tool_adapter
            .with_generation_reactions(Arc::clone(&generation_reactions))
            .with_draw_queue_notice_sender(Arc::new(
                dialog_messages::DrawQueueNoticeDispatcherSender::new(Arc::clone(
                    &dispatcher_queue,
                )),
            )),
    );
    {
        let watcher_store: Arc<dyn dialog_turn::DeliveryObligationStore> =
            Arc::clone(&delivery_obligation_store) as _;
        let watcher_tickets: Arc<dyn dialog_turn::TicketRecordSource> =
            Arc::new(dialog_turn::FallbackTicketRecordSource::new(
                Arc::clone(&task_queue_for_updates),
                Some(openplotva_storage::PostgresTaskQueueStore::new(
                    service_clients.postgres.clone(),
                )),
            ));
        let watcher_signal = dialog_turn::DispatcherTerminalUserSignal::new(
            telegram.clone(),
            Arc::clone(&dispatcher_queue),
        );
        let mut obligation_notifier = dialog_turn::DispatcherDeliveryObligationNotifier::new(
            Arc::clone(&dispatcher_queue),
            watcher_signal.clone(),
        );
        obligation_notifier.explainer = Arc::clone(&media_failure_explainer);
        {
            let reactions = &generation_reactions;
            obligation_notifier =
                obligation_notifier.with_lifecycle_reactions(Arc::clone(reactions));
        }
        let watcher_notifier: Arc<dyn dialog_turn::DeliveryObligationNotifier> =
            Arc::new(obligation_notifier);
        let watcher_dispatch_failures = dialog_turn::DispatchFailureSignalScan {
            ring: Arc::clone(&dispatch_failure_ring),
            signal: Arc::new(watcher_signal),
        };
        let watcher_timeouts = dialog_turn::DeliveryObligationTimeouts::from_secs(
            config.llm.dialog.image_delivery_timeout_secs,
            config.llm.dialog.music_delivery_timeout_secs,
        );
        let watcher_interval =
            Duration::from_secs(config.llm.dialog.obligation_watch_interval_secs.max(1) as u64);
        let watcher_stop = worker_stops.subscribe(RuntimeWorkerPhase::Processor);
        worker_registry.register(
            "delivery-obligation-watcher",
            RuntimeWorkerPhase::Processor,
            tokio::spawn(async move {
                dialog_turn::run_delivery_obligation_watcher(
                    watcher_store,
                    watcher_tickets,
                    watcher_notifier,
                    watcher_timeouts,
                    watcher_interval,
                    Some(watcher_dispatch_failures),
                    wait_for_runtime_stop(watcher_stop),
                )
                .await;
            }),
        );
        readiness_checks.push(ReadinessCheck::ok(
            "dialog_delivery_obligations",
            format!(
                "Delivery-obligation watcher every {}s (image deadline {}s, music deadline {}s)",
                config.llm.dialog.obligation_watch_interval_secs.max(1),
                config.llm.dialog.image_delivery_timeout_secs,
                config.llm.dialog.music_delivery_timeout_secs,
            ),
        ));
    }
    let vision_data_urls = vision::TelegramClientVisionDataUrlProvider::new(telegram.clone());
    let vision_caption_data_urls = vision_data_urls
        .clone()
        .with_max_side(u32::try_from(config.vision.caption_max_side).unwrap_or_default());
    let dialog_tool_vision = Arc::new(
        vision::TelegramVisionDescriber::new(
            PostgresTelegramFileStore::new(service_clients.postgres.clone()),
            vision::RoutedVisionCaptioner::new(
                attempt_walker.clone(),
                config,
                vision_caption_data_urls.clone(),
            ),
        )
        .with_model_name(config.vision.model.clone())
        .with_history_store(history_store.clone()),
    );
    let dialog_context_vision = Arc::new(vision::TelegramDialogVisionInputMaterializer::new(
        vision::TelegramVisionDescriber::new(
            PostgresTelegramFileStore::new(service_clients.postgres.clone()),
            vision::RoutedVisionCaptioner::new(
                attempt_walker.clone(),
                config,
                vision_caption_data_urls,
            ),
        )
        .with_model_name(config.vision.model.clone())
        .with_history_store(history_store.clone()),
        vision_data_urls.clone(),
        dialog_context::dialog_vision_direct_image_limit(Some(config.vision.direct_image_limit)),
    ));
    let asr_budget = Duration::from_secs(120);
    let dialog_context_asr_runtime = Arc::new(
        asr::TelegramDialogAsrInputMaterializer::new(
            PostgresTelegramFileStore::new(service_clients.postgres.clone()),
            asr::TelegramClientVoiceDownloader::new(telegram.clone()),
            asr::RoutedAsrTranscriber::new(
                routed_attempts::RoutedAttemptWalker::new(
                    Arc::clone(&router_handle),
                    Arc::clone(&router_breakers),
                    Arc::clone(&router_triggers),
                    Arc::clone(&router_pools),
                )
                .with_reporter(routing_event_reporter.clone()),
                asr::DiscoveryAsrConfig {
                    base_url: config.llm.discovery.base_url.clone(),
                    service_name: asr::DEFAULT_ASR_DISCOVERY_SERVICE_NAME.to_owned(),
                    endpoint_name: asr::DEFAULT_ASR_DISCOVERY_ENDPOINT_NAME.to_owned(),
                    request_timeout: asr_budget,
                    task_timeout: asr_budget,
                    poll_interval: Duration::from_millis(100),
                    capacity_wait: Duration::from_secs(2),
                },
            ),
        )
        .with_taskman_queue(Arc::clone(&task_queue_for_updates))
        .with_taskman_durability(Arc::new(shared_task_queue.clone())),
    );
    let dialog_context_asr: Arc<dyn asr::DialogAsrInputMaterializer> =
        dialog_context_asr_runtime.clone();
    let dialog_tool_media: Arc<dyn dialog_tools::VisionDescriber> =
        Arc::new(vision::TelegramMediaDescriber::new(
            dialog_tool_vision,
            Arc::clone(&dialog_context_asr),
            PostgresTelegramFileStore::new(service_clients.postgres.clone()),
        ));
    let asr_task_queue = Arc::clone(&task_queue_for_updates);
    let asr_task_processor = Arc::clone(&dialog_context_asr_runtime);
    let asr_task_stop = worker_stops.subscribe(RuntimeWorkerPhase::Processor);
    let asr_task_worker = tokio::spawn(async move {
        let report = asr::run_asr_worker_every_until(
            asr_task_queue.as_ref(),
            asr_task_processor.as_ref(),
            asr::ASR_TASKMAN_POLL_INTERVAL,
            wait_for_runtime_stop(asr_task_stop),
        )
        .await;
        tracing::info!(?report, "priority ASR taskman worker stopped");
    });
    worker_registry.register(
        "asr-task-worker",
        RuntimeWorkerPhase::Processor,
        asr_task_worker,
    );
    shared_taskman_worker_counts.insert(openplotva_taskman::ASR_GPU1_QUEUE_NAME.to_owned(), 1);
    readiness_checks.push(ReadinessCheck::ok(
        "asr_jobs",
        "Priority ASR taskman worker blocks new draw claims and uses CPU fallback for active draw",
    ));
    let rates_fetcher = Arc::new(
        rates::MarketRatesClient::from_config(&config.market_rates)
            .context("failed to initialize market rates provider")?,
    );
    let rates_tool_dispatcher = Arc::new(rates::RatesDelivery::new(Arc::clone(&rich_outbox)));
    let history_summarizer = Some(Arc::new(
        history_summary::routed_history_summary_service_from_app_config(
            config,
            Arc::new(history_store.clone()),
            attempt_walker.clone(),
        ),
    ) as Arc<dyn dialog_tools::ChatHistorySummarizer>);
    readiness_checks.push(ReadinessCheck::ok(
        "history_summary",
        "Chat history summary dialog tool wired to Postgres history store and routed provider",
    ));
    let youtube_summarizer = match youtube::GeminiYouTubeSummarizer::from_app_config(config) {
        Some(transcript) => {
            readiness_checks.push(ReadinessCheck::ok(
                "youtube_summary",
                "YouTube transcript fetcher and routed provider summary tool wired",
            ));
            Some(Arc::new(youtube::RoutedYouTubeSummarizer::new(
                transcript,
                attempt_walker.clone(),
                config,
            )) as Arc<dyn dialog_tools::YouTubeSummarizer>)
        }
        None => {
            readiness_checks.push(ReadinessCheck::skipped(
                "youtube_summary",
                "YouTube summary dialog tool unavailable: Google AI key is not configured",
            ));
            None
        }
    };
    let serper_client = match serper::SerperClient::from_app_config(config) {
        Ok(Some(client)) => {
            readiness_checks.push(ReadinessCheck::ok(
                "serper",
                "Serper HTTP adapter configured",
            ));
            Some(Arc::new(client))
        }
        Ok(None) => {
            readiness_checks.push(ReadinessCheck::skipped(
                "serper",
                "SERPER_API_KEY is not configured",
            ));
            None
        }
        Err(error) => {
            tracing::warn!(%error, "Serper dialog tools unavailable");
            readiness_checks.push(ReadinessCheck::skipped(
                "serper",
                format!("Serper unavailable: {error}"),
            ));
            None
        }
    };
    let mut direct_draw_api_config = image_jobs::aifarm_draw_api_config_from_app_config(config);
    direct_draw_api_config.timeout = Duration::from_secs(120);
    let direct_draw_api_effects = Arc::new(
        dialog_messages::DirectDrawApiRuntimeEffects::new(
            telegram.clone(),
            image_jobs::AifarmDrawApiImageGenerator::new(direct_draw_api_config),
        )
        .with_send_policies(rate_limit_policy.clone(), permission_policy.clone())
        .with_history_store(Arc::new(history_store.clone()), bot_identity.id),
    );
    let mut app_dialog_toolbox = dialog_tools::AppDialogToolbox::new(
        Some(Arc::clone(&rates_fetcher)),
        Some(rates_tool_dispatcher),
        Some(dialog_translator),
    )
    .with_queue_status_provider(dialog_tool_adapter.clone())
    .with_drawing_canceller(dialog_tool_adapter.clone())
    .with_image_scheduler(dialog_tool_adapter.clone())
    .with_song_scheduler(dialog_tool_adapter.clone())
    .with_image_fallback_notice_sender(Arc::new(
        dialog_messages::DrawQueueNoticeDispatcherSender::new(Arc::clone(&dispatcher_queue)),
    ))
    .with_vision_describer(dialog_tool_media);
    let dialog_history_searcher: Arc<dyn agent_runtime::HistorySearcher> = Arc::new(
        agent_runtime::PostgresHistorySearch::new(history_store.clone()),
    );
    app_dialog_toolbox = app_dialog_toolbox.with_history_searcher(dialog_history_searcher);
    app_dialog_toolbox =
        app_dialog_toolbox.with_agent_tools(Arc::new(agent_tools::AgentContextTools {
            history: history_store.clone(),
            memory: memory_store.clone(),
            vip: vip_status_for_updates.clone(),
            direct_draw: direct_draw_api_effects.clone(),
            admin_ids: Arc::from(config.admins.admin_ids.clone()),
        }));
    if let Some(history_summarizer) = history_summarizer {
        app_dialog_toolbox = app_dialog_toolbox.with_history_summarizer(history_summarizer);
    }
    if let Some(youtube_summarizer) = youtube_summarizer {
        app_dialog_toolbox = app_dialog_toolbox.with_youtube_summarizer(youtube_summarizer);
    }
    if let Some(serper) = serper_client.as_ref() {
        let url_crawler: Arc<dyn dialog_tools::UrlCrawler> = serper.clone();
        app_dialog_toolbox = app_dialog_toolbox.with_url_crawler(url_crawler);
    }
    match config.web_search_provider {
        openplotva_config::SearchProvider::Serper => {
            if let Some(serper) = serper_client {
                app_dialog_toolbox = app_dialog_toolbox.with_web_searcher(serper);
                readiness_checks.push(ReadinessCheck::ok(
                    "web_search",
                    "Serper search provider wired",
                ));
            } else {
                readiness_checks.push(ReadinessCheck::skipped(
                    "web_search",
                    "SERPER_API_KEY is not configured",
                ));
            }
        }
        openplotva_config::SearchProvider::Deepseek => {
            match deepseek::DeepSeekClient::from_config(&config.deepseek) {
                Ok(Some(client)) => {
                    app_dialog_toolbox = app_dialog_toolbox.with_web_searcher(Arc::new(client));
                    readiness_checks.push(ReadinessCheck::ok(
                        "web_search",
                        "DeepSeek search provider wired",
                    ));
                }
                Ok(None) => readiness_checks.push(ReadinessCheck::skipped(
                    "web_search",
                    "DEEPSEEK_API_KEY is not configured",
                )),
                Err(error) => {
                    tracing::warn!(%error, "DeepSeek search provider unavailable");
                    readiness_checks.push(ReadinessCheck::skipped(
                        "web_search",
                        format!("DeepSeek unavailable: {error}"),
                    ));
                }
            }
            if serper_client.is_none() {
                match serper::SerperClient::for_crawling(config.serper.timeout_seconds) {
                    Ok(crawler) => {
                        app_dialog_toolbox = app_dialog_toolbox.with_url_crawler(Arc::new(crawler))
                    }
                    Err(error) => {
                        readiness_checks.push(ReadinessCheck::skipped(
                            "crawl_url",
                            format!("HTTP crawler unavailable: {error}"),
                        ));
                    }
                }
            }
        }
    }
    if config.sandbox.url.is_empty() {
        readiness_checks.push(ReadinessCheck::skipped(
            "python_sandbox",
            "SANDBOX_URL is empty; run_python is not offered",
        ));
    } else {
        match openplotva_sandbox::client::SandboxClient::new(
            &config.sandbox.url,
            Duration::from_secs(u64::try_from(config.sandbox.timeout_seconds.max(1)).unwrap_or(75)),
        ) {
            Ok(client) => {
                let io = Arc::new(
                    python_sandbox::TelegramSandboxIo::new(
                        telegram.clone(),
                        PostgresTelegramFileStore::new(service_clients.postgres.clone()),
                    )
                    .with_history_store(Arc::new(history_store.clone()), bot_identity.id),
                );
                app_dialog_toolbox = app_dialog_toolbox.with_python_sandbox(Arc::new(
                    python_sandbox::PythonSandboxTool::new(Arc::new(client), io.clone(), io),
                ));
                readiness_checks.push(ReadinessCheck::ok(
                    "python_sandbox",
                    "run_python uses the sandbox supervisor",
                ));
            }
            Err(error) => {
                tracing::warn!(%error, "Python sandbox client unavailable");
                readiness_checks.push(ReadinessCheck::skipped(
                    "python_sandbox",
                    format!("Python sandbox client unavailable: {error}"),
                ));
            }
        }
    }
    let dialog_toolbox: Arc<dyn openplotva_dialog::DialogToolbox> = Arc::new(app_dialog_toolbox);
    let gradius_for_dialog = if !config.gradius.enabled {
        readiness_checks.push(ReadinessCheck::skipped(
            "gradius",
            "Gradius dialogue advertising is disabled",
        ));
        None
    } else if config.gradius.api_key.trim().is_empty() {
        readiness_checks.push(ReadinessCheck::skipped(
            "gradius",
            "Gradius is enabled but GRADIUS_API_KEY is empty",
        ));
        None
    } else {
        match openplotva_llm::gradius::GradiusPrivacyRedactor::new(
            gradius_ads::gradius_privacy_config(config),
        ) {
            Ok(redactor) => {
                let dialogue: Arc<dyn gradius_ads::GradiusDialogueClient> =
                    Arc::new(openplotva_llm::gradius::GradiusClient::new(
                        openplotva_llm::gradius::GradiusClientConfig {
                            enabled: true,
                            api_key: config.gradius.api_key.clone(),
                            base_url: config.gradius.base_url.clone(),
                            request_timeout: Duration::from_secs(
                                u64::try_from(config.gradius.request_timeout_seconds.max(1))
                                    .unwrap_or(5),
                            ),
                        },
                    ));
                let privacy: Arc<dyn gradius_ads::GradiusTextRedactor> = Arc::new(redactor);
                let ledger: Arc<dyn gradius_ads::GradiusAdLedger> = Arc::new(
                    openplotva_storage::gradius_ads::PostgresGradiusAdStore::new(
                        service_clients.postgres.clone(),
                    ),
                );
                let vip: Arc<dyn gradius_ads::GradiusVipChecker> = vip_status_for_updates.clone();
                readiness_checks.push(ReadinessCheck::ok(
                    "gradius",
                    "Gradius dialogue advertising is enabled with fail-closed privacy redaction",
                ));
                Some(Arc::new(
                    gradius_ads::GradiusAdService::new(dialogue, privacy, ledger, vip)
                        .with_banner_delivery(bot_identity.id),
                ) as Arc<dyn gradius_ads::GradiusAdAppender>)
            }
            Err(error) => {
                tracing::warn!(%error, "Gradius privacy redactor unavailable");
                readiness_checks.push(ReadinessCheck::skipped(
                    "gradius",
                    format!("Gradius privacy redactor unavailable: {error}"),
                ));
                None
            }
        }
    };
    // The dialog session engine drives every turn: engine-owned tool loop,
    // multi-message turns, per-(chat, thread) serialization with initiator
    // injection.
    let dialog_session_wiring: Arc<dialog_turn::SessionWorkerWiring> =
        Arc::new(dialog_turn::SessionWorkerWiring {
            toolbox: Arc::clone(&dialog_toolbox),
            registry: Arc::new(dialog_turn::DialogSessionRegistry::new()),
            reactor: Some(
                Arc::new(reactions::GenerationReactionSignaler::new(telegram.clone()))
                    as Arc<dyn dialog_turn::SessionReactor>,
            ),
            gradius: gradius_for_dialog,
            max_iterations: config.llm.dialog.session_max_iterations,
            max_messages: config.llm.dialog.session_max_messages,
            tool_extension_secs: config.llm.dialog.session_tool_extension_secs,
            hard_cap_secs: config.llm.dialog.session_hard_cap_secs,
            max_draws: config.llm.dialog.session_max_draws,
            max_songs: config.llm.dialog.session_max_songs,
        });
    let embedding_attempt_walker = attempt_walker.clone();
    let memory_query_embedder = if dialog_memory_context_enabled(config) {
        Some(Arc::new(embedder::RoutedDiscoveryEmbedder::new(
            embedding_attempt_walker.clone(),
            embedder::DiscoveryEmbedderConfig {
                base_url: config.llm.discovery.base_url.clone(),
                service_name: config.memory.embedder_service_name.clone(),
                endpoint_name: config.memory.embedder_endpoint_name.clone(),
                request_timeout: memory_runtime::MEMORY_RETRIEVAL_EMBEDDING_TIMEOUT,
                task_timeout: memory_runtime::MEMORY_RETRIEVAL_EMBEDDING_TIMEOUT,
                poll_interval: Duration::from_millis(100),
                capacity_wait: Duration::from_secs(2),
            },
        )) as Arc<dyn memory_runtime::EmbeddingProvider>)
    } else {
        None
    };
    let shield_query_timeout =
        Duration::from_secs(config.shield.retrieval_timeout_seconds.max(1) as u64);
    let shield_query_embedder = Some(Arc::new(embedder::RoutedDiscoveryEmbedder::new(
        embedding_attempt_walker,
        embedder::DiscoveryEmbedderConfig {
            base_url: config.llm.discovery.base_url.clone(),
            service_name: config.shield.embedder_service_name.clone(),
            endpoint_name: config.shield.embedder_endpoint_name.clone(),
            request_timeout: shield_query_timeout,
            task_timeout: shield_query_timeout,
            poll_interval: Duration::from_millis(100),
            capacity_wait: Duration::from_secs(2),
        },
    )) as Arc<dyn memory_runtime::EmbeddingProvider>);
    {
        let poller_handle = Arc::clone(&router_handle);
        let poller_triggers = Arc::clone(&router_triggers);
        let poller_breakers = Arc::clone(&router_breakers);
        let poller_queue = Arc::clone(&task_queue_for_updates);
        let poller_stop = worker_stops.subscribe(RuntimeWorkerPhase::Ancillary);
        let poller_interval = Duration::from_secs(
            config
                .persistent_queue
                .dialog_aifarm_fallback_poll_interval_seconds
                .max(1) as u64,
        );
        worker_registry.register(
            "router-trigger-poller",
            RuntimeWorkerPhase::Ancillary,
            tokio::spawn(async move {
                dialog_jobs::run_router_trigger_poller(
                    poller_handle,
                    poller_triggers,
                    poller_breakers,
                    poller_queue,
                    poller_interval,
                    wait_for_runtime_stop(poller_stop),
                )
                .await;
            }),
        );
    }
    let dialog_provider_for_updates = {
        let mut dialog_provider = dialog_runtime::router_dialog_provider(
            config,
            Arc::clone(&router_handle),
            Arc::clone(&router_breakers),
            Arc::clone(&router_triggers),
            Arc::clone(&router_pools),
            Some(routing_event_reporter.clone()),
            Arc::clone(&prompt_store),
        );
        // One shared check-event recorder serves both the REAL and the
        // SAFE (virtual-dialog) WhiteCircle wraps.
        let white_circle_recorder: Option<
            Arc<dyn openplotva_llm::whitecircle::WhiteCircleCheckEventRecorder>,
        > = if dialog_runtime::white_circle_effective_enabled(config) {
            let (recorder, recorder_worker) =
                runtime_safety::PostgresWhiteCircleCheckEventRecorder::spawn(
                    service_clients.postgres.clone(),
                    worker_stops.subscribe(RuntimeWorkerPhase::Ancillary),
                );
            worker_registry.register(
                "white-circle-event-recorder",
                RuntimeWorkerPhase::Ancillary,
                recorder_worker,
            );
            Some(Arc::new(recorder))
        } else {
            None
        };
        if let Some(recorder) = white_circle_recorder.clone() {
            dialog_provider =
                wrap_dialog_provider_with_white_circle(dialog_provider, config, recorder);
        }
        let dialog_provider: openplotva_llm::ChatProviderHandle = dialog_provider;
        let dialog_provider_for_updates = Some(Arc::clone(&dialog_provider));
        let dialog_effects =
            dialog_jobs::DialogDispatcherEffects::new(Arc::clone(&dispatcher_queue))
                .with_durability_barrier(Arc::new(shared_task_queue.clone()))
                .with_durable_outbox(
                    openplotva_storage::PostgresTelegramOutboxStore::new(
                        service_clients.critical_postgres.clone(),
                    ),
                    bot_identity.id,
                );
        let dialog_terminal_signal = dialog_turn::DispatcherTerminalUserSignal::new(
            telegram.clone(),
            Arc::clone(&dispatcher_queue),
        );
        let mut dialog_materializer = dialog_jobs::PostgresDialogInputMaterializer::new(
            chat_settings_store.clone(),
            store.clone(),
            history_store.clone(),
            dialog_jobs::DialogBotIdentity::new(bot_identity.id, bot_identity.first_name.clone()),
        );
        if dialog_memory_context_enabled(config) {
            dialog_materializer = dialog_materializer
                .with_memory_store(memory_store.clone(), memory_card_usage.clone());
            if let Some(embedder) = memory_query_embedder.clone() {
                dialog_materializer =
                    dialog_materializer.with_memory_embedder(embedder, config.memory.embedding_dim);
            }
        }
        if config.shield.enabled {
            dialog_materializer = dialog_materializer.with_shield_store(
                PostgresShieldStore::new(service_clients.postgres.clone()),
                shield_options_from_config(&config.shield),
                shield_history_tail_messages_from_config(&config.shield),
            );
            if let Some(embedder) = shield_query_embedder.clone() {
                dialog_materializer = dialog_materializer.with_shield_embedder(embedder);
            }
        }
        dialog_materializer =
            dialog_materializer.with_asr_materializer(Arc::clone(&dialog_context_asr));
        dialog_materializer = dialog_materializer.with_vision_materializer(dialog_context_vision);
        let _ = media_failure_explainer.set(agent_tools::MediaFailureExplainer {
            provider: Arc::clone(&dialog_provider),
            materializer: dialog_materializer.clone(),
            history: history_store.clone(),
        });
        let safe_dialog_toolbox: Arc<dyn openplotva_dialog::DialogToolbox> = Arc::new(
            runtime_virtual_dialog::RuntimeVirtualSafeToolbox::new(Arc::clone(&dialog_toolbox)),
        );
        let console_safe_toolbox = Arc::clone(&safe_dialog_toolbox);
        virtual_dialog_manager.set_executor(Arc::new(
            runtime_virtual_dialog::RuntimeVirtualDialogExecutor::new(
                virtual_dialog_store.clone(),
                store.clone(),
                history_store.clone(),
                dialog_materializer.clone(),
                Arc::clone(&dialog_provider),
                console_safe_toolbox,
                Arc::clone(&dialog_toolbox),
                taskman_inspector.clone(),
                llm_trace_buffer.clone(),
                dialog_jobs::DialogBotIdentity::new(
                    bot_identity.id,
                    bot_identity.first_name.clone(),
                ),
            )
            .with_run_buffer(llm_run_buffer.clone()),
        ));
        readiness_checks.push(ReadinessCheck::ok(
                "runtime_virtual_dialogs",
                "Runtime virtual dialogs wired to routed dialog provider, storage history, and SAFE/REAL tool modes",
            ));
        let dialog_tool_history = history_store.clone();
        let dialog_max_llm_job_attempts = config.persistent_queue.llm_job_max_attempts;
        let dialog_turn_budget_secs = config.llm.dialog.turn_budget_secs;
        let dialog_turn_max_queue_age_secs = config.llm.dialog.turn_max_queue_age_secs;
        let dialog_turn_max_regenerations = config.llm.dialog.turn_max_regenerations;
        let dialog_terminal_reaction_emoji = config.llm.dialog.terminal_reaction_emoji.clone();
        let dialog_terminal_signal_max_age_secs = config.llm.dialog.terminal_signal_max_age_secs;
        // The dialog worker count follows the dialog primary's lanes; the
        // lane queues in the walker are the real limit, so extra workers
        // wait in the primary's queue during their model rounds and run
        // their non-model phases meanwhile. The env knob is only the
        // fallback when the primary has no lane pool.
        let dialog_worker_fallback =
            u32::try_from(config.persistent_queue.dialog_aifarm_workers.max(0)).unwrap_or(0);
        let dialog_workers_cap =
            u32::try_from(config.persistent_queue.dialog_workers_cap.max(1)).unwrap_or(1);
        let (dialog_scale_tx, dialog_scale_rx) =
            tokio::sync::watch::channel(dialog_worker_fallback);
        let dialog_lanes = model_routing::LaneWorkerScale {
            workflow_key: "dialog",
            per_lane: u32::try_from(config.persistent_queue.dialog_workers_per_lane.max(1))
                .unwrap_or(1),
            floor: 0,
            cap: dialog_workers_cap,
            scale: dialog_scale_tx,
        };
        let initial_dialog_workers = dialog_lanes
            .desired(&router_handle.snapshot())
            .unwrap_or(dialog_worker_fallback);
        let _ = dialog_lanes.scale.send(initial_dialog_workers);
        let dialog_worker_gauge = Arc::new(dialog_workers::WorkerGauge::new());

        let dialog_worker_spawner = {
            let queue = Arc::clone(&task_queue_for_updates);
            let provider = Arc::clone(&dialog_provider);
            let effects = dialog_effects.clone();
            let materializer = dialog_materializer.clone();
            let tool_history = dialog_tool_history.clone();
            let routing_events = routing_event_reporter.clone();
            let turn_outcomes = turn_outcome_observer.clone();
            let terminal_signal = dialog_terminal_signal.clone();
            let reaction_emoji = dialog_terminal_reaction_emoji.clone();
            let obligations = Arc::clone(&delivery_obligation_store);
            let session_wiring = dialog_session_wiring.clone();
            let llm_runs = llm_run_buffer.clone();
            let activity_pulse = Arc::clone(&telegram_activity_pulse);
            let stop_rx = worker_stops.subscribe(RuntimeWorkerPhase::Processor);
            move |index: usize, retire: tokio::sync::oneshot::Receiver<()>| {
                let worker_queue = Arc::clone(&queue);
                let worker_provider = Arc::clone(&provider);
                let worker_effects = effects.clone();
                let worker_materializer = materializer.clone();
                let worker_tool_history = tool_history.clone();
                let worker_routing_events = routing_events.clone();
                let worker_turn_outcomes = turn_outcomes.clone();
                let worker_terminal_signal = terminal_signal.clone();
                let worker_reaction_emoji = reaction_emoji.clone();
                let worker_obligations = Arc::clone(&obligations);
                let worker_session = session_wiring.clone();
                let worker_llm_runs = llm_runs.clone();
                let worker_activity_pulse = Arc::clone(&activity_pulse);
                let worker_stop = stop_rx.clone();
                tokio::spawn(async move {
                    // A retired worker finishes its current job and exits
                    // at the next loop tick, exactly like a runtime stop.
                    let stop_future = async move {
                        tokio::select! {
                            () = wait_for_runtime_stop(worker_stop) => {}
                            _ = retire => {}
                        }
                    };
                    let report =
                            dialog_jobs::run_dialog_job_worker_with_materializer_and_history_every_until(
                                worker_queue.as_ref(),
                                worker_provider.as_ref(),
                                &worker_effects,
                                dialog_jobs::DialogJobWorkerLoopOptions {
                                    materializer: &worker_materializer,
                                    tool_history: &worker_tool_history,
                                    routing_events: Some(&worker_routing_events),
                                    turn_outcomes: Some(&worker_turn_outcomes),
                                    queue_names: &dialog_jobs::DIALOG_JOB_WORKER_QUEUES,
                                    interval: dialog_jobs::DIALOG_JOB_POLL_INTERVAL,
                                    max_llm_job_attempts: dialog_max_llm_job_attempts,
                                    turn_budget_secs: dialog_turn_budget_secs,
                                    turn_max_queue_age_secs: dialog_turn_max_queue_age_secs,
                                    max_regenerations: dialog_turn_max_regenerations,
                                    terminal_signal: dialog_turn::TurnSignalPolicy::new(
                                        Some(&worker_terminal_signal),
                                        &worker_reaction_emoji,
                                        dialog_terminal_signal_max_age_secs,
                                    ),
                                    obligations: Some(worker_obligations.as_ref()),
                                    session: worker_session.as_ref(),
                                    llm_runs: Some(&worker_llm_runs),
                                    activity_pulse: Some(worker_activity_pulse.as_ref()),
                                },
                                stop_future,
                            )
                            .await;

                    tracing::info!(?report, index, "dialog taskman worker stopped");
                })
            }
        };
        let dialog_supervisor = dialog_workers::spawn_worker_supervisor(
            Arc::new(dialog_worker_spawner),
            dialog_scale_rx,
            Arc::clone(&dialog_worker_gauge),
            worker_stops.subscribe(RuntimeWorkerPhase::Processor),
        );
        worker_registry.register(
            "dialog-worker-supervisor",
            RuntimeWorkerPhase::Processor,
            dialog_supervisor,
        );
        workers.dialog_worker_gauge = Some(Arc::clone(&dialog_worker_gauge));
        workers.router_runtime = Some(Arc::new(model_routing::RouterRuntime {
            handle: Arc::clone(&router_handle),
            pools: Arc::clone(&router_pools),
            dialog_workers: Some(dialog_lanes),
            memory_workers: memory_worker_scale.take(),
            lane_check: Some(lane_check.clone()),
        }));
        tracing::info!(
            initial_dialog_workers,
            cap = dialog_workers_cap,
            fallback = dialog_worker_fallback,
            "dialog worker scale derived from lane pools"
        );
        readiness_checks.push(ReadinessCheck::ok(
                "dialog_jobs",
                format!(
                    "Started dialog worker supervisor with {initial_dialog_workers} lane-derived workers (cap {dialog_workers_cap})"
                ),
            ));
        if initial_dialog_workers > 0 {
            shared_taskman_worker_counts.insert(
                openplotva_taskman::TEXT_QUEUE_NAME.to_owned(),
                initial_dialog_workers as i32,
            );
            shared_taskman_worker_counts.insert(
                openplotva_taskman::DIALOG_AIFARM_QUEUE_NAME.to_owned(),
                initial_dialog_workers as i32,
            );
        }
        dialog_provider_for_updates
    };

    let media_prompt_optimizer = media::MediaPromptOptimizerService::new(Some(Arc::new(
        media::RoutedMediaPromptOptimizer::new(
            attempt_walker.clone(),
            config,
            Arc::clone(&prompt_store),
        ),
    )
        as media::AppMediaPromptOptimizer));
    let media_max_llm_job_attempts = config.persistent_queue.llm_job_max_attempts;

    // Chat history and requester memory for the image optimizer and the song
    // director: two concurrent, time-boxed searches rendered as a Context block.
    let chat_context = Arc::new(agent_runtime::ChatContextGatherer::new(
        Arc::new(agent_runtime::PostgresHistorySearch::new(
            history_store.clone(),
        )),
        Arc::new(agent_runtime::PostgresMemorySearch::new(
            memory_store.clone(),
        )),
    ));
    let vip_image_queue = Arc::clone(&task_queue_for_updates);
    let image_attempt_walker = attempt_walker.clone();
    let vip_slot = |workflow_key: &str| {
        image_jobs::RoutedImageGenerator::new(
            image_attempt_walker.clone(),
            image_jobs::aifarm_draw_api_config_from_app_config(config),
        )
        .with_workflow_key(workflow_key)
    };
    let [
        vip_first_workflow_key,
        vip_second_workflow_key,
        vip_third_workflow_key,
    ] = VIP_IMAGE_GENERATION_WORKFLOW_KEYS;
    let vip_image_generator = image_jobs::OptimizingImageGenerator::new(
        image_jobs::ParallelImageGenerator::new(
            image_jobs::ParallelImageGenerator::new(
                vip_slot(vip_first_workflow_key),
                vip_slot(vip_second_workflow_key),
            ),
            vip_slot(vip_third_workflow_key),
        ),
        media_prompt_optimizer.clone(),
    )
    .with_context_provider(chat_context.clone());
    let mut vip_image_effects = image_jobs::TelegramImageJobEffects::new(telegram.clone())
        .with_last_generation_writer(Arc::new(service_clients.redis.last_generation_store()));
    {
        let reactions = &generation_reactions;
        vip_image_effects = vip_image_effects.with_reaction_ux(Arc::clone(reactions));
    }
    let vip_image_stop = worker_stops.subscribe(RuntimeWorkerPhase::Processor);
    let vip_image_activity_pulse = Arc::clone(&telegram_activity_pulse);
    let vip_image_worker = tokio::spawn(async move {
        let report = image_jobs::run_image_gen_worker_every_until_with_max_attempts(
            vip_image_queue.as_ref(),
            &vip_image_generator,
            &vip_image_effects,
            &image_jobs::IMAGE_VIP_JOB_WORKER_QUEUES,
            image_jobs::ImageWorkerRunOptions {
                interval: image_jobs::IMAGE_JOB_POLL_INTERVAL,
                max_llm_job_attempts: media_max_llm_job_attempts,
                activity_pulse: Some(vip_image_activity_pulse.as_ref()),
            },
            wait_for_runtime_stop(vip_image_stop),
        )
        .await;

        tracing::info!(?report, "VIP image generation taskman worker stopped");
    });
    worker_registry.register(
        "vip-image-generation-worker",
        RuntimeWorkerPhase::Processor,
        vip_image_worker,
    );

    let vip_image_edit_queue = Arc::clone(&task_queue_for_updates);
    let vip_image_edit_references: Arc<dyn image_edit_references::ImageEditReferenceLoader> =
        Arc::new(image_edit_references::TelegramImageEditReferenceLoader::new(telegram.clone()));
    let vip_image_edit_provider = image_edit_references::TelegramPictureBytesEditor::new(
        image_jobs::OptimizingImageEditor::new(
            image_jobs::RoutedImageEditor::new(
                image_attempt_walker.clone(),
                vision_data_urls.clone(),
                image_jobs::aifarm_draw_api_config_from_app_config(config),
            )
            .with_workflow_key(VIP_IMAGE_EDITOR_WORKFLOW_KEY),
            media_prompt_optimizer.clone(),
        )
        .with_reference_loader(Arc::clone(&vip_image_edit_references)),
        vip_image_edit_references,
    );
    let mut vip_image_edit_effects = image_jobs::TelegramImageJobEffects::new(telegram.clone())
        .with_last_generation_writer(Arc::new(service_clients.redis.last_generation_store()));
    {
        let reactions = &generation_reactions;
        vip_image_edit_effects = vip_image_edit_effects.with_reaction_ux(Arc::clone(reactions));
    }
    let vip_image_edit_stop = worker_stops.subscribe(RuntimeWorkerPhase::Processor);
    let vip_image_edit_activity_pulse = Arc::clone(&telegram_activity_pulse);
    let vip_image_edit_worker = tokio::spawn(async move {
        let report = image_jobs::run_image_edit_worker_every_until_with_max_attempts(
            vip_image_edit_queue.as_ref(),
            &vip_image_edit_provider,
            &vip_image_edit_effects,
            &image_jobs::IMAGE_VIP_JOB_WORKER_QUEUES,
            image_jobs::ImageWorkerRunOptions {
                interval: image_jobs::IMAGE_JOB_POLL_INTERVAL,
                max_llm_job_attempts: media_max_llm_job_attempts,
                activity_pulse: Some(vip_image_edit_activity_pulse.as_ref()),
            },
            wait_for_runtime_stop(vip_image_edit_stop),
        )
        .await;

        tracing::info!(?report, "VIP image edit taskman worker stopped");
    });
    worker_registry.register(
        "vip-image-edit-worker",
        RuntimeWorkerPhase::Processor,
        vip_image_edit_worker,
    );

    let regular_image_queue = Arc::clone(&task_queue_for_updates);
    let regular_image_generator = image_jobs::OptimizingImageGenerator::new(
        image_jobs::RoutedImageGenerator::new(
            image_attempt_walker,
            image_jobs::aifarm_draw_api_config_from_app_config(config),
        )
        .with_workflow_key(REGULAR_IMAGE_GENERATION_WORKFLOW_KEY),
        media_prompt_optimizer,
    )
    .with_context_provider(chat_context.clone());
    let mut regular_image_effects = image_jobs::TelegramImageJobEffects::new(telegram.clone())
        .with_last_generation_writer(Arc::new(service_clients.redis.last_generation_store()));
    if config.gradius.utility_image_enabled
        && let Some((ads, outbox)) = &gradius_generation
    {
        regular_image_effects = regular_image_effects.with_utility_ads(Arc::new(
            gradius_utility_outbox::GradiusUtilityImageAds {
                ads: Arc::clone(ads),
                outbox: Arc::clone(outbox),
            },
        ));
    }
    {
        let reactions = &generation_reactions;
        regular_image_effects = regular_image_effects.with_reaction_ux(Arc::clone(reactions));
    }
    let regular_image_stop = worker_stops.subscribe(RuntimeWorkerPhase::Processor);
    let regular_image_activity_pulse = Arc::clone(&telegram_activity_pulse);
    let regular_image_worker = tokio::spawn(async move {
        let report = image_jobs::run_image_gen_worker_every_until_with_max_attempts(
            regular_image_queue.as_ref(),
            &regular_image_generator,
            &regular_image_effects,
            &image_jobs::IMAGE_REGULAR_JOB_WORKER_QUEUES,
            image_jobs::ImageWorkerRunOptions {
                interval: image_jobs::IMAGE_JOB_POLL_INTERVAL,
                max_llm_job_attempts: media_max_llm_job_attempts,
                activity_pulse: Some(regular_image_activity_pulse.as_ref()),
            },
            wait_for_runtime_stop(regular_image_stop),
        )
        .await;

        tracing::info!(?report, "regular image generation taskman worker stopped");
    });
    worker_registry.register(
        "regular-image-generation-worker",
        RuntimeWorkerPhase::Processor,
        regular_image_worker,
    );
    readiness_checks.push(ReadinessCheck::ok(
        "image_jobs",
        "Image taskman workers started for VIP Flux+Boogu generation, Flux-only editing, and regular Boogu generation",
    ));
    for queue_name in image_jobs::IMAGE_VIP_JOB_WORKER_QUEUES {
        shared_taskman_worker_counts
            .entry(queue_name.to_owned())
            .and_modify(|count| *count += 2)
            .or_insert(2);
    }
    for queue_name in image_jobs::IMAGE_REGULAR_JOB_WORKER_QUEUES {
        shared_taskman_worker_counts
            .entry(queue_name.to_owned())
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }
    if config.music.acestep.enabled {
        let music_queue = Arc::clone(&task_queue_for_updates);
        let music_song_prompt_generator: Arc<dyn music_jobs::SongPromptGenerator + Send + Sync> =
            Arc::new(music_jobs::RoutedSongPromptGenerator::new(
                attempt_walker.clone(),
                config,
                Arc::clone(&prompt_store),
            ));
        let song_context: Arc<dyn music_jobs::SongContextProvider> = chat_context.clone();
        let music_material_provider = music_jobs::AifarmSongMaterialProvider::new(
            music_song_prompt_generator,
            PostgresVirtualMessageStore::new(service_clients.postgres.clone()),
        )
        .with_context_provider(song_context);
        let music_attempt_walker = attempt_walker.clone();
        let music_generator = music_jobs::RoutedMusicGenerator::new(
            music_attempt_walker,
            music_jobs::acestep_config_from_app_config(config),
        );
        let mut music_effects = music_jobs::TelegramMusicJobEffects::new(
            Arc::clone(&permission_policy),
            PostgresTelegramFileStore::new(service_clients.postgres.clone()),
            telegram.clone(),
            media_uploader.clone(),
            Arc::clone(&rich_outbox),
            PostgresGeneratedSongStore::new(service_clients.critical_postgres.clone()),
        );
        {
            let reactions = &generation_reactions;
            music_effects = music_effects.with_reaction_ux(Arc::clone(reactions));
        }
        let music_stop = worker_stops.subscribe(RuntimeWorkerPhase::Processor);
        let music_activity_pulse = Arc::clone(&telegram_activity_pulse);
        let music_worker = tokio::spawn(async move {
            let report = music_jobs::run_music_worker_every_until_with_max_attempts(
                music_queue.as_ref(),
                &music_material_provider,
                &music_generator,
                &music_effects,
                music_jobs::MusicWorkerRunOptions {
                    interval: music_jobs::MUSIC_JOB_POLL_INTERVAL,
                    max_llm_job_attempts: media_max_llm_job_attempts,
                    activity_pulse: Some(music_activity_pulse.as_ref()),
                },
                wait_for_runtime_stop(music_stop),
            )
            .await;

            tracing::info!(?report, "music generation taskman worker stopped");
        });
        worker_registry.register(
            "music-generation-worker",
            RuntimeWorkerPhase::Processor,
            music_worker,
        );
        readiness_checks.push(ReadinessCheck::ok(
                    "music_jobs",
                    "Music taskman worker started for music-vip queue with routed song reprompt and routed ACE-Step provider",
                ));
        for queue_name in music_jobs::MUSIC_JOB_WORKER_QUEUES {
            shared_taskman_worker_counts
                .entry(queue_name.to_owned())
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "music_jobs",
            "ACESTEP_ENABLED=false",
        ));
    }
    let shared_taskman_worker_ids =
        task_queue::shared_task_queue_worker_ids(&shared_taskman_worker_counts);
    taskman_inspector.set_shared_queue(
        Arc::clone(&task_queue_for_updates),
        shared_taskman_worker_counts,
    );
    taskman_inspector.set_sql_store(Arc::new(openplotva_storage::PostgresTaskQueueStore::new(
        service_clients.postgres.clone(),
    )));
    let shared_task_queue_heartbeat_runtime = shared_task_queue.clone();
    let shared_task_queue_heartbeat_stop = worker_stops.subscribe(RuntimeWorkerPhase::Processor);
    let shared_task_queue_heartbeat_worker_ids = shared_taskman_worker_ids.clone();
    let shared_task_queue_heartbeat_worker = tokio::spawn(async move {
        let report = task_queue::run_shared_task_queue_heartbeat_worker_until(
            shared_task_queue_heartbeat_runtime,
            shared_task_queue_heartbeat_worker_ids,
            shared_task_queue_heartbeat_interval,
            wait_for_runtime_stop(shared_task_queue_heartbeat_stop),
        )
        .await;

        tracing::info!(?report, "shared taskman worker heartbeat stopped");
    });
    readiness_checks.push(ReadinessCheck::ok(
        "shared_task_queue_worker_heartbeat",
        format!(
            "worker heartbeat every {}s for {} shared taskman workers",
            shared_task_queue_heartbeat_interval.as_secs(),
            shared_taskman_worker_ids.len()
        ),
    ));
    worker_registry.register(
        "shared-task-queue-heartbeat",
        RuntimeWorkerPhase::Processor,
        shared_task_queue_heartbeat_worker,
    );

    let ephemeral_cleanup_store = ephemeral_store.clone();
    let ephemeral_cleanup_telegram = telegram.clone();
    let ephemeral_cleanup_stop = worker_stops.subscribe(RuntimeWorkerPhase::Ancillary);
    let ephemeral_cleanup_worker = tokio::spawn(async move {
        let report = virtual_messages::run_ephemeral_cleanup_worker_until(
            &ephemeral_cleanup_store,
            |method| {
                let telegram = ephemeral_cleanup_telegram.clone();
                async move {
                    openplotva_telegram::execute_telegram_method(&telegram, method)
                        .await
                        .map(|_| ())
                }
            },
            wait_for_runtime_stop(ephemeral_cleanup_stop),
        )
        .await;

        tracing::info!(?report, "ephemeral message cleanup worker stopped");
    });
    readiness_checks.push(ReadinessCheck::ok(
        "ephemeral_messages",
        "Telegram ephemeral message cleanup worker started",
    ));
    worker_registry.register(
        "ephemeral-message-cleanup",
        RuntimeWorkerPhase::Ancillary,
        ephemeral_cleanup_worker,
    );

    let update_ingress_guard = Arc::new(openplotva_updates::UpdateIngressGuard::with_defaults());

    if !config.service_probe.produce_updates {
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_update_producer",
            "OPENPLOTVA_PRODUCE_UPDATES=false",
        ));
    } else if config.bot.webhook.enabled {
        if config.bot.webhook.url.is_empty() {
            anyhow::bail!("BOT_WEBHOOK_URL is required when webhook updates are enabled");
        }
        let webhook_setup = webhook_setup_from_config(&config.bot.webhook)
            .context("load Telegram webhook certificate")?;
        let webhook_startup = TelegramWebhookSetupClient::new(bot_key.to_owned(), telegram.clone());
        webhook_startup
            .set_webhook(&webhook_setup)
            .await
            .map_err(|error| anyhow::anyhow!("set Telegram webhook: {error}"))?;
        let webhook_sender =
            openplotva_telegram::webhook_update_stream(update_stream.clone(), bot_identity.id)
                .with_ingress_guard(Arc::clone(&update_ingress_guard));
        let readiness_webhook_sender = webhook_sender.clone();
        workers.readiness_probes.push(Arc::new(move || {
            telegram_webhook_ingress_readiness(&readiness_webhook_sender)
        }));
        workers.webhook_route = Some(TelegramWebhookRoute {
            sender: webhook_sender,
            secret_token: Arc::from(config.bot.webhook.secret_token.clone()),
        });
        readiness_checks.push(ReadinessCheck::ok(
            "telegram_update_producer",
            "Telegram webhook appends raw updates directly to the durable Redis Stream",
        ));
    } else {
        let update_startup = telegram.clone();
        let update_source = telegram.clone();
        let update_stream = update_stream.clone();
        let update_ingress_guard = Arc::clone(&update_ingress_guard);
        let update_stop = worker_stops.subscribe(RuntimeWorkerPhase::Ingress);
        let update_bot_id = bot_identity.id;
        let update_producer_worker = tokio::spawn(async move {
            if let Err(error) = update_startup.delete_webhook().await {
                tracing::warn!(%error, "Telegram long-poll Stream producer stopped before polling");
                return;
            }
            let report =
                openplotva_telegram::run_long_poll_stream_producer_with_ingress_guard_until(
                    &update_source,
                    &update_stream,
                    update_bot_id,
                    update_ingress_guard.as_ref(),
                    wait_for_runtime_stop(update_stop),
                )
                .await;
            tracing::info!(?report, "Telegram long-poll Stream producer stopped");
        });
        worker_registry.register(
            "telegram-update-producer",
            RuntimeWorkerPhase::Ingress,
            update_producer_worker,
        );
        readiness_checks.push(ReadinessCheck::ok(
            "telegram_update_producer",
            "Telegram long polling atomically appends batches and cursors to the durable Redis Stream",
        ));
    }

    {
        let materializer_config = openplotva_updates::UpdateStreamMaterializerConfig {
            batch_max_rows: config.update_queue.materializer_batch_max_rows,
            batch_max_bytes: config.update_queue.materializer_batch_max_bytes,
            batch_max_wait: Duration::from_millis(
                u64::try_from(config.update_queue.materializer_batch_max_wait_ms).unwrap_or(50),
            ),
            db_timeout: Duration::from_millis(
                u64::try_from(config.update_queue.materializer_db_timeout_ms).unwrap_or(5_000),
            ),
            ..openplotva_updates::UpdateStreamMaterializerConfig::default()
        };
        let projection_materializer_config =
            update_materializer::UpdateProjectionMaterializerConfig {
                flush_interval: Duration::from_millis(
                    u64::try_from(config.update_queue.projection_flush_interval_ms)
                        .unwrap_or(10_000),
                ),
                flush_max_mutations: config.update_queue.projection_flush_max_mutations,
                stage_hard_limit_rows: config.update_queue.projection_stage_hard_limit_rows,
            };
        let materializer_stream = update_stream.clone();
        let materializer_store = openplotva_storage::PostgresTelegramDeliveryStore::new(
            service_clients.critical_postgres.clone(),
        );
        let materializer_metrics = update_materializer::UpdateMaterializerMetrics::default();
        let readiness_metrics = materializer_metrics.clone();
        let readiness_stage_hard_limit =
            i64::try_from(projection_materializer_config.stage_hard_limit_rows).unwrap_or(i64::MAX);
        workers.readiness_probes.push(Arc::new(move || {
            let snapshot = readiness_metrics.snapshot();
            if !snapshot.supervisor_running {
                return ReadinessCheck::error(
                    "telegram_update_materializer_live",
                    format!(
                        "supervisor is not running; {} restarts",
                        snapshot.supervisor_restarts
                    ),
                );
            }
            if snapshot.projection_stage_rows >= readiness_stage_hard_limit
                || snapshot
                    .projection_oldest_stage_age
                    .is_some_and(|age| age > Duration::from_secs(30))
            {
                ReadinessCheck::error(
                    "telegram_update_materializer_live",
                    format!(
                        "projection staging degraded; {} rows, oldest age {} ms, {} flush errors",
                        snapshot.projection_stage_rows,
                        snapshot
                            .projection_oldest_stage_age
                            .map_or(0, |age| age.as_millis()),
                        snapshot.projection_flush_errors,
                    ),
                )
            } else {
                ReadinessCheck::ok(
                    "telegram_update_materializer_live",
                    format!(
                        "supervisor running; lease held={}; {} restarts; {} staged projection rows",
                        snapshot.lease_held,
                        snapshot.supervisor_restarts,
                        snapshot.projection_stage_rows,
                    ),
                )
            }
        }));
        updates_inspector.set_stream_plane(
            update_stream.clone(),
            service_clients.postgres.clone(),
            materializer_metrics.clone(),
        );
        let materializer_bot_id = bot_identity.id;
        let materializer_stop = worker_stops.subscribe(RuntimeWorkerPhase::Processor);
        let materializer_worker = tokio::spawn(async move {
            match update_materializer::run_supervised_update_materializer_until(
                materializer_stream,
                materializer_store,
                materializer_bot_id,
                materializer_config,
                projection_materializer_config,
                materializer_metrics,
                materializer_stop,
            )
            .await
            {
                Ok(()) => tracing::info!("Telegram update bulk materializer supervisor stopped"),
                Err(error) => {
                    tracing::error!(%error, "Telegram update bulk materializer configuration rejected")
                }
            }
        });
        worker_registry.register(
            "telegram-update-materializer",
            RuntimeWorkerPhase::Processor,
            materializer_worker,
        );
        readiness_checks.push(ReadinessCheck::ok(
            "telegram_update_materializer",
            format!(
                "Redis Stream bulk materializer supervisor started ({} rows, {} bytes)",
                config.update_queue.materializer_batch_max_rows,
                config.update_queue.materializer_batch_max_bytes,
            ),
        ));
    }

    if config.service_probe.consume_updates {
        let dialog_debounce = Arc::new(dialog_debounce::InMemoryDialogDebounce::new());
        workers.dialog_debounce = Some(Arc::clone(&dialog_debounce));
        let dialog_scheduler = Arc::new(
            dialog_messages::TaskmanDialogMessageScheduler::new(
                Arc::clone(&task_queue_for_updates),
                Arc::clone(&dialog_debounce),
                dialog_debounce::DIALOG_DEBOUNCE_INTERVAL,
                Arc::new(shared_task_queue.clone()),
            )
            .with_typing_effects(Arc::new(
                dialog_messages::DialogTypingActionRuntimeEffects::new(
                    telegram.clone(),
                    Arc::clone(&permission_policy),
                ),
            ))
            .with_task_enqueue_rate_limit(task_enqueue_rate_limit.clone()),
        );
        let random_dialog_effects = Arc::new(dialog_messages::RandomDialogDispatcherEffects::new(
            Arc::clone(&dispatcher_queue_for_updates),
        ));
        let terminal = Arc::new(RuntimeUnhandledUpdateHandler);
        let dialog_terminal = Arc::new(
            dialog_messages::DialogMessageUpdateHandler::new(
                dialog_scheduler,
                Arc::new(chat_settings_store.clone()),
                random_dialog_effects,
                Arc::new(dialog_messages::ThreadRandomDialogRng),
                dialog_messages::DialogMessageUpdateConfig::from_app_config(
                    config,
                    bot_user_from_get_me(&bot_identity),
                ),
                terminal,
            )
            .with_image_scheduler(dialog_tool_adapter.clone())
            .with_song_scheduler(dialog_tool_adapter.clone())
            .with_direct_draw_api_effects(direct_draw_api_effects)
            .with_gate_counters(Arc::clone(&ingestion_gate_counters)),
        );
        let skipped = Arc::new(skipped::SkippedUpdateHandler::new(dialog_terminal));
        let mut guest_effects = guest::GuestRuntimeEffects::new(telegram.clone())
            .with_dialog_provider(dialog_provider_for_updates.clone());
        if config.shield.enabled {
            guest_effects = guest_effects.with_shield_store(
                PostgresShieldStore::new(service_clients.postgres.clone()),
                shield_options_from_config(&config.shield),
            );
            if let Some(embedder) = shield_query_embedder.clone() {
                guest_effects = guest_effects.with_shield_embedder(embedder);
            }
        }
        let guest_handler = Arc::new(guest::GuestMessageUpdateHandler::new(
            Arc::new(guest_effects),
            guest::GuestMessageConfig {
                bot_user: bot_user_from_get_me(&bot_identity),
                shield_query_max_chars: shield_options_from_config(&config.shield).query_max_chars,
            },
            skipped,
        ));
        let delete_lyrics = Arc::new(delete_lyrics::DeleteLyricsCallbackUpdateHandler::new(
            Arc::clone(&telegram_effects),
            Arc::clone(&telegram_effects),
            guest_handler,
        ));
        let delete_drawing = Arc::new(delete_drawing::DeleteDrawingCallbackUpdateHandler::new(
            Arc::new(service_clients.redis.last_generation_store()),
            Arc::clone(&telegram_effects),
            Arc::clone(&vip_status_for_updates),
            Arc::clone(&telegram_effects),
            delete_lyrics,
        ));
        let checkin_theme = Arc::new(checkin::CheckinThemeCallbackUpdateHandler::new(
            Arc::clone(&control_queue_for_updates),
            Arc::new(checkin::CheckinThemeRuntimeEffects::new(
                Arc::clone(&dispatcher_queue_for_updates),
                telegram.clone(),
            )),
            delete_drawing,
        ));
        let vip_callbacks = Arc::new(payments::VipCancellationCallbackUpdateHandler::new(
            Arc::clone(&payment_store_for_updates),
            Arc::clone(&payment_runtime_effects),
            checkin_theme,
        ));
        let delete_message = Arc::new(delete_message::DeleteMessageCallbackUpdateHandler::new(
            Arc::clone(&telegram_effects),
            Arc::clone(&telegram_effects),
            vip_callbacks,
        ));
        let callbacks = Arc::new(callbacks::CallbackQueryUpdateHandler::new(
            Arc::clone(&telegram_effects),
            rate_limits_for_updates,
            delete_message,
        ));
        let inline = Arc::new(inline::InlineQueryUpdateHandler::new(
            Arc::clone(&telegram_effects),
            callbacks,
        ));
        let text_reply_settings_gate = Arc::new(updates::BoxedUpdateHandler::new(Arc::new(
            message_gate::TextReplySettingsGateUpdateHandler::new(
                Arc::new(chat_settings_store.clone()),
                inline,
            ),
        )));
        let reset_handler = Arc::new(reset::ResetCommandUpdateHandler::new(
            Arc::clone(&history_store_for_updates),
            Arc::clone(&dispatcher_queue_for_updates),
            Arc::new(telegram.clone()),
            bot_identity.username.clone(),
            text_reply_settings_gate,
        ));
        let delete_drawing_command =
            Arc::new(delete_drawing::DeleteDrawingCommandUpdateHandler::new(
                Arc::new(service_clients.redis.last_generation_store()),
                Arc::new(delete_drawing::DeleteDrawingCommandDispatcherEffects::new(
                    Arc::clone(&dispatcher_queue_for_updates),
                )),
                reset_handler,
            ));
        let mut rates_effects = rates::RatesDelivery::new(Arc::clone(&rich_outbox));
        if config.gradius.utility_rates_enabled
            && let Some((ads, outbox)) = &gradius_utility
        {
            rates_effects = rates_effects.with_utility_ads(Arc::clone(ads), Arc::clone(outbox));
        }
        let rates_command = Arc::new(rates::RatesCommandUpdateHandler::new(
            rates::RatesBotIdentity {
                user: bot_user_from_get_me(&bot_identity),
            },
            Arc::new(MessageGateCheckedRatesPermission),
            Some(Arc::clone(&rates_fetcher)),
            Arc::new(RuntimeRatesHeaderProvider),
            Arc::new(rates_effects),
            delete_drawing_command,
        ));
        let checkin_command = Arc::new(checkin::CheckinCommandUpdateHandler::new(
            Arc::clone(&control_queue_for_updates),
            checkin_game_store_for_updates,
            Arc::clone(&permission_policy),
            Arc::new(checkin::CheckinCommandDelivery::new(
                Arc::clone(&dispatcher_queue_for_updates),
                Arc::clone(&rich_outbox),
            )),
            bot_identity.username.clone(),
            rates_command,
        ));
        let post_service_blocked_gate =
            Arc::new(message_gate::PostServiceBlockedChatUpdateHandler::new(
                Arc::new(service_clients.redis.blocked_chat_store()),
                checkin_command,
            ));
        let settings_control_queue = Arc::new(settings::ProjectionAwareSettingsControlQueue::new(
            Arc::clone(&control_queue_for_updates),
            Arc::new(join_greeting_runtime.clone()),
        ));
        let settings_handler = Arc::new(updates::BoxedUpdateHandler::new(Arc::new(
            settings::SettingsUpdateHandler::new(
                Arc::clone(&dispatcher_queue_for_updates),
                settings_control_queue,
                settings::SettingsUpdateHandlerConfig::new(
                    bot_identity.username.clone(),
                    bot_identity.id,
                    config.server.url.clone(),
                ),
                post_service_blocked_gate,
            ),
        )));
        let chat_communication = Arc::new(members::ChatSettingsCommunicationEffects::new(
            chat_settings_store.clone(),
        ));
        let payment_handler = Arc::new(
            payments::PaymentUpdateHandler::new(
                Arc::clone(&control_queue_for_updates),
                Arc::clone(&vip_status_for_updates),
                Arc::clone(&payment_rich_effects),
                settings_handler,
            )
            .with_bot_username(bot_identity.username.clone()),
        );
        let admin_chat_settings_handler =
            Arc::new(settings::AdminChatSettingsCommandUpdateHandler::new(
                Arc::clone(&dispatcher_queue_for_updates),
                Arc::new(telegram.clone()),
                config.admins.admin_ids.clone(),
                bot_identity.username.clone(),
                config.server.url.clone(),
                Arc::clone(&payment_handler),
            ));
        let admin_settings_handler = Arc::new(admin::AdminSettingsCommandUpdateHandler::new(
            Arc::new(admin::AdminDispatcherEffects::new(Arc::clone(
                &dispatcher_queue_for_updates,
            ))),
            config.admins.admin_ids.clone(),
            bot_identity.username.clone(),
            config.server.url.clone(),
            admin_chat_settings_handler,
        ));
        let admin_enable_chat_handler = Arc::new(admin::AdminEnableChatCommandUpdateHandler::new(
            Arc::new(telegram.clone()),
            Arc::clone(&chat_communication),
            Arc::new(admin::AdminDispatcherEffects::new(Arc::clone(
                &dispatcher_queue_for_updates,
            ))),
            config.admins.admin_ids.clone(),
            bot_identity.username.clone(),
            admin_settings_handler,
        ));
        let admin_gemini_cache_purger =
            runtime_gemini_cache::GeminiExplicitCachePurger::from_config(&config.google_ai)
                .map(Arc::new);
        let admin_gemini_cache_handler =
            Arc::new(admin::AdminGeminiCacheCommandUpdateHandler::new(
                admin_gemini_cache_purger,
                Arc::new(admin::AdminDispatcherEffects::new(Arc::clone(
                    &dispatcher_queue_for_updates,
                ))),
                config.admins.admin_ids.clone(),
                bot_identity.username.clone(),
                admin_enable_chat_handler,
            ));
        let admin_redis_cache_handler = Arc::new(admin::AdminRedisCacheCommandUpdateHandler::new(
            Arc::new(service_clients.redis.client().clone()),
            Arc::new(admin::AdminDispatcherEffects::new(Arc::clone(
                &dispatcher_queue_for_updates,
            ))),
            config.admins.admin_ids.clone(),
            bot_identity.username.clone(),
            admin_gemini_cache_handler,
        ));
        let admin_runtime_token_manager = config.runtime_api.enabled.then(|| {
            Arc::new(runtime_api::RuntimeTokenManager::new(
                PostgresRuntimeTokenStore::new(service_clients.postgres.clone()),
            ))
        });
        let runtime_api_tls_public_key_pin = workers
            .runtime_api_tls_public_key_pin
            .clone()
            .unwrap_or_default();
        let admin_runtime_token_handler =
            Arc::new(admin::AdminRuntimeTokenCommandUpdateHandler::new(
                admin_runtime_token_manager,
                Arc::new(admin::AdminDispatcherEffects::new(Arc::clone(
                    &dispatcher_queue_for_updates,
                ))),
                config.admins.admin_ids.clone(),
                bot_identity.username.clone(),
                runtime_api_tls_public_key_pin,
                admin_redis_cache_handler,
            ));
        let admin_help_handler = Arc::new(admin::AdminHelpCommandUpdateHandler::new(
            Arc::new(admin::AdminDispatcherEffects::new(Arc::clone(
                &dispatcher_queue_for_updates,
            ))),
            config.admins.admin_ids.clone(),
            bot_identity.username.clone(),
            admin_runtime_token_handler,
        ));
        let admin_taskman_inspector: Arc<dyn openplotva_server::RuntimeTaskmanInspector> =
            Arc::new(taskman_inspector.clone());
        let admin_dispatcher_inspector: Arc<dyn openplotva_server::RuntimeDispatcherInspector> =
            Arc::new(dispatcher_inspector.clone());
        let admin_updates_inspector: Arc<dyn openplotva_server::RuntimeUpdatesInspector> =
            Arc::new(updates_inspector.clone());
        let admin_dialog_debounce = Arc::clone(&dialog_debounce);
        let admin_queue_handler = Arc::new(updates::BoxedUpdateHandler::new(Arc::new(
            admin::AdminQueueCommandUpdateHandler::new(
                admin::AdminQueueRuntimeConfig::new(admin_queue_config_from_app_config(config))
                    .with_taskman(admin_taskman_inspector)
                    .with_dispatcher(admin_dispatcher_inspector)
                    .with_updates(admin_updates_inspector)
                    .with_dialog_debounce_len(Arc::new(move || admin_dialog_debounce.len())),
                Arc::new(admin::AdminDispatcherEffects::new(Arc::clone(
                    &dispatcher_queue_for_updates,
                ))),
                config.admins.admin_ids.clone(),
                bot_identity.username.clone(),
                admin_help_handler,
            ),
        )));
        let admin_vip_handler = Arc::new(payments::AdminVipCommandUpdateHandler::new(
            Arc::clone(&dispatcher_queue_for_updates),
            Arc::clone(&payment_store_for_updates),
            Arc::clone(&payment_runtime_effects),
            config.admins.admin_ids.clone(),
            bot_identity.username.clone(),
            admin_queue_handler,
        ));
        let diagnostics_handler = Arc::new(diagnostics::DiagnosticsCommandUpdateHandler::new(
            diagnostics::DiagnosticsBotIdentity {
                username: bot_identity.username.clone(),
            },
            Arc::new(diagnostics::DiagnosticsDispatcherEffects::new(
                Arc::clone(&dispatcher_queue_for_updates),
                telegram.clone(),
                service_clients.redis.client().clone(),
            )),
            admin_vip_handler,
        ));
        let help_handler = Arc::new(help::HelpCommandUpdateHandler::new(
            help::HelpBotIdentity {
                first_name: bot_identity.first_name.clone(),
                username: bot_identity.username.clone(),
                token: bot_key.to_owned(),
            },
            Arc::new(help::HelpDispatcherEffects::new(
                telegram.clone(),
                Arc::clone(&payment_handler),
                Arc::clone(&rich_outbox),
            )),
            diagnostics_handler,
        ));
        let left_member = Arc::new(members::LeftChatMemberUpdateHandler::new(
            Arc::clone(&chat_communication),
            bot_identity.id,
            help_handler,
        ));
        let member_state = Arc::new(members::ChatMemberStateUpdateHandler::new(
            Arc::clone(&control_queue_for_updates),
            chat_communication,
            bot_identity.id,
            left_member,
        ));
        let edited_effects = Arc::new(
            edited::TaskmanEditedMessageEffects::new(
                Arc::clone(&task_queue_for_updates),
                Arc::new(shared_task_queue.clone()),
            )
            .with_dialog_debounce(dialog_debounce),
        );
        let edited = Arc::new(edited::EditedMessageUpdateHandler::new(
            edited_effects,
            bot_user_from_get_me(&bot_identity),
            member_state,
        ));
        let gated_handler = Arc::new(message_gate::MessageGateUpdateHandler::new(
            Arc::clone(&rate_limit_policy),
            permission_policy_for_updates,
            Arc::new(service_clients.redis.blocked_chat_store()),
            bot_identity.username.clone(),
            edited,
        ));
        let typed_router = Arc::new(updates::TypedUpdateRouter::new(gated_handler));
        let handler = Arc::new(updates::UpdateHandlerWithHistory::new(
            Arc::clone(&history_store_for_updates),
            typed_router,
            bot_identity.id,
        ));
        let update_stage_tracker = Arc::new(updates_inspector.stage_tracker());
        let update_consumer_stop = worker_stops.subscribe(RuntimeWorkerPhase::Processor);
        let delivery_store = openplotva_storage::PostgresTelegramDeliveryStore::new(
            service_clients.critical_postgres.clone(),
        );
        let update_consumer_worker = tokio::spawn(async move {
            let report = updates::run_materialized_update_consumer_until(
                delivery_store,
                update_consumer_config,
                handler,
                update_stage_tracker,
                wait_for_runtime_stop(update_consumer_stop),
            )
            .await;
            tracing::info!(?report, "Postgres Telegram inbox consumer stopped");
        });
        worker_registry.register(
            "telegram-update-consumer",
            RuntimeWorkerPhase::Processor,
            update_consumer_worker,
        );
        readiness_checks.push(ReadinessCheck::ok(
            "telegram_update_consumer",
            format!(
                "Postgres Telegram inbox consumer started with {} workers and leased, fenced claims",
                update_consumer_config.worker_limit
            ),
        ));
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_update_consumer",
            "OPENPLOTVA_CONSUME_UPDATES=false",
        ));
    }

    readiness_checks.push(ReadinessCheck::ok(
        "outbound_dispatcher",
        "Telegram outbound dispatcher workers started",
    ));
    worker_registry.install(build_dispatcher_worker_group(
        DispatcherWorkerGroupInputs {
            history: history_store,
            telegram,
            rich: rich_api,
            ephemeral: ephemeral_store,
            rate_limits: Arc::clone(&rate_limit_policy),
            permissions: Arc::clone(&permission_policy),
            queue: Arc::clone(&dispatcher_queue),
            limiters: dispatcher_limiters,
            failure_ring: dispatch_failure_ring,
            routing_admin_reports: routing_admin_report_store,
        },
        worker_stops.as_ref(),
    ));
    workers.dispatcher = Some(DispatcherRuntime {
        queue: dispatcher_queue,
        persistence: dispatcher_persistence,
    });

    Ok(workers)
}

async fn shutdown_runtime_workers(workers: RuntimeWorkers) {
    let RuntimeWorkers {
        worker_registry,
        worker_stops,
        dispatcher,
        shared_task_queue,
        dialog_debounce,
        webhook_route: _,
        telegram,
        delete_webhook_on_shutdown,
        bot_username: _,
        bot_id: _,
        dispatcher_inspector: _,
        cache_inspector: _,
        taskman_inspector: _,
        memory_restart_trigger: _,
        llm_trace_buffer: _,
        llm_run_buffer: _,
        routing_event_buffer: _,
        routing_event_reporter: _,
        runtime_api_tls_public_key_pin: _,
        attempt_walker: _,
        router_handle: _,
        router_breakers: _,
        router_triggers: _,
        router_pools: _,
        openrouter_free_gate: _,
        router_runtime: _,
        dialog_worker_gauge: _,
        memory_card_cache: _,
        readiness_probes: _,
    } = workers;
    let started_at = Instant::now();
    let deadline = started_at + RUNTIME_WORKER_SHUTDOWN_TIMEOUT;
    let processor_deadline = started_at + RUNTIME_PROCESSOR_SHUTDOWN_TIMEOUT;
    let outbound_deadline = deadline - RUNTIME_PERSISTENCE_RESERVE;
    let persistence_deadline = deadline - RUNTIME_FINALIZATION_RESERVE;

    if let Some(dialog_debounce) = dialog_debounce {
        let stopped = dialog_debounce.stop_all();
        if stopped > 0 {
            tracing::info!(stopped, "stopped pending dialog debounce timers");
        }
    }

    let drained = shutdown_runtime_workers_before_persistence(
        worker_registry.as_ref(),
        worker_stops.as_ref(),
        processor_deadline,
        outbound_deadline,
    )
    .await;
    if drained {
        if let Some(dispatcher) = dispatcher {
            persist_dispatcher_queue_on_shutdown(dispatcher, persistence_deadline).await;
        }
        if let Some(shared_task_queue) = shared_task_queue {
            persist_shared_task_queue_on_shutdown(shared_task_queue, persistence_deadline).await;
        }
        worker_stops.signal(RuntimeWorkerPhase::Ancillary);
        if !await_runtime_worker_phase_until(
            worker_registry.as_ref(),
            RuntimeWorkerPhase::Ancillary,
            deadline,
        )
        .await
        {
            abort_runtime_workers(worker_registry.take_all());
        }
    } else {
        tracing::warn!(
            "runtime workers did not drain before the shutdown deadline; skipped queue persistence"
        );
    }

    if delete_webhook_on_shutdown {
        match telegram {
            Some(telegram) => {
                match timeout_at(
                    deadline,
                    delete_webhook_on_shutdown_if_enabled(true, &telegram),
                )
                .await
                {
                    Ok(WebhookShutdownCleanupReport::Deleted) => {
                        tracing::debug!("Telegram webhook configuration deleted during shutdown");
                    }
                    Ok(WebhookShutdownCleanupReport::Failed { error }) => {
                        tracing::warn!(%error, "failed to delete Telegram webhook during shutdown");
                    }
                    Ok(WebhookShutdownCleanupReport::TimedOut) | Err(_) => {
                        tracing::warn!("timed out deleting Telegram webhook during shutdown");
                    }
                    Ok(WebhookShutdownCleanupReport::SkippedDisabled) => {}
                }
            }
            None => {
                tracing::warn!("Telegram webhook cleanup was requested without a Telegram client");
            }
        }
    }
}

async fn shutdown_runtime_workers_before_persistence(
    registry: &RuntimeWorkerRegistry,
    stops: &RuntimeWorkerStops,
    processor_deadline: Instant,
    outbound_deadline: Instant,
) -> bool {
    for phase in [RuntimeWorkerPhase::Server, RuntimeWorkerPhase::Ingress] {
        stops.signal(phase);
    }
    for phase in [RuntimeWorkerPhase::Server, RuntimeWorkerPhase::Ingress] {
        if !await_runtime_worker_phase_until(registry, phase, processor_deadline).await {
            abort_runtime_workers(registry.take_all());
            return false;
        }
    }

    stops.signal(RuntimeWorkerPhase::Processor);
    if !await_runtime_worker_phase_until(
        registry,
        RuntimeWorkerPhase::Processor,
        processor_deadline,
    )
    .await
    {
        abort_runtime_workers(registry.take_all());
        return false;
    }

    stops.signal(RuntimeWorkerPhase::Outbound);
    if !await_runtime_worker_phase_until(registry, RuntimeWorkerPhase::Outbound, outbound_deadline)
        .await
    {
        abort_runtime_workers(registry.take_all());
        return false;
    }

    true
}

async fn await_runtime_worker_phase_until(
    registry: &RuntimeWorkerRegistry,
    phase: RuntimeWorkerPhase,
    deadline: Instant,
) -> bool {
    let mut workers = registry.take_phase(phase).into_iter();
    while let Some(NamedRuntimeWorker {
        name,
        phase: _,
        mut handle,
    }) = workers.next()
    {
        match timeout_at(deadline, &mut handle).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) if error.is_cancelled() => {}
            Ok(Err(error)) => {
                tracing::warn!(worker = %name, %error, "runtime worker stopped with an error");
            }
            Err(_) => {
                tracing::warn!(worker = %name, "runtime worker exceeded the shutdown deadline");
                handle.abort();
                abort_runtime_workers(workers);
                return false;
            }
        }
    }
    true
}

fn abort_runtime_workers(workers: impl IntoIterator<Item = NamedRuntimeWorker>) {
    for worker in workers {
        tracing::warn!(worker = %worker.name, "aborting runtime worker after shutdown deadline");
        worker.handle.abort();
    }
}

async fn persist_dispatcher_queue_on_shutdown(dispatcher: DispatcherRuntime, deadline: Instant) {
    let save_result = timeout_at(
        deadline,
        dispatcher
            .persistence
            .save_queue_on_shutdown(&dispatcher.queue),
    )
    .await;

    match save_result {
        Ok(Ok(queue)) => {
            if !queue.items.is_empty() || queue.skipped > 0 {
                tracing::info!(
                    saved = queue.items.len(),
                    skipped = queue.skipped,
                    key = dispatcher.persistence.key(),
                    "saved outbound dispatcher queue during shutdown"
                );
            }
        }
        Ok(Err(error)) => {
            tracing::warn!(%error, "failed to save outbound dispatcher queue during shutdown");
        }
        Err(_) => {
            tracing::warn!(
                timeout_ms = openplotva_telegram::DEFAULT_DISPATCHER_SHUTDOWN_TIMEOUT.as_millis(),
                "timed out saving outbound dispatcher queue during shutdown"
            );
        }
    }
}

async fn persist_shared_task_queue_on_shutdown(
    queue: task_queue::SharedTaskQueueRuntime,
    deadline: Instant,
) {
    let save_result = timeout_at(deadline, async { queue.flush_dirty().await }).await;

    match save_result {
        Ok(Ok(flushed)) => {
            tracing::info!(flushed, "flushed shared taskman queue during shutdown");
        }
        Ok(Err(error)) => {
            tracing::warn!(%error, "failed to flush shared taskman queue during shutdown");
        }
        Err(_) => {
            tracing::warn!(
                timeout_ms = task_queue::SHARED_TASK_QUEUE_SHUTDOWN_TIMEOUT.as_millis(),
                "timed out flushing shared taskman queue during shutdown"
            );
        }
    }
}

/// Wrap a dialog provider with the WhiteCircle pre-tool safety gate, sharing
/// the caller's check-event recorder.
fn wrap_dialog_provider_with_white_circle(
    provider: openplotva_llm::ChatProviderHandle,
    config: &AppConfig,
    recorder: Arc<dyn openplotva_llm::whitecircle::WhiteCircleCheckEventRecorder>,
) -> openplotva_llm::ChatProviderHandle {
    Arc::new(
        openplotva_llm::whitecircle::WhiteCirclePreToolChatProvider::new(
            provider,
            openplotva_llm::whitecircle::WhiteCircleClient::new(
                dialog_runtime::white_circle_client_config_from_app_config(config),
            ),
            Some(recorder),
            dialog_runtime::white_circle_pre_tool_config_from_app_config(config),
        ),
    )
}

async fn wait_for_runtime_stop(mut stop: watch::Receiver<bool>) {
    loop {
        if *stop.borrow() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}

/// The message gate already ran upstream, so the rates command may always answer.
#[derive(Clone, Copy, Debug)]
struct MessageGateCheckedRatesPermission;

impl rates::RatesSendPermission for MessageGateCheckedRatesPermission {
    fn can_send_rates_text(&self, _chat: &carapax::types::Chat) -> bool {
        true
    }
}

#[derive(Clone, Copy, Debug)]
struct RuntimeRatesHeaderProvider;

impl rates::RatesHeaderProvider for RuntimeRatesHeaderProvider {
    fn rates_header(&self, user_full_name: &str) -> String {
        rates::random_rates_header(user_full_name)
    }
}

fn bot_user_from_get_me(bot: &carapax::types::Bot) -> carapax::types::User {
    let mut user = carapax::types::User::new(bot.id, bot.first_name.clone(), true);
    user.username = Some(bot.username.clone().into());
    user.last_name = bot.last_name.clone();
    user
}

#[derive(Clone, Debug, Default)]
struct RuntimeUnhandledUpdateHandler;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
enum RuntimeUnhandledUpdateError {
    #[error("unported fetcher route for {update_name}")]
    Unported { update_name: &'static str },
}

impl updates::UpdateHandler for RuntimeUnhandledUpdateHandler {
    type Error = RuntimeUnhandledUpdateError;

    async fn handle_update(&self, update: carapax::types::Update) -> Result<(), Self::Error> {
        if matches!(&update.update_type, carapax::types::UpdateType::Message(_)) {
            tracing::debug!(
                update_id = update.id,
                "residual message update consumed at quiet terminal"
            );
            return Ok(());
        }
        Err(RuntimeUnhandledUpdateError::Unported {
            update_name: openplotva_updates::update_name(&update),
        })
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(%error, "failed to install Ctrl-C handler");
        }
    };

    #[cfg(unix)]
    {
        let terminate = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut signal) => {
                    signal.recv().await;
                }
                Err(error) => {
                    tracing::warn!(%error, "failed to install SIGTERM handler");
                }
            }
        };

        tokio::select! {
            _ = ctrl_c => {}
            _ = terminate => {}
        }
    }

    #[cfg(not(unix))]
    ctrl_c.await;

    tracing::info!("shutdown signal received");
}

#[cfg(test)]
#[path = "../../openplotva-telegram/src/test_api.rs"]
mod test_bot_api;
#[cfg(test)]
mod tests {
    use crate::http::assets::{found, static_web_asset_response};
    use crate::http::auth::{
        admin_auth_check, admin_auth_date_is_fresh, admin_auth_response, admin_auth_user_state,
        admin_bootstrap, admin_session_is_authorized, admin_static_asset_requires_auth,
    };
    use openplotva_storage::database as sqlx;

    use crate::http::common::parse_memory_limit;
    use crate::http::settings::{
        apply_private_chat_response_defaults, authenticate_settings_init_data,
        build_deputy_display_name, chat_list_title, chat_member_can_manage_settings,
        chat_member_record_from_upsert, current_unix_timestamp, new_settings_response,
        normalize_deputy_ids, parse_deputy_candidates_limit, parse_deputy_update_request,
        parse_optional_i32, parse_settings_get_access, parse_settings_memory_access,
        parse_settings_update_request, settings_chat_display, settings_chat_full_info_type_name,
        settings_chat_state_from_full_info, settings_get_response, settings_reply_flags,
        stored_chat_member_is_fresh,
    };

    use std::{
        collections::BTreeMap,
        error::Error,
        fmt,
        sync::{Arc, Mutex, MutexGuard},
        time::Duration,
    };

    #[tokio::test]
    async fn runtime_worker_readiness_names_an_early_exit() {
        let registry = Arc::new(super::RuntimeWorkerRegistry::default());
        registry.register(
            "early-exit-worker",
            super::RuntimeWorkerPhase::Ancillary,
            tokio::spawn(async {}),
        );
        tokio::task::yield_now().await;

        let readiness = registry.readiness_check();

        assert_eq!(readiness.status, "error");
        assert!(readiness.detail.contains("early-exit-worker"));
    }

    #[tokio::test]
    async fn runtime_worker_readiness_names_a_panicked_worker() {
        let registry = Arc::new(super::RuntimeWorkerRegistry::default());
        registry.register(
            "panicked-worker",
            super::RuntimeWorkerPhase::Ancillary,
            tokio::spawn(async { panic!("expected worker panic") }),
        );
        tokio::task::yield_now().await;

        let readiness = registry.readiness_check();

        assert_eq!(readiness.status, "error");
        assert!(readiness.detail.contains("panicked-worker"));
    }

    #[tokio::test]
    async fn dropping_runtime_worker_registry_cancels_registered_workers() {
        struct DropSignal(Arc<std::sync::atomic::AtomicBool>);

        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let started = Arc::new(tokio::sync::Notify::new());
        let registry = super::RuntimeWorkerRegistry::default();
        registry.register(
            "startup-worker",
            super::RuntimeWorkerPhase::Ancillary,
            tokio::spawn({
                let cancelled = Arc::clone(&cancelled);
                let started = Arc::clone(&started);
                async move {
                    let _drop_signal = DropSignal(cancelled);
                    started.notify_one();
                    std::future::pending::<()>().await;
                }
            }),
        );
        started.notified().await;

        drop(registry);
        tokio::task::yield_now().await;

        assert!(cancelled.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn runtime_worker_registry_installs_an_owned_worker_group() {
        let registry = super::RuntimeWorkerRegistry::default();
        let mut group = super::RuntimeWorkerGroup::new(super::RuntimeWorkerPhase::Processor);
        group.register("group-worker", tokio::spawn(async {}));

        registry.install(group);
        tokio::task::yield_now().await;

        let readiness = registry.readiness_check();
        assert_eq!(readiness.status, "error");
        assert!(readiness.detail.contains("group-worker"));
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_worker_shutdown_uses_one_deadline_for_forty_eight_hung_tasks() {
        let registry = Arc::new(super::RuntimeWorkerRegistry::default());
        for index in 0..48 {
            registry.register(
                format!("hung-worker-{index}"),
                super::RuntimeWorkerPhase::Processor,
                tokio::spawn(std::future::pending()),
            );
        }
        let timeout = Duration::from_secs(10);
        let started_at = tokio::time::Instant::now();

        let completed = super::await_runtime_worker_phase_until(
            &registry,
            super::RuntimeWorkerPhase::Processor,
            started_at + timeout,
        )
        .await;

        assert!(!completed);
        assert_eq!(tokio::time::Instant::now() - started_at, timeout);
    }

    #[tokio::test]
    async fn runtime_worker_shutdown_drains_outbound_after_processors() {
        let registry = Arc::new(super::RuntimeWorkerRegistry::default());
        let stops = super::RuntimeWorkerStops::new();
        let order = Arc::new(Mutex::new(Vec::new()));
        for (name, phase) in [
            ("ingress", super::RuntimeWorkerPhase::Ingress),
            ("processor", super::RuntimeWorkerPhase::Processor),
            ("outbound", super::RuntimeWorkerPhase::Outbound),
        ] {
            let stop = stops.subscribe(phase);
            let order = Arc::clone(&order);
            registry.register(
                name,
                phase,
                tokio::spawn(async move {
                    super::wait_for_runtime_stop(stop).await;
                    order
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(name);
                }),
            );
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let completed = super::shutdown_runtime_workers_before_persistence(
            &registry, &stops, deadline, deadline,
        )
        .await;
        order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push("persistence");

        assert!(completed);
        assert_eq!(
            *order
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            ["ingress", "processor", "outbound", "persistence"]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_worker_shutdown_reserves_a_later_deadline_for_outbound() {
        let registry = Arc::new(super::RuntimeWorkerRegistry::default());
        let stops = super::RuntimeWorkerStops::new();
        for (name, phase, drain_time) in [
            (
                "processor",
                super::RuntimeWorkerPhase::Processor,
                Duration::from_secs(1),
            ),
            (
                "outbound",
                super::RuntimeWorkerPhase::Outbound,
                Duration::from_secs(2),
            ),
        ] {
            let stop = stops.subscribe(phase);
            registry.register(
                name,
                phase,
                tokio::spawn(async move {
                    super::wait_for_runtime_stop(stop).await;
                    tokio::time::sleep(drain_time).await;
                }),
            );
        }
        let started_at = tokio::time::Instant::now();

        let completed = super::shutdown_runtime_workers_before_persistence(
            &registry,
            &stops,
            started_at + Duration::from_millis(1_500),
            started_at + Duration::from_secs(4),
        )
        .await;

        assert!(completed);
        assert_eq!(
            tokio::time::Instant::now() - started_at,
            Duration::from_secs(3)
        );
    }

    #[test]
    fn postgres_readiness_only_fails_at_full_active_capacity() {
        assert!(!super::postgres_capacity_is_exhausted(7, 8, 0));
        assert!(!super::postgres_capacity_is_exhausted(8, 8, 1));
        assert!(super::postgres_capacity_is_exhausted(8, 8, 0));
    }

    #[test]
    fn durable_update_consumer_caps_backlog_fanout() {
        assert_eq!(super::durable_update_consumer_worker_limit(96), 24);
    }

    #[test]
    fn durable_update_consumer_preserves_smaller_worker_limits() {
        assert_eq!(super::durable_update_consumer_worker_limit(8), 8);
    }

    #[test]
    fn telegram_outbox_readiness_fails_before_worker_start() {
        let metrics = super::telegram_outbox::TelegramOutboxWorkerMetrics::default();

        assert_eq!(super::telegram_outbox_readiness(&metrics).status, "error");
    }

    #[test]
    fn image_worker_workflows_keep_free_vip_and_editor_slots_apart() {
        assert_eq!(
            super::REGULAR_IMAGE_GENERATION_WORKFLOW_KEY,
            super::image_jobs::IMAGE_GENERATION_BOOGU_TURBO_WORKFLOW_KEY
        );
        assert_eq!(
            super::VIP_IMAGE_GENERATION_WORKFLOW_KEYS,
            [
                super::image_jobs::IMAGE_GENERATION_FLUX_WORKFLOW_KEY,
                super::image_jobs::IMAGE_GENERATION_MOSS_WORKFLOW_KEY,
                super::image_jobs::IMAGE_GENERATION_FIBO_WORKFLOW_KEY,
            ]
        );
        assert_eq!(
            super::VIP_IMAGE_EDITOR_WORKFLOW_KEY,
            super::image_jobs::IMAGE_EDIT_FLUX_WORKFLOW_KEY
        );
    }

    #[tokio::test]
    async fn admin_user_delete_clears_pending_projections_before_they_can_flush()
    -> Result<(), Box<dyn Error>> {
        let Ok(postgres_dsn) = std::env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let bot_id = i64::try_from(suffix % 1_000_000_000)? + 8_000_000_000;
        let user_id = bot_id + 1_000_000_000;
        let chat_id = -(bot_id + 2_000_000_000);
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&postgres_dsn)
            .await?;
        openplotva_storage::run_migrations_on(&pool).await?;

        sqlx::query("INSERT INTO users (id, first_name) VALUES ($1, 'Delete me')")
            .bind(user_id)
            .execute(&pool)
            .await?;
        sqlx::query("INSERT INTO chats (id, type) VALUES ($1, 'supergroup')")
            .bind(chat_id)
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT INTO chat_members (chat_id, user_id, status) VALUES ($1, $2, 'member')",
        )
        .bind(chat_id)
        .bind(user_id)
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO telegram_users_stage \
             (bot_id, user_id, first_name, observed_at, stream_ms, stream_seq) \
             VALUES ($1, $2, 'Staged name', CURRENT_TIMESTAMP, 1, 0)",
        )
        .bind(bot_id)
        .bind(user_id)
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO telegram_chat_members_stage \
             (bot_id, chat_id, user_id, status, observed_at, stream_ms, stream_seq) \
             VALUES ($1, $2, $3, 'administrator', CURRENT_TIMESTAMP, 1, 0)",
        )
        .bind(bot_id)
        .bind(chat_id)
        .bind(user_id)
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO telegram_activity_stage \
             (bot_id, chat_id, user_id, last_message_at, last_active_at, observed_at, stream_ms, stream_seq) \
             VALUES ($1, $2, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1, 0)",
        )
        .bind(bot_id)
        .bind(chat_id)
        .bind(user_id)
        .execute(&pool)
        .await?;

        openplotva_storage::entities::PostgresEntityStore::new(pool.clone())
            .delete_user(user_id)
            .await?;

        let remaining: (i64, i64, i64) = sqlx::query_as(
            "SELECT \
             (SELECT count(*) FROM telegram_users_stage WHERE user_id = $1), \
             (SELECT count(*) FROM telegram_chat_members_stage WHERE user_id = $1), \
             (SELECT count(*) FROM telegram_activity_stage WHERE user_id = $1)",
        )
        .bind(user_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(remaining, (0, 0, 0));
        assert!(
            !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM telegram_users_effective WHERE id = $1)",
            )
            .bind(user_id)
            .fetch_one(&pool)
            .await?
        );

        let flush = openplotva_storage::PostgresTelegramProjectionStore::new(pool.clone())
            .flush_staged_projections(bot_id, usize::MAX)
            .await?;
        assert_eq!(flush.users, 0);
        assert_eq!(flush.members, 0);
        assert_eq!(flush.member_activity, 0);
        assert_eq!(flush.active_users, 0);
        assert!(
            !sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)")
                .bind(user_id)
                .fetch_one(&pool)
                .await?
        );

        sqlx::query("DELETE FROM chats WHERE id = $1")
            .bind(chat_id)
            .execute(&pool)
            .await?;
        Ok(())
    }

    mod routing_admin_inputs {
        use serde_json::json;

        use crate::http::routing::{
            model_input_from_json, pool_input_from_json, provider_input_from_json,
            routing_param_descriptors,
        };

        #[test]
        fn provider_input_accepts_typed_protocol_and_hint() {
            let input = provider_input_from_json(&json!({
                "name": "my-sglang",
                "kind": "chat",
                "protocol": "openai_compat",
                "runtime_hint": "sglang",
                "endpoint": "https://sglang.local/v1",
                "api_key_ref": "MY_KEY",
            }))
            .expect("valid provider input");
            assert_eq!(input.protocol.as_deref(), Some("openai_compat"));
            assert_eq!(input.runtime_hint.as_deref(), Some("sglang"));
        }

        #[test]
        fn provider_input_rejects_unknown_protocol_and_hint() {
            let error = provider_input_from_json(&json!({
                "name": "x",
                "protocol": "grpc",
            }))
            .expect_err("unknown protocol must be rejected");
            assert!(error.contains("unknown protocol"));

            let error = provider_input_from_json(&json!({
                "name": "x",
                "runtime_hint": "llamacpp",
            }))
            .expect_err("unknown runtime hint must be rejected");
            assert!(error.contains("unknown runtime_hint"));
        }

        #[test]
        fn provider_input_validates_config_against_schema() {
            let error = provider_input_from_json(&json!({
                "name": "x",
                "protocol": "openai_compat",
                "config": { "timeout_ms": 0 },
            }))
            .expect_err("zero timeout must be rejected");
            assert!(error.contains("timeout_ms"));
        }

        #[test]
        fn model_input_parses_pool_id_and_validates_config() {
            let input = model_input_from_json(&json!({
                "provider_id": 5,
                "model_name": "qwen3.6-35b-a3b",
                "pool_id": 7,
                "config": { "temperature": 0.4 },
            }))
            .expect("valid model input");
            assert_eq!(input.pool_id, Some(7));

            let error = model_input_from_json(&json!({
                "provider_id": 5,
                "model_name": "x",
                "config": { "temperature": 9.0 },
            }))
            .expect_err("out-of-range temperature must be rejected");
            assert!(error.contains("temperature"));
        }

        #[test]
        fn pool_input_parses_config_as_json_object() {
            let input = pool_input_from_json(&json!({
                "name": "openrouter-free",
                "max_concurrency": 1,
                "description": "free pool",
                "config": { "managed_by": "openrouter_free_pool" },
            }))
            .expect("valid pool input");
            assert_eq!(input.config["managed_by"], json!("openrouter_free_pool"));

            let error = pool_input_from_json(&json!({
                "name": "openrouter-free",
                "config": "not an object",
            }))
            .expect_err("scalar pool config must be rejected");
            assert!(error.contains("config"));
        }

        #[test]
        fn param_descriptors_cover_every_protocol() {
            let descriptors = routing_param_descriptors();
            let list = descriptors.as_array().expect("array");
            assert!(list.len() >= 6, "one descriptor per protocol at minimum");
            assert!(
                list.iter()
                    .any(|entry| entry["runtime_hint"] == json!("sglang"))
            );
        }
    }

    use crate::http::common::{
        admin_i64_from_json, admin_non_empty_string, parse_admin_non_negative_i32,
        parse_admin_optional_bool, parse_admin_positive_i32,
        query_values as admin_auth_query_values,
    };
    use crate::http::routing::routing_json_config_patch;
    use crate::runtime_redis::{
        runtime_redis_prefix_groups_from_keys, runtime_redis_value_from_bytes,
    };
    use axum::{
        body::{Bytes, to_bytes},
        http::{HeaderMap, Method, StatusCode, header},
    };
    use carapax::types::{
        InputFile, ReplyParameters, SendMessage, SendPhoto, Update as TelegramUpdate,
    };
    use openplotva_core::{ChatSettings, ChatSettingsUpdate};
    use openplotva_storage::redis_inspector::prefix_groups_from_keys as admin_redis_prefix_groups_from_keys;
    use openplotva_telegram::{
        DispatcherConfig, DispatcherMessage, DispatcherQueue, DispatcherSendStatus,
        DispatcherWorkItem, MessageFingerprint, TelegramMessage, TelegramOutboundExecuteError,
        TelegramOutboundMethod, TelegramOutboundResponse,
    };
    use serde_json::json;
    use time::{OffsetDateTime, format_description::well_known::Rfc3339};

    use super::{
        ADMIN_API_ROUTE_PATTERNS, DispatchFailureRing, RuntimeUnhandledUpdateHandler,
        WebhookShutdownCleanupReport, configure_telegram_bot_commands,
        delete_webhook_on_shutdown_if_enabled, dialog_memory_context_enabled,
        runtime_api_graphql_snapshot, shield_history_tail_messages_from_config,
        shield_options_from_config, static_web_routes, telegram_webhook_response, virtual_messages,
    };
    use crate::runtime_dispatcher::{
        DISPATCHER_DEBOUNCE_CACHE_SIZE, DISPATCHER_DEBOUNCE_WINDOW, DISPATCHER_MAX_QUEUE_SIZE,
        dispatcher_config, send_dispatcher_work_item_with_transport,
        send_dispatcher_work_item_with_transport_and_history,
    };

    use crate::http::observability::{
        admin_gradius_opportunities_filter, admin_llm_analytics_summary_json,
        admin_loglevel_response, admin_safety_check_json, admin_safety_checks_filter, admin_state,
        admin_state_response,
    };

    use crate::http::taskman::{
        admin_taskman_job_cancel_response, admin_taskman_job_response,
        admin_taskman_job_restart_response, admin_taskman_jobs_clear_response,
        admin_taskman_jobs_filter, admin_taskman_jobs_response,
    };

    use crate::http::shield::{
        admin_shield_category_matched, admin_shield_document_input, admin_shield_document_json,
        admin_shield_documents_response, admin_shield_embeddings_rebuild_response,
        admin_shield_test_json, admin_shield_test_response, parse_shield_limit,
    };

    use crate::http::memory::{
        AdminMemoryOverride, admin_memory_cards_response, admin_memory_resolve_override,
        admin_memory_restart_override, admin_memory_restart_response, admin_memory_runs_response,
    };

    use crate::http::entities::{
        admin_chat_get_response, admin_chats_search_by_member_response,
        admin_user_grant_vip_response, admin_vip_summary_json,
    };
    use crate::permissions::{
        ChatPermissionContext, ChatPermissionPolicy, ChatPermissionStore, ChatPermissionStoreFuture,
    };
    use crate::rate_limits::{ChatRateLimitPolicy, RateLimitStore, RateLimitStoreFuture};
    use crate::updates::UpdateHandler;

    #[test]
    fn dispatcher_config_matches_server_runtime_defaults() {
        let config = dispatcher_config();

        assert_eq!(config.max_queue_size, DISPATCHER_MAX_QUEUE_SIZE);
        assert!(config.dedupe_config.enabled);
        assert_eq!(
            config.dedupe_config.default_window,
            DISPATCHER_DEBOUNCE_WINDOW
        );
        assert_eq!(
            config.dedupe_config.max_cache_size,
            DISPATCHER_DEBOUNCE_CACHE_SIZE
        );
    }

    #[tokio::test]
    async fn dispatcher_stop_mid_send_accounts_for_sent_and_persisted_items()
    -> Result<(), Box<dyn Error>> {
        let queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        for (virtual_id, content_hash) in [("sending", 1), ("queued", 2)] {
            queue.enqueue(
                DispatcherMessage::new(
                    MessageFingerprint {
                        chat_id: 42,
                        message_type: "test".to_owned(),
                        content_hash,
                        debounce_key: None,
                    },
                    virtual_id,
                )
                .with_method(TelegramOutboundMethod::from(SendMessage::new(
                    42, virtual_id,
                ))),
                true,
            );
        }
        let send_started = Arc::new(tokio::sync::Notify::new());
        let release_send = Arc::new(tokio::sync::Notify::new());
        let (stop, stop_rx) = tokio::sync::watch::channel(false);
        let worker = tokio::spawn({
            let queue = Arc::clone(&queue);
            let send_started = Arc::clone(&send_started);
            let release_send = Arc::clone(&release_send);
            async move {
                queue
                    .run_immediate_worker_until(super::wait_for_runtime_stop(stop_rx), move |_| {
                        let send_started = Arc::clone(&send_started);
                        let release_send = Arc::clone(&release_send);
                        async move {
                            send_started.notify_one();
                            release_send.notified().await;
                            DispatcherSendStatus::Sent
                        }
                    })
                    .await
            }
        });

        send_started.notified().await;
        stop.send(true)?;
        release_send.notify_one();
        worker.await?;

        assert_eq!(queue.stats().processed_total, 1);
        let persisted =
            openplotva_telegram::persistent_queue_from_drain(queue.drain_for_shutdown(), 10)?;
        assert_eq!(persisted.items.len(), 1);
        assert_eq!(persisted.items[0].virtual_id, "queued");
        Ok(())
    }

    #[test]
    fn routing_config_patch_accepts_object_only() {
        let patch = routing_json_config_patch(&json!({
            "config": {
                "temperature": 0.2,
                "enable_thinking": false
            }
        }))
        .expect("config patch");

        assert_eq!(patch["temperature"], json!(0.2));
        assert_eq!(patch["enable_thinking"], json!(false));
    }

    #[test]
    fn routing_config_patch_rejects_non_object_payloads() {
        assert!(routing_json_config_patch(&json!({"config": ["bad"]})).is_err());
        assert!(routing_json_config_patch(&json!({})).is_err());
    }

    #[test]
    fn dialog_memory_runtime_wiring_follows_go_memory_service_gate() -> Result<(), Box<dyn Error>> {
        let disabled = openplotva_config::AppConfig::from_raw(openplotva_config::RawConfig {
            memory_enabled: Some("false".to_owned()),
            ..openplotva_config::RawConfig::default()
        })?;
        let enabled = openplotva_config::AppConfig::from_raw(openplotva_config::RawConfig {
            memory_enabled: Some("true".to_owned()),
            ..openplotva_config::RawConfig::default()
        })?;

        assert!(!dialog_memory_context_enabled(&disabled));
        assert!(dialog_memory_context_enabled(&enabled));
        Ok(())
    }

    #[test]
    fn runtime_snapshot_and_shield_options_preserve_go_shield_config() -> Result<(), Box<dyn Error>>
    {
        let config = openplotva_config::AppConfig::from_raw(openplotva_config::RawConfig {
            shield_enabled: Some("true".to_owned()),
            shield_embedder_service_name: Some("shield-svc".to_owned()),
            shield_embedding_dim: Some("256".to_owned()),
            shield_max_matches: Some("5".to_owned()),
            shield_vector_min_score: Some("0.51".to_owned()),
            shield_lexical_min_score: Some("0.09".to_owned()),
            shield_query_max_chars: Some("1234".to_owned()),
            shield_retrieval_timeout_seconds: Some("7".to_owned()),
            shield_history_tail_messages: Some("3".to_owned()),
            vision_discovery_service_name: Some("vision-service".to_owned()),
            vision_discovery_endpoint_name: Some("vision-endpoint".to_owned()),
            vision_model: Some("vision-model".to_owned()),
            vision_max_tokens: Some("333".to_owned()),
            vision_temperature: Some("0.2".to_owned()),
            vision_direct_image_limit: Some("4".to_owned()),
            vision_request_timeout_seconds: Some("88".to_owned()),
            acestep_enabled: Some("true".to_owned()),
            acestep_base_url: Some(" https://ace.test ".to_owned()),
            ..openplotva_config::RawConfig::default()
        })?;

        let options = shield_options_from_config(&config.shield);
        let snapshot = runtime_api_graphql_snapshot(&config);

        assert!(options.enabled);
        assert_eq!(options.embedding_dim, 256);
        assert_eq!(options.max_matches, 5);
        assert_eq!(options.vector_min_score, 0.51);
        assert_eq!(options.lexical_min_score, 0.09);
        assert_eq!(options.query_max_chars, 1234);
        assert_eq!(options.retrieval_timeout_seconds, 7);
        assert_eq!(shield_history_tail_messages_from_config(&config.shield), 3);
        assert!(snapshot.shield_enabled);
        assert_eq!(snapshot.shield_embedder_url.as_deref(), Some("shield-svc"));
        assert_eq!(snapshot.shield_max_matches, 5);
        assert_eq!(snapshot.shield_vector_min_score, 0.51);
        assert_eq!(snapshot.shield_lexical_min_score, 0.09);
        assert_eq!(snapshot.shield_retrieval_timeout_seconds, 7);
        assert_eq!(snapshot.shield_history_tail_messages, 3);
        assert_eq!(snapshot.vision_discovery_service_name, "vision-service");
        assert_eq!(snapshot.vision_discovery_endpoint_name, "vision-endpoint");
        assert_eq!(snapshot.vision_model, "vision-model");
        assert_eq!(snapshot.vision_max_tokens, 333);
        assert_eq!(snapshot.vision_temperature, 0.2);
        assert_eq!(snapshot.vision_direct_image_limit, 4);
        assert_eq!(snapshot.vision_request_timeout_seconds, 88);
        assert!(snapshot.ace_step_enabled);
        assert_eq!(
            snapshot.ace_step_base_url.as_deref(),
            Some("https://ace.test")
        );
        assert_eq!(snapshot.active_draw_providers, vec!["drawapi".to_owned()]);
        Ok(())
    }

    #[test]
    fn runtime_redis_prefix_groups_match_go_segment_rules() {
        let groups = runtime_redis_prefix_groups_from_keys(
            "plotva:",
            [
                "plotva:updates:queue".to_owned(),
                "plotva:updates:dead".to_owned(),
                "plotva:message_queue".to_owned(),
                "plotva:".to_owned(),
                "other:key".to_owned(),
            ],
        );

        assert_eq!(
            groups,
            vec![
                openplotva_server::RuntimeRedisPrefixGroup {
                    prefix: "plotva:message_queue".to_owned(),
                    count: 1,
                },
                openplotva_server::RuntimeRedisPrefixGroup {
                    prefix: "plotva:updates:".to_owned(),
                    count: 2,
                },
            ]
        );
    }

    #[test]
    fn runtime_redis_value_truncates_by_bytes_like_go_limit() {
        assert_eq!(
            runtime_redis_value_from_bytes("key", b"abcdef", 3),
            openplotva_server::RuntimeRedisValue {
                key: "key".to_owned(),
                value: "abc".to_owned(),
                truncated: true,
            }
        );
        assert_eq!(
            runtime_redis_value_from_bytes("key", b"abc", 64 * 1024),
            openplotva_server::RuntimeRedisValue {
                key: "key".to_owned(),
                value: "abc".to_owned(),
                truncated: false,
            }
        );
    }

    #[tokio::test]
    async fn runtime_unhandled_update_handler_consumes_residual_messages_like_go()
    -> Result<(), Box<dyn Error>> {
        RuntimeUnhandledUpdateHandler
            .handle_update(sample_message_update(700)?)
            .await
            .expect("residual message terminal should be a quiet no-op");
        Ok(())
    }

    #[tokio::test]
    async fn dispatcher_send_checks_permissions_before_telegram_transport()
    -> Result<(), Box<dyn Error>> {
        let rate_limits = Arc::new(ChatRateLimitPolicy::new(RateLimitStoreStub));
        let permission_store = PermissionStoreStub::with_context(ChatPermissionContext {
            chat_type: Some("supergroup".to_owned()),
            settings: Some(ChatSettings {
                enable_global_text_reply: false,
                ..ChatSettings::defaults(42)
            }),
        });
        let permissions = Arc::new(ChatPermissionPolicy::new(permission_store));
        let item = queued_method_item(TelegramOutboundMethod::from(SendMessage::new(42, "hello")));
        let called = Arc::new(Mutex::new(false));
        let called_for_send = Arc::clone(&called);

        let status =
            send_dispatcher_work_item_with_transport(rate_limits, permissions, item, move |_| {
                *lock(&called_for_send) = true;
                async { Err::<TelegramOutboundResponse, _>(permission_error()) }
            })
            .await;

        assert_eq!(status, DispatcherSendStatus::Failed);
        assert!(!*lock(&called));
        Ok(())
    }

    #[tokio::test]
    async fn dispatcher_send_respects_go_bypass_chat_restrictions_flag()
    -> Result<(), Box<dyn Error>> {
        let rate_limits = Arc::new(ChatRateLimitPolicy::new(RateLimitStoreStub));
        let permission_store = PermissionStoreStub::with_context(ChatPermissionContext {
            chat_type: Some("supergroup".to_owned()),
            settings: Some(ChatSettings {
                enable_global_text_reply: false,
                ..ChatSettings::defaults(42)
            }),
        });
        let permissions = Arc::new(ChatPermissionPolicy::new(permission_store.clone()));
        let item =
            queued_bypass_method_item(TelegramOutboundMethod::from(SendMessage::new(42, "hello")));
        let called = Arc::new(Mutex::new(false));
        let called_for_send = Arc::clone(&called);

        let status =
            send_dispatcher_work_item_with_transport(rate_limits, permissions, item, move |_| {
                *lock(&called_for_send) = true;
                async {
                    Ok::<_, TelegramOutboundExecuteError>(TelegramOutboundResponse::Message(
                        Box::new(telegram_message(42, 100)),
                    ))
                }
            })
            .await;

        assert_eq!(status, DispatcherSendStatus::Sent);
        assert!(*lock(&called));
        assert!(permission_store.saved_updates().is_empty());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn dispatcher_send_retries_short_rate_limit_then_succeeds() -> Result<(), Box<dyn Error>>
    {
        let rate_limits = Arc::new(ChatRateLimitPolicy::new(RateLimitStoreStub));
        let permission_store = PermissionStoreStub::with_context(ChatPermissionContext {
            chat_type: Some("supergroup".to_owned()),
            settings: Some(ChatSettings::defaults(42)),
        });
        let permissions = Arc::new(ChatPermissionPolicy::new(permission_store));
        let item = queued_method_item(TelegramOutboundMethod::from(SendMessage::new(42, "hello")));
        let calls = Arc::new(Mutex::new(0u32));
        let calls_for_send = Arc::clone(&calls);

        let status =
            send_dispatcher_work_item_with_transport(rate_limits, permissions, item, move |_| {
                let attempt = {
                    let mut calls = lock(&calls_for_send);
                    *calls += 1;
                    *calls
                };
                async move {
                    if attempt == 1 {
                        Err(openplotva_telegram::TelegramOutboundExecuteError::from(
                            openplotva_telegram::RichApiError::Api {
                                code: 429,
                                description: "Too Many Requests: retry after 1".to_owned(),
                                retry_after: Some(1),
                            },
                        ))
                    } else {
                        Ok(TelegramOutboundResponse::Message(Box::new(
                            telegram_message(42, 100),
                        )))
                    }
                }
            })
            .await;

        assert_eq!(status, DispatcherSendStatus::Sent);
        assert_eq!(*lock(&calls), 2, "one inline retry after the short 429");
        Ok(())
    }

    #[tokio::test]
    async fn maintenance_dispatch_receipt_confirms_one_actual_send_after_replay()
    -> Result<(), Box<dyn Error>> {
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
        let store =
            openplotva_storage::maintenance_notifications::MaintenanceNotificationStore::new(
                pool.clone(),
            );
        let key = "e".repeat(64);
        store.create(&key, 42, "Synthetic PR ready").await?;
        store
            .claim_due(OffsetDateTime::now_utc() + time::Duration::seconds(1))
            .await?;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for _ in 0..2 {
            let queue = DispatcherQueue::new(DispatcherConfig::default());
            let digest = crate::routing_admin_reports::FormattedIncidentDigest {
                fingerprint: key.clone(),
                text: "Synthetic PR ready".into(),
                latest_occurrence: None,
                has_incidents: true,
            };
            let message = crate::routing_admin_reports::build_admin_report_dispatch(
                42,
                &digest,
                crate::routing_admin_reports::AdminReportDeliveryPlan::Send,
                &format!("maintenance-notification:{key}"),
            )?;
            queue.enqueue(message, true);
            let item = queue.dequeue_immediate().expect("notification queued");
            let rate_limits = Arc::new(ChatRateLimitPolicy::new(RateLimitStoreStub));
            let permissions = Arc::new(ChatPermissionPolicy::new(
                PermissionStoreStub::with_context(ChatPermissionContext {
                    chat_type: Some("private".into()),
                    settings: Some(ChatSettings::defaults(42)),
                }),
            ));
            let calls = Arc::clone(&calls);
            let status = send_dispatcher_work_item_with_transport_and_history(
                virtual_messages::NoopEditHistorySink,
                virtual_messages::NoopEphemeralMessageTracker,
                rate_limits,
                permissions,
                None,
                Some(
                    openplotva_storage::llm_routing::PostgresRoutingAdminReportStore::new(
                        pool.clone(),
                    ),
                ),
                item,
                move |_| {
                    calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    async {
                        Ok::<_, openplotva_telegram::TelegramOutboundExecuteError>(
                            TelegramOutboundResponse::Message(Box::new(telegram_message(42, 999))),
                        )
                    }
                },
            )
            .await;
            assert_eq!(status, DispatcherSendStatus::Sent);
        }
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
        let receipt = store.get(&key).await?.expect("receipt");
        assert_eq!(receipt.state, "sent");
        assert_eq!(receipt.telegram_message_id, Some(999));
        Ok(())
    }

    #[tokio::test]
    async fn dispatcher_send_records_terminal_failure_in_ring() -> Result<(), Box<dyn Error>> {
        let rate_limits = Arc::new(ChatRateLimitPolicy::new(RateLimitStoreStub));
        let permission_store = PermissionStoreStub::with_context(ChatPermissionContext {
            chat_type: Some("supergroup".to_owned()),
            settings: Some(ChatSettings::defaults(42)),
        });
        let permissions = Arc::new(ChatPermissionPolicy::new(permission_store));
        let ring = Arc::new(DispatchFailureRing::default());
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        queue.enqueue(
            DispatcherMessage::new(
                MessageFingerprint {
                    chat_id: 42,
                    message_type: "text".to_owned(),
                    content_hash: 7,
                    debounce_key: Some("r100".to_owned()),
                },
                "v-protected",
            )
            .with_method(TelegramOutboundMethod::from(SendMessage::new(42, "hello")))
            .with_protected(true),
            true,
        );
        let item = queue.dequeue_immediate().expect("queued work item");

        let status = send_dispatcher_work_item_with_transport_and_history(
            virtual_messages::NoopEditHistorySink,
            virtual_messages::NoopEphemeralMessageTracker,
            rate_limits,
            permissions,
            Some(Arc::clone(&ring)),
            None,
            item,
            |_| async { Err::<TelegramOutboundResponse, _>(permission_error()) },
        )
        .await;

        assert_eq!(status, DispatcherSendStatus::Failed);
        let failures = ring.snapshot(10);
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].virtual_id, "v-protected");
        assert_eq!(failures[0].chat_id, 42);
        assert_eq!(failures[0].method_kind, "SendMessage");
        assert_eq!(failures[0].class, "terminal_permission");
        assert!(failures[0].protected);
        assert_eq!(failures[0].reply_to_message_id, Some(100));
        Ok(())
    }

    #[tokio::test]
    async fn dispatcher_send_auto_disables_settings_after_permission_error()
    -> Result<(), Box<dyn Error>> {
        let rate_limits = Arc::new(ChatRateLimitPolicy::new(RateLimitStoreStub));
        let permission_store = PermissionStoreStub::with_context(ChatPermissionContext {
            chat_type: Some("supergroup".to_owned()),
            settings: Some(ChatSettings::defaults(42)),
        });
        let permissions = Arc::new(ChatPermissionPolicy::new(permission_store.clone()));
        let item = queued_method_item(TelegramOutboundMethod::from(SendPhoto::new(
            42,
            InputFile::file_id("photo-id"),
        )));

        let status =
            send_dispatcher_work_item_with_transport(rate_limits, permissions, item, |_| async {
                Err::<TelegramOutboundResponse, _>(permission_error())
            })
            .await;

        assert_eq!(status, DispatcherSendStatus::Failed);
        assert_eq!(
            permission_store.saved_updates(),
            vec![ChatSettingsUpdate {
                chat_id: 42,
                chat_type: "supergroup".to_owned(),
                enable_global_draw_reply: false,
                ..chat_settings_update_defaults(42)
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn dispatcher_send_retries_reply_missing_without_reply_target()
    -> Result<(), Box<dyn Error>> {
        let rate_limits = Arc::new(ChatRateLimitPolicy::new(RateLimitStoreStub));
        let permission_store = PermissionStoreStub::with_context(ChatPermissionContext {
            chat_type: Some("supergroup".to_owned()),
            settings: Some(ChatSettings::defaults(42)),
        });
        let permissions = Arc::new(ChatPermissionPolicy::new(permission_store.clone()));
        let item = queued_method_item(TelegramOutboundMethod::from(
            SendMessage::new(42, "hello").with_reply_parameters(
                ReplyParameters::new(100)
                    .with_chat_id(42)
                    .with_allow_sending_without_reply(true),
            ),
        ));
        let calls = Arc::new(Mutex::new(Vec::new()));

        let status = send_dispatcher_work_item_with_transport(rate_limits, permissions, item, {
            let calls = Arc::clone(&calls);
            move |method| {
                let calls = Arc::clone(&calls);
                async move {
                    let has_reply = match &method {
                        TelegramOutboundMethod::SendMessage(method) => {
                            serde_json::to_value(method.as_ref())
                                .expect("sendMessage JSON")
                                .get("reply_parameters")
                                .is_some()
                        }
                        _ => false,
                    };
                    let call_number = {
                        let mut calls = calls.lock().expect("send calls");
                        calls.push(has_reply);
                        calls.len()
                    };
                    if call_number == 1 {
                        Err(reply_missing_error())
                    } else {
                        Ok(TelegramOutboundResponse::Message(Box::new(
                            telegram_message(42, 101),
                        )))
                    }
                }
            }
        })
        .await;

        assert_eq!(status, DispatcherSendStatus::Sent);
        assert_eq!(*calls.lock().expect("send calls"), vec![true, false]);
        assert!(permission_store.saved_updates().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn webhook_shutdown_deletes_webhook_only_when_webhook_config_enabled_like_go()
    -> Result<(), Box<dyn Error>> {
        let disabled = DeleteWebhookStub::default();

        let disabled_report = delete_webhook_on_shutdown_if_enabled(false, &disabled).await;

        assert_eq!(disabled.calls(), 0);
        assert_eq!(
            disabled_report,
            WebhookShutdownCleanupReport::SkippedDisabled
        );

        let enabled = DeleteWebhookStub::default();

        let enabled_report = delete_webhook_on_shutdown_if_enabled(true, &enabled).await;

        assert_eq!(enabled.calls(), 1);
        assert_eq!(enabled_report, WebhookShutdownCleanupReport::Deleted);
        Ok(())
    }

    #[tokio::test]
    async fn telegram_webhook_response_maps_security_and_redis_failures()
    -> Result<(), Box<dyn Error>> {
        let redis = redis::Client::open("redis://127.0.0.1:1/0")?;
        let stream = openplotva_updates::RedisUpdateStream::with_key(
            redis,
            "openplotva:webhook-response-test",
        );
        let sender = openplotva_telegram::webhook_update_stream(stream, 77)
            .with_send_timeout(Duration::ZERO);
        assert_eq!(
            super::telegram_webhook_ingress_readiness(&sender).status,
            "ok"
        );
        let body = serde_json::to_vec(&sample_message_update(30)?)?;
        let mut headers = HeaderMap::new();
        headers.insert(
            openplotva_telegram::TELEGRAM_WEBHOOK_SECRET_HEADER,
            "secret".parse()?,
        );

        let response =
            telegram_webhook_response(&sender, Method::GET, headers.clone(), "secret", body.into())
                .await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);

        headers.insert(
            openplotva_telegram::TELEGRAM_WEBHOOK_SECRET_HEADER,
            "wrong".parse()?,
        );
        let response = telegram_webhook_response(
            &sender,
            Method::POST,
            headers,
            "secret",
            Bytes::from_static(b"{}"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let mut headers = HeaderMap::new();
        headers.insert(
            openplotva_telegram::TELEGRAM_WEBHOOK_SECRET_HEADER,
            "secret".parse()?,
        );
        let response = telegram_webhook_response(
            &sender,
            Method::POST,
            headers,
            "secret",
            Bytes::from_static(b"not-json"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            super::telegram_webhook_ingress_readiness(&sender).status,
            "error"
        );
        Ok(())
    }

    #[tokio::test]
    async fn static_web_assets_serve_copied_go_settings_files() -> Result<(), Box<dyn Error>> {
        let response =
            static_web_asset_response(openplotva_web::StaticAssetGroup::Settings, "index.js");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE),
            Some(&"text/javascript; charset=utf-8".parse()?)
        );
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(
            openplotva_web::static_asset_sha256_hex(&body),
            "eeb1bdb73dad8da034183e339749f48f2aa31f7311b7be21e633fc56cc0eb7e5"
        );
        Ok(())
    }

    #[test]
    fn admin_static_index_auth_matches_go_cookie_gate() -> Result<(), Box<dyn Error>> {
        assert!(admin_static_asset_requires_auth(""));
        assert!(admin_static_asset_requires_auth("/index.html"));
        assert!(!admin_static_asset_requires_auth("login.html"));

        let secret = "test-bot-token";
        let signed = openplotva_web::admin_session_cookie(7, secret);
        let value = signed.split(';').next().expect("cookie pair");

        let mut headers = HeaderMap::new();
        assert!(!admin_session_is_authorized(&headers, &[7], secret));
        headers.insert(header::COOKIE, format!("theme=dark; {value}").parse()?);
        assert!(admin_session_is_authorized(&headers, &[7, 9], secret));
        assert!(!admin_session_is_authorized(&headers, &[9], secret));

        // Unsigned/forged legacy cookie must be rejected
        let mut forged_headers = HeaderMap::new();
        forged_headers.insert(header::COOKIE, "admin_session=7".parse()?);
        assert!(!admin_session_is_authorized(&forged_headers, &[7], secret));

        let redirect = found("/admin/login.html");
        assert_eq!(redirect.status(), StatusCode::FOUND);
        assert_eq!(
            redirect.headers().get(header::LOCATION),
            Some(&"/admin/login.html".parse()?)
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_auth_api_matches_go_login_contract() -> Result<(), Box<dyn Error>> {
        let routes = static_web_routes(
            vec![7],
            "123:ABC",
            "https://plotva.example",
            "PlotvaBot",
            None,
        );
        let auth_date = OffsetDateTime::now_utc().unix_timestamp().to_string();
        let auth_pairs = [
            ("auth_date", auth_date.as_str()),
            ("first_name", "Ada"),
            ("id", "7"),
            ("username", "ada"),
        ];
        let hash = openplotva_web::telegram_auth_hash(auth_pairs.into_iter(), "123:ABC");
        let valid_query =
            format!("id=7&first_name=Ada&auth_date={auth_date}&username=ada&hash={hash}");

        let response = admin_auth_response(
            &crate::http::auth::test_state(&routes),
            Method::GET,
            Some(valid_query.as_str()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(
            response.headers().get(header::LOCATION),
            Some(&"/admin/".parse()?)
        );
        assert_eq!(
            response
                .headers()
                .get(header::SET_COOKIE)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.contains("Secure; HttpOnly; SameSite=Lax")),
            Some(true)
        );

        let response = admin_auth_response(
            &crate::http::auth::test_state(&routes),
            Method::PUT,
            Some(valid_query.as_str()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"method not allowed\"}\n");

        let response = admin_auth_response(
            &crate::http::auth::test_state(&routes),
            Method::GET,
            Some("id=7"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = admin_auth_response(
            &crate::http::auth::test_state(&routes),
            Method::GET,
            Some("id=nope&hash=x"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = admin_auth_response(
            &crate::http::auth::test_state(&routes),
            Method::GET,
            Some("id=7&auth_date=1700000000&hash=bad"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let non_admin_routes = static_web_routes(vec![9], "123:ABC", "", "", None);
        let response = admin_auth_response(
            &crate::http::auth::test_state(&non_admin_routes),
            Method::GET,
            Some(valid_query.as_str()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let no_token_routes = static_web_routes(vec![7], "", "", "", None);
        let response = admin_auth_response(
            &crate::http::auth::test_state(&no_token_routes),
            Method::GET,
            Some(valid_query.as_str()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        Ok(())
    }

    #[test]
    fn admin_auth_date_freshness_window() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("ts");
        let mut v = BTreeMap::new();
        // fresh (just now)
        v.insert("auth_date".to_owned(), "1700000000".to_owned());
        assert!(admin_auth_date_is_fresh(&v, now));
        // 23h ago: fresh
        v.insert(
            "auth_date".to_owned(),
            (1_700_000_000 - 23 * 3600).to_string(),
        );
        assert!(admin_auth_date_is_fresh(&v, now));
        // 25h ago: stale
        v.insert(
            "auth_date".to_owned(),
            (1_700_000_000 - 25 * 3600).to_string(),
        );
        assert!(!admin_auth_date_is_fresh(&v, now));
        // far future: rejected
        v.insert("auth_date".to_owned(), (1_700_000_000 + 3600).to_string());
        assert!(!admin_auth_date_is_fresh(&v, now));
        // missing: rejected
        v.remove("auth_date");
        assert!(!admin_auth_date_is_fresh(&v, now));
    }

    #[tokio::test]
    async fn admin_auth_check_and_bootstrap_match_go_json_shapes() -> Result<(), Box<dyn Error>> {
        let routes = static_web_routes(
            vec![7],
            "123:ABC",
            "https://plotva.example",
            "PlotvaBot",
            None,
        );

        let response = admin_auth_check(
            axum::extract::State(crate::http::auth::test_state(&routes)),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE),
            Some(&"application/json".parse()?)
        );
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"authenticated\":false}\n");

        let mut headers = HeaderMap::new();
        let signed = openplotva_web::admin_session_cookie(7, "123:ABC");
        let value = signed.split(';').next().expect("cookie pair");
        headers.insert(header::COOKIE, value.parse()?);
        let response = admin_auth_check(
            axum::extract::State(crate::http::auth::test_state(&routes)),
            headers,
        )
        .await;
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"authenticated\":true,\"user_id\":7}\n");

        let response =
            admin_bootstrap(axum::extract::State(crate::http::auth::test_state(&routes))).await;
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(
            &body[..],
            b"{\"bot_username\":\"PlotvaBot\",\"webapp_url\":\"https://plotva.example\"}\n"
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_state_matches_go_json_shape_without_live_services() -> Result<(), Box<dyn Error>>
    {
        let routes = static_web_routes(vec![7], "123:ABC", "https://plotva.example", "", None);
        let response = admin_state_response(&crate::http::observability::test_state(&routes)).await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(value["log_level"], "info");
        assert_eq!(value["queue"]["regularQueueSize"], 0);
        assert_eq!(value["queue"]["immediateQueueSize"], 0);
        assert_eq!(value["queue"]["processedTotal"], 0);
        assert_eq!(value["queue"]["dedupedTotal"], 0);
        assert_eq!(value["cache"]["Size"], 0);
        assert_eq!(value["planner"]["Capacity"], 0);
        Ok(())
    }

    #[tokio::test]
    async fn admin_state_requires_signed_admin_session() -> Result<(), Box<dyn Error>> {
        let routes = static_web_routes(vec![7], "123:ABC", "", "", None);

        let denied = admin_state(
            HeaderMap::new(),
            axum::extract::State(crate::http::observability::test_state(&routes)),
        )
        .await;
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);

        let mut headers = HeaderMap::new();
        let cookie = openplotva_web::admin_session_cookie(7, "123:ABC");
        headers.insert(
            header::COOKIE,
            cookie.split(';').next().expect("cookie pair").parse()?,
        );
        let allowed = admin_state(
            headers,
            axum::extract::State(crate::http::observability::test_state(&routes)),
        )
        .await;
        assert_eq!(allowed.status(), StatusCode::OK);
        Ok(())
    }

    #[tokio::test]
    async fn admin_loglevel_matches_go_auth_method_and_level_contract() -> Result<(), Box<dyn Error>>
    {
        let routes = static_web_routes(vec![7], "123:ABC", "https://plotva.example", "", None);
        let mut headers = HeaderMap::new();
        {
            let signed = openplotva_web::admin_session_cookie(7, "123:ABC");
            let value = signed.split(';').next().expect("cookie pair");
            headers.insert(header::COOKIE, value.parse()?);
        }

        let response = admin_loglevel_response(
            &crate::http::observability::test_state(&routes),
            Method::POST,
            &headers,
            br#"{"level":" warning "}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(value, json!({ "ok": true, "level": "warning" }));
        assert_eq!(body.last(), Some(&b'\n'));

        let response = admin_loglevel_response(
            &crate::http::observability::test_state(&routes),
            Method::GET,
            &headers,
            br#"{"level":"debug"}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);

        let response = admin_loglevel_response(
            &crate::http::observability::test_state(&routes),
            Method::POST,
            &HeaderMap::new(),
            br#"{"level":"debug"}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"unauthorized\"}\n");
        assert_eq!(body.last(), Some(&b'\n'));

        let mut non_admin_headers = HeaderMap::new();
        non_admin_headers.insert("X-Telegram-User-ID", "8".parse()?);
        let response = admin_loglevel_response(
            &crate::http::observability::test_state(&routes),
            Method::POST,
            &non_admin_headers,
            br#"{"level":"debug"}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let mut spoofed_admin_headers = HeaderMap::new();
        spoofed_admin_headers.insert("X-Telegram-User-ID", "7".parse()?);
        let response = admin_loglevel_response(
            &crate::http::observability::test_state(&routes),
            Method::POST,
            &spoofed_admin_headers,
            br#"{"level":"debug"}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"unauthorized\"}\n");
        assert_eq!(body.last(), Some(&b'\n'));

        let response = admin_loglevel_response(
            &crate::http::observability::test_state(&routes),
            Method::POST,
            &headers,
            br#"{"level":"trace"}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        Ok(())
    }

    #[test]
    fn admin_safety_query_and_row_mapping_match_go_contract() {
        let filter =
            admin_safety_checks_filter(Some("limit=5000&offset=-1&flagged=yes&q=%20risk%20"));
        assert_eq!(filter.limit, 1000);
        assert_eq!(filter.offset, 0);
        assert_eq!(filter.flagged, Some(true));
        assert_eq!(filter.q, "risk");
        assert_eq!(parse_admin_positive_i32(Some("0"), 200, 1000), 200);
        assert_eq!(parse_admin_positive_i32(Some("42"), 200, 1000), 42);
        assert_eq!(parse_admin_non_negative_i32(Some("-7"), 3), 3);
        assert_eq!(parse_admin_optional_bool(Some("no")), Some(false));
        assert_eq!(parse_admin_optional_bool(Some("maybe")), None);

        let row = openplotva_server::RuntimeSafetyCheckData {
            id: 5,
            created_at: "2026-05-21T00:00:00Z".to_owned(),
            source: "whitecircle".to_owned(),
            chat_id: Some(-100),
            request_messages: Some(json!([{"role": "user"}])),
            flagged: Some(true),
            response_json: Some(json!({"flagged": true})),
            duration_ms: 17,
            ..openplotva_server::RuntimeSafetyCheckData::default()
        };
        let value = admin_safety_check_json(&row);
        assert_eq!(value["id"], 5);
        assert_eq!(value["chat_id"], -100);
        assert_eq!(value["request_messages"][0]["role"], "user");
        assert_eq!(value["flagged"], true);
        assert_eq!(value["duration_ms"], 17);
        assert!(value.get("flow").is_none());
    }

    #[test]
    fn admin_llm_analytics_json_keeps_static_ui_shape_over_runtime_core()
    -> Result<(), Box<dyn Error>> {
        let summary = openplotva_server::RuntimeLlmAnalyticsData {
            range: "24h0m0s".to_owned(),
            bucket: "minute".to_owned(),
            since: "2026-05-20T00:00:00Z".to_owned(),
            totals: openplotva_server::RuntimeLlmAnalyticsTotalsData {
                total_count: 3,
                error_count: 1,
                avg_duration_ms: 20,
            },
            series: vec![openplotva_server::RuntimeLlmAnalyticsSeriesPointData {
                ts: "2026-05-21T00:00:00Z".to_owned(),
                total_count: 2,
                error_count: 0,
                avg_duration_ms: 10,
            }],
            model_series: vec![openplotva_server::RuntimeLlmAnalyticsModelSeriesPointData {
                ts: "2026-05-21T00:00:00Z".to_owned(),
                model: "model-a".to_owned(),
                request_count: 2,
                avg_generation_tps: 11.5,
                output_tokens: 200,
                ..openplotva_server::RuntimeLlmAnalyticsModelSeriesPointData::default()
            }],
            models: vec![openplotva_server::RuntimeLlmAnalyticsModelStatData {
                model: "model-a".to_owned(),
                request_count: 3,
                p95_duration_ms: 40,
                input_tokens: 100,
                avg_effective_output_tps: 7.5,
                ..openplotva_server::RuntimeLlmAnalyticsModelStatData::default()
            }],
            inference_params: vec![
                openplotva_server::RuntimeLlmAnalyticsInferenceParamStatData {
                    provider: "AI Farm".to_owned(),
                    source: "aifarm".to_owned(),
                    model: "model-a".to_owned(),
                    max_tokens: Some(512),
                    temperature: Some(0.7),
                    tool_mode: "text".to_owned(),
                    response_format: "json".to_owned(),
                    request_count: 2,
                    avg_effective_output_tps: 7.5,
                    ..openplotva_server::RuntimeLlmAnalyticsInferenceParamStatData::default()
                },
            ],
            runtime_jobs: vec![openplotva_server::RuntimeJobAnalyticsStatData {
                job_type: "image".to_owned(),
                queue_name: "image-vip".to_owned(),
                provider: "aifarm".to_owned(),
                created_count: 1,
                ..openplotva_server::RuntimeJobAnalyticsStatData::default()
            }],
            ai_farm_capacity: Some(openplotva_server::RuntimeAifarmCapacitySnapshotData {
                service: "dialog".to_owned(),
                available: 2,
                ready: true,
                observed_at: "2026-05-21T00:00:00Z".to_owned(),
                ..openplotva_server::RuntimeAifarmCapacitySnapshotData::default()
            }),
            ..openplotva_server::RuntimeLlmAnalyticsData::default()
        };

        let tool_calls = openplotva_dialog::tool_telemetry::ToolTelemetrySnapshot {
            since: "2026-05-20T00:00:00Z".to_owned(),
            total: 2,
            by_outcome: vec![openplotva_dialog::tool_telemetry::ToolTelemetryCounter {
                key: "detected".to_owned(),
                count: 1,
            }],
            by_form: Vec::new(),
            by_tool: vec![openplotva_dialog::tool_telemetry::ToolTelemetryCounter {
                key: "draw_image".to_owned(),
                count: 2,
            }],
            recent: vec![openplotva_dialog::tool_telemetry::ToolTelemetryEvent {
                at: OffsetDateTime::parse("2026-05-21T00:00:00Z", &Rfc3339)?,
                provider: "aifarm".to_owned(),
                model: "model-a".to_owned(),
                tool: "draw_image".to_owned(),
                form: "fenced".to_owned(),
                outcome: "detected".to_owned(),
                iteration: 1,
                ..openplotva_dialog::tool_telemetry::ToolTelemetryEvent::default()
            }],
        };
        let value = admin_llm_analytics_summary_json(&summary, &tool_calls);
        assert_eq!(value["totals"]["total_count"], 3);
        assert_eq!(value["series"][0]["total_count"], 2);
        assert_eq!(value["model_series"][0]["output_tokens"], 200);
        assert_eq!(value["inference_params"][0]["max_tokens"], 512);
        assert_eq!(value["tool_calls"]["total"], 2);
        assert_eq!(value["tool_calls"]["by_tool"][0]["key"], "draw_image");
        assert!(value.get("memory_runs").is_none());
        assert!(value.get("memory_runs_error").is_none());
        assert_eq!(value["models"][0]["model"], "model-a");
        assert_eq!(value["models"][0]["input_tokens"], 100);
        assert_eq!(value["models"][0]["avg_effective_output_tps"], 7.5);
        assert_eq!(value["runtime_jobs"][0]["queue_name"], "image-vip");
        assert_eq!(value["aifarm_capacity"]["available"], 2);
        Ok(())
    }

    #[tokio::test]
    async fn admin_shield_rest_matches_go_disabled_contract() -> Result<(), Box<dyn Error>> {
        let routes = static_web_routes(vec![7], "123:ABC", "https://plotva.example", "", None);
        let mut headers = HeaderMap::new();
        {
            let signed = openplotva_web::admin_session_cookie(7, "123:ABC");
            let value = signed.split(';').next().expect("cookie pair");
            headers.insert(header::COOKIE, value.parse()?);
        }

        let response = admin_shield_documents_response(
            &crate::http::shield::test_state(&routes),
            Method::GET,
            &headers,
            Some("limit=999&include_disabled=yes&q=risk"),
            &[],
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"count\":0,\"documents\":[]}\n");

        let response = admin_shield_documents_response(
            &crate::http::shield::test_state(&routes),
            Method::POST,
            &headers,
            None,
            br#"{"title":"Risk"}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"shield disabled\"}\n");

        let response = admin_shield_documents_response(
            &crate::http::shield::test_state(&routes),
            Method::DELETE,
            &headers,
            None,
            &[],
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"id required\"}\n");

        let response = admin_shield_documents_response(
            &crate::http::shield::test_state(&routes),
            Method::DELETE,
            &headers,
            Some("id=5"),
            &[],
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"ok\":true}\n");

        let response = admin_shield_embeddings_rebuild_response(
            &crate::http::shield::test_state(&routes),
            Method::POST,
            &headers,
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let response = admin_shield_test_response(
            &crate::http::shield::test_state(&routes),
            Method::GET,
            &headers,
            br#"{}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);

        let response = admin_shield_test_response(
            &crate::http::shield::test_state(&routes),
            Method::POST,
            &headers,
            br#"{}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        Ok(())
    }

    #[test]
    fn admin_shield_json_and_helpers_match_go_contract() -> Result<(), Box<dyn Error>> {
        assert_eq!(parse_shield_limit(None), 100);
        assert_eq!(parse_shield_limit(Some("0")), 100);
        assert_eq!(parse_shield_limit(Some("999")), 500);

        let input = admin_shield_document_input(
            crate::http::shield::AdminShieldDocumentRequest {
                title: " Risk Title ".to_owned(),
                body: " Body ".to_owned(),
                ..crate::http::shield::AdminShieldDocumentRequest::default()
            },
            true,
        );
        let normalized = openplotva_shield::normalize_document_input(input)?;
        assert_eq!(normalized.slug, "risk-title");
        assert_eq!(normalized.category, "general");
        assert!(normalized.enabled);

        let created_at =
            OffsetDateTime::parse("2026-05-21T10:00:00Z", &Rfc3339).expect("test timestamp");
        let document = openplotva_shield::Document {
            id: 42,
            slug: "risk-title".to_owned(),
            title: "Risk Title".to_owned(),
            body: "Body".to_owned(),
            category: "Safety".to_owned(),
            enabled: true,
            priority: 9,
            created_at: Some(created_at),
            updated_at: Some(created_at),
        };
        let document_json = admin_shield_document_json(&document);
        assert_eq!(document_json["created_at"], "2026-05-21T10:00:00Z");
        assert_eq!(document_json["category"], "Safety");

        let result = openplotva_shield::SearchResult {
            query: "risk".to_owned(),
            matches: vec![openplotva_shield::ScoredDocument {
                document,
                lexical_score: 0.3,
                vector_score: 0.7,
            }],
            context: "<shield></shield>".to_owned(),
            lexical_only: false,
            ..openplotva_shield::SearchResult::default()
        };
        assert!(admin_shield_category_matched(&result.matches, " safety "));
        let value = admin_shield_test_json(&result, " safety ");
        assert_eq!(value["expected_category"], "safety");
        assert_eq!(value["category_matched"], true);
        assert_eq!(value["matches"][0]["document"]["slug"], "risk-title");
        assert!(value.get("candidates").is_none());
        assert!(value.get("debug").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn admin_taskman_rest_lists_gets_cancels_restarts_and_clears_shared_queue()
    -> Result<(), Box<dyn Error>> {
        let routes = static_web_routes(vec![7], "123:ABC", "https://plotva.example", "", None);
        let queue = Arc::new(openplotva_taskman::InMemoryTaskQueue::new());
        let created_at = OffsetDateTime::parse("2026-05-21T10:00:00Z", &Rfc3339)?;
        let job_id = queue.assign(
            openplotva_taskman::IMAGE_VIP_QUEUE_NAME,
            openplotva_taskman::StatelessJobItem {
                title: "draw".to_owned(),
                created: created_at,
                priority: openplotva_taskman::HIGH_PRIORITY,
                processing_timeout_seconds: 90,
                data: openplotva_taskman::JobPayload {
                    job_type: openplotva_taskman::JobType::ImageGen,
                    telegram_data: Some(openplotva_taskman::TelegramData {
                        chat_id: -200,
                        user_id: 100,
                        message_id: 5,
                        thread_message_id: Some(10),
                        user_full_name: "Ada".to_owned(),
                        chat_title: "Plotva Lab".to_owned(),
                    }),
                    image_data: None,
                    music_data: None,
                    dialog_data: None,
                    asr_data: None,
                    control_data: None,
                    agent_data: None,
                },
            },
        );
        routes
            .taskman_inspector
            .set_shared_queue(queue.clone(), BTreeMap::new());
        let diagnostic_id = job_id;
        let mut headers = HeaderMap::new();
        {
            let signed = openplotva_web::admin_session_cookie(7, "123:ABC");
            let value = signed.split(';').next().expect("cookie pair");
            headers.insert(header::COOKIE, value.parse()?);
        }

        let response = admin_taskman_jobs_response(
            &crate::http::taskman::test_state(&routes),
            Method::GET,
            &headers,
            Some("status=pending&queue=image-vip"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL),
            Some(&"no-cache".parse()?)
        );
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(value["total"], 1);
        assert_eq!(value["items"][0]["id"], diagnostic_id);
        assert_eq!(value["items"][0]["job_type"], "image_gen");
        assert_eq!(value["items"][0]["thread_message_id"], 10);

        let response = admin_taskman_job_response(
            &crate::http::taskman::test_state(&routes),
            Method::GET,
            &headers,
            Some(&format!("id={diagnostic_id}")),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(value["job"]["payload"]["type"], "image_gen");
        assert_eq!(value["messages"], json!([]));

        let response = admin_taskman_job_cancel_response(
            &crate::http::taskman::test_state(&routes),
            Method::POST,
            &headers,
            Some(&format!("job_id={diagnostic_id}")),
        );
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            queue.record(job_id).expect("cancelled job").status,
            openplotva_taskman::JobStatus::Cancelled
        );

        let response = admin_taskman_job_restart_response(
            &crate::http::taskman::test_state(&routes),
            Method::PUT,
            &headers,
            Some(&format!("job_id={diagnostic_id}")),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert!(value["new_job_id"].as_i64().unwrap_or_default() > diagnostic_id);

        let response = admin_taskman_jobs_clear_response(
            &crate::http::taskman::test_state(&routes),
            Method::DELETE,
            &headers,
            Some("status=cancelled"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(value["matched"], 1);
        assert_eq!(value["deleted"], 1);
        assert_eq!(value["deleted_active"], 0);
        Ok(())
    }

    #[tokio::test]
    async fn admin_memory_rest_disabled_and_query_contract_match_go() -> Result<(), Box<dyn Error>>
    {
        let routes = static_web_routes(vec![7], "123:ABC", "https://plotva.example", "", None);
        let mut headers = HeaderMap::new();
        {
            let signed = openplotva_web::admin_session_cookie(7, "123:ABC");
            let value = signed.split(';').next().expect("cookie pair");
            headers.insert(header::COOKIE, value.parse()?);
        }

        let response = admin_memory_cards_response(
            &crate::http::memory::test_state(&routes),
            Method::GET,
            &headers,
            Some("limit=999"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(value["count"], 0);
        assert!(value["cards"].is_null());

        let response = admin_memory_cards_response(
            &crate::http::memory::test_state(&routes),
            Method::DELETE,
            &headers,
            Some("id=42"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"ok\":true}\n");

        let response = admin_memory_runs_response(
            &crate::http::memory::test_state(&routes),
            Method::GET,
            &headers,
            None,
            &[],
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(value["count"], 0);
        assert!(value["runs"].is_null());
        assert_eq!(value["policy"]["min_messages_per_run"], 20);
        assert_eq!(
            value["policy"].get("max_queued_runs"),
            Some(&serde_json::Value::Null)
        );
        assert!(value["policy"].get("max_daily_enqueued_runs").is_none());
        assert_eq!(value["policy"]["token_estimator"], "heuristic");
        assert!(value["enqueue_rollups"].is_null());

        let response = admin_memory_runs_response(
            &crate::http::memory::test_state(&routes),
            Method::POST,
            &headers,
            Some("id=42"),
            &[],
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"memory disabled\"}\n");

        let response = admin_memory_restart_response(
            &crate::http::memory::test_state(&routes),
            Method::POST,
            &headers,
            Some("id=bad"),
            &[],
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"invalid id\"}\n");

        let response = admin_memory_restart_response(
            &crate::http::memory::test_state(&routes),
            Method::GET,
            &headers,
            None,
            &[],
        )
        .await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        Ok(())
    }

    #[tokio::test]
    async fn admin_memory_restart_override_matches_go_query_and_json_rules()
    -> Result<(), Box<dyn Error>> {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "Application/JSON; charset=utf-8".parse()?,
        );
        let (override_, explicit) = admin_memory_restart_override(
            &headers,
            Some("override=true&provider=Gemini&model=%20gemini-model%20"),
            br#"{"override":false,"provider":"aifarm","model":"ignored"}"#,
        )
        .expect("valid override");
        assert!(explicit);
        assert_eq!(override_.provider, "gemini");
        assert_eq!(override_.model, "gemini-model");

        let (override_, explicit) =
            admin_memory_restart_override(&HeaderMap::new(), Some("override=1"), b"{not json}")
                .expect("non-json content type skips body");
        assert!(explicit);
        assert_eq!(override_.provider, "");

        let err = admin_memory_restart_override(&headers, Some("provider=bogus"), b"{}")
            .expect_err("invalid provider");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes((*err).into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"invalid override\"}\n");
        Ok(())
    }

    #[test]
    fn admin_memory_override_runtime_resolves_aifarm_and_genkit_providers() {
        let config = openplotva_config::AppConfig::from_raw(openplotva_config::RawConfig {
            googleai_key: Some("gemini-key".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");
        let (provider, model) = admin_memory_resolve_override(
            &AdminMemoryOverride {
                provider: String::new(),
                model: " override-model ".to_owned(),
            },
            &config,
            "default-model",
        )
        .expect("aifarm default");
        assert_eq!(provider, "aifarm");
        assert_eq!(model, "override-model");

        let (provider, model) = admin_memory_resolve_override(
            &AdminMemoryOverride {
                provider: "aifarm".to_owned(),
                model: String::new(),
            },
            &config,
            " default-model ",
        )
        .expect("explicit aifarm");
        assert_eq!(provider, "aifarm");
        assert_eq!(model, "default-model");

        let (provider, model) = admin_memory_resolve_override(
            &AdminMemoryOverride {
                provider: "gemini".to_owned(),
                model: String::new(),
            },
            &config,
            "default-model",
        )
        .expect("gemini aliases to genkit provider");
        assert_eq!(provider, "genkit");
        assert_eq!(model, openplotva_llm::gemini::MODEL_GEMINI_FLASH_LITE);
    }

    #[tokio::test]
    async fn admin_taskman_query_and_disabled_contract_match_go() -> Result<(), Box<dyn Error>> {
        let filter = admin_taskman_jobs_filter(Some(
            "q=%20image%20&status=pending,completed&queue=image-vip,music-vip&user_id=42&chat_id=100&time_field=completed_at&from=1700000000&to=2026-05-19T12:00:00Z&sort_by=priority&sort_dir=asc&offset=20&limit=50",
        ))
        .expect("valid filter");
        assert_eq!(filter.q, "image");
        assert_eq!(filter.status, vec!["pending", "completed"]);
        assert_eq!(filter.queue, vec!["image-vip", "music-vip"]);
        assert_eq!(filter.user_id, Some(42));
        assert_eq!(filter.chat_id, Some(100));
        assert_eq!(filter.time_field, "completed_at");
        assert_eq!(filter.from, "2023-11-14T22:13:20Z");
        assert_eq!(filter.to, "2026-05-19T12:00:00Z");
        assert_eq!(filter.sort_by, "priority");
        assert_eq!(filter.sort_dir, "asc");
        assert_eq!(filter.offset, 20);
        assert_eq!(filter.limit, 50);

        let err = admin_taskman_jobs_filter(Some("status=unknown"))
            .expect_err("invalid status should be rejected");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(err.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"invalid status\"}\n");

        let routes = static_web_routes(vec![7], "123:ABC", "https://plotva.example", "", None);
        let mut headers = HeaderMap::new();
        {
            let signed = openplotva_web::admin_session_cookie(7, "123:ABC");
            let value = signed.split(';').next().expect("cookie pair");
            headers.insert(header::COOKIE, value.parse()?);
        }
        let response = admin_taskman_jobs_response(
            &crate::http::taskman::test_state(&routes),
            Method::GET,
            &headers,
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"task manager not configured\"}\n");

        let response = admin_taskman_job_response(
            &crate::http::taskman::test_state(&routes),
            Method::GET,
            &headers,
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let response = admin_taskman_jobs_response(
            &crate::http::taskman::test_state(&routes),
            Method::POST,
            &headers,
            Some("status=pending"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        Ok(())
    }

    #[tokio::test]
    async fn postgres_admin_entity_json_preserves_nulls_base64_and_permissions()
    -> Result<(), Box<dyn Error>> {
        let Ok(dsn) = std::env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&dsn)
            .await?;
        openplotva_storage::run_migrations_on(&pool).await?;
        let user_id = OffsetDateTime::now_utc().unix_timestamp_nanos() as i64;
        let chat_id = -user_id;
        sqlx::query("INSERT INTO users (id,first_name,is_premium,is_vip,settings,discovered,updated) VALUES ($1,'Alice',TRUE,FALSE,$2,'2026-01-02T03:04:05Z','2026-01-02T03:04:05Z')")
            .bind(user_id).bind(json!({"language":"pl"})).execute(&pool).await?;
        sqlx::query("INSERT INTO chats (id,type,title) VALUES ($1,'supergroup','Group')")
            .bind(chat_id)
            .execute(&pool)
            .await?;
        sqlx::query("INSERT INTO chat_settings (chat_id,mood_alignment,reactivity_percentage,enable_global_text_reply) VALUES ($1,NULL,0,FALSE)").bind(chat_id).execute(&pool).await?;
        sqlx::query("INSERT INTO chat_permissions (chat_id,status,last_checked_at,can_delete_messages) VALUES ($1,'member',now(),TRUE)").bind(chat_id).execute(&pool).await?;
        let mut routes = static_web_routes(vec![7], "123:ABC", "https://plotva.example", "", None);
        routes.postgres = Some(pool.clone());
        let mut headers = HeaderMap::new();
        let signed = openplotva_web::admin_session_cookie(7, "123:ABC");
        headers.insert(
            header::COOKIE,
            signed.split(';').next().expect("cookie").parse()?,
        );
        let query = format!("id={user_id}");
        let response = crate::http::entities::admin_user_get_response(
            &crate::http::entities::test_state(&routes),
            Method::GET,
            &headers,
            Some(&query),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await?)?;
        assert_eq!(
            body["user"],
            json!({
                "id":user_id,"first_name":"Alice","last_name":null,"username":null,
                "language_code":null,"is_premium":true,"is_vip":false,
                "settings":base64::Engine::encode(&base64::engine::general_purpose::STANDARD, br#"{"language":"pl"}"#),
                "discovered":"2026-01-02T03:04:05Z","updated":"2026-01-02T03:04:05Z"
            })
        );
        let query = format!("chat_id={chat_id}");
        let response = admin_chat_get_response(
            &crate::http::entities::test_state(&routes),
            Method::GET,
            &headers,
            Some(&query),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await?)?;
        assert_eq!(body["info"]["title"], "Group");
        assert_eq!(body["settings"]["mood_alignment"], serde_json::Value::Null);
        assert_eq!(body["settings"]["reactivity_percentage"], 0);
        assert_eq!(body["settings"]["enable_global_text_reply"], false);
        assert_eq!(body["permissions"]["can_delete_messages"], true);
        assert_eq!(body["members_count"], 0);
        assert_eq!(body["blocked"], false);
        sqlx::query("DELETE FROM chat_permissions WHERE chat_id = $1")
            .bind(chat_id)
            .execute(&pool)
            .await?;
        sqlx::query("DELETE FROM chat_settings WHERE chat_id = $1")
            .bind(chat_id)
            .execute(&pool)
            .await?;
        sqlx::query("DELETE FROM chats WHERE id = $1")
            .bind(chat_id)
            .execute(&pool)
            .await?;
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn admin_chat_and_user_routes_keep_go_auth_method_and_param_errors()
    -> Result<(), Box<dyn Error>> {
        let routes = static_web_routes(vec![7], "123:ABC", "https://plotva.example", "", None);
        let mut headers = HeaderMap::new();
        {
            let signed = openplotva_web::admin_session_cookie(7, "123:ABC");
            let value = signed.split(';').next().expect("cookie pair");
            headers.insert(header::COOKIE, value.parse()?);
        }

        let response = admin_chat_get_response(
            &crate::http::entities::test_state(&routes),
            Method::POST,
            &headers,
            Some("chat_id=1"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"method not allowed\"}\n");

        let response = admin_chat_get_response(
            &crate::http::entities::test_state(&routes),
            Method::GET,
            &HeaderMap::new(),
            Some("chat_id=1"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"unauthorized\"}\n");

        let response = admin_chat_get_response(
            &crate::http::entities::test_state(&routes),
            Method::GET,
            &headers,
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"chat_id required\"}\n");

        let response = admin_chat_get_response(
            &crate::http::entities::test_state(&routes),
            Method::GET,
            &headers,
            Some("chat_id=nope"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"invalid chat_id\"}\n");

        let response = admin_chats_search_by_member_response(
            &crate::http::entities::test_state(&routes),
            Method::GET,
            &headers,
            Some("user_id=nope"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"username or user_id required\"}\n");

        let response = admin_user_grant_vip_response(
            &crate::http::entities::test_state(&routes),
            Method::POST,
            &headers,
            br#"{"user_id":"bad","days":1}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(
            &body[..],
            b"{\"error\":\"user_id and non-zero signed days required\"}\n"
        );
        Ok(())
    }

    #[test]
    fn admin_vip_and_json_helpers_match_go_web_shapes() {
        assert_eq!(admin_i64_from_json(&json!(42)), Some(42));
        assert_eq!(admin_i64_from_json(&json!("42")), Some(42));
        assert_eq!(admin_i64_from_json(&json!("nope")), None);
        assert_eq!(
            admin_non_empty_string("  hello   world "),
            Some("hello   world".to_owned())
        );
        assert_eq!(admin_non_empty_string("   "), None);
        assert_eq!(
            admin_vip_summary_json(None),
            json!({
                "active": false,
                "has_history": false,
                "remaining_seconds": 0,
                "remaining_days": 0
            })
        );
    }

    #[test]
    fn admin_redis_prefix_groups_match_go_admin_map_rules() {
        let groups = admin_redis_prefix_groups_from_keys(
            "plotva:",
            [
                "plotva:updates:queue".to_owned(),
                "plotva:updates:dead".to_owned(),
                "plotva:message_queue".to_owned(),
                "plotva:".to_owned(),
                "other:key".to_owned(),
            ],
        );

        assert_eq!(groups.get("plotva:updates:"), Some(&2));
        assert_eq!(groups.get("plotva:message_queue"), Some(&1));
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn admin_auth_query_and_user_persistence_shape_match_go() {
        let values = admin_auth_query_values(Some(
            "first_name=%20%20&last_name=%20Lovelace%20&username=%20ada%20&language_code=%20ru%20&id=7",
        ));
        let user = admin_auth_user_state(&values, 7);
        assert_eq!(user.id, 7);
        assert_eq!(user.first_name, "Telegram Admin");
        assert_eq!(user.last_name.as_deref(), Some("Lovelace"));
        assert_eq!(user.username.as_deref(), Some("ada"));
        assert_eq!(user.language_code.as_deref(), Some("ru"));
        assert_eq!(user.is_premium, None);
    }

    #[test]
    fn admin_gradius_filters_preserve_supported_dimensions_and_reject_bad_ids() {
        let values = admin_auth_query_values(Some(
            "range=7d&integration_kind=native_utility&source=rates-command&outcome=ad&delivery_state=delivered&user_id=7&chat_id=8&dialog_job_id=9&model=qwen&q=discount&offset=4&limit=999",
        ));
        let filter = admin_gradius_opportunities_filter(&values).expect("Gradius filter");
        assert_eq!(filter.range, "7d");
        assert_eq!(filter.integration_kind, "native_utility");
        assert_eq!(filter.source, "rates-command");
        assert_eq!(filter.outcome, "ad");
        assert_eq!(filter.delivery_state, "delivered");
        assert_eq!(filter.user_id, Some(7));
        assert_eq!(filter.chat_id, Some(8));
        assert_eq!(filter.dialog_job_id, Some(9));
        assert_eq!(filter.model, "qwen");
        assert_eq!(filter.q, "discount");
        assert_eq!(filter.offset, 4);
        assert_eq!(filter.limit, 500);

        let invalid = admin_auth_query_values(Some("user_id=not-an-id"));
        assert_eq!(
            admin_gradius_opportunities_filter(&invalid),
            Err("user_id must be an integer".to_owned())
        );
    }

    #[tokio::test]
    async fn settings_api_without_services_fails_loud_with_go_cors_headers()
    -> Result<(), Box<dyn Error>> {
        let routes = static_web_routes(Vec::new(), "", "", "", None);
        let response = settings_get_response(
            &crate::http::settings::test_state(&routes),
            Some("chat_id=42&signature=780e28cf"),
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&"*".parse()?)
        );
        assert_eq!(
            response.headers().get(header::ACCESS_CONTROL_ALLOW_METHODS),
            Some(&"GET, POST, PUT, OPTIONS".parse()?)
        );
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(
            &body[..],
            b"{\"error\": \"settings store not configured\"}\n"
        );
        Ok(())
    }

    #[test]
    fn settings_get_access_accepts_user_or_chat_signature_like_go() {
        let values =
            admin_auth_query_values(Some("chat_id=-1001234567890&user_id=42&signature=780e28cf"));
        let access = parse_settings_get_access(&values, &[42]).expect("user signature");
        assert_eq!(access.chat_id, -1001234567890);
        assert_eq!(access.user_id, 42);
        assert!(access.is_global_admin);

        let values =
            admin_auth_query_values(Some("chat_id=-1001234567890&user_id=42&signature=8ebdb694"));
        assert!(parse_settings_get_access(&values, &[]).is_ok());

        let values = admin_auth_query_values(Some("chat_id=0&signature=780e28cf"));
        assert!(parse_settings_get_access(&values, &[]).is_err());

        let values = admin_auth_query_values(Some("chat_id=42"));
        assert!(parse_settings_get_access(&values, &[]).is_err());
    }

    #[tokio::test]
    async fn settings_memory_access_does_not_inherit_global_admin_bypass_like_go()
    -> Result<(), Box<dyn Error>> {
        let routes = static_web_routes(
            vec![42],
            SETTINGS_GATE_TOKEN,
            "https://plotva.example",
            "",
            None,
        );
        let init_data = fresh_settings_init_data(42);

        let response = match parse_settings_memory_access(
            &crate::http::settings::test_state(&routes),
            Some("chat_id=-1001234567890&user_id=42&signature=780e28cf"),
            Some(&init_data),
        )
        .await
        {
            Ok(_) => panic!("memory APIs must run the real group permission check"),
            Err(response) => response,
        };

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"permission check failed\"}\n");
        Ok(())
    }

    const SETTINGS_GATE_TOKEN: &str = "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11";

    fn hmac_sha256_hex_for_test(key: &[u8], message: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        const BLOCK: usize = 64;
        let mut normalized = [0_u8; BLOCK];
        if key.len() > BLOCK {
            normalized[..32].copy_from_slice(&Sha256::digest(key));
        } else {
            normalized[..key.len()].copy_from_slice(key);
        }
        let mut inner_pad = [0x36_u8; BLOCK];
        let mut outer_pad = [0x5c_u8; BLOCK];
        for index in 0..BLOCK {
            inner_pad[index] ^= normalized[index];
            outer_pad[index] ^= normalized[index];
        }
        let mut inner = Sha256::new();
        inner.update(inner_pad);
        inner.update(message);
        let inner_hash = inner.finalize();
        let mut outer = Sha256::new();
        outer.update(outer_pad);
        outer.update(inner_hash);
        hex::encode(outer.finalize())
    }

    fn fresh_settings_init_data(user_id: i64) -> String {
        let auth_date = current_unix_timestamp();
        let user = format!(r#"{{"id":{user_id},"first_name":"Ada","username":"ada"}}"#);
        let pairs = [("auth_date", auth_date.to_string()), ("user", user)];
        let data_check_string = pairs
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("\n");
        let secret_key_hex =
            hmac_sha256_hex_for_test(b"WebAppData", SETTINGS_GATE_TOKEN.as_bytes());
        let secret_key = hex::decode(secret_key_hex).expect("hmac hex decodes to bytes");
        let hash = hmac_sha256_hex_for_test(&secret_key, data_check_string.as_bytes());
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        for (key, value) in &pairs {
            serializer.append_pair(key, value);
        }
        serializer.append_pair("hash", &hash);
        serializer.finish()
    }

    #[test]
    fn settings_init_data_authenticates_matching_caller() {
        let routes = static_web_routes(Vec::new(), SETTINGS_GATE_TOKEN, "", "", None);
        let init_data = fresh_settings_init_data(42);
        assert!(
            authenticate_settings_init_data(
                &crate::http::settings::test_state(&routes),
                Some(&init_data),
                42
            )
            .is_ok()
        );
    }

    #[test]
    fn settings_init_data_rejects_caller_user_id_mismatch() {
        let routes = static_web_routes(Vec::new(), SETTINGS_GATE_TOKEN, "", "", None);
        let init_data = fresh_settings_init_data(42);
        assert!(
            authenticate_settings_init_data(
                &crate::http::settings::test_state(&routes),
                Some(&init_data),
                99
            )
            .is_err()
        );
    }

    #[test]
    fn settings_init_data_rejects_forged_header() {
        let routes = static_web_routes(Vec::new(), SETTINGS_GATE_TOKEN, "", "", None);
        let forged = "auth_date=1700000000&user=%7B%22id%22%3A42%7D&hash=deadbeef";
        assert!(
            authenticate_settings_init_data(
                &crate::http::settings::test_state(&routes),
                Some(forged),
                42
            )
            .is_err()
        );
    }

    #[test]
    fn settings_init_data_rejects_zero_claimed_user_id() {
        let routes = static_web_routes(Vec::new(), SETTINGS_GATE_TOKEN, "", "", None);
        let init_data = fresh_settings_init_data(42);
        assert!(
            authenticate_settings_init_data(
                &crate::http::settings::test_state(&routes),
                Some(&init_data),
                0
            )
            .is_err()
        );
    }

    #[test]
    fn settings_init_data_absent_rejects() {
        let routes = static_web_routes(Vec::new(), SETTINGS_GATE_TOKEN, "", "", None);
        assert!(
            authenticate_settings_init_data(&crate::http::settings::test_state(&routes), None, 42)
                .is_err()
        );
    }

    #[test]
    fn stored_chat_membership_freshness_is_bounded() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp");
        let fresh = openplotva_storage::ChatMemberRecord {
            telegram_observed_at: Some(now - time::Duration::minutes(4)),
            ..openplotva_storage::ChatMemberRecord::default()
        };
        let stale = openplotva_storage::ChatMemberRecord {
            telegram_observed_at: Some(now - time::Duration::minutes(6)),
            ..openplotva_storage::ChatMemberRecord::default()
        };

        assert!(stored_chat_member_is_fresh(Some(&fresh), now));
        assert!(!stored_chat_member_is_fresh(Some(&stale), now));
        assert!(!stored_chat_member_is_fresh(None, now));
    }

    #[test]
    fn settings_response_and_display_helpers_match_go_shape() {
        let chat = openplotva_core::ChatState::new(
            -10042,
            "supergroup",
            Some("Plotva Lab".to_owned()),
            Some("examplelab".to_owned()),
            None,
            None,
            None,
        );
        assert_eq!(
            settings_chat_display(&chat),
            ("Plotva Lab".to_owned(), "supergroup".to_owned())
        );
        let private = openplotva_core::ChatState::new(
            42,
            "private",
            None,
            None,
            Some("Ada".to_owned()),
            Some("Lovelace".to_owned()),
            None,
        );
        assert_eq!(
            settings_chat_display(&private),
            ("Ada Lovelace".to_owned(), "private".to_owned())
        );

        let mut settings = openplotva_core::ChatSettings::defaults(42);
        settings.enable_global_text_reply = false;
        settings.enable_global_draw_reply = false;
        settings.daily_game_theme = None;
        let mut response = new_settings_response(&settings, String::new(), "private".to_owned());
        apply_private_chat_response_defaults(&mut response, 42, 42);
        assert_eq!(response.chat_id, 42);
        assert!(response.enable_global_text_reply);
        assert!(response.enable_global_draw_reply);
        assert!(response.enable_daily_game);
        assert_eq!(response.daily_game_theme, "auto");
        assert!(!response.is_vip);
        assert!(response.deputies.is_empty());
    }

    #[test]
    fn settings_update_request_validates_signature_and_private_reply_flags_like_go() {
        let long_persona = "я".repeat(1_001);
        let body = serde_json::json!({
            "chat_id": 42,
            "user_id": 42,
            "signature": "780e28cf",
            "custom_persona": long_persona,
            "enable_global_text_reply": false,
            "enable_global_draw_reply": false
        })
        .to_string();
        let req = match parse_settings_update_request(body.as_bytes()) {
            Ok(req) => req,
            Err(_) => panic!("valid settings request"),
        };
        assert_eq!(
            req.custom_persona
                .as_ref()
                .map(|value| value.chars().count()),
            Some(1_000)
        );
        assert_eq!(settings_reply_flags("private", &req), (true, true));

        let bad = br#"{"chat_id":42,"user_id":42,"signature":"bad"}"#;
        assert!(parse_settings_update_request(bad).is_err());
        let missing = br#"{"chat_id":42,"user_id":42}"#;
        assert!(parse_settings_update_request(missing).is_err());
    }

    #[test]
    fn settings_chat_full_info_conversion_preserves_go_refresh_fields() -> Result<(), Box<dyn Error>>
    {
        assert_eq!(
            settings_chat_full_info_type_name(carapax::types::ChatFullInfoType::Channel),
            "channel"
        );
        assert_eq!(
            settings_chat_full_info_type_name(carapax::types::ChatFullInfoType::Group),
            "group"
        );
        assert_eq!(
            settings_chat_full_info_type_name(carapax::types::ChatFullInfoType::Private),
            "private"
        );
        assert_eq!(
            settings_chat_full_info_type_name(carapax::types::ChatFullInfoType::Supergroup),
            "supergroup"
        );

        let chat: openplotva_telegram::ChatFullInfo = serde_json::from_value(json!({
            "id": -100123,
            "type": "supergroup",
            "accent_color_id": 1,
            "max_reaction_count": 11,
            "title": " Plotva Lab ",
            "username": "examplelab",
            "is_forum": true
        }))?;
        let state = settings_chat_state_from_full_info(chat);

        assert_eq!(state.id, -100123);
        assert_eq!(state.chat_type, "supergroup");
        assert_eq!(state.title.as_deref(), Some(" Plotva Lab "));
        assert_eq!(state.username.as_deref(), Some("examplelab"));
        assert_eq!(state.is_forum, Some(true));
        Ok(())
    }

    #[test]
    fn settings_side_helpers_match_go_deputy_and_memory_shapes() {
        assert_eq!(normalize_deputy_ids(&[5, 0, -1, 4, 5, 3], 4), vec![3, 5]);
        assert_eq!(parse_deputy_candidates_limit(None), 50);
        assert_eq!(parse_deputy_candidates_limit(Some("0")), 50);
        assert_eq!(parse_deputy_candidates_limit(Some("250")), 100);
        assert_eq!(parse_memory_limit(None), 100);
        assert_eq!(parse_memory_limit(Some("999")), 500);
        assert_eq!(parse_optional_i32("-1"), 0);
        assert_eq!(parse_optional_i32("42"), 42);
        assert_eq!(
            build_deputy_display_name(7, " Ada ", Some(" Lovelace "), Some("ada")),
            "Ada Lovelace"
        );
        assert_eq!(
            build_deputy_display_name(7, "", None, Some(" ada ")),
            "@ada"
        );
        assert_eq!(build_deputy_display_name(7, "", None, None), "User 7");

        let creator = openplotva_storage::ChatMemberRecord {
            status: openplotva_storage::CHAT_MEMBER_STATUS_CREATOR.to_owned(),
            ..openplotva_storage::ChatMemberRecord::default()
        };
        let admin = openplotva_storage::ChatMemberRecord {
            status: openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR.to_owned(),
            can_promote_members: Some(true),
            ..openplotva_storage::ChatMemberRecord::default()
        };
        let member = openplotva_storage::ChatMemberRecord {
            status: openplotva_storage::CHAT_MEMBER_STATUS_MEMBER.to_owned(),
            ..openplotva_storage::ChatMemberRecord::default()
        };
        assert!(chat_member_can_manage_settings(&creator, false));
        assert!(chat_member_can_manage_settings(&admin, false));
        assert!(chat_member_can_manage_settings(&member, true));
        assert!(!chat_member_can_manage_settings(&member, false));

        let upsert = openplotva_storage::ChatMemberUpsert {
            chat_id: -100,
            user_id: 42,
            status: openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR.to_owned(),
            can_promote_members: Some(true),
            can_delete_messages: Some(false),
            ..openplotva_storage::ChatMemberUpsert::default()
        };
        let fresh = chat_member_record_from_upsert(&upsert);
        assert_eq!(fresh.chat_id, -100);
        assert_eq!(fresh.user_id, 42);
        assert_eq!(
            fresh.status,
            openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR
        );
        assert_eq!(fresh.can_promote_members, Some(true));
        assert_eq!(fresh.can_delete_messages, Some(false));

        let unnamed = openplotva_core::ChatState::new(99, "group", None, None, None, None, None);
        assert_eq!(chat_list_title(&unnamed), "Chat 99");
    }

    #[test]
    fn settings_deputy_update_request_matches_go_validation_shape() {
        let req = parse_deputy_update_request(
            br#"{"chat_id":-100,"user_id":42,"signature":"780e28cf","deputy_ids":[9,9,42,0]}"#,
        )
        .expect("valid deputy update request");
        assert_eq!(req.chat_id, -100);
        assert_eq!(req.user_id, 42);
        assert_eq!(normalize_deputy_ids(&req.deputy_ids, req.user_id), vec![9]);

        assert!(
            parse_deputy_update_request(br#"{"chat_id":0,"user_id":42,"signature":"780e28cf"}"#)
                .is_err()
        );
        assert!(
            parse_deputy_update_request(br#"{"chat_id":-100,"user_id":42,"signature":"bad"}"#)
                .is_err()
        );
    }

    #[test]
    fn admin_llm_dialogs_filter_parses_query_params() {
        use crate::http::observability::admin_llm_dialogs_filter;
        let filter = admin_llm_dialogs_filter(Some(
            "kind=dialog&chat_id=-100500&errors_only=true&q=%D0%BF%D1%80%D0%B8%D0%B2%D0%B5%D1%82&limit=50",
        ));
        assert_eq!(filter.kind.as_deref(), Some("dialog"));
        assert_eq!(filter.chat_id, Some(-100_500));
        assert!(filter.errors_only);
        assert_eq!(filter.q, "привет");
        assert_eq!(filter.limit, 50);

        let defaults = admin_llm_dialogs_filter(Some("kind=all"));
        assert_eq!(defaults.kind, None, "kind=all means no kind filter");
        assert_eq!(defaults.chat_id, None);
        assert!(!defaults.errors_only);
        assert_eq!(defaults.limit, 200);
    }

    #[test]
    fn app_router_builds_with_admin_api_routes_before_static_wildcard() {
        assert_eq!(
            ADMIN_API_ROUTE_PATTERNS,
            [
                "/admin/api/auth",
                "/admin/api/auth_check",
                "/admin/api/state",
                "/admin/api/loglevel",
                "/admin/api/logs/stream",
                "/admin/api/bootstrap",
                "/admin/api/metrics",
                "/admin/api/safety/checks",
                "/admin/api/gradius/opportunities",
                "/admin/api/gradius/summary",
                "/admin/api/analytics/llm/summary",
                "/admin/api/analytics/overview",
                "/admin/api/llm/dialogs",
                "/admin/api/llm/dialogs/detail",
                "/admin/api/llm/dialogs/clear",
                "/admin/api/memory/cards",
                "/admin/api/memory/runs",
                "/admin/api/memory/restart",
                "/admin/api/memory/card",
                "/admin/api/memory/overview",
                "/admin/api/shield/documents",
                "/admin/api/shield/embeddings/rebuild",
                "/admin/api/shield/test",
                "/admin/api/redis/list",
                "/admin/api/redis/get",
                "/admin/api/redis/delete_prefix",
                "/admin/api/redis/prefixes",
                "/admin/api/redis/delete_key",
                "/admin/api/redis/flushdb",
                "/admin/api/chat",
                "/admin/api/chat/settings",
                "/admin/api/chat/block",
                "/admin/api/chat/unblock",
                "/admin/api/chat/members",
                "/admin/api/chats",
                "/admin/api/chats/search_by_member",
                "/admin/api/users",
                "/admin/api/vip/users",
                "/admin/api/user",
                "/admin/api/user/grant_vip",
                "/admin/api/user/revoke_vip",
                "/admin/api/user/delete",
                "/admin/api/taskman/jobs",
                "/admin/api/taskman/jobs/clear",
                "/admin/api/taskman/job",
                "/admin/api/taskman/job/cancel",
                "/admin/api/taskman/job/restart",
            ]
        );
        let _ = super::router();
    }

    #[test]
    fn fatal_error_report_masks_bot_token_in_error_chain() {
        let error = anyhow::anyhow!(
            "error sending request for url (https://api.telegram.org/bot123456789:AAFfake_value_1234567890abcdefghij/getMe)"
        )
        .context("get Telegram bot identity");

        let report = super::fatal_error_report(&error);

        assert!(
            report.starts_with("Error: get Telegram bot identity"),
            "{report}"
        );
        assert!(report.contains("bot123456789:<redacted>/getMe"), "{report}");
        assert!(
            !report.contains("AAFfake_value_1234567890abcdefghij"),
            "{report}"
        );
    }

    #[test]
    fn webhook_setup_from_config_reads_certificate_when_cert_and_key_are_set_like_go()
    -> Result<(), Box<dyn Error>> {
        let cert_path = unique_temp_file("openplotva-webhook-cert.pem");
        std::fs::write(&cert_path, b"cert-bytes")?;
        let config = openplotva_config::BotWebhookConfig {
            enabled: true,
            url: "https://example.test/telegram/webhook".to_owned(),
            cert_file: cert_path.to_string_lossy().into_owned(),
            key_file: "/unused-key.pem".to_owned(),
            secret_token: "secret".to_owned(),
        };

        let setup = super::webhook_setup_from_config(&config)?;

        assert_eq!(setup.url, "https://example.test/telegram/webhook");
        assert_eq!(setup.secret_token.as_deref(), Some("secret"));
        let certificate = setup.certificate.expect("certificate bytes");
        assert_eq!(certificate.name, "cert.pem");
        assert_eq!(certificate.bytes, b"cert-bytes");
        let _ = std::fs::remove_file(cert_path);
        Ok(())
    }

    #[test]
    fn runtime_api_reuses_generated_tls_material_from_persistent_paths()
    -> Result<(), Box<dyn Error>> {
        let cert_path = unique_temp_file("openplotva-runtime-api.crt");
        let key_path = unique_temp_file("openplotva-runtime-api.key");
        let config = openplotva_config::AppConfig::from_raw(openplotva_config::RawConfig {
            runtime_api_enabled: Some("true".to_owned()),
            runtime_api_cert_file: Some(cert_path.to_string_lossy().into_owned()),
            runtime_api_key_file: Some(key_path.to_string_lossy().into_owned()),
            ..openplotva_config::RawConfig::default()
        })?;

        let generated = super::runtime_api_tls_material(&config.runtime_api)?;
        let reloaded = super::runtime_api_tls_material(&config.runtime_api)?;

        assert_eq!(generated.source, "generated-persisted");
        assert_eq!(reloaded.source, "configured");
        assert_eq!(generated.cert_pem, reloaded.cert_pem);
        assert_eq!(generated.key_pem, reloaded.key_pem);
        assert_eq!(generated.tls_public_key_pin, reloaded.tls_public_key_pin);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&key_path)?.permissions().mode() & 0o777,
                0o600
            );
        }

        let _ = std::fs::remove_file(cert_path);
        let _ = std::fs::remove_file(key_path);
        Ok(())
    }

    #[test]
    fn webhook_setup_from_config_rejects_enabled_webhook_without_secret()
    -> Result<(), Box<dyn Error>> {
        let cert_path = unique_temp_file("openplotva-webhook-cert-no-key.pem");
        std::fs::write(&cert_path, b"cert-bytes")?;
        let config = openplotva_config::BotWebhookConfig {
            enabled: true,
            url: "https://example.test/telegram/webhook".to_owned(),
            cert_file: cert_path.to_string_lossy().into_owned(),
            key_file: String::new(),
            secret_token: String::new(),
        };

        let error = super::webhook_setup_from_config(&config).expect_err("secret is required");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("BOT_WEBHOOK_SECRET_TOKEN"));
        let _ = std::fs::remove_file(cert_path);
        Ok(())
    }

    #[test]
    fn webhook_multipart_plan_contains_go_set_webhook_fields_and_certificate()
    -> Result<(), Box<dyn Error>> {
        let setup = openplotva_telegram::WebhookSetup::new(
            "https://example.test/telegram/webhook",
            Some("secret".to_owned()),
        )
        .with_certificate(openplotva_telegram::WebhookCertificate::new(
            "cert.pem",
            b"cert-bytes".to_vec(),
        ));

        let plan = super::telegram_webhook_multipart_plan(&setup)?;

        assert_eq!(plan.certificate_name, "cert.pem");
        assert_eq!(plan.certificate_bytes, b"cert-bytes");
        let mut allowed_updates = openplotva_telegram::product_allowed_update_set()
            .into_iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?;
        allowed_updates.sort_by_key(serde_json::Value::to_string);
        assert_eq!(
            plan.fields,
            vec![
                (
                    "allowed_updates".to_owned(),
                    serde_json::to_string(&allowed_updates)?
                ),
                ("secret_token".to_owned(), "secret".to_owned()),
                (
                    "url".to_owned(),
                    "https://example.test/telegram/webhook".to_owned()
                ),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn telegram_bot_command_setup_deletes_then_sets_go_scopes() -> Result<(), Box<dyn Error>>
    {
        let executor = CommandSetupStub::default();

        let report = configure_telegram_bot_commands(&executor).await?;

        assert_eq!(
            report.set_scopes,
            vec!["privateCommands", "groupCommands", "groupAdminCommands"]
        );
        assert_eq!(
            executor.request_kinds(),
            vec![
                "deleteMyCommands",
                "setMyCommands",
                "setMyCommands",
                "setMyCommands"
            ]
        );
        assert_eq!(
            executor.scope_types(),
            vec![
                "all_private_chats",
                "all_group_chats",
                "all_chat_administrators"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn telegram_bot_command_setup_stops_before_sets_when_delete_fails() {
        let executor = CommandSetupStub::failing_delete("delete failed");

        let error = configure_telegram_bot_commands(&executor)
            .await
            .expect_err("delete failure");

        assert_eq!(error.to_string(), "delete bot commands: delete failed");
        assert_eq!(executor.request_kinds(), vec!["deleteMyCommands"]);
    }

    #[tokio::test]
    async fn telegram_bot_command_setup_reports_scope_when_set_fails() {
        let executor = CommandSetupStub::failing_set("groupCommands", "set failed");

        let error = configure_telegram_bot_commands(&executor)
            .await
            .expect_err("set failure");

        assert_eq!(
            error.to_string(),
            "set groupCommands bot commands: set failed"
        );
        assert_eq!(
            executor.request_kinds(),
            vec!["deleteMyCommands", "setMyCommands", "setMyCommands"]
        );
    }

    #[derive(Clone, Default)]
    struct DeleteWebhookStub {
        calls: Arc<Mutex<usize>>,
        error: Option<&'static str>,
    }

    impl DeleteWebhookStub {
        fn calls(&self) -> usize {
            *self.calls.lock().expect("delete webhook calls")
        }
    }

    impl super::DeleteWebhookExecutor for DeleteWebhookStub {
        type Error = StartupError;

        fn delete_webhook<'a>(&'a self) -> super::DeleteWebhookFuture<'a, Self::Error> {
            *self.calls.lock().expect("delete webhook calls") += 1;
            let result = self
                .error
                .map_or(Ok(()), |message| Err(StartupError(message)));
            Box::pin(async move { result })
        }
    }

    #[derive(Debug)]
    struct StartupError(&'static str);

    impl fmt::Display for StartupError {
        fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
            out.write_str(self.0)
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct CommandRequest {
        kind: &'static str,
        payload: serde_json::Value,
    }

    #[derive(Clone, Default)]
    struct CommandSetupStub {
        requests: Arc<Mutex<Vec<CommandRequest>>>,
        delete_error: Option<&'static str>,
        set_error_scope: Option<&'static str>,
        set_error: Option<&'static str>,
    }

    impl CommandSetupStub {
        fn failing_delete(error: &'static str) -> Self {
            Self {
                delete_error: Some(error),
                ..Self::default()
            }
        }

        fn failing_set(scope: &'static str, error: &'static str) -> Self {
            Self {
                set_error_scope: Some(scope),
                set_error: Some(error),
                ..Self::default()
            }
        }

        fn request_kinds(&self) -> Vec<&'static str> {
            self.requests
                .lock()
                .expect("command requests")
                .iter()
                .map(|request| request.kind)
                .collect()
        }

        fn scope_types(&self) -> Vec<String> {
            self.requests
                .lock()
                .expect("command requests")
                .iter()
                .filter(|request| request.kind == "setMyCommands")
                .map(|request| {
                    request
                        .payload
                        .get("scope")
                        .and_then(|scope| scope.get("type"))
                        .and_then(serde_json::Value::as_str)
                        .expect("scope type")
                        .to_owned()
                })
                .collect()
        }
    }

    impl super::BotCommandSetupExecutor for CommandSetupStub {
        type Error = StartupError;

        fn delete_my_commands<'a>(
            &'a self,
            method: openplotva_telegram::DeleteBotCommands,
        ) -> super::BotCommandSetupFuture<'a, Self::Error> {
            self.requests
                .lock()
                .expect("command requests")
                .push(CommandRequest {
                    kind: "deleteMyCommands",
                    payload: serde_json::to_value(method).expect("deleteMyCommands JSON"),
                });
            let result = self
                .delete_error
                .map_or(Ok(()), |message| Err(StartupError(message)));
            Box::pin(async move { result })
        }

        fn set_my_commands<'a>(
            &'a self,
            scope: &'static str,
            method: openplotva_telegram::SetBotCommands,
        ) -> super::BotCommandSetupFuture<'a, Self::Error> {
            self.requests
                .lock()
                .expect("command requests")
                .push(CommandRequest {
                    kind: "setMyCommands",
                    payload: serde_json::to_value(method).expect("setMyCommands JSON"),
                });
            let result = if self.set_error_scope == Some(scope) {
                Err(StartupError(self.set_error.expect("set error")))
            } else {
                Ok(())
            };
            Box::pin(async move { result })
        }
    }

    #[derive(Clone, Copy, Default)]
    struct RateLimitStoreStub;

    impl RateLimitStore for RateLimitStoreStub {
        type Error = StartupError;

        fn load_expiry<'a>(
            &'a self,
            _chat_id: i64,
        ) -> RateLimitStoreFuture<'a, Result<Option<OffsetDateTime>, Self::Error>> {
            Box::pin(async { Ok(None) })
        }

        fn save_expiry<'a>(
            &'a self,
            _chat_id: i64,
            _expiry: OffsetDateTime,
            _ttl: Duration,
        ) -> RateLimitStoreFuture<'a, Result<(), Self::Error>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Clone, Default)]
    struct PermissionStoreStub {
        state: Arc<Mutex<PermissionStoreState>>,
    }

    #[derive(Default)]
    struct PermissionStoreState {
        context: ChatPermissionContext,
        saved: Vec<ChatSettingsUpdate>,
    }

    impl PermissionStoreStub {
        fn with_context(context: ChatPermissionContext) -> Self {
            let store = Self::default();
            store.state().context = context;
            store
        }

        fn saved_updates(&self) -> Vec<ChatSettingsUpdate> {
            self.state().saved.clone()
        }

        fn state(&self) -> MutexGuard<'_, PermissionStoreState> {
            match self.state.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            }
        }
    }

    impl ChatPermissionStore for PermissionStoreStub {
        type Error = StartupError;

        fn load_context<'a>(
            &'a self,
            _chat_id: i64,
        ) -> ChatPermissionStoreFuture<'a, Result<ChatPermissionContext, Self::Error>> {
            Box::pin(async move { Ok(self.state().context.clone()) })
        }

        fn save_settings<'a>(
            &'a self,
            update: ChatSettingsUpdate,
        ) -> ChatPermissionStoreFuture<'a, Result<(), Self::Error>> {
            Box::pin(async move {
                self.state().saved.push(update);
                Ok(())
            })
        }
    }

    fn queued_method_item(method: TelegramOutboundMethod) -> DispatcherWorkItem {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let message = DispatcherMessage::new(
            MessageFingerprint {
                chat_id: 42,
                message_type: "test".to_owned(),
                content_hash: 7,
                debounce_key: None,
            },
            "v1",
        )
        .with_method(method);
        queue.enqueue(message, true);
        queue.dequeue_immediate().expect("queued work item")
    }

    fn queued_bypass_method_item(method: TelegramOutboundMethod) -> DispatcherWorkItem {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let message = DispatcherMessage::new(
            MessageFingerprint {
                chat_id: 42,
                message_type: "test".to_owned(),
                content_hash: 7,
                debounce_key: None,
            },
            "v1",
        )
        .with_method(method)
        .with_bypass_chat_restrictions(true);
        queue.enqueue(message, true);
        queue.dequeue_immediate().expect("queued work item")
    }

    fn permission_error() -> TelegramOutboundExecuteError {
        let response: carapax::types::Response<serde_json::Value> = serde_json::from_str(
            r#"{"ok":false,"error_code":400,"description":"Bad Request: CHAT_WRITE_FORBIDDEN"}"#,
        )
        .expect("permission error JSON");
        match response.into_result() {
            Ok(_) => panic!("test response unexpectedly succeeded"),
            Err(error) => carapax::api::ExecuteError::Response(error).into(),
        }
    }

    fn reply_missing_error() -> TelegramOutboundExecuteError {
        let response: carapax::types::Response<serde_json::Value> = serde_json::from_str(
            r#"{"ok":false,"error_code":400,"description":"Bad Request: reply message not found"}"#,
        )
        .expect("reply missing error JSON");
        match response.into_result() {
            Ok(_) => panic!("test response unexpectedly succeeded"),
            Err(error) => carapax::api::ExecuteError::Response(error).into(),
        }
    }

    fn telegram_message(chat_id: i64, message_id: i64) -> TelegramMessage {
        serde_json::from_value(json!({
            "message_id": message_id,
            "date": 0,
            "chat": {
                "type": "supergroup",
                "id": chat_id,
                "title": "Plotva",
            },
        }))
        .expect("telegram message")
    }

    fn chat_settings_update_defaults(chat_id: i64) -> ChatSettingsUpdate {
        ChatSettingsUpdate {
            chat_id,
            chat_type: "supergroup".to_owned(),
            mood_alignment: Some("neutral".to_owned()),
            custom_persona: None,
            reactivity_percentage: 3,
            proactivity_percentage: 0,
            enable_global_text_reply: true,
            enable_global_draw_reply: true,
            enable_obscenifier: true,
            enable_profanity: true,
            enable_greet_joiners: false,
            enable_daily_game: true,
            daily_game_theme: "auto".to_owned(),
            greeting_html: None,
        }
    }

    fn lock<T>(mutex: &Arc<Mutex<T>>) -> MutexGuard<'_, T> {
        match mutex.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn sample_message_update(update_id: i64) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": update_id,
            "message": {
                "message_id": 77,
                "date": 1_710_000_000,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "text": "/start hello"
            }
        }))
    }

    fn unique_temp_file(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("current time after epoch")
                .as_nanos()
        ));
        path
    }
}
