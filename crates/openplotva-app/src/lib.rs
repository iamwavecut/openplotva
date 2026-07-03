//! Composition root for the OpenPlotva application shell.

pub mod activity;
pub mod admin;
pub mod agent_runtime;
pub mod callbacks;
pub mod checkin;
pub mod control_jobs;
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
pub mod guest;
pub mod help;
pub mod history_summary;
pub mod image_jobs;
pub mod ingestion_telemetry;
pub mod inline;
pub mod media;
pub mod members;
pub mod memory_runtime;
pub mod message_gate;
pub mod model_routing;
pub mod music_jobs;
pub mod payments;
pub mod permissions;
pub mod rate_limits;
pub mod rates;
pub mod reactions;
pub mod reset;
pub mod rich;
mod routed_attempts;
pub mod runtime_api;
mod runtime_cache;
mod runtime_dispatcher;
mod runtime_entities;
mod runtime_gemini_cache;
mod runtime_llm;
mod runtime_llm_analytics;
mod runtime_retention;
mod runtime_routing;
mod runtime_safety;
mod runtime_sql;
mod runtime_taskman;
mod runtime_updates;
mod runtime_virtual_dialog;
pub mod serper;
pub mod settings;
pub mod skipped;
pub mod subscription_sync;
pub mod task_queue;
pub mod translate;
pub mod updates;
pub mod virtual_messages;
pub mod vision;
pub mod youtube;

pub use runtime_dispatcher::{
    DISPATCH_FAILURE_CLASS_CHAT_RATE_LIMITED, DISPATCH_FAILURE_CLASS_MISSING_METHOD,
    DispatchFailureRecord, DispatchFailureRing, reply_message_id_from_fingerprint_key,
};

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    convert::Infallible,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use anyhow::Context as _;
use axum::{
    body::Bytes,
    extract::{Extension, Path, RawQuery},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{any, get},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use futures_util::{Stream, stream};
use openplotva_config::AppConfig;
use openplotva_server::{ReadinessCheck, ReadinessResponse};
use openplotva_storage::{
    PostgresChatMemberStore, PostgresChatSettingsStore, PostgresHistoryStore, PostgresMemoryStore,
    PostgresPaymentStore, PostgresRuntimeTokenStore, PostgresRuntimeVirtualDialogStore,
    PostgresShieldStore, PostgresTelegramFileStore, PostgresVipStore, PostgresVirtualMessageStore,
    RedisBlockedChatStore, RedisEphemeralMessageStore, RedisRateLimitStore, ServiceClients,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{PgPool, Row, postgres::PgRow};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Interval, timeout},
};

const GO_DISPATCHER_MAX_QUEUE_SIZE: usize = 10_000;
const GO_DISPATCHER_DEBOUNCE_WINDOW: Duration = Duration::from_secs(3);
const GO_DISPATCHER_DEBOUNCE_CACHE_SIZE: usize = 1_000;
const GO_WEBHOOK_DELETE_ON_STOP_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct RuntimeWorkers {
    handles: Vec<JoinHandle<()>>,
    stop: Option<watch::Sender<bool>>,
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
    routing_event_buffer: Option<runtime_routing::RoutingEventBuffer>,
    routing_event_reporter: Option<runtime_routing::RoutingEventReporter>,
    runtime_api_tls_public_key_pin: Option<String>,
    router_handle: Option<Arc<openplotva_llm::router::RouterHandle>>,
    router_breakers: Option<Arc<openplotva_llm::router::BreakerSet>>,
    router_triggers: Option<Arc<openplotva_llm::router::TriggerState>>,
    router_pools: Option<Arc<openplotva_llm::router::PoolRegistry>>,
    router_runtime: Option<Arc<model_routing::RouterRuntime>>,
    dialog_worker_gauge: Option<Arc<dialog_workers::WorkerGauge>>,
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

/// Report from the app-level long-polling update producer startup task.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TelegramUpdateProducerStartupReport {
    /// `deleteWebhook` error that stopped the producer before polling.
    pub delete_webhook_error: Option<String>,
    /// `setWebhook` error that stopped the producer before accepting webhook updates.
    pub set_webhook_error: Option<String>,
    /// Report from the producer loop when startup succeeded.
    pub producer: Option<openplotva_updates::UpdateProducerRunReport>,
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

#[derive(Clone)]
struct TelegramWebhookRoute {
    sender: openplotva_telegram::WebhookUpdateSender,
    secret_token: Arc<str>,
}

#[derive(Clone)]
struct StaticWebRoutes {
    admin_ids: Arc<[i64]>,
    bot_token: Arc<str>,
    require_settings_init_data: bool,
    webapp_url: Arc<str>,
    bot_username: Arc<str>,
    bot_id: Option<i64>,
    default_log_level: Arc<str>,
    postgres: Option<PgPool>,
    redis: Option<redis::Client>,
    log_buffer: Option<Arc<openplotva_observability::RuntimeLogBuffer>>,
    telegram: Option<openplotva_telegram::TelegramClient>,
    dispatcher_inspector: runtime_dispatcher::RuntimeDispatcherInspectorHandle,
    cache_inspector: runtime_cache::RuntimeCacheInspectorHandle,
    taskman_inspector: runtime_taskman::RuntimeTaskmanInspectorHandle,
    llm_trace_buffer: Option<runtime_llm::RuntimeLlmTraceBuffer>,
    routing_event_buffer: Option<runtime_routing::RoutingEventBuffer>,
    routing_event_reporter: Option<runtime_routing::RoutingEventReporter>,
    llm_discovery_base_url: Arc<str>,
    llm_discovery_service_name: Arc<str>,
    runtime_sql_timeout_ms: i32,
    state_store: Option<PostgresVirtualMessageStore>,
    settings_store: Option<PostgresChatSettingsStore>,
    member_store: Option<PostgresChatMemberStore>,
    vip_store: Option<PostgresVipStore>,
    vip_status: Option<Arc<dyn payments::VipStatusChecker + Send + Sync>>,
    memory_store: Option<PostgresMemoryStore>,
    memory_admin_enabled: bool,
    memory_retention: Duration,
    memory_enqueue_policy: openplotva_memory::MemoryRunEnqueuePolicy,
    memory_consolidation_model: Arc<str>,
    memory_max_input_tokens: i32,
    memory_max_messages_per_run: i32,
    memory_token_estimator_source: Arc<str>,
    memory_restart_trigger: Option<Arc<tokio::sync::Notify>>,
    memory_override_runtime: Option<AdminMemoryOverrideRuntime>,
    shield_store: Option<PostgresShieldStore>,
    shield_options: openplotva_shield::Options,
    shield_embedder: Option<Arc<dyn memory_runtime::EmbeddingProvider>>,
    router_handle: Option<Arc<openplotva_llm::router::RouterHandle>>,
    router_breakers: Option<Arc<openplotva_llm::router::BreakerSet>>,
    router_triggers: Option<Arc<openplotva_llm::router::TriggerState>>,
    router_pools: Option<Arc<openplotva_llm::router::PoolRegistry>>,
    router_runtime: Option<Arc<model_routing::RouterRuntime>>,
    dialog_worker_gauge: Option<Arc<dialog_workers::WorkerGauge>>,
}

#[derive(Clone)]
struct AdminMemoryOverrideRuntime {
    config: Arc<AppConfig>,
    store: PostgresMemoryStore,
    lock: Arc<tokio::sync::Mutex<()>>,
    router_handle: Arc<openplotva_llm::router::RouterHandle>,
    router_breakers: Arc<openplotva_llm::router::BreakerSet>,
    router_triggers: Arc<openplotva_llm::router::TriggerState>,
    router_pools: Arc<openplotva_llm::router::PoolRegistry>,
    routing_event_reporter: Option<runtime_routing::RoutingEventReporter>,
}

fn static_web_routes(
    admin_ids: Vec<i64>,
    bot_token: impl Into<String>,
    webapp_url: impl Into<String>,
    bot_username: impl Into<String>,
    state_store: Option<PostgresVirtualMessageStore>,
) -> StaticWebRoutes {
    StaticWebRoutes {
        admin_ids: Arc::from(admin_ids),
        bot_token: Arc::from(bot_token.into()),
        require_settings_init_data: false,
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
        memory_token_estimator_source: Arc::from(""),
        memory_restart_trigger: None,
        memory_override_runtime: None,
        shield_store: None,
        shield_options: openplotva_shield::Options::default(),
        shield_embedder: None,
        router_handle: None,
        router_breakers: None,
        router_triggers: None,
        router_pools: None,
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
) -> StaticWebRoutes {
    let mut routes = static_web_routes(
        config.admins.admin_ids.clone(),
        config.bot.key.clone().unwrap_or_default(),
        config.server.url.clone(),
        bot_username,
        service_clients.map(|clients| PostgresVirtualMessageStore::new(clients.postgres.clone())),
    );
    routes.require_settings_init_data = config.server.require_settings_init_data;
    routes.default_log_level = Arc::from(config.observability.log_level.clone());
    routes.log_buffer = Some(log_buffer);
    routes.telegram = runtime_workers.telegram.clone();
    routes.bot_id = runtime_workers.bot_id;
    routes.dispatcher_inspector = runtime_workers.dispatcher_inspector.clone();
    routes.cache_inspector = runtime_workers.cache_inspector.clone();
    routes.taskman_inspector = runtime_workers.taskman_inspector.clone();
    routes.llm_trace_buffer = runtime_workers.llm_trace_buffer.clone();
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
    routes.memory_token_estimator_source = Arc::from(memory_token_estimator_source(config));
    routes.memory_restart_trigger = runtime_workers.memory_restart_trigger.clone();
    routes.router_handle = runtime_workers.router_handle.clone();
    routes.router_breakers = runtime_workers.router_breakers.clone();
    routes.router_triggers = runtime_workers.router_triggers.clone();
    routes.router_pools = runtime_workers.router_pools.clone();
    routes.router_runtime = runtime_workers.router_runtime.clone();
    routes.dialog_worker_gauge = runtime_workers.dialog_worker_gauge.clone();
    routes.shield_options = shield_options_from_config(&config.shield);
    if let Some(clients) = service_clients {
        routes.postgres = Some(clients.postgres.clone());
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
        let memory_store = PostgresMemoryStore::new(clients.postgres.clone());
        routes.memory_store = Some(memory_store.clone());
        if config.memory.enabled
            && let (
                Some(router_handle),
                Some(router_breakers),
                Some(router_triggers),
                Some(router_pools),
            ) = (
                routes.router_handle.clone(),
                routes.router_breakers.clone(),
                routes.router_triggers.clone(),
                routes.router_pools.clone(),
            )
        {
            routes.memory_override_runtime = Some(AdminMemoryOverrideRuntime {
                config: Arc::new(config.clone()),
                store: memory_store,
                lock: Arc::new(tokio::sync::Mutex::new(())),
                router_handle,
                router_breakers,
                router_triggers,
                router_pools,
                routing_event_reporter: routes.routing_event_reporter.clone(),
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

fn memory_token_estimator_source(config: &AppConfig) -> String {
    let base = config
        .memory
        .token_estimator_url
        .trim()
        .trim_end_matches('/');
    if base.is_empty() {
        "heuristic".to_owned()
    } else {
        format!("http-token-estimator:{base}/estimate")
    }
}

/// Build the HTTP router without binding a socket.
pub fn router() -> axum::Router {
    router_with_readiness_and_static_web(
        ReadinessResponse::ready(Vec::new()),
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
        static_web_routes(Vec::new(), "", "", "", None),
        route,
    )
}

fn router_with_readiness_and_static_web(
    readiness: ReadinessResponse,
    static_web: StaticWebRoutes,
) -> axum::Router {
    install_static_web_routes(
        openplotva_server::router_with_readiness(readiness),
        static_web,
    )
}

fn router_with_readiness_static_web_and_telegram_webhook_route(
    readiness: ReadinessResponse,
    static_web: StaticWebRoutes,
    route: TelegramWebhookRoute,
) -> axum::Router {
    router_with_readiness_and_static_web(readiness, static_web)
        .route(
            openplotva_telegram::TELEGRAM_WEBHOOK_PATH,
            any(telegram_webhook),
        )
        .layer(Extension(route))
}

fn install_static_web_routes(router: axum::Router, static_web: StaticWebRoutes) -> axum::Router {
    router
        .route("/settings", get(settings_redirect))
        .route("/settings/", get(settings_index))
        .route("/settings/{*path}", get(settings_asset))
        .route("/api/settings", any(settings_api))
        .route("/api/settings/", any(settings_api))
        .route("/api/settings/deputies", any(settings_deputies_api))
        .route("/api/settings/deputies/", any(settings_deputies_api))
        .route(
            "/api/settings/deputies/candidates",
            any(settings_deputy_candidates_api),
        )
        .route(
            "/api/settings/deputies/candidates/",
            any(settings_deputy_candidates_api),
        )
        .route("/api/settings/memory", any(settings_memory_api))
        .route("/api/settings/memory/", any(settings_memory_api))
        .route("/api/chats", any(settings_chats_api))
        .route("/admin/api/auth", any(admin_auth))
        .route("/admin/api/auth_check", get(admin_auth_check))
        .route("/admin/api/state", any(admin_state))
        .route("/admin/api/loglevel", any(admin_loglevel))
        .route("/admin/api/routing", any(admin_routing))
        .route("/admin/api/routing/status", any(admin_routing_status))
        .route("/admin/api/routing/events", any(admin_routing_events))
        .route("/admin/api/logs/stream", any(admin_logs_stream))
        .route("/admin/api/bootstrap", get(admin_bootstrap))
        .route("/admin/api/metrics", any(admin_state))
        .route("/admin/api/llm/requests", any(admin_llm_requests))
        .route(
            "/admin/api/llm/requests/clear",
            any(admin_llm_requests_clear),
        )
        .route("/admin/api/safety/checks", any(admin_safety_checks))
        .route(
            "/admin/api/analytics/llm/summary",
            any(admin_llm_analytics_summary),
        )
        .route("/admin/api/memory/cards", any(admin_memory_cards))
        .route("/admin/api/memory/runs", any(admin_memory_runs))
        .route("/admin/api/memory/restart", any(admin_memory_restart))
        .route("/admin/api/memory/card", any(admin_memory_card))
        .route("/admin/api/memory/overview", any(admin_memory_overview))
        .route("/admin/api/shield/documents", any(admin_shield_documents))
        .route(
            "/admin/api/shield/embeddings/rebuild",
            any(admin_shield_embeddings_rebuild),
        )
        .route("/admin/api/shield/test", any(admin_shield_test))
        .route("/admin/api/redis/list", any(admin_redis_list))
        .route("/admin/api/redis/get", any(admin_redis_get))
        .route(
            "/admin/api/redis/delete_prefix",
            any(admin_redis_delete_prefix),
        )
        .route("/admin/api/redis/prefixes", any(admin_redis_prefixes))
        .route("/admin/api/redis/delete_key", any(admin_redis_delete_key))
        .route("/admin/api/redis/flushdb", any(admin_redis_flushdb))
        .route("/admin/api/chat", any(admin_chat_get))
        .route("/admin/api/chat/settings", any(admin_chat_settings))
        .route("/admin/api/chat/block", any(admin_chat_block))
        .route("/admin/api/chat/unblock", any(admin_chat_unblock))
        .route("/admin/api/chat/members", any(admin_chat_members))
        .route("/admin/api/chats", any(admin_chats_list))
        .route(
            "/admin/api/chats/search_by_member",
            any(admin_chats_search_by_member),
        )
        .route("/admin/api/users", any(admin_users_list))
        .route("/admin/api/vip/users", any(admin_vip_users_list))
        .route("/admin/api/user", any(admin_user_get))
        .route("/admin/api/user/grant_vip", any(admin_user_grant_vip))
        .route("/admin/api/user/revoke_vip", any(admin_user_revoke_vip))
        .route("/admin/api/user/delete", any(admin_user_delete))
        .route("/admin/api/taskman/jobs", any(admin_taskman_jobs))
        .route(
            "/admin/api/taskman/jobs/clear",
            any(admin_taskman_jobs_clear),
        )
        .route("/admin/api/taskman/job", any(admin_taskman_job))
        .route(
            "/admin/api/taskman/job/cancel",
            any(admin_taskman_job_cancel),
        )
        .route(
            "/admin/api/taskman/job/restart",
            any(admin_taskman_job_restart),
        )
        .route("/admin", get(admin_redirect))
        .route("/admin/", get(admin_index))
        .route("/admin/{*path}", get(admin_asset))
        .layer(Extension(static_web))
}

#[cfg(test)]
const GO_ADMIN_API_ROUTE_PATTERNS: &[&str] = &[
    "/admin/api/auth",
    "/admin/api/auth_check",
    "/admin/api/state",
    "/admin/api/loglevel",
    "/admin/api/logs/stream",
    "/admin/api/bootstrap",
    "/admin/api/metrics",
    "/admin/api/llm/requests",
    "/admin/api/llm/requests/clear",
    "/admin/api/safety/checks",
    "/admin/api/analytics/llm/summary",
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

async fn settings_redirect() -> Response {
    moved_permanently("/settings/")
}

async fn admin_redirect() -> Response {
    moved_permanently("/admin/")
}

async fn settings_index() -> Response {
    static_web_asset_response(openplotva_web::StaticAssetGroup::Settings, "")
}

async fn settings_asset(Path(path): Path<String>) -> Response {
    static_web_asset_response(openplotva_web::StaticAssetGroup::Settings, &path)
}

async fn settings_api(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
    body: Bytes,
) -> Response {
    let init_data = settings_init_data_header(&headers);
    match method {
        Method::OPTIONS => settings_options_response(),
        Method::GET => settings_get_response(&routes, raw_query.as_deref(), init_data).await,
        Method::POST | Method::PUT => settings_update_response(&routes, &body, init_data).await,
        _ => settings_error_response(StatusCode::METHOD_NOT_ALLOWED, "Method not allowed"),
    }
}

async fn settings_deputies_api(
    method: Method,
    headers: HeaderMap,
    RawQuery(_raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
    body: Bytes,
) -> Response {
    let init_data = settings_init_data_header(&headers);
    match method {
        Method::OPTIONS => settings_side_options_response("GET, PUT, OPTIONS"),
        Method::PUT => settings_deputies_update_response(&routes, &body, init_data).await,
        _ => settings_side_error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "Method not allowed",
            "GET, PUT, OPTIONS",
        ),
    }
}

async fn settings_deputy_candidates_api(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    let init_data = settings_init_data_header(&headers);
    match method {
        Method::OPTIONS => settings_side_options_response("GET, PUT, OPTIONS"),
        Method::GET => {
            settings_deputy_candidates_response(&routes, raw_query.as_deref(), init_data).await
        }
        _ => settings_side_error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "Method not allowed",
            "GET, PUT, OPTIONS",
        ),
    }
}

async fn settings_memory_api(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    let init_data = settings_init_data_header(&headers);
    match method {
        Method::OPTIONS => settings_side_options_response("GET, DELETE, OPTIONS"),
        Method::GET => settings_memory_get_response(&routes, raw_query.as_deref(), init_data).await,
        Method::DELETE => {
            settings_memory_delete_response(&routes, raw_query.as_deref(), init_data).await
        }
        _ => settings_side_error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
            "GET, DELETE, OPTIONS",
        ),
    }
}

async fn settings_chats_api(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    if method != Method::GET {
        return (StatusCode::METHOD_NOT_ALLOWED, "Method not allowed\n").into_response();
    }
    let init_data = settings_init_data_header(&headers);
    settings_chats_response(&routes, raw_query.as_deref(), init_data).await
}

async fn admin_auth(
    method: Method,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_auth_response(&routes, method, raw_query.as_deref()).await
}

async fn admin_auth_check(
    Extension(routes): Extension<StaticWebRoutes>,
    headers: HeaderMap,
) -> Response {
    let authenticated_user_id = admin_session_user_id(&headers, &routes.bot_token)
        .filter(|user_id| routes.admin_ids.contains(user_id));
    match authenticated_user_id {
        Some(user_id) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({
                "authenticated": true,
                "user_id": user_id,
            }),
        ),
        None => admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "authenticated": false }),
        ),
    }
}

async fn admin_bootstrap(Extension(routes): Extension<StaticWebRoutes>) -> Response {
    admin_json_response(
        StatusCode::OK,
        serde_json::json!({
            "webapp_url": routes.webapp_url.as_ref(),
            "bot_username": routes.bot_username.as_ref(),
        }),
    )
}

async fn admin_state(Extension(routes): Extension<StaticWebRoutes>) -> Response {
    admin_state_response(&routes).await
}

async fn admin_loglevel(
    method: Method,
    headers: HeaderMap,
    Extension(routes): Extension<StaticWebRoutes>,
    body: Bytes,
) -> Response {
    admin_loglevel_response(&routes, method, &headers, &body).await
}

async fn admin_logs_stream(
    headers: HeaderMap,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(buffer) = routes.log_buffer.clone() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    Sse::new(admin_logs_sse_stream(buffer))
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response()
}

async fn admin_llm_requests(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_llm_requests_response(&routes, method, &headers, raw_query.as_deref())
}

async fn admin_llm_requests_clear(
    method: Method,
    headers: HeaderMap,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_llm_requests_clear_response(&routes, method, &headers)
}

async fn admin_safety_checks(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_safety_checks_response(&routes, method, &headers, raw_query.as_deref()).await
}

async fn admin_llm_analytics_summary(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_llm_analytics_summary_response(&routes, method, &headers, raw_query.as_deref()).await
}

async fn admin_memory_cards(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_memory_cards_response(&routes, method, &headers, raw_query.as_deref()).await
}

async fn admin_memory_card(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
    body: Bytes,
) -> Response {
    admin_memory_card_response(&routes, method, &headers, raw_query.as_deref(), &body).await
}

async fn admin_memory_overview(
    method: Method,
    headers: HeaderMap,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    admin_memory_overview_response(&routes, &headers).await
}

async fn admin_memory_runs(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
    body: Bytes,
) -> Response {
    admin_memory_runs_response(&routes, method, &headers, raw_query.as_deref(), &body).await
}

async fn admin_memory_restart(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
    body: Bytes,
) -> Response {
    admin_memory_restart_response(&routes, method, &headers, raw_query.as_deref(), &body).await
}

async fn admin_shield_documents(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
    body: Bytes,
) -> Response {
    admin_shield_documents_response(&routes, method, &headers, raw_query.as_deref(), &body).await
}

async fn admin_shield_embeddings_rebuild(
    method: Method,
    headers: HeaderMap,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_shield_embeddings_rebuild_response(&routes, method, &headers).await
}

async fn admin_shield_test(
    method: Method,
    headers: HeaderMap,
    Extension(routes): Extension<StaticWebRoutes>,
    body: Bytes,
) -> Response {
    admin_shield_test_response(&routes, method, &headers, &body).await
}

async fn admin_taskman_jobs(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_taskman_jobs_response(&routes, method, &headers, raw_query.as_deref())
}

async fn admin_taskman_jobs_clear(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_taskman_jobs_clear_response(&routes, method, &headers, raw_query.as_deref())
}

async fn admin_taskman_job(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_taskman_job_response(&routes, method, &headers, raw_query.as_deref()).await
}

async fn admin_taskman_job_cancel(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_taskman_job_cancel_response(&routes, method, &headers, raw_query.as_deref())
}

async fn admin_taskman_job_restart(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_taskman_job_restart_response(&routes, method, &headers, raw_query.as_deref())
}

async fn admin_redis_list(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let pattern = admin_auth_query_values(raw_query.as_deref())
        .remove("pattern")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "*".to_owned());
    let Some(redis) = routes.redis_inspector() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    match redis.scan_all_keys(&pattern).await {
        Ok(keys) => admin_json_response(StatusCode::OK, serde_json::json!({ "keys": keys })),
        Err(error) => {
            tracing::warn!(%error, pattern, "failed to list admin redis keys");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

async fn admin_redis_get(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query.as_deref());
    let Some(key) = values.get("key").filter(|value| !value.is_empty()) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "key required");
    };
    let Some(redis) = routes.redis_inspector() else {
        return admin_error_response(StatusCode::NOT_FOUND, "not found");
    };
    match redis.raw_value(key).await {
        Ok(Some(value)) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "key": key, "value": value }),
        ),
        Ok(None) => admin_error_response(StatusCode::NOT_FOUND, "not found"),
        Err(error) => {
            tracing::warn!(%error, key, "failed to get admin redis value");
            admin_error_response(StatusCode::NOT_FOUND, "not found")
        }
    }
}

async fn admin_redis_delete_prefix(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    if method != Method::DELETE {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query.as_deref());
    let Some(prefix) = values.get("prefix").filter(|value| !value.is_empty()) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "prefix required");
    };
    let Some(redis) = routes.redis_inspector() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    match redis.delete_by_prefix(prefix).await {
        Ok(()) => admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true })),
        Err(error) => {
            tracing::warn!(%error, prefix, "failed to delete admin redis prefix");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

async fn admin_redis_prefixes(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let prefix = admin_auth_query_values(raw_query.as_deref())
        .remove("prefix")
        .unwrap_or_default();
    let Some(redis) = routes.redis_inspector() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    match redis.scan_all_keys(&format!("{prefix}*")).await {
        Ok(keys) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "groups": admin_redis_prefix_groups_from_keys(&prefix, keys) }),
        ),
        Err(error) => {
            tracing::warn!(%error, prefix, "failed to list admin redis prefixes");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

async fn admin_redis_delete_key(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    if method != Method::DELETE {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query.as_deref());
    let Some(key) = values.get("key").filter(|value| !value.is_empty()) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "key required");
    };
    let Some(redis) = routes.redis_inspector() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    match redis.delete_key(key).await {
        Ok(()) => admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true })),
        Err(error) => {
            tracing::warn!(%error, key, "failed to delete admin redis key");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

async fn admin_redis_flushdb(
    method: Method,
    headers: HeaderMap,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    if method != Method::POST {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(redis) = routes.redis_inspector() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    match redis.flushdb().await {
        Ok(()) => admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true })),
        Err(error) => {
            tracing::warn!(%error, "failed to flush admin redis db");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

async fn admin_chat_get(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_chat_get_response(&routes, method, &headers, raw_query.as_deref()).await
}

async fn admin_chat_settings(
    method: Method,
    headers: HeaderMap,
    Extension(routes): Extension<StaticWebRoutes>,
    body: Bytes,
) -> Response {
    admin_chat_settings_response(&routes, method, &headers, &body).await
}

async fn admin_chat_block(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_chat_block_response(&routes, method, &headers, raw_query.as_deref()).await
}

async fn admin_chat_unblock(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_chat_unblock_response(&routes, method, &headers, raw_query.as_deref()).await
}

async fn admin_chat_members(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_chat_members_response(&routes, method, &headers, raw_query.as_deref()).await
}

async fn admin_chats_list(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_chats_list_response(&routes, method, &headers, raw_query.as_deref()).await
}

async fn admin_chats_search_by_member(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_chats_search_by_member_response(&routes, method, &headers, raw_query.as_deref()).await
}

async fn admin_users_list(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_users_list_response(&routes, method, &headers, raw_query.as_deref()).await
}

async fn admin_vip_users_list(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_vip_users_list_response(&routes, method, &headers, raw_query.as_deref()).await
}

async fn admin_user_get(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_user_get_response(&routes, method, &headers, raw_query.as_deref()).await
}

async fn admin_user_grant_vip(
    method: Method,
    headers: HeaderMap,
    Extension(routes): Extension<StaticWebRoutes>,
    body: Bytes,
) -> Response {
    admin_user_grant_vip_response(&routes, method, &headers, &body).await
}

async fn admin_user_revoke_vip(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_user_revoke_vip_response(&routes, method, &headers, raw_query.as_deref()).await
}

async fn admin_user_delete(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    admin_user_delete_response(&routes, method, &headers, raw_query.as_deref()).await
}

async fn admin_index(
    Extension(routes): Extension<StaticWebRoutes>,
    headers: HeaderMap,
) -> Response {
    admin_web_asset_response(&routes, &headers, "")
}

async fn admin_asset(
    Path(path): Path<String>,
    Extension(routes): Extension<StaticWebRoutes>,
    headers: HeaderMap,
) -> Response {
    admin_web_asset_response(&routes, &headers, &path)
}

fn admin_web_asset_response(routes: &StaticWebRoutes, headers: &HeaderMap, path: &str) -> Response {
    if admin_static_asset_requires_auth(path)
        && !admin_session_is_authorized(headers, &routes.admin_ids, &routes.bot_token)
    {
        return found("/admin/login.html");
    }
    static_web_asset_response(openplotva_web::StaticAssetGroup::Admin, path)
}

fn static_web_asset_response(group: openplotva_web::StaticAssetGroup, path: &str) -> Response {
    let Some(asset) = openplotva_web::static_asset(group, path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, asset.content_type),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        asset.bytes,
    )
        .into_response()
}

fn moved_permanently(location: &'static str) -> Response {
    (
        StatusCode::MOVED_PERMANENTLY,
        [(header::LOCATION, location)],
    )
        .into_response()
}

fn found(location: &'static str) -> Response {
    (StatusCode::FOUND, [(header::LOCATION, location)]).into_response()
}

#[derive(Debug, Deserialize)]
struct SettingsUpdateRequest {
    chat_id: i64,
    #[serde(default)]
    user_id: i64,
    mood_alignment: Option<String>,
    custom_persona: Option<String>,
    reactivity_percentage: Option<i32>,
    proactivity_percentage: Option<i32>,
    enable_obscenifier: Option<bool>,
    enable_profanity: Option<bool>,
    enable_greet_joiners: Option<bool>,
    enable_global_text_reply: Option<bool>,
    enable_global_draw_reply: Option<bool>,
    signature: String,
    enable_daily_game: Option<bool>,
    daily_game_theme: Option<String>,
    greeting_html: Option<String>,
    disable_random_reactivity: Option<bool>,
    hide_original_draw_prompt: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SettingsDeputySummary {
    id: i64,
    first_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    username: Option<String>,
    status: String,
    display_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SettingsResponseBody {
    chat_id: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    chat_title: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    chat_type: String,
    mood_alignment: Option<String>,
    custom_persona: Option<String>,
    reactivity_percentage: i32,
    proactivity_percentage: i32,
    enable_obscenifier: bool,
    enable_profanity: bool,
    enable_greet_joiners: bool,
    enable_global_text_reply: bool,
    enable_global_draw_reply: bool,
    enable_daily_game: bool,
    daily_game_theme: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    greeting_html: Option<String>,
    is_vip: bool,
    disable_random_reactivity: bool,
    hide_original_draw_prompt: bool,
    is_deputy: bool,
    can_manage_deputies: bool,
    deputies: Vec<SettingsDeputySummary>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SettingsAccess {
    chat_id: i64,
    user_id: i64,
    is_global_admin: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SettingsAccessError {
    InvalidChatId,
    MissingSignature,
    InvalidSignature,
}

#[derive(Debug, Deserialize)]
struct DeputyUpdateRequest {
    chat_id: i64,
    user_id: i64,
    signature: String,
    #[serde(default)]
    deputy_ids: Vec<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct DeputyCandidatesResponse {
    items: Vec<SettingsDeputySummary>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct DeputyUpdateResponse {
    ok: bool,
    deputies: Vec<SettingsDeputySummary>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DeputyOwnerAccess {
    chat_id: i64,
    user_id: i64,
    is_global_admin: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ChatListItem {
    id: i64,
    title: String,
    #[serde(rename = "type")]
    chat_type: String,
}

#[derive(Clone, Debug, Serialize)]
struct SettingsMemoryResponse {
    chat_id: i64,
    user_id: i64,
    count: usize,
    cards: Vec<openplotva_memory::Card>,
}

async fn settings_get_response(
    routes: &StaticWebRoutes,
    raw_query: Option<&str>,
    init_data: Option<&str>,
) -> Response {
    let Some(settings_store) = &routes.settings_store else {
        return settings_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
        );
    };
    let Some(member_store) = &routes.member_store else {
        return settings_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
        );
    };

    let values = admin_auth_query_values(raw_query);
    let claimed_user_id = values
        .get("user_id")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    if authenticate_settings_init_data(routes, init_data, claimed_user_id).is_err() {
        return settings_error_response(StatusCode::UNAUTHORIZED, "Unauthorized");
    }
    let access = match parse_settings_get_access(&values, &routes.admin_ids) {
        Ok(access) => access,
        Err(error) => return settings_access_error_response(error),
    };

    if let Err(error) = ensure_settings_chat_available(routes, access.chat_id, access.user_id).await
    {
        return error;
    }
    if let Err(error) = authorize_settings_user(routes, access).await {
        return error;
    }

    let mut settings = match settings_store.get_chat_settings(access.chat_id).await {
        Ok(Some(settings)) => settings,
        Ok(None) | Err(_) => openplotva_core::ChatSettings::defaults(access.chat_id),
    };
    settings.custom_persona =
        openplotva_web::truncate_custom_persona(settings.custom_persona.take());

    let (chat_title, chat_type) =
        settings_chat_display_for_read(routes, access.chat_id, access.user_id).await;
    let mut response = new_settings_response(&settings, chat_title, chat_type.clone());
    apply_private_chat_response_defaults(&mut response, access.chat_id, access.user_id);
    apply_user_settings_response(routes, &mut response, access.user_id).await;
    apply_deputy_settings_response(
        member_store,
        &mut response,
        access.chat_id,
        access.user_id,
        &chat_type,
        access.is_global_admin,
    )
    .await;
    settings_json_response(StatusCode::OK, &response)
}

async fn settings_update_response(
    routes: &StaticWebRoutes,
    body: &[u8],
    init_data: Option<&str>,
) -> Response {
    let Some(settings_store) = &routes.settings_store else {
        return settings_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
        );
    };
    let req = match parse_settings_update_request(body) {
        Ok(req) => req,
        Err(response) => return *response,
    };
    if authenticate_settings_init_data(routes, init_data, req.user_id).is_err() {
        return settings_error_response(StatusCode::UNAUTHORIZED, "Unauthorized");
    }
    if let Err(error) = ensure_settings_chat_available(routes, req.chat_id, req.user_id).await {
        return error;
    }
    let chat = match settings_update_chat(routes, req.chat_id, req.user_id).await {
        Ok(chat) => chat,
        Err(response) => return response,
    };
    let access = SettingsAccess {
        chat_id: req.chat_id,
        user_id: req.user_id,
        is_global_admin: routes.admin_ids.contains(&req.user_id),
    };
    if let Err(error) = authorize_settings_user(routes, access).await {
        return error;
    }

    let update = settings_update_from_request(settings_store, &req, &chat).await;
    if let Err(error) = settings_store.upsert_chat_settings(&update).await {
        tracing::error!(%error, chat_id = req.chat_id, "failed to update chat settings");
        return settings_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to update settings",
        );
    }
    update_user_settings_from_request(routes, &req).await;
    settings_json_response(StatusCode::OK, &serde_json::json!({ "status": "success" }))
}

async fn settings_chats_response(
    routes: &StaticWebRoutes,
    raw_query: Option<&str>,
    init_data: Option<&str>,
) -> Response {
    let values = admin_auth_query_values(raw_query);
    let Some(user_id_raw) = values.get("user_id") else {
        return settings_json_raw_response(
            StatusCode::BAD_REQUEST,
            r#"{"error":"missing user_id"}"#,
        );
    };
    let Ok(user_id) = user_id_raw.parse::<i64>() else {
        return settings_json_raw_response(
            StatusCode::BAD_REQUEST,
            r#"{"error":"invalid user_id"}"#,
        );
    };
    if authenticate_settings_init_data(routes, init_data, user_id).is_err() {
        return settings_json_raw_response(StatusCode::UNAUTHORIZED, r#"{"error":"unauthorized"}"#);
    }
    let Some(signature) = values.get("signature").filter(|value| !value.is_empty()) else {
        return settings_json_raw_response(
            StatusCode::BAD_REQUEST,
            r#"{"error":"missing signature"}"#,
        );
    };
    if !openplotva_web::validate_settings_access_signature(user_id, 0, signature) {
        return settings_json_raw_response(
            StatusCode::FORBIDDEN,
            r#"{"error":"invalid signature"}"#,
        );
    }
    let Some(settings_store) = &routes.settings_store else {
        return settings_json_response(StatusCode::OK, &Vec::<ChatListItem>::new());
    };
    let Some(member_store) = &routes.member_store else {
        return settings_json_response(StatusCode::OK, &Vec::<ChatListItem>::new());
    };
    let chats = managed_chat_list_items(routes, settings_store, member_store, user_id).await;
    settings_json_response(StatusCode::OK, &chats)
}

async fn settings_deputy_candidates_response(
    routes: &StaticWebRoutes,
    raw_query: Option<&str>,
    init_data: Option<&str>,
) -> Response {
    let access = match parse_deputy_owner_access(routes, raw_query, init_data).await {
        Ok(access) => access,
        Err(response) => return response,
    };
    if !access.is_global_admin
        && let Err(response) =
            ensure_deputy_management_permission(routes, access.chat_id, access.user_id).await
    {
        return response;
    }
    let query_values = admin_auth_query_values(raw_query);
    let query = query_values
        .get("query")
        .or_else(|| query_values.get("q"))
        .map(|value| value.trim())
        .unwrap_or("");
    let limit = parse_deputy_candidates_limit(query_values.get("limit").map(String::as_str));
    let Some(member_store) = &routes.member_store else {
        return settings_side_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
            "GET, PUT, OPTIONS",
        );
    };
    let rows = match member_store
        .search_chat_member_candidates(access.chat_id, query, limit as i32)
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(%error, chat_id = access.chat_id, user_id = access.user_id, "failed to search deputy candidates");
            return settings_side_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to search deputy candidates",
                "GET, PUT, OPTIONS",
            );
        }
    };
    let items = rows
        .into_iter()
        .filter(|row| row.id != 0 && row.id != access.user_id)
        .map(|row| SettingsDeputySummary {
            id: row.id,
            display_name: build_deputy_display_name(
                row.id,
                &row.first_name,
                row.last_name.as_deref(),
                row.username.as_deref(),
            ),
            first_name: row.first_name,
            last_name: row.last_name,
            username: row.username,
            status: row.status,
        })
        .collect::<Vec<_>>();
    settings_side_json_response(
        StatusCode::OK,
        &DeputyCandidatesResponse { items },
        "GET, PUT, OPTIONS",
    )
}

async fn settings_deputies_update_response(
    routes: &StaticWebRoutes,
    body: &[u8],
    init_data: Option<&str>,
) -> Response {
    let req = match parse_deputy_update_request(body) {
        Ok(req) => req,
        Err(response) => return *response,
    };
    if authenticate_settings_init_data(routes, init_data, req.user_id).is_err() {
        return settings_side_error_response(
            StatusCode::UNAUTHORIZED,
            "Unauthorized",
            "GET, PUT, OPTIONS",
        );
    }
    if let Err(error) = ensure_settings_chat_available(routes, req.chat_id, req.user_id).await {
        return error;
    }
    if !routes.admin_ids.contains(&req.user_id)
        && let Err(response) =
            ensure_deputy_management_permission(routes, req.chat_id, req.user_id).await
    {
        return response;
    }
    let deputy_ids = normalize_deputy_ids(&req.deputy_ids, req.user_id);
    if let Err(message) = validate_deputy_ids(routes, req.chat_id, &deputy_ids).await {
        return settings_side_json_raw_response(
            StatusCode::BAD_REQUEST,
            &serde_json::json!({ "error": message }).to_string(),
            "GET, PUT, OPTIONS",
        );
    }
    let Some(member_store) = &routes.member_store else {
        return settings_side_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
            "GET, PUT, OPTIONS",
        );
    };
    if let Err(error) = member_store
        .replace_chat_deputies(req.chat_id, &deputy_ids)
        .await
    {
        tracing::warn!(%error, chat_id = req.chat_id, user_id = req.user_id, "failed to replace chat deputies");
        return settings_side_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to update deputies",
            "GET, PUT, OPTIONS",
        );
    }
    let deputies = list_deputy_summaries(routes, req.chat_id)
        .await
        .unwrap_or_default();
    settings_side_json_response(
        StatusCode::OK,
        &DeputyUpdateResponse { ok: true, deputies },
        "GET, PUT, OPTIONS",
    )
}

async fn settings_memory_get_response(
    routes: &StaticWebRoutes,
    raw_query: Option<&str>,
    init_data: Option<&str>,
) -> Response {
    let (chat_id, user_id, scope) =
        match parse_settings_memory_access(routes, raw_query, init_data).await {
            Ok(access) => access,
            Err(response) => return response,
        };
    let limit = parse_memory_limit(
        admin_auth_query_values(raw_query)
            .get("limit")
            .map(String::as_str),
    );
    let cards = match &routes.memory_store {
        Some(store) => match store.list_visible_cards(&scope, limit as i32).await {
            Ok(cards) => cards,
            Err(error) => {
                tracing::error!(%error, chat_id, user_id, "failed to list settings memory");
                return settings_side_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to list memory",
                    "GET, DELETE, OPTIONS",
                );
            }
        },
        None => Vec::new(),
    };
    settings_side_json_response(
        StatusCode::OK,
        &SettingsMemoryResponse {
            chat_id,
            user_id,
            count: cards.len(),
            cards,
        },
        "GET, DELETE, OPTIONS",
    )
}

async fn settings_memory_delete_response(
    routes: &StaticWebRoutes,
    raw_query: Option<&str>,
    init_data: Option<&str>,
) -> Response {
    let (_, user_id, scope) = match parse_settings_memory_access(routes, raw_query, init_data).await
    {
        Ok(access) => access,
        Err(response) => return response,
    };
    let id = admin_auth_query_values(raw_query)
        .get("id")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|id| *id != 0);
    let Some(id) = id else {
        return settings_side_error_response(
            StatusCode::BAD_REQUEST,
            "id required",
            "GET, DELETE, OPTIONS",
        );
    };
    let deleted = match &routes.memory_store {
        Some(store) => match store.soft_delete_visible_card(id, user_id, &scope).await {
            Ok(deleted) => deleted,
            Err(error) => {
                tracing::error!(%error, id, user_id, "failed to soft-delete settings memory card");
                return settings_side_error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to delete memory",
                    "GET, DELETE, OPTIONS",
                );
            }
        },
        None => {
            return settings_side_json_response(
                StatusCode::OK,
                &serde_json::json!({ "ok": true }),
                "GET, DELETE, OPTIONS",
            );
        }
    };
    if !deleted {
        return settings_side_error_response(
            StatusCode::NOT_FOUND,
            "memory card not found",
            "GET, DELETE, OPTIONS",
        );
    }
    settings_side_json_response(
        StatusCode::OK,
        &serde_json::json!({ "ok": true }),
        "GET, DELETE, OPTIONS",
    )
}

/// Telegram WebApp `initData` freshness window (1 hour).
const SETTINGS_INIT_DATA_MAX_AGE_SECONDS: u64 = 3_600;

const SETTINGS_INIT_DATA_HEADER: &str = "x-telegram-init-data";

/// Outcome of authenticating a settings request with Telegram WebApp `initData`.
enum SettingsInitDataDecision {
    /// `initData` was present and validated; the caller identity is established.
    Authorized,
    /// `initData` was absent and the soft-cutover flag is off; fall through to the
    /// legacy signature check (which stays as a routing/defense-in-depth gate).
    FellThrough,
}

fn settings_init_data_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(SETTINGS_INIT_DATA_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

/// Authenticate a settings WebApp request via Telegram `initData`.
///
/// `initData` is the authority for *who* the caller is; the existing per-target authorization
/// (`authorize_settings_user`, admin/deputy membership) still governs *what* they may do.
///
/// - Header present: validate it; on failure or a caller/claim mismatch, reject. On success the
///   caller is authenticated as `claimed_user_id`.
/// - Header absent: reject when `require_settings_init_data` is set (hard cutover); otherwise warn
///   and fall through to the legacy signature check (soft cutover for the cached front-end).
///
/// `Err(())` signals the gate should answer `401 Unauthorized`; the gate owns the response shape.
fn authenticate_settings_init_data(
    routes: &StaticWebRoutes,
    init_data: Option<&str>,
    claimed_user_id: i64,
) -> Result<SettingsInitDataDecision, ()> {
    let now_unix = current_unix_timestamp();
    match init_data {
        Some(init_data) => {
            let Some(user) = openplotva_web::validate_webapp_init_data(
                init_data,
                &routes.bot_token,
                SETTINGS_INIT_DATA_MAX_AGE_SECONDS,
                now_unix,
            ) else {
                tracing::warn!(claimed_user_id, "settings initData failed validation");
                return Err(());
            };
            if claimed_user_id != 0 && user.id != claimed_user_id {
                tracing::warn!(
                    caller_user_id = user.id,
                    claimed_user_id,
                    "settings initData caller does not match requested user_id"
                );
                return Err(());
            }
            Ok(SettingsInitDataDecision::Authorized)
        }
        None => {
            if routes.require_settings_init_data {
                tracing::warn!(
                    claimed_user_id,
                    "settings request rejected: initData required but absent"
                );
                return Err(());
            }
            tracing::warn!(
                claimed_user_id,
                "settings request without initData; falling through to legacy signature (soft cutover)"
            );
            Ok(SettingsInitDataDecision::FellThrough)
        }
    }
}

fn current_unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

fn parse_settings_get_access(
    values: &BTreeMap<String, String>,
    admin_ids: &[i64],
) -> Result<SettingsAccess, SettingsAccessError> {
    let chat_id = values
        .get("chat_id")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|chat_id| *chat_id != 0)
        .ok_or(SettingsAccessError::InvalidChatId)?;
    let signature = values
        .get("signature")
        .filter(|value| !value.is_empty())
        .ok_or(SettingsAccessError::MissingSignature)?;
    let user_id = values
        .get("user_id")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    if !openplotva_web::validate_settings_access_signature(chat_id, user_id, signature) {
        return Err(SettingsAccessError::InvalidSignature);
    }
    Ok(SettingsAccess {
        chat_id,
        user_id,
        is_global_admin: admin_ids.contains(&user_id),
    })
}

fn settings_access_error_response(error: SettingsAccessError) -> Response {
    match error {
        SettingsAccessError::InvalidChatId => {
            settings_error_response(StatusCode::BAD_REQUEST, "Invalid chat_id")
        }
        SettingsAccessError::MissingSignature => {
            settings_error_response(StatusCode::BAD_REQUEST, "Missing signature")
        }
        SettingsAccessError::InvalidSignature => {
            settings_error_response(StatusCode::FORBIDDEN, "Invalid signature")
        }
    }
}

fn parse_deputy_update_request(body: &[u8]) -> Result<DeputyUpdateRequest, Box<Response>> {
    let req = serde_json::from_slice::<DeputyUpdateRequest>(body).map_err(|_| {
        Box::new(settings_side_error_response(
            StatusCode::BAD_REQUEST,
            "Invalid JSON in request body",
            "GET, PUT, OPTIONS",
        ))
    })?;
    if req.chat_id == 0 || req.user_id == 0 {
        return Err(Box::new(settings_side_error_response(
            StatusCode::BAD_REQUEST,
            "chat_id and user_id are required",
            "GET, PUT, OPTIONS",
        )));
    }
    if req.signature.trim().is_empty() {
        return Err(Box::new(settings_side_error_response(
            StatusCode::BAD_REQUEST,
            "Missing signature",
            "GET, PUT, OPTIONS",
        )));
    }
    if !openplotva_web::validate_settings_access_signature(req.chat_id, req.user_id, &req.signature)
    {
        return Err(Box::new(settings_side_error_response(
            StatusCode::FORBIDDEN,
            "Invalid signature",
            "GET, PUT, OPTIONS",
        )));
    }
    Ok(req)
}

async fn parse_deputy_owner_access(
    routes: &StaticWebRoutes,
    raw_query: Option<&str>,
    init_data: Option<&str>,
) -> Result<DeputyOwnerAccess, Response> {
    let values = admin_auth_query_values(raw_query);
    let chat_id = values
        .get("chat_id")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value != 0)
        .ok_or_else(|| {
            settings_side_error_response(
                StatusCode::BAD_REQUEST,
                "Invalid chat_id",
                "GET, PUT, OPTIONS",
            )
        })?;
    let user_id = values
        .get("user_id")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value != 0)
        .ok_or_else(|| {
            settings_side_error_response(
                StatusCode::BAD_REQUEST,
                "Invalid user_id",
                "GET, PUT, OPTIONS",
            )
        })?;
    if authenticate_settings_init_data(routes, init_data, user_id).is_err() {
        return Err(settings_side_error_response(
            StatusCode::UNAUTHORIZED,
            "Unauthorized",
            "GET, PUT, OPTIONS",
        ));
    }
    let signature = values
        .get("signature")
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            settings_side_error_response(
                StatusCode::BAD_REQUEST,
                "Missing signature",
                "GET, PUT, OPTIONS",
            )
        })?;
    if !openplotva_web::validate_settings_access_signature(chat_id, user_id, signature) {
        return Err(settings_side_error_response(
            StatusCode::FORBIDDEN,
            "Invalid signature",
            "GET, PUT, OPTIONS",
        ));
    }
    if chat_id == user_id {
        return Err(settings_side_error_response(
            StatusCode::BAD_REQUEST,
            "Deputies are available only for chats",
            "GET, PUT, OPTIONS",
        ));
    }
    ensure_settings_chat_available(routes, chat_id, user_id).await?;
    let Some(settings_store) = &routes.settings_store else {
        return Err(settings_side_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
            "GET, PUT, OPTIONS",
        ));
    };
    let chat = settings_store
        .get_chat_state(chat_id)
        .await
        .map_err(|_| {
            settings_side_error_response(
                StatusCode::NOT_FOUND,
                "Chat not found",
                "GET, PUT, OPTIONS",
            )
        })?
        .ok_or_else(|| {
            settings_side_error_response(
                StatusCode::NOT_FOUND,
                "Chat not found",
                "GET, PUT, OPTIONS",
            )
        })?;
    if !chat_allows_deputies(&chat) {
        return Err(settings_side_error_response(
            StatusCode::BAD_REQUEST,
            "Deputies are available only for chats",
            "GET, PUT, OPTIONS",
        ));
    }
    Ok(DeputyOwnerAccess {
        chat_id,
        user_id,
        is_global_admin: routes.admin_ids.contains(&user_id),
    })
}

async fn ensure_deputy_management_permission(
    routes: &StaticWebRoutes,
    chat_id: i64,
    user_id: i64,
) -> Result<(), Response> {
    let Some(member_store) = &routes.member_store else {
        return Err(settings_side_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
            "GET, PUT, OPTIONS",
        ));
    };
    match member_store.get_chat_member(chat_id, user_id).await {
        Ok(Some(member)) if member.status == openplotva_storage::CHAT_MEMBER_STATUS_CREATOR => {
            return Ok(());
        }
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(%error, chat_id, user_id, "deputy permission check failed");
            return Err(settings_side_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Permission check failed",
                "GET, PUT, OPTIONS",
            ));
        }
    }
    if routes.telegram.is_some()
        && let Ok(Some(member)) = refresh_chat_member_for_web(routes, chat_id, user_id).await
        && member.status == openplotva_storage::CHAT_MEMBER_STATUS_CREATOR
    {
        return Ok(());
    }
    Err(settings_side_error_response(
        StatusCode::FORBIDDEN,
        "Unauthorized access",
        "GET, PUT, OPTIONS",
    ))
}

fn normalize_deputy_ids(values: &[i64], current_user_id: i64) -> Vec<i64> {
    let mut seen = HashSet::with_capacity(values.len());
    let mut result = Vec::with_capacity(values.len());
    for value in values {
        if *value <= 0 || *value == current_user_id || !seen.insert(*value) {
            continue;
        }
        result.push(*value);
    }
    result.sort_unstable();
    result
}

async fn validate_deputy_ids(
    routes: &StaticWebRoutes,
    chat_id: i64,
    deputy_ids: &[i64],
) -> Result<(), String> {
    if deputy_ids.is_empty() {
        return Ok(());
    }
    let Some(member_store) = &routes.member_store else {
        return Err(format!("invalid deputy {}", deputy_ids[0]));
    };
    let members = member_store
        .list_chat_members_by_user_ids(chat_id, deputy_ids)
        .await
        .map_err(|_| format!("invalid deputy {}", deputy_ids[0]))?;
    let mut by_id = members
        .into_iter()
        .map(|member| (member.user_id, member))
        .collect::<HashMap<_, _>>();
    for deputy_id in deputy_ids {
        if routes.telegram.is_some()
            && !by_id.get(deputy_id).is_some_and(|member| {
                openplotva_storage::is_active_chat_member_status(&member.status)
            })
            && let Ok(Some(member)) = refresh_chat_member_for_web(routes, chat_id, *deputy_id).await
        {
            by_id.insert(*deputy_id, member);
        }
        let Some(member) = by_id.get(deputy_id) else {
            return Err(format!("invalid deputy {deputy_id}"));
        };
        if !openplotva_storage::is_active_chat_member_status(&member.status) {
            return Err(format!("invalid deputy {deputy_id}"));
        }
    }
    Ok(())
}

async fn list_deputy_summaries(
    routes: &StaticWebRoutes,
    chat_id: i64,
) -> Result<Vec<SettingsDeputySummary>, String> {
    let member_store = routes
        .member_store
        .as_ref()
        .ok_or_else(|| "settings store not configured".to_owned())?;
    list_deputy_summaries_from_store(member_store, chat_id).await
}

async fn list_deputy_summaries_from_store(
    member_store: &PostgresChatMemberStore,
    chat_id: i64,
) -> Result<Vec<SettingsDeputySummary>, String> {
    let deputy_ids = member_store
        .list_chat_deputy_ids(chat_id)
        .await
        .map_err(|error| error.to_string())?;
    if deputy_ids.is_empty() {
        return Ok(Vec::new());
    }
    let members = member_store
        .list_chat_members_by_user_ids(chat_id, &deputy_ids)
        .await
        .map_err(|error| error.to_string())?;
    let users = member_store
        .list_user_states_by_ids(&deputy_ids)
        .await
        .map_err(|error| error.to_string())?;
    let members_by_id = members
        .into_iter()
        .map(|member| (member.user_id, member))
        .collect::<HashMap<_, _>>();
    let users_by_id = users
        .into_iter()
        .map(|user| (user.id, user))
        .collect::<HashMap<_, _>>();
    let mut deputies = Vec::with_capacity(deputy_ids.len());
    for deputy_id in deputy_ids {
        let Some(member) = members_by_id.get(&deputy_id) else {
            continue;
        };
        if !openplotva_storage::is_active_chat_member_status(&member.status) {
            continue;
        }
        let Some(user) = users_by_id.get(&deputy_id) else {
            continue;
        };
        deputies.push(SettingsDeputySummary {
            id: user.id,
            first_name: user.first_name.clone(),
            last_name: user.last_name.clone(),
            username: user.username.clone(),
            status: member.status.clone(),
            display_name: build_deputy_display_name(
                user.id,
                &user.first_name,
                user.last_name.as_deref(),
                user.username.as_deref(),
            ),
        });
    }
    deputies.sort_by(|left, right| {
        left.display_name
            .cmp(&right.display_name)
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(deputies)
}

fn build_deputy_display_name(
    user_id: i64,
    first_name: &str,
    last_name: Option<&str>,
    username: Option<&str>,
) -> String {
    let mut name = first_name.trim().to_owned();
    if let Some(last_name) = last_name.map(str::trim).filter(|value| !value.is_empty()) {
        if !name.is_empty() {
            name.push(' ');
        }
        name.push_str(last_name);
    }
    if !name.is_empty() {
        return name;
    }
    if let Some(username) = username.map(str::trim).filter(|value| !value.is_empty()) {
        return format!("@{username}");
    }
    format!("User {user_id}")
}

fn parse_deputy_candidates_limit(value: Option<&str>) -> usize {
    match value.and_then(|value| value.trim().parse::<usize>().ok()) {
        Some(limit) if limit > 100 => 100,
        Some(limit) if limit > 0 => limit,
        _ => 50,
    }
}

fn chat_allows_deputies(chat: &openplotva_core::ChatState) -> bool {
    !chat.chat_type.trim().is_empty() && chat.chat_type != "private"
}

fn parse_settings_update_request(body: &[u8]) -> Result<SettingsUpdateRequest, Box<Response>> {
    let mut req = serde_json::from_slice::<SettingsUpdateRequest>(body).map_err(|error| {
        tracing::error!(%error, "invalid JSON in settings update request");
        Box::new(settings_error_response(
            StatusCode::BAD_REQUEST,
            "Invalid JSON in request body",
        ))
    })?;
    if req.chat_id == 0 {
        return Err(Box::new(settings_error_response(
            StatusCode::BAD_REQUEST,
            "Chat ID is required",
        )));
    }
    if req.signature.is_empty() {
        return Err(Box::new(settings_error_response(
            StatusCode::BAD_REQUEST,
            "Missing signature",
        )));
    }
    if !openplotva_web::validate_settings_access_signature(req.chat_id, req.user_id, &req.signature)
    {
        return Err(Box::new(settings_error_response(
            StatusCode::FORBIDDEN,
            "Invalid signature",
        )));
    }
    req.custom_persona = openplotva_web::truncate_custom_persona(req.custom_persona);
    Ok(req)
}

async fn ensure_settings_chat_available(
    routes: &StaticWebRoutes,
    chat_id: i64,
    user_id: i64,
) -> Result<(), Response> {
    if chat_id == user_id {
        return Ok(());
    }
    let Some(settings_store) = &routes.settings_store else {
        return Err(settings_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
        ));
    };
    match settings_store.get_chat_state(chat_id).await {
        Ok(Some(_)) => ensure_settings_bot_membership(routes, chat_id).await,
        Ok(None) => {
            refresh_settings_chat_from_telegram(routes, chat_id).await?;
            ensure_settings_bot_membership(routes, chat_id).await
        }
        Err(error) => {
            tracing::warn!(%error, chat_id, "failed to validate chat availability");
            Err(settings_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to validate chat availability",
            ))
        }
    }
}

async fn refresh_settings_chat_from_telegram(
    routes: &StaticWebRoutes,
    chat_id: i64,
) -> Result<(), Response> {
    let Some(telegram) = &routes.telegram else {
        return Err(settings_error_response(
            StatusCode::NOT_FOUND,
            "Chat not found",
        ));
    };
    let Some(state_store) = &routes.state_store else {
        return Err(settings_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to validate chat availability",
        ));
    };
    let chat = telegram
        .execute(openplotva_telegram::build_get_chat_method(chat_id))
        .await
        .map_err(|error| {
            tracing::debug!(%error, chat_id, "Telegram getChat failed for settings availability");
            settings_error_response(StatusCode::NOT_FOUND, "Chat not found")
        })?;
    let chat_state = settings_chat_state_from_full_info(chat);
    state_store
        .upsert_chat_state(&chat_state)
        .await
        .map_err(|error| {
            tracing::warn!(%error, chat_id, "failed to persist Telegram chat freshness for settings availability");
            settings_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to validate chat availability",
            )
        })
}

async fn ensure_settings_bot_membership(
    routes: &StaticWebRoutes,
    chat_id: i64,
) -> Result<(), Response> {
    let (Some(telegram), Some(bot_id)) = (&routes.telegram, routes.bot_id) else {
        return Ok(());
    };
    let Some(member_store) = &routes.member_store else {
        return Err(settings_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to validate chat availability",
        ));
    };
    match member_store.get_chat_member(chat_id, bot_id).await {
        Ok(Some(member)) if openplotva_storage::is_active_chat_member_status(&member.status) => {
            return Ok(());
        }
        Ok(_) => {}
        Err(error) => {
            tracing::debug!(%error, chat_id, bot_id, "failed to load cached bot membership for settings availability");
        }
    }
    let member = settings::GroupSettingsMemberApi::get_chat_member(telegram, chat_id, bot_id)
        .await
        .map_err(|error| {
            tracing::warn!(%error, chat_id, bot_id, "Telegram bot membership check failed for settings availability");
            settings_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to validate chat availability",
            )
        })?;
    let upsert = settings::chat_member_upsert_from_telegram(chat_id, bot_id, &member);
    if let Err(error) = member_store.upsert_chat_member(&upsert).await {
        tracing::debug!(%error, chat_id, bot_id, "failed to persist Telegram bot membership freshness for settings availability");
    }
    let user_state = settings::user_state_from_telegram_user(member.get_user());
    if let Err(error) = member_store.upsert_user_state(&user_state).await {
        tracing::debug!(%error, chat_id, bot_id, "failed to persist Telegram bot user freshness for settings availability");
    }
    if openplotva_storage::is_active_chat_member_status(&upsert.status) {
        Ok(())
    } else {
        Err(settings_error_response(
            StatusCode::NOT_FOUND,
            "Chat not found",
        ))
    }
}

fn settings_chat_state_from_full_info(
    chat: openplotva_telegram::ChatFullInfo,
) -> openplotva_core::ChatState {
    openplotva_core::ChatState::new(
        chat.id,
        settings_chat_full_info_type_name(chat.chat_type),
        chat.title,
        chat.username,
        chat.first_name,
        chat.last_name,
        chat.is_forum,
    )
}

fn settings_chat_full_info_type_name(chat_type: carapax::types::ChatFullInfoType) -> &'static str {
    match chat_type {
        carapax::types::ChatFullInfoType::Channel => "channel",
        carapax::types::ChatFullInfoType::Group => "group",
        carapax::types::ChatFullInfoType::Private => "private",
        carapax::types::ChatFullInfoType::Supergroup => "supergroup",
    }
}

async fn authorize_settings_user(
    routes: &StaticWebRoutes,
    access: SettingsAccess,
) -> Result<(), Response> {
    if access.user_id == 0 || access.is_global_admin {
        return Ok(());
    }
    match settings_user_can_edit(routes, access.chat_id, access.user_id).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(settings_error_response(
            StatusCode::FORBIDDEN,
            "Unauthorized access",
        )),
        Err(error) => {
            tracing::warn!(%error, chat_id = access.chat_id, user_id = access.user_id, "settings permission check failed");
            Err(settings_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Permission check failed",
            ))
        }
    }
}

async fn authorize_settings_memory_user(
    routes: &StaticWebRoutes,
    access: SettingsAccess,
) -> Result<(), Response> {
    if access.user_id == 0 {
        return Ok(());
    }
    match settings_user_can_edit(routes, access.chat_id, access.user_id).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(settings_error_response(
            StatusCode::FORBIDDEN,
            "Unauthorized access",
        )),
        Err(error) => {
            tracing::warn!(%error, chat_id = access.chat_id, user_id = access.user_id, "settings memory permission check failed");
            Err(settings_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Permission check failed",
            ))
        }
    }
}

async fn settings_user_can_edit(
    routes: &StaticWebRoutes,
    chat_id: i64,
    user_id: i64,
) -> Result<bool, String> {
    if user_id == 0 {
        return Err("user ID is required".to_owned());
    }
    if chat_id == user_id {
        return Ok(true);
    }
    let settings_store = routes
        .settings_store
        .as_ref()
        .ok_or_else(|| "settings store not configured".to_owned())?;
    let member_store = routes
        .member_store
        .as_ref()
        .ok_or_else(|| "settings store not configured".to_owned())?;
    let Some(chat) = settings_store
        .get_chat_state(chat_id)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Err("failed to get chat info".to_owned());
    };
    if chat.chat_type == "private" {
        return Ok(false);
    }
    let mut member = member_store
        .get_chat_member(chat_id, user_id)
        .await
        .map_err(|error| error.to_string())?;
    if openplotva_storage::stored_member_can_open_group_settings(member.as_ref()) {
        return Ok(true);
    }
    if routes.telegram.is_some() {
        member = refresh_chat_member_for_web(routes, chat_id, user_id).await?;
        if openplotva_storage::stored_member_can_open_group_settings(member.as_ref()) {
            return Ok(true);
        }
    }
    let Some(member) = member.as_ref() else {
        return Ok(false);
    };
    if !openplotva_storage::is_active_chat_member_status(&member.status) {
        return Ok(false);
    }
    let deputy_ids = member_store
        .list_chat_deputy_ids(chat_id)
        .await
        .map_err(|error| error.to_string())?;
    Ok(deputy_ids.contains(&user_id))
}

async fn refresh_chat_member_for_web(
    routes: &StaticWebRoutes,
    chat_id: i64,
    user_id: i64,
) -> Result<Option<openplotva_storage::ChatMemberRecord>, String> {
    let member_store = routes
        .member_store
        .as_ref()
        .ok_or_else(|| "settings store not configured".to_owned())?;
    let Some(telegram) = &routes.telegram else {
        return member_store
            .get_chat_member(chat_id, user_id)
            .await
            .map_err(|error| error.to_string());
    };
    if chat_id == 0 || user_id == 0 {
        return Ok(None);
    }

    match settings::GroupSettingsMemberApi::get_chat_member(telegram, chat_id, user_id).await {
        Ok(member) => {
            let upsert = settings::chat_member_upsert_from_telegram(chat_id, user_id, &member);
            if let Err(error) = member_store.upsert_chat_member(&upsert).await {
                tracing::debug!(%error, chat_id, user_id, "failed to persist Telegram member freshness for web route");
                return Ok(Some(chat_member_record_from_upsert(&upsert)));
            }
            let user_state = settings::user_state_from_telegram_user(member.get_user());
            if let Err(error) = member_store.upsert_user_state(&user_state).await {
                tracing::debug!(%error, chat_id, user_id, "failed to persist Telegram user freshness for web route");
            }
            member_store
                .get_chat_member(chat_id, user_id)
                .await
                .map(|stored| stored.or_else(|| Some(chat_member_record_from_upsert(&upsert))))
                .map_err(|error| error.to_string())
        }
        Err(error) => {
            tracing::debug!(%error, chat_id, user_id, "Telegram member freshness failed for web route; using cached row");
            member_store
                .get_chat_member(chat_id, user_id)
                .await
                .map_err(|error| error.to_string())
        }
    }
}

fn chat_member_record_from_upsert(
    upsert: &openplotva_storage::ChatMemberUpsert,
) -> openplotva_storage::ChatMemberRecord {
    openplotva_storage::ChatMemberRecord {
        chat_id: upsert.chat_id,
        user_id: upsert.user_id,
        status: upsert.status.clone(),
        is_anonymous: upsert.is_anonymous,
        custom_title: upsert.custom_title.clone(),
        can_be_edited: upsert.can_be_edited,
        can_manage_chat: upsert.can_manage_chat,
        can_delete_messages: upsert.can_delete_messages,
        can_manage_video_chats: upsert.can_manage_video_chats,
        can_restrict_members: upsert.can_restrict_members,
        can_promote_members: upsert.can_promote_members,
        can_change_info: upsert.can_change_info,
        can_invite_users: upsert.can_invite_users,
        can_post_messages: upsert.can_post_messages,
        can_edit_messages: upsert.can_edit_messages,
        can_pin_messages: upsert.can_pin_messages,
        can_manage_topics: upsert.can_manage_topics,
        can_send_messages: upsert.can_send_messages,
        can_send_media_messages: upsert.can_send_media_messages,
        can_send_polls: upsert.can_send_polls,
        can_send_other_messages: upsert.can_send_other_messages,
        can_add_web_page_previews: upsert.can_add_web_page_previews,
        until_date: upsert.until_date,
    }
}

async fn settings_update_chat(
    routes: &StaticWebRoutes,
    chat_id: i64,
    user_id: i64,
) -> Result<openplotva_core::ChatState, Response> {
    let Some(settings_store) = &routes.settings_store else {
        return Err(settings_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "settings store not configured",
        ));
    };
    match settings_store.get_chat_state(chat_id).await {
        Ok(Some(chat)) => Ok(chat),
        Ok(None) if chat_id == user_id && user_id != 0 => {
            create_private_settings_chat(routes, chat_id).await
        }
        Ok(None) => Err(settings_error_response(
            StatusCode::NOT_FOUND,
            "Chat not found",
        )),
        Err(error) => {
            tracing::error!(%error, chat_id, "chat not found for settings update");
            Err(settings_error_response(
                StatusCode::NOT_FOUND,
                "Chat not found",
            ))
        }
    }
}

async fn create_private_settings_chat(
    routes: &StaticWebRoutes,
    chat_id: i64,
) -> Result<openplotva_core::ChatState, Response> {
    let chat = openplotva_core::ChatState::new(chat_id, "private", None, None, None, None, None);
    if let Some(store) = &routes.state_store
        && let Err(error) = store.upsert_chat_state(&chat).await
    {
        tracing::error!(%error, chat_id, "failed to create private chat record");
        return Err(settings_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to initialize chat",
        ));
    }
    Ok(chat)
}

async fn settings_chat_display_for_read(
    routes: &StaticWebRoutes,
    chat_id: i64,
    user_id: i64,
) -> (String, String) {
    let Some(settings_store) = &routes.settings_store else {
        return (String::new(), String::new());
    };
    match settings_store.get_chat_state(chat_id).await {
        Ok(Some(chat)) => settings_chat_display(&chat),
        Ok(None) | Err(_) if chat_id == user_id && user_id != 0 => {
            let _ = create_private_settings_chat(routes, chat_id).await;
            (String::new(), "private".to_owned())
        }
        Ok(None) | Err(_) => (String::new(), String::new()),
    }
}

fn settings_chat_display(chat: &openplotva_core::ChatState) -> (String, String) {
    if let Some(title) = chat.title.as_ref().filter(|value| !value.is_empty()) {
        return (title.clone(), chat.chat_type.clone());
    }
    let private_name = settings_chat_name(chat.first_name.as_deref(), chat.last_name.as_deref());
    if !private_name.is_empty() {
        return (private_name, chat.chat_type.clone());
    }
    if let Some(username) = chat.username.as_ref().filter(|value| !value.is_empty()) {
        return (format!("@{username}"), chat.chat_type.clone());
    }
    (String::new(), chat.chat_type.clone())
}

fn settings_chat_name(first_name: Option<&str>, last_name: Option<&str>) -> String {
    let Some(first_name) = first_name else {
        return String::new();
    };
    let mut title = first_name.to_owned();
    if let Some(last_name) = last_name.filter(|value| !value.is_empty()) {
        title.push(' ');
        title.push_str(last_name);
    }
    title
}

fn new_settings_response(
    settings: &openplotva_core::ChatSettings,
    chat_title: String,
    chat_type: String,
) -> SettingsResponseBody {
    SettingsResponseBody {
        chat_id: settings.chat_id,
        chat_title,
        chat_type,
        mood_alignment: settings.mood_alignment.clone(),
        custom_persona: settings.custom_persona.clone(),
        reactivity_percentage: settings.reactivity_percentage,
        proactivity_percentage: settings.proactivity_percentage,
        enable_obscenifier: settings.enable_obscenifier,
        enable_profanity: settings.enable_profanity,
        enable_greet_joiners: settings.enable_greet_joiners,
        enable_global_text_reply: settings.enable_global_text_reply,
        enable_global_draw_reply: settings.enable_global_draw_reply,
        enable_daily_game: settings.enable_daily_game.unwrap_or(true),
        daily_game_theme: chat_setting_daily_game_theme(settings.daily_game_theme.as_deref()),
        greeting_html: settings.greeting_html.clone(),
        is_vip: false,
        disable_random_reactivity: false,
        hide_original_draw_prompt: false,
        is_deputy: false,
        can_manage_deputies: false,
        deputies: Vec::new(),
    }
}

fn chat_setting_daily_game_theme(theme: Option<&str>) -> String {
    theme
        .filter(|value| !value.is_empty())
        .unwrap_or("auto")
        .to_owned()
}

fn apply_private_chat_response_defaults(
    response: &mut SettingsResponseBody,
    chat_id: i64,
    user_id: i64,
) {
    if chat_id == user_id && user_id != 0 {
        response.enable_global_text_reply = true;
        response.enable_global_draw_reply = true;
    }
}

async fn apply_user_settings_response(
    routes: &StaticWebRoutes,
    response: &mut SettingsResponseBody,
    user_id: i64,
) {
    if user_id != 0 {
        response.is_vip = settings_user_is_vip(routes, user_id).await;
        if let Some(settings_store) = &routes.settings_store
            && let Ok(Some(user_settings)) = settings_store.get_user_settings(user_id).await
        {
            response.disable_random_reactivity = user_settings.disable_random_reactivity;
            if response.is_vip {
                response.hide_original_draw_prompt = user_settings.hide_original_draw_prompt;
            }
        }
    }
    if !response.is_vip {
        response.hide_original_draw_prompt = false;
    }
}

async fn settings_user_is_vip(routes: &StaticWebRoutes, user_id: i64) -> bool {
    if user_id <= 0 {
        return false;
    }
    if let Some(vip_status) = &routes.vip_status {
        return vip_status
            .is_vip_at(user_id, OffsetDateTime::now_utc())
            .await;
    }
    let Some(vip_store) = &routes.vip_store else {
        return false;
    };
    if let Ok(Some(summary)) = vip_store.get_vip_summary_by_user(user_id).await
        && summary.is_active
        && OffsetDateTime::now_utc() < summary.effective_expires_at
    {
        return true;
    }
    vip_store
        .get_vip_cache(user_id)
        .await
        .ok()
        .flatten()
        .is_some_and(|cache| cache.is_vip && OffsetDateTime::now_utc() < cache.expires_at)
}

async fn apply_deputy_settings_response(
    member_store: &PostgresChatMemberStore,
    response: &mut SettingsResponseBody,
    chat_id: i64,
    user_id: i64,
    chat_type: &str,
    is_global_admin: bool,
) {
    if chat_id == user_id || chat_type == "private" {
        return;
    }
    let deputy_ids = member_store
        .list_chat_deputy_ids(chat_id)
        .await
        .unwrap_or_default();
    let member = member_store
        .get_chat_member(chat_id, user_id)
        .await
        .ok()
        .flatten();
    response.is_deputy = member
        .as_ref()
        .is_some_and(|member| openplotva_storage::is_active_chat_member_status(&member.status))
        && deputy_ids.contains(&user_id);
    response.can_manage_deputies = is_global_admin
        || member
            .as_ref()
            .is_some_and(|member| member.status == openplotva_storage::CHAT_MEMBER_STATUS_CREATOR);
    if response.can_manage_deputies {
        response.deputies = list_deputy_summaries_from_store(member_store, chat_id)
            .await
            .unwrap_or_default();
    }
}

async fn settings_update_from_request(
    settings_store: &PostgresChatSettingsStore,
    req: &SettingsUpdateRequest,
    chat: &openplotva_core::ChatState,
) -> openplotva_core::ChatSettingsUpdate {
    let (enable_global_text_reply, enable_global_draw_reply) =
        settings_reply_flags(&chat.chat_type, req);
    let (enable_daily_game, daily_game_theme) =
        daily_game_update(settings_store, req, is_group_chat_type(&chat.chat_type)).await;
    openplotva_core::ChatSettingsUpdate {
        chat_id: req.chat_id,
        chat_type: chat.chat_type.clone(),
        mood_alignment: req.mood_alignment.clone(),
        custom_persona: req.custom_persona.clone(),
        reactivity_percentage: req.reactivity_percentage.unwrap_or(50),
        proactivity_percentage: req.proactivity_percentage.unwrap_or(50),
        enable_global_text_reply,
        enable_global_draw_reply,
        enable_obscenifier: req.enable_obscenifier.unwrap_or(true),
        enable_profanity: req.enable_profanity.unwrap_or(true),
        enable_greet_joiners: req.enable_greet_joiners.unwrap_or(false),
        enable_daily_game,
        daily_game_theme: chat_setting_daily_game_theme(daily_game_theme.as_deref()),
        greeting_html: req.greeting_html.clone(),
    }
}

fn settings_reply_flags(chat_type: &str, req: &SettingsUpdateRequest) -> (bool, bool) {
    let mut enable_global_text_reply = req.enable_global_text_reply.unwrap_or(true);
    let mut enable_global_draw_reply = req.enable_global_draw_reply.unwrap_or(true);
    if chat_type == "private" {
        enable_global_text_reply = true;
        enable_global_draw_reply = true;
    }
    (enable_global_text_reply, enable_global_draw_reply)
}

async fn daily_game_update(
    settings_store: &PostgresChatSettingsStore,
    req: &SettingsUpdateRequest,
    is_group: bool,
) -> (bool, Option<String>) {
    let current = settings_store
        .get_chat_settings(req.chat_id)
        .await
        .ok()
        .flatten();
    let mut daily_enabled = current
        .as_ref()
        .and_then(|settings| settings.enable_daily_game)
        .unwrap_or(true);
    let mut daily_theme = current.and_then(|settings| settings.daily_game_theme);
    if is_group {
        if let Some(enabled) = req.enable_daily_game {
            daily_enabled = enabled;
        }
        if req.daily_game_theme.is_some() {
            daily_theme = req.daily_game_theme.clone();
        }
    }
    (daily_enabled, daily_theme)
}

fn is_group_chat_type(chat_type: &str) -> bool {
    chat_type == "group" || chat_type == "supergroup"
}

async fn update_user_settings_from_request(routes: &StaticWebRoutes, req: &SettingsUpdateRequest) {
    if req.user_id == 0
        || (req.disable_random_reactivity.is_none() && req.hide_original_draw_prompt.is_none())
    {
        return;
    }
    let Some(settings_store) = &routes.settings_store else {
        return;
    };
    let current = settings_store
        .get_user_settings(req.user_id)
        .await
        .ok()
        .flatten();
    let disable_random_reactivity = req.disable_random_reactivity.unwrap_or_else(|| {
        current
            .as_ref()
            .is_some_and(|settings| settings.disable_random_reactivity)
    });
    let mut hide_original_draw_prompt = current
        .as_ref()
        .is_some_and(|settings| settings.hide_original_draw_prompt);
    if settings_user_is_vip(routes, req.user_id).await {
        if let Some(hide) = req.hide_original_draw_prompt {
            hide_original_draw_prompt = hide;
        }
    } else {
        hide_original_draw_prompt = false;
    }
    if let Err(error) = settings_store
        .upsert_user_settings(
            req.user_id,
            disable_random_reactivity,
            hide_original_draw_prompt,
        )
        .await
    {
        tracing::warn!(%error, user_id = req.user_id, "failed to update user settings");
    }
}

async fn managed_chat_list_items(
    routes: &StaticWebRoutes,
    settings_store: &PostgresChatSettingsStore,
    member_store: &PostgresChatMemberStore,
    user_id: i64,
) -> Vec<ChatListItem> {
    let chats = match settings_store.list_user_chats(user_id).await {
        Ok(chats) => chats,
        Err(error) => {
            tracing::debug!(%error, user_id, "failed to list user chats");
            return Vec::new();
        }
    };
    let memberships = member_store.list_user_chat_memberships(user_id).await;
    let membership_fallback = memberships.is_err();
    let members_by_chat_id = memberships
        .unwrap_or_default()
        .into_iter()
        .map(|member| (member.chat_id, member))
        .collect::<HashMap<_, _>>();
    let deputy_chats = member_store.list_user_deputy_chat_ids(user_id).await;
    let deputy_fallback = deputy_chats.is_err();
    let deputy_chat_ids = deputy_chats
        .unwrap_or_default()
        .into_iter()
        .collect::<HashSet<_>>();

    let mut result = Vec::with_capacity(chats.len());
    for chat in chats {
        let mut member = if membership_fallback {
            member_store
                .get_chat_member(chat.id, user_id)
                .await
                .ok()
                .flatten()
        } else {
            members_by_chat_id.get(&chat.id).cloned()
        };
        if routes.telegram.is_some()
            && !member
                .as_ref()
                .is_some_and(|member| chat_member_can_manage_settings(member, false))
            && let Ok(fresh) = refresh_chat_member_for_web(routes, chat.id, user_id).await
        {
            member = fresh;
        }
        let Some(member) = member else {
            continue;
        };
        let is_deputy = user_is_deputy_for_managed_chat(
            member_store,
            user_id,
            chat.id,
            &member,
            &deputy_chat_ids,
            deputy_fallback,
        )
        .await;
        if !chat_member_can_manage_settings(&member, is_deputy) {
            continue;
        }
        result.push(ChatListItem {
            id: chat.id,
            title: chat_list_title(&chat),
            chat_type: chat.chat_type,
        });
    }
    result
}

async fn user_is_deputy_for_managed_chat(
    member_store: &PostgresChatMemberStore,
    user_id: i64,
    chat_id: i64,
    member: &openplotva_storage::ChatMemberRecord,
    deputy_chat_ids: &HashSet<i64>,
    fallback: bool,
) -> bool {
    if chat_member_can_manage_settings(member, false)
        || !openplotva_storage::is_active_chat_member_status(&member.status)
    {
        return false;
    }
    if !fallback {
        return deputy_chat_ids.contains(&chat_id);
    }
    member_store
        .list_chat_deputy_ids(chat_id)
        .await
        .is_ok_and(|ids| ids.contains(&user_id))
}

fn chat_member_can_manage_settings(
    member: &openplotva_storage::ChatMemberRecord,
    is_deputy: bool,
) -> bool {
    member.status == openplotva_storage::CHAT_MEMBER_STATUS_CREATOR
        || is_deputy
        || member.status == openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR
            && member.can_promote_members == Some(true)
}

fn chat_list_title(chat: &openplotva_core::ChatState) -> String {
    let (title, _) = settings_chat_display(chat);
    if title.is_empty() {
        format!("Chat {}", chat.id)
    } else {
        title
    }
}

async fn parse_settings_memory_access(
    routes: &StaticWebRoutes,
    raw_query: Option<&str>,
    init_data: Option<&str>,
) -> Result<(i64, i64, openplotva_memory::RetrievalScope), Response> {
    let values = admin_auth_query_values(raw_query);
    let chat_id = values
        .get("chat_id")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value != 0)
        .ok_or_else(|| {
            settings_side_error_response(
                StatusCode::BAD_REQUEST,
                "invalid chat_id",
                "GET, DELETE, OPTIONS",
            )
        })?;
    let signature = values
        .get("signature")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            settings_side_error_response(
                StatusCode::BAD_REQUEST,
                "missing signature",
                "GET, DELETE, OPTIONS",
            )
        })?;
    let user_id = values
        .get("user_id")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    if authenticate_settings_init_data(routes, init_data, user_id).is_err() {
        return Err(settings_side_error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "GET, DELETE, OPTIONS",
        ));
    }
    if !openplotva_web::validate_settings_access_signature(chat_id, user_id, signature) {
        return Err(settings_side_error_response(
            StatusCode::FORBIDDEN,
            "invalid signature",
            "GET, DELETE, OPTIONS",
        ));
    }
    if user_id != 0
        && let Err(error) = authorize_settings_memory_user(
            routes,
            SettingsAccess {
                chat_id,
                user_id,
                is_global_admin: false,
            },
        )
        .await
    {
        return Err(match error.status() {
            StatusCode::FORBIDDEN => settings_side_error_response(
                StatusCode::FORBIDDEN,
                "unauthorized access",
                "GET, DELETE, OPTIONS",
            ),
            _ => settings_side_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "permission check failed",
                "GET, DELETE, OPTIONS",
            ),
        });
    }
    let thread_id = values
        .get("thread_id")
        .map(String::as_str)
        .map(parse_optional_i32)
        .unwrap_or(0);
    let mut scope = openplotva_memory::RetrievalScope {
        chat_id,
        thread_id,
        user_id,
        ..openplotva_memory::RetrievalScope::default()
    };
    if let Some(settings_store) = &routes.settings_store
        && let Ok(Some(meta)) = settings_store.get_dialog_memory_chat_meta(chat_id).await
    {
        scope.chat_type = meta.chat_type;
        scope.username = meta.username;
        scope.active_usernames = meta.active_usernames;
    }
    Ok((chat_id, user_id, scope))
}

fn parse_memory_limit(value: Option<&str>) -> usize {
    match value.and_then(|value| value.trim().parse::<usize>().ok()) {
        Some(limit) if limit > 500 => 500,
        Some(limit) if limit > 0 => limit,
        _ => 100,
    }
}

fn parse_optional_i32(value: &str) -> i32 {
    value
        .trim()
        .parse::<i32>()
        .ok()
        .filter(|value| *value >= 0)
        .unwrap_or(0)
}

fn settings_options_response() -> Response {
    let mut response = StatusCode::OK.into_response();
    add_settings_api_headers(response.headers_mut());
    response
}

fn settings_error_response(status: StatusCode, error: &str) -> Response {
    settings_json_raw_response(status, &format!(r#"{{"error": "{error}"}}"#))
}

fn settings_json_response<T: Serialize>(status: StatusCode, value: &T) -> Response {
    let body = match serde_json::to_string(value) {
        Ok(body) => body,
        Err(error) => {
            tracing::error!(%error, "failed to encode settings response");
            r#"{"error": "Failed to encode response"}"#.to_owned()
        }
    };
    settings_json_raw_response(status, &body)
}

fn settings_json_raw_response(status: StatusCode, body: &str) -> Response {
    let mut response = (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        format!("{body}\n"),
    )
        .into_response();
    add_settings_api_headers(response.headers_mut());
    response
}

fn settings_side_options_response(methods: &'static str) -> Response {
    let mut response = StatusCode::OK.into_response();
    add_settings_side_api_headers(response.headers_mut(), methods);
    response
}

fn settings_side_error_response(
    status: StatusCode,
    error: &str,
    methods: &'static str,
) -> Response {
    settings_side_json_raw_response(
        status,
        &serde_json::json!({ "error": error }).to_string(),
        methods,
    )
}

fn settings_side_json_response<T: Serialize>(
    status: StatusCode,
    value: &T,
    methods: &'static str,
) -> Response {
    let body = serde_json::to_string(value).unwrap_or_else(|error| {
        tracing::error!(%error, "failed to encode settings side response");
        r#"{"error":"Failed to encode response"}"#.to_owned()
    });
    settings_side_json_raw_response(status, &body, methods)
}

fn settings_side_json_raw_response(
    status: StatusCode,
    body: &str,
    methods: &'static str,
) -> Response {
    let mut response = (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        format!("{body}\n"),
    )
        .into_response();
    add_settings_side_api_headers(response.headers_mut(), methods);
    response
}

fn add_settings_api_headers(headers: &mut HeaderMap) {
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, PUT, OPTIONS"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Content-Type, Authorization"),
    );
}

fn add_settings_side_api_headers(headers: &mut HeaderMap, methods: &'static str) {
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static(methods),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Content-Type, Authorization"),
    );
}

/// Maximum age of a Telegram Login `auth_date` accepted for admin authentication.
const ADMIN_AUTH_MAX_AGE_SECONDS: i64 = 86_400; // 24h; tighten later if desired
/// Small tolerance for clock skew on `auth_date` values slightly in the future.
const ADMIN_AUTH_FUTURE_SKEW_SECONDS: i64 = 60;

/// Returns true if `auth_date` (unix seconds) is within the accepted freshness window
/// relative to `now`. Missing/unparseable dates and far-future dates are rejected.
fn admin_auth_date_is_fresh(values: &BTreeMap<String, String>, now: OffsetDateTime) -> bool {
    let Some(auth_date) = values
        .get("auth_date")
        .and_then(|v| v.trim().parse::<i64>().ok())
    else {
        return false;
    };
    let now_unix = now.unix_timestamp();
    let age = now_unix - auth_date;
    (-ADMIN_AUTH_FUTURE_SKEW_SECONDS..=ADMIN_AUTH_MAX_AGE_SECONDS).contains(&age)
}

async fn admin_auth_response(
    routes: &StaticWebRoutes,
    method: Method,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::GET && method != Method::POST {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }

    let values = admin_auth_query_values(raw_query);
    let Some(user_id_raw) = values.get("id").filter(|value| !value.is_empty()) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "missing parameters");
    };
    let Some(hash) = values.get("hash").filter(|value| !value.is_empty()) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "missing parameters");
    };
    let Ok(user_id) = user_id_raw.parse::<i64>() else {
        return admin_error_response(StatusCode::BAD_REQUEST, "invalid user id");
    };
    if routes.bot_token.is_empty() {
        return admin_error_response(StatusCode::BAD_REQUEST, "bot token not configured");
    }

    let pairs = values
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()));
    if !openplotva_web::validate_telegram_auth(pairs, &routes.bot_token, hash) {
        tracing::error!("invalid admin auth signature");
        return admin_error_response(StatusCode::FORBIDDEN, "invalid auth");
    }

    if !admin_auth_date_is_fresh(&values, OffsetDateTime::now_utc()) {
        tracing::error!("admin auth rejected: stale or missing auth_date");
        return admin_error_response(StatusCode::FORBIDDEN, "auth expired");
    }

    if !routes.admin_ids.contains(&user_id) {
        tracing::error!(user_id, "authenticated Telegram user is not an admin");
        return admin_error_response(StatusCode::FORBIDDEN, "forbidden");
    }

    persist_admin_session_user(routes, &values, user_id).await;
    (
        StatusCode::FOUND,
        [
            (header::LOCATION, "/admin/".to_owned()),
            (
                header::SET_COOKIE,
                openplotva_web::admin_session_cookie(user_id, &routes.bot_token),
            ),
        ],
    )
        .into_response()
}

fn admin_auth_query_values(raw_query: Option<&str>) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    let Some(raw_query) = raw_query else {
        return values;
    };
    for (key, value) in url::form_urlencoded::parse(raw_query.as_bytes()) {
        values
            .entry(key.into_owned())
            .or_insert_with(|| value.into_owned());
    }
    values
}

async fn persist_admin_session_user(
    routes: &StaticWebRoutes,
    values: &BTreeMap<String, String>,
    user_id: i64,
) {
    let Some(store) = &routes.state_store else {
        return;
    };
    let user = admin_auth_user_state(values, user_id);
    if let Err(error) = store.upsert_user_state(&user).await {
        tracing::warn!(%error, user_id, "failed to persist admin session user");
    }
}

fn admin_auth_user_state(
    values: &BTreeMap<String, String>,
    user_id: i64,
) -> openplotva_core::UserState {
    openplotva_core::UserState::new(
        user_id,
        trimmed_auth_value(values, "first_name").unwrap_or_else(|| "Telegram Admin".to_owned()),
        trimmed_auth_value(values, "last_name"),
        trimmed_auth_value(values, "username"),
        trimmed_auth_value(values, "language_code"),
        None,
    )
}

fn trimmed_auth_value(values: &BTreeMap<String, String>, key: &str) -> Option<String> {
    values
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn admin_error_response(status: StatusCode, error: &str) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        format!(r#"{{"error":"{error}"}}"#) + "\n",
    )
        .into_response()
}

fn admin_json_response(status: StatusCode, value: serde_json::Value) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        format!("{value}\n"),
    )
        .into_response()
}

fn admin_json_no_cache_response(status: StatusCode, value: serde_json::Value) -> Response {
    (
        status,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        format!("{value}\n"),
    )
        .into_response()
}

const ADMIN_PAGE_LIMIT: i64 = 1000;
const SQL_ADMIN_LIST_USERS_FILTERED: &str = "SELECT * FROM users WHERE ($1::text IS NULL OR username ILIKE '%' || $1::text || '%' OR first_name ILIKE '%' || $1::text || '%' OR last_name ILIKE '%' || $1::text || '%') ORDER BY id LIMIT $2 OFFSET $3";
const SQL_ADMIN_GET_USER: &str = "SELECT * FROM users WHERE id = $1";
const SQL_ADMIN_GET_USER_BY_USERNAME: &str = "SELECT * FROM users WHERE username = $1 LIMIT 1";
const SQL_ADMIN_ENSURE_USER: &str = "INSERT INTO users (id, first_name, is_premium, is_vip) VALUES ($1, $2, FALSE, FALSE) ON CONFLICT (id) DO NOTHING";
const SQL_ADMIN_LIST_VIP_USERS: &str = "WITH latest_vip AS (SELECT DISTINCT ON (user_id) id, user_id, event_type, delta_seconds, effective_expires_at, reason, created_at FROM vip_events ORDER BY user_id, id DESC), subscription_stats AS (SELECT user_id, COUNT(*)::bigint AS subscriptions_count, MAX(expires_at) AS latest_subscription_expires_at FROM subscriptions GROUP BY user_id) SELECT u.*, latest_vip.id AS latest_event_id, latest_vip.event_type AS latest_event_type, latest_vip.delta_seconds AS latest_delta_seconds, latest_vip.effective_expires_at AS vip_expires_at, latest_vip.reason AS latest_reason, latest_vip.created_at AS latest_created_at, (latest_vip.effective_expires_at > CURRENT_TIMESTAMP) AS vip_active, CASE WHEN latest_vip.effective_expires_at > CURRENT_TIMESTAMP THEN FLOOR(EXTRACT(EPOCH FROM (latest_vip.effective_expires_at - CURRENT_TIMESTAMP)))::bigint ELSE 0::bigint END AS remaining_seconds, COALESCE(subscription_stats.subscriptions_count, 0)::bigint AS subscriptions_count, subscription_stats.latest_subscription_expires_at AS latest_subscription_expires_at FROM latest_vip JOIN users u ON u.id = latest_vip.user_id LEFT JOIN subscription_stats ON subscription_stats.user_id = u.id WHERE ($1::text = 'all' OR ($1::text = 'active' AND latest_vip.effective_expires_at > CURRENT_TIMESTAMP) OR ($1::text = 'expired' AND latest_vip.effective_expires_at <= CURRENT_TIMESTAMP)) AND ($2::text IS NULL OR CAST(u.id AS text) LIKE '%' || $2::text || '%' OR u.username ILIKE '%' || $2::text || '%' OR u.first_name ILIKE '%' || $2::text || '%' OR u.last_name ILIKE '%' || $2::text || '%') ORDER BY latest_vip.effective_expires_at DESC, latest_vip.id DESC LIMIT $3 OFFSET $4";
const SQL_ADMIN_SAFE_DELETE_USER: &str = "WITH deleted_memberships AS (DELETE FROM chat_members WHERE user_id = $1 RETURNING user_id), deleted_vip AS (DELETE FROM vip_cache WHERE user_id = $1 RETURNING user_id) DELETE FROM users WHERE id = $1";
const SQL_ADMIN_LIST_CHATS: &str = "SELECT * FROM chats ORDER BY id LIMIT $1 OFFSET $2";
const SQL_ADMIN_LIST_CHATS_FILTERED: &str = "SELECT * FROM chats WHERE ($1::text IS NULL OR CAST(id AS text) LIKE '%' || $1::text || '%' OR title ILIKE '%' || $1::text || '%' OR username ILIKE '%' || $1::text || '%' OR first_name ILIKE '%' || $1::text || '%' OR last_name ILIKE '%' || $1::text || '%') ORDER BY id LIMIT $2 OFFSET $3";
const SQL_ADMIN_SEARCH_CHATS_BY_MEMBER: &str = "SELECT DISTINCT c.* FROM chats c JOIN chat_members cm ON c.id = cm.chat_id JOIN users u ON cm.user_id = u.id WHERE ($1::text IS NULL OR LOWER(u.username) = LOWER($1::text)) AND ($2::bigint IS NULL OR u.id = $2::bigint) ORDER BY c.id LIMIT $3";
const SQL_ADMIN_GET_CHAT: &str = "SELECT * FROM chats WHERE id = $1";
const SQL_ADMIN_GET_CHAT_TYPE: &str = "SELECT type FROM chats WHERE id = $1";
const SQL_ADMIN_GET_CHAT_SETTINGS: &str = "SELECT * FROM chat_settings WHERE chat_id = $1";
const SQL_ADMIN_GET_CHAT_PERMISSIONS: &str =
    "SELECT * FROM chat_permissions WHERE chat_id = $1 LIMIT 1";
const SQL_ADMIN_COUNT_CHAT_MEMBERS: &str =
    "SELECT COUNT(*)::bigint AS count FROM chat_members WHERE chat_id = $1";
const SQL_ADMIN_LIST_CHAT_MEMBERS_WITH_USERS: &str = "SELECT cm.chat_id AS member_chat_id, cm.user_id AS member_user_id, cm.status AS member_status, cm.is_anonymous AS member_is_anonymous, cm.custom_title AS member_custom_title, cm.can_be_edited AS member_can_be_edited, cm.can_manage_chat AS member_can_manage_chat, cm.can_delete_messages AS member_can_delete_messages, cm.can_manage_video_chats AS member_can_manage_video_chats, cm.can_restrict_members AS member_can_restrict_members, cm.can_promote_members AS member_can_promote_members, cm.can_change_info AS member_can_change_info, cm.can_invite_users AS member_can_invite_users, cm.can_post_messages AS member_can_post_messages, cm.can_edit_messages AS member_can_edit_messages, cm.can_pin_messages AS member_can_pin_messages, cm.can_manage_topics AS member_can_manage_topics, cm.can_send_messages AS member_can_send_messages, cm.can_send_media_messages AS member_can_send_media_messages, cm.can_send_polls AS member_can_send_polls, cm.can_send_other_messages AS member_can_send_other_messages, cm.can_add_web_page_previews AS member_can_add_web_page_previews, cm.until_date AS member_until_date, cm.created_at AS member_created_at, cm.updated_at AS member_updated_at, cm.last_message_at AS member_last_message_at, u.id AS user_id, u.is_premium AS user_is_premium, u.first_name AS user_first_name, u.last_name AS user_last_name, u.username AS user_username, u.language_code AS user_language_code, u.is_vip AS user_is_vip, u.settings AS user_settings, u.discovered AS user_discovered, u.updated AS user_updated FROM chat_members cm LEFT JOIN users u ON u.id = cm.user_id WHERE cm.chat_id = $1";

#[derive(Debug, Deserialize)]
struct AdminChatSettingsUpdateRequest {
    chat_id: i64,
    mood_alignment: Option<String>,
    custom_persona: Option<String>,
    reactivity_percentage: Option<i32>,
    proactivity_percentage: Option<i32>,
    enable_obscenifier: Option<bool>,
    enable_profanity: Option<bool>,
    enable_greet_joiners: Option<bool>,
    enable_global_text_reply: Option<bool>,
    enable_global_draw_reply: Option<bool>,
    enable_daily_game: Option<bool>,
    daily_game_theme: Option<String>,
    greeting_html: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AdminVipGrantRequest {
    user_id: serde_json::Value,
    days: i64,
    #[serde(default)]
    reason: String,
}

#[derive(Debug, Default, Deserialize)]
struct AdminMemoryRestartRequest {
    #[serde(default, rename = "override")]
    override_: bool,
    #[serde(default)]
    provider: String,
    #[serde(default)]
    model: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct AdminMemoryOverride {
    provider: String,
    model: String,
}

#[derive(Debug, Default, Deserialize)]
struct AdminShieldDocumentRequest {
    #[serde(default)]
    id: i64,
    #[serde(default)]
    slug: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    category: String,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    priority: i32,
}

#[derive(Debug, Default, Deserialize)]
struct AdminShieldTestRequest {
    #[serde(default)]
    query: String,
    #[serde(default)]
    expected_category: String,
    #[serde(default)]
    max_matches: i32,
    #[serde(default)]
    debug: bool,
}

async fn admin_memory_cards_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    match method {
        Method::GET => admin_memory_cards_list_response(routes, headers, raw_query).await,
        Method::DELETE => admin_memory_card_delete_response(routes, headers, raw_query).await,
        _ => admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
    }
}

async fn admin_memory_card_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &[u8],
) -> Response {
    match method {
        Method::GET => admin_memory_card_detail_response(routes, headers, raw_query).await,
        Method::PATCH => admin_memory_card_update_response(routes, headers, body).await,
        _ => admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
    }
}

#[derive(serde::Deserialize)]
struct MemoryCardUpdate {
    #[serde(default)]
    id: i64,
    #[serde(default)]
    confidence: Option<f64>,
    #[serde(default)]
    salience: Option<f64>,
    #[serde(default)]
    portable: Option<bool>,
    #[serde(default)]
    status: Option<String>,
}

async fn admin_memory_card_update_response(
    routes: &StaticWebRoutes,
    headers: &HeaderMap,
    body: &[u8],
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let req: MemoryCardUpdate = match serde_json::from_slice(body) {
        Ok(req) => req,
        Err(_) => return admin_error_response(StatusCode::BAD_REQUEST, "invalid body"),
    };
    if req.id == 0 {
        return admin_error_response(StatusCode::BAD_REQUEST, "id required");
    }
    let Some(store) = routes
        .memory_store
        .as_ref()
        .filter(|_| routes.memory_admin_enabled)
    else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "memory disabled");
    };
    let confidence = req.confidence.map(|v| v.clamp(0.0, 1.0));
    let salience = req.salience.map(|v| v.clamp(0.0, 1.0));
    if (confidence.is_some() || salience.is_some() || req.portable.is_some())
        && let Err(error) = store
            .update_card_fields(req.id, confidence, salience, req.portable)
            .await
    {
        tracing::warn!(%error, id = req.id, "failed to update memory card fields");
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    }
    if let Some(status) = req.status.as_deref() {
        let result = match status {
            "deleted" => {
                store
                    .soft_delete_card(req.id, current_admin_user_id(headers, &routes.bot_token))
                    .await
            }
            "active" => store.restore_card(req.id).await,
            _ => Ok(()),
        };
        if let Err(error) = result {
            tracing::warn!(%error, id = req.id, "failed to change memory card status");
            return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
        }
    }
    admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true }))
}

async fn admin_memory_card_detail_response(
    routes: &StaticWebRoutes,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let id = match admin_memory_required_id(raw_query, "id required") {
        Ok(id) => id,
        Err(response) => return *response,
    };
    let Some(store) = routes
        .memory_store
        .as_ref()
        .filter(|_| routes.memory_admin_enabled)
    else {
        return admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "card": serde_json::Value::Null, "links": [] }),
        );
    };
    let card = match store.get_card(id).await {
        Ok(Some(card)) => card,
        Ok(None) => return admin_error_response(StatusCode::NOT_FOUND, "not found"),
        Err(error) => {
            tracing::warn!(%error, id, "failed to load admin memory card");
            return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
        }
    };
    let links = store.list_card_links(id).await.unwrap_or_default();
    admin_json_response(
        StatusCode::OK,
        serde_json::json!({ "card": card, "links": links }),
    )
}

async fn admin_memory_overview_response(routes: &StaticWebRoutes, headers: &HeaderMap) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(store) = routes
        .memory_store
        .as_ref()
        .filter(|_| routes.memory_admin_enabled)
    else {
        return admin_json_response(StatusCode::OK, serde_json::json!({}));
    };
    match store.memory_overview().await {
        Ok(overview) => admin_json_response(
            StatusCode::OK,
            serde_json::to_value(&overview).unwrap_or(serde_json::Value::Null),
        ),
        Err(error) => {
            tracing::warn!(%error, "failed to load admin memory overview");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

async fn admin_memory_cards_list_response(
    routes: &StaticWebRoutes,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(store) = routes
        .memory_store
        .as_ref()
        .filter(|_| routes.memory_admin_enabled)
    else {
        return admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "count": 0, "cards": serde_json::Value::Null }),
        );
    };
    let filter = admin_memory_card_filter(raw_query);
    match store.list_cards(&filter).await {
        Ok(cards) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({
                "count": cards.len(),
                "cards": cards,
            }),
        ),
        Err(error) => {
            tracing::warn!(%error, "failed to list admin memory cards");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

async fn admin_memory_card_delete_response(
    routes: &StaticWebRoutes,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let id = match admin_memory_required_id(raw_query, "id required") {
        Ok(id) => id,
        Err(response) => return *response,
    };
    if let Some(store) = routes
        .memory_store
        .as_ref()
        .filter(|_| routes.memory_admin_enabled)
        && let Err(error) = store
            .soft_delete_card(id, current_admin_user_id(headers, &routes.bot_token))
            .await
    {
        tracing::warn!(%error, id, "failed to delete admin memory card");
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    }
    admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true }))
}

async fn admin_memory_runs_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &[u8],
) -> Response {
    match method {
        Method::GET => admin_memory_runs_list_response(routes, headers, raw_query).await,
        Method::POST => admin_memory_run_retry_response(routes, headers, raw_query, body).await,
        _ => admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
    }
}

async fn admin_memory_runs_list_response(
    routes: &StaticWebRoutes,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(store) = routes
        .memory_store
        .as_ref()
        .filter(|_| routes.memory_admin_enabled)
    else {
        return admin_json_response(
            StatusCode::OK,
            serde_json::json!({
                "count": 0,
                "runs": serde_json::Value::Null,
                "policy": admin_memory_policy_json(routes),
                "enqueue_rollups": serde_json::Value::Null,
            }),
        );
    };
    let limit = parse_memory_limit(
        admin_auth_query_values(raw_query)
            .get("limit")
            .map(String::as_str),
    );
    let runs = match store.list_runs(limit as i32).await {
        Ok(runs) => runs,
        Err(error) => {
            tracing::warn!(%error, "failed to list admin memory runs");
            return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
        }
    };
    let enqueue_rollups = match store.list_enqueue_rollups(24).await {
        Ok(rollups) => rollups,
        Err(error) => {
            tracing::warn!(%error, "failed to list admin memory enqueue rollups");
            return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
        }
    };
    admin_json_response(
        StatusCode::OK,
        serde_json::json!({
            "count": runs.len(),
            "runs": runs,
            "policy": admin_memory_policy_json(routes),
            "enqueue_rollups": enqueue_rollups,
        }),
    )
}

fn admin_memory_policy_json(routes: &StaticWebRoutes) -> serde_json::Value {
    serde_json::json!({
        "min_messages_per_run": routes.memory_enqueue_policy.min_messages_per_run,
        "max_queued_runs": routes.memory_enqueue_policy.max_queued_runs,
        "max_daily_enqueued_runs": routes.memory_enqueue_policy.max_daily_enqueued_runs,
        "max_messages_per_run": routes.memory_max_messages_per_run,
        "max_input_tokens": routes.memory_max_input_tokens,
        "consolidation_model": routes.memory_consolidation_model.as_ref(),
    })
}

async fn admin_memory_run_retry_response(
    routes: &StaticWebRoutes,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &[u8],
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let id = match admin_memory_required_id(raw_query, "id required") {
        Ok(id) => id,
        Err(response) => return *response,
    };
    admin_memory_restart_execute_response(routes, headers, raw_query, body, id, false).await
}

async fn admin_memory_restart_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &[u8],
) -> Response {
    if method != Method::POST {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let id = match admin_memory_optional_restart_id(raw_query) {
        Ok(id) => id,
        Err(response) => return *response,
    };
    let run_now = admin_auth_query_values(raw_query)
        .get("run_now")
        .and_then(|value| parse_go_bool(value))
        .unwrap_or(false);
    admin_memory_restart_execute_response(routes, headers, raw_query, body, id, run_now).await
}

async fn admin_memory_restart_execute_response(
    routes: &StaticWebRoutes,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &[u8],
    id: i64,
    run_now: bool,
) -> Response {
    let Some(store) = routes
        .memory_store
        .as_ref()
        .filter(|_| routes.memory_admin_enabled)
    else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "memory disabled");
    };
    let (override_, explicit_override) =
        match admin_memory_restart_override(headers, raw_query, body) {
            Ok(value) => value,
            Err(response) => return *response,
        };
    if explicit_override {
        return admin_memory_restart_override_execute_response(routes, override_, id, run_now)
            .await;
    }
    let mut retried_failed_runs = 0_i64;
    let mut queued_runs = 0_i64;
    let result = if run_now {
        store
            .ensure_daily_runs(
                OffsetDateTime::now_utc(),
                routes.memory_retention,
                routes.memory_enqueue_policy,
            )
            .await
            .map(|queued| queued_runs = admin_u64_to_i64(queued))
    } else if id > 0 {
        store.retry_run(id).await.map(|()| retried_failed_runs = 1)
    } else {
        store
            .retry_failed_runs()
            .await
            .map(|retried| retried_failed_runs = admin_u64_to_i64(retried))
    };
    if let Err(error) = result {
        tracing::warn!(%error, id, run_now, "failed to restart admin memory runs");
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    }
    let started = routes
        .memory_restart_trigger
        .as_ref()
        .map(|trigger| {
            trigger.notify_one();
            true
        })
        .unwrap_or(false);
    let provider = if explicit_override && !override_.provider.trim().is_empty() {
        normalize_memory_restart_provider(&override_.provider)
    } else {
        "aifarm".to_owned()
    };
    let model = if explicit_override && !override_.model.trim().is_empty() {
        override_.model.trim().to_owned()
    } else {
        admin_memory_restart_model(&routes.memory_consolidation_model)
    };
    admin_memory_restart_ok_response(
        id,
        retried_failed_runs,
        queued_runs,
        started,
        explicit_override,
        provider,
        model,
    )
}

async fn admin_memory_restart_override_execute_response(
    routes: &StaticWebRoutes,
    override_: AdminMemoryOverride,
    id: i64,
    run_now: bool,
) -> Response {
    let Some(runtime) = routes.memory_override_runtime.clone() else {
        tracing::warn!(
            provider = override_.provider,
            model = override_.model,
            "admin memory override drain is unavailable"
        );
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    let (provider, model) = match admin_memory_resolve_override(
        &override_,
        &runtime.config,
        &routes.memory_consolidation_model,
    ) {
        Ok(value) => value,
        Err(unsupported_provider) => {
            tracing::warn!(
                provider = unsupported_provider,
                model = override_.model,
                "admin memory override provider is unsupported"
            );
            return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
        }
    };
    let extractor = memory_runtime::routed_memory_extractor_from_app_config(
        &runtime.config,
        routed_attempts::RoutedAttemptWalker::new(
            Arc::clone(&runtime.router_handle),
            Arc::clone(&runtime.router_breakers),
            Arc::clone(&runtime.router_triggers),
            Arc::clone(&runtime.router_pools),
        )
        .with_reporter_opt(runtime.routing_event_reporter.clone()),
    );
    let embedder = match memory_runtime::memory_write_embedder_from_config(
        &runtime.config.memory,
        &runtime.config.llm.discovery.base_url,
    ) {
        Ok(embedder) => embedder.map(|client| {
            embedder::RoutedDiscoveryEmbedder::new(
                routed_attempts::RoutedAttemptWalker::new(
                    Arc::clone(&runtime.router_handle),
                    Arc::clone(&runtime.router_breakers),
                    Arc::clone(&runtime.router_triggers),
                    Arc::clone(&runtime.router_pools),
                )
                .with_reporter_opt(runtime.routing_event_reporter.clone()),
                client.config().clone(),
            )
        }),
        Err(error) => {
            tracing::warn!(%error, provider, model, "failed to build admin memory override embedder");
            return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
        }
    };
    let Ok(guard) = runtime.lock.clone().try_lock_owned() else {
        return admin_memory_restart_ok_response(id, 0, 0, false, true, provider, model);
    };
    let mut retried_failed_runs = 0_i64;
    let mut queued_runs = 0_i64;
    let result = if run_now {
        runtime
            .store
            .ensure_daily_runs(
                OffsetDateTime::now_utc(),
                routes.memory_retention,
                routes.memory_enqueue_policy,
            )
            .await
            .map(|queued| queued_runs = admin_u64_to_i64(queued))
    } else if id > 0 {
        runtime
            .store
            .retry_run(id)
            .await
            .map(|()| retried_failed_runs = 1)
    } else {
        runtime
            .store
            .retry_failed_runs()
            .await
            .map(|retried| retried_failed_runs = admin_u64_to_i64(retried))
    };
    if let Err(error) = result {
        tracing::warn!(
            %error,
            id,
            run_now,
            provider,
            model,
            "failed to restart admin memory runs with override"
        );
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    }
    let mut cfg =
        memory_runtime::memory_service_worker_config_from_memory_config(&runtime.config.memory)
            .process;
    cfg.episode_model = model.clone();
    tokio::spawn(admin_memory_override_drain(
        runtime,
        extractor,
        embedder,
        cfg,
        model.clone(),
        guard,
    ));
    admin_memory_restart_ok_response(
        id,
        retried_failed_runs,
        queued_runs,
        true,
        true,
        provider,
        model,
    )
}

async fn admin_memory_override_drain(
    runtime: AdminMemoryOverrideRuntime,
    extractor: memory_runtime::RoutedMemoryExtractor,
    embedder: Option<embedder::RoutedDiscoveryEmbedder>,
    cfg: memory_runtime::MemoryRunProcessConfig,
    model: String,
    _guard: tokio::sync::OwnedMutexGuard<()>,
) {
    let started_at = std::time::Instant::now();
    let mut completed = 0_i64;
    let mut failed = 0_i64;
    let mut store_errors = 0_i64;
    tracing::info!(model, workers = 1, "admin memory override drain started");
    loop {
        match memory_runtime::process_next_memory_run(
            &extractor,
            &runtime.store,
            embedder.as_ref(),
            cfg.clone(),
        )
        .await
        {
            Ok(report) if report.processed => {
                completed += 1;
                tracing::info!(?report, "admin memory override run processed");
            }
            Ok(_) => break,
            Err(memory_runtime::MemoryRunProcessError::Process { run_id, source }) => {
                failed += 1;
                tracing::warn!(%source, run_id, "admin memory override run failed");
            }
            Err(memory_runtime::MemoryRunProcessError::Store { source }) => {
                store_errors += 1;
                tracing::warn!(%source, "admin memory override store operation failed");
                break;
            }
        }
    }
    tracing::info!(
        model,
        completed,
        failed,
        store_errors,
        duration = ?started_at.elapsed(),
        "admin memory override drain finished"
    );
}

fn admin_memory_resolve_override(
    override_: &AdminMemoryOverride,
    config: &AppConfig,
    default_model: &str,
) -> Result<(String, String), String> {
    let provider = normalize_memory_restart_provider(&override_.provider);
    let provider = if provider.is_empty() {
        "aifarm".to_owned()
    } else {
        provider
    };
    match provider.as_str() {
        "aifarm" => {
            let model = if override_.model.trim().is_empty() {
                admin_memory_restart_model(default_model)
            } else {
                override_.model.trim().to_owned()
            };
            Ok((provider, model))
        }
        "genkit" | "gemini" => {
            let model =
                memory_runtime::genkit_memory_extractor_model(config, Some(&override_.model));
            Ok(("genkit".to_owned(), model))
        }
        _ => Err(provider),
    }
}

fn admin_memory_restart_ok_response(
    id: i64,
    retried_failed_runs: i64,
    queued_runs: i64,
    started: bool,
    override_: bool,
    provider: String,
    model: String,
) -> Response {
    let mut value = serde_json::Map::new();
    value.insert("ok".to_owned(), serde_json::json!(true));
    if id != 0 {
        value.insert("run_id".to_owned(), serde_json::json!(id));
    }
    value.insert(
        "retried_failed_runs".to_owned(),
        serde_json::json!(retried_failed_runs),
    );
    value.insert("queued_runs".to_owned(), serde_json::json!(queued_runs));
    value.insert("started".to_owned(), serde_json::json!(started));
    value.insert("override".to_owned(), serde_json::json!(override_));
    if !provider.is_empty() {
        value.insert("provider".to_owned(), serde_json::json!(provider));
    }
    if !model.is_empty() {
        value.insert("model".to_owned(), serde_json::json!(model));
    }
    admin_json_response(StatusCode::OK, serde_json::Value::Object(value))
}

fn admin_memory_card_filter(raw_query: Option<&str>) -> openplotva_memory::CardFilter {
    let values = admin_auth_query_values(raw_query);
    openplotva_memory::CardFilter {
        chat_id: values
            .get("chat_id")
            .and_then(|value| value.trim().parse::<i64>().ok())
            .unwrap_or_default(),
        thread_id: values
            .get("thread_id")
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .and_then(|value| value.parse::<i32>().ok()),
        user_id: values
            .get("user_id")
            .and_then(|value| value.trim().parse::<i64>().ok())
            .unwrap_or_default(),
        status: values
            .get("status")
            .map(|value| value.trim().to_owned())
            .unwrap_or_default(),
        card_type: values
            .get("card_type")
            .map(|value| value.trim().to_owned())
            .unwrap_or_default(),
        visibility: values
            .get("visibility")
            .map(|value| value.trim().to_owned())
            .unwrap_or_default(),
        as_of: values.get("as_of").and_then(|value| {
            time::OffsetDateTime::parse(
                value.trim(),
                &time::format_description::well_known::Rfc3339,
            )
            .ok()
        }),
        limit: parse_memory_limit(values.get("limit").map(String::as_str)) as i32,
    }
}

fn admin_memory_required_id(raw_query: Option<&str>, missing: &str) -> Result<i64, Box<Response>> {
    let values = admin_auth_query_values(raw_query);
    let Some(raw) = values.get("id") else {
        return Err(Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            missing,
        )));
    };
    let id = raw.trim().parse::<i64>().map_err(|_| {
        Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            if missing == "invalid id" {
                "invalid id"
            } else {
                missing
            },
        ))
    })?;
    if id == 0 {
        return Err(Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            missing,
        )));
    }
    Ok(id)
}

fn admin_memory_optional_restart_id(raw_query: Option<&str>) -> Result<i64, Box<Response>> {
    let values = admin_auth_query_values(raw_query);
    let Some(raw) = values.get("id").map(String::as_str).map(str::trim) else {
        return Ok(0);
    };
    if raw.is_empty() {
        return Ok(0);
    }
    let id = raw
        .parse::<i64>()
        .map_err(|_| Box::new(admin_error_response(StatusCode::BAD_REQUEST, "invalid id")))?;
    if id <= 0 {
        return Err(Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            "invalid id",
        )));
    }
    Ok(id)
}

fn admin_memory_restart_override(
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &[u8],
) -> Result<(AdminMemoryOverride, bool), Box<Response>> {
    let mut req = if admin_request_is_json(headers) {
        if body.is_empty() {
            AdminMemoryRestartRequest::default()
        } else {
            serde_json::from_slice::<AdminMemoryRestartRequest>(body).map_err(|_| {
                Box::new(admin_error_response(
                    StatusCode::BAD_REQUEST,
                    "invalid override",
                ))
            })?
        }
    } else {
        AdminMemoryRestartRequest::default()
    };
    let values = admin_auth_query_values(raw_query);
    if let Some(raw) = values.get("override").map(String::as_str).map(str::trim)
        && !raw.is_empty()
        && let Some(value) = parse_go_bool(raw)
    {
        req.override_ = value;
    }
    if let Some(raw) = values.get("provider").map(String::as_str).map(str::trim)
        && !raw.is_empty()
    {
        req.provider = raw.to_owned();
    }
    if let Some(raw) = values.get("model").map(String::as_str).map(str::trim)
        && !raw.is_empty()
    {
        req.model = raw.to_owned();
    }
    let provider = req.provider.trim().to_lowercase();
    if !matches!(provider.as_str(), "" | "aifarm" | "genkit" | "gemini") {
        return Err(Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            "invalid override",
        )));
    }
    let model = req.model.trim().to_owned();
    let explicit = req.override_ || !provider.is_empty() || !model.is_empty();
    Ok((AdminMemoryOverride { provider, model }, explicit))
}

fn admin_request_is_json(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| contains_ascii_fold(value, "application/json"))
}

fn contains_ascii_fold(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    haystack.as_bytes().windows(needle.len()).any(|window| {
        std::str::from_utf8(window).is_ok_and(|part| part.eq_ignore_ascii_case(needle))
    })
}

fn parse_go_bool(value: &str) -> Option<bool> {
    match value.trim() {
        "1" => Some(true),
        "0" => Some(false),
        value if value.eq_ignore_ascii_case("t") => Some(true),
        value if value.eq_ignore_ascii_case("true") => Some(true),
        value if value.eq_ignore_ascii_case("f") => Some(false),
        value if value.eq_ignore_ascii_case("false") => Some(false),
        _ => None,
    }
}

fn admin_memory_restart_model(model: &str) -> String {
    let model = model.trim();
    if model.is_empty() {
        openplotva_memory::DEFAULT_MEMORY_CONSOLIDATION_MODEL.to_owned()
    } else {
        model.to_owned()
    }
}

fn normalize_memory_restart_provider(provider: &str) -> String {
    let provider = provider.trim();
    if provider.eq_ignore_ascii_case("aifarm") {
        "aifarm".to_owned()
    } else if provider.eq_ignore_ascii_case("genkit") {
        "genkit".to_owned()
    } else if provider.eq_ignore_ascii_case("gemini") {
        "gemini".to_owned()
    } else {
        provider.to_lowercase()
    }
}

fn admin_u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

async fn admin_shield_documents_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &[u8],
) -> Response {
    match method {
        Method::GET => admin_shield_documents_list_response(routes, headers, raw_query).await,
        Method::POST => admin_shield_document_create_response(routes, headers, body).await,
        Method::PUT => {
            admin_shield_document_update_response(routes, headers, raw_query, body).await
        }
        Method::DELETE => admin_shield_document_delete_response(routes, headers, raw_query).await,
        _ => admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed"),
    }
}

async fn admin_shield_documents_list_response(
    routes: &StaticWebRoutes,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(store) = routes.shield_store.as_ref() else {
        return admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "count": 0, "documents": [] }),
        );
    };
    let values = admin_auth_query_values(raw_query);
    let filter = openplotva_shield::ListFilter {
        query: values.get("q").cloned().unwrap_or_default(),
        include_disabled: parse_admin_optional_bool(
            values.get("include_disabled").map(String::as_str),
        )
        .unwrap_or(false),
        limit: parse_shield_limit(values.get("limit").map(String::as_str)),
        offset: 0,
    };
    match store.list_documents(&filter).await {
        Ok(documents) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({
                "count": documents.len(),
                "documents": documents.iter().map(admin_shield_document_json).collect::<Vec<_>>(),
            }),
        ),
        Err(error) => {
            tracing::warn!(%error, "failed to list admin shield documents");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

async fn admin_shield_document_create_response(
    routes: &StaticWebRoutes,
    headers: &HeaderMap,
    body: &[u8],
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(store) = routes.shield_store.as_ref() else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "shield disabled");
    };
    let Ok(req) = serde_json::from_slice::<AdminShieldDocumentRequest>(body) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "invalid json");
    };
    let input =
        match openplotva_shield::normalize_document_input(admin_shield_document_input(req, true)) {
            Ok(input) => input,
            Err(error) => return admin_error_response(StatusCode::BAD_REQUEST, &error.to_string()),
        };
    let embedding = admin_shield_embed_title(routes, &input.title).await;
    match store.create_document(input, embedding.as_ref()).await {
        Ok(document) => admin_json_response(StatusCode::OK, admin_shield_document_json(&document)),
        Err(error) => {
            tracing::warn!(%error, "failed to create admin shield document");
            admin_error_response(StatusCode::BAD_REQUEST, &error.to_string())
        }
    }
}

async fn admin_shield_document_update_response(
    routes: &StaticWebRoutes,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    body: &[u8],
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(store) = routes.shield_store.as_ref() else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "shield disabled");
    };
    let Ok(req) = serde_json::from_slice::<AdminShieldDocumentRequest>(body) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "invalid json");
    };
    let values = admin_auth_query_values(raw_query);
    let mut id = req.id;
    if let Some(raw_id) = values
        .get("id")
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        let Ok(parsed) = raw_id.parse::<i64>() else {
            return admin_error_response(StatusCode::BAD_REQUEST, "invalid id");
        };
        if parsed <= 0 {
            return admin_error_response(StatusCode::BAD_REQUEST, "invalid id");
        }
        id = parsed;
    }
    if id <= 0 {
        return admin_error_response(StatusCode::BAD_REQUEST, "id required");
    }
    let input = match openplotva_shield::normalize_document_input(admin_shield_document_input(
        req, false,
    )) {
        Ok(input) => input,
        Err(error) => return admin_error_response(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    let embedding = admin_shield_embed_title(routes, &input.title).await;
    match store.update_document(id, input, embedding.as_ref()).await {
        Ok(document) => admin_json_response(StatusCode::OK, admin_shield_document_json(&document)),
        Err(error) => {
            tracing::warn!(%error, id, "failed to update admin shield document");
            admin_error_response(StatusCode::BAD_REQUEST, &error.to_string())
        }
    }
}

async fn admin_shield_document_delete_response(
    routes: &StaticWebRoutes,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query);
    let id = match admin_required_i64(&values, "id", "id required", "id required") {
        Ok(value) if value > 0 => value,
        Ok(_) => return admin_error_response(StatusCode::BAD_REQUEST, "id required"),
        Err(response) => return *response,
    };
    if let Some(store) = routes.shield_store.as_ref()
        && let Err(error) = store.delete_document(id).await
    {
        tracing::warn!(%error, id, "failed to delete admin shield document");
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    }
    admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true }))
}

async fn admin_shield_embeddings_rebuild_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
) -> Response {
    if method != Method::POST {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(store) = routes.shield_store.as_ref() else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "shield disabled");
    };
    let Some(embedder) = routes.shield_embedder.as_ref() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    let documents = match store
        .documents_without_embeddings(routes.shield_options.rebuild_batch_size)
        .await
    {
        Ok(documents) => documents,
        Err(error) => {
            tracing::warn!(%error, "failed to list shield documents without embeddings");
            return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
        }
    };
    let mut processed = 0_i32;
    let mut failed = 0_i32;
    for document in documents {
        processed += 1;
        let embedding = match embedder
            .embed_one(
                document.title.trim(),
                routes.shield_options.embedding_dim,
                openplotva_shield::TITLE_EMBEDDING_TASK,
            )
            .await
        {
            Ok(embedding) => embedding,
            Err(error) => {
                failed += 1;
                tracing::warn!(
                    %error,
                    document_id = document.id,
                    "shield title embedding rebuild failed"
                );
                continue;
            }
        };
        if let Err(error) = store
            .update_embedding(document.id, embedding.as_ref())
            .await
        {
            failed += 1;
            tracing::warn!(
                %error,
                document_id = document.id,
                "shield embedding update failed"
            );
        }
    }
    admin_json_response(
        StatusCode::OK,
        serde_json::json!({ "ok": true, "processed": processed, "failed": failed }),
    )
}

async fn admin_shield_test_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    body: &[u8],
) -> Response {
    if method != Method::POST {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(store) = routes.shield_store.as_ref() else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "shield disabled");
    };
    let Ok(req) = serde_json::from_slice::<AdminShieldTestRequest>(body) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "invalid json");
    };
    if req.query.trim().is_empty() {
        return admin_error_response(StatusCode::BAD_REQUEST, "query required");
    }
    let request = openplotva_shield::SearchRequest {
        query: req.query.clone(),
        max_matches: req.max_matches,
        include_candidates: req.debug,
    };
    let embedding = admin_shield_embed_query(routes, &req.query).await;
    match store
        .search_with_vector(&request, &routes.shield_options, embedding.as_ref())
        .await
    {
        Ok(result) => admin_json_response(
            StatusCode::OK,
            admin_shield_test_json(&result, &req.expected_category),
        ),
        Err(error) => {
            tracing::warn!(%error, "failed to test admin shield query");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

fn admin_shield_document_input(
    req: AdminShieldDocumentRequest,
    default_enabled: bool,
) -> openplotva_shield::DocumentInput {
    openplotva_shield::DocumentInput {
        slug: req.slug,
        title: req.title,
        body: req.body,
        category: req.category,
        enabled: req.enabled.unwrap_or(default_enabled),
        priority: req.priority,
    }
}

async fn admin_shield_embed_title(
    routes: &StaticWebRoutes,
    title: &str,
) -> Option<openplotva_storage::PgEmbeddingVector> {
    let embedder = routes.shield_embedder.as_ref()?;
    match embedder
        .embed_one(
            title.trim(),
            routes.shield_options.embedding_dim,
            openplotva_shield::TITLE_EMBEDDING_TASK,
        )
        .await
    {
        Ok(embedding) => embedding,
        Err(error) => {
            tracing::warn!(%error, "shield title embedding failed; storing lexical-only document");
            None
        }
    }
}

async fn admin_shield_embed_query(
    routes: &StaticWebRoutes,
    query: &str,
) -> Option<openplotva_storage::PgEmbeddingVector> {
    let embedder = routes.shield_embedder.as_ref()?;
    match embedder
        .embed_one(
            query,
            routes.shield_options.embedding_dim,
            openplotva_shield::QUERY_EMBEDDING_TASK,
        )
        .await
    {
        Ok(embedding) => embedding,
        Err(error) => {
            tracing::warn!(%error, "shield embedding failed; using lexical-only retrieval");
            None
        }
    }
}

fn parse_shield_limit(raw: Option<&str>) -> i32 {
    parse_admin_positive_i32(raw, 100, 500)
}

fn admin_shield_category_matched(matches: &[openplotva_shield::Match], expected: &str) -> bool {
    let expected = expected.trim();
    if expected.is_empty() {
        return false;
    }
    matches
        .iter()
        .any(|item| item.document.category.trim().eq_ignore_ascii_case(expected))
}

fn admin_shield_document_json(document: &openplotva_shield::Document) -> serde_json::Value {
    serde_json::json!({
        "id": document.id,
        "slug": document.slug,
        "title": document.title,
        "body": document.body,
        "category": document.category,
        "enabled": document.enabled,
        "priority": document.priority,
        "created_at": admin_time_json(document.created_at),
        "updated_at": admin_time_json(document.updated_at),
    })
}

fn admin_shield_scored_document_json(
    item: &openplotva_shield::ScoredDocument,
) -> serde_json::Value {
    serde_json::json!({
        "document": admin_shield_document_json(&item.document),
        "lexical_score": item.lexical_score,
        "vector_score": item.vector_score,
    })
}

fn admin_shield_candidate_json(candidate: &openplotva_shield::Candidate) -> serde_json::Value {
    let mut value = serde_json::Map::new();
    value.insert(
        "document".to_owned(),
        admin_shield_document_json(&candidate.document),
    );
    value.insert(
        "lexical_score".to_owned(),
        serde_json::json!(candidate.lexical_score),
    );
    value.insert(
        "vector_score".to_owned(),
        serde_json::json!(candidate.vector_score),
    );
    value.insert("matched".to_owned(), serde_json::json!(candidate.matched));
    if !candidate.signals.is_empty() {
        value.insert("signals".to_owned(), serde_json::json!(candidate.signals));
    }
    serde_json::Value::Object(value)
}

fn admin_shield_test_json(
    result: &openplotva_shield::SearchResult,
    expected_category: &str,
) -> serde_json::Value {
    let mut value = serde_json::Map::new();
    value.insert("query".to_owned(), serde_json::json!(result.query));
    value.insert(
        "matches".to_owned(),
        serde_json::json!(
            result
                .matches
                .iter()
                .map(admin_shield_scored_document_json)
                .collect::<Vec<_>>()
        ),
    );
    value.insert("context".to_owned(), serde_json::json!(result.context));
    value.insert(
        "lexical_only".to_owned(),
        serde_json::json!(result.lexical_only),
    );
    if !result.candidates.is_empty() {
        value.insert(
            "candidates".to_owned(),
            serde_json::json!(
                result
                    .candidates
                    .iter()
                    .map(admin_shield_candidate_json)
                    .collect::<Vec<_>>()
            ),
        );
    }
    if let Some(debug) = result.debug.as_ref() {
        value.insert(
            "debug".to_owned(),
            serde_json::json!({
                "max_matches": debug.max_matches,
                "candidate_limit": debug.candidate_limit,
                "lexical_min_score": debug.lexical_min_score,
                "vector_min_score": debug.vector_min_score,
            }),
        );
    }
    let expected = expected_category.trim();
    if !expected.is_empty() {
        value.insert("expected_category".to_owned(), serde_json::json!(expected));
        value.insert(
            "category_matched".to_owned(),
            serde_json::json!(admin_shield_category_matched(&result.matches, expected)),
        );
    }
    serde_json::Value::Object(value)
}

fn admin_taskman_jobs_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    if !routes.taskman_inspector.is_configured() {
        return admin_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "task manager not configured",
        );
    }
    let filter = match admin_taskman_jobs_filter(raw_query) {
        Ok(filter) => filter,
        Err(response) => return *response,
    };
    match openplotva_server::RuntimeTaskmanInspector::list_jobs(&routes.taskman_inspector, filter) {
        Ok(result) => {
            admin_json_no_cache_response(StatusCode::OK, admin_taskman_list_json(&result))
        }
        Err(error) => {
            tracing::warn!(%error, "failed to list admin taskman jobs");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

fn admin_taskman_jobs_clear_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::POST && method != Method::DELETE {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    if !routes.taskman_inspector.is_configured() {
        return admin_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "task manager not configured",
        );
    }
    let mut filter = match admin_taskman_jobs_filter(raw_query) {
        Ok(filter) => filter,
        Err(response) => return *response,
    };
    filter.offset = 0;
    filter.limit = 0;
    match routes.taskman_inspector.delete_jobs(filter) {
        Ok(result) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({
                "ok": true,
                "matched": result.matched,
                "deleted": result.deleted,
                "deleted_active": result.deleted_active,
                "skipped_active": result.skipped_active,
            }),
        ),
        Err(error) => {
            tracing::warn!(%error, "failed to clear admin taskman jobs");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

async fn admin_taskman_job_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    if !routes.taskman_inspector.is_configured() {
        return admin_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "task manager not configured",
        );
    }
    let job_id = match admin_taskman_job_id(raw_query) {
        Ok(job_id) => job_id,
        Err(response) => return *response,
    };
    match openplotva_server::RuntimeTaskmanInspector::job(&routes.taskman_inspector, job_id).await {
        Ok(Some(details)) => {
            admin_json_no_cache_response(StatusCode::OK, admin_taskman_details_json(&details))
        }
        Ok(None) => admin_error_response(StatusCode::NOT_FOUND, "not found"),
        Err(error) => {
            tracing::warn!(%error, job_id, "failed to load admin taskman job");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

fn admin_taskman_job_cancel_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::POST && method != Method::PUT {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    if !routes.taskman_inspector.is_configured() {
        return admin_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "task manager not configured",
        );
    }
    let job_id = match admin_taskman_job_id(raw_query) {
        Ok(job_id) => job_id,
        Err(response) => return *response,
    };
    match routes.taskman_inspector.cancel_job(job_id) {
        Ok(()) => admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true })),
        Err(error) => {
            tracing::warn!(%error, job_id, "failed to cancel admin taskman job");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

fn admin_taskman_job_restart_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::POST && method != Method::PUT {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    if !routes.taskman_inspector.is_configured() {
        return admin_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "task manager not configured",
        );
    }
    let job_id = match admin_taskman_job_id(raw_query) {
        Ok(job_id) => job_id,
        Err(response) => return *response,
    };
    match routes.taskman_inspector.restart_job(job_id) {
        Ok(new_job_id) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "ok": true, "new_job_id": new_job_id }),
        ),
        Err(error) => {
            tracing::warn!(%error, job_id, "failed to restart admin taskman job");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

fn admin_taskman_jobs_filter(
    raw_query: Option<&str>,
) -> Result<openplotva_server::RuntimeTaskmanJobsFilter, Box<Response>> {
    let values = admin_auth_query_values(raw_query);
    Ok(openplotva_server::RuntimeTaskmanJobsFilter {
        q: values
            .get("q")
            .map(|value| value.trim().to_owned())
            .unwrap_or_default(),
        status: admin_taskman_csv_values(&values, "status")
            .into_iter()
            .map(|status| {
                admin_taskman_valid_choice(
                    &status,
                    &["pending", "processing", "completed", "failed", "cancelled"],
                    "invalid status",
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
        queue: admin_taskman_csv_values(&values, "queue"),
        user_id: admin_taskman_i64(&values, "user_id", "invalid user_id")?,
        chat_id: admin_taskman_i64(&values, "chat_id", "invalid chat_id")?,
        time_field: values
            .get("time_field")
            .map(|value| {
                admin_taskman_optional_choice(
                    value,
                    &["created_at", "started_at", "completed_at"],
                    "invalid time_field",
                )
            })
            .transpose()?
            .unwrap_or_default(),
        from: values
            .get("from")
            .map(|value| admin_taskman_time(value, "invalid from"))
            .transpose()?
            .unwrap_or_default(),
        to: values
            .get("to")
            .map(|value| admin_taskman_time(value, "invalid to"))
            .transpose()?
            .unwrap_or_default(),
        sort_by: values
            .get("sort_by")
            .map(|value| {
                admin_taskman_optional_choice(
                    value,
                    &["id", "priority", "created_at", "started_at", "completed_at"],
                    "invalid sort_by",
                )
            })
            .transpose()?
            .unwrap_or_default(),
        sort_dir: values
            .get("sort_dir")
            .map(|value| admin_taskman_optional_choice(value, &["asc", "desc"], "invalid sort_dir"))
            .transpose()?
            .unwrap_or_default(),
        offset: admin_taskman_i32(&values, "offset", "invalid offset")?.unwrap_or_default(),
        limit: admin_taskman_i32(&values, "limit", "invalid limit")?.unwrap_or_default(),
    })
}

fn admin_taskman_job_id(raw_query: Option<&str>) -> Result<i64, Box<Response>> {
    let values = admin_auth_query_values(raw_query);
    let raw = values
        .get("job_id")
        .or_else(|| values.get("id"))
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Box::new(admin_error_response(
                StatusCode::BAD_REQUEST,
                "job_id required",
            ))
        })?;
    let id = raw.parse::<i64>().map_err(|_| {
        Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            "invalid job_id",
        ))
    })?;
    if id <= 0 {
        return Err(Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            "invalid job_id",
        )));
    }
    Ok(id)
}

fn admin_taskman_csv_values(values: &BTreeMap<String, String>, key: &str) -> Vec<String> {
    values
        .get(key)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn admin_taskman_i64(
    values: &BTreeMap<String, String>,
    key: &str,
    invalid: &str,
) -> Result<Option<i64>, Box<Response>> {
    let Some(value) = values
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    value
        .parse::<i64>()
        .map(Some)
        .map_err(|_| Box::new(admin_error_response(StatusCode::BAD_REQUEST, invalid)))
}

fn admin_taskman_i32(
    values: &BTreeMap<String, String>,
    key: &str,
    invalid: &str,
) -> Result<Option<i32>, Box<Response>> {
    let Some(value) = values
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    value
        .parse::<i32>()
        .map(Some)
        .map_err(|_| Box::new(admin_error_response(StatusCode::BAD_REQUEST, invalid)))
}

fn admin_taskman_optional_choice(
    value: &str,
    allowed: &[&str],
    invalid: &str,
) -> Result<String, Box<Response>> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(String::new());
    }
    admin_taskman_valid_choice(value, allowed, invalid)
}

fn admin_taskman_valid_choice(
    value: &str,
    allowed: &[&str],
    invalid: &str,
) -> Result<String, Box<Response>> {
    if allowed.contains(&value) {
        Ok(value.to_owned())
    } else {
        Err(Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            invalid,
        )))
    }
}

fn admin_taskman_time(value: &str, invalid: &str) -> Result<String, Box<Response>> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(String::new());
    }
    if let Ok(parsed) = OffsetDateTime::parse(value, &Rfc3339) {
        return Ok(admin_format_time(parsed));
    }
    if let Ok(timestamp) = value.parse::<i64>()
        && let Ok(parsed) = OffsetDateTime::from_unix_timestamp(timestamp)
    {
        return Ok(admin_format_time(parsed));
    }
    Err(Box::new(admin_error_response(
        StatusCode::BAD_REQUEST,
        invalid,
    )))
}

fn admin_taskman_list_json(
    result: &openplotva_server::RuntimeTaskmanJobListResultData,
) -> serde_json::Value {
    serde_json::json!({
        "total": result.total,
        "offset": result.offset,
        "limit": result.limit,
        "summary": {
            "by_status": result.summary.by_status,
            "by_queue": result.summary.by_queue,
        },
        "items": result.items.iter().map(admin_taskman_list_entry_json).collect::<Vec<_>>(),
    })
}

fn admin_taskman_list_entry_json(
    item: &openplotva_server::RuntimeTaskmanJobListEntryData,
) -> serde_json::Value {
    let mut value = admin_taskman_job_base_json(
        item.id,
        &item.queue_name,
        item.priority,
        &item.title,
        Some(serde_json::json!(item.job_type)),
        &item.status,
        item.user_id,
        item.chat_id,
        item.trigger_message_id,
        item.thread_message_id,
        item.progress_message_id,
        item.queue_position_message_id,
        item.result_message_id,
        item.worker_id.as_deref(),
        &item.created_at,
        item.started_at.as_deref(),
        item.completed_at.as_deref(),
        item.error_message.as_deref(),
        item.processing_timeout_seconds,
        item.prompt_hash.as_deref(),
        item.estimated_processing_time,
        item.actual_processing_time,
    );
    if let Some(preview) = item.preview.as_deref().filter(|value| !value.is_empty()) {
        value.insert("preview".to_owned(), serde_json::json!(preview));
    }
    serde_json::Value::Object(value)
}

fn admin_taskman_details_json(
    details: &openplotva_server::RuntimeTaskmanJobDetailsData,
) -> serde_json::Value {
    let mut value = serde_json::Map::new();
    value.insert("job".to_owned(), admin_taskman_job_json(&details.job));
    value.insert(
        "messages".to_owned(),
        serde_json::json!(
            details
                .messages
                .iter()
                .map(admin_taskman_message_json)
                .collect::<Vec<_>>()
        ),
    );
    if let Some(events) = details.events.as_ref() {
        value.insert("events".to_owned(), events.clone());
    }
    serde_json::Value::Object(value)
}

fn admin_taskman_job_json(job: &openplotva_server::RuntimeTaskmanJobData) -> serde_json::Value {
    serde_json::Value::Object(admin_taskman_job_base_json(
        job.id,
        &job.queue_name,
        job.priority,
        &job.title,
        job.payload.clone(),
        &job.status,
        job.user_id,
        job.chat_id,
        job.trigger_message_id,
        job.thread_message_id,
        job.progress_message_id,
        job.queue_position_message_id,
        job.result_message_id,
        job.worker_id.as_deref(),
        &job.created_at,
        job.started_at.as_deref(),
        job.completed_at.as_deref(),
        job.error_message.as_deref(),
        job.processing_timeout_seconds,
        job.prompt_hash.as_deref(),
        job.estimated_processing_time,
        job.actual_processing_time,
    ))
}

#[allow(clippy::too_many_arguments)]
fn admin_taskman_job_base_json(
    id: i64,
    queue_name: &str,
    priority: i32,
    title: &str,
    payload_or_type: Option<serde_json::Value>,
    status: &str,
    user_id: i64,
    chat_id: i64,
    trigger_message_id: i32,
    thread_message_id: Option<i32>,
    progress_message_id: Option<i32>,
    queue_position_message_id: Option<i32>,
    result_message_id: Option<i32>,
    worker_id: Option<&str>,
    created_at: &str,
    started_at: Option<&str>,
    completed_at: Option<&str>,
    error_message: Option<&str>,
    processing_timeout_seconds: i32,
    prompt_hash: Option<&str>,
    estimated_processing_time: Option<i32>,
    actual_processing_time: Option<i32>,
) -> serde_json::Map<String, serde_json::Value> {
    let mut value = serde_json::Map::new();
    value.insert("id".to_owned(), serde_json::json!(id));
    value.insert("queue_name".to_owned(), serde_json::json!(queue_name));
    value.insert("priority".to_owned(), serde_json::json!(priority));
    value.insert("title".to_owned(), serde_json::json!(title));
    if let Some(payload_or_type) = payload_or_type {
        let key = if payload_or_type.is_string() {
            "job_type"
        } else {
            "payload"
        };
        value.insert(key.to_owned(), payload_or_type);
    }
    value.insert("status".to_owned(), serde_json::json!(status));
    value.insert("user_id".to_owned(), serde_json::json!(user_id));
    value.insert("chat_id".to_owned(), serde_json::json!(chat_id));
    value.insert(
        "trigger_message_id".to_owned(),
        serde_json::json!(trigger_message_id),
    );
    admin_insert_i32_option(&mut value, "thread_message_id", thread_message_id);
    admin_insert_i32_option(&mut value, "progress_message_id", progress_message_id);
    admin_insert_i32_option(
        &mut value,
        "queue_position_message_id",
        queue_position_message_id,
    );
    admin_insert_i32_option(&mut value, "result_message_id", result_message_id);
    admin_insert_str_option(&mut value, "worker_id", worker_id);
    value.insert("created_at".to_owned(), serde_json::json!(created_at));
    admin_insert_str_option(&mut value, "started_at", started_at);
    admin_insert_str_option(&mut value, "completed_at", completed_at);
    admin_insert_str_option(&mut value, "error_message", error_message);
    value.insert(
        "processing_timeout_seconds".to_owned(),
        serde_json::json!(processing_timeout_seconds),
    );
    admin_insert_str_option(&mut value, "prompt_hash", prompt_hash);
    admin_insert_i32_option(
        &mut value,
        "estimated_processing_time",
        estimated_processing_time,
    );
    admin_insert_i32_option(&mut value, "actual_processing_time", actual_processing_time);
    value
}

fn admin_taskman_message_json(
    message: &openplotva_server::RuntimeTaskmanJobMessageData,
) -> serde_json::Value {
    serde_json::json!({
        "id": message.id,
        "job_id": message.job_id,
        "message_type": message.message_type,
        "chat_id": message.chat_id,
        "message_id": message.message_id,
        "created_at": message.created_at,
        "status": message.status,
    })
}

fn admin_insert_i32_option(
    value: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    item: Option<i32>,
) {
    if let Some(item) = item {
        value.insert(key.to_owned(), serde_json::json!(item));
    }
}

fn admin_insert_str_option(
    value: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    item: Option<&str>,
) {
    if let Some(item) = item {
        value.insert(key.to_owned(), serde_json::json!(item));
    }
}

async fn admin_chat_get_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query);
    let chat_id =
        match admin_required_i64(&values, "chat_id", "chat_id required", "invalid chat_id") {
            Ok(value) => value,
            Err(response) => return *response,
        };
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_json_response(
            StatusCode::OK,
            serde_json::json!({
                "info": null,
                "settings": null,
                "permissions": null,
                "members_count": 0,
                "blocked": false,
            }),
        );
    };
    sync_admin_members_for_web(routes, chat_id).await;
    let info = admin_fetch_optional_json(pool, SQL_ADMIN_GET_CHAT, chat_id, admin_chat_json).await;
    let settings = admin_fetch_optional_json(
        pool,
        SQL_ADMIN_GET_CHAT_SETTINGS,
        chat_id,
        admin_settings_json,
    )
    .await;
    let permissions = admin_fetch_optional_json(
        pool,
        SQL_ADMIN_GET_CHAT_PERMISSIONS,
        chat_id,
        admin_permissions_json,
    )
    .await;
    let members_count = sqlx::query_scalar::<_, i64>(SQL_ADMIN_COUNT_CHAT_MEMBERS)
        .bind(chat_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .unwrap_or(0);
    let blocked = match routes.redis.clone() {
        Some(redis) => RedisBlockedChatStore::new(redis)
            .blocked_until(chat_id)
            .await
            .ok()
            .flatten()
            .is_some(),
        None => false,
    };
    admin_json_response(
        StatusCode::OK,
        serde_json::json!({
            "info": info,
            "settings": settings,
            "permissions": permissions,
            "members_count": members_count,
            "blocked": blocked,
        }),
    )
}

async fn admin_chat_settings_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    body: &[u8],
) -> Response {
    if method != Method::POST && method != Method::PUT {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Ok(mut req) = serde_json::from_slice::<AdminChatSettingsUpdateRequest>(body) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "bad request");
    };
    if req.chat_id == 0 {
        return admin_error_response(StatusCode::BAD_REQUEST, "chat_id required");
    }
    req.custom_persona = openplotva_web::truncate_custom_persona(req.custom_persona.take());
    let Some(settings_store) = routes.settings_store.as_ref() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    let cur = settings_store
        .get_chat_settings(req.chat_id)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| openplotva_core::ChatSettings::defaults(req.chat_id));
    let chat_type = admin_chat_type(routes, req.chat_id).await;
    let update = openplotva_core::ChatSettingsUpdate {
        chat_id: req.chat_id,
        chat_type,
        mood_alignment: req.mood_alignment,
        custom_persona: req.custom_persona,
        reactivity_percentage: req
            .reactivity_percentage
            .unwrap_or(cur.reactivity_percentage),
        proactivity_percentage: req
            .proactivity_percentage
            .unwrap_or(cur.proactivity_percentage),
        enable_global_text_reply: req
            .enable_global_text_reply
            .unwrap_or(cur.enable_global_text_reply),
        enable_global_draw_reply: req
            .enable_global_draw_reply
            .unwrap_or(cur.enable_global_draw_reply),
        enable_obscenifier: req.enable_obscenifier.unwrap_or(cur.enable_obscenifier),
        enable_profanity: req.enable_profanity.unwrap_or(cur.enable_profanity),
        enable_greet_joiners: req.enable_greet_joiners.unwrap_or(cur.enable_greet_joiners),
        enable_daily_game: req
            .enable_daily_game
            .unwrap_or_else(|| cur.enable_daily_game.unwrap_or(true)),
        daily_game_theme: req
            .daily_game_theme
            .unwrap_or_else(|| cur.daily_game_theme.unwrap_or_default()),
        greeting_html: req.greeting_html,
    };
    match settings_store.upsert_chat_settings(&update).await {
        Ok(()) => admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true })),
        Err(error) => {
            tracing::warn!(%error, chat_id = req.chat_id, "failed to update admin chat settings");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

async fn admin_chat_block_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::POST {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query);
    let chat_id =
        match admin_required_i64(&values, "chat_id", "chat_id required", "invalid chat_id") {
            Ok(value) => value,
            Err(response) => return *response,
        };
    let minutes = values
        .get("minutes")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(10);
    let until = OffsetDateTime::now_utc() + time::Duration::minutes(minutes);
    let Some(redis) = routes.redis.clone() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    let ttl = Duration::from_secs((minutes as u64).saturating_mul(60));
    match RedisBlockedChatStore::new(redis)
        .block_chat_until_with_ttl(chat_id, until, ttl)
        .await
    {
        Ok(()) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "ok": true, "until": admin_format_time(until) }),
        ),
        Err(error) => {
            tracing::warn!(%error, chat_id, "failed to block chat from admin web");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

async fn admin_chat_unblock_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::DELETE {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query);
    let chat_id =
        match admin_required_i64(&values, "chat_id", "chat_id required", "invalid chat_id") {
            Ok(value) => value,
            Err(response) => return *response,
        };
    let Some(redis) = routes.redis_inspector() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    let Some(redis_client) = routes.redis.clone() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    let key = RedisBlockedChatStore::new(redis_client).key_for_chat(chat_id);
    match redis.delete_key(&key).await {
        Ok(()) => admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true })),
        Err(error) => {
            tracing::warn!(%error, chat_id, "failed to unblock chat from admin web");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

async fn admin_chat_members_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query);
    let chat_id =
        match admin_required_i64(&values, "chat_id", "chat_id required", "invalid chat_id") {
            Ok(value) => value,
            Err(response) => return *response,
        };
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to load members");
    };
    sync_admin_members_for_web(routes, chat_id).await;
    match sqlx::query(SQL_ADMIN_LIST_CHAT_MEMBERS_WITH_USERS)
        .bind(chat_id)
        .fetch_all(pool)
        .await
    {
        Ok(rows) => {
            let members = rows
                .into_iter()
                .map(|row| {
                    serde_json::json!({
                        "member": admin_chat_member_prefixed_json(&row),
                        "user": admin_user_prefixed_json(&row),
                    })
                })
                .collect::<Vec<_>>();
            admin_json_response(StatusCode::OK, serde_json::json!({ "members": members }))
        }
        Err(error) => {
            tracing::warn!(%error, chat_id, "failed to list admin chat members");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to load members")
        }
    }
}

async fn sync_admin_members_for_web(routes: &StaticWebRoutes, chat_id: i64) {
    let (Some(member_store), Some(telegram)) = (&routes.member_store, &routes.telegram) else {
        return;
    };
    match settings::sync_chat_admins_with_sources(member_store, telegram, chat_id).await {
        Ok(report) => tracing::debug!(
            chat_id,
            source = ?report.source,
            admin_count = report.admin_count,
            member_upsert_errors = report.member_upsert_errors,
            user_upsert_errors = report.user_upsert_errors,
            cache_errors = report.cache_errors,
            "refreshed admin members for web route"
        ),
        Err(error) => {
            tracing::debug!(%error, chat_id, "Telegram admin freshness failed for web route");
        }
    }
}

async fn admin_chats_list_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    let q = admin_auth_query_values(raw_query)
        .remove("q")
        .map(|value| value.trim().to_lowercase())
        .filter(|value| !value.is_empty());
    let rows = match q.as_deref() {
        Some(search) => {
            sqlx::query(SQL_ADMIN_LIST_CHATS_FILTERED)
                .bind(search)
                .bind(ADMIN_PAGE_LIMIT)
                .bind(0_i64)
                .fetch_all(pool)
                .await
        }
        None => {
            sqlx::query(SQL_ADMIN_LIST_CHATS)
                .bind(ADMIN_PAGE_LIMIT)
                .bind(0_i64)
                .fetch_all(pool)
                .await
        }
    };
    match rows {
        Ok(rows) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "chats": rows.iter().map(admin_chat_json).collect::<Vec<_>>() }),
        ),
        Err(error) => {
            tracing::warn!(%error, "failed to list admin chats");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

async fn admin_chats_search_by_member_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query);
    let username = values
        .get("username")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let user_id = values
        .get("user_id")
        .and_then(|value| value.parse::<i64>().ok());
    if username.is_none() && user_id.is_none() {
        return admin_error_response(StatusCode::BAD_REQUEST, "username or user_id required");
    }
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "database error");
    };
    match sqlx::query(SQL_ADMIN_SEARCH_CHATS_BY_MEMBER)
        .bind(username.as_deref())
        .bind(user_id)
        .bind(ADMIN_PAGE_LIMIT)
        .fetch_all(pool)
        .await
    {
        Ok(rows) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "chats": rows.iter().map(admin_chat_json).collect::<Vec<_>>() }),
        ),
        Err(error) => {
            tracing::warn!(%error, "failed to search admin chats by member");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "database error")
        }
    }
}

async fn admin_users_list_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "database error");
    };
    let q = admin_auth_query_values(raw_query)
        .remove("q")
        .map(|value| value.trim().to_lowercase())
        .filter(|value| !value.is_empty());
    match sqlx::query(SQL_ADMIN_LIST_USERS_FILTERED)
        .bind(q.as_deref())
        .bind(ADMIN_PAGE_LIMIT)
        .bind(0_i64)
        .fetch_all(pool)
        .await
    {
        Ok(rows) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CACHE_CONTROL, "no-cache"),
            ],
            format!(
                "{}\n",
                serde_json::json!({ "users": rows.iter().map(admin_user_json).collect::<Vec<_>>() })
            ),
        )
            .into_response(),
        Err(error) => {
            tracing::warn!(%error, "failed to list admin users");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "database error")
        }
    }
}

async fn admin_vip_users_list_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "database error");
    };
    let values = admin_auth_query_values(raw_query);
    let status = values
        .get("status")
        .map(|value| value.trim().to_lowercase())
        .filter(|value| matches!(value.as_str(), "active" | "expired" | "all"))
        .unwrap_or_else(|| "active".to_owned());
    let q = values
        .get("q")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    match sqlx::query(SQL_ADMIN_LIST_VIP_USERS)
        .bind(status)
        .bind(q.as_deref())
        .bind(ADMIN_PAGE_LIMIT)
        .bind(0_i64)
        .fetch_all(pool)
        .await
    {
        Ok(rows) => admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "users": rows.iter().map(admin_vip_user_json).collect::<Vec<_>>() }),
        ),
        Err(error) => {
            tracing::warn!(%error, "failed to list admin vip users");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "database error")
        }
    }
}

async fn admin_user_get_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query);
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_error_response(StatusCode::NOT_FOUND, "not found");
    };
    let user_row = if let Some(id_raw) = values.get("id").filter(|value| !value.is_empty()) {
        let Ok(id) = id_raw.parse::<i64>() else {
            return admin_error_response(StatusCode::BAD_REQUEST, "invalid id");
        };
        sqlx::query(SQL_ADMIN_GET_USER)
            .bind(id)
            .fetch_optional(pool)
            .await
    } else if let Some(username) = values.get("username").filter(|value| !value.is_empty()) {
        sqlx::query(SQL_ADMIN_GET_USER_BY_USERNAME)
            .bind(username)
            .fetch_optional(pool)
            .await
    } else {
        return admin_error_response(StatusCode::BAD_REQUEST, "id or username required");
    };
    let user_row = match user_row {
        Ok(Some(row)) => row,
        Ok(None) | Err(_) => return admin_error_response(StatusCode::NOT_FOUND, "not found"),
    };
    let user_id = user_row.try_get::<i64, _>("id").unwrap_or_default();
    let payment_store = PostgresPaymentStore::new(pool.clone());
    let vip_store = routes
        .vip_store
        .clone()
        .unwrap_or_else(|| PostgresVipStore::new(pool.clone()));
    let subscription = payment_store
        .get_active_subscription(user_id)
        .await
        .ok()
        .flatten()
        .map(admin_subscription_json);
    let vip_cache = vip_store
        .get_vip_cache(user_id)
        .await
        .ok()
        .flatten()
        .map(admin_vip_cache_json);
    let vip_summary = match vip_store.get_vip_summary_by_user(user_id).await {
        Ok(summary) => admin_vip_summary_json(summary.as_ref()),
        Err(_) => admin_vip_summary_json(None),
    };
    let vip_events = match vip_store.list_vip_events_by_user(user_id).await {
        Ok(events) => events.iter().map(admin_vip_event_json).collect::<Vec<_>>(),
        Err(error) => {
            tracing::warn!(%error, user_id, "failed to load admin vip events");
            return admin_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load vip events",
            );
        }
    };
    let subscriptions = match payment_store.list_subscriptions_by_user(user_id).await {
        Ok(items) => items
            .iter()
            .map(admin_subscription_artifact_json)
            .collect::<Vec<_>>(),
        Err(error) => {
            tracing::warn!(%error, user_id, "failed to load admin subscriptions");
            return admin_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load subscriptions",
            );
        }
    };
    admin_json_response(
        StatusCode::OK,
        serde_json::json!({
            "user": admin_user_json(&user_row),
            "subscription": subscription,
            "vip": vip_cache,
            "vip_summary": vip_summary,
            "vip_events": vip_events,
            "subscriptions": subscriptions,
        }),
    )
}

async fn admin_user_grant_vip_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    body: &[u8],
) -> Response {
    if method != Method::POST && method != Method::PUT {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Ok(req) = serde_json::from_slice::<AdminVipGrantRequest>(body) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "bad request");
    };
    let Some(user_id) = admin_i64_from_json(&req.user_id) else {
        return admin_error_response(
            StatusCode::BAD_REQUEST,
            "user_id and non-zero signed days required",
        );
    };
    if user_id == 0 || req.days == 0 {
        return admin_error_response(
            StatusCode::BAD_REQUEST,
            "user_id and non-zero signed days required",
        );
    }
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_error_response(StatusCode::NOT_FOUND, "user not found");
    };
    let placeholder_name = format!("Telegram user {user_id}");
    if let Err(error) = sqlx::query(SQL_ADMIN_ENSURE_USER)
        .bind(user_id)
        .bind(&placeholder_name)
        .execute(pool)
        .await
    {
        tracing::warn!(%error, user_id, "failed to ensure admin vip user");
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "user not found");
    }
    let admin_id = current_admin_user_id(headers, &routes.bot_token);
    let reason = admin_non_empty_string(&req.reason)
        .unwrap_or_else(|| format!("web admin adjustment by {admin_id}"));
    let actor_user_id = admin_existing_user_id(pool, admin_id).await;
    let vip_store = routes
        .vip_store
        .clone()
        .unwrap_or_else(|| PostgresVipStore::new(pool.clone()));
    match vip_store
        .create_vip_event(openplotva_storage::VipEventCreate {
            user_id,
            event_type: openplotva_core::VIP_EVENT_TYPE_ADMIN_ADJUSTMENT,
            delta_seconds: openplotva_core::vip_days_to_seconds(req.days),
            subscription_id: None,
            actor_user_id,
            reason: Some(&reason),
        })
        .await
    {
        Ok(event) => {
            let cache_invalidated = match vip_store.delete_vip_cache(user_id).await {
                Ok(()) => true,
                Err(error) => {
                    tracing::warn!(%error, user_id, "failed to invalidate admin vip cache");
                    false
                }
            };
            admin_json_response(
                StatusCode::OK,
                serde_json::json!({
                    "ok": true,
                    "event_id": event.id,
                    "expires_at": admin_format_time(event.effective_expires_at),
                    "cache_invalidated": cache_invalidated,
                }),
            )
        }
        Err(error) => {
            tracing::warn!(%error, user_id, "failed to grant admin vip");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

async fn admin_user_revoke_vip_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::DELETE {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query);
    let user_id =
        match admin_required_i64(&values, "user_id", "user_id required", "invalid user_id") {
            Ok(value) => value,
            Err(response) => return *response,
        };
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_json_response(
            StatusCode::OK,
            serde_json::json!({ "ok": true, "revoked": false }),
        );
    };
    let vip_store = routes
        .vip_store
        .clone()
        .unwrap_or_else(|| PostgresVipStore::new(pool.clone()));
    let summary = vip_store
        .get_vip_summary_by_user(user_id)
        .await
        .ok()
        .flatten();
    if !admin_vip_summary_can_be_revoked(summary.as_ref()) {
        let cache_invalidated = match vip_store.delete_vip_cache(user_id).await {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, user_id, "failed to invalidate inactive admin vip cache");
                false
            }
        };
        return admin_json_response(
            StatusCode::OK,
            serde_json::json!({
                "ok": true,
                "revoked": false,
                "cache_invalidated": cache_invalidated,
            }),
        );
    }
    let summary = summary.expect("checked");
    let admin_id = current_admin_user_id(headers, &routes.bot_token);
    let reason = values
        .get("reason")
        .and_then(|value| admin_non_empty_string(value))
        .unwrap_or_else(|| format!("web admin revoke by {admin_id}"));
    let actor_user_id = admin_existing_user_id(pool, admin_id).await;
    match vip_store
        .create_vip_event(openplotva_storage::VipEventCreate {
            user_id,
            event_type: openplotva_core::VIP_EVENT_TYPE_ADMIN_REVOKE,
            delta_seconds: -summary.remaining_seconds,
            subscription_id: None,
            actor_user_id,
            reason: Some(&reason),
        })
        .await
    {
        Ok(_) => {
            let cache_invalidated = match vip_store.delete_vip_cache(user_id).await {
                Ok(()) => true,
                Err(error) => {
                    tracing::warn!(%error, user_id, "failed to invalidate revoked admin vip cache");
                    false
                }
            };
            admin_json_response(
                StatusCode::OK,
                serde_json::json!({
                    "ok": true,
                    "revoked": true,
                    "cache_invalidated": cache_invalidated,
                }),
            )
        }
        Err(error) => {
            tracing::warn!(%error, user_id, "failed to revoke admin vip");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

async fn admin_user_delete_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::DELETE {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let values = admin_auth_query_values(raw_query);
    let user_id = match admin_required_i64(&values, "id", "id required", "invalid id") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let Some(pool) = routes.postgres.as_ref() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    match sqlx::query(SQL_ADMIN_SAFE_DELETE_USER)
        .bind(user_id)
        .execute(pool)
        .await
    {
        Ok(_) => admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true })),
        Err(error) => {
            tracing::warn!(%error, user_id, "failed to delete admin user");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

async fn admin_chat_type(routes: &StaticWebRoutes, chat_id: i64) -> String {
    let Some(pool) = routes.postgres.as_ref() else {
        return "private".to_owned();
    };
    sqlx::query_scalar::<_, String>(SQL_ADMIN_GET_CHAT_TYPE)
        .bind(chat_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "private".to_owned())
}

async fn admin_fetch_optional_json(
    pool: &PgPool,
    query: &'static str,
    id: i64,
    mapper: fn(&PgRow) -> serde_json::Value,
) -> Option<serde_json::Value> {
    sqlx::query(query)
        .bind(id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .as_ref()
        .map(mapper)
}

fn admin_required_i64(
    values: &BTreeMap<String, String>,
    key: &str,
    missing: &str,
    invalid: &str,
) -> Result<i64, Box<Response>> {
    let Some(value) = values.get(key).filter(|value| !value.is_empty()) else {
        return Err(Box::new(admin_error_response(
            StatusCode::BAD_REQUEST,
            missing,
        )));
    };
    value
        .parse::<i64>()
        .map_err(|_| Box::new(admin_error_response(StatusCode::BAD_REQUEST, invalid)))
}

fn current_admin_user_id(headers: &HeaderMap, secret: &str) -> i64 {
    headers
        .get("X-Telegram-User-ID")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<i64>().ok())
        .or_else(|| admin_session_user_id(headers, secret))
        .unwrap_or(0)
}

async fn admin_existing_user_id(pool: &PgPool, user_id: i64) -> Option<i64> {
    if user_id <= 0 {
        return None;
    }
    sqlx::query(SQL_ADMIN_GET_USER)
        .bind(user_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .map(|_| user_id)
}

fn admin_non_empty_string(value: &str) -> Option<String> {
    let normalized = value.trim().to_owned();
    (!normalized.is_empty()).then_some(normalized)
}

fn admin_i64_from_json(value: &serde_json::Value) -> Option<i64> {
    value.as_i64().or_else(|| {
        value
            .as_str()
            .and_then(|value| value.trim().parse::<i64>().ok())
    })
}

fn admin_format_time(value: OffsetDateTime) -> String {
    value
        .format(&Rfc3339)
        .unwrap_or_else(|_| value.unix_timestamp().to_string())
}

fn admin_time_json(value: Option<OffsetDateTime>) -> serde_json::Value {
    value
        .map(admin_format_time)
        .map(serde_json::Value::String)
        .unwrap_or(serde_json::Value::Null)
}

fn admin_row_time(row: &PgRow, key: &str) -> serde_json::Value {
    admin_time_json(row.try_get::<Option<OffsetDateTime>, _>(key).ok().flatten())
}

fn admin_jsonb_base64(row: &PgRow, key: &str) -> serde_json::Value {
    row.try_get::<Option<serde_json::Value>, _>(key)
        .ok()
        .flatten()
        .and_then(|value| serde_json::to_vec(&value).ok())
        .map(|bytes| BASE64_STANDARD.encode(bytes))
        .map(serde_json::Value::String)
        .unwrap_or(serde_json::Value::Null)
}

fn admin_i64_json(row: &PgRow, key: &str) -> serde_json::Value {
    row.try_get::<Option<i64>, _>(key)
        .ok()
        .flatten()
        .map(serde_json::Value::from)
        .unwrap_or(serde_json::Value::Null)
}

fn admin_bool_json(row: &PgRow, key: &str) -> serde_json::Value {
    row.try_get::<Option<bool>, _>(key)
        .ok()
        .flatten()
        .map(serde_json::Value::from)
        .unwrap_or(serde_json::Value::Null)
}

fn admin_string_json(row: &PgRow, key: &str) -> serde_json::Value {
    row.try_get::<Option<String>, _>(key)
        .ok()
        .flatten()
        .map(serde_json::Value::from)
        .unwrap_or(serde_json::Value::Null)
}

fn admin_chat_json(row: &PgRow) -> serde_json::Value {
    serde_json::json!({
        "id": row.try_get::<i64, _>("id").unwrap_or_default(),
        "type": row.try_get::<String, _>("type").unwrap_or_default(),
        "title": admin_string_json(row, "title"),
        "username": admin_string_json(row, "username"),
        "first_name": admin_string_json(row, "first_name"),
        "last_name": admin_string_json(row, "last_name"),
        "is_forum": admin_bool_json(row, "is_forum"),
        "active_usernames": admin_jsonb_base64(row, "active_usernames"),
        "available_reactions": admin_jsonb_base64(row, "available_reactions"),
        "bio": admin_string_json(row, "bio"),
        "has_private_forwards": admin_bool_json(row, "has_private_forwards"),
        "has_restricted_voice_and_video_messages": admin_bool_json(row, "has_restricted_voice_and_video_messages"),
        "join_to_send_messages": admin_bool_json(row, "join_to_send_messages"),
        "join_by_request": admin_bool_json(row, "join_by_request"),
        "description": admin_string_json(row, "description"),
        "invite_link": admin_string_json(row, "invite_link"),
        "pinned_message": admin_jsonb_base64(row, "pinned_message"),
        "permissions": admin_jsonb_base64(row, "permissions"),
        "slow_mode_delay": admin_i64_json(row, "slow_mode_delay"),
        "message_auto_delete_time": admin_i64_json(row, "message_auto_delete_time"),
        "has_aggressive_anti_spam_enabled": admin_bool_json(row, "has_aggressive_anti_spam_enabled"),
        "has_hidden_members": admin_bool_json(row, "has_hidden_members"),
        "has_protected_content": admin_bool_json(row, "has_protected_content"),
        "has_visible_history": admin_bool_json(row, "has_visible_history"),
        "sticker_set_name": admin_string_json(row, "sticker_set_name"),
        "can_set_sticker_set": admin_bool_json(row, "can_set_sticker_set"),
        "linked_chat_id": admin_i64_json(row, "linked_chat_id"),
        "location": admin_jsonb_base64(row, "location"),
        "discovered": admin_row_time(row, "discovered"),
        "updated": admin_row_time(row, "updated"),
    })
}

fn admin_user_json(row: &PgRow) -> serde_json::Value {
    serde_json::json!({
        "id": row.try_get::<i64, _>("id").unwrap_or_default(),
        "is_premium": admin_bool_json(row, "is_premium"),
        "first_name": row.try_get::<String, _>("first_name").unwrap_or_default(),
        "last_name": admin_string_json(row, "last_name"),
        "username": admin_string_json(row, "username"),
        "language_code": admin_string_json(row, "language_code"),
        "is_vip": admin_bool_json(row, "is_vip"),
        "settings": admin_jsonb_base64(row, "settings"),
        "discovered": admin_row_time(row, "discovered"),
        "updated": admin_row_time(row, "updated"),
    })
}

fn admin_vip_user_json(row: &PgRow) -> serde_json::Value {
    let remaining_seconds = row
        .try_get::<Option<i64>, _>("remaining_seconds")
        .ok()
        .flatten()
        .unwrap_or_default();
    let remaining_days = ((remaining_seconds as f64 / openplotva_core::VIP_SECONDS_PER_DAY as f64)
        .ceil() as i64)
        .max(0);
    serde_json::json!({
        "user": admin_user_json(row),
        "vip_summary": {
            "active": row.try_get::<Option<bool>, _>("vip_active").ok().flatten().unwrap_or(false),
            "has_history": true,
            "expires_at": admin_row_time(row, "vip_expires_at"),
            "remaining_seconds": remaining_seconds,
            "remaining_days": remaining_days,
            "latest_event_id": row.try_get::<Option<i64>, _>("latest_event_id").ok().flatten(),
            "latest_event_type": admin_string_json(row, "latest_event_type"),
            "latest_delta_seconds": row.try_get::<Option<i64>, _>("latest_delta_seconds").ok().flatten().unwrap_or_default(),
            "latest_reason": admin_string_json(row, "latest_reason"),
            "latest_created_at": admin_row_time(row, "latest_created_at"),
        },
        "subscriptions_count": row.try_get::<Option<i64>, _>("subscriptions_count").ok().flatten().unwrap_or_default(),
        "latest_subscription_expires_at": admin_row_time(row, "latest_subscription_expires_at"),
    })
}

fn admin_settings_json(row: &PgRow) -> serde_json::Value {
    serde_json::json!({
        "chat_id": row.try_get::<i64, _>("chat_id").unwrap_or_default(),
        "mood_alignment": admin_string_json(row, "mood_alignment"),
        "custom_persona": admin_string_json(row, "custom_persona"),
        "updated": admin_row_time(row, "updated"),
        "reactivity_percentage": row.try_get::<i32, _>("reactivity_percentage").unwrap_or_default(),
        "proactivity_percentage": row.try_get::<i32, _>("proactivity_percentage").unwrap_or_default(),
        "enable_global_text_reply": row.try_get::<bool, _>("enable_global_text_reply").unwrap_or(false),
        "enable_global_draw_reply": row.try_get::<bool, _>("enable_global_draw_reply").unwrap_or(false),
        "enable_obscenifier": row.try_get::<bool, _>("enable_obscenifier").unwrap_or(false),
        "enable_profanity": row.try_get::<bool, _>("enable_profanity").unwrap_or(false),
        "enable_greet_joiners": row.try_get::<bool, _>("enable_greet_joiners").unwrap_or(false),
        "enable_daily_game": admin_bool_json(row, "enable_daily_game"),
        "daily_game_theme": admin_string_json(row, "daily_game_theme"),
        "greeting_html": admin_string_json(row, "greeting_html"),
    })
}

fn admin_permissions_json(row: &PgRow) -> serde_json::Value {
    serde_json::json!({
        "chat_id": row.try_get::<i64, _>("chat_id").unwrap_or_default(),
        "status": row.try_get::<String, _>("status").unwrap_or_default(),
        "can_manage_chat": admin_bool_json(row, "can_manage_chat"),
        "can_delete_messages": admin_bool_json(row, "can_delete_messages"),
        "can_manage_video_chats": admin_bool_json(row, "can_manage_video_chats"),
        "can_restrict_members": admin_bool_json(row, "can_restrict_members"),
        "can_promote_members": admin_bool_json(row, "can_promote_members"),
        "can_change_info": admin_bool_json(row, "can_change_info"),
        "can_invite_users": admin_bool_json(row, "can_invite_users"),
        "can_post_messages": admin_bool_json(row, "can_post_messages"),
        "can_edit_messages": admin_bool_json(row, "can_edit_messages"),
        "can_pin_messages": admin_bool_json(row, "can_pin_messages"),
        "can_manage_topics": admin_bool_json(row, "can_manage_topics"),
        "can_send_messages": admin_bool_json(row, "can_send_messages"),
        "can_send_media_messages": admin_bool_json(row, "can_send_media_messages"),
        "can_send_polls": admin_bool_json(row, "can_send_polls"),
        "can_send_other_messages": admin_bool_json(row, "can_send_other_messages"),
        "can_add_web_page_previews": admin_bool_json(row, "can_add_web_page_previews"),
        "last_checked_at": admin_row_time(row, "last_checked_at"),
        "last_error_at": admin_row_time(row, "last_error_at"),
        "error_count": row.try_get::<Option<i32>, _>("error_count").ok().flatten(),
        "error_message": admin_string_json(row, "error_message"),
        "created_at": admin_row_time(row, "created_at"),
        "updated_at": admin_row_time(row, "updated_at"),
    })
}

fn admin_chat_member_prefixed_json(row: &PgRow) -> serde_json::Value {
    serde_json::json!({
        "chat_id": row.try_get::<i64, _>("member_chat_id").unwrap_or_default(),
        "user_id": row.try_get::<i64, _>("member_user_id").unwrap_or_default(),
        "status": row.try_get::<String, _>("member_status").unwrap_or_default(),
        "is_anonymous": admin_bool_json(row, "member_is_anonymous"),
        "custom_title": admin_string_json(row, "member_custom_title"),
        "can_be_edited": admin_bool_json(row, "member_can_be_edited"),
        "can_manage_chat": admin_bool_json(row, "member_can_manage_chat"),
        "can_delete_messages": admin_bool_json(row, "member_can_delete_messages"),
        "can_manage_video_chats": admin_bool_json(row, "member_can_manage_video_chats"),
        "can_restrict_members": admin_bool_json(row, "member_can_restrict_members"),
        "can_promote_members": admin_bool_json(row, "member_can_promote_members"),
        "can_change_info": admin_bool_json(row, "member_can_change_info"),
        "can_invite_users": admin_bool_json(row, "member_can_invite_users"),
        "can_post_messages": admin_bool_json(row, "member_can_post_messages"),
        "can_edit_messages": admin_bool_json(row, "member_can_edit_messages"),
        "can_pin_messages": admin_bool_json(row, "member_can_pin_messages"),
        "can_manage_topics": admin_bool_json(row, "member_can_manage_topics"),
        "can_send_messages": admin_bool_json(row, "member_can_send_messages"),
        "can_send_media_messages": admin_bool_json(row, "member_can_send_media_messages"),
        "can_send_polls": admin_bool_json(row, "member_can_send_polls"),
        "can_send_other_messages": admin_bool_json(row, "member_can_send_other_messages"),
        "can_add_web_page_previews": admin_bool_json(row, "member_can_add_web_page_previews"),
        "until_date": admin_row_time(row, "member_until_date"),
        "created_at": admin_row_time(row, "member_created_at"),
        "updated_at": admin_row_time(row, "member_updated_at"),
        "last_message_at": admin_row_time(row, "member_last_message_at"),
    })
}

fn admin_user_prefixed_json(row: &PgRow) -> serde_json::Value {
    if row
        .try_get::<Option<i64>, _>("user_id")
        .ok()
        .flatten()
        .is_none()
    {
        return serde_json::Value::Null;
    }
    serde_json::json!({
        "id": row.try_get::<i64, _>("user_id").unwrap_or_default(),
        "is_premium": admin_bool_json(row, "user_is_premium"),
        "first_name": row.try_get::<String, _>("user_first_name").unwrap_or_default(),
        "last_name": admin_string_json(row, "user_last_name"),
        "username": admin_string_json(row, "user_username"),
        "language_code": admin_string_json(row, "user_language_code"),
        "is_vip": admin_bool_json(row, "user_is_vip"),
        "settings": admin_jsonb_base64(row, "user_settings"),
        "discovered": admin_row_time(row, "user_discovered"),
        "updated": admin_row_time(row, "user_updated"),
    })
}

fn admin_subscription_json(item: openplotva_storage::SubscriptionRecord) -> serde_json::Value {
    serde_json::json!({
        "id": item.id,
        "user_id": item.user_id,
        "telegram_payment_charge_id": item.telegram_payment_charge_id,
        "provider_payment_charge_id": item.provider_payment_charge_id,
        "expires_at": admin_format_time(item.expires_at),
        "created_at": admin_format_time(item.created_at),
        "updated_at": admin_format_time(item.updated_at),
        "canceled_at": admin_time_json(item.canceled_at),
        "refunded_at": admin_time_json(item.refunded_at),
    })
}

fn admin_subscription_artifact_json(
    item: &openplotva_storage::SubscriptionRecord,
) -> serde_json::Value {
    serde_json::json!({
        "id": item.id,
        "telegram_payment_charge_id": item.telegram_payment_charge_id,
        "provider_payment_charge_id": item.provider_payment_charge_id,
        "expires_at": admin_format_time(item.expires_at),
        "created_at": admin_format_time(item.created_at),
        "updated_at": admin_format_time(item.updated_at),
        "canceled_at": admin_time_json(item.canceled_at),
        "refunded_at": admin_time_json(item.refunded_at),
        "status": admin_subscription_status(item.expires_at, item.canceled_at, item.refunded_at),
    })
}

fn admin_vip_cache_json(item: openplotva_storage::VipCacheRecord) -> serde_json::Value {
    serde_json::json!({
        "user_id": item.user_id,
        "is_vip": item.is_vip,
        "expires_at": admin_format_time(item.expires_at),
        "created_at": admin_format_time(item.created_at),
        "updated_at": admin_format_time(item.updated_at),
    })
}

fn admin_vip_summary_json(
    summary: Option<&openplotva_storage::VipSummaryRecord>,
) -> serde_json::Value {
    let Some(summary) = summary else {
        return serde_json::json!({
            "active": false,
            "has_history": false,
            "remaining_seconds": 0,
            "remaining_days": 0,
        });
    };
    let active = summary.is_active && OffsetDateTime::now_utc() < summary.effective_expires_at;
    let remaining_days = if active {
        let seconds = (summary.effective_expires_at - OffsetDateTime::now_utc()).whole_seconds();
        ((seconds as f64 / openplotva_core::VIP_SECONDS_PER_DAY as f64).round() as i64).max(0)
    } else {
        0
    };
    serde_json::json!({
        "active": active,
        "has_history": true,
        "expires_at": admin_format_time(summary.effective_expires_at),
        "remaining_seconds": summary.remaining_seconds,
        "remaining_days": remaining_days.max(0),
        "latest_event_id": summary.latest_event_id,
        "latest_event_type": summary.latest_event_type,
        "latest_reason": summary.latest_reason,
        "latest_created_at": admin_format_time(summary.latest_created_at),
    })
}

fn admin_vip_event_json(item: &openplotva_storage::VipEventListRecord) -> serde_json::Value {
    let actor_label = item
        .actor_username
        .as_ref()
        .filter(|value| !value.is_empty())
        .map(|value| format!("@{value}"))
        .or_else(|| {
            item.actor_first_name
                .clone()
                .filter(|value| !value.is_empty())
        })
        .or_else(|| item.actor_user_id.map(|value| value.to_string()))
        .unwrap_or_default();
    let subscription_status = if item.subscription_id.is_some() {
        admin_subscription_status(
            item.subscription_expires_at
                .unwrap_or(item.effective_expires_at),
            item.subscription_canceled_at,
            item.subscription_refunded_at,
        )
    } else {
        String::new()
    };
    serde_json::json!({
        "id": item.id,
        "event_type": item.event_type,
        "delta_seconds": item.delta_seconds,
        "delta_days": item.delta_seconds as f64 / openplotva_core::VIP_SECONDS_PER_DAY as f64,
        "effective_expires_at": admin_format_time(item.effective_expires_at),
        "actor_user_id": item.actor_user_id,
        "actor_label": actor_label,
        "reason": item.reason,
        "created_at": admin_format_time(item.created_at),
        "subscription_id": item.subscription_id,
        "telegram_payment_charge_id": item.telegram_payment_charge_id,
        "provider_payment_charge_id": item.provider_payment_charge_id,
        "subscription_status": subscription_status,
    })
}

fn admin_subscription_status(
    expires_at: OffsetDateTime,
    canceled_at: Option<OffsetDateTime>,
    refunded_at: Option<OffsetDateTime>,
) -> String {
    if refunded_at.is_some() {
        "refunded".to_owned()
    } else if canceled_at.is_some() {
        "canceled".to_owned()
    } else if OffsetDateTime::now_utc() < expires_at {
        "active".to_owned()
    } else {
        "expired".to_owned()
    }
}

fn admin_vip_summary_can_be_revoked(
    summary: Option<&openplotva_storage::VipSummaryRecord>,
) -> bool {
    summary
        .map(|summary| {
            summary.is_active && OffsetDateTime::now_utc() < summary.effective_expires_at
        })
        .unwrap_or(false)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdminAuthFailure {
    Unauthorized,
    Forbidden,
}

fn admin_auth_failure_response(error: AdminAuthFailure) -> Response {
    match error {
        AdminAuthFailure::Unauthorized => {
            admin_error_response(StatusCode::FORBIDDEN, "unauthorized")
        }
        AdminAuthFailure::Forbidden => admin_error_response(StatusCode::FORBIDDEN, "forbidden"),
    }
}

fn require_admin_request(
    headers: &HeaderMap,
    admin_ids: &[i64],
    secret: &str,
) -> Result<(), AdminAuthFailure> {
    let user_id = headers
        .get("X-Telegram-User-ID")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .or_else(|| admin_session_user_id(headers, secret).map(|value| value.to_string()));
    let Some(user_id) = user_id else {
        return Err(AdminAuthFailure::Unauthorized);
    };
    let Ok(user_id) = user_id.trim().parse::<i64>() else {
        return Err(AdminAuthFailure::Unauthorized);
    };
    if !admin_ids.contains(&user_id) {
        return Err(AdminAuthFailure::Forbidden);
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct AdminLogLevelRequest {
    level: String,
}

async fn admin_state_response(routes: &StaticWebRoutes) -> Response {
    let level = admin_app_setting(routes, "log_level")
        .await
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| routes.default_log_level.to_string());
    let dispatcher_stats =
        openplotva_server::RuntimeDispatcherInspector::stats(&routes.dispatcher_inspector);
    let cache_stats = openplotva_server::RuntimeCacheInspector::stats(&routes.cache_inspector);
    admin_json_response(
        StatusCode::OK,
        serde_json::json!({
            "log_level": level,
            "queue": {
                "regularQueueSize": dispatcher_stats.regular_queue_size,
                "immediateQueueSize": dispatcher_stats.immediate_queue_size,
                "processedTotal": dispatcher_stats.processed_total,
                "dedupedTotal": dispatcher_stats.deduped_total,
            },
            "cache": admin_cache_stats_json(cache_stats.cache),
            "planner": admin_cache_stats_json(cache_stats.planner_cache),
        }),
    )
}

fn admin_cache_stats_json(stats: openplotva_server::RuntimeCacheStatsData) -> serde_json::Value {
    serde_json::json!({
        "Size": stats.size,
        "Capacity": stats.capacity,
        "Hits": stats.hits,
        "Misses": stats.misses,
        "MemSize": stats.mem_size,
    })
}

async fn admin_loglevel_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    body: &[u8],
) -> Response {
    if method != Method::POST && method != Method::PUT {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Ok(req) = serde_json::from_slice::<AdminLogLevelRequest>(body) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "bad request");
    };
    let level = req.level.trim().to_lowercase();
    match level.as_str() {
        "debug" | "info" | "warn" | "warning" | "error" => {}
        _ => return admin_error_response(StatusCode::BAD_REQUEST, "invalid level"),
    }
    if let Err(error) = openplotva_observability::set_log_level(&level) {
        tracing::warn!(%error, level, "failed to update runtime log level");
    }
    if let Err(error) = admin_upsert_app_setting(routes, "log_level", &level).await {
        tracing::warn!(%error, level, "failed to persist admin log level");
    }
    admin_json_response(
        StatusCode::OK,
        serde_json::json!({ "ok": true, "level": level }),
    )
}

fn admin_llm_requests_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let requests = match routes.llm_trace_buffer.as_ref() {
        Some(buffer) => openplotva_server::RuntimeLlmTraceInspector::llm_requests(
            buffer,
            admin_llm_requests_filter(raw_query),
        ),
        None => Ok(Vec::new()),
    };
    match requests {
        Ok(requests) => {
            let requests = requests
                .iter()
                .map(admin_llm_request_json)
                .collect::<Vec<_>>();
            admin_json_no_cache_response(
                StatusCode::OK,
                serde_json::json!({
                    "count": requests.len(),
                    "requests": requests,
                }),
            )
        }
        Err(error) => {
            tracing::warn!(%error, "failed to list admin llm requests");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

fn admin_llm_requests_clear_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
) -> Response {
    if method != Method::POST {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    if let Some(buffer) = &routes.llm_trace_buffer {
        buffer.clear();
    }
    admin_json_response(StatusCode::OK, serde_json::json!({ "ok": true }))
}

async fn admin_safety_checks_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(pool) = routes.postgres.clone() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    let reader = runtime_safety::PostgresRuntimeSafetyCheckReader::new(pool);
    match openplotva_server::RuntimeSafetyCheckReader::safety_checks(
        &reader,
        admin_safety_checks_filter(raw_query),
    )
    .await
    {
        Ok(connection) => {
            let checks = connection
                .items
                .iter()
                .map(admin_safety_check_json)
                .collect::<Vec<_>>();
            admin_json_no_cache_response(
                StatusCode::OK,
                serde_json::json!({
                    "count": checks.len(),
                    "checks": checks,
                }),
            )
        }
        Err(error) => {
            tracing::warn!(%error, "failed to list admin safety checks");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

async fn admin_llm_analytics_summary_response(
    routes: &StaticWebRoutes,
    method: Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(pool) = routes.postgres.clone() else {
        return admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed");
    };
    let taskman: Arc<dyn openplotva_server::RuntimeTaskmanInspector> =
        Arc::new(routes.taskman_inspector.clone());
    let reader = runtime_llm_analytics::PostgresRuntimeLlmAnalyticsReader::new(pool)
        .with_discovery_capacity(
            routes.llm_discovery_base_url.to_string(),
            routes.llm_discovery_service_name.to_string(),
        )
        .with_sql_timeout_ms(routes.runtime_sql_timeout_ms)
        .with_taskman(taskman);
    let range = admin_auth_query_values(raw_query)
        .remove("range")
        .map(|value| value.trim().to_owned())
        .unwrap_or_default();
    match openplotva_server::RuntimeLlmAnalyticsReader::llm_analytics(&reader, &range).await {
        Ok(summary) => {
            let since = admin_llm_analytics_since_time(&summary);
            let tool_calls = openplotva_dialog::tool_telemetry::snapshot_since(since, 50);
            admin_json_no_cache_response(
                StatusCode::OK,
                admin_llm_analytics_summary_json(&summary, &tool_calls),
            )
        }
        Err(error) => {
            tracing::warn!(%error, "failed to build admin llm analytics summary");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

fn admin_llm_analytics_since_time(
    summary: &openplotva_server::RuntimeLlmAnalyticsData,
) -> OffsetDateTime {
    OffsetDateTime::parse(&summary.since, &Rfc3339).unwrap_or_else(|_| OffsetDateTime::now_utc())
}

fn admin_llm_requests_filter(
    raw_query: Option<&str>,
) -> openplotva_server::RuntimeLlmRequestsFilter {
    let values = admin_auth_query_values(raw_query);
    openplotva_server::RuntimeLlmRequestsFilter {
        q: values
            .get("q")
            .map(|value| value.trim())
            .unwrap_or("")
            .to_owned(),
        source: values
            .get("source")
            .map(|value| value.trim())
            .unwrap_or("")
            .to_owned(),
        model: values
            .get("model")
            .map(|value| value.trim())
            .unwrap_or("")
            .to_owned(),
        chat_id: values
            .get("chat_id")
            .and_then(|value| value.trim().parse::<i64>().ok()),
        user_id: values
            .get("user_id")
            .and_then(|value| value.trim().parse::<i64>().ok()),
        message_id: values
            .get("message_id")
            .and_then(|value| value.trim().parse::<i32>().ok()),
        error_only: values
            .get("error_only")
            .is_some_and(|value| value.trim() == "1" || value.trim() == "true"),
        empty_only: values
            .get("empty_only")
            .is_some_and(|value| value.trim() == "1" || value.trim() == "true"),
        limit: parse_admin_positive_i32(values.get("limit").map(String::as_str), 1000, 1000),
    }
}

fn admin_safety_checks_filter(
    raw_query: Option<&str>,
) -> openplotva_server::RuntimeSafetyChecksFilter {
    let values = admin_auth_query_values(raw_query);
    openplotva_server::RuntimeSafetyChecksFilter {
        q: values
            .get("q")
            .map(|value| value.trim())
            .unwrap_or("")
            .to_owned(),
        flagged: parse_admin_optional_bool(values.get("flagged").map(String::as_str)),
        offset: parse_admin_non_negative_i32(values.get("offset").map(String::as_str), 0),
        limit: parse_admin_positive_i32(values.get("limit").map(String::as_str), 200, 1000),
    }
}

fn parse_admin_positive_i32(value: Option<&str>, default_value: i32, max_value: i32) -> i32 {
    match value
        .map(str::trim)
        .and_then(|value| value.parse::<i32>().ok())
    {
        Some(parsed) if parsed > 0 => parsed.min(max_value),
        _ => default_value,
    }
}

fn parse_admin_non_negative_i32(value: Option<&str>, default_value: i32) -> i32 {
    match value
        .map(str::trim)
        .and_then(|value| value.parse::<i32>().ok())
    {
        Some(parsed) if parsed >= 0 => parsed,
        _ => default_value,
    }
}

fn parse_admin_optional_bool(value: Option<&str>) -> Option<bool> {
    match value.map(str::trim).map(str::to_lowercase).as_deref() {
        Some("true" | "1" | "yes") => Some(true),
        Some("false" | "0" | "no") => Some(false),
        _ => None,
    }
}

fn admin_llm_request_json(request: &openplotva_server::RuntimeLlmRequestData) -> serde_json::Value {
    let mut root = serde_json::Map::new();
    admin_insert(&mut root, "id", request.id);
    admin_insert(&mut root, "at", &request.at);
    admin_insert_opt(&mut root, "provider", request.provider.as_deref());
    admin_insert_opt(&mut root, "request_kind", request.request_kind.as_deref());
    admin_insert(&mut root, "source", &request.source);
    admin_insert_opt(&mut root, "mode", request.mode.as_deref());
    admin_insert_opt(&mut root, "flow", request.flow.as_deref());
    admin_insert(&mut root, "iteration", request.iteration);
    admin_insert_opt(&mut root, "model", request.model.as_deref());
    admin_insert(&mut root, "chat", admin_llm_chat_json(&request.chat));
    admin_insert(&mut root, "user", admin_llm_user_json(&request.user));
    admin_insert(
        &mut root,
        "message",
        admin_llm_message_json(&request.message),
    );
    admin_insert(
        &mut root,
        "gen_config",
        admin_llm_gen_config_json(&request.gen_config),
    );
    admin_insert_opt_value(&mut root, "docs", request.docs.as_ref());
    admin_insert_opt_value(&mut root, "messages", request.messages.as_ref());
    admin_insert_opt_value(&mut root, "raw_request", request.raw_request.as_ref());
    admin_insert_opt_value(
        &mut root,
        "resolved_cache_content",
        request.resolved_cache_content.as_ref(),
    );
    admin_insert_opt_value(&mut root, "raw_response", request.raw_response.as_ref());
    admin_insert_opt_value(&mut root, "transport", request.transport.as_ref());
    admin_insert_opt_value(
        &mut root,
        "inference_params",
        request.inference_params.as_ref(),
    );
    admin_insert_opt_value(&mut root, "usage", request.usage.as_ref());
    admin_insert_opt_value(&mut root, "timings", request.timings.as_ref());
    admin_insert(&mut root, "prompt_chars", request.prompt_chars);
    if request.prompt_messages != 0 {
        admin_insert(&mut root, "prompt_messages", request.prompt_messages);
    }
    admin_insert(&mut root, "docs_chars", request.docs_chars);
    if request.duration_ms != 0 {
        admin_insert(&mut root, "duration_ms", request.duration_ms);
    }
    admin_insert(&mut root, "result", admin_llm_result_json(&request.result));
    serde_json::Value::Object(root)
}

fn admin_llm_chat_json(chat: &openplotva_server::RuntimeLlmRequestChatData) -> serde_json::Value {
    let mut root = serde_json::Map::new();
    admin_insert(&mut root, "chat_id", chat.chat_id);
    admin_insert_opt(&mut root, "thread_id", chat.thread_id);
    admin_insert_opt(&mut root, "chat_title", chat.chat_title.as_deref());
    serde_json::Value::Object(root)
}

fn admin_llm_user_json(user: &openplotva_server::RuntimeLlmRequestUserData) -> serde_json::Value {
    let mut root = serde_json::Map::new();
    admin_insert(&mut root, "user_id", user.user_id);
    admin_insert_opt(&mut root, "full_name", user.full_name.as_deref());
    serde_json::Value::Object(root)
}

fn admin_llm_message_json(
    message: &openplotva_server::RuntimeLlmRequestMessageData,
) -> serde_json::Value {
    serde_json::json!({ "message_id": message.message_id })
}

fn admin_llm_gen_config_json(
    gen_config: &openplotva_server::RuntimeLlmGenConfigData,
) -> serde_json::Value {
    let mut root = serde_json::Map::new();
    if gen_config.max_output_tokens != 0 {
        admin_insert(&mut root, "max_output_tokens", gen_config.max_output_tokens);
    }
    if gen_config.temperature != 0.0 {
        admin_insert(&mut root, "temperature", gen_config.temperature);
    }
    if gen_config.top_p != 0.0 {
        admin_insert(&mut root, "top_p", gen_config.top_p);
    }
    if gen_config.top_k != 0 {
        admin_insert(&mut root, "top_k", gen_config.top_k);
    }
    admin_insert_opt_value(
        &mut root,
        "safety_settings",
        gen_config.safety_settings.as_ref(),
    );
    serde_json::Value::Object(root)
}

fn admin_llm_result_json(
    result: &openplotva_server::RuntimeLlmRequestResultData,
) -> serde_json::Value {
    let mut root = serde_json::Map::new();
    admin_insert(&mut root, "duration_ms", result.duration_ms);
    admin_insert_opt(&mut root, "error", result.error.as_deref());
    admin_insert_opt(
        &mut root,
        "response_text_preview",
        result.response_text_preview.as_deref(),
    );
    serde_json::Value::Object(root)
}

fn admin_safety_check_json(check: &openplotva_server::RuntimeSafetyCheckData) -> serde_json::Value {
    let mut root = serde_json::Map::new();
    admin_insert(&mut root, "id", check.id);
    admin_insert(&mut root, "created_at", &check.created_at);
    admin_insert(&mut root, "source", &check.source);
    admin_insert_opt(&mut root, "flow", check.flow.as_deref());
    admin_insert_opt(&mut root, "mode", check.mode.as_deref());
    admin_insert_opt(&mut root, "chat_id", check.chat_id);
    admin_insert_opt(&mut root, "thread_id", check.thread_id);
    admin_insert_opt(&mut root, "message_id", check.message_id);
    admin_insert_opt(&mut root, "user_id", check.user_id);
    admin_insert(&mut root, "deployment_id", &check.deployment_id);
    admin_insert_opt(
        &mut root,
        "external_session_id",
        check.external_session_id.as_deref(),
    );
    admin_insert(
        &mut root,
        "request_messages",
        check
            .request_messages
            .clone()
            .unwrap_or(serde_json::Value::Null),
    );
    admin_insert_opt(&mut root, "flagged", check.flagged);
    admin_insert_opt(
        &mut root,
        "internal_session_id",
        check.internal_session_id.as_deref(),
    );
    admin_insert_opt_value(&mut root, "policies", check.policies.as_ref());
    admin_insert_opt_value(&mut root, "response_json", check.response_json.as_ref());
    admin_insert(&mut root, "duration_ms", check.duration_ms);
    admin_insert_opt(&mut root, "error", check.error.as_deref());
    serde_json::Value::Object(root)
}

fn admin_llm_analytics_summary_json(
    summary: &openplotva_server::RuntimeLlmAnalyticsData,
    tool_calls: &openplotva_dialog::tool_telemetry::ToolTelemetrySnapshot,
) -> serde_json::Value {
    let mut root = serde_json::Map::new();
    admin_insert(&mut root, "range", &summary.range);
    admin_insert(&mut root, "bucket", &summary.bucket);
    admin_insert(&mut root, "since", &summary.since);
    admin_insert(
        &mut root,
        "totals",
        serde_json::json!({
            "total_count": summary.totals.total_count,
            "error_count": summary.totals.error_count,
            "avg_duration_ms": summary.totals.avg_duration_ms,
        }),
    );
    admin_insert(&mut root, "series", admin_llm_series_json(&summary.series));
    admin_insert(
        &mut root,
        "model_series",
        admin_llm_model_series_json(&summary.model_series),
    );
    admin_insert(
        &mut root,
        "top_chats",
        admin_llm_top_chats_json(&summary.top_chats),
    );
    admin_insert(&mut root, "models", admin_llm_models_json(&summary.models));
    admin_insert(
        &mut root,
        "providers",
        admin_llm_providers_json(&summary.providers),
    );
    admin_insert(
        &mut root,
        "inference_params",
        admin_llm_inference_params_json(&summary.inference_params),
    );
    admin_insert(
        &mut root,
        "stage_metrics",
        admin_llm_stage_metrics_json(&summary.stage_metrics),
    );
    admin_insert(
        &mut root,
        "runtime_jobs",
        admin_runtime_jobs_json(&summary.runtime_jobs),
    );
    admin_insert_opt(
        &mut root,
        "runtime_jobs_error",
        summary.runtime_jobs_error.as_deref(),
    );
    admin_insert(&mut root, "tool_calls", tool_calls.to_json());
    let ai_farm_capacity = summary
        .ai_farm_capacity
        .as_ref()
        .map(admin_aifarm_capacity_json);
    admin_insert_opt_value(&mut root, "aifarm_capacity", ai_farm_capacity.as_ref());
    serde_json::Value::Object(root)
}

fn admin_llm_series_json(
    series: &[openplotva_server::RuntimeLlmAnalyticsSeriesPointData],
) -> Vec<serde_json::Value> {
    series
        .iter()
        .map(|point| {
            serde_json::json!({
                "ts": point.ts,
                "total_count": point.total_count,
                "error_count": point.error_count,
                "avg_duration_ms": point.avg_duration_ms,
            })
        })
        .collect()
}

fn admin_llm_model_series_json(
    series: &[openplotva_server::RuntimeLlmAnalyticsModelSeriesPointData],
) -> Vec<serde_json::Value> {
    series
        .iter()
        .map(|point| {
            serde_json::json!({
                "ts": point.ts,
                "model": point.model,
                "request_count": point.request_count,
                "error_count": point.error_count,
                "avg_duration_ms": point.avg_duration_ms,
                "avg_generation_tps": point.avg_generation_tps,
                "avg_effective_output_tps": point.avg_effective_output_tps,
                "output_tokens": point.output_tokens,
            })
        })
        .collect()
}

fn admin_llm_top_chats_json(
    chats: &[openplotva_server::RuntimeLlmAnalyticsTopChatData],
) -> Vec<serde_json::Value> {
    chats
        .iter()
        .map(|chat| {
            let mut root = serde_json::Map::new();
            admin_insert(&mut root, "chat_id", chat.chat_id);
            admin_insert_opt(&mut root, "title", chat.title.as_deref());
            admin_insert_opt(&mut root, "username", chat.username.as_deref());
            admin_insert(&mut root, "request_count", chat.request_count);
            serde_json::Value::Object(root)
        })
        .collect()
}

fn admin_llm_models_json(
    models: &[openplotva_server::RuntimeLlmAnalyticsModelStatData],
) -> Vec<serde_json::Value> {
    models
        .iter()
        .map(|model| {
            serde_json::json!({
                "model": model.model,
                "request_count": model.request_count,
                "error_count": model.error_count,
                "avg_duration_ms": model.avg_duration_ms,
                "p50_duration_ms": model.p50_duration_ms,
                "p95_duration_ms": model.p95_duration_ms,
                "input_tokens": model.input_tokens,
                "output_tokens": model.output_tokens,
                "total_tokens": model.total_tokens,
                "avg_generation_tps": model.avg_generation_tps,
                "avg_effective_output_tps": model.avg_effective_output_tps,
                "p50_effective_output_tps": model.p50_effective_output_tps,
                "p95_effective_output_tps": model.p95_effective_output_tps,
            })
        })
        .collect()
}

fn admin_llm_providers_json(
    providers: &[openplotva_server::RuntimeLlmAnalyticsProviderStatData],
) -> Vec<serde_json::Value> {
    providers
        .iter()
        .map(|provider| {
            serde_json::json!({
                "provider": provider.provider,
                "source": provider.source,
                "request_count": provider.request_count,
                "error_count": provider.error_count,
                "avg_duration_ms": provider.avg_duration_ms,
                "p50_duration_ms": provider.p50_duration_ms,
                "p95_duration_ms": provider.p95_duration_ms,
                "input_tokens": provider.input_tokens,
                "output_tokens": provider.output_tokens,
                "total_tokens": provider.total_tokens,
                "avg_generation_tps": provider.avg_generation_tps,
                "avg_effective_output_tps": provider.avg_effective_output_tps,
            })
        })
        .collect()
}

fn admin_llm_inference_params_json(
    params: &[openplotva_server::RuntimeLlmAnalyticsInferenceParamStatData],
) -> Vec<serde_json::Value> {
    params
        .iter()
        .map(|param| {
            let mut root = serde_json::Map::new();
            admin_insert(&mut root, "provider", &param.provider);
            admin_insert(&mut root, "source", &param.source);
            admin_insert(&mut root, "model", &param.model);
            admin_insert_opt(&mut root, "max_tokens", param.max_tokens);
            admin_insert_opt(&mut root, "temperature", param.temperature);
            admin_insert_opt(&mut root, "top_p", param.top_p);
            admin_insert_opt(&mut root, "top_k", param.top_k);
            admin_insert_opt(&mut root, "candidate_count", param.candidate_count);
            admin_insert(&mut root, "tool_mode", &param.tool_mode);
            admin_insert(&mut root, "response_format", &param.response_format);
            admin_insert(&mut root, "request_count", param.request_count);
            admin_insert(&mut root, "error_count", param.error_count);
            admin_insert(&mut root, "avg_duration_ms", param.avg_duration_ms);
            admin_insert(
                &mut root,
                "avg_effective_output_tps",
                param.avg_effective_output_tps,
            );
            serde_json::Value::Object(root)
        })
        .collect()
}

fn admin_llm_stage_metrics_json(
    metrics: &[openplotva_server::RuntimeLlmAnalyticsStageMetricData],
) -> Vec<serde_json::Value> {
    metrics
        .iter()
        .map(|metric| {
            serde_json::json!({
                "stage": metric.stage,
                "source": metric.source,
                "request_count": metric.request_count,
                "error_count": metric.error_count,
                "avg_duration_ms": metric.avg_duration_ms,
                "p50_duration_ms": metric.p50_duration_ms,
                "p95_duration_ms": metric.p95_duration_ms,
                "avg_iteration": metric.avg_iteration,
                "max_iteration": metric.max_iteration,
            })
        })
        .collect()
}

fn admin_runtime_jobs_json(
    jobs: &[openplotva_server::RuntimeJobAnalyticsStatData],
) -> Vec<serde_json::Value> {
    jobs.iter()
        .map(|job| {
            serde_json::json!({
                "job_type": job.job_type,
                "queue_name": job.queue_name,
                "provider": job.provider,
                "created_count": job.created_count,
                "completed_count": job.completed_count,
                "failed_count": job.failed_count,
                "avg_wait_ms": job.avg_wait_ms,
                "p95_wait_ms": job.p95_wait_ms,
                "avg_processing_ms": job.avg_processing_ms,
                "p95_processing_ms": job.p95_processing_ms,
            })
        })
        .collect()
}

fn admin_aifarm_capacity_json(
    capacity: &openplotva_server::RuntimeAifarmCapacitySnapshotData,
) -> serde_json::Value {
    let mut root = serde_json::Map::new();
    admin_insert(&mut root, "service", &capacity.service);
    admin_insert(
        &mut root,
        "max_concurrent_jobs",
        capacity.max_concurrent_jobs,
    );
    admin_insert(&mut root, "running", capacity.running);
    admin_insert(&mut root, "queued", capacity.queued);
    admin_insert(&mut root, "available", capacity.available);
    admin_insert(&mut root, "locked", capacity.locked);
    admin_insert(&mut root, "ready", capacity.ready);
    admin_insert(&mut root, "observed_at", &capacity.observed_at);
    admin_insert_opt(&mut root, "error", capacity.error.as_deref());
    serde_json::Value::Object(root)
}

fn admin_insert<T: Serialize>(
    map: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: T,
) {
    map.insert(key.to_owned(), serde_json::json!(value));
}

fn admin_insert_opt<T: Serialize>(
    map: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: Option<T>,
) {
    if let Some(value) = value {
        admin_insert(map, key, value);
    }
}

fn admin_insert_opt_value(
    map: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: Option<&serde_json::Value>,
) {
    if let Some(value) = value {
        map.insert(key.to_owned(), value.clone());
    }
}

async fn admin_app_setting(routes: &StaticWebRoutes, key: &str) -> Option<String> {
    let pool = routes.postgres.as_ref()?;
    sqlx::query_scalar::<_, String>("SELECT value FROM app_settings WHERE key = $1")
        .bind(key)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
}

async fn admin_upsert_app_setting(
    routes: &StaticWebRoutes,
    key: &str,
    value: &str,
) -> Result<(), sqlx::Error> {
    let Some(pool) = routes.postgres.as_ref() else {
        return Ok(());
    };
    sqlx::query(
        "INSERT INTO app_settings (key, value, updated_at) \
         VALUES ($1, $2, NOW()) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
    )
    .bind(key)
    .bind(value)
    .execute(pool)
    .await?;
    Ok(())
}

fn routing_master_secret() -> String {
    std::env::var("MASTER_KEY").unwrap_or_default()
}

fn routing_json_str(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

fn routing_json_i32(value: &serde_json::Value, key: &str) -> Option<i32> {
    value
        .get(key)
        .and_then(serde_json::Value::as_i64)
        .map(|n| n as i32)
}

fn routing_json_bool(value: &serde_json::Value, key: &str, default: bool) -> bool {
    value
        .get(key)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(default)
}

fn routing_json_required_bool(value: &serde_json::Value, key: &str) -> Result<bool, String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| format!("{key} is required"))
}

fn routing_json_config_patch(value: &serde_json::Value) -> Result<serde_json::Value, String> {
    let patch = value
        .get("config")
        .or_else(|| value.get("patch"))
        .cloned()
        .ok_or_else(|| "config patch is required".to_owned())?;
    if !patch.is_object() {
        return Err("config patch must be a JSON object".to_owned());
    }
    Ok(patch)
}

fn routing_capabilities(value: &serde_json::Value) -> Vec<String> {
    value
        .get("capabilities")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn provider_key_state(provider: &openplotva_storage::llm_routing::ProviderRecord) -> &'static str {
    if provider.api_key_encrypted.is_some() {
        "encrypted"
    } else if provider.api_key_ref.is_some() {
        "ref"
    } else {
        "unset"
    }
}

/// Build a provider input from admin JSON, sealing a plaintext `api_key` under the
/// master key. The plaintext is never persisted or echoed; only the ciphertext is
/// stored, and `api_key_ref`/`api_key_encrypted` are mutually exclusive.
fn provider_input_from_json(
    value: &serde_json::Value,
) -> Result<openplotva_storage::llm_routing::ProviderInput, String> {
    let protocol = routing_json_str(value, "protocol");
    if let Some(protocol) = protocol.as_deref() {
        let Some(parsed) = openplotva_llm::provider_schema::Protocol::from_db(protocol) else {
            return Err(format!("unknown protocol {protocol}"));
        };
        let config = value.get("config").cloned().unwrap_or_else(|| json!({}));
        openplotva_llm::provider_schema::validate_provider_config(parsed, &config)
            .map_err(|error| error.to_string())?;
    }
    let runtime_hint = routing_json_str(value, "runtime_hint");
    if let Some(hint) = runtime_hint.as_deref()
        && openplotva_llm::provider_schema::RuntimeHint::from_db(hint).is_none()
    {
        return Err(format!("unknown runtime_hint {hint}"));
    }
    let mut input = openplotva_storage::llm_routing::ProviderInput {
        name: routing_json_str(value, "name").ok_or("name is required")?,
        kind: routing_json_str(value, "kind").unwrap_or_else(|| "chat".to_owned()),
        protocol,
        runtime_hint,
        endpoint: routing_json_str(value, "endpoint"),
        discovery_service_name: routing_json_str(value, "discovery_service_name"),
        discovery_endpoint_name: routing_json_str(value, "discovery_endpoint_name"),
        api_key_ref: routing_json_str(value, "api_key_ref"),
        api_key_encrypted: None,
        enabled: routing_json_bool(value, "enabled", true),
        config: value.get("config").cloned().unwrap_or_else(|| json!({})),
    };
    if let Some(plaintext) = routing_json_str(value, "api_key")
        && !plaintext.trim().is_empty()
    {
        let sealed =
            openplotva_storage::llm_routing::seal_key(&routing_master_secret(), &plaintext)
                .map_err(|error| error.to_string())?;
        input.api_key_encrypted = Some(sealed);
        input.api_key_ref = None;
    }
    Ok(input)
}

fn model_input_from_json(
    value: &serde_json::Value,
) -> Result<openplotva_storage::llm_routing::ModelInput, String> {
    if let Some(config) = value.get("config") {
        openplotva_llm::provider_schema::validate_model_config(config)
            .map_err(|error| error.to_string())?;
    }
    Ok(openplotva_storage::llm_routing::ModelInput {
        provider_id: value
            .get("provider_id")
            .and_then(serde_json::Value::as_i64)
            .ok_or("provider_id is required")?,
        model_name: routing_json_str(value, "model_name").ok_or("model_name is required")?,
        display_name: routing_json_str(value, "display_name"),
        base_url: routing_json_str(value, "base_url"),
        capabilities: routing_capabilities(value),
        embedding_dim: routing_json_i32(value, "embedding_dim"),
        pool_id: value.get("pool_id").and_then(serde_json::Value::as_i64),
        enabled: routing_json_bool(value, "enabled", true),
        config: value.get("config").cloned().unwrap_or_else(|| json!({})),
    })
}

fn assignment_input_from_json(
    value: &serde_json::Value,
) -> Result<openplotva_storage::llm_routing::AssignmentInput, String> {
    Ok(openplotva_storage::llm_routing::AssignmentInput {
        workflow_key: routing_json_str(value, "workflow_key").ok_or("workflow_key is required")?,
        scope: routing_json_str(value, "scope").unwrap_or_else(|| "global".to_owned()),
        role: routing_json_str(value, "role").unwrap_or_else(|| "primary".to_owned()),
        provider_model_id: value
            .get("provider_model_id")
            .and_then(serde_json::Value::as_i64)
            .ok_or("provider_model_id is required")?,
        weight: routing_json_i32(value, "weight"),
        fallback_order: routing_json_i32(value, "fallback_order"),
        canary_percent: routing_json_i32(value, "canary_percent"),
        enabled: routing_json_bool(value, "enabled", true),
        inference_overrides: value
            .get("inference_overrides")
            .cloned()
            .unwrap_or_else(|| json!({})),
        cb_failure_threshold: routing_json_i32(value, "cb_failure_threshold").unwrap_or(5),
        cb_cooldown_ms: routing_json_i32(value, "cb_cooldown_ms").unwrap_or(30_000),
    })
}

fn trigger_input_from_json(
    value: &serde_json::Value,
) -> Result<openplotva_storage::llm_routing::TriggerInput, String> {
    let mut input = trigger_input_from_json_without_engage(value)?;
    input.engage_assignment_id = value
        .get("engage_assignment_id")
        .and_then(serde_json::Value::as_i64)
        .ok_or("engage_assignment_id is required")?;
    Ok(input)
}

/// Trigger fields without the engage target, for the atomic
/// assignment-plus-trigger create where the target id does not exist yet.
fn trigger_input_from_json_without_engage(
    value: &serde_json::Value,
) -> Result<openplotva_storage::llm_routing::TriggerInput, String> {
    Ok(openplotva_storage::llm_routing::TriggerInput {
        workflow_key: routing_json_str(value, "workflow_key").ok_or("workflow_key is required")?,
        trigger_type: routing_json_str(value, "trigger_type").ok_or("trigger_type is required")?,
        engage_assignment_id: 0,
        enabled: routing_json_bool(value, "enabled", true),
        queue_name: routing_json_str(value, "queue_name"),
        high_watermark: routing_json_i32(value, "high_watermark"),
        low_watermark: routing_json_i32(value, "low_watermark"),
        params: value.get("params").cloned().unwrap_or_else(|| json!({})),
    })
}

/// Server-side model-list fetch for the admin "Fetch models" flow: calls the
/// provider's listing endpoint and returns a diff against its imported models
/// (`new` / `existing` / `gone`). Read-only: import is a separate action.
async fn admin_fetch_provider_models(
    pool: &PgPool,
    provider_id: i64,
    body: &serde_json::Value,
) -> Result<serde_json::Value, (StatusCode, String)> {
    let bad = |error: String| (StatusCode::BAD_REQUEST, error);
    let provider = openplotva_storage::llm_routing::get_provider(pool, provider_id)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| bad(format!("unknown provider {provider_id}")))?;
    let protocol = provider
        .protocol
        .as_deref()
        .and_then(openplotva_llm::provider_schema::Protocol::from_db)
        .ok_or_else(|| bad("provider has no protocol; set one first".to_owned()))?;
    if !protocol.supports_model_listing() {
        return Err(bad(format!(
            "protocol {} has no model listing endpoint; add models manually",
            protocol.as_str()
        )));
    }
    let base_url = routing_json_str(body, "base_url")
        .or_else(|| provider.endpoint.clone())
        .filter(|url| !url.trim().is_empty())
        .ok_or_else(|| bad("provider has no endpoint to list models from".to_owned()))?;
    let api_key = provider
        .api_key_ref
        .as_deref()
        .filter(|reference| !reference.trim().is_empty())
        .and_then(|reference| std::env::var(reference.trim()).ok())
        .or_else(|| {
            provider.api_key_encrypted.as_deref().and_then(|sealed| {
                openplotva_storage::llm_routing::open_key(&routing_master_secret(), sealed).ok()
            })
        });
    let remote = openplotva_llm::model_listing::list_openai_compat_models(
        &base_url,
        api_key.as_deref(),
        Duration::from_secs(10),
    )
    .await
    .map_err(|error| (StatusCode::BAD_GATEWAY, error.to_string()))?;

    let imported: Vec<(i64, String)> = openplotva_storage::llm_routing::list_models(pool)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .filter(|model| model.provider_id == provider_id)
        .map(|model| (model.id, model.model_name))
        .collect();
    let imported_names: std::collections::HashSet<&str> =
        imported.iter().map(|(_, name)| name.as_str()).collect();
    let remote_names: std::collections::HashSet<&str> =
        remote.iter().map(|model| model.name.as_str()).collect();

    let new: Vec<&str> = remote
        .iter()
        .map(|model| model.name.as_str())
        .filter(|name| !imported_names.contains(name))
        .collect();
    let existing: Vec<serde_json::Value> = imported
        .iter()
        .filter(|(_, name)| remote_names.contains(name.as_str()))
        .map(|(id, name)| json!({ "id": id, "model_name": name }))
        .collect();
    let gone: Vec<serde_json::Value> = imported
        .iter()
        .filter(|(_, name)| !remote_names.contains(name.as_str()))
        .map(|(id, name)| json!({ "id": id, "model_name": name }))
        .collect();
    Ok(json!({
        "ok": true,
        "provider_id": provider_id,
        "base_url": base_url,
        "models": remote
            .iter()
            .map(|model| json!({ "model_name": model.name, "meta": model.raw }))
            .collect::<Vec<_>>(),
        "diff": { "new": new, "existing": existing, "gone": gone },
    }))
}

async fn admin_routing_snapshot_json(pool: &PgPool) -> Result<serde_json::Value, String> {
    let snapshot = openplotva_storage::llm_routing::load_snapshot(pool)
        .await
        .map_err(|error| error.to_string())?;
    let providers: Vec<serde_json::Value> = snapshot
        .providers
        .iter()
        .map(|provider| {
            json!({
                "id": provider.id,
                "name": provider.name,
                "kind": provider.kind,
                "protocol": provider.protocol,
                "runtime_hint": provider.runtime_hint,
                "endpoint": provider.endpoint,
                "discovery_service_name": provider.discovery_service_name,
                "discovery_endpoint_name": provider.discovery_endpoint_name,
                "key_state": provider_key_state(provider),
                "api_key_ref": provider.api_key_ref,
                "enabled": provider.enabled,
                "config": provider.config,
            })
        })
        .collect();
    let models: Vec<serde_json::Value> = snapshot
        .models
        .iter()
        .map(|model| {
            json!({
                "id": model.id,
                "provider_id": model.provider_id,
                "model_name": model.model_name,
                "display_name": model.display_name,
                "base_url": model.base_url,
                "capabilities": model.capabilities,
                "embedding_dim": model.embedding_dim,
                "pool_id": model.pool_id,
                "enabled": model.enabled,
                "config": model.config,
            })
        })
        .collect();
    let workflows: Vec<serde_json::Value> = snapshot
        .workflows
        .iter()
        .map(|workflow| {
            json!({
                "key": workflow.key,
                "kind": workflow.kind,
                "full_routing": workflow.full_routing,
                "retry_max_hops": workflow.retry_max_hops,
                "retry_wall_ms": workflow.retry_wall_ms,
                "enabled": workflow.enabled,
            })
        })
        .collect();
    let assignments: Vec<serde_json::Value> = snapshot
        .assignments
        .iter()
        .map(|assignment| {
            json!({
                "id": assignment.id,
                "workflow_key": assignment.workflow_key,
                "scope": assignment.scope,
                "role": assignment.role,
                "provider_model_id": assignment.provider_model_id,
                "weight": assignment.weight,
                "fallback_order": assignment.fallback_order,
                "canary_percent": assignment.canary_percent,
                "enabled": assignment.enabled,
                "inference_overrides": assignment.inference_overrides,
                "cb_failure_threshold": assignment.cb_failure_threshold,
                "cb_cooldown_ms": assignment.cb_cooldown_ms,
            })
        })
        .collect();
    let triggers: Vec<serde_json::Value> = snapshot
        .triggers
        .iter()
        .map(|trigger| {
            json!({
                "id": trigger.id,
                "workflow_key": trigger.workflow_key,
                "trigger_type": trigger.trigger_type,
                "engage_assignment_id": trigger.engage_assignment_id,
                "enabled": trigger.enabled,
                "queue_name": trigger.queue_name,
                "high_watermark": trigger.high_watermark,
                "low_watermark": trigger.low_watermark,
                "params": trigger.params,
            })
        })
        .collect();
    let pools: Vec<serde_json::Value> = snapshot
        .pools
        .iter()
        .map(|pool| {
            json!({
                "id": pool.id,
                "name": pool.name,
                "max_concurrency": pool.max_concurrency,
                "description": pool.description,
            })
        })
        .collect();
    Ok(json!({
        "providers": providers,
        "models": models,
        "workflows": workflows,
        "assignments": assignments,
        "triggers": triggers,
        "pools": pools,
        "param_descriptors": routing_param_descriptors(),
    }))
}

/// Static parameter-form descriptors for every (protocol, runtime hint) pair,
/// so the admin UI renders typed provider/model forms without hardcoding them.
fn routing_param_descriptors() -> serde_json::Value {
    use openplotva_llm::provider_schema::{Protocol, RuntimeHint, param_descriptor};
    let mut descriptors = Vec::new();
    for protocol in Protocol::all() {
        descriptors.push(param_descriptor(*protocol, None));
        if *protocol == Protocol::OpenAiCompat {
            for hint in RuntimeHint::all() {
                descriptors.push(param_descriptor(*protocol, Some(*hint)));
            }
        }
    }
    serde_json::Value::Array(descriptors)
}

async fn admin_routing_apply_action(
    pool: &PgPool,
    routes: &StaticWebRoutes,
    body: &serde_json::Value,
) -> Result<serde_json::Value, (StatusCode, String)> {
    let action = body
        .get("action")
        .and_then(serde_json::Value::as_str)
        .ok_or((StatusCode::BAD_REQUEST, "action is required".to_owned()))?;
    let bad = |error: String| (StatusCode::BAD_REQUEST, error);
    let storage_err = |error: openplotva_storage::StorageError| {
        (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    };
    let id_of = |body: &serde_json::Value| body.get("id").and_then(serde_json::Value::as_i64);

    use openplotva_storage::llm_routing as routing;
    let result = match action {
        "create_provider" => {
            let input = provider_input_from_json(body).map_err(bad)?;
            let id = routing::insert_provider(pool, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true, "id": id })
        }
        "update_provider" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let input = provider_input_from_json(body).map_err(bad)?;
            routing::update_provider(pool, id, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "set_provider_enabled" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let enabled = routing_json_required_bool(body, "enabled").map_err(bad)?;
            routing::set_provider_enabled(pool, id, enabled)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "patch_provider_config" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let patch = routing_json_config_patch(body).map_err(bad)?;
            routing::patch_provider_config(pool, id, &patch)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "delete_provider" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            routing::delete_provider(pool, id)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "create_model" => {
            let input = model_input_from_json(body).map_err(bad)?;
            let id = routing::insert_model(pool, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true, "id": id })
        }
        "update_model" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let input = model_input_from_json(body).map_err(bad)?;
            routing::update_model(pool, id, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "set_model_enabled" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let enabled = routing_json_required_bool(body, "enabled").map_err(bad)?;
            routing::set_model_enabled(pool, id, enabled)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "patch_model_config" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let patch = routing_json_config_patch(body).map_err(bad)?;
            routing::patch_model_config(pool, id, &patch)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "delete_model" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            routing::delete_model(pool, id).await.map_err(storage_err)?;
            json!({ "ok": true })
        }
        "create_assignment" => {
            let input = assignment_input_from_json(body).map_err(bad)?;
            let id = routing::insert_assignment(pool, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true, "id": id })
        }
        "update_assignment" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let input = assignment_input_from_json(body).map_err(bad)?;
            routing::update_assignment(pool, id, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "delete_assignment" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            routing::delete_assignment(pool, id)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "delete_assignments_for_scope" => {
            let workflow = routing_json_str(body, "workflow_key")
                .ok_or(bad("workflow_key is required".to_owned()))?;
            let scope = routing_json_str(body, "scope").unwrap_or_else(|| "global".to_owned());
            routing::delete_assignments_for_scope(pool, &workflow, &scope)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "create_trigger" => {
            let input = trigger_input_from_json(body).map_err(bad)?;
            let id = routing::insert_trigger(pool, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true, "id": id })
        }
        "update_trigger" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let input = trigger_input_from_json(body).map_err(bad)?;
            routing::update_trigger(pool, id, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "delete_trigger" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            routing::delete_trigger(pool, id)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "update_workflow" => {
            let key = routing_json_str(body, "key").ok_or(bad("key is required".to_owned()))?;
            routing::update_workflow(
                pool,
                &key,
                routing_json_bool(body, "full_routing", true),
                routing_json_i32(body, "retry_max_hops").unwrap_or(3),
                routing_json_i32(body, "retry_wall_ms").unwrap_or(60_000),
                routing_json_bool(body, "enabled", true),
            )
            .await
            .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "set_workflow_enabled" => {
            let key = routing_json_str(body, "key").ok_or(bad("key is required".to_owned()))?;
            let enabled = routing_json_required_bool(body, "enabled").map_err(bad)?;
            routing::set_workflow_enabled(pool, &key, enabled)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "create_pool" => {
            let name = routing_json_str(body, "name").ok_or(bad("name is required".to_owned()))?;
            let input = openplotva_storage::llm_routing::PoolInput {
                name,
                max_concurrency: routing_json_i32(body, "max_concurrency"),
                description: routing_json_str(body, "description"),
            };
            if input.max_concurrency.is_some_and(|max| max < 1) {
                return Err(bad("max_concurrency must be positive or null".to_owned()));
            }
            let id = routing::insert_pool(pool, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true, "id": id })
        }
        "update_pool" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let name = routing_json_str(body, "name").ok_or(bad("name is required".to_owned()))?;
            let input = openplotva_storage::llm_routing::PoolInput {
                name,
                max_concurrency: routing_json_i32(body, "max_concurrency"),
                description: routing_json_str(body, "description"),
            };
            if input.max_concurrency.is_some_and(|max| max < 1) {
                return Err(bad("max_concurrency must be positive or null".to_owned()));
            }
            routing::update_pool(pool, id, &input)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "delete_pool" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            // FK is ON DELETE SET NULL: attached models degrade to unpooled.
            routing::delete_pool(pool, id).await.map_err(storage_err)?;
            json!({ "ok": true })
        }
        "set_model_pool" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            let pool_id = body.get("pool_id").and_then(serde_json::Value::as_i64);
            routing::set_model_pool(pool, id, pool_id)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true })
        }
        "set_primary_weights" => {
            let weights = body
                .get("weights")
                .and_then(serde_json::Value::as_array)
                .ok_or(bad("weights array is required".to_owned()))?
                .iter()
                .map(|entry| {
                    let id = entry
                        .get("id")
                        .and_then(serde_json::Value::as_i64)
                        .ok_or_else(|| "each weight entry needs an id".to_owned())?;
                    let weight = routing_json_i32(entry, "weight");
                    if weight.is_some_and(|weight| !(0..=100).contains(&weight)) {
                        return Err(format!("weight for assignment {id} must be 0..=100"));
                    }
                    Ok((id, weight))
                })
                .collect::<Result<Vec<_>, String>>()
                .map_err(bad)?;
            routing::set_assignment_weights(pool, &weights)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true, "updated": weights.len() })
        }
        "set_fallback_order" => {
            let ordered_ids = body
                .get("ordered_ids")
                .and_then(serde_json::Value::as_array)
                .ok_or(bad("ordered_ids array is required".to_owned()))?
                .iter()
                .map(|entry| {
                    entry
                        .as_i64()
                        .ok_or_else(|| "ordered_ids must be assignment ids".to_owned())
                })
                .collect::<Result<Vec<_>, String>>()
                .map_err(bad)?;
            routing::set_assignment_fallback_orders(pool, &ordered_ids)
                .await
                .map_err(storage_err)?;
            json!({ "ok": true, "updated": ordered_ids.len() })
        }
        "create_trigger_with_assignment" => {
            let mut assignment = assignment_input_from_json(body).map_err(bad)?;
            assignment.role = "overflow".to_owned();
            if assignment.weight.is_none() {
                assignment.weight = Some(100);
            }
            let mut trigger = trigger_input_from_json_without_engage(body).map_err(bad)?;
            trigger.workflow_key = assignment.workflow_key.clone();
            let (assignment_id, trigger_id) =
                routing::insert_trigger_with_assignment(pool, &assignment, &trigger)
                    .await
                    .map_err(storage_err)?;
            json!({ "ok": true, "assignment_id": assignment_id, "trigger_id": trigger_id })
        }
        "fetch_provider_models" => {
            let id = id_of(body).ok_or(bad("id is required".to_owned()))?;
            return admin_fetch_provider_models(pool, id, body).await;
        }
        "import_models" => {
            let provider_id = body
                .get("provider_id")
                .and_then(serde_json::Value::as_i64)
                .ok_or(bad("provider_id is required".to_owned()))?;
            let names: Vec<String> = body
                .get("names")
                .and_then(serde_json::Value::as_array)
                .ok_or(bad("names array is required".to_owned()))?
                .iter()
                .filter_map(|name| name.as_str())
                .map(str::to_owned)
                .collect();
            if names.is_empty() {
                return Err(bad("names must contain at least one model name".to_owned()));
            }
            let defaults = body.get("defaults").cloned().unwrap_or_else(|| json!({}));
            let capabilities = routing_capabilities(&defaults);
            let capabilities = if capabilities.is_empty() {
                vec!["chat".to_owned()]
            } else {
                capabilities
            };
            let pool_id = defaults.get("pool_id").and_then(serde_json::Value::as_i64);
            let existing: std::collections::HashSet<String> = routing::list_models(pool)
                .await
                .map_err(storage_err)?
                .into_iter()
                .filter(|model| model.provider_id == provider_id)
                .map(|model| model.model_name)
                .collect();
            let mut created = Vec::new();
            for name in names {
                if existing.contains(&name) {
                    continue;
                }
                let id = routing::insert_model(
                    pool,
                    &openplotva_storage::llm_routing::ModelInput {
                        provider_id,
                        model_name: name,
                        display_name: None,
                        base_url: None,
                        capabilities: capabilities.clone(),
                        embedding_dim: None,
                        pool_id,
                        enabled: true,
                        config: json!({}),
                    },
                )
                .await
                .map_err(storage_err)?;
                created.push(id);
            }
            json!({ "ok": true, "created_ids": created })
        }
        "reload" => json!({ "ok": true }),
        other => return Err(bad(format!("unknown action {other}"))),
    };

    // Every mutating action bumps the revision and atomically reloads the router so
    // the change takes effect without a restart.
    if action != "reload" {
        let revision = admin_app_setting(routes, "llm.routing.revision")
            .await
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        let _ =
            admin_upsert_app_setting(routes, "llm.routing.revision", &(revision + 1).to_string())
                .await;
    }
    let reload_result = if let Some(runtime) = routes.router_runtime.as_ref() {
        model_routing::reload_router_runtime(runtime, pool).await
    } else if let Some(handle) = routes.router_handle.as_ref() {
        let reloaded = model_routing::reload_router(handle, pool).await;
        if reloaded.is_ok()
            && let Some(pools) = routes.router_pools.as_ref()
        {
            pools.apply(&handle.snapshot().pool_specs());
        }
        reloaded
    } else {
        Ok(())
    };
    if let Err(error) = reload_result {
        if let Some(reporter) = routes.routing_event_reporter.as_ref() {
            reporter.record(runtime_routing::router_reload_failed_event(
                "admin_routing_reload",
                &error.to_string(),
            ));
        }
        tracing::warn!(%error, "failed to reload router after admin routing change");
    }
    Ok(result)
}

async fn admin_routing(
    method: Method,
    headers: HeaderMap,
    Extension(routes): Extension<StaticWebRoutes>,
    body: Bytes,
) -> Response {
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(pool) = routes.postgres.clone() else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
    };

    if method == Method::GET {
        return match admin_routing_snapshot_json(&pool).await {
            Ok(value) => admin_json_response(StatusCode::OK, value),
            Err(error) => admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, &error),
        };
    }
    if method != Method::POST && method != Method::PUT {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&body) else {
        return admin_error_response(StatusCode::BAD_REQUEST, "invalid json");
    };
    match admin_routing_apply_action(&pool, &routes, &parsed).await {
        Ok(value) => admin_json_response(StatusCode::OK, value),
        Err((status, error)) => admin_error_response(status, &error),
    }
}

/// Cheap, poll-safe live view for the routing cockpit: pool occupancy,
/// breaker states, trigger engagement, capacity cooldowns, worker scale, and
/// the in-process event ring. No database round-trip.
async fn admin_routing_status(
    method: Method,
    headers: HeaderMap,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let now = std::time::Instant::now();
    let pools: Vec<serde_json::Value> = routes
        .router_pools
        .as_ref()
        .map(|pools| pools.occupancy())
        .unwrap_or_default()
        .into_iter()
        .map(|occupancy| {
            json!({
                "id": occupancy.id,
                "max_concurrency": occupancy.max_concurrency,
                "in_flight": occupancy.in_flight,
            })
        })
        .collect();
    let breakers: Vec<serde_json::Value> = routes
        .router_breakers
        .as_ref()
        .map(|breakers| breakers.states_at(now))
        .unwrap_or_default()
        .into_iter()
        .map(|state| {
            json!({
                "provider_id": state.provider,
                "model_id": state.model,
                "consecutive_failures": state.consecutive_failures,
                "open": state.open,
                "cooldown_remaining_ms": state
                    .cooldown_remaining
                    .map(|left| u64::try_from(left.as_millis()).unwrap_or(u64::MAX)),
            })
        })
        .collect();
    let engaged_triggers = routes
        .router_triggers
        .as_ref()
        .map(|triggers| triggers.engaged_ids())
        .unwrap_or_default();
    let capacity_cooldowns: Vec<serde_json::Value> = routes
        .router_triggers
        .as_ref()
        .map(|triggers| triggers.capacity_snapshot_at(now))
        .unwrap_or_default()
        .into_iter()
        .map(|(provider, model, left)| {
            json!({
                "provider_id": provider,
                "model_id": model,
                "remaining_ms": u64::try_from(left.as_millis()).unwrap_or(u64::MAX),
            })
        })
        .collect();
    let workers = routes
        .dialog_worker_gauge
        .as_ref()
        .map(|gauge| {
            json!({
                "dialog": { "desired": gauge.desired(), "running": gauge.running() }
            })
        })
        .unwrap_or_else(|| json!({}));
    let recent_events: Vec<serde_json::Value> = routes
        .routing_event_buffer
        .as_ref()
        .map(|buffer| buffer.routing_events(20))
        .unwrap_or_default()
        .into_iter()
        .map(|event| routing_event_data_json(&event))
        .collect();
    let revision = admin_app_setting(&routes, "llm.routing.revision")
        .await
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    admin_json_no_cache_response(
        StatusCode::OK,
        json!({
            "revision": revision,
            "pools": pools,
            "breakers": breakers,
            "engaged_triggers": engaged_triggers,
            "capacity_cooldowns": capacity_cooldowns,
            "workers": workers,
            "recent_events": recent_events,
        }),
    )
}

/// Keyset-paginated journal over `llm_routing_events` with optional
/// `workflow` / `severity` filters and a `before_id` cursor.
async fn admin_routing_events(
    method: Method,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Extension(routes): Extension<StaticWebRoutes>,
) -> Response {
    if method != Method::GET {
        return admin_error_response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    if let Err(error) = require_admin_request(&headers, &routes.admin_ids, &routes.bot_token) {
        return admin_auth_failure_response(error);
    }
    let Some(pool) = routes.postgres.clone() else {
        return admin_error_response(StatusCode::SERVICE_UNAVAILABLE, "database unavailable");
    };
    let values = admin_auth_query_values(raw_query.as_deref());
    let limit = values
        .get("limit")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(50);
    let before_id = values
        .get("before_id")
        .and_then(|value| value.parse::<i64>().ok());
    let workflow = values
        .get("workflow")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let severity = values
        .get("severity")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    match openplotva_storage::llm_routing::list_routing_events_page(
        &pool,
        limit,
        before_id,
        workflow.as_deref(),
        severity.as_deref(),
    )
    .await
    {
        Ok(events) => {
            let next_before_id = events.last().map(|event| event.id);
            let events: Vec<serde_json::Value> = events
                .iter()
                .map(|event| {
                    json!({
                        "id": event.id,
                        "created_at": event
                            .created_at
                            .format(&time::format_description::well_known::Rfc3339)
                            .unwrap_or_default(),
                        "severity": event.severity,
                        "event_type": event.event_type,
                        "workflow_key": event.workflow_key,
                        "provider_id": event.provider_id,
                        "model_id": event.model_id,
                        "queue_name": event.queue_name,
                        "job_id": event.job_id,
                        "chat_id": event.chat_id,
                        "summary": event.summary,
                        "detail": event.detail,
                    })
                })
                .collect();
            admin_json_no_cache_response(
                StatusCode::OK,
                json!({ "events": events, "next_before_id": next_before_id }),
            )
        }
        Err(error) => {
            tracing::warn!(%error, "failed to list routing events");
            admin_error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed")
        }
    }
}

fn routing_event_data_json(event: &runtime_routing::RoutingEventData) -> serde_json::Value {
    json!({
        "id": event.id,
        "at_millis": event.at_millis,
        "severity": event.severity,
        "event_type": event.event_type,
        "workflow_key": event.workflow_key,
        "provider_id": event.provider_id,
        "model_id": event.model_id,
        "queue_name": event.queue_name,
        "summary": event.summary,
        "detail": event.detail,
    })
}

#[derive(Debug)]
struct AdminLogStreamState {
    buffer: Arc<openplotva_observability::RuntimeLogBuffer>,
    after_seq: u64,
    pending: VecDeque<openplotva_observability::RuntimeLogEntry>,
    interval: Interval,
}

fn admin_logs_sse_stream(
    buffer: Arc<openplotva_observability::RuntimeLogBuffer>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    let after_seq = buffer.latest_seq();
    stream::unfold(
        AdminLogStreamState {
            buffer,
            after_seq,
            pending: VecDeque::new(),
            interval: tokio::time::interval(Duration::from_secs(1)),
        },
        |mut state| async move {
            loop {
                if let Some(entry) = state.pending.pop_front() {
                    state.after_seq = state.after_seq.max(entry.seq);
                    let data = serde_json::json!({
                        "seq": entry.seq,
                        "time": entry.time,
                        "level": entry.level,
                        "message": entry.message,
                        "attrs": entry.attrs,
                    })
                    .to_string();
                    return Some((Ok(Event::default().data(data)), state));
                }
                state.interval.tick().await;
                state.pending = VecDeque::from(state.buffer.logs(state.after_seq, 100, "", ""));
            }
        },
    )
}

fn admin_static_asset_requires_auth(path: &str) -> bool {
    let path = path.trim_matches('/');
    path.is_empty() || path == "index.html"
}

impl StaticWebRoutes {
    fn redis_inspector(&self) -> Option<RedisRuntimeInspector> {
        self.redis.clone().map(RedisRuntimeInspector::new)
    }
}

fn admin_session_is_authorized(headers: &HeaderMap, admin_ids: &[i64], secret: &str) -> bool {
    admin_session_user_ids(headers, secret)
        .into_iter()
        .any(|user_id| admin_ids.contains(&user_id))
}

fn admin_session_user_id(headers: &HeaderMap, secret: &str) -> Option<i64> {
    admin_session_user_ids(headers, secret).into_iter().next()
}

fn admin_session_user_ids(headers: &HeaderMap, secret: &str) -> Vec<i64> {
    let Some(cookie) = headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())
    else {
        return Vec::new();
    };
    cookie
        .split(';')
        .filter_map(|part| {
            part.trim()
                .strip_prefix(openplotva_web::ADMIN_SESSION_COOKIE_NAME)
                .and_then(|value| value.strip_prefix('='))
        })
        .filter_map(|value| openplotva_web::verify_admin_session_value(value, secret))
        .collect()
}

/// Run the current OpenPlotva app shell.
pub async fn run() -> anyhow::Result<()> {
    let config = AppConfig::from_env().context("load configuration")?;
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
            static_web,
            webhook_route,
        )
    } else {
        router_with_readiness_and_static_web(readiness, static_web)
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
        "startup connection established",
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

pub async fn run_long_poll_update_producer_after_delete_webhook<Startup, Source, Queue, Stop>(
    startup: &Startup,
    source: &Source,
    queue: &Queue,
    stop: Stop,
) -> TelegramUpdateProducerStartupReport
where
    Startup: DeleteWebhookExecutor + Sync,
    Source: openplotva_updates::UpdateProducerSource + Sync,
    Queue: openplotva_updates::UpdateProducerQueue + Sync,
    Stop: Future<Output = ()>,
{
    if let Err(error) = startup.delete_webhook().await {
        return TelegramUpdateProducerStartupReport {
            delete_webhook_error: Some(error.to_string()),
            set_webhook_error: None,
            producer: None,
        };
    }

    TelegramUpdateProducerStartupReport {
        delete_webhook_error: None,
        set_webhook_error: None,
        producer: Some(openplotva_updates::run_update_producer_until(source, queue, stop).await),
    }
}

pub async fn run_long_poll_update_producer_with_ingress_guard_after_delete_webhook<
    Startup,
    Source,
    Queue,
    Stop,
>(
    startup: &Startup,
    source: &Source,
    queue: &Queue,
    ingress_guard: &openplotva_updates::UpdateIngressGuard,
    stop: Stop,
) -> TelegramUpdateProducerStartupReport
where
    Startup: DeleteWebhookExecutor + Sync,
    Source: openplotva_updates::UpdateProducerSource + Sync,
    Queue: openplotva_updates::UpdateProducerQueue + Sync,
    Stop: Future<Output = ()>,
{
    if let Err(error) = startup.delete_webhook().await {
        return TelegramUpdateProducerStartupReport {
            delete_webhook_error: Some(error.to_string()),
            set_webhook_error: None,
            producer: None,
        };
    }

    TelegramUpdateProducerStartupReport {
        delete_webhook_error: None,
        set_webhook_error: None,
        producer: Some(
            openplotva_updates::run_update_producer_with_ingress_guard_until(
                source,
                queue,
                ingress_guard,
                stop,
            )
            .await,
        ),
    }
}

pub async fn run_webhook_update_producer_after_set_webhook<Startup, Source, Queue, Stop>(
    startup: &Startup,
    setup: &openplotva_telegram::WebhookSetup,
    source: &Source,
    queue: &Queue,
    stop: Stop,
) -> TelegramUpdateProducerStartupReport
where
    Startup: SetWebhookExecutor + Sync,
    Source: openplotva_updates::UpdateProducerSource + Sync,
    Queue: openplotva_updates::UpdateProducerQueue + Sync,
    Stop: Future<Output = ()>,
{
    if let Err(error) = startup.set_webhook(setup).await {
        return TelegramUpdateProducerStartupReport {
            delete_webhook_error: None,
            set_webhook_error: Some(error.to_string()),
            producer: None,
        };
    }

    TelegramUpdateProducerStartupReport {
        delete_webhook_error: None,
        set_webhook_error: None,
        producer: Some(openplotva_updates::run_update_producer_until(source, queue, stop).await),
    }
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

    match timeout(GO_WEBHOOK_DELETE_ON_STOP_TIMEOUT, startup.delete_webhook()).await {
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

async fn telegram_webhook(
    Extension(route): Extension<TelegramWebhookRoute>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    telegram_webhook_response(&route.sender, method, headers, &route.secret_token, body).await
}

pub async fn telegram_webhook_response(
    sender: &openplotva_telegram::WebhookUpdateSender,
    method: Method,
    headers: HeaderMap,
    secret_token: &str,
    body: Bytes,
) -> Response {
    let provided_secret = headers
        .get(openplotva_telegram::TELEGRAM_WEBHOOK_SECRET_HEADER)
        .and_then(|value| value.to_str().ok());

    match sender
        .handle_webhook_request(method.as_str(), provided_secret, secret_token, &body)
        .await
    {
        Ok(()) => StatusCode::OK.into_response(),
        Err(error) => {
            let status = StatusCode::from_u16(error.http_status())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            match error.error_body() {
                Some(body) => (status, body).into_response(),
                None => status.into_response(),
            }
        }
    }
}

pub fn webhook_setup_from_config(
    config: &openplotva_config::BotWebhookConfig,
) -> std::io::Result<openplotva_telegram::WebhookSetup> {
    let mut setup = openplotva_telegram::WebhookSetup::new(
        config.url.clone(),
        Some(config.secret_token.clone()).filter(|secret_token| !secret_token.is_empty()),
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
    let mut fields = vec![
        (
            "allowed_updates".to_owned(),
            serde_json::to_string(openplotva_updates::GO_ALLOWED_UPDATE_NAMES)?,
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

/// Memory-consolidation parallelism. Runs are claimed with
/// `FOR UPDATE SKIP LOCKED` under a lease (one run = one chat window), so
/// parallel workers safely chew different chats. The effective count follows
/// the workflow's capacity pools — a pooled route (e.g. the 16-slot
/// vram.cloud pool) invites parallel workers up to the cap, while an unpooled
/// route keeps the configured count (historically one). `0` disables workers.
fn effective_memory_consolidation_workers(
    configured_workers: i32,
    pool_derived: Option<u32>,
    cap: i32,
) -> i32 {
    if configured_workers <= 0 {
        return 0;
    }
    let cap = cap.max(1);
    let derived = pool_derived
        .and_then(|derived| i32::try_from(derived).ok())
        .unwrap_or(1)
        .clamp(1, cap);
    configured_workers.max(derived)
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
struct RedisRuntimeInspector {
    client: redis::Client,
}

impl RedisRuntimeInspector {
    fn new(client: redis::Client) -> Self {
        Self { client }
    }

    async fn scan_all_keys(&self, pattern: &str) -> Result<Vec<String>, redis::RedisError> {
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let mut cursor = 0_u64;
        let mut keys = Vec::new();

        loop {
            let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(1000)
                .query_async(&mut connection)
                .await?;
            keys.extend(batch);
            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }

        Ok(keys)
    }

    async fn scan_keys(
        &self,
        pattern: &str,
        limit: usize,
    ) -> Result<Vec<String>, redis::RedisError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let mut cursor = 0_u64;
        let mut keys = Vec::new();

        loop {
            let remaining = limit.saturating_sub(keys.len());
            if remaining == 0 {
                break;
            }
            let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(remaining.min(100))
                .query_async(&mut connection)
                .await?;

            keys.extend(batch.into_iter().take(remaining));
            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }

        Ok(keys)
    }

    async fn prefix_groups(
        &self,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<openplotva_server::RuntimeRedisPrefixGroup>, redis::RedisError> {
        let keys = self.scan_keys(&format!("{prefix}*"), limit).await?;
        Ok(runtime_redis_prefix_groups_from_keys(prefix, keys))
    }

    async fn value(
        &self,
        key: &str,
        max_bytes: usize,
    ) -> Result<Option<openplotva_server::RuntimeRedisValue>, redis::RedisError> {
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(key)
            .query_async(&mut connection)
            .await?;
        Ok(value.map(|bytes| runtime_redis_value_from_bytes(key, &bytes, max_bytes)))
    }

    async fn raw_value(&self, key: &str) -> Result<Option<String>, redis::RedisError> {
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(key)
            .query_async(&mut connection)
            .await?;
        Ok(value.map(|bytes| String::from_utf8_lossy(&bytes).into_owned()))
    }

    async fn delete_key(&self, key: &str) -> Result<(), redis::RedisError> {
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let _: () = redis::cmd("DEL")
            .arg(key)
            .query_async(&mut connection)
            .await?;
        Ok(())
    }

    async fn delete_by_prefix(&self, prefix: &str) -> Result<(), redis::RedisError> {
        let pattern = format!("{prefix}*");
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let mut cursor = 0_u64;

        loop {
            let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(1000)
                .query_async(&mut connection)
                .await?;
            if !batch.is_empty() {
                let _: () = redis::cmd("DEL")
                    .arg(batch)
                    .query_async(&mut connection)
                    .await?;
            }
            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }

        Ok(())
    }

    async fn flushdb(&self) -> Result<(), redis::RedisError> {
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        redis::cmd("FLUSHDB").query_async(&mut connection).await
    }
}

fn runtime_redis_prefix_groups_from_keys(
    prefix: &str,
    keys: impl IntoIterator<Item = String>,
) -> Vec<openplotva_server::RuntimeRedisPrefixGroup> {
    let mut groups = HashMap::<String, i32>::new();
    for key in keys {
        let Some(rest) = key.strip_prefix(prefix) else {
            continue;
        };
        if rest.is_empty() {
            continue;
        }
        let segment = match rest.find(':') {
            Some(index) => &rest[..=index],
            None => rest,
        };
        *groups.entry(format!("{prefix}{segment}")).or_default() += 1;
    }

    let mut keys = groups.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    keys.into_iter()
        .map(|prefix| openplotva_server::RuntimeRedisPrefixGroup {
            count: groups[&prefix],
            prefix,
        })
        .collect()
}

fn admin_redis_prefix_groups_from_keys(
    prefix: &str,
    keys: impl IntoIterator<Item = String>,
) -> HashMap<String, i32> {
    let mut groups = HashMap::<String, i32>::new();
    for key in keys {
        let Some(rest) = key.strip_prefix(prefix) else {
            continue;
        };
        if rest.is_empty() {
            continue;
        };
        let segment = match rest.find(':') {
            Some(index) => &rest[..=index],
            None => rest,
        };
        *groups.entry(format!("{prefix}{segment}")).or_default() += 1;
    }
    groups
}

fn runtime_redis_value_from_bytes(
    key: &str,
    bytes: &[u8],
    max_bytes: usize,
) -> openplotva_server::RuntimeRedisValue {
    let truncated = max_bytes > 0 && bytes.len() > max_bytes;
    let bytes = if truncated {
        &bytes[..max_bytes]
    } else {
        bytes
    };
    openplotva_server::RuntimeRedisValue {
        key: key.to_owned(),
        value: String::from_utf8_lossy(bytes).into_owned(),
        truncated,
    }
}

impl openplotva_server::RuntimeRedisInspector for RedisRuntimeInspector {
    fn prefix_groups<'a>(
        &'a self,
        prefix: &'a str,
        limit: usize,
    ) -> openplotva_server::RuntimeRedisInspectorFuture<
        'a,
        Vec<openplotva_server::RuntimeRedisPrefixGroup>,
    > {
        Box::pin(async move {
            self.prefix_groups(prefix, limit)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn keys<'a>(
        &'a self,
        pattern: &'a str,
        limit: usize,
    ) -> openplotva_server::RuntimeRedisInspectorFuture<'a, Vec<String>> {
        Box::pin(async move {
            self.scan_keys(pattern, limit)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn value<'a>(
        &'a self,
        key: &'a str,
        max_bytes: usize,
    ) -> openplotva_server::RuntimeRedisInspectorFuture<
        'a,
        Option<openplotva_server::RuntimeRedisValue>,
    > {
        Box::pin(async move {
            self.value(key, max_bytes)
                .await
                .map_err(|error| error.to_string())
        })
    }
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
        let cert_pem = std::fs::read(&config.cert_file)
            .with_context(|| format!("read runtime API certificate {}", config.cert_file))?;
        let key_pem = std::fs::read(&config.key_file)
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

    let (stop, _) = watch::channel(false);
    let taskman_inspector = runtime_taskman::RuntimeTaskmanInspectorHandle::default();
    let virtual_dialog_manager =
        runtime_virtual_dialog::RuntimeVirtualDialogManagerHandle::default();
    let dispatcher_inspector = runtime_dispatcher::RuntimeDispatcherInspectorHandle::default();
    let dispatch_failure_ring = Arc::new(DispatchFailureRing::default());
    let cache_inspector = runtime_cache::RuntimeCacheInspectorHandle::default();
    let mut workers = RuntimeWorkers {
        handles: Vec::new(),
        stop: Some(stop.clone()),
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
        routing_event_buffer: None,
        routing_event_reporter: None,
        runtime_api_tls_public_key_pin: None,
        router_handle: None,
        router_breakers: None,
        router_triggers: None,
        router_pools: None,
        router_runtime: None,
        dialog_worker_gauge: None,
    };
    let routing_event_buffer = runtime_routing::RoutingEventBuffer::default();
    let (routing_event_recorder, routing_event_recorder_worker) =
        runtime_routing::PostgresRoutingEventRecorder::spawn(
            service_clients.postgres.clone(),
            stop.subscribe(),
        );
    workers.handles.push(routing_event_recorder_worker);
    let mut routing_event_reporter = runtime_routing::RoutingEventReporter::new(
        routing_event_buffer.clone(),
        Some(routing_event_recorder.clone()),
        None,
        runtime_routing::DEFAULT_ROUTING_ADMIN_REPORT_COOLDOWN,
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
    if let Err(error) = model_routing::backfill_gpu_models(&service_clients.postgres, config).await
    {
        routing_event_reporter.record(runtime_routing::routing_backfill_failed_event(
            "backfill_gpu_models",
            &error.to_string(),
        ));
        tracing::warn!(%error, "failed to backfill GPU Qwen models into routing tables");
    }
    if let Err(error) =
        model_routing::backfill_dialog_qwen_fallback(&service_clients.postgres, config).await
    {
        routing_event_reporter.record(runtime_routing::routing_backfill_failed_event(
            "backfill_dialog_qwen_fallback",
            &error.to_string(),
        ));
        tracing::warn!(%error, "failed to backfill dialog GPU Qwen fallback");
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
    if let Err(error) = model_routing::backfill_memory_vram_pool(&service_clients.postgres).await {
        routing_event_reporter.record(runtime_routing::routing_backfill_failed_event(
            "backfill_memory_vram_pool",
            &error.to_string(),
        ));
        tracing::warn!(%error, "failed to route memory consolidation onto the vram pool");
    }
    let router_breakers = Arc::new(openplotva_llm::router::BreakerSet::new());
    let router_triggers = Arc::new(openplotva_llm::router::TriggerState::new());
    let router_pools = Arc::new(openplotva_llm::router::PoolRegistry::new());
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
    workers.router_handle = Some(Arc::clone(&router_handle));
    workers.router_breakers = Some(Arc::clone(&router_breakers));
    workers.router_triggers = Some(Arc::clone(&router_triggers));
    workers.router_pools = Some(Arc::clone(&router_pools));
    router_pools.apply(&router_handle.snapshot().pool_specs());
    let update_queue =
        openplotva_updates::RedisUpdateQueue::new(service_clients.redis.client().clone());
    let update_queue_backend = config.update_queue.backend.as_str();
    let update_ingress_guard = Arc::new(openplotva_updates::UpdateIngressGuard::with_defaults());
    let updates_inspector =
        runtime_updates::RuntimeUpdatesInspectorHandle::new(update_queue.clone());
    let ingestion_gate_counters = Arc::new(ingestion_telemetry::IngestionGateCounters::default());
    updates_inspector.set_gate_counters(Arc::clone(&ingestion_gate_counters));
    let llm_trace_buffer = runtime_llm::RuntimeLlmTraceBuffer::default();
    let (llm_event_recorder, llm_event_recorder_worker) =
        runtime_llm::PostgresRuntimeLlmEventRecorder::spawn(
            service_clients.postgres.clone(),
            stop.subscribe(),
        );
    workers.handles.push(llm_event_recorder_worker);
    let turn_outcome_buffer = dialog_turn::RuntimeTurnOutcomeBuffer::default();
    let (turn_outcome_recorder, turn_outcome_recorder_worker) =
        dialog_turn::PostgresDialogTurnOutcomeRecorder::spawn(
            service_clients.postgres.clone(),
            stop.subscribe(),
        );
    workers.handles.push(turn_outcome_recorder_worker);
    let turn_outcome_observer = dialog_turn::DialogTurnObserver::new(
        turn_outcome_buffer.clone(),
        Some(turn_outcome_recorder),
    );
    let turn_outcome_cleanup_pool = service_clients.postgres.clone();
    let turn_outcome_cleanup_stop = stop.subscribe();
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
    workers.handles.push(turn_outcome_cleanup_worker);
    let llm_request_events_retention_days = config.runtime_api.llm_request_events_retention_days;
    if llm_request_events_retention_days > 0 {
        let llm_event_cleanup_pool = service_clients.postgres.clone();
        let llm_event_cleanup_stop = stop.subscribe();
        let llm_event_cleanup_interval = runtime_llm::LLM_REQUEST_EVENTS_CLEANUP_INTERVAL;
        let llm_event_cleanup_worker = tokio::spawn(async move {
            let report = runtime_llm::run_llm_request_event_cleanup_worker_until(
                llm_event_cleanup_pool,
                llm_event_cleanup_interval,
                llm_request_events_retention_days,
                runtime_llm::LLM_REQUEST_EVENTS_CLEANUP_BATCH_SIZE,
                wait_for_runtime_stop(llm_event_cleanup_stop),
            )
            .await;

            tracing::info!(?report, "llm_request_events cleanup worker stopped");
        });
        readiness_checks.push(ReadinessCheck::ok(
            "llm_request_events_cleanup",
            format!(
                "LLM analytics rollup and raw request events cleanup every {}s, retention {}d",
                llm_event_cleanup_interval.as_secs(),
                llm_request_events_retention_days
            ),
        ));
        workers.handles.push(llm_event_cleanup_worker);
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "llm_request_events_cleanup",
            "LLM raw request events cleanup disabled",
        ));
    }
    let chat_history_retention_days = config.llm.history_summary.chat_history_retention_days;
    if chat_history_retention_days > 0 {
        let pool = service_clients.postgres.clone();
        let stop_rx = stop.subscribe();
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
        workers.handles.push(worker);
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "chat_history_retention",
            "chat_history partition retention disabled",
        ));
    }
    let telegram_files_retention_days = config.vision.telegram_files_retention_days;
    if telegram_files_retention_days > 0 {
        let pool = service_clients.postgres.clone();
        let stop_rx = stop.subscribe();
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
        workers.handles.push(worker);
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_files_retention",
            "telegram_files retention disabled",
        ));
    }
    let whitecircle_checks_retention_days = config.white_circle.whitecircle_checks_retention_days;
    if whitecircle_checks_retention_days > 0 {
        let pool = service_clients.postgres.clone();
        let stop_rx = stop.subscribe();
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
        workers.handles.push(worker);
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "whitecircle_checks_retention",
            "whitecircle_checks retention disabled",
        ));
    }
    if llm_request_events_retention_days > 0 {
        let routing_event_cleanup_pool = service_clients.postgres.clone();
        let routing_event_cleanup_stop = stop.subscribe();
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
        workers.handles.push(routing_event_cleanup_worker);
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "llm_routing_events_cleanup",
            "LLM routing events cleanup disabled",
        ));
    }
    workers.llm_trace_buffer = Some(llm_trace_buffer.clone());
    let llm_observer: Arc<dyn openplotva_llm::LlmCallObserver> =
        Arc::new(runtime_llm::RuntimeLlmObserver::new(
            llm_trace_buffer.clone(),
            Some(llm_event_recorder.clone()),
        ));
    openplotva_llm::trace::set_observer(llm_observer);
    let memory_store = PostgresMemoryStore::new(service_clients.postgres.clone());
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
            turn_outcome_inspector: Some(Arc::new(turn_outcome_buffer.clone())),
            routing_event_inspector: Some(Arc::new(routing_event_buffer.clone())),
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
            log_inspector: Some(Arc::new(RuntimeLogInspector::new(Arc::clone(&log_buffer)))),
            taskman_inspector: Some(runtime_taskman_reader),
            updates_inspector: Some(Arc::new(updates_inspector.clone())),
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
            stop.subscribe(),
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
        workers.handles.push(worker);
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "runtime_api",
            "RUNTIME_API_ENABLED=false",
        ));
    }

    if config.memory.enabled && config.bot.key.is_none() {
        let memory_write_embedder = Some(embedder::RoutedDiscoveryEmbedder::new(
            routed_attempts::RoutedAttemptWalker::new(
                Arc::clone(&router_handle),
                Arc::clone(&router_breakers),
                Arc::clone(&router_triggers),
                Arc::clone(&router_pools),
            )
            .with_reporter(routing_event_reporter.clone()),
            embedder::DiscoveryEmbedderConfig {
                base_url: config.llm.discovery.base_url.clone(),
                service_name: config.memory.embedder_service_name.clone(),
                endpoint_name: config.memory.embedder_endpoint_name.clone(),
                request_timeout: memory_runtime::EMBEDDER_DEFAULT_TIMEOUT,
                task_timeout: memory_runtime::EMBEDDER_DEFAULT_TIMEOUT,
                poll_interval: Duration::from_millis(100),
                capacity_wait: Duration::from_secs(2),
            },
        ));
        let memory_extractor = memory_runtime::routed_memory_extractor_from_app_config(
            config,
            routed_attempts::RoutedAttemptWalker::new(
                Arc::clone(&router_handle),
                Arc::clone(&router_breakers),
                Arc::clone(&router_triggers),
                Arc::clone(&router_pools),
            )
            .with_reporter(routing_event_reporter.clone()),
        );
        let memory_worker_store = memory_store.clone();
        let memory_worker_config =
            memory_runtime::memory_service_worker_config_from_memory_config(&config.memory);
        let memory_worker_stop = stop.subscribe();
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
        workers.handles.push(memory_worker);
    } else if !config.memory.enabled {
        readiness_checks.push(ReadinessCheck::skipped(
            "memory_service",
            "MEMORY_ENABLED=false",
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
    let rich_sender: Arc<dyn rich::RichSender> = Arc::new(rich::RichMessenger::new(
        rich_api.clone(),
        openplotva_media::uploader::UploaderClient::new(
            openplotva_media::uploader::UploaderConfig {
                base_url: config.uploader.base_url.clone(),
                secret: config.uploader.secret.clone(),
                timeout: std::time::Duration::from_secs(
                    config.uploader.timeout_seconds.max(0) as u64
                ),
            },
        )
        .context("create media uploader client")?,
    ));
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
        let cleanup_stop = stop.subscribe();
        let cleanup_interval = runtime_virtual_dialog::RUNTIME_VIRTUAL_DIALOG_CLEANUP_INTERVAL;
        let cleanup_worker = tokio::spawn(async move {
            let deleted = runtime_virtual_dialog::run_runtime_virtual_dialog_cleanup_worker_until(
                cleanup_store,
                cleanup_history,
                cleanup_taskman,
                cleanup_llm_traces,
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
        workers.handles.push(cleanup_worker);
    }
    let ephemeral_store = service_clients.redis.ephemeral_message_store();

    let dispatcher_persistence = openplotva_telegram::RedisDispatcherQueueStore::new(
        service_clients.redis.client().clone(),
        openplotva_telegram::DEFAULT_DISPATCHER_QUEUE_KEY,
        GO_DISPATCHER_MAX_QUEUE_SIZE,
    );
    let dispatcher_queue = Arc::new(openplotva_telegram::DispatcherQueue::new(
        go_dispatcher_config(),
    ));
    let routing_admin_notifier: Arc<dyn runtime_routing::RoutingAdminNotifier> =
        Arc::new(runtime_routing::DispatcherRoutingAdminNotifier::new(
            Arc::<[i64]>::from(config.admins.admin_ids.clone()),
            Arc::clone(&dispatcher_queue),
        ));
    routing_event_reporter = runtime_routing::RoutingEventReporter::new(
        routing_event_buffer.clone(),
        Some(routing_event_recorder.clone()),
        Some(routing_admin_notifier),
        runtime_routing::DEFAULT_ROUTING_ADMIN_REPORT_COOLDOWN,
    );
    workers.routing_event_reporter = Some(routing_event_reporter.clone());
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
    cache_inspector.set_policy_caches(
        Arc::clone(&rate_limit_policy),
        Arc::clone(&permission_policy),
    );
    let chat_settings_store = PostgresChatSettingsStore::new(service_clients.postgres.clone());
    let chat_member_store = PostgresChatMemberStore::new(service_clients.postgres.clone());
    let stale_inactive_member_cleanup_store = chat_member_store.clone();
    let stale_inactive_member_cleanup_stop = stop.subscribe();
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
    workers.handles.push(stale_inactive_member_cleanup_worker);

    let payment_store = payments::PostgresSuccessfulPaymentStore::new(
        store.clone(),
        PostgresPaymentStore::new(service_clients.postgres.clone()),
        PostgresVipStore::new(service_clients.postgres.clone()),
    );
    let payment_successful_effects = payments::SuccessfulPaymentDispatcherEffects::new(
        Arc::clone(&dispatcher_queue),
        payments::NoopVipCacheInvalidator,
    );
    let payment_effects =
        payments::PaymentRuntimeEffects::new(telegram.clone(), payment_successful_effects);
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
        let subscription_sync_stop = stop.subscribe();
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
        workers.handles.push(subscription_sync_worker);
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "subscription_sync",
            "SUBSCRIPTION_SYNC_ENABLED=false",
        ));
    }
    let taskman_ids = openplotva_taskman::TaskQueueIdAllocator::new();
    let translation_cache = service_clients.redis.translation_cache_store();
    let translator = match translate::t8_translator_from_app_config(config, translation_cache) {
        Ok(translator) => RuntimeTranslator::Ready(Box::new(translator)),
        Err(error) => {
            tracing::warn!(%error, "translation provider unavailable for unified control jobs");
            RuntimeTranslator::Unavailable(error.to_string())
        }
    };
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
        join_greeting_runtime,
    );
    let member_effects = members::MemberStateRuntimeEffects::new(
        chat_member_store.clone(),
        telegram.clone(),
        telegram.clone(),
        admin_cache_store,
    );
    let checkin_game_store = checkin::PostgresCheckinGameStore::new(
        chat_settings_store.clone(),
        chat_member_store.clone(),
    );
    let checkin_effects = checkin::CheckinGameRuntimeEffects::new(
        checkin_game_store.clone(),
        checkin::TelegramCheckinGameSender::new(
            ephemeral_store.clone(),
            telegram.clone(),
            rich_api.clone(),
        ),
        Arc::clone(&permission_policy),
        bot_identity.id,
    );
    let bot_username = bot_identity.username.clone();
    let telegram_effects = Arc::new(telegram.clone());
    let payment_rich_effects = Arc::new(payments::RichPaymentEffects::new(
        telegram.clone(),
        Arc::clone(&rich_sender),
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
    let control_queue = payments::PersistentPaymentControlJobQueue::from_task_queue(
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
    let shared_task_queue_db_journal = shared_task_queue
        .db_journal()
        .expect("Postgres taskman runtime should have a DB journal");
    let shared_task_queue_db_sync_stop = stop.subscribe();
    let shared_task_queue_db_sync_worker = tokio::spawn(async move {
        let report = task_queue::run_task_queue_db_sync_worker_until(
            shared_task_queue_db_journal,
            wait_for_runtime_stop(shared_task_queue_db_sync_stop),
        )
        .await;

        tracing::info!(?report, "shared taskman Postgres sync worker stopped");
    });
    readiness_checks.push(ReadinessCheck::ok(
        "shared_task_queue_postgres_sync",
        format!(
            "Postgres sync after {}s dirty window or {} mutations",
            task_queue::TASK_QUEUE_DB_SYNC_INTERVAL.as_secs(),
            task_queue::TASK_QUEUE_DB_SYNC_MUTATION_THRESHOLD
        ),
    ));
    workers.handles.push(shared_task_queue_db_sync_worker);
    let shared_task_queue_recovery_runtime = shared_task_queue.clone();
    let shared_task_queue_recovery_stop = stop.subscribe();
    let shared_task_queue_recovery_worker = tokio::spawn(async move {
        let report = task_queue::run_shared_task_queue_recovery_worker_until(
            shared_task_queue_recovery_runtime,
            shared_task_queue_recovery_interval,
            wait_for_runtime_stop(shared_task_queue_recovery_stop),
        )
        .await;

        tracing::info!(?report, "shared taskman recovery worker stopped");
    });
    readiness_checks.push(ReadinessCheck::ok(
        "shared_task_queue_recovery",
        format!(
            "stale processing recovery every {}s",
            shared_task_queue_recovery_interval.as_secs()
        ),
    ));
    workers.handles.push(shared_task_queue_recovery_worker);
    let shared_task_queue_cleanup_runtime = shared_task_queue.clone();
    let shared_task_queue_cleanup_stop = stop.subscribe();
    let shared_task_queue_cleanup_worker = tokio::spawn(async move {
        let report = task_queue::run_shared_task_queue_terminal_cleanup_worker_until(
            shared_task_queue_cleanup_runtime,
            shared_task_queue_cleanup_interval,
            shared_task_queue_completed_retention,
            wait_for_runtime_stop(shared_task_queue_cleanup_stop),
        )
        .await;

        tracing::info!(?report, "shared taskman terminal cleanup worker stopped");
    });
    readiness_checks.push(ReadinessCheck::ok(
        "shared_task_queue_terminal_cleanup",
        format!(
            "terminal cleanup every {}s, retention {}d",
            shared_task_queue_cleanup_interval.as_secs(),
            shared_task_queue_completed_retention.whole_days()
        ),
    ));
    workers.handles.push(shared_task_queue_cleanup_worker);
    let shared_task_queue_purge_store =
        openplotva_storage::PostgresTaskQueueStore::new(service_clients.postgres.clone());
    let shared_task_queue_purge_stop = stop.subscribe();
    let shared_task_queue_purge_worker = tokio::spawn(async move {
        let report = task_queue::run_shared_task_queue_db_purge_worker_until(
            shared_task_queue_purge_store,
            shared_task_queue_cleanup_interval,
            shared_task_queue_completed_retention,
            wait_for_runtime_stop(shared_task_queue_purge_stop),
        )
        .await;

        tracing::info!(?report, "shared taskman Postgres purge worker stopped");
    });
    readiness_checks.push(ReadinessCheck::ok(
        "shared_task_queue_postgres_purge",
        format!(
            "Postgres purge every {}s, retention {}d",
            shared_task_queue_cleanup_interval.as_secs(),
            shared_task_queue_completed_retention.whole_days()
        ),
    ));
    workers.handles.push(shared_task_queue_purge_worker);
    let shared_task_queue_stuck_runtime = shared_task_queue.clone();
    let shared_task_queue_stuck_stop = stop.subscribe();
    let shared_task_queue_stuck_worker = tokio::spawn(async move {
        let report = task_queue::run_shared_task_queue_stuck_cleanup_worker_until(
            shared_task_queue_stuck_runtime,
            task_queue::SHARED_TASK_QUEUE_STUCK_SCAN_INTERVAL,
            task_queue::SHARED_TASK_QUEUE_STUCK_DURATION,
            wait_for_runtime_stop(shared_task_queue_stuck_stop),
        )
        .await;

        tracing::info!(?report, "shared taskman stuck-job cleanup worker stopped");
    });
    readiness_checks.push(ReadinessCheck::ok(
        "shared_task_queue_stuck_cleanup",
        format!(
            "stuck-job cleanup every {}s, stuck after {}s",
            task_queue::SHARED_TASK_QUEUE_STUCK_SCAN_INTERVAL.as_secs(),
            task_queue::SHARED_TASK_QUEUE_STUCK_DURATION.as_secs()
        ),
    ));
    workers.handles.push(shared_task_queue_stuck_worker);
    let shared_task_queue_placeholder_runtime = shared_task_queue.clone();
    let shared_task_queue_placeholder_effects = telegram.clone();
    let shared_task_queue_placeholder_stop = stop.subscribe();
    let shared_task_queue_placeholder_worker = tokio::spawn(async move {
        let report = task_queue::run_shared_task_queue_placeholder_cleanup_worker_until(
            shared_task_queue_placeholder_runtime,
            shared_task_queue_placeholder_effects,
            shared_task_queue_placeholder_cleanup_interval,
            shared_task_queue_placeholder_max_age,
            std::time::Duration::from_secs(1),
            wait_for_runtime_stop(shared_task_queue_placeholder_stop),
        )
        .await;

        tracing::info!(?report, "shared taskman placeholder cleanup worker stopped");
    });
    readiness_checks.push(ReadinessCheck::ok(
        "shared_task_queue_placeholder_cleanup",
        format!(
            "placeholder cleanup every {}s, max age {}s",
            shared_task_queue_placeholder_cleanup_interval.as_secs(),
            shared_task_queue_placeholder_max_age.as_secs()
        ),
    ));
    workers.handles.push(shared_task_queue_placeholder_worker);
    workers.shared_task_queue = Some(shared_task_queue.clone());
    let mut shared_taskman_worker_counts = BTreeMap::new();
    shared_taskman_worker_counts.insert(openplotva_taskman::CONTROL_QUEUE_NAME.to_owned(), 1);
    if config.memory.enabled {
        let memory_write_embedder = Some(embedder::RoutedDiscoveryEmbedder::new(
            routed_attempts::RoutedAttemptWalker::new(
                Arc::clone(&router_handle),
                Arc::clone(&router_breakers),
                Arc::clone(&router_triggers),
                Arc::clone(&router_pools),
            )
            .with_reporter(routing_event_reporter.clone()),
            embedder::DiscoveryEmbedderConfig {
                base_url: config.llm.discovery.base_url.clone(),
                service_name: config.memory.embedder_service_name.clone(),
                endpoint_name: config.memory.embedder_endpoint_name.clone(),
                request_timeout: memory_runtime::EMBEDDER_DEFAULT_TIMEOUT,
                task_timeout: memory_runtime::EMBEDDER_DEFAULT_TIMEOUT,
                poll_interval: Duration::from_millis(100),
                capacity_wait: Duration::from_secs(2),
            },
        ));
        let memory_scheduler_store = memory_store.clone();
        let memory_scheduler_queue = Arc::clone(&task_queue_for_updates);
        let memory_scheduler_config =
            memory_runtime::memory_service_worker_config_from_memory_config(&config.memory);
        let memory_scheduler_trigger = Arc::clone(&memory_restart_trigger);
        let memory_scheduler_stop = stop.subscribe();
        let memory_scheduler = tokio::spawn(async move {
            let report =
                memory_runtime::run_memory_consolidation_taskman_scheduler_with_trigger_until(
                    &memory_scheduler_store,
                    memory_scheduler_queue.as_ref(),
                    memory_scheduler_config,
                    memory_scheduler_trigger,
                    wait_for_runtime_stop(memory_scheduler_stop),
                )
                .await;

            tracing::info!(?report, "memory-consolidation taskman scheduler stopped");
        });
        workers.handles.push(memory_scheduler);

        let configured_memory_worker_count = config.persistent_queue.memory_consolidation_workers;
        let memory_workers_cap = config.persistent_queue.memory_workers_cap.max(1);
        let derived_memory_workers = openplotva_llm::router::derived_worker_count(
            &router_handle.snapshot(),
            "memory_consolidation",
            1,
            u32::try_from(memory_workers_cap).unwrap_or(1),
        );
        let memory_worker_count = effective_memory_consolidation_workers(
            configured_memory_worker_count,
            derived_memory_workers,
            memory_workers_cap,
        );
        if memory_worker_count != configured_memory_worker_count {
            tracing::info!(
                configured_workers = configured_memory_worker_count,
                effective_workers = memory_worker_count,
                cap = memory_workers_cap,
                "memory-consolidation worker count derived from capacity pools"
            );
        }
        for index in 0..memory_worker_count {
            let memory_worker_extractor = memory_runtime::routed_memory_extractor_from_app_config(
                config,
                routed_attempts::RoutedAttemptWalker::new(
                    Arc::clone(&router_handle),
                    Arc::clone(&router_breakers),
                    Arc::clone(&router_triggers),
                    Arc::clone(&router_pools),
                )
                .with_reporter(routing_event_reporter.clone()),
            );
            let memory_worker_store = memory_store.clone();
            let memory_worker_embedder = memory_write_embedder.clone();
            let memory_worker_queue = Arc::clone(&task_queue_for_updates);
            let memory_worker_id = format!(
                "{}-{index}",
                memory_runtime::MEMORY_CONSOLIDATION_JOB_WORKER_PREFIX
            );
            let memory_worker_config = memory_runtime::MemoryConsolidationQueueWorkerConfig {
                process: memory_runtime::memory_service_worker_config_from_memory_config(
                    &config.memory,
                )
                .process,
                worker_id: memory_worker_id.clone(),
                interval: memory_runtime::MEMORY_CONSOLIDATION_JOB_POLL_INTERVAL,
                pipeline_target: usize::try_from(memory_worker_count).unwrap_or(1),
            };
            let memory_worker_stop = stop.subscribe();
            let memory_worker = tokio::spawn(async move {
                let report = memory_runtime::run_memory_consolidation_taskman_worker_until(
                    memory_worker_queue.as_ref(),
                    &memory_worker_extractor,
                    &memory_worker_store,
                    memory_worker_embedder.as_ref(),
                    memory_worker_config,
                    wait_for_runtime_stop(memory_worker_stop),
                )
                .await;

                tracing::info!(?report, worker_id = %memory_worker_id, "memory-consolidation taskman worker stopped");
            });
            workers.handles.push(memory_worker);
        }
        readiness_checks.push(ReadinessCheck::ok(
            "memory_service",
            format!(
                "Memory service schedules memory-consolidation taskman jobs and starts {memory_worker_count} routed workers over SQLx persistence"
            ),
        ));
        workers.memory_restart_trigger = Some(Arc::clone(&memory_restart_trigger));
        shared_taskman_worker_counts.insert(
            openplotva_taskman::MEMORY_CONSOLIDATION_QUEUE_NAME.to_owned(),
            memory_worker_count,
        );
    }
    let payment_store_for_updates = Arc::new(payment_store.clone());
    let vip_status_for_updates = Arc::new(payments::VipStatusWithExternalMembership::new(
        payment_store.clone(),
        payments::TelegramExternalVipMembershipChecker::new(telegram.clone()),
        config.vip.chat_id,
    ));
    let store_for_updates = Arc::new(store.clone());
    let history_store_for_updates = Arc::new(history_store.clone());
    let chat_members_for_updates = Arc::new(chat_member_store.clone());
    let (message_activity_store, message_activity_worker) =
        activity::BufferedMessageActivityStore::spawn(
            Arc::clone(&chat_members_for_updates),
            stop.subscribe(),
        );
    let message_activity_store = Arc::new(message_activity_store);
    workers.handles.push(message_activity_worker);
    let checkin_game_store_for_updates = Arc::new(checkin_game_store.clone());
    let rate_limits_for_updates = Arc::clone(&rate_limit_policy);
    let permission_policy_for_updates = Arc::clone(&permission_policy);
    let dispatcher_queue_for_updates = Arc::clone(&dispatcher_queue);
    let control_dispatcher_queue = Arc::clone(&dispatcher_queue);
    let dialog_translator = Arc::new(translator.clone());
    let control_stop = stop.subscribe();
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
    workers.handles.push(control_worker);

    let music_service_available = config.music.acestep.enabled;
    let delivery_obligation_store = Arc::new(
        openplotva_storage::PostgresDeliveryObligationStore::new(service_clients.postgres.clone()),
    );
    // One shared reaction signaler feeds the tool adapter, image/music
    // workers, and the obligations watcher.
    let generation_reactions: reactions::GenerationReactions =
        Arc::new(reactions::GenerationReactionSignaler::new(telegram.clone()));
    let dialog_tool_adapter =
        dialog_tools::TaskmanDialogToolAdapter::new(Arc::clone(&task_queue_for_updates))
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
                    dialog_tools::TelegramBotFileUrlProvider::new(
                        telegram.clone(),
                        bot_key.to_owned(),
                    ),
                ),
            ));
    let dialog_tool_adapter =
        Arc::new(dialog_tool_adapter.with_generation_reactions(Arc::clone(&generation_reactions)));
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
        let watcher_stop = stop.subscribe();
        workers.handles.push(tokio::spawn(async move {
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
        }));
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
    let dialog_tool_vision = Arc::new(
        vision::TelegramVisionDescriber::new(
            PostgresTelegramFileStore::new(service_clients.postgres.clone()),
            vision::RoutedVisionCaptioner::new(
                routed_attempts::RoutedAttemptWalker::new(
                    Arc::clone(&router_handle),
                    Arc::clone(&router_breakers),
                    Arc::clone(&router_triggers),
                    Arc::clone(&router_pools),
                )
                .with_reporter(routing_event_reporter.clone()),
                vision::aifarm_vision_captioner_config_from_app_config(config),
                vision_data_urls.clone(),
            ),
        )
        .with_model_name(config.vision.model.clone())
        .with_history_store(history_store.clone()),
    );
    let dialog_context_vision = Arc::new(vision::TelegramDialogVisionInputMaterializer::new(
        vision::TelegramVisionDescriber::new(
            PostgresTelegramFileStore::new(service_clients.postgres.clone()),
            vision::RoutedVisionCaptioner::new(
                routed_attempts::RoutedAttemptWalker::new(
                    Arc::clone(&router_handle),
                    Arc::clone(&router_breakers),
                    Arc::clone(&router_triggers),
                    Arc::clone(&router_pools),
                )
                .with_reporter(routing_event_reporter.clone()),
                vision::aifarm_vision_captioner_config_from_app_config(config),
                vision_data_urls.clone(),
            ),
        )
        .with_model_name(config.vision.model.clone())
        .with_history_store(history_store.clone()),
        vision_data_urls.clone(),
        dialog_context::dialog_vision_direct_image_limit(Some(config.vision.direct_image_limit)),
    ));
    let rates_fetcher = Arc::new(
        rates::MarketRatesClient::from_config(&config.market_rates)
            .context("failed to initialize market rates provider")?,
    );
    let rates_tool_dispatcher =
        Arc::new(rates::RatesToolRichEffects::new(Arc::clone(&rich_sender)));
    let history_summarizer = Some(Arc::new(
        history_summary::routed_history_summary_service_from_app_config(
            config,
            Arc::new(history_store.clone()),
            routed_attempts::RoutedAttemptWalker::new(
                Arc::clone(&router_handle),
                Arc::clone(&router_breakers),
                Arc::clone(&router_triggers),
                Arc::clone(&router_pools),
            )
            .with_reporter(routing_event_reporter.clone()),
        ),
    ) as Arc<dyn dialog_tools::ChatHistorySummarizer>);
    readiness_checks.push(ReadinessCheck::ok(
        "history_summary",
        "Chat history summary dialog tool wired to Postgres history store and routed provider",
    ));
    let youtube_summarizer = match youtube::RuntimeYouTubeSummarizer::from_app_config(config) {
        Ok(Some(summarizer)) => {
            let provider = summarizer.provider_label();
            readiness_checks.push(ReadinessCheck::ok(
                "youtube_summary",
                format!("YouTube transcript fetcher and {provider} summary tool wired"),
            ));
            Some(Arc::new(summarizer) as Arc<dyn dialog_tools::YouTubeSummarizer>)
        }
        Ok(None) => {
            readiness_checks.push(ReadinessCheck::skipped(
                "youtube_summary",
                "YouTube summary dialog tool unavailable: Google AI key is not configured",
            ));
            None
        }
        Err(error) => {
            tracing::warn!(%error, "YouTube summary dialog tool unavailable");
            readiness_checks.push(ReadinessCheck::skipped(
                "youtube_summary",
                format!("YouTube summary dialog tool unavailable: {error}"),
            ));
            None
        }
    };
    let serper_client = match serper::SerperClient::from_app_config(config) {
        Ok(Some(client)) => {
            readiness_checks.push(ReadinessCheck::ok(
                "serper",
                "Serper web_search/crawl_url dialog tools wired as the primary search provider",
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
    let mut app_dialog_toolbox = dialog_tools::AppDialogToolbox::new(
        Some(Arc::clone(&rates_fetcher)),
        Some(rates_tool_dispatcher),
        Some(dialog_translator),
    )
    .with_queue_status_provider(dialog_tool_adapter.clone())
    .with_drawing_canceller(dialog_tool_adapter.clone())
    .with_image_scheduler(dialog_tool_adapter.clone())
    .with_song_scheduler(dialog_tool_adapter.clone())
    .with_vision_describer(dialog_tool_vision);
    let dialog_history_searcher: Arc<dyn agent_runtime::HistorySearcher> = Arc::new(
        agent_runtime::PostgresHistorySearch::new(history_store.clone()),
    );
    app_dialog_toolbox = app_dialog_toolbox.with_history_searcher(dialog_history_searcher);
    if let Some(history_summarizer) = history_summarizer {
        app_dialog_toolbox = app_dialog_toolbox.with_history_summarizer(history_summarizer);
    }
    if let Some(youtube_summarizer) = youtube_summarizer {
        app_dialog_toolbox = app_dialog_toolbox.with_youtube_summarizer(youtube_summarizer);
    }
    if let Some(serper) = serper_client.as_ref() {
        let web_searcher: Arc<dyn dialog_tools::WebSearchProvider> = serper.clone();
        let url_crawler: Arc<dyn dialog_tools::UrlCrawler> = serper.clone();
        app_dialog_toolbox = app_dialog_toolbox
            .with_web_searcher(web_searcher)
            .with_url_crawler(url_crawler);
    }
    let dialog_toolbox: Arc<dyn openplotva_dialog::DialogToolbox> = Arc::new(app_dialog_toolbox);
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
            max_iterations: config.llm.dialog.session_max_iterations,
            max_messages: config.llm.dialog.session_max_messages,
            tool_extension_secs: config.llm.dialog.session_tool_extension_secs,
            hard_cap_secs: config.llm.dialog.session_hard_cap_secs,
            max_draws: config.llm.dialog.session_max_draws,
            max_songs: config.llm.dialog.session_max_songs,
        });
    let embedding_attempt_walker = routed_attempts::RoutedAttemptWalker::new(
        Arc::clone(&router_handle),
        Arc::clone(&router_breakers),
        Arc::clone(&router_triggers),
        Arc::clone(&router_pools),
    )
    .with_reporter(routing_event_reporter.clone());
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
        let poller_stop = stop.subscribe();
        let poller_interval = Duration::from_secs(
            config
                .persistent_queue
                .dialog_aifarm_fallback_poll_interval_seconds
                .max(1) as u64,
        );
        workers.handles.push(tokio::spawn(async move {
            dialog_jobs::run_router_trigger_poller(
                poller_handle,
                poller_triggers,
                poller_breakers,
                poller_queue,
                poller_interval,
                wait_for_runtime_stop(poller_stop),
            )
            .await;
        }));
    }
    let genkit_fallback = dialog_runtime::genkit_dialog_provider_from_app_config(config);
    let mut dialog_provider_for_updates: Option<openplotva_llm::ChatProviderHandle> = None;
    match Ok::<_, dialog_runtime::DialogProviderBuildError>(dialog_runtime::router_dialog_provider(
        config,
        Arc::clone(&dialog_toolbox),
        Arc::clone(&router_handle),
        Arc::clone(&router_breakers),
        Arc::clone(&router_triggers),
        Arc::clone(&router_pools),
        genkit_fallback,
        Some(routing_event_reporter.clone()),
    )) {
        Ok(mut dialog_provider) => {
            // One shared check-event recorder serves both the REAL and the
            // SAFE (virtual-dialog) WhiteCircle wraps.
            let white_circle_recorder: Option<
                Arc<dyn openplotva_llm::whitecircle::WhiteCircleCheckEventRecorder>,
            > = if dialog_runtime::white_circle_effective_enabled(config) {
                let (recorder, recorder_worker) =
                    runtime_safety::PostgresWhiteCircleCheckEventRecorder::spawn(
                        service_clients.postgres.clone(),
                        stop.subscribe(),
                    );
                workers.handles.push(recorder_worker);
                Some(Arc::new(recorder))
            } else {
                None
            };
            if let Some(recorder) = white_circle_recorder.clone() {
                dialog_provider =
                    wrap_dialog_provider_with_white_circle(dialog_provider, config, recorder);
            }
            // Trace rows are now emitted at the low-level model clients via the registered
            // RuntimeLlmObserver; the dialog provider is used directly (no tracing wrap).
            let dialog_provider: openplotva_llm::ChatProviderHandle = dialog_provider;
            dialog_provider_for_updates = Some(Arc::clone(&dialog_provider));
            let dialog_effects =
                dialog_jobs::DialogDispatcherEffects::new(Arc::clone(&dispatcher_queue));
            let dialog_terminal_signal = dialog_turn::DispatcherTerminalUserSignal::new(
                telegram.clone(),
                Arc::clone(&dispatcher_queue),
            );
            let mut dialog_materializer = dialog_jobs::PostgresDialogInputMaterializer::new(
                chat_settings_store.clone(),
                store.clone(),
                history_store.clone(),
                dialog_jobs::DialogBotIdentity::new(
                    bot_identity.id,
                    bot_identity.first_name.clone(),
                ),
            );
            if dialog_memory_context_enabled(config) {
                dialog_materializer = dialog_materializer.with_memory_store(memory_store.clone());
                if let Some(embedder) = memory_query_embedder.clone() {
                    dialog_materializer = dialog_materializer
                        .with_memory_embedder(embedder, config.memory.embedding_dim);
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
                dialog_materializer.with_vision_materializer(dialog_context_vision);
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
                ),
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
            let dialog_terminal_signal_max_age_secs =
                config.llm.dialog.terminal_signal_max_age_secs;
            // The dialog worker count derives from the capacity pools of the
            // dialog workflow's models; the semaphores in the walker are the
            // real limit, so extra workers just wait for a slot. The env knob
            // is only the fallback when no dialog route exists.
            let dialog_worker_fallback =
                u32::try_from(config.persistent_queue.dialog_aifarm_workers.max(0)).unwrap_or(0);
            let dialog_workers_cap =
                u32::try_from(config.persistent_queue.dialog_workers_cap.max(1)).unwrap_or(1);
            let dialog_unpooled_share =
                u32::try_from(config.persistent_queue.dialog_unpooled_share.max(0)).unwrap_or(0);
            let initial_dialog_workers = openplotva_llm::router::derived_worker_count(
                &router_handle.snapshot(),
                "dialog",
                dialog_unpooled_share,
                dialog_workers_cap,
            )
            .unwrap_or(dialog_worker_fallback);
            let (dialog_scale_tx, dialog_scale_rx) =
                tokio::sync::watch::channel(initial_dialog_workers);
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
                let stop_rx = stop.subscribe();
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
                                },
                                stop_future,
                            )
                            .await;

                        tracing::info!(?report, index, "dialog taskman worker stopped");
                    })
                }
            };
            let dialog_supervisor = dialog_workers::spawn_dialog_worker_supervisor(
                Arc::new(dialog_worker_spawner),
                dialog_scale_rx,
                Arc::clone(&dialog_worker_gauge),
                stop.subscribe(),
            );
            workers.handles.push(dialog_supervisor);
            workers.dialog_worker_gauge = Some(Arc::clone(&dialog_worker_gauge));
            workers.router_runtime = Some(Arc::new(model_routing::RouterRuntime {
                handle: Arc::clone(&router_handle),
                pools: Arc::clone(&router_pools),
                dialog_scale: Some(dialog_scale_tx),
                dialog_unpooled_share,
                dialog_worker_cap: dialog_workers_cap,
            }));
            tracing::info!(
                initial_dialog_workers,
                cap = dialog_workers_cap,
                fallback = dialog_worker_fallback,
                "dialog worker scale derived from capacity pools"
            );
            readiness_checks.push(ReadinessCheck::ok(
                "dialog_jobs",
                format!(
                    "Started dialog worker supervisor with {initial_dialog_workers} pool-derived workers (cap {dialog_workers_cap})"
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
        }
        Err(error) => {
            tracing::warn!(%error, "dialog provider unavailable for dialog taskman worker");
            readiness_checks.push(ReadinessCheck::skipped(
                "dialog_jobs",
                format!("dialog provider unavailable: {error}"),
            ));
            readiness_checks.push(ReadinessCheck::skipped(
                "runtime_virtual_dialogs",
                format!("dialog provider unavailable: {error}"),
            ));
        }
    }

    let media_prompt_optimizer = media::MediaPromptOptimizerService::new(Some(Arc::new(
        media::RoutedMediaPromptOptimizer::new(
            routed_attempts::RoutedAttemptWalker::new(
                Arc::clone(&router_handle),
                Arc::clone(&router_breakers),
                Arc::clone(&router_triggers),
                Arc::clone(&router_pools),
            )
            .with_reporter(routing_event_reporter.clone()),
            config,
        ),
    )
        as media::AppMediaPromptOptimizer));
    let media_max_llm_job_attempts = config.persistent_queue.llm_job_max_attempts;

    // Drawing-prompt agent: refine the draw prompt with the user's memory and chat
    // history before generation. Web search is intentionally stubbed until the search
    // pipeline is reworked. Built once and wraps both the VIP and regular image
    // generators; when disabled or the reasoner is missing it is a transparent
    // pass-through that leaves the single-pass optimizer in charge.
    let image_agent_settings = agent_runtime::ImageAgentSettings::from_app_config(
        config,
        agent_runtime::IMAGE_SYSTEM_PROMPT.to_owned(),
    );
    let (image_agent_reasoner, image_agent_tools) = if image_agent_settings.enabled {
        let registry = agent_runtime::build_routed_agent_provider_registry(
            config,
            routed_attempts::RoutedAttemptWalker::new(
                Arc::clone(&router_handle),
                Arc::clone(&router_breakers),
                Arc::clone(&router_triggers),
                Arc::clone(&router_pools),
            )
            .with_reporter(routing_event_reporter.clone()),
        );
        let reasoner = registry.get(&image_agent_settings.reasoner_provider);
        let history: Arc<dyn agent_runtime::HistorySearcher> = Arc::new(
            agent_runtime::PostgresHistorySearch::new(history_store.clone()),
        );
        let memory: Arc<dyn agent_runtime::MemorySearcher> = Arc::new(
            agent_runtime::PostgresMemorySearch::new(memory_store.clone()),
        );
        let tools: Arc<dyn openplotva_agent::AgentTools> = Arc::new(
            agent_runtime::AppAgentTools::new(
                agent_runtime::unavailable_web_search(),
                agent_runtime::unavailable_url_crawler(),
            )
            .with_history_searcher(history)
            .with_memory_searcher(memory),
        );
        (reasoner, Some(tools))
    } else {
        (None, None)
    };
    if image_agent_settings.enabled && image_agent_reasoner.is_some() {
        readiness_checks.push(ReadinessCheck::ok(
            "image_agent",
            "Drawing-prompt agent active (memory + chat history; web search stubbed) wrapping VIP and regular generators",
        ));
    } else if image_agent_settings.enabled {
        readiness_checks.push(ReadinessCheck::skipped(
            "image_agent",
            "Drawing-prompt agent enabled but the reasoner provider is missing from the registry",
        ));
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "image_agent",
            "LLM_AGENTIC_IMAGE_ENABLED=false",
        ));
    }
    let vip_image_queue = Arc::clone(&task_queue_for_updates);
    let image_attempt_walker = routed_attempts::RoutedAttemptWalker::new(
        Arc::clone(&router_handle),
        Arc::clone(&router_breakers),
        Arc::clone(&router_triggers),
        Arc::clone(&router_pools),
    )
    .with_reporter(routing_event_reporter.clone());
    let routed_vip_flux_image_generator = image_jobs::RoutedImageGenerator::new(
        image_attempt_walker.clone(),
        image_jobs::aifarm_draw_api_config_from_app_config(config),
    )
    .with_workflow_key(image_jobs::IMAGE_GENERATION_FLUX_WORKFLOW_KEY);
    let routed_vip_boogu_image_generator = image_jobs::RoutedImageGenerator::new(
        image_attempt_walker.clone(),
        image_jobs::aifarm_draw_api_config_from_app_config(config),
    )
    .with_workflow_key(image_jobs::IMAGE_GENERATION_BOOGU_TURBO_WORKFLOW_KEY);
    let vip_image_generator = image_jobs::OptimizingImageGenerator::new(
        image_jobs::ParallelImageGenerator::new(
            routed_vip_flux_image_generator,
            routed_vip_boogu_image_generator,
        ),
        media_prompt_optimizer.clone(),
    );
    let vip_image_generator = agent_runtime::ImageAgentImageGenerator::new(
        vip_image_generator,
        image_agent_reasoner.clone(),
        image_agent_tools.clone(),
        image_agent_settings.clone(),
    );
    let draw_chat_counter: Arc<dyn image_jobs::ChatMessageCounter> =
        Arc::new(PostgresHistoryStore::new(service_clients.postgres.clone()));
    let mut vip_image_effects =
        image_jobs::TelegramImageJobEffects::new(telegram.clone(), Arc::clone(&rich_sender))
            .with_chat_counter(Arc::clone(&draw_chat_counter));
    {
        let reactions = &generation_reactions;
        vip_image_effects = vip_image_effects.with_reaction_ux(Arc::clone(reactions));
    }
    let vip_image_stop = stop.subscribe();
    let vip_image_worker = tokio::spawn(async move {
        let report = image_jobs::run_image_gen_worker_every_until_with_max_attempts(
            vip_image_queue.as_ref(),
            &vip_image_generator,
            &vip_image_effects,
            &image_jobs::IMAGE_VIP_JOB_WORKER_QUEUES,
            image_jobs::IMAGE_JOB_POLL_INTERVAL,
            media_max_llm_job_attempts,
            wait_for_runtime_stop(vip_image_stop),
        )
        .await;

        tracing::info!(?report, "VIP image generation taskman worker stopped");
    });
    workers.handles.push(vip_image_worker);

    let vip_image_edit_queue = Arc::clone(&task_queue_for_updates);
    let vip_image_edit_provider = image_jobs::OptimizingImageEditor::new(
        image_jobs::ParallelImageEditor::new(
            image_jobs::RoutedImageEditor::new(
                image_attempt_walker.clone(),
                vision_data_urls.clone(),
                image_jobs::aifarm_draw_api_config_from_app_config(config),
            )
            .with_workflow_key(image_jobs::IMAGE_EDIT_FLUX_WORKFLOW_KEY),
            image_jobs::RoutedImageEditor::new(
                image_attempt_walker.clone(),
                vision_data_urls.clone(),
                image_jobs::aifarm_draw_api_config_from_app_config(config),
            )
            .with_workflow_key(image_jobs::IMAGE_EDIT_BOOGU_TURBO_WORKFLOW_KEY),
        ),
        media_prompt_optimizer.clone(),
    );
    let mut vip_image_edit_effects =
        image_jobs::TelegramImageJobEffects::new(telegram.clone(), Arc::clone(&rich_sender))
            .with_chat_counter(Arc::clone(&draw_chat_counter));
    {
        let reactions = &generation_reactions;
        vip_image_edit_effects = vip_image_edit_effects.with_reaction_ux(Arc::clone(reactions));
    }
    let vip_image_edit_stop = stop.subscribe();
    let vip_image_edit_worker = tokio::spawn(async move {
        let report = image_jobs::run_image_edit_worker_every_until_with_max_attempts(
            vip_image_edit_queue.as_ref(),
            &vip_image_edit_provider,
            &vip_image_edit_effects,
            &image_jobs::IMAGE_VIP_JOB_WORKER_QUEUES,
            image_jobs::IMAGE_JOB_POLL_INTERVAL,
            media_max_llm_job_attempts,
            wait_for_runtime_stop(vip_image_edit_stop),
        )
        .await;

        tracing::info!(?report, "VIP image edit taskman worker stopped");
    });
    workers.handles.push(vip_image_edit_worker);

    let regular_image_queue = Arc::clone(&task_queue_for_updates);
    let regular_image_generator = image_jobs::OptimizingImageGenerator::new(
        image_jobs::RoutedImageGenerator::new(
            image_attempt_walker,
            image_jobs::aifarm_draw_api_config_from_app_config(config),
        ),
        media_prompt_optimizer,
    );
    let regular_image_generator = agent_runtime::ImageAgentImageGenerator::new(
        regular_image_generator,
        image_agent_reasoner,
        image_agent_tools,
        image_agent_settings,
    );
    let mut regular_image_effects =
        image_jobs::TelegramImageJobEffects::new(telegram.clone(), Arc::clone(&rich_sender))
            .with_chat_counter(Arc::clone(&draw_chat_counter));
    {
        let reactions = &generation_reactions;
        regular_image_effects = regular_image_effects.with_reaction_ux(Arc::clone(reactions));
    }
    let regular_image_stop = stop.subscribe();
    let regular_image_worker = tokio::spawn(async move {
        let report = image_jobs::run_image_gen_worker_every_until_with_max_attempts(
            regular_image_queue.as_ref(),
            &regular_image_generator,
            &regular_image_effects,
            &image_jobs::IMAGE_REGULAR_JOB_WORKER_QUEUES,
            image_jobs::IMAGE_JOB_POLL_INTERVAL,
            media_max_llm_job_attempts,
            wait_for_runtime_stop(regular_image_stop),
        )
        .await;

        tracing::info!(?report, "regular image generation taskman worker stopped");
    });
    workers.handles.push(regular_image_worker);
    readiness_checks.push(ReadinessCheck::ok(
        "image_jobs",
        "Image taskman workers started for VIP generation/edit and regular generation queues with draw-api providers and optional Boogu add-ons",
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
                routed_attempts::RoutedAttemptWalker::new(
                    Arc::clone(&router_handle),
                    Arc::clone(&router_breakers),
                    Arc::clone(&router_triggers),
                    Arc::clone(&router_pools),
                )
                .with_reporter(routing_event_reporter.clone()),
                config,
            ));
        let base_song_material: Arc<dyn music_jobs::SongMaterialProvider + Send + Sync> =
            Arc::new(music_jobs::AifarmSongMaterialProvider::new(
                music_song_prompt_generator,
                PostgresVirtualMessageStore::new(service_clients.postgres.clone()),
            ));
        let song_agent_settings = agent_runtime::SongAgentSettings::from_app_config(
            config,
            agent_runtime::SONG_SYSTEM_PROMPT.to_owned(),
        );
        let (song_agent_reasoner, song_agent_tools) = if song_agent_settings.enabled {
            let registry = agent_runtime::build_routed_agent_provider_registry(
                config,
                routed_attempts::RoutedAttemptWalker::new(
                    Arc::clone(&router_handle),
                    Arc::clone(&router_breakers),
                    Arc::clone(&router_triggers),
                    Arc::clone(&router_pools),
                )
                .with_reporter(routing_event_reporter.clone()),
            );
            let reasoner = registry.get(&song_agent_settings.reasoner_provider);
            let tools: Option<Arc<dyn openplotva_agent::AgentTools>> =
                serper_client.as_ref().map(|serper| {
                    let web: Arc<dyn dialog_tools::WebSearchProvider> = serper.clone();
                    let crawl: Arc<dyn dialog_tools::UrlCrawler> = serper.clone();
                    let history: Arc<dyn agent_runtime::HistorySearcher> = Arc::new(
                        agent_runtime::PostgresHistorySearch::new(history_store.clone()),
                    );
                    let memory: Arc<dyn agent_runtime::MemorySearcher> = Arc::new(
                        agent_runtime::PostgresMemorySearch::new(memory_store.clone()),
                    );
                    Arc::new(
                        agent_runtime::AppAgentTools::new(web, crawl)
                            .with_history_searcher(history)
                            .with_memory_searcher(memory),
                    ) as Arc<dyn openplotva_agent::AgentTools>
                });
            (reasoner, tools)
        } else {
            (None, None)
        };
        let music_material_provider = agent_runtime::SongAgentMaterialProvider::new(
            song_agent_reasoner,
            song_agent_settings,
            song_agent_tools,
            base_song_material,
        );
        let music_attempt_walker = routed_attempts::RoutedAttemptWalker::new(
            Arc::clone(&router_handle),
            Arc::clone(&router_breakers),
            Arc::clone(&router_triggers),
            Arc::clone(&router_pools),
        )
        .with_reporter(routing_event_reporter.clone());
        let music_generator = music_jobs::RoutedMusicGenerator::new(
            music_attempt_walker,
            music_jobs::acestep_config_from_app_config(config),
        );
        let mut music_effects = music_jobs::TelegramMusicJobEffects::new(
            Arc::clone(&permission_policy),
            PostgresTelegramFileStore::new(service_clients.postgres.clone()),
            telegram.clone(),
            Arc::clone(&rich_sender),
        );
        {
            let reactions = &generation_reactions;
            music_effects = music_effects.with_reaction_ux(Arc::clone(reactions));
        }
        let music_stop = stop.subscribe();
        let music_worker = tokio::spawn(async move {
            let report = music_jobs::run_music_worker_every_until_with_max_attempts(
                music_queue.as_ref(),
                &music_material_provider,
                &music_generator,
                &music_effects,
                music_jobs::MUSIC_JOB_POLL_INTERVAL,
                media_max_llm_job_attempts,
                wait_for_runtime_stop(music_stop),
            )
            .await;

            tracing::info!(?report, "music generation taskman worker stopped");
        });
        workers.handles.push(music_worker);
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
    let shared_task_queue_heartbeat_stop = stop.subscribe();
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
    workers.handles.push(shared_task_queue_heartbeat_worker);

    let ephemeral_cleanup_store = ephemeral_store.clone();
    let ephemeral_cleanup_telegram = telegram.clone();
    let ephemeral_cleanup_stop = stop.subscribe();
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
    workers.handles.push(ephemeral_cleanup_worker);

    let immediate_history = history_store.clone();
    let immediate_telegram = telegram.clone();
    let immediate_rich = rich_api.clone();
    let immediate_ephemeral = ephemeral_store.clone();
    let immediate_rate_limits = Arc::clone(&rate_limit_policy);
    let immediate_permissions = Arc::clone(&permission_policy);
    let immediate_queue = Arc::clone(&dispatcher_queue);
    let immediate_failure_ring = Arc::clone(&dispatch_failure_ring);
    let immediate_stop = stop.subscribe();
    let immediate_worker = tokio::spawn(async move {
        let outcome = immediate_queue
            .run_immediate_worker_until(wait_for_runtime_stop(immediate_stop), |item| {
                send_dispatcher_work_item(
                    immediate_history.clone(),
                    immediate_telegram.clone(),
                    immediate_rich.clone(),
                    immediate_ephemeral.clone(),
                    Arc::clone(&immediate_rate_limits),
                    Arc::clone(&immediate_permissions),
                    Some(Arc::clone(&immediate_failure_ring)),
                    item,
                )
            })
            .await;

        tracing::info!(?outcome, "outbound immediate dispatcher worker stopped");
    });

    if update_queue_backend != "list" {
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_update_producer",
            format!("UPDATE_QUEUE_BACKEND={update_queue_backend} is spike-only; production default remains list"),
        ));
    } else if !config.service_probe.produce_updates {
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_update_producer",
            "OPENPLOTVA_PRODUCE_UPDATES=false",
        ));
    } else if config.bot.webhook.enabled {
        if config.bot.webhook.url.is_empty() {
            tracing::warn!("BOT_WEBHOOK_URL is required when webhook updates are enabled");
            readiness_checks.push(ReadinessCheck::skipped(
                "telegram_update_producer",
                "BOT_WEBHOOK_URL is not set",
            ));
        } else {
            let (webhook_sender, webhook_source) =
                openplotva_telegram::webhook_update_channel(config.bot.webhook.update_buffer_size);
            let webhook_sender =
                webhook_sender.with_ingress_guard(Arc::clone(&update_ingress_guard));
            let webhook_setup = match webhook_setup_from_config(&config.bot.webhook) {
                Ok(setup) => Some(setup),
                Err(error) => {
                    tracing::warn!(%error, "failed to load Telegram webhook certificate");
                    readiness_checks.push(ReadinessCheck::skipped(
                        "telegram_update_producer",
                        "failed to load BOT_WEBHOOK_CERT_FILE",
                    ));
                    None
                }
            };
            if let Some(webhook_setup) = webhook_setup {
                let webhook_startup =
                    TelegramWebhookSetupClient::new(bot_key.to_owned(), telegram.clone());
                let update_queue = update_queue.clone();
                let update_stop = stop.subscribe();
                let update_producer_worker = tokio::spawn(async move {
                    let report = run_webhook_update_producer_after_set_webhook(
                        &webhook_startup,
                        &webhook_setup,
                        &webhook_source,
                        &update_queue,
                        wait_for_runtime_stop(update_stop),
                    )
                    .await;

                    if let Some(error) = report.set_webhook_error.as_deref() {
                        tracing::warn!(
                            %error,
                            "Telegram webhook update producer stopped before accepting updates"
                        );
                    } else {
                        tracing::info!(?report, "Telegram webhook update producer stopped");
                    }
                });
                workers.webhook_route = Some(TelegramWebhookRoute {
                    sender: webhook_sender,
                    secret_token: Arc::from(config.bot.webhook.secret_token.clone()),
                });
                workers.handles.push(update_producer_worker);
                readiness_checks.push(ReadinessCheck::ok(
                    "telegram_update_producer",
                    "Telegram webhook update producer worker started",
                ));
            }
        }
    } else {
        let update_startup = telegram.clone();
        let update_source = openplotva_telegram::LongPollUpdateSource::new(telegram.clone());
        let update_queue = update_queue.clone();
        let update_ingress_guard = Arc::clone(&update_ingress_guard);
        let update_stop = stop.subscribe();
        let update_producer_worker = tokio::spawn(async move {
            let report = run_long_poll_update_producer_with_ingress_guard_after_delete_webhook(
                &update_startup,
                &update_source,
                &update_queue,
                update_ingress_guard.as_ref(),
                wait_for_runtime_stop(update_stop),
            )
            .await;

            if let Some(error) = report.delete_webhook_error.as_deref() {
                tracing::warn!(%error, "Telegram long-poll update producer stopped before polling");
            } else {
                tracing::info!(?report, "Telegram long-poll update producer stopped");
            }
        });
        workers.handles.push(update_producer_worker);
        readiness_checks.push(ReadinessCheck::ok(
            "telegram_update_producer",
            "Telegram long-poll update producer worker started",
        ));
    }

    if update_queue_backend != "list" {
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_update_consumer",
            format!("UPDATE_QUEUE_BACKEND={update_queue_backend} is spike-only; production default remains list"),
        ));
    } else if config.service_probe.consume_updates {
        let dialog_debounce = Arc::new(dialog_debounce::InMemoryDialogDebounce::new());
        workers.dialog_debounce = Some(Arc::clone(&dialog_debounce));
        let dialog_scheduler = Arc::new(
            dialog_messages::TaskmanDialogMessageScheduler::new(
                Arc::clone(&task_queue_for_updates),
                Arc::clone(&dialog_debounce),
                dialog_debounce::GO_DIALOG_DEBOUNCE_INTERVAL,
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
        let mut direct_draw_api_config = image_jobs::aifarm_draw_api_config_from_app_config(config);
        direct_draw_api_config.timeout = Duration::from_secs(120);
        let direct_draw_api_effects = Arc::new(
            dialog_messages::DirectDrawApiRuntimeEffects::new(
                telegram.clone(),
                image_jobs::AifarmDrawApiImageGenerator::new(direct_draw_api_config),
            )
            .with_send_policies(rate_limit_policy.clone(), permission_policy.clone())
            .with_history_store(history_store_for_updates.clone(), bot_identity.id),
        );
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
        let text_reply_settings_gate =
            Arc::new(message_gate::TextReplySettingsGateUpdateHandler::new(
                Arc::new(chat_settings_store.clone()),
                inline,
            ));
        let reset_handler = Arc::new(reset::ResetCommandUpdateHandler::new(
            Arc::clone(&history_store_for_updates),
            Arc::clone(&dispatcher_queue_for_updates),
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
        let translate_handler = Arc::new(translate::TranslateCommandUpdateHandler::new(
            translate::TranslateBotIdentity {
                user: bot_user_from_get_me(&bot_identity),
            },
            Arc::new(MessageGateCheckedTranslatePermission),
            Arc::clone(&control_queue_for_updates),
            Arc::new(translate::TranslateDispatcherEffects::new(Arc::clone(
                &dispatcher_queue_for_updates,
            ))),
            delete_drawing_command,
        ));
        let rates_handler = Arc::new(rates::RatesCommandUpdateHandler::new(
            rates::RatesBotIdentity {
                user: bot_user_from_get_me(&bot_identity),
            },
            Arc::new(MessageGateCheckedRatesPermission),
            Some(Arc::clone(&rates_fetcher)),
            Arc::new(RuntimeRatesHeaderProvider),
            Arc::new(rates::RatesRichEffects::new(Arc::clone(&rich_sender))),
            translate_handler,
        ));
        let checkin_command = Arc::new(checkin::CheckinCommandUpdateHandler::new(
            Arc::clone(&control_queue_for_updates),
            checkin_game_store_for_updates,
            Arc::clone(&permission_policy),
            Arc::new(checkin::CheckinCommandDispatcherEffects::new(
                Arc::clone(&dispatcher_queue_for_updates),
                Arc::clone(&rich_sender),
            )),
            bot_identity.username.clone(),
            rates_handler,
        ));
        let post_service_blocked_gate =
            Arc::new(message_gate::PostServiceBlockedChatUpdateHandler::new(
                Arc::new(service_clients.redis.blocked_chat_store()),
                checkin_command,
            ));
        let settings_handler = Arc::new(settings::SettingsUpdateHandler::new(
            Arc::clone(&dispatcher_queue_for_updates),
            Arc::clone(&chat_members_for_updates),
            Arc::clone(&control_queue_for_updates),
            settings::SettingsUpdateHandlerConfig::new(
                bot_identity.username.clone(),
                bot_identity.id,
                config.server.url.clone(),
            ),
            post_service_blocked_gate,
        ));
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
        let admin_queue_handler = Arc::new(admin::AdminQueueCommandUpdateHandler::new(
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
        ));
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
                Arc::clone(&rich_sender),
            )),
            diagnostics_handler,
        ));
        let left_member = Arc::new(members::LeftChatMemberUpdateHandler::new(
            Arc::clone(&chat_members_for_updates),
            Arc::clone(&chat_communication),
            bot_identity.id,
            help_handler,
        ));
        let member_state = Arc::new(members::ChatMemberStateUpdateHandler::new(
            Arc::clone(&chat_members_for_updates),
            Arc::clone(&control_queue_for_updates),
            chat_communication,
            bot_identity.id,
            left_member,
        ));
        let activity = Arc::new(activity::MessageActivityUpdateHandler::new(
            Arc::clone(&message_activity_store),
            member_state,
        ));
        let edited_effects = Arc::new(
            edited::TaskmanEditedMessageEffects::new(Arc::clone(&task_queue_for_updates))
                .with_dialog_debounce(dialog_debounce),
        );
        let edited = Arc::new(edited::EditedMessageUpdateHandler::new(
            edited_effects,
            bot_user_from_get_me(&bot_identity),
            activity,
        ));
        let history_handler = Arc::new(updates::UpdateHandlerWithHistory::new(
            Arc::clone(&history_store_for_updates),
            edited,
            bot_identity.id,
        ));
        let handler = Arc::new(message_gate::MessageGateUpdateHandler::new(
            Arc::clone(&rate_limit_policy),
            permission_policy_for_updates,
            Arc::new(service_clients.redis.blocked_chat_store()),
            bot_identity.username.clone(),
            history_handler,
        ));
        let update_consumer_queue = Arc::new(update_queue.clone());
        let update_stage_tracker = Arc::new(updates_inspector.stage_tracker());
        let update_history_store = Arc::clone(&history_store_for_updates);
        let update_bot_id = bot_identity.id;
        let update_consumer_stop = stop.subscribe();
        let update_consumer_worker = tokio::spawn(async move {
            let report = updates::run_update_consumer_with_history_stage_tracker_until(
                update_consumer_queue,
                openplotva_updates::UpdateConsumerConfig::default(),
                store_for_updates,
                update_history_store,
                update_bot_id,
                handler,
                update_stage_tracker,
                wait_for_runtime_stop(update_consumer_stop),
            )
            .await;

            tracing::info!(?report, "Telegram decoded update consumer stopped");
        });
        workers.handles.push(update_consumer_worker);
        readiness_checks.push(ReadinessCheck::ok(
            "telegram_update_consumer",
            "Telegram decoded update consumer worker started for ported fetcher slices",
        ));
    } else {
        readiness_checks.push(ReadinessCheck::skipped(
            "telegram_update_consumer",
            "OPENPLOTVA_CONSUME_UPDATES=false",
        ));
    }

    let regular_history = history_store;
    let regular_telegram = telegram;
    let regular_rich = rich_api;
    let regular_ephemeral = ephemeral_store;
    let regular_rate_limits = Arc::clone(&rate_limit_policy);
    let regular_permissions = Arc::clone(&permission_policy);
    let regular_queue = Arc::clone(&dispatcher_queue);
    let regular_limiters = Arc::clone(&dispatcher_limiters);
    let regular_failure_ring = Arc::clone(&dispatch_failure_ring);
    let regular_stop = stop.subscribe();
    let regular_worker = tokio::spawn(async move {
        let outcome = regular_queue
            .run_regular_worker_until(
                &regular_limiters,
                wait_for_runtime_stop(regular_stop),
                |item| {
                    send_dispatcher_work_item(
                        regular_history.clone(),
                        regular_telegram.clone(),
                        regular_rich.clone(),
                        regular_ephemeral.clone(),
                        Arc::clone(&regular_rate_limits),
                        Arc::clone(&regular_permissions),
                        Some(Arc::clone(&regular_failure_ring)),
                        item,
                    )
                },
            )
            .await;

        tracing::info!(?outcome, "outbound regular dispatcher worker stopped");
    });

    let cleanup_limiters = Arc::clone(&dispatcher_limiters);
    let cleanup_stop = stop.subscribe();
    let cleanup_worker = tokio::spawn(async move {
        let outcome = openplotva_telegram::run_limiter_cleanup_until(
            &cleanup_limiters,
            openplotva_telegram::DispatcherRuntimeConfig::default(),
            wait_for_runtime_stop(cleanup_stop),
        )
        .await;

        tracing::info!(
            ?outcome,
            "outbound dispatcher limiter cleanup worker stopped"
        );
    });

    readiness_checks.push(ReadinessCheck::ok(
        "outbound_dispatcher",
        "Telegram outbound dispatcher workers started",
    ));
    workers.handles.push(immediate_worker);
    workers.handles.push(regular_worker);
    workers.handles.push(cleanup_worker);
    workers.dispatcher = Some(DispatcherRuntime {
        queue: dispatcher_queue,
        persistence: dispatcher_persistence,
    });

    Ok(workers)
}

async fn shutdown_runtime_workers(workers: RuntimeWorkers) {
    let RuntimeWorkers {
        handles,
        stop,
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
        routing_event_buffer: _,
        routing_event_reporter: _,
        runtime_api_tls_public_key_pin: _,
        router_handle: _,
        router_breakers: _,
        router_triggers: _,
        router_pools: _,
        router_runtime: _,
        dialog_worker_gauge: _,
    } = workers;

    if let Some(stop) = stop {
        let _ = stop.send(true);
    }

    if let Some(dispatcher) = dispatcher {
        persist_dispatcher_queue_on_shutdown(dispatcher).await;
    }

    if let Some(dialog_debounce) = dialog_debounce {
        let stopped = dialog_debounce.stop_all();
        if stopped > 0 {
            tracing::info!(stopped, "stopped pending dialog debounce timers");
        }
    }

    for worker in handles {
        await_runtime_worker_shutdown(worker).await;
    }

    if let Some(shared_task_queue) = shared_task_queue {
        persist_shared_task_queue_on_shutdown(shared_task_queue).await;
    }

    if delete_webhook_on_shutdown {
        match telegram {
            Some(telegram) => match delete_webhook_on_shutdown_if_enabled(true, &telegram).await {
                WebhookShutdownCleanupReport::Deleted => {
                    tracing::debug!("Telegram webhook configuration deleted during shutdown");
                }
                WebhookShutdownCleanupReport::Failed { error } => {
                    tracing::warn!(%error, "failed to delete Telegram webhook during shutdown");
                }
                WebhookShutdownCleanupReport::TimedOut => {
                    tracing::warn!("timed out deleting Telegram webhook during shutdown");
                }
                WebhookShutdownCleanupReport::SkippedDisabled => {}
            },
            None => {
                tracing::warn!("Telegram webhook cleanup was requested without a Telegram client");
            }
        }
    }
}

async fn persist_dispatcher_queue_on_shutdown(dispatcher: DispatcherRuntime) {
    let save_result = timeout(
        openplotva_telegram::DEFAULT_DISPATCHER_SHUTDOWN_TIMEOUT,
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

async fn await_runtime_worker_shutdown(mut worker: JoinHandle<()>) {
    match timeout(
        openplotva_telegram::DEFAULT_DISPATCHER_SHUTDOWN_TIMEOUT,
        &mut worker,
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) if error.is_cancelled() => {}
        Ok(Err(error)) => tracing::warn!(%error, "runtime worker stopped with an error"),
        Err(_) => {
            worker.abort();
            match worker.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => tracing::warn!(%error, "runtime worker abort failed"),
            }
        }
    }
}

async fn persist_shared_task_queue_on_shutdown(queue: task_queue::SharedTaskQueueRuntime) {
    let save_result = timeout(task_queue::SHARED_TASK_QUEUE_SHUTDOWN_TIMEOUT, async {
        queue.flush_dirty().await
    })
    .await;

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

#[allow(clippy::too_many_arguments)]
async fn send_dispatcher_work_item(
    history: PostgresHistoryStore,
    telegram: openplotva_telegram::TelegramClient,
    rich: openplotva_telegram::RichApiClient,
    ephemeral: RedisEphemeralMessageStore,
    rate_limits: Arc<rate_limits::ChatRateLimitPolicy<RedisRateLimitStore>>,
    permissions: Arc<permissions::ChatPermissionPolicy<PostgresChatSettingsStore>>,
    failure_ring: Option<Arc<DispatchFailureRing>>,
    item: openplotva_telegram::DispatcherWorkItem,
) -> openplotva_telegram::DispatcherSendStatus {
    send_dispatcher_work_item_with_transport_and_history(
        history,
        ephemeral,
        rate_limits,
        permissions,
        failure_ring,
        item,
        |method| {
            let telegram = telegram.clone();
            let rich = rich.clone();
            async move {
                openplotva_telegram::execute_telegram_method_with_rich(&telegram, &rich, method)
                    .await
            }
        },
    )
    .await
}

#[cfg(test)]
async fn send_dispatcher_work_item_with_transport<R, P, SendFn, SendFuture>(
    rate_limits: Arc<rate_limits::ChatRateLimitPolicy<R>>,
    permissions: Arc<permissions::ChatPermissionPolicy<P>>,
    item: openplotva_telegram::DispatcherWorkItem,
    send: SendFn,
) -> openplotva_telegram::DispatcherSendStatus
where
    R: rate_limits::RateLimitStore + Send + Sync,
    P: permissions::ChatPermissionStore + Send + Sync,
    SendFn: Fn(openplotva_telegram::TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<
        Output = Result<
            openplotva_telegram::TelegramOutboundResponse,
            openplotva_telegram::TelegramOutboundExecuteError,
        >,
    >,
{
    send_dispatcher_work_item_with_transport_and_history(
        virtual_messages::NoopEditHistorySink,
        virtual_messages::NoopEphemeralMessageTracker,
        rate_limits,
        permissions,
        None,
        item,
        send,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn send_dispatcher_work_item_with_transport_and_history<H, E, R, P, SendFn, SendFuture>(
    history: H,
    ephemeral: E,
    rate_limits: Arc<rate_limits::ChatRateLimitPolicy<R>>,
    permissions: Arc<permissions::ChatPermissionPolicy<P>>,
    failure_ring: Option<Arc<DispatchFailureRing>>,
    item: openplotva_telegram::DispatcherWorkItem,
    send: SendFn,
) -> openplotva_telegram::DispatcherSendStatus
where
    H: virtual_messages::EditHistorySink,
    E: virtual_messages::EphemeralMessageTracker + Sync,
    R: rate_limits::RateLimitStore + Send + Sync,
    P: permissions::ChatPermissionStore + Send + Sync,
    SendFn: Fn(openplotva_telegram::TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<
        Output = Result<
            openplotva_telegram::TelegramOutboundResponse,
            openplotva_telegram::TelegramOutboundExecuteError,
        >,
    >,
{
    let chat_id = item.metadata().chat_id;
    let bypass_chat_restrictions = item.bypasses_chat_restrictions();
    let virtual_id = item.metadata().virtual_id.clone();
    let protected = item.metadata().protected;
    let reply_to_message_id =
        reply_message_id_from_fingerprint_key(&item.metadata().fingerprint_key);
    let method_kind_label = item
        .method_kind()
        .map(|kind| format!("{kind:?}"))
        .unwrap_or_else(|| "none".to_owned());
    let record_failure = |error: String, class: &'static str| {
        if let Some(ring) = failure_ring.as_deref() {
            ring.record(DispatchFailureRecord {
                at: OffsetDateTime::now_utc(),
                virtual_id: virtual_id.clone(),
                chat_id,
                method_kind: method_kind_label.clone(),
                error,
                class,
                protected,
                reply_to_message_id,
            });
        }
    };
    if chat_id != 0 {
        let check = rate_limits
            .is_rate_limited_at(chat_id, OffsetDateTime::now_utc())
            .await;
        if let Some(load_error) = check.load_error.as_deref() {
            tracing::debug!(
                chat_id,
                %load_error,
                "failed to load persisted Telegram rate-limit state"
            );
        }
        if check.rate_limited {
            tracing::debug!(chat_id, "skipping Telegram send for rate-limited chat");
            record_failure(
                "skipped send for rate-limited chat".to_owned(),
                DISPATCH_FAILURE_CLASS_CHAT_RATE_LIMITED,
            );
            return openplotva_telegram::DispatcherSendStatus::Failed;
        }
    }
    if chat_id != 0
        && !bypass_chat_restrictions
        && let Some(method_kind) = item.method_kind()
    {
        for action in permissions::dispatcher_required_actions(method_kind) {
            let report = permissions
                .can_perform_action_at(chat_id, None, action, OffsetDateTime::now_utc())
                .await;
            if let Some(load_error) = report.load_error.as_deref() {
                tracing::debug!(
                    chat_id,
                    action,
                    %load_error,
                    "failed to load Telegram permission state"
                );
            }
            if !report.allowed {
                tracing::debug!(
                    chat_id,
                    action,
                    "skipping Telegram send for chat permission settings"
                );
                record_failure(
                    format!("skipped send: chat permission settings deny {action}"),
                    openplotva_telegram::OutboundSendErrorClass::TerminalPermission.as_str(),
                );
                return openplotva_telegram::DispatcherSendStatus::Failed;
            }
        }
    }

    let retry_virtual_id = virtual_id.clone();
    let report = virtual_messages::send_work_item_with_history_and_ephemeral(
        &history,
        &ephemeral,
        item,
        OffsetDateTime::now_utc(),
        |method| {
            let rate_limits = Arc::clone(&rate_limits);
            let permissions = Arc::clone(&permissions);
            async move {
                let method_kind = method.kind();
                match openplotva_telegram::send_outbound_method_with_bounded_retry(
                    &send,
                    method,
                    &retry_virtual_id,
                    chat_id,
                )
                .await
                {
                    Ok(response) => Ok(response),
                    Err(error) => {
                        if matches!(
                            method_kind,
                            openplotva_telegram::TelegramOutboundMethodKind::SendMessage
                                | openplotva_telegram::TelegramOutboundMethodKind::SendRichMessage
                        ) && error.is_reply_missing()
                        {
                            tracing::warn!(
                                chat_id,
                                "reply target missing for Telegram send"
                            );
                            return Ok(openplotva_telegram::TelegramOutboundResponse::Boolean(true));
                        }
                        if let Some(retry_after) =
                            rate_limits::telegram_retry_after_from_outbound_error(&error)
                        {
                            let report = rate_limits
                                .set_rate_limit_at(chat_id, retry_after, OffsetDateTime::now_utc())
                                .await;
                            if let Some(save_error) = report.save_error.as_deref() {
                                tracing::warn!(
                                    chat_id,
                                    retry_after_seconds = retry_after.as_secs(),
                                    %save_error,
                                    "failed to persist Telegram rate-limit state"
                                );
                            }
                        }
                        if chat_id != 0
                            && error.is_permission_error()
                        {
                            let report = permissions
                                .record_send_permission_error(chat_id, method_kind)
                                .await;
                            if let Some(load_error) = report.load_error.as_deref() {
                                tracing::warn!(
                                    chat_id,
                                    method = ?method_kind,
                                    %load_error,
                                    "failed to load chat permission state after Telegram permission error"
                                );
                            }
                            if let Some(save_error) = report.save_error.as_deref() {
                                tracing::warn!(
                                    chat_id,
                                    method = ?method_kind,
                                    %save_error,
                                    "failed to persist chat permission state after Telegram permission error"
                                );
                            }
                        }
                        Err(error)
                    }
                }
            }
        },
    )
    .await;
    if report.ephemeral_track_error.is_some() {
        tracing::warn!(
            virtual_id = report.virtual_id,
            real_message_id = ?report.sent_message_id,
            ephemeral_track_error = ?report.ephemeral_track_error,
            "failed to track outbound ephemeral message"
        );
    }
    if matches!(
        report.status,
        openplotva_telegram::DispatcherSendStatus::Failed
    ) {
        let error_class = report
            .error_class
            .unwrap_or(DISPATCH_FAILURE_CLASS_MISSING_METHOD);
        let send_error = report.send_error.clone().unwrap_or_default();
        tracing::warn!(
            virtual_id = report.virtual_id,
            chat_id,
            error_class,
            send_error,
            method_kind = method_kind_label,
            "outbound dispatcher send failed"
        );
        record_failure(send_error, error_class);
    }
    report.status
}

fn go_dispatcher_config() -> openplotva_telegram::DispatcherConfig {
    openplotva_telegram::DispatcherConfig {
        max_queue_size: GO_DISPATCHER_MAX_QUEUE_SIZE,
        dedupe_config: openplotva_telegram::DebouncerConfig {
            enabled: true,
            default_window: GO_DISPATCHER_DEBOUNCE_WINDOW,
            max_cache_size: GO_DISPATCHER_DEBOUNCE_CACHE_SIZE,
            per_chat_settings: Default::default(),
        },
        ..openplotva_telegram::DispatcherConfig::default()
    }
}

#[derive(Clone, Debug)]
enum RuntimeTranslator {
    Ready(Box<translate::RuntimeT8Translator>),
    Unavailable(String),
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
enum RuntimeTranslatorError {
    #[error("{0}")]
    Provider(#[from] translate::T8TranslatorError),
    #[error("{0}")]
    Unavailable(String),
}

impl translate::TextTranslator for RuntimeTranslator {
    type Error = RuntimeTranslatorError;

    fn translate_text<'a>(
        &'a self,
        text: &'a str,
        target_lang: &'a str,
    ) -> translate::TranslateProviderFuture<'a, Self::Error> {
        Box::pin(async move {
            match self {
                Self::Ready(translator) => translator
                    .translate_text(text, target_lang)
                    .await
                    .map_err(RuntimeTranslatorError::Provider),
                Self::Unavailable(reason) => {
                    Err(RuntimeTranslatorError::Unavailable(reason.clone()))
                }
            }
        })
    }
}

fn bot_user_from_get_me(bot: &carapax::types::Bot) -> carapax::types::User {
    let mut user = carapax::types::User::new(bot.id, bot.first_name.clone(), true);
    user.username = Some(bot.username.clone().into());
    user.last_name = bot.last_name.clone();
    user
}

#[derive(Clone, Copy, Debug)]
struct MessageGateCheckedTranslatePermission;

impl translate::TranslateSendPermission for MessageGateCheckedTranslatePermission {
    fn can_send_translate_text(&self, _chat: &carapax::types::Chat) -> bool {
        true
    }
}

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
        user_full_name.to_owned()
    }
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

    fn handle_update<'a>(
        &'a self,
        update: carapax::types::Update,
    ) -> updates::UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
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
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        error::Error,
        fmt,
        sync::{Arc, Mutex, MutexGuard},
        time::Duration,
    };

    mod routing_admin_inputs {
        use serde_json::json;

        use crate::{model_input_from_json, provider_input_from_json, routing_param_descriptors};

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

    use axum::{
        body::{Bytes, to_bytes},
        extract::Extension,
        http::{HeaderMap, Method, StatusCode, header},
    };
    use carapax::types::{InputFile, SendMessage, SendPhoto, Update as TelegramUpdate};
    use openplotva_core::{ChatSettings, ChatSettingsUpdate};
    use openplotva_telegram::{
        DispatcherConfig, DispatcherMessage, DispatcherQueue, DispatcherSendStatus,
        DispatcherWorkItem, MessageFingerprint, TelegramMessage, TelegramOutboundExecuteError,
        TelegramOutboundMethod, TelegramOutboundResponse,
    };
    use openplotva_updates::{
        UpdateProducerQueue, UpdateProducerQueueFuture, UpdateProducerSource,
        UpdateProducerSourceFuture,
    };
    use serde_json::json;
    use time::{OffsetDateTime, format_description::well_known::Rfc3339};

    use super::{
        AdminMemoryOverride, DispatchFailureRing, GO_ADMIN_API_ROUTE_PATTERNS,
        GO_DISPATCHER_DEBOUNCE_CACHE_SIZE, GO_DISPATCHER_DEBOUNCE_WINDOW,
        GO_DISPATCHER_MAX_QUEUE_SIZE, RuntimeUnhandledUpdateHandler, SettingsInitDataDecision,
        WebhookShutdownCleanupReport, admin_auth_check, admin_auth_date_is_fresh,
        admin_auth_query_values, admin_auth_response, admin_auth_user_state, admin_bootstrap,
        admin_chat_get_response, admin_chats_search_by_member_response, admin_i64_from_json,
        admin_llm_analytics_summary_json, admin_llm_requests_clear_response,
        admin_llm_requests_filter, admin_llm_requests_response, admin_loglevel_response,
        admin_memory_cards_response, admin_memory_resolve_override, admin_memory_restart_override,
        admin_memory_restart_response, admin_memory_runs_response, admin_non_empty_string,
        admin_redis_prefix_groups_from_keys, admin_safety_check_json, admin_safety_checks_filter,
        admin_session_is_authorized, admin_shield_category_matched, admin_shield_document_input,
        admin_shield_document_json, admin_shield_documents_response,
        admin_shield_embeddings_rebuild_response, admin_shield_test_json,
        admin_shield_test_response, admin_state_response, admin_static_asset_requires_auth,
        admin_taskman_job_cancel_response, admin_taskman_job_response,
        admin_taskman_job_restart_response, admin_taskman_jobs_clear_response,
        admin_taskman_jobs_filter, admin_taskman_jobs_response, admin_user_grant_vip_response,
        admin_vip_summary_json, apply_private_chat_response_defaults,
        authenticate_settings_init_data, build_deputy_display_name, chat_list_title,
        chat_member_can_manage_settings, chat_member_record_from_upsert,
        configure_telegram_bot_commands, current_unix_timestamp,
        delete_webhook_on_shutdown_if_enabled, dialog_memory_context_enabled,
        effective_memory_consolidation_workers, found, go_dispatcher_config, new_settings_response,
        normalize_deputy_ids, parse_admin_non_negative_i32, parse_admin_optional_bool,
        parse_admin_positive_i32, parse_deputy_candidates_limit, parse_deputy_update_request,
        parse_memory_limit, parse_optional_i32, parse_settings_get_access,
        parse_settings_memory_access, parse_settings_update_request, parse_shield_limit,
        routing_json_config_patch, run_long_poll_update_producer_after_delete_webhook,
        run_webhook_update_producer_after_set_webhook, runtime_api_graphql_snapshot,
        runtime_redis_prefix_groups_from_keys, runtime_redis_value_from_bytes,
        send_dispatcher_work_item_with_transport,
        send_dispatcher_work_item_with_transport_and_history, settings_chat_display,
        settings_chat_full_info_type_name, settings_chat_state_from_full_info,
        settings_get_response, settings_reply_flags, shield_history_tail_messages_from_config,
        shield_options_from_config, static_web_asset_response, static_web_routes,
        telegram_webhook_response, virtual_messages,
    };
    use crate::permissions::{
        ChatPermissionContext, ChatPermissionPolicy, ChatPermissionStore, ChatPermissionStoreFuture,
    };
    use crate::rate_limits::{ChatRateLimitPolicy, RateLimitStore, RateLimitStoreFuture};
    use crate::updates::UpdateHandler;

    #[test]
    fn go_dispatcher_config_matches_server_runtime_defaults() {
        let config = go_dispatcher_config();

        assert_eq!(config.max_queue_size, GO_DISPATCHER_MAX_QUEUE_SIZE);
        assert!(config.dedupe_config.enabled);
        assert_eq!(
            config.dedupe_config.default_window,
            GO_DISPATCHER_DEBOUNCE_WINDOW
        );
        assert_eq!(
            config.dedupe_config.max_cache_size,
            GO_DISPATCHER_DEBOUNCE_CACHE_SIZE
        );
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
    fn memory_consolidation_worker_count_follows_pools() {
        // Explicit zero disables workers regardless of pools.
        assert_eq!(effective_memory_consolidation_workers(0, Some(16), 8), 0);
        // Unpooled route (no derivation) keeps the configured count.
        assert_eq!(effective_memory_consolidation_workers(1, None, 8), 1);
        // A pooled route scales up to the cap.
        assert_eq!(effective_memory_consolidation_workers(1, Some(16), 8), 8);
        assert_eq!(effective_memory_consolidation_workers(1, Some(3), 8), 3);
        // An explicit operator count above the derivation wins.
        assert_eq!(effective_memory_consolidation_workers(12, Some(16), 8), 12);
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
    async fn dispatcher_send_consumes_reply_missing_send_message_like_go_dialog_response()
    -> Result<(), Box<dyn Error>> {
        let rate_limits = Arc::new(ChatRateLimitPolicy::new(RateLimitStoreStub));
        let permission_store = PermissionStoreStub::with_context(ChatPermissionContext {
            chat_type: Some("supergroup".to_owned()),
            settings: Some(ChatSettings::defaults(42)),
        });
        let permissions = Arc::new(ChatPermissionPolicy::new(permission_store.clone()));
        let item = queued_method_item(TelegramOutboundMethod::from(SendMessage::new(42, "hello")));

        let status =
            send_dispatcher_work_item_with_transport(rate_limits, permissions, item, |_| async {
                Err::<TelegramOutboundResponse, _>(reply_missing_error())
            })
            .await;

        assert_eq!(status, DispatcherSendStatus::Sent);
        assert!(permission_store.saved_updates().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn long_poll_update_startup_deletes_webhook_before_producing_updates_like_go()
    -> Result<(), Box<dyn Error>> {
        let startup = DeleteWebhookStub::default();
        let source = ProducerSourceStub::new(vec![Some(sample_message_update(10)?), None]);
        let queue = ProducerQueueStub::default();

        let report = run_long_poll_update_producer_after_delete_webhook(
            &startup,
            &source,
            &queue,
            std::future::pending(),
        )
        .await;

        assert_eq!(startup.calls(), 1);
        assert!(report.delete_webhook_error.is_none());
        let producer = report.producer.expect("producer report");
        assert_eq!(producer.received, 1);
        assert_eq!(producer.enqueued, 1);
        assert!(producer.source_closed);
        assert_eq!(queue.enqueued_ids(), vec![10]);
        assert_eq!(source.calls(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn long_poll_update_startup_stops_before_producer_when_delete_webhook_fails_like_go()
    -> Result<(), Box<dyn Error>> {
        let startup = DeleteWebhookStub::failing("delete webhook failed");
        let source = ProducerSourceStub::new(vec![Some(sample_message_update(10)?), None]);
        let queue = ProducerQueueStub::default();

        let report = run_long_poll_update_producer_after_delete_webhook(
            &startup,
            &source,
            &queue,
            std::future::pending(),
        )
        .await;

        assert_eq!(startup.calls(), 1);
        assert_eq!(
            report.delete_webhook_error.as_deref(),
            Some("delete webhook failed")
        );
        assert!(report.producer.is_none());
        assert!(queue.enqueued_ids().is_empty());
        assert_eq!(source.calls(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn webhook_update_startup_sets_webhook_before_producing_updates_like_go()
    -> Result<(), Box<dyn Error>> {
        let startup = SetWebhookStub::default();
        let setup = openplotva_telegram::WebhookSetup::new(
            "https://example.test/telegram/webhook",
            Some("secret".to_owned()),
        );
        let source = ProducerSourceStub::new(vec![Some(sample_message_update(20)?), None]);
        let queue = ProducerQueueStub::default();

        let report = run_webhook_update_producer_after_set_webhook(
            &startup,
            &setup,
            &source,
            &queue,
            std::future::pending(),
        )
        .await;

        assert_eq!(startup.calls(), 1);
        assert!(report.set_webhook_error.is_none());
        let producer = report.producer.expect("producer report");
        assert_eq!(producer.received, 1);
        assert_eq!(producer.enqueued, 1);
        assert_eq!(queue.enqueued_ids(), vec![20]);
        let payload = startup.payloads().pop().expect("setWebhook payload");
        assert_eq!(payload["url"], "https://example.test/telegram/webhook");
        assert_eq!(payload["secret_token"], "secret");
        assert!(
            payload
                .get("allowed_updates")
                .and_then(serde_json::Value::as_array)
                .is_some_and(
                    |updates| updates.len() == openplotva_updates::GO_ALLOWED_UPDATE_NAMES.len()
                )
        );
        Ok(())
    }

    #[tokio::test]
    async fn webhook_update_startup_stops_before_producer_when_set_webhook_fails_like_go()
    -> Result<(), Box<dyn Error>> {
        let startup = SetWebhookStub::failing("set webhook failed");
        let setup = openplotva_telegram::WebhookSetup::new(
            "https://example.test/telegram/webhook",
            Some("secret".to_owned()),
        );
        let source = ProducerSourceStub::new(vec![Some(sample_message_update(20)?), None]);
        let queue = ProducerQueueStub::default();

        let report = run_webhook_update_producer_after_set_webhook(
            &startup,
            &setup,
            &source,
            &queue,
            std::future::pending(),
        )
        .await;

        assert_eq!(startup.calls(), 1);
        assert_eq!(
            report.set_webhook_error.as_deref(),
            Some("set webhook failed")
        );
        assert!(report.producer.is_none());
        assert!(queue.enqueued_ids().is_empty());
        assert_eq!(source.calls(), 0);
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
    async fn telegram_webhook_response_accepts_update_and_maps_go_errors()
    -> Result<(), Box<dyn Error>> {
        let (sender, source) = openplotva_telegram::webhook_update_channel(1);
        let body = serde_json::to_vec(&sample_message_update(30)?)?;
        let mut headers = HeaderMap::new();
        headers.insert(
            openplotva_telegram::TELEGRAM_WEBHOOK_SECRET_HEADER,
            "secret".parse()?,
        );

        let response = telegram_webhook_response(
            &sender,
            Method::POST,
            headers.clone(),
            "secret",
            Bytes::from(body.clone()),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            source.next_update_now().await.expect("accepted update").id,
            30
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

        let response = telegram_webhook_response(
            &sender,
            Method::POST,
            HeaderMap::new(),
            "",
            Bytes::from_static(b"not-json"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], br#"{"error":"invalid update"}"#);
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
            "8c798715212832b795e3c347714e734f0bfdc896dec9b15ed842c46f796661fd"
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

        let response = admin_auth_response(&routes, Method::GET, Some(valid_query.as_str())).await;
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(
            response.headers().get(header::LOCATION),
            Some(&"/admin/".parse()?)
        );
        assert_eq!(
            response.headers().get(header::SET_COOKIE),
            Some(&openplotva_web::admin_session_cookie(7, "123:ABC").parse()?)
        );

        let response = admin_auth_response(&routes, Method::PUT, Some(valid_query.as_str())).await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"method not allowed\"}\n");

        let response = admin_auth_response(&routes, Method::GET, Some("id=7")).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = admin_auth_response(&routes, Method::GET, Some("id=nope&hash=x")).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = admin_auth_response(
            &routes,
            Method::GET,
            Some("id=7&auth_date=1700000000&hash=bad"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let non_admin_routes = static_web_routes(vec![9], "123:ABC", "", "", None);
        let response =
            admin_auth_response(&non_admin_routes, Method::GET, Some(valid_query.as_str())).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let no_token_routes = static_web_routes(vec![7], "", "", "", None);
        let response =
            admin_auth_response(&no_token_routes, Method::GET, Some(valid_query.as_str())).await;
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

        let response = admin_auth_check(Extension(routes.clone()), HeaderMap::new()).await;
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
        let response = admin_auth_check(Extension(routes.clone()), headers).await;
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"authenticated\":true,\"user_id\":7}\n");

        let response = admin_bootstrap(Extension(routes)).await;
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
        let response = admin_state_response(&routes).await;

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
    async fn admin_loglevel_matches_go_auth_method_and_level_contract() -> Result<(), Box<dyn Error>>
    {
        let routes = static_web_routes(vec![7], "123:ABC", "https://plotva.example", "", None);
        let mut headers = HeaderMap::new();
        {
            let signed = openplotva_web::admin_session_cookie(7, "123:ABC");
            let value = signed.split(';').next().expect("cookie pair");
            headers.insert(header::COOKIE, value.parse()?);
        }

        let response =
            admin_loglevel_response(&routes, Method::POST, &headers, br#"{"level":" warning "}"#)
                .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(value, json!({ "ok": true, "level": "warning" }));
        assert_eq!(body.last(), Some(&b'\n'));

        let response =
            admin_loglevel_response(&routes, Method::GET, &headers, br#"{"level":"debug"}"#).await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);

        let response = admin_loglevel_response(
            &routes,
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
            &routes,
            Method::POST,
            &non_admin_headers,
            br#"{"level":"debug"}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"forbidden\"}\n");
        assert_eq!(body.last(), Some(&b'\n'));

        let response =
            admin_loglevel_response(&routes, Method::POST, &headers, br#"{"level":"trace"}"#).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        Ok(())
    }

    #[tokio::test]
    async fn admin_llm_requests_static_rest_lists_and_clears_live_trace_buffer()
    -> Result<(), Box<dyn Error>> {
        let mut routes = static_web_routes(vec![7], "123:ABC", "https://plotva.example", "", None);
        let buffer = super::runtime_llm::RuntimeLlmTraceBuffer::new(4);
        buffer.record(openplotva_server::RuntimeLlmRequestData {
            at: "2026-05-21T00:00:00Z".to_owned(),
            provider: Some("aifarm".to_owned()),
            source: "dialog".to_owned(),
            model: Some("model-a".to_owned()),
            chat: openplotva_server::RuntimeLlmRequestChatData {
                chat_id: -100,
                chat_title: Some("Plotva Lab".to_owned()),
                ..openplotva_server::RuntimeLlmRequestChatData::default()
            },
            user: openplotva_server::RuntimeLlmRequestUserData {
                user_id: 7,
                full_name: Some("Ada".to_owned()),
            },
            message: openplotva_server::RuntimeLlmRequestMessageData { message_id: 77 },
            gen_config: openplotva_server::RuntimeLlmGenConfigData {
                max_output_tokens: 128,
                ..openplotva_server::RuntimeLlmGenConfigData::default()
            },
            raw_response: Some(json!({"content": "answer"})),
            result: openplotva_server::RuntimeLlmRequestResultData {
                duration_ms: 42,
                response_text_preview: Some("answer".to_owned()),
                ..openplotva_server::RuntimeLlmRequestResultData::default()
            },
            ..openplotva_server::RuntimeLlmRequestData::default()
        });
        routes.llm_trace_buffer = Some(buffer);
        let mut headers = HeaderMap::new();
        {
            let signed = openplotva_web::admin_session_cookie(7, "123:ABC");
            let value = signed.split(';').next().expect("cookie pair");
            headers.insert(header::COOKIE, value.parse()?);
        }

        let response =
            admin_llm_requests_response(&routes, Method::GET, &headers, Some("q=answer"));
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL),
            Some(&"no-cache".parse()?)
        );
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(value["count"], 1);
        assert_eq!(value["requests"][0]["id"], 1);
        assert_eq!(value["requests"][0]["provider"], "aifarm");
        assert_eq!(value["requests"][0]["chat"]["chat_title"], "Plotva Lab");
        assert_eq!(value["requests"][0]["gen_config"]["max_output_tokens"], 128);
        assert_eq!(
            value["requests"][0]["result"]["response_text_preview"],
            "answer"
        );

        let response = admin_llm_requests_clear_response(&routes, Method::POST, &headers);
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"ok\":true}\n");

        let response = admin_llm_requests_response(&routes, Method::GET, &headers, None);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(value, json!({"count": 0, "requests": []}));

        let response = admin_llm_requests_response(&routes, Method::POST, &headers, None);
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
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
            &routes,
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
            &routes,
            Method::POST,
            &headers,
            None,
            br#"{"title":"Risk"}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"shield disabled\"}\n");

        let response =
            admin_shield_documents_response(&routes, Method::DELETE, &headers, None, &[]).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"id required\"}\n");

        let response =
            admin_shield_documents_response(&routes, Method::DELETE, &headers, Some("id=5"), &[])
                .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"ok\":true}\n");

        let response =
            admin_shield_embeddings_rebuild_response(&routes, Method::POST, &headers).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let response = admin_shield_test_response(&routes, Method::GET, &headers, br#"{}"#).await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);

        let response = admin_shield_test_response(&routes, Method::POST, &headers, br#"{}"#).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        Ok(())
    }

    #[test]
    fn admin_shield_json_and_helpers_match_go_contract() -> Result<(), Box<dyn Error>> {
        assert_eq!(parse_shield_limit(None), 100);
        assert_eq!(parse_shield_limit(Some("0")), 100);
        assert_eq!(parse_shield_limit(Some("999")), 500);

        let input = admin_shield_document_input(
            super::AdminShieldDocumentRequest {
                title: " Risk Title ".to_owned(),
                body: " Body ".to_owned(),
                ..super::AdminShieldDocumentRequest::default()
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
            &routes,
            Method::GET,
            &headers,
            Some("status=pending&queue=image-vip"),
        );
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
            &routes,
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
            &routes,
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
            &routes,
            Method::PUT,
            &headers,
            Some(&format!("job_id={diagnostic_id}")),
        );
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert!(value["new_job_id"].as_i64().unwrap_or_default() > diagnostic_id);

        let response = admin_taskman_jobs_clear_response(
            &routes,
            Method::DELETE,
            &headers,
            Some("status=cancelled"),
        );
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

        let response =
            admin_memory_cards_response(&routes, Method::GET, &headers, Some("limit=999")).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(value["count"], 0);
        assert!(value["cards"].is_null());

        let response =
            admin_memory_cards_response(&routes, Method::DELETE, &headers, Some("id=42")).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"ok\":true}\n");

        let response = admin_memory_runs_response(&routes, Method::GET, &headers, None, &[]).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        assert_eq!(value["count"], 0);
        assert!(value["runs"].is_null());
        assert_eq!(value["policy"]["min_messages_per_run"], 20);
        assert_eq!(value["policy"]["max_queued_runs"], 5000);
        assert_eq!(value["policy"]["max_daily_enqueued_runs"], 2000);
        assert!(value["enqueue_rollups"].is_null());

        let response =
            admin_memory_runs_response(&routes, Method::POST, &headers, Some("id=42"), &[]).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"memory disabled\"}\n");

        let response =
            admin_memory_restart_response(&routes, Method::POST, &headers, Some("id=bad"), &[])
                .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"invalid id\"}\n");

        let response =
            admin_memory_restart_response(&routes, Method::GET, &headers, None, &[]).await;
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
        let response = admin_taskman_jobs_response(&routes, Method::GET, &headers, None);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"task manager not configured\"}\n");

        let response = admin_taskman_job_response(&routes, Method::GET, &headers, None).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let response =
            admin_taskman_jobs_response(&routes, Method::POST, &headers, Some("status=pending"));
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        Ok(())
    }

    #[test]
    fn admin_llm_request_filter_keeps_defaults_and_optional_ids() {
        let filter = admin_llm_requests_filter(Some(
            "limit=0&q=%20needle%20&source=dialog&model=m&chat_id=-100&user_id=bad",
        ));
        assert_eq!(filter.limit, 1000);
        assert_eq!(filter.q, "needle");
        assert_eq!(filter.source, "dialog");
        assert_eq!(filter.model, "m");
        assert_eq!(filter.chat_id, Some(-100));
        assert_eq!(filter.user_id, None);
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

        let response =
            admin_chat_get_response(&routes, Method::POST, &headers, Some("chat_id=1")).await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"method not allowed\"}\n");

        let response =
            admin_chat_get_response(&routes, Method::GET, &HeaderMap::new(), Some("chat_id=1"))
                .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"unauthorized\"}\n");

        let response = admin_chat_get_response(&routes, Method::GET, &headers, None).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"chat_id required\"}\n");

        let response =
            admin_chat_get_response(&routes, Method::GET, &headers, Some("chat_id=nope")).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"invalid chat_id\"}\n");

        let response = admin_chats_search_by_member_response(
            &routes,
            Method::GET,
            &headers,
            Some("user_id=nope"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        assert_eq!(&body[..], b"{\"error\":\"username or user_id required\"}\n");

        let response = admin_user_grant_vip_response(
            &routes,
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

    #[tokio::test]
    async fn settings_api_without_services_fails_loud_with_go_cors_headers()
    -> Result<(), Box<dyn Error>> {
        let routes = static_web_routes(Vec::new(), "", "", "", None);
        let response =
            settings_get_response(&routes, Some("chat_id=42&signature=780e28cf"), None).await;
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
        let routes = static_web_routes(vec![42], "123:ABC", "https://plotva.example", "", None);

        let response = match parse_settings_memory_access(
            &routes,
            Some("chat_id=-1001234567890&user_id=42&signature=780e28cf"),
            None,
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
        assert!(matches!(
            authenticate_settings_init_data(&routes, Some(&init_data), 42),
            Ok(SettingsInitDataDecision::Authorized)
        ));
    }

    #[test]
    fn settings_init_data_rejects_caller_user_id_mismatch() {
        let routes = static_web_routes(Vec::new(), SETTINGS_GATE_TOKEN, "", "", None);
        let init_data = fresh_settings_init_data(42);
        assert!(authenticate_settings_init_data(&routes, Some(&init_data), 99).is_err());
    }

    #[test]
    fn settings_init_data_rejects_forged_header() {
        let routes = static_web_routes(Vec::new(), SETTINGS_GATE_TOKEN, "", "", None);
        let forged = "auth_date=1700000000&user=%7B%22id%22%3A42%7D&hash=deadbeef";
        assert!(authenticate_settings_init_data(&routes, Some(forged), 42).is_err());
    }

    #[test]
    fn settings_init_data_absent_with_flag_off_falls_through() {
        let routes = static_web_routes(Vec::new(), SETTINGS_GATE_TOKEN, "", "", None);
        assert!(!routes.require_settings_init_data);
        assert!(matches!(
            authenticate_settings_init_data(&routes, None, 42),
            Ok(SettingsInitDataDecision::FellThrough)
        ));
    }

    #[test]
    fn settings_init_data_absent_with_flag_on_rejects() {
        let mut routes = static_web_routes(Vec::new(), SETTINGS_GATE_TOKEN, "", "", None);
        routes.require_settings_init_data = true;
        assert!(authenticate_settings_init_data(&routes, None, 42).is_err());
    }

    #[test]
    fn settings_response_and_display_helpers_match_go_shape() {
        let chat = openplotva_core::ChatState::new(
            -10042,
            "supergroup",
            Some("Plotva Lab".to_owned()),
            Some("plotvalab".to_owned()),
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
            "username": "plotvalab",
            "is_forum": true
        }))?;
        let state = settings_chat_state_from_full_info(chat);

        assert_eq!(state.id, -100123);
        assert_eq!(state.chat_type, "supergroup");
        assert_eq!(state.title.as_deref(), Some(" Plotva Lab "));
        assert_eq!(state.username.as_deref(), Some("plotvalab"));
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
    fn app_router_builds_with_admin_api_routes_before_static_wildcard() {
        assert_eq!(
            GO_ADMIN_API_ROUTE_PATTERNS,
            [
                "/admin/api/auth",
                "/admin/api/auth_check",
                "/admin/api/state",
                "/admin/api/loglevel",
                "/admin/api/logs/stream",
                "/admin/api/bootstrap",
                "/admin/api/metrics",
                "/admin/api/llm/requests",
                "/admin/api/llm/requests/clear",
                "/admin/api/safety/checks",
                "/admin/api/analytics/llm/summary",
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
            update_buffer_size: openplotva_config::DEFAULT_BOT_WEBHOOK_UPDATE_BUFFER_SIZE,
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
    fn webhook_setup_from_config_ignores_cert_file_without_key_file_like_go()
    -> Result<(), Box<dyn Error>> {
        let cert_path = unique_temp_file("openplotva-webhook-cert-no-key.pem");
        std::fs::write(&cert_path, b"cert-bytes")?;
        let config = openplotva_config::BotWebhookConfig {
            enabled: true,
            url: "https://example.test/telegram/webhook".to_owned(),
            cert_file: cert_path.to_string_lossy().into_owned(),
            key_file: String::new(),
            secret_token: String::new(),
            update_buffer_size: openplotva_config::DEFAULT_BOT_WEBHOOK_UPDATE_BUFFER_SIZE,
        };

        let setup = super::webhook_setup_from_config(&config)?;

        assert!(setup.secret_token.is_none());
        assert!(setup.certificate.is_none());
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
        assert_eq!(
            plan.fields,
            vec![
                (
                    "allowed_updates".to_owned(),
                    serde_json::to_string(openplotva_updates::GO_ALLOWED_UPDATE_NAMES)?
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
        fn failing(error: &'static str) -> Self {
            Self {
                calls: Arc::new(Mutex::new(0)),
                error: Some(error),
            }
        }

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

    #[derive(Clone, Default)]
    struct SetWebhookStub {
        requests: Arc<Mutex<Vec<serde_json::Value>>>,
        error: Option<&'static str>,
    }

    impl SetWebhookStub {
        fn failing(error: &'static str) -> Self {
            Self {
                error: Some(error),
                ..Self::default()
            }
        }

        fn calls(&self) -> usize {
            self.requests.lock().expect("setWebhook requests").len()
        }

        fn payloads(&self) -> Vec<serde_json::Value> {
            self.requests.lock().expect("setWebhook requests").clone()
        }
    }

    impl super::SetWebhookExecutor for SetWebhookStub {
        type Error = StartupError;

        fn set_webhook<'a>(
            &'a self,
            setup: &'a openplotva_telegram::WebhookSetup,
        ) -> super::SetWebhookFuture<'a, Self::Error> {
            let method = openplotva_telegram::build_set_webhook_method(setup);
            self.requests
                .lock()
                .expect("setWebhook requests")
                .push(serde_json::to_value(method).expect("setWebhook JSON"));
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

    #[derive(Clone)]
    struct ProducerSourceStub {
        updates: Arc<Mutex<VecDeque<Option<TelegramUpdate>>>>,
        calls: Arc<Mutex<usize>>,
    }

    impl ProducerSourceStub {
        fn new(updates: Vec<Option<TelegramUpdate>>) -> Self {
            Self {
                updates: Arc::new(Mutex::new(updates.into())),
                calls: Arc::new(Mutex::new(0)),
            }
        }

        fn calls(&self) -> usize {
            *self.calls.lock().expect("source calls")
        }
    }

    impl UpdateProducerSource for ProducerSourceStub {
        fn next_update<'a>(&'a self) -> UpdateProducerSourceFuture<'a> {
            *self.calls.lock().expect("source calls") += 1;
            let update = self.updates.lock().expect("updates").pop_front().flatten();
            Box::pin(async move { update })
        }
    }

    #[derive(Clone, Default)]
    struct ProducerQueueStub {
        updates: Arc<Mutex<Vec<TelegramUpdate>>>,
    }

    impl ProducerQueueStub {
        fn enqueued_ids(&self) -> Vec<i64> {
            self.updates
                .lock()
                .expect("enqueued updates")
                .iter()
                .map(|update| update.id)
                .collect()
        }
    }

    impl UpdateProducerQueue for ProducerQueueStub {
        type Error = StartupError;

        fn enqueue_update<'a>(
            &'a self,
            update: &'a TelegramUpdate,
        ) -> UpdateProducerQueueFuture<'a, Self::Error> {
            self.updates
                .lock()
                .expect("enqueued updates")
                .push(update.clone());
            Box::pin(async { Ok(()) })
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
