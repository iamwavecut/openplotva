//! Composition root for the OpenPlotva application shell.

pub mod pending_ops;
pub mod permissions;
pub mod rate_limits;
mod reference_snapshot;
pub mod updates;
pub mod virtual_messages;

use std::{fmt, future::Future, pin::Pin, sync::Arc, time::Duration};

use anyhow::Context as _;
use axum::{
    body::Bytes,
    extract::Extension,
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
};
use openplotva_config::AppConfig;
use openplotva_server::{ReadinessCheck, ReadinessResponse};
use openplotva_storage::{
    PostgresChatSettingsStore, PostgresHistoryStore, PostgresVirtualMessageStore,
    RedisRateLimitStore, ServiceClients,
};
use serde::Deserialize;
use thiserror::Error;
use time::OffsetDateTime;
use tokio::{sync::watch, task::JoinHandle, time::timeout};

const GO_DISPATCHER_MAX_QUEUE_SIZE: usize = 10_000;
const GO_DISPATCHER_DEBOUNCE_WINDOW: Duration = Duration::from_secs(3);
const GO_DISPATCHER_DEBOUNCE_CACHE_SIZE: usize = 1_000;

#[derive(Default)]
struct RuntimeWorkers {
    handles: Vec<JoinHandle<()>>,
    stop: Option<watch::Sender<bool>>,
    dispatcher: Option<DispatcherRuntime>,
    webhook_route: Option<TelegramWebhookRoute>,
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

    /// Execute Go's startup `deleteWebhook` request.
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

    /// Execute Go's startup `setWebhook` request.
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

    /// Execute Go's startup `deleteMyCommands` request.
    fn delete_my_commands<'a>(
        &'a self,
        method: openplotva_telegram::DeleteBotCommands,
    ) -> BotCommandSetupFuture<'a, Self::Error>;

    /// Execute one Go startup `setMyCommands` request for a named command set.
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
    /// Go inventory names for command scopes successfully registered.
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
        /// Go inventory name for the command scope.
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

#[derive(Clone)]
struct TelegramWebhookRoute {
    sender: openplotva_telegram::WebhookUpdateSender,
    secret_token: Arc<str>,
}

/// Build the HTTP router without binding a socket.
pub fn router() -> axum::Router {
    openplotva_server::router()
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

    router_with_readiness_and_telegram_webhook_route(readiness, route)
}

fn router_with_readiness_and_telegram_webhook_route(
    readiness: ReadinessResponse,
    route: TelegramWebhookRoute,
) -> axum::Router {
    openplotva_server::router_with_readiness(readiness)
        .route(
            openplotva_telegram::TELEGRAM_WEBHOOK_PATH,
            any(telegram_webhook),
        )
        .layer(Extension(route))
}

/// Run the current OpenPlotva app shell.
pub async fn run() -> anyhow::Result<()> {
    let config = AppConfig::from_env().context("load configuration")?;
    openplotva_observability::init(&config.observability);

    let reference_snapshot = reference_snapshot::verify(&config.reference_snapshot).context("verify Go reference snapshot")?;
    let mut readiness_checks = vec![reference_snapshot.readiness_check()];
    let service_clients = connect_services(&config, &mut readiness_checks).await?;
    let runtime_workers =
        start_runtime_workers(&config, service_clients.as_ref(), &mut readiness_checks).await?;

    let listener = tokio::net::TcpListener::bind(&config.server.bind_addr)
        .await
        .with_context(|| format!("bind HTTP listener to {}", config.server.bind_addr))?;
    let local_addr = listener
        .local_addr()
        .context("read HTTP listener address")?;

    tracing::info!(address = %local_addr, "openplotva listening");

    let readiness = ReadinessResponse::ready(readiness_checks);
    let app = if let Some(webhook_route) = runtime_workers.webhook_route.clone() {
        router_with_readiness_and_telegram_webhook_route(readiness, webhook_route)
    } else {
        openplotva_server::router_with_readiness(readiness)
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

/// Run Go's long-poll startup order: delete webhook, then produce updates.
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

/// Run Go's webhook startup order: set webhook, then produce updates from the webhook source.
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

/// Configure Telegram bot commands in the same order as Go `initBot`.
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

async fn start_runtime_workers(
    config: &AppConfig,
    service_clients: Option<&ServiceClients>,
    readiness_checks: &mut Vec<ReadinessCheck>,
) -> anyhow::Result<RuntimeWorkers> {
    let Some(service_clients) = service_clients else {
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
            "telegram_commands",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        return Ok(RuntimeWorkers::default());
    };

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
            "telegram_commands",
            "BOT_KEY is not set",
        ));
        return Ok(RuntimeWorkers::default());
    };

    let telegram = openplotva_telegram::telegram_client(bot_key.to_owned())
        .context("create Telegram Bot API client")?;
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

    let dispatcher_persistence = openplotva_telegram::RedisDispatcherQueueStore::new(
        service_clients.redis.client().clone(),
        openplotva_telegram::DEFAULT_DISPATCHER_QUEUE_KEY,
        GO_DISPATCHER_MAX_QUEUE_SIZE,
    );
    let dispatcher_queue = Arc::new(openplotva_telegram::DispatcherQueue::new(
        go_dispatcher_config(),
    ));
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

    let (stop, _) = watch::channel(false);
    let mut workers = RuntimeWorkers {
        handles: Vec::new(),
        stop: Some(stop.clone()),
        dispatcher: None,
        webhook_route: None,
    };

    let pending_store = store.clone();
    let pending_history = history_store.clone();
    let pending_telegram = telegram.clone();
    let pending_stop = stop.subscribe();
    let pending_worker = tokio::spawn(async move {
        let report = pending_ops::run_pending_op_worker_with_history_until(
            &pending_store,
            &pending_history,
            |method| {
                let telegram = pending_telegram.clone();
                async move {
                    openplotva_telegram::execute_telegram_method(&telegram, method)
                        .await
                        .map(|_| ())
                }
            },
            wait_for_runtime_stop(pending_stop),
        )
        .await;

        tracing::info!(?report, "pending operation worker stopped");
    });
    readiness_checks.push(ReadinessCheck::ok(
        "pending_ops",
        "Telegram pending-operation worker started",
    ));
    workers.handles.push(pending_worker);

    let immediate_store = store.clone();
    let immediate_telegram = telegram.clone();
    let immediate_rate_limits = Arc::clone(&rate_limit_policy);
    let immediate_permissions = Arc::clone(&permission_policy);
    let immediate_queue = Arc::clone(&dispatcher_queue);
    let immediate_stop = stop.subscribe();
    let immediate_worker = tokio::spawn(async move {
        let outcome = immediate_queue
            .run_immediate_worker_until(wait_for_runtime_stop(immediate_stop), |item| {
                send_dispatcher_work_item(
                    immediate_store.clone(),
                    immediate_telegram.clone(),
                    Arc::clone(&immediate_rate_limits),
                    Arc::clone(&immediate_permissions),
                    item,
                )
            })
            .await;

        tracing::info!(?outcome, "outbound immediate dispatcher worker stopped");
    });

    if config.bot.webhook.enabled {
        if config.bot.webhook.url.is_empty() {
            tracing::warn!("BOT_WEBHOOK_URL is required when webhook updates are enabled");
            readiness_checks.push(ReadinessCheck::skipped(
                "telegram_update_producer",
                "BOT_WEBHOOK_URL is not set",
            ));
        } else {
            let (webhook_sender, webhook_source) = openplotva_telegram::webhook_update_channel(
                openplotva_telegram::GO_WEBHOOK_UPDATE_BUFFER_SIZE,
            );
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
                let update_queue = openplotva_updates::RedisUpdateQueue::new(
                    service_clients.redis.client().clone(),
                );
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
        let update_queue =
            openplotva_updates::RedisUpdateQueue::new(service_clients.redis.client().clone());
        let update_stop = stop.subscribe();
        let update_producer_worker = tokio::spawn(async move {
            let report = run_long_poll_update_producer_after_delete_webhook(
                &update_startup,
                &update_source,
                &update_queue,
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

    let regular_store = store;
    let regular_telegram = telegram;
    let regular_rate_limits = Arc::clone(&rate_limit_policy);
    let regular_permissions = Arc::clone(&permission_policy);
    let regular_queue = Arc::clone(&dispatcher_queue);
    let regular_limiters = Arc::clone(&dispatcher_limiters);
    let regular_stop = stop.subscribe();
    let regular_worker = tokio::spawn(async move {
        let outcome = regular_queue
            .run_regular_worker_until(
                &regular_limiters,
                wait_for_runtime_stop(regular_stop),
                |item| {
                    send_dispatcher_work_item(
                        regular_store.clone(),
                        regular_telegram.clone(),
                        Arc::clone(&regular_rate_limits),
                        Arc::clone(&regular_permissions),
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
        webhook_route: _,
    } = workers;

    if let Some(stop) = stop {
        let _ = stop.send(true);
    }

    if let Some(dispatcher) = dispatcher {
        persist_dispatcher_queue_on_shutdown(dispatcher).await;
    }

    for worker in handles {
        await_runtime_worker_shutdown(worker).await;
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

async fn send_dispatcher_work_item(
    store: PostgresVirtualMessageStore,
    telegram: openplotva_telegram::TelegramClient,
    rate_limits: Arc<rate_limits::ChatRateLimitPolicy<RedisRateLimitStore>>,
    permissions: Arc<permissions::ChatPermissionPolicy<PostgresChatSettingsStore>>,
    item: openplotva_telegram::DispatcherWorkItem,
) -> openplotva_telegram::DispatcherSendStatus {
    send_dispatcher_work_item_with_transport(
        store,
        rate_limits,
        permissions,
        item,
        |method| async move { openplotva_telegram::execute_telegram_method(&telegram, method).await },
    )
    .await
}

async fn send_dispatcher_work_item_with_transport<V, R, P, SendFn, SendFuture>(
    store: V,
    rate_limits: Arc<rate_limits::ChatRateLimitPolicy<R>>,
    permissions: Arc<permissions::ChatPermissionPolicy<P>>,
    item: openplotva_telegram::DispatcherWorkItem,
    send: SendFn,
) -> openplotva_telegram::DispatcherSendStatus
where
    V: virtual_messages::VirtualMessageStore + Sync,
    R: rate_limits::RateLimitStore + Send + Sync,
    P: permissions::ChatPermissionStore + Send + Sync,
    SendFn: FnOnce(openplotva_telegram::TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<
        Output = Result<openplotva_telegram::TelegramOutboundResponse, carapax::api::ExecuteError>,
    >,
{
    let chat_id = item.metadata().chat_id;
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
            return openplotva_telegram::DispatcherSendStatus::Failed;
        }
    }
    if chat_id != 0
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
                return openplotva_telegram::DispatcherSendStatus::Failed;
            }
        }
    }

    let report = virtual_messages::send_work_item_and_resolve(&store, item, |method| {
        let rate_limits = Arc::clone(&rate_limits);
        let permissions = Arc::clone(&permissions);
        async move {
            let method_kind = method.kind();
            match send(method).await {
                Ok(response) => Ok(response),
                Err(error) => {
                    if let Some(retry_after) =
                        rate_limits::telegram_retry_after_from_execute_error(&error)
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
                        && permissions::telegram_execute_error_is_permission_error(&error)
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
    })
    .await;
    if report.resolve_error.is_some() {
        tracing::warn!(
            virtual_id = report.virtual_id,
            real_message_id = ?report.resolved_message_id,
            resolve_error = ?report.resolve_error,
            "failed to resolve outbound virtual message"
        );
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
        collections::VecDeque,
        error::Error,
        fmt,
        pin::Pin,
        sync::{Arc, Mutex, MutexGuard},
        time::Duration,
    };

    use axum::{
        body::{Bytes, to_bytes},
        http::{HeaderMap, Method, StatusCode},
    };
    use carapax::types::{InputFile, SendMessage, SendPhoto, Update as TelegramUpdate};
    use openplotva_core::{ChatSettings, ChatSettingsUpdate, MessageIdMapping};
    use openplotva_telegram::{
        DispatcherConfig, DispatcherMessage, DispatcherQueue, DispatcherSendStatus,
        DispatcherWorkItem, MessageFingerprint, TelegramOutboundMethod, TelegramOutboundResponse,
    };
    use openplotva_updates::{
        UpdateProducerQueue, UpdateProducerQueueFuture, UpdateProducerSource,
        UpdateProducerSourceFuture,
    };
    use serde_json::json;
    use time::OffsetDateTime;

    use super::{
        GO_DISPATCHER_DEBOUNCE_CACHE_SIZE, GO_DISPATCHER_DEBOUNCE_WINDOW,
        GO_DISPATCHER_MAX_QUEUE_SIZE, configure_telegram_bot_commands, go_dispatcher_config,
        run_long_poll_update_producer_after_delete_webhook,
        run_webhook_update_producer_after_set_webhook, send_dispatcher_work_item_with_transport,
        telegram_webhook_response,
    };
    use crate::permissions::{
        ChatPermissionContext, ChatPermissionPolicy, ChatPermissionStore, ChatPermissionStoreFuture,
    };
    use crate::rate_limits::{ChatRateLimitPolicy, RateLimitStore, RateLimitStoreFuture};
    use crate::virtual_messages::VirtualMessageStore;

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

    #[tokio::test]
    async fn dispatcher_send_checks_permissions_before_telegram_transport()
    -> Result<(), Box<dyn Error>> {
        let store = VirtualMessageStoreStub;
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

        let status = send_dispatcher_work_item_with_transport(
            store,
            rate_limits,
            permissions,
            item,
            move |_| {
                *lock(&called_for_send) = true;
                async { Err::<TelegramOutboundResponse, _>(permission_error()) }
            },
        )
        .await;

        assert_eq!(status, DispatcherSendStatus::Failed);
        assert!(!*lock(&called));
        Ok(())
    }

    #[tokio::test]
    async fn dispatcher_send_auto_disables_settings_after_permission_error()
    -> Result<(), Box<dyn Error>> {
        let store = VirtualMessageStoreStub;
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

        let status = send_dispatcher_work_item_with_transport(
            store,
            rate_limits,
            permissions,
            item,
            |_| async { Err::<TelegramOutboundResponse, _>(permission_error()) },
        )
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

    struct VirtualMessageStoreStub;

    impl VirtualMessageStore for VirtualMessageStoreStub {
        type Error = StartupError;

        fn get_mapping_by_virtual<'a>(
            &'a self,
            _vmsg_id: String,
        ) -> Pin<
            Box<
                dyn std::future::Future<Output = Result<Option<MessageIdMapping>, Self::Error>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async { Ok(None) })
        }

        fn insert_virtual_message<'a>(
            &'a self,
            _vmsg_id: String,
            _chat_id: i64,
            _thread_id: Option<i32>,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + Send + 'a>>
        {
            Box::pin(async { Ok(()) })
        }

        fn resolve_virtual_message<'a>(
            &'a self,
            _vmsg_id: String,
            _real_message_id: i32,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + Send + 'a>>
        {
            Box::pin(async { Ok(()) })
        }

        fn enqueue_message_op<'a>(
            &'a self,
            _vmsg_id: String,
            _chat_id: i64,
            _op: &'static str,
            _payload_json: Option<String>,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<i64, Self::Error>> + Send + 'a>>
        {
            Box::pin(async { Ok(1) })
        }

        fn delete_mapping_by_virtual<'a>(
            &'a self,
            _vmsg_id: String,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + Send + 'a>>
        {
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

    fn permission_error() -> carapax::api::ExecuteError {
        let response: carapax::types::Response<serde_json::Value> = serde_json::from_str(
            r#"{"ok":false,"error_code":400,"description":"Bad Request: CHAT_WRITE_FORBIDDEN"}"#,
        )
        .expect("permission error JSON");
        match response.into_result() {
            Ok(_) => panic!("test response unexpectedly succeeded"),
            Err(error) => carapax::api::ExecuteError::Response(error),
        }
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
