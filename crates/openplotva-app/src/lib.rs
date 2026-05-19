//! Composition root for the OpenPlotva application shell.

pub mod pending_ops;
mod reference_snapshot;
pub mod virtual_messages;

use std::{sync::Arc, time::Duration};

use anyhow::Context as _;
use openplotva_config::AppConfig;
use openplotva_server::{ReadinessCheck, ReadinessResponse};
use openplotva_storage::{PostgresVirtualMessageStore, ServiceClients};
use tokio::{sync::watch, task::JoinHandle, time::timeout};

const GO_DISPATCHER_MAX_QUEUE_SIZE: usize = 10_000;
const GO_DISPATCHER_DEBOUNCE_WINDOW: Duration = Duration::from_secs(3);
const GO_DISPATCHER_DEBOUNCE_CACHE_SIZE: usize = 1_000;

#[derive(Default)]
struct RuntimeWorkers {
    handles: Vec<JoinHandle<()>>,
    stop: Option<watch::Sender<bool>>,
    dispatcher: Option<DispatcherRuntime>,
}

struct DispatcherRuntime {
    queue: Arc<openplotva_telegram::DispatcherQueue>,
    persistence: openplotva_telegram::RedisDispatcherQueueStore,
}

/// Build the HTTP router without binding a socket.
pub fn router() -> axum::Router {
    openplotva_server::router()
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

    let app = openplotva_server::router_with_readiness(ReadinessResponse::ready(readiness_checks));

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
        return Ok(RuntimeWorkers::default());
    };

    let telegram = openplotva_telegram::telegram_client(bot_key.to_owned())
        .context("create Telegram Bot API client")?;
    let store = PostgresVirtualMessageStore::new(service_clients.postgres.clone());

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

    let (stop, _) = watch::channel(false);
    let mut workers = RuntimeWorkers {
        handles: Vec::new(),
        stop: Some(stop.clone()),
        dispatcher: None,
    };

    let pending_store = store.clone();
    let pending_telegram = telegram.clone();
    let pending_stop = stop.subscribe();
    let pending_worker = tokio::spawn(async move {
        let report = pending_ops::run_pending_op_worker_until(
            &pending_store,
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
    let immediate_queue = Arc::clone(&dispatcher_queue);
    let immediate_stop = stop.subscribe();
    let immediate_worker = tokio::spawn(async move {
        let outcome = immediate_queue
            .run_immediate_worker_until(wait_for_runtime_stop(immediate_stop), |item| {
                send_dispatcher_work_item(immediate_store.clone(), immediate_telegram.clone(), item)
            })
            .await;

        tracing::info!(?outcome, "outbound immediate dispatcher worker stopped");
    });

    let regular_store = store;
    let regular_telegram = telegram;
    let regular_queue = Arc::clone(&dispatcher_queue);
    let regular_limiters = Arc::clone(&dispatcher_limiters);
    let regular_stop = stop.subscribe();
    let regular_worker = tokio::spawn(async move {
        let outcome = regular_queue
            .run_regular_worker_until(
                &regular_limiters,
                wait_for_runtime_stop(regular_stop),
                |item| {
                    send_dispatcher_work_item(regular_store.clone(), regular_telegram.clone(), item)
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
    item: openplotva_telegram::DispatcherWorkItem,
) -> openplotva_telegram::DispatcherSendStatus {
    let report = virtual_messages::send_work_item_and_resolve(&store, item, |method| {
        let telegram = telegram.clone();
        async move { openplotva_telegram::execute_telegram_method(&telegram, method).await }
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
    use super::{
        GO_DISPATCHER_DEBOUNCE_CACHE_SIZE, GO_DISPATCHER_DEBOUNCE_WINDOW,
        GO_DISPATCHER_MAX_QUEUE_SIZE, go_dispatcher_config,
    };

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
}
