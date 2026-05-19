//! Composition root for the OpenPlotva application shell.

pub mod pending_ops;
mod reference_snapshot;
pub mod virtual_messages;

use anyhow::Context as _;
use openplotva_config::AppConfig;
use openplotva_server::{ReadinessCheck, ReadinessResponse};
use openplotva_storage::{PostgresVirtualMessageStore, ServiceClients};
use tokio::task::JoinHandle;

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
        start_runtime_workers(&config, service_clients.as_ref(), &mut readiness_checks)?;

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

fn start_runtime_workers(
    config: &AppConfig,
    service_clients: Option<&ServiceClients>,
    readiness_checks: &mut Vec<ReadinessCheck>,
) -> anyhow::Result<Vec<JoinHandle<()>>> {
    let Some(service_clients) = service_clients else {
        readiness_checks.push(ReadinessCheck::skipped(
            "pending_ops",
            "OPENPLOTVA_CONNECT_SERVICES=false",
        ));
        return Ok(Vec::new());
    };

    let Some(bot_key) = config.bot.key.as_deref() else {
        readiness_checks.push(ReadinessCheck::skipped("pending_ops", "BOT_KEY is not set"));
        return Ok(Vec::new());
    };

    let telegram = openplotva_telegram::telegram_client(bot_key.to_owned())
        .context("create Telegram Bot API client")?;
    let store = PostgresVirtualMessageStore::new(service_clients.postgres.clone());

    let worker = tokio::spawn(async move {
        let report = pending_ops::run_pending_op_worker_until(
            &store,
            |method| {
                let telegram = telegram.clone();
                async move {
                    openplotva_telegram::execute_telegram_method(&telegram, method)
                        .await
                        .map(|_| ())
                }
            },
            std::future::pending::<()>(),
        )
        .await;

        tracing::info!(?report, "pending operation worker stopped");
    });
    readiness_checks.push(ReadinessCheck::ok(
        "pending_ops",
        "Telegram pending-operation worker started",
    ));

    Ok(vec![worker])
}

async fn shutdown_runtime_workers(workers: Vec<JoinHandle<()>>) {
    for worker in workers {
        worker.abort();
        match worker.await {
            Ok(()) => {}
            Err(error) if error.is_cancelled() => {}
            Err(error) => tracing::warn!(%error, "runtime worker stopped with an error"),
        }
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
