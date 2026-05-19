//! Composition root for the OpenPlotva application shell.

mod reference_snapshot;

use anyhow::Context as _;
use openplotva_config::AppConfig;
use openplotva_server::{ReadinessCheck, ReadinessResponse};

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

    let listener = tokio::net::TcpListener::bind(&config.server.bind_addr)
        .await
        .with_context(|| format!("bind HTTP listener to {}", config.server.bind_addr))?;
    let local_addr = listener
        .local_addr()
        .context("read HTTP listener address")?;

    tracing::info!(address = %local_addr, "openplotva listening");

    let app = openplotva_server::router_with_readiness(ReadinessResponse::ready(readiness_checks));

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve HTTP app")?;

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
