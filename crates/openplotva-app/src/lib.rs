//! Composition root for the OpenPlotva application shell.

use anyhow::Context as _;
use openplotva_config::AppConfig;

/// Build the HTTP router without binding a socket.
pub fn router() -> axum::Router {
    openplotva_server::router()
}

/// Run the current OpenPlotva app shell.
pub async fn run() -> anyhow::Result<()> {
    let config = AppConfig::from_env().context("load configuration")?;
    openplotva_observability::init(&config.observability);

    let listener = tokio::net::TcpListener::bind(config.server.bind)
        .await
        .with_context(|| format!("bind HTTP listener to {}", config.server.bind))?;
    let local_addr = listener
        .local_addr()
        .context("read HTTP listener address")?;

    tracing::info!(address = %local_addr, "openplotva listening");

    axum::serve(listener, router())
        .await
        .context("serve HTTP app")
}
