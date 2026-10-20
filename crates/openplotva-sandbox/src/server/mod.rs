//! The supervisor: session homes, nsjail runs, and the HTTP API.

pub mod config;
pub mod home;
pub mod http;
pub mod runner;
pub mod session;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;

pub use config::SupervisorConfig;
use home::HomeStorage;
use runner::NsjailRunner;
use session::{SessionLimits, SessionManager};

const CGROUP_ROOT: &str = "/sys/fs/cgroup";
const REAP_INTERVAL: Duration = Duration::from_secs(1);

/// Run the supervisor until SIGTERM or Ctrl-C.
pub async fn serve(config: SupervisorConfig) -> anyhow::Result<()> {
    enable_cgroup_v2_controllers(Path::new(CGROUP_ROOT))
        .context("nsjail needs writable cgroup v2 controllers")?;
    reset_dir(&config.sessions_dir).await?;
    let scratch = config.sessions_dir.join(".calls");
    tokio::fs::create_dir_all(&scratch).await?;

    let sessions = Arc::new(SessionManager::new(
        HomeStorage::Tmpfs {
            size_mib: config.home_size_mib,
        },
        config.sessions_dir.clone(),
        config.matplotlib_cache.clone(),
        SessionLimits {
            max_sessions: config.max_sessions,
            idle_ttl: config.idle_ttl,
            max_age: config.max_age,
            slot_wait: config.slot_wait,
        },
    ));
    let runner = Arc::new(NsjailRunner::new(
        config.nsjail_path.clone(),
        config.nsjail_config.clone(),
        config.python.clone(),
        scratch,
        config.call_timeout,
        config.max_output_bytes,
    ));
    let state = Arc::new(http::AppState {
        sessions: Arc::clone(&sessions),
        runner,
        max_listed_files: config.max_listed_files,
        max_read_bytes: config.max_read_bytes,
        max_input_bytes: config.home_size_mib * 1024 * 1024,
    });

    let reaper = {
        let sessions = Arc::clone(&sessions);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(REAP_INTERVAL);
            loop {
                ticker.tick().await;
                sessions.reap().await;
            }
        })
    };

    let listener = tokio::net::TcpListener::bind(config.bind)
        .await
        .with_context(|| format!("bind {}", config.bind))?;
    tracing::info!(bind = %config.bind, max_sessions = config.max_sessions, "sandbox supervisor listening");
    axum::serve(listener, http::router(state, config.max_request_bytes))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    reaper.abort();
    sessions.close_all().await;
    Ok(())
}

/// Move every process into a child cgroup and enable all controllers for
/// children, so nsjail can create a cgroup per run. A no-op when the
/// controllers are already enabled.
fn enable_cgroup_v2_controllers(root: &Path) -> std::io::Result<()> {
    let subtree = root.join("cgroup.subtree_control");
    if !std::fs::read_to_string(&subtree)?.trim().is_empty() {
        return Ok(());
    }
    let init = root.join("init");
    std::fs::create_dir_all(&init)?;
    for pid in std::fs::read_to_string(root.join("cgroup.procs"))?.split_whitespace() {
        if let Err(error) = std::fs::write(init.join("cgroup.procs"), pid) {
            tracing::debug!(pid, %error, "could not move process into the init cgroup");
        }
    }
    for controller in std::fs::read_to_string(root.join("cgroup.controllers"))?.split_whitespace() {
        std::fs::write(&subtree, format!("+{controller}"))?;
    }
    Ok(())
}

async fn reset_dir(dir: &Path) -> anyhow::Result<()> {
    if let Ok(mut entries) = tokio::fs::read_dir(dir).await {
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            HomeStorage::Tmpfs { size_mib: 0 }.destroy(&path).await;
        }
    }
    tokio::fs::create_dir_all(dir)
        .await
        .with_context(|| format!("create {}", dir.display()))
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(%error, "failed to listen for Ctrl-C");
            std::future::pending::<()>().await;
        }
    };
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::warn!(%error, "failed to listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
