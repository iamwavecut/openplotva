use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, bail};

/// Supervisor settings, read from `SANDBOX_*` environment variables.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupervisorConfig {
    /// `SANDBOX_BIND`.
    pub bind: SocketAddr,
    /// `SANDBOX_NSJAIL`.
    pub nsjail_path: PathBuf,
    /// `SANDBOX_NSJAIL_CONFIG`.
    pub nsjail_config: PathBuf,
    /// `SANDBOX_PYTHON`.
    pub python: PathBuf,
    /// `SANDBOX_SESSIONS_DIR`: session homes and per-call scratch files.
    pub sessions_dir: PathBuf,
    /// `SANDBOX_MATPLOTLIB_CACHE`: prebuilt font list copied into each home.
    pub matplotlib_cache: Option<PathBuf>,
    /// `SANDBOX_MAX_SESSIONS`.
    pub max_sessions: usize,
    /// `SANDBOX_IDLE_SECONDS`: a session closes this long after its last call.
    pub idle_ttl: Duration,
    /// `SANDBOX_MAX_SESSION_SECONDS`: a session closes this long after it opened.
    pub max_age: Duration,
    /// `SANDBOX_HOME_MIB`: tmpfs size of one session home.
    pub home_size_mib: u64,
    /// `SANDBOX_SLOT_WAIT_SECONDS`: how long a new session waits for a free slot.
    pub slot_wait: Duration,
    /// `SANDBOX_CALL_TIMEOUT_SECONDS`: supervisor backstop above the nsjail time limit.
    pub call_timeout: Duration,
    /// Bytes kept from each of stdout and stderr.
    pub max_output_bytes: usize,
    /// Entries returned in a file listing.
    pub max_listed_files: usize,
    /// Largest file the supervisor serves.
    pub max_read_bytes: u64,
    /// Largest accepted request body.
    pub max_request_bytes: usize,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([0, 0, 0, 0], 8090)),
            nsjail_path: PathBuf::from("/usr/sbin/nsjail"),
            nsjail_config: PathBuf::from("/etc/sandbox/python.cfg"),
            python: PathBuf::from("/opt/sandbox/venv/bin/python"),
            sessions_dir: PathBuf::from("/sessions"),
            matplotlib_cache: Some(PathBuf::from("/opt/sandbox/matplotlib")),
            max_sessions: 4,
            idle_ttl: Duration::from_secs(60),
            max_age: Duration::from_secs(300),
            home_size_mib: 100,
            slot_wait: Duration::from_secs(10),
            call_timeout: Duration::from_secs(45),
            max_output_bytes: 256 * 1024,
            max_listed_files: 100,
            max_read_bytes: 50 * 1024 * 1024,
            max_request_bytes: 96 * 1024 * 1024,
        }
    }
}

impl SupervisorConfig {
    /// Read the configuration from the process environment.
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let defaults = Self::default();
        let value = |key: &str| lookup(key).filter(|value| !value.trim().is_empty());
        let seconds = |key: &str, default: Duration| -> anyhow::Result<Duration> {
            value(key).map_or(Ok(default), |raw| {
                raw.trim()
                    .parse::<u64>()
                    .map(Duration::from_secs)
                    .with_context(|| format!("{key} must be whole seconds"))
            })
        };
        let config = Self {
            bind: value("SANDBOX_BIND").map_or(Ok(defaults.bind), |raw| {
                raw.trim().parse().context("SANDBOX_BIND must be host:port")
            })?,
            nsjail_path: value("SANDBOX_NSJAIL").map_or(defaults.nsjail_path, PathBuf::from),
            nsjail_config: value("SANDBOX_NSJAIL_CONFIG")
                .map_or(defaults.nsjail_config, PathBuf::from),
            python: value("SANDBOX_PYTHON").map_or(defaults.python, PathBuf::from),
            sessions_dir: value("SANDBOX_SESSIONS_DIR")
                .map_or(defaults.sessions_dir, PathBuf::from),
            matplotlib_cache: match lookup("SANDBOX_MATPLOTLIB_CACHE") {
                Some(raw) if raw.trim().is_empty() => None,
                Some(raw) => Some(PathBuf::from(raw.trim())),
                None => defaults.matplotlib_cache,
            },
            max_sessions: value("SANDBOX_MAX_SESSIONS").map_or(
                Ok(defaults.max_sessions),
                |raw| {
                    raw.trim()
                        .parse()
                        .context("SANDBOX_MAX_SESSIONS must be a positive integer")
                },
            )?,
            idle_ttl: seconds("SANDBOX_IDLE_SECONDS", defaults.idle_ttl)?,
            max_age: seconds("SANDBOX_MAX_SESSION_SECONDS", defaults.max_age)?,
            home_size_mib: value("SANDBOX_HOME_MIB").map_or(Ok(defaults.home_size_mib), |raw| {
                raw.trim()
                    .parse()
                    .context("SANDBOX_HOME_MIB must be a positive integer")
            })?,
            slot_wait: seconds("SANDBOX_SLOT_WAIT_SECONDS", defaults.slot_wait)?,
            call_timeout: seconds("SANDBOX_CALL_TIMEOUT_SECONDS", defaults.call_timeout)?,
            ..defaults
        };
        if config.max_sessions == 0 || config.home_size_mib == 0 {
            bail!("SANDBOX_MAX_SESSIONS and SANDBOX_HOME_MIB must be positive");
        }
        if config.idle_ttl.is_zero() || config.max_age < config.idle_ttl {
            bail!("SANDBOX_MAX_SESSION_SECONDS must be at least SANDBOX_IDLE_SECONDS");
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn config(vars: &[(&str, &str)]) -> anyhow::Result<SupervisorConfig> {
        let vars = vars
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect::<HashMap<_, _>>();
        SupervisorConfig::from_lookup(|key| vars.get(key).cloned())
    }

    #[test]
    fn defaults_match_the_agreed_limits() {
        let config = config(&[]).expect("defaults");
        assert_eq!(config.max_sessions, 4);
        assert_eq!(config.idle_ttl, Duration::from_secs(60));
        assert_eq!(config.max_age, Duration::from_secs(300));
        assert_eq!(config.home_size_mib, 100);
    }

    #[test]
    fn overrides_and_rejects_inconsistent_values() {
        let config = config(&[
            ("SANDBOX_BIND", "127.0.0.1:9000"),
            ("SANDBOX_MAX_SESSIONS", "2"),
            ("SANDBOX_MATPLOTLIB_CACHE", ""),
        ])
        .expect("overrides");
        assert_eq!(config.bind, SocketAddr::from(([127, 0, 0, 1], 9000)));
        assert_eq!(config.max_sessions, 2);
        assert_eq!(config.matplotlib_cache, None);

        assert!(
            super::SupervisorConfig::from_lookup(|key| {
                (key == "SANDBOX_MAX_SESSIONS").then(|| "0".to_owned())
            })
            .is_err()
        );
        assert!(
            super::SupervisorConfig::from_lookup(|key| {
                (key == "SANDBOX_MAX_SESSION_SECONDS").then(|| "10".to_owned())
            })
            .is_err()
        );
    }
}
