//! Environment-backed configuration for OpenPlotva.

use std::{net::SocketAddr, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Default HTTP bind address for the local app shell.
pub const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8080";

/// Default tracing filter for local development.
pub const DEFAULT_LOG_FILTER: &str = "openplotva=info,tower_http=info";

/// Top-level application configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppConfig {
    /// HTTP server configuration.
    pub server: ServerConfig,
    /// Logging and tracing configuration.
    pub observability: ObservabilityConfig,
}

/// HTTP server configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Address the HTTP server should bind.
    pub bind: SocketAddr,
}

/// Logging and tracing configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    /// Tracing subscriber filter expression.
    pub log_filter: String,
}

/// Configuration loading failures.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// `OPENPLOTVA_BIND_ADDR` did not parse as a socket address.
    #[error("invalid OPENPLOTVA_BIND_ADDR {value:?}: {source}")]
    InvalidBindAddr {
        /// Raw value from configuration.
        value: String,
        /// Parser error from the standard library.
        #[source]
        source: std::net::AddrParseError,
    },
}

impl AppConfig {
    /// Load configuration from process environment variables.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_values(
            std::env::var("OPENPLOTVA_BIND_ADDR").ok(),
            std::env::var("OPENPLOTVA_LOG_FILTER").ok(),
        )
    }

    /// Build configuration from optional raw values.
    pub fn from_values(
        bind_addr: Option<String>,
        log_filter: Option<String>,
    ) -> Result<Self, ConfigError> {
        let bind_addr = bind_addr.unwrap_or_else(|| DEFAULT_BIND_ADDR.to_owned());
        let bind =
            SocketAddr::from_str(&bind_addr).map_err(|source| ConfigError::InvalidBindAddr {
                value: bind_addr.clone(),
                source,
            })?;

        Ok(Self {
            server: ServerConfig { bind },
            observability: ObservabilityConfig {
                log_filter: log_filter.unwrap_or_else(|| DEFAULT_LOG_FILTER.to_owned()),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{AppConfig, DEFAULT_BIND_ADDR, DEFAULT_LOG_FILTER};

    #[test]
    fn defaults_match_local_shell_contract() {
        let config = AppConfig::from_values(None, None).expect("defaults should parse");

        assert_eq!(config.server.bind.to_string(), DEFAULT_BIND_ADDR);
        assert_eq!(config.observability.log_filter, DEFAULT_LOG_FILTER);
    }

    #[test]
    fn invalid_bind_addr_is_rejected() {
        let error = AppConfig::from_values(Some("not-a-socket".to_owned()), None)
            .expect_err("invalid socket address should fail");

        assert!(error.to_string().contains("OPENPLOTVA_BIND_ADDR"));
    }
}
