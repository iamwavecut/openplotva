//! Environment-backed configuration for OpenPlotva.

use std::{
    io,
    num::{ParseIntError, TryFromIntError},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Default HTTP host inherited from the Go `WEBAPP_HOST` config.
pub const DEFAULT_WEBAPP_HOST: &str = "0.0.0.0";

/// Default HTTP port inherited from the Go `WEBAPP_PORT` config.
pub const DEFAULT_WEBAPP_PORT: u16 = 8080;

/// Default public WebApp URL inherited from the Go `WEBAPP_URL` config.
pub const DEFAULT_WEBAPP_URL: &str = "http://127.0.0.1:8080";

/// Default application log level inherited from the Go `LOG_LEVEL` config.
pub const DEFAULT_LOG_LEVEL: &str = "info";

/// Default tracing filter for local development.
pub const DEFAULT_LOG_FILTER: &str = "openplotva=info,tower_http=info";

/// Default Postgres host inherited from the Go `DB_POSTGRES_HOST` config.
pub const DEFAULT_POSTGRES_HOST: &str = "127.0.0.1";

/// Default Postgres port inherited from the Go `DB_POSTGRES_PORT` config.
pub const DEFAULT_POSTGRES_PORT: u16 = 5432;

/// Default Postgres user inherited from the Go `DB_POSTGRES_USER` config.
pub const DEFAULT_POSTGRES_USER: &str = "plotva";

/// Default Postgres password inherited from the Go `DB_POSTGRES_PASSWORD` config.
pub const DEFAULT_POSTGRES_PASSWORD: &str = "plotva";

/// Default Postgres database inherited from the Go `DB_POSTGRES_DB` config.
pub const DEFAULT_POSTGRES_DATABASE: &str = "plotva";

/// Default Postgres SSL mode inherited from the Go `DB_POSTGRES_SSL_MODE` config.
pub const DEFAULT_POSTGRES_SSL_MODE: &str = "disable";

/// SSL mode used by the current Go startup DSN.
pub const GO_STARTUP_POSTGRES_SSL_MODE: &str = "disable";

/// Default Redis host inherited from the Go `REDIS_HOST` config.
pub const DEFAULT_REDIS_HOST: &str = "127.0.0.1";

/// Default Redis port inherited from the Go `REDIS_PORT` config.
pub const DEFAULT_REDIS_PORT: u16 = 6379;

/// Default Redis DB inherited from the Go `REDIS_DB` config.
pub const DEFAULT_REDIS_DB: i64 = 0;

pub const DEFAULT_REFERENCE_SOURCE_REPOSITORY: &str = "/Users/Shared/src/github.com/iamwavecut/reference-app";

pub const DEFAULT_RUNTIME_CONTRACT_PATH: &str = "docs/contract/reference-snapshot.json";

pub const DEFAULT_RUNTIME_CONTRACT_ENFORCE: bool = true;

pub const DEFAULT_CONNECT_SERVICES: bool = false;

/// SQLx migration execution is opt-in until existing Go DB compatibility is handled.
pub const DEFAULT_RUN_MIGRATIONS: bool = false;

/// Top-level application configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppConfig {
    /// HTTP server configuration.
    pub server: ServerConfig,
    /// Logging and tracing configuration.
    pub observability: ObservabilityConfig,
    /// Postgres configuration.
    pub database: DatabaseConfig,
    /// Redis/Dragonfly configuration.
    pub redis: RedisConfig,
    pub reference_snapshot: ReferenceSnapshotConfig,
    /// Runtime service-probe configuration.
    pub service_probe: ServiceProbeConfig,
}

/// HTTP server configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Host the HTTP server should bind, from `WEBAPP_HOST`.
    pub host: String,
    /// Port the HTTP server should bind, from `WEBAPP_PORT`.
    pub port: u16,
    /// Full bind address assembled from host/port or the Rust-only local override.
    pub bind_addr: String,
    /// Public WebApp URL, from `WEBAPP_URL`.
    pub url: String,
}

/// Logging and tracing configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    pub log_level: String,
    /// Tracing subscriber filter expression.
    pub log_filter: String,
}

/// Database configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Postgres configuration.
    pub postgres: PostgresConfig,
}

/// Postgres configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PostgresConfig {
    /// Postgres host.
    pub host: String,
    /// Postgres port.
    pub port: u16,
    /// Postgres user.
    pub user: String,
    /// Postgres password.
    pub password: String,
    /// Postgres database name.
    pub database: String,
    /// Configured SSL mode.
    pub ssl_mode: String,
}

impl PostgresConfig {
    /// Build the DSN used by the current Go startup path.
    pub fn go_startup_dsn(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}?sslmode={}",
            self.user,
            self.password,
            self.host,
            self.port,
            self.database,
            GO_STARTUP_POSTGRES_SSL_MODE
        )
    }
}

/// Redis/Dragonfly configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RedisConfig {
    /// Redis host.
    pub host: String,
    /// Redis port.
    pub port: u16,
    /// Redis password.
    pub password: String,
    /// Redis DB number.
    pub db: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReferenceSnapshotConfig {
    pub repository: PathBuf,
    pub lock_path: PathBuf,
    /// Whether app startup should fail when the Go checkout differs from the lock.
    pub enforce: bool,
}

/// Service-probe configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceProbeConfig {
    /// Whether startup should connect to Postgres and Redis.
    pub connect_services: bool,
    /// Whether startup should apply SQLx migrations after connecting to Postgres.
    pub run_migrations: bool,
}

/// Raw optional config values used by tests and environment loading.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RawConfig {
    /// Rust-only full bind-address override for local shell work.
    pub openplotva_bind_addr: Option<String>,
    /// Rust-only tracing filter override.
    pub openplotva_log_filter: Option<String>,
    /// `LOG_LEVEL`.
    pub log_level: Option<String>,
    /// `WEBAPP_HOST`.
    pub webapp_host: Option<String>,
    /// `WEBAPP_PORT`.
    pub webapp_port: Option<String>,
    /// `WEBAPP_URL`.
    pub webapp_url: Option<String>,
    /// `DB_POSTGRES_HOST`.
    pub db_postgres_host: Option<String>,
    /// `DB_POSTGRES_PORT`.
    pub db_postgres_port: Option<String>,
    /// `DB_POSTGRES_USER`.
    pub db_postgres_user: Option<String>,
    /// `DB_POSTGRES_PASSWORD`.
    pub db_postgres_password: Option<String>,
    /// `DB_POSTGRES_DB`.
    pub db_postgres_db: Option<String>,
    /// `DB_POSTGRES_SSL_MODE`.
    pub db_postgres_ssl_mode: Option<String>,
    /// `REDIS_HOST`.
    pub redis_host: Option<String>,
    /// `REDIS_PORT`.
    pub redis_port: Option<String>,
    /// `REDIS_PASSWORD`.
    pub redis_password: Option<String>,
    /// `REDIS_DB`.
    pub redis_db: Option<String>,
    /// `OPENPLOTVA_REFERENCE_SOURCE_REPOSITORY`.
    pub openplotva_reference_source_repository: Option<String>,
    /// `OPENPLOTVA_RUNTIME_CONTRACT_PATH`.
    pub openplotva_reference_snapshot_path: Option<String>,
    /// `OPENPLOTVA_DISABLED_LEGACY_LOCK`.
    pub openplotva_enforce_reference_snapshot: Option<String>,
    /// `OPENPLOTVA_CONNECT_SERVICES`.
    pub openplotva_connect_services: Option<String>,
    /// `OPENPLOTVA_RUN_MIGRATIONS`.
    pub openplotva_run_migrations: Option<String>,
}

/// Configuration loading failures.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// `.env` exists but could not be loaded.
    #[error("failed to load .env: {source}")]
    Dotenv {
        /// Parser or IO error from dotenvy.
        #[source]
        source: dotenvy::Error,
    },
    /// An integer environment variable failed to parse.
    #[error("invalid {name} {value:?}: {source}")]
    InvalidInteger {
        /// Environment variable name.
        name: &'static str,
        /// Raw value from the environment.
        value: String,
        /// Parser error.
        #[source]
        source: ParseIntError,
    },
    /// An integer parsed but did not fit the target type.
    #[error("invalid {name} {value}: {source}")]
    IntegerOutOfRange {
        /// Environment variable name.
        name: &'static str,
        /// Parsed value.
        value: i64,
        /// Conversion error.
        #[source]
        source: TryFromIntError,
    },
    /// A boolean environment variable failed to parse.
    #[error("invalid {name} {value:?}: expected true/false, t/f, or 1/0")]
    InvalidBoolean {
        /// Environment variable name.
        name: &'static str,
        /// Raw value from the environment.
        value: String,
    },
}

impl AppConfig {
    /// Load configuration from process environment variables.
    pub fn from_env() -> Result<Self, ConfigError> {
        load_dotenv()?;
        Self::from_raw(RawConfig::from_env())
    }

    /// Build configuration from optional raw values kept for scaffold tests.
    pub fn from_values(
        bind_addr: Option<String>,
        log_filter: Option<String>,
    ) -> Result<Self, ConfigError> {
        Self::from_raw(RawConfig {
            openplotva_bind_addr: bind_addr,
            openplotva_log_filter: log_filter,
            ..RawConfig::default()
        })
    }

    /// Build configuration from raw optional values.
    pub fn from_raw(raw: RawConfig) -> Result<Self, ConfigError> {
        let webapp_host = raw
            .webapp_host
            .unwrap_or_else(|| DEFAULT_WEBAPP_HOST.to_owned());
        let webapp_port = parse_u16("WEBAPP_PORT", raw.webapp_port, DEFAULT_WEBAPP_PORT)?;
        let bind_addr = raw
            .openplotva_bind_addr
            .unwrap_or_else(|| format!("{webapp_host}:{webapp_port}"));
        let log_level = raw
            .log_level
            .unwrap_or_else(|| DEFAULT_LOG_LEVEL.to_owned());
        let log_filter = raw.openplotva_log_filter.unwrap_or_else(|| {
            if log_level == DEFAULT_LOG_LEVEL {
                DEFAULT_LOG_FILTER.to_owned()
            } else {
                format!("openplotva={log_level},tower_http={log_level}")
            }
        });

        let postgres = PostgresConfig {
            host: raw
                .db_postgres_host
                .unwrap_or_else(|| DEFAULT_POSTGRES_HOST.to_owned()),
            port: parse_u16(
                "DB_POSTGRES_PORT",
                raw.db_postgres_port,
                DEFAULT_POSTGRES_PORT,
            )?,
            user: raw
                .db_postgres_user
                .unwrap_or_else(|| DEFAULT_POSTGRES_USER.to_owned()),
            password: raw
                .db_postgres_password
                .unwrap_or_else(|| DEFAULT_POSTGRES_PASSWORD.to_owned()),
            database: raw
                .db_postgres_db
                .unwrap_or_else(|| DEFAULT_POSTGRES_DATABASE.to_owned()),
            ssl_mode: raw
                .db_postgres_ssl_mode
                .unwrap_or_else(|| DEFAULT_POSTGRES_SSL_MODE.to_owned()),
        };

        Ok(Self {
            server: ServerConfig {
                host: webapp_host,
                port: webapp_port,
                bind_addr,
                url: raw
                    .webapp_url
                    .unwrap_or_else(|| DEFAULT_WEBAPP_URL.to_owned()),
            },
            observability: ObservabilityConfig {
                log_level,
                log_filter,
            },
            database: DatabaseConfig { postgres },
            redis: RedisConfig {
                host: raw
                    .redis_host
                    .unwrap_or_else(|| DEFAULT_REDIS_HOST.to_owned()),
                port: parse_u16("REDIS_PORT", raw.redis_port, DEFAULT_REDIS_PORT)?,
                password: raw.redis_password.unwrap_or_default(),
                db: parse_i64("REDIS_DB", raw.redis_db, DEFAULT_REDIS_DB)?,
            },
            reference_snapshot: ReferenceSnapshotConfig {
                repository: raw
                    .openplotva_reference_source_repository
                    .unwrap_or_else(|| DEFAULT_REFERENCE_SOURCE_REPOSITORY.to_owned())
                    .into(),
                lock_path: raw
                    .openplotva_reference_snapshot_path
                    .unwrap_or_else(|| DEFAULT_RUNTIME_CONTRACT_PATH.to_owned())
                    .into(),
                enforce: parse_bool(
                    "OPENPLOTVA_DISABLED_LEGACY_LOCK",
                    raw.openplotva_enforce_reference_snapshot,
                    DEFAULT_RUNTIME_CONTRACT_ENFORCE,
                )?,
            },
            service_probe: ServiceProbeConfig {
                connect_services: parse_bool(
                    "OPENPLOTVA_CONNECT_SERVICES",
                    raw.openplotva_connect_services,
                    DEFAULT_CONNECT_SERVICES,
                )?,
                run_migrations: parse_bool(
                    "OPENPLOTVA_RUN_MIGRATIONS",
                    raw.openplotva_run_migrations,
                    DEFAULT_RUN_MIGRATIONS,
                )?,
            },
        })
    }
}

impl RawConfig {
    /// Load raw values from process environment variables.
    pub fn from_env() -> Self {
        Self {
            openplotva_bind_addr: env("OPENPLOTVA_BIND_ADDR"),
            openplotva_log_filter: env("OPENPLOTVA_LOG_FILTER"),
            log_level: env("LOG_LEVEL"),
            webapp_host: env("WEBAPP_HOST"),
            webapp_port: env("WEBAPP_PORT"),
            webapp_url: env("WEBAPP_URL"),
            db_postgres_host: env("DB_POSTGRES_HOST"),
            db_postgres_port: env("DB_POSTGRES_PORT"),
            db_postgres_user: env("DB_POSTGRES_USER"),
            db_postgres_password: env("DB_POSTGRES_PASSWORD"),
            db_postgres_db: env("DB_POSTGRES_DB"),
            db_postgres_ssl_mode: env("DB_POSTGRES_SSL_MODE"),
            redis_host: env("REDIS_HOST"),
            redis_port: env("REDIS_PORT"),
            redis_password: env("REDIS_PASSWORD"),
            redis_db: env("REDIS_DB"),
            openplotva_reference_source_repository: env("OPENPLOTVA_REFERENCE_SOURCE_REPOSITORY"),
            openplotva_reference_snapshot_path: env("OPENPLOTVA_RUNTIME_CONTRACT_PATH"),
            openplotva_enforce_reference_snapshot: env("OPENPLOTVA_DISABLED_LEGACY_LOCK"),
            openplotva_connect_services: env("OPENPLOTVA_CONNECT_SERVICES"),
            openplotva_run_migrations: env("OPENPLOTVA_RUN_MIGRATIONS"),
        }
    }
}

fn load_dotenv() -> Result<(), ConfigError> {
    match dotenvy::from_path(".env") {
        Ok(_) => Ok(()),
        Err(dotenvy::Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ConfigError::Dotenv { source }),
    }
}

fn env(name: &'static str) -> Option<String> {
    std::env::var(name).ok()
}

fn parse_u16(name: &'static str, value: Option<String>, default: u16) -> Result<u16, ConfigError> {
    let Some(value) = value else {
        return Ok(default);
    };
    let parsed = value
        .parse::<i64>()
        .map_err(|source| ConfigError::InvalidInteger {
            name,
            value: value.clone(),
            source,
        })?;
    u16::try_from(parsed).map_err(|source| ConfigError::IntegerOutOfRange {
        name,
        value: parsed,
        source,
    })
}

fn parse_i64(name: &'static str, value: Option<String>, default: i64) -> Result<i64, ConfigError> {
    let Some(value) = value else {
        return Ok(default);
    };
    value
        .parse::<i64>()
        .map_err(|source| ConfigError::InvalidInteger {
            name,
            value,
            source,
        })
}

fn parse_bool(
    name: &'static str,
    value: Option<String>,
    default: bool,
) -> Result<bool, ConfigError> {
    let Some(value) = value else {
        return Ok(default);
    };
    match value.to_ascii_lowercase().as_str() {
        "1" | "t" | "true" => Ok(true),
        "0" | "f" | "false" => Ok(false),
        _ => Err(ConfigError::InvalidBoolean { name, value }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AppConfig, DEFAULT_REFERENCE_SOURCE_REPOSITORY, DEFAULT_LOG_FILTER, DEFAULT_RUNTIME_CONTRACT_PATH,
        DEFAULT_WEBAPP_URL, RawConfig,
    };

    #[test]
    fn defaults_match_go_service_spine_contract() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_raw(RawConfig::default())?;

        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.server.bind_addr, "0.0.0.0:8080");
        assert_eq!(config.server.url, DEFAULT_WEBAPP_URL);
        assert_eq!(config.observability.log_level, "info");
        assert_eq!(config.observability.log_filter, DEFAULT_LOG_FILTER);
        assert_eq!(
            config.database.postgres.go_startup_dsn(),
            "postgres://plotva:plotva@127.0.0.1:5432/plotva?sslmode=disable"
        );
        assert_eq!(config.database.postgres.ssl_mode, "disable");
        assert_eq!(config.redis.host, "127.0.0.1");
        assert_eq!(config.redis.port, 6379);
        assert_eq!(config.redis.password, "");
        assert_eq!(config.redis.db, 0);
        assert_eq!(
            config.reference_snapshot.repository.to_string_lossy(),
            DEFAULT_REFERENCE_SOURCE_REPOSITORY
        );
        assert_eq!(
            config.reference_snapshot.lock_path.to_string_lossy(),
            DEFAULT_RUNTIME_CONTRACT_PATH
        );
        assert!(config.reference_snapshot.enforce);
        assert!(!config.service_probe.connect_services);
        assert!(!config.service_probe.run_migrations);

        Ok(())
    }

    #[test]
    fn rust_bind_override_keeps_local_shell_flexible() -> Result<(), super::ConfigError> {
        let config = AppConfig::from_values(
            Some("127.0.0.1:18080".to_owned()),
            Some("openplotva=debug".to_owned()),
        )?;

        assert_eq!(config.server.bind_addr, "127.0.0.1:18080");
        assert_eq!(config.observability.log_filter, "openplotva=debug");

        Ok(())
    }

    #[test]
    fn invalid_port_is_rejected() {
        let error = AppConfig::from_raw(RawConfig {
            webapp_port: Some("99999".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::IntegerOutOfRange {
                name: "WEBAPP_PORT",
                ..
            })
        ));
    }

    #[test]
    fn invalid_bool_is_rejected() {
        let error = AppConfig::from_raw(RawConfig {
            openplotva_connect_services: Some("maybe".to_owned()),
            ..RawConfig::default()
        })
        .err();

        assert!(matches!(
            error,
            Some(super::ConfigError::InvalidBoolean {
                name: "OPENPLOTVA_CONNECT_SERVICES",
                ..
            })
        ));
    }
}
