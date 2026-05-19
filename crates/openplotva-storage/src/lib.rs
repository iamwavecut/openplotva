//! Storage boundary for Postgres, pgvector, SQLx, Redis, and Dragonfly.

use std::time::Duration;

use openplotva_config::{PostgresConfig, RedisConfig};
use redis::{
    Client as RedisClient, ConnectionAddr, ConnectionInfo, IntoConnectionInfo, RedisConnectionInfo,
};
use sqlx::{
    PgPool,
    migrate::{MigrateError, Migrator},
    postgres::PgPoolOptions,
};
use thiserror::Error;

/// Human-readable crate purpose used by scaffold tests and docs.
pub const PURPOSE: &str = "storage";

const POSTGRES_MAX_CONNECTIONS: u32 = 50;
const POSTGRES_MIN_CONNECTIONS: u32 = 10;
const POSTGRES_MAX_CONNECTION_LIFETIME: Duration = Duration::from_secs(45 * 60);

/// SQLx migrator for the converted Go schema migrations.
pub static MIGRATOR: Migrator = sqlx::migrate!("../../migrations");

/// Connected service clients kept alive by the application shell.
#[derive(Clone, Debug)]
pub struct ServiceClients {
    /// SQLx Postgres pool.
    pub postgres: PgPool,
    /// Redis client that has passed a startup ping.
    pub redis: RedisStore,
    /// Whether SQLx migrations were applied during startup.
    pub migrations_applied: bool,
}

/// Redis client wrapper.
#[derive(Clone, Debug)]
pub struct RedisStore {
    client: RedisClient,
}

impl RedisStore {
    /// Access the underlying Redis client.
    pub fn client(&self) -> &RedisClient {
        &self.client
    }
}

/// Storage connection failures.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Postgres connection or ping failed.
    #[error("failed to connect to Postgres: {source}")]
    Postgres {
        /// SQLx error.
        #[from]
        source: sqlx::Error,
    },
    /// Redis client configuration failed.
    #[error("invalid Redis configuration: {source}")]
    RedisConfig {
        /// Redis configuration error.
        source: redis::RedisError,
    },
    /// Redis connection or ping failed.
    #[error("failed to connect to Redis: {source}")]
    Redis {
        /// Redis connection error.
        source: redis::RedisError,
    },
    /// SQLx migration execution failed.
    #[error("failed to apply SQL migrations: {source}")]
    Migrate {
        /// SQLx migration error.
        #[from]
        source: MigrateError,
    },
}

/// Connect to Postgres and Redis using the current service-spine settings.
pub async fn connect_service_clients(
    postgres: &PostgresConfig,
    redis: &RedisConfig,
    run_migrations: bool,
) -> Result<ServiceClients, StorageError> {
    let postgres = connect_postgres(postgres).await?;
    if run_migrations {
        run_migrations_on(&postgres).await?;
    }
    let redis = connect_redis(redis).await?;

    Ok(ServiceClients {
        postgres,
        redis,
        migrations_applied: run_migrations,
    })
}

pub async fn connect_postgres(config: &PostgresConfig) -> Result<PgPool, StorageError> {
    PgPoolOptions::new()
        .max_connections(POSTGRES_MAX_CONNECTIONS)
        .min_connections(POSTGRES_MIN_CONNECTIONS)
        .max_lifetime(POSTGRES_MAX_CONNECTION_LIFETIME)
        .connect(&config.go_startup_dsn())
        .await
        .map_err(StorageError::from)
}

/// Apply all pending SQLx migrations to an already connected Postgres pool.
pub async fn run_migrations_on(pool: &PgPool) -> Result<(), StorageError> {
    MIGRATOR.run(pool).await.map_err(StorageError::from)
}

/// Connect to Redis/Dragonfly and verify the selected DB with `PING`.
pub async fn connect_redis(config: &RedisConfig) -> Result<RedisStore, StorageError> {
    let client = RedisClient::open(
        redis_connection_info(config).map_err(|source| StorageError::RedisConfig { source })?,
    )
    .map_err(|source| StorageError::RedisConfig { source })?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|source| StorageError::Redis { source })?;
    let _: String = redis::cmd("PING")
        .query_async(&mut connection)
        .await
        .map_err(|source| StorageError::Redis { source })?;

    Ok(RedisStore { client })
}

fn redis_connection_info(config: &RedisConfig) -> redis::RedisResult<ConnectionInfo> {
    let redis_settings = if config.password.is_empty() {
        RedisConnectionInfo::default().set_db(config.db)
    } else {
        RedisConnectionInfo::default()
            .set_db(config.db)
            .set_password(&config.password)
    };

    ConnectionAddr::Tcp(config.host.clone(), config.port)
        .into_connection_info()
        .map(|info| info.set_redis_settings(redis_settings))
}

#[cfg(test)]
mod tests {
    use std::{error::Error, fs, path::Path};

    use openplotva_config::{DEFAULT_REDIS_DB, RedisConfig};
    use redis::ConnectionAddr;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct MigrationInventoryEntry {
        path: String,
        sha256: String,
    }

    #[test]
    fn converted_migration_corpus_is_embedded() {
        assert_eq!(super::MIGRATOR.iter().count(), 96);
    }

    #[test]
    fn converted_migrations_reference_frozen_go_inventory() -> Result<(), Box<dyn Error>> {
        let inventory = include_str!("../../../docs/contract/generated/migrations.json");
        let entries: Vec<MigrationInventoryEntry> = serde_json::from_str(inventory)?;
        assert_eq!(entries.len(), 48);

        for entry in entries {
            let source_file = Path::new(&entry.path)
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or("migration inventory path has no file name")?;
            let stem = source_file
                .strip_suffix(".sql")
                .ok_or("migration inventory path is not a SQL file")?;
            let up_path = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join(format!("../../migrations/{stem}.up.sql"));
            let down_path = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join(format!("../../migrations/{stem}.down.sql"));

            let up = fs::read_to_string(up_path)?;
            let down = fs::read_to_string(down_path)?;
            let source_header =
                format!("-- Converted from reference-app/internal/db/sql/migrations/{source_file}");
            let hash_header = format!("-- Source SHA-256: {}", entry.sha256);

            assert!(up.contains(&source_header));
            assert!(up.contains(&hash_header));
            assert!(down.contains(&source_header));
            assert!(down.contains(&hash_header));
            assert!(!up.contains("+migrate"));
            assert!(!down.contains("+migrate"));
        }

        Ok(())
    }

    #[test]
    fn redis_connection_info_matches_go_cache_defaults() -> Result<(), redis::RedisError> {
        let config = RedisConfig {
            host: "127.0.0.1".to_owned(),
            port: 6379,
            password: String::new(),
            db: DEFAULT_REDIS_DB,
        };

        let info = super::redis_connection_info(&config)?;

        assert_eq!(
            info.addr(),
            &ConnectionAddr::Tcp("127.0.0.1".to_owned(), 6379)
        );
        assert_eq!(info.redis_settings().db(), 0);
        assert_eq!(info.redis_settings().password(), None);

        Ok(())
    }
}
