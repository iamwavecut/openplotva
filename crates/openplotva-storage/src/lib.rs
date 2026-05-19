//! Storage boundary for Postgres, pgvector, SQLx, Redis, and Dragonfly.

use std::time::Duration;

use openplotva_config::{PostgresConfig, RedisConfig};
use redis::{
    Client as RedisClient, ConnectionAddr, ConnectionInfo, IntoConnectionInfo, RedisConnectionInfo,
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use thiserror::Error;

/// Human-readable crate purpose used by scaffold tests and docs.
pub const PURPOSE: &str = "storage";

const POSTGRES_MAX_CONNECTIONS: u32 = 50;
const POSTGRES_MIN_CONNECTIONS: u32 = 10;
const POSTGRES_MAX_CONNECTION_LIFETIME: Duration = Duration::from_secs(45 * 60);

/// Connected service clients kept alive by the application shell.
#[derive(Clone, Debug)]
pub struct ServiceClients {
    /// SQLx Postgres pool.
    pub postgres: PgPool,
    /// Redis client that has passed a startup ping.
    pub redis: RedisStore,
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
}

/// Connect to Postgres and Redis using the current service-spine settings.
pub async fn connect_service_clients(
    postgres: &PostgresConfig,
    redis: &RedisConfig,
) -> Result<ServiceClients, StorageError> {
    let postgres = connect_postgres(postgres).await?;
    let redis = connect_redis(redis).await?;

    Ok(ServiceClients { postgres, redis })
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
    use openplotva_config::{DEFAULT_REDIS_DB, RedisConfig};
    use redis::ConnectionAddr;

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
