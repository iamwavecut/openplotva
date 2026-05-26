//! Runtime API token manager and app-level runtime API wiring helpers.

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use openplotva_server::{
    RUNTIME_API_TOKEN_ID_BYTES, RUNTIME_API_TOKEN_SECRET_SIZE, RUNTIME_API_TOKEN_TTL, RuntimeToken,
    RuntimeTokenParseError, RuntimeTokenValidationFuture, RuntimeTokenValidator,
    format_runtime_token, hash_runtime_token_secret, parse_runtime_token, runtime_token_expired_at,
    runtime_token_secret_hash_matches,
};
use openplotva_storage::{RuntimeApiTokenRecord, StorageError};
use rand::RngExt;
use thiserror::Error;
use time::OffsetDateTime;

/// Boxed future returned by runtime API token stores.
pub type RuntimeTokenStoreFuture<'a, Output, Error> =
    Pin<Box<dyn Future<Output = Result<Output, Error>> + Send + 'a>>;

/// Storage boundary used by the runtime API token manager.
pub trait RuntimeTokenStore {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Insert a runtime API token row.
    fn create_runtime_api_token<'a>(
        &'a self,
        id: &'a str,
        token_hash: &'a [u8],
    ) -> RuntimeTokenStoreFuture<'a, RuntimeApiTokenRecord, Self::Error>;

    /// Load a runtime API token row by token ID.
    fn runtime_api_token<'a>(
        &'a self,
        id: &'a str,
    ) -> RuntimeTokenStoreFuture<'a, Option<RuntimeApiTokenRecord>, Self::Error>;

    /// List runtime API token rows created at or after the cutoff.
    fn runtime_api_tokens_created_since<'a>(
        &'a self,
        cutoff: OffsetDateTime,
    ) -> RuntimeTokenStoreFuture<'a, Vec<RuntimeApiTokenRecord>, Self::Error>;

    /// Delete runtime API token rows older than the cutoff.
    fn delete_runtime_api_tokens_older_than<'a>(
        &'a self,
        cutoff: OffsetDateTime,
    ) -> RuntimeTokenStoreFuture<'a, u64, Self::Error>;
}

impl RuntimeTokenStore for openplotva_storage::PostgresRuntimeTokenStore {
    type Error = StorageError;

    fn create_runtime_api_token<'a>(
        &'a self,
        id: &'a str,
        token_hash: &'a [u8],
    ) -> RuntimeTokenStoreFuture<'a, RuntimeApiTokenRecord, Self::Error> {
        Box::pin(async move { self.create_runtime_api_token(id, token_hash).await })
    }

    fn runtime_api_token<'a>(
        &'a self,
        id: &'a str,
    ) -> RuntimeTokenStoreFuture<'a, Option<RuntimeApiTokenRecord>, Self::Error> {
        Box::pin(async move { self.get_runtime_api_token(id).await })
    }

    fn runtime_api_tokens_created_since<'a>(
        &'a self,
        cutoff: OffsetDateTime,
    ) -> RuntimeTokenStoreFuture<'a, Vec<RuntimeApiTokenRecord>, Self::Error> {
        Box::pin(async move { self.list_runtime_api_tokens_created_since(cutoff).await })
    }

    fn delete_runtime_api_tokens_older_than<'a>(
        &'a self,
        cutoff: OffsetDateTime,
    ) -> RuntimeTokenStoreFuture<'a, u64, Self::Error> {
        Box::pin(async move { self.delete_runtime_api_tokens_older_than(cutoff).await })
    }
}

/// Runtime API token-manager error.
#[derive(Debug, Error)]
pub enum RuntimeTokenManagerError<StoreError> {
    #[error("{0}")]
    Parse(#[from] RuntimeTokenParseError),
    /// Token row is missing.
    #[error("runtime token not found")]
    NotFound,
    /// Token row is expired.
    #[error("runtime token expired")]
    Expired,
    /// Presented secret hash does not match the stored hash.
    #[error("runtime token hash mismatch")]
    HashMismatch,
    /// Storage call failed.
    #[error("{0}")]
    Store(StoreError),
}

/// Result returned when issuing a new runtime API token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IssuedRuntimeToken {
    pub token: String,
    /// Public token metadata.
    pub meta: RuntimeToken,
}

#[derive(Clone)]
pub struct RuntimeTokenManager<Store> {
    store: Store,
    now: Arc<dyn Fn() -> OffsetDateTime + Send + Sync>,
}

impl<Store> RuntimeTokenManager<Store> {
    /// Build a runtime token manager using the real clock.
    pub fn new(store: Store) -> Self {
        Self {
            store,
            now: Arc::new(OffsetDateTime::now_utc),
        }
    }

    /// Override the clock for tests.
    pub fn with_now(mut self, now: impl Fn() -> OffsetDateTime + Send + Sync + 'static) -> Self {
        self.now = Arc::new(now);
        self
    }
}

impl<Store> RuntimeTokenManager<Store>
where
    Store: RuntimeTokenStore + Send + Sync,
{
    #[must_use]
    pub fn ttl(&self) -> time::Duration {
        RUNTIME_API_TOKEN_TTL
    }

    /// Issue and persist a runtime API token.
    pub async fn issue(
        &self,
    ) -> Result<IssuedRuntimeToken, RuntimeTokenManagerError<Store::Error>> {
        self.cleanup_expired().await;
        let token_id = random_hex(RUNTIME_API_TOKEN_ID_BYTES);
        let secret = random_hex(RUNTIME_API_TOKEN_SECRET_SIZE);
        let secret_hash = hash_runtime_token_secret(&secret);
        let row = self
            .store
            .create_runtime_api_token(&token_id, &secret_hash)
            .await
            .map_err(RuntimeTokenManagerError::Store)?;
        Ok(IssuedRuntimeToken {
            token: format_runtime_token(&token_id, &secret),
            meta: runtime_token_from_record(row),
        })
    }

    pub async fn list(&self) -> Result<Vec<RuntimeToken>, RuntimeTokenManagerError<Store::Error>> {
        self.cleanup_expired().await;
        let cutoff = (self.now)() - RUNTIME_API_TOKEN_TTL;
        let rows = self
            .store
            .runtime_api_tokens_created_since(cutoff)
            .await
            .map_err(RuntimeTokenManagerError::Store)?;
        Ok(rows.into_iter().map(runtime_token_from_record).collect())
    }

    /// Validate a full bearer token and return token metadata on success.
    pub async fn validate(
        &self,
        token: &str,
    ) -> Result<RuntimeToken, RuntimeTokenManagerError<Store::Error>> {
        let parsed = parse_runtime_token(token)?;
        let row = self
            .store
            .runtime_api_token(&parsed.id)
            .await
            .map_err(RuntimeTokenManagerError::Store)?
            .ok_or(RuntimeTokenManagerError::NotFound)?;
        if runtime_token_expired_at(Some(row.created_at), (self.now)()) {
            self.cleanup_expired().await;
            return Err(RuntimeTokenManagerError::Expired);
        }
        if !runtime_token_secret_hash_matches(&row.token_hash, &parsed.secret) {
            return Err(RuntimeTokenManagerError::HashMismatch);
        }
        Ok(runtime_token_from_record(row))
    }

    async fn cleanup_expired(&self) {
        let cutoff = (self.now)() - RUNTIME_API_TOKEN_TTL;
        if let Err(error) = self
            .store
            .delete_runtime_api_tokens_older_than(cutoff)
            .await
        {
            tracing::warn!(%error, "failed to clean up expired runtime API tokens");
        }
    }
}

impl<Store> RuntimeTokenValidator for RuntimeTokenManager<Store>
where
    Store: RuntimeTokenStore + Send + Sync,
{
    fn validate_runtime_token<'a>(&'a self, token: &'a str) -> RuntimeTokenValidationFuture<'a> {
        Box::pin(async move { self.validate(token).await.is_ok() })
    }
}

fn runtime_token_from_record(row: RuntimeApiTokenRecord) -> RuntimeToken {
    RuntimeToken {
        id: row.id,
        created_at: Some(row.created_at),
    }
}

fn random_hex(size: usize) -> String {
    let mut bytes = vec![0_u8; size];
    rand::rng().fill(&mut bytes);
    hex::encode(bytes)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use openplotva_storage::RuntimeApiTokenRecord;
    use time::{Duration as TimeDuration, OffsetDateTime};

    use super::{RuntimeTokenManager, RuntimeTokenStore, RuntimeTokenStoreFuture};

    #[derive(Clone, Debug)]
    struct MemoryTokenStore {
        rows: Arc<Mutex<Vec<RuntimeApiTokenRecord>>>,
        now: OffsetDateTime,
    }

    impl Default for MemoryTokenStore {
        fn default() -> Self {
            Self {
                rows: Arc::new(Mutex::new(Vec::new())),
                now: OffsetDateTime::UNIX_EPOCH,
            }
        }
    }

    impl RuntimeTokenStore for MemoryTokenStore {
        type Error = String;

        fn create_runtime_api_token<'a>(
            &'a self,
            id: &'a str,
            token_hash: &'a [u8],
        ) -> RuntimeTokenStoreFuture<'a, RuntimeApiTokenRecord, Self::Error> {
            Box::pin(async move {
                let row = RuntimeApiTokenRecord {
                    id: id.to_owned(),
                    token_hash: token_hash.to_vec(),
                    created_at: self.now,
                };
                self.rows
                    .lock()
                    .map_err(|error| error.to_string())?
                    .push(row.clone());
                Ok(row)
            })
        }

        fn runtime_api_token<'a>(
            &'a self,
            id: &'a str,
        ) -> RuntimeTokenStoreFuture<'a, Option<RuntimeApiTokenRecord>, Self::Error> {
            Box::pin(async move {
                Ok(self
                    .rows
                    .lock()
                    .map_err(|error| error.to_string())?
                    .iter()
                    .find(|row| row.id == id)
                    .cloned())
            })
        }

        fn runtime_api_tokens_created_since<'a>(
            &'a self,
            cutoff: OffsetDateTime,
        ) -> RuntimeTokenStoreFuture<'a, Vec<RuntimeApiTokenRecord>, Self::Error> {
            Box::pin(async move {
                let mut rows: Vec<_> = self
                    .rows
                    .lock()
                    .map_err(|error| error.to_string())?
                    .iter()
                    .filter(|row| row.created_at >= cutoff)
                    .cloned()
                    .collect();
                rows.sort_by(|left, right| {
                    right
                        .created_at
                        .cmp(&left.created_at)
                        .then_with(|| left.id.cmp(&right.id))
                });
                Ok(rows)
            })
        }

        fn delete_runtime_api_tokens_older_than<'a>(
            &'a self,
            cutoff: OffsetDateTime,
        ) -> RuntimeTokenStoreFuture<'a, u64, Self::Error> {
            Box::pin(async move {
                let mut rows = self.rows.lock().map_err(|error| error.to_string())?;
                let before = rows.len();
                rows.retain(|row| row.created_at >= cutoff);
                u64::try_from(before - rows.len()).map_err(|error| error.to_string())
            })
        }
    }

    #[tokio::test]
    async fn runtime_token_manager_issues_lists_and_validates_like_go() {
        let now = OffsetDateTime::UNIX_EPOCH + TimeDuration::days(30);
        let store = MemoryTokenStore {
            now,
            ..MemoryTokenStore::default()
        };
        let manager = RuntimeTokenManager::new(store).with_now(move || now);

        let issued = manager
            .issue()
            .await
            .unwrap_or_else(|error| panic!("issue token: {error}"));
        assert_eq!(issued.meta.created_at, Some(now));
        let listed = manager
            .list()
            .await
            .unwrap_or_else(|error| panic!("list tokens: {error}"));
        assert_eq!(listed, vec![issued.meta.clone()]);
        let validated = manager
            .validate(&issued.token)
            .await
            .unwrap_or_else(|error| panic!("validate token: {error}"));
        assert_eq!(validated, issued.meta);
        assert!(manager.validate("prt_missing.secret").await.is_err());
    }

    #[tokio::test]
    async fn runtime_token_manager_deletes_expired_tokens_before_listing() {
        let now = OffsetDateTime::UNIX_EPOCH + TimeDuration::days(30);
        let expired = RuntimeApiTokenRecord {
            id: "old".to_owned(),
            token_hash: Vec::new(),
            created_at: now - TimeDuration::hours(25),
        };
        let store = MemoryTokenStore {
            rows: Arc::new(Mutex::new(vec![expired])),
            now,
        };
        let manager = RuntimeTokenManager::new(store).with_now(move || now);

        let listed = manager
            .list()
            .await
            .unwrap_or_else(|error| panic!("list tokens: {error}"));
        assert!(listed.is_empty());
    }
}
