//! App-level Telegram rate-limit policy matching Go server behavior.

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Mutex, MutexGuard},
    time::Duration,
};

use time::OffsetDateTime;

/// Go server in-memory policy cache TTL.
pub const GO_RATE_LIMIT_CACHE_TTL: time::Duration = time::Duration::minutes(30);

/// Boxed future returned by rate-limit persistence stores.
pub type RateLimitStoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Persistent store boundary for chat rate-limit expiries.
pub trait RateLimitStore {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Load one chat's persisted expiry.
    fn load_expiry<'a>(
        &'a self,
        chat_id: i64,
    ) -> RateLimitStoreFuture<'a, Result<Option<OffsetDateTime>, Self::Error>>;

    /// Save one chat's expiry with the Telegram retry duration as TTL.
    fn save_expiry<'a>(
        &'a self,
        chat_id: i64,
        expiry: OffsetDateTime,
        ttl: Duration,
    ) -> RateLimitStoreFuture<'a, Result<(), Self::Error>>;
}

impl RateLimitStore for openplotva_storage::RedisRateLimitStore {
    type Error = openplotva_storage::StorageError;

    fn load_expiry<'a>(
        &'a self,
        chat_id: i64,
    ) -> RateLimitStoreFuture<'a, Result<Option<OffsetDateTime>, Self::Error>> {
        Box::pin(async move { self.chat_rate_limit_expiry(chat_id).await })
    }

    fn save_expiry<'a>(
        &'a self,
        chat_id: i64,
        expiry: OffsetDateTime,
        ttl: Duration,
    ) -> RateLimitStoreFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move { self.set_chat_rate_limit(chat_id, expiry, ttl).await })
    }
}

/// Source used to decide the latest rate-limit check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RateLimitCheckSource {
    /// The in-memory Go policy cache had an entry.
    Cache,
    /// Redis/Dragonfly had an active persisted expiry.
    PersistentStore,
    /// Redis/Dragonfly had no active persisted expiry.
    Missing,
    /// Redis/Dragonfly returned an error, which Go treats as not rate-limited.
    LoadError,
}

/// Result of checking one chat's rate-limit state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RateLimitCheckReport {
    /// Whether the chat is still rate-limited.
    pub rate_limited: bool,
    /// Source used by the check.
    pub source: RateLimitCheckSource,
    pub load_error: Option<String>,
}

/// Result of recording one Telegram retry window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RateLimitSetReport {
    /// Absolute retry expiry stored in memory and persistence.
    pub expiry: OffsetDateTime,
    /// Persistent save error ignored after updating the in-memory policy cache.
    pub save_error: Option<String>,
}

#[derive(Debug)]
pub struct ChatRateLimitPolicy<S> {
    store: S,
    cache: Mutex<HashMap<i64, CachedRateLimit>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CachedRateLimit {
    expiry: OffsetDateTime,
    cached_until: OffsetDateTime,
}

impl<S> ChatRateLimitPolicy<S> {
    /// Build an empty policy cache over a persistent store.
    pub fn new(store: S) -> Self {
        Self {
            store,
            cache: Mutex::new(HashMap::new()),
        }
    }
}

impl<S> ChatRateLimitPolicy<S>
where
    S: RateLimitStore + Send + Sync,
{
    /// Return whether a chat is rate-limited at `now`.
    pub async fn is_rate_limited_at(
        &self,
        chat_id: i64,
        now: OffsetDateTime,
    ) -> RateLimitCheckReport {
        if let Some(cached) = self.cached_expiry(chat_id, now) {
            return RateLimitCheckReport {
                rate_limited: now < cached,
                source: RateLimitCheckSource::Cache,
                load_error: None,
            };
        }

        let expiry = match self.store.load_expiry(chat_id).await {
            Ok(expiry) => expiry,
            Err(error) => {
                return RateLimitCheckReport {
                    rate_limited: false,
                    source: RateLimitCheckSource::LoadError,
                    load_error: Some(error.to_string()),
                };
            }
        };

        if let Some(expiry) = expiry.filter(|expiry| now < *expiry) {
            self.cache_expiry(chat_id, expiry, now);
            return RateLimitCheckReport {
                rate_limited: true,
                source: RateLimitCheckSource::PersistentStore,
                load_error: None,
            };
        }

        RateLimitCheckReport {
            rate_limited: false,
            source: RateLimitCheckSource::Missing,
            load_error: None,
        }
    }

    /// Record a Telegram retry duration in memory and persistent storage.
    pub async fn set_rate_limit_at(
        &self,
        chat_id: i64,
        retry_after: Duration,
        now: OffsetDateTime,
    ) -> RateLimitSetReport {
        let expiry = now + time_duration(retry_after);
        self.cache_expiry(chat_id, expiry, now);
        let save_error = self
            .store
            .save_expiry(chat_id, expiry, retry_after)
            .await
            .err()
            .map(|error| error.to_string());
        RateLimitSetReport { expiry, save_error }
    }

    fn cached_expiry(&self, chat_id: i64, now: OffsetDateTime) -> Option<OffsetDateTime> {
        let mut cache = self.cache();
        let cached = cache.get(&chat_id).copied()?;
        if now > cached.cached_until {
            cache.remove(&chat_id);
            return None;
        }
        Some(cached.expiry)
    }

    fn cache_expiry(&self, chat_id: i64, expiry: OffsetDateTime, now: OffsetDateTime) {
        self.cache().insert(
            chat_id,
            CachedRateLimit {
                expiry,
                cached_until: now + GO_RATE_LIMIT_CACHE_TTL,
            },
        );
    }

    fn cache(&self) -> MutexGuard<'_, HashMap<i64, CachedRateLimit>> {
        match self.cache.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

fn time_duration(duration: Duration) -> time::Duration {
    time::Duration::try_from(duration).unwrap_or(time::Duration::MAX)
}

pub fn telegram_retry_after_from_execute_error(
    error: &carapax::api::ExecuteError,
) -> Option<Duration> {
    match error {
        carapax::api::ExecuteError::Response(response) if response.error_code() == Some(429) => {
            Some(Duration::from_secs(response.retry_after().unwrap_or(60)))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        fmt,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use time::OffsetDateTime;

    use super::{
        ChatRateLimitPolicy, RateLimitCheckSource, RateLimitStore, RateLimitStoreFuture,
        telegram_retry_after_from_execute_error,
    };

    #[tokio::test]
    async fn policy_sets_cache_and_persistent_expiry_from_retry_duration()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let retry_after = Duration::from_secs(17);
        let store = RateLimitStoreStub::default();
        let policy = ChatRateLimitPolicy::new(store.clone());

        let report = policy.set_rate_limit_at(42, retry_after, now).await;

        assert_eq!(report.expiry, now + time::Duration::seconds(17));
        assert!(report.save_error.is_none());
        assert_eq!(
            store.saved_rows(),
            vec![SavedRateLimit {
                chat_id: 42,
                expiry: report.expiry,
                ttl: retry_after,
            }]
        );
        let active = policy
            .is_rate_limited_at(42, now + time::Duration::seconds(16))
            .await;
        assert!(active.rate_limited);
        assert_eq!(active.source, RateLimitCheckSource::Cache);
        let boundary = policy.is_rate_limited_at(42, report.expiry).await;
        assert!(!boundary.rate_limited);
        assert_eq!(boundary.source, RateLimitCheckSource::Cache);

        Ok(())
    }

    #[tokio::test]
    async fn policy_uses_cache_before_persistent_store_like_go() -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let expiry = now + time::Duration::seconds(60);
        let store = RateLimitStoreStub::default();
        store.set_loaded_expiry(Some(expiry));
        let policy = ChatRateLimitPolicy::new(store.clone());

        let loaded = policy.is_rate_limited_at(7, now).await;
        assert!(loaded.rate_limited);
        assert_eq!(loaded.source, RateLimitCheckSource::PersistentStore);
        assert_eq!(store.load_count(), 1);

        store.set_load_error(Some("redis down"));
        let cached = policy
            .is_rate_limited_at(7, now + time::Duration::seconds(30))
            .await;
        assert!(cached.rate_limited);
        assert_eq!(cached.source, RateLimitCheckSource::Cache);
        assert_eq!(store.load_count(), 1);

        let expired_from_cache = policy.is_rate_limited_at(7, expiry).await;
        assert!(!expired_from_cache.rate_limited);
        assert_eq!(expired_from_cache.source, RateLimitCheckSource::Cache);
        assert_eq!(store.load_count(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn policy_treats_persistent_load_errors_as_not_limited_like_go()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let store = RateLimitStoreStub::default();
        store.set_load_error(Some("decode rate limit expiry"));
        let policy = ChatRateLimitPolicy::new(store);

        let report = policy.is_rate_limited_at(99, now).await;

        assert!(!report.rate_limited);
        assert_eq!(report.source, RateLimitCheckSource::LoadError);
        assert_eq!(
            report.load_error.as_deref(),
            Some("decode rate limit expiry")
        );

        Ok(())
    }

    #[test]
    fn telegram_retry_after_uses_go_429_default_and_response_parameters()
    -> Result<(), Box<dyn Error>> {
        let with_retry = response_error(
            r#"{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":13}}"#,
        )?;
        assert_eq!(
            telegram_retry_after_from_execute_error(&with_retry),
            Some(Duration::from_secs(13))
        );

        let without_retry =
            response_error(r#"{"ok":false,"error_code":429,"description":"Too Many Requests"}"#)?;
        assert_eq!(
            telegram_retry_after_from_execute_error(&without_retry),
            Some(Duration::from_secs(60))
        );

        let non_429_with_retry = response_error(
            r#"{"ok":false,"error_code":400,"description":"Bad Request","parameters":{"retry_after":13}}"#,
        )?;
        assert_eq!(
            telegram_retry_after_from_execute_error(&non_429_with_retry),
            None
        );

        Ok(())
    }

    fn response_error(json: &str) -> Result<carapax::api::ExecuteError, Box<dyn Error>> {
        let response: carapax::types::Response<serde_json::Value> = serde_json::from_str(json)?;
        match response.into_result() {
            Ok(_) => Err("test response unexpectedly succeeded".into()),
            Err(error) => Ok(carapax::api::ExecuteError::Response(error)),
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct SavedRateLimit {
        chat_id: i64,
        expiry: OffsetDateTime,
        ttl: Duration,
    }

    #[derive(Clone, Default)]
    struct RateLimitStoreStub {
        state: Arc<Mutex<RateLimitStoreState>>,
    }

    #[derive(Default)]
    struct RateLimitStoreState {
        loaded_expiry: Option<OffsetDateTime>,
        load_error: Option<&'static str>,
        save_error: Option<&'static str>,
        load_count: usize,
        saved: Vec<SavedRateLimit>,
    }

    #[derive(Debug)]
    struct RateLimitStoreStubError(&'static str);

    impl fmt::Display for RateLimitStoreStubError {
        fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
            out.write_str(self.0)
        }
    }

    impl Error for RateLimitStoreStubError {}

    impl RateLimitStoreStub {
        fn set_loaded_expiry(&self, expiry: Option<OffsetDateTime>) {
            self.state().loaded_expiry = expiry;
        }

        fn set_load_error(&self, error: Option<&'static str>) {
            self.state().load_error = error;
        }

        fn load_count(&self) -> usize {
            self.state().load_count
        }

        fn saved_rows(&self) -> Vec<SavedRateLimit> {
            self.state().saved.clone()
        }

        fn state(&self) -> std::sync::MutexGuard<'_, RateLimitStoreState> {
            match self.state.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            }
        }
    }

    impl RateLimitStore for RateLimitStoreStub {
        type Error = RateLimitStoreStubError;

        fn load_expiry<'a>(
            &'a self,
            _chat_id: i64,
        ) -> RateLimitStoreFuture<'a, Result<Option<OffsetDateTime>, Self::Error>> {
            Box::pin(async move {
                let mut state = self.state();
                state.load_count += 1;
                if let Some(error) = state.load_error {
                    return Err(RateLimitStoreStubError(error));
                }
                Ok(state.loaded_expiry)
            })
        }

        fn save_expiry<'a>(
            &'a self,
            chat_id: i64,
            expiry: OffsetDateTime,
            ttl: Duration,
        ) -> RateLimitStoreFuture<'a, Result<(), Self::Error>> {
            Box::pin(async move {
                let mut state = self.state();
                state.saved.push(SavedRateLimit {
                    chat_id,
                    expiry,
                    ttl,
                });
                if let Some(error) = state.save_error {
                    return Err(RateLimitStoreStubError(error));
                }
                Ok(())
            })
        }
    }
}
