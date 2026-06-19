use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use time::OffsetDateTime;

pub const GO_RATE_LIMIT_CACHE_TTL: time::Duration = time::Duration::minutes(30);
pub const TASK_ENQUEUE_WINDOW: Duration = Duration::from_secs(60);
pub const TASK_ENQUEUE_TTL: Duration = Duration::from_secs(10 * 60);
pub const TASK_ENQUEUE_USER_CHAT_LIMIT: usize = 30;
pub const TASK_ENQUEUE_CHAT_LIMIT: usize = 300;
pub const TASK_ENQUEUE_GLOBAL_LIMIT: usize = 3_000;

/// Boxed future returned by rate-limit persistence stores.
pub type RateLimitStoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type TaskEnqueueRateLimitFuture<'a> =
    Pin<Box<dyn Future<Output = TaskEnqueueRateLimitReport> + Send + 'a>>;
pub type TaskEnqueueRateLimitGrowFuture<'a> =
    Pin<Box<dyn Future<Output = TaskEnqueueRateLimitGrowReport> + Send + 'a>>;

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

pub trait TaskEnqueueRateLimitStore {
    type Error: fmt::Display + Send + Sync + 'static;

    /// Prune entries older than `cutoff_unix_ms` and return the current window count.
    fn count_task_enqueue<'a>(
        &'a self,
        key: &'a str,
        cutoff_unix_ms: i64,
    ) -> RateLimitStoreFuture<'a, Result<usize, Self::Error>>;

    /// Atomically add one entry (`member`, scored at `score_unix_ms`), prune entries
    /// older than `cutoff_unix_ms`, refresh the TTL, and return the resulting count.
    /// A sorted-set ADD is atomic, so concurrent enqueues never lose increments.
    fn record_task_enqueue<'a>(
        &'a self,
        key: &'a str,
        score_unix_ms: i64,
        member: &'a str,
        cutoff_unix_ms: i64,
        ttl: Duration,
    ) -> RateLimitStoreFuture<'a, Result<usize, Self::Error>>;
}

pub trait TaskEnqueueRateLimit: Send + Sync {
    fn check_task_enqueue_rate_limit<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
        now: OffsetDateTime,
    ) -> TaskEnqueueRateLimitFuture<'a>;

    fn grow_task_enqueue_rate_limit<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
        now: OffsetDateTime,
    ) -> TaskEnqueueRateLimitGrowFuture<'a>;
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

impl TaskEnqueueRateLimitStore for openplotva_storage::RedisTaskEnqueueRateLimitStore {
    type Error = openplotva_storage::StorageError;

    fn count_task_enqueue<'a>(
        &'a self,
        key: &'a str,
        cutoff_unix_ms: i64,
    ) -> RateLimitStoreFuture<'a, Result<usize, Self::Error>> {
        Box::pin(async move { self.count_task_enqueue(key, cutoff_unix_ms).await })
    }

    fn record_task_enqueue<'a>(
        &'a self,
        key: &'a str,
        score_unix_ms: i64,
        member: &'a str,
        cutoff_unix_ms: i64,
        ttl: Duration,
    ) -> RateLimitStoreFuture<'a, Result<usize, Self::Error>> {
        Box::pin(async move {
            self.record_task_enqueue(key, score_unix_ms, member, cutoff_unix_ms, ttl)
                .await
        })
    }
}

/// Source used to decide the latest rate-limit check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RateLimitCheckSource {
    Cache,
    /// Redis/Dragonfly had an active persisted expiry.
    PersistentStore,
    /// Redis/Dragonfly had no active persisted expiry.
    Missing,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskEnqueueRateLimitScope {
    UserChat,
    Chat,
    Global,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TaskEnqueueRateLimitReport {
    pub limited: bool,
    pub scope: Option<TaskEnqueueRateLimitScope>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TaskEnqueueRateLimitGrowReport {
    pub save_errors: Vec<String>,
}

#[derive(Debug)]
pub struct TaskEnqueueRateLimitPolicy<S> {
    store: S,
    // Random per-instance prefix so sorted-set members stay unique even if several
    // processes ever share one Redis (HA / rolling deploy); within one process the
    // monotonic seq alone is enough.
    instance: u64,
    member_seq: AtomicU64,
}

impl<S> TaskEnqueueRateLimitPolicy<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            instance: rand::random(),
            member_seq: AtomicU64::new(0),
        }
    }
}

impl<S> TaskEnqueueRateLimitPolicy<S>
where
    S: TaskEnqueueRateLimitStore + Send + Sync,
{
    pub async fn check_task_enqueue_rate_limit(
        &self,
        chat_id: i64,
        user_id: i64,
        now: OffsetDateTime,
    ) -> TaskEnqueueRateLimitReport {
        let cutoff = task_enqueue_cutoff_unix_ms(now);
        let mut last_error = None;
        for (scope, key, limit) in task_enqueue_rate_limit_keys(chat_id, user_id) {
            match self.store.count_task_enqueue(&key, cutoff).await {
                Ok(count) if count >= limit => {
                    return TaskEnqueueRateLimitReport {
                        limited: true,
                        scope: Some(scope),
                        error: last_error,
                    };
                }
                Ok(_) => {}
                // Fail open: a store outage must not drop legitimate enqueues.
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        TaskEnqueueRateLimitReport {
            limited: false,
            scope: None,
            error: last_error,
        }
    }

    pub async fn grow_task_enqueue_rate_limit(
        &self,
        chat_id: i64,
        user_id: i64,
        now: OffsetDateTime,
    ) -> TaskEnqueueRateLimitGrowReport {
        let cutoff = task_enqueue_cutoff_unix_ms(now);
        let score = unix_millis(now);
        let mut save_errors = Vec::new();
        for (_, key, _) in task_enqueue_rate_limit_keys(chat_id, user_id) {
            let member = format!(
                "{score}-{:x}-{}",
                self.instance,
                self.member_seq.fetch_add(1, Ordering::Relaxed)
            );
            if let Err(error) = self
                .store
                .record_task_enqueue(&key, score, &member, cutoff, TASK_ENQUEUE_TTL)
                .await
            {
                save_errors.push(error.to_string());
            }
        }
        TaskEnqueueRateLimitGrowReport { save_errors }
    }
}

impl<S> TaskEnqueueRateLimit for TaskEnqueueRateLimitPolicy<S>
where
    S: TaskEnqueueRateLimitStore + Send + Sync,
{
    fn check_task_enqueue_rate_limit<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
        now: OffsetDateTime,
    ) -> TaskEnqueueRateLimitFuture<'a> {
        Box::pin(async move {
            TaskEnqueueRateLimitPolicy::check_task_enqueue_rate_limit(self, chat_id, user_id, now)
                .await
        })
    }

    fn grow_task_enqueue_rate_limit<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
        now: OffsetDateTime,
    ) -> TaskEnqueueRateLimitGrowFuture<'a> {
        Box::pin(async move {
            TaskEnqueueRateLimitPolicy::grow_task_enqueue_rate_limit(self, chat_id, user_id, now)
                .await
        })
    }
}

#[derive(Debug)]
pub struct ChatRateLimitPolicy<S> {
    store: S,
    cache: Mutex<HashMap<i64, CachedRateLimit>>,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
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
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
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
        let Some(cached) = cache.get(&chat_id).copied() else {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        if now > cached.cached_until {
            cache.remove(&chat_id);
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
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

    pub(crate) fn cache_stats(&self) -> crate::runtime_cache::PolicyCacheStats {
        let size = self.cache().len();
        crate::runtime_cache::PolicyCacheStats {
            size,
            hits: self.cache_hits.load(Ordering::Relaxed),
            misses: self.cache_misses.load(Ordering::Relaxed),
            mem_size: size.saturating_mul(std::mem::size_of::<CachedRateLimit>()) as u64,
        }
    }
}

fn task_enqueue_rate_limit_keys(
    chat_id: i64,
    user_id: i64,
) -> [(TaskEnqueueRateLimitScope, String, usize); 3] {
    [
        (
            TaskEnqueueRateLimitScope::UserChat,
            format!("user_chat:{chat_id}:{user_id}"),
            TASK_ENQUEUE_USER_CHAT_LIMIT,
        ),
        (
            TaskEnqueueRateLimitScope::Chat,
            format!("chat:{chat_id}"),
            TASK_ENQUEUE_CHAT_LIMIT,
        ),
        (
            TaskEnqueueRateLimitScope::Global,
            "global".to_owned(),
            TASK_ENQUEUE_GLOBAL_LIMIT,
        ),
    ]
}

fn unix_millis(now: OffsetDateTime) -> i64 {
    (now.unix_timestamp_nanos() / 1_000_000) as i64
}

fn task_enqueue_cutoff_unix_ms(now: OffsetDateTime) -> i64 {
    unix_millis(now) - TASK_ENQUEUE_WINDOW.as_millis() as i64
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

pub fn telegram_retry_after_from_outbound_error(
    error: &openplotva_telegram::TelegramOutboundExecuteError,
) -> Option<Duration> {
    error.retry_after().map(Duration::from_secs)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        error::Error,
        fmt,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use time::OffsetDateTime;

    use super::{
        ChatRateLimitPolicy, RateLimitCheckSource, RateLimitStore, RateLimitStoreFuture,
        TASK_ENQUEUE_CHAT_LIMIT, TASK_ENQUEUE_GLOBAL_LIMIT, TASK_ENQUEUE_USER_CHAT_LIMIT,
        TaskEnqueueRateLimitPolicy, TaskEnqueueRateLimitScope, TaskEnqueueRateLimitStore,
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
        let stats = policy.cache_stats();
        assert_eq!(stats.size, 1);
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 0);
        assert!(stats.mem_size > 0);

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

    #[tokio::test]
    async fn task_enqueue_policy_blocks_after_user_chat_limit_and_grows_after_success()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let store = TaskEnqueueRateLimitStoreStub::default();
        let policy = TaskEnqueueRateLimitPolicy::new(store.clone());

        for _ in 0..TASK_ENQUEUE_USER_CHAT_LIMIT {
            let report = policy.check_task_enqueue_rate_limit(100, 7, now).await;
            assert!(!report.limited);
            policy.grow_task_enqueue_rate_limit(100, 7, now).await;
        }

        let limited = policy.check_task_enqueue_rate_limit(100, 7, now).await;

        assert!(limited.limited);
        assert_eq!(limited.scope, Some(TaskEnqueueRateLimitScope::UserChat));
        assert_eq!(store.saved_count(), TASK_ENQUEUE_USER_CHAT_LIMIT * 3);
        Ok(())
    }

    #[tokio::test]
    async fn task_enqueue_policy_uses_updated_chat_and_global_defaults()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let chat_policy = TaskEnqueueRateLimitPolicy::new(TaskEnqueueRateLimitStoreStub::default());
        for user_id in 1..=TASK_ENQUEUE_CHAT_LIMIT {
            assert!(
                !chat_policy
                    .check_task_enqueue_rate_limit(200, user_id as i64, now)
                    .await
                    .limited
            );
            chat_policy
                .grow_task_enqueue_rate_limit(200, user_id as i64, now)
                .await;
        }
        let chat_limited = chat_policy
            .check_task_enqueue_rate_limit(200, 10_000, now)
            .await;
        assert!(chat_limited.limited);
        assert_eq!(chat_limited.scope, Some(TaskEnqueueRateLimitScope::Chat));

        let global_policy =
            TaskEnqueueRateLimitPolicy::new(TaskEnqueueRateLimitStoreStub::default());
        for index in 1..=TASK_ENQUEUE_GLOBAL_LIMIT {
            assert!(
                !global_policy
                    .check_task_enqueue_rate_limit(index as i64, index as i64, now)
                    .await
                    .limited
            );
            global_policy
                .grow_task_enqueue_rate_limit(index as i64, index as i64, now)
                .await;
        }
        let global_limited = global_policy
            .check_task_enqueue_rate_limit(10_000, 10_000, now)
            .await;
        assert!(global_limited.limited);
        assert_eq!(
            global_limited.scope,
            Some(TaskEnqueueRateLimitScope::Global)
        );
        Ok(())
    }

    #[tokio::test]
    async fn task_enqueue_policy_fails_open_when_store_errors() -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let store = TaskEnqueueRateLimitStoreStub::default();
        store.set_save_error(Some("redis down"));
        let policy = TaskEnqueueRateLimitPolicy::new(store);

        for _ in 0..TASK_ENQUEUE_USER_CHAT_LIMIT {
            policy.grow_task_enqueue_rate_limit(100, 7, now).await;
        }

        // A store outage must not drop legitimate enqueues: fail open, surfacing the error.
        let report = policy.check_task_enqueue_rate_limit(100, 7, now).await;

        assert!(!report.limited);
        assert!(report.error.is_some());
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

    #[derive(Clone, Default)]
    struct TaskEnqueueRateLimitStoreStub {
        state: Arc<Mutex<TaskEnqueueRateLimitStoreState>>,
    }

    #[derive(Default)]
    struct TaskEnqueueRateLimitStoreState {
        entries: HashMap<String, Vec<(i64, String)>>,
        save_error: Option<&'static str>,
        saved_count: usize,
    }

    impl TaskEnqueueRateLimitStoreStub {
        fn set_save_error(&self, error: Option<&'static str>) {
            self.state().save_error = error;
        }

        fn saved_count(&self) -> usize {
            self.state().saved_count
        }

        fn state(&self) -> std::sync::MutexGuard<'_, TaskEnqueueRateLimitStoreState> {
            match self.state.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            }
        }
    }

    impl TaskEnqueueRateLimitStore for TaskEnqueueRateLimitStoreStub {
        type Error = RateLimitStoreStubError;

        fn count_task_enqueue<'a>(
            &'a self,
            key: &'a str,
            cutoff_unix_ms: i64,
        ) -> RateLimitStoreFuture<'a, Result<usize, Self::Error>> {
            Box::pin(async move {
                let mut state = self.state();
                if let Some(error) = state.save_error {
                    return Err(RateLimitStoreStubError(error));
                }
                let entries = state.entries.entry(key.to_owned()).or_default();
                entries.retain(|(score, _)| *score >= cutoff_unix_ms);
                Ok(entries.len())
            })
        }

        fn record_task_enqueue<'a>(
            &'a self,
            key: &'a str,
            score_unix_ms: i64,
            member: &'a str,
            cutoff_unix_ms: i64,
            _ttl: Duration,
        ) -> RateLimitStoreFuture<'a, Result<usize, Self::Error>> {
            Box::pin(async move {
                let mut state = self.state();
                state.saved_count += 1;
                if let Some(error) = state.save_error {
                    return Err(RateLimitStoreStubError(error));
                }
                let entries = state.entries.entry(key.to_owned()).or_default();
                entries.push((score_unix_ms, member.to_owned()));
                entries.retain(|(score, _)| *score >= cutoff_unix_ms);
                Ok(entries.len())
            })
        }
    }
}
