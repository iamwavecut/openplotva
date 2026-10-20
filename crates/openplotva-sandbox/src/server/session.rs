//! Session lifecycle: one home per key, at most `max_sessions` alive, closed
//! after `idle_ttl` without calls or `max_age` after opening.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};

use super::home::HomeStorage;
use crate::api::SessionInfo;

/// One live session.
#[derive(Debug)]
pub struct Session {
    key: String,
    home: PathBuf,
    opened: Instant,
    last_used: Mutex<Instant>,
    run_lock: Arc<AsyncMutex<()>>,
    closed: AtomicBool,
    _slot: OwnedSemaphorePermit,
}

impl Session {
    #[must_use]
    pub fn home(&self) -> &Path {
        &self.home
    }

    fn touch(&self, now: Instant) {
        *lock(&self.last_used) = now;
    }

    fn expired(&self, now: Instant, idle_ttl: Duration, max_age: Duration) -> bool {
        now.duration_since(*lock(&self.last_used)) >= idle_ttl
            || now.duration_since(self.opened) >= max_age
    }
}

/// Exclusive use of a session for one call; the session stays alive while held.
#[derive(Debug)]
pub struct SessionLease {
    _guard: OwnedMutexGuard<()>,
    session: Arc<Session>,
    /// True when this lease opened the session.
    pub created: bool,
    idle_ttl: Duration,
    max_age: Duration,
}

impl SessionLease {
    #[must_use]
    pub fn home(&self) -> &Path {
        self.session.home()
    }

    /// Mark the session used now and describe its remaining lifetime.
    #[must_use]
    pub fn finish(&self) -> SessionInfo {
        let now = Instant::now();
        self.session.touch(now);
        let age = now.duration_since(self.session.opened);
        let remaining = self.max_age.saturating_sub(age);
        SessionInfo {
            created: self.created,
            idle_expires_in_seconds: self.idle_ttl.min(remaining).as_secs(),
            expires_in_seconds: remaining.as_secs(),
        }
    }
}

/// Error returned when a session cannot be opened.
#[derive(Debug, Error)]
pub enum SessionError {
    #[error("all {0} sandbox sessions are busy")]
    Busy(usize),
    #[error("session home: {0}")]
    Home(#[from] std::io::Error),
}

/// Owns every session and its slot.
#[derive(Debug)]
pub struct SessionManager {
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    slots: Arc<Semaphore>,
    max_sessions: usize,
    storage: HomeStorage,
    root: PathBuf,
    matplotlib_cache: Option<PathBuf>,
    idle_ttl: Duration,
    max_age: Duration,
    slot_wait: Duration,
    counter: AtomicU64,
}

/// Session settings for [`SessionManager::new`].
#[derive(Clone, Debug)]
pub struct SessionLimits {
    pub max_sessions: usize,
    pub idle_ttl: Duration,
    pub max_age: Duration,
    pub slot_wait: Duration,
}

impl SessionManager {
    #[must_use]
    pub fn new(
        storage: HomeStorage,
        root: PathBuf,
        matplotlib_cache: Option<PathBuf>,
        limits: SessionLimits,
    ) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            slots: Arc::new(Semaphore::new(limits.max_sessions)),
            max_sessions: limits.max_sessions,
            storage,
            root,
            matplotlib_cache,
            idle_ttl: limits.idle_ttl,
            max_age: limits.max_age,
            slot_wait: limits.slot_wait,
            counter: AtomicU64::new(0),
        }
    }

    #[must_use]
    pub fn storage(&self) -> &HomeStorage {
        &self.storage
    }

    /// Number of live sessions.
    #[must_use]
    pub fn live(&self) -> usize {
        lock(&self.sessions).len()
    }

    /// Take exclusive use of the session for `key`, opening one when it is
    /// missing or expired.
    pub async fn lease(&self, key: &str) -> Result<SessionLease, SessionError> {
        loop {
            let (session, created) = match self.existing(key) {
                Some(session) => (session, false),
                None => (self.open(key).await?, true),
            };
            let guard = Arc::clone(&session.run_lock).lock_owned().await;
            if session.closed.load(Ordering::Acquire) {
                continue;
            }
            session.touch(Instant::now());
            return Ok(SessionLease {
                session,
                _guard: guard,
                created,
                idle_ttl: self.idle_ttl,
                max_age: self.max_age,
            });
        }
    }

    /// Take exclusive use of a live session without opening one.
    pub async fn lease_existing(&self, key: &str) -> Option<SessionLease> {
        let session = self.existing(key)?;
        let guard = Arc::clone(&session.run_lock).lock_owned().await;
        (!session.closed.load(Ordering::Acquire)).then(|| SessionLease {
            session,
            _guard: guard,
            created: false,
            idle_ttl: self.idle_ttl,
            max_age: self.max_age,
        })
    }

    /// Close the session for `key` now.
    pub async fn close(&self, key: &str) -> bool {
        let Some(session) = lock(&self.sessions).remove(key) else {
            return false;
        };
        self.destroy(session).await;
        true
    }

    /// Close every expired session that is not running a call.
    pub async fn reap(&self) -> usize {
        let now = Instant::now();
        let expired = {
            let mut sessions = lock(&self.sessions);
            let keys = sessions
                .iter()
                .filter(|(_, session)| {
                    session.run_lock.try_lock().is_ok()
                        && session.expired(now, self.idle_ttl, self.max_age)
                })
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| sessions.remove(&key))
                .collect::<Vec<_>>()
        };
        let count = expired.len();
        for session in expired {
            self.destroy(session).await;
        }
        count
    }

    /// Close every session, for shutdown.
    pub async fn close_all(&self) {
        let sessions = lock(&self.sessions)
            .drain()
            .map(|(_, session)| session)
            .collect::<Vec<_>>();
        for session in sessions {
            self.destroy(session).await;
        }
    }

    fn existing(&self, key: &str) -> Option<Arc<Session>> {
        let now = Instant::now();
        let mut sessions = lock(&self.sessions);
        let session = sessions.get(key)?;
        if session.expired(now, self.idle_ttl, self.max_age) && session.run_lock.try_lock().is_ok()
        {
            let stale = sessions.remove(key);
            drop(sessions);
            if let Some(stale) = stale {
                stale.closed.store(true, Ordering::Release);
                let storage = self.storage.clone();
                tokio::spawn(async move {
                    let _guard = stale.run_lock.lock().await;
                    storage.destroy(&stale.home).await;
                });
            }
            return None;
        }
        Some(Arc::clone(session))
    }

    async fn open(&self, key: &str) -> Result<Arc<Session>, SessionError> {
        let slot = tokio::time::timeout(self.slot_wait, Arc::clone(&self.slots).acquire_owned())
            .await
            .map_err(|_| SessionError::Busy(self.max_sessions))?
            .map_err(|_| SessionError::Busy(self.max_sessions))?;
        if let Some(session) = self.existing(key) {
            return Ok(session);
        }
        let sequence = self.counter.fetch_add(1, Ordering::Relaxed);
        let home = self.root.join(format!("{key}.{sequence}"));
        if let Err(error) = self
            .storage
            .create(&home, self.matplotlib_cache.as_deref())
            .await
        {
            self.storage.destroy(&home).await;
            return Err(error.into());
        }
        let now = Instant::now();
        let session = Arc::new(Session {
            key: key.to_owned(),
            home,
            opened: now,
            last_used: Mutex::new(now),
            run_lock: Arc::new(AsyncMutex::new(())),
            closed: AtomicBool::new(false),
            _slot: slot,
        });
        let previous = lock(&self.sessions).insert(key.to_owned(), Arc::clone(&session));
        if let Some(previous) = previous {
            self.destroy(previous).await;
        }
        tracing::info!(%key, home = %session.home.display(), "sandbox session opened");
        Ok(session)
    }

    async fn destroy(&self, session: Arc<Session>) {
        session.closed.store(true, Ordering::Release);
        let _guard = session.run_lock.lock().await;
        self.storage.destroy(&session.home).await;
        tracing::info!(key = %session.key, "sandbox session closed");
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager(name: &str, limits: SessionLimits) -> (SessionManager, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "openplotva-sandbox-sessions-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        (
            SessionManager::new(HomeStorage::PlainDir, root.clone(), None, limits),
            root,
        )
    }

    fn limits(idle_ms: u64, age_ms: u64) -> SessionLimits {
        SessionLimits {
            max_sessions: 2,
            idle_ttl: Duration::from_millis(idle_ms),
            max_age: Duration::from_millis(age_ms),
            slot_wait: Duration::from_millis(50),
        }
    }

    #[tokio::test]
    async fn reuses_a_live_session_and_keeps_its_files() {
        let (sessions, root) = manager("reuse", limits(5_000, 60_000));
        let first = sessions.lease("chat-1").await.expect("open");
        assert!(first.created);
        std::fs::write(first.home().join("data.txt"), b"1").expect("write");
        let info = first.finish();
        assert!(info.created);
        assert!(info.idle_expires_in_seconds <= 5);
        drop(first);

        let second = sessions.lease("chat-1").await.expect("reuse");
        assert!(!second.created);
        assert!(second.home().join("data.txt").is_file());
        drop(second);
        assert_eq!(sessions.live(), 1);
        sessions.close_all().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn refuses_a_new_session_when_every_slot_is_taken() {
        let (sessions, root) = manager("busy", limits(5_000, 60_000));
        let _a = sessions.lease("a").await.expect("a");
        let _b = sessions.lease("b").await.expect("b");
        assert!(matches!(
            sessions.lease("c").await,
            Err(SessionError::Busy(2))
        ));
        drop(_a);
        drop(_b);
        sessions.close_all().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn idle_sessions_are_reaped_and_their_slot_freed() {
        let (sessions, root) = manager("idle", limits(40, 60_000));
        let lease = sessions.lease("a").await.expect("open");
        let home = lease.home().to_path_buf();
        let _ = lease.finish();
        drop(lease);
        assert_eq!(sessions.reap().await, 0);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(sessions.reap().await, 1);
        assert!(!home.exists());
        assert_eq!(sessions.live(), 0);

        let _a = sessions.lease("a").await.expect("slot freed");
        let _b = sessions.lease("b").await.expect("second slot");
        drop(_a);
        drop(_b);
        sessions.close_all().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn calls_keep_a_session_alive_only_until_its_maximum_age() {
        let (sessions, root) = manager("age", limits(1_000, 150));
        let first = sessions.lease("a").await.expect("open");
        let first_home = first.home().to_path_buf();
        drop(first);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let touched = sessions.lease("a").await.expect("still alive");
        assert!(!touched.created);
        drop(touched);
        tokio::time::sleep(Duration::from_millis(80)).await;
        let renewed = sessions.lease("a").await.expect("reopened");
        assert!(renewed.created);
        assert_ne!(renewed.home(), first_home.as_path());
        drop(renewed);
        sessions.close_all().await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn a_running_call_blocks_expiry() {
        let (sessions, root) = manager("running", limits(20, 60_000));
        let lease = sessions.lease("a").await.expect("open");
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(sessions.reap().await, 0);
        assert!(sessions.lease_existing("b").await.is_none());
        drop(lease);
        sessions.close_all().await;
        let _ = std::fs::remove_dir_all(root);
    }
}
