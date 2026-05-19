use std::{
    collections::{HashMap, VecDeque},
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

use crate::MessageFingerprint;

/// Go default outbound deduplication window.
pub const DEFAULT_DEBOUNCE_WINDOW: Duration = Duration::from_secs(30);

/// Go default number of outbound fingerprints kept in the debouncer cache.
pub const DEFAULT_DEBOUNCE_CACHE_SIZE: usize = 1000;

/// Go outbound debouncer settings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DebouncerConfig {
    /// Whether outbound deduplication is enabled.
    pub enabled: bool,
    /// Default deduplication window. `Duration::ZERO` maps to Go's 30 second default.
    pub default_window: Duration,
    /// Maximum cache size. `0` maps to Go's 1000 entry default.
    pub max_cache_size: usize,
    /// Per-chat deduplication windows keyed by Telegram chat ID.
    pub per_chat_settings: HashMap<i64, Duration>,
}

impl Default for DebouncerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_window: Duration::ZERO,
            max_cache_size: 0,
            per_chat_settings: HashMap::new(),
        }
    }
}

impl DebouncerConfig {
    fn normalized(mut self) -> Self {
        if self.enabled {
            if self.default_window == Duration::ZERO {
                self.default_window = DEFAULT_DEBOUNCE_WINDOW;
            }
            if self.max_cache_size == 0 {
                self.max_cache_size = DEFAULT_DEBOUNCE_CACHE_SIZE;
            }
        }
        self
    }
}

#[derive(Debug)]
pub struct Debouncer {
    config: DebouncerConfig,
    state: Mutex<DebouncerState>,
}

#[derive(Debug, Default)]
struct DebouncerState {
    entries: HashMap<String, DebouncerEntry>,
    oldest_to_newest: VecDeque<String>,
    deduped_count: i64,
}

#[derive(Clone, Copy, Debug)]
struct DebouncerEntry {
    last_sent: Instant,
    expires_at: Instant,
}

impl Debouncer {
    /// Build a debouncer with Go's zero-value default handling.
    pub fn new(config: DebouncerConfig) -> Self {
        Self {
            config: config.normalized(),
            state: Mutex::new(DebouncerState::default()),
        }
    }

    /// Return whether the outbound message should be sent now.
    pub fn should_process(&self, fingerprint: &MessageFingerprint) -> bool {
        self.should_process_at(fingerprint, Instant::now())
    }

    /// Record that an outbound message was sent now.
    pub fn record_sent(&self, fingerprint: &MessageFingerprint) {
        self.record_sent_at(fingerprint, Instant::now());
    }

    /// Return the number of suppressed duplicates observed by `should_process`.
    pub fn deduped_count(&self) -> i64 {
        self.state().deduped_count
    }

    pub(crate) fn should_process_at(&self, fingerprint: &MessageFingerprint, now: Instant) -> bool {
        if !self.config.enabled {
            return true;
        }

        let key = fingerprint.to_string();
        let mut state = self.state();
        let Some(entry) = state.entries.get(&key).copied() else {
            return true;
        };

        if now > entry.expires_at {
            state.remove(&key);
            return true;
        }

        state.touch(&key);
        let window = self
            .config
            .per_chat_settings
            .get(&fingerprint.chat_id)
            .copied()
            .unwrap_or(self.config.default_window);
        if now.saturating_duration_since(entry.last_sent) > window {
            return true;
        }

        state.deduped_count += 1;
        false
    }

    pub(crate) fn record_sent_at(&self, fingerprint: &MessageFingerprint, now: Instant) {
        if !self.config.enabled {
            return;
        }

        let key = fingerprint.to_string();
        let entry = DebouncerEntry {
            last_sent: now,
            expires_at: now + self.config.default_window,
        };
        let mut state = self.state();
        state.insert(key, entry, self.config.max_cache_size);
    }

    fn state(&self) -> MutexGuard<'_, DebouncerState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl DebouncerState {
    fn insert(&mut self, key: String, entry: DebouncerEntry, max_cache_size: usize) {
        self.entries.insert(key.clone(), entry);
        self.touch(&key);
        while self.entries.len() > max_cache_size {
            let Some(oldest) = self.oldest_to_newest.pop_front() else {
                break;
            };
            self.entries.remove(&oldest);
        }
    }

    fn remove(&mut self, key: &str) {
        self.entries.remove(key);
        self.oldest_to_newest.retain(|stored| stored != key);
    }

    fn touch(&mut self, key: &str) {
        self.oldest_to_newest.retain(|stored| stored != key);
        self.oldest_to_newest.push_back(key.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use super::{Debouncer, DebouncerConfig};
    use crate::{MESSAGE_TYPE_TEXT, MessageFingerprint, hash_content};

    fn text_fingerprint(chat_id: i64, text: &str) -> MessageFingerprint {
        MessageFingerprint {
            chat_id,
            message_type: MESSAGE_TYPE_TEXT.to_owned(),
            content_hash: hash_content(text),
            debounce_key: None,
        }
    }

    #[test]
    fn disabled_debouncer_always_allows_and_does_not_count_dedupes() {
        let debouncer = Debouncer::new(DebouncerConfig::default());
        let fp = text_fingerprint(42, "hello");
        let now = std::time::Instant::now();

        assert!(debouncer.should_process_at(&fp, now));
        debouncer.record_sent_at(&fp, now);
        assert!(debouncer.should_process_at(&fp, now));
        assert_eq!(debouncer.deduped_count(), 0);
    }

    #[test]
    fn enabled_debouncer_uses_go_default_window_with_strict_boundary() {
        let debouncer = Debouncer::new(DebouncerConfig {
            enabled: true,
            default_window: Duration::ZERO,
            max_cache_size: 0,
            per_chat_settings: HashMap::new(),
        });
        let fp = text_fingerprint(42, "hello");
        let now = std::time::Instant::now();

        assert!(debouncer.should_process_at(&fp, now));
        debouncer.record_sent_at(&fp, now);

        assert!(!debouncer.should_process_at(&fp, now + Duration::from_secs(30)));
        assert!(
            debouncer
                .should_process_at(&fp, now + Duration::from_secs(30) + Duration::from_nanos(1))
        );
        assert_eq!(debouncer.deduped_count(), 1);
    }

    #[test]
    fn enabled_debouncer_uses_per_chat_window_before_default_ttl_expires() {
        let mut per_chat_settings = HashMap::new();
        per_chat_settings.insert(42, Duration::from_secs(5));
        let debouncer = Debouncer::new(DebouncerConfig {
            enabled: true,
            default_window: Duration::from_secs(30),
            max_cache_size: 1000,
            per_chat_settings,
        });
        let fp = text_fingerprint(42, "hello");
        let now = std::time::Instant::now();

        debouncer.record_sent_at(&fp, now);

        assert!(!debouncer.should_process_at(&fp, now + Duration::from_secs(5)));
        assert!(
            debouncer
                .should_process_at(&fp, now + Duration::from_secs(5) + Duration::from_nanos(1))
        );
        assert_eq!(debouncer.deduped_count(), 1);
    }

    #[test]
    fn enabled_debouncer_evicts_oldest_entry_at_go_default_capacity_boundary() {
        let debouncer = Debouncer::new(DebouncerConfig {
            enabled: true,
            default_window: Duration::from_secs(30),
            max_cache_size: 1,
            per_chat_settings: HashMap::new(),
        });
        let first = text_fingerprint(42, "first");
        let second = text_fingerprint(42, "second");
        let now = std::time::Instant::now();

        debouncer.record_sent_at(&first, now);
        debouncer.record_sent_at(&second, now + Duration::from_secs(1));

        assert!(debouncer.should_process_at(&first, now + Duration::from_secs(2)));
        assert!(!debouncer.should_process_at(&second, now + Duration::from_secs(2)));
        assert_eq!(debouncer.deduped_count(), 1);
    }
}
