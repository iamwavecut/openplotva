use std::{
    collections::HashMap,
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

/// Go default interval between regular outbound sends for the same chat.
pub const DEFAULT_DISPATCH_INTERVAL: Duration = Duration::from_millis(50);

/// Go default idle age before unused per-chat dispatch limiters are cleaned up.
pub const DEFAULT_RATE_LIMITER_MAX_IDLE: Duration = Duration::from_secs(30 * 60);

/// Per-chat burst-one limiter collection used by the outbound dispatcher.
#[derive(Debug)]
pub struct ChatLimiters {
    base_interval: Duration,
    state: Mutex<HashMap<i64, RateLimiterInfo>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RateLimiterInfo {
    limiter: BurstOneRateLimiter,
    last_access: Instant,
}

impl RateLimiterInfo {
    fn new(now: Instant) -> Self {
        Self {
            limiter: BurstOneRateLimiter::new(now),
            last_access: now,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BurstOneRateLimiter {
    next_available_at: Instant,
}

impl BurstOneRateLimiter {
    fn new(now: Instant) -> Self {
        Self {
            next_available_at: now,
        }
    }

    fn allow_at(&mut self, now: Instant, interval: Duration) -> bool {
        if interval.is_zero() {
            self.next_available_at = now;
            return true;
        }
        if now < self.next_available_at {
            return false;
        }
        self.next_available_at = now + interval;
        true
    }

    fn reserve_delay_at(&mut self, now: Instant, interval: Duration) -> Duration {
        if interval.is_zero() {
            self.next_available_at = now;
            return Duration::ZERO;
        }
        let scheduled_at = self.next_available_at.max(now);
        self.next_available_at = scheduled_at + interval;
        scheduled_at.saturating_duration_since(now)
    }
}

impl ChatLimiters {
    /// Build an empty per-chat limiter map.
    pub fn new(base_interval: Duration) -> Self {
        Self {
            base_interval,
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Return whether a regular outbound send for `chat_id` may happen now.
    pub fn allow(&self, chat_id: i64) -> bool {
        self.allow_at(chat_id, Instant::now())
    }

    /// Reserve a regular outbound send slot and return how long the caller should wait.
    pub fn reserve_delay(&self, chat_id: i64) -> Duration {
        self.reserve_delay_at(chat_id, Instant::now())
    }

    /// Remove idle per-chat limiters and return the number removed.
    pub fn cleanup(&self, max_idle_duration: Duration) -> usize {
        self.cleanup_at(max_idle_duration, Instant::now())
    }

    /// Return the number of active per-chat limiters.
    pub fn active_len(&self) -> usize {
        self.state().len()
    }

    pub(crate) fn allow_at(&self, chat_id: i64, now: Instant) -> bool {
        let mut state = self.state();
        let info = state
            .entry(chat_id)
            .or_insert_with(|| RateLimiterInfo::new(now));
        info.last_access = now;
        info.limiter.allow_at(now, self.base_interval)
    }

    pub(crate) fn reserve_delay_at(&self, chat_id: i64, now: Instant) -> Duration {
        let mut state = self.state();
        let info = state
            .entry(chat_id)
            .or_insert_with(|| RateLimiterInfo::new(now));
        info.last_access = now;
        info.limiter.reserve_delay_at(now, self.base_interval)
    }

    pub(crate) fn cleanup_at(&self, max_idle_duration: Duration, now: Instant) -> usize {
        let max_idle_duration = normalized_max_idle(max_idle_duration);
        let mut state = self.state();
        let before = state.len();
        state
            .retain(|_, info| now.saturating_duration_since(info.last_access) <= max_idle_duration);
        before - state.len()
    }

    fn state(&self) -> MutexGuard<'_, HashMap<i64, RateLimiterInfo>> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

fn normalized_max_idle(max_idle_duration: Duration) -> Duration {
    if max_idle_duration.is_zero() {
        DEFAULT_RATE_LIMITER_MAX_IDLE
    } else {
        max_idle_duration
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{ChatLimiters, DEFAULT_DISPATCH_INTERVAL, DEFAULT_RATE_LIMITER_MAX_IDLE};

    #[test]
    fn per_chat_limiter_allows_first_message_and_blocks_until_interval() {
        let limiters = ChatLimiters::new(DEFAULT_DISPATCH_INTERVAL);
        let now = std::time::Instant::now();

        assert!(limiters.allow_at(42, now));
        assert!(!limiters.allow_at(42, now));
        assert!(!limiters.allow_at(
            42,
            now + DEFAULT_DISPATCH_INTERVAL - Duration::from_nanos(1)
        ));
        assert!(limiters.allow_at(42, now + DEFAULT_DISPATCH_INTERVAL));
        assert!(limiters.allow_at(43, now));
    }

    #[test]
    fn per_chat_limiter_reservations_preserve_wait_slot_order() {
        let limiters = ChatLimiters::new(DEFAULT_DISPATCH_INTERVAL);
        let now = std::time::Instant::now();

        assert_eq!(limiters.reserve_delay_at(42, now), Duration::ZERO);
        assert_eq!(
            limiters.reserve_delay_at(42, now),
            DEFAULT_DISPATCH_INTERVAL
        );
        assert_eq!(
            limiters.reserve_delay_at(42, now),
            DEFAULT_DISPATCH_INTERVAL * 2
        );
        assert_eq!(limiters.reserve_delay_at(43, now), Duration::ZERO);
    }

    #[test]
    fn cleanup_uses_go_default_idle_window_with_strict_boundary() {
        let limiters = ChatLimiters::new(DEFAULT_DISPATCH_INTERVAL);
        let now = std::time::Instant::now();
        limiters.allow_at(42, now);
        limiters.allow_at(43, now + Duration::from_secs(1));

        assert_eq!(
            limiters.cleanup_at(Duration::ZERO, now + DEFAULT_RATE_LIMITER_MAX_IDLE),
            0
        );
        assert_eq!(
            limiters.cleanup_at(
                Duration::ZERO,
                now + DEFAULT_RATE_LIMITER_MAX_IDLE + Duration::from_nanos(1),
            ),
            1
        );
        assert_eq!(limiters.active_len(), 1);
    }
}
