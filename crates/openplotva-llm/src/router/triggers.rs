//! Trigger specifications and live engagement state.
//!
//! A trigger engages an overflow [`Candidate`] while its condition holds. The
//! background poller (in the app layer) evaluates conditions and flips engagement
//! in [`TriggerState`]; the pure policy only reads "is this trigger engaged".

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::table::Candidate;
use super::table::{ModelId, ProviderId};

/// Database id of a `workflow_triggers` row.
pub type TriggerId = i64;

/// The condition the poller evaluates for a trigger.
#[derive(Clone, Debug)]
pub enum TriggerCondition {
    /// Engage while the named queue's pending depth is at/above `high`; disengage
    /// once it drops below `low` (hysteresis, generalizing the old dialog gate).
    QueueDepth {
        queue: String,
        high: usize,
        low: usize,
    },
    /// Engage while the target's windowed failure ratio exceeds `threshold`.
    ErrorRate {
        provider: ProviderId,
        model: ModelId,
        threshold: f64,
        window: Duration,
    },
    /// Engage during a daily UTC window `[start_minute, end_minute)` (minutes since
    /// midnight); a window that wraps past midnight is supported when start > end.
    TimeOfDay { start_minute: u32, end_minute: u32 },
    /// Engage while the target provider/model is marked capacity-constrained.
    ProviderCapacity {
        provider: ProviderId,
        model: ModelId,
        cooldown: Duration,
    },
}

/// A compiled trigger: a condition plus the overflow candidate it engages.
#[derive(Clone, Debug)]
pub struct TriggerSpec {
    pub id: TriggerId,
    pub condition: TriggerCondition,
    pub engage: Candidate,
}

/// Set of currently-engaged trigger ids. Written by the poller, read by policy.
/// The lock is held only for the synchronous set operation, never across `.await`.
#[derive(Default)]
pub struct TriggerState {
    engaged: Mutex<HashSet<TriggerId>>,
    capacity_until: Mutex<HashMap<(ProviderId, ModelId), Instant>>,
}

impl TriggerState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashSet<TriggerId>> {
        self.engaged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_capacity(&self) -> std::sync::MutexGuard<'_, HashMap<(ProviderId, ModelId), Instant>> {
        self.capacity_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn set_engaged(&self, id: TriggerId, engaged: bool) {
        let mut guard = self.lock();
        if engaged {
            guard.insert(id);
        } else {
            guard.remove(&id);
        }
    }

    #[must_use]
    pub fn is_engaged(&self, id: TriggerId) -> bool {
        self.lock().contains(&id)
    }

    pub fn mark_capacity_unavailable(
        &self,
        provider: ProviderId,
        model: ModelId,
        cooldown: Duration,
    ) {
        self.mark_capacity_unavailable_at(provider, model, cooldown, Instant::now());
    }

    pub fn mark_capacity_unavailable_at(
        &self,
        provider: ProviderId,
        model: ModelId,
        cooldown: Duration,
        now: Instant,
    ) {
        let until = now + cooldown;
        let mut guard = self.lock_capacity();
        let entry = guard.entry((provider, model)).or_insert(until);
        if *entry < until {
            *entry = until;
        }
    }

    #[must_use]
    pub fn provider_capacity_unavailable(&self, provider: ProviderId, model: ModelId) -> bool {
        self.provider_capacity_unavailable_at(provider, model, Instant::now())
    }

    #[must_use]
    pub fn provider_capacity_unavailable_at(
        &self,
        provider: ProviderId,
        model: ModelId,
        now: Instant,
    ) -> bool {
        let mut guard = self.lock_capacity();
        match guard.get(&(provider, model)).copied() {
            Some(until) if now < until => true,
            Some(_) => {
                guard.remove(&(provider, model));
                false
            }
            None => false,
        }
    }
}

impl super::policy::TriggerView for TriggerState {
    fn engaged(&self, id: TriggerId) -> bool {
        self.is_engaged(id)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn provider_capacity_unavailable_expires_after_cooldown() {
        let state = TriggerState::new();
        let now = Instant::now();

        state.mark_capacity_unavailable_at(10, 20, Duration::from_secs(30), now);

        assert!(state.provider_capacity_unavailable_at(10, 20, now + Duration::from_secs(29)));
        assert!(!state.provider_capacity_unavailable_at(10, 20, now + Duration::from_secs(30)));
        assert!(!state.provider_capacity_unavailable_at(10, 21, now + Duration::from_secs(1)));
    }
}
