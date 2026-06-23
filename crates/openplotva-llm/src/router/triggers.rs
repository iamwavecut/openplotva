//! Trigger specifications and live engagement state.
//!
//! A trigger engages an overflow [`Candidate`] while its condition holds. The
//! background poller (in the app layer) evaluates conditions and flips engagement
//! in [`TriggerState`]; the pure policy only reads "is this trigger engaged".

use std::collections::HashSet;
use std::sync::Mutex;
use std::time::Duration;

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
}

impl super::policy::TriggerView for TriggerState {
    fn engaged(&self, id: TriggerId) -> bool {
        self.is_engaged(id)
    }
}
