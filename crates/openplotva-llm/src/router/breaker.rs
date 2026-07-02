//! Per-target circuit breaker and windowed error-rate tracking.
//!
//! State lives outside the [`super::table::RoutingTable`] so it survives an
//! `ArcSwap` reload. Stale keys for targets removed by a reload are harmless and
//! lazily forgotten. All access takes a short synchronous lock that is never held
//! across an `.await`, per the project concurrency rules.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::policy::Liveness;
use super::table::{ModelId, ProviderId};

/// Per-assignment breaker tuning resolved from `workflow_assignments`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BreakerConfig {
    /// Open the circuit after this many consecutive failures.
    pub fail_threshold: u32,
    /// Allow a half-open probe after this cooldown.
    pub cooldown: Duration,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            fail_threshold: 5,
            cooldown: Duration::from_millis(30_000),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct TargetState {
    consecutive_failures: u32,
    opened_at: Option<Instant>,
    cooldown: Duration,
    // Windowed error-rate accounting for the `error_rate` trigger.
    window_start: Instant,
    window_total: u32,
    window_failures: u32,
}

impl TargetState {
    fn new(now: Instant) -> Self {
        Self {
            consecutive_failures: 0,
            opened_at: None,
            cooldown: Duration::ZERO,
            window_start: now,
            window_total: 0,
            window_failures: 0,
        }
    }
}

/// Concurrent breaker map keyed by `(provider, model)`.
#[derive(Default)]
pub struct BreakerSet {
    inner: Mutex<HashMap<(ProviderId, ModelId), TargetState>>,
}

impl BreakerSet {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<(ProviderId, ModelId), TargetState>> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A target is live unless its circuit is open and still inside the cooldown.
    /// An unknown target (never failed) is live; an elapsed cooldown is half-open
    /// and reported live so exactly one probe can run.
    #[must_use]
    pub fn is_live_at(&self, provider: ProviderId, model: ModelId, now: Instant) -> bool {
        let guard = self.lock();
        match guard.get(&(provider, model)) {
            None => true,
            Some(state) => match state.opened_at {
                None => true,
                Some(opened) => now.duration_since(opened) >= state.cooldown,
            },
        }
    }

    pub fn record_success(&self, provider: ProviderId, model: ModelId) {
        self.record_success_at(provider, model, Instant::now());
    }

    pub fn record_success_at(&self, provider: ProviderId, model: ModelId, now: Instant) {
        let mut guard = self.lock();
        let state = guard
            .entry((provider, model))
            .or_insert_with(|| TargetState::new(now));
        state.consecutive_failures = 0;
        state.opened_at = None;
        state.window_total = state.window_total.saturating_add(1);
    }

    pub fn record_failure(&self, provider: ProviderId, model: ModelId, cfg: BreakerConfig) {
        self.record_failure_at(provider, model, cfg, Instant::now());
    }

    pub fn record_failure_at(
        &self,
        provider: ProviderId,
        model: ModelId,
        cfg: BreakerConfig,
        now: Instant,
    ) {
        let mut guard = self.lock();
        let state = guard
            .entry((provider, model))
            .or_insert_with(|| TargetState::new(now));
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        state.window_total = state.window_total.saturating_add(1);
        state.window_failures = state.window_failures.saturating_add(1);
        if state.consecutive_failures >= cfg.fail_threshold.max(1) {
            state.opened_at = Some(now);
            state.cooldown = cfg.cooldown;
        }
    }

    /// Point-in-time view of every tracked target, for the admin status
    /// endpoint. Targets that never failed are not tracked and not reported.
    #[must_use]
    pub fn states_at(&self, now: Instant) -> Vec<BreakerStateView> {
        let guard = self.lock();
        let mut out: Vec<BreakerStateView> = guard
            .iter()
            .map(|((provider, model), state)| {
                let cooldown_remaining = state
                    .opened_at
                    .and_then(|opened| state.cooldown.checked_sub(now.duration_since(opened)));
                BreakerStateView {
                    provider: *provider,
                    model: *model,
                    consecutive_failures: state.consecutive_failures,
                    open: cooldown_remaining.is_some_and(|left| !left.is_zero()),
                    cooldown_remaining,
                }
            })
            .collect();
        drop(guard);
        out.sort_by_key(|view| (view.provider, view.model));
        out
    }

    /// Failure ratio over the configured window, recomputed when the window
    /// elapses. Returns `0.0` for an unknown target or an empty window.
    #[must_use]
    pub fn error_rate(
        &self,
        provider: ProviderId,
        model: ModelId,
        window: Duration,
        now: Instant,
    ) -> f64 {
        let mut guard = self.lock();
        let Some(state) = guard.get_mut(&(provider, model)) else {
            return 0.0;
        };
        if now.duration_since(state.window_start) >= window {
            state.window_start = now;
            state.window_total = 0;
            state.window_failures = 0;
            return 0.0;
        }
        if state.window_total == 0 {
            return 0.0;
        }
        f64::from(state.window_failures) / f64::from(state.window_total)
    }
}

/// Point-in-time breaker state of one target, for operator diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BreakerStateView {
    pub provider: ProviderId,
    pub model: ModelId,
    pub consecutive_failures: u32,
    /// `true` while the circuit is open and inside its cooldown.
    pub open: bool,
    /// Time left until a half-open probe is allowed, when open.
    pub cooldown_remaining: Option<Duration>,
}

/// Snapshot liveness view at a fixed instant, so a single `select()` pass sees a
/// consistent picture of which targets are open.
pub struct BreakerLiveness<'a> {
    set: &'a BreakerSet,
    now: Instant,
}

impl<'a> BreakerLiveness<'a> {
    #[must_use]
    pub fn new(set: &'a BreakerSet, now: Instant) -> Self {
        Self { set, now }
    }
}

impl Liveness for BreakerLiveness<'_> {
    fn is_live(&self, provider: ProviderId, model: ModelId) -> bool {
        self.set.is_live_at(provider, model, self.now)
    }
}
