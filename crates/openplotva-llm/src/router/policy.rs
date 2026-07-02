//! Pure, synchronous, kind-agnostic selection policy.
//!
//! [`select`] turns a [`WorkflowRoute`] plus live signals (circuit liveness,
//! trigger engagement, an RNG) into an ordered list of [`Attempt`]s the executor
//! walks under the retry budget. It contains no IO and no async, so the whole
//! routing decision is unit-testable with a seeded RNG.

use std::cmp::Ordering;
use std::time::Duration;

use rand::RngExt;

use super::breaker::BreakerConfig;
use super::table::{Candidate, InferenceOverrides, ModelId, ProviderId, Role, WorkflowRoute};
use super::triggers::TriggerId;

/// Bound on how far the executor walks a fallback chain for one request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryBudget {
    pub max_hops: u32,
    pub wall_clock: Duration,
}

impl Default for RetryBudget {
    fn default() -> Self {
        Self {
            max_hops: 3,
            wall_clock: Duration::from_millis(60_000),
        }
    }
}

/// One resolved target to try, with the overrides and experiment tag to apply.
#[derive(Clone, Debug, PartialEq)]
pub struct Attempt {
    pub provider: ProviderId,
    pub model: ModelId,
    pub role: Role,
    pub overrides: InferenceOverrides,
    pub variant: Option<String>,
    /// Circuit-breaker tuning to apply when recording this attempt's outcome.
    pub breaker: BreakerConfig,
}

impl Attempt {
    fn of(candidate: &Candidate) -> Self {
        Self {
            provider: candidate.provider,
            model: candidate.model,
            role: candidate.role,
            overrides: candidate.overrides.clone(),
            variant: candidate.experiment_variant.clone(),
            breaker: candidate.breaker,
        }
    }
}

/// Circuit-liveness view, snapshotted at a fixed instant for one selection pass.
pub trait Liveness {
    fn is_live(&self, provider: ProviderId, model: ModelId) -> bool;
}

/// Trigger-engagement view, written by the background poller.
pub trait TriggerView {
    fn engaged(&self, id: TriggerId) -> bool;
}

/// Build the ordered attempt chain for one request, truncated to
/// `retry.max_hops`. See [`select_chain`] for the ordering rules.
#[must_use]
pub fn select(
    route: &WorkflowRoute,
    liveness: &dyn Liveness,
    triggers: &dyn TriggerView,
    rng: &mut impl RngExt,
) -> Vec<Attempt> {
    let mut attempts = select_chain(route, liveness, triggers, rng);
    let cap = route.retry.max_hops.max(1) as usize;
    attempts.truncate(cap);
    attempts
}

/// Build the full ordered attempt chain for one request, without the
/// `max_hops` truncation. The capacity-aware executor walks this chain and
/// budgets hops by attempts actually started, so a busy (pool-exhausted)
/// candidate can be skipped without hiding a free candidate further down.
///
/// Full-routing workflows: an optional canary (rolled by `canary_percent`), then
/// a weighted random permutation of the live primaries plus any engaged-and-live
/// overflow targets (a dead target's weight share is thereby redistributed
/// proportionally to the remaining live weights), then the ordered fallback tail,
/// then any dead targets as last-resort half-open probes. Config-only workflows:
/// the single primary plus the ordered fallback tail (live first), no weighting,
/// no triggers, no canary.
#[must_use]
pub fn select_chain(
    route: &WorkflowRoute,
    liveness: &dyn Liveness,
    triggers: &dyn TriggerView,
    rng: &mut impl RngExt,
) -> Vec<Attempt> {
    let is_live = |c: &Candidate| liveness.is_live(c.provider, c.model);

    let mut live: Vec<&Candidate> = Vec::new();
    let mut dead: Vec<&Candidate> = Vec::new();

    if route.full_routing {
        // Optional canary, rolled independently of the weighted pool.
        if let Some(canary) = route.canary.iter().find(|c| is_live(c)) {
            let pct = canary.canary_percent.min(100);
            if pct > 0 && rng.random_range(0..100) < pct {
                live.push(canary);
            }
        }

        // Weighted pool: live primaries + engaged-and-live overflow candidates.
        let mut pool: Vec<&Candidate> = route.primary.iter().filter(|c| is_live(c)).collect();
        for trigger in &route.triggers {
            if triggers.engaged(trigger.id) && is_live(&trigger.engage) {
                pool.push(&trigger.engage);
            }
        }
        live.extend(weighted_permutation(&pool, rng));

        // Ordered fallback tail (live).
        let mut tail: Vec<&Candidate> = route.fallback_tail.iter().filter(|c| is_live(c)).collect();
        tail.sort_by_key(|c| c.fallback_order);
        live.extend(tail);

        // Dead bucket: every other candidate, as a last-resort probe.
        for candidate in route
            .primary
            .iter()
            .chain(route.triggers.iter().map(|t| &t.engage))
            .chain(route.fallback_tail.iter())
        {
            if !is_live(candidate) {
                dead.push(candidate);
            }
        }
    } else {
        // Config-only: primary then ordered fallback tail; live first, dead last.
        let mut chain: Vec<&Candidate> = route.primary.iter().collect();
        let mut tail: Vec<&Candidate> = route.fallback_tail.iter().collect();
        tail.sort_by_key(|c| c.fallback_order);
        chain.extend(tail);
        for candidate in chain {
            if is_live(candidate) {
                live.push(candidate);
            } else {
                dead.push(candidate);
            }
        }
    }

    let mut attempts: Vec<Attempt> = live.into_iter().chain(dead).map(Attempt::of).collect();

    // Deduplicate while preserving order: a target may appear as both primary and
    // overflow; a request should not retry the identical (provider, model) twice.
    let mut seen: Vec<(ProviderId, ModelId)> = Vec::new();
    attempts.retain(|a| {
        let key = (a.provider, a.model);
        if seen.contains(&key) {
            false
        } else {
            seen.push(key);
            true
        }
    });

    attempts
}

/// Efraimidis–Spirakis weighted random permutation: each item draws a key
/// `u^(1/weight)` and items are ordered by descending key, so the first item is
/// chosen with probability proportional to its weight and the rest form a
/// weight-respecting fallback order. Keys are computed once (never inside the
/// comparator) to keep a consistent total order.
fn weighted_permutation<'a>(
    candidates: &[&'a Candidate],
    rng: &mut impl RngExt,
) -> Vec<&'a Candidate> {
    let mut keyed: Vec<(f64, &Candidate)> = candidates
        .iter()
        .map(|candidate| {
            let u: f64 = rng.random_range(0.0..1.0);
            let weight = f64::from(candidate.weight.max(1));
            (u.powf(1.0 / weight), *candidate)
        })
        .collect();
    keyed.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));
    keyed.into_iter().map(|(_, candidate)| candidate).collect()
}
