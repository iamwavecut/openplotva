//! DB-free control plane for admin-configurable LLM routing.
//!
//! The app layer loads a [`RoutingTable`] from Postgres, publishes it through a
//! [`RouterHandle`], and runs the kind-specific transport adapters. Everything in
//! this module is engine-agnostic policy over abstract provider/model ids, so it
//! carries no sqlx, no vendor SDKs, and no async — only the selection decision.

pub mod breaker;
pub mod handle;
pub mod policy;
pub mod table;
pub mod triggers;

pub use breaker::{BreakerConfig, BreakerLiveness, BreakerSet};
pub use handle::RouterHandle;
pub use policy::{Attempt, Liveness, RetryBudget, TriggerView, select};
pub use table::{
    Candidate, InferenceOverrides, Kind, ModelId, ModelRow, ProviderId, ProviderRow, Role,
    RoutingTable, Scope, WorkflowRoute,
};
pub use triggers::{TriggerCondition, TriggerId, TriggerSpec, TriggerState};

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::*;

    fn candidate(provider: ProviderId, model: ModelId, role: Role, weight: u32) -> Candidate {
        Candidate {
            provider,
            model,
            role,
            weight,
            fallback_order: 0,
            canary_percent: 0,
            overrides: InferenceOverrides::default(),
            experiment_variant: None,
            breaker: BreakerConfig::default(),
        }
    }

    fn route(full: bool) -> WorkflowRoute {
        WorkflowRoute {
            key: "dialog".to_owned(),
            kind: Kind::Chat,
            full_routing: full,
            primary: Vec::new(),
            fallback_tail: Vec::new(),
            canary: Vec::new(),
            triggers: Vec::new(),
            shadow: Vec::new(),
            retry: RetryBudget::default(),
        }
    }

    struct AllLive;
    impl Liveness for AllLive {
        fn is_live(&self, _p: ProviderId, _m: ModelId) -> bool {
            true
        }
    }

    struct DeadSet(HashSet<(ProviderId, ModelId)>);
    impl Liveness for DeadSet {
        fn is_live(&self, p: ProviderId, m: ModelId) -> bool {
            !self.0.contains(&(p, m))
        }
    }

    struct NoTriggers;
    impl TriggerView for NoTriggers {
        fn engaged(&self, _id: TriggerId) -> bool {
            false
        }
    }

    struct EngagedSet(HashSet<TriggerId>);
    impl TriggerView for EngagedSet {
        fn engaged(&self, id: TriggerId) -> bool {
            self.0.contains(&id)
        }
    }

    #[test]
    fn weighted_distribution_is_roughly_proportional() {
        let mut r = route(true);
        r.primary = vec![
            candidate(1, 1, Role::Primary, 70),
            candidate(2, 2, Role::Primary, 20),
            candidate(3, 3, Role::Primary, 10),
        ];
        r.retry.max_hops = 1;
        let mut rng = StdRng::seed_from_u64(7);
        let mut counts = [0_u32; 3];
        for _ in 0..10_000 {
            let attempts = select(&r, &AllLive, &NoTriggers, &mut rng);
            match attempts[0].provider {
                1 => counts[0] += 1,
                2 => counts[1] += 1,
                3 => counts[2] += 1,
                other => panic!("unexpected provider {other}"),
            }
        }
        // ~70/20/10 with generous slack for sampling noise.
        assert!((6500..=7500).contains(&counts[0]), "p70 got {}", counts[0]);
        assert!((1500..=2500).contains(&counts[1]), "p20 got {}", counts[1]);
        assert!((600..=1400).contains(&counts[2]), "p10 got {}", counts[2]);
    }

    #[test]
    fn dead_primary_share_redistributes_proportionally() {
        let mut r = route(true);
        r.primary = vec![
            candidate(1, 1, Role::Primary, 70),
            candidate(2, 2, Role::Primary, 20),
            candidate(3, 3, Role::Primary, 10),
        ];
        r.retry.max_hops = 1;
        // Kill the 70% target; remaining 20/10 should split ~2:1.
        let dead = DeadSet(HashSet::from([(1, 1)]));
        let mut rng = StdRng::seed_from_u64(11);
        let mut counts = [0_u32; 2];
        for _ in 0..9_000 {
            let attempts = select(&r, &dead, &NoTriggers, &mut rng);
            match attempts[0].provider {
                2 => counts[0] += 1,
                3 => counts[1] += 1,
                other => panic!("unexpected provider {other}"),
            }
        }
        // 20/(20+10)=2/3 and 10/30=1/3.
        assert!((5400..=6600).contains(&counts[0]), "p got {}", counts[0]);
        assert!((2400..=3600).contains(&counts[1]), "p got {}", counts[1]);
    }

    #[test]
    fn fallback_tail_follows_primaries_in_order() {
        let mut r = route(true);
        r.primary = vec![candidate(1, 1, Role::Primary, 100)];
        let mut f1 = candidate(2, 2, Role::Fallback, 0);
        f1.fallback_order = 1;
        let mut f0 = candidate(3, 3, Role::Fallback, 0);
        f0.fallback_order = 0;
        r.fallback_tail = vec![f1, f0];
        r.retry.max_hops = 9;
        let mut rng = StdRng::seed_from_u64(1);
        let attempts = select(&r, &AllLive, &NoTriggers, &mut rng);
        assert_eq!(attempts[0].provider, 1);
        assert_eq!(attempts[1].provider, 3); // order 0 first
        assert_eq!(attempts[2].provider, 2); // order 1 next
    }

    #[test]
    fn engaged_trigger_injects_overflow_into_pool() {
        let mut r = route(true);
        r.primary = vec![candidate(1, 1, Role::Primary, 100)];
        r.triggers = vec![TriggerSpec {
            id: 42,
            condition: TriggerCondition::QueueDepth {
                queue: "dialog-aifarm".to_owned(),
                high: 30,
                low: 20,
            },
            engage: candidate(9, 9, Role::Overflow, 100),
        }];
        r.retry.max_hops = 5;
        let mut rng = StdRng::seed_from_u64(3);

        let not_engaged = select(&r, &AllLive, &NoTriggers, &mut rng);
        assert!(not_engaged.iter().all(|a| a.provider != 9));

        let engaged = select(&r, &AllLive, &EngagedSet(HashSet::from([42])), &mut rng);
        assert!(engaged.iter().any(|a| a.provider == 9));
    }

    #[test]
    fn config_only_ignores_weights_keeps_primary_then_fallback() {
        let mut r = route(false);
        r.primary = vec![candidate(1, 1, Role::Primary, 0)];
        r.fallback_tail = vec![candidate(2, 2, Role::Fallback, 0)];
        r.retry.max_hops = 9;
        let mut rng = StdRng::seed_from_u64(5);
        let attempts = select(&r, &AllLive, &NoTriggers, &mut rng);
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].provider, 1);
        assert_eq!(attempts[1].provider, 2);
    }

    #[test]
    fn max_hops_truncates_chain() {
        let mut r = route(true);
        r.primary = vec![
            candidate(1, 1, Role::Primary, 50),
            candidate(2, 2, Role::Primary, 50),
        ];
        r.fallback_tail = vec![candidate(3, 3, Role::Fallback, 0)];
        r.retry.max_hops = 1;
        let mut rng = StdRng::seed_from_u64(9);
        let attempts = select(&r, &AllLive, &NoTriggers, &mut rng);
        assert_eq!(attempts.len(), 1);
    }

    #[test]
    fn dead_targets_appear_last_as_probes() {
        let mut r = route(true);
        r.primary = vec![candidate(1, 1, Role::Primary, 100)];
        r.fallback_tail = vec![candidate(2, 2, Role::Fallback, 0)];
        r.retry.max_hops = 9;
        let dead = DeadSet(HashSet::from([(1, 1)]));
        let mut rng = StdRng::seed_from_u64(2);
        let attempts = select(&r, &dead, &NoTriggers, &mut rng);
        // live fallback first, dead primary appended last.
        assert_eq!(attempts[0].provider, 2);
        assert_eq!(attempts[1].provider, 1);
    }

    #[test]
    fn breaker_opens_after_threshold_and_recovers_after_cooldown() {
        use std::time::{Duration, Instant};
        let set = BreakerSet::new();
        let cfg = BreakerConfig {
            fail_threshold: 3,
            cooldown: Duration::from_millis(100),
        };
        let t0 = Instant::now();
        assert!(set.is_live_at(1, 1, t0));
        set.record_failure_at(1, 1, cfg, t0);
        set.record_failure_at(1, 1, cfg, t0);
        assert!(set.is_live_at(1, 1, t0)); // 2 < 3, still closed
        set.record_failure_at(1, 1, cfg, t0);
        assert!(!set.is_live_at(1, 1, t0)); // opened
        assert!(!set.is_live_at(1, 1, t0 + Duration::from_millis(50)));
        assert!(set.is_live_at(1, 1, t0 + Duration::from_millis(150))); // half-open probe
        set.record_success_at(1, 1, t0 + Duration::from_millis(160));
        assert!(set.is_live_at(1, 1, t0 + Duration::from_millis(170)));
    }
}
