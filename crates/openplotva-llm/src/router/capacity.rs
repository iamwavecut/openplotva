//! Shared capacity pools: a concurrency budget over one physical resource.
//!
//! Models attached to the same pool contend for its slots; a model is
//! selectable while its pool has a free slot. The registry lives outside the
//! [`super::table::RoutingTable`] (like [`super::breaker::BreakerSet`]) so
//! in-flight accounting survives an `ArcSwap` reload: [`PoolRegistry::apply`]
//! resizes cells in place instead of swapping them, and a [`PoolPermit`] owns
//! its cell `Arc` directly so a permit outlives even a deleted pool.
//!
//! A deliberate non-semaphore design: resizing `max` downward under load must
//! not lose in-flight counts (tokio `Semaphore` shrink needs `forget_permits`
//! debt bookkeeping). All locks are short synchronous sections; only the RAII
//! permit — not a guard — crosses `.await`, per the project concurrency rules.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::Notify;

use super::table::{PoolId, RoutingTable};

/// Desired state of one pool, resolved from the routing table.
/// `max_concurrency == None` means unlimited: gauge-only, never blocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolSpec {
    pub id: PoolId,
    pub max_concurrency: Option<u32>,
}

/// Live occupancy of one pool, for the admin status endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolOccupancy {
    pub id: PoolId,
    pub max_concurrency: Option<u32>,
    pub in_flight: u32,
}

#[derive(Debug)]
struct CellState {
    max: Option<u32>,
    in_flight: u32,
}

#[derive(Debug)]
struct PoolCell {
    state: Mutex<CellState>,
}

impl PoolCell {
    fn lock(&self) -> MutexGuard<'_, CellState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// RAII slot in a capacity pool. Dropping the permit frees the slot and wakes
/// one waiter, so cancellation of an in-flight attempt can never leak a slot.
/// A permit for an unpooled model is a no-op.
#[derive(Debug, Default)]
pub struct PoolPermit {
    cell: Option<Arc<PoolCell>>,
    released: Option<Arc<Notify>>,
}

impl PoolPermit {
    fn unpooled() -> Self {
        Self::default()
    }
}

impl Drop for PoolPermit {
    fn drop(&mut self) {
        if let Some(cell) = self.cell.take() {
            let mut state = cell.lock();
            state.in_flight = state.in_flight.saturating_sub(1);
            drop(state);
            if let Some(released) = self.released.take() {
                // `notify_one` stores a wakeup when nobody waits yet, closing
                // the race between a failed acquire scan and the wait call.
                released.notify_one();
            }
        }
    }
}

/// Registry of live pool cells, keyed by pool id.
#[derive(Debug, Default)]
pub struct PoolRegistry {
    cells: Mutex<HashMap<PoolId, Arc<PoolCell>>>,
    released: Arc<Notify>,
}

impl PoolRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<PoolId, Arc<PoolCell>>> {
        self.cells
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Reconcile the registry with a freshly loaded routing table: insert new
    /// pools, update `max` in place (in-flight accounting preserved), drop
    /// cells for removed pools (outstanding permits keep their cell alive and
    /// still decrement it harmlessly). Wakes waiters so a grown pool is
    /// noticed immediately.
    pub fn apply(&self, specs: &[PoolSpec]) {
        {
            let mut cells = self.lock();
            cells.retain(|id, _| specs.iter().any(|spec| spec.id == *id));
            for spec in specs {
                match cells.get(&spec.id) {
                    Some(cell) => cell.lock().max = spec.max_concurrency,
                    None => {
                        cells.insert(
                            spec.id,
                            Arc::new(PoolCell {
                                state: Mutex::new(CellState {
                                    max: spec.max_concurrency,
                                    in_flight: 0,
                                }),
                            }),
                        );
                    }
                }
            }
        }
        self.released.notify_waiters();
    }

    /// Try to take a slot. `None` pool (unpooled model) and pools unknown to
    /// the registry always succeed with a no-op permit; a full pool returns
    /// `None`. Never blocks.
    #[must_use]
    pub fn try_acquire(&self, pool: Option<PoolId>) -> Option<PoolPermit> {
        let Some(pool) = pool else {
            return Some(PoolPermit::unpooled());
        };
        let cell = {
            let cells = self.lock();
            match cells.get(&pool) {
                Some(cell) => Arc::clone(cell),
                None => return Some(PoolPermit::unpooled()),
            }
        };
        {
            let mut state = cell.lock();
            if let Some(max) = state.max
                && state.in_flight >= max
            {
                return None;
            }
            state.in_flight = state.in_flight.saturating_add(1);
        }
        Some(PoolPermit {
            cell: Some(cell),
            released: Some(Arc::clone(&self.released)),
        })
    }

    /// Wait until any permit is released anywhere in the registry (or a pool
    /// is resized), bounded by `timeout`. Returns `false` on timeout. Callers
    /// re-run their acquire scan on wakeup; a spurious wake is harmless.
    pub async fn wait_for_release(&self, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, self.released.notified())
            .await
            .is_ok()
    }

    /// Current occupancy of every registered pool, sorted by id.
    #[must_use]
    pub fn occupancy(&self) -> Vec<PoolOccupancy> {
        let cells = self.lock();
        let mut out: Vec<PoolOccupancy> = cells
            .iter()
            .map(|(id, cell)| {
                let state = cell.lock();
                PoolOccupancy {
                    id: *id,
                    max_concurrency: state.max,
                    in_flight: state.in_flight,
                }
            })
            .collect();
        drop(cells);
        out.sort_by_key(|occupancy| occupancy.id);
        out
    }
}

/// Derive a worker count for a workflow from the capacity of its candidates:
/// the slot budgets of the distinct pools its enabled models attach to, plus
/// `unpooled_share` for every distinct model that is unpooled or in an
/// unlimited pool. Clamped to `1..=cap`. Returns `None` when the workflow has
/// no route (callers keep their configured fallback count).
#[must_use]
pub fn derived_worker_count(
    table: &RoutingTable,
    workflow_key: &str,
    unpooled_share: u32,
    cap: u32,
) -> Option<u32> {
    let mut model_ids: Vec<super::table::ModelId> = Vec::new();
    let mut seen_route = false;
    for vip in [false, true] {
        let Some(route) = table.resolve(workflow_key, vip) else {
            continue;
        };
        seen_route = true;
        let candidates = route
            .primary
            .iter()
            .chain(route.fallback_tail.iter())
            .chain(route.canary.iter())
            .chain(route.shadow.iter())
            .chain(route.triggers.iter().map(|trigger| &trigger.engage));
        for candidate in candidates {
            if !model_ids.contains(&candidate.model) {
                model_ids.push(candidate.model);
            }
        }
    }
    if !seen_route {
        return None;
    }

    let mut counted_pools: Vec<PoolId> = Vec::new();
    let mut total: u32 = 0;
    for model_id in model_ids {
        let pool_slots = table
            .model(model_id)
            .and_then(|model| model.pool_id)
            .and_then(|pool_id| table.pool(pool_id).map(|spec| (pool_id, spec)));
        match pool_slots {
            Some((pool_id, spec)) => {
                if counted_pools.contains(&pool_id) {
                    continue;
                }
                counted_pools.push(pool_id);
                total = total.saturating_add(spec.max_concurrency.unwrap_or(unpooled_share));
            }
            None => total = total.saturating_add(unpooled_share),
        }
    }
    Some(total.clamp(1, cap.max(1)))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::super::breaker::BreakerConfig;
    use super::super::policy::RetryBudget;
    use super::super::table::{
        Candidate, InferenceOverrides, Kind, ModelRow, ProviderRow, Role, RoutingTable, Scope,
        WorkflowRoute,
    };
    use super::*;

    fn registry_with(specs: &[PoolSpec]) -> PoolRegistry {
        let registry = PoolRegistry::new();
        registry.apply(specs);
        registry
    }

    #[test]
    fn shared_single_slot_pool_serializes() {
        let registry = registry_with(&[PoolSpec {
            id: 1,
            max_concurrency: Some(1),
        }]);

        let first = registry.try_acquire(Some(1)).expect("first slot");
        assert!(
            registry.try_acquire(Some(1)).is_none(),
            "second acquire must fail while the slot is held"
        );
        drop(first);
        assert!(registry.try_acquire(Some(1)).is_some());
    }

    #[test]
    fn unlimited_pool_never_blocks_but_counts() {
        let registry = registry_with(&[PoolSpec {
            id: 1,
            max_concurrency: None,
        }]);

        let _a = registry.try_acquire(Some(1)).expect("a");
        let _b = registry.try_acquire(Some(1)).expect("b");
        assert_eq!(registry.occupancy()[0].in_flight, 2);
    }

    #[test]
    fn unpooled_and_unknown_pools_are_noop_permits() {
        let registry = registry_with(&[]);
        let permit = registry.try_acquire(None).expect("unpooled");
        drop(permit);
        let permit = registry.try_acquire(Some(42)).expect("unknown pool");
        drop(permit);
        assert!(registry.occupancy().is_empty());
    }

    #[test]
    fn resize_preserves_in_flight_accounting() {
        let registry = registry_with(&[PoolSpec {
            id: 1,
            max_concurrency: Some(2),
        }]);
        let _a = registry.try_acquire(Some(1)).expect("a");
        let _b = registry.try_acquire(Some(1)).expect("b");

        // Shrink below occupancy: admission pauses until drain.
        registry.apply(&[PoolSpec {
            id: 1,
            max_concurrency: Some(1),
        }]);
        assert_eq!(registry.occupancy()[0].in_flight, 2);
        assert!(registry.try_acquire(Some(1)).is_none());

        // Grow: admission resumes without losing the in-flight count.
        registry.apply(&[PoolSpec {
            id: 1,
            max_concurrency: Some(3),
        }]);
        assert_eq!(registry.occupancy()[0].in_flight, 2);
        assert!(registry.try_acquire(Some(1)).is_some());
    }

    #[test]
    fn permit_of_removed_pool_still_decrements_harmlessly() {
        let registry = registry_with(&[PoolSpec {
            id: 1,
            max_concurrency: Some(1),
        }]);
        let permit = registry.try_acquire(Some(1)).expect("slot");

        registry.apply(&[]);
        assert!(registry.occupancy().is_empty());
        drop(permit);

        registry.apply(&[PoolSpec {
            id: 1,
            max_concurrency: Some(1),
        }]);
        assert!(registry.try_acquire(Some(1)).is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn wait_for_release_wakes_on_drop() {
        let registry = Arc::new(registry_with(&[PoolSpec {
            id: 1,
            max_concurrency: Some(1),
        }]));
        let permit = registry.try_acquire(Some(1)).expect("slot");

        let waiter = {
            let registry = Arc::clone(&registry);
            tokio::spawn(async move { registry.wait_for_release(Duration::from_secs(60)).await })
        };
        tokio::task::yield_now().await;
        drop(permit);
        assert!(waiter.await.expect("join"), "waiter must wake on release");
    }

    #[tokio::test(start_paused = true)]
    async fn wait_for_release_times_out() {
        let registry = registry_with(&[PoolSpec {
            id: 1,
            max_concurrency: Some(1),
        }]);
        let _permit = registry.try_acquire(Some(1)).expect("slot");
        assert!(!registry.wait_for_release(Duration::from_millis(50)).await);
    }

    #[tokio::test(start_paused = true)]
    async fn release_before_wait_is_not_lost() {
        let registry = registry_with(&[PoolSpec {
            id: 1,
            max_concurrency: Some(1),
        }]);
        let permit = registry.try_acquire(Some(1)).expect("slot");
        drop(permit);
        // The stored notify_one wakeup covers the scan-then-wait race window.
        assert!(registry.wait_for_release(Duration::from_millis(1)).await);
    }

    fn candidate(model: i64) -> Candidate {
        Candidate {
            provider: 1,
            model,
            role: Role::Primary,
            weight: 100,
            fallback_order: 0,
            canary_percent: 0,
            overrides: InferenceOverrides::default(),
            experiment_variant: None,
            breaker: BreakerConfig::default(),
        }
    }

    fn model_row(id: i64, pool_id: Option<PoolId>) -> ModelRow {
        ModelRow {
            id,
            provider: 1,
            model_name: format!("model-{id}"),
            base_url: None,
            capabilities: vec!["chat".to_owned()],
            embedding_dim: None,
            pool_id,
            enabled: true,
            config: serde_json::Value::Null,
        }
    }

    fn table_with(
        models: Vec<ModelRow>,
        pools: Vec<PoolSpec>,
        primaries: Vec<i64>,
    ) -> RoutingTable {
        let providers = std::iter::once((
            1,
            ProviderRow {
                id: 1,
                name: "aifarm".to_owned(),
                kind: Kind::Chat,
                protocol: None,
                endpoint: None,
                discovery_service_name: None,
                discovery_endpoint_name: None,
                api_key_ref: None,
                api_key_encrypted: None,
                enabled: true,
                config: serde_json::Value::Null,
            },
        ))
        .collect();
        let models = models.into_iter().map(|m| (m.id, m)).collect();
        let mut table = RoutingTable::new(providers, models);
        table.set_pools(pools.into_iter().map(|spec| (spec.id, spec)).collect());
        table.set_route(
            Scope::Global,
            WorkflowRoute {
                key: "dialog".to_owned(),
                kind: Kind::Chat,
                full_routing: true,
                primary: primaries.into_iter().map(candidate).collect(),
                fallback_tail: Vec::new(),
                canary: Vec::new(),
                triggers: Vec::new(),
                shadow: Vec::new(),
                retry: RetryBudget::default(),
            },
        );
        table
    }

    #[test]
    fn derived_worker_count_sums_distinct_pools_and_unpooled_shares() {
        let table = table_with(
            vec![
                model_row(10, Some(1)), // aifarm pool of 2
                model_row(20, Some(2)), // vram pool of 16
                model_row(21, Some(2)), // same pool: counted once
                model_row(30, None),    // unpooled: unpooled_share
            ],
            vec![
                PoolSpec {
                    id: 1,
                    max_concurrency: Some(2),
                },
                PoolSpec {
                    id: 2,
                    max_concurrency: Some(16),
                },
            ],
            vec![10, 20, 21, 30],
        );

        assert_eq!(derived_worker_count(&table, "dialog", 2, 24), Some(20));
        assert_eq!(
            derived_worker_count(&table, "dialog", 2, 8),
            Some(8),
            "cap clamps the derived count"
        );
        assert_eq!(
            derived_worker_count(&table, "missing", 2, 24),
            None,
            "unknown workflow keeps the configured fallback"
        );
    }

    #[test]
    fn derived_worker_count_treats_unlimited_pool_as_unpooled_share() {
        let table = table_with(
            vec![model_row(10, Some(1))],
            vec![PoolSpec {
                id: 1,
                max_concurrency: None,
            }],
            vec![10],
        );
        assert_eq!(derived_worker_count(&table, "dialog", 3, 24), Some(3));
    }

    #[test]
    fn derived_worker_count_never_returns_zero() {
        let table = table_with(vec![], vec![], vec![]);
        assert_eq!(derived_worker_count(&table, "dialog", 0, 24), Some(1));
    }
}
