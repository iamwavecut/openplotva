//! Lane pools: the width of one physical inference resource and its queue.
//!
//! A pool admits up to `max_concurrency` requests at once. Further requests
//! wait in a per-pool priority queue (lower [`LanePriority`] first, arrival
//! order within a priority), and a released lane is handed directly to the head
//! waiter as a [`PoolPermit`] sent through a oneshot channel: no wake-all, no
//! race. A waiter that gives up leaves the queue; a permit already sent to it
//! releases the lane again when the channel drops, so no lane can leak.
//!
//! The registry lives outside the [`super::table::RoutingTable`] (like
//! [`super::breaker::BreakerSet`]) so in-flight accounting survives an
//! `ArcSwap` reload: [`PoolRegistry::apply`] resizes cells in place, and a
//! [`PoolPermit`] owns its cell `Arc` so a permit outlives even a deleted pool.
//! All locks are short synchronous sections; only the RAII permit, never a
//! guard, crosses `.await`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use super::table::{PoolId, RoutingTable};

/// Order in which the waiters of one lane pool are served: lower first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LanePriority {
    #[default]
    Interactive = 0,
    UserTool = 1,
    Background = 2,
}

impl LanePriority {
    #[must_use]
    pub fn from_db(value: i16) -> Self {
        match value {
            i16::MIN..=0 => Self::Interactive,
            1 => Self::UserTool,
            _ => Self::Background,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::UserTool => "user_tool",
            Self::Background => "background",
        }
    }
}

/// Desired state of one pool, resolved from the routing table.
/// `max_concurrency == None` means unlimited: gauge-only, never blocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolSpec {
    pub id: PoolId,
    pub max_concurrency: Option<u32>,
}

/// Live state of one pool, for the admin status endpoint and the runtime API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolOccupancy {
    pub id: PoolId,
    pub max_concurrency: Option<u32>,
    pub in_flight: u32,
    /// Waiters per priority: interactive, user tool, background.
    pub queued: [u32; 3],
    pub mean_hold_ms: Option<u64>,
}

/// Weight of the newest sample in the mean lane hold time.
const HOLD_MEAN_WEIGHT: f64 = 0.2;

type WaiterKey = (LanePriority, u64);

#[derive(Debug)]
struct CellState {
    max: Option<u32>,
    in_flight: u32,
    waiters: BTreeMap<WaiterKey, oneshot::Sender<PoolPermit>>,
    next_seq: u64,
    mean_hold: Option<Duration>,
}

impl CellState {
    fn new(max: Option<u32>) -> Self {
        Self {
            max,
            in_flight: 0,
            waiters: BTreeMap::new(),
            next_seq: 0,
            mean_hold: None,
        }
    }

    fn has_free_lane(&self) -> bool {
        self.max.is_none_or(|max| self.in_flight < max)
    }

    fn record_hold(&mut self, held: Duration) {
        self.mean_hold = Some(match self.mean_hold {
            Some(mean) => {
                let mean = mean.as_secs_f64();
                Duration::from_secs_f64(mean + HOLD_MEAN_WEIGHT * (held.as_secs_f64() - mean))
            }
            None => held,
        });
    }
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

/// RAII lane of a pool. Dropping it hands the lane to the next waiter, so
/// cancellation of an in-flight attempt can never leak a lane. A permit for an
/// unpooled model is a no-op.
#[derive(Debug, Default)]
pub struct PoolPermit {
    cell: Option<Arc<PoolCell>>,
    acquired_at: Option<Instant>,
}

impl PoolPermit {
    fn unpooled() -> Self {
        Self::default()
    }

    fn held(cell: Arc<PoolCell>) -> Self {
        Self {
            cell: Some(cell),
            acquired_at: Some(Instant::now()),
        }
    }
}

impl Drop for PoolPermit {
    fn drop(&mut self) {
        if let Some(cell) = self.cell.take() {
            let mut state = cell.lock();
            state.in_flight = state.in_flight.saturating_sub(1);
            if let Some(acquired_at) = self.acquired_at {
                state.record_hold(acquired_at.elapsed());
            }
            serve_waiters(&cell, &mut state);
        }
    }
}

/// Hand free lanes to waiters in queue order. A send to a waiter that already
/// gave up returns the permit; it is disarmed (the lock is held, so its drop
/// must not release) and the lane goes to the next waiter.
fn serve_waiters(cell: &Arc<PoolCell>, state: &mut CellState) {
    while state.has_free_lane() {
        let Some((_, waiter)) = state.waiters.pop_first() else {
            return;
        };
        state.in_flight = state.in_flight.saturating_add(1);
        if let Err(mut unclaimed) = waiter.send(PoolPermit::held(Arc::clone(cell))) {
            unclaimed.cell = None;
            state.in_flight = state.in_flight.saturating_sub(1);
        }
    }
}

/// Queue entry of one waiting `acquire`. Dropping it (budget end, cancellation)
/// leaves the queue; after a handoff the entry is already gone.
struct QueuedWaiter<'a> {
    cell: &'a PoolCell,
    key: WaiterKey,
}

impl Drop for QueuedWaiter<'_> {
    fn drop(&mut self) {
        self.cell.lock().waiters.remove(&self.key);
    }
}

/// Registry of live pool cells, keyed by pool id.
#[derive(Debug, Default)]
pub struct PoolRegistry {
    cells: Mutex<HashMap<PoolId, Arc<PoolCell>>>,
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

    fn cell(&self, pool: Option<PoolId>) -> Option<Arc<PoolCell>> {
        pool.and_then(|pool| self.lock().get(&pool).cloned())
    }

    /// Reconcile the registry with a freshly loaded routing table: insert new
    /// pools, update `max` in place (in-flight accounting preserved), drop
    /// cells of removed pools (outstanding permits keep their cell alive). A
    /// grown pool serves its waiters at once; a shrunk one admits nothing new
    /// until in-flight work drains below the new width.
    pub fn apply(&self, specs: &[PoolSpec]) {
        let mut cells = self.lock();
        cells.retain(|id, _| specs.iter().any(|spec| spec.id == *id));
        for spec in specs {
            match cells.get(&spec.id) {
                Some(cell) => {
                    let mut state = cell.lock();
                    state.max = spec.max_concurrency;
                    serve_waiters(cell, &mut state);
                }
                None => {
                    cells.insert(
                        spec.id,
                        Arc::new(PoolCell {
                            state: Mutex::new(CellState::new(spec.max_concurrency)),
                        }),
                    );
                }
            }
        }
    }

    /// Take a free lane without waiting. Fails while the pool is full or while
    /// others wait, so it never jumps the queue. Unpooled models and pools
    /// unknown to the registry always succeed with a no-op permit.
    #[must_use]
    pub fn try_acquire(&self, pool: Option<PoolId>) -> Option<PoolPermit> {
        let Some(cell) = self.cell(pool) else {
            return Some(PoolPermit::unpooled());
        };
        {
            let mut state = cell.lock();
            if !state.waiters.is_empty() || !state.has_free_lane() {
                return None;
            }
            state.in_flight = state.in_flight.saturating_add(1);
        }
        Some(PoolPermit::held(cell))
    }

    /// Whether [`Self::try_acquire`] would succeed right now.
    #[must_use]
    pub fn has_free_lane(&self, pool: Option<PoolId>) -> bool {
        self.cell(pool).is_none_or(|cell| {
            let state = cell.lock();
            state.waiters.is_empty() && state.has_free_lane()
        })
    }

    /// Wait in the pool's queue for a lane. Returns `None` once `budget` ends;
    /// the waiter then leaves the queue. `budget == None` waits without limit.
    pub async fn acquire(
        &self,
        pool: Option<PoolId>,
        priority: LanePriority,
        budget: Option<Duration>,
    ) -> Option<PoolPermit> {
        let Some(cell) = self.cell(pool) else {
            return Some(PoolPermit::unpooled());
        };
        let (receiver, _queued) = {
            let mut state = cell.lock();
            if state.waiters.is_empty() && state.has_free_lane() {
                state.in_flight = state.in_flight.saturating_add(1);
                drop(state);
                return Some(PoolPermit::held(cell));
            }
            let (sender, receiver) = oneshot::channel();
            let key = (priority, state.next_seq);
            state.next_seq = state.next_seq.wrapping_add(1);
            state.waiters.insert(key, sender);
            (
                receiver,
                QueuedWaiter {
                    cell: cell.as_ref(),
                    key,
                },
            )
        };
        match budget {
            Some(budget) => tokio::time::timeout(budget, receiver).await.ok()?.ok(),
            None => receiver.await.ok(),
        }
    }

    /// Expected wait for a new request of `priority`: the waiters it would
    /// queue behind, in rounds of the pool width, times the mean lane hold
    /// time. `Some(ZERO)` when a lane is free; `None` for unpooled or
    /// unlimited pools and before the pool's first lane was released.
    #[must_use]
    pub fn forecast_wait(&self, pool: Option<PoolId>, priority: LanePriority) -> Option<Duration> {
        let cell = self.cell(pool)?;
        let state = cell.lock();
        let width = state.max?;
        if state.waiters.is_empty() && state.has_free_lane() {
            return Some(Duration::ZERO);
        }
        let mean_hold = state.mean_hold?;
        let ahead = state
            .waiters
            .keys()
            .filter(|(waiting, _)| *waiting <= priority)
            .count();
        let rounds = (ahead + 1).div_ceil(width.max(1) as usize);
        Some(mean_hold.saturating_mul(u32::try_from(rounds).unwrap_or(u32::MAX)))
    }

    /// Current state of every registered pool, sorted by id.
    #[must_use]
    pub fn occupancy(&self) -> Vec<PoolOccupancy> {
        let cells = self.lock();
        let mut out: Vec<PoolOccupancy> = cells
            .iter()
            .map(|(id, cell)| {
                let state = cell.lock();
                let mut queued = [0_u32; 3];
                for (priority, _) in state.waiters.keys() {
                    let slot = &mut queued[*priority as usize];
                    *slot = slot.saturating_add(1);
                }
                PoolOccupancy {
                    id: *id,
                    max_concurrency: state.max,
                    in_flight: state.in_flight,
                    queued,
                    mean_hold_ms: state
                        .mean_hold
                        .map(|mean| u64::try_from(mean.as_millis()).unwrap_or(u64::MAX)),
                }
            })
            .collect();
        drop(cells);
        out.sort_by_key(|occupancy| occupancy.id);
        out
    }
}

/// Workers for a workflow: the width of its heaviest primary's lane pool times
/// `per_lane` (at least one per lane), clamped to `1..=cap`. Fallback capacity
/// is not throughput, so the rest of the chain does not count. `None` when the
/// workflow has no route or its primary has no bounded lane pool (callers keep
/// their configured count).
#[must_use]
pub fn lane_worker_count(
    table: &RoutingTable,
    workflow_key: &str,
    per_lane: u32,
    cap: u32,
) -> Option<u32> {
    let route = table.resolve(workflow_key, false)?;
    let primary = route
        .primary
        .iter()
        .max_by_key(|candidate| candidate.weight)?;
    let pool = table.model(primary.model)?.pool_id?;
    let width = table.pool(pool)?.max_concurrency?;
    Some(width.saturating_mul(per_lane.max(1)).clamp(1, cap.max(1)))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
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

    fn registry(max: u32) -> PoolRegistry {
        registry_with(&[PoolSpec {
            id: 1,
            max_concurrency: Some(max),
        }])
    }

    #[tokio::test]
    async fn released_lane_goes_to_the_oldest_waiter_of_the_best_priority() {
        let pools = Arc::new(registry(1));
        let held = pools.try_acquire(Some(1)).expect("free lane");
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for (name, priority) in [
            ("background", LanePriority::Background),
            ("interactive-1", LanePriority::Interactive),
            ("interactive-2", LanePriority::Interactive),
        ] {
            let pools = Arc::clone(&pools);
            let order = Arc::clone(&order);
            tasks.push(tokio::spawn(async move {
                let permit = pools.acquire(Some(1), priority, None).await.expect("lane");
                order.lock().expect("order").push(name);
                drop(permit);
            }));
            tokio::task::yield_now().await;
        }
        drop(held);
        for task in tasks {
            task.await.expect("task");
        }
        assert_eq!(
            *order.lock().expect("order"),
            vec!["interactive-1", "interactive-2", "background"]
        );
    }

    #[tokio::test]
    async fn try_acquire_does_not_jump_the_queue() {
        let pools = Arc::new(registry(1));
        let held = pools.try_acquire(Some(1)).expect("free lane");
        let waiter = {
            let pools = Arc::clone(&pools);
            tokio::spawn(
                async move { pools.acquire(Some(1), LanePriority::Background, None).await },
            )
        };
        tokio::task::yield_now().await;
        assert!(!pools.has_free_lane(Some(1)));
        drop(held);
        let handed = waiter
            .await
            .expect("task")
            .expect("lane handed to the waiter");
        assert!(pools.try_acquire(Some(1)).is_none());
        drop(handed);
        assert!(pools.try_acquire(Some(1)).is_some());
    }

    #[tokio::test]
    async fn a_waiter_that_gives_up_after_the_handoff_does_not_leak_the_lane() {
        let pools = Arc::new(registry(1));
        let held = pools.try_acquire(Some(1)).expect("free lane");
        let waiter = {
            let pools = Arc::clone(&pools);
            tokio::spawn(async move {
                pools
                    .acquire(Some(1), LanePriority::Interactive, None)
                    .await
            })
        };
        tokio::task::yield_now().await;
        drop(held);
        waiter.abort();
        let _ = waiter.await;
        assert!(
            pools.try_acquire(Some(1)).is_some(),
            "the handed lane came back"
        );
        assert_eq!(pools.occupancy()[0].queued, [0, 0, 0]);
    }

    #[tokio::test]
    async fn a_waiter_dropped_before_the_handoff_is_skipped() {
        let pools = Arc::new(registry(1));
        let held = pools.try_acquire(Some(1)).expect("free lane");
        let gone = {
            let pools = Arc::clone(&pools);
            tokio::spawn(async move {
                pools
                    .acquire(Some(1), LanePriority::Interactive, None)
                    .await
            })
        };
        tokio::task::yield_now().await;
        let next = {
            let pools = Arc::clone(&pools);
            tokio::spawn(async move { pools.acquire(Some(1), LanePriority::UserTool, None).await })
        };
        tokio::task::yield_now().await;
        gone.abort();
        let _ = gone.await;
        drop(held);
        let handed = next.await.expect("task").expect("lane for the next waiter");
        assert_eq!(pools.occupancy()[0].in_flight, 1);
        drop(handed);
        assert_eq!(pools.occupancy()[0].in_flight, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_budget_ends_the_wait_and_leaves_the_queue() {
        let pools = registry(1);
        let _held = pools.try_acquire(Some(1)).expect("free lane");
        let waited = pools
            .acquire(
                Some(1),
                LanePriority::Interactive,
                Some(Duration::from_secs(2)),
            )
            .await;
        assert!(waited.is_none());
        assert_eq!(pools.occupancy()[0].queued, [0, 0, 0]);
    }

    #[tokio::test]
    async fn growing_a_pool_serves_waiters_and_shrinking_admits_nothing_new() {
        let pools = Arc::new(registry(1));
        let held = pools.try_acquire(Some(1)).expect("free lane");
        let waiter = {
            let pools = Arc::clone(&pools);
            tokio::spawn(async move {
                pools
                    .acquire(Some(1), LanePriority::Interactive, None)
                    .await
            })
        };
        tokio::task::yield_now().await;
        pools.apply(&[PoolSpec {
            id: 1,
            max_concurrency: Some(2),
        }]);
        let second = waiter
            .await
            .expect("task")
            .expect("served by the grown pool");
        pools.apply(&[PoolSpec {
            id: 1,
            max_concurrency: Some(1),
        }]);
        drop(held);
        assert!(
            pools.try_acquire(Some(1)).is_none(),
            "still at the new width"
        );
        drop(second);
        assert!(pools.try_acquire(Some(1)).is_some());
    }

    #[tokio::test]
    async fn queued_counts_follow_priorities() {
        let pools = Arc::new(registry(1));
        let _held = pools.try_acquire(Some(1)).expect("free lane");
        let mut waiters = Vec::new();
        for priority in [
            LanePriority::Background,
            LanePriority::Background,
            LanePriority::UserTool,
        ] {
            let pools = Arc::clone(&pools);
            waiters.push(tokio::spawn(async move {
                pools.acquire(Some(1), priority, None).await
            }));
            tokio::task::yield_now().await;
        }
        assert_eq!(pools.occupancy()[0].queued, [0, 1, 2]);
        for waiter in &waiters {
            waiter.abort();
        }
        for waiter in waiters {
            let _ = waiter.await;
        }
        assert_eq!(pools.occupancy()[0].queued, [0, 0, 0]);
    }

    #[tokio::test]
    async fn forecast_counts_waiters_ahead_and_mean_hold_time() {
        let pools = Arc::new(registry(2));
        assert_eq!(
            pools.forecast_wait(Some(1), LanePriority::Interactive),
            Some(Duration::ZERO)
        );
        let first = pools.try_acquire(Some(1)).expect("lane");
        let second = pools.try_acquire(Some(1)).expect("lane");
        assert_eq!(
            pools.forecast_wait(Some(1), LanePriority::Interactive),
            None,
            "no hold history yet"
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
        drop(first);
        let first = pools.try_acquire(Some(1)).expect("lane");
        let one_round = pools
            .forecast_wait(Some(1), LanePriority::Interactive)
            .expect("history");
        assert!(one_round >= Duration::from_millis(30), "{one_round:?}");

        let mut waiters = Vec::new();
        for _ in 0..2 {
            let pools = Arc::clone(&pools);
            waiters.push(tokio::spawn(async move {
                pools
                    .acquire(Some(1), LanePriority::Interactive, None)
                    .await
            }));
            tokio::task::yield_now().await;
        }
        let two_rounds = pools
            .forecast_wait(Some(1), LanePriority::Interactive)
            .expect("history");
        assert_eq!(
            two_rounds,
            one_round * 2,
            "two waiters ahead fill one round"
        );
        assert_eq!(
            pools.forecast_wait(Some(1), LanePriority::Background),
            Some(two_rounds)
        );
        for waiter in &waiters {
            waiter.abort();
        }
        drop((first, second));
        assert_eq!(pools.forecast_wait(None, LanePriority::Interactive), None);
    }

    #[test]
    fn priorities_parse_from_the_database() {
        assert_eq!(LanePriority::from_db(0), LanePriority::Interactive);
        assert_eq!(LanePriority::from_db(1), LanePriority::UserTool);
        assert_eq!(LanePriority::from_db(2), LanePriority::Background);
        assert_eq!(LanePriority::from_db(7), LanePriority::Background);
        assert_eq!(LanePriority::from_db(-1), LanePriority::Interactive);
        assert!(LanePriority::Interactive < LanePriority::Background);
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
                runtime_hint: None,
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
                lane_priority: LanePriority::Interactive,
                fallback_reserve: Duration::from_secs(30),
            },
        );
        table
    }

    fn with_fallbacks(mut table: RoutingTable, fallbacks: Vec<i64>) -> RoutingTable {
        let mut route = table.resolve("dialog", false).expect("route").clone();
        route.fallback_tail = fallbacks
            .into_iter()
            .map(|model| Candidate {
                role: Role::Fallback,
                ..candidate(model)
            })
            .collect();
        table.set_route(Scope::Global, route);
        table
    }

    fn dialog_chain() -> RoutingTable {
        with_fallbacks(
            table_with(
                vec![
                    model_row(10, Some(1)), // GPU0: eight lanes
                    model_row(20, Some(2)), // GPU2: eight lanes
                    model_row(30, None),    // hosted API
                ],
                vec![
                    PoolSpec {
                        id: 1,
                        max_concurrency: Some(8),
                    },
                    PoolSpec {
                        id: 2,
                        max_concurrency: Some(8),
                    },
                ],
                vec![10],
            ),
            vec![20, 30],
        )
    }

    #[test]
    fn workers_follow_the_primary_lane_pool_only() {
        let table = dialog_chain();
        assert_eq!(lane_worker_count(&table, "dialog", 2, 24), Some(16));
        assert_eq!(
            lane_worker_count(&table, "dialog", 2, 10),
            Some(10),
            "cap clamps the count"
        );
        assert_eq!(
            lane_worker_count(&table, "dialog", 0, 24),
            Some(8),
            "at least one worker per lane"
        );
        assert_eq!(
            lane_worker_count(&table, "missing", 2, 24),
            None,
            "unknown workflow keeps the configured count"
        );
    }

    #[test]
    fn the_heaviest_primary_decides_the_worker_count() {
        let mut table = dialog_chain();
        let mut route = table.resolve("dialog", false).expect("route").clone();
        route.primary = vec![
            Candidate {
                weight: 10,
                ..candidate(30)
            },
            Candidate {
                weight: 90,
                ..candidate(10)
            },
        ];
        table.set_route(Scope::Global, route);
        assert_eq!(lane_worker_count(&table, "dialog", 2, 24), Some(16));
    }

    #[test]
    fn an_unpooled_or_unlimited_primary_has_no_lane_count() {
        let unpooled = table_with(vec![model_row(30, None)], vec![], vec![30]);
        assert_eq!(lane_worker_count(&unpooled, "dialog", 2, 24), None);
        let unlimited = table_with(
            vec![model_row(10, Some(1))],
            vec![PoolSpec {
                id: 1,
                max_concurrency: None,
            }],
            vec![10],
        );
        assert_eq!(lane_worker_count(&unlimited, "dialog", 2, 24), None);
    }
}
