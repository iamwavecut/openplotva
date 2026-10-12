# Backend Lanes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Load every remote inference backend at its physical width: requests wait in their backend's priority queue, fall back only on failure or budget, and worker counts follow the primary backend's lanes.

**Architecture:** `PoolRegistry` (openplotva-llm router) becomes a set of lane pools with per-pool priority queues that hand a released lane directly to the next waiter. `RoutedAttemptWalker` admits every candidate through its lane queue with a deadline-derived budget and a hold-time forecast instead of skipping busy candidates. Workflows carry `lane_priority` and `fallback_reserve_ms` from the database; worker counts derive from the primary lane pool; a loader check compares lane widths with Discovery.

**Tech Stack:** Rust 1.99 (edition 2024), tokio (`oneshot`, `time`), sqlx/Postgres migrations, async-graphql runtime API.

**Spec:** `docs/superpowers/specs/2026-10-12-backend-lanes-design.md`

## Global Constraints

- Lane pool names: `aifarm-gpu0-dialog`, `aifarm-gpu2-utility`, `aifarm-gpu1-media`, `aifarm-embedder`, `aifarm-gpu1-privacy`.
- Widths: GPU0 dialog 8, GPU2 utility 8, embedder 8, GPU1 media 1 (draw, music, speech recognition), GPU1 privacy 4; external APIs stay unpooled.
- Priorities: 0 interactive, 1 user tool, 2 background; lower is served first; FIFO within a priority.
- Fallback reserve: 30,000 ms for interactive and user-tool workflows, 0 for background workflows; the last candidate in a chain waits until the deadline.
- Workers: `PERSISTENT_QUEUE_DIALOG_WORKERS_PER_LANE` default 2, `PERSISTENT_QUEUE_MEMORY_WORKERS_PER_LANE` default 2, memory worker cap default 32.
- Lane config mismatches never fail readiness; they emit `lane_config_mismatch` routing events.
- Migrations are numbered up/down pairs; data changes record previous values for the down migration.
- Follow AGENTS.md: `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, relevant `cargo test`, ready PR, review loop, deploy only on request.

## Review Focus

1. A waiter cancelled after a lane was handed to it must not leak the lane (the permit inside the channel releases it). Test in Task 1.
2. Shrinking a pool below its in-flight count admits nothing new until work drains; growing it serves queued waiters at once. Test in Task 1.
3. A request whose deadline already passed is not queued at all and fails with the capacity-timeout error immediately. Test in Task 3.
4. After a restart, background memory waiters must not take GPU2 lanes ahead of a dialog fallback that arrives later. Test in Task 1 (priority order).
5. A candidate with an open circuit breaker is tried only when its lane is free and is never waited for. Test in Task 3.

---

### Task 1: Lane queue in `PoolRegistry`

**Files:**
- Modify: `crates/openplotva-llm/src/router/capacity.rs` (whole file except `derived_worker_count`)
- Modify: `crates/openplotva-llm/src/router/mod.rs:16` (exports)

**Interfaces:**
- Consumes: `PoolId`, `RoutingTable` from `router::table`.
- Produces:
  - `pub enum LanePriority { Interactive = 0, UserTool = 1, Background = 2 }` with `from_db(i16) -> Self`, `as_str(self) -> &'static str`, `Default` = `Interactive`, `Ord` (lower first).
  - `PoolRegistry::acquire(&self, pool: Option<PoolId>, priority: LanePriority, budget: Option<Duration>) -> Option<PoolPermit>` (async; `None` when the budget ends).
  - `PoolRegistry::try_acquire(&self, pool: Option<PoolId>) -> Option<PoolPermit>` (non-blocking; fails while waiters exist).
  - `PoolRegistry::has_free_lane(&self, pool: Option<PoolId>) -> bool`.
  - `PoolRegistry::forecast_wait(&self, pool: Option<PoolId>, priority: LanePriority) -> Option<Duration>`.
  - `PoolOccupancy { id, max_concurrency, in_flight, queued: [u32; 3], mean_hold_ms: Option<u64> }`.
  - `wait_for_release` is removed.

- [ ] **Step 1: Write the failing tests** (replace the pool tests in `capacity.rs`'s `mod tests` that call `wait_for_release`; keep the `derived_worker_count` tests untouched)

```rust
    fn registry(max: u32) -> PoolRegistry {
        let registry = PoolRegistry::new();
        registry.apply(&[PoolSpec { id: 1, max_concurrency: Some(max) }]);
        registry
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
            tokio::spawn(async move { pools.acquire(Some(1), LanePriority::Background, None).await })
        };
        tokio::task::yield_now().await;
        drop(held);
        let handed = waiter.await.expect("task").expect("lane handed to the waiter");
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
            tokio::spawn(async move { pools.acquire(Some(1), LanePriority::Interactive, None).await })
        };
        tokio::task::yield_now().await;
        drop(held);
        waiter.abort();
        let _ = waiter.await;
        tokio::task::yield_now().await;
        assert!(pools.try_acquire(Some(1)).is_some(), "the handed lane came back");
        assert_eq!(pools.occupancy()[0].queued, [0, 0, 0]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_budget_ends_the_wait_and_leaves_the_queue() {
        let pools = registry(1);
        let _held = pools.try_acquire(Some(1)).expect("free lane");
        let waited = pools
            .acquire(Some(1), LanePriority::Interactive, Some(Duration::from_secs(2)))
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
            tokio::spawn(async move { pools.acquire(Some(1), LanePriority::Interactive, None).await })
        };
        tokio::task::yield_now().await;
        pools.apply(&[PoolSpec { id: 1, max_concurrency: Some(2) }]);
        let second = waiter.await.expect("task").expect("served by the grown pool");
        pools.apply(&[PoolSpec { id: 1, max_concurrency: Some(1) }]);
        drop(held);
        assert!(pools.try_acquire(Some(1)).is_none(), "still at the new width");
        drop(second);
        assert!(pools.try_acquire(Some(1)).is_some());
    }

    #[tokio::test]
    async fn forecast_counts_waiters_ahead_and_mean_hold_time() {
        let pools = Arc::new(registry(2));
        assert_eq!(pools.forecast_wait(Some(1), LanePriority::Interactive), Some(Duration::ZERO));
        let first = pools.try_acquire(Some(1)).expect("lane");
        let second = pools.try_acquire(Some(1)).expect("lane");
        assert_eq!(pools.forecast_wait(Some(1), LanePriority::Interactive), None, "no history yet");
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(first);
        let first = pools.try_acquire(Some(1)).expect("lane");
        let forecast = pools.forecast_wait(Some(1), LanePriority::Interactive).expect("history");
        assert!(forecast >= Duration::from_millis(15));
        drop((first, second));
        assert_eq!(pools.forecast_wait(None, LanePriority::Interactive), None);
    }

    #[test]
    fn priorities_parse_from_the_database() {
        assert_eq!(LanePriority::from_db(0), LanePriority::Interactive);
        assert_eq!(LanePriority::from_db(1), LanePriority::UserTool);
        assert_eq!(LanePriority::from_db(2), LanePriority::Background);
        assert_eq!(LanePriority::from_db(7), LanePriority::Background);
        assert!(LanePriority::Interactive < LanePriority::Background);
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p openplotva-llm --lib router::capacity`
Expected: compile errors (`LanePriority`, `acquire`, `forecast_wait`, `queued` not defined).

- [ ] **Step 3: Implement the lane queue** (replace everything above `derived_worker_count` in `capacity.rs`)

```rust
//! Lane pools: the width of one physical inference resource and its queue.
//!
//! A pool admits up to `max_concurrency` requests. Further requests wait in a
//! priority queue (lower priority value first, arrival order within a
//! priority), and a released lane is handed directly to the next waiter as a
//! [`PoolPermit`] sent through a oneshot channel. A waiter that gives up drops
//! its receiver; a permit already inside the channel then releases the lane on
//! drop, so no lane can leak. The registry lives outside the
//! [`super::table::RoutingTable`] so in-flight accounting survives a reload:
//! [`PoolRegistry::apply`] resizes cells in place.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use super::table::{PoolId, RoutingTable};

/// Order in which waiters of one lane pool are served: lower first.
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

#[derive(Debug)]
struct CellState {
    max: Option<u32>,
    in_flight: u32,
    waiters: BTreeMap<(LanePriority, u64), oneshot::Sender<PoolPermit>>,
    next_seq: u64,
    mean_hold_ms: Option<f64>,
}

impl CellState {
    fn has_free_lane(&self) -> bool {
        self.max.is_none_or(|max| self.in_flight < max)
    }

    fn record_hold(&mut self, held: Duration) {
        let sample = held.as_secs_f64() * 1000.0;
        self.mean_hold_ms = Some(match self.mean_hold_ms {
            Some(mean) => mean + HOLD_MEAN_WEIGHT * (sample - mean),
            None => sample,
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

/// RAII lane of a pool. Dropping it frees the lane for the next waiter, so
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

/// Hand free lanes to waiters in queue order. A waiter that already gave up
/// returns the permit; it is disarmed and the lane goes to the next waiter.
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
    /// pools, update `max` in place (in-flight accounting preserved), drop cells
    /// of removed pools (outstanding permits keep their cell alive). A grown
    /// pool serves its waiters at once.
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
                            state: Mutex::new(CellState {
                                max: spec.max_concurrency,
                                in_flight: 0,
                                waiters: BTreeMap::new(),
                                next_seq: 0,
                                mean_hold_ms: None,
                            }),
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
        let mut state = cell.lock();
        if !state.waiters.is_empty() || !state.has_free_lane() {
            return None;
        }
        state.in_flight = state.in_flight.saturating_add(1);
        drop(state);
        Some(PoolPermit::held(cell))
    }

    /// Whether `try_acquire` would succeed right now.
    #[must_use]
    pub fn has_free_lane(&self, pool: Option<PoolId>) -> bool {
        self.cell(pool).is_none_or(|cell| {
            let state = cell.lock();
            state.waiters.is_empty() && state.has_free_lane()
        })
    }

    /// Wait in the pool's queue for a lane. `None` once `budget` ends; the
    /// waiter then leaves the queue. `budget == None` waits without a limit.
    pub async fn acquire(
        &self,
        pool: Option<PoolId>,
        priority: LanePriority,
        budget: Option<Duration>,
    ) -> Option<PoolPermit> {
        let Some(cell) = self.cell(pool) else {
            return Some(PoolPermit::unpooled());
        };
        let (key, receiver) = {
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
            (key, receiver)
        };
        let handed = match budget {
            Some(budget) => match tokio::time::timeout(budget, receiver).await {
                Ok(handed) => handed.ok(),
                Err(_) => None,
            },
            None => receiver.await.ok(),
        };
        if handed.is_none() {
            cell.lock().waiters.remove(&key);
        }
        handed
    }

    /// Expected wait for a new request of `priority`: the waiters it would
    /// queue behind, in rounds of the pool width, times the mean lane hold
    /// time. `Some(ZERO)` when a lane is free; `None` for unpooled or unlimited
    /// pools and before the first lane was released.
    #[must_use]
    pub fn forecast_wait(&self, pool: Option<PoolId>, priority: LanePriority) -> Option<Duration> {
        let cell = self.cell(pool)?;
        let mut state = cell.lock();
        let width = state.max?;
        state.waiters.retain(|_, waiter| !waiter.is_closed());
        if state.waiters.is_empty() && state.has_free_lane() {
            return Some(Duration::ZERO);
        }
        let mean_hold_ms = state.mean_hold_ms?;
        let ahead = state
            .waiters
            .keys()
            .filter(|(waiting, _)| *waiting <= priority)
            .count() as u64;
        let rounds = (ahead + 1).div_ceil(u64::from(width.max(1)));
        Some(Duration::from_secs_f64(mean_hold_ms * rounds as f64 / 1000.0))
    }

    /// Current state of every registered pool, sorted by id.
    #[must_use]
    pub fn occupancy(&self) -> Vec<PoolOccupancy> {
        let cells = self.lock();
        let mut out: Vec<PoolOccupancy> = cells
            .iter()
            .map(|(id, cell)| {
                let mut state = cell.lock();
                state.waiters.retain(|_, waiter| !waiter.is_closed());
                let mut queued = [0_u32; 3];
                for (priority, _) in state.waiters.keys() {
                    queued[*priority as usize] = queued[*priority as usize].saturating_add(1);
                }
                PoolOccupancy {
                    id: *id,
                    max_concurrency: state.max,
                    in_flight: state.in_flight,
                    queued,
                    mean_hold_ms: state.mean_hold_ms.map(|mean| mean.round() as u64),
                }
            })
            .collect();
        drop(cells);
        out.sort_by_key(|occupancy| occupancy.id);
        out
    }
}
```

Update the export in `crates/openplotva-llm/src/router/mod.rs:16`:

```rust
pub use capacity::{
    LanePriority, PoolOccupancy, PoolPermit, PoolRegistry, PoolSpec, derived_worker_count,
};
```

Add `"sync"` is already in the crate's tokio features (`crates/openplotva-llm/Cargo.toml:27`), so `oneshot` is available.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p openplotva-llm --lib router::capacity`
Expected: PASS. The crate's other code that called `wait_for_release` does not exist inside `openplotva-llm`; the app crate is fixed in Task 3.

- [ ] **Step 5: Commit**

```bash
git add crates/openplotva-llm/src/router/capacity.rs crates/openplotva-llm/src/router/mod.rs
git commit -m "Give lane pools a priority queue that hands released lanes to the next waiter"
```

---

### Task 2: Workflow lane settings in the database and the routing table

**Files:**
- Create: `migrations/199_workflow_lane_settings.up.sql`, `migrations/199_workflow_lane_settings.down.sql`
- Modify: `crates/openplotva-storage/src/llm_routing.rs` (`WorkflowRecord`, `SQL_LIST_WORKFLOWS`, `workflow_from_row`, two test literals)
- Modify: `crates/openplotva-llm/src/router/table.rs` (`WorkflowRoute`)
- Modify: `crates/openplotva-app/src/model_routing.rs` (`build_route`, memory workflow seeding inserts, 7 `WorkflowRecord` test literals)
- Modify: `crates/openplotva-app/src/routed_attempts.rs` (`RoutedRequestContext`, 2 `WorkflowRecord` test literals)
- Modify: `WorkflowRecord`/`WorkflowRoute` literals in `crates/openplotva-app/src/dialog_runtime.rs`, `crates/openplotva-app/src/image_jobs.rs`, `crates/openplotva-llm/src/router/{capacity.rs,mod.rs,table.rs}`
- Test: `crates/openplotva-storage/src/lib.rs` (migration contract test), `crates/openplotva-app/src/model_routing.rs` (route mapping test)

**Interfaces:**
- Consumes: `LanePriority` (Task 1).
- Produces: `WorkflowRecord { .., lane_priority: i16, fallback_reserve_ms: i32 }`; `WorkflowRoute { .., lane_priority: LanePriority, fallback_reserve: Duration }`; `RoutedRequestContext { .., lane_priority: Option<LanePriority> }`.

- [ ] **Step 1: Write the migration contract test** in `crates/openplotva-storage/src/lib.rs` next to `vision_fallback_joins_the_gpu2_pool_and_is_reversible`

```rust
    #[test]
    fn workflow_lane_settings_default_and_are_reversible() {
        const UP: &str = include_str!("../../../migrations/199_workflow_lane_settings.up.sql");
        const DOWN: &str = include_str!("../../../migrations/199_workflow_lane_settings.down.sql");

        for value in [
            "ADD COLUMN lane_priority SMALLINT NOT NULL DEFAULT 0",
            "ADD COLUMN fallback_reserve_ms INTEGER NOT NULL DEFAULT 30000",
            "SET lane_priority = 1",
            "'history_summary', 'youtube_summary', 'media_prompt_optimizer'",
            "'image_generation', 'image_generation_flux', 'image_generation_boogu_turbo'",
            "'image_edit', 'image_edit_flux', 'image_edit_boogu_turbo', 'music'",
            "SET lane_priority = 2, fallback_reserve_ms = 0",
            "WHERE key IN ('memory_extraction', 'memory_subject_merge')",
        ] {
            assert!(UP.contains(value), "missing lane settings contract {value}");
        }
        assert!(DOWN.contains("DROP COLUMN lane_priority"));
        assert!(DOWN.contains("DROP COLUMN fallback_reserve_ms"));
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p openplotva-storage --lib workflow_lane_settings`
Expected: FAIL (`include_str!` cannot find the files).

- [ ] **Step 3: Write the migrations**

`migrations/199_workflow_lane_settings.up.sql`:

```sql
-- Each workflow's place in its lane queues and the time it keeps for a fallback.
-- Priority 0 is interactive, 1 a user-facing tool, 2 background work; lower is
-- served first. Background work waits for its lane without falling back.

ALTER TABLE workflows
    ADD COLUMN lane_priority SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN fallback_reserve_ms INTEGER NOT NULL DEFAULT 30000;

UPDATE workflows SET lane_priority = 1
WHERE key IN ('history_summary', 'youtube_summary', 'media_prompt_optimizer',
              'image_generation', 'image_generation_flux', 'image_generation_boogu_turbo',
              'image_edit', 'image_edit_flux', 'image_edit_boogu_turbo', 'music');

UPDATE workflows SET lane_priority = 2, fallback_reserve_ms = 0
WHERE key IN ('memory_extraction', 'memory_subject_merge');
```

`migrations/199_workflow_lane_settings.down.sql`:

```sql
ALTER TABLE workflows
    DROP COLUMN lane_priority,
    DROP COLUMN fallback_reserve_ms;
```

- [ ] **Step 4: Run the contract test to verify it passes**

Run: `cargo test -p openplotva-storage --lib workflow_lane_settings`
Expected: PASS.

- [ ] **Step 5: Carry the columns through storage**

In `crates/openplotva-storage/src/llm_routing.rs`:

```rust
#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowRecord {
    pub key: String,
    pub kind: String,
    pub full_routing: bool,
    pub retry_max_hops: i32,
    pub retry_wall_ms: i32,
    pub enabled: bool,
    pub lane_priority: i16,
    pub fallback_reserve_ms: i32,
}
```

```rust
const SQL_LIST_WORKFLOWS: &str = "SELECT key, kind, full_routing, retry_max_hops, retry_wall_ms, enabled, lane_priority, fallback_reserve_ms FROM workflows ORDER BY key ASC";
```

```rust
fn workflow_from_row(row: PgRow) -> Result<WorkflowRecord, StorageError> {
    Ok(WorkflowRecord {
        key: row.try_get("key")?,
        kind: row.try_get("kind")?,
        full_routing: row.try_get("full_routing")?,
        retry_max_hops: row.try_get("retry_max_hops")?,
        retry_wall_ms: row.try_get("retry_wall_ms")?,
        enabled: row.try_get("enabled")?,
        lane_priority: row.try_get("lane_priority")?,
        fallback_reserve_ms: row.try_get("fallback_reserve_ms")?,
    })
}
```

Every other `WorkflowRecord { .. }` literal in the workspace gets `lane_priority: 0, fallback_reserve_ms: 30_000,` (13 literals; find them with `rg -n "WorkflowRecord \{" crates`).

- [ ] **Step 6: Carry the settings into `WorkflowRoute` and the request context**

In `crates/openplotva-llm/src/router/table.rs`, add to `WorkflowRoute` (after `retry`):

```rust
    /// Place of this workflow's requests in their lane queues.
    pub lane_priority: super::capacity::LanePriority,
    /// Time kept for the next candidate when a lane wait has a deadline.
    pub fallback_reserve: std::time::Duration,
```

Every `WorkflowRoute { .. }` literal (5; `rg -n "WorkflowRoute \{" crates`) gets
`lane_priority: LanePriority::Interactive, fallback_reserve: Duration::from_secs(30),` with the imports the file needs.

In `crates/openplotva-app/src/model_routing.rs` `build_route`, extend the returned route:

```rust
        retry: RetryBudget {
            max_hops: workflow.retry_max_hops.max(1) as u32,
            wall_clock: Duration::from_millis(workflow.retry_wall_ms.max(0) as u64),
        },
        lane_priority: openplotva_llm::router::LanePriority::from_db(workflow.lane_priority),
        fallback_reserve: Duration::from_millis(workflow.fallback_reserve_ms.max(0) as u64),
```

In the memory cascade seeding (`model_routing.rs`, the two `INSERT INTO workflows` statements for memory workflows), insert the background settings so fresh installs match the migration:

```rust
            "INSERT INTO workflows (key, kind, full_routing, retry_max_hops, retry_wall_ms, enabled, lane_priority, fallback_reserve_ms) \
             VALUES ($1, 'chat', FALSE, 2, 900000, TRUE, 2, 0) \
             ON CONFLICT (key) DO UPDATE SET kind = EXCLUDED.kind, \
             full_routing = EXCLUDED.full_routing, retry_max_hops = EXCLUDED.retry_max_hops, \
             retry_wall_ms = EXCLUDED.retry_wall_ms, enabled = EXCLUDED.enabled, \
             lane_priority = EXCLUDED.lane_priority, fallback_reserve_ms = EXCLUDED.fallback_reserve_ms",
```

(the extraction statement keeps its own `retry_max_hops`/`retry_wall_ms` values and gains the same two columns with `2, 0`).

In `crates/openplotva-app/src/routed_attempts.rs` `RoutedRequestContext`, add:

```rust
    /// Lane queue priority; `None` takes the workflow's `lane_priority`.
    pub lane_priority: Option<LanePriority>,
```

and import `LanePriority` from `openplotva_llm::router`.

- [ ] **Step 7: Write the route mapping test** in `crates/openplotva-app/src/model_routing.rs` tests

```rust
    #[test]
    fn workflow_lane_settings_reach_the_route() {
        let mut snapshot = minimal_dialog_snapshot();
        snapshot.workflows[0].lane_priority = 2;
        snapshot.workflows[0].fallback_reserve_ms = 0;
        let table = build_routing_table(&snapshot);
        let route = table.resolve("dialog", false).expect("route");
        assert_eq!(route.lane_priority, openplotva_llm::router::LanePriority::Background);
        assert_eq!(route.fallback_reserve, Duration::ZERO);
    }
```

If `minimal_dialog_snapshot` does not exist in that test module, build the snapshot inline from the module's existing helper that the route tests use (search the module for `fn snapshot`/`RoutingSnapshot {`), with one provider, one model, the `dialog` workflow and one primary assignment.

- [ ] **Step 8: Run the tests**

Run: `cargo test -p openplotva-storage --lib && cargo test -p openplotva-llm --lib && cargo test -p openplotva-app --lib model_routing`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add migrations/199_workflow_lane_settings.up.sql migrations/199_workflow_lane_settings.down.sql crates
git commit -m "Store each workflow's lane priority and fallback reserve"
```

---

### Task 3: Walker admits candidates through their lane queues

**Files:**
- Modify: `crates/openplotva-app/src/routed_attempts.rs` (`RoutedAttemptWalker`, `run`, tests)

**Interfaces:**
- Consumes: `PoolRegistry::{try_acquire, has_free_lane, acquire, forecast_wait}`, `WorkflowRoute::{lane_priority, fallback_reserve}`, `RoutedRequestContext::lane_priority`.
- Produces: routing events `lane_wait` (info: `waited_ms`, `acquired`, `priority`, `budget_ms`) and `lane_bypass` (info: `forecast_ms`, `budget_ms`, `priority`); `capacity_wait_timeout` stays the event and error when no candidate got a lane. Removes `with_primary_slot_wait`, `with_max_slot_wait`, `wait_for_primary_slot`, `DEFAULT_MAX_SLOT_WAIT`.

- [ ] **Step 1: Write the failing walker tests** (in `routed_attempts.rs` tests; they reuse `pooled_snapshot(max_hops)`: model 10 in pool 1 width 1 as the first primary, model 20 as the second candidate)

```rust
    fn fallback_snapshot() -> RoutingSnapshot {
        let mut snapshot = pooled_snapshot(3);
        snapshot.assignments[0].weight = Some(100);
        snapshot.assignments[1].role = "fallback".to_owned();
        snapshot.assignments[1].weight = None;
        snapshot.assignments[1].fallback_order = Some(1);
        snapshot.models[1].pool_id = None;
        snapshot
    }

    async fn run_recording(
        walker: &RoutedAttemptWalker,
        deadline: Option<std::time::Instant>,
    ) -> (Result<&'static str, RoutedAttemptRunError<std::io::Error>>, Vec<i64>) {
        let executed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let models = Arc::clone(&executed);
        let result = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    deadline,
                    ..RoutedRequestContext::default()
                },
                move |attempt| {
                    let models = Arc::clone(&models);
                    async move {
                        models.lock().expect("models").push(attempt.model_id);
                        Ok::<_, std::io::Error>("ok")
                    }
                },
                |_error| None,
            )
            .await;
        let executed = executed.lock().expect("models").clone();
        (result, executed)
    }

    #[tokio::test]
    async fn a_busy_primary_is_waited_for_instead_of_falling_back() {
        let snapshot = fallback_snapshot();
        let pools = Arc::new(PoolRegistry::new());
        let walker = walker_with_pools(&snapshot, Arc::clone(&pools));
        let held = pools.try_acquire(Some(1)).expect("occupy the primary lane");
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(held);
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(120);

        let (result, executed) = run_recording(&walker, Some(deadline)).await;

        release.await.expect("release");
        assert_eq!(result.expect("primary output"), "ok");
        assert_eq!(executed, vec![10]);
    }

    #[tokio::test]
    async fn a_forecast_beyond_the_budget_falls_back_without_waiting() {
        let snapshot = fallback_snapshot();
        let pools = Arc::new(PoolRegistry::new());
        let walker = walker_with_pools(&snapshot, Arc::clone(&pools));
        let warmup = pools.try_acquire(Some(1)).expect("lane");
        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(warmup);
        let _held = pools.try_acquire(Some(1)).expect("occupy the primary lane");
        // 30 s reserve leaves 100 ms of budget, below the 300 ms mean hold.
        let deadline = std::time::Instant::now() + Duration::from_millis(30_100);

        let (result, executed) = run_recording(&walker, Some(deadline)).await;

        assert_eq!(result.expect("fallback output"), "ok");
        assert_eq!(executed, vec![20]);
    }

    #[tokio::test]
    async fn an_expired_deadline_is_not_queued() {
        let mut snapshot = pooled_snapshot(3);
        snapshot.assignments.truncate(1);
        let pools = Arc::new(PoolRegistry::new());
        let walker = walker_with_pools(&snapshot, Arc::clone(&pools));
        let _held = pools.try_acquire(Some(1)).expect("occupy the only lane");
        let started = std::time::Instant::now();

        let (result, executed) = run_recording(&walker, Some(started)).await;

        assert!(executed.is_empty());
        assert!(started.elapsed() < Duration::from_millis(500));
        assert!(matches!(
            result,
            Err(RoutedAttemptRunError::Routing(error))
                if error.kind() == RoutedRoutingErrorKind::CapacityWaitTimeout
                    || error.kind() == RoutedRoutingErrorKind::DeadlineExceeded
        ));
    }

    #[tokio::test]
    async fn a_candidate_with_an_open_breaker_is_never_waited_for() {
        let snapshot = fallback_snapshot();
        let pools = Arc::new(PoolRegistry::new());
        let breakers = Arc::new(BreakerSet::new());
        let table = crate::model_routing::build_routing_table(&snapshot);
        pools.apply(&table.pool_specs());
        let breaker = table
            .resolve("dialog", false)
            .expect("route")
            .primary[0]
            .breaker;
        for _ in 0..breaker.failure_threshold.max(1) {
            breakers.record_failure(1, 10, breaker);
        }
        let walker = RoutedAttemptWalker::new(
            RouterHandle::new(table),
            Arc::clone(&breakers),
            Arc::new(TriggerState::new()),
            Arc::clone(&pools),
        );
        let _held = pools.try_acquire(Some(1)).expect("occupy the primary lane");

        let (result, executed) = run_recording(&walker, None).await;

        assert_eq!(result.expect("fallback output"), "ok");
        assert_eq!(executed, vec![20]);
    }

    #[tokio::test]
    async fn a_free_primary_sibling_is_taken_before_waiting() {
        let mut snapshot = pooled_snapshot(3);
        snapshot.assignments[0].weight = Some(100);
        snapshot.assignments[1].weight = Some(1);
        snapshot.models[1].pool_id = None;
        let pools = Arc::new(PoolRegistry::new());
        let walker = walker_with_pools(&snapshot, Arc::clone(&pools));
        let _held = pools.try_acquire(Some(1)).expect("occupy the heavy primary");

        let (result, executed) = run_recording(&walker, None).await;

        assert_eq!(result.expect("sibling output"), "ok");
        assert_eq!(executed, vec![20]);
    }
```

Use the field name of the breaker threshold that `BreakerConfig` actually has (`rg -n "pub struct BreakerConfig" -A8 crates/openplotva-llm/src/router/breaker.rs`). Delete the old tests that encode skipping a busy candidate: `busy_pool_skips_to_free_candidate_without_breaker_mutation`, `busy_skips_do_not_consume_the_hop_budget`, and any test that relies on `with_primary_slot_wait`, `with_max_slot_wait` or a 1 s capacity timeout; keep `failed_target_is_not_reselected_while_fallback_pool_is_busy` after adapting it to queue waits (it must still never re-select the failed target).

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p openplotva-app --lib routed_attempts`
Expected: FAIL (the busy primary is skipped, the forecast bypass does not exist).

- [ ] **Step 3: Replace admission in the walker**

Remove from `RoutedAttemptWalker`: the fields `max_slot_wait` and `primary_slot_wait`, their builder methods, `wait_for_primary_slot`, and `DEFAULT_MAX_SLOT_WAIT`. Add this method:

```rust
    /// Admit one candidate through its lane queue. A free lane is taken at once.
    /// A candidate with an open breaker is never waited for. Otherwise the
    /// request waits in the lane queue until `deadline - reserve`, where the
    /// reserve is kept only when another candidate follows; it skips the wait
    /// when the forecast already exceeds that budget.
    async fn admit_lane(
        &self,
        pool: Option<PoolId>,
        live: bool,
        has_next_candidate: bool,
        route: &WorkflowRoute,
        context: &RoutedRequestContext,
        attempt: &Attempt,
        total_slot_wait: &mut Duration,
    ) -> Option<PoolPermit> {
        if let Some(permit) = self.pools.try_acquire(pool) {
            return Some(permit);
        }
        if !live {
            return None;
        }
        let priority = context.lane_priority.unwrap_or(route.lane_priority);
        let reserve = if has_next_candidate {
            route.fallback_reserve
        } else {
            Duration::ZERO
        };
        let budget = context.deadline.map(|deadline| {
            deadline
                .saturating_duration_since(Instant::now())
                .saturating_sub(reserve)
        });
        if let Some(budget) = budget {
            let forecast = self.pools.forecast_wait(pool, priority);
            if budget.is_zero() || forecast.is_some_and(|forecast| forecast > budget) {
                self.record_event(routing_event_with_severity(
                    "lane_bypass",
                    "info",
                    context,
                    Some(attempt.provider),
                    Some(attempt.model),
                    "lane wait would exceed the request budget; trying the next candidate",
                    json!({
                        "priority": priority.as_str(),
                        "budget_ms": budget.as_millis(),
                        "forecast_ms": forecast.map(|forecast| forecast.as_millis()),
                    }),
                ));
                return None;
            }
        }
        let started = Instant::now();
        let permit = self.pools.acquire(pool, priority, budget).await;
        let waited = started.elapsed();
        *total_slot_wait += waited;
        self.record_event(routing_event_with_severity(
            "lane_wait",
            "info",
            context,
            Some(attempt.provider),
            Some(attempt.model),
            if permit.is_some() {
                "lane acquired after waiting in its queue"
            } else {
                "lane wait reached the request budget"
            },
            json!({
                "priority": priority.as_str(),
                "waited_ms": waited.as_millis(),
                "budget_ms": budget.map(|budget| budget.as_millis()),
                "acquired": permit.is_some(),
            }),
        ));
        permit
    }
```

In `run`:

1. After `let mut attempts = { ... }` and the `preferred_target` reordering, move candidates whose lane is free ahead of busy candidates of the same role, keeping the order otherwise:

```rust
            attempts.sort_by_key(|attempt| {
                let pool = table.model(attempt.model).and_then(|row| row.pool_id);
                (attempt.role != Role::Primary, !self.pools.has_free_lane(pool))
            });
```

   Apply this sort before the `preferred_target` block so a preferred target still goes first.
2. Iterate by index so the admission knows whether another candidate follows:

```rust
            let candidates = attempts;
            for (index, attempt) in candidates.iter().enumerate() {
                let attempt = attempt.clone();
                let has_next_candidate = candidates[index + 1..].iter().any(|next| {
                    !attempted_targets.contains(&(next.provider, next.model))
                });
                let live = self.breakers.is_live_at(attempt.provider, attempt.model, Instant::now());
```

3. Replace the permit block (`let mut permit = self.pools.try_acquire(pool_id); if permit.is_none() && attempt.role == Role::Primary ...`) with:

```rust
                    let Some(permit) = self
                        .admit_lane(
                            pool_id,
                            live,
                            has_next_candidate,
                            route,
                            &context,
                            &attempt,
                            &mut total_slot_wait,
                        )
                        .await
                    else {
                        if first_execution {
                            busy_skips += 1;
                        }
                        break;
                    };
```

4. Replace the block after the candidate loop (from `if busy_skips == 0 || started_attempts >= max_hops` to the end of the `'passes` loop body) with:

```rust
            if executed_this_pass == 0 && busy_skips > 0 && last_error.is_none() {
                self.record_event(routing_event(
                    "capacity_wait_timeout",
                    &context,
                    None,
                    None,
                    "workflow capacity unavailable: no candidate lane within the request budget",
                    json!({
                        "busy_candidates": busy_skips,
                        "waited_ms": total_slot_wait.as_millis(),
                    }),
                ));
                return Err(RoutedAttemptRunError::Routing(RoutedRoutingError::new(
                    RoutedRoutingErrorKind::CapacityWaitTimeout,
                    format!(
                        "workflow capacity unavailable: no candidate lane after {}ms",
                        total_slot_wait.as_millis()
                    ),
                )));
            }
            break;
```

   Remove the now-unused `wait_reported` variable. `quota_skips` stays (OpenRouter free gate).

- [ ] **Step 4: Run the walker tests**

Run: `cargo test -p openplotva-app --lib routed_attempts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/openplotva-app/src/routed_attempts.rs
git commit -m "Admit routed attempts through lane queues: fall back on failure or budget"
```

---

### Task 4: Remove the per-site wait knobs

**Files:**
- Modify: `crates/openplotva-app/src/dialog_runtime.rs` (`INTERACTIVE_CAPACITY_WAIT_TIMEOUT`, `RouterChatProvider::new`, `with_primary_slot_wait`, the router construction near line 516, the test near line 1040)
- Modify: `crates/openplotva-app/src/lib.rs` (three `.with_primary_slot_wait(..)` calls)
- Modify: `crates/openplotva-app/src/history_summary.rs`, `crates/openplotva-app/src/youtube.rs`, `crates/openplotva-app/src/media.rs` (slot-wait constants)
- Modify: `crates/openplotva-config/src/lib.rs` (`DEFAULT_DIALOG_PRIMARY_CAPACITY_WAIT_MS`, `aifarm_pool_primary_capacity_wait_ms`, its raw field and env read)

**Interfaces:**
- Consumes: walker without wait knobs (Task 3).
- Produces: nothing new; `DIALOG_AIFARM_POOL_PRIMARY_CAPACITY_WAIT_MS` is no longer read.

- [ ] **Step 1: Rewrite the dialog capacity-timeout test**

The test in `dialog_runtime.rs` that asserts `started.elapsed() == INTERACTIVE_CAPACITY_WAIT_TIMEOUT` becomes: with every dialog lane held and a request deadline 2 s ahead, the provider returns the capacity-timeout error after about 2 s and records `capacity_wait_timeout`:

```rust
        assert!(started.elapsed() >= Duration::from_secs(2));
        assert!(
            events
                .iter()
                .any(|event| event.event_type == "capacity_wait_timeout")
        );
```

(keep the rest of the test's setup; replace `INTERACTIVE_CAPACITY_WAIT_TIMEOUT` with the 2 s deadline the test passes in its context; use `#[tokio::test(start_paused = true)]` if the test is not paused already).

- [ ] **Step 2: Remove the knobs and constants**

Delete `INTERACTIVE_CAPACITY_WAIT_TIMEOUT` and the `.with_max_slot_wait(..)` call in `RouterChatProvider::new`, the `with_primary_slot_wait` method, and the `primary_slot_wait` read and call near line 516. Delete `.with_primary_slot_wait(..)` in `lib.rs` (history summary, YouTube, media optimizer) and the constants `HISTORY_SUMMARY_PRIMARY_SLOT_WAIT`, `YOUTUBE_PRIMARY_SLOT_WAIT`, `MEDIA_OPTIMIZER_PRIMARY_SLOT_WAIT`. In `openplotva-config`, delete `DEFAULT_DIALOG_PRIMARY_CAPACITY_WAIT_MS`, the `aifarm_pool_primary_capacity_wait_ms` field, the `dialog_aifarm_pool_primary_capacity_wait_ms` raw field, its parse and env read, and the config tests that set them.

- [ ] **Step 3: Build and test**

Run: `cargo test -p openplotva-config && cargo test -p openplotva-app --lib dialog_runtime`
Expected: PASS; `cargo build -p openplotva-app` has no unused-import warnings.

- [ ] **Step 4: Commit**

```bash
git add crates
git commit -m "Drop per-call slot waits now that lanes queue requests"
```

---

### Task 5: Callers pass background priority for memory work

**Files:**
- Modify: `crates/openplotva-app/src/embedder.rs` (`RoutedDiscoveryEmbedder`, `embedding_context`)
- Modify: `crates/openplotva-app/src/memory_runtime.rs` (`RoutedDiscoveryRedactor::redact_text` context)
- Modify: `crates/openplotva-app/src/lib.rs` (memory write embedder construction)

**Interfaces:**
- Consumes: `RoutedRequestContext::lane_priority`, `LanePriority`.
- Produces: `RoutedDiscoveryEmbedder::with_lane_priority(self, LanePriority) -> Self`.

- [ ] **Step 1: Write the failing test** in `embedder.rs` tests

```rust
    #[test]
    fn memory_writes_embed_at_background_priority() {
        assert_eq!(embedding_context(None).lane_priority, None);
        assert_eq!(
            embedding_context(Some(LanePriority::Background)).lane_priority,
            Some(LanePriority::Background)
        );
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p openplotva-app --lib memory_writes_embed_at_background_priority`
Expected: FAIL (`embedding_context` takes no argument).

- [ ] **Step 3: Implement**

```rust
pub struct RoutedDiscoveryEmbedder {
    walker: RoutedAttemptWalker,
    base_config: DiscoveryEmbedderConfig,
    lane_priority: Option<LanePriority>,
}
```

`new` sets `lane_priority: None`; add:

```rust
    /// Queue this embedder's requests at `priority` instead of the workflow default.
    #[must_use]
    pub fn with_lane_priority(mut self, priority: LanePriority) -> Self {
        self.lane_priority = Some(priority);
        self
    }
```

```rust
fn embedding_context(lane_priority: Option<LanePriority>) -> RoutedRequestContext {
    RoutedRequestContext {
        workflow_key: "embedding".to_owned(),
        lane_priority,
        ..RoutedRequestContext::default()
    }
}
```

and pass `self.lane_priority` at both call sites. In `lib.rs`, the memory write embedder (`memory_write_embedder`) gets `.with_lane_priority(openplotva_llm::router::LanePriority::Background)`. In `memory_runtime.rs`, `RoutedDiscoveryRedactor::redact_text`'s context gets `lane_priority: Some(LanePriority::Background),`.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p openplotva-app --lib embedder && cargo test -p openplotva-app --lib memory_runtime`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/openplotva-app/src/embedder.rs crates/openplotva-app/src/memory_runtime.rs crates/openplotva-app/src/lib.rs
git commit -m "Queue memory embeddings and redactions at background priority"
```

---

### Task 6: Lane pools named by device and purpose

**Files:**
- Create: `migrations/200_backend_lane_pools.up.sql`, `migrations/200_backend_lane_pools.down.sql`
- Modify: `crates/openplotva-app/src/model_routing.rs:1816` (`MEMORY_GPU2_POOL`) and its tests that name the pool
- Test: `crates/openplotva-storage/src/lib.rs` (contract test)

**Interfaces:**
- Produces: pool rows `aifarm-gpu0-dialog`, `aifarm-gpu2-utility`, `aifarm-gpu1-media` (renamed, same ids), `aifarm-embedder` (8), `aifarm-gpu1-privacy` (4); the ASR, embedder and privacy filter model rows attached.

- [ ] **Step 1: Write the contract test**

```rust
    #[test]
    fn backend_lane_pools_are_renamed_attached_and_reversible() {
        const UP: &str = include_str!("../../../migrations/200_backend_lane_pools.up.sql");
        const DOWN: &str = include_str!("../../../migrations/200_backend_lane_pools.down.sql");

        for value in [
            "('aifarm-vllm-gpu0', 'aifarm-gpu0-dialog'",
            "('aifarm-gpu2-qwen27b', 'aifarm-gpu2-utility'",
            "('aifarm-draw-gpu1', 'aifarm-gpu1-media'",
            "('aifarm-embedder', 8,",
            "('aifarm-gpu1-privacy', 4,",
            "('aifarm-asr', 'aifarm-gpu1-media')",
            "('aifarm-embed', 'aifarm-embedder')",
            "('privacy-filter', 'aifarm-gpu1-privacy')",
            "backend_lanes_v1_previous_name",
            "backend_lanes_v1_previous_pool_id",
            "created_by_backend_lanes_v1",
        ] {
            assert!(UP.contains(value), "missing lane pool contract {value}");
        }
        assert!(DOWN.contains("WHERE config ? 'backend_lanes_v1_previous_pool_id'"));
        assert!(DOWN.contains("DELETE FROM llm_capacity_pools WHERE config ? 'created_by_backend_lanes_v1'"));
        assert!(DOWN.contains("WHERE config ? 'backend_lanes_v1_previous_name'"));
    }
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p openplotva-storage --lib backend_lane_pools`
Expected: FAIL (files missing).

- [ ] **Step 3: Write the migrations**

`migrations/200_backend_lane_pools.up.sql`:

```sql
-- Lane pools describe physical resources and are named aifarm-<device>-<purpose>.
-- GPU1's draw, music and speech recognition share one sequential lane under the
-- VRAM arbiter; its privacy filter runs beside that lane. Renamed pools keep their
-- ids, so model rows keep their pool_id. Every change records what it replaced.

LOCK TABLE llm_capacity_pools, llm_providers, provider_models IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM llm_capacity_pools
        WHERE config ?| ARRAY['backend_lanes_v1_previous_name', 'created_by_backend_lanes_v1']
    ) OR EXISTS (
        SELECT 1 FROM provider_models WHERE config ? 'backend_lanes_v1_previous_pool_id'
    ) THEN
        RAISE EXCEPTION 'backend lanes marker collision';
    END IF;
END $$;

UPDATE llm_capacity_pools AS pool
SET config = pool.config || jsonb_build_object(
        'backend_lanes_v1_previous_name', pool.name,
        'backend_lanes_v1_previous_description', pool.description
    ),
    name = renamed.new_name,
    description = renamed.description,
    updated_at = now()
FROM (
    VALUES
        ('aifarm-vllm-gpu0', 'aifarm-gpu0-dialog', 'GPU0 dialog model: eight lanes.'),
        ('aifarm-gpu2-qwen27b', 'aifarm-gpu2-utility', 'GPU2 utility model: eight lanes.'),
        ('aifarm-draw-gpu1', 'aifarm-gpu1-media', 'GPU1 draw, music and speech recognition: one lane under the VRAM arbiter.')
) AS renamed (old_name, new_name, description)
WHERE pool.name = renamed.old_name;

INSERT INTO llm_capacity_pools (name, max_concurrency, description, config)
SELECT new_pool.name, new_pool.width, new_pool.description,
       '{"created_by_backend_lanes_v1": true}'::jsonb
FROM (
    VALUES
        ('aifarm-embedder', 8, 'Embedder: eight lanes.'),
        ('aifarm-gpu1-privacy', 4, 'GPU1 privacy filter: four lanes beside the media lane.')
) AS new_pool (name, width, description)
WHERE NOT EXISTS (SELECT 1 FROM llm_capacity_pools WHERE name = new_pool.name);

UPDATE provider_models AS model
SET config = model.config || jsonb_build_object(
        'backend_lanes_v1_previous_pool_id', model.pool_id
    ),
    pool_id = target_pool.id
FROM llm_providers AS provider,
     (
         VALUES
             ('aifarm-asr', 'aifarm-gpu1-media'),
             ('aifarm-embed', 'aifarm-embedder'),
             ('privacy-filter', 'aifarm-gpu1-privacy')
     ) AS mapping (provider_name, pool_name),
     llm_capacity_pools AS target_pool
WHERE provider.id = model.provider_id
  AND provider.name = mapping.provider_name
  AND target_pool.name = mapping.pool_name
  AND model.pool_id IS DISTINCT FROM target_pool.id;
```

`migrations/200_backend_lane_pools.down.sql`:

```sql
LOCK TABLE llm_capacity_pools, provider_models IN SHARE ROW EXCLUSIVE MODE;

UPDATE provider_models
SET pool_id = (config->>'backend_lanes_v1_previous_pool_id')::BIGINT,
    config = config - 'backend_lanes_v1_previous_pool_id'
WHERE config ? 'backend_lanes_v1_previous_pool_id';

DELETE FROM llm_capacity_pools WHERE config ? 'created_by_backend_lanes_v1';

UPDATE llm_capacity_pools
SET name = config->>'backend_lanes_v1_previous_name',
    description = config->>'backend_lanes_v1_previous_description',
    config = config - 'backend_lanes_v1_previous_name' - 'backend_lanes_v1_previous_description',
    updated_at = now()
WHERE config ? 'backend_lanes_v1_previous_name';
```

- [ ] **Step 4: Follow the rename in code**

`crates/openplotva-app/src/model_routing.rs:1816`:

```rust
const MEMORY_GPU2_POOL: &str = "aifarm-gpu2-utility";
```

Update the test SQL and assertions in `model_routing.rs` that name `aifarm-gpu2-qwen27b` as the memory pool (`rg -n "aifarm-gpu2-qwen27b" crates/openplotva-app/src/model_routing.rs`). Leave the historical migration contract tests in `openplotva-storage` untouched.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p openplotva-storage --lib backend_lane_pools && cargo test -p openplotva-app --lib model_routing`
Expected: PASS.

- [ ] **Step 6: Verify the migration on a copy of production routing data**

Run the up and down SQL against the production database inside one transaction that ends with `ROLLBACK`, as done for migrations 197 and 198 (copy both files into the Postgres container, `BEGIN; CREATE TEMP TABLE ... AS SELECT ... ; \i up; SELECT ...; \i down; SELECT count(*) of rows differing; ROLLBACK;`).
Expected: after up the five pool names exist with widths 8, 8, 1, 8, 4 and the ASR, embedder and privacy filter models point at their pools; after down 0 rows differ.

- [ ] **Step 7: Commit**

```bash
git add migrations/200_backend_lane_pools.up.sql migrations/200_backend_lane_pools.down.sql crates
git commit -m "Name lane pools by device and purpose and pool GPU1, embedder and privacy filter"
```

---

### Task 7: Workers per lane

**Files:**
- Modify: `crates/openplotva-llm/src/router/capacity.rs` (`derived_worker_count` → `lane_worker_count`, tests)
- Modify: `crates/openplotva-llm/src/router/mod.rs:16` (export)
- Modify: `crates/openplotva-config/src/lib.rs` (`dialog_unpooled_share` → `dialog_workers_per_lane`, add `memory_workers_per_lane`, memory cap default 32)
- Modify: `crates/openplotva-app/src/model_routing.rs` (`RouterRuntime` fields and `apply_published_table`)
- Modify: `crates/openplotva-app/src/lib.rs` (dialog and memory worker sizing near lines 12172 and 12971)

**Interfaces:**
- Produces: `pub fn lane_worker_count(table: &RoutingTable, workflow_key: &str, per_lane: u32, cap: u32) -> Option<u32>`; config fields `dialog_workers_per_lane: i32` (env `PERSISTENT_QUEUE_DIALOG_WORKERS_PER_LANE`, default 2) and `memory_workers_per_lane: i32` (env `PERSISTENT_QUEUE_MEMORY_WORKERS_PER_LANE`, default 2); `DEFAULT_PERSISTENT_QUEUE_MEMORY_WORKERS_CAP = 32`; `RouterRuntime { .., dialog_workers_per_lane: u32, dialog_worker_cap: u32 }`.

- [ ] **Step 1: Write the failing test** (replace the `derived_worker_count` tests in `capacity.rs`)

```rust
    #[test]
    fn workers_follow_the_primary_lane_pool_only() {
        let table = table_with_dialog_chain();
        assert_eq!(lane_worker_count(&table, "dialog", 2, 24), Some(16));
        assert_eq!(lane_worker_count(&table, "dialog", 2, 10), Some(10));
        assert_eq!(lane_worker_count(&table, "missing", 2, 24), None);
    }
```

where `table_with_dialog_chain()` builds (inside the test module) a `RoutingTable` with a dialog route whose primary model is in pool 1 (width 8) and whose fallback models are in pool 2 (width 8) and unpooled — the same shape the old `derived_worker_count` tests used; reuse and rename their setup helper.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p openplotva-llm --lib workers_follow_the_primary_lane_pool_only`
Expected: FAIL (`lane_worker_count` not defined).

- [ ] **Step 3: Implement**

```rust
/// Workers for a workflow: the width of its heaviest primary's lane pool times
/// `per_lane`, clamped to `1..=cap`. Fallback capacity is not throughput.
/// `None` when the workflow has no route or its primary has no bounded pool.
#[must_use]
pub fn lane_worker_count(
    table: &RoutingTable,
    workflow_key: &str,
    per_lane: u32,
    cap: u32,
) -> Option<u32> {
    let route = table.resolve(workflow_key, false)?;
    let primary = route.primary.iter().max_by_key(|candidate| candidate.weight)?;
    let pool = table.model(primary.model)?.pool_id?;
    let width = table.pool(pool)?.max_concurrency?;
    Some(width.saturating_mul(per_lane.max(1)).clamp(1, cap.max(1)))
}
```

Delete `derived_worker_count` and export `lane_worker_count` instead. In `openplotva-config`: replace `dialog_unpooled_share` (field, raw field, env read, default const) with `dialog_workers_per_lane` (`PERSISTENT_QUEUE_DIALOG_WORKERS_PER_LANE`, `DEFAULT_PERSISTENT_QUEUE_DIALOG_WORKERS_PER_LANE: i32 = 2`); add `memory_workers_per_lane` (`PERSISTENT_QUEUE_MEMORY_WORKERS_PER_LANE`, default 2); set `DEFAULT_PERSISTENT_QUEUE_MEMORY_WORKERS_CAP` to 32. In `RouterRuntime` rename `dialog_unpooled_share` to `dialog_workers_per_lane` and call `lane_worker_count(&table, "dialog", self.dialog_workers_per_lane, self.dialog_worker_cap)` in `apply_published_table`. In `lib.rs` use `lane_worker_count` for the initial dialog workers (with `dialog_workers_per_lane`) and for memory workers:

```rust
        let derived_memory_workers = openplotva_llm::router::lane_worker_count(
            &router_handle.snapshot(),
            memory_runtime::MEMORY_EXTRACTION_WORKFLOW_KEY,
            u32::try_from(config.persistent_queue.memory_workers_per_lane.max(1)).unwrap_or(1),
            u32::try_from(memory_workers_cap).unwrap_or(1),
        );
```

Update `effective_memory_consolidation_workers` tests if they assume the cap of 4.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p openplotva-llm --lib && cargo test -p openplotva-config && cargo test -p openplotva-app --lib`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates
git commit -m "Size dialog and memory workers from their primary lane pool"
```

---

### Task 8: Lane configuration check against Discovery

**Files:**
- Create: `crates/openplotva-app/src/lane_checks.rs`
- Modify: `crates/openplotva-llm/src/router/table.rs` (`RoutingTable::routed_models`, `RoutingTable::pool_ids`)
- Modify: `crates/openplotva-app/src/lib.rs` (module declaration, startup check, readiness entry)
- Modify: `crates/openplotva-app/src/model_routing.rs` (`reload_router_runtime` schedules the check)

**Interfaces:**
- Consumes: `RoutingTable`, the routing event reporter (`crate::runtime_routing::RoutingEventReporter`), Discovery `GET /v1/services/{name}/capacity` → `{"capacity": {"max_concurrent_jobs": N, ..}}`.
- Produces:
  - `pub struct LaneConfigIssue { pub kind: &'static str, pub pool_id: Option<i64>, pub detail: String }`
  - `pub fn lane_config_issues(table: &RoutingTable, discovery_widths: &HashMap<String, u32>) -> Vec<LaneConfigIssue>`
  - `pub async fn fetch_discovery_widths(http: &reqwest::Client, base_url: &str, services: &[String]) -> HashMap<String, u32>`
  - `pub async fn check_lane_config(http, base_url, table, reporter) -> Vec<LaneConfigIssue>`

- [ ] **Step 1: Write the failing tests** in `lane_checks.rs`

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn table(asr_pool: Option<i64>, gpu1_width: u32) -> RoutingTable {
        // providers: 1 draw-api (draw-api service), 2 asr (asr-api service), 3 openrouter (no service)
        // models: 10 draw in pool 3, 20 asr in `asr_pool`, 30 luna unpooled
        // routes: image_generation -> 10, voice_transcription -> 20, dialog -> 30
        // pools: 3 width `gpu1_width`
        crate::lane_checks::tests_support::routing_table(asr_pool, gpu1_width)
    }

    fn widths() -> HashMap<String, u32> {
        HashMap::from([("draw-api".to_owned(), 1), ("asr-api".to_owned(), 1)])
    }

    #[test]
    fn a_shared_sequential_lane_spanning_two_services_is_valid() {
        assert!(lane_config_issues(&table(Some(3), 1), &widths()).is_empty());
    }

    #[test]
    fn a_farm_model_outside_any_pool_is_reported() {
        let issues = lane_config_issues(&table(None, 1), &widths());
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].kind, "unpooled_farm_model");
    }

    #[test]
    fn a_pool_wider_than_its_service_is_reported() {
        let issues = lane_config_issues(&table(Some(3), 2), &widths());
        assert!(issues.iter().any(|issue| issue.kind == "width_mismatch"));
    }

    #[test]
    fn unknown_discovery_widths_are_not_reported() {
        assert!(lane_config_issues(&table(Some(3), 2), &HashMap::new()).is_empty());
    }
}
```

and a `#[cfg(test)] pub(crate) mod tests_support` in the same file that builds that table with `RoutingTable::new`, `set_pools`, and `set_route` (one `Candidate` per route, `Role::Primary`, weight 100, default `BreakerConfig`, `InferenceOverrides::default()`, `RetryBudget::default()`, `LanePriority::Interactive`, 30 s reserve).

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p openplotva-app --lib lane_checks`
Expected: FAIL (module missing).

- [ ] **Step 3: Implement**

In `table.rs` add:

```rust
    /// Models any route can reach: primaries, fallbacks, canaries, shadows and
    /// trigger targets of every workflow and scope.
    pub fn routed_models(&self) -> impl Iterator<Item = &ModelRow> {
        let mut ids: Vec<ModelId> = self
            .routes
            .values()
            .flat_map(|scoped| scoped.global.iter().chain(scoped.vip.iter()))
            .flat_map(|route| {
                route
                    .primary
                    .iter()
                    .chain(route.fallback_tail.iter())
                    .chain(route.canary.iter())
                    .chain(route.shadow.iter())
                    .chain(route.triggers.iter().map(|trigger| &trigger.engage))
                    .map(|candidate| candidate.model)
            })
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids.into_iter().filter_map(move |id| self.models.get(&id))
    }
```

`lane_checks.rs`:

```rust
//! Checks that lane pools match the farm: every routed farm model sits in a
//! pool, a Discovery service never splits across pools, and a pool is not wider
//! than the services it spans. Mismatches are reported, never enforced.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use openplotva_llm::router::RoutingTable;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LaneConfigIssue {
    pub kind: &'static str,
    pub pool_id: Option<i64>,
    pub detail: String,
}

#[must_use]
pub fn lane_config_issues(
    table: &RoutingTable,
    discovery_widths: &HashMap<String, u32>,
) -> Vec<LaneConfigIssue> {
    let mut issues = Vec::new();
    let mut pools_by_service: BTreeMap<String, BTreeSet<i64>> = BTreeMap::new();
    let mut services_by_pool: BTreeMap<i64, BTreeSet<String>> = BTreeMap::new();
    for model in table.routed_models().filter(|model| model.enabled) {
        let Some(service) = table
            .provider(model.provider)
            .filter(|provider| provider.enabled)
            .and_then(|provider| provider.discovery_service_name.clone())
            .filter(|service| !service.trim().is_empty())
        else {
            continue;
        };
        match model.pool_id {
            Some(pool) => {
                pools_by_service.entry(service.clone()).or_default().insert(pool);
                services_by_pool.entry(pool).or_default().insert(service);
            }
            None => issues.push(LaneConfigIssue {
                kind: "unpooled_farm_model",
                pool_id: None,
                detail: format!("model {} reaches {service} without a lane pool", model.model_name),
            }),
        }
    }
    for (service, pools) in &pools_by_service {
        if pools.len() > 1 {
            issues.push(LaneConfigIssue {
                kind: "split_service",
                pool_id: None,
                detail: format!("{service} is split across pools {pools:?}"),
            });
        }
    }
    for (pool, services) in &services_by_pool {
        let Some(width) = table.pool(*pool).and_then(|spec| spec.max_concurrency) else {
            continue;
        };
        let known: Vec<u32> = services
            .iter()
            .filter_map(|service| discovery_widths.get(service).copied())
            .collect();
        let Some(narrowest) = known.iter().copied().min() else {
            continue;
        };
        let mismatch = if services.len() == 1 { width != narrowest } else { width > narrowest };
        if mismatch {
            issues.push(LaneConfigIssue {
                kind: "width_mismatch",
                pool_id: Some(*pool),
                detail: format!("pool {pool} has {width} lanes; Discovery allows {narrowest} for {services:?}"),
            });
        }
    }
    issues
}

/// `max_concurrent_jobs` per Discovery service; services that do not answer are left out.
pub async fn fetch_discovery_widths(
    http: &reqwest::Client,
    base_url: &str,
    services: &[String],
) -> HashMap<String, u32> {
    let mut widths = HashMap::new();
    for service in services {
        let url = format!("{}/v1/services/{service}/capacity", base_url.trim_end_matches('/'));
        let Ok(response) = http.get(url).timeout(Duration::from_secs(5)).send().await else {
            continue;
        };
        let Ok(body) = response.json::<serde_json::Value>().await else {
            continue;
        };
        if let Some(width) = body["capacity"]["max_concurrent_jobs"]
            .as_u64()
            .and_then(|width| u32::try_from(width).ok())
        {
            widths.insert(service.clone(), width);
        }
    }
    widths
}
```

Plus `pub async fn check_lane_config(http: &reqwest::Client, base_url: &str, table: &RoutingTable, reporter: Option<&crate::runtime_routing::RoutingEventReporter>) -> Vec<LaneConfigIssue>` that collects the routed services from `table.routed_models()`, calls `fetch_discovery_widths`, runs `lane_config_issues`, records one `lane_config_mismatch` routing event (severity `warn`, workflow key `lanes`, `detail = {"kind": .., "pool_id": .., "detail": ..}`) per issue and logs `tracing::warn!` for each. Build the event with the same constructor the walker's events use (`crate::runtime_routing::RoutingEvent`, see `routing_event_with_severity` in `routed_attempts.rs`).

In `lib.rs`: declare `mod lane_checks;`; after the router table is loaded at startup, run `check_lane_config` once with a `reqwest::Client` and `config.llm.discovery.base_url`, and push `ReadinessCheck::ok("lane_config", ..)` whose detail is `"lane pools match Discovery"` or `"<n> lane config mismatches: <kinds>"` (never `error`, so readiness stays green). In `reload_router_runtime`, spawn the same check after `apply_published_table` when the runtime carries a Discovery base URL and HTTP client (add those two fields to `RouterRuntime`).

- [ ] **Step 4: Run the tests**

Run: `cargo test -p openplotva-app --lib lane_checks && cargo test -p openplotva-llm --lib`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates
git commit -m "Report lane pools that do not match the farm's Discovery limits"
```

---

### Task 9: Lane observability

**Files:**
- Modify: `crates/openplotva-app/src/lib.rs` (admin routing status JSON near line 8843; runtime API wiring near line 11266)
- Modify: `crates/openplotva-server/src/runtime_graphql.rs` (`RuntimeLaneInspector`, `RuntimeLaneData`, `llmLanes` query, diagnostics field)
- Test: `crates/openplotva-server/src/runtime_graphql.rs` tests

**Interfaces:**
- Consumes: `PoolRegistry::occupancy()` (Task 1), pool names from the routing table.
- Produces: GraphQL `llmLanes: [RuntimeLane!]!` with fields `id`, `name`, `maxConcurrency`, `inFlight`, `queuedInteractive`, `queuedUserTool`, `queuedBackground`, `meanHoldMs`.

- [ ] **Step 1: Write the failing GraphQL test** next to the existing runtime GraphQL tests that use stub inspectors

```rust
    struct LaneInspectorStub;

    impl RuntimeLaneInspector for LaneInspectorStub {
        fn lanes(&self) -> Vec<RuntimeLaneData> {
            vec![RuntimeLaneData {
                id: 1,
                name: "aifarm-gpu0-dialog".to_owned(),
                max_concurrency: Some(8),
                in_flight: 8,
                queued_interactive: 2,
                queued_user_tool: 0,
                queued_background: 0,
                mean_hold_ms: Some(3400),
            }]
        }
    }

    #[tokio::test]
    async fn llm_lanes_reports_occupancy_and_queues() {
        let schema = runtime_schema_with(|diagnostics| {
            diagnostics.lane_inspector = Some(Arc::new(LaneInspectorStub));
        });
        let response = schema
            .execute("{ llmLanes { name maxConcurrency inFlight queuedInteractive meanHoldMs } }")
            .await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        let data = response.data.into_json().expect("json");
        assert_eq!(data["llmLanes"][0]["name"], "aifarm-gpu0-dialog");
        assert_eq!(data["llmLanes"][0]["inFlight"], 8);
        assert_eq!(data["llmLanes"][0]["queuedInteractive"], 2);
    }
```

Use the test module's existing schema builder with diagnostics (search the module for how `routing_event_inspector` is set in tests and follow that helper; name it `runtime_schema_with` only if no equivalent exists).

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p openplotva-server --lib llm_lanes_reports_occupancy_and_queues`
Expected: FAIL (types missing).

- [ ] **Step 3: Implement**

```rust
/// Runtime API lane pool read boundary.
pub trait RuntimeLaneInspector: Send + Sync {
    fn lanes(&self) -> Vec<RuntimeLaneData>;
}

#[derive(Clone, Debug, async_graphql::SimpleObject)]
#[graphql(name = "RuntimeLane")]
pub struct RuntimeLaneData {
    pub id: i64,
    pub name: String,
    pub max_concurrency: Option<u32>,
    pub in_flight: u32,
    pub queued_interactive: u32,
    pub queued_user_tool: u32,
    pub queued_background: u32,
    pub mean_hold_ms: Option<u64>,
}
```

Add `pub lane_inspector: Option<Arc<dyn RuntimeLaneInspector>>` to the diagnostics struct and the query root (mirroring `routing_event_inspector`), and the resolver:

```rust
    async fn llm_lanes(&self) -> Vec<RuntimeLaneData> {
        self.lane_inspector
            .as_deref()
            .map(RuntimeLaneInspector::lanes)
            .unwrap_or_default()
    }
```

In `lib.rs`, implement the inspector over `Arc<PoolRegistry>` and `Arc<RouterHandle>` (name from the snapshot's pool rows; add `RoutingTable::pool_name(id) -> Option<&str>` by storing names next to specs in `set_pools`, or carry names in `PoolSpec` as `name: String` if simpler — keep `PoolSpec: Clone` and update its constructors). Extend the admin routing status JSON with `"queued": {"interactive": .., "user_tool": .., "background": ..}`, `"mean_hold_ms"` and `"name"` per pool.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p openplotva-server --lib && cargo test -p openplotva-app --lib`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates
git commit -m "Expose lane occupancy, queues and hold times"
```

---

### Task 10: Verification and delivery

**Files:** none new.

- [ ] **Step 1: Full local gate**

Run:
```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p openplotva-llm -p openplotva-storage -p openplotva-app -p openplotva-server -p openplotva-config --lib
```
Expected: all clean and green.

- [ ] **Step 2: Storage live tests on a fresh Postgres 17** (as CI runs them)

Run a throwaway `pgvector/pgvector:pg17` container, set `OPENPLOTVA_TEST_POSTGRES_DSN`, and run
`cargo test -p openplotva-storage --lib -- --test-threads=1 outbox_roundtrip_is_idempotent_ordered_and_fenced live_`.
Expected: all pass (migrations 199 and 200 apply through the migrator). Remove the container, its image and volume afterwards.

- [ ] **Step 3: Pull request and review loop**

Push `feat/backend-lanes`, open a ready PR with the summary, migrations, verification and deployment risk (dialog turns may wait in Gemma's queue instead of moving to GPU2; Luna traffic drops). Handle every review finding per AGENTS.md before merging.

- [ ] **Step 4: Deploy when the owner asks, then verify in production**

After `deploy-production.yml` succeeds:
- `_sqlx_migrations` has 199 and 200; `llm_capacity_pools` has the five lane names; the ASR, embedder and privacy filter models point at their pools.
- The startup log shows dialog workers 16 and memory workers 16; readiness detail for `lane_config` says the pools match Discovery.
- Over the first hour: dialog requests served by GPU2 and OpenRouter are only failures and budget bypasses (`lane_bypass`), `lane_wait` events show queueing on GPU0, `llmLanes` shows queues per priority.

---

## Implementation Notes

Where the delivered code differs from the task text above:

- Task 1: a `QueuedWaiter` guard removes a waiter from the queue on any exit (budget end or cancellation), so no closed senders linger. The pools also keep the last 128 admission waits for p50/p95 (`PoolOccupancy::wait_p50_ms`, `wait_p95_ms`).
- Task 3: the previous step's preferred target is moved to the front only while its lane is free. Free-lane reordering touches only live primary and overflow candidates. A request that ran and failed re-selects once more, so a candidate passed over for its budget gets the time that is left. A failed attempt is reported as such, not as a capacity timeout.
- Task 7: `RouterRuntime` holds two `LaneWorkerScale`s (dialog and memory). Memory workers now run under the same supervisor as dialog workers (`dialog_workers::spawn_worker_supervisor`), so both fleets resize on a routing reload. `PERSISTENT_QUEUE_MEMORY_CONSOLIDATION_WORKERS` is the floor, and `0` disables memory workers.
- Task 8: `lane_checks::LaneCheck` reuses the analytics `DiscoveryCapacityClient` and fetches all services at once. Readiness reports the summary, including services whose width Discovery did not return.
- Task 9: the runtime API type is `Lane` (`llmLanes`), with `waitP50Ms` and `waitP95Ms`. Pool names come from `RoutingTable::pool_name`.
