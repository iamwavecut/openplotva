# DB Bloat Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop unbounded growth of the production Postgres DB (29 GB) by wiring per-table retention workers, dropping unused indexes, retiring the dead virtual-message tables, and preparing one-time reclaim ops — without changing any external contract.

**Architecture:** Three new daily retention workers (chat_history partition-drop, telegram_files batched delete, whitecircle_checks batched delete) mirror the proven `run_llm_request_event_cleanup_worker_until` pattern. Data-layer SQL + functions live in `openplotva-storage`; the worker loops live in a new `openplotva-app/src/runtime_retention.rs`; each is gated by a `*_RETENTION_DAYS` config knob (`0` disables). The existing llm-events cleanup worker is extended to also purge `memory_runs`. The dead virtual-message subsystem (`message_id_map` + `message_ops_queue`) is removed in code and dropped via migration. Index drops + LZ4 compression ship as migrations. Heap-bloat reclaim (`pg_repack`) and the uploader media cron are one-time OPS steps, NOT run automatically.

**Tech Stack:** Rust (tokio, sqlx 0.8, time crate), Postgres 17 (pgvector image), embedded migrations via `sqlx::migrate!("../../migrations")`.

## Global Constraints

- Retention windows (maintainer-approved, exact): chat_history = **8 days**, telegram_files = **7 days** (by `last_seen_at`), whitecircle_checks = **30 days**. memory_runs purge cutoff = the llm worker's `retention_days` (default **14**, must stay ≥ 7).
- Every retention worker is gated by an env knob where `0` (or negative) **disables** it (`enabled = retention_days > 0`), mirroring `llm_request_events_retention_days`.
- Workers default to their approved windows but MUST be deployed in **log-only/dry-run posture first** where noted, then flipped on after the maintainer eyeballs one run.
- Migrations: next free number is **129**. Files are `migrations/NNN_name.up.sql` + `migrations/NNN_name.down.sql`. No `-- Source SHA-256:` header is needed (Rust-era migrations 120/128 omit it; no test/tool enforces it).
- TEST CONSTRAINT (enforced by `concurrent_index_migrations_are_single_statement_no_tx_files`, `crates/openplotva-storage/src/lib.rs:8102`): any migration containing `CONCURRENTLY` MUST have `-- no-transaction` as its first line AND exactly one SQL statement (one `;`). So each `CREATE/DROP INDEX CONCURRENTLY` is its own migration file.
- No external-contract change: no Telegram payload/route/callback change, no GraphQL shape change, DB schema *meaning* preserved, Redis untouched.
- Never run destructive prod DDL/DELETE or deploy from this plan's coding tasks. Prod application happens via `gh workflow run deploy-production.yml --ref main` (migrations auto-run on startup) + the OPS appendix, all gated on maintainer go-ahead and a confirmed backup.
- After Rust edits: `cargo fmt --all`. Verify per task. Final gates: `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test -p openplotva-storage`, `cargo test -p openplotva-config`, `cargo test -p openplotva-app`. DB-backed storage tests are gated on `OPENPLOTVA_TEST_POSTGRES_DSN` (skip silently if unset — set it to the local compose DB to actually run them).

---

## File Structure

- `migrations/129..138_*.{up,down}.sql` — Create: index drops, the new `last_seen_at` index, LZ4 compression, virtual-message table drop. One responsibility each.
- `crates/openplotva-storage/src/lib.rs` — Modify: add 3 SQL consts + 3 data functions (`drop_expired_chat_history_partitions`, `delete_old_telegram_files_batch`, `delete_old_whitecircle_checks_batch`) and their DB-backed tests; add `SQL_DELETE_OLD_MEMORY_RUNS` usage is in app (see below); remove the now-dead virtual-message storage methods.
- `crates/openplotva-app/src/runtime_retention.rs` — Create: the three worker loops (`run_chat_history_partition_retention_worker_until`, `run_telegram_files_retention_worker_until`, `run_whitecircle_checks_retention_worker_until`) + report structs + unit tests.
- `crates/openplotva-app/src/runtime_llm.rs` — Modify: add `SQL_DELETE_OLD_MEMORY_RUNS` const and execute it inside `delete_old_llm_request_events_batch` (memory_runs purge).
- `crates/openplotva-app/src/lib.rs` — Modify: register `mod runtime_retention;`, spawn the 3 workers next to the llm cleanup spawn (~9153-9184); remove the virtual-message wiring (pending worker spawn ~10793, redis vmsg restore ~9428-9440).
- `crates/openplotva-app/src/virtual_messages.rs`, `crates/openplotva-app/src/pending_ops.rs`, `crates/openplotva-app/src/runtime_pending_ops.rs` — Delete (virtual-message retirement), plus their `mod`/`use` references.
- `crates/openplotva-config/src/lib.rs` — Modify: add 3 retention knobs (const + typed field + raw field + parse) mirroring `llm_request_events_retention_days`.
- `tools/db-reclaim.sh`, `tools/uploader-retention.cron` — Create: one-time OPS appendix (pg_repack + uploader find-delete). NOT auto-run.

---

## Task 1: Add retention config knobs

**Files:**
- Modify: `crates/openplotva-config/src/lib.rs` (mirror `llm_request_events_retention_days`: const `:35`, typed field `:378`, raw field `:880`, parse `:1461`)

**Interfaces:**
- Produces: `config.history_summary.chat_history_retention_days: i32`, `config.vision.telegram_files_retention_days: i32`, `config.white_circle.whitecircle_checks_retention_days: i32`. Env: `CHAT_HISTORY_RETENTION_DAYS`, `TELEGRAM_FILES_RETENTION_DAYS`, `WHITECIRCLE_CHECKS_RETENTION_DAYS`.

- [ ] **Step 1: Add the failing config test** — append to the config test module a test asserting defaults:

```rust
#[test]
fn retention_knob_defaults_match_approved_windows() {
    let config = AppConfig::from_env_raw(RawConfig::default()).expect("config");
    assert_eq!(config.history_summary.chat_history_retention_days, 8);
    assert_eq!(config.vision.telegram_files_retention_days, 7);
    assert_eq!(config.white_circle.whitecircle_checks_retention_days, 30);
}
```

> Adjust `AppConfig::from_env_raw`/`RawConfig` to the actual constructor used by sibling tests (grep for an existing `assert_eq!(config.memory.retention_hours, 168)` test and copy its setup verbatim).

- [ ] **Step 2: Run it to confirm it fails** — `cargo test -p openplotva-config retention_knob_defaults` → FAIL (unknown field).

- [ ] **Step 3: Add the three consts** near line 35:

```rust
pub const DEFAULT_CHAT_HISTORY_RETENTION_DAYS: i32 = 8;
pub const DEFAULT_TELEGRAM_FILES_RETENTION_DAYS: i32 = 7;
pub const DEFAULT_WHITECIRCLE_CHECKS_RETENTION_DAYS: i32 = 30;
```

- [ ] **Step 4: Add typed fields** — on `HistorySummaryConfig` add `pub chat_history_retention_days: i32,`; on `VisionConfig` add `pub telegram_files_retention_days: i32,`; on `WhiteCircleConfig` add `pub whitecircle_checks_retention_days: i32,` (each with a one-line doc comment naming the env var, matching the style at `:377`).

- [ ] **Step 5: Add raw fields** near `:880` on the raw/env struct:

```rust
/// `CHAT_HISTORY_RETENTION_DAYS`.
pub chat_history_retention_days: Option<String>,
/// `TELEGRAM_FILES_RETENTION_DAYS`.
pub telegram_files_retention_days: Option<String>,
/// `WHITECIRCLE_CHECKS_RETENTION_DAYS`.
pub whitecircle_checks_retention_days: Option<String>,
```

- [ ] **Step 6: Add parse lines** in each sub-config's builder block (mirror `:1461`):

```rust
chat_history_retention_days: parse_i32(
    "CHAT_HISTORY_RETENTION_DAYS",
    raw.chat_history_retention_days,
    DEFAULT_CHAT_HISTORY_RETENTION_DAYS,
)?,
// telegram_files_retention_days: ... DEFAULT_TELEGRAM_FILES_RETENTION_DAYS
// whitecircle_checks_retention_days: ... DEFAULT_WHITECIRCLE_CHECKS_RETENTION_DAYS
```

- [ ] **Step 7: Document env vars** — add the three to `.env.example` with their defaults and a one-line note (`0 disables`).

- [ ] **Step 8: Run + commit** — `cargo fmt --all && cargo test -p openplotva-config` → PASS. `git commit -m "config: add chat_history/telegram_files/whitecircle retention knobs"`.

---

## Task 2: Storage data functions for retention (with DB-backed tests)

**Files:**
- Modify: `crates/openplotva-storage/src/lib.rs` (SQL consts near `:724` where `SQL_ENSURE_CHAT_HISTORY_PARTITION` lives; functions near the history/telegram store impls; tests in the test module that uses `OPENPLOTVA_TEST_POSTGRES_DSN`, pattern at `:9077`)

**Interfaces:**
- Produces (free async fns on `PgPool`, matching how the llm worker uses the pool directly):
  - `pub async fn drop_expired_chat_history_partitions(pool: &PgPool, retention_days: i32) -> Result<Vec<String>, sqlx::Error>`
  - `pub async fn delete_old_telegram_files_batch(pool: &PgPool, retention_days: i32, batch_size: i64) -> Result<u64, sqlx::Error>`
  - `pub async fn delete_old_whitecircle_checks_batch(pool: &PgPool, retention_days: i32, batch_size: i64) -> Result<u64, sqlx::Error>`

- [ ] **Step 1: Add SQL consts** near `:724`:

```rust
pub const SQL_DROP_EXPIRED_CHAT_HISTORY_PARTITIONS: &str =
    "SELECT drop_expired_chat_history_partitions((current_date - $1::int))";
pub const SQL_DELETE_OLD_TELEGRAM_FILES_BATCH: &str = r#"
WITH doomed AS (
    SELECT file_unique_id
    FROM telegram_files
    WHERE last_seen_at < now() - ($1::int * interval '1 day')
    ORDER BY last_seen_at ASC
    LIMIT $2
)
DELETE FROM telegram_files t
USING doomed
WHERE t.file_unique_id = doomed.file_unique_id"#;
pub const SQL_DELETE_OLD_WHITECIRCLE_CHECKS_BATCH: &str = r#"
WITH doomed AS (
    SELECT id
    FROM whitecircle_checks
    WHERE created_at < now() - ($1::int * interval '1 day')
    ORDER BY created_at ASC
    LIMIT $2
)
DELETE FROM whitecircle_checks w
USING doomed
WHERE w.id = doomed.id"#;
```

- [ ] **Step 2: Write failing DB-backed tests** in the test module (gate on `OPENPLOTVA_TEST_POSTGRES_DSN` exactly like `:9073`). Insert one in-window + one out-of-window row, run the function, assert only the old one is removed. Example for telegram_files:

```rust
#[tokio::test]
async fn delete_old_telegram_files_batch_removes_only_aged_rows() -> Result<(), Box<dyn Error>> {
    let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else { return Ok(()); };
    let pool = PgPoolOptions::new().max_connections(2).connect(&dsn).await?;
    super::run_migrations_on(&pool).await?;
    let fresh = format!("freshunique{}", start_id_suffix()); // unique to avoid PK clash
    let stale = format!("staleunique{}", start_id_suffix());
    sqlx::query("INSERT INTO telegram_files (file_unique_id, latest_file_id, media_kind, last_seen_at) VALUES ($1,'f','photo', now()), ($2,'f','photo', now() - interval '30 days')")
        .bind(&fresh).bind(&stale).execute(&pool).await?;
    let deleted = super::delete_old_telegram_files_batch(&pool, 7, 10_000).await?;
    assert!(deleted >= 1);
    let stale_left: i64 = sqlx::query_scalar("SELECT count(*) FROM telegram_files WHERE file_unique_id = $1").bind(&stale).fetch_one(&pool).await?;
    let fresh_left: i64 = sqlx::query_scalar("SELECT count(*) FROM telegram_files WHERE file_unique_id = $1").bind(&fresh).fetch_one(&pool).await?;
    assert_eq!(stale_left, 0);
    assert_eq!(fresh_left, 1);
    Ok(())
}
```

> Write the analogous test for `delete_old_whitecircle_checks_batch` (insert two rows with `created_at` now / now-90d, assert the aged one is deleted). For `drop_expired_chat_history_partitions`, insert one row via `ensure_chat_history_partition((current_date-20))` + a direct partition insert, then assert the function returns a partition name and the row count drops; keep it minimal and skip if partitioning fixtures are heavy — the unit assertion on the SQL const + a smoke on a freshly created old partition is sufficient.

- [ ] **Step 3: Run to confirm fail** — `OPENPLOTVA_TEST_POSTGRES_DSN=<local> cargo test -p openplotva-storage delete_old_telegram_files_batch` → FAIL (fn missing).

- [ ] **Step 4: Implement the three functions** (place near the other history/telegram store helpers):

```rust
pub async fn drop_expired_chat_history_partitions(
    pool: &PgPool,
    retention_days: i32,
) -> Result<Vec<String>, sqlx::Error> {
    if retention_days <= 0 {
        return Ok(Vec::new());
    }
    let dropped: Vec<String> = sqlx::query_scalar(SQL_DROP_EXPIRED_CHAT_HISTORY_PARTITIONS)
        .bind(retention_days)
        .fetch_one(pool)
        .await?;
    Ok(dropped)
}

pub async fn delete_old_telegram_files_batch(
    pool: &PgPool,
    retention_days: i32,
    batch_size: i64,
) -> Result<u64, sqlx::Error> {
    if retention_days <= 0 || batch_size <= 0 {
        return Ok(0);
    }
    let result = sqlx::query(SQL_DELETE_OLD_TELEGRAM_FILES_BATCH)
        .bind(retention_days)
        .bind(batch_size)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

pub async fn delete_old_whitecircle_checks_batch(
    pool: &PgPool,
    retention_days: i32,
    batch_size: i64,
) -> Result<u64, sqlx::Error> {
    if retention_days <= 0 || batch_size <= 0 {
        return Ok(0);
    }
    let result = sqlx::query(SQL_DELETE_OLD_WHITECIRCLE_CHECKS_BATCH)
        .bind(retention_days)
        .bind(batch_size)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}
```

- [ ] **Step 5: Run + commit** — `cargo fmt --all`; with local DSN set: `cargo test -p openplotva-storage` → PASS. `git commit -m "storage: add chat_history/telegram_files/whitecircle retention data functions"`.

---

## Task 3: Retention worker module (`runtime_retention.rs`)

**Files:**
- Create: `crates/openplotva-app/src/runtime_retention.rs`
- Modify: `crates/openplotva-app/src/lib.rs` (add `mod runtime_retention;` near the other `mod runtime_*;` declarations)

**Interfaces:**
- Consumes: `openplotva_storage::{drop_expired_chat_history_partitions, delete_old_telegram_files_batch, delete_old_whitecircle_checks_batch}` (Task 2); `wait_for_runtime_stop` (existing helper used at `lib.rs:9164`).
- Produces: `run_chat_history_partition_retention_worker_until`, `run_telegram_files_retention_worker_until`, `run_whitecircle_checks_retention_worker_until`, consts `RETENTION_CLEANUP_INTERVAL`, `RETENTION_DELETE_BATCH_SIZE`, `RETENTION_INTER_BATCH_PAUSE`, report structs.

- [ ] **Step 1: Write the module with a unit test for the disabled guard.** Create `crates/openplotva-app/src/runtime_retention.rs`:

```rust
//! Daily data-retention workers: drop expired chat_history partitions and
//! batch-delete aged telegram_files / whitecircle_checks rows. Each worker is
//! a no-op when its retention_days is <= 0 (the knob's "disabled" value).
//!
//! Mirrors the proven runtime_llm cleanup worker: a drain-then-sleep loop that
//! cancels promptly on the runtime stop signal and never holds a lock across
//! the sleep.

use std::time::Duration;

use sqlx::PgPool;

/// Once per day. Long enough that DDL/bulk deletes are infrequent; the drain
/// loop inside one tick still clears any large initial backlog gradually.
pub const RETENTION_CLEANUP_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
pub const RETENTION_DELETE_BATCH_SIZE: i64 = 10_000;
/// Small pause between batches so a large initial drain does not spike
/// replication lag or autovacuum pressure.
pub const RETENTION_INTER_BATCH_PAUSE: Duration = Duration::from_millis(200);

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PartitionRetentionReport {
    pub enabled: bool,
    pub dropped_partitions: u64,
    pub ticks: u64,
    pub errors: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BatchedRetentionReport {
    pub enabled: bool,
    pub deleted: u64,
    pub ticks: u64,
    pub errors: u64,
}

pub async fn run_chat_history_partition_retention_worker_until<Stop>(
    pool: PgPool,
    interval: Duration,
    retention_days: i32,
    stop: Stop,
) -> PartitionRetentionReport
where
    Stop: std::future::Future<Output = ()>,
{
    let mut report = PartitionRetentionReport {
        enabled: retention_days > 0,
        ..PartitionRetentionReport::default()
    };
    if !report.enabled {
        return report;
    }
    tokio::pin!(stop);
    loop {
        match openplotva_storage::drop_expired_chat_history_partitions(&pool, retention_days).await {
            Ok(dropped) => {
                report.dropped_partitions += dropped.len() as u64;
                if !dropped.is_empty() {
                    tracing::info!(?dropped, retention_days, "dropped expired chat_history partitions");
                }
            }
            Err(error) => {
                report.errors += 1;
                tracing::warn!(%error, retention_days, "failed to drop expired chat_history partitions");
            }
        }
        report.ticks += 1;
        let sleep = tokio::time::sleep(interval);
        tokio::pin!(sleep);
        tokio::select! {
            () = &mut stop => break,
            () = &mut sleep => {}
        }
    }
    report
}

async fn run_batched_retention_worker_until<Stop, F, Fut>(
    label: &'static str,
    interval: Duration,
    retention_days: i32,
    batch_size: i64,
    inter_batch: Duration,
    mut delete_batch: F,
    stop: Stop,
) -> BatchedRetentionReport
where
    Stop: std::future::Future<Output = ()>,
    F: FnMut(i64) -> Fut,
    Fut: std::future::Future<Output = Result<u64, sqlx::Error>>,
{
    let mut report = BatchedRetentionReport {
        enabled: retention_days > 0,
        ..BatchedRetentionReport::default()
    };
    if !report.enabled {
        return report;
    }
    tokio::pin!(stop);
    'outer: loop {
        loop {
            match delete_batch(batch_size).await {
                Ok(deleted) => {
                    report.deleted += deleted;
                    if (deleted as i64) < batch_size {
                        break;
                    }
                }
                Err(error) => {
                    report.errors += 1;
                    tracing::warn!(%error, label, retention_days, "retention batch delete failed");
                    break;
                }
            }
            let pause = tokio::time::sleep(inter_batch);
            tokio::pin!(pause);
            tokio::select! {
                () = &mut stop => break 'outer,
                () = &mut pause => {}
            }
        }
        report.ticks += 1;
        let sleep = tokio::time::sleep(interval);
        tokio::pin!(sleep);
        tokio::select! {
            () = &mut stop => break,
            () = &mut sleep => {}
        }
    }
    report
}

pub async fn run_telegram_files_retention_worker_until<Stop>(
    pool: PgPool,
    interval: Duration,
    retention_days: i32,
    batch_size: i64,
    inter_batch: Duration,
    stop: Stop,
) -> BatchedRetentionReport
where
    Stop: std::future::Future<Output = ()>,
{
    run_batched_retention_worker_until(
        "telegram_files",
        interval,
        retention_days,
        batch_size,
        inter_batch,
        |batch| {
            let pool = pool.clone();
            async move {
                openplotva_storage::delete_old_telegram_files_batch(&pool, retention_days, batch).await
            }
        },
        stop,
    )
    .await
}

pub async fn run_whitecircle_checks_retention_worker_until<Stop>(
    pool: PgPool,
    interval: Duration,
    retention_days: i32,
    batch_size: i64,
    inter_batch: Duration,
    stop: Stop,
) -> BatchedRetentionReport
where
    Stop: std::future::Future<Output = ()>,
{
    run_batched_retention_worker_until(
        "whitecircle_checks",
        interval,
        retention_days,
        batch_size,
        inter_batch,
        |batch| {
            let pool = pool.clone();
            async move {
                openplotva_storage::delete_old_whitecircle_checks_batch(&pool, retention_days, batch).await
            }
        },
        stop,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn partition_worker_disabled_returns_immediately() {
        // A never-completing stop future proves the disabled branch returns
        // without awaiting anything (no pool access).
        let pool = PgPool::connect_lazy("postgres://invalid/invalid").expect("lazy pool");
        let report = run_chat_history_partition_retention_worker_until(
            pool,
            Duration::from_secs(1),
            0,
            std::future::pending::<()>(),
        )
        .await;
        assert!(!report.enabled);
        assert_eq!(report.ticks, 0);
    }
}
```

- [ ] **Step 2: Register module** — add `mod runtime_retention;` to `crates/openplotva-app/src/lib.rs` alongside the existing `mod runtime_llm;` / `mod runtime_safety;` declarations.

- [ ] **Step 3: Run + commit** — `cargo fmt --all && cargo test -p openplotva-app runtime_retention` → PASS (the disabled-guard test runs with no DB). `git commit -m "app: add runtime_retention workers (chat_history/telegram_files/whitecircle)"`.

---

## Task 4: Spawn the retention workers (gated, with readiness checks)

**Files:**
- Modify: `crates/openplotva-app/src/lib.rs` — insert immediately after the llm cleanup spawn block (after `:9184`), mirroring that block exactly (gate on `> 0`, clone pool, subscribe stop, spawn, push readiness check + handle; else skipped readiness).

**Interfaces:**
- Consumes: `config.history_summary.chat_history_retention_days`, `config.vision.telegram_files_retention_days`, `config.white_circle.whitecircle_checks_retention_days`; `runtime_retention::*`; `service_clients.postgres`, `stop`, `workers.handles`, `readiness_checks`, `ReadinessCheck`, `wait_for_runtime_stop`.

- [ ] **Step 1: Add the chat_history spawn block** (model on `:9153-9184`):

```rust
let chat_history_retention_days = config.history_summary.chat_history_retention_days;
if chat_history_retention_days > 0 {
    let pool = service_clients.postgres.clone();
    let stop_rx = stop.subscribe();
    let worker = tokio::spawn(async move {
        let report = runtime_retention::run_chat_history_partition_retention_worker_until(
            pool,
            runtime_retention::RETENTION_CLEANUP_INTERVAL,
            chat_history_retention_days,
            wait_for_runtime_stop(stop_rx),
        )
        .await;
        tracing::info!(?report, "chat_history partition retention worker stopped");
    });
    readiness_checks.push(ReadinessCheck::ok(
        "chat_history_retention",
        format!("chat_history partitions dropped daily, retention {chat_history_retention_days}d"),
    ));
    workers.handles.push(worker);
} else {
    readiness_checks.push(ReadinessCheck::skipped(
        "chat_history_retention",
        "chat_history partition retention disabled",
    ));
}
```

- [ ] **Step 2: Add the telegram_files spawn block** — same shape, calling `run_telegram_files_retention_worker_until(pool, RETENTION_CLEANUP_INTERVAL, telegram_files_retention_days, RETENTION_DELETE_BATCH_SIZE, RETENTION_INTER_BATCH_PAUSE, wait_for_runtime_stop(stop_rx))`, readiness id `telegram_files_retention`.

- [ ] **Step 3: Add the whitecircle_checks spawn block** — same shape, calling `run_whitecircle_checks_retention_worker_until(...)`, readiness id `whitecircle_checks_retention`.

- [ ] **Step 4: Run + commit** — `cargo fmt --all && cargo clippy -p openplotva-app --all-targets -- -D warnings && cargo test -p openplotva-app` → PASS. `git commit -m "app: spawn chat_history/telegram_files/whitecircle retention workers (knob-gated)"`.

---

## Task 5: memory_runs purge inside the llm cleanup worker

**Files:**
- Modify: `crates/openplotva-app/src/runtime_llm.rs` — add a const near `:205` and execute it inside `delete_old_llm_request_events_batch` (`:622-662`), within the existing advisory-locked transaction, after the rollup loop.

**Interfaces:**
- Consumes: the existing `retention_days` param + advisory-lock tx in `delete_old_llm_request_events_batch`.

- [ ] **Step 1: Add a failing SQL-shape test** to the runtime_llm test module (mirrors `:1343`):

```rust
#[test]
fn delete_old_memory_runs_targets_only_terminal_runs() {
    assert!(SQL_DELETE_OLD_MEMORY_RUNS.contains("status IN ('completed','skipped','failed')"));
    assert!(SQL_DELETE_OLD_MEMORY_RUNS.contains("range_start_at <"));
}
```

- [ ] **Step 2: Run to confirm fail** — `cargo test -p openplotva-app delete_old_memory_runs_targets` → FAIL (const missing).

- [ ] **Step 3: Add the const** near `SQL_ROLLUP_MEMORY_RUNS` (`:205`):

```rust
// Purge terminal memory_runs after they are rolled up. Cutoff = retention_days,
// which (default 14) MUST stay >= the memory pipeline's ensure_daily window
// (MEMORY_RETENTION_HOURS=168=7d) so re-creation never violates the runs'
// UNIQUE idempotency constraint. Never touches queued/processing runs.
const SQL_DELETE_OLD_MEMORY_RUNS: &str = r#"
DELETE FROM memory_runs
WHERE status IN ('completed','skipped','failed')
  AND range_start_at < now() - ($1::int * interval '1 day')"#;
```

- [ ] **Step 4: Execute it in the tx** — inside `delete_old_llm_request_events_batch`, after the `for granularity in [...]` rollup loop (after `:654`) and before the llm delete (`:655`):

```rust
sqlx::query(SQL_DELETE_OLD_MEMORY_RUNS)
    .bind(retention_days)
    .execute(&mut *tx)
    .await?;
```

- [ ] **Step 5: Run + commit** — `cargo fmt --all && cargo test -p openplotva-app` → PASS. `git commit -m "app: purge terminal memory_runs in the llm cleanup worker"`.

---

## Task 6: Index-drop + last_seen_at + LZ4 migrations

**Files:**
- Create: `migrations/129..137_*.{up,down}.sql`

Each `CONCURRENTLY` migration: first line `-- no-transaction`, exactly one statement. Down migrations recreate the dropped index (so the change is reversible), matching the original definition from `migrations/22` / `migrations/29`.

- [ ] **Step 1: 129 — drop unused telegram_files index `requested`.**
  - `129_drop_idx_telegram_files_requested.up.sql`:
    ```sql
    -- no-transaction
    DROP INDEX CONCURRENTLY IF EXISTS idx_telegram_files_requested;
    ```
  - `.down.sql`:
    ```sql
    -- no-transaction
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_telegram_files_requested ON telegram_files (recognition_requested_at) WHERE recognition_completed_at IS NULL;
    ```
- [ ] **Step 2: 130 — drop `idx_telegram_files_pending_status`** (up: `DROP INDEX CONCURRENTLY IF EXISTS idx_telegram_files_pending_status;`; down: `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_telegram_files_pending_status ON telegram_files (vision_status) WHERE vision_status <> 'completed';`).
- [ ] **Step 3: 131 — drop `idx_telegram_files_last_seen`** (up: `DROP INDEX CONCURRENTLY IF EXISTS idx_telegram_files_last_seen;`; down: `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_telegram_files_last_seen ON telegram_files (last_seen_chat_id, last_seen_at DESC);`).
- [ ] **Step 4: 132 — create the retention-serving `last_seen_at` index.**
  - up: `-- no-transaction` + `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_telegram_files_last_seen_at ON telegram_files (last_seen_at);`
  - down: `-- no-transaction` + `DROP INDEX CONCURRENTLY IF EXISTS idx_telegram_files_last_seen_at;`
- [ ] **Step 5: 133 — drop `idx_whitecircle_checks_chat_created_at`** (down recreates `ON whitecircle_checks (chat_id, created_at DESC)`).
- [ ] **Step 6: 134 — drop `idx_whitecircle_checks_flagged_created_at`** (down recreates `ON whitecircle_checks (flagged, created_at DESC)`).
- [ ] **Step 7: 135 — drop `idx_whitecircle_checks_external_session_created_at`** (down recreates `ON whitecircle_checks (external_session_id, created_at DESC)`).
- [ ] **Step 8: 136 — LZ4 compression on the 3 whitecircle JSONB columns** (transactional, one ALTER, no CONCURRENTLY):
  - `136_whitecircle_checks_lz4.up.sql`:
    ```sql
    ALTER TABLE whitecircle_checks
        ALTER COLUMN request_messages SET COMPRESSION lz4,
        ALTER COLUMN policies SET COMPRESSION lz4,
        ALTER COLUMN response_json SET COMPRESSION lz4;
    ```
  - `.down.sql`:
    ```sql
    ALTER TABLE whitecircle_checks
        ALTER COLUMN request_messages SET COMPRESSION pglz,
        ALTER COLUMN policies SET COMPRESSION pglz,
        ALTER COLUMN response_json SET COMPRESSION pglz;
    ```
  > Note: `SET COMPRESSION` only affects future inserts/rewrites; existing rows recompress when `pg_repack` runs (OPS appendix). Requires PG14+ (prod is PG17).

- [ ] **Step 9: Verify migrations compile + the concurrency test passes** — `cargo test -p openplotva-storage concurrent_index_migrations_are_single_statement_no_tx_files` → PASS (proves each CONCURRENTLY file is no_tx + single statement). `cargo build -p openplotva-storage` → OK (embeds the new files).
- [ ] **Step 10: Commit** — `git commit -m "migrations: drop unused telegram_files/whitecircle indexes, add last_seen_at index, LZ4 whitecircle JSONB"`.

---

## Task 7: Retire the virtual-message subsystem (HIGHEST RISK — code removal + table drop)

> The tables can only be dropped together with the code that touches them: with the tables gone, `resolve_virtual_message`'s UPDATE would raise "relation does not exist" on the live send path. So removal + migration deploy atomically. Confirmed dead: `message_ops_queue` is empty in prod (0 rows); deferred edit/delete fns have no non-test callers; post-cutover inserts fail on `VARCHAR(32)` overflow and are already ignored; resolution is inline at send and does not depend on the row. Do this task in compile-checkpointed sub-steps.

**Files:**
- Modify: `crates/openplotva-app/src/lib.rs` (remove pending-op worker spawn `~:10793`; remove redis vmsg restore `~:9428-9440`; remove `mod virtual_messages/pending_ops/runtime_pending_ops` + their `use`s; remove `insert_virtual_message`/`resolve_virtual_message` call sites in the `queue_*` producers and `send_work_item_and_resolve_inner`).
- Modify: `crates/openplotva-storage/src/lib.rs` (remove `insert_virtual_message`, `resolve_virtual_message`, `get_mapping_by_virtual`, `get_mapping_by_real`, `delete_mapping_by_virtual`, `enqueue_message_op` + their SQL consts + their tests).
- Modify: `crates/openplotva-telegram/src/persistence.rs` (remove the `virtual_id` field from `DispatcherMessage`/work-item metadata if it becomes unused).
- Delete: `crates/openplotva-app/src/virtual_messages.rs`, `crates/openplotva-app/src/pending_ops.rs`, `crates/openplotva-app/src/runtime_pending_ops.rs`.
- Create: `migrations/137_drop_virtual_message_tables.{up,down}.sql`.

- [ ] **Step 1: Re-confirm dead-path on prod before deleting anything** (read-only): `ssh geta.moe "docker exec -i openplotva-postgresql-1 psql -U plotva -d plotva -c \"SELECT count(*) FROM message_ops_queue; SELECT length(vmsg_id) len, count(*) FROM message_id_map WHERE created_at >= '2026-06-03' GROUP BY 1 ORDER BY 1;\""` — expect ops=0 and only len ≤ 32 short-prefix rows. (Already verified 2026-06-27; re-check at execution time.)
- [ ] **Step 2: Remove the deferred-op consumer.** Delete the `pending_worker` spawn block (`lib.rs:~10793`, calling `pending_ops::run_pending_op_worker_with_history_until`) and the runtime-API pending-ops surface wiring (`runtime_pending_ops`). Delete files `pending_ops.rs`, `runtime_pending_ops.rs` and their `mod`/`use`. `cargo build -p openplotva-app` → fix any references until it compiles.
- [ ] **Step 3: Remove the producer-side persistence.** In each `queue_*` producer (e.g. `virtual_messages.rs:738-769`, callers in `lib.rs`) stop calling `next_virtual_id()` + `insert_virtual_message()`; in `send_work_item_and_resolve_inner` (`lib.rs:~1359`) remove the `resolve_virtual_message` call. Delete `virtual_messages.rs` and its `mod`/`use`. If `DispatcherMessage.virtual_id` (`openplotva-telegram/src/persistence.rs:54`) is now unused, remove the field and the redis restore handling (`lib.rs:9428-9440`). `cargo build -p openplotva-app -p openplotva-telegram` → compile-clean.
- [ ] **Step 4: Remove orphaned storage methods + tests** — delete `insert_virtual_message`/`resolve_virtual_message`/`get_mapping_by_virtual`/`get_mapping_by_real`/`delete_mapping_by_virtual`/`enqueue_message_op` and their SQL consts and unit tests (e.g. the `get_mapping_by_real` test at `:10547`). `cargo build -p openplotva-storage` → clean.
- [ ] **Step 5: Add the drop migration** (mirrors `migrations/15_message_virtual_id.down.sql`; FK CASCADE makes order safe):
  - `137_drop_virtual_message_tables.up.sql`:
    ```sql
    DROP TABLE IF EXISTS message_ops_queue;
    DROP TABLE IF EXISTS message_id_map;
    ```
  - `.down.sql` — recreate both tables + indexes verbatim from `migrations/15_message_virtual_id.up.sql` (so the migration is reversible).
- [ ] **Step 6: Full build + tests** — `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p openplotva-app -p openplotva-storage -p openplotva-telegram` → PASS. Run `tools/update-queue-smoke.sh` if available (the dispatcher/outbound path is what this touches) to confirm sends still resolve inline.
- [ ] **Step 7: Commit** — `git commit -m "retire dead virtual-message subsystem (drop message_id_map/message_ops_queue + code)"`.

---

## Task 8: OPS appendix (NOT auto-run) — heap reclaim + uploader retention

**Files:**
- Create: `tools/db-reclaim.sh`, `tools/uploader-retention.cron`

> These are run manually on the prod host AFTER deploy, once the retention workers have drained their backlogs, and only with a confirmed `pg_dump`/snapshot in hand.

- [ ] **Step 1: `tools/db-reclaim.sh`** — documented, idempotent, run-on-host script (uses `pg_repack` in the postgres container; falls back to printing instructions if `pg_repack` is absent). Targets, in order: `telegram_files`, `whitecircle_checks`, `users`, `llm_request_events`, plus `REINDEX INDEX CONCURRENTLY idx_telegram_files_latest_file_id`. Each step echoes before/after `pg_total_relation_size`. Header comment: requires ~2× table disk transient; never `VACUUM FULL` on hot tables; needs maintainer go-ahead + backup.
- [ ] **Step 2: `tools/uploader-retention.cron`** — a systemd-timer or cron line: `find /var/lib/docker/volumes/plotva-uploader_uploader-data/_data -type f -mtime +7 -delete` (verify the exact volume/mount name on the host first via `docker volume ls` / `docker inspect plotva-uploader-uploader-1`). Header notes this is the temporary measure pending the CDN rework; the embedded `https://plotva.geta.moe/...` URLs are canonical in already-sent messages, so 7 days is the maintainer-chosen floor.
- [ ] **Step 3: Per-table autovacuum tuning (optional migration or ops)** — `ALTER TABLE telegram_files SET (autovacuum_vacuum_scale_factor=0.02, autovacuum_vacuum_threshold=50000);` and likewise for `users`, `chat_members`. Ship as `migrations/138_autovacuum_tuning.{up,down}.sql` (transactional, catalog-only) so it deploys with the rest; down resets to defaults.
- [ ] **Step 4: Commit** — `git commit -m "ops: db-reclaim + uploader retention scripts, autovacuum tuning migration"`.

---

## Sequencing, deploy & safeguards

1. **Implement Tasks 1–6 + 8 first** (low risk: additive workers, index drops, LZ4, autovacuum). Land Task 7 (virtual-message retirement) as its own commit/PR — it is invasive code surgery and the easiest to defer if the build surgery proves deeper than expected.
2. **Deploy posture:** the retention knobs default to the approved windows, so a normal deploy turns them ON. For the FIRST production deploy, set `CHAT_HISTORY_RETENTION_DAYS=0`, `TELEGRAM_FILES_RETENTION_DAYS=0`, `WHITECIRCLE_CHECKS_RETENTION_DAYS=0` in the prod env, deploy, and confirm the workers register as "skipped" / log what they *would* delete (temporarily lower the partition worker to log-only by inspecting `drop_expired_chat_history_partitions` output on a manual `SELECT` first). Then flip the knobs to `8`/`7`/`30` and redeploy/restart.
3. **Migrations auto-run on deploy** (`OPENPLOTVA_RUN_MIGRATIONS=true`): index drops + LZ4 + (Task 7) table drop apply during the startup gap. Confirm a fresh `pg_dump` exists first.
4. **After the workers drain** (telegram_files ~6.7M and whitecircle ~1M aged rows delete over hours via batched drain), run `tools/db-reclaim.sh` to return heap/index high-water space to the OS. `message_id_map` (Task 7) reclaims instantly on `DROP TABLE` — no repack needed.
5. **Uploader cron** installed last, after confirming the volume path and the Telegram media re-fetch tolerance.
6. **Safeguards:** index drops/creates use `CONCURRENTLY` (no table-write lock). Bulk deletes are batched with inter-batch pauses. Partition drop is `DROP TABLE` (instant, irreversible — the first real run should be eyeballed). `pg_repack` not `VACUUM FULL` on hot tables. Everything reversible via down-migrations except committed data deletes — hence the backup gate.

## Self-review notes

- Spec coverage: chat_history (T1/T2/T3/T4), telegram_files (T1/T2/T3/T4/T6), whitecircle (T1/T2/T3/T4/T6 incl. index drops + LZ4), message_id_map retire (T7), memory_runs purge (T5), index drops (T6), pg_repack + uploader + autovacuum (T8). All maintainer decisions (8/7/30/retire) encoded in Global Constraints.
- Type consistency: storage fns return `Result<_, sqlx::Error>`; worker fns take `(pool, interval, retention_days[, batch_size, inter_batch], stop)` and return `*RetentionReport`; config fields are `i32` gated `> 0`. Env var names are the stable contract.
- Open verification (carry into execution): exact config sub-struct constructor names (`HistorySummaryConfig`/`VisionConfig`/`WhiteCircleConfig` raw+parse wiring), the `DispatcherMessage.virtual_id` removal blast radius in `openplotva-telegram`, and the exact uploader docker volume mount name.
