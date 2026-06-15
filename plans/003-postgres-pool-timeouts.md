# Plan 003: Bound the Postgres connection pool with acquire and idle timeouts

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 9f32c4b..HEAD -- crates/openplotva-storage/src/lib.rs`
> If that file changed since this plan was written, compare the "Current state"
> excerpt against the live code before proceeding; on a mismatch, treat it as a
> STOP condition.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: perf
- **Planned at**: commit `9f32c4b`, 2026-06-15

## Why this matters

The Postgres pool is built with only `max_connections`, `min_connections`, and
`max_lifetime` — no `acquire_timeout` and no `idle_timeout`. The same pool serves the
outbound send path (rate-limit persistence, virtual-message bookkeeping). If the
database slows or briefly becomes unreachable, callers wait on the sqlx default
acquire timeout (30s) for a connection, and idle connections to a bounced/failed-over
database are never proactively recycled. A prior production incident (June 2026) traced
hangs partly to unbounded waits in these paths. Adding an explicit, smaller
`acquire_timeout` turns connection starvation into a fast, observable error instead of
a silent stall, and `idle_timeout` lets the pool heal after a database restart. Both
are pure resilience additions with no behavior change on the happy path.

## Current state

- `crates/openplotva-storage/src/lib.rs:44-46` — pool sizing constants:

  ```rust
  const POSTGRES_MAX_CONNECTIONS: u32 = 50;
  const POSTGRES_MIN_CONNECTIONS: u32 = 10;
  const POSTGRES_MAX_CONNECTION_LIFETIME: Duration = Duration::from_secs(45 * 60);
  ```

- `crates/openplotva-storage/src/lib.rs:6768-6776` — the pool builder (the only
  non-test `PgPoolOptions` for the runtime pool; the many other `PgPoolOptions::new()`
  occurrences in this file are inside `#[cfg(test)]` modules and are OUT of scope):

  ```rust
  pub async fn connect_postgres(config: &PostgresConfig) -> Result<PgPool, StorageError> {
      PgPoolOptions::new()
          .max_connections(POSTGRES_MAX_CONNECTIONS)
          .min_connections(POSTGRES_MIN_CONNECTIONS)
          .max_lifetime(POSTGRES_MAX_CONNECTION_LIFETIME)
          .connect(&config.startup_dsn())
          .await
          .map_err(StorageError::from)
  }
  ```

- This same pool is later used by `run_migrations_on(&postgres)` (see
  `connect_services` just above, around `lib.rs:6755`). That is the reason a
  **blanket per-statement timeout is intentionally NOT part of this plan** — a global
  `statement_timeout` would also apply to migrations (including index builds) and could
  abort a legitimately long migration. Statement timeouts are applied per-query where
  needed (an example already exists at `lib.rs:5487`: `SET LOCAL statement_timeout = 10000`).

Convention: timeouts/sizes are named `const` items at the top of the file (lines
44–46). Add the new ones there, next to the existing pool constants, matching their
`Duration::from_secs(...)` style.

## Commands you will need

| Purpose            | Command                                       | Expected on success |
|--------------------|-----------------------------------------------|---------------------|
| Compile storage    | `cargo check -p openplotva-storage`           | exit 0              |
| Storage tests      | `cargo test -p openplotva-storage`            | all pass (DSN-gated live tests are `#[ignore]` and skipped) |
| Clippy             | `cargo clippy -p openplotva-storage --all-targets -- -D warnings` | exit 0 |
| Format             | `cargo fmt --all`                             | rewrites in place   |

## Scope

**In scope** (the only file you should modify):
- `crates/openplotva-storage/src/lib.rs` — the two new constants and the
  `connect_postgres` builder only.

**Out of scope** (do NOT touch):
- Any `PgPoolOptions::new()` inside `#[cfg(test)]` modules (the `max_connections(1)` /
  `max_connections(2)` test pools).
- Adding a pool-wide `statement_timeout` / `after_connect` hook — explicitly deferred
  (see Why this matters; would endanger migrations). If you believe it is needed, note
  it as a follow-up, do not implement it here.
- The Redis connection builder.

## Git workflow

- Branch: `advisor/003-pg-pool-timeouts`
- One commit. Message style: imperative, capitalized (e.g. "Bound the Postgres pool
  with acquire and idle timeouts").
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Add the two timeout constants

Next to `lib.rs:44-46`, add:

```rust
/// Max wait for a pooled connection before the caller gets an error instead of
/// hanging. Kept below the dispatcher send budget so connection starvation surfaces
/// as a fast, observable failure rather than a silent stall in the send path.
const POSTGRES_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(10);
/// Recycle connections that have sat idle this long so the pool heals after a
/// database restart/failover instead of pinning dead sockets.
const POSTGRES_IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
```

**Verify**: `cargo check -p openplotva-storage` still compiles (constants unused until
Step 2 — a temporary `dead_code` warning is acceptable mid-step but must be gone after
Step 2).

### Step 2: Apply them in `connect_postgres`

```rust
PgPoolOptions::new()
    .max_connections(POSTGRES_MAX_CONNECTIONS)
    .min_connections(POSTGRES_MIN_CONNECTIONS)
    .max_lifetime(POSTGRES_MAX_CONNECTION_LIFETIME)
    .acquire_timeout(POSTGRES_ACQUIRE_TIMEOUT)
    .idle_timeout(POSTGRES_IDLE_TIMEOUT)
    .connect(&config.startup_dsn())
    .await
    .map_err(StorageError::from)
```

Note the sqlx version is `0.9.0-alpha.1`. The method names `acquire_timeout(Duration)`
and `idle_timeout(impl Into<Option<Duration>>)` are expected to exist. If either does
not compile, STOP (see STOP conditions) — do not guess an alternative API.

**Verify**: `cargo check -p openplotva-storage` exits 0 with no `dead_code` warning for
the new constants.

### Step 3: Lint, format, test

```sh
cargo fmt --all
cargo clippy -p openplotva-storage --all-targets -- -D warnings
cargo test -p openplotva-storage
```

**Verify**: all exit 0; tests pass.

## Test plan

- This is a pool-configuration change; the existing storage tests (59 of them) must
  still pass and are the regression guard that the builder still produces a working
  pool.
- No new unit test is required (pool timeouts are not observable without a real DB,
  and the live round-trip tests are DSN-gated `#[ignore]`). If you want a guard, add a
  trivial test asserting `connect_postgres` against a bad DSN returns an error within
  roughly the acquire timeout — but only if it does not require a live database; if it
  would, skip it and note so.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `grep -c "acquire_timeout" crates/openplotva-storage/src/lib.rs` ≥ 1 and the call is inside `connect_postgres`.
- [ ] `grep -c "idle_timeout" crates/openplotva-storage/src/lib.rs` ≥ 1 and the call is inside `connect_postgres`.
- [ ] `cargo clippy -p openplotva-storage --all-targets -- -D warnings` exits 0 (no `dead_code`).
- [ ] `cargo test -p openplotva-storage` passes.
- [ ] No pool-wide `statement_timeout` / `after_connect` was added.
- [ ] Only `crates/openplotva-storage/src/lib.rs` is modified (`git status`).
- [ ] `plans/README.md` status row updated.

## STOP conditions

Stop and report back (do not improvise) if:

- `acquire_timeout` or `idle_timeout` is not a method on `PgPoolOptions` in this sqlx
  version (compile error) — report the exact error; do not substitute a different API.
- `connect_postgres` no longer matches the "Current state" excerpt.
- The same `connect_postgres` pool turns out NOT to be the runtime pool (e.g. a
  separate runtime pool builder exists elsewhere) — report it so the change targets the
  right pool.

## Maintenance notes

- If a future change adds a genuinely long-running query to the runtime pool, an
  `acquire_timeout` of 10s is fine (it bounds connection *acquisition*, not query
  execution), but revisit if `min_connections` or workload changes substantially.
- Deferred follow-up: a per-connection default `statement_timeout` via `after_connect`,
  applied to a **non-migration** pool only (migrations would need their own
  timeout-free pool). That is a larger change (split the pools) and is intentionally not
  in this plan.
- Reviewer should confirm the change is on the runtime pool, not a test pool, and that
  no `statement_timeout` crept in.
