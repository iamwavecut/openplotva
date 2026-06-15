# Plan 008: Move sqlx off the alpha pin onto stable 0.9.0

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 9f32c4b..HEAD -- Cargo.toml Cargo.lock`
> If the sqlx pin already changed, re-read the current pin before proceeding.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: MED
- **Depends on**: plans/001-ci-run-tests-and-clippy.md (so the test suite actually gates this bump in CI)
- **Category**: migration
- **Planned at**: commit `9f32c4b`, 2026-06-15

## Why this matters

The workspace pins `sqlx = "0.9.0-alpha.1"` — an alpha release — for the entire database
layer (`openplotva-storage` and everything above it). Stable `0.9.0` is published on
crates.io. Running the database core on an alpha means no patch releases without a manual
bump, and a higher chance of latent bugs in the most safety-critical layer. Moving to the
stable release removes that risk. The migration is low-mechanical (see "Current state":
the repo uses only runtime `sqlx::query(...)`, no compile-time `query!` macros and no
offline cache, so there is no metadata to regenerate), but it is rated MED risk because
an alpha→stable bump can still rename or adjust APIs that must be reconciled.

## Current state

- `Cargo.toml:61` — the pin:

  ```toml
  sqlx = { version = "0.9.0-alpha.1", default-features = false }
  ```

- `crates/openplotva-storage/Cargo.toml:23` — feature set:

  ```toml
  sqlx = { workspace = true, features = ["macros", "migrate", "postgres", "runtime-tokio", "time"] }
  ```

- Verified facts that scope the risk:
  - **189** runtime `sqlx::query(...)` / `sqlx::query_as::<...>` call sites; **0**
    compile-time `sqlx::query!` / `query_as!` macros.
  - No `.sqlx/` offline cache directory and no `DATABASE_URL`/`SQLX_OFFLINE` build-time
    dependency — so the build does not need a live database or cached query metadata.
  - The pool builder is `connect_postgres` in `openplotva-storage/src/lib.rs` (also
    touched by plan 003); migrations run via `sqlx`'s migrate feature.

- crates.io latest stable: `0.9.0` (the immediate successor to `0.9.0-alpha.1`).

## Commands you will need

| Purpose            | Command                                              | Expected on success |
|--------------------|------------------------------------------------------|---------------------|
| Update the lock    | `cargo update -p sqlx --precise 0.9.0`               | lock now shows 0.9.0 |
| Compile workspace  | `cargo check --workspace --all-targets --all-features` | exit 0            |
| Full fast gate     | `tools/rust-fast-gate.sh`                            | exit 0              |
| Storage live tests | `cargo test -p openplotva-storage -- --ignored --test-threads=1` | only run if a disposable Postgres is available (see Step 4) |

## Scope

**In scope**:
- `Cargo.toml` — the sqlx version pin.
- `Cargo.lock` — updated by `cargo update`.
- `crates/openplotva-storage/src/lib.rs` — ONLY if the bump produces mechanical compile
  errors (e.g. a renamed method) that are localized to sqlx API usage. Any such change
  must be minimal and obviously equivalent.

**Out of scope**:
- Changing sqlx feature flags (keep the existing set unless a feature was renamed
  upstream — if so, that is a STOP-and-report, not a guess).
- Behavioral changes to queries or migrations.
- Other dependency bumps.

## Git workflow

- Branch: `advisor/008-sqlx-stable`
- One commit (manifest + lock together; plus any mechanical storage fix). Message style:
  imperative, capitalized (e.g. "Move sqlx to stable 0.9.0").
- Do NOT push or open a PR unless instructed.

## Steps

### Step 1: Bump the pin

In `Cargo.toml:61` change:

```toml
sqlx = { version = "0.9.0-alpha.1", default-features = false }
```

to:

```toml
sqlx = { version = "0.9.0", default-features = false }
```

**Verify**: `grep -n 'sqlx = { version' Cargo.toml` shows `0.9.0` (no `-alpha`).

### Step 2: Update the lockfile precisely

```sh
cargo update -p sqlx --precise 0.9.0
```

**Verify**: `awk '/name = "sqlx"/{getline; print}' Cargo.lock` shows `version = "0.9.0"`.

### Step 3: Compile and run the full gate

```sh
cargo check --workspace --all-targets --all-features
tools/rust-fast-gate.sh
```

**Verify**: both exit 0. The fast gate runs fmt + check + clippy + the full
(non-ignored) test suite.

If compilation fails with a small number of mechanical API errors confined to
`openplotva-storage/src/lib.rs` (e.g. a renamed option method), fix them to the obvious
0.9.0 equivalent and re-run. If the errors are numerous, spread across crates, or
require judgment about changed semantics, STOP and report the full error list (see STOP
conditions).

### Step 4 (optional, only with a disposable database): run the DSN-gated live tests

The storage crate has `#[ignore]`d live tests that exercise real Postgres round-trips.
If — and only if — a disposable Postgres is available (e.g. `docker compose up -d postgres`
per the README) and a DSN is exported, run them serially to catch runtime (not just
compile-time) regressions:

```sh
cargo test -p openplotva-storage -- --ignored --test-threads=1
```

(Per project notes these live tests share one database and must run with
`--test-threads=1`.) If no database is available, SKIP this step and say so in your
report — do not fabricate a result.

## Test plan

- No new tests; the existing workspace suite (run by the fast gate) is the regression
  guard for the bump.
- If a live Postgres is available, the DSN-gated storage tests (Step 4) provide runtime
  confidence; otherwise note they were not run.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `Cargo.toml` pins `sqlx` at `0.9.0` (no `-alpha`).
- [ ] `Cargo.lock` resolves sqlx to `0.9.0`.
- [ ] `tools/rust-fast-gate.sh` exits 0 (fmt + check + clippy + full test suite pass).
- [ ] If any source change was needed, it is confined to `crates/openplotva-storage/src/lib.rs` and is mechanically equivalent.
- [ ] Step 4 either passed against a real DB or was explicitly skipped (stated in the report) — not silently omitted.
- [ ] `plans/README.md` status row updated.

## STOP conditions

Stop and report back (do not improvise) if:

- The bump produces compile errors that are not a small, obvious, localized rename
  (report the full `cargo check` error output).
- A sqlx feature flag used in `crates/openplotva-storage/Cargo.toml` was renamed/removed
  in 0.9.0.
- The fast gate's test step surfaces failures that did not exist before the bump (run
  `git stash` to confirm they are bump-induced) — report which tests.
- `0.9.0` is not actually resolvable (e.g. yanked) — report and stop.

## Maintenance notes

- After this lands, periodically bump within the 0.9.x line (now that CI runs tests, a
  patch bump is low-risk).
- This interacts with plan 003 (both touch the sqlx pool API); if 003 has not landed,
  the `acquire_timeout`/`idle_timeout` methods still exist on 0.9.0 so order does not
  matter, but a reviewer should sanity-check the pool builder compiles against whichever
  version is active.
- Reviewer should confirm the diff is essentially the manifest + lockfile, with at most a
  tiny mechanical storage change.
