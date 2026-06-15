# Plan 005: Bound the producer run-report error vector

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 9f32c4b..HEAD -- crates/openplotva-updates/src/lib.rs`
> On any change, compare the "Current state" excerpts to the live code; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: bug
- **Planned at**: commit `9f32c4b`, 2026-06-15

## Why this matters

The update producer accumulates one error string per failed enqueue into
`UpdateProducerRunReport.enqueue_errors`, a `Vec<String>` that is never bounded. The
producer runs for the whole process lifetime, so a sustained Redis/Dragonfly outage
(every enqueue failing) grows this vector without limit — a slow memory leak under
exactly the conditions where the system is already degraded, and a report that becomes
too large to be useful. Cap the vector and keep a count of how many errors were dropped,
so the report stays bounded while still signalling "many failures occurred".

Note: an audit pass referenced a sibling `dequeue_errors` field on a consumer report —
**that field does not exist in the current code** (verified). The only unbounded
error vector is `enqueue_errors` on the producer report; scope this plan to it. If you
find another unbounded `*_errors: Vec<...>` while working, report it (do not silently
expand scope).

## Current state

- `crates/openplotva-updates/src/lib.rs:593-603` — the report struct:

  ```rust
  #[derive(Clone, Debug, Default, Eq, PartialEq)]
  pub struct UpdateProducerRunReport {
      pub received: usize,
      pub enqueued: usize,
      pub skipped: usize,
      pub dropped_by_ingress_guard: usize,
      pub enqueue_errors: Vec<String>,
      /// Whether the source closed before shutdown was requested.
      pub source_closed: bool,
  }
  ```

- `crates/openplotva-updates/src/lib.rs:690-696` — the unbounded push, inside the
  producer loop:

  ```rust
  result = &mut queued => match result {
      Ok(()) => report.enqueued += 1,
      Err(error) => {
          let error = error.to_string();
          tracing::warn!(%error, "failed to enqueue Telegram update");
          report.enqueue_errors.push(error);
      }
  },
  ```

- Tests that constrain the field's behavior (must keep passing):
  - `lib.rs:3911` — `assert!(report.enqueue_errors.is_empty())`
  - `lib.rs:3917-3931` — `update_producer_continues_after_enqueue_errors_like_go` asserts
    `report.enqueue_errors == vec!["redis unavailable".to_owned()]` after one failure.

  A single recorded error must therefore still appear verbatim; only growth past the cap
  changes.

Convention: limits are `const` items (the file already uses constants for windows/limits).
Add the cap as a named `const`.

## Commands you will need

| Purpose          | Command                                  | Expected on success |
|------------------|------------------------------------------|---------------------|
| Compile          | `cargo check -p openplotva-updates`      | exit 0              |
| Tests            | `cargo test -p openplotva-updates`       | all pass            |
| Clippy           | `cargo clippy -p openplotva-updates --all-targets -- -D warnings` | exit 0 |
| Format           | `cargo fmt --all`                        | rewrites in place   |

## Scope

**In scope**:
- `crates/openplotva-updates/src/lib.rs` — the report struct, the push site, and a new
  unit test.

**Out of scope**:
- The enqueue logic / Redis client itself.
- Changing the `enqueue_errors` field type away from `Vec<String>` (keep the type so
  existing consumers/tests compile; just bound its growth and add a counter).

## Git workflow

- Branch: `advisor/005-bound-enqueue-errors`
- One commit. Message style: imperative, capitalized (e.g. "Bound producer enqueue-error
  accumulation").
- Do NOT push or open a PR unless instructed.

## Steps

### Step 1: Add a cap constant and a dropped-count field

Add a constant near the other limit constants:

```rust
/// Cap on retained enqueue-error strings per producer run so a sustained queue
/// outage cannot grow the report without bound.
const MAX_ENQUEUE_ERRORS: usize = 64;
```

Add a counter field to `UpdateProducerRunReport` (keeps `Default`/`Eq`/`PartialEq`):

```rust
    pub enqueue_errors: Vec<String>,
    /// Count of enqueue errors that occurred beyond `MAX_ENQUEUE_ERRORS` and were not
    /// retained in `enqueue_errors`.
    pub dropped_enqueue_errors: usize,
```

**Verify**: `cargo check -p openplotva-updates` compiles (existing tests may still pass
since the new field defaults to 0; confirm in Step 3).

### Step 2: Bound the push

Replace the push site (lib.rs:690-696) so it stops retaining strings past the cap but
still counts them:

```rust
      Err(error) => {
          let error = error.to_string();
          tracing::warn!(%error, "failed to enqueue Telegram update");
          if report.enqueue_errors.len() < MAX_ENQUEUE_ERRORS {
              report.enqueue_errors.push(error);
          } else {
              report.dropped_enqueue_errors += 1;
          }
      }
```

**Verify**: `cargo check -p openplotva-updates` exits 0.

### Step 3: Confirm existing tests still pass, then add a cap test

Run `cargo test -p openplotva-updates`. The two existing tests (`is_empty`, the
single-error `..._like_go` test) must still pass — with one error pushed, the cap is not
hit and `dropped_enqueue_errors` stays 0.

Add a test that drives more than `MAX_ENQUEUE_ERRORS` enqueue failures and asserts:
- `report.enqueue_errors.len() == MAX_ENQUEUE_ERRORS`
- `report.dropped_enqueue_errors == (total_failures - MAX_ENQUEUE_ERRORS)`

Model it after `update_producer_continues_after_enqueue_errors_like_go` (lib.rs:3917) —
reuse its failing-queue test double, just trigger more failures.

**Verify**: `cargo test -p openplotva-updates` passes including the new test.

### Step 4: Format, lint

```sh
cargo fmt --all
cargo clippy -p openplotva-updates --all-targets -- -D warnings
```

**Verify**: exit 0.

## Test plan

- New test: cap behavior (retained == cap, dropped counter == overflow). In the existing
  test module of `crates/openplotva-updates/src/lib.rs`.
- Existing tests `is_empty` and `..._like_go` unchanged and still pass.
- Verification: `cargo test -p openplotva-updates` → all pass.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `grep -c "MAX_ENQUEUE_ERRORS" crates/openplotva-updates/src/lib.rs` ≥ 2 (the const and its use at the push site).
- [ ] The push site is guarded by `report.enqueue_errors.len() < MAX_ENQUEUE_ERRORS`.
- [ ] A new test proves the vector stops at the cap and `dropped_enqueue_errors` counts the overflow.
- [ ] `cargo test -p openplotva-updates` passes; `cargo clippy -p openplotva-updates --all-targets -- -D warnings` exits 0.
- [ ] Only `crates/openplotva-updates/src/lib.rs` is modified (`git status`).
- [ ] `plans/README.md` status row updated.

## STOP conditions

Stop and report back if:

- The report struct or push site no longer matches the "Current state" excerpts.
- You find another unbounded `*_errors: Vec<...>` accumulated over the process lifetime
  (report it; it may warrant the same fix but is out of this plan's scope).

## Maintenance notes

- If a structured metrics sink is later added, prefer emitting an enqueue-failure
  counter there and shrinking `MAX_ENQUEUE_ERRORS` further; the cap exists only to keep
  the in-memory report bounded.
- Reviewer should confirm the single-error contract test still holds (one failure → one
  retained string, dropped counter 0).
