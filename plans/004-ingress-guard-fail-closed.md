# Plan 004: Ingress flood guard recovers from a poisoned lock instead of failing open

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 9f32c4b..HEAD -- crates/openplotva-updates/src/lib.rs`
> If that file changed, compare the "Current state" excerpt against the live
> code before proceeding; on a mismatch, treat it as a STOP condition.

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: bug
- **Planned at**: commit `9f32c4b`, 2026-06-15

## Why this matters

The Telegram update ingress flood guard holds per-chat rate state behind a `Mutex`. If
that mutex is ever poisoned (a thread panicked while holding it), the guard currently
returns `Allowed` — i.e. it **fails open and disables flood protection** for the rest
of the process lifetime. Everywhere else in this same file the code recovers from a
poisoned lock with `.unwrap_or_else(|poisoned| poisoned.into_inner())`, which keeps the
guarded state usable. This one site diverges from that convention. The fix is a
one-block change to match the rest of the file: recover the inner state and keep
enforcing limits.

## Current state

- `crates/openplotva-updates/src/lib.rs:1073-1083` — the divergent fail-open path inside
  `check_update_at`:

  ```rust
  pub fn check_update_at(
      &self,
      update: &TelegramUpdate,
      now: SystemTime,
  ) -> UpdateIngressDecision {
      let Some(chat_id) = update_chat_id(update).filter(|chat_id| *chat_id != 0) else {
          return UpdateIngressDecision::Allowed { chat_id: None };
      };
      let Ok(mut chats) = self.chats.lock() else {
          return UpdateIngressDecision::Allowed {
              chat_id: Some(chat_id),
          };
      };
      let state = chats.entry(chat_id).or_default();
      // ... flood/block logic ...
  }
  ```

- The established convention in the same file (recover, don't fail open) — e.g.
  `lib.rs:6342-6343` and `:6358-6359`:

  ```rust
  .lock()
  .unwrap_or_else(|poisoned| poisoned.into_inner())
  ```

`self.chats` is a `Mutex<HashMap<i64, ChatIngressState>>` (the guarded state). Recovering
with `into_inner()` is safe here: a poisoned map is still a valid map; continuing to
apply flood limits to it is strictly better than allowing everything.

## Commands you will need

| Purpose          | Command                                  | Expected on success |
|------------------|------------------------------------------|---------------------|
| Compile          | `cargo check -p openplotva-updates`      | exit 0              |
| Tests            | `cargo test -p openplotva-updates`       | all pass            |
| Clippy           | `cargo clippy -p openplotva-updates --all-targets -- -D warnings` | exit 0 |
| Format           | `cargo fmt --all`                        | rewrites in place   |

## Scope

**In scope**:
- `crates/openplotva-updates/src/lib.rs` — only the lock acquisition in
  `check_update_at` (and a new unit test).

**Out of scope**:
- The flood/block algorithm itself (limits, windows, block duration) — unchanged.
- Any other `.lock()` site in the file — they already use the correct pattern.

## Git workflow

- Branch: `advisor/004-ingress-fail-closed`
- One commit. Message style: imperative, capitalized (e.g. "Recover ingress flood guard
  from a poisoned lock").
- Do NOT push or open a PR unless instructed.

## Steps

### Step 1: Replace the fail-open lock with the recover-and-continue pattern

In `check_update_at`, replace:

```rust
    let Ok(mut chats) = self.chats.lock() else {
        return UpdateIngressDecision::Allowed {
            chat_id: Some(chat_id),
        };
    };
```

with the same recovery pattern used elsewhere in the file:

```rust
    let mut chats = self
        .chats
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
```

**Verify**: `cargo check -p openplotva-updates` exits 0; `grep -n "into_inner" crates/openplotva-updates/src/lib.rs` now includes a hit inside `check_update_at`.

### Step 2: Add a regression test

Add a unit test (in the existing `#[cfg(test)] mod tests` of this file) that poisons the
guard's mutex and asserts the guard still enforces limits (does not return `Allowed`
unconditionally). Poison the lock by panicking inside a `catch_unwind` while holding it,
then drive enough updates to trip the flood limit and assert a `DroppedFlood` (or
`DroppedBlocked`) decision is produced.

Model the test harness (constructing the guard, building a `TelegramUpdate` with a
chat id, advancing `now`) after the existing ingress-guard tests in this file (search
for `check_update_at` usages in the test module to copy the setup).

If poisoning the mutex cleanly is impractical in a unit test, instead add a test that
simply exercises `check_update_at` past the flood threshold and asserts the drop
decision (a guard against the limits regressing), and note in a comment that the
poison-recovery path mirrors the file-wide `into_inner` convention.

**Verify**: `cargo test -p openplotva-updates` passes, including the new test.

### Step 3: Format, lint

```sh
cargo fmt --all
cargo clippy -p openplotva-updates --all-targets -- -D warnings
```

**Verify**: exit 0.

## Test plan

- New test: poisoned-lock (or flood-threshold) behavior asserts the guard does NOT
  fail open. Lives in the existing test module of `crates/openplotva-updates/src/lib.rs`.
- Verification: `cargo test -p openplotva-updates` → all pass including the new test.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `check_update_at` no longer contains an early `return UpdateIngressDecision::Allowed` on a lock error (it uses `unwrap_or_else(|poisoned| poisoned.into_inner())`).
- [ ] A new test in the updates crate covers the no-fail-open behavior and passes.
- [ ] `cargo clippy -p openplotva-updates --all-targets -- -D warnings` exits 0.
- [ ] `cargo test -p openplotva-updates` passes.
- [ ] Only `crates/openplotva-updates/src/lib.rs` is modified (`git status`).
- [ ] `plans/README.md` status row updated.

## STOP conditions

Stop and report back if:

- `check_update_at` no longer matches the "Current state" excerpt.
- `self.chats` is not a `Mutex` (e.g. it was changed to `RwLock`/`parking_lot`) — the
  recovery idiom differs; report and stop.

## Maintenance notes

- This aligns the one outlier with the file's established poison-recovery convention; a
  reviewer should confirm no other `let Ok(...) = ...lock() else { return Allowed }`
  pattern remains in the crate.
- A deeper follow-up (out of scope) is to eliminate the panic sources that could poison
  the lock in the first place, but recovery is the correct default regardless.
