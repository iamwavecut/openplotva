# Plan 006: Compare the Telegram auth HMAC in constant time

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 9f32c4b..HEAD -- crates/openplotva-web/src/lib.rs`
> On any change, compare the "Current state" excerpt to live code; mismatch is a
> STOP condition.

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none (shares a `constant_time_eq` helper with plan 002; either may add it)
- **Category**: security
- **Planned at**: commit `9f32c4b`, 2026-06-15

## Why this matters

`validate_telegram_auth` verifies the Telegram Login Widget / WebApp HMAC, which is
keyed by the bot token (a real secret). It compares the computed tag to the
attacker-supplied value with `eq_ignore_ascii_case`, which short-circuits on the first
differing byte. A short-circuiting comparison of a secret-derived MAC against
attacker-controlled input is a classic timing-oracle: in principle it leaks how many
leading bytes matched, enabling byte-by-byte forgery of a valid hash without the secret.
The standard mitigation (used by Telegram client libraries and `hmac.compare_digest`-style
APIs) is a constant-time comparison. This is a contract-preserving hardening: the same
valid hashes still validate (including the uppercase-hex form the tests rely on for Go
parity); only the comparison's timing profile changes.

## Current state

- `crates/openplotva-web/src/lib.rs:126-132` — the non-constant-time comparison:

  ```rust
  #[must_use]
  pub fn validate_telegram_auth<'src, I>(pairs: I, bot_token: &str, provided_hash: &str) -> bool
  where
      I: IntoIterator<Item = (&'src str, &'src str)>,
  {
      telegram_auth_hash(pairs, bot_token).eq_ignore_ascii_case(provided_hash)
  }
  ```

- `telegram_auth_hash` (lib.rs:110-124) returns **lowercase** hex (via `hmac_sha256_hex`
  → `hex::encode`).

- The contract test that must keep passing (lib.rs:360-379) feeds an **uppercase**
  `provided_hash` and expects acceptance, and a wrong bot token and expects rejection:

  ```rust
  let hash = telegram_auth_hash(pairs, "123:ABC"); // lowercase
  assert!(validate_telegram_auth(pairs, "123:ABC",
      "C340B883EB8C3556A6F1C1B9086B792FFDD782F369B68777CA404488F89FCFEC")); // uppercase, must accept
  assert!(!validate_telegram_auth(pairs, "wrong", &hash)); // must reject
  ```

So case-insensitivity must be preserved (lowercase both sides), then compared in
constant time.

## Commands you will need

| Purpose       | Command                            | Expected on success |
|---------------|------------------------------------|---------------------|
| Web tests     | `cargo test -p openplotva-web`     | all pass            |
| Clippy        | `cargo clippy -p openplotva-web --all-targets -- -D warnings` | exit 0 |
| Format        | `cargo fmt --all`                  | rewrites in place   |

## Scope

**In scope**:
- `crates/openplotva-web/src/lib.rs` — `validate_telegram_auth`, the shared
  `constant_time_eq` helper (add if absent), and a new test.

**Out of scope**:
- `telegram_auth_hash` / `hmac_sha256_hex` (correct; leave them).
- The unkeyed settings signature (`validate_settings_access_signature`) — a constant-time
  compare does NOT help there because the algorithm is public/unkeyed; that surface is
  addressed by plan 009, not here. Do not change it.
- Adding a crypto dependency (`subtle`, etc.) — use the small helper below.

## Git workflow

- Branch: `advisor/006-ct-telegram-hmac`
- One commit. Message style: imperative, capitalized (e.g. "Compare Telegram auth HMAC
  in constant time").
- Do NOT push or open a PR unless instructed.

## Steps

### Step 1: Ensure a constant-time compare helper exists

If `constant_time_eq` does NOT already exist in `crates/openplotva-web/src/lib.rs` (it
may, if plan 002 landed first), add it:

```rust
/// Constant-time byte-slice equality. Returns false on length mismatch, then compares
/// every remaining byte without early exit.
#[must_use]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
```

If it already exists, do not add a duplicate.

**Verify**: `grep -c "fn constant_time_eq" crates/openplotva-web/src/lib.rs` returns `1`.

### Step 2: Use it in `validate_telegram_auth` (preserving case-insensitivity)

```rust
#[must_use]
pub fn validate_telegram_auth<'src, I>(pairs: I, bot_token: &str, provided_hash: &str) -> bool
where
    I: IntoIterator<Item = (&'src str, &'src str)>,
{
    let expected = telegram_auth_hash(pairs, bot_token); // lowercase hex
    let provided = provided_hash.to_ascii_lowercase();
    constant_time_eq(expected.as_bytes(), provided.as_bytes())
}
```

**Verify**: `cargo test -p openplotva-web` passes — in particular the existing
`telegram_admin_auth_hash_matches_go_login_widget_hmac` test (uppercase accepted, wrong
token rejected) still passes.

### Step 3: Add a focused test

Add a test asserting equivalence with the old behavior plus the constant-time path:
lowercase-accept, uppercase-accept, single-bit-flip-reject, length-mismatch-reject.

```rust
#[test]
fn validate_telegram_auth_is_case_insensitive_and_constant_time() {
    let pairs = [("id", "7"), ("auth_date", "1700000000")];
    let hash = telegram_auth_hash(pairs, "123:ABC");
    assert!(validate_telegram_auth(pairs, "123:ABC", &hash));               // lowercase
    assert!(validate_telegram_auth(pairs, "123:ABC", &hash.to_uppercase())); // uppercase
    let mut tampered = hash.clone();
    tampered.replace_range(0..1, if &hash[0..1] == "0" { "1" } else { "0" });
    assert!(!validate_telegram_auth(pairs, "123:ABC", &tampered));           // one char off
    assert!(!validate_telegram_auth(pairs, "123:ABC", "deadbeef"));          // length mismatch
}
```

**Verify**: `cargo test -p openplotva-web` passes including the new test.

### Step 4: Format, lint

```sh
cargo fmt --all
cargo clippy -p openplotva-web --all-targets -- -D warnings
```

**Verify**: exit 0.

## Test plan

- New test (Step 3) covers case-insensitive accept, tamper reject, length-mismatch reject.
- Existing Go-parity HMAC test continues to pass unchanged.
- Verification: `cargo test -p openplotva-web` → all pass.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `validate_telegram_auth` no longer calls `eq_ignore_ascii_case` (`grep -c "eq_ignore_ascii_case" crates/openplotva-web/src/lib.rs` returns `0`).
- [ ] `validate_telegram_auth` calls `constant_time_eq`.
- [ ] Exactly one `fn constant_time_eq` exists in the file.
- [ ] `cargo test -p openplotva-web` passes including the new test and the existing Go-parity test.
- [ ] `cargo clippy -p openplotva-web --all-targets -- -D warnings` exits 0.
- [ ] Only `crates/openplotva-web/src/lib.rs` is modified (`git status`).
- [ ] `plans/README.md` status row updated.

## STOP conditions

Stop and report back if:

- `validate_telegram_auth` or `telegram_auth_hash` no longer matches the "Current state"
  excerpt (e.g. the hash is no longer lowercase hex — then lowercasing both sides may be
  wrong; report and stop).
- The existing Go-parity test would have to change its expected hash to pass — that means
  behavior, not just timing, changed; STOP (the comparison must accept exactly the same
  inputs as before).

## Maintenance notes

- If plan 002 and this plan both run, they share the one `constant_time_eq` helper —
  ensure only one definition exists.
- Reviewer should confirm the change is purely the comparison (same accept/reject set),
  and that the unkeyed settings signature was NOT touched (that is plan 009).
