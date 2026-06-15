# Plan 007: Reject stale Telegram Login data on admin authentication

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 9f32c4b..HEAD -- crates/openplotva-app/src/lib.rs`
> Plan 002 also edits `admin_auth_response`; if 002 has landed, the line numbers
> below will have shifted — locate `admin_auth_response` by name and compare the
> logic, not the line numbers. On a structural mismatch, treat it as a STOP
> condition.

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: LOW
- **Depends on**: plans/002-sign-admin-session-cookie.md (same function `admin_auth_response`; run after 002 to avoid a merge conflict)
- **Category**: security
- **Planned at**: commit `9f32c4b`, 2026-06-15

## Why this matters

The admin login handler validates the Telegram Login Widget HMAC but never checks the
`auth_date` field inside that signed payload. Telegram's documentation recommends
verifying `auth_date` freshness precisely because a captured login URL (which contains a
permanently-valid HMAC over fixed fields) can otherwise be replayed indefinitely to mint
a new admin session. Adding a freshness window closes that replay vector. This complements
plan 002 (which stops cookie forgery): 002 makes the session cookie unforgeable, and this
plan stops an old login link from minting a fresh valid cookie.

## Current state

- `crates/openplotva-app/src/lib.rs:3223-3271` — `admin_auth_response`. It parses the
  query, checks the HMAC, checks the admin allow-list, then issues the cookie. There is
  no `auth_date` check:

  ```rust
  let pairs = values.iter().map(|(key, value)| (key.as_str(), value.as_str()));
  if !openplotva_web::validate_telegram_auth(pairs, &routes.bot_token, hash) {
      tracing::error!("invalid admin auth signature");
      return admin_error_response(StatusCode::FORBIDDEN, "invalid auth");
  }

  if !routes.admin_ids.contains(&user_id) {
      tracing::error!(user_id, "authenticated Telegram user is not an admin");
      return admin_error_response(StatusCode::FORBIDDEN, "forbidden");
  }

  persist_admin_session_user(routes, &values, user_id).await;
  // ... set cookie ...
  ```

- `values` is a `BTreeMap<String, String>` from `admin_auth_query_values` (lib.rs:3273);
  the Telegram Login payload includes an `auth_date` field as a unix-seconds string (see
  the web-crate test data `("auth_date", "1700000000")` at `web/src/lib.rs:365`).

- Time handling convention: the file already uses `time::OffsetDateTime`
  (imported at `lib.rs:91`: `use time::{OffsetDateTime, ...}`), with
  `OffsetDateTime::now_utc()` used throughout (e.g. `lib.rs:2743`, `:2751`).

## Commands you will need

| Purpose       | Command                            | Expected on success |
|---------------|------------------------------------|---------------------|
| App tests     | `cargo test -p openplotva-app`     | all pass            |
| Clippy        | `cargo clippy -p openplotva-app --all-targets -- -D warnings` | exit 0 |
| Format        | `cargo fmt --all`                  | rewrites in place   |

## Scope

**In scope**:
- `crates/openplotva-app/src/lib.rs` — `admin_auth_response` (and a small helper +
  test). Prefer extracting the freshness logic into a pure, unit-testable helper
  function so it can be tested without an HTTP handler.

**Out of scope**:
- `validate_telegram_auth` (plan 006) and the cookie signing (plan 002).
- The WebApp settings signature path.
- Any change to what fields Telegram sends.

## Git workflow

- Branch: `advisor/007-admin-auth-date`
- One commit. Message style: imperative, capitalized (e.g. "Reject stale Telegram login
  data on admin auth").
- Do NOT push or open a PR unless instructed.

## Steps

### Step 1: Add a pure freshness-check helper

Add a constant and a testable helper near `admin_auth_response`:

```rust
/// Maximum age of a Telegram Login `auth_date` accepted for admin authentication.
const ADMIN_AUTH_MAX_AGE_SECONDS: i64 = 86_400; // 24h; tighten later if desired
/// Small tolerance for clock skew on `auth_date` values slightly in the future.
const ADMIN_AUTH_FUTURE_SKEW_SECONDS: i64 = 60;

/// Returns true if `auth_date` (unix seconds) is within the accepted freshness window
/// relative to `now`. Missing/unparseable dates and far-future dates are rejected.
fn admin_auth_date_is_fresh(values: &BTreeMap<String, String>, now: OffsetDateTime) -> bool {
    let Some(auth_date) = values.get("auth_date").and_then(|v| v.trim().parse::<i64>().ok()) else {
        return false;
    };
    let now_unix = now.unix_timestamp();
    let age = now_unix - auth_date;
    age >= -ADMIN_AUTH_FUTURE_SKEW_SECONDS && age <= ADMIN_AUTH_MAX_AGE_SECONDS
}
```

**Verify**: `cargo check -p openplotva-app` compiles (helper unused until Step 2 — a
temporary warning is fine, gone after Step 2).

### Step 2: Call the helper after the HMAC check passes

In `admin_auth_response`, immediately after the `validate_telegram_auth` success (before
the admin-list check), add:

```rust
  if !admin_auth_date_is_fresh(&values, OffsetDateTime::now_utc()) {
      tracing::error!("admin auth rejected: stale or missing auth_date");
      return admin_error_response(StatusCode::FORBIDDEN, "auth expired");
  }
```

**Verify**: `cargo check -p openplotva-app` exits 0 with no unused-function warning.

### Step 3: Add unit tests for the helper

Test the pure helper directly (no HTTP needed):

```rust
#[test]
fn admin_auth_date_freshness_window() {
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("ts");
    let mut v = BTreeMap::new();
    // fresh (just now)
    v.insert("auth_date".to_owned(), "1700000000".to_owned());
    assert!(admin_auth_date_is_fresh(&v, now));
    // 23h ago: fresh
    v.insert("auth_date".to_owned(), (1_700_000_000 - 23 * 3600).to_string());
    assert!(admin_auth_date_is_fresh(&v, now));
    // 25h ago: stale
    v.insert("auth_date".to_owned(), (1_700_000_000 - 25 * 3600).to_string());
    assert!(!admin_auth_date_is_fresh(&v, now));
    // far future: rejected
    v.insert("auth_date".to_owned(), (1_700_000_000 + 3600).to_string());
    assert!(!admin_auth_date_is_fresh(&v, now));
    // missing: rejected
    v.remove("auth_date");
    assert!(!admin_auth_date_is_fresh(&v, now));
}
```

**Verify**: `cargo test -p openplotva-app` passes including this test.

### Step 4: Format, lint

```sh
cargo fmt --all
cargo clippy -p openplotva-app --all-targets -- -D warnings
```

**Verify**: exit 0.

## Test plan

- New unit test on `admin_auth_date_is_fresh`: fresh, near-edge fresh, stale, future,
  missing. In the existing app test module.
- Existing admin-auth tests must still pass; if any existing test exercises the full
  `admin_auth_response` with a hardcoded old `auth_date` (e.g. `1700000000`) and expects
  success, it will now fail freshness. If such a test exists, update it to pass a
  `now`-relative `auth_date` (or note it and STOP if it cannot be made deterministic).
- Verification: `cargo test -p openplotva-app` → all pass.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `admin_auth_response` calls `admin_auth_date_is_fresh` after the HMAC check and returns FORBIDDEN on failure.
- [ ] `grep -c "auth_date" crates/openplotva-app/src/lib.rs` shows the field read in the new helper.
- [ ] The freshness helper unit test exists and passes.
- [ ] `cargo test -p openplotva-app` passes; `cargo clippy -p openplotva-app --all-targets -- -D warnings` exits 0.
- [ ] Only `crates/openplotva-app/src/lib.rs` is modified (`git status`).
- [ ] `plans/README.md` status row updated.

## STOP conditions

Stop and report back if:

- An existing test drives `admin_auth_response` with a fixed past `auth_date` and cannot
  be made `now`-relative without broader changes.
- `admin_auth_response` has been restructured so the HMAC check is no longer the clear
  insertion point.
- Telegram Login data in this codebase turns out NOT to carry `auth_date` (verify via
  the existing tests/handlers) — then this check would reject all logins; STOP.

## Maintenance notes

- 24h is a conservative default chosen to not surprise existing admins. Once confirmed
  in production, consider tightening to 1h.
- Reviewer should confirm the window also rejects missing and far-future `auth_date`,
  and that the helper is pure (testable without HTTP/time mocking beyond passing `now`).
- This shares `admin_auth_response` with plan 002 — confirm both changes coexist (002
  changes the cookie issuance line; this adds a guard earlier in the same function).
