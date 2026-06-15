# Plan 002: Make the admin session cookie unforgeable (sign it)

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 9f32c4b..HEAD -- crates/openplotva-web/src/lib.rs crates/openplotva-app/src/lib.rs`
> If either file changed since this plan was written, compare the "Current
> state" excerpts against the live code before proceeding; on a mismatch,
> treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: none (but easier to verify once plan 001 has CI running tests)
- **Category**: security
- **Planned at**: commit `9f32c4b`, 2026-06-15

## Why this matters

The admin panel issues a session cookie whose value is the **raw Telegram user id with
no integrity protection**: `admin_session=7`. The Telegram Login HMAC is checked only
once, at `/admin/auth`; every subsequent request is authorized by reading the integer
out of the cookie and checking membership in the admin allow-list. Telegram user ids are
not secrets (they are visible to the bot in every message and configured in
`ADMINS_ADMIN_IDS`), so anyone who learns an admin's id can send
`Cookie: admin_session=<admin_id>` and gain full admin access without ever passing the
HMAC. This is a broken-authentication / forgeable-bearer-token issue on a
network-reachable admin surface.

The fix: sign the cookie value with an HMAC keyed by a server-side secret (the bot
token, which the request layer already holds), and reject any cookie whose signature
does not verify. This is inherited from the Go original (the cookie shape test asserts
Go parity), so it is a deliberate hardening of an existing contract: existing admin
sessions will be invalidated once and admins must re-login — that is expected and
acceptable.

## Current state

- `crates/openplotva-web/src/lib.rs:135-138` — builds the unsigned cookie:

  ```rust
  pub fn admin_session_cookie(user_id: i64) -> String {
      format!(
          "{ADMIN_SESSION_COOKIE_NAME}={user_id}; Path={ADMIN_SESSION_COOKIE_PATH}; Max-Age={ADMIN_SESSION_COOKIE_MAX_AGE_SECONDS}; HttpOnly; SameSite=Lax"
      )
  }
  ```

- `crates/openplotva-web/src/lib.rs:212-237` — a working HMAC-SHA256 helper already
  exists in this file (reuse it; do not add a new crypto dependency):

  ```rust
  fn hmac_sha256_hex(key: &[u8], message: &[u8]) -> String { /* standard HMAC-SHA256, returns lowercase hex */ }
  ```

- `crates/openplotva-app/src/lib.rs:3259-3270` — login handler sets the cookie after a
  successful Telegram HMAC + admin-list check:

  ```rust
  persist_admin_session_user(routes, &values, user_id).await;
  (
      StatusCode::FOUND,
      [
          (header::LOCATION, "/admin/".to_owned()),
          (header::SET_COOKIE, openplotva_web::admin_session_cookie(user_id)),
      ],
  ).into_response()
  ```

- `crates/openplotva-app/src/lib.rs:7060-7086` — the cookie is parsed and trusted as a
  bare integer (THE VULNERABILITY):

  ```rust
  fn admin_session_is_authorized(headers: &HeaderMap, admin_ids: &[i64]) -> bool {
      admin_session_user_ids(headers).into_iter().any(|user_id| admin_ids.contains(&user_id))
  }
  fn admin_session_user_id(headers: &HeaderMap) -> Option<i64> {
      admin_session_user_ids(headers).into_iter().next()
  }
  fn admin_session_user_ids(headers: &HeaderMap) -> Vec<i64> {
      let Some(cookie) = headers.get(header::COOKIE).and_then(|value| value.to_str().ok()) else {
          return Vec::new();
      };
      cookie
          .split(';')
          .filter_map(|part| part.trim()
              .strip_prefix(openplotva_web::ADMIN_SESSION_COOKIE_NAME)
              .and_then(|value| value.strip_prefix('=')))
          .filter_map(|value| value.parse::<i64>().ok())
          .collect()
  }
  ```

- `crates/openplotva-app/src/lib.rs:388-405` — the secret is available wherever the
  request handlers run; `StaticWebRoutes` holds `bot_token: Arc<str>`:

  ```rust
  struct StaticWebRoutes {
      admin_ids: Arc<[i64]>,
      bot_token: Arc<str>,
      // ...
      state_store: Option<PostgresVirtualMessageStore>,
  }
  ```

- The four readers of the cookie helpers (all called from handlers that take
  `Extension(routes): Extension<StaticWebRoutes>`, so `routes.bot_token` is in scope):
  - `admin_session_user_id(&headers)` at `lib.rs:862` (inside `admin_auth_check`)
  - `admin_session_is_authorized(headers, &routes.admin_ids)` at `lib.rs:1384`
  - `admin_session_user_id(headers)` at `lib.rs:5807`
  - `admin_session_user_id(headers)` at `lib.rs:6198`

Convention: cryptographic/signature primitives live in `openplotva-web` (per
`AGENTS.md`: "openplotva-web owns ... WebApp signatures, login/auth helpers, and
cookie/signature primitives"). Put the new sign/verify functions there and reuse the
existing `hmac_sha256_hex`. Tests in that crate assert exact string shapes (e.g.
`admin_session_cookie_matches_go_cookie_shape` at `web/src/lib.rs:381`); you will update
that test to the new signed shape.

## Commands you will need

| Purpose         | Command                                              | Expected on success |
|-----------------|------------------------------------------------------|---------------------|
| Web crate tests | `cargo test -p openplotva-web`                       | all pass            |
| App crate tests | `cargo test -p openplotva-app`                       | all pass            |
| Clippy          | `cargo clippy --workspace --all-targets --all-features -- -D warnings` | exit 0 |
| Format          | `cargo fmt --all`                                    | rewrites in place   |
| Full fast gate  | `tools/rust-fast-gate.sh`                            | exit 0              |

## Scope

**In scope** (the only files you should modify):
- `crates/openplotva-web/src/lib.rs` (add sign/verify + a constant-time compare; update the cookie shape test)
- `crates/openplotva-app/src/lib.rs` (thread the secret into the cookie issue + parse; update affected tests)

**Out of scope** (do NOT touch):
- The Telegram Login HMAC path (`validate_telegram_auth`, `admin_auth_response` HMAC
  check) — that is correct and is hardened separately in plans 006 and 010.
- `web/admin/*` static assets and the login page — the cookie is set/read server-side;
  the browser only echoes it back.
- Adding any new crate dependency (`subtle`, `hmac`, etc.) — reuse the existing
  `hmac_sha256_hex`.

## Git workflow

- Branch: `advisor/002-sign-admin-cookie`
- Commit per logical unit (web crate, then app crate). Message style: imperative,
  capitalized (e.g. "Sign the admin session cookie with an HMAC").
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Confirm all four cookie-reader call sites can see the secret

Read `lib.rs` around lines 855–875, 1380–1390, 5800–5810, 6190–6200. Confirm each
function that calls `admin_session_user_id`/`admin_session_is_authorized` has either
`routes: StaticWebRoutes` (or `&StaticWebRoutes`) in scope, or already receives the
bot token.

**Verify**: you can name, for each of the four call sites, the in-scope expression that
yields the bot token (expected: `routes.bot_token` / `&routes.bot_token`).

**STOP** if any call site has no access to the secret and would require threading
`routes` through more than one additional function signature — report the call site and
stop, so the scope can be re-evaluated.

### Step 2: Add sign + verify + constant-time compare to `openplotva-web`

In `crates/openplotva-web/src/lib.rs` add:

```rust
/// Constant-time byte-slice equality. Returns false immediately on length mismatch,
/// then compares every remaining byte without early exit.
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

/// HMAC tag (hex) binding an admin user id to the server secret.
#[must_use]
pub fn admin_session_signature(user_id: i64, secret: &str) -> String {
    hmac_sha256_hex(secret.as_bytes(), user_id.to_string().as_bytes())
}

/// Verify a signed admin-session cookie value of the form `<user_id>.<hex_sig>`.
/// Returns the user id only when the signature verifies. Rejects unsigned legacy
/// values (no `.`), tamper, and malformed input.
#[must_use]
pub fn verify_admin_session_value(value: &str, secret: &str) -> Option<i64> {
    let (user_part, sig) = value.split_once('.')?;
    let user_id = user_part.parse::<i64>().ok()?;
    let expected = admin_session_signature(user_id, secret);
    if constant_time_eq(expected.as_bytes(), sig.as_bytes()) {
        Some(user_id)
    } else {
        None
    }
}
```

Then change `admin_session_cookie` to embed the signature in the value:

```rust
#[must_use]
pub fn admin_session_cookie(user_id: i64, secret: &str) -> String {
    let sig = admin_session_signature(user_id, secret);
    format!(
        "{ADMIN_SESSION_COOKIE_NAME}={user_id}.{sig}; Path={ADMIN_SESSION_COOKIE_PATH}; Max-Age={ADMIN_SESSION_COOKIE_MAX_AGE_SECONDS}; HttpOnly; SameSite=Lax"
    )
}
```

(If `constant_time_eq` already exists in this file because plan 006 landed first, do
not add a second copy — reuse it.)

**Verify**: `cargo check -p openplotva-web` compiles (the app crate will not yet — that
is expected until Step 4).

### Step 3: Update the web crate cookie-shape test

The existing test `admin_session_cookie_matches_go_cookie_shape` (`web/src/lib.rs:381`)
asserts the old unsigned shape and now fails to compile (signature changed). Update it
to call the new two-arg form and assert the signed shape, and add a round-trip test:

```rust
#[test]
fn admin_session_cookie_is_signed_and_round_trips() {
    let secret = "123:ABC";
    let cookie = admin_session_cookie(7, secret);
    // value is "7.<64 hex chars>"
    let value = cookie.split(';').next().expect("cookie pair")
        .strip_prefix("admin_session=").expect("name prefix");
    assert_eq!(verify_admin_session_value(value, secret), Some(7));
    // tamper / wrong secret / legacy unsigned are rejected
    assert_eq!(verify_admin_session_value("7", secret), None);
    assert_eq!(verify_admin_session_value(value, "wrong"), None);
    assert!(cookie.contains("HttpOnly; SameSite=Lax"));
}
```

**Verify**: `cargo test -p openplotva-web` passes.

### Step 4: Thread the secret through the app-side issue and parse

In `crates/openplotva-app/src/lib.rs`:

1. Cookie issue at `lib.rs:3266`: change to
   `openplotva_web::admin_session_cookie(user_id, &routes.bot_token)`.

2. Parse helpers (`lib.rs:7060-7086`): add a `secret: &str` parameter and verify each
   cookie value instead of `parse::<i64>()`:

   ```rust
   fn admin_session_is_authorized(headers: &HeaderMap, admin_ids: &[i64], secret: &str) -> bool {
       admin_session_user_ids(headers, secret).into_iter().any(|user_id| admin_ids.contains(&user_id))
   }
   fn admin_session_user_id(headers: &HeaderMap, secret: &str) -> Option<i64> {
       admin_session_user_ids(headers, secret).into_iter().next()
   }
   fn admin_session_user_ids(headers: &HeaderMap, secret: &str) -> Vec<i64> {
       let Some(cookie) = headers.get(header::COOKIE).and_then(|value| value.to_str().ok()) else {
           return Vec::new();
       };
       cookie
           .split(';')
           .filter_map(|part| part.trim()
               .strip_prefix(openplotva_web::ADMIN_SESSION_COOKIE_NAME)
               .and_then(|value| value.strip_prefix('=')))
           .filter_map(|value| openplotva_web::verify_admin_session_value(value, secret))
           .collect()
   }
   ```

3. Update the four call sites (Step 1) to pass the secret:
   - `lib.rs:862`: `admin_session_user_id(&headers, &routes.bot_token)`
   - `lib.rs:1384`: `admin_session_is_authorized(headers, &routes.admin_ids, &routes.bot_token)`
   - `lib.rs:5807` and `lib.rs:6198`: `admin_session_user_id(headers, &routes.bot_token)`

**Verify**: `cargo check -p openplotva-app` compiles.

### Step 5: Fix the app-side tests that construct the cookie by hand

Several app tests insert a literal `admin_session=7` cookie (e.g. `lib.rs:11105`,
`:11192`, `:11235`, `:11290`, `:11380`, `:11559`, `:11702`, `:11777`, `:11922`,
`:11954`) and call the now-three-arg helpers. For each, build the cookie with the same
secret the assertion uses, e.g.:

```rust
let secret = "test-bot-token";
let signed = openplotva_web::admin_session_cookie(7, secret); // "admin_session=7.<sig>; ..."
let value = signed.split(';').next().unwrap(); // "admin_session=7.<sig>"
headers.insert(header::COOKIE, value.parse()?);
assert!(admin_session_is_authorized(&headers, &[7, 9], secret));
```

Do NOT weaken any assertion's intent — a test that expected `admin_session=7` to be
authorized must now sign with the test secret and still expect authorization; a test
that expected rejection must still reject (e.g. a tampered or wrong-secret value).

**Verify**: `cargo test -p openplotva-app` passes.

### Step 6: Format, lint, full gate

```sh
cargo fmt --all
tools/rust-fast-gate.sh
```

**Verify**: exit 0.

## Test plan

- New web-crate test: `admin_session_cookie_is_signed_and_round_trips` (Step 3) —
  covers: signed value verifies, legacy unsigned value rejected, wrong-secret rejected,
  cookie attributes preserved. Model after the existing `admin_session_cookie_*` test.
- Add one app-crate test asserting a **forged** unsigned cookie is rejected:
  `headers.insert(COOKIE, "admin_session=7".parse()?)` →
  `assert!(!admin_session_is_authorized(&headers, &[7], "secret"))`. Model after the
  existing `admin_session_is_authorized` test near `lib.rs:11104`.
- Updated app tests (Step 5) all pass with signed cookies.
- Verification: `cargo test -p openplotva-web && cargo test -p openplotva-app` → all
  pass including the new tests.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `tools/rust-fast-gate.sh` exits 0.
- [ ] `grep -n "verify_admin_session_value" crates/openplotva-app/src/lib.rs` shows it used in `admin_session_user_ids`.
- [ ] No reader parses the cookie value directly as an integer: `admin_session_user_ids` no longer contains `.parse::<i64>()` on the cookie value (it calls `verify_admin_session_value`).
- [ ] A test exists proving an unsigned/forged `admin_session=<id>` cookie is NOT authorized, and it passes.
- [ ] `cargo test -p openplotva-web` and `cargo test -p openplotva-app` pass.
- [ ] Only the two in-scope files are modified (`git status`).
- [ ] `plans/README.md` status row updated.

## STOP conditions

Stop and report back (do not improvise) if:

- Step 1 finds a cookie reader with no reachable secret (would require a multi-hop
  refactor).
- A reader of the cookie exists outside `openplotva-app` (e.g. in `openplotva-server`)
  that also trusts the bare integer — report it; the fix must cover all readers.
- The bot token can be empty in some deployments AND admin login is still expected to
  work in that mode — if so, signing with an empty secret is unsafe; STOP and report so
  a dedicated `ADMIN_SESSION_SECRET` can be designed instead.
- Any "Current state" excerpt no longer matches the live code.

## Maintenance notes

- This invalidates all existing admin sessions once (cookie format changed); admins
  re-login via `/admin/auth`. Mention this in the PR description.
- The signing secret is the bot token; if the bot token rotates, existing admin cookies
  stop verifying (sessions drop) — acceptable, and arguably desirable.
- Reviewer should confirm: (a) the legacy unsigned format is rejected (not merely
  un-preferred), (b) the comparison is the constant-time helper, (c) no reader path was
  missed.
- Follow-up considered and deferred: a `Secure` cookie attribute and a dedicated
  `ADMIN_SESSION_SECRET` (instead of reusing the bot token) — both reasonable, neither
  required to close the forgery hole, which is what this plan does.
