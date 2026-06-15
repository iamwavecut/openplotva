# Plan 010: Authenticate the settings WebApp with Telegram initData

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving on. If a
> STOP condition occurs, stop and report. Read the companion design doc
> `docs/settings-webapp-auth-design.md` in full before starting — it carries the
> rationale, the exact key-derivation, and the open questions this plan resolves.
>
> **Drift check (run first)**: `git diff --stat 9f32c4b..HEAD -- crates/openplotva-web/src/lib.rs crates/openplotva-app/src/lib.rs web/settings/`
> Plan 002 and 006 also edit `openplotva-web/src/lib.rs` (cookie signing,
> `constant_time_eq`); if they have landed, the helper already exists — reuse it,
> don't duplicate. On a structural mismatch with the excerpts below, STOP.

## Status

- **Priority**: P1
- **Effort**: L
- **Risk**: MED-HIGH (touches the WebApp auth contract and the front-end)
- **Depends on**: plans/006 (reuses `constant_time_eq`; if 006 hasn't landed, this plan adds the helper). plans/009 produced the design doc this plan implements.
- **Category**: security
- **Planned at**: commit `9f32c4b`, 2026-06-15

## Why this matters

The settings WebApp endpoints (read AND write of chat/user settings, deputies, memory) are
authorized only by an **unkeyed signature derived from a public salt** — anyone can compute it
offline and act on any chat/user (full detail + the six gate sites + parity analysis in
`docs/settings-webapp-auth-design.md`). This plan adds Telegram **WebApp initData** HMAC
validation (keyed by the bot token, unforgeable) as the authority for caller identity, keeping
the existing signature only as a routing hint. This closes the broken access control without
breaking distributed WebApp URLs.

## Current state

Read `docs/settings-webapp-auth-design.md` for the complete picture. Key facts:

- `crates/openplotva-web/src/lib.rs:141-153` — `settings_signature` (unkeyed, public salt
  `plotva-signature-salt-2024`) and `validate_settings_access_signature`.
- `crates/openplotva-web/src/lib.rs:110-132` — `telegram_auth_hash` / `validate_telegram_auth`
  (Login Widget HMAC: `secret = SHA256(bot_token)`). **WebApp initData uses a DIFFERENT key
  derivation** — see Step 1.
- `crates/openplotva-web/src/lib.rs:212-237` — `hmac_sha256_hex(key, msg)` helper (reuse it).
- Six gate sites in `crates/openplotva-app/src/lib.rs`: `1648`, `1889`, `1935`, `1983`, `2251`,
  `3047` — each calls `validate_settings_access_signature` and trusts client-supplied IDs.
- `StaticWebRoutes` (`app/src/lib.rs:388-405`) holds `bot_token: Arc<str>` — already available to
  the handlers.
- Front-end `web/settings/index.js:1539-1582` (`initTelegram`) reads `tg.initDataUnsafe.user.id`
  but never `tg.initData`; API helpers `:406-490`, `:1430-1467` send only signature/chat_id/user_id.

## Commands you will need

| Purpose            | Command                                              | Expected |
|--------------------|------------------------------------------------------|----------|
| Web crate tests    | `cargo test -p openplotva-web`                       | new initData tests pass |
| App crate tests    | `cargo test -p openplotva-app`                       | settings auth tests pass |
| Clippy             | `cargo clippy --workspace --all-targets --all-features -- -D warnings` | exit 0 |
| Format             | `cargo fmt --all`                                    | rewrites |
| Full gate          | `tools/rust-fast-gate.sh`                            | exit 0 (modulo any unrelated pre-existing failures — see STOP) |

## Scope

**In scope**:
- `crates/openplotva-web/src/lib.rs` — `validate_webapp_init_data`, `TelegramWebAppUser`, tests.
- `crates/openplotva-app/src/lib.rs` — thread `bot_token` into the six gates, add the initData
  check + the soft-cutover config flag, tests.
- `crates/openplotva-config/src/lib.rs` — ONLY if a config flag for the soft cutover is added
  (a single boolean, e.g. `SETTINGS_REQUIRE_INIT_DATA`).
- `web/settings/index.js` (and `landing.html` if needed) — capture and send `initData`.

**Out of scope**:
- Removing or changing `settings_signature` / the salt / the URL builders — the signature stays
  as a routing hint (do NOT delete it).
- The admin-panel auth (plans 002/007).

## Steps

### Step 1: Add `validate_webapp_init_data` to the web crate

Implement per the design doc. Critical: the WebApp key derivation is
`secret_key = HMAC_SHA256(key="WebAppData", message=bot_token)`, then
`hash = HMAC_SHA256(secret_key, data_check_string)` — NOT `SHA256(bot_token)` (that is the Login
Widget path). Reuse `hmac_sha256_hex` (decode its hex output to bytes for the second key). Compare
the hash with `constant_time_eq` (reuse plan 006's helper; add it if absent). Take `now_unix: i64`
as a parameter for testability. Parse `auth_date` and enforce `max_age_seconds`.

**Verify**: `cargo test -p openplotva-web` — add tests proving: a correctly-signed initData (built
with a known bot token) validates and returns the user; tampered hash rejected; stale `auth_date`
rejected; missing `hash`/`user` rejected. Use a hand-computed fixture (compute the expected hash
in the test with the same helper).

### Step 2: Add the soft-cutover config flag

Add a boolean config (default: false = soft/log-and-allow during rollout) e.g.
`SETTINGS_REQUIRE_INIT_DATA`, wired through `openplotva-config` like the other settings, and
exposed on `StaticWebRoutes`.

**Verify**: `cargo check -p openplotva-app` compiles; the flag is readable in the gate handlers.

### Step 3: Enforce initData at the six gates

At each of the six gate sites, BEFORE trusting the request:
1. Read the `X-Telegram-Init-Data` header.
2. If present: `validate_webapp_init_data(header, &routes.bot_token, max_age, now)`. On failure →
   401. On success → `caller_user_id = user.id`; verify the caller is authorized for the target
   (personal: `caller_user_id == chat_id`; group: existing `authorize_settings_user`).
3. If absent: if `SETTINGS_REQUIRE_INIT_DATA` is true → 401; else `warn`-log and fall through to
   the legacy signature check (soft cutover).
4. Keep `validate_settings_access_signature` as defense-in-depth; on its failure `warn`-log.

Centralize this in one helper used by all six gates to avoid drift.

**Verify**: `cargo test -p openplotva-app` — add tests: valid initData authorizes; forged/absent
initData with the flag ON is rejected; absent initData with the flag OFF logs and falls through.

### Step 4: Front-end — capture and send initData

In `web/settings/index.js`: in `initTelegram()` read `tg.initData`, store it; in the
`apiRequestJSON` helper add header `X-Telegram-Init-Data: <initData>` to every request. Update the
embedded-asset sha256 constant in `web/src/lib.rs` for `web/settings/index.js` after editing it
(the `embedded_web_assets_match_expected_hashes` test enforces this — recompute with
`shasum -a 256 web/settings/index.js`).

**Verify**: `cargo test -p openplotva-web` (asset-hash test passes with the regenerated hash);
manual or smoke check that the header is sent.

### Step 5: Format, lint, gate

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

**Verify**: exit 0.

## Test plan

- Web: initData validation (valid/tampered/stale/missing-field), key-derivation fixture.
- App: gate authorization (valid initData authorizes; forged/absent under flag rejected;
  soft-cutover fall-through). Model after the existing settings-access tests near `app/lib.rs:1889`.
- Asset-hash test passes after regenerating the `index.js` sha256 constant.

## Done criteria

- [ ] `validate_webapp_init_data` exists in `openplotva-web` with the WebAppData key derivation and a constant-time hash compare; its tests pass.
- [ ] All six gates enforce initData (header read, validated, caller identity checked) with the soft-cutover flag honored.
- [ ] `validate_settings_access_signature` is retained as a routing/defense-in-depth check (not deleted).
- [ ] Front-end sends `X-Telegram-Init-Data`; the `index.js` asset hash constant is regenerated and `embedded_web_assets_match_expected_hashes` passes.
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings` exits 0.
- [ ] `cargo test -p openplotva-web` and `cargo test -p openplotva-app` pass (new tests included).
- [ ] Only in-scope files modified.
- [ ] `plans/README.md` status row updated.

## STOP conditions

- The bot token can be empty in a deployment mode where settings must still work (open question 1)
  — STOP and get the maintainer's decision before hardening to reject.
- `validate_webapp_init_data` cannot reproduce a real Telegram initData hash in tests (key
  derivation wrong) — STOP; do not ship an HMAC you cannot verify against a fixture.
- The full gate surfaces test failures **unrelated** to this change (e.g. the pre-existing
  failures tracked by plan 011) — note them and proceed on the per-crate tests; do not try to fix
  unrelated failures here.

## Maintenance notes

- After the front-end ships and adoption is confirmed, flip `SETTINGS_REQUIRE_INIT_DATA` to true to
  harden (reject missing initData). Track that flip as a follow-up.
- Reviewer should confirm: the WebAppData key derivation (not the Login Widget one), constant-time
  compare, the signature retained as routing-only, and that all six gates were updated (not five).
- The unkeyed signature and its salt are intentionally NOT removed (URL contract); a reviewer
  expecting them to disappear is mistaken.
