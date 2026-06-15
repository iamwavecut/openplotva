# Plan 009: Design spike — authenticate the settings WebApp with Telegram initData, not an unkeyed signature

> **Executor instructions**: This is a DESIGN / INVESTIGATION plan, not a code
> change. Your deliverable is (a) a written design document and (b) a concrete
> follow-up implementation plan in `plans/`. Do NOT modify production source or
> the WebApp assets in this plan. Read code, read the Go reference, prototype
> in throwaway scratch only, and write the design. If a STOP condition occurs,
> report it in the design doc's "Open questions".
>
> **Drift check (run first)**: `git diff --stat 9f32c4b..HEAD -- crates/openplotva-web/src/lib.rs crates/openplotva-app/src/lib.rs web/settings/`
> Note any changes; they inform the design.

## Status

- **Priority**: P1 (impact) — but scoped as a spike because the fix touches an external contract
- **Effort**: L (the implementation it designs is L; this spike is M)
- **Risk**: LOW (no production code changes in this plan)
- **Depends on**: none
- **Category**: security
- **Planned at**: commit `9f32c4b`, 2026-06-15

## Why this matters

Every settings WebApp endpoint — read AND write of per-chat and per-user settings — is
authorized **solely** by `validate_settings_access_signature`, which checks an unkeyed
value: `SHA224(chat_id + ":" + "plotva-signature-salt-2024")` truncated to 8 reversed
hex characters. The salt is a hardcoded constant in this open-source repository, so
anyone can compute the valid signature for any `chat_id`/`user_id` offline and then read
or modify another chat's settings. There is no Telegram `initData` HMAC check on these
endpoints. This is a broken access control on a user-data surface.

It is not a one-line fix: the signature is embedded in WebApp URLs distributed to users
(`settings_button_url`, `settings_selection_*_url`), the front-end (`web/settings/`)
sends it back, and the scheme is inherited from go-plotva for parity (tests assert the
exact Go shapes). The correct fix is to authenticate requests with the Telegram WebApp
`initData` HMAC — which is keyed by the bot token and therefore unforgeable — while
keeping the existing signature only as a non-security routing parameter. Because this
spans server + front-end + a public URL contract, it needs a design before code. This
plan produces that design and a follow-up implementation plan.

## Current state (facts for the spike to build on)

- `crates/openplotva-web/src/lib.rs:141-153` — the unkeyed signature and its validator:

  ```rust
  pub const SETTINGS_SIGNATURE_SALT: &str = "plotva-signature-salt-2024";
  pub fn settings_signature(chat_id: i64) -> String {
      let input = format!("{chat_id}:{SETTINGS_SIGNATURE_SALT}");
      let hash_hex = hex::encode(Sha224::digest(input.as_bytes()));
      hash_hex.chars().rev().take(8).collect()
  }
  pub fn validate_settings_access_signature(chat_id: i64, user_id: i64, signature: &str) -> bool {
      if user_id != 0 && settings_signature(user_id) == signature { return true; }
      settings_signature(chat_id) == signature
  }
  ```

- The six server-side gates that rely on it (all in `crates/openplotva-app/src/lib.rs`):
  `1648`, `1889` (`parse_settings_get_access`), `1935`, `1983`, `2251`, `3047`. None of
  them validate Telegram `initData`.

- URL builders that embed the signature (the public contract — `web/src/lib.rs`):
  `settings_button_url:173`, `settings_selection_personal_url:182`,
  `settings_selection_chat_url:190`, `private_settings_web_app_url:197`.

- **The crypto already exists, but for a different Telegram flow.** `telegram_auth_hash`
  / `validate_telegram_auth` (`web/src/lib.rs:110-132`) implement the Telegram **Login
  Widget** data-check-string HMAC, where the secret key is `SHA256(bot_token)`. Telegram
  **WebApp `initData`** uses the same data-check-string idea but a DIFFERENT key
  derivation: `secret_key = HMAC_SHA256(key="WebAppData", message=bot_token)`, then
  `hash = HMAC_SHA256(secret_key, data_check_string)`. The spike must account for this
  difference — a new `validate_webapp_init_data` is needed; `validate_telegram_auth`
  cannot be reused as-is.

- Front-end: `web/settings/index.js`, `web/settings/index.html`,
  `web/settings/landing.html`. The Telegram WebApp runtime exposes
  `window.Telegram.WebApp.initData` (the raw query string to validate server-side). The
  spike must determine what the front-end currently sends and what it would need to send.

- Go reference for parity/intent: the original lives locally at
  `/Users/Shared/src/github.com/iamwavecut/go-plotva` (per project notes). Check how Go
  authenticated settings requests — confirm whether Go also relied only on the unkeyed
  signature (decision drift) or had an `initData` check the Rust port dropped.

## Commands you will need

| Purpose                | Command                                                       |
|------------------------|---------------------------------------------------------------|
| Find settings handlers | `grep -n "validate_settings_access_signature" crates/openplotva-app/src/lib.rs` |
| Read front-end         | `sed -n '1,200p' web/settings/index.js`                       |
| Go reference search    | `grep -rn "initData\|WebAppData\|signature" /Users/Shared/src/github.com/iamwavecut/go-plotva` (if the path exists) |

## Scope

**In scope** (create only):
- A design document: `docs/settings-webapp-auth-design.md` (or follow the repo's
  existing `docs/superpowers/specs/` convention if you prefer — match what is there).
- A follow-up implementation plan: `plans/0NN-settings-webapp-initdata-auth.md`
  (next free number), written to the same template as the other plans here.

**Out of scope** (do NOT modify in this plan):
- Any production Rust source, any `web/settings/*` asset, the URL builders, the salt.
- Do not change `validate_settings_access_signature` here — the implementation plan you
  write will do that, after the design is reviewed.

## Steps

### Step 1: Establish the exact threat and the current request shape

- Confirm the settings endpoints' request shape (query params / JSON body) at the six
  gate sites; note what identifies the chat/user and where the signature comes from.
- Read `web/settings/index.js` to see what the front-end sends today and whether it has
  access to `window.Telegram.WebApp.initData`.
- Document the concrete attack: given the public salt, compute a valid signature for an
  arbitrary `chat_id` and show (in prose, no working exploit string) that it passes
  `validate_settings_access_signature`.

**Deliverable**: "Threat" and "Current request shape" sections of the design doc.

### Step 2: Determine Go parity / decision drift

- Inspect go-plotva (if available) for how it authorized settings requests. Record
  whether the unkeyed signature was the whole story in Go, or whether an `initData`
  check existed and was lost in the port.

**Deliverable**: "Parity findings" section (cite Go file:line if found; say "Go
reference unavailable" if the path is missing).

### Step 3: Specify the `initData` validation

- Specify `validate_webapp_init_data(init_data: &str, bot_token: &str, max_age) -> Option<TelegramWebAppUser>`:
  parse the `initData` query string, build the data-check-string (sorted `key=value`
  lines excluding `hash`), derive `secret_key = HMAC_SHA256("WebAppData", bot_token)`,
  compute `HMAC_SHA256(secret_key, data_check_string)`, constant-time compare to `hash`,
  and enforce `auth_date` freshness. Note it belongs in `openplotva-web` (owns WebApp
  signatures) and can reuse the existing `hmac_sha256_hex` helper.
- Specify how the server endpoints combine it with the existing routing signature:
  recommended — keep the signature purely for routing (which chat the WebApp targets)
  and make `initData` the authority for *who is calling*; verify the caller is allowed to
  act on that chat (membership/admin), not merely that they produced a computable
  signature.

**Deliverable**: "Design" section with the function signature, key derivation, and the
endpoint authorization rule.

### Step 4: Plan the migration and list open questions

- Decide the rollout: can `initData` validation be added as a required check without
  breaking existing distributed URLs? (The signature stays in the URL; the new check is
  on `initData`, which the WebApp always has.) What about non-WebApp callers, if any?
- Identify front-end changes (sending `initData` with each settings request) and whether
  `index.js` already does so.
- List open questions and risks (empty bot token in some envs; deep links opened outside
  Telegram that lack `initData`; admin overrides).

**Deliverable**: a complete follow-up implementation plan in `plans/` and an "Open
questions" section.

## Done criteria

- [ ] `docs/settings-webapp-auth-design.md` (or a spec under the repo's docs convention) exists with sections: Threat, Current request shape, Parity findings, Design (incl. `validate_webapp_init_data` signature + WebAppData key derivation), Migration/rollout, Open questions.
- [ ] A follow-up implementation plan exists in `plans/` (next free number) written to the plan template, with concrete in-scope files and verification gates.
- [ ] No production source or `web/settings/*` asset was modified by THIS plan (`git status` shows only the new doc + new plan file).
- [ ] `plans/README.md` status row updated (and the new follow-up plan added to the index).

## STOP conditions (record in the design's "Open questions", do not block)

- go-plotva reference path does not exist → note "parity unverified", proceed with the
  Telegram-spec-based design.
- The front-end cannot supply `initData` in some legitimate flow (e.g. a settings link
  opened in a normal browser) → document the gap and propose handling (reject, or a
  separate authenticated path).
- The bot token may be empty in some deployment mode where settings must still work →
  flag that `initData` HMAC needs a non-empty token, and propose a fallback decision for
  the maintainer.

## Maintenance notes

- This spike deliberately produces a design + plan rather than code because the change
  crosses the WebApp URL contract and front-end. The maintainer should review the design
  before the follow-up plan is executed.
- The unkeyed signature need not be removed — downgrading it to "routing only" preserves
  existing URLs while moving authority to `initData`. Make that explicit so a reviewer
  does not expect the signature to disappear.
