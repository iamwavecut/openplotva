# Settings WebApp authentication — design (replace the unkeyed signature with Telegram initData)

Date: 2026-06-15
Status: design (approved-for-planning); implementation tracked by `plans/010-settings-webapp-initdata-auth.md`
Origin: `improve` audit finding F4 / spike `plans/009-settings-webapp-auth-spike.md`, investigated against commit `9f32c4b`.

## Threat

Every settings WebApp endpoint is authorized **solely** by `validate_settings_access_signature`
(`crates/openplotva-web/src/lib.rs:148-153`). The signature is
`settings_signature(id) = hex(SHA224("{id}:plotva-signature-salt-2024")).chars().rev().take(8)`
(`web/src/lib.rs:141-145`). The salt `plotva-signature-salt-2024` is a **public constant**
committed in this open-source repository, and the hash is **unkeyed**. Therefore anyone who
knows a target `chat_id` or `user_id` — both public Telegram numeric identifiers — can compute
the valid 8-character signature offline with no secret material and:

- read or overwrite the settings of any chat the bot is in,
- enumerate/delete memory cards for any user,
- manipulate deputy assignments.

There is **no Telegram `initData` HMAC check** anywhere on these endpoints (the strings
`initData` / `WebAppData` appear nowhere in `openplotva-web` or `openplotva-app`).

## Parity (inherited, not a porting regression)

The Go original at `/Users/Shared/src/github.com/iamwavecut/go-plotva` has the **same** scheme:
same salt and SHA-224/reverse/take-8 algorithm (`internal/utils/security.go:13-29`), and the
settings handlers check only the signature with no initData
(`internal/web/settings.go:105-113,458-468,673-680`; `internal/web/chats_handler.go:58-65`;
`internal/web/settings_deputies.go:158-162,235-245`). So this is an **inherited design
decision**, not a drift introduced by the Rust port. Fixing it is a deliberate, forward-looking
hardening — it changes behavior relative to Go parity and must be a conscious decision.

## Current request shape (the six gates)

All in `crates/openplotva-app/src/lib.rs`; every one trusts client-supplied `chat_id`/`user_id`:

| # | Site | Endpoint | Shape | Call |
|---|------|----------|-------|------|
| 1 | `:1648` `settings_chats_response` | GET `/api/chats` | query | `validate_settings_access_signature(user_id, 0, signature)` |
| 2 | `:1889` `parse_settings_get_access` | GET `/api/settings` | query | `(chat_id, user_id, signature)` |
| 3 | `:1935` `parse_deputy_update_request` | PUT `/api/settings/deputies` | JSON body | `(chat_id, user_id, signature)` |
| 4 | `:1983` `parse_deputy_owner_access` | GET/DELETE deputies & candidates | query | `(chat_id, user_id, signature)` |
| 5 | `:2251` `parse_settings_update_request` | PUT `/api/settings` | JSON body | `(chat_id, user_id, signature)` |
| 6 | `:3047` memory access parser | GET/DELETE `/api/settings/memory` | query | `(chat_id, user_id, signature)` |

## Front-end (web/settings/)

- `index.html:10` loads the Telegram WebApp SDK, so `window.Telegram.WebApp.initData` IS
  available at runtime.
- `index.js` `initTelegram()` (`:1539-1582`) reads `tg.initDataUnsafe.user.id` but **never reads
  the raw signed `tg.initData`**. It is never stored, never sent. API calls (`:406-490`,
  `:1430-1467`) send only `signature`, `chat_id`, `user_id`.
- Required FE change (small, localized): in `initTelegram()` capture `tg.initData`, store it in
  the Framework7 store, and send it as an `X-Telegram-Init-Data` header in the `apiRequestJSON`
  helper on every request. `landing.html` shares the same WebApp session so `index.html` can
  re-read `initData`.

## Design — `validate_webapp_init_data`

Add to `crates/openplotva-web/src/lib.rs` (the crate that owns WebApp signatures):

```rust
pub struct TelegramWebAppUser { pub id: i64, pub first_name: String, pub username: Option<String> }

pub fn validate_webapp_init_data(
    init_data: &str,
    bot_token: &str,
    max_age_seconds: u64,
    now_unix: i64,           // pass time in for testability
) -> Option<TelegramWebAppUser>;
```

**Key derivation — WebApp differs from the Login Widget already implemented:**
- Login Widget (`validate_telegram_auth`, `:127-132`): `secret = SHA256(bot_token)`, then
  `HMAC_SHA256(secret, data_check_string)`.
- WebApp initData (NEW): `secret_key = HMAC_SHA256(key="WebAppData", message=bot_token)`, then
  `hash = HMAC_SHA256(secret_key, data_check_string)`.

Reuse the existing `hmac_sha256_hex` helper (`:212-237`) for both HMAC steps (decode the first
hex result back to 32 bytes for the second key). Use a **constant-time** comparison for the hash
(the helper introduced by plan 006, `constant_time_eq`).

Algorithm:
1. Parse `init_data` as a URL query string; extract `hash` (absent → None).
2. Extract `auth_date` (unix seconds); if `now_unix - auth_date > max_age_seconds` → None.
3. Build `data_check_string`: all `key=value` pairs except `hash`, sorted by key, joined by `\n`.
4. `secret_key = HMAC_SHA256("WebAppData", bot_token)`.
5. `computed = HMAC_SHA256(secret_key, data_check_string)`; constant-time compare to `hash`.
6. On match, percent-decode the `user` JSON field and deserialize into `TelegramWebAppUser`.

## Endpoint authorization rule

- The existing routing signature **stays in the URL** as a routing-only parameter — existing
  distributed URLs keep working; it is no longer the authority for identity.
- `initData` becomes the **authority for caller identity**:
  1. Validate `initData`; reject 401 if invalid/absent.
  2. `caller_user_id = validated user.id`.
  3. For personal settings (`chat_id == user_id`): require `caller_user_id == chat_id`.
     For group settings: run the existing `ensure_settings_chat_available` / `authorize_settings_user`
     (admin + deputy) checks against `caller_user_id` on the target chat.
  4. `validate_settings_access_signature` is retained as defense-in-depth routing; its failure
     should `warn`-log, not silently allow.
- `bot_token` is already in `StaticWebRoutes`; thread it to the six gates.

## Migration / rollout

- Server first: add `validate_webapp_init_data` and the initData check to all six gates. No URL
  migration — the signature stays as a routing hint.
- Then front-end: capture + send `initData`.
- Soft cutover: during the window where the server is updated but a cached old front-end has no
  `initData`, gate behind a config flag that logs-and-allows on missing initData, then flip to
  reject once the front-end is deployed.

## Open questions (decide before/while implementing)

1. **Empty bot token** (standalone/web-only modes): `validate_webapp_init_data` needs a non-empty
   token. Reject all WebApp requests, or fall back to signature-only behind a flag? (Maintainer.)
2. **Links opened outside Telegram** (plain browser) have no `initData`. Recommend: reject with an
   error page (settings are Telegram-only) — the front-end already warns on missing signature.
3. **`max_age_seconds`**: Telegram suggests 86400; tighter is safer but breaks long sessions.
   Suggested default 3600, configurable.
4. **Constant-time compare**: required for the hash (timing oracle); use plan 006's helper.
5. **Missing `user` field**: in some inline contexts Telegram may omit it — return None gracefully.
6. **Deputies**: initData establishes *who* the caller is; the existing `authorize_settings_user`
   (admin/deputy) logic still establishes *what they may do*. Both layers remain necessary.

## Citations

Web crate: `web/src/lib.rs:141-153` (signature+salt), `:110-132` (Login Widget HMAC), `:173-202`
(URL builders), `:212-237` (`hmac_sha256_hex`). App gates: `app/src/lib.rs:1648,1889,1935,1983,2251,3047`.
Front-end: `web/settings/index.js:1539-1582,406-490,1430-1467`, `web/settings/index.html:10`,
`web/settings/landing.html:122-165`. Go parity: `go-plotva/internal/utils/security.go:13-29`,
`internal/web/settings.go:105-113`, `internal/web/chats_handler.go:58-65`,
`internal/web/settings_deputies.go:158-162`.
