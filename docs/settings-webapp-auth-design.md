# Settings WebApp authentication contract

Status: implemented. Last reconciled with source on 2026-08-02.

The public settings signature remains in generated URLs for routing compatibility, but
it is not identity proof. Telegram WebApp `initData` is the authority for the caller.

## Request contract

- `web/settings/index.js` reads the raw `Telegram.WebApp.initData` string and sends it
  on every API request as `X-Telegram-Init-Data`.
- The server rejects a missing, malformed, forged, or older-than-one-hour value with
  `401 Unauthorized`.
- Validation uses Telegram's WebAppData derivation: HMAC-SHA256 of the bot token under
  `WebAppData`, followed by HMAC-SHA256 of the sorted data-check string. The supplied
  hash is compared in constant time.
- The validated Telegram user ID must be positive and equal the request's claimed
  `user_id`. Existing admin/deputy checks then decide what that caller may do to the
  target chat.
- The legacy URL signature remains a routing and defense-in-depth check so distributed
  links keep their shape; possession of it never bypasses `initData` authentication.

## Ownership and proof

The signature primitive and `validate_webapp_init_data` live in
`crates/openplotva-web/src/lib.rs`. Request gating lives in
`crates/openplotva-app/src/lib.rs`; the Settings WebApp header wiring lives in
`web/settings/index.js`.

Regression coverage includes valid callers, mismatched caller IDs, missing data,
forged hashes, malformed data, stale timestamps, and the WebApp-specific key
derivation. Changes must preserve both the authentication and authorization layers.
