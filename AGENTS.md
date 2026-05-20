# OpenPlotva Agent Instructions

## Scope And Priority

- These instructions apply to `/Users/Shared/src/github.com/iamwavecut/openplotva`.
- More specific user requests, future nested `AGENTS.md` files, and source files under direct edit take priority.
- This repository is the Rust implementation of `/Users/Shared/src/github.com/iamwavecut/reference-app`.
- The Go repository is reference material only. Do not edit tracked files in `reference-app`.

## Goal

Implementation Plotva from Go to modern Rust while preserving observable behavior.

Done means:

- Rust app builds, runs, and passes contract checks against the frozen Go behavior.
- No user-facing behavior changes exist unless recorded as approved deviations.
- Contract means semantic/runtime compatibility, not byte-for-byte reproduction of Go internals such as `encoding/gob` bitstreams.
- The original Go repository remains unchanged.
- README and developer docs are suitable for future open-source release.

## Current Critical Path

The objective is still the full behavior-preserving Rust implementation, but day-to-day work should focus on the highest-leverage sequence:

1. Keep the Go reference snapshot and generated contract inventories current.
2. Build a runnable Rust service spine: config, lifecycle, observability, HTTP health/static serving, Postgres, Dragonfly/Redis, and reference-snapshot enforcement.
3. Port the behavior-critical Telegram boundary around `carapax`: update ingestion, outbound payload model, command/callback contract, HTML sanitization, splitting, rate limits, deduplication, and virtual message IDs.
4. Port storage and migrations with SQL behavior contract before higher-level business features.
5. Port taskman, dialog, LLM, memory, media, and web features in tested vertical slices.

Do not spend time polishing broad abstractions until the relevant contract inventory or vertical slice exists.

## Reference Snapshot And Contract

- Initial observed Go reference snapshot:
  - Commit: `56506a95a749629235ecf1ea35c54d5a4172fdbd`
  - Commit time: `2026-05-19T16:46:12+02:00`
  - Subject: `Refactor everything`
- Store the active lock in `docs/contract/reference-snapshot.json`.
- Before every major milestone, compare `/Users/Shared/src/github.com/iamwavecut/reference-app` `HEAD` to the stored lock.
- The Rust app enforces this check on startup by default through `OPENPLOTVA_DISABLED_LEGACY_LOCK=true`.
- If Go `HEAD` changed, classify the diff, update inventories/tests, port the behavior, write a catch-up note, then advance the lock.
- Keep `docs/contract/deviations.md` empty unless the user explicitly approves a deviation.
- Preserve user-facing strings, prompts, Telegram payload shapes, HTML sanitization, callback data, payment behavior, DB schema meaning, HTTP routes, GraphQL schema, and admin/settings assets unless a deviation is approved.

## Repository Boundaries

- Create and edit files only inside this repository unless the user explicitly asks otherwise.
- Never edit tracked files in `/Users/Shared/src/github.com/iamwavecut/reference-app`.
- Use `git -C /Users/Shared/src/github.com/iamwavecut/reference-app ...`, `rg`, tests, and read-only inspection for the Go baseline.
- An ignored reference clone under `openplotva/.reference/reference-app` is allowed if it helps contract work.
- Preserve unrelated user changes in this repository. Do not reset or clean without explicit approval.

## Rust Standards

- Use Rust 2024 edition.
- Use a virtual Cargo workspace with `resolver = "3"`.
- Pin `rust-version` to the current stable toolchain at scaffold time.
- Prefer workspace dependencies and workspace lints.
- Keep crate boundaries explicit:
  - `openplotva-app` is the composition root.
  - Domain crates must not depend on web, Telegram, SQLx, or vendor SDKs unless that crate owns the integration boundary.
  - Use `anyhow` only at app boundaries; use typed errors such as `thiserror` in library/domain crates.

## Preferred Stack

- Runtime/web: `tokio`, `axum`, `tower-http`, `tracing`, OpenTelemetry.
- Database: `sqlx` for async Postgres, runtime/embedded migrations where appropriate, `pgvector` with SQLx support for memory/shield embeddings.
- Redis/Dragonfly: `redis` with Tokio support; add `deadpool-redis` only when pooling or isolation is genuinely needed.
- Current service-spine probes are gated by `OPENPLOTVA_CONNECT_SERVICES=false` by default. Do not treat skipped probes as storage contract; enable them when working on live Postgres/Redis behavior.
- Converted SQLx migrations are gated by `OPENPLOTVA_RUN_MIGRATIONS=false` by default. Run them on fresh/scratch databases until the existing Go `sql-migrate` table compatibility path is handled.
- Telegram Bot API: use `tg-rs/carapax` as the integration base. Do not use `frankenstein` unless the user reverses this decision.
- Telegram Bot API objects currently come from `carapax::types`; keep command/callback/API constructor catalog tests in `openplotva-telegram` aligned with `docs/contract/generated/telegram.json`.
- Fetcher-facing inline keyboards should use the `openplotva-telegram` helper wrappers for Go `NewInlineKeyboardButtonData`, `NewInlineKeyboardButtonURL`, `NewInlineKeyboardRow`, `NewInlineKeyboardMarkup`, and settings WebApp button semantics so callback data, URLs, WebApp payloads, empty keyboards, and copied rows stay payload-compatible.
- Fetcher-facing callback data helpers live in `openplotva-telegram`: `parse_callback_action`, `parse_callback_i64`, `delete_drawing_callback_data`, `delete_lyrics_callback_data`, delete drawing/lyrics inline-keyboard builders, `callback_handler_for_action`, `callback_query_route`, `callback_query_ack_request`, `callback_query_ack_method`, `settings_callback_ack_method`, `checkin_theme_selection_ack_method`, and the check-in theme callback helpers mirror Go's long/short callback keys, `processCallbackQuery` pre-handler order, legacy/actionless ack split, known handler groups, rate-limit skip, settings callback routing, Go's empty `answerCallbackQuery` terminal acknowledgements, cached settings acknowledgements, and initiator-only theme-selection alert behavior. Keep `dl_x` generated as callback data but unrouted unless the Go handler list changes.
- Settings WebApp signatures and URL shapes live in `openplotva-web`: preserve Go `utils.GenerateSignature`, `CreateSettingsButton`, `settingsSelectionBaseURL`, settings-selection query parameters, and the private `/settings/index.html` base-URL joining behavior exactly. Telegram-specific keyboard objects still belong in `openplotva-telegram`.
- Direct fetcher-style request methods such as `sendChatAction`, `answerCallbackQuery`, `answerInlineQuery`, `answerGuestQuery`, `answerPreCheckoutQuery`, `createInvoiceLink`, `refundStarPayment`, `editUserStarSubscription`, `editMessageCaption`, and `editMessageReplyMarkup` belong in the `openplotva-telegram` outbound/transport layer; Go sends these through direct server request/send paths, not through dispatcher queues, so do not add virtual-message mapping or queue persistence unless a later ported call site actually queues them. Chat actions use the same send-text permission gate as Go before sending. Callback, inline, and payment direct requests must preserve Go's omitted empty/false/zero parameter behavior and raw method payload shapes. Guest-query answers return Telegram's sent inline-message result and should not participate in virtual-message ID resolution. Guest HTML answer helpers in this layer preserve Go sanitization, 4096/3900 rune limits, 120-rune descriptions, stable `guest-*` result IDs, fallback/add-to-chat text, and `startgroup=guest` URLs; full guest dialog routing is still separate fetcher work. Payment helpers in this layer only cover the Bot API request/payload surface: Stars invoice payload strings, VIP/donation `createInvoiceLink` parameters, pre-checkout acknowledgement, refunds, and subscription cancellation. SQL row helpers for `subscriptions` and `donations` live in `openplotva-storage::PostgresPaymentStore`; VIP cache/event storage lives in `openplotva-storage::PostgresVipStore`. App-level payment orchestration belongs in `openplotva-app::payments`; `execute_payment_control_job_at` dispatches payment-owned `vip_invoice`, `donate_invoice`, and `successful_payment` taskman jobs into the ported payment executors. `process_payment_control_job_once_at` and the worker loop own the injectable payment control-job execution/finalization contract; `InMemoryPaymentControlJobQueue` is the current Rust-native queue core because the frozen Go migration corpus drops the old SQL `job_queue` tables. `PersistentPaymentControlJobQueue` saves a versioned Rust-native JSON snapshot (`openplotva.payment-control-job-queue.v1+json`) after each queue mutation, reloads from `~/.plotva/openplotva-payment-control-jobs.snap`, and requeues `Processing` jobs to `Pending` on startup. The app shell starts this worker when services and `BOT_KEY` are connected; full taskman WAL semantics and decoded-update/fetcher queue producers remain separate runtime work.
- The Rust app shell configures Telegram bot commands when `OPENPLOTVA_CONNECT_SERVICES=true` and `BOT_KEY` is set: delete existing commands first, then apply the private, group, and group-admin scoped command lists in Go `initBot` order. Command setup failures abort startup before runtime workers.
- The Rust app shell starts the payment control-job worker when `OPENPLOTVA_CONNECT_SERVICES=true` and `BOT_KEY` is set. It uses `PostgresSuccessfulPaymentStore`, direct `TelegramClient` invoice effects, dispatcher-backed successful-payment reply effects, and the persistent Rust-native payment control-job queue snapshot. Do not wire `PaymentUpdateHandler` to a placeholder/no-op decoded-update consumer just to feed this queue; wait for the real fetcher route or a tested consumer bridge.
- The live outbound dispatcher in `openplotva-app` should keep the Go server runtime defaults: `plotva:message_queue`, max queue/persisted items `10000`, dedupe enabled with a `3s` window and `1000` cache entries, `50ms` dispatch interval, `10m` limiter cleanup cadence, `30m` limiter max idle, and `10s` shutdown persistence timeout.
- Telegram update ingestion must preserve Go's Redis list contract: key `plotva:updates:queue`, `RPUSH` producers, `BLPOP` consumers, `LLEN` diagnostics, timeout handling, and FIFO ordering.
- Approved deviation: do not maintain bitwise compatibility with Go `encoding/gob` payloads. For every gob-backed persistence surface encountered during the implementation, use a Rust-native serde codec while preserving the observable runtime contract: keys, ordering, field meaning, TTLs, lifecycle semantics, and diagnostics. Current instances: update payloads use the `openplotva.update.v1+carapax-json.zstd` envelope around `carapax::types::Update`; dispatcher shutdown persistence stores the `PersistentDispatcherItem` JSON directly under `plotva:message_queue`; persisted chat rate-limit expiries under `plotva:rate_limited_chat:*` use JSON timestamp values; cached group admin IDs under `chat:{id}:admins` use JSON integer arrays; tracked ephemeral messages under `ephemeral_messages:{chat_id}:{message_id}` use JSON values with nanosecond expiry timestamps; the payment control-job queue snapshot stores `openplotva.payment-control-job-queue.v1+json` at `~/.plotva/openplotva-payment-control-jobs.snap`. Treat mixed Go/Rust in-flight gob-backed data as unsupported during cutover. Contract tests for these surfaces should assert decoded values and lifecycle behavior, not gob byte layouts. Do not spend implementation effort recreating gob codecs or compatibility shims unless the user explicitly reverses this deviation.
- Keep update producer filtering in `openplotva-updates` separate from consumer stats naming: use `GO_ALLOWED_UPDATES`/`GO_ALLOWED_UPDATE_NAMES`, `producer_update_type`, `is_allowed_producer_update`, and `run_update_producer_until` for webhook/long-polling enqueue decisions. `update_name` is the consumer report label and intentionally does not cover every Go fetcher classifier.
- Keep Telegram update startup methods and sources in `openplotva-telegram`: use `build_get_updates_method`, `build_set_webhook_method`, `build_delete_webhook_method`, `LongPollUpdateSource`, `webhook_update_channel`, and `TELEGRAM_WEBHOOK_PATH` for concrete `carapax` source wiring instead of reconstructing allowed updates, offsets, retries, webhook channel timeouts, or request validation at the app layer.
- The Rust app shell starts Telegram update ingestion when `OPENPLOTVA_CONNECT_SERVICES=true` and `BOT_KEY` is set. By default it deletes any webhook first and feeds `LongPollUpdateSource` into `plotva:updates:queue`; `deleteWebhook` failure is logged inside the worker and must not fail app startup, matching Go's background `StartBot` goroutine. When `BOT_WEBHOOK_ENABLED=true` and `BOT_WEBHOOK_URL` is set, it registers `setWebhook`, installs `/telegram/webhook`, validates `X-Telegram-Bot-Api-Secret-Token`, uploads `BOT_WEBHOOK_CERT_FILE` as `cert.pem` when both `BOT_WEBHOOK_CERT_FILE` and `BOT_WEBHOOK_KEY_FILE` are set, and feeds `WebhookUpdateSource` into the same queue. Use the raw multipart path only for certificate mode because `carapax` serializes `setWebhook` as JSON.
- Telegram update consumer work should preserve Go `internal/processor` timing semantics: `5s` dequeue pop timeout, `10s` state timeout, `45s` handle timeout, `1m` stale-update side-effect cutoff with the strict Go `!date.Add(maxAge).After(now)` boundary, and a `4 * available_parallelism` worker limit.
- Keep Telegram update state extraction in `openplotva-updates` and Postgres persistence in `openplotva-storage`; shared chat/user state structs belong in `openplotva-core` so storage does not depend on `carapax`.
- Keep Telegram-free history/dialog metadata models in `openplotva-core`: `MessageSender`, sender type constants, `ChatMessageMeta`, `ToolCall`, and `ChatAttachment` mirror Go `sharedtypes`/`utils` shapes while SDK-specific extraction stays in `openplotva-updates`.
- Keep Telegram message attachment extraction in `openplotva-updates` over `carapax::types::Message`, producing `openplotva-core::ChatAttachment`. Preserve Go `utils.TelegramMessageAttachments`: source trimming/default `message`, caller-supplied caption trimming, latest photo size, promoted image document/sticker behavior when requested, unpromoted sticker default, no caption on voice attachments, and case-insensitive MIME prefix checks. Storage/history code should consume the core attachment model, not Telegram SDK types.
- Keep fetcher-style message metadata helpers in `openplotva-updates` until the fetcher route is ported: `fetcher_message_text` mirrors Go `Fetcher.getTextFromMessage` text/caption/sticker/audio/video/document/contact fallback behavior, `build_fetcher_message_context` preserves the original pre-fallback `Message.Text` and Go `buildMessageMeta` output, `resolve_message_sender` mirrors Go `utils.ResolveMessageSender`, `build_message_meta` mirrors Go `buildMessageMeta`, `detect_message_type` prefers trimmed `message`-sourced attachment kinds before Telegram message fields, fallback order is voice, video, audio, document, location/venue, contact, photo/sticker image, text, then `text`, and `collect_media_attachments` reuses the attachment extractor with promoted first-image references and dedupes by the Go attachment key rules.
- Keep fetcher-style message routing helpers in `openplotva-updates` until the fetcher route is ported: `parse_if_addressed` mirrors Go `parseIfAdressed` including bot first-name/transliteration stripping, group mention stripping, private-chat addressing, reply-to-bot addressing, and forum topic-root reply suppression; `is_settings_command_message` mirrors Go's `/settings` command target rules and must stay aligned with the permission/block bypass path; `parse_edit_command` mirrors Go `parseEditCommand` edit-verb detection for image-edit routing and caption fallback; `resolve_draw_prompt_from_message` and `draw_prompt_with_reply_context` mirror Go draw reply-context prompt handling and intentionally use `ReplyToMessage.Text`, not media/caption fallback text; `compose_image_prompt` and `edited_image_prompt_update` mirror Go pending-image-job prompt updates by appending unique trimmed vision/attachment context after the base prompt; `should_handle_addressed_message` and `should_handle_random_response` mirror Go bot-loop and captionless-media random-response gates; `react_message_words` mirrors Go `React` command-word splitting and first-word fallback; guest helpers `guest_visible_text`, `strip_guest_address_prefix`, `guest_current_request_text`, `guest_request_has_visible_text`, `guest_has_other_bot_mention`, `guest_message_reject_reason`, `is_guest_unsupported_feature_request`, `normalize_guest_command_word`, `looks_like_guest_history_summary_request`, `GuestChainMessage`, `format_guest_chain_for_prompt`, `build_guest_dialog_text`, and `build_guest_shield_query_text` mirror Go guest-message visible-text, address stripping, reply fallback, guard-order semantics, add-to-chat unsupported-feature gating, guest-chain prompt formatting, dialog text shaping, and Shield query text/truncation. Full guest dialog routing remains separate fetcher work.
- App-level `/settings` command handling lives in `openplotva-app::settings`: preserve Go's private WebApp button behavior, blank-`WEBAPP_URL` no-send path, `settingsWebAppURL` signature URL, text `Откройте настройки бота:`, WebApp button text, reply parameters, immediate queue placement, virtual-message insertion, and queue-carried `BypassChatRestrictions=true`. Group `/settings` builds the high-priority `"group settings"` control job for the `control` queue, preserves Go caller/sender guards, sends the exact immediate decline/wait/failure notices, and executes the group-settings control job through injectable permission/admin-sync effects. The executor sends Go's permission-check failure text, no-rights text, and private-chat deep-link button text/URL with `BypassChatRestrictions=true`; it intentionally omits topic `message_thread_id` when reconstructing Go `buildControlMessage`. Concrete `canOpenGroupSettings` storage/Telegram lookup is ported through `PostgresChatMemberStore`, `carapax` `getChatMember`, and Rust conversion from `carapax::types::ChatMember` to Go-shaped upsert params; `carapax` Bot API 10 admin variants do not expose legacy aggregate send permissions, so send-permission upserts are populated from restricted-member variants where the API exposes them. Concrete `syncChatAdmins` API/DB fallback is ported through `carapax` `getChatAdministrators`, Go-shaped admin membership upserts, stored-admin fallback, admin user persistence, and best-effort Redis admin-ID cache writes under `chat:{id}:admins` with a 30 minute TTL. The Redis key/codec/TTL/filter storage surface for ephemeral messages is ported in `openplotva-storage`; the runtime one-minute auto-delete worker and virtual-message bridge for wait/failure notices are not ported yet.
- App-level update consumer glue belongs in `openplotva-app::updates`: it may combine extracted state persistence with an injected handler and source, but must not start a default no-op handler that drains real queued updates before fetcher routing is ported. Keep the long-running loop bounded by Go-style stage capacity from `UpdateConsumerConfig::worker_limit`.
- App-level outbound producers currently live in `openplotva-app::virtual_messages`. Text and sticker sends insert unresolved `message_id_map` rows before dispatcher enqueue, keep insert failures non-fatal like Go logs, use Go-compatible fingerprints, respect immediate queue placement, carry Go `BypassChatRestrictions` into dispatcher metadata, and let `send_work_item_and_resolve` resolve mappings from successful Telegram `Message` responses. Sticker and direct-media producers backed by form-only `carapax` methods should attach explicit persistence payloads for shutdown/replay. Direct media producers that correspond to Go `SendChattable`, `SendMediaGroup`, or `EditMessageMediaWithContext` must not create virtual-message rows unless Go does; they should carry empty virtual IDs.
- Pending edit/delete history side effects are storage-backed in the runtime: use `openplotva_storage::PostgresHistoryStore` through the app-level `PendingOpHistory` trait, update text payloads in `chat_history_entries`, delete message entries by `(chat_id, message_id)`, and invalidate the Go Redis cache key prefix `plotva:chat_history_cache:v2:`. These history failures are non-fatal to Telegram pending-op completion, matching Go's log-and-continue service calls.
- Decoded inbound history side effects should use app-level `openplotva_app::updates::persist_update_history`, which derives Go fetcher-style original text and metadata from `carapax` messages, then routes `message` updates through `persist_inbound_message_history` and `edited_message` updates through `persist_edited_message_history`. This preserves Go text/caption/original-text normalization, sender metadata filling, `msg:<message_id>` entry IDs, user/model role selection from the current bot ID, forwarded/via-bot/thread metadata, Go `UpsertHistoryEntry`, `ensure_chat_history_partition($1::date)`, upserts on `(bucket_day, chat_id, entry_id)`, and `plotva:chat_history_cache:v2:*` invalidation. Edited messages preserve Go `Fetcher.processEditedMessage`/`history.Service.UpdateMessage` semantics: update an existing text entry only, normalize text/original-text/meta attachments, avoid creating missing history rows, and invalidate the same cache key.
- When a decoded-update handler is ready to own fetcher-style message handling, wrap it with `openplotva_app::updates::UpdateHandlerWithHistory`, `process_update_with_state_and_history_store_at`, or `handle_update_with_history` instead of duplicating storage calls or forking the consumer loop. These helpers keep history failures non-fatal and logged, while handler failures still fail the handle stage; they are opt-in and must not be used to install a placeholder/no-op consumer.
- Redis-backed chat rate-limit expiry storage belongs in `openplotva-storage::RedisRateLimitStore`; app-level cache/check/write policy belongs in `openplotva-app::rate_limits`. Preserve the Go key prefix `plotva:rate_limited_chat:`, Redis TTL behavior, 30 minute in-memory policy cache TTL, strict `now < expiry` active boundary, and `429` retry handling with a 60 second fallback when Telegram omits `retry_after`. The storage codec intentionally uses the approved Rust-native JSON timestamp value instead of Go gob. The live outbound dispatcher should check this policy before sends and record new retry windows from real `carapax`/Telegram `429` errors.
- Payment SQL row storage belongs in `openplotva-storage::PostgresPaymentStore`: preserve Go `subscriptions`/`donations` SQL, `telegram_payment_charge_id` conflict behavior, active-subscription filtering that excludes canceled/refunded/admin-grant rows, first-write-wins cancel/refund timestamps, and donation duplicate handling through no-row results. VIP ledger SQL storage belongs in `openplotva-storage::PostgresVipStore`: preserve Go `vip_cache` queries, `vip_create_event` function calls, event idempotence for subscription-scoped payment/refund events, summary/list row shapes, actor joins, subscription joins, and cleanup of expired cache rows. Successful-payment orchestration belongs in `openplotva-app::payments`: preserve Go `XTR` filtering, payload-prefix routing, user persistence as non-fatal, duplicate no-row fallback lookups, subscription VIP ledger event creation, effective-expiry success text, donation thank-you text, and VIP cache invalidation. Concrete successful-payment text side effects use `SuccessfulPaymentDispatcherEffects` to queue replies through the normal virtual-message dispatcher, and `NoopVipCacheInvalidator` is only for slices that do not yet carry the runtime VIP cache. VIP/donation invoice control-job execution also belongs in `openplotva-app::payments`: use injected effects around `createInvoiceLink`, Go HTML button text, missing-user/error-message behavior, and returned-link validation; the concrete `TelegramClient` effects should build direct `carapax` methods while keeping the pure executor independently testable. Payment taskman dispatch belongs in this same app module: keep `vip_invoice`, `donate_invoice`, and `successful_payment` routing in `execute_payment_control_job_at`, and keep non-payment control kinds out of the payment dispatcher. Go-shaped payment update queueing also lives here: `payment_invoice_control_job_from_update_at`, `enqueue_payment_invoice_command_update_or_else_at`, `enqueue_payment_invoice_command_update_with_vip_status_or_else_at`, `successful_payment_control_job_from_update_at`, and `enqueue_successful_payment_update_or_else_at` build and assign high-priority `"vip invoice"`, `"donate invoice"`, and `"successful payment"` jobs to the `control` queue while preserving Go's private `/vip` existing-status branch before invoice queueing. Payment control-job worker code also lives here: decode queued `StatelessJobItem` payloads through `openplotva-taskman`, execute payment-owned jobs, complete missing-user/successful-payment outcomes that Go treats as non-fatal, and mark unsupported/payment-invoice execution errors failed like Go taskman executor errors. The payment-aware decoded-update wrapper `handle_payment_update_or_else_at` composes Go pre-checkout acknowledgement, successful-payment control-job queueing, status-aware private `/vip` handling before invoice-command queueing, private `/donate` invoice-command queueing, and final delegation to the future real fetcher route; `PaymentUpdateHandler` is the installable `UpdateHandler` adapter around a real downstream handler and must not be wired with a no-op placeholder. Do not replace this path with inline successful-payment processing once taskman is active. Non-private `/vip` and `/donate` redirect builders live here too: preserve Go deep-link shapes, button text, reply parameters, no parse mode, VIP benefits text, donation benefit text, valid donation amount forwarding, and invalid donation amount fallback to `start=donate`. Existing-VIP status lookup and message builders also live here: preserve Go `Fetcher.IsVIP` gate, `buildVIPStatusView` order (active VIP summary, active subscription for the cancel button, then external `vip_cache`), active-ledger status text, external-cache status text, `vip.DisplayDaysLeft` rounding, HTML parse mode, reply parameters with `allow_sending_without_reply=true`, and the `{"action":"cancel_vip"}` button only when an active subscription row exists. `/admin_grant_vip` handling also lives here: preserve Go positional argument splitting, explicit-target or reply-target resolution, numeric/`@username`/`https://t.me/username` target forms through stored users, `-365..=365` nonzero duration validation, invalid-duration warning plus one-day default, default reason text, `vip_create_event` admin-adjustment writes, cache invalidation, usage/error/success texts, and VIP benefits body. `/admin_cancel_vip` handling also lives here: preserve Go target parsing for numeric/`@username`, refund flag parsing, active-status lookup/no-active replies, `vip_create_event` admin-revoke writes, Telegram Stars refund requests, direct user refund notices, subscription refunded marking, `refund_reversal` VIP ledger writes, cache invalidation, and success/failure text. The invoice-command path reports non-private redirects, missing VIP users, invalid donation amounts, existing VIP status sends, and status lookup/send failures so the caller can send the exact Go side effects. Keep Telegram SDK types, admin command text, direct Bot API request builders, and taskman/control-job scheduling outside the storage stores.
- Taskman control-job value objects belong in `openplotva-taskman`: preserve Go `control` queue naming, priority numbers, `StatelessJobItem`/`JobPayload` JSON field names, `ControlKind` strings, Telegram metadata shape, and payment `ControlPayment` shape. Payment invoice scheduling should build `ControlJobData::{vip_invoice, donate_invoice}`-equivalent payloads through this crate, then let app/fetcher code own assignment and execution. Use `control_job_params_from_stateless_job`/`control_job_params_from_payload` to mirror Go `ControlJobExecutor` decoding instead of duplicating payload-field extraction in app code.
- Chat permission policy primitives live in `openplotva-server`, with Telegram-free `ChatSettings`/`ChatSettingsUpdate` models in `openplotva-core` and SQLx chat-settings helpers in `openplotva-storage`. The app-level outbound dispatcher now checks Go permission actions before `sendMessage`, `sendSticker`, and `editMessageText`, skips those checks for queue-carried Go `BypassChatRestrictions`, reflects successful direct `EditText` sends into history like Go's post-send callback, and auto-disables chat settings after Telegram permission errors for Go `HandleSendError`-style send methods. Real fetcher call sites that set or need bypass, fetcher permission call sites, and the remaining non-dispatch permission surfaces still need to be ported before marking permissions complete.
- LLM: define Plotva-owned provider traits. Implement with `genai`, `async-openai`, and raw `reqwest` only for provider gaps.
- Prompts: keep `.prompt` files and use Rust `handlebars` first. Do not implementation prompt language before contract is proven.
- Runtime API: use `async-graphql` for existing diagnostics. Use `utoipa` only for documentation until contract is complete.

## Target Structure

Use this top-level shape unless a later user request changes it:

```text
Cargo.toml
rust-toolchain.toml
README.md
docs/
  architecture/
  contract/
crates/
  openplotva-app/
  openplotva-config/
  openplotva-core/
  openplotva-observability/
  openplotva-storage/
  openplotva-telegram/
  openplotva-server/
  openplotva-updates/
  openplotva-taskman/
  openplotva-dialog/
  openplotva-llm/
  openplotva-prompts/
  openplotva-history/
  openplotva-memory/
  openplotva-shield/
  openplotva-media/
  openplotva-web/
migrations/
prompts/
web/admin/
web/settings/
tools/embedder/
tools/contract-inventory/
tools/token-estimator/
tests/contract/
```

## Implementation Order

1. Scaffold private GitHub repo, Rust workspace, CI, README, and reference-snapshot docs.
2. Generate Go contract inventories: env defaults, routes, GraphQL schema, migrations, prompts, static assets, Telegram methods/types used, command strings, dialog tools, tests.
3. Build the top-level Rust shell: config, lifecycle, logging, health endpoint, static file serving, Postgres/Dragonfly connections, compose startup.
4. Port storage and migrations.
5. Port Telegram runtime around `carapax`.
6. Port taskman and processor.
7. Port prompts, dialog, and LLM providers.
8. Port memory, history, and shield behavior.
9. Port media, tools, web, translations, rates, admin/settings/runtime API.
10. Run contract harness until no unapproved deviations remain.

## Testing And Verification

- Required Rust checks before claiming completion:
  - `cargo fmt --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test --workspace`
  - Integration checks with Postgres and Dragonfly when the touched area needs them.
- Required Go baseline checks from `/Users/Shared/src/github.com/iamwavecut/reference-app`:
  - `go test ./...`
  - `go vet ./...`
- If a Go baseline check already fails, record it as baseline evidence rather than hiding it.
- Contract tests should cover prompt rendering, web asset hashes, config defaults, migrations, SQL behavior, Telegram HTML sanitizer/splitter, update replay, outbound Telegram payloads, task ordering/retry/cancel, dialog tool parsing, structured JSON salvage, history summaries, memory redaction/retrieval, Shield retrieval, runtime API auth, GraphQL, SQL, and admin/settings routes.

## Documentation

- Keep `README.md` current with local setup, required services, env vars, compose run, tests, architecture map, and future deployment notes.
- Keep architecture notes under `docs/architecture/`.
- Keep contract inventories, reference-snapshot material, baseline results, and approved deviations under `docs/contract/`.
- Keep Telegram-specific port notes in `docs/contract/telegram-port.md`.
- Regenerate machine-built contract inventories with `cargo run -p openplotva-tool-contract-inventory`; do not edit files under `docs/contract/generated/` by hand.
- Keep `migrations/` aligned with the frozen Go files in `internal/db/sql/migrations`; if the reference snapshot advances, reconvert the affected migrations and preserve source SHA comments.
- Prefer concise docs that name source-of-truth files and commands over broad architecture tours.

## Working Style

- Read before editing: inspect exports, callers, shared utilities, local docs, and the Go baseline path relevant to the change.
- Make surgical changes. Avoid speculative abstractions and adjacent cleanup.
- Match local Rust style once established.
- Prefer maintained crates over local implementations, but keep Plotva-specific orchestration, contract glue, protocol sanitization, Telegram HTML policy, scheduling, and provider-gap code local.
- Report exact verification commands and meaningful results. Say clearly when broader checks were skipped.
