# Rich Messages — app integration guide (execution plan)

Companion to `2026-06-15-rich-messages-design.md`. The reusable core is DONE + committed
(rich-HTML sanitizer, raw `RichApiClient`, uploader client + config, `RichMessenger` +
`compose_*` in `crates/openplotva-app/src/rich.rs`). This guide is the exact, ordered plan
to wire each user-facing scenario onto that core. Each scenario is an independent commit;
keep the app compiling after each.

## Shared step 0 — composition-root scaffolding (do once, first)

In `crates/openplotva-app/src/lib.rs`, after the Telegram client is built (`telegram = telegram_client_with_base_url(bot_key, &config.bot.api_base_url)`, ~line 8282), construct the rich clients + messenger once and clone an `Arc<RichMessenger>` into each scenario's effects:

```rust
let rich_api_client = openplotva_telegram::RichApiClient::with_base_url(
    bot_key,                       // same token used for `telegram`
    &config.bot.api_base_url,      // empty => https://api.telegram.org
)?;
let uploader_client = openplotva_media::uploader::UploaderClient::new(
    openplotva_media::uploader::UploaderConfig {
        base_url: config.uploader.base_url.clone(),
        secret: config.uploader.secret.clone(),
        timeout: std::time::Duration::from_secs(config.uploader.timeout_seconds.max(0) as u64),
    },
)?;
let rich_messenger = std::sync::Arc::new(
    crate::rich::RichMessenger::new(rich_api_client, uploader_client),
);
```

Then pass `Arc::clone(&rich_messenger)` into each effects constructor below.

## Known fixes vs the drafted specs

- `RichSendOptions.allow_sending_without_reply` is **`bool`**, not `Option<bool>`.
- Refer to the messenger as **`crate::rich::RichMessenger`** (not `openplotva_app::rich::…`).
- Rich sends bypass the `DispatcherQueue` (no per-chat rate-limit/persistence) — same as the
  image worker's existing direct `editMessageMedia`. Acceptable for v1; a follow-up could route
  rich through the dispatcher for parity. Streaming throttle uses `RichApiError::retry_after()`.
- `RichMessenger.send` re-sanitizes; for the dialog path (already sanitized upstream) that's
  idempotent and safe.
- **Scenarios depend on `Arc<dyn crate::rich::RichSender>`, NOT the concrete `RichMessenger`.**
  `RichSender` (in `rich.rs`) abstracts send/edit/draft/upload so handler/worker unit tests
  inject a mock instead of hitting HTTP — `RichMessenger` implements it. `send_rich` resolves to
  the message id (`i64`). Each effects struct that currently holds `store`/`queue` for sending
  should be slimmed to hold `Arc<dyn RichSender>` (drop the now-unused queue plumbing + `Store`
  generic + `with_virtual_id_factory`) to avoid dead fields; update its `::new` call sites
  (tests + composition root) accordingly. A `MockRichSender` test double exists in `rich.rs`'s
  test module — relocate it to module-level `#[cfg(test)] pub(crate)` when the first scenario
  test needs it cross-module.
- Composition root: build `let rich_sender: Arc<dyn crate::rich::RichSender> = Arc::new(RichMessenger::new(rich_api_client, uploader_client));` once and `Arc::clone` into each effects.

## Per-scenario plan

### A1 — Dialog replies (TOP priority) · `dialog_jobs.rs`
- `DialogDispatcherEffects` (struct ~516, `new` ~524): add `rich_messenger: Arc<crate::rich::RichMessenger>` field + ctor arg.
- `send_dialog_answer` (~615-656): replace the `TextMessageRequest` + `queue_text_message_parts` body with `self.rich_messenger.send(params.chat_id, answer, &RichSendOptions{ message_thread_id: params.thread_id.map(i64::from), reply_to_message_id: Some(i64::from(params.message_id)), allow_sending_without_reply: true, disable_notification: false, reply_markup: None }).await.map_err(|e| DialogDispatchEffectError::RichSend(e.to_string()))?;`
- Extend `DialogDispatchEffectError` with `RichSend(String)`.
- Composition root ~9026: pass `Arc::clone(&rich_messenger)` as 3rd arg.
- Tests: update all `DialogDispatcherEffects::new(store, queue)` call sites (+3rd arg). Contracts: sanitize-at-boundary (don't double-sanitize semantics), reply-to, thread id, dedup/staleness upstream unchanged.

### A2 — Song one message · `music_jobs.rs` (+ `delete_lyrics.rs`)
- `TelegramMusicJobEffects` (struct ~584, `new` ~591): add `rich: Arc<crate::rich::RichMessenger>`.
- `send_generated_song`: upload audio bytes via `self.rich.uploader().upload_bytes(bytes, "audio/mpeg", Some(&build_song_file_name(user_full_name, title, "mp3")))`; build `rich::compose_song_message(&SongMessage{ title, styles: &material.raw_style, audio_url: &url, lyrics: &material.lyrics, footer_html: &build_song_caption_with_support(...) })`; send ONE message via `self.rich.send(...)`. Remove `send_song_lyrics` (and its separate-message + lyrics-delete keyboard; drop or re-home the delete affordance).
- Composition root ~9279: 4th arg `Arc::clone(&rich_messenger)`.
- Tests: EffectsStub gains the field; remove `send_song_lyrics` stub/tests. Contracts: VIP gating, music-vip queue, style normalization, support/VIP links, author text, file naming via uploader explicit-name.

### A3 — Image draw rework (free + VIP) · `image_jobs.rs` (+ direct path in `dialog_messages.rs`)
- **Status (reverted 2026-07): classic album flow restored.** Rich delivery for draws was rolled back: rich-message media embeds only by external HTTPS URL, which RU-filtered clients often cannot fetch, and several official/alternative Telegram clients do not render rich messages at all. Draws now send the placeholder album upfront (`sendPhoto` for one slot, `sendMediaGroup` otherwise; caption on frame 0; `IMAGE_PLACEHOLDER_FILE_ID`), fill each frame progressively via `editMessageMedia` as streamed provider images arrive (arrival order, bytes-first with URL fallback — no uploader hop), delete unfilled trailing frames last-first on shortfall, and record delivered frames into the Redis last-generation store so `/delete_drawing` works. Image edits use the same album machinery sized by `editor.expected_image_count()`.
- `ImageJobEffects` is back to the album shape: `send_initial_placeholders`, `replace_placeholder_image`, `delete_placeholder_image`, `send_nsfw_blocked_message`, `record_last_generation`, plus the kept reaction lifecycle (`signal_draw_progress`/`clear_draw_signal`). The obligations watcher contract is unchanged (`result_message_id` = first album frame).
- The rich queue-wait message was replaced by the Go-parity ephemeral queue-position notice (position > 10, dispatcher-sent with `ephemeral_delete_after`; ETA from the taskman clean-stats estimator), hooked in `TaskmanDialogToolAdapter` and shared by the `draw_image` tool and the legacy `%draw`/`!draw` path.
- Rich messaging remains in production for music, rates, check-in, help, payments, and dialog replies (A1/A2/A4/B1).

### A4 — Currency rates table · `rates.rs`
- Add `format_rates_command_message_rich(header, &snapshot) -> String` using `rich::compose_rates_table` (rows: USD/RUB, EUR/RUB, EUR/USD, BTC/USD with `format_rate_value`/`format_rate_delta`).
- Add `RatesRichEffects { messenger: Arc<crate::rich::RichMessenger> }` implementing `RatesEffects::send_rates_text` by sending `plan.message.text` (now rich HTML) via `messenger.send` (chat from `plan.message.chat`/`reply_to.chat`, reply = `plan.reply_to.message_id`).
- Handler (~1080): call the rich formatter instead of `format_rates_command_message`.
- Composition root ~9684: use `RatesRichEffects::new(Arc::clone(&rich_messenger))`.
- Tests: update handler-test exact-text assertions (e.g. ~1481) to the rich table; the legacy `format_rates_command_message` (pub) + its snapshot tests can be removed once unused.

### B1 — Check-in leaderboard · `checkin.rs`
- `CheckinCommandDispatcherEffects` (~693/703): add `rich` field + ctor arg.
- `today_winner_with_stats_html`/`send_checkin_today_winner_with_stats` (~1304/725): keep the today-winner header on the classic path; send the yearly standings as `rich::compose_leaderboard(theme.name, rows)` via `rich.send` (rows from `get_yearly_top` + `daily_rank_title` rotation). Composition root ~9695: 3rd arg. Contracts: ranking order, rank-title rotation, win counts, theme header.

### B2 — VIP / benefits / donate · `payments.rs`
- Construct `Arc<RichMessenger>` into `SuccessfulPaymentDispatcherEffects`/`PaymentRuntimeEffects` (~8412). Render `active_vip_status_message_at` benefits as a rich list/table (expiry emphasized) via the messenger; keep **invoices/payments classic** (payloads/URLs/parse_mode unchanged). Add `rich::compose_vip_status(...)` composer. Contracts: exact benefit wording, days-left math, DD.MM.YYYY, cancel_vip callback data, invoice payloads.

### B3 — /help & /start · `help.rs`
- `HelpTextPlan` gains `use_rich: bool`; `HelpDispatcherEffects` gains `rich` field/ctor arg (~125). `send_help_text` routes rich (private) when `use_rich`. Add `render_help_message_rich`/`render_help_intro_rich` (headings + `<details>` collapsibles for secondary sections, quick-start visible). `private_help_plan`/`help_intro_plan` set `use_rich=true`. Composition root ~9858. Contracts: `{{.BotName}}` escaped, deep links, group redirect stays classic. Update the 2 template tests.

## Execution order
0 (scaffolding) → A1 dialog → A4 rates → A2 song → A3 image → B1 → B3 → B2. Build + `cargo test -p openplotva-app` per scenario; commit green. Live render validation + uploader deploy happen after (see design doc §7).
