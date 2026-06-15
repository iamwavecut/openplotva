# Rich Messages migration — design & implementation plan

Date: 2026-06-15. Status: APPROVED for phases A + B (C/D deferred). Worktree feature; merges to `main` later.

## 1. Goal

Migrate OpenPlotva's user-facing output to Telegram **Rich Messages** (Bot API 10.1, 2026-06-11) and add **virtual streaming**. All messages Plotva sends become rich; galleries, songs, and the image-drawing flow become structured rich messages; deterministic tabular output (currency rates, leaderboards) uses native rich blocks instead of whitespace/markup emulation.

## 2. Verified API facts (primary source: https://core.telegram.org/bots/api, fetched 2026-06-15)

Authoritative details live in memory `telegram-rich-messages-bot-api-10-1`. Summary:

- **`sendRichMessage`**: `rich_message: InputRichMessage` (exactly one of `html`/`markdown`; we use **`html`**). Works in any chat (incl. groups), supports `reply_markup`, `reply_parameters`. Returns a `Message`.
- **`sendRichMessageDraft`**: STREAMING, **private chat only** (`chat_id` = private). Ephemeral **30s** preview; reusing the same non-zero `draft_id` **animates** transitions; returns `True` (no message id). Must finalize with `sendRichMessage`.
- **`editMessageText`** gained `rich_message` → can convert a text-only rich message into one with media and grow a slideshow in place (VERIFIED live: text → 1 img → slideshow(2) → slideshow(3)+caption all via `editMessageText`).
- **Media is HTTPS-URL only** in rich content (`<img>/<audio>/<video>`, `<tg-slideshow>/<tg-collage>`). No `file_id`, no `attach://`. Telegram fetches & re-hosts at send time — the URL only needs reachability at that moment.
- **Limits**: 32768 chars · 500 blocks · 16 nesting levels · 50 media · 20 table columns. Named-entity whitelist: `&lt; &gt; &amp; &quot; &apos; &nbsp; &hellip; &mdash; &ndash; &lsquo; &rsquo; &ldquo; &rdquo;` + numeric.
- **Block tags**: `<h1>-<h6>`, `<p>`, `<table bordered striped><caption><tr><td colspan rowspan align valign>`, `<details open><summary>`, `<blockquote><cite>`, `<aside>` (pull quote), `<ul>/<ol>` incl. `<li><input type=checkbox>`, `<footer>`, `<hr/>`, `<pre><code class=language-x>`, `<tg-slideshow>`, `<tg-collage>`, `<figure>/<figcaption><cite>`, `<tg-math-block>`, `<tg-map>`, `<tg-thinking>` (draft-only). Inline: `<b><i><u><s><code><mark><sub><sup><tg-spoiler><a><tg-emoji><tg-time><tg-math><tg-reference>`.
- Rich `<audio src=...mp3>` = music block; `.ogg` = voice note. No native title/performer attribute (display name derives from the URL filename / file).
- `carapax 0.38` / `tgbot 0.46` do NOT know Rich Messages → implement as **raw Bot API calls** through the existing reqwest client.

## 3. Decisions

### In scope now — A (core) + B (strong candidates)

**A (confirmed core):**
1. **Dialog messages → rich** (top priority). Prompt-refinement for "when to use rich, no overuse" is a SEPARATE next iteration.
2. **Song → one rich message**: `<audio>` + styles + lyrics in `<details>`/`<tg-spoiler>` + author in `<footer>`. Removes the separate lyrics message and its "🗑" button.
3. **Image draw (free + VIP) → one rich message**: grey placeholder ("thinking / queue", may use animated emoji) → in-place edits, slideshow 1→N with "N of M" progress → final gallery. Eliminates placeholder media-groups and stickers. 1..∞ images via slideshow.
4. **Currency rates (`$`/`;`) → rich `<table>`** instead of whitespace pseudo-grid.

**B (approved):**
5. Check-in yearly leaderboard → table with 🥇🥈🥉, long year collapsed in `expandable`/`<details>`.
6. `/help`, `/start` → headings + collapsible `<details>` for secondary sections, quick-start visible.
7. VIP status / benefits / donate → heading + checklist list, expiry emphasized, benefits as table.
8. Single-photo and image-edit captions → unified style with the gallery.
9. Song track name → preserve "Author — Title" via the uploader's explicit-filename mode (rich `<audio>` has no native title/performer).
10. Queue/progress/errors for draw & song → grey text/`<footer>` inside the rich placeholder, not separate messages.

### Deferred — C (admin/internal) and D (grow-with-prompts), revisit later

- **C**: 11) admin queue stats/status → tables + expandable; 12) admin help catalog → `<details>` per category; 13) runtime-token list → table; 14) `/debug` keep `<pre><code>` (low prio); 15) new-member greeting → rich list with mentions.
- **D** (with prompt refinement): 16) sources/reasoning disclosure via `<details>` + `<tg-reference>`; 17) weather/web_search → structured tables; 18) math via `<tg-math>`/`<tg-math-block>`; 19) checklists via `<ul><input type=checkbox>`.

### Leave plain (anti-overuse)

`/ping`, short confirmations (frame/lyrics delete), callback toasts, error toasts.

### Cross-cutting decisions

- **Media hosting**: standalone minimal **Go uploader** at `plotva.geta.moe` (own private GitHub repo, minimal Dockerfile, docker-compose). Accepts bytes / remote URL / data-url. Protected by a shared secret (in Plotva's env on the same server). Stores under web-served dir. **Naming**: default = XID-prefixed name + original lowercase extension; **explicit-name mode** keeps a caller-provided filename (used for song audio "Author — Title.mp3" so the inline player shows it).
- **Streaming**: animated `sendRichMessageDraft` only in private chats; groups use `editMessageText(rich_message)` pseudo-progress (throttled). `<tg-thinking>` only in private drafts.
- **Edit-rate / 429**: add per-(chat,message) edit throttle (~1 update/sec/chat) and honor `ResponseParameters.retry_after`; suppress "message is not modified" 400s.

## 4. Architecture

### 4.1 geta.moe uploader (separate repo)

- Single Go file + `Dockerfile` + `docker-compose.yml` + README. Domain `plotva.geta.moe`.
- `POST /upload` (auth: `Authorization: Bearer <secret>` or `X-Upload-Secret`). Body accepts: multipart file, raw bytes (Content-Type drives extension), JSON `{url}` (server fetches), JSON `{data_url}`. Optional `name` field (or `?name=`) → explicit-filename mode.
- Naming: explicit `name` → sanitized as-is (lowercase ext preserved); else `<xid>.<lower-ext>` where ext inferred from MIME/URL/data-url.
- Serves files at `GET https://plotva.geta.moe/<file>`. Disk storage; retention configurable (default keep; Telegram re-hosts so long-term retention optional).
- Response: `{ "url": "https://plotva.geta.moe/<file>", "name": "<file>" }`.
- Limits: max body size, allowed MIME prefixes (image/, audio/, video/), secret required.

### 4.2 Rust foundation (in openplotva-telegram + openplotva-app)

- **Raw rich methods** (`openplotva-telegram/src/transport.rs` + `outbound.rs`): new `TelegramOutboundMethod` variants for `sendRichMessage`, `sendRichMessageDraft`, `editMessageText(rich_message)` executed as raw JSON POST via the existing `reqwest` client (carapax lacks them). Builders mirror existing `build_*_method` shape; persistence payloads added.
- **Rich-HTML sanitizer** (`openplotva-telegram/src/rich_html.rs`, new — separate from classic `html.rs`): html5ever-based; rich tag allowlist; rich named-entity whitelist; attribute policy (src https-only, table/list/details attrs, figcaption/cite); structural validation (block nesting, 500/16/50/20 limits); media-URL validation. Reuse classic `html.rs` escaping primitives where safe.
- **Uploader client** (`openplotva-media` or `openplotva-app`): `upload_media(bytes|url|data_url, explicit_name: Option<String>) -> Url`. Reads base URL + secret from config/env. Used by image/song/dialog media paths to obtain rich `<img>/<audio>` src.
- **Edit throttle + 429** (`openplotva-telegram/src/rate_limit.rs` + `transport.rs`): per-(chat,message) min-interval; classify 429 → honor `retry_after`; classify "message is not modified" → no-op.
- **Rich send wrapper / streaming helper** (`openplotva-app`): orchestrates private draft loop (thinking → partial → finalize) vs group edit loop; reuses the existing virtual-message + taskman message-id persistence so a worker can hold and edit the message.

## 5. Contracts to preserve (per scenario)

- **Song**: VIP-only gating; music-vip queue (HIGHEST_PRIORITY, max 2/user, queue-position notice when depth+1>3); style = 3-7 normalized English tags; lyrics min-structure; caption support/VIP links; author-gated lyrics delete still reachable (moves into the rich message — re-home the delete affordance or keep as inline button on the rich message).
- **Image**: caption template (prompt as `<blockquote expandable>` >25 words else `<code>`, author, VIP 👑 link, `#nsfw 18+`, support link); ≤1024 visible caption; queue routing VIP vs regular; NSFW Forbidden short-circuit + block message; aspect-ratio handling. Album item count was 1..10 — slideshow now allows 1..N (still cap sensibly).
- **Rates**: trigger (`$`/`;`), exact pairs (USD/RUB, EUR/RUB, EUR/USD cross, BTC/USD), precision (2/4/0), trend-emoji thresholds, dialog-tool ToolResult shape. NOTE: snapshot tests at rates.rs L1483/L1539/L1716 pin exact bytes — update them; confirm rendered appearance (not Go byte-parity) is the real contract.
- **All**: sanitize Telegram-visible HTML at the boundary; escape dynamic values; per-chat rate limiting; message-id persistence for edits.

## 6. Phased implementation plan

**Phase 0 — artifacts (this doc + memory).**

**Phase 1 — foundation:**
- 1a. geta.moe uploader service (code + Docker + compose + README) in its own dir/repo.
- 1b. Rich-HTML sanitizer module (TDD: tag allowlist, entity whitelist, media-url, limits, nesting).
- 1c. Raw rich methods in transport/outbound (+ persistence payloads).
- 1d. Uploader client + config/env wiring.
- 1e. Edit throttle + 429/`retry_after` + "not modified" handling.
- 1f. Rich send/stream helper (private draft vs group edit), reusing virtual-message + taskman id persistence.

**Phase 2 — A scenarios:** dialog rich output → song → image-draw streaming → currency table.

**Phase 3 — B scenarios:** leaderboard → help/start → VIP/donate → unified single-photo/image-edit captions → audio naming.

Each phase: `cargo fmt --all`, focused `cargo test -p <crate>`, and live validation against the test bot.

## 7. Risks / open items

- Old-client rendering of rich messages — validate empirically; decide fallback if needed.
- Whether draft updates count against rate limits — verify in prod before tuning cadence.
- Uploader deployment (DNS for plotva.geta.moe, secret provisioning, server compose) requires operator action — code is ready; deploy is handed off.
- Song lyrics-delete affordance re-homing when lyrics merge into the audio message.
- Re-evaluate C/D after A/B land.

## 8. Verification

Live test bot (`@peasdabot`) used throughout for real-Telegram rendering; token kept outside the repo. Confirmed working so far: `sendRichMessage` (table/slideshow/song), `sendRichMessageDraft` streaming (private), `editMessageText(rich_message)` text→media→grow.
