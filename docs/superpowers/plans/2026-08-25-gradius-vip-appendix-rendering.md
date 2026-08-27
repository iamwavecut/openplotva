# Gradius Dialogue Advertising Implementation Plan

**Goal:** Add privacy-preserving Gradius dialogue ads for eligible free private-chat users, render provider Markdown as safe Telegram HTML, append a deterministic Plotva VIP spoiler, and enforce product-side pacing and caps.

**Architecture:** `openplotva-llm` owns the Gradius protocol and existing privacy/synthetic-ID primitives. `openplotva-telegram` owns Markdown-to-Telegram-HTML conversion. `openplotva-storage` owns the transactionally reserved attempt and impression ledger. `openplotva-app` composes eligibility, fail-closed privacy redaction, provider calls, deterministic appendix rendering, and final Telegram delivery.

**Spec:** `https://docs.gradius.pro/llms.txt` and the current task conversation. Gradius content remains provider-owned and its registered links remain unchanged; the Plotva VIP appendix is separate application content.

## Global Constraints

- Do not edit `crates/openplotva-llm/src/gradius_vip_hints.rs` or rewrite any phrase.
- Never send raw user or assistant text to Gradius. Any redaction, configuration, storage, parsing, or rendering failure skips the ad.
- Derive deterministic irreversible Gradius chat/user IDs with the existing `GradiusSyntheticIds` implementation.
- Support private chats and non-VIP users only.
- First eligible answer: after three completed answers in the interaction, or five minutes from interaction start. Reset the interaction after 30 minutes of inactivity.
- Enforce a rolling user cap of ten shown ads per 24 hours and a one-hour minimum user gap. Do not rate-limit provider attempts after no-ad responses or provider errors; neither outcome consumes the impression cap.
- Let one returned ad hold the delivery slot while its durable Telegram batch is active. Reconcile missed terminal callbacks from the outbox and release an orphaned pre-enqueue slot after 15 minutes.
- Do not add a per-chat cap until product policy is decided.
- Treat `content.content` as Markdown and preserve its HTTP(S)/Telegram redirect links. Reject unsafe URLs and raw HTML instead of weakening Telegram output safety.
- Because dashboard placement is `end`, accept an ad only when Gradius reports an end insertion index for the redacted assistant text. Never map arbitrary redacted offsets back into original HTML.
- Append the ad and VIP appendix as an atomic tail. If the Telegram limit requires splitting, keep the complete tail in the final message part.
- Replace exact `VIP` words in the selected phrase with the `https://t.me/PlotvoBot?start=vip` link, escape the rest, and wrap the complete phrase in `<tg-spoiler>`.
- Any text message containing the ad or VIP appendix disables link previews in immediate-dispatch and durable-outbox paths.
- Keep all changes local; do not commit, push, open a PR, or deploy without explicit authorization.

## Task 1: Gradius protocol client

- [x] Write RED tests for the exact dialogue URL/query, `Auth` header, JSON body, user/assistant roles, empty responses, unknown ad types, malformed responses, and non-success statuses.
- [x] Implement typed request/response/error types and a testable reqwest transport in `openplotva-llm`.
- [x] Verify focused protocol tests are GREEN without logging the API key or provider payloads.

## Task 2: Markdown to Telegram HTML

- [x] Write RED tests for links, emphasis, strikeout, inline/fenced code, blockquotes, lists, line breaks, escaping, raw HTML rejection, and unsafe schemes.
- [x] Implement event-based conversion in `openplotva-telegram` and sanitize/validate the result at the Telegram boundary.
- [x] Verify focused converter tests are GREEN and URLs are neither rewritten nor synthesized.

## Task 3: Configuration and privacy composition

- [x] Write RED config tests for disabled-by-default Gradius settings and environment overrides.
- [x] Add server-side Gradius API/base-URL/timeout settings without persisting or exposing the key.
- [x] Compose the Gradius redactor from the existing privacy-filter settings independently of whether memory consolidation redaction is enabled.
- [x] Ensure missing or unavailable privacy infrastructure disables Gradius fail-closed.

## Task 4: Transactional eligibility and persistence

- [x] Add migration 183 up/down files for a per-dialog-job event ledger and the indexes needed by rolling caps and interaction lookup.
- [x] Write RED storage tests for interaction count/time eligibility, 30-minute reset, ten-per-24h cap, one-hour gap, unrestricted back-to-back provider attempts, idempotent reservation, no-ad/error accounting, and saved-ad replay.
- [x] Implement per-user advisory locking and idempotent reservation/finalization in `openplotva-storage`.
- [x] Record only internal IDs/timestamps plus provider ad output needed for retry; never persist unredacted dialogue text.

## Task 5: VIP appendix and preview suppression

- [x] Write RED renderer tests for exact `VIP` replacement, hyphen boundaries, Unicode-safe escaping, and deterministic phrase selection.
- [x] Implement the application renderer while leaving the supplied catalog byte-for-byte unchanged.
- [x] Write RED tests for immediate and durable text sends with `contains_advertising`.
- [x] Implement one effective preview policy: existing suppression OR advertising suppression.

## Task 6: Dialogue runtime integration

- [x] Write RED orchestration tests covering private/free eligibility, verified VIP exclusion, synthetic IDs, user-then-assistant call order, two fail-closed redactions, no-ad/error behavior, end-index validation, saved-ad retry, and advertising tail assembly.
- [x] Add a verified VIP lookup that preserves the existing public behavior but returns errors to the advertising path so it can fail closed.
- [x] Wire the Gradius service into `SessionWorkerWiring` and the final-answer path.
- [x] Append Markdown-rendered ad HTML plus the deterministic VIP spoiler to the final answer and mark it as advertising for delivery.

## Task 7: Verification

- [x] Prove the phrase catalog hash remains `6c9d629fd01d9c4c22fde24038887847f5907b935141287b4740a59b6c5c02fb` and inspect its focused diff.
- [x] Run `cargo fmt --all`, focused crate tests, and migration checks.
- [x] Run `cargo clippy --workspace --all-targets -- -D warnings` and `git diff --check`.
- [x] Report local implementation, exact verification, provider idempotency limitations, and anything not delivered externally.

The strict workspace clippy command and the complete workspace test suite pass on
the repository-pinned Rust 1.95.0 toolchain.
