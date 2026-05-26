# OpenPlotva Agent Notes

## Operating Rules

- Answer the user in their language; write code, comments, docs, and commits in English unless asked otherwise.
- Verify repository facts before claiming them. Do not describe crate ownership, runtime flow, commands, or deployment state from memory.
- Favor broad product progress over auxiliary scaffolding. Avoid placeholders, provenance notes, and speculative abstractions.
- Dirty user changes are normal in this migrated repository; account for them and continue unless they directly conflict with the task. Never reset, clean, push, deploy, restart services, or implementation history unless explicitly asked in the current request.
- Never commit secrets: live `.env` values, Telegram tokens, provider keys, database dumps, Redis snapshots, private file IDs, or smoke inputs.
- Report exact verification commands and results. If checks are skipped, say so and name the risk.

## Code Style

- Prefer self-explanatory code: clear names, typed data, small cohesive functions, and explicit boundaries over comments.
- Add comments only for non-obvious intent, invariants, protocol constraints, concurrency/lifetime reasoning, or safety notes.
- Remove obsolete comments instead of updating them mechanically. Do not leave TODOs, commented-out alternatives, or release-irrelevant history in code.
- Use `cargo fmt --all` after Rust edits. Match nearby conventions before introducing new patterns.
- Use typed errors in library/domain crates; use `anyhow` at application boundaries.
- Avoid `unwrap()` in runtime code. Use `expect()` only for local, obvious invariants.
- In async code, do not hold locks across `.await`; make cancellation, queue draining, and worker lifetime explicit.

## Architecture Boundaries

- `openplotva-app` is the composition root for config wiring, runtime startup, handlers, workers, and HTTP route assembly.
- Domain crates must not depend on web, Telegram, SQLx, or vendor SDKs unless the crate owns that integration boundary.
- Declare traits and adapters close to their consumers. Extend existing crates before creating new top-level crates.
- Keep vendor request/response types inside the owning integration crate or app boundary.
- `openplotva-telegram` owns Bot API builders, transport helpers, update sources, callback helpers, HTML handling, deduplication, rate limits, and outbound persistence.
- `openplotva-storage` owns SQLx Postgres, Redis stores, embedded migrations, and persistence adapters.
- `openplotva-web` owns admin/settings assets, WebApp signatures, login/auth helpers, and cookie/signature primitives.
- Provider-specific LLM, media, memory, shield, history, payment, and queue code should stay in the crate/module that already owns that boundary.
- Prompt templates stay as `.prompt` files under `prompts/`; preserve partial names, role markers, cache-sensitive system text, and template data shape.

## External Contracts

- Treat these as public contracts: Telegram payloads and callback data, commands, HTTP routes, GraphQL/runtime API shape, DB schema meaning, Redis keys/order/TTLs, prompts, shipped web assets, provider/payment requests, and operator-visible diagnostics.
- Preserve payload shape, lifecycle behavior, ordering, TTLs, error visibility, and security checks unless the user explicitly approves a behavior change.
- Prefer the smallest Rust-native implementation that preserves the contract. Do not copy convoluted internal control flow when the external artifact is the same.
- Sanitize Telegram-visible HTML at the boundary before sending, splitting, deduping, or persisting.
- Treat Settings WebApp state, Telegram login data, payment callbacks, provider credentials, and admin/runtime tokens as security boundaries.

## Useful Checks

- Formatting: `cargo fmt --all`.
- Focused Rust tests: `cargo test -p <crate> <filter>`.
- Local/runtime smokes when relevant: `tools/local-smoke.sh`, `tools/service-smoke.sh`, `tools/provider-smoke.sh`, `tools/update-queue-smoke.sh`, `tools/live-update-injection-smoke.sh`, `tools/container-smoke.sh`.
