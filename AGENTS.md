# OpenPlotva Agent Instructions

## Scope And Priority

- These instructions apply to `/Users/Shared/src/github.com/iamwavecut/openplotva`.
- More specific user requests, future nested `AGENTS.md` files, and source files under direct edit take priority.
- This repository is the Rust implementation of `/Users/Shared/src/github.com/iamwavecut/reference-app`.
- The Go repository is reference material only. Do not edit tracked files in `reference-app`.

## Goal

Implementation Plotva from Go to modern Rust while preserving behavior.

Done means:

- Rust app builds, runs, and passes contract checks against the frozen Go behavior.
- No user-facing behavior changes exist unless recorded as approved deviations.
- The original Go repository remains unchanged.
- README and developer docs are suitable for future open-source release.

## Reference Snapshot And Contract

- Initial observed Go reference snapshot:
  - Commit: `56506a95a749629235ecf1ea35c54d5a4172fdbd`
  - Commit time: `2026-05-19T16:46:12+02:00`
  - Subject: `Refactor everything`
- Store the active lock in `docs/contract/reference-snapshot.json`.
- Before every major milestone, compare `/Users/Shared/src/github.com/iamwavecut/reference-app` `HEAD` to the stored lock.
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
- Telegram Bot API: use `tg-rs/carapax` as the integration base. Do not use `frankenstein` unless the user reverses this decision.
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
- Prefer concise docs that name source-of-truth files and commands over broad architecture tours.

## Working Style

- Read before editing: inspect exports, callers, shared utilities, local docs, and the Go baseline path relevant to the change.
- Make surgical changes. Avoid speculative abstractions and adjacent cleanup.
- Match local Rust style once established.
- Prefer maintained crates over local implementations, but keep Plotva-specific orchestration, contract glue, protocol sanitization, Telegram HTML policy, scheduling, and provider-gap code local.
- Report exact verification commands and meaningful results. Say clearly when broader checks were skipped.
