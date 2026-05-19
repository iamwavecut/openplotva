# OpenPlotva

OpenPlotva is a Rust implementation of Plotva, preserving the behavior of the Go implementation in `/Users/Shared/src/github.com/iamwavecut/reference-app`.

The repository is private while the implementation is in progress. The code and docs should stay clean enough for a future open-source release.

## Current Status

- Rust workspace scaffolded with Rust 2024, Cargo resolver 3, and Rust `1.95.0`.
- Initial Go reference snapshot recorded in `docs/contract/reference-snapshot.json`.
- The Go repository is read-only reference material for this implementation.
- The first runnable Rust shell exposes `/api/health`; behavior contract work is still ahead.

## Local Setup

Required local tools:

- Rust `1.95.0`
- Cargo with `rustfmt` and `clippy`
- `rg`
- Go toolchain for baseline checks in the source repository
- Postgres with `pgvector` and Dragonfly/Redis for integration work once storage is ported

Build and test the current Rust scaffold:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Regenerate contract inventories from the locked Go source:

```sh
cargo run -p openplotva-tool-contract-inventory
```

Run the current app shell:

```sh
OPENPLOTVA_BIND_ADDR=127.0.0.1:8080 cargo run -p openplotva-app
curl -fsS http://127.0.0.1:8080/api/health
```

## Reference Snapshot

The frozen Go behavior starts at:

- Repository: `/Users/Shared/src/github.com/iamwavecut/reference-app`
- Commit: `56506a95a749629235ecf1ea35c54d5a4172fdbd`
- Commit time: `2026-05-19T16:46:12+02:00`
- Subject: `Refactor everything`

Before each major milestone, compare the Go `HEAD` with `docs/contract/reference-snapshot.json`. If it changed, classify and port the diff before advancing the lock.

## Architecture Map

The intended crate layout mirrors the Go ownership map. The current navigation source for the Go baseline is `/Users/Shared/src/github.com/iamwavecut/reference-app/docs/CODEBASE_MAP.md`; verify source files when a contract decision depends on a specific behavior.

- `openplotva-app`: composition root and lifecycle wiring.
- `openplotva-config`: environment-backed configuration and validation.
- `openplotva-core`: domain primitives shared across crates.
- `openplotva-observability`: logging, tracing, and future OpenTelemetry setup.
- `openplotva-storage`: Postgres, SQLx, pgvector, migrations, and Redis/Dragonfly integration.
- `openplotva-telegram`: Telegram Bot API boundary using `tg-rs/carapax`.
- `openplotva-server`: HTTP API, health, static assets, and runtime endpoints.
- `openplotva-updates`: update ingestion and replay.
- `openplotva-taskman`: observable, persisted, retried background work.
- `openplotva-dialog`: provider-neutral dialog contracts and tool parsing.
- `openplotva-llm`: Plotva-owned provider traits and SDK adapters.
- `openplotva-prompts`: `.prompt` loading and Handlebars rendering.
- `openplotva-history`: chat history and summary cascade behavior.
- `openplotva-memory`: long-term memory extraction, redaction, and retrieval.
- `openplotva-shield`: protective retrieval.
- `openplotva-media`: image, vision, music, and file/media providers.
- `openplotva-web`: admin/settings WebApp assets and backend helpers.
- `openplotva-tool-contract-inventory`: deterministic inventory generator for the locked Go source.

More detail belongs under `docs/architecture/` as behavior is ported.

## Contract Rules

Keep these unchanged unless an approved deviation is recorded:

- User-facing strings and prompts.
- Telegram payload shapes, callback data, HTML sanitization, and splitting.
- DB schema meaning and SQL behavior.
- HTTP routes, GraphQL schema, runtime API auth, and diagnostic SQL behavior.
- Payment behavior, task ordering, retry/cancel semantics, and queue priorities.
- Admin/settings UI assets, translations, and static file hashes.

Approved deviations must be written in `docs/contract/deviations.md`.

## Baseline Checks

Rust completion gates:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Go baseline commands from `/Users/Shared/src/github.com/iamwavecut/reference-app`:

```sh
go test ./...
go vet ./...
```

If the Go baseline already fails, record the exact failure under `docs/contract/` and keep it separate from Rust implementation regressions.
