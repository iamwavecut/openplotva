# OpenPlotva

OpenPlotva is a Rust implementation of Plotva, preserving the behavior of the Go implementation in `/Users/Shared/src/github.com/iamwavecut/reference-app`.

The repository is private while the implementation is in progress. The code and docs should stay clean enough for a future open-source release.

## Current Status

- Rust workspace scaffolded with Rust 2024, Cargo resolver 3, and Rust `1.95.0`.
- Initial Go reference snapshot recorded in `docs/contract/reference-snapshot.json`.
- The Go repository is read-only reference material for this implementation.
- The first runnable Rust shell exposes `/api/health` and `/api/ready`.
- App startup enforces the Go reference snapshot by default and can optionally probe Postgres plus Redis/Dragonfly.

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
WEBAPP_HOST=127.0.0.1 WEBAPP_PORT=8080 cargo run -p openplotva-app
curl -fsS http://127.0.0.1:8080/api/health
curl -fsS http://127.0.0.1:8080/api/ready
```

The app loads `.env` like the Go implementation. The current service-spine env vars are:

| Env | Default | Notes |
| --- | --- | --- |
| `LOG_LEVEL` | `info` | Go-compatible log level; mapped into the Rust tracing filter. |
| `OPENPLOTVA_LOG_FILTER` | `openplotva=info,tower_http=info` | Rust-only tracing filter override. |
| `WEBAPP_HOST` | `0.0.0.0` | Go-compatible HTTP bind host. |
| `WEBAPP_PORT` | `8080` | Go-compatible HTTP bind port. |
| `WEBAPP_URL` | `http://127.0.0.1:8080` | Public WebApp URL. |
| `DB_POSTGRES_HOST` | `127.0.0.1` | Postgres host. |
| `DB_POSTGRES_PORT` | `5432` | Postgres port. |
| `DB_POSTGRES_USER` | `plotva` | Postgres user. |
| `DB_POSTGRES_PASSWORD` | `plotva` | Postgres password. |
| `DB_POSTGRES_DB` | `plotva` | Postgres database. |
| `DB_POSTGRES_SSL_MODE` | `disable` | Loaded for config contract; current Go startup still hardcodes `sslmode=disable`. |
| `REDIS_HOST` | `127.0.0.1` | Redis/Dragonfly host. |
| `REDIS_PORT` | `6379` | Redis/Dragonfly port. |
| `REDIS_PASSWORD` | empty | Redis/Dragonfly password. |
| `REDIS_DB` | `0` | Redis/Dragonfly DB. |
| `BOT_KEY` | empty | Go-compatible Telegram Bot API token. When set with `OPENPLOTVA_CONNECT_SERVICES=true`, the Rust shell configures bot commands and starts pending-operation, outbound dispatcher, and long-poll update producer workers. |
| `BOT_DEBUG` | `false` | Go-compatible bot debug flag, currently loaded for config contract. |
| `OPENPLOTVA_REFERENCE_SOURCE_REPOSITORY` | `/Users/Shared/src/github.com/iamwavecut/reference-app` | Read-only Go source used for lock checks. |
| `OPENPLOTVA_RUNTIME_CONTRACT_PATH` | `docs/contract/reference-snapshot.json` | Reference-snapshot JSON file. |
| `OPENPLOTVA_DISABLED_LEGACY_LOCK` | `true` | Fails startup when Go `HEAD` differs from the lock. |
| `OPENPLOTVA_CONNECT_SERVICES` | `false` | When `true`, startup connects to Postgres and Redis/Dragonfly and `/api/ready` reports them as `ok`. |
| `OPENPLOTVA_RUN_MIGRATIONS` | `false` | When `true` with `OPENPLOTVA_CONNECT_SERVICES=true`, startup applies the converted SQLx migrations. Use fresh/scratch DBs until existing Go DB migration-table compatibility is complete. |

`OPENPLOTVA_BIND_ADDR` is still accepted as a Rust-only local override for the assembled bind address. Prefer `WEBAPP_HOST` and `WEBAPP_PORT` for contract work.

## Reference Snapshot

The frozen Go behavior starts at:

- Repository: `/Users/Shared/src/github.com/iamwavecut/reference-app`
- Commit: `56506a95a749629235ecf1ea35c54d5a4172fdbd`
- Commit time: `2026-05-19T16:46:12+02:00`
- Subject: `Refactor everything`

Before each major milestone, compare the Go `HEAD` with `docs/contract/reference-snapshot.json`. If it changed, classify and port the diff before advancing the lock.

The Rust app performs this check on startup unless `OPENPLOTVA_DISABLED_LEGACY_LOCK=false` is set. Use that override only for isolated development where the Go source checkout is intentionally unavailable.

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

Current approved deviation: the Rust Telegram update queue keeps the Go Redis
key and FIFO operations, but stores zstd-compressed serde JSON envelopes over
`carapax::types::Update` instead of Go gob payloads.

## Migrations

The Rust repo carries a SQLx-compatible conversion of the frozen Go migrations under `migrations/`.

- Source of truth: `/Users/Shared/src/github.com/iamwavecut/reference-app/internal/db/sql/migrations`
- Frozen inventory: `docs/contract/generated/migrations.json`
- Conversion: each Go `sql-migrate` file is split into reversible SQLx `.up.sql` and `.down.sql` files.
- Runtime execution: `OPENPLOTVA_CONNECT_SERVICES=true OPENPLOTVA_RUN_MIGRATIONS=true BOT_KEY=... cargo run -p openplotva-app`

With `BOT_KEY` set, the current runtime shell deletes and re-registers scoped Telegram bot commands, deletes any existing webhook, and starts the long-poll update producer into `plotva:updates:queue`. It does not yet install the real fetcher update consumer route, so queued updates are preserved rather than drained by a placeholder handler.

Current caveat: SQLx records migration state in `_sqlx_migrations`, while the Go app uses `rubenv/sql-migrate`. Use the Rust migration runner on fresh or scratch databases until the existing production DB compatibility path is explicitly ported and tested.

## Baseline Checks

Rust completion gates:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Optional live Postgres storage smoke, run only against a scratch/local database:

```sh
OPENPLOTVA_TEST_POSTGRES_DSN='postgres://plotva:plotva@127.0.0.1:5432/plotva?sslmode=disable' cargo test -p openplotva-storage live_virtual_message_store_round_trips_when_postgres_dsn_is_set -- --nocapture
```

Go baseline commands from `/Users/Shared/src/github.com/iamwavecut/reference-app`:

```sh
go test ./...
go vet ./...
```

If the Go baseline already fails, record the exact failure under `docs/contract/` and keep it separate from Rust implementation regressions.
