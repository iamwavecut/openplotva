# OpenPlotva

OpenPlotva is a Rust Telegram bot and web service runtime.

It provides:

- Telegram update ingestion, outbound dispatch, command handling, callbacks, inline queries, and payment flows.
- Admin and settings web applications.
- Dialog, memory, shield, history-summary, search, media, image, vision, and music provider integrations.
- Postgres, Redis/Dragonfly, SQLx migrations, runtime diagnostics, and optional GraphQL runtime API.

## Requirements

- Rust 1.95.0
- Docker with Compose for local Postgres and Dragonfly
- PostgreSQL with pgvector for persistent deployments
- Redis-compatible storage, such as Dragonfly or Redis

## Local development

Start disposable services:

```sh
docker compose up -d postgres dragonfly
```

Run the app:

```sh
WEBAPP_HOST=127.0.0.1 WEBAPP_PORT=8080 cargo run -p openplotva-app
```

Useful local checks:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
tools/local-smoke.sh
tools/service-smoke.sh
tools/provider-smoke.sh
tools/update-queue-smoke.sh
```

Build the runtime image:

```sh
docker build -t openplotva:local .
docker compose up --build openplotva
```

## Configuration

The app reads `.env` files and environment variables. Important groups:

- `WEBAPP_*` for HTTP binding and public WebApp URL.
- `DB_POSTGRES_*` for Postgres.
- `REDIS_*` for Redis or Dragonfly.
- `BOT_*` for Telegram Bot API configuration.
- `ADMINS_ADMIN_IDS` for administrative Telegram users.
- `RUNTIME_API_*` for the optional diagnostic API.
- `PERSISTENT_QUEUE_*` for worker and queue behavior.
- `DISCOVERY_*`, `DIALOG_*`, `GOOGLEAI_*`, `OPENROUTER_*`, `ACESTEP_*`, `MEMORY_*`, `SHIELD_*`, `VISION_*`, and `SERPER_*` for provider integrations.

Service connections are opt-in by default for local shell runs:

```sh
OPENPLOTVA_CONNECT_SERVICES=true OPENPLOTVA_RUN_MIGRATIONS=true cargo run -p openplotva-app
```

Telegram update production and consumption can be controlled independently:

```sh
OPENPLOTVA_PRODUCE_UPDATES=false OPENPLOTVA_CONSUME_UPDATES=true cargo run -p openplotva-app
```

## Repository layout

- `crates/openplotva-app`: application composition root and runtime workers.
- `crates/openplotva-config`: environment-backed configuration.
- `crates/openplotva-core`: shared domain primitives.
- `crates/openplotva-dialog`: dialog types, history shaping, tools, and parsing.
- `crates/openplotva-history`: chat history and summary support.
- `crates/openplotva-llm`: LLM provider clients and retry classification.
- `crates/openplotva-media`: image, vision, and music provider clients.
- `crates/openplotva-memory`: memory extraction and retrieval.
- `crates/openplotva-observability`: tracing and log buffering.
- `crates/openplotva-prompts`: prompt loading and rendering.
- `crates/openplotva-server`: HTTP, readiness, and runtime API types.
- `crates/openplotva-shield`: protective retrieval and safety checks.
- `crates/openplotva-storage`: Postgres, Redis, migrations, and persistence stores.
- `crates/openplotva-taskman`: background job data structures and queues.
- `crates/openplotva-telegram`: Telegram Bot API transport and outbound payloads.
- `crates/openplotva-updates`: update queue codec and helpers.
- `crates/openplotva-web`: admin/settings WebApp helpers and embedded assets.
- `tools`: local smoke checks and auxiliary service binaries.
