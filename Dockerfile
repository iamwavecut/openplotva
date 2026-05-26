# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.95.0

FROM rust:${RUST_VERSION}-bookworm AS builder
WORKDIR /workspace

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY tools ./tools
COPY migrations ./migrations
COPY prompts ./prompts
COPY web ./web
COPY docs ./docs

RUN cargo build --release -p openplotva-app

FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 openplotva

COPY --from=builder /workspace/target/release/openplotva-app /usr/local/bin/openplotva-app

ENV WEBAPP_HOST=0.0.0.0 \
    WEBAPP_PORT=8080 \
    RUNTIME_API_HOST=0.0.0.0 \
    OPENPLOTVA_CONNECT_SERVICES=true \
    OPENPLOTVA_RUN_MIGRATIONS=true \
    OPENPLOTVA_CONSUME_UPDATES=false \
    OPENPLOTVA_PRODUCE_UPDATES=false

EXPOSE 8080 9091
USER openplotva

HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=12 \
  CMD curl -fsS http://127.0.0.1:8080/api/health >/dev/null || exit 1

ENTRYPOINT ["openplotva-app"]
