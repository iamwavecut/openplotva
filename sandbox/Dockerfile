# syntax=docker/dockerfile:1.7
# Python sandbox image: nsjail, CPython with the packages from
# requirements.txt, and the openplotva-sandbox supervisor.
# Build from the repository root: docker build -f sandbox/Dockerfile .
ARG PYTHON_IMAGE=python:3.14.8-slim-trixie
ARG RUST_VERSION=1.99.0

FROM debian:trixie-slim AS nsjail-build
ARG NSJAIL_REF=3.6
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bison ca-certificates flex g++ git libnl-route-3-dev libprotobuf-dev \
        make pkg-config protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --branch "${NSJAIL_REF}" https://github.com/google/nsjail.git /src/nsjail \
    && make -C /src/nsjail -j"$(nproc)"

FROM ${PYTHON_IMAGE} AS venv
COPY sandbox/requirements.txt /tmp/requirements.txt
RUN python -m venv /opt/sandbox/venv \
    && /opt/sandbox/venv/bin/pip install --no-cache-dir --no-deps -r /tmp/requirements.txt \
    && /opt/sandbox/venv/bin/pip check \
    && /opt/sandbox/venv/bin/pip uninstall -y pip

FROM ${PYTHON_IMAGE} AS base
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg fontconfig fonts-dejavu-core fonts-liberation graphviz libcairo2 \
        libnl-route-3-200 libprotobuf32t64 \
    && rm -rf /var/lib/apt/lists/* \
    && fc-cache -f
COPY --from=nsjail-build /src/nsjail/nsjail /usr/sbin/nsjail
COPY --from=venv /opt/sandbox/venv /opt/sandbox/venv
# Matplotlib rebuilds its font list when its config dir is read-only, so the
# prebuilt copy here is copied into each session home.
RUN MPLCONFIGDIR=/opt/sandbox/matplotlib /opt/sandbox/venv/bin/python -c "import matplotlib.font_manager"
COPY sandbox/nsjail/python.cfg /etc/sandbox/python.cfg
EXPOSE 8090
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=6 \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8090/health', timeout=2)"
# The supervisor mounts session tmpfs homes and creates nsjail cgroups, so it
# runs as root; every script runs as uid 65534 inside nsjail.
# nosemgrep: dockerfile.security.missing-user-entrypoint.missing-user-entrypoint
ENTRYPOINT ["openplotva-sandbox"]

FROM rust:${RUST_VERSION}-bookworm AS builder
WORKDIR /workspace
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY migrations ./migrations
COPY prompts ./prompts
COPY web ./web
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/target \
    cargo build --locked --release -p openplotva-sandbox \
    && cp /workspace/target/release/openplotva-sandbox /tmp/openplotva-sandbox

FROM base AS runtime
COPY --from=builder /tmp/openplotva-sandbox /usr/local/bin/openplotva-sandbox

FROM base AS runtime-prebuilt
COPY target/release/openplotva-sandbox /usr/local/bin/openplotva-sandbox
