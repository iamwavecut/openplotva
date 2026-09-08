# syntax=docker/dockerfile:1
FROM rust:1.95.0-bookworm@sha256:6258907abe69656e41cd992e0b705cdcfabcbbe3db374f92ed2d47121282d4a1
ARG TARGETARCH
LABEL openplotva.maintenance.omp="18.1.14" openplotva.maintenance.rust="1.95.0"
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 ca-certificates curl git pkg-config libssl-dev clang cmake \
    && rm -rf /var/lib/apt/lists/* \
    && rustup component add rustfmt clippy \
    && useradd --create-home --uid 1000 worker
RUN case "$TARGETARCH" in \
      amd64) asset=omp-linux-x64; checksum=27dd66cb6a2c39fff51e1fe6db40f1f376fee4a3d04457f236edaa5d0290e4d2 ;; \
      arm64) asset=omp-linux-arm64; checksum=93ba1143e95c6a138ecfaea5efa6057e45aa6e40853d88a55b48586d118f7325 ;; \
      *) exit 1 ;; \
    esac \
    && curl -fSL --retry 3 "https://github.com/can1357/oh-my-pi/releases/download/v18.1.14/$asset" -o /usr/local/bin/omp \
    && echo "$checksum  /usr/local/bin/omp" | sha256sum --check --strict \
    && chmod 0555 /usr/local/bin/omp \
    && /usr/local/bin/omp --version \
    && ln -s /work/omp /home/worker/.omp
WORKDIR /opt/source
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo fetch --locked && rm -rf /opt/source
COPY tools/maintenance/worker.py tools/maintenance/contracts.py tools/maintenance/policy.md tools/maintenance/omp-config.yml /opt/maintenance/
COPY tools/maintenance/maintenance-evidence /usr/local/bin/maintenance-evidence
RUN chmod 0555 /usr/local/bin/maintenance-evidence \
    && chmod -R a+rX /opt/maintenance /usr/local/cargo /usr/local/rustup
USER 1000:1000
WORKDIR /work/repo
CMD ["/usr/local/bin/omp", "--version"]
