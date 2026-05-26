#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  tools/rust-fast-gate.sh

Runs the fast blocking Rust quality gate used by CI and local development:
  - cargo fmt --all -- --check
  - cargo check --workspace --all-targets --all-features
  - cargo clippy --workspace --all-targets --all-features -- -D warnings
  - cargo test --workspace
USAGE
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
  "")
    ;;
  *)
    echo "unknown argument: $1" >&2
    usage >&2
    exit 2
    ;;
esac

if [[ -d /opt/homebrew/bin && ":$PATH:" != *":/opt/homebrew/bin:"* ]]; then
  export PATH="$PATH:/opt/homebrew/bin"
fi

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

run() {
  echo "+ $*"
  "$@"
}

run cargo fmt --all -- --check
run cargo check --workspace --all-targets --all-features
run cargo clippy --workspace --all-targets --all-features -- -D warnings
run cargo test --workspace

echo "rust-fast-gate-ok"
