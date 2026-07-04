#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  tools/rust-fast-gate.sh [--skip-clippy]

Runs the fast blocking Rust quality gate used by CI and local development:
  - cargo fmt --all -- --check
  - cargo clippy --workspace --all-targets --all-features -- -D warnings
  - cargo test --workspace

Options:
  --skip-clippy  Skip clippy when an earlier CI step already ran the same command.
USAGE
}

skip_clippy=false

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
  --skip-clippy)
    skip_clippy=true
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
if [[ "$skip_clippy" == false ]]; then
  run cargo clippy --workspace --all-targets --all-features -- -D warnings
else
  echo "+ skip cargo clippy --workspace --all-targets --all-features -- -D warnings"
fi
run cargo test --workspace

echo "rust-fast-gate-ok"
