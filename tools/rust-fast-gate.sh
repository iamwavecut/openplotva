#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  tools/rust-fast-gate.sh [--skip-clippy] [--ephemeral]

Runs the fast blocking Rust quality gate used by CI and local development:
  - completed documentation records contain no unchecked tasks
  - cargo fmt --all -- --check
  - cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
  - cargo test --locked --workspace

Options:
  --skip-clippy  Skip clippy when CI runs the same command in a separate job.
  --ephemeral    Build in a disposable target directory, removed even on failure.
                 Overrides CARGO_TARGET_DIR for this invocation only.
                 Defaults to no debug data or incremental state, as in CI.

Test temporary files are always isolated and removed on exit. Normal runs reuse
Cargo's target directory; --ephemeral trades that reuse for no build leftovers.
USAGE
}

skip_clippy=false
ephemeral=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --skip-clippy)
      skip_clippy=true
      ;;
    --ephemeral)
      ephemeral=true
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if [[ -d /opt/homebrew/bin && ":$PATH:" != *":/opt/homebrew/bin:"* ]]; then
  export PATH="$PATH:/opt/homebrew/bin"
fi

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

gate_tmp="$(mktemp -d "${TMPDIR:-/tmp}/openplotva-fast-gate.XXXXXX")"
cleanup() {
  rm -rf -- "$gate_tmp"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir "$gate_tmp/tmp"
export TMPDIR="$gate_tmp/tmp"
if [[ "$ephemeral" == true ]]; then
  export CARGO_TARGET_DIR="$gate_tmp/target"
  export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"
  export CARGO_PROFILE_DEV_DEBUG="${CARGO_PROFILE_DEV_DEBUG:-0}"
fi

run() {
  echo "+ $*"
  "$@"
}

check_completed_records() {
  if [[ ! -d docs/decisions ]]; then
    echo "missing completed-record directory: docs/decisions" >&2
    return 1
  fi
  if rg -n -- '- \[ \]' docs/decisions; then
    echo "completed decision records must not contain unchecked task boxes" >&2
    return 1
  fi
}

run check_completed_records
run cargo fmt --all -- --check
if [[ "$skip_clippy" == false ]]; then
  run cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
else
  echo "+ skip cargo clippy --locked --workspace --all-targets --all-features -- -D warnings"
fi
run cargo test --locked --workspace

echo "rust-fast-gate-ok"
