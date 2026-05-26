#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  tools/rust-deep-gate.sh [--baseline-rev <git-rev>] [--skip-hack] [--skip-semver]

Runs slower Rust analysis checks intended for scheduled/manual use:
  - cargo hack check --workspace --feature-powerset
  - cargo semver-checks --baseline-rev <git-rev>

Defaults:
  baseline rev: origin/main

Required cargo subcommands:
  cargo install cargo-hack --locked --version 0.6.44
  cargo install cargo-semver-checks --locked --version 0.47.0
USAGE
}

export PATH="/opt/homebrew/bin:$PATH"

baseline_rev="origin/main"
run_hack=1
run_semver=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --baseline-rev)
      if [[ -z "${2:-}" ]]; then
        echo "--baseline-rev requires a value" >&2
        exit 2
      fi
      baseline_rev="$2"
      shift 2
      ;;
    --skip-hack)
      run_hack=0
      shift
      ;;
    --skip-semver)
      run_semver=0
      shift
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

require_cargo_subcommand() {
  local subcommand="$1"
  local install_hint="$2"

  if ! cargo "$subcommand" --version >/dev/null 2>&1; then
    echo "missing cargo subcommand: cargo ${subcommand}" >&2
    echo "install it with: ${install_hint}" >&2
    exit 2
  fi
}

ensure_clean_manifests() {
  local status
  status="$(git status --porcelain -- 'Cargo.toml' 'crates/*/Cargo.toml' 'tools/*/Cargo.toml')"
  if [[ -n "$status" ]]; then
    echo "refusing to run cargo-hack while Cargo manifests are dirty:" >&2
    echo "$status" >&2
    echo "commit, stash, or revert manifest edits before running this slow gate" >&2
    exit 1
  fi
}

run() {
  echo "+ $*"
  "$@"
}

if [[ "$run_hack" -eq 1 ]]; then
  require_cargo_subcommand hack "cargo install cargo-hack --locked --version 0.6.44"
  ensure_clean_manifests
  run cargo hack check --workspace --feature-powerset
else
  echo "+ skip cargo hack"
fi

if [[ "$run_semver" -eq 1 ]]; then
  require_cargo_subcommand semver-checks "cargo install cargo-semver-checks --locked --version 0.47.0"
  run cargo semver-checks --baseline-rev "$baseline_rev"
else
  echo "+ skip cargo semver-checks"
fi

echo "rust-deep-gate-ok"
