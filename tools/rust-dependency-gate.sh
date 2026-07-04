#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  tools/rust-dependency-gate.sh [--skip-deny]

Runs the dependency, advisory, license, and supply-chain gate:
  - cargo deny check
  - cargo audit, with a documented sqlx optional-MySQL false-positive ignore
  - cargo machete
  - cargo vet --locked, only when supply-chain/ exists

Options:
  --skip-deny  Skip cargo deny when an earlier CI step already ran it.

Required cargo subcommands:
  cargo install cargo-deny --locked --version 0.19.7
  cargo install cargo-audit --locked --version 0.22.1
  cargo install cargo-machete --locked --version 0.9.2

Optional when supply-chain/ exists:
  cargo install cargo-vet --locked
USAGE
}

skip_deny=false

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
  --skip-deny)
    skip_deny=true
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

require_cargo_subcommand() {
  local subcommand="$1"
  local install_hint="$2"

  if ! cargo "$subcommand" --version >/dev/null 2>&1; then
    echo "missing cargo subcommand: cargo ${subcommand}" >&2
    echo "install it with: ${install_hint}" >&2
    exit 2
  fi
}

run() {
  echo "+ $*"
  "$@"
}

require_cargo_subcommand audit "cargo install cargo-audit --locked --version 0.22.1"
require_cargo_subcommand machete "cargo install cargo-machete --locked --version 0.9.2"

if [[ "$skip_deny" == false ]]; then
  require_cargo_subcommand deny "cargo install cargo-deny --locked --version 0.19.7"
  run cargo deny check
else
  echo "+ skip cargo deny check"
fi
# cargo audit scans every package recorded in Cargo.lock, including sqlx's
# optional MySQL support. The workspace enables only Postgres sqlx features;
# cargo deny checks that selected feature graph above.
run cargo audit --ignore RUSTSEC-2023-0071
run cargo machete

if [[ -d supply-chain ]]; then
  require_cargo_subcommand vet "cargo install cargo-vet --locked"
  run cargo vet --locked
else
  echo "+ skip cargo vet --locked (supply-chain/ is not configured)"
fi

echo "rust-dependency-gate-ok"
