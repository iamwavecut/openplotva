#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  tools/rust-sast-gate.sh

Runs local SAST checks:
  - semgrep scan --config=auto .
  - optional local CodeQL when the codeql CLI is present or explicitly required

Optional env:
  OPENPLOTVA_RUST_SAST_CODEQL=0        never run local CodeQL
  OPENPLOTVA_RUST_SAST_CODEQL=1        require local CodeQL and fail if missing
  OPENPLOTVA_CODEQL_DB_DIR             CodeQL database path, default target/codeql/openplotva-rust
  OPENPLOTVA_CODEQL_SARIF              CodeQL SARIF output, default target/codeql/openplotva-rust.sarif
  OPENPLOTVA_CODEQL_QUERY_SUITE        CodeQL query suite, default rust-security-and-quality.qls
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

require_command() {
  local command="$1"
  local install_hint="$2"

  if ! command -v "$command" >/dev/null 2>&1; then
    echo "missing command: ${command}" >&2
    echo "install it with: ${install_hint}" >&2
    exit 2
  fi
}

is_truthy() {
  case "${1:-}" in
    1|true|TRUE|True|yes|YES|Yes|on|ON|On)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

run() {
  echo "+ $*"
  "$@"
}

require_command semgrep "brew install semgrep or pipx install semgrep"
run semgrep scan --config=auto .

case "${OPENPLOTVA_RUST_SAST_CODEQL:-auto}" in
  0|false|FALSE|False|no|NO|No|off|OFF|Off)
    echo "+ skip local CodeQL (OPENPLOTVA_RUST_SAST_CODEQL=0)"
    ;;
  *)
    if command -v codeql >/dev/null 2>&1; then
      db_dir="${OPENPLOTVA_CODEQL_DB_DIR:-target/codeql/openplotva-rust}"
      sarif_file="${OPENPLOTVA_CODEQL_SARIF:-target/codeql/openplotva-rust.sarif}"
      query_suite="${OPENPLOTVA_CODEQL_QUERY_SUITE:-rust-security-and-quality.qls}"
      mkdir -p "$(dirname "$db_dir")" "$(dirname "$sarif_file")"
      run codeql database create "$db_dir" --language=rust --source-root "$repo_root" --overwrite
      run codeql database analyze "$db_dir" "$query_suite" --format=sarif-latest --output "$sarif_file"
      echo "+ CodeQL SARIF: ${sarif_file}"
    elif is_truthy "${OPENPLOTVA_RUST_SAST_CODEQL:-auto}"; then
      echo "codeql is required because OPENPLOTVA_RUST_SAST_CODEQL is truthy" >&2
      exit 2
    else
      echo "+ skip local CodeQL (codeql CLI is not installed)"
    fi
    ;;
esac

echo "rust-sast-gate-ok"
