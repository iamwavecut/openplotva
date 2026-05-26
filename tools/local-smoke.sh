#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  tools/local-smoke.sh

Starts the Rust app shell on a free localhost port with external services off,
checks core HTTP/static contracts, then stops the process.

Optional env:
  OPENPLOTVA_SMOKE_PORT      first port to try, default 18080
  OPENPLOTVA_SMOKE_LOG_DIR   log directory, default mktemp
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

export PATH="/opt/homebrew/bin:$PATH"

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

port="${OPENPLOTVA_SMOKE_PORT:-18080}"
while nc -z 127.0.0.1 "$port" >/dev/null 2>&1; do
  port=$((port + 1))
done

base_url="http://127.0.0.1:${port}"
log_dir="${OPENPLOTVA_SMOKE_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/openplotva-smoke.XXXXXX")}"
log_file="${log_dir}/openplotva-app.log"
health_file="${log_dir}/health.json"
ready_file="${log_dir}/ready.json"
state_file="${log_dir}/admin-state.json"

pid=""
cleanup() {
  if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "+ start openplotva-app at ${base_url}"
env \
  OPENPLOTVA_BIND_ADDR="127.0.0.1:${port}" \
  WEBAPP_HOST="127.0.0.1" \
  WEBAPP_PORT="${port}" \
  WEBAPP_URL="${base_url}" \
  OPENPLOTVA_CONNECT_SERVICES="false" \
  OPENPLOTVA_RUN_MIGRATIONS="false" \
  OPENPLOTVA_CONSUME_UPDATES="false" \
  RUNTIME_API_ENABLED="false" \
  OPENPLOTVA_LOG_FILTER="${OPENPLOTVA_LOG_FILTER:-openplotva=warn,tower_http=warn}" \
  cargo run -p openplotva-app >"$log_file" 2>&1 &
pid="$!"

wait_for_health() {
  for _ in $(seq 1 120); do
    if curl -fsS "${base_url}/api/health" >"$health_file" 2>/dev/null; then
      return 0
    fi
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      echo "app exited before health check passed" >&2
      tail -n 80 "$log_file" >&2 || true
      return 1
    fi
    sleep 0.5
  done
  echo "timed out waiting for /api/health" >&2
  tail -n 80 "$log_file" >&2 || true
  return 1
}

expect_body_contains() {
  local file="$1"
  local needle="$2"
  if ! grep -Fq "$needle" "$file"; then
    echo "expected ${file} to contain ${needle}" >&2
    cat "$file" >&2
    exit 1
  fi
}

status_code() {
  curl -sS -o /dev/null -w '%{http_code}' "${base_url}${1}"
}

expect_status() {
  local path="$1"
  local expected="$2"
  local got
  got="$(status_code "$path")"
  if [[ "$got" != "$expected" ]]; then
    echo "expected ${path} -> ${expected}, got ${got}" >&2
    tail -n 80 "$log_file" >&2 || true
    exit 1
  fi
  echo "+ ${path} ${got}"
}

wait_for_health
expect_body_contains "$health_file" '"status":"ok"'
expect_body_contains "$health_file" '"service":"openplotva"'
echo "+ /api/health ok"

curl -fsS "${base_url}/api/ready" >"$ready_file"
expect_body_contains "$ready_file" '"status":"ok"'
echo "+ /api/ready ok"

expect_status "/settings" "301"
expect_status "/settings/" "200"
expect_status "/admin/login.html" "200"
expect_status "/admin/api/state" "200"

curl -fsS "${base_url}/admin/api/state" >"$state_file"
expect_body_contains "$state_file" '"log_level":"info"'
expect_body_contains "$state_file" '"queue":'
expect_body_contains "$state_file" '"aifarm_pool_enabled":'

echo "local-smoke-ok"
echo "log: ${log_file}"
