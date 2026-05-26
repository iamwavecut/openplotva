#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  tools/container-smoke.sh

Builds and boots the release container through compose with disposable
Postgres/Dragonfly services, verifies HTTP health/readiness, and proves SQLx
migrations ran inside the packaged app service.

Optional env:
  OPENPLOTVA_CONTAINER_SMOKE_LOG_DIR      log directory, default OPENPLOTVA_SMOKE_LOG_DIR or mktemp
  OPENPLOTVA_CONTAINER_SMOKE_PROJECT      compose project name, default unique openplotva-container-smoke-*
  OPENPLOTVA_CONTAINER_SMOKE_WAIT_SECONDS health wait timeout, default 180
  OPENPLOTVA_CONTAINER_SMOKE_KEEP=1       keep containers/volumes for debugging
  OPENPLOTVA_CONTAINER_SMOKE_APP_PORT     host app port, default free port
  OPENPLOTVA_CONTAINER_SMOKE_RUNTIME_PORT host runtime API port, default free port
  OPENPLOTVA_CONTAINER_SMOKE_POSTGRES_PORT host Postgres port, default free port
  OPENPLOTVA_CONTAINER_SMOKE_REDIS_PORT   host Dragonfly port, default free port
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

export PATH="/opt/homebrew/bin:$PATH"

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

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

free_port() {
  python3 - <<'PY'
import socket
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

log_dir="${OPENPLOTVA_CONTAINER_SMOKE_LOG_DIR:-${OPENPLOTVA_SMOKE_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/openplotva-container-smoke.XXXXXX")}}"
mkdir -p "$log_dir"

project="${OPENPLOTVA_CONTAINER_SMOKE_PROJECT:-openplotva-container-smoke-$(date +%s)-$$}"
compose=(docker compose -p "$project")

export DB_POSTGRES_USER="${DB_POSTGRES_USER:-plotva}"
export DB_POSTGRES_PASSWORD="${DB_POSTGRES_PASSWORD:-plotva}"
export DB_POSTGRES_DB="${DB_POSTGRES_DB:-plotva}"
export REDIS_DB="${REDIS_DB:-0}"
export OPENPLOTVA_DEV_APP_PORT="${OPENPLOTVA_CONTAINER_SMOKE_APP_PORT:-$(free_port)}"
export OPENPLOTVA_DEV_RUNTIME_API_PORT="${OPENPLOTVA_CONTAINER_SMOKE_RUNTIME_PORT:-$(free_port)}"
export OPENPLOTVA_DEV_POSTGRES_PORT="${OPENPLOTVA_CONTAINER_SMOKE_POSTGRES_PORT:-$(free_port)}"
export OPENPLOTVA_DEV_REDIS_PORT="${OPENPLOTVA_CONTAINER_SMOKE_REDIS_PORT:-$(free_port)}"
export WEBAPP_URL="${WEBAPP_URL:-http://127.0.0.1:${OPENPLOTVA_DEV_APP_PORT}}"
export RUNTIME_API_ENABLED="${RUNTIME_API_ENABLED:-false}"

base_url="http://127.0.0.1:${OPENPLOTVA_DEV_APP_PORT}"
health_file="${log_dir}/health.json"
ready_file="${log_dir}/ready.json"
migrations_file="${log_dir}/sqlx-migrations.count"

dump_logs() {
  echo "+ compose project: ${project}" >&2
  "${compose[@]}" ps >&2 || true
  "${compose[@]}" logs --no-color --tail=200 openplotva postgres dragonfly >&2 || true
}

cleanup() {
  if ! is_truthy "${OPENPLOTVA_CONTAINER_SMOKE_KEEP:-0}"; then
    "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  else
    echo "+ kept compose project ${project}" >&2
  fi
}

trap 'status=$?; if [[ "$status" -ne 0 ]]; then dump_logs; fi; cleanup; exit "$status"' EXIT

expect_body_contains() {
  local file="$1"
  local needle="$2"
  if ! grep -Fq "$needle" "$file"; then
    echo "expected ${file} to contain ${needle}" >&2
    cat "$file" >&2
    exit 1
  fi
}

wait_for_health() {
  local wait_seconds="${OPENPLOTVA_CONTAINER_SMOKE_WAIT_SECONDS:-180}"
  for _ in $(seq 1 "$wait_seconds"); do
    if curl -fsS "${base_url}/api/health" >"$health_file" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for ${base_url}/api/health" >&2
  return 1
}

echo "+ compose build/up release container at ${base_url}"
"${compose[@]}" up -d --build openplotva

wait_for_health
expect_body_contains "$health_file" '"status":"ok"'
expect_body_contains "$health_file" '"service":"openplotva"'
echo "+ /api/health ok"

curl -fsS "${base_url}/api/ready" >"$ready_file"
expect_body_contains "$ready_file" '"status":"ok"'
echo "+ /api/ready ok"

"${compose[@]}" exec -T -e PGPASSWORD="$DB_POSTGRES_PASSWORD" postgres \
  psql -U "$DB_POSTGRES_USER" -d "$DB_POSTGRES_DB" -Atc 'select count(*) from _sqlx_migrations;' \
  >"$migrations_file"
migration_count="$(tr -d '[:space:]' <"$migrations_file")"
if ! [[ "$migration_count" =~ ^[1-9][0-9]*$ ]]; then
  echo "expected positive SQLx migration count, got ${migration_count:-<empty>}" >&2
  exit 1
fi
echo "+ sqlx migrations ${migration_count}"

echo "container-smoke-ok"
echo "log: ${log_dir}"
