#!/usr/bin/env bash
set -euo pipefail

deploy_root="${OPENPLOTVA_DEPLOY_ROOT:-/home/wavecut/openplotva}"
go_root="${OPENPLOTVA_GO_ROOT:-/home/wavecut/go-plotva}"
compose_file="${deploy_root}/compose.production.yml"
env_file="${deploy_root}/.env.production"
operation="${OPENPLOTVA_DEPLOY_OPERATION:-prepare}"
image="${OPENPLOTVA_DEPLOY_IMAGE:?OPENPLOTVA_DEPLOY_IMAGE is required}"
confirm="${OPENPLOTVA_DEPLOY_CONFIRM:-}"
required_confirm="geta.moe/openplotva"

cd "$deploy_root"

log() {
  printf '+ %s\n' "$*"
}

fail() {
  printf 'openplotva deploy error: %s\n' "$*" >&2
  exit 1
}

require_confirm() {
  if [[ "$confirm" != "$required_confirm" ]]; then
    fail "${operation} requires OPENPLOTVA_DEPLOY_CONFIRM=${required_confirm}"
  fi
}

env_file_has_key() {
  local key="$1"
  grep -Eq "^[[:space:]]*${key}=" "$env_file"
}

env_file_value() {
  local key="$1"
  awk -F= -v key="$key" '$1 == key { value = substr($0, length(key) + 2) } END { print value }' "$env_file"
}

bootstrap_env() {
  install -d -m 755 "$deploy_root" "${deploy_root}/backups"
  if [[ ! -f "$env_file" ]]; then
    [[ -f "${go_root}/.env" ]] || fail "${go_root}/.env is required to bootstrap ${env_file}"
    install -m 600 "${go_root}/.env" "$env_file"
    log "created ${env_file} from existing server-local Go production env at ${go_root}/.env"
  fi
}

validate_env() {
  local missing=()
  local required=(
    ADMINS_ADMIN_IDS
    BOT_KEY
    WEBAPP_URL
  )
  for key in "${required[@]}"; do
    if ! env_file_has_key "$key" || [[ -z "$(env_file_value "$key" | tr -d '[:space:]')" ]]; then
      missing+=("$key")
    fi
  done
  if ((${#missing[@]} > 0)); then
    printf 'Missing required production env keys in %s:\n' "$env_file" >&2
    printf '  - %s\n' "${missing[@]}" >&2
    exit 1
  fi

  local optional_keys=(
    ACESTEP_API_KEY
    DIALOG_AIFARM_POOL_API_KEY
    GOOGLEAI_KEY
    OPENROUTER_KEY
    SERPER_API_KEY
    WHITECIRCLE_API_KEY
  )
  local missing_optional=()
  for key in "${optional_keys[@]}"; do
    if ! env_file_has_key "$key" || [[ -z "$(env_file_value "$key" | tr -d '[:space:]')" ]]; then
      missing_optional+=("$key")
    fi
  done
  if ((${#missing_optional[@]} > 0)); then
    printf 'Optional provider env keys are absent in %s:\n' "$env_file"
    printf '  - %s\n' "${missing_optional[@]}"
  fi
}

validate_server_prerequisites() {
  [[ -f "$compose_file" ]] || fail "${compose_file} is missing"
  [[ -d "$go_root" ]] || fail "${go_root} is missing"
  docker network inspect go-plotva_plotva-net >/dev/null
  docker network inspect search-subnet >/dev/null
  (cd "$go_root" && docker compose ps postgresql dragonfly >/dev/null)
  docker ps --format '{{.Names}}' | grep -qx 'go-plotva-postgresql-1' || fail "go-plotva-postgresql-1 is not running"
  docker ps --format '{{.Names}}' | grep -qx 'go-plotva-dragonfly-1' || fail "go-plotva-dragonfly-1 is not running"
}

docker_login_and_pull() {
  [[ -n "${GHCR_PULL_TOKEN:-}" ]] || fail "GHCR_PULL_TOKEN is required"
  [[ -n "${GHCR_USERNAME:-}" ]] || fail "GHCR_USERNAME is required"
  printf '%s' "$GHCR_PULL_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin >/dev/null
  docker pull "$image"
}

compose_config() {
  OPENPLOTVA_IMAGE="$image" docker compose --env-file "$env_file" -p openplotva -f "$compose_file" config --quiet
}

wait_for_http() {
  local url="$1"
  local name="$2"
  local output="${deploy_root}/${name}.json"
  for _ in $(seq 1 90); do
    if curl -fsS "$url" >"$output" 2>/dev/null; then
      log "${name} ok"
      return 0
    fi
    sleep 2
  done
  log "recent Rust app logs"
  docker logs --tail=160 openplotva-openplotva-1 >&2 || true
  fail "timed out waiting for ${url}"
}

verify_rust_app() {
  wait_for_http "http://127.0.0.1:8080/api/health" "health"
  wait_for_http "http://127.0.0.1:8080/api/ready" "ready"
  OPENPLOTVA_IMAGE="$image" docker compose --env-file "$env_file" -p openplotva -f "$compose_file" ps openplotva
}

prepare_state_volume() {
  log "preparing Rust state volume ownership"
  docker volume create openplotva_openplotva-state >/dev/null
  docker run --rm \
    --entrypoint /bin/sh \
    -v openplotva_openplotva-state:/state \
    "$image" \
    -c 'chown -R 10001:999 /state'
}

start_rust_app() {
  prepare_state_volume
  OPENPLOTVA_IMAGE="$image" docker compose --env-file "$env_file" -p openplotva -f "$compose_file" up -d --remove-orphans openplotva
  verify_rust_app
}

backup_postgres() {
  local short_sha="${image##*:}"
  short_sha="${short_sha:0:12}"
  local timestamp
  timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
  local backup="${deploy_root}/backups/plotva-${timestamp}-${short_sha}.dump"
  log "creating Postgres backup at ${backup}"
  (cd "$go_root" && docker compose exec -T postgresql pg_dump -U plotva -d plotva -Fc -Z 6) >"$backup"
  [[ -s "$backup" ]] || fail "Postgres backup is empty: ${backup}"
  log "Postgres backup created"
}

vacuum_postgres() {
  log "running safe Postgres analyze/vacuum maintenance"
  (cd "$go_root" && docker compose exec -T postgresql vacuumdb -U plotva -d plotva --analyze --jobs=2)
}

flush_redis() {
  local redis_db
  redis_db="$(env_file_value REDIS_DB)"
  redis_db="${redis_db:-0}"
  log "flushing Dragonfly DB ${redis_db}"
  (cd "$go_root" && docker compose exec -T dragonfly redis-cli -n "$redis_db" FLUSHDB)
}

stop_go_app() {
  log "stopping Go app service only"
  (cd "$go_root" && docker compose stop app)
}

prepare() {
  bootstrap_env
  validate_env
  validate_server_prerequisites
  docker_login_and_pull
  compose_config
  log "prepare completed without stopping production services"
}

first_cutover() {
  require_confirm
  bootstrap_env
  validate_env
  validate_server_prerequisites
  docker_login_and_pull
  compose_config
  stop_go_app
  backup_postgres
  vacuum_postgres
  flush_redis
  start_rust_app
  log "first cutover completed; request /admin_runtime_token and run runtime API smoke with the returned token and TLS pin"
}

redeploy() {
  require_confirm
  bootstrap_env
  validate_env
  validate_server_prerequisites
  if docker ps --format '{{.Names}}' | grep -qx 'go-plotva-app-1'; then
    fail "go-plotva-app-1 is still running; use first-cutover or stop Go app before redeploy"
  fi
  docker_login_and_pull
  compose_config
  start_rust_app
  log "redeploy completed"
}

case "$operation" in
  prepare)
    prepare
    ;;
  first-cutover)
    first_cutover
    ;;
  redeploy)
    redeploy
    ;;
  *)
    fail "unsupported operation: ${operation}"
    ;;
esac
