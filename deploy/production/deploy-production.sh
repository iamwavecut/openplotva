#!/usr/bin/env bash
set -euo pipefail

deploy_root="${OPENPLOTVA_DEPLOY_ROOT:-/home/wavecut/openplotva}"
compose_file="${deploy_root}/compose.production.yml"
maintenance_compose_file="${OPENPLOTVA_MAINTENANCE_COMPOSE_FILE:-${deploy_root}/compose.maintenance.yml}"
maintenance_env_file="${OPENPLOTVA_MAINTENANCE_ENV_FILE:-/etc/openplotva-maintenance/runtime.env}"
env_file="${deploy_root}/.env.production"
project="${OPENPLOTVA_COMPOSE_PROJECT:-openplotva}"
image="${OPENPLOTVA_DEPLOY_IMAGE:?OPENPLOTVA_DEPLOY_IMAGE is required}"
dragonfly_image="${DRAGONFLY_IMAGE:-docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.1}"
update_stream_valkey_image="${UPDATE_STREAM_VALKEY_IMAGE:-${UPDATE_STREAM_REDIS_IMAGE:-valkey/valkey:8.1-alpine}}"
alpine_image="${OPENPLOTVA_DEPLOY_ALPINE_IMAGE:-alpine:3.20}"
maintenance_env_snapshot=""
maintenance_overlay_active=false

log() {
  printf '+ %s\n' "$*"
}

fail() {
  printf 'openplotva deploy error: %s\n' "$*" >&2
  exit 1
}

compose() {
  local db_password
  db_password="$(effective_db_postgres_password)"
  local -a compose_args=(docker compose --env-file "$env_file" -p "$project" -f "$compose_file")
  if [[ "$maintenance_overlay_active" == true ]]; then
    compose_args+=(--env-file "$maintenance_env_snapshot" -f "$maintenance_compose_file")
  fi
  OPENPLOTVA_IMAGE="$image" DRAGONFLY_IMAGE="$dragonfly_image" UPDATE_STREAM_VALKEY_IMAGE="$update_stream_valkey_image" DB_POSTGRES_PASSWORD="$db_password" "${compose_args[@]}" "$@"
}

env_file_has_key() {
  local key="$1"
  grep -Eq "^[[:space:]]*${key}=" "$env_file"
}

env_file_value() {
  local key="$1"
  awk -F= -v key="$key" '$1 == key { value = substr($0, length(key) + 2) } END { print value }' "$env_file"
}

install_layout() {
  install -d -m 755 "$deploy_root"
  cd "$deploy_root"
}

cleanup_maintenance_env_snapshot() {
  if [[ -n "$maintenance_env_snapshot" ]]; then
    rm -f -- "$maintenance_env_snapshot"
    maintenance_env_snapshot=""
  fi
}

existing_app_has_maintenance_enabled() {
  local container="${project}-openplotva-1"
  container_exists "$container" || return 1
  [[ "$(docker inspect -f '{{range .Config.Env}}{{if or (eq . "MAINTENANCE_ENABLED=true") (eq . "MAINTENANCE_ENABLED=1") (eq . "MAINTENANCE_ENABLED=t")}}true{{end}}{{end}}' "$container" 2>/dev/null)" == "true" ]]
}

configure_maintenance_overlay() {
  if [[ ! -f "$maintenance_compose_file" ]]; then
    if existing_app_has_maintenance_enabled; then
      fail "maintenance overlay is missing while the existing app is enabled"
    fi
    return 0
  fi

  if ! sudo -n test -f "$maintenance_env_file" 2>/dev/null; then
    if existing_app_has_maintenance_enabled; then
      fail "maintenance runtime env is unavailable while the existing app is enabled"
    fi
    log "maintenance overlay skipped: protected runtime env is unavailable"
    return 0
  fi
  if sudo -n test -L "$maintenance_env_file" 2>/dev/null; then
    fail "maintenance runtime env must be a regular root-owned file"
  fi

  local metadata
  metadata="$(sudo -n stat -c '%u:%g:%a:%F' -- "$maintenance_env_file" 2>/dev/null)" ||
    fail "cannot inspect protected maintenance runtime env"
  [[ "$metadata" == "0:0:600:regular file" ]] ||
    fail "protected maintenance runtime env must be root-owned mode 0600"

  maintenance_env_snapshot="$(mktemp "${deploy_root}/.maintenance-runtime.env.XXXXXX")" ||
    fail "cannot create temporary maintenance runtime env"
  chmod 600 "$maintenance_env_snapshot"
  # Root reads the protected source; the deploy user owns the temporary destination.
  # shellcheck disable=SC2024
  if ! sudo -n cat -- "$maintenance_env_file" >"$maintenance_env_snapshot"; then
    cleanup_maintenance_env_snapshot
    fail "cannot read protected maintenance runtime env"
  fi
  trap cleanup_maintenance_env_snapshot EXIT
  maintenance_overlay_active=true
  log "maintenance overlay enabled from protected runtime configuration"
}

bootstrap_env() {
  if [[ -f "$env_file" ]]; then
    return
  fi
  [[ -n "${OPENPLOTVA_PRODUCTION_ENV_B64:-}" ]] || {
    fail "${env_file} is missing; provide OPENPLOTVA_PRODUCTION_ENV_B64 for first deploy or create the file on the server"
  }
  umask 077
  printf '%s' "$OPENPLOTVA_PRODUCTION_ENV_B64" | base64 -d > "$env_file"
  chmod 600 "$env_file"
  log "created ${env_file} from OPENPLOTVA_PRODUCTION_ENV_B64"
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
  if [[ -z "$(effective_db_postgres_password | tr -d '[:space:]')" ]]; then
    missing+=(DB_POSTGRES_PASSWORD)
  fi
  if ((${#missing[@]} > 0)); then
    printf 'Missing required production env keys in %s:\n' "$env_file" >&2
    printf '  - %s\n' "${missing[@]}" >&2
    exit 1
  fi
}

validate_runtime_store_images() {
  case "$dragonfly_image" in
    *dragonflydb/dragonfly:*) ;;
    *) fail "DRAGONFLY_IMAGE must use the Dragonfly server image, got ${dragonfly_image}" ;;
  esac
  case "$update_stream_valkey_image" in
    valkey/valkey:*|docker.io/valkey/valkey:*) ;;
    *)
      fail "UPDATE_STREAM_VALKEY_IMAGE must use Valkey for durable ingress, got ${update_stream_valkey_image}"
      ;;
  esac
}

docker_login_and_pull() {
  [[ -n "${GHCR_PULL_TOKEN:-}" ]] || fail "GHCR_PULL_TOKEN is required"
  [[ -n "${GHCR_USERNAME:-}" ]] || fail "GHCR_USERNAME is required"
  printf '%s' "$GHCR_PULL_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin >/dev/null
  docker pull "$image"
  compose pull dragonfly redis-ingress
}

compose_config() {
  compose config --quiet
}

volume_exists() {
  docker volume inspect "$1" >/dev/null 2>&1
}

container_exists() {
  docker container inspect "$1" >/dev/null 2>&1
}

container_running() {
  [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || true)" == "true" ]]
}

volume_empty() {
  local volume="$1"
  if ! volume_exists "$volume"; then
    return 0
  fi
  docker run --rm -v "${volume}:/data:ro" "$alpine_image" \
    sh -c 'test -z "$(find /data -mindepth 1 -maxdepth 1 -print -quit)"'
}

legacy_volume_needs_import() {
  local source="$1"
  local target="$2"
  volume_exists "$source" && volume_empty "$target"
}

legacy_postgres_exists() {
  volume_exists "go-plotva_postgres_data" || container_exists go-plotva-postgresql-1
}

effective_db_postgres_password() {
  local value
  value="$(env_file_value DB_POSTGRES_PASSWORD)"
  if [[ -n "$(printf '%s' "$value" | tr -d '[:space:]')" ]]; then
    printf '%s' "$value"
  elif legacy_postgres_exists; then
    printf '%s' "${OPENPLOTVA_LEGACY_DB_POSTGRES_PASSWORD:-plotva}"
  fi
}

legacy_import_needed() {
  legacy_volume_needs_import "go-plotva_postgres_data" "${project}_postgres-data" ||
    legacy_volume_needs_import "go-plotva_dragonflydata" "${project}_dragonfly-data"
}

stop_current_app_for_import() {
  if container_running "${project}-openplotva-1"; then
    log "stopping current app before one-time data import"
    compose stop openplotva
  fi
}

copy_volume() {
  local source="$1"
  local target="$2"
  volume_exists "$source" || return 0
  if ! volume_empty "$target"; then
    log "keeping existing non-empty volume ${target}"
    return 0
  fi
  log "copying volume ${source} to ${target}"
  docker volume create "$target" >/dev/null
  docker run --rm \
    -v "${source}:/from:ro" \
    -v "${target}:/to" \
    "$alpine_image" \
    sh -c 'cd /from && tar cf - . | tar xpf - -C /to'
}

import_file_volumes() {
  if legacy_volume_needs_import "go-plotva_dragonflydata" "${project}_dragonfly-data"; then
    if container_running go-plotva-dragonfly-1; then
      log "saving legacy Dragonfly before volume import"
      docker exec go-plotva-dragonfly-1 redis-cli SAVE >/dev/null
    fi
    copy_volume "go-plotva_dragonflydata" "${project}_dragonfly-data"
  fi
}

legacy_postgres_import_mode() {
  local target="${project}_postgres-data"
  if ! legacy_volume_needs_import "go-plotva_postgres_data" "$target"; then
    printf 'none'
  elif container_running go-plotva-postgresql-1; then
    printf 'dump'
  else
    printf 'volume'
  fi
}

import_postgres_volume() {
  copy_volume "go-plotva_postgres_data" "${project}_postgres-data"
}

ensure_service() {
  local service="$1"
  local container="${project}-${service}-1"
  if container_running "$container"; then
    log "${service} already running"
    return
  fi
  log "starting ${service}"
  compose up -d --no-deps --no-recreate "$service"
}

container_config_image() {
  docker inspect -f '{{.Config.Image}}' "$1" 2>/dev/null || true
}

save_dragonfly_if_running() {
  local container
  container="$(compose ps -q dragonfly)"
  if [[ -n "$container" ]] && container_running "$container"; then
    log "saving Dragonfly before image change"
    docker exec "$container" redis-cli SAVE >/dev/null
  fi
}

log_dragonfly_info() {
  local container
  local info
  container="$(compose ps -q dragonfly)"
  [[ -n "$container" ]] || return 0
  info="$(docker exec "$container" redis-cli INFO server 2>/dev/null | tr -d '\r' | grep -E '^(dragonfly_version|redis_version):' || true)"
  [[ -n "$info" ]] || return 0
  while IFS= read -r line; do
    log "dragonfly ${line}"
  done <<<"$info"
}

verify_dragonfly_engine() {
  local container
  local info
  container="$(compose ps -q dragonfly)"
  [[ -n "$container" ]] || fail "dragonfly container is missing"
  info="$(docker exec "$container" redis-cli INFO server 2>/dev/null | tr -d '\r')"
  grep -q '^dragonfly_version:' <<<"$info" ||
    fail "primary Redis-compatible state service is not Dragonfly"
  log "primary Redis-compatible state service verified as Dragonfly"
}

ensure_dragonfly() {
  local container="${project}-dragonfly-1"
  local running_image
  if container_exists "$container"; then
    running_image="$(container_config_image "$container")"
    if [[ "$running_image" != "$dragonfly_image" ]]; then
      log "recreating dragonfly for image ${dragonfly_image} (was ${running_image:-unknown})"
      save_dragonfly_if_running
      compose up -d --no-deps --force-recreate dragonfly
    else
      ensure_service dragonfly
    fi
  else
    ensure_service dragonfly
  fi
  wait_for_service_health dragonfly
  verify_dragonfly_engine
  log_dragonfly_info
}

verify_update_stream_persistence() {
  local container
  local persistence
  local server
  local appendfsync
  local memory
  container="$(compose ps -q redis-ingress)"
  [[ -n "$container" ]] || fail "redis-ingress container is missing"
  server="$(docker exec "$container" valkey-cli INFO server 2>/dev/null | tr -d '\r')"
  grep -q '^server_name:valkey$' <<<"$server" ||
    fail "durable ingress service is not Valkey"
  persistence="$(docker exec "$container" valkey-cli INFO persistence 2>/dev/null | tr -d '\r')"
  grep -q '^aof_enabled:1$' <<<"$persistence" || fail "redis-ingress AOF is disabled"
  grep -q '^aof_last_write_status:ok$' <<<"$persistence" || fail "redis-ingress AOF last write failed"
  appendfsync="$(docker exec "$container" valkey-cli CONFIG GET appendfsync 2>/dev/null | tr -d '\r')"
  grep -qx 'always' <<<"$appendfsync" || fail "redis-ingress appendfsync is not always"
  memory="$(docker exec "$container" valkey-cli INFO memory 2>/dev/null | tr -d '\r')"
  grep -q '^maxmemory_policy:noeviction$' <<<"$memory" ||
    fail "redis-ingress maxmemory policy is not noeviction"
  log "durable ingress verified as Valkey with writable AOF, appendfsync always, and noeviction"
}

wait_for_service_health() {
  local service="$1"
  local container
  container="$(compose ps -q "$service")"
  [[ -n "$container" ]] || fail "${service} container is missing"
  for _ in $(seq 1 90); do
    local status
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}running{{end}}' "$container")"
    case "$status" in
      healthy|running)
        log "${service} healthy"
        return 0
        ;;
      unhealthy)
        docker logs --tail=120 "$container" >&2 || true
        fail "${service} is unhealthy"
        ;;
    esac
    sleep 10
  done
  docker logs --tail=120 "$container" >&2 || true
  fail "timed out waiting for ${service} health"
}

import_postgres_dump() {
  local target_container="${project}-postgresql-1"
  local db_user
  local db_name
  db_user="$(env_file_value DB_POSTGRES_USER)"
  db_user="${db_user:-plotva}"
  db_name="$(env_file_value DB_POSTGRES_DB)"
  db_name="${db_name:-plotva}"

  container_running go-plotva-postgresql-1 || fail "legacy Postgres container is not running for logical import"
  container_running "$target_container" || fail "target Postgres container is not running for logical import"

  log "importing legacy Postgres data into ${target_container}"
  docker exec go-plotva-postgresql-1 pg_dump -U plotva -d plotva -Fc -Z 6 |
    docker exec -i "$target_container" pg_restore \
      -U "$db_user" \
      -d "$db_name" \
      --clean \
      --if-exists \
      --no-owner
}

ensure_state_volume() {
  local state_volume="${project}_openplotva-state"
  log "preparing app state volume ownership"
  docker volume create "$state_volume" >/dev/null
  docker run --rm \
    --entrypoint /bin/sh \
    -v "${state_volume}:/state" \
    "$image" \
    -c 'chown -R 10001:999 /state'
}

start_dependencies() {
  local postgres_mode="$1"
  if [[ "$postgres_mode" == "volume" ]]; then
    import_postgres_volume
  fi

  ensure_service postgresql
  wait_for_service_health postgresql

  if [[ "$postgres_mode" == "dump" ]]; then
    import_postgres_dump
  fi

  ensure_dragonfly
  ensure_service redis-ingress
  wait_for_service_health redis-ingress
  verify_update_stream_persistence
}

start_app() {
  ensure_state_volume
  log "recreating openplotva app"
  compose up -d --no-deps --force-recreate --remove-orphans openplotva
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
  log "recent app logs"
  docker logs --tail=160 "${project}-openplotva-1" >&2 || true
  fail "timed out waiting for ${url}"
}

verify_app() {
  wait_for_http "http://127.0.0.1:8080/api/health" "health"
  wait_for_http "http://127.0.0.1:8080/api/ready" "ready"
  compose ps openplotva
}

remove_non_current_app_images() {
  local current_image_id
  local image_id
  local image_repository
  current_image_id="$(docker inspect -f '{{.Image}}' "${project}-openplotva-1")"
  image_repository="${image%:*}"
  while IFS= read -r image_id; do
    [[ -n "$image_id" && "$image_id" != "$current_image_id" ]] || continue
    docker image rm -f "$image_id" >/dev/null
  done < <(
    docker image ls \
      --no-trunc \
      --filter "reference=${image_repository}:*" \
      --format '{{.ID}}' |
      sort -u
  )
  log "removed non-current local application images"
}

main() {
  install_layout
  bootstrap_env
  validate_env
  validate_runtime_store_images
  configure_maintenance_overlay
  compose_config
  docker_login_and_pull

  local postgres_mode
  postgres_mode="$(legacy_postgres_import_mode)"
  if legacy_import_needed; then
    stop_current_app_for_import
    import_file_volumes
  fi

  start_dependencies "$postgres_mode"
  start_app
  verify_app
  remove_non_current_app_images
  log "production deployment applied"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
