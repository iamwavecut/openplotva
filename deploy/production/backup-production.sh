#!/usr/bin/env bash
set -euo pipefail

deploy_root="${OPENPLOTVA_DEPLOY_ROOT:-/home/wavecut/openplotva}"
compose_file="${OPENPLOTVA_BACKUP_COMPOSE_FILE:-${deploy_root}/compose.production.yml}"
env_file="${OPENPLOTVA_BACKUP_ENV_FILE:-${deploy_root}/.env.production}"
project="${OPENPLOTVA_COMPOSE_PROJECT:-openplotva}"
backup_root="${OPENPLOTVA_BACKUP_ROOT:-${deploy_root}/backups}"
backup_keep="${OPENPLOTVA_BACKUP_KEEP:-14}"
postgres_service="${OPENPLOTVA_BACKUP_POSTGRES_SERVICE:-postgresql}"
dragonfly_service="${OPENPLOTVA_BACKUP_DRAGONFLY_SERVICE:-dragonfly}"
ingress_service="${OPENPLOTVA_BACKUP_INGRESS_SERVICE:-redis-ingress}"
alpine_image="${OPENPLOTVA_DEPLOY_ALPINE_IMAGE:-alpine:3.20}"
runtime_image="${OPENPLOTVA_DEPLOY_IMAGE:-openplotva:backup-placeholder}"
dragonfly_image="${DRAGONFLY_IMAGE:-docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.1}"
update_stream_valkey_image="${UPDATE_STREAM_VALKEY_IMAGE:-valkey/valkey:8.1-alpine}"

log() {
  printf '+ backup: %s\n' "$*"
}

fail() {
  printf 'openplotva backup error: %s\n' "$*" >&2
  exit 1
}

env_file_value() {
  local key="$1"
  awk -F= -v key="$key" '$1 == key { value = substr($0, length(key) + 2) } END { print value }' "$env_file"
}

effective_db_postgres_password() {
  local value
  value="$(env_file_value DB_POSTGRES_PASSWORD)"
  printf '%s' "${value:-plotva}"
}

compose() {
  local db_password
  db_password="$(effective_db_postgres_password)"
  OPENPLOTVA_IMAGE="$runtime_image" \
    DRAGONFLY_IMAGE="$dragonfly_image" \
    UPDATE_STREAM_VALKEY_IMAGE="$update_stream_valkey_image" \
    DB_POSTGRES_PASSWORD="$db_password" \
    docker compose --env-file "$env_file" -p "$project" -f "$compose_file" "$@"
}

service_container() {
  compose ps -q "$1"
}

require_nonempty_file() {
  local path="$1"
  [[ -s "$path" ]] || fail "backup artifact is empty: ${path}"
}

write_and_verify_checksums() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum ./* >SHA256SUMS
    sha256sum --check SHA256SUMS >/dev/null
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 ./* >SHA256SUMS
    shasum -a 256 --check SHA256SUMS >/dev/null
  else
    fail "sha256sum or shasum is required"
  fi
  chmod 600 SHA256SUMS
}

snapshot_dragonfly_service() {
  local service="$1"
  local output="$2"
  local container
  local data_volume
  local last_saved_file
  local snapshot_prefix

  container="$(service_container "$service")"
  [[ -n "$container" ]] || fail "${service} container is missing"
  log "creating consistent ${service} DFS snapshot"
  docker exec "$container" redis-cli SAVE >/dev/null
  last_saved_file="$(
    docker exec "$container" redis-cli INFO persistence 2>/dev/null |
      tr -d '\r' |
      sed -n 's/^last_saved_file://p'
  )"
  case "$last_saved_file" in
    dump-*-summary.dfs) ;;
    *) fail "${service} reported an invalid snapshot file: ${last_saved_file:-missing}" ;;
  esac
  snapshot_prefix="${last_saved_file%-summary.dfs}"
  data_volume="$(
    docker inspect -f '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}' "$container"
  )"
  [[ -n "$data_volume" ]] || fail "${service} /data mount is not a named volume"
  docker run --rm \
    -v "${data_volume}:/from:ro" \
    "$alpine_image" \
    sh -c 'cd /from; set -- "$1"-*.dfs; test -f "$1"; tar czf - "$@"' \
    sh "$snapshot_prefix" >"$output"
  require_nonempty_file "$output"
  tar -tzf "$output" | grep -Fxq "$last_saved_file" ||
    fail "${service} archive does not contain ${last_saved_file}"
  chmod 600 "$output"
}

snapshot_valkey_service() {
  local service="$1"
  local output="$2"
  local container
  local header

  container="$(service_container "$service")"
  [[ -n "$container" ]] || fail "${service} container is missing"
  log "creating consistent ${service} RDB snapshot"
  docker exec "$container" valkey-cli SAVE >/dev/null
  docker exec "$container" cat /data/dump.rdb >"$output"
  require_nonempty_file "$output"
  header="$(head -c 5 "$output")"
  [[ "$header" == "REDIS" ]] || fail "${service} snapshot has an invalid RDB header"
  docker run --rm \
    --entrypoint valkey-check-rdb \
    -v "$(dirname "$output"):/backup:ro" \
    "$update_stream_valkey_image" \
    "/backup/$(basename "$output")" >/dev/null
  chmod 600 "$output"
}

prune_old_backups() {
  local first_stale
  local path
  [[ "$backup_keep" =~ ^[1-9][0-9]*$ ]] ||
    fail "OPENPLOTVA_BACKUP_KEEP must be a positive integer"
  first_stale=$((backup_keep + 1))
  while IFS= read -r path; do
    case "$path" in
      "${backup_root}"/predeploy-*)
        rm -rf -- "$path"
        log "pruned ${path}"
        ;;
      *)
        fail "refusing to prune unexpected path ${path}"
        ;;
    esac
  done < <(
    find "$backup_root" -mindepth 1 -maxdepth 1 -type d -name 'predeploy-*' -print |
      sort -r |
      tail -n "+${first_stale}"
  )
}

main() {
  [[ -f "$compose_file" ]] || fail "compose file is missing: ${compose_file}"
  [[ -f "$env_file" ]] || fail "environment file is missing: ${env_file}"

  local postgres_container
  postgres_container="$(service_container "$postgres_service")"
  if [[ -z "$postgres_container" ]]; then
    log "no current PostgreSQL container; pre-deploy backup skipped"
    return 0
  fi

  local db_user
  local db_name
  local timestamp
  local temporary_dir
  local final_dir
  local cleanup_command
  db_user="$(env_file_value DB_POSTGRES_USER)"
  db_user="${db_user:-plotva}"
  db_name="$(env_file_value DB_POSTGRES_DB)"
  db_name="${db_name:-plotva}"
  timestamp="$(date -u +%Y%m%dT%H%M%SZ)"

  install -d -m 700 "$backup_root"
  temporary_dir="$(mktemp -d "${backup_root}/.predeploy-${timestamp}.XXXXXX")"
  final_dir="${backup_root}/predeploy-${timestamp}"
  printf -v cleanup_command 'rm -rf -- %q' "$temporary_dir"
  trap "$cleanup_command" EXIT

  log "dumping PostgreSQL database ${db_name}"
  compose exec -T "$postgres_service" \
    pg_dump -U "$db_user" -d "$db_name" -Fc -Z 6 >"${temporary_dir}/postgres.dump"
  require_nonempty_file "${temporary_dir}/postgres.dump"
  compose exec -T "$postgres_service" \
    pg_restore --list <"${temporary_dir}/postgres.dump" >/dev/null
  chmod 600 "${temporary_dir}/postgres.dump"

  snapshot_dragonfly_service \
    "$dragonfly_service" "${temporary_dir}/dragonfly-snapshot.tar.gz"
  snapshot_valkey_service \
    "$ingress_service" "${temporary_dir}/redis-ingress.rdb"

  local state_volume="${project}_openplotva-state"
  if docker volume inspect "$state_volume" >/dev/null 2>&1; then
    log "archiving application state volume"
    docker run --rm \
      -v "${state_volume}:/from:ro" \
      "$alpine_image" \
      sh -c 'cd /from && tar czf - .' \
      >"${temporary_dir}/openplotva-state.tar.gz"
    require_nonempty_file "${temporary_dir}/openplotva-state.tar.gz"
    tar -tzf "${temporary_dir}/openplotva-state.tar.gz" >/dev/null
    chmod 600 "${temporary_dir}/openplotva-state.tar.gz"
  fi

  (
    cd "$temporary_dir"
    write_and_verify_checksums
  )
  [[ ! -e "$final_dir" ]] || fail "backup destination already exists: ${final_dir}"
  mv "$temporary_dir" "$final_dir"
  trap - EXIT
  log "verified runtime backup created at ${final_dir}"
  prune_old_backups
}

main "$@"
