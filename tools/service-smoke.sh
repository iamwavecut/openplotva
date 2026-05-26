#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  tools/service-smoke.sh

Starts disposable Postgres+Dragonfly services through compose.yml, runs the app
with service connections and SQLx migrations enabled, verifies readiness plus
real DB-backed admin/settings API paths and the TLS runtime API, then stops the
app and removes the disposable compose project.

Optional env:
  OPENPLOTVA_SERVICE_SMOKE_PORT       app port, default first free from 18180
  OPENPLOTVA_SERVICE_SMOKE_RUNTIME_PORT runtime API port, default first free from 19091
  OPENPLOTVA_SERVICE_SMOKE_PG_PORT    Postgres host port, default first free from 55432
  OPENPLOTVA_SERVICE_SMOKE_REDIS_PORT Dragonfly host port, default first free from 56379
  OPENPLOTVA_SERVICE_SMOKE_WEB_UI      run browser admin/settings UI smoke when set to 1
  OPENPLOTVA_WEB_UI_BROWSER            browser executable for UI smoke, default Google Chrome if present
  OPENPLOTVA_WEB_UI_HEADLESS           browser headless mode for UI smoke, default 1
  OPENPLOTVA_SERVICE_SMOKE_KEEP       keep compose services when set to 1
  OPENPLOTVA_SMOKE_LOG_DIR            log directory, default mktemp
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

export PATH="/opt/homebrew/bin:$PATH"

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  echo "docker compose is required for service smoke" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for service smoke" >&2
  exit 1
fi

if ! command -v shasum >/dev/null 2>&1; then
  echo "shasum is required for service smoke" >&2
  exit 1
fi

free_port_from() {
  local port="$1"
  while nc -z 127.0.0.1 "$port" >/dev/null 2>&1; do
    port=$((port + 1))
  done
  printf '%s\n' "$port"
}

app_port="$(free_port_from "${OPENPLOTVA_SERVICE_SMOKE_PORT:-18180}")"
runtime_port="$(free_port_from "${OPENPLOTVA_SERVICE_SMOKE_RUNTIME_PORT:-19091}")"
pg_port="$(free_port_from "${OPENPLOTVA_SERVICE_SMOKE_PG_PORT:-55432}")"
redis_port="$(free_port_from "${OPENPLOTVA_SERVICE_SMOKE_REDIS_PORT:-56379}")"
project="openplotva-smoke-$$"
base_url="http://127.0.0.1:${app_port}"
runtime_base_url="https://127.0.0.1:${runtime_port}"
log_dir="${OPENPLOTVA_SMOKE_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/openplotva-service-smoke.XXXXXX")}"
app_log="${log_dir}/openplotva-app.log"
health_file="${log_dir}/health.json"
ready_file="${log_dir}/ready.json"
state_file="${log_dir}/admin-state.json"
auth_file="${log_dir}/admin-auth.json"
auth_cookie_file="${log_dir}/admin-auth-cookie.json"
settings_file="${log_dir}/settings.json"
settings_update_file="${log_dir}/settings-update.json"
settings_after_file="${log_dir}/settings-after.json"
settings_bad_file="${log_dir}/settings-bad.json"
chats_file="${log_dir}/settings-chats.json"
group_settings_file="${log_dir}/settings-group.json"
deputy_candidates_file="${log_dir}/settings-deputy-candidates.json"
deputy_update_file="${log_dir}/settings-deputy-update.json"
group_settings_after_file="${log_dir}/settings-group-after.json"
memory_file="${log_dir}/settings-memory.json"
memory_delete_file="${log_dir}/settings-memory-delete.json"
memory_after_file="${log_dir}/settings-memory-after.json"
admin_safety_file="${log_dir}/admin-safety.json"
admin_analytics_file="${log_dir}/admin-analytics.json"
admin_chat_update_file="${log_dir}/admin-chat-update.json"
admin_chat_after_update_file="${log_dir}/admin-chat-after-update.json"
admin_chat_block_file="${log_dir}/admin-chat-block.json"
admin_chat_blocked_file="${log_dir}/admin-chat-blocked.json"
admin_chat_unblock_file="${log_dir}/admin-chat-unblock.json"
admin_chat_unblocked_file="${log_dir}/admin-chat-unblocked.json"
admin_vip_grant_file="${log_dir}/admin-vip-grant.json"
admin_vip_after_grant_file="${log_dir}/admin-vip-after-grant.json"
admin_vip_revoke_file="${log_dir}/admin-vip-revoke.json"
admin_vip_after_revoke_file="${log_dir}/admin-vip-after-revoke.json"
runtime_unauth_file="${log_dir}/runtime-unauthorized.txt"
runtime_method_file="${log_dir}/runtime-method.txt"
runtime_graphql_file="${log_dir}/runtime-graphql.json"
app_pid=""

compose() {
  OPENPLOTVA_DEV_POSTGRES_PORT="$pg_port" \
    OPENPLOTVA_DEV_REDIS_PORT="$redis_port" \
    docker compose -p "$project" "$@"
}

cleanup() {
  if [[ -n "$app_pid" ]] && kill -0 "$app_pid" >/dev/null 2>&1; then
    kill "$app_pid" >/dev/null 2>&1 || true
    wait "$app_pid" >/dev/null 2>&1 || true
  fi
  if [[ "${OPENPLOTVA_SERVICE_SMOKE_KEEP:-0}" != "1" ]]; then
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  else
    echo "compose project kept: ${project}"
  fi
}
trap cleanup EXIT

wait_for_tcp() {
  local name="$1"
  local port="$2"
  for _ in $(seq 1 120); do
    if nc -z 127.0.0.1 "$port" >/dev/null 2>&1; then
      echo "+ ${name} tcp ready on ${port}"
      return 0
    fi
    sleep 0.5
  done
  echo "timed out waiting for ${name} on port ${port}" >&2
  compose ps >&2 || true
  exit 1
}

wait_for_http() {
  for _ in $(seq 1 160); do
    if curl -fsS "${base_url}/api/health" >"$health_file" 2>/dev/null; then
      return 0
    fi
    if [[ -n "$app_pid" ]] && ! kill -0 "$app_pid" >/dev/null 2>&1; then
      echo "app exited before health check passed" >&2
      tail -n 120 "$app_log" >&2 || true
      exit 1
    fi
    sleep 0.5
  done
  echo "timed out waiting for /api/health" >&2
  tail -n 120 "$app_log" >&2 || true
  exit 1
}

start_app() {
  local label="${1:-start openplotva-app}"
  echo "+ ${label} with services at ${base_url}"
  env \
    OPENPLOTVA_BIND_ADDR="127.0.0.1:${app_port}" \
    WEBAPP_HOST="127.0.0.1" \
    WEBAPP_PORT="${app_port}" \
    WEBAPP_URL="${base_url}" \
    OPENPLOTVA_CONNECT_SERVICES="true" \
    OPENPLOTVA_RUN_MIGRATIONS="true" \
    OPENPLOTVA_CONSUME_UPDATES="false" \
    RUNTIME_API_ENABLED="true" \
    RUNTIME_API_HOST="127.0.0.1" \
    RUNTIME_API_PORT="${runtime_port}" \
    BOT_KEY="" \
    DB_POSTGRES_HOST="127.0.0.1" \
    DB_POSTGRES_PORT="${pg_port}" \
    DB_POSTGRES_USER="plotva" \
    DB_POSTGRES_PASSWORD="plotva" \
    DB_POSTGRES_DB="plotva" \
    REDIS_HOST="127.0.0.1" \
    REDIS_PORT="${redis_port}" \
    REDIS_PASSWORD="" \
    REDIS_DB="0" \
    BOT_USERNAME="SmokePlotvaBot" \
    OPENPLOTVA_LOG_FILTER="${OPENPLOTVA_LOG_FILTER:-openplotva=warn,tower_http=warn}" \
    cargo run -p openplotva-app >>"$app_log" 2>&1 &
  app_pid="$!"
}

stop_app() {
  if [[ -n "$app_pid" ]] && kill -0 "$app_pid" >/dev/null 2>&1; then
    kill "$app_pid" >/dev/null 2>&1 || true
    wait "$app_pid" >/dev/null 2>&1 || true
  fi
  app_pid=""
}

seed_existing_migration_history() {
  local bridge_sql="${log_dir}/existing-migration-history.sql"
  {
    echo "DROP TABLE IF EXISTS gorp_migrations;"
    echo "CREATE TABLE gorp_migrations (id TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now());"
    for migration in migrations/*.up.sql; do
      local stem
      stem="$(basename "$migration" .up.sql)"
      printf "INSERT INTO gorp_migrations (id, applied_at) VALUES ('%s.sql', now()) ON CONFLICT (id) DO NOTHING;\n" "$stem"
    done
    echo "DROP TABLE IF EXISTS _sqlx_migrations;"
  } >"$bridge_sql"
  compose exec -T postgres psql -v ON_ERROR_STOP=1 -U plotva -d plotva <"$bridge_sql" >/dev/null
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

expect_jq_equals() {
  local file="$1"
  local query="$2"
  local expected="$3"
  local actual
  actual="$(jq -r "$query" "$file")"
  if [[ "$actual" != "$expected" ]]; then
    echo "expected ${file} ${query} to equal ${expected}, got ${actual}" >&2
    cat "$file" >&2
    exit 1
  fi
}

expect_http_status() {
  local status="$1"
  local url="$2"
  local file="$3"
  shift 3
  local actual
  actual="$(curl "$@" -sS -o "$file" -w '%{http_code}' "$url")"
  if [[ "$actual" != "$status" ]]; then
    echo "expected ${url} to return ${status}, got ${actual}" >&2
    cat "$file" >&2
    exit 1
  fi
}

seed_runtime_api_token() {
  local token_id="service-smoke-token"
  local secret="service-smoke-secret"
  local token_hash
  token_hash="$(printf '%s' "$secret" | shasum -a 256 | awk '{print $1}')"
  compose exec -T postgres psql -v ON_ERROR_STOP=1 -U plotva -d plotva >/dev/null <<SQL
INSERT INTO runtime_api_tokens (id, token_hash)
VALUES ('${token_id}', decode('${token_hash}', 'hex'))
ON CONFLICT (id) DO UPDATE
SET token_hash = EXCLUDED.token_hash,
    created_at = CURRENT_TIMESTAMP;
SQL
  printf 'prt_%s.%s\n' "$token_id" "$secret"
}

seed_group_settings_fixtures() {
  echo "+ seed settings group/deputy/memory fixtures"
  compose exec -T postgres psql -v ON_ERROR_STOP=1 -U plotva -d plotva >/dev/null <<'SQL'
INSERT INTO users (id, first_name, username)
VALUES
  (7, 'Owner', 'owner'),
  (8, 'Deputy', 'deputy'),
  (9, 'Member', 'member')
ON CONFLICT (id) DO UPDATE
SET first_name = EXCLUDED.first_name,
    username = EXCLUDED.username,
    updated = CURRENT_TIMESTAMP;

INSERT INTO chats (id, type, title, username, is_forum)
VALUES (-100777, 'supergroup', 'Smoke Group', 'smoke_group', true)
ON CONFLICT (id) DO UPDATE
SET type = EXCLUDED.type,
    title = EXCLUDED.title,
    username = EXCLUDED.username,
    is_forum = EXCLUDED.is_forum,
    updated = CURRENT_TIMESTAMP;

INSERT INTO chat_members (
  chat_id,
  user_id,
  status,
  can_manage_chat,
  can_promote_members,
  can_change_info
)
VALUES
  (-100777, 7, 'creator', true, true, true),
  (-100777, 8, 'administrator', true, false, true),
  (-100777, 9, 'member', false, false, false)
ON CONFLICT (chat_id, user_id) DO UPDATE
SET status = EXCLUDED.status,
    can_manage_chat = EXCLUDED.can_manage_chat,
    can_promote_members = EXCLUDED.can_promote_members,
    can_change_info = EXCLUDED.can_change_info,
    updated_at = CURRENT_TIMESTAMP;

DELETE FROM chat_deputies WHERE chat_id = -100777;
INSERT INTO chat_deputies (chat_id, user_id) VALUES (-100777, 8);

DELETE FROM memory_cards WHERE dedup_hash = 'service-smoke-memory';
INSERT INTO memory_cards (
  visibility,
  card_type,
  subject,
  predicate,
  object,
  fact_text,
  dedup_hash,
  confidence,
  salience,
  origin_chat_id,
  origin_user_id,
  chat_id,
  user_id,
  last_observed_at
)
VALUES (
  'chat',
  'preference',
  'Smoke Group',
  'likes',
  'real DB settings smoke',
  'Smoke Group likes real DB settings smoke.',
  'service-smoke-memory',
  0.9,
  0.8,
  -100777,
  7,
  -100777,
  0,
  CURRENT_TIMESTAMP
);

DELETE FROM whitecircle_checks WHERE deployment_id = 'service-smoke';
INSERT INTO whitecircle_checks (
  source,
  flow,
  mode,
  chat_id,
  thread_id,
  message_id,
  user_id,
  deployment_id,
  external_session_id,
  request_messages,
  flagged,
  internal_session_id,
  policies,
  response_json,
  duration_ms
)
VALUES (
  'service-smoke',
  'dialog',
  'block',
  -100777,
  0,
  5001,
  7,
  'service-smoke',
  'wc-smoke-ext',
  '[{"role":"user","content":"smoke risky text"}]'::jsonb,
  true,
  'wc-smoke-int',
  '{"violence":true}'::jsonb,
  '{"flagged":true,"categories":{"violence":true}}'::jsonb,
  123
);

DELETE FROM llm_request_events WHERE flow = 'service-smoke-flow';
INSERT INTO llm_request_events (
  created_at,
  source,
  flow,
  chat_id,
  thread_id,
  message_id,
  user_id,
  model,
  iteration,
  prompt_chars,
  prompt_messages,
  docs_chars,
  duration_ms,
  error,
  provider,
  request_kind,
  input_tokens,
  output_tokens,
  total_tokens,
  cached_tokens,
  thoughts_tokens,
  tool_use_prompt_tokens,
  prompt_eval_tokens,
  prompt_eval_ms,
  prompt_tps,
  generation_tokens,
  generation_ms,
  generation_tps,
  effective_output_tps,
  effective_total_tps,
  max_tokens,
  temperature,
  top_p,
  top_k,
  candidate_count,
  tool_mode,
  response_format,
  inference_params
)
VALUES
  (
    CURRENT_TIMESTAMP - INTERVAL '5 minutes',
    'aifarm',
    'service-smoke-flow',
    -100777,
    0,
    5001,
    7,
    'smoke-model-a',
    1,
    120,
    2,
    40,
    2400,
    NULL,
    'AI Farm',
    'dialog',
    100,
    40,
    140,
    10,
    0,
    5,
    100,
    100.0,
    1000.0,
    40,
    1000.0,
    40.0,
    20.0,
    70.0,
    512,
    0.7,
    0.9,
    40,
    1,
    'auto',
    'json',
    '{"service_smoke":true}'::jsonb
  ),
  (
    CURRENT_TIMESTAMP - INTERVAL '4 minutes',
    'genkit',
    'service-smoke-flow',
    -100777,
    0,
    5002,
    7,
    'smoke-model-b',
    2,
    80,
    1,
    20,
    600,
    'service smoke llm error',
    'Gemini/GenKit',
    'dialog',
    50,
    10,
    60,
    0,
    0,
    0,
    50,
    50.0,
    1000.0,
    10,
    500.0,
    20.0,
    10.0,
    100.0,
    256,
    0.4,
    0.8,
    NULL,
    1,
    'none',
    'text',
    '{"service_smoke":true,"error":true}'::jsonb
  );

DELETE FROM memory_runs WHERE prompt_version = 'service-smoke-memory';
INSERT INTO memory_runs (
  chat_id,
  thread_id,
  range_start_at,
  range_end_at,
  prompt_version,
  status,
  attempts,
  message_count,
  cards_inserted,
  cards_updated,
  cards_superseded,
  episodes_inserted,
  input_token_estimate,
  output_token_estimate,
  error,
  created_at,
  started_at,
  completed_at,
  updated_at
)
VALUES (
  -100777,
  0,
  CURRENT_TIMESTAMP - INTERVAL '1 hour',
  CURRENT_TIMESTAMP - INTERVAL '30 minutes',
  'service-smoke-memory',
  'completed',
  1,
  12,
  1,
  0,
  0,
  1,
  345,
  67,
  '',
  CURRENT_TIMESTAMP - INTERVAL '10 minutes',
  CURRENT_TIMESTAMP - INTERVAL '4 minutes',
  CURRENT_TIMESTAMP - INTERVAL '3 minutes',
  CURRENT_TIMESTAMP - INTERVAL '3 minutes'
);
SQL
}

echo "+ docker compose up postgres dragonfly (${project})"
compose up --wait --wait-timeout 120 postgres dragonfly
wait_for_tcp "postgres" "$pg_port"
wait_for_tcp "dragonfly" "$redis_port"

start_app "start openplotva-app"
wait_for_http
expect_body_contains "$health_file" '"status":"ok"'
echo "+ /api/health ok"

curl -fsS "${base_url}/api/ready" >"$ready_file"
expect_body_contains "$ready_file" '"name":"postgres","status":"ok"'
expect_body_contains "$ready_file" '"name":"redis","status":"ok"'
expect_body_contains "$ready_file" '"name":"migrations","status":"ok"'
expect_body_contains "$ready_file" '"name":"runtime_api","status":"ok"'
echo "+ /api/ready services ok"

stop_app
seed_existing_migration_history
start_app "restart openplotva-app after existing migration history bridge"
wait_for_http
curl -fsS "${base_url}/api/ready" >"$ready_file"
expect_body_contains "$ready_file" '"name":"migrations","status":"ok"'
bridge_count="$(compose exec -T postgres psql -U plotva -d plotva -Atc "SELECT count(*) FROM _sqlx_migrations")"
if [[ "$bridge_count" != "48" ]]; then
  echo "expected bridged _sqlx_migrations count 48, got ${bridge_count}" >&2
  exit 1
fi
echo "+ existing migration history bridge ok"

curl -fsS "${base_url}/admin/api/state" >"$state_file"
expect_body_contains "$state_file" '"log_level":"info"'
expect_body_contains "$state_file" '"queue":'
echo "+ /admin/api/state ok"

curl -fsS "${base_url}/admin/api/auth_check" >"$auth_file"
expect_jq_equals "$auth_file" '.authenticated' "false"
curl -fsS -H "Cookie: admin_session=1001" "${base_url}/admin/api/auth_check" >"$auth_cookie_file"
expect_jq_equals "$auth_cookie_file" '.authenticated' "true"
expect_jq_equals "$auth_cookie_file" '.user_id' "1001"
echo "+ /admin/api/auth_check ok"

expect_http_status "403" "${base_url}/api/settings?chat_id=42&user_id=42&signature=bad" "$settings_bad_file"
expect_jq_equals "$settings_bad_file" '.error' "Invalid signature"

curl -fsS "${base_url}/api/settings?chat_id=42&user_id=42&signature=780e28cf" >"$settings_file"
expect_jq_equals "$settings_file" '.chat_id' "42"
expect_jq_equals "$settings_file" '.chat_type' "private"
expect_jq_equals "$settings_file" '.enable_global_text_reply' "true"

curl -fsS \
  -X PUT \
  -H "Content-Type: application/json" \
  --data '{"chat_id":42,"user_id":42,"signature":"780e28cf","mood_alignment":"smoke-mood","custom_persona":"service smoke persona","reactivity_percentage":67,"proactivity_percentage":12,"enable_obscenifier":false,"enable_profanity":true,"enable_greet_joiners":true,"enable_global_text_reply":false,"enable_global_draw_reply":false,"disable_random_reactivity":true,"hide_original_draw_prompt":true}' \
  "${base_url}/api/settings" >"$settings_update_file"
expect_jq_equals "$settings_update_file" '.status' "success"

curl -fsS "${base_url}/api/settings?chat_id=42&user_id=42&signature=780e28cf" >"$settings_after_file"
expect_jq_equals "$settings_after_file" '.mood_alignment' "smoke-mood"
expect_jq_equals "$settings_after_file" '.custom_persona' "service smoke persona"
expect_jq_equals "$settings_after_file" '.reactivity_percentage' "67"
expect_jq_equals "$settings_after_file" '.proactivity_percentage' "12"
expect_jq_equals "$settings_after_file" '.enable_obscenifier' "false"
expect_jq_equals "$settings_after_file" '.enable_global_text_reply' "true"
expect_jq_equals "$settings_after_file" '.disable_random_reactivity' "true"
expect_jq_equals "$settings_after_file" '.hide_original_draw_prompt' "false"
echo "+ /api/settings real-db get/put ok"

seed_group_settings_fixtures

curl -fsS "${base_url}/api/chats?user_id=7&signature=68b3a1ec" >"$chats_file"
expect_jq_equals "$chats_file" '.[] | select(.id == -100777) | .title' "Smoke Group"
expect_jq_equals "$chats_file" '.[] | select(.id == -100777) | .type' "supergroup"

curl -fsS "${base_url}/api/settings?chat_id=-100777&user_id=7&signature=b8e86493" >"$group_settings_file"
expect_jq_equals "$group_settings_file" '.chat_title' "Smoke Group"
expect_jq_equals "$group_settings_file" '.chat_type' "supergroup"
expect_jq_equals "$group_settings_file" '.can_manage_deputies' "true"
expect_jq_equals "$group_settings_file" '.deputies[0].id' "8"

curl -fsS "${base_url}/api/settings/deputies/candidates?chat_id=-100777&user_id=7&signature=b8e86493&q=Deputy&limit=10" >"$deputy_candidates_file"
expect_jq_equals "$deputy_candidates_file" '.items[0].id' "8"
expect_jq_equals "$deputy_candidates_file" '.items[0].display_name' "Deputy"

curl -fsS \
  -X PUT \
  -H "Content-Type: application/json" \
  --data '{"chat_id":-100777,"user_id":7,"signature":"b8e86493","deputy_ids":[9]}' \
  "${base_url}/api/settings/deputies" >"$deputy_update_file"
expect_jq_equals "$deputy_update_file" '.ok' "true"
expect_jq_equals "$deputy_update_file" '.deputies[0].id' "9"

curl -fsS "${base_url}/api/settings?chat_id=-100777&user_id=7&signature=b8e86493" >"$group_settings_after_file"
expect_jq_equals "$group_settings_after_file" '.deputies[0].id' "9"

curl -fsS "${base_url}/api/settings/memory?chat_id=-100777&user_id=7&signature=b8e86493&limit=5" >"$memory_file"
expect_jq_equals "$memory_file" '.count' "1"
expect_jq_equals "$memory_file" '.cards[0].fact_text' "Smoke Group likes real DB settings smoke."
memory_id="$(jq -r '.cards[0].id' "$memory_file")"
curl -fsS \
  -X DELETE \
  "${base_url}/api/settings/memory?chat_id=-100777&user_id=7&signature=b8e86493&id=${memory_id}" >"$memory_delete_file"
expect_jq_equals "$memory_delete_file" '.ok' "true"
curl -fsS "${base_url}/api/settings/memory?chat_id=-100777&user_id=7&signature=b8e86493&limit=5" >"$memory_after_file"
expect_jq_equals "$memory_after_file" '.count' "0"
echo "+ /api/settings side APIs real-db ok"

curl -fsS -H "Cookie: admin_session=1001" \
  "${base_url}/admin/api/safety/checks?q=wc-smoke-ext&flagged=true&limit=10" >"$admin_safety_file"
expect_jq_equals "$admin_safety_file" '.count' "1"
expect_jq_equals "$admin_safety_file" '.checks[0].external_session_id' "wc-smoke-ext"
expect_jq_equals "$admin_safety_file" '.checks[0].flagged' "true"
expect_jq_equals "$admin_safety_file" '.checks[0].request_messages[0].content' "smoke risky text"

curl -fsS -H "Cookie: admin_session=1001" \
  "${base_url}/admin/api/analytics/llm/summary?range=24h" >"$admin_analytics_file"
expect_jq_equals "$admin_analytics_file" '.totals.total_count' "2"
expect_jq_equals "$admin_analytics_file" '.totals.error_count' "1"
expect_jq_equals "$admin_analytics_file" '.providers[] | select(.provider == "AI Farm") | .request_count' "1"
expect_jq_equals "$admin_analytics_file" '.models[] | select(.model == "smoke-model-a") | .output_tokens' "40"
expect_jq_equals "$admin_analytics_file" '.inference_params[] | select(.model == "smoke-model-a") | .max_tokens' "512"
expect_jq_equals "$admin_analytics_file" '.top_chats[] | select(.chat_id == -100777) | .title' "Smoke Group"
expect_jq_equals "$admin_analytics_file" '.memory_runs.completed_count' "1"
expect_jq_equals "$admin_analytics_file" '.memory_runs.statuses[] | select(.status == "completed") | .message_count' "12"
echo "+ admin safety/analytics seeded telemetry ok"

curl -fsS \
  -X POST \
  -H "Cookie: admin_session=1001" \
  -H "Content-Type: application/json" \
  --data '{"chat_id":-100777,"mood_alignment":"admin-smoke-mood","custom_persona":"admin smoke persona","reactivity_percentage":77,"proactivity_percentage":23,"enable_global_text_reply":true,"enable_global_draw_reply":true,"enable_obscenifier":true,"enable_profanity":false,"enable_greet_joiners":false,"enable_daily_game":false,"daily_game_theme":"admin-smoke-theme","greeting_html":"<b>hello smoke</b>"}' \
  "${base_url}/admin/api/chat/settings" >"$admin_chat_update_file"
expect_jq_equals "$admin_chat_update_file" '.ok' "true"
curl -fsS -H "Cookie: admin_session=1001" \
  "${base_url}/admin/api/chat?chat_id=-100777" >"$admin_chat_after_update_file"
expect_jq_equals "$admin_chat_after_update_file" '.settings.mood_alignment' "admin-smoke-mood"
expect_jq_equals "$admin_chat_after_update_file" '.settings.custom_persona' "admin smoke persona"
expect_jq_equals "$admin_chat_after_update_file" '.settings.reactivity_percentage' "77"
expect_jq_equals "$admin_chat_after_update_file" '.settings.enable_global_draw_reply' "true"
expect_jq_equals "$admin_chat_after_update_file" '.settings.daily_game_theme' "admin-smoke-theme"

curl -fsS \
  -X POST \
  -H "Cookie: admin_session=1001" \
  "${base_url}/admin/api/chat/block?chat_id=-100777&minutes=10" >"$admin_chat_block_file"
expect_jq_equals "$admin_chat_block_file" '.ok' "true"
curl -fsS -H "Cookie: admin_session=1001" \
  "${base_url}/admin/api/chat?chat_id=-100777" >"$admin_chat_blocked_file"
expect_jq_equals "$admin_chat_blocked_file" '.blocked' "true"
curl -fsS \
  -X DELETE \
  -H "Cookie: admin_session=1001" \
  "${base_url}/admin/api/chat/unblock?chat_id=-100777" >"$admin_chat_unblock_file"
expect_jq_equals "$admin_chat_unblock_file" '.ok' "true"
curl -fsS -H "Cookie: admin_session=1001" \
  "${base_url}/admin/api/chat?chat_id=-100777" >"$admin_chat_unblocked_file"
expect_jq_equals "$admin_chat_unblocked_file" '.blocked' "false"

curl -fsS \
  -X POST \
  -H "Cookie: admin_session=1001" \
  -H "Content-Type: application/json" \
  --data '{"user_id":7,"days":3,"reason":"service smoke vip","vip":true}' \
  "${base_url}/admin/api/user/grant_vip" >"$admin_vip_grant_file"
expect_jq_equals "$admin_vip_grant_file" '.ok' "true"
curl -fsS -H "Cookie: admin_session=1001" \
  "${base_url}/admin/api/user?id=7" >"$admin_vip_after_grant_file"
expect_jq_equals "$admin_vip_after_grant_file" '.vip_summary.active' "true"
expect_jq_equals "$admin_vip_after_grant_file" '.vip_summary.latest_event_type' "admin_adjustment"
expect_jq_equals "$admin_vip_after_grant_file" '.vip_summary.latest_reason' "service smoke vip"
expect_jq_equals "$admin_vip_after_grant_file" '.vip_events[0].reason' "service smoke vip"
curl -fsS \
  -X DELETE \
  -H "Cookie: admin_session=1001" \
  "${base_url}/admin/api/user/revoke_vip?user_id=7&reason=service%20smoke%20revoke" >"$admin_vip_revoke_file"
expect_jq_equals "$admin_vip_revoke_file" '.ok' "true"
expect_jq_equals "$admin_vip_revoke_file" '.revoked' "true"
curl -fsS -H "Cookie: admin_session=1001" \
  "${base_url}/admin/api/user?id=7" >"$admin_vip_after_revoke_file"
expect_jq_equals "$admin_vip_after_revoke_file" '.vip_summary.latest_event_type' "admin_revoke"
expect_jq_equals "$admin_vip_after_revoke_file" '.vip_summary.latest_reason' "service smoke revoke"
expect_jq_equals "$admin_vip_after_revoke_file" '.vip_summary.remaining_seconds' "0"
echo "+ admin chat/vip mutation APIs ok"

runtime_token="$(seed_runtime_api_token)"

expect_http_status "401" "${runtime_base_url}/graphql" "$runtime_unauth_file" -k
expect_body_contains "$runtime_unauth_file" "unauthorized"

actual="$(curl -skS -H "Authorization: Bearer ${runtime_token}" -o "$runtime_method_file" -w '%{http_code}' "${runtime_base_url}/graphql")"
if [[ "$actual" != "405" ]]; then
  echo "expected runtime GraphQL GET to return 405, got ${actual}" >&2
  cat "$runtime_method_file" >&2
  exit 1
fi
expect_body_contains "$runtime_method_file" '"method not allowed"'

curl -skS \
  -H "Authorization: Bearer ${runtime_token}" \
  -H "Content-Type: application/json" \
  --data '{"query":"query { runtimeState { logLevel } healthSnapshot { db { status } redis { status } } configSnapshot { runtimeApiEnabled runtimeApiPort sqlRowLimit } }"}' \
  "${runtime_base_url}/graphql" >"$runtime_graphql_file"
expect_jq_equals "$runtime_graphql_file" '.data.runtimeState.logLevel' "info"
expect_jq_equals "$runtime_graphql_file" '.data.healthSnapshot.db.status' "ok"
expect_jq_equals "$runtime_graphql_file" '.data.healthSnapshot.redis.status' "ok"
expect_jq_equals "$runtime_graphql_file" '.data.configSnapshot.runtimeApiEnabled' "true"
expect_jq_equals "$runtime_graphql_file" '.data.configSnapshot.runtimeApiPort' "$runtime_port"

echo "+ runtime API TLS/auth/graphql ok"

if [[ "${OPENPLOTVA_SERVICE_SMOKE_WEB_UI:-0}" == "1" ]]; then
  if ! command -v npm >/dev/null 2>&1; then
    echo "npm is required for browser UI smoke" >&2
    exit 1
  fi

  seed_group_settings_fixtures

  chrome_default="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
  if [[ -z "${OPENPLOTVA_WEB_UI_BROWSER:-}" && -x "$chrome_default" ]]; then
    export OPENPLOTVA_WEB_UI_BROWSER="$chrome_default"
  fi

  export OPENPLOTVA_WEB_UI_BASE_URL="$base_url"
  export OPENPLOTVA_WEB_UI_ARTIFACT_DIR="${log_dir}/web-ui"
  export OPENPLOTVA_WEB_UI_HEADLESS="${OPENPLOTVA_WEB_UI_HEADLESS:-1}"
  mkdir -p "$OPENPLOTVA_WEB_UI_ARTIFACT_DIR"

  ui_node_dir="${log_dir}/web-ui-node"
  npm --prefix "$ui_node_dir" install --silent --no-audit --no-fund @playwright/test@1.60.0

  NODE_PATH="${ui_node_dir}/node_modules${NODE_PATH:+:$NODE_PATH}" \
    "$ui_node_dir/node_modules/.bin/playwright" test \
    tools/service-smoke.web-ui.spec.js \
    --browser=chromium \
    --workers=1 \
    --reporter=line \
    --output="$OPENPLOTVA_WEB_UI_ARTIFACT_DIR"
  echo "+ admin/settings browser UI smoke ok"
fi

echo "service-smoke-ok"
echo "log: ${app_log}"
