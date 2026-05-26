#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  tools/strict-live-preflight.sh

Runs lightweight live credential probes before the full strict-live gate:
  - Telegram getMe
  - Telegram getChatMember for OPENPLOTVA_SMOKE_CHAT_ID/OPENPLOTVA_SMOKE_USER_ID
  - Telegram getChatAdministrators for OPENPLOTVA_SMOKE_CHAT_ID
  - Serper.dev search using SERPER_API_KEY

The script never prints secret values.
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

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 2
  fi
}

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "${name} is required for strict-live preflight" >&2
    exit 1
  fi
}

preflight_fail() {
  echo "strict-live-preflight-failed: $*" >&2
  exit 1
}

validate_shape() {
  local name="$1"
  local description="$2"
  local pattern="$3"
  if [[ ! "${!name:-}" =~ $pattern ]]; then
    preflight_fail "${name} has invalid ${description}; value redacted"
  fi
}

http_request() {
  local label="$1"
  local output="$2"
  shift 2
  local status
  if ! status="$(curl -sS -o "$output" -w '%{http_code}' \
    --connect-timeout "${OPENPLOTVA_STRICT_LIVE_PREFLIGHT_CONNECT_TIMEOUT_SECONDS:-5}" \
    --max-time "${OPENPLOTVA_STRICT_LIVE_PREFLIGHT_TIMEOUT_SECONDS:-20}" \
    "$@")"; then
    preflight_fail "${label} request failed or timed out"
  fi
  if [[ ! "$status" =~ ^[0-9]{3}$ || "$((10#$status))" -lt 200 || "$((10#$status))" -ge 300 ]]; then
    local description
    description="$(jq -r '.description? // .error.message? // .error? // empty' "$output" 2>/dev/null || true)"
    if [[ -n "$description" ]]; then
      preflight_fail "${label} returned HTTP ${status}: ${description}"
    fi
    preflight_fail "${label} returned HTTP ${status}"
  fi
}

body_must_be_nonempty() {
  local label="$1"
  local path="$2"
  local bytes
  bytes="$(wc -c <"$path" | tr -d '[:space:]')"
  if [[ "${bytes:-0}" -le 0 ]]; then
    preflight_fail "${label} returned an empty body"
  fi
  echo "+ ${label} ok (${bytes} bytes)"
}

body_must_not_be_error_or_html() {
  local label="$1"
  local path="$2"
  if grep -Eq '^[[:space:]]*<' "$path"; then
    preflight_fail "${label} returned an HTML-like body instead of provider data"
  fi
  if jq -e 'type == "object" and ((.error? // null) != null or (.errors? // null) != null)' \
    "$path" >/dev/null 2>&1; then
    local description
    description="$(jq -r '.error.message? // .error.description? // .error? // (.errors | tostring) // empty' "$path" 2>/dev/null || true)"
    if [[ -n "$description" ]]; then
      preflight_fail "${label} returned an error envelope: ${description}"
    fi
    preflight_fail "${label} returned an error envelope"
  fi
}

telegram_api_url() {
  local method="$1"
  local base="${BOT_API_BASE_URL:-https://api.telegram.org}"
  printf '%s/bot%s/%s\n' "${base%/}" "$BOT_KEY" "$method"
}

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

require curl
require jq

require_env BOT_KEY
require_env OPENPLOTVA_SMOKE_CHAT_ID
require_env OPENPLOTVA_SMOKE_USER_ID
require_env SERPER_API_KEY
validate_shape BOT_KEY "Telegram token shape" '^[0-9]+:[A-Za-z0-9_-]{20,}$'
validate_shape OPENPLOTVA_SMOKE_CHAT_ID "integer chat id shape" '^-?[0-9]+$'
validate_shape OPENPLOTVA_SMOKE_USER_ID "positive integer user id shape" '^[1-9][0-9]*$'
validate_shape SERPER_API_KEY "non-placeholder API key shape" '^.+$'

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/openplotva-strict-live-preflight.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

telegram_getme="${tmp_dir}/telegram-getme.json"
telegram_member="${tmp_dir}/telegram-member.json"
telegram_admins="${tmp_dir}/telegram-admins.json"
serper_search="${tmp_dir}/serper-search.json"

echo "+ strict-live preflight: Telegram getMe"
http_request "Telegram getMe" "$telegram_getme" "$(telegram_api_url getMe)"
jq -e '.ok == true and .result.is_bot == true and (.result.id | type == "number")' \
  "$telegram_getme" >/dev/null || preflight_fail "Telegram getMe returned an invalid bot response"
telegram_bot_id="$(jq -r '.result.id' "$telegram_getme")"
jq -r '"+ Telegram bot @" + (.result.username // "<no username>") + " id=" + (.result.id | tostring)' \
  "$telegram_getme"

echo "+ strict-live preflight: Telegram getChatMember"
http_request "Telegram getChatMember" "$telegram_member" \
  --request POST \
  --data-urlencode "chat_id=${OPENPLOTVA_SMOKE_CHAT_ID}" \
  --data-urlencode "user_id=${OPENPLOTVA_SMOKE_USER_ID}" \
  "$(telegram_api_url getChatMember)"
jq -e '.ok == true and (.result.status | type == "string")' "$telegram_member" >/dev/null \
  || preflight_fail "Telegram getChatMember returned an invalid response"
jq -e '.result.status != "left" and .result.status != "kicked" and ((.result.status != "restricted") or (.result.is_member != false))' \
  "$telegram_member" >/dev/null \
  || preflight_fail "Telegram smoke user is not an active chat member; status redacted"
jq -r '"+ Telegram smoke member status=" + .result.status' "$telegram_member"

echo "+ strict-live preflight: Telegram getChatAdministrators"
http_request "Telegram getChatAdministrators" "$telegram_admins" \
  --request POST \
  --data-urlencode "chat_id=${OPENPLOTVA_SMOKE_CHAT_ID}" \
  "$(telegram_api_url getChatAdministrators)"
jq -e '.ok == true and (.result | type == "array")' "$telegram_admins" >/dev/null \
  || preflight_fail "Telegram getChatAdministrators returned an invalid response"
jq -r '"+ Telegram administrators observed=" + ((.result | length) | tostring)' "$telegram_admins"
if jq -e --argjson bot_id "$telegram_bot_id" 'any(.result[]?; .user.id == $bot_id)' \
  "$telegram_admins" >/dev/null; then
  jq -r --argjson bot_id "$telegram_bot_id" \
    '.result[] | select(.user.id == $bot_id) | "+ Telegram bot admin status=" + (.status // "administrator")' \
    "$telegram_admins"
else
  echo "+ Telegram bot admin status=not-admin; continuing because strict-live membership proof only requires getChatMember/getChatAdministrators access"
fi

echo "+ strict-live preflight: Serper search"
serper_search_url="${SERPER_BASE_URL:-https://google.serper.dev}"
serper_search_payload="$(jq -nc \
  --arg q "${OPENPLOTVA_SERPER_SMOKE_QUERY:-OpenPlotva Telegram bot}" \
  '{q: $q, hl: "ru", autocorrect: true, num: 10, page: 1}')"
http_request "Serper search" "$serper_search" \
  --request POST \
  "${serper_search_url%/}/search" \
  -H "X-API-KEY: ${SERPER_API_KEY}" \
  -H "Content-Type: application/json" \
  --data "$serper_search_payload"
body_must_be_nonempty "Serper search" "$serper_search"
body_must_not_be_error_or_html "Serper search" "$serper_search"
jq -e 'type == "object"' "$serper_search" >/dev/null \
  || preflight_fail "Serper search returned non-JSON provider data"

echo "strict-live-preflight-ok"
