#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  tools/provider-smoke.sh

Runs external-provider smoke checks that are intentionally kept out of normal
batch gates. Credentialed checks run only when their env is present or
explicitly enabled.

Default checks:
  - deterministic offline RBC request/decoder contract smokes
  - deterministic offline Serper request/crawl contract smokes
  - deterministic offline media/provider contract smokes batched by app/media filters
  - deterministic offline LLM/provider contract smokes batched by provider filters
  - live RBC USD/RUB provider smoke when the RBC endpoint is reachable

Optional env:
  BOT_KEY                                      run Telegram getMe smoke when set
  OPENPLOTVA_PROVIDER_SMOKE_RBC               auto, 1, or 0; default auto
  OPENPLOTVA_PROVIDER_SMOKE_SERPER            auto, 1, or 0; default auto
  OPENPLOTVA_PROVIDER_SMOKE_GEMINI            auto, 1, or 0; default auto
  OPENPLOTVA_PROVIDER_SMOKE_OPENROUTER        auto, 1, or 0; default auto
  OPENPLOTVA_PROVIDER_SMOKE_LOOPBACK          auto, 1, or 0; default auto runs selected live-shaped fake provider smokes when live inputs are absent
  SERPER_API_KEY                               Serper API key for live smoke
  GOOGLEAI_KEY                                 Gemini direct/provider smoke key
  OPENROUTER_KEY                               OpenRouter GenKit-compatible dialog smoke key
  OPENPLOTVA_SERPER_SMOKE_QUERY               live Serper search query
  OPENPLOTVA_PROVIDER_SMOKE_TELEGRAM          run Telegram getMe smoke when BOT_KEY is set, default 1
  OPENPLOTVA_PROVIDER_SMOKE_TELEGRAM_MEMBERSHIP
                                               auto, loopback, 1, or 0; runs Rust getChatMember/getChatAdministrators smoke
  OPENPLOTVA_PROVIDER_SMOKE_TELEGRAM_LOOPBACK  auto, 1, or 0; default auto runs loopback membership smoke when live inputs are absent
  OPENPLOTVA_SMOKE_CHAT_ID                     Telegram group/supergroup ID for membership smoke
  OPENPLOTVA_SMOKE_USER_ID                     Telegram user ID for membership smoke
  OPENPLOTVA_PROVIDER_SMOKE_DISCOVERY         auto, loopback, 1, or 0; default auto
  DISCOVERY_BASE_URL                          Discovery base URL for optional smoke
  DIALOG_DISCOVERY_SERVICE_NAME               Discovery service name, default llm-openai
  OPENPLOTVA_SMOKE_LOG_DIR                    output directory, default mktemp
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

export PATH="/opt/homebrew/bin:$PATH"

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"


log_dir="${OPENPLOTVA_SMOKE_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/openplotva-provider-smoke.XXXXXX")}"
telegram_getme_file="${log_dir}/telegram-getme.json"
discovery_capacity_file="${log_dir}/discovery-capacity.json"
rbc_preflight_file="${log_dir}/rbc-preflight.html"
telegram_loopback_log="${log_dir}/telegram-loopback.log"
loopback_pids=()

cleanup_loopbacks() {
  for pid in "${loopback_pids[@]}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
}
trap cleanup_loopbacks EXIT

require() {
  local command="$1"
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "${command} is required for provider smoke" >&2
    exit 1
  fi
}

provider_loopback_enabled() {
  local mode="${OPENPLOTVA_PROVIDER_SMOKE_LOOPBACK:-auto}"
  case "$mode" in
    auto|1)
      return 0
      ;;
    0)
      return 1
      ;;
    *)
      echo "OPENPLOTVA_PROVIDER_SMOKE_LOOPBACK must be auto, 1, or 0" >&2
      exit 2
      ;;
  esac
}

google_ai_key_source_present() {
  [[ -n "${GOOGLEAI_KEY:-}" || -n "${GOOGLEAI_KEY_STATS_FILE:-}" ]]
}

validate_auto_mode() {
  local name="$1"
  local value="$2"
  if [[ "$value" != "auto" && "$value" != "1" && "$value" != "0" ]]; then
    echo "${name} must be auto, 1, or 0" >&2
    exit 2
  fi
}

free_tcp_port() {
  python3 - <<'PY'
import socket
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

wait_for_http_server() {
  local url="$1"
  local deadline=$((SECONDS + 15))
  while (( SECONDS < deadline )); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  echo "timed out waiting for ${url}" >&2
  return 1
}

start_loopback_provider_api() {
  local port="$1"
  local chat_id="$2"
  local user_id="$3"
  echo "+ starting loopback provider API on 127.0.0.1:${port}"
  OPENPLOTVA_LOOPBACK_SMOKE_CHAT_ID="$chat_id" \
    OPENPLOTVA_LOOPBACK_SMOKE_USER_ID="$user_id" \
    python3 -u - "$port" >"$telegram_loopback_log" 2>&1 <<'PY' &
import json
import os
import sys
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

port = int(sys.argv[1])
chat_id = int(os.environ.get("OPENPLOTVA_LOOPBACK_SMOKE_CHAT_ID", "-100424242"))
user_id = int(os.environ.get("OPENPLOTVA_LOOPBACK_SMOKE_USER_ID", "424242"))

def bot_user():
    return {"id": 900000001, "is_bot": True, "first_name": "Plotva", "username": "PlotvaBot"}

def member_user():
    return {"id": user_id, "is_bot": False, "first_name": "OpenPlotva"}

def parse_body(handler):
    length = int(handler.headers.get("content-length", "0") or "0")
    raw = handler.rfile.read(length) if length else b""
    ctype = handler.headers.get("content-type", "")
    if not raw:
        return {}
    if "application/json" in ctype:
        try:
            value = json.loads(raw.decode("utf-8"))
            return value if isinstance(value, dict) else {}
        except Exception:
            return {}
    return {k: v[-1] for k, v in urllib.parse.parse_qs(raw.decode("utf-8", "replace")).items()}

def result_for(method, params):
    if method == "getMe":
        return bot_user()
    if method == "getChatMember":
        return {
            "status": "administrator",
            "user": member_user(),
            "can_be_edited": False,
            "is_anonymous": False,
            "can_manage_chat": True,
            "can_promote_members": True,
            "can_delete_messages": True,
            "can_restrict_members": True,
            "can_manage_video_chats": True,
            "can_change_info": True,
            "can_invite_users": True,
            "can_post_stories": True,
            "can_edit_stories": True,
            "can_delete_stories": True,
            "can_pin_messages": True,
            "can_manage_topics": True,
        }
    if method == "getChatAdministrators":
        return [{
            "status": "creator",
            "user": member_user(),
            "is_anonymous": False,
        }]
    return True

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        self.handle_any()

    def do_POST(self):
        self.handle_any()

    def handle_any(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.endswith("/v1/services/llm-openai/capacity") or "/v1/services/" in parsed.path and parsed.path.endswith("/capacity"):
            body = json.dumps({"available": True, "queue_depth": 0, "capacity": 1}, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        method = parsed.path.rstrip("/").split("/")[-1]
        params = parse_body(self)
        query = urllib.parse.parse_qs(parsed.query)
        for key, values in query.items():
            if values:
                params.setdefault(key, values[-1])
        if "chat_id" in params and int(params["chat_id"]) != chat_id:
            result = False
        else:
            result = result_for(method, params)
        body = json.dumps({"ok": True, "result": result}, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
PY
  local pid=$!
  loopback_pids+=("$pid")
  wait_for_http_server "http://127.0.0.1:${port}/botloopback/getMe"
}


run_rbc_contract_smoke() {
  echo "+ offline RBC provider command/tool contract smokes"
  cargo test -p openplotva-app rbc_fixture_smoke -- --nocapture
}

run_serper_contract_smoke() {
  echo "+ offline Serper provider command/tool contract smokes"
  cargo test -p openplotva-app serper -- --nocapture
}

run_serper_live_smoke() {
  local mode="${OPENPLOTVA_PROVIDER_SMOKE_SERPER:-auto}"
  if [[ "$mode" == "0" ]]; then
    echo "+ skip Serper live smoke"
    return
  fi
  if [[ "$mode" != "auto" && "$mode" != "1" ]]; then
    echo "OPENPLOTVA_PROVIDER_SMOKE_SERPER must be auto, 1, or 0" >&2
    exit 2
  fi
  if [[ -z "${SERPER_API_KEY:-}" ]]; then
    if [[ "$mode" == "1" ]]; then
      echo "SERPER_API_KEY is required for live Serper smoke" >&2
      exit 1
    fi
    echo "+ skip Serper live smoke: SERPER_API_KEY not set"
    return
  fi

  echo "+ live Serper search smoke"
  cargo test -p openplotva-app live_serper_smoke_searches \
    -- --ignored --nocapture
}

run_media_provider_contract_smoke() {
  echo "+ offline app AIFarm/media provider contract smokes"
  cargo test -p openplotva-app aifarm_ -- --nocapture
  echo "+ offline app image job worker/provider smokes"
  cargo test -p openplotva-app image_jobs -- --nocapture
  echo "+ offline app music job worker/provider smokes"
  cargo test -p openplotva-app music_jobs -- --nocapture
  echo "+ offline app vision provider/materializer smokes"
  cargo test -p openplotva-app vision -- --nocapture
  echo "+ offline ACE-Step client contract smokes"
  cargo test -p openplotva-media client_ -- --nocapture
}

run_llm_provider_contract_smoke() {
  echo "+ offline LLM provider contract smokes"
  cargo test -p openplotva-llm aifarm_ -- --nocapture
  cargo test -p openplotva-llm gemini_ -- --nocapture
  run_genkit_openai_compatible_contract_smoke
  cargo test -p openplotva-app provider -- --nocapture
  cargo test -p openplotva-app app_memory_extractor -- --nocapture
  cargo test -p openplotva-app gemini_ -- --nocapture
}

run_genkit_openai_compatible_contract_smoke() {
  echo "+ offline GenKit OpenRouter compatibility smokes"
  cargo test -p openplotva-app genkit_openai_compatible -- --nocapture
  cargo test -p openplotva-app genkit_dialog_provider_factory_builds_openrouter_plugin_route -- --nocapture
  cargo test -p openplotva-app genkit_dialog_provider_factory_preserves_go_google_key_requirement_for_plugin_routes -- --nocapture
  cargo test -p openplotva-app provider_media_genkit_fallback_routes -- --nocapture
  cargo test -p openplotva-app provider_youtube_config_routes -- --nocapture
}

run_gemini_live_smoke() {
  local mode="${OPENPLOTVA_PROVIDER_SMOKE_GEMINI:-auto}"
  validate_auto_mode OPENPLOTVA_PROVIDER_SMOKE_GEMINI "$mode"
  if [[ "$mode" == "0" ]]; then
    echo "+ skip Gemini live smoke"
    return
  fi
  if [[ -z "${GOOGLEAI_KEY:-}" ]]; then
    if [[ "$mode" == "1" ]]; then
      echo "GOOGLEAI_KEY is required for live Gemini provider smoke" >&2
      exit 1
    fi
    echo "+ skip Gemini live smoke: GOOGLEAI_KEY not set"
    return
  fi
  echo "+ live Gemini dialog provider smoke"
  cargo test -p openplotva-llm live_gemini_dialog_provider_smoke_completes_minimal_prompt \
    -- --ignored --nocapture
}

run_openrouter_live_smoke() {
  local mode="${OPENPLOTVA_PROVIDER_SMOKE_OPENROUTER:-auto}"
  validate_auto_mode OPENPLOTVA_PROVIDER_SMOKE_OPENROUTER "$mode"
  if [[ "$mode" == "0" ]]; then
    echo "+ skip OpenRouter live smoke"
    return
  fi
  if [[ -z "${OPENROUTER_KEY:-}" ]]; then
    if [[ "$mode" == "1" ]]; then
      echo "OPENROUTER_KEY is required for live OpenRouter smoke" >&2
      exit 1
    fi
    echo "+ skip OpenRouter live smoke: OPENROUTER_KEY not set"
    return
  fi
  if ! google_ai_key_source_present; then
    if [[ "$mode" == "1" ]]; then
      echo "GOOGLEAI_KEY or GOOGLEAI_KEY_STATS_FILE is required by the configured GenKit plugin route" >&2
      exit 1
    fi
    echo "+ skip OpenRouter live smoke: Google AI key source not set"
    return
  fi
  echo "+ live OpenRouter GenKit-compatible dialog smoke"
  cargo test -p openplotva-app live_genkit_openrouter_dialog_smoke_completes_minimal_prompt \
    -- --ignored --nocapture
}

run_rbc_smoke() {
  local mode="${OPENPLOTVA_PROVIDER_SMOKE_RBC:-auto}"
  if [[ "$mode" == "0" ]]; then
    echo "+ skip RBC smoke"
    return
  fi

  if [[ "$mode" == "auto" ]]; then
    require curl
    local timestamp
    local status
    timestamp="$(date +%s)000"
    if ! status="$(curl -sS -o "$rbc_preflight_file" -w '%{http_code}' \
      --connect-timeout "${OPENPLOTVA_PROVIDER_SMOKE_RBC_CONNECT_TIMEOUT_SECONDS:-5}" \
      --max-time "${OPENPLOTVA_PROVIDER_SMOKE_RBC_TIMEOUT_SECONDS:-10}" \
      -H 'Accept: */*' \
      -H 'Accept-Language: ru,be;q=0.9,en-US;q=0.8,en;q=0.7' \
      -H 'Cache-Control: no-cache' \
      -H 'Pragma: no-cache' \
      -H 'Referer: https://www.rbc.ru/quote/ticker/338247' \
      -H 'User-Agent: OpenPlotva/0.1' \
      "https://www.rbc.ru/quote/ajax/key-indicator-update/?_=${timestamp}")"; then
      echo "+ skip RBC smoke: endpoint preflight failed or timed out from this network"
      return
    fi
    if [[ "$status" -lt 200 || "$status" -ge 300 ]]; then
      echo "+ skip RBC smoke: endpoint returned HTTP ${status} from this network"
      return
    fi
  fi

  if [[ "$mode" != "auto" && "$mode" != "1" ]]; then
    echo "OPENPLOTVA_PROVIDER_SMOKE_RBC must be auto, 1, or 0" >&2
    exit 2
  fi

  echo "+ live RBC USD/RUB smoke"
  cargo test -p openplotva-app rbc_rates_client_live_smoke_fetches_usd_rub \
    -- --ignored --nocapture
}

run_telegram_smoke() {
  if [[ "${OPENPLOTVA_PROVIDER_SMOKE_TELEGRAM:-1}" != "1" ]]; then
    echo "+ skip Telegram smoke"
    return
  fi
  if [[ -z "${BOT_KEY:-}" ]]; then
    echo "+ skip Telegram smoke: BOT_KEY not set"
    return
  fi
  require curl
  require jq
  echo "+ Telegram getMe smoke"
  curl -fsS "https://api.telegram.org/bot${BOT_KEY}/getMe" >"$telegram_getme_file"
  jq -e '.ok == true and .result.is_bot == true and (.result.id | type == "number")' \
    "$telegram_getme_file" >/dev/null
  jq -r '"+ bot @" + (.result.username // "<no username>") + " id=" + (.result.id | tostring)' \
    "$telegram_getme_file"
}

run_telegram_membership_smoke() {
  local mode="${OPENPLOTVA_PROVIDER_SMOKE_TELEGRAM_MEMBERSHIP:-auto}"
  if [[ "$mode" == "0" ]]; then
    echo "+ skip Telegram membership smoke"
    return
  fi
  if [[ "$mode" != "auto" && "$mode" != "loopback" && "$mode" != "1" ]]; then
    echo "OPENPLOTVA_PROVIDER_SMOKE_TELEGRAM_MEMBERSHIP must be auto, loopback, 1, or 0" >&2
    exit 2
  fi
  if [[ "$mode" == "loopback" ]]; then
    run_telegram_membership_loopback_smoke
    return
  fi
  if [[ -z "${BOT_KEY:-}" ]]; then
    if [[ "$mode" == "1" ]]; then
      echo "BOT_KEY is required for Telegram membership smoke" >&2
      exit 1
    fi
    run_telegram_membership_loopback_smoke
    return
  fi
  if [[ -z "${OPENPLOTVA_SMOKE_CHAT_ID:-}" || -z "${OPENPLOTVA_SMOKE_USER_ID:-}" ]]; then
    if [[ "$mode" == "1" ]]; then
      echo "OPENPLOTVA_SMOKE_CHAT_ID and OPENPLOTVA_SMOKE_USER_ID are required" >&2
      exit 1
    fi
    run_telegram_membership_loopback_smoke
    return
  fi
  echo "+ live Telegram membership smoke"
  cargo test -p openplotva-app live_telegram_membership_smoke_gets_member_and_admins \
    -- --ignored --nocapture
}

run_telegram_membership_loopback_smoke() {
  local loopback_mode="${OPENPLOTVA_PROVIDER_SMOKE_TELEGRAM_LOOPBACK:-auto}"
  if [[ "$loopback_mode" == "0" ]]; then
    echo "+ skip Telegram membership smoke: live inputs absent and loopback disabled"
    return
  fi
  if [[ "$loopback_mode" != "auto" && "$loopback_mode" != "1" ]]; then
    echo "OPENPLOTVA_PROVIDER_SMOKE_TELEGRAM_LOOPBACK must be auto, 1, or 0" >&2
    exit 2
  fi
  require curl
  local port
  local chat_id="${OPENPLOTVA_SMOKE_CHAT_ID:--100424242}"
  local user_id="${OPENPLOTVA_SMOKE_USER_ID:-424242}"
  port="$(free_tcp_port)"
  start_loopback_provider_api "$port" "$chat_id" "$user_id"
  echo "+ loopback Telegram membership smoke"
  BOT_KEY=loopback-token \
    BOT_API_BASE_URL="http://127.0.0.1:${port}" \
    OPENPLOTVA_SMOKE_CHAT_ID="$chat_id" \
    OPENPLOTVA_SMOKE_USER_ID="$user_id" \
    cargo test -p openplotva-app live_telegram_membership_smoke_gets_member_and_admins \
      -- --ignored --nocapture
}

run_discovery_smoke() {
  local mode="${OPENPLOTVA_PROVIDER_SMOKE_DISCOVERY:-auto}"
  if [[ "$mode" == "0" ]]; then
    echo "+ skip Discovery smoke"
    return
  fi
  if [[ "$mode" == "loopback" ]]; then
    run_discovery_loopback_smoke
    return
  fi
  if [[ "$mode" != "auto" && "$mode" != "1" ]]; then
    echo "OPENPLOTVA_PROVIDER_SMOKE_DISCOVERY must be auto, loopback, 1, or 0" >&2
    exit 2
  fi
  if [[ -z "${DISCOVERY_BASE_URL:-}" ]]; then
    if [[ "$mode" == "1" ]]; then
      echo "DISCOVERY_BASE_URL is required when OPENPLOTVA_PROVIDER_SMOKE_DISCOVERY=1" >&2
      exit 1
    fi
    if provider_loopback_enabled; then
      run_discovery_loopback_smoke
    else
      echo "+ skip Discovery smoke: DISCOVERY_BASE_URL not set and loopback disabled"
    fi
    return
  fi
  run_discovery_capacity_smoke "$DISCOVERY_BASE_URL"
}

run_discovery_loopback_smoke() {
  require curl
  require jq
  local port
  port="$(free_tcp_port)"
  start_loopback_provider_api "$port" "-100424242" "424242"
  echo "+ loopback Discovery capacity smoke"
  run_discovery_capacity_smoke "http://127.0.0.1:${port}"
}

run_discovery_capacity_smoke() {
  local discovery_base_url="$1"
  if [[ -z "$discovery_base_url" ]]; then
    echo "DISCOVERY_BASE_URL is required" >&2
    exit 1
  fi
  require curl
  require jq
  local base="${discovery_base_url%/}"
  local service="${DIALOG_DISCOVERY_SERVICE_NAME:-llm-openai}"
  echo "+ Discovery capacity smoke ${base}/v1/services/${service}/capacity"
  curl -fsS --max-time 10 "${base}/v1/services/${service}/capacity" >"$discovery_capacity_file"
  jq -e 'type == "object"' "$discovery_capacity_file" >/dev/null
}

run_rbc_contract_smoke
run_serper_contract_smoke
run_serper_live_smoke
run_media_provider_contract_smoke
run_llm_provider_contract_smoke
run_gemini_live_smoke
run_openrouter_live_smoke
run_rbc_smoke
run_telegram_smoke
run_telegram_membership_smoke
run_discovery_smoke

echo "provider-smoke-ok"
echo "log: ${log_dir}"
