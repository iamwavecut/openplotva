#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  tools/live-update-injection-smoke.sh

Starts disposable Postgres + Dragonfly, runs openplotva-app with real Telegram
outbound workers and update consumption enabled, disables Telegram update
production, injects a known Telegram update into Redis, then waits for the
observable live artifact.

Default cases:
  bang_draw                 enqueues !draw and waits for a drawing-sticker artifact
  percent_draw              enqueues addressed % and waits for image history artifact
  image_edit_missing_prompt enqueues captioned edit verb without prompt and waits for ephemeral notice
  song_notice               enqueues !song and waits for an ephemeral scheduling notice

Extra cases:
  image_edit_file           seeds disposable VIP, enqueues a captioned photo edit, waits for connected image-edit artifacts
  vision_caption            enqueues an addressed captioned photo and waits for vision caption DB artifact
  music_vip                 seeds disposable VIP, enqueues !song, waits for music-vip completion
  guest_unsupported         injects guest_message draw request and waits for answerGuestQuery attempt
  guest_dialog              injects normal guest_message text and waits for dialog answer attempt
  guest_dialog_provider     same as guest_dialog, but fails if the fallback dialog answer is selected
  inline_query              injects inline_query and waits for answerInlineQuery attempt
  callback_empty            injects callback_query with empty data and waits for answerCallbackQuery attempt
  pre_checkout              injects pre_checkout_query and waits for answerPreCheckoutQuery attempt
  skipped_catalog           injects skipped update catalog and fails on unported terminal logs
  reset_notice              injects /reset and waits for history reset plus confirmation artifacts
  debug_no_reply            injects /debug without reply and waits for ephemeral diagnostic artifact
  delete_drawing_miss       injects /delete_drawing with no stored generation and waits for ephemeral artifact
  successful_payment_vip    injects successful_payment subscription and waits for VIP ledger artifact
  successful_payment_donate injects successful_payment donation and waits for donation artifact

Required env:
  BOT_KEY
  OPENPLOTVA_SMOKE_CHAT_ID
  OPENPLOTVA_SMOKE_USER_ID

Optional env:
  OPENPLOTVA_LIVE_UPDATE_SMOKE_CASES       comma list, or default/strict_live/all selector
                                           default keeps the four historical cases
                                          strict_live runs broad non-provider terminal/direct/skipped proofs
                                           loopback runs credential-free connected direct proofs
                                           loopback_media adds fake file image-edit proof to defaults
                                           loopback_all runs defaults plus media/direct/payment proofs
                                           all runs strict_live plus credential-aware optional cases
                                           offline_media runs deterministic no-credential media/provider substitutes only
  OPENPLOTVA_LIVE_UPDATE_SMOKE_PROMPT      prompt suffix
  OPENPLOTVA_LIVE_UPDATE_SMOKE_EDIT_PROMPT image edit prompt suffix
  OPENPLOTVA_LIVE_UPDATE_SMOKE_GUEST_TEXT  guest message text, default !draw guest cat
  OPENPLOTVA_LIVE_UPDATE_SMOKE_GUEST_DIALOG_TEXT normal guest dialog text
  OPENPLOTVA_LIVE_UPDATE_SMOKE_INLINE_TEXT inline query text
  OPENPLOTVA_LIVE_UPDATE_SMOKE_VISION_PROMPT vision prompt suffix
  OPENPLOTVA_LIVE_UPDATE_SMOKE_SONG_TOPIC  song topic suffix
  OPENPLOTVA_LIVE_UPDATE_SMOKE_VIP_DAYS    disposable VIP seed duration, default 1
  OPENPLOTVA_LIVE_UPDATE_SMOKE_PAYMENT_AMOUNT Stars amount for payment smokes, default 300
  OPENPLOTVA_LIVE_UPDATE_SMOKE_TIMEOUT_SECONDS
  OPENPLOTVA_LIVE_UPDATE_SMOKE_KEEP        keep disposable services when 1
  OPENPLOTVA_LIVE_UPDATE_SMOKE_POSTGRES_PORT
  OPENPLOTVA_LIVE_UPDATE_SMOKE_REDIS_PORT
  OPENPLOTVA_LIVE_UPDATE_SMOKE_WEB_PORT
  OPENPLOTVA_LIVE_UPDATE_SMOKE_PROVIDER_CASES add provider-backed optional cases when truthy
  OPENPLOTVA_LIVE_UPDATE_SMOKE_TELEGRAM_MODE=loopback use local fake Bot API plus fake Discovery
  OPENPLOTVA_LIVE_UPDATE_SMOKE_AIFARM_POOL=1 enable the runtime AIFarm pool before cases run
  BOT_API_BASE_URL                             optional Bot API base URL, auto-set in loopback mode
  OPENPLOTVA_SMOKE_PHOTO_FILE_ID           required for image_edit_file and vision_caption
  OPENPLOTVA_SMOKE_PHOTO_FILE_UNIQUE_ID    optional for image_edit_file and vision_caption
  OPENPLOTVA_SMOKE_LOG_DIR                 log directory, default mktemp
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

export PATH="/opt/homebrew/bin:$PATH"

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

require() {
  local command="$1"
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "${command} is required for live update injection smoke" >&2
    exit 1
  fi
}

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "${name} is required for live update injection smoke" >&2
    exit 1
  fi
}

is_truthy() {
  [[ "${1:-}" =~ ^([Tt][Rr][Uu][Ee]|1|[Yy][Ee][Ss])$ ]]
}

google_ai_key_source_present() {
  [[ -n "${GOOGLEAI_KEY:-}" || -n "${GOOGLEAI_KEY_STATS_FILE:-}" ]]
}

openrouter_dialog_provider_ready() {
  [[ -n "${OPENROUTER_KEY:-}" ]] && google_ai_key_source_present
}

dialog_provider_env_present() {
  [[ -n "${DIALOG_API_KEY:-}" \
    || -n "${DIALOG_AIFARM_POOL_API_KEY:-}" \
    || -n "${DIALOG_NVIDIA_API_KEY:-}" ]] \
    || google_ai_key_source_present \
    || openrouter_dialog_provider_ready
}

is_offline_media_selector() {
  case "$(printf '%s' "${OPENPLOTVA_LIVE_UPDATE_SMOKE_CASES:-}" | tr '-' '_' | xargs)" in
    offline_media|offline_provider_media|dry_media|dry_provider_media)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

is_loopback_telegram_mode() {
  case "$(printf '%s' "${OPENPLOTVA_LIVE_UPDATE_SMOKE_TELEGRAM_MODE:-}" | tr '[:upper:]' '[:lower:]' | xargs)" in
    loopback|fake|local)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

start_loopback_telegram_api() {
  local port="$1"
  local calls_file="$2"
  echo "+ starting loopback Telegram Bot API on 127.0.0.1:${port}"
  OPENPLOTVA_LOOPBACK_BOT_ID="${OPENPLOTVA_LOOPBACK_BOT_ID:-900000001}" \
    OPENPLOTVA_LOOPBACK_BOT_USERNAME="${OPENPLOTVA_LOOPBACK_BOT_USERNAME:-PlotvaBot}" \
    OPENPLOTVA_LOOPBACK_SMOKE_CHAT_ID="${OPENPLOTVA_SMOKE_CHAT_ID}" \
    python3 -u - "$port" "$calls_file" <<'PY' >"$telegram_api_log" 2>&1 &
import json
import base64
import os
import re
import sys
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

port = int(sys.argv[1])
calls_file = sys.argv[2]
bot_id = int(os.environ.get("OPENPLOTVA_LOOPBACK_BOT_ID", "900000001"))
bot_username = os.environ.get("OPENPLOTVA_LOOPBACK_BOT_USERNAME", "PlotvaBot")
default_chat_id = int(os.environ.get("OPENPLOTVA_LOOPBACK_SMOKE_CHAT_ID", "-100424242"))
message_id = 100000
job_kinds = {}
loopback_png = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC"
)
loopback_mp3 = b"ID3\x04\x00\x00\x00\x00\x00\x05LOOPBACK-MP3"

def bot_user():
    return {
        "id": bot_id,
        "is_bot": True,
        "first_name": "Plotva",
        "username": bot_username,
        "allows_users_to_create_topics": False,
        "can_connect_to_business": False,
        "can_join_groups": True,
        "can_manage_bots": False,
        "can_read_all_group_messages": False,
        "has_main_web_app": False,
        "has_topics_enabled": False,
        "supports_guest_queries": True,
        "supports_join_request_queries": True,
        "supports_inline_queries": True,
    }

def chat_for(chat_id):
    chat_id = int(chat_id or default_chat_id)
    if chat_id < 0:
        return {"id": chat_id, "type": "supergroup", "title": "OpenPlotva Loopback"}
    return {"id": chat_id, "type": "private", "first_name": "OpenPlotva", "username": "openplotva_loopback"}

def next_message(params, kind):
    global message_id
    message_id += 1
    chat_id = params.get("chat_id") or default_chat_id
    message = {
        "message_id": message_id,
        "date": int(time.time()),
        "chat": chat_for(chat_id),
        "from": bot_user(),
    }
    if kind == "sendSticker":
        message["sticker"] = {"file_id": "loopback-sticker", "file_unique_id": "loopback-sticker-unique", "type": "regular", "width": 512, "height": 512, "is_animated": False, "is_video": False}
    elif kind == "sendPhoto":
        message["photo"] = [{"file_id": "loopback-photo", "file_unique_id": "loopback-photo-unique", "width": 512, "height": 512}]
        if params.get("caption"):
            message["caption"] = str(params.get("caption"))
    elif kind == "sendAudio":
        message["audio"] = {"file_id": "loopback-audio", "file_unique_id": "loopback-audio-unique", "duration": 1, "file_name": "loopback.mp3"}
        if params.get("caption"):
            message["caption"] = str(params.get("caption"))
    else:
        message["text"] = str(params.get("text") or "loopback ok")
    return message

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
    if "application/x-www-form-urlencoded" in ctype:
        return {k: v[-1] for k, v in urllib.parse.parse_qs(raw.decode("utf-8", "replace")).items()}
    fields = {}
    text = raw.decode("utf-8", "replace")
    for name in ("chat_id", "text", "caption", "message_id", "inline_message_id"):
        match = re.search(r'name="' + re.escape(name) + r'"\r?\n\r?\n(.*?)\r?\n--', text, re.S)
        if match:
            fields[name] = match.group(1).strip()
    return fields

def discovery_result_payload():
    payload = {"image_url": ["https://loopback.openplotva.test/generated.png"]}
    return base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")

def decode_invocation_body(params):
    body = params.get("invocation", {}).get("body") if isinstance(params.get("invocation"), dict) else None
    if not body:
        return {}
    for decoder in (
        base64.b64decode,
        lambda value: base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)),
    ):
        try:
            payload = decoder(str(body)).decode("utf-8", "replace")
            value = json.loads(payload)
            return value if isinstance(value, dict) else {}
        except Exception:
            continue
    return {}

def infer_provider_job_kind(params):
    invocation = params.get("invocation") if isinstance(params.get("invocation"), dict) else {}
    if str(invocation.get("service_name", "")).strip() == "draw-api":
        return "draw"
    body = decode_invocation_body(params)
    schema_name = str(
        body.get("response_format", {})
            .get("json_schema", {})
            .get("name", "")
    )
    if schema_name == "optimize_song_prompt":
        return "song_prompt"
    if schema_name in {"optimize_prompt", "optimize_edit_prompt"}:
        return "image_prompt"
    messages = body.get("messages") if isinstance(body, dict) else None
    if isinstance(messages, list) and "image_url" in json.dumps(messages, ensure_ascii=False):
        return "vision"
    return "chat"

def song_prompt_content():
    lyrics = "\n".join([
        "[Verse 1]",
        "Loopback lights wake the quiet wire",
        "Synthetic rivers carry sparks higher",
        "Every queue keeps time with the drum",
        "Plotva swims until the proof has come",
        "[Chorus]",
        "Run the smoke and let it sing",
        "Every worker finds its wing",
        "From the cache to Telegram",
        "Loopback turns the gate to green",
        "[Verse 2]",
        "Tiny packets cross the midnight frame",
        "Provider shadows answer to their name",
        "Audio rises from the local stream",
        "Workers wake the old machine",
        "[Chorus]",
        "Run the smoke and let it sing",
        "Every worker finds its wing",
        "From the cache to Telegram",
        "Loopback turns the gate to green",
    ])
    return json.dumps({
        "title": "Loopback Gate",
        "input_topic": "openplotva loopback proof",
        "style": "synthwave, steady pulse, warm bass, 102 bpm",
        "vocal_language": "en",
        "lyrics": lyrics,
    })

def discovery_chat_completion_payload(kind="chat"):
    if kind == "song_prompt":
        content = song_prompt_content()
    elif kind == "vision":
        content = "loopback vision caption"
    elif kind == "image_prompt":
        content = json.dumps({
            "input": "loopback",
            "outputs": ["loopback optimized prompt"],
            "nsfw_result": "safe",
        })
    else:
        content = "loopback dialog answer"
    payload = {
        "id": "loopback-chat-completion",
        "object": "chat.completion",
        "created": int(time.time()),
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": content,
            },
            "finish_reason": "stop",
        }],
        "usage": {"prompt_tokens": 1, "completion_tokens": 8, "total_tokens": 9},
    }
    return base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")

def discovery_job_id(params):
    return str(params.get("idempotency_key") or params.get("job_id") or "loopback-draw-job")

def discovery_job_response_body(job_id):
    kind = job_kinds.get(job_id, "")
    if kind in {"song_prompt", "vision", "image_prompt", "chat"}:
        return discovery_chat_completion_payload(kind)
    return discovery_result_payload()

def is_provider_path(path):
    return path.endswith("/v1/jobs") or "/v1/jobs/" in path or path.endswith("/chat/completions")

def acestep_completion_response():
    return {
        "id": "loopback-acestep",
        "object": "chat.completion",
        "created": int(time.time()),
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "loopback ace-step audio",
                "audio": [{
                    "audio_url": {
                        "url": "data:audio/mpeg;base64," + base64.b64encode(loopback_mp3).decode("ascii")
                    }
                }],
            },
            "finish_reason": "stop",
        }],
    }

def result_for(path, method, params):
    if path.endswith("/v1/jobs/blocking"):
        job_id = discovery_job_id(params)
        job_kinds[job_id] = infer_provider_job_kind(params)
        return {"job_id": job_id, "state": "processing"}
    if path.endswith("/v1/jobs"):
        job_id = discovery_job_id(params)
        job_kinds[job_id] = infer_provider_job_kind(params)
        return {"job_id": job_id, "state": "queued"}
    if "/v1/jobs/" in path:
        job_id = path.rstrip("/").split("/")[-1] or discovery_job_id(params)
        return {
            "job": {
                "job_id": job_id,
                "state": "completed",
                "result": {
                    "response": {
                        "status_code": 200,
                        "body": discovery_job_response_body(job_id),
                        "content_type": "application/json",
                    }
                },
            }
        }
    if path.endswith("/chat/completions"):
        if isinstance(params, dict) and "audio_config" in params:
            return acestep_completion_response()
        return {
            "id": "loopback-chat-completion",
            "object": "chat.completion",
            "created": int(time.time()),
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "loopback vision caption"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 3, "total_tokens": 4},
        }
    if method == "getMe":
        return bot_user()
    if method in {"deleteWebhook", "setMyCommands", "deleteMyCommands", "sendChatAction", "answerCallbackQuery", "answerInlineQuery", "answerPreCheckoutQuery", "deleteMessage", "refundStarPayment", "editUserStarSubscription"}:
        return True
    if method == "answerGuestQuery":
        return {"inline_message_id": "loopback-guest-inline-message"}
    if method == "createInvoiceLink":
        return "https://t.me/PlotvaBot?start=loopback_invoice"
    if method == "getFile":
        return {"file_id": params.get("file_id", "loopback-file"), "file_unique_id": "loopback-file-unique", "file_path": "photos/loopback.jpg"}
    if method == "getChat":
        return chat_for(params.get("chat_id") or default_chat_id)
    if method == "getChatMember":
        return {"status": "administrator", "user": {"id": int(params.get("user_id") or 424242), "is_bot": False, "first_name": "OpenPlotva"}, "can_promote_members": True}
    if method == "getChatAdministrators":
        return [{"status": "creator", "user": {"id": int(os.environ.get("OPENPLOTVA_SMOKE_USER_ID", "424242")), "is_bot": False, "first_name": "OpenPlotva"}, "is_anonymous": False}]
    if method.startswith("editMessage"):
        return True
    if method in {"sendMessage", "sendRichMessage", "sendSticker", "sendPhoto", "sendAudio"}:
        return next_message(params, method)
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
        if parsed.path.startswith("/file/bot"):
            with open(calls_file, "a", encoding="utf-8") as calls:
                calls.write(json.dumps({"method": "downloadFile", "params": {"path": parsed.path}}, ensure_ascii=False) + "\n")
            self.send_response(200)
            self.send_header("content-type", "image/png")
            self.send_header("content-length", str(len(loopback_png)))
            self.end_headers()
            self.wfile.write(loopback_png)
            return
        method = parsed.path.rstrip("/").split("/")[-1]
        params = parse_body(self)
        query = urllib.parse.parse_qs(parsed.query)
        for key, values in query.items():
            if values:
                params.setdefault(key, values[-1])
        with open(calls_file, "a", encoding="utf-8") as calls:
            calls.write(json.dumps({"method": method, "params": params}, ensure_ascii=False) + "\n")
        result = result_for(parsed.path, method, params)
        if is_provider_path(parsed.path):
            body = json.dumps(result, ensure_ascii=False).encode("utf-8")
        else:
            body = json.dumps({"ok": True, "result": result}, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
PY
  telegram_api_pid="$!"
  wait_for_tcp telegram-api "$port"
}

run_offline_media_substitute_smoke() {
  echo "+ offline media substitute selector=${OPENPLOTVA_LIVE_UPDATE_SMOKE_CASES}"
  echo "+ deterministic substitute proof only; no Telegram update is injected and no live provider is called"
  echo "+ offline decoded image_edit_file-style routing substitute"
  cargo test -p openplotva-app live_redis_decoded_image_edit_schedules_and_completes_vip_job_when_url_is_set -- --nocapture
  echo "+ offline decoded album image-edit routing substitute"
  cargo test -p openplotva-app live_redis_decoded_album_image_edit_captures_sibling_media_when_url_is_set -- --nocapture
  echo "+ offline vision_caption-style provider/materializer substitutes"
  cargo test -p openplotva-app vision -- --nocapture
  echo "+ offline music_vip-style worker/provider substitutes"
  cargo test -p openplotva-app music_jobs -- --nocapture
  echo "live-update-injection-smoke-offline-media-ok"
}


free_port_from() {
  local port="$1"
  while nc -z 127.0.0.1 "$port" >/dev/null 2>&1; do
    port=$((port + 1))
  done
  printf '%s\n' "$port"
}

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

wait_for_postgres_ready() {
  local deadline=$((SECONDS + timeout_seconds))
  local stable=0
  while true; do
    if compose exec -T postgres pg_isready -U plotva -d plotva >/dev/null 2>&1 \
      && compose exec -T postgres psql -v ON_ERROR_STOP=1 -U plotva -d plotva -Atc "SELECT 1" >/dev/null 2>&1; then
      stable=$((stable + 1))
      if [[ "$stable" -ge 2 ]]; then
        echo "+ postgres ready for connections"
        return 0
      fi
    else
      stable=0
    fi
    if [[ "$SECONDS" -ge "$deadline" ]]; then
      echo "timed out waiting for Postgres readiness" >&2
      compose logs postgres >&2 || true
      exit 1
    fi
    sleep 0.5
  done
}

wait_for_http() {
  local url="$1"
  local output="$2"
  local deadline=$((SECONDS + timeout_seconds))
  while true; do
    if curl -fsS "$url" >"$output" 2>/dev/null; then
      echo "+ ${url} ready"
      return 0
    fi
    if [[ -n "${app_pid:-}" ]] && ! kill -0 "$app_pid" >/dev/null 2>&1; then
      echo "openplotva-app exited before ${url} became ready" >&2
      tail -n 200 "$app_log" >&2 || true
      exit 1
    fi
    if [[ "$SECONDS" -ge "$deadline" ]]; then
      echo "timed out waiting for ${url}" >&2
      tail -n 200 "$app_log" >&2 || true
      exit 1
    fi
    sleep 0.5
  done
}

compose() {
  OPENPLOTVA_DEV_POSTGRES_PORT="$postgres_port" \
    OPENPLOTVA_DEV_REDIS_PORT="$redis_port" \
    DB_POSTGRES_USER=plotva \
    DB_POSTGRES_PASSWORD=plotva \
    DB_POSTGRES_DB=plotva \
    docker compose -p "$project" "$@"
}

cleanup() {
  if [[ -n "${telegram_api_pid:-}" ]] && kill -0 "$telegram_api_pid" >/dev/null 2>&1; then
    kill "$telegram_api_pid" >/dev/null 2>&1 || true
    wait "$telegram_api_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n "${app_pid:-}" ]] && kill -0 "$app_pid" >/dev/null 2>&1; then
    kill "$app_pid" >/dev/null 2>&1 || true
    wait "$app_pid" >/dev/null 2>&1 || true
  fi
  if [[ "$started_compose" -eq 1 && "${OPENPLOTVA_LIVE_UPDATE_SMOKE_KEEP:-0}" != "1" ]]; then
    compose down -v >/dev/null 2>&1 || true
  fi
}

telegram_get_me() {
  local output="$1"
  local base_url="${BOT_API_BASE_URL:-https://api.telegram.org}"
  base_url="${base_url%/}"
  curl -fsS "${base_url}/bot${BOT_KEY}/getMe" >"$output"
  jq -e '.ok == true and .result.is_bot == true' "$output" >/dev/null
  jq -r '"+ bot @" + (.result.username // "<no username>") + " id=" + (.result.id | tostring)' "$output"
}

first_admin_id() {
  local ids="${ADMINS_ADMIN_IDS:-${ADMINS:-}}"
  ids="${ids//;/,}"
  ids="${ids// /,}"
  IFS=',' read -r -a parts <<<"$ids"
  for part in "${parts[@]}"; do
    part="$(printf '%s' "$part" | xargs)"
    if [[ "$part" =~ ^-?[0-9]+$ ]]; then
      printf '%s\n' "$part"
      return 0
    fi
  done
  if [[ "${OPENPLOTVA_SMOKE_USER_ID:-}" =~ ^-?[0-9]+$ ]]; then
    printf '%s\n' "$OPENPLOTVA_SMOKE_USER_ID"
    return 0
  fi
  return 1
}

enable_aifarm_pool_if_requested() {
  if ! is_truthy "${OPENPLOTVA_LIVE_UPDATE_SMOKE_AIFARM_POOL:-false}"; then
    return 0
  fi
  local admin_id
  admin_id="$(first_admin_id)" || {
    echo "OPENPLOTVA_LIVE_UPDATE_SMOKE_AIFARM_POOL requires ADMINS_ADMIN_IDS or OPENPLOTVA_SMOKE_USER_ID" >&2
    exit 1
  }
  local output="${log_dir}/aifarm-pool-enable.json"
  curl -fsS \
    -X POST \
    -H "Content-Type: application/json" \
    -H "X-Telegram-User-ID: ${admin_id}" \
    --data '{"enabled":true}' \
    "${base_url}/admin/api/aifarm/pool" >"$output"
  jq -e '.ok == true and .enabled == true' "$output" >/dev/null
  echo "+ runtime AIFarm pool enabled through admin API"
}

write_message_update() {
  local output="$1"
  local text="$2"
  local now
  local update_id
  local message_id
  local chat_type
  update_sequence=$((update_sequence + 1))
  now="$(date +%s)"
  update_id="$(((now % 10000000) * 100 + update_sequence))"
  message_id="$((100000 + (now % 800000) + update_sequence))"
  if [[ "$OPENPLOTVA_SMOKE_CHAT_ID" == -* ]]; then
    chat_type="supergroup"
  else
    chat_type="private"
  fi
  jq -n \
    --argjson update_id "$update_id" \
    --argjson message_id "$message_id" \
    --argjson date "$now" \
    --argjson chat_id "$OPENPLOTVA_SMOKE_CHAT_ID" \
    --arg chat_type "$chat_type" \
    --argjson user_id "$OPENPLOTVA_SMOKE_USER_ID" \
    --arg text "$text" \
    '{
      update_id: $update_id,
      message: {
        message_id: $message_id,
        date: $date,
        chat: (
          if $chat_type == "private" then
            {id: $chat_id, type: "private", first_name: "OpenPlotva", username: "openplotva_smoke_chat"}
          else
            {id: $chat_id, type: "supergroup", title: "OpenPlotva Smoke"}
          end
        ),
        from: {
          id: $user_id,
          is_bot: false,
          first_name: "OpenPlotva",
          username: "openplotva_smoke"
        },
        text: $text
      }
    }
    | if ($text | test("^/[A-Za-z0-9_]+(@[A-Za-z0-9_]+)?($|[[:space:]])")) then
        .message.entities = [{
          offset: 0,
          length: ($text | match("^\\S+").string | length),
          type: "bot_command"
        }]
      else
        .
      end' >"$output"
}

write_photo_caption_update() {
  local output="$1"
  local caption="$2"
  local photo_file_id="$3"
  local photo_file_unique_id="${4:-}"
  local now
  local update_id
  local message_id
  local chat_type
  update_sequence=$((update_sequence + 1))
  now="$(date +%s)"
  update_id="$(((now % 10000000) * 100 + update_sequence))"
  message_id="$((100000 + (now % 800000) + update_sequence))"
  if [[ -z "$photo_file_unique_id" ]]; then
    photo_file_unique_id="openplotva-smoke-photo-${now}-${update_sequence}"
  fi
  if [[ "$OPENPLOTVA_SMOKE_CHAT_ID" == -* ]]; then
    chat_type="supergroup"
  else
    chat_type="private"
  fi
  jq -n \
    --argjson update_id "$update_id" \
    --argjson message_id "$message_id" \
    --argjson date "$now" \
    --argjson chat_id "$OPENPLOTVA_SMOKE_CHAT_ID" \
    --arg chat_type "$chat_type" \
    --argjson user_id "$OPENPLOTVA_SMOKE_USER_ID" \
    --arg caption "$caption" \
    --arg photo_file_id "$photo_file_id" \
    --arg photo_file_unique_id "$photo_file_unique_id" \
    '{
      update_id: $update_id,
      message: {
        message_id: $message_id,
        date: $date,
        chat: (
          if $chat_type == "private" then
            {id: $chat_id, type: "private", first_name: "OpenPlotva", username: "openplotva_smoke_chat"}
          else
            {id: $chat_id, type: "supergroup", title: "OpenPlotva Smoke"}
          end
        ),
        from: {
          id: $user_id,
          is_bot: false,
          first_name: "OpenPlotva",
          username: "openplotva_smoke"
        },
        caption: $caption,
        photo: [
          {
            file_id: $photo_file_id,
            file_unique_id: $photo_file_unique_id,
            width: 1024,
            height: 1024,
            file_size: 262144
          }
        ]
      }
    }' >"$output"
}

write_guest_message_update() {
  local output="$1"
  local text="$2"
  local chat_id="${3:-$OPENPLOTVA_SMOKE_CHAT_ID}"
  local user_id="${4:-$OPENPLOTVA_SMOKE_USER_ID}"
  local now
  local update_id
  local message_id
  local chat_type
  local guest_query_id
  update_sequence=$((update_sequence + 1))
  now="$(date +%s)"
  update_id="$(((now % 10000000) * 100 + update_sequence))"
  message_id="$((100000 + (now % 800000) + update_sequence))"
  guest_query_id="openplotva-smoke-guest-${update_id}"
  if [[ "$chat_id" == -* ]]; then
    chat_type="supergroup"
  else
    chat_type="private"
  fi
  jq -n \
    --argjson update_id "$update_id" \
    --argjson message_id "$message_id" \
    --argjson date "$now" \
    --argjson chat_id "$chat_id" \
    --arg chat_type "$chat_type" \
    --argjson user_id "$user_id" \
    --arg text "$text" \
    --arg guest_query_id "$guest_query_id" \
    '{
      update_id: $update_id,
      guest_message: {
        message_id: $message_id,
        date: $date,
        chat: (
          if $chat_type == "private" then
            {id: $chat_id, type: "private", first_name: "OpenPlotva", username: "openplotva_smoke_chat"}
          else
            {id: $chat_id, type: "supergroup", title: "OpenPlotva Smoke"}
          end
        ),
        from: {
          id: $user_id,
          is_bot: false,
          first_name: "OpenPlotva",
          username: "openplotva_smoke"
        },
        text: $text,
        guest_query_id: $guest_query_id,
        guest_bot_caller_user: {
          id: $user_id,
          is_bot: false,
          first_name: "OpenPlotva",
          username: "openplotva_smoke"
        }
      }
    }' >"$output"
}

write_inline_query_update() {
  local output="$1"
  local query="$2"
  local now
  local update_id
  local inline_query_id
  update_sequence=$((update_sequence + 1))
  now="$(date +%s)"
  update_id="$(((now % 10000000) * 100 + update_sequence))"
  inline_query_id="openplotva-smoke-inline-${update_id}"
  jq -n \
    --argjson update_id "$update_id" \
    --argjson user_id "$OPENPLOTVA_SMOKE_USER_ID" \
    --arg inline_query_id "$inline_query_id" \
    --arg query "$query" \
    '{
      update_id: $update_id,
      inline_query: {
        id: $inline_query_id,
        from: {
          id: $user_id,
          is_bot: false,
          first_name: "OpenPlotva",
          username: "openplotva_smoke"
        },
        query: $query,
        offset: ""
      }
    }' >"$output"
}

write_callback_query_update() {
  local output="$1"
  local data="${2:-}"
  local now
  local update_id
  local message_id
  local callback_query_id
  local chat_type
  update_sequence=$((update_sequence + 1))
  now="$(date +%s)"
  update_id="$(((now % 10000000) * 100 + update_sequence))"
  message_id="$((100000 + (now % 800000) + update_sequence))"
  callback_query_id="openplotva-smoke-callback-${update_id}"
  if [[ "$OPENPLOTVA_SMOKE_CHAT_ID" == -* ]]; then
    chat_type="supergroup"
  else
    chat_type="private"
  fi
  jq -n \
    --argjson update_id "$update_id" \
    --argjson message_id "$message_id" \
    --argjson date "$now" \
    --argjson chat_id "$OPENPLOTVA_SMOKE_CHAT_ID" \
    --arg chat_type "$chat_type" \
    --argjson user_id "$OPENPLOTVA_SMOKE_USER_ID" \
    --arg callback_query_id "$callback_query_id" \
    --arg data "$data" \
    '{
      update_id: $update_id,
      callback_query: {
        id: $callback_query_id,
        from: {
          id: $user_id,
          is_bot: false,
          first_name: "OpenPlotva",
          username: "openplotva_smoke"
        },
        message: {
          message_id: $message_id,
          date: $date,
          chat: (
            if $chat_type == "private" then
              {id: $chat_id, type: "private", first_name: "OpenPlotva", username: "openplotva_smoke_chat"}
            else
              {id: $chat_id, type: "supergroup", title: "OpenPlotva Smoke"}
            end
          ),
          text: "OpenPlotva smoke callback"
        },
        chat_instance: "openplotva-smoke-chat-instance",
        data: $data
      }
    }' >"$output"
}

write_pre_checkout_update() {
  local output="$1"
  local now
  local update_id
  local pre_checkout_id
  local amount="${OPENPLOTVA_LIVE_UPDATE_SMOKE_PRECHECKOUT_AMOUNT:-300}"
  local payload="${OPENPLOTVA_LIVE_UPDATE_SMOKE_PRECHECKOUT_PAYLOAD:-subscription_${OPENPLOTVA_SMOKE_USER_ID}}"
  update_sequence=$((update_sequence + 1))
  now="$(date +%s)"
  update_id="$(((now % 10000000) * 100 + update_sequence))"
  pre_checkout_id="openplotva-smoke-precheckout-${update_id}"
  jq -n \
    --argjson update_id "$update_id" \
    --argjson user_id "$OPENPLOTVA_SMOKE_USER_ID" \
    --arg pre_checkout_id "$pre_checkout_id" \
    --argjson amount "$amount" \
    --arg payload "$payload" \
    '{
      update_id: $update_id,
      pre_checkout_query: {
        id: $pre_checkout_id,
        from: {
          id: $user_id,
          is_bot: false,
          first_name: "OpenPlotva",
          username: "openplotva_smoke"
        },
        currency: "XTR",
        total_amount: $amount,
        invoice_payload: $payload
      }
    }' >"$output"
}

write_successful_payment_update() {
  local output="$1"
  local payload="$2"
  local amount="${3:-${OPENPLOTVA_LIVE_UPDATE_SMOKE_PAYMENT_AMOUNT:-300}}"
  local now
  local update_id
  local message_id
  local charge_id
  local provider_charge_id
  local chat_type
  update_sequence=$((update_sequence + 1))
  now="$(date +%s)"
  update_id="$(((now % 10000000) * 100 + update_sequence))"
  message_id="$((100000 + (now % 800000) + update_sequence))"
  charge_id="openplotva-smoke-charge-${update_id}"
  provider_charge_id="openplotva-smoke-provider-${update_id}"
  if [[ "$OPENPLOTVA_SMOKE_CHAT_ID" == -* ]]; then
    chat_type="supergroup"
  else
    chat_type="private"
  fi
  jq -n \
    --argjson update_id "$update_id" \
    --argjson message_id "$message_id" \
    --argjson date "$now" \
    --argjson chat_id "$OPENPLOTVA_SMOKE_CHAT_ID" \
    --arg chat_type "$chat_type" \
    --argjson user_id "$OPENPLOTVA_SMOKE_USER_ID" \
    --arg payload "$payload" \
    --argjson amount "$amount" \
    --arg charge_id "$charge_id" \
    --arg provider_charge_id "$provider_charge_id" \
    '{
      update_id: $update_id,
      message: {
        message_id: $message_id,
        date: $date,
        chat: (
          if $chat_type == "private" then
            {id: $chat_id, type: "private", first_name: "OpenPlotva", username: "openplotva_smoke_chat"}
          else
            {id: $chat_id, type: "supergroup", title: "OpenPlotva Smoke"}
          end
        ),
        from: {
          id: $user_id,
          is_bot: false,
          first_name: "OpenPlotva",
          last_name: "Smoke",
          username: "openplotva_smoke",
          language_code: "en",
          is_premium: true
        },
        successful_payment: {
          currency: "XTR",
          total_amount: $amount,
          invoice_payload: $payload,
          telegram_payment_charge_id: $charge_id,
          provider_payment_charge_id: $provider_charge_id
        }
      }
    }' >"$output"
}

addressed_text() {
  local text="$1"
  if [[ "$OPENPLOTVA_SMOKE_CHAT_ID" == -* ]]; then
    if [[ -z "$bot_username" ]]; then
      echo "bot username is required for addressed group smoke cases" >&2
      exit 1
    fi
    printf '@%s %s\n' "$bot_username" "$text"
  else
    printf '%s\n' "$text"
  fi
}

command_text_for_bot() {
  local text="$1"
  local head
  local tail=""
  if [[ "$OPENPLOTVA_SMOKE_CHAT_ID" != -* || "$text" != /* ]]; then
    printf '%s\n' "$text"
    return 0
  fi
  if [[ -z "$bot_username" ]]; then
    echo "bot username is required for targeted group command smoke cases" >&2
    exit 1
  fi
  head="${text%% *}"
  if [[ "$text" == *" "* ]]; then
    tail="${text#* }"
  fi
  if [[ "$head" != *@* ]]; then
    head="${head}@${bot_username}"
  fi
  if [[ -n "$tail" ]]; then
    printf '%s %s\n' "$head" "$tail"
  else
    printf '%s\n' "$head"
  fi
}

update_queue_helper_bin=""

ensure_update_queue_helper() {
  if [[ -n "$update_queue_helper_bin" ]]; then
    return 0
  fi
  cargo build -q -p openplotva-updates --bin openplotva-enqueue-update
  local target_dir
  target_dir="$(cargo metadata --format-version 1 --no-deps | jq -r '.target_directory')"
  update_queue_helper_bin="${target_dir}/debug/openplotva-enqueue-update"
  if [[ ! -x "$update_queue_helper_bin" ]]; then
    echo "openplotva-enqueue-update helper was not built at ${update_queue_helper_bin}" >&2
    exit 1
  fi
}

run_update_queue_helper() {
  ensure_update_queue_helper
  "$update_queue_helper_bin" "$@"
}

enqueue_update() {
  local update_json="$1"
  run_update_queue_helper \
    enqueue \
    --redis-url "$redis_url" \
    --json-file "$update_json" \
    --allowed-only
}

enqueue_update_unfiltered() {
  local update_json="$1"
  run_update_queue_helper \
    enqueue \
    --redis-url "$redis_url" \
    --json-file "$update_json"
}

queue_len() {
  run_update_queue_helper \
    len \
    --redis-url "$redis_url"
}

wait_queue_empty() {
  run_update_queue_helper \
    wait-len \
    --redis-url "$redis_url" \
    --expected 0 \
    --timeout-seconds "$timeout_seconds"
}

wait_log_contains() {
  local needle="$1"
  local start_line="${2:-1}"
  local deadline=$((SECONDS + timeout_seconds))
  while true; do
    if [[ -r "$app_log" ]] && tail -n +"$start_line" "$app_log" | grep -Fq "$needle"; then
      return 0
    fi
    if [[ -n "${app_pid:-}" ]] && ! kill -0 "$app_pid" >/dev/null 2>&1; then
      echo "openplotva-app exited while waiting for log marker: ${needle}" >&2
      tail -n 200 "$app_log" >&2 || true
      exit 1
    fi
    if [[ "$SECONDS" -ge "$deadline" ]]; then
      echo "timed out waiting for log marker: ${needle}" >&2
      tail -n 200 "$app_log" >&2 || true
      exit 1
    fi
    sleep 1
  done
}

telegram_api_method_count() {
  local method="$1"
  if [[ ! -r "${telegram_api_calls:-}" ]]; then
    printf '0\n'
    return 0
  fi
  jq -sr --arg method "$method" '[.[]? | select(.method == $method)] | length' "$telegram_api_calls"
}

telegram_file_cache_count() {
  local file_unique_id="$1"
  compose exec -T postgres psql -v ON_ERROR_STOP=1 -v file_unique_id="$file_unique_id" -U plotva -d plotva -At <<'SQL'
    SELECT count(*)
    FROM telegram_files
    WHERE file_unique_id = :'file_unique_id'
      AND COALESCE(latest_file_id, '') <> '';
SQL
}

wait_loopback_telegram_method_count_at_least() {
  local method="$1"
  local at_least="$2"
  local deadline=$((SECONDS + timeout_seconds))
  local count
  if ! is_loopback_telegram_mode; then
    return 0
  fi
  while true; do
    count="$(telegram_api_method_count "$method" | tr -d '[:space:]')"
    if [[ "${count:-0}" =~ ^[0-9]+$ && "$count" -ge "$at_least" ]]; then
      return 0
    fi
    if [[ -n "${app_pid:-}" ]] && ! kill -0 "$app_pid" >/dev/null 2>&1; then
      echo "openplotva-app exited while waiting for loopback Telegram method: ${method}" >&2
      tail -n 200 "$app_log" >&2 || true
      [[ -r "${telegram_api_calls:-}" ]] && tail -n 80 "$telegram_api_calls" >&2 || true
      exit 1
    fi
    if [[ "$SECONDS" -ge "$deadline" ]]; then
      echo "timed out waiting for loopback Telegram method: ${method}; last count=${count:-<empty>}, expected=${at_least}" >&2
      tail -n 200 "$app_log" >&2 || true
      [[ -r "${telegram_api_calls:-}" ]] && tail -n 80 "$telegram_api_calls" >&2 || true
      exit 1
    fi
    sleep 1
  done
}

wait_telegram_file_cached() {
  local file_unique_id="$1"
  local deadline=$((SECONDS + timeout_seconds))
  local count
  while true; do
    count="$(telegram_file_cache_count "$file_unique_id" | tail -n 1 | tr -d '[:space:]')"
    if [[ "${count:-0}" =~ ^[0-9]+$ && "$count" -ge 1 ]]; then
      return 0
    fi
    if [[ -n "${app_pid:-}" ]] && ! kill -0 "$app_pid" >/dev/null 2>&1; then
      echo "openplotva-app exited while waiting for Telegram file cache artifact" >&2
      tail -n 200 "$app_log" >&2 || true
      exit 1
    fi
    if [[ "$SECONDS" -ge "$deadline" ]]; then
      echo "timed out waiting for Telegram file cache artifact; file_unique_id=${file_unique_id} count=${count:-<empty>}" >&2
      tail -n 200 "$app_log" >&2 || true
      exit 1
    fi
    sleep 1
  done
}

wait_loopback_image_edit_artifacts() {
  local photo_file_unique_id="$1"
  local before_get_file="$2"
  local before_send_sticker="$3"
  local before_blocking="$4"
  local before_jobs="$5"
  local before_delete_message="$6"
  wait_telegram_file_cached "$photo_file_unique_id"
  wait_loopback_telegram_method_count_at_least getFile "$((before_get_file + 1))"
  wait_loopback_telegram_method_count_at_least sendSticker "$((before_send_sticker + 1))"
  wait_loopback_telegram_method_count_at_least blocking "$((before_blocking + 1))"
  wait_loopback_telegram_method_count_at_least jobs "$((before_jobs + 1))"
  wait_loopback_telegram_method_count_at_least deleteMessage "$((before_delete_message + 1))"
  echo "+ image-edit loopback file/provider/Telegram artifacts observed"
}

append_case_unique() {
  local list="$1"
  local next="$2"
  if [[ -z "$list" ]]; then
    printf '%s\n' "$next"
  elif [[ ",${list}," == *",${next},"* ]]; then
    printf '%s\n' "$list"
  else
    printf '%s,%s\n' "$list" "$next"
  fi
}

configured_smoke_cases() {
  local requested="${OPENPLOTVA_LIVE_UPDATE_SMOKE_CASES:-default}"
  local default_cases="bang_draw,percent_draw,image_edit_missing_prompt,song_notice"
  local direct_cases="guest_unsupported,guest_dialog,inline_query,callback_empty,pre_checkout,skipped_catalog,reset_notice,debug_no_reply,delete_drawing_miss,successful_payment_vip,successful_payment_donate"
  local provider_direct_cases="guest_unsupported,guest_dialog_provider,inline_query,callback_empty,pre_checkout,skipped_catalog,reset_notice,debug_no_reply,delete_drawing_miss,successful_payment_vip,successful_payment_donate"
  local strict_live_cases="${default_cases},${direct_cases}"
  local loopback_cases="$direct_cases"
  local loopback_media_cases="${default_cases},image_edit_file,vision_caption,music_vip"
  local force_provider_cases="${OPENPLOTVA_LIVE_UPDATE_SMOKE_PROVIDER_CASES:-false}"
  local selected

  case "$(printf '%s' "$requested" | tr '-' '_' | xargs)" in
    ""|default)
      selected="$default_cases"
      ;;
    strict_live|strict|broad)
      selected="$strict_live_cases"
      ;;
    loopback|loopback_live|direct_proof|direct)
      selected="$loopback_cases"
      ;;
    loopback_media|loopback_file|file_loopback)
      selected="$loopback_media_cases"
      ;;
    loopback_all|loopback_full|full_loopback)
      if is_loopback_telegram_mode || is_truthy "$force_provider_cases" || dialog_provider_env_present; then
        selected="${loopback_media_cases},${provider_direct_cases}"
      else
        selected="${loopback_media_cases},${loopback_cases}"
      fi
      ;;
    all)
      if is_truthy "$force_provider_cases" || dialog_provider_env_present; then
        selected="${default_cases},${provider_direct_cases}"
      else
        selected="$strict_live_cases"
      fi
      if [[ -n "${OPENPLOTVA_SMOKE_PHOTO_FILE_ID:-}" ]]; then
        selected="$(append_case_unique "$selected" "image_edit_file")"
      fi
      if [[ -n "${OPENPLOTVA_SMOKE_PHOTO_FILE_ID:-}" ]] \
        && { is_truthy "$force_provider_cases" || dialog_provider_env_present; }; then
        selected="$(append_case_unique "$selected" "vision_caption")"
      fi
      if is_truthy "${ACESTEP_ENABLED:-false}"; then
        selected="$(append_case_unique "$selected" "music_vip")"
      fi
      ;;
    offline_media|offline_provider_media|dry_media|dry_provider_media)
      selected="offline_media"
      ;;
    *)
      selected="$requested"
      ;;
  esac

  printf '%s\n' "$selected"
}

scan_count() {
  local pattern="$1"
  run_update_queue_helper \
    scan-count \
    --redis-url "$redis_url" \
    --pattern "$pattern"
}

wait_scan_count() {
  local pattern="$1"
  local at_least="$2"
  run_update_queue_helper \
    wait-scan-count \
    --redis-url "$redis_url" \
    --pattern "$pattern" \
    --at-least "$at_least" \
    --timeout-seconds "$timeout_seconds"
}

sql_scalar() {
  local sql="$1"
  compose exec -T postgres psql -U plotva -d plotva -Atc "$sql"
}

wait_sql_count_at_least() {
  local sql="$1"
  local at_least="$2"
  local label="$3"
  local deadline=$((SECONDS + timeout_seconds))
  local value
  while true; do
    value="$(sql_scalar "$sql" | tail -n 1 | tr -d '[:space:]')"
    if [[ "${value:-0}" =~ ^[0-9]+$ && "$value" -ge "$at_least" ]]; then
      return 0
    fi
    if [[ -n "${app_pid:-}" ]] && ! kill -0 "$app_pid" >/dev/null 2>&1; then
      echo "openplotva-app exited while waiting for ${label}" >&2
      tail -n 200 "$app_log" >&2 || true
      exit 1
    fi
    if [[ "$SECONDS" -ge "$deadline" ]]; then
      echo "timed out waiting for ${label}; last count=${value:-<empty>}" >&2
      tail -n 200 "$app_log" >&2 || true
      exit 1
    fi
    sleep 1
  done
}

history_image_count() {
  sql_scalar "SELECT count(*) FROM chat_history_entries WHERE chat_id = ${OPENPLOTVA_SMOKE_CHAT_ID} AND kind = 'text' AND role = 'model' AND payload->'meta'->>'type' = 'image'"
}

vip_payment_event_count() {
  sql_scalar "SELECT count(*) FROM vip_events WHERE user_id = ${OPENPLOTVA_SMOKE_USER_ID} AND event_type = 'payment'"
}

donation_count() {
  sql_scalar "SELECT count(*) FROM donations WHERE user_id = ${OPENPLOTVA_SMOKE_USER_ID}"
}

telegram_file_vision_status() {
  local file_unique_id="$1"
  compose exec -T postgres psql -v ON_ERROR_STOP=1 -v file_unique_id="$file_unique_id" -U plotva -d plotva -At <<'SQL'
    SELECT COALESCE(vision_status, '') || E'\t' ||
           CASE WHEN NULLIF(BTRIM(COALESCE(vision_caption, '')), '') IS NULL THEN '0' ELSE '1' END
    FROM telegram_files
    WHERE file_unique_id = :'file_unique_id'
    LIMIT 1;
SQL
}

seed_vip_for_smoke_user() {
  local days="${OPENPLOTVA_LIVE_UPDATE_SMOKE_VIP_DAYS:-1}"
  local seconds
  if [[ ! "$days" =~ ^[1-9][0-9]*$ ]]; then
    echo "OPENPLOTVA_LIVE_UPDATE_SMOKE_VIP_DAYS must be a positive integer" >&2
    exit 1
  fi
  seconds=$((days * 86400))
  compose exec -T postgres psql -v ON_ERROR_STOP=1 -U plotva -d plotva -Atc "
    INSERT INTO users (id, first_name, username, discovered, updated)
    VALUES (${OPENPLOTVA_SMOKE_USER_ID}, 'OpenPlotva', 'openplotva_smoke', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    ON CONFLICT (id) DO UPDATE
      SET first_name = EXCLUDED.first_name,
          username = EXCLUDED.username,
          updated = CURRENT_TIMESTAMP;
    INSERT INTO vip_cache (user_id, is_vip, expires_at)
    VALUES (${OPENPLOTVA_SMOKE_USER_ID}, TRUE, CURRENT_TIMESTAMP + (${seconds} * INTERVAL '1 second'))
    ON CONFLICT (user_id) DO UPDATE
      SET is_vip = TRUE,
          expires_at = EXCLUDED.expires_at,
          updated_at = CURRENT_TIMESTAMP;
    SELECT id FROM vip_create_event(
      ${OPENPLOTVA_SMOKE_USER_ID},
      'admin_adjustment',
      ${seconds},
      NULL,
      ${OPENPLOTVA_SMOKE_USER_ID},
      'openplotva live music smoke'
    );
  " >/dev/null
  echo "+ seeded disposable VIP for ${OPENPLOTVA_SMOKE_USER_ID} (${days}d)"
}

wait_history_image_count() {
  local at_least="$1"
  local deadline=$((SECONDS + timeout_seconds))
  local count
  while true; do
    count="$(history_image_count | tr -d '[:space:]')"
    if [[ "${count:-0}" =~ ^[0-9]+$ && "$count" -ge "$at_least" ]]; then
      echo "$count"
      return 0
    fi
    if [[ -n "${app_pid:-}" ]] && ! kill -0 "$app_pid" >/dev/null 2>&1; then
      echo "openplotva-app exited while waiting for history image artifact" >&2
      tail -n 200 "$app_log" >&2 || true
      exit 1
    fi
    if [[ "$SECONDS" -ge "$deadline" ]]; then
      echo "timed out waiting for history image count >= ${at_least}; current=${count:-<empty>}" >&2
      tail -n 200 "$app_log" >&2 || true
      exit 1
    fi
    sleep 1
  done
}

dump_taskman_jobs() {
  compose exec -T postgres psql -U plotva -d plotva -Atc \
    "SELECT id, status, record FROM taskman_jobs WHERE queue_name='$1' ORDER BY id DESC LIMIT 5" \
    >&2 2>/dev/null || true
}

latest_music_job_status() {
  compose exec -T postgres psql -U plotva -d plotva -Atc \
    "SELECT status FROM taskman_jobs WHERE queue_name='music-vip' AND job_type='music_gen' AND user_id=${OPENPLOTVA_SMOKE_USER_ID} AND chat_id=${OPENPLOTVA_SMOKE_CHAT_ID} ORDER BY id DESC LIMIT 1" \
    2>/dev/null || true
}

wait_music_job_completed() {
  local deadline=$((SECONDS + timeout_seconds))
  local status
  while true; do
    status="$(latest_music_job_status | tr -d '[:space:]')"
    case "$status" in
      completed)
        echo "+ music-vip task completed"
        return 0
        ;;
      failed|cancelled)
        echo "music-vip task ended with status=${status}" >&2
        tail -n 120 "$app_log" >&2 || true
        dump_taskman_jobs music-vip
        exit 1
        ;;
    esac
    if [[ -n "${app_pid:-}" ]] && ! kill -0 "$app_pid" >/dev/null 2>&1; then
      echo "openplotva-app exited while waiting for music-vip completion" >&2
      tail -n 200 "$app_log" >&2 || true
      exit 1
    fi
    if [[ "$SECONDS" -ge "$deadline" ]]; then
      echo "timed out waiting for music-vip completion; current=${status:-<none>}" >&2
      tail -n 200 "$app_log" >&2 || true
      dump_taskman_jobs music-vip
      exit 1
    fi
    sleep 1
  done
}

latest_image_edit_job_status() {
  compose exec -T postgres psql -U plotva -d plotva -Atc \
    "SELECT status FROM taskman_jobs WHERE queue_name='image-vip' AND job_type='image_edit' AND user_id=${OPENPLOTVA_SMOKE_USER_ID} AND chat_id=${OPENPLOTVA_SMOKE_CHAT_ID} ORDER BY id DESC LIMIT 1" \
    2>/dev/null || true
}

latest_image_edit_generated_url_count() {
  compose exec -T postgres psql -U plotva -d plotva -Atc \
    "SELECT COALESCE(jsonb_array_length(record->'job'->'data'->'image_data'->'image_urls'), 0) FROM taskman_jobs WHERE queue_name='image-vip' AND job_type='image_edit' AND user_id=${OPENPLOTVA_SMOKE_USER_ID} AND chat_id=${OPENPLOTVA_SMOKE_CHAT_ID} ORDER BY id DESC LIMIT 1" \
    2>/dev/null || printf '0\n'
}

wait_image_edit_job_completed() {
  local deadline=$((SECONDS + timeout_seconds))
  local status
  local generated_count
  while true; do
    status="$(latest_image_edit_job_status | tr -d '[:space:]')"
    generated_count="$(latest_image_edit_generated_url_count | tr -d '[:space:]')"
    case "$status" in
      completed)
        if [[ "${generated_count:-0}" =~ ^[0-9]+$ && "$generated_count" -gt 0 ]]; then
          echo "+ image-edit task completed with generated URL artifact"
          return 0
        fi
        ;;
      failed|cancelled)
        echo "image-edit task ended with status=${status}" >&2
        tail -n 120 "$app_log" >&2 || true
        dump_taskman_jobs image-vip
        exit 1
        ;;
    esac
    if [[ -n "${app_pid:-}" ]] && ! kill -0 "$app_pid" >/dev/null 2>&1; then
      echo "openplotva-app exited while waiting for image-edit completion" >&2
      tail -n 200 "$app_log" >&2 || true
      exit 1
    fi
    if [[ "$SECONDS" -ge "$deadline" ]]; then
      echo "timed out waiting for image-edit completion; current=${status:-<none>} generated_urls=${generated_count:-<none>}" >&2
      tail -n 200 "$app_log" >&2 || true
      dump_taskman_jobs image-vip
      exit 1
    fi
    sleep 1
  done
}

wait_telegram_file_vision_completed() {
  local file_unique_id="$1"
  local deadline=$((SECONDS + timeout_seconds))
  local line
  local status
  local caption_present
  while true; do
    line="$(telegram_file_vision_status "$file_unique_id" | tail -n 1 || true)"
    status="${line%%$'\t'*}"
    caption_present="${line#*$'\t'}"
    if [[ "$line" != *$'\t'* ]]; then
      caption_present="0"
    fi
    case "$status" in
      completed)
        if [[ "$caption_present" == "1" ]]; then
          echo "+ vision caption completed for ${file_unique_id}"
          return 0
        fi
        ;;
      failed)
        echo "vision caption failed for ${file_unique_id}" >&2
        tail -n 160 "$app_log" >&2 || true
        exit 1
        ;;
    esac
    if [[ -n "${app_pid:-}" ]] && ! kill -0 "$app_pid" >/dev/null 2>&1; then
      echo "openplotva-app exited while waiting for vision caption" >&2
      tail -n 200 "$app_log" >&2 || true
      exit 1
    fi
    if [[ "$SECONDS" -ge "$deadline" ]]; then
      echo "timed out waiting for vision caption; status=${status:-<none>} file_unique_id=${file_unique_id}" >&2
      tail -n 200 "$app_log" >&2 || true
      exit 1
    fi
    sleep 1
  done
}

run_bang_draw_case() {
  local update_json="${log_dir}/bang-draw-update.json"
  local prompt="${OPENPLOTVA_LIVE_UPDATE_SMOKE_PROMPT:-openplotva live smoke}"
  local pattern="ephemeral_messages:${OPENPLOTVA_SMOKE_CHAT_ID}:*"
  local before
  local enqueue_result
  before="$(scan_count "$pattern")"
  write_message_update "$update_json" "!draw ${prompt}"
  echo "+ enqueue bang_draw update"
  enqueue_result="$(enqueue_update "$update_json")"
  if [[ "$enqueue_result" != "queued" ]]; then
    echo "bang_draw update was not enqueued: ${enqueue_result}" >&2
    exit 1
  fi
  echo "+ update queue len after enqueue: $(queue_len)"
  wait_queue_empty >/dev/null
  echo "+ update queue drained"
  wait_scan_count "$pattern" "$((before + 1))" >/dev/null
  echo "+ bang_draw live artifact observed under ${pattern}"
}

run_percent_draw_case() {
  local update_json="${log_dir}/percent-draw-update.json"
  local prompt="${OPENPLOTVA_LIVE_UPDATE_SMOKE_PROMPT:-openplotva live smoke}"
  local before
  local enqueue_result
  before="$(history_image_count | tr -d '[:space:]')"
  if [[ ! "${before:-0}" =~ ^[0-9]+$ ]]; then
    echo "could not read history image count: ${before:-<empty>}" >&2
    exit 1
  fi
  write_message_update "$update_json" "$(addressed_text "% ${prompt}")"
  echo "+ enqueue percent_draw update"
  enqueue_result="$(enqueue_update "$update_json")"
  if [[ "$enqueue_result" != "queued" ]]; then
    echo "percent_draw update was not enqueued: ${enqueue_result}" >&2
    exit 1
  fi
  echo "+ update queue len after enqueue: $(queue_len)"
  wait_queue_empty >/dev/null
  echo "+ update queue drained"
  wait_history_image_count "$((before + 1))" >/dev/null
  echo "+ percent_draw history image artifact observed"
}

run_ephemeral_text_case() {
  local case_name="$1"
  local text="$2"
  local update_json="${log_dir}/${case_name}-update.json"
  local pattern="ephemeral_messages:${OPENPLOTVA_SMOKE_CHAT_ID}:*"
  local before
  local enqueue_result
  before="$(scan_count "$pattern")"
  write_message_update "$update_json" "$(command_text_for_bot "$text")"
  echo "+ enqueue ${case_name} update"
  enqueue_result="$(enqueue_update "$update_json")"
  if [[ "$enqueue_result" != "queued" ]]; then
    echo "${case_name} update was not enqueued: ${enqueue_result}" >&2
    exit 1
  fi
  echo "+ update queue len after enqueue: $(queue_len)"
  wait_queue_empty >/dev/null
  echo "+ update queue drained"
  wait_scan_count "$pattern" "$((before + 1))" >/dev/null
  echo "+ ${case_name} ephemeral artifact observed under ${pattern}"
}

run_image_edit_missing_prompt_case() {
  local update_json="${log_dir}/image-edit-missing-prompt-update.json"
  local pattern="ephemeral_messages:${OPENPLOTVA_SMOKE_CHAT_ID}:*"
  local before
  local enqueue_result
  before="$(scan_count "$pattern")"
  write_photo_caption_update \
    "$update_json" \
    "$(addressed_text "fix")" \
    "openplotva-smoke-missing-prompt-photo" \
    "openplotva-smoke-missing-prompt-photo-${update_sequence}"
  echo "+ enqueue image_edit_missing_prompt update"
  enqueue_result="$(enqueue_update "$update_json")"
  if [[ "$enqueue_result" != "queued" ]]; then
    echo "image_edit_missing_prompt update was not enqueued: ${enqueue_result}" >&2
    exit 1
  fi
  echo "+ update queue len after enqueue: $(queue_len)"
  wait_queue_empty >/dev/null
  echo "+ update queue drained"
  wait_scan_count "$pattern" "$((before + 1))" >/dev/null
  echo "+ image_edit_missing_prompt ephemeral artifact observed under ${pattern}"
}

run_image_edit_file_case() {
  require_env OPENPLOTVA_SMOKE_PHOTO_FILE_ID
  local update_json="${log_dir}/image-edit-file-update.json"
  local prompt="${OPENPLOTVA_LIVE_UPDATE_SMOKE_EDIT_PROMPT:-contrast}"
  local photo_file_unique_id="${OPENPLOTVA_SMOKE_PHOTO_FILE_UNIQUE_ID:-openplotva-smoke-image-edit-$(date +%s)-${update_sequence}}"
  local enqueue_result
  local before_get_file
  local before_send_sticker
  local before_blocking
  local before_jobs
  local before_delete_message
  before_get_file="$(telegram_api_method_count getFile)"
  before_send_sticker="$(telegram_api_method_count sendSticker)"
  before_blocking="$(telegram_api_method_count blocking)"
  before_jobs="$(telegram_api_method_count jobs)"
  before_delete_message="$(telegram_api_method_count deleteMessage)"
  seed_vip_for_smoke_user
  write_photo_caption_update \
    "$update_json" \
    "$(addressed_text "fix ${prompt}")" \
    "$OPENPLOTVA_SMOKE_PHOTO_FILE_ID" \
    "$photo_file_unique_id"
  echo "+ enqueue image_edit_file update"
  enqueue_result="$(enqueue_update "$update_json")"
  if [[ "$enqueue_result" != "queued" ]]; then
    echo "image_edit_file update was not enqueued: ${enqueue_result}" >&2
    exit 1
  fi
  echo "+ update queue len after enqueue: $(queue_len)"
  wait_queue_empty >/dev/null
  echo "+ update queue drained"
  if is_loopback_telegram_mode; then
    wait_loopback_image_edit_artifacts \
      "$photo_file_unique_id" \
      "$before_get_file" \
      "$before_send_sticker" \
      "$before_blocking" \
      "$before_jobs" \
      "$before_delete_message"
  else
    wait_image_edit_job_completed
  fi
}

run_vision_caption_case() {
  require_env OPENPLOTVA_SMOKE_PHOTO_FILE_ID
  jq -e '.checks[] | select(.name == "dialog_jobs" and .status == "ok")' "$ready_file" >/dev/null
  local update_json="${log_dir}/vision-caption-update.json"
  local prompt="${OPENPLOTVA_LIVE_UPDATE_SMOKE_VISION_PROMPT:-what is in this image?}"
  local photo_file_unique_id="${OPENPLOTVA_SMOKE_PHOTO_FILE_UNIQUE_ID:-openplotva-smoke-vision-$(date +%s)-${update_sequence}}"
  local enqueue_result
  write_photo_caption_update \
    "$update_json" \
    "$(addressed_text "${prompt}")" \
    "$OPENPLOTVA_SMOKE_PHOTO_FILE_ID" \
    "$photo_file_unique_id"
  echo "+ enqueue vision_caption update"
  enqueue_result="$(enqueue_update "$update_json")"
  if [[ "$enqueue_result" != "queued" ]]; then
    echo "vision_caption update was not enqueued: ${enqueue_result}" >&2
    exit 1
  fi
  echo "+ update queue len after enqueue: $(queue_len)"
  wait_queue_empty >/dev/null
  echo "+ update queue drained"
  wait_telegram_file_vision_completed "$photo_file_unique_id"
}

run_song_notice_case() {
  local topic="${OPENPLOTVA_LIVE_UPDATE_SMOKE_SONG_TOPIC:-openplotva live smoke song}"
  run_ephemeral_text_case song_notice "!song ${topic}"
}

run_music_vip_case() {
  if ! is_truthy "${ACESTEP_ENABLED:-false}"; then
    echo "ACESTEP_ENABLED=true is required for music_vip live smoke" >&2
    exit 1
  fi
  jq -e '.checks[] | select(.name == "music_jobs" and .status == "ok")' "$ready_file" >/dev/null
  local update_json="${log_dir}/music-vip-update.json"
  local topic="${OPENPLOTVA_LIVE_UPDATE_SMOKE_SONG_TOPIC:-openplotva live smoke song}"
  local enqueue_result
  local before_send_rich
  before_send_rich="$(telegram_api_method_count sendRichMessage)"
  seed_vip_for_smoke_user
  write_message_update "$update_json" "!song ${topic}"
  echo "+ enqueue music_vip update"
  enqueue_result="$(enqueue_update "$update_json")"
  if [[ "$enqueue_result" != "queued" ]]; then
    echo "music_vip update was not enqueued: ${enqueue_result}" >&2
    exit 1
  fi
  echo "+ update queue len after enqueue: $(queue_len)"
  wait_queue_empty >/dev/null
  echo "+ update queue drained"
  if is_loopback_telegram_mode; then
    wait_loopback_telegram_method_count_at_least sendRichMessage "$((before_send_rich + 1))"
    wait_music_job_completed
    echo "+ music-vip loopback ACE-Step/sendRichMessage artifact observed"
  else
    wait_music_job_completed
  fi
}

run_guest_unsupported_case() {
  local update_json="${log_dir}/guest-unsupported-update.json"
  local text="${OPENPLOTVA_LIVE_UPDATE_SMOKE_GUEST_TEXT:-!draw guest cat}"
  local enqueue_result
  local start_line
  local before_method_count
  before_method_count="$(telegram_api_method_count answerGuestQuery)"
  start_line="$(( $(wc -l <"$app_log" | tr -d ' ') + 1 ))"
  write_guest_message_update "$update_json" "$text"
  echo "+ enqueue guest_unsupported update"
  enqueue_result="$(enqueue_update "$update_json")"
  if [[ "$enqueue_result" != "queued" ]]; then
    echo "guest_unsupported update was not enqueued: ${enqueue_result}" >&2
    exit 1
  fi
  echo "+ update queue len after enqueue: $(queue_len)"
  wait_queue_empty >/dev/null
  echo "+ update queue drained"
  if is_loopback_telegram_mode; then
    wait_loopback_telegram_method_count_at_least answerGuestQuery "$((before_method_count + 1))"
  else
    wait_log_contains "guest answer requested" "$start_line"
    wait_log_contains "guest unsupported feature answered" "$start_line"
  fi
  echo "+ guest_unsupported answerGuestQuery attempt observed in app log"
}

run_guest_dialog_case() {
  local require_provider="${1:-0}"
  local case_name="guest_dialog"
  local update_json
  local text="${OPENPLOTVA_LIVE_UPDATE_SMOKE_GUEST_DIALOG_TEXT:-hello plotva from live guest smoke}"
  local chat_id="$OPENPLOTVA_SMOKE_CHAT_ID"
  local enqueue_result
  local start_line
  local before_method_count
  before_method_count="$(telegram_api_method_count answerGuestQuery)"

  if [[ "$require_provider" == "1" ]]; then
    case_name="guest_dialog_provider"
    chat_id="${OPENPLOTVA_LIVE_UPDATE_SMOKE_GUEST_PROVIDER_CHAT_ID:-}"
    if [[ -z "$chat_id" ]]; then
      if [[ "$OPENPLOTVA_SMOKE_CHAT_ID" == -* ]]; then
        chat_id="$((OPENPLOTVA_SMOKE_CHAT_ID - 9001))"
      else
        chat_id="$((OPENPLOTVA_SMOKE_CHAT_ID + 9001))"
      fi
    fi
    jq -e '.checks[] | select(.name == "dialog_jobs" and .status == "ok")' "$ready_file" >/dev/null
  fi

  update_json="${log_dir}/${case_name}-update.json"
  start_line="$(( $(wc -l <"$app_log" | tr -d ' ') + 1 ))"
  write_guest_message_update "$update_json" "$text" "$chat_id"
  echo "+ enqueue ${case_name} update"
  enqueue_result="$(enqueue_update "$update_json")"
  if [[ "$enqueue_result" != "queued" ]]; then
    echo "${case_name} update was not enqueued: ${enqueue_result}" >&2
    exit 1
  fi
  echo "+ update queue len after enqueue: $(queue_len)"
  wait_queue_empty >/dev/null
  echo "+ update queue drained"
  if is_loopback_telegram_mode && [[ "$require_provider" != "1" ]]; then
    wait_loopback_telegram_method_count_at_least answerGuestQuery "$((before_method_count + 1))"
  else
    wait_log_contains "guest message accepted for dialog" "$start_line"
    wait_log_contains "guest answer requested" "$start_line"
    wait_log_contains "guest message answered" "$start_line"
    wait_loopback_telegram_method_count_at_least answerGuestQuery "$((before_method_count + 1))"
  fi

  if [[ "$require_provider" == "1" ]]; then
    if tail -n +"$start_line" "$app_log" | grep -Fq "guest dialog fallback answer selected"; then
      echo "guest_dialog_provider selected fallback answer; live dialog provider path was not proven" >&2
      tail -n 200 "$app_log" >&2 || true
      exit 1
    fi
    if tail -n +"$start_line" "$app_log" | grep -F "guest effect failed" | grep -Fq "run guest dialog"; then
      echo "guest_dialog_provider logged a guest dialog effect failure" >&2
      tail -n 200 "$app_log" >&2 || true
      exit 1
    fi
  fi

  echo "+ ${case_name} dialog answer path observed in app log"
}

run_inline_query_case() {
  local update_json="${log_dir}/inline-query-update.json"
  local text="${OPENPLOTVA_LIVE_UPDATE_SMOKE_INLINE_TEXT:-openplotva live inline smoke}"
  local enqueue_result
  local start_line
  local before_method_count
  before_method_count="$(telegram_api_method_count answerInlineQuery)"
  start_line="$(( $(wc -l <"$app_log" | tr -d ' ') + 1 ))"
  write_inline_query_update "$update_json" "$text"
  echo "+ enqueue inline_query update"
  enqueue_result="$(enqueue_update "$update_json")"
  if [[ "$enqueue_result" != "queued" ]]; then
    echo "inline_query update was not enqueued: ${enqueue_result}" >&2
    exit 1
  fi
  echo "+ update queue len after enqueue: $(queue_len)"
  wait_queue_empty >/dev/null
  echo "+ update queue drained"
  if is_loopback_telegram_mode; then
    wait_loopback_telegram_method_count_at_least answerInlineQuery "$((before_method_count + 1))"
  else
    wait_log_contains "inline query answer requested" "$start_line"
    wait_log_contains "inline query consumed" "$start_line"
  fi
  echo "+ inline_query answerInlineQuery path observed in app log"
}

run_callback_empty_case() {
  local update_json="${log_dir}/callback-empty-update.json"
  local enqueue_result
  local start_line
  local before_method_count
  before_method_count="$(telegram_api_method_count answerCallbackQuery)"
  start_line="$(( $(wc -l <"$app_log" | tr -d ' ') + 1 ))"
  write_callback_query_update "$update_json" ""
  echo "+ enqueue callback_empty update"
  enqueue_result="$(enqueue_update "$update_json")"
  if [[ "$enqueue_result" != "queued" ]]; then
    echo "callback_empty update was not enqueued: ${enqueue_result}" >&2
    exit 1
  fi
  echo "+ update queue len after enqueue: $(queue_len)"
  wait_queue_empty >/dev/null
  echo "+ update queue drained"
  if is_loopback_telegram_mode; then
    wait_loopback_telegram_method_count_at_least answerCallbackQuery "$((before_method_count + 1))"
  else
    wait_log_contains "callback query acknowledgement requested" "$start_line"
    wait_log_contains "callback query acknowledgement completed" "$start_line"
  fi
  echo "+ callback_empty answerCallbackQuery path observed in app log"
}

run_pre_checkout_case() {
  local update_json="${log_dir}/pre-checkout-update.json"
  local enqueue_result
  local start_line
  local before_method_count
  before_method_count="$(telegram_api_method_count answerPreCheckoutQuery)"
  start_line="$(( $(wc -l <"$app_log" | tr -d ' ') + 1 ))"
  write_pre_checkout_update "$update_json"
  echo "+ enqueue pre_checkout update"
  enqueue_result="$(enqueue_update "$update_json")"
  if [[ "$enqueue_result" != "queued" ]]; then
    echo "pre_checkout update was not enqueued: ${enqueue_result}" >&2
    exit 1
  fi
  echo "+ update queue len after enqueue: $(queue_len)"
  wait_queue_empty >/dev/null
  echo "+ update queue drained"
  if is_loopback_telegram_mode; then
    wait_loopback_telegram_method_count_at_least answerPreCheckoutQuery "$((before_method_count + 1))"
  else
    wait_log_contains "pre-checkout answer requested" "$start_line"
    wait_log_contains "pre-checkout answer completed" "$start_line"
  fi
  echo "+ pre_checkout answerPreCheckoutQuery path observed in app log"
}

write_skipped_catalog_update() {
  local output_file="$1"
  local kind="$2"
  local update_id="$3"

  case "$kind" in
    channel_post)
      jq -n --argjson update_id "$update_id" '{
        update_id: $update_id,
        channel_post: {
          message_id: 2,
          date: 1710000000,
          chat: {id: -10042, type: "channel", title: "News"},
          text: "post"
        }
      }' >"$output_file"
      ;;
    edited_channel_post)
      jq -n --argjson update_id "$update_id" '{
        update_id: $update_id,
        edited_channel_post: {
          message_id: 2,
          date: 1710000000,
          chat: {id: -10042, type: "channel", title: "News"},
          edit_date: 1710000001,
          text: "post edited"
        }
      }' >"$output_file"
      ;;
    chosen_inline_result)
      jq -n --argjson update_id "$update_id" '{
        update_id: $update_id,
        chosen_inline_result: {
          result_id: "result-1",
          from: {id: 111, is_bot: false, first_name: "Ada"},
          query: "hello"
        }
      }' >"$output_file"
      ;;
    shipping_query)
      jq -n --argjson update_id "$update_id" '{
        update_id: $update_id,
        shipping_query: {
          id: "ship-1",
          from: {id: 111, is_bot: false, first_name: "Ada"},
          invoice_payload: "payload",
          shipping_address: {
            country_code: "RU",
            state: "Chechen Republic",
            city: "Gudermes",
            street_line1: "Nuradilov st., 12",
            street_line2: "",
            post_code: "366200"
          }
        }
      }' >"$output_file"
      ;;
    poll)
      jq -n --argjson update_id "$update_id" '{
        update_id: $update_id,
        poll: {
          id: "poll-1",
          question: "Rust?",
          options: [
            {persistent_id: "1", text: "Yes", voter_count: 1},
            {persistent_id: "2", text: "No", voter_count: 0}
          ],
          total_voter_count: 1,
          is_closed: false,
          is_anonymous: true,
          type: "regular",
          allows_multiple_answers: false,
          allows_revoting: false,
          members_only: false
        }
      }' >"$output_file"
      ;;
    poll_answer)
      jq -n --argjson update_id "$update_id" '{
        update_id: $update_id,
        poll_answer: {
          poll_id: "poll-1",
          user: {id: 111, is_bot: false, first_name: "Ada"},
          option_ids: [0],
          option_persistent_ids: []
        }
      }' >"$output_file"
      ;;
    poll_answer_voter_chat)
      jq -n --argjson update_id "$update_id" '{
        update_id: $update_id,
        poll_answer: {
          poll_id: "poll-2",
          voter_chat: {id: -10043, type: "supergroup", title: "Poll Team"},
          option_ids: [1],
          option_persistent_ids: []
        }
      }' >"$output_file"
      ;;
    my_chat_member|chat_member)
      jq -n --argjson update_id "$update_id" --arg field "$kind" '
        {update_id: $update_id}
        | .[$field] = {
          chat: {id: -10042, type: "supergroup", title: "Lab"},
          from: {id: 111, is_bot: false, first_name: "Ada"},
          date: 1710000000,
          old_chat_member: {status: "left", user: {id: 222, is_bot: false, first_name: "User"}},
          new_chat_member: {status: "member", user: {id: 222, is_bot: false, first_name: "User"}}
        }
      ' >"$output_file"
      ;;
    chat_join_request)
      jq -n --argjson update_id "$update_id" '{
        update_id: $update_id,
        chat_join_request: {
          chat: {id: -10042, type: "supergroup", title: "Team"},
          from: {id: 111, is_bot: false, first_name: "Ada"},
          user_chat_id: 111,
          date: 1710000000
        }
      }' >"$output_file"
      ;;
    message_reaction)
      jq -n --argjson update_id "$update_id" '{
        update_id: $update_id,
        message_reaction: {
          chat: {id: 42, type: "private", first_name: "Ada"},
          message_id: 1,
          date: 1710000000,
          old_reaction: [{type: "emoji", emoji: "\uD83D\uDC4E"}],
          new_reaction: [{type: "emoji", emoji: "\uD83D\uDC4D"}]
        }
      }' >"$output_file"
      ;;
    message_reaction_count)
      jq -n --argjson update_id "$update_id" '{
        update_id: $update_id,
        message_reaction_count: {
          chat: {id: 42, type: "private", first_name: "Ada"},
          message_id: 1,
          date: 1710000000,
          reactions: [{type: {type: "emoji", emoji: "\uD83D\uDC4D"}, total_count: 1}]
        }
      }' >"$output_file"
      ;;
    future_unknown)
      jq -n --argjson update_id "$update_id" '{
        update_id: $update_id,
        future_update_shape: {value: true}
      }' >"$output_file"
      ;;
    business_connection)
      jq -n --argjson update_id "$update_id" '{
        update_id: $update_id,
        business_connection: {
          id: "business-1",
          user: {id: 114, is_bot: false, first_name: "Business"},
          user_chat_id: 114,
          date: 1710000000,
          is_enabled: true
        }
      }' >"$output_file"
      ;;
    business_message|edited_business_message)
      jq -n --argjson update_id "$update_id" --arg field "$kind" '
        {update_id: $update_id}
        | .[$field] = {
          message_id: 1,
          date: 1710000000,
          business_connection_id: "business-1",
          chat: {id: 115, type: "private", first_name: "BusinessChat"},
          from: {id: 115, is_bot: false, first_name: "BusinessChat"},
          text: "business text"
        }
      ' >"$output_file"
      ;;
    deleted_business_messages)
      jq -n --argjson update_id "$update_id" '{
        update_id: $update_id,
        deleted_business_messages: {
          business_connection_id: "business-1",
          chat: {id: 115, type: "private", first_name: "BusinessChat"},
          message_ids: [1, 2]
        }
      }' >"$output_file"
      ;;
    chat_boost)
      jq -n --argjson update_id "$update_id" '{
        update_id: $update_id,
        chat_boost: {
          chat: {id: -10045, type: "supergroup", title: "Boost Team"},
          boost: {
            boost_id: "boost-1",
            add_date: 1710000000,
            expiration_date: 1710086400,
            source: {source: "gift_code", user: {id: 118, is_bot: false, first_name: "Booster"}}
          }
        }
      }' >"$output_file"
      ;;
    removed_chat_boost)
      jq -n --argjson update_id "$update_id" '{
        update_id: $update_id,
        removed_chat_boost: {
          boost_id: "boost-1",
          chat: {id: -10045, type: "supergroup", title: "Boost Team"},
          remove_date: 1710086400,
          source: {source: "gift_code", user: {id: 118, is_bot: false, first_name: "Booster"}}
        }
      }' >"$output_file"
      ;;
    managed_bot)
      jq -n --argjson update_id "$update_id" '{
        update_id: $update_id,
        managed_bot: {
          bot: {id: 900, is_bot: true, first_name: "ManagedBot"},
          user: {id: 116, is_bot: false, first_name: "Owner"}
        }
      }' >"$output_file"
      ;;
    purchased_paid_media)
      jq -n --argjson update_id "$update_id" '{
        update_id: $update_id,
        purchased_paid_media: {
          from: {id: 117, is_bot: false, first_name: "Buyer"},
          paid_media_payload: "paid-media-payload"
        }
      }' >"$output_file"
      ;;
    *)
      echo "unknown skipped catalog kind: $kind" >&2
      exit 1
      ;;
  esac
}


assert_no_unported_terminal_logs() {
  local start_line="${1:-1}"
  if tail -n +"$start_line" "$app_log" | grep -Fq "unported fetcher route"; then
    echo "live update smoke reached RuntimeUnhandledUpdateHandler" >&2
    tail -n 200 "$app_log" >&2 || true
    exit 1
  fi
}

run_skipped_catalog_case() {
  local start_line
  local kind
  local update_id
  local update_json
  local enqueue_result
  start_line="$(( $(wc -l <"$app_log" | tr -d ' ') + 1 ))"
  update_id=21000
  for kind in channel_post edited_channel_post chosen_inline_result shipping_query poll poll_answer poll_answer_voter_chat my_chat_member chat_member chat_join_request message_reaction message_reaction_count future_unknown business_connection business_message edited_business_message deleted_business_messages chat_boost removed_chat_boost managed_bot purchased_paid_media; do
    update_id=$((update_id + 1))
    update_json="${log_dir}/skipped-${kind}-update.json"
    write_skipped_catalog_update "$update_json" "$kind" "$update_id"
    echo "+ enqueue skipped_catalog ${kind} update"
    enqueue_result="$(enqueue_update_unfiltered "$update_json")"
    if [[ "$enqueue_result" != "queued" ]]; then
      echo "skipped_catalog ${kind} update was not enqueued: ${enqueue_result}" >&2
      exit 1
    fi
  done
  echo "+ update queue len after enqueue: $(queue_len)"
  wait_queue_empty >/dev/null
  echo "+ skipped_catalog update queue drained"
  if tail -n +"$start_line" "$app_log" | grep -Fq "unported fetcher route"; then
    echo "skipped_catalog reached RuntimeUnhandledUpdateHandler" >&2
    tail -n 200 "$app_log" >&2 || true
    exit 1
  fi
  echo "+ skipped_catalog consumed without unported terminal logs"
}

run_reset_notice_case() {
  local update_json="${log_dir}/reset_notice-update.json"
  local pattern="ephemeral_messages:${OPENPLOTVA_SMOKE_CHAT_ID}:*"
  local before
  local enqueue_result
  before="$(sql_scalar "SELECT count(*) FROM chat_history_resets WHERE chat_id = ${OPENPLOTVA_SMOKE_CHAT_ID} AND thread_id = 0" | tail -n 1 | tr -d '[:space:]')"
  if [[ ! "${before:-0}" =~ ^[0-9]+$ ]]; then
    echo "could not read reset history count: ${before:-<empty>}" >&2
    exit 1
  fi
  write_message_update "$update_json" "$(command_text_for_bot "/reset")"
  echo "+ enqueue reset_notice update"
  enqueue_result="$(enqueue_update "$update_json")"
  if [[ "$enqueue_result" != "queued" ]]; then
    echo "reset_notice update was not enqueued: ${enqueue_result}" >&2
    exit 1
  fi
  echo "+ update queue len after enqueue: $(queue_len)"
  wait_queue_empty >/dev/null
  echo "+ update queue drained"
  wait_sql_count_at_least \
    "SELECT count(*) FROM chat_history_resets WHERE chat_id = ${OPENPLOTVA_SMOKE_CHAT_ID} AND thread_id = 0" \
    "$((before + 1))" \
    "reset history row"
  wait_scan_count "$pattern" 1 >/dev/null
  echo "+ reset_notice history reset and confirmation artifacts observed"
}

run_debug_no_reply_case() {
  run_ephemeral_text_case debug_no_reply "/debug"
}

run_delete_drawing_miss_case() {
  run_ephemeral_text_case delete_drawing_miss "/delete_drawing"
}

run_successful_payment_vip_case() {
  local update_json="${log_dir}/successful-payment-vip-update.json"
  local before
  local enqueue_result
  before="$(vip_payment_event_count)"
  write_successful_payment_update "$update_json" "subscription_${OPENPLOTVA_SMOKE_USER_ID}"
  echo "+ enqueue successful_payment_vip update"
  enqueue_result="$(enqueue_update "$update_json")"
  if [[ "$enqueue_result" != "queued" ]]; then
    echo "successful_payment_vip update was not enqueued: ${enqueue_result}" >&2
    exit 1
  fi
  echo "+ update queue len after enqueue: $(queue_len)"
  wait_queue_empty >/dev/null
  echo "+ update queue drained"
  wait_sql_count_at_least \
    "SELECT count(*) FROM vip_events WHERE user_id = ${OPENPLOTVA_SMOKE_USER_ID} AND event_type = 'payment'" \
    "$((before + 1))" \
    "VIP payment event"
  echo "+ successful_payment_vip VIP ledger artifact observed"
}

run_successful_payment_donate_case() {
  local update_json="${log_dir}/successful-payment-donate-update.json"
  local amount="${OPENPLOTVA_LIVE_UPDATE_SMOKE_PAYMENT_AMOUNT:-300}"
  local before
  local enqueue_result
  before="$(donation_count)"
  write_successful_payment_update "$update_json" "donation_${OPENPLOTVA_SMOKE_USER_ID}_${amount}" "$amount"
  echo "+ enqueue successful_payment_donate update"
  enqueue_result="$(enqueue_update "$update_json")"
  if [[ "$enqueue_result" != "queued" ]]; then
    echo "successful_payment_donate update was not enqueued: ${enqueue_result}" >&2
    exit 1
  fi
  echo "+ update queue len after enqueue: $(queue_len)"
  wait_queue_empty >/dev/null
  echo "+ update queue drained"
  wait_sql_count_at_least \
    "SELECT count(*) FROM donations WHERE user_id = ${OPENPLOTVA_SMOKE_USER_ID}" \
    "$((before + 1))" \
    "donation payment row"
  echo "+ successful_payment_donate donation artifact observed"
}

require docker
require nc
require curl
require jq
if is_loopback_telegram_mode; then
  require python3
  export BOT_KEY="${BOT_KEY:-123456:OPENPLOTVA_LOOPBACK}"
  export OPENPLOTVA_SMOKE_CHAT_ID="${OPENPLOTVA_SMOKE_CHAT_ID:--100424242}"
  export OPENPLOTVA_SMOKE_USER_ID="${OPENPLOTVA_SMOKE_USER_ID:-424242}"
  export OPENPLOTVA_SMOKE_PHOTO_FILE_ID="${OPENPLOTVA_SMOKE_PHOTO_FILE_ID:-loopback-photo-file}"
  export OPENPLOTVA_SMOKE_PHOTO_FILE_UNIQUE_ID="${OPENPLOTVA_SMOKE_PHOTO_FILE_UNIQUE_ID:-loopback-photo-unique}"
  export DIALOG_PROVIDER="${DIALOG_PROVIDER:-aifarm}"
  export DIALOG_API_KEY="${DIALOG_API_KEY:-loopback-dialog-key}"
  export DIALOG_FALLBACK_PROVIDER="${DIALOG_FALLBACK_PROVIDER:-aifarm}"
  # Loopback mode owns a fake ACE-Step endpoint. Force it on so an auto-loaded
  # strict-live template with ACESTEP_ENABLED=false cannot disable the
  # credential-free music_vip proof.
  export ACESTEP_ENABLED=true
fi

if is_offline_media_selector; then
  run_offline_media_substitute_smoke
  exit 0
fi

require_env BOT_KEY
require_env OPENPLOTVA_SMOKE_CHAT_ID
require_env OPENPLOTVA_SMOKE_USER_ID

ensure_update_queue_helper

postgres_port="$(free_port_from "${OPENPLOTVA_LIVE_UPDATE_SMOKE_POSTGRES_PORT:-55632}")"
redis_port="$(free_port_from "${OPENPLOTVA_LIVE_UPDATE_SMOKE_REDIS_PORT:-56679}")"
web_port="$(free_port_from "${OPENPLOTVA_LIVE_UPDATE_SMOKE_WEB_PORT:-18180}")"
telegram_api_port="$(free_port_from "${OPENPLOTVA_LIVE_UPDATE_SMOKE_TELEGRAM_PORT:-18081}")"
redis_url="redis://127.0.0.1:${redis_port}/0"
project="openplotva-live-update-smoke-$$"
log_dir="${OPENPLOTVA_SMOKE_LOG_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/openplotva-live-update-smoke.XXXXXX")}"
timeout_seconds="${OPENPLOTVA_LIVE_UPDATE_SMOKE_TIMEOUT_SECONDS:-420}"
started_compose=0
app_pid=""
telegram_api_pid=""
app_log="${log_dir}/openplotva-app.log"
telegram_api_log="${log_dir}/telegram-api.log"
telegram_api_calls="${log_dir}/telegram-api-calls.jsonl"
ready_file="${log_dir}/ready.json"
telegram_getme_file="${log_dir}/telegram-getme.json"
host_cargo_home="${CARGO_HOME:-${HOME}/.cargo}"
host_rustup_home="${RUSTUP_HOME:-${HOME}/.rustup}"
update_sequence=0

mkdir -p "$log_dir/home"
trap cleanup EXIT

if is_loopback_telegram_mode; then
  export BOT_API_BASE_URL="http://127.0.0.1:${telegram_api_port}"
  export DISCOVERY_BASE_URL="${DISCOVERY_BASE_URL:-${BOT_API_BASE_URL}}"
  export ACESTEP_BASE_URL="${ACESTEP_BASE_URL:-${BOT_API_BASE_URL}}"
  export ACESTEP_API_MODE="${ACESTEP_API_MODE:-completion}"
  export ACESTEP_REQUEST_TIMEOUT_SECONDS="${ACESTEP_REQUEST_TIMEOUT_SECONDS:-30}"
  export ACESTEP_TASK_TIMEOUT_SECONDS="${ACESTEP_TASK_TIMEOUT_SECONDS:-60}"
  export ACESTEP_POLL_INTERVAL_SECONDS="${ACESTEP_POLL_INTERVAL_SECONDS:-1}"
  start_loopback_telegram_api "$telegram_api_port" "$telegram_api_calls"
elif [[ -n "${BOT_API_BASE_URL:-}" ]]; then
  export BOT_API_BASE_URL="${BOT_API_BASE_URL%/}"
fi

echo "+ Telegram getMe preflight"
telegram_get_me "$telegram_getme_file"
bot_username="$(jq -r '.result.username // ""' "$telegram_getme_file")"

echo "+ starting disposable Postgres/Dragonfly project=${project}"
compose up -d postgres dragonfly
started_compose=1
wait_for_tcp postgres "$postgres_port"
wait_for_postgres_ready
wait_for_tcp dragonfly "$redis_port"

echo "+ starting openplotva-app with update production disabled"
(
  export HOME="${log_dir}/home"
  export CARGO_HOME="$host_cargo_home"
  export RUSTUP_HOME="$host_rustup_home"
  export WEBAPP_HOST=127.0.0.1
  export WEBAPP_PORT="$web_port"
  export WEBAPP_URL="http://127.0.0.1:${web_port}"
  export DB_POSTGRES_HOST=127.0.0.1
  export DB_POSTGRES_PORT="$postgres_port"
  export DB_POSTGRES_USER=plotva
  export DB_POSTGRES_PASSWORD=plotva
  export DB_POSTGRES_DB=plotva
  export REDIS_HOST=127.0.0.1
  export REDIS_PORT="$redis_port"
  export REDIS_DB=0
  export OPENPLOTVA_CONNECT_SERVICES=true
  export OPENPLOTVA_RUN_MIGRATIONS=true
  export OPENPLOTVA_PRODUCE_UPDATES=false
  export OPENPLOTVA_CONSUME_UPDATES=true
  export BOT_API_BASE_URL="${BOT_API_BASE_URL:-}"
  export DISCOVERY_BASE_URL="${DISCOVERY_BASE_URL:-}"
  export ACESTEP_BASE_URL="${ACESTEP_BASE_URL:-}"
  export ACESTEP_API_MODE="${ACESTEP_API_MODE:-}"
  export ACESTEP_REQUEST_TIMEOUT_SECONDS="${ACESTEP_REQUEST_TIMEOUT_SECONDS:-}"
  export ACESTEP_TASK_TIMEOUT_SECONDS="${ACESTEP_TASK_TIMEOUT_SECONDS:-}"
  export ACESTEP_POLL_INTERVAL_SECONDS="${ACESTEP_POLL_INTERVAL_SECONDS:-}"
  export RUNTIME_API_ENABLED=false
  export BOT_WEBHOOK_ENABLED=false
  cargo run -q -p openplotva-app
) >"$app_log" 2>&1 &
app_pid="$!"

base_url="http://127.0.0.1:${web_port}"
wait_for_http "${base_url}/api/health" "${log_dir}/health.json"
wait_for_http "${base_url}/api/ready" "$ready_file"
jq -e '.checks[] | select(.name == "telegram_update_producer" and .status == "skipped") | (.message // .detail // "") | contains("OPENPLOTVA_PRODUCE_UPDATES=false")' "$ready_file" >/dev/null
jq -e '.checks[] | select(.name == "telegram_update_consumer" and .status == "ok")' "$ready_file" >/dev/null
echo "+ runtime ready with producer disabled and consumer enabled"
enable_aifarm_pool_if_requested

selected_cases="$(configured_smoke_cases)"
echo "+ live update smoke cases: ${selected_cases}"
smoke_terminal_start_line="$(( $(wc -l <"$app_log" | tr -d ' ') + 1 ))"
IFS=',' read -r -a cases <<<"$selected_cases"
for raw_case in "${cases[@]}"; do
  case_name="$(printf '%s' "$raw_case" | tr '-' '_' | xargs)"
  case "$case_name" in
    bang_draw)
      run_bang_draw_case
      ;;
    percent_draw)
      run_percent_draw_case
      ;;
    image_edit_missing_prompt)
      run_image_edit_missing_prompt_case
      ;;
    image_edit_missing_image)
      echo "+ image_edit_missing_image renamed to image_edit_missing_prompt for runtime-compatible routing"
      run_image_edit_missing_prompt_case
      ;;
    image_edit_file)
      run_image_edit_file_case
      ;;
    vision_caption)
      run_vision_caption_case
      ;;
    song_notice|song_vip_notice)
      run_song_notice_case
      ;;
    music_vip)
      run_music_vip_case
      ;;
    guest_dialog|guest_normal)
      run_guest_dialog_case 0
      ;;
    guest_dialog_provider)
      run_guest_dialog_case 1
      ;;
    guest_unsupported|guest)
      run_guest_unsupported_case
      ;;
    inline_query|inline)
      run_inline_query_case
      ;;
    callback_empty|callback)
      run_callback_empty_case
      ;;
    pre_checkout|precheckout)
      run_pre_checkout_case
      ;;
    skipped_catalog|skipped|skipped_updates)
      run_skipped_catalog_case
      ;;
    reset_notice|reset)
      run_reset_notice_case
      ;;
    debug_no_reply|debug)
      run_debug_no_reply_case
      ;;
    delete_drawing_miss|delete_drawing)
      run_delete_drawing_miss_case
      ;;
    successful_payment_vip|payment_vip)
      run_successful_payment_vip_case
      ;;
    successful_payment_donate|successful_payment_donation|payment_donate|payment_donation)
      run_successful_payment_donate_case
      ;;
    "")
      ;;
    *)
      echo "unsupported live update smoke case: ${raw_case}" >&2
      exit 2
      ;;
  esac
done

assert_no_unported_terminal_logs "$smoke_terminal_start_line"

echo "live-update-injection-smoke-ok"
echo "log: ${log_dir}"
