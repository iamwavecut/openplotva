#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  tools/update-queue-smoke.sh

Runs the decoded Telegram update Redis/Dragonfly replay smoke against a
disposable Dragonfly service, then removes the scratch container and volume.

Checks:
  - Update queue uses Redis list semantics: RPUSH producer, BLPOP consumer,
    FIFO order, LLEN/empty diagnostics
  - zstd/carapax JSON update codec round-trips decoded updates
  - all app-level `live_redis_decoded_*` replay tests run in one Cargo
    invocation against the same scratch Redis URL, instead of recompiling and
    launching one gate per route
  - one dequeued update can enter the runtime-shaped state/handle processor shell
  - one blocked-chat decoded message can cross Redis, persist state, and stop
    before downstream routing while `/settings` bypasses the blocked-chat gate
  - decoded private `/help`, group `/help@BotUsername`, private `/start`, and
    private `/start settings` can cross Redis, persist state, emit runtime-shaped
    help plans, delegate settings payloads, and avoid unported terminal
    delegation
  - decoded `/admin_help` and `/admin_queue_status` commands can cross Redis,
    persist state, queue runtime-shaped HTML admin replies, preserve group topic
    replies, and avoid unported terminal delegation
  - decoded `/admin_runtime_token`, `/admin_clear_cache`, and
    `/admin_clear_gemini_cache` commands can cross Redis, persist state, queue
    runtime-shaped admin replies, call their concrete boundaries, and avoid unported
    terminal delegation
  - decoded `/admin_settings` and `/admin_enable_chat@BotUsername` commands can
    cross Redis, persist state, queue runtime-shaped admin-panel/enabled-chat replies,
    run target resolution and chat-communication effects, preserve group topic
    replies, and avoid unported terminal delegation
  - decoded `/admin_grant_vip`, `/admin_cancel_vip@BotUsername`, and
    `/admin_refund` commands can cross Redis, persist state, run concrete VIP
    ledger/refund/cache effects, preserve authorization/routing, and avoid
    unported terminal delegation
  - decoded `chat_member` and `my_chat_member` updates can cross Redis, persist
    state, run member-state side effects, assign sync control jobs, and stop at
    the skipped terminal without loud unported delegation
  - decoded private `/settings`, group `/settings@BotUsername`, and
    `new_chat_members` updates can cross Redis, persist state, queue the
    runtime-shaped settings replies/control jobs, upsert new members, and preserve
    new-member delegation behavior
  - one decoded addressed text update can cross Redis, state stage, dialog
    debounce assignment, provider execution, virtual-message insertion, and
    dispatcher-backed sendMessage payload capture
  - one decoded random group message can cross Redis, state stage, random
    reactivity selection, dialog debounce assignment, provider execution,
    virtual-message insertion, and dispatcher-backed sendMessage payload capture
  - one decoded random obscenifier message can cross Redis, state stage,
    random reactivity and obscenifier selection, virtual-message insertion,
    and dispatcher-backed sendMessage payload capture
  - one decoded edited draw update can cross Redis, state stage, edited-message
    handler, pending dialog/image job mutation, debounce mutation, and no
    downstream delegation
  - decoded `$` and `;` updates can cross Redis, state stage, RBC fixture
    provider, rates command routing, and dispatcher-backed Telegram payload queueing
  - one decoded addressed translation update can cross Redis, state stage,
    control-job assignment, provider execution, and dispatcher-backed result
    payload queueing
  - one decoded `/reset` update can cross Redis, state stage, history reset
    cursor, virtual-message insertion, and one-minute ephemeral confirmation
    queueing
  - one decoded `/debug` no-reply update can cross Redis, state stage,
    virtual-message insertion, and two-minute ephemeral error payload queueing
  - one decoded `/ping` update can cross Redis, state stage, Redis AI ping
    timeout, and start/result Telegram payload queueing
  - one decoded `pre_checkout_query` update can cross Redis, state stage,
    direct acknowledgement, and no downstream delegation
  - one decoded `callback_query` update can cross Redis, state stage, terminal
    empty-data routing, direct answerCallbackQuery, and no downstream delegation
  - one decoded generic delete callback can cross Redis, state stage, callback
    pre-handler delegation, getChatMember authorization, deleteMessage, and
    answerCallbackQuery
  - one decoded VIP cancel callback can cross Redis, state stage, callback
    pre-handler delegation, editMessageText, and answerCallbackQuery
  - one decoded generated-lyrics delete callback can cross Redis, state stage,
    callback pre-handler delegation, getChatMember authorization, deleteMessage,
    and answerCallbackQuery
  - one decoded delete-drawing close callback can cross Redis, state stage,
    callback pre-handler delegation, deleteMessage, and answerCallbackQuery
  - one decoded check-in theme callback can cross Redis, state stage, callback
    pre-handler delegation, answerCallbackQuery, and high-priority control-job
    assignment
  - decoded `/checkin` commands can cross Redis, state stage, auto-theme
    selector routing, control-job assignment, existing-winner stats, and no
    downstream delegation
  - decoded payment producer updates can cross Redis, state stage, high-priority
    successful-payment/VIP/donate control-job assignment, and existing-VIP status
    short-circuiting
  - decoded targeted group `/vip@BotUsername` and
    `/donate@BotUsername amount` payment redirects can cross Redis, state stage,
    direct redirect payload shaping, and bare/wrong-target/unrelated group
    commands can delegate downstream
  - one decoded `/donate` update can cross Redis, state stage, shared control
    queue assignment, unified control worker execution, invoice-link request,
    and invoice-button message artifact capture
  - one decoded `/delete_drawing` no-generation update can cross Redis, state
    stage, last-generation miss, virtual-message insertion, and two-minute
    ephemeral HTML error payload queueing
  - one decoded `%` direct-draw update can cross Redis, state stage, image
    generator, Telegram photo payload shaping, and history recording
  - one decoded `!draw` update can cross Redis, state stage, `image-regular`
    task assignment, and worker completion
  - one decoded image-edit update can cross Redis, state stage, file metadata,
    VIP-gated file resolution, `image_edit` task assignment, and worker completion
  - one decoded image-edit validation update can cross Redis, state stage,
    virtual-message insertion, immediate two-minute ephemeral error payload
    queueing, and no downstream delegation
  - one decoded `!song` update can cross Redis, state stage, VIP gate,
    `music-vip` task assignment, and worker completion
  - one decoded song validation update can cross Redis, state stage,
    missing-topic, VIP-only, service-unavailable, and audio-permission branches
    into runtime-shaped ephemeral notices
  - one decoded `guest_message` update can cross Redis, state stage with no
    chat/user identity writes, guest Shield/dialog, answerGuestQuery payload
    shaping, and ephemeral chain remember
  - one decoded `inline_query` update can cross Redis, state stage,
    answerInlineQuery method capture, and no downstream delegation

Optional env:
  OPENPLOTVA_UPDATE_QUEUE_SMOKE_REDIS_URL    use an existing scratch Redis URL
  OPENPLOTVA_UPDATE_QUEUE_SMOKE_REDIS_PORT   Dragonfly host port, default first free from 56579
  OPENPLOTVA_UPDATE_QUEUE_SMOKE_KEEP         keep disposable Dragonfly when set to 1
  OPENPLOTVA_UPDATE_QUEUE_STREAM_SMOKE       run Dragonfly Streams protocol spike when set to 1
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

export PATH="/opt/homebrew/bin:$PATH"

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"


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

redis_url="${OPENPLOTVA_UPDATE_QUEUE_SMOKE_REDIS_URL:-}"
redis_port=""
project="openplotva-update-queue-smoke-$$"
started_compose=0

compose() {
  OPENPLOTVA_DEV_REDIS_PORT="$redis_port" docker compose -p "$project" "$@"
}

redis_cli() {
  if [[ "$started_compose" -eq 1 ]]; then
    compose exec -T dragonfly redis-cli "$@"
  else
    command -v redis-cli >/dev/null 2>&1 || {
      echo "redis-cli is required when OPENPLOTVA_UPDATE_QUEUE_STREAM_SMOKE=1 uses an external Redis URL" >&2
      exit 1
    }
    redis-cli -u "$redis_url" "$@"
  fi
}

run_stream_smoke() {
  local key="openplotva:stream-smoke:${project}"
  local group="openplotva-smoke"
  local id1
  local id2
  local read_output
  local claim_output

  echo "+ Dragonfly Streams protocol spike smoke"
  redis_cli DEL "$key" >/dev/null
  redis_cli XGROUP CREATE "$key" "$group" 0 MKSTREAM >/dev/null
  id1="$(redis_cli XADD "$key" "*" payload one | tr -d '\r')"
  id2="$(redis_cli XADD "$key" "*" payload two | tr -d '\r')"
  read_output="$(redis_cli XREADGROUP GROUP "$group" consumer-a COUNT 2 BLOCK 100 STREAMS "$key" ">" | tr -d '\r')"
  grep -Fq "$id1" <<<"$read_output"
  grep -Fq "$id2" <<<"$read_output"
  redis_cli XACK "$key" "$group" "$id1" >/dev/null
  redis_cli XPENDING "$key" "$group" >/dev/null
  claim_output="$(redis_cli XAUTOCLAIM "$key" "$group" consumer-b 0 0-0 COUNT 10 | tr -d '\r')"
  grep -Fq "$id2" <<<"$claim_output"
  redis_cli XACK "$key" "$group" "$id2" >/dev/null
  redis_cli DEL "$key" >/dev/null
  echo "stream-smoke-ok"
}

cleanup() {
  if [[ "$started_compose" -eq 1 ]]; then
    if [[ "${OPENPLOTVA_UPDATE_QUEUE_SMOKE_KEEP:-0}" != "1" ]]; then
      compose down -v --remove-orphans >/dev/null 2>&1 || true
    else
      echo "compose project kept: ${project}"
    fi
  fi
}
trap cleanup EXIT

if [[ -z "$redis_url" ]]; then
  if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
    echo "docker compose is required for update queue smoke" >&2
    exit 1
  fi
  if ! command -v nc >/dev/null 2>&1; then
    echo "nc is required for update queue smoke port checks" >&2
    exit 1
  fi

  redis_port="$(free_port_from "${OPENPLOTVA_UPDATE_QUEUE_SMOKE_REDIS_PORT:-56579}")"
  redis_url="redis://127.0.0.1:${redis_port}/0"

  echo "+ docker compose up dragonfly (${project})"
  compose up -d dragonfly >/dev/null
  started_compose=1
  wait_for_tcp "dragonfly" "$redis_port"
else
  echo "+ using existing Redis URL for update queue smoke"
fi

if [[ "${OPENPLOTVA_UPDATE_QUEUE_STREAM_SMOKE:-0}" == "1" ]]; then
  run_stream_smoke
fi

echo "+ Redis update queue FIFO/native-codec smoke"
OPENPLOTVA_TEST_REDIS_URL="$redis_url" \
  cargo test -p openplotva-updates \
  live_redis_queue_round_trips_encoded_updates_when_url_is_set \
  -- --nocapture

echo "+ Redis decoded app replay smoke"
OPENPLOTVA_TEST_REDIS_URL="$redis_url" \
  cargo test -p openplotva-app \
  live_redis_decoded_ \
  -- --nocapture

echo "update-queue-smoke-ok"
