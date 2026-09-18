#!/usr/bin/env bash
set -euo pipefail

flow=${1:?usage: export_prod_requests.sh <flow> <count>}
count=${2:-10}
: "${PROMPT_EVAL_SSH_TARGET:?set PROMPT_EVAL_SSH_TARGET}"
: "${PROMPT_EVAL_PSQL:?set PROMPT_EVAL_PSQL}"
case "$flow" in *[!a-z_]*) echo "invalid flow: $flow" >&2; exit 2 ;; esac
case "$count" in *[!0-9]*) echo "invalid count: $count" >&2; exit 2 ;; esac

here=$(cd "$(dirname "$0")" && pwd)
out="$here/local/$flow"
mkdir -p "$out"
query="select id, raw_request from llm_request_events where flow = '$flow' and raw_request is not null and coalesce(is_rollup, false) = false order by id desc limit $count"
ssh -o BatchMode=yes "$PROMPT_EVAL_SSH_TARGET" "$PROMPT_EVAL_PSQL -At -F '	' -c \"$query\"" |
  python3 -c '
import json, sys
flow, out = sys.argv[1], sys.argv[2]
for line in sys.stdin:
    event_id, _, raw = line.rstrip("\n").partition("\t")
    request = json.loads(raw)
    messages = request.get("messages", [])
    prompts = {
        "memory_extraction": "memory/extraction",
        "memory_subject_merge": "memory/subject_merge",
        "history_summary": "history/summary",
        "optimize_prompt": "image/optimizer",
        "optimize_edit_prompt": "image/edit_optimizer",
        "song_director": "music/song_director",
        "youtube_summary": "youtube/summary_system",
    }
    fixture = {
        "flow": flow,
        "id": f"{flow}/prod-{event_id}",
        "prompt": prompts.get(flow, ""),
        "vars": {"variant_count": 1, "maxDuration": 360},
        "schema": flow if flow in prompts and flow != "youtube_summary" else None,
        "messages": messages,
        "request": {k: v for k, v in request.items() if k in ("temperature", "max_tokens", "top_p", "top_k")},
        "checks": ["json"],
    }
    with open(f"{out}/prod-{event_id}.json", "w", encoding="utf-8") as handle:
        json.dump(fixture, handle, ensure_ascii=False, indent=2)
    print(f"{out}/prod-{event_id}.json")
' "$flow" "$out"
