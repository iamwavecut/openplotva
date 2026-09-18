# Prompt eval harness

Replays the worker prompts under `prompts/` against any OpenAI-compatible
`chat/completions` endpoint and scores the outputs with deterministic checks.
Python 3.9+, standard library only.

```bash
python3 tools/prompt-eval/prompt_eval.py --self-test
python3 tools/prompt-eval/prompt_eval.py --list
python3 tools/prompt-eval/prompt_eval.py --render --flow song_director
python3 tools/prompt-eval/prompt_eval.py --endpoint "$EVAL_URL" --model "$EVAL_MODEL" \
  --api-key-env EVAL_API_KEY --flow memory_extraction --runs 3
python3 tools/prompt-eval/prompt_eval.py --endpoint "$EVAL_URL" --model "$EVAL_MODEL" \
  --prompt-dir /path/to/old/prompts --compare prompts
```

`--render` prints the requests that would be sent, without calling anything.
Results go to `tools/prompt-eval/local/runs/<timestamp>/` (git-ignored): one JSONL
line per call plus `summary.md` with pass rates, latency and token usage per flow.

## Fixtures

`fixtures/<flow>/*.json` are synthetic and committed; `local/` holds private
fixtures (for example exported production requests) and is never committed.

| key | meaning |
|---|---|
| `flow` | worker flow name, used for grouping and `--flow` |
| `prompt` | prompt template name without `.prompt`, rendered as the system turn (or split by `{{role}}` markers) |
| `user_prompt` | optional second template rendered as the user text |
| `vars` | template variables (`{{name}}` is HTML-escaped like Handlebars, `{{{name}}}` is raw) |
| `user` | user turn: a string, or JSON that is pretty-printed like the bot does |
| `image` | optional path (relative to this directory) attached before the text; the fixture is skipped if missing |
| `schema` | name of `schemas/<name>.json`; sent as `response_format` (`--mode response_format`), as a forced tool (`--mode tools`), or not at all (`--mode prompt_only`) |
| `tool_name` | schema/tool name for the two structured modes |
| `request` | extra request fields (`temperature`, `max_tokens`, ...); `--set key=value` overrides |
| `checks` | list of checks below |

## Checks

| check | passes when |
|---|---|
| `json` | the output parses as JSON after fence stripping or brace salvage |
| `strict_json` | the output is bare JSON |
| `schema` | the parsed output satisfies the fixture schema (type, required, properties, items, enum, min/maxItems, additionalProperties) |
| `lang:<ru\|uk\|be\|en>[:<path>]` | ≥ 90 % of letters at the path (or the whole output) are in the language's script |
| `no_phrases:<a;b>[@<path>]` | none of the phrases occur (case-insensitive), in the raw output or only at the path |
| `no_substring:<s>[@<path>]` | the literal substring is absent, from the raw output or only at the path |
| `max_items:<path>:<n>` / `min_items:<path>:<n>` | the array at the path has at most / at least n items |
| `ids_from_input` | every integer under a `*_id` / `*_ids` key occurs in the user payload (0 allowed) |
| `partition_ids` | subject merge: every input card id appears exactly once across clusters, `demote_ids`, `keep_ids` |
| `label:<prefix>` | the first non-empty line starts with the prefix |
| `equals:<path>=<value>` | every value at the path equals the given value |
| `contains:<s>` | the raw output contains the substring |
| `word_range:<path>:<min>:<max>` | every string at the path has a word count in range |
| `html_tags:<a,b>` | only the listed HTML tags appear |
| `max_blank_run:<n>` | no run of more than n blank lines in the raw output |
| `no_repeat_lines:<n>` | no line longer than 8 characters repeats more than n times |

Paths are dotted keys with `[]` to expand arrays, e.g. `outputs[]`,
`candidate_cards[].fact_text`, `summary_json.recap`.

## Exporting real requests (local only)

`export_prod_requests.sh <flow> <n>` turns stored worker requests into local
fixtures under `local/<flow>/`. It needs two environment variables that are
deliberately not written anywhere in the repository: `PROMPT_EVAL_SSH_TARGET`
(the host that can reach the database) and `PROMPT_EVAL_PSQL` (the command that
opens `psql` against the bot database there). Exported fixtures contain real chat
data: keep them in `local/`.
