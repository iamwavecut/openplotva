# Admin Analytics → "External Requests" Dashboard

Repo: `openplotva`, worktree `.claude/worktrees/heuristic-goldstine-c78822`, branch
`feat/analytics-dashboard-redesign`. All column/table facts verified against prod
(Runtime API `sqlRead`) on 2026-07-04; re-verify before relying.

## Context

The admin `Analytics` tab grew a pile of low-signal charts (top-chats bars, a
tool-call-parser table, a generic requests line) while the data behind external
calls got much richer (run correlation, prefill/decode timings, TPS, token
breakdown, a generic `telemetry_rollups` table, shield checks, routing events).
Owner brief: replace the page with a dashboard that **registers and visualizes
every request to external systems**, with a first-class group for **LLMs and
providers — performance and latency** (TTFT, TPS, tokens in/out, request-length
aggregates). Draw pattern wisdom from the sibling `mobiways/api-server` admin
dashboard (snapshot-driven sections, health strip, chart factory, dual-axis),
but build our own front end on the `pl-*` token design system.

Diagnostic that motivated this (for the record): the "charts miss the qwen
memory model" report was **not** a broken query and **not** stalled memory —
memory consolidation runs in a daily window and was actively producing qwen
extraction rows; the backend model-series returned full 24h qwen buckets. The
current page simply under-renders. The redesign supersedes the one-chart fix.

## Goals

- One page that inventories **all outbound external calls**: LLM providers,
  shield/moderation, generation/job backends, routing health, reply outcomes.
- LLM/provider section is the centerpiece: latency split into **prefill (TTFT) /
  decode / end-to-end** with p50/p95, **throughput** (server vs effective TPS),
  **token economy** (in/out/total/cached/thoughts), **request shape**
  (chars/messages/docs), errors — sliced by provider, model, and flow.
- Cut low-signal panels. Every remaining chart answers an operational question.
- Token-driven Chart.js (colors resolved from `tokens.css` at runtime, never
  hardcoded), reusable chart factory, loading/empty/error states, pl-* only.

## Non-goals

- No change to GraphQL `llmRequests`/`dialogTurnOutcomes` operator contracts.
- No new persistence; read-only aggregation over existing tables/rollups.
- Settings WebApp and other tabs untouched.

## Data sources (verified)

| System | Table | Key columns |
|---|---|---|
| LLM providers | `llm_request_events` (330k) | provider, model, flow, source, request_kind; `duration_ms`, `prompt_eval_ms` (**TTFT**), `generation_ms`; `prompt_tps`, `generation_tps`, `effective_output_tps`, `effective_total_tps`; `input_tokens`, `output_tokens`, `total_tokens`, `cached_tokens`, `thoughts_tokens`, `tool_use_prompt_tokens`; `prompt_chars`, `prompt_messages`, `docs_chars`; `error`; inference params; `run_id`/`run_seq`; rollup mirror (`is_rollup`, `*_sum`, `p50/p95_*`, `request_count`, `error_count`, `bucket_*`) |
| Shield / moderation | `whitecircle_checks` (223k) | source, flow, mode, `duration_ms`, `flagged`, `error`, deployment_id |
| Generation / job backends | `taskman_jobs` (live: 56k completed/24h, all with started_at/completed_at) — compute wait=`started_at-created_at`, processing=`completed_at-started_at`, per job_type×status + p95. NOT `telemetry_rollups` (its taskman/job buckets are stale since 2026-07-02 13:00 — separate bug, spawned as its own task). |
| Routing health | `llm_routing_events` (1.9k) | severity, event_type, workflow_key, provider_id, model_id, queue_name, summary |
| Reply outcomes | `dialog_turn_outcomes` (10k) | outcome, reason, provider, model, `elapsed_ms`, `budget_ms`, user_signal |

Notes: `llm_request_events` already carries hourly/daily rollup rows alongside raw
rows — every aggregate SQL must branch `CASE WHEN is_rollup THEN <sum/p95> ELSE
<raw> END` and filter `(NOT is_rollup OR rollup_granularity='hour')`, matching the
existing reader. `prompt_eval_ms` is the TTFT proxy for the llama.cpp/vLLM-style
servers (time to finish prefill = time to first token). `telemetry_rollups`
already computes p95 wait/processing per job queue — reuse, do not recompute.

## Page IA (new `Analytics`)

Topbar: title, range `pl-select` (1h/6h/24h/3d/7d/14d/30d, default 24h), manual
Refresh, "updated HH:MM" note. All sections render from one snapshot; a range
change refetches.

**Health strip** — KPI chips (metric-card), computed over the window, with
warn/danger tint on thresholds:
external calls · error % · p95 e2e latency · median TTFT · median gen TPS ·
tokens in→out · cache-hit % · open routing incidents · shield flag %.

**Section 1 — LLMs & Providers** (centerpiece):
1. Throughput over time — stacked bar of requests by provider + dual-axis error-%
   line.
2. Latency split — grouped bars per provider: TTFT p50/p95, decode p50/p95, e2e
   p50/p95 (or a small-multiple per lane).
3. TPS — server `generation_tps` vs `effective_output_tps` per model (bars).
4. Token economy over time — stacked area: input / output / cached / thoughts;
   plus a per-model totals bar.
5. Request shape — aggregates of `prompt_chars` / `prompt_messages` / `docs_chars`
   (avg + p95) per flow.
6. Model leaderboard table — model · provider · reqs · err% · TTFT p50/p95 · gen
   TPS · tokens in/out · cache% · avg iters.
7. Provider table — provider · reqs · err% · p95 e2e · TPS · request share.
8. Flow mix — where calls originate (dialog / memory_extraction / agentic_image /
   optimize_* / history_summary / vision / shield) as share + error rate.

**Section 2 — Other external systems:**
9. Shield/moderation — calls over time, flag rate, `duration_ms` p95 (whitecircle).
10. Job pipeline — per job_type wait p95 vs processing p95 + success/fail
    (telemetry_rollups); dialog/image/music/memory/control.
11. Routing incidents — recent `llm_routing_events` (severity-colored) table +
    counts by event_type.

**Cut:** top-chats chart, tool-call-parser table (belongs in LLM Dialogs if
anywhere), the bare generic requests line (folded into #1), inference-params table
(demoted to an optional detail).

## Data contract (snapshot JSON)

`GET /admin/api/analytics/overview?range=` → `admin_json_no_cache_response`:

```
{ range, bucket, since, generated_at,
  health: { external_calls, error_pct, p95_e2e_ms, ttft_p50_ms, gen_tps_p50,
            tokens_in, tokens_out, cache_hit_pct, routing_incidents_open,
            shield_flag_pct },
  llm: {
    series: [{ ts, provider, request_count, error_count, tokens_out }],
    latency: [{ key(provider|model), ttft_p50, ttft_p95, decode_p50, decode_p95,
                e2e_p50, e2e_p95, request_count }],
    tps:     [{ model, gen_tps, effective_tps, request_count }],
    tokens_series: [{ ts, input, output, cached, thoughts }],
    shape:   [{ flow, avg_prompt_chars, p95_prompt_chars, avg_prompt_messages,
                avg_docs_chars, request_count }],
    models:  [{ model, provider, request_count, error_count, ttft_p50, ttft_p95,
                gen_tps, tokens_in, tokens_out, cache_hit_pct, avg_iterations }],
    providers:[{ provider, request_count, error_count, p95_e2e_ms, gen_tps,
                request_share }],
    flows:   [{ flow, request_count, error_count }] },
  shield: { series:[{ ts, checks, flagged, p95_duration_ms }], flag_pct, total },
  jobs:   [{ job_type, queue_name, job_count, completed, failed, cancelled,
             p95_wait_ms, p95_processing_ms }],
  routing:{ recent:[{ at, severity, event_type, provider_id, model_id, summary }],
            by_type:[{ event_type, n }] } }
```

Frontend filters/derives client-side where cheap; server does the heavy percentile
aggregation. Old `/analytics/llm/summary` is retired with the old page.

## Backend design

- Extend `runtime_llm_analytics.rs` (or add `runtime_analytics_overview.rs`
  beside it) with the new aggregate queries; keep the rollup-aware CASE pattern.
  New percentile lanes (TTFT/decode/e2e) come from `prompt_eval_ms`,
  `generation_ms`, `duration_ms` (raw) unioned with the rollup `p50/p95_*` columns
  where present, else `percentile_cont` over raw.
- Shield/jobs/routing read their own tables/rollups (small, no rollup mirror).
- New trait method `RuntimeAnalyticsOverviewReader` + admin REST handler
  `admin_analytics_overview` at `/admin/api/analytics/overview`, added to
  `GO_ADMIN_API_ROUTE_PATTERNS` + the parity test. Optional GraphQL later.
- Reuse `sql_timeout`, `admin_json_no_cache_response`, `require_admin_request`.

## Frontend design

- New `analytics.js`-style block in `index.html` (admin is single-file): a chart
  factory `plCharts` — `buildChart(canvasId, cfg)`, `line/barDataset(...)`,
  `chartOptions({...})`, `destroyAll()`, and `palette()` that reads
  `--p-*` values from `getComputedStyle(document.documentElement)` so colors stay
  in `tokens.css` (no hex/rgb in JS — satisfies the design-system guard).
- Section renderers each take a sub-object of the snapshot (mobiways pattern),
  with skeleton → empty → error via `PL.skeleton/empty/error`; Refresh + Clear-less
  (read-only). Dual-axis combos for volume+rate.
- New `.an-*` classes in `admin.css` referencing tokens; new categorical/lane
  tokens in `tokens.css` if needed (prefill/decode/e2e, in/out/cached/thoughts).
- Chart.js already loaded (`chart.umd`); render into `<canvas>` (allowed; not a
  raw control). Update the three asset `sha256` in `openplotva-web/src/lib.rs`.

## Verification

- Backend: unit tests for each aggregate SQL (columns, rollup CASE, since filter,
  empty-safe), overview JSON shape, route parity, 403/method guards.
- `cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings`;
  `cargo test --workspace`; `cargo test -p openplotva-web` (guard + hashes).
- Frontend: local no-services run with a prod-shaped mocked `/overview`; verify
  every section renders (health chips, all charts with data, tables), states,
  range change, token-derived colors, light theme, mobile width, keyboard.
- Rewrite the web-ui smoke's analytics assertions for the new page.
- `openplotva-design-system-review` skill before merge.
- Prod after deploy: `/overview` returns populated sections; charts show qwen
  memory extraction in its window and all providers; compare p95s to `sqlRead`.

## Rollout

Branch `feat/analytics-dashboard-redesign`; no AI attribution; CI + design-review;
merge; deploy `deploy-production.yml --ref main`; verify live via Runtime API +
the admin tab.
