# Backend Lanes Design

## Goal

Load every remote inference backend at its physical width and no further. A request waits in
its own backend's queue instead of jumping to another backend because the first one is busy.
A fallback model serves a request only when the primary fails or when waiting would exhaust the
request's budget.

## Current State

- Capacity pools (`llm_capacity_pools`, `provider_models.pool_id`) are counters. `PoolRegistry`
  keeps `in_flight` per pool and one registry-wide `Notify`. A release in any pool wakes every
  waiter of every pool, and waiters race to re-acquire. There is no queue order and no priority.
- The walker (`RoutedAttemptWalker::run`) skips a candidate whose pool is full. A primary-role
  candidate may first wait `primary_slot_wait` (dialog 12 s, media optimizer 15 s, YouTube and
  history summary 60 s); then the walker takes the next candidate with a free slot. When a whole
  pass is busy it waits for any release up to `max_slot_wait` (dialog 1 s, default 300 s).
- Unpooled models (GPT Luna) are always free, so a busy Gemma on GPU0 spills to Qwen on GPU2 and
  then to Luna. In the 24 h before 2026-10-12 13:00 UTC the dialog recorded 3,365 primary slot
  waits; 1,525 dialog runs started on GPU2 and Luna served 3,255 dialog requests.
- Dialog workers are derived as the sum of the pool widths of every model in the dialog chain
  plus `PERSISTENT_QUEUE_DIALOG_UNPOOLED_SHARE` per unpooled model, capped by
  `PERSISTENT_QUEUE_DIALOG_WORKERS_CAP`. Production computes Gemma 8 + Qwen 8 + Luna 10 = 26 and
  runs 24 workers against 8 Gemma lanes, treating fallback capacity as extra throughput.
- Memory workers are `max(configured, min(memory pool width, cap 4))`.
- Pools attach to model rows, not to servers. A model row that reaches a server without a pool
  bypasses its width (the vision fallback did until migration 198).
- Server widths are configured twice: in the bot's pools and in each service's Discovery
  registration (`max_concurrent_jobs`). They drift apart without any signal.
- Walkers are constructed in 25 places, each with its own wait settings.

Measured turn shape (2026-10-12): a dialog turn's median is about 9 s, of which one model
round is about 3–4 s; materialization, tools and Telegram sends take the rest. With 24 workers
GPU0 averaged about 2 busy lanes of 8 and peaked at 8.

## Design

### Backends and lanes

A *lane pool* is the width of one physical resource: the number of requests it can run at once.
Usually that is one server; several services that must not run concurrently share one pool. The
existing `llm_capacity_pools` rows become lane pools; their meaning changes from "budget shared
by some models" to "the resource's width".

| Lane pool | Backend | Width |
| --- | --- | ---: |
| `aifarm-gpu0-dialog` (was `aifarm-vllm-gpu0`) | dialog model on GPU0 (dialog, vision, agents, media optimizer) | 8 |
| `aifarm-gpu2-utility` (was `aifarm-gpu2-qwen27b`) | utility model on GPU2 (memory, history summary, YouTube fallbacks, dialog fallback, vision fallback) | 8 |
| `aifarm-embedder` (new) | embedder (CPU) | 8 |
| `aifarm-gpu1-media` (was `aifarm-draw-gpu1`) | draw, music and speech recognition on GPU1, one at a time | 1 |
| `aifarm-gpu1-privacy` (new) | privacy filter on GPU1, beside the media lane | 4 |
| external APIs | OpenRouter and other hosted APIs | unpooled |

Pool names follow `aifarm-<device>-<purpose>`, so they stay true when a model or a serving
engine changes. The unused `vram-cloud` and `boogu-gpu` pools are left as they are.

GPU1 runs under the VRAM arbiter, which loads and offloads models on demand. Draw, music and
speech recognition share the `aifarm-gpu1-media` lane: while one of them runs, the others wait, so the
arbiter loads one heavy model set at a time instead of swapping between concurrent jobs. The
privacy filter keeps its own pool and runs beside that lane; the arbiter already keeps it
resident or moves it to the CPU.

Invariants, checked whenever the routing table loads:

1. Every enabled model whose provider targets a Discovery service belongs to a lane pool.
2. All enabled models that target the same Discovery service belong to the same lane pool. A
   pool may span several services when they share one sequential resource (GPU1's draw, music
   and speech recognition).
3. A lane pool's width does not exceed the `max_concurrent_jobs` of any Discovery service it
   spans, and equals it when the pool spans one service.

A violation emits a `lane_config_mismatch` routing event and a readiness warning; routing keeps
working with the bot's configured width. Invariant 3 is checked against Discovery's
`GET /v1/services/{name}/capacity`, so the farm registration stays the server-side truth and
drift becomes visible.

### Lane queue

`PoolRegistry` keeps one queue per lane pool. `acquire(pool, priority, deadline)` either takes a
free lane or enqueues a waiter. A released lane is handed directly to the head waiter: highest
priority first, then arrival order. There is no wake-all and no race. A waiter that gives up
(deadline, cancellation) leaves the queue; a handoff to a dropped waiter passes to the next one.
Resizing keeps in-flight accounting: a grown pool serves waiters at once, and a shrunk pool lets
in-flight work finish before new admissions. `try_acquire` stays for non-blocking checks.

Priorities, stored per workflow in a new `workflows.lane_priority` column:

| Priority | Workflows |
| --- | --- |
| 0 interactive | `dialog`, `agentic_*`, `vision`, `voice_transcription`, `redaction` and `embedding` on the dialog path |
| 1 user tool | `history_summary`, `youtube_summary`, `media_prompt_optimizer`, image and music workflows |
| 2 background | `memory_extraction` (including its resolution calls), `memory_subject_merge`, memory-write `embedding` and `redaction` |

Embedding and redaction serve both the dialog path and memory writes. The caller passes its
priority in `RoutedRequestContext`; the workflow column is the default.

On the GPU1 media lane, a voice message's transcription goes ahead of queued draws and songs, but a
running draw or song is never interrupted: the transcription waits for it to finish.

Each pool also keeps an exponentially weighted mean of lane hold time, used for the forecast
below and exposed for observability.

### Walker policy: fallback on failure or budget

For each candidate in chain order, the walker:

1. skips it if its circuit breaker is open (failure);
2. computes the forecast wait: `ceil((waiters ahead + 1) / width) × mean hold time`;
3. if the request has a deadline and the forecast exceeds the admission budget, moves to the
   next candidate without waiting (budget);
4. otherwise waits in the candidate's queue until a lane is handed over or the admission budget
   ends, then moves to the next candidate.

The admission budget is the time left until the request deadline minus a fallback reserve: the
time the next candidate needs to answer. The reserve is a per-workflow value
(`workflows.fallback_reserve_ms`): 30 s for interactive and user-tool workflows, 0 for
background ones. A request without a deadline (background work) waits in its queue without a
limit and never falls back because of load.

`primary_slot_wait`, the busy-skip pass and the per-construction `max_slot_wait` disappear; the
policy is the same for all 25 walker constructions. Unpooled candidates are admitted at once.

### Workers

Workers run whole requests and are no longer derived from fallback capacity.

- Dialog workers = width of the dialog primary's lane pool × `DIALOG_WORKERS_PER_LANE`
  (default 2), so 16 in production. Workers beyond the busy lanes wait in Gemma's queue during
  their model rounds and run their non-model phases meanwhile. `PERSISTENT_QUEUE_DIALOG_UNPOOLED_SHARE`
  and the fallback terms of `derived_worker_count` are removed.
- Memory workers = width of the memory primary's lane pool × `MEMORY_WORKERS_PER_LANE`
  (default 2), so 16. Memory runs spend about 70% of their time outside the model; extra workers
  keep GPU2's lanes busy at background priority, behind dialog fallbacks and user tools.
- Both resize when the routing table reloads.

### Observability

Per lane pool: width, in-flight, queue depth per priority, mean hold time, and recent wait
percentiles, in the admin routing status and the runtime GraphQL API. Routing events:
`lane_wait` (waited, position, priority) replaces `primary_slot_wait` and `capacity_pool_wait`;
`lane_bypass` records a budget skip with the forecast; `lane_config_mismatch` records invariant
violations.

### Hourly memory runs

Memory runs already cover one chat hour each, but they were enqueued once a day for the previous
UTC day, which put a whole day of memory work into a morning burst. The scheduler now ticks at
`HH:02` UTC and enqueues every chat hour that completed since the last tick. An hourly tick scans
the last two completed hours. The startup tick and the `03:02` tick scan the whole retention
window, for hours that a longer outage or the daily cap left without a run. The daily cap still
counts runs created since midnight UTC, and the enqueue rollups still describe whole UTC days.
`MEMORY_DAILY_SCHEDULE` and `MEMORY_DAILY_WINDOW_HOURS` are removed.

## Migrations

1. Rename the pools to `aifarm-gpu0-dialog`, `aifarm-gpu2-utility` and `aifarm-gpu1-media`,
   attach the speech recognition model to `aifarm-gpu1-media`, and add `aifarm-embedder` (8) and
   `aifarm-gpu1-privacy` (4) with their model rows. Pool ids do not change, so model rows keep
   their `pool_id`; the code constant that names the GPU2 pool (`MEMORY_GPU2_POOL`) follows the
   rename. Previous names are recorded for the down migration.
2. `workflows.lane_priority SMALLINT NOT NULL DEFAULT 0` and
   `workflows.fallback_reserve_ms INTEGER NOT NULL DEFAULT 30000`, set to the values above.
   The down migration drops both columns and the new pools.

## Error Handling

- A lane wait that ends at the deadline returns the existing capacity-timeout error, so turn
  outcome classification and user signals do not change.
- A dropped waiter or a cancelled request never holds or leaks a lane; permits stay RAII.
- If Discovery is unreachable during the width check, the check is skipped and retried on the
  next reload; routing is unaffected.

## Testing

- Lane queue unit tests: FIFO within a priority, priority order, handoff on release, waiter
  cancellation, deadline expiry, grow and shrink under load, no leaked permits.
- Walker tests: a busy primary is waited for instead of skipped; an open breaker falls back
  at once; a forecast beyond the budget falls back without waiting; a request without a deadline
  waits; unpooled candidates are immediate.
- Worker sizing tests for dialog and memory.
- Invariant tests for the routing table loader, including the vision fallback case and a pool
  that spans several services.
- A GPU1 test: a transcription queued behind a running draw starts only after the draw releases
  the lane, and ahead of a second queued draw.
- Production verification after deploy: dialog requests served by GPU2 and Luna drop to
  failures and budget skips only; GPU0 lane occupancy and queue waits are visible; memory runs at
  background priority without delaying dialog fallbacks.

## Out of Scope

- The GPU1 arbiter's internal scheduling.
- Discovery server changes beyond the read-only width check.
- Startup catch-up and restart cost.
