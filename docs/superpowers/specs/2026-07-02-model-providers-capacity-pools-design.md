# Model Providers: Capacity Pools, Typed Providers, Routing Ops (2026-07-02)

## Problem

The declarative routing system (migrations 121–131) unified workflow routing
but left three structural gaps:

1. **Parallelism was not first-class.** The global dialog worker count (2)
   capped everything regardless of the selected model, so vram.cloud's 16
   parallel request slots were unreachable, while the Boogu draw models —
   which share one GPU and must render strictly one at a time — were
   "parallelized" through duplicated slot workflows (migration 143).
2. **Providers had no typed transport.** `kind` is a capability class, not a
   wire protocol; `provider.config`/`model.config` JSONB were reserved but
   unused; and the dialog runtime resolved transport clients from a static
   env-built map keyed by provider name, silently sending any admin-created
   provider's traffic to the default aifarm client.
3. **The admin section was junky**: no loading/error states, race-prone
   reorder (two sequential POSTs), an orphan-assignment trigger flow, and no
   visibility into what actually serves traffic.

## Decisions

### Capacity pools (migration 146)

`llm_capacity_pools(id, name, max_concurrency NULL=unlimited, description)`;
`provider_models.pool_id` (`ON DELETE SET NULL` — deleting a pool degrades
its models to unpooled, never breaks routing). A pool is a shared concurrency
budget over one physical resource; several models may share one pool (the
Boogu case: flux + turbo + edit on one GPU = one 1-slot pool). A model is
selectable while its pool has a free slot.

Runtime: `router::capacity::PoolRegistry` — counter + `Notify` cells,
deliberately not semaphores, so a live resize preserves in-flight accounting.
The RAII `PoolPermit` frees its slot on drop (cancellation-safe) and keeps
its cell alive even if a reload removed the pool. The registry lives beside
`BreakerSet`/`TriggerState` and survives `ArcSwap` table reloads;
`apply(pool_specs)` reconciles it after every reload.

Walker semantics (`RoutedAttemptWalker`): a full pool is a **busy skip** —
no breaker mutation (busy ≠ dead), no hop consumed (`select_chain` is
untruncated; hops count started attempts). A fully-busy chain waits for any
released slot, bounded by the request deadline and a 300 s sanity cap; slot
waiting is excluded from `retry_wall_ms`, which budgets execution only
(pool queueing replaces the server-side queueing some backends did before).
A wait timeout renders as `CapacityWaitTimeout` whose message contains the
phrase "capacity unavailable" on purpose: `retry.rs` message rules classify
it retryable, so every call site requeues the job (pinned by test).

Seeded pools (guarded backfill `llm.routing.capacity_pools_v1`):
`aifarm-dialog` = dialog worker count (2), `vram-cloud` = 16,
`boogu-gpu` = 1 over all `aifarm-draw` models. Attach only
`WHERE pool_id IS NULL`, so operator re-assignments survive reboots.

### Derived dialog workers

The dialog worker count derives from the routing table: the summed budgets of
the dialog workflow's distinct pools plus `unpooled_share` (default 2) per
unpooled/unlimited model, clamped by `PERSISTENT_QUEUE_DIALOG_WORKERS_CAP`
(default 24). A supervisor (`dialog_workers.rs`) watches the derived count:
scale-ups spawn, scale-downs retire via a oneshot the worker's stop future
selects on (the worker finishes its current job first), crashed workers
respawn on a reap tick, and a runtime stop joins every owned worker.
`PERSISTENT_QUEUE_DIALOG_AIFARM_WORKERS` remains only the fallback when the
table has no dialog route. Admin routing edits reload through
`RouterRuntime`, which republishes the table, reconciles the pool registry,
and pushes the re-derived scale — pool edits resize workers with no restart.

### Typed providers (migration 147)

`llm_providers.protocol` (nullable): the wire payload shape —
`openai_compat | genkit | acestep | discovery_jobs | discovery_draw |
privacy_filter`. Discovery resolution stays orthogonal (presence of
`discovery_service_name`). `runtime_hint`
(`llama_cpp | vllm | sglang | ollama | tgi`) only keys the parameter schemas
the admin UI offers. A pure classifier backfills NULLs
(`llm.routing.provider_protocol_v1`) and never overwrites operator values.
Typed config views live in `openplotva_llm::provider_schema`
(`#[serde(default)]` + flatten-tolerant, no version column); the admin API
validates provider/model configs against them and serves
`param_descriptor(protocol, hint)` form descriptors in the snapshot.

### Dynamic chat-client factory

`ChatClientFactory` (dialog_runtime.rs) resolves the transport client per
routed attempt: env-built clients (aifarm, genkit/gemini, vram-cloud, GPU
reasoner) win by name and keep their toolbox wiring; any other provider is
built from its row — `openai_compat` via direct endpoint or discovery, key
from the env ref or the AES-GCM blob opened under `MASTER_KEY` — and cached
under a row fingerprint. Protocols without a dynamic dialog adapter fail the
attempt with a retryable `ProviderUnavailable`, so the walker moves to the
next candidate. Result: a provider created in the admin panel is routable
immediately, with the correct endpoint and key.

### Admin API and UI («Routing Ops»)

New actions: pool CRUD, `set_model_pool`, transactional
`set_primary_weights`/`set_fallback_order` (one revision bump + one reload
per draft save), atomic `create_trigger_with_assignment`,
`fetch_provider_models` (server-side `/v1/models` with a new/existing/gone
diff; manual add-by-name stays available for every protocol) and
`import_models`. New endpoints: `GET /admin/api/routing/status` (pool
occupancy, breaker states, trigger engagement, capacity cooldowns, worker
gauge, event ring — no DB hit, poll-safe) and
`GET /admin/api/routing/events` (keyset-paginated `llm_routing_events`
journal). The web/admin section is rebuilt around five views — Cockpit
(live health + events feed), Board (draft-then-save weights, fallback
reorder, trigger editor, mini cascade), Catalog (typed provider/model forms,
fetch-models diff import), Pools (visual concurrency editing with live
occupancy), Graph (workflow→model→provider→pool topology) — on pl-* design
system components (`pl-slotbar`, `pl-flow`, `pl-diff-list` added).

## Deliberate scope

- Only the chat kind gets dynamic client construction; acestep/embedding/
  draw/privacy transports stay specialized per workflow.
- The Boogu slot workflows (migration 143) remain as the "N images per user
  action" fan-out; correct serialization now comes from the shared pool.
- No background model-list sync; fetch is on-demand from the admin panel.
- Non-dialog queue workers keep env-driven counts; only dialog derives from
  pools (its walker still enforces pool limits for every workflow).

## Accepted behavior shifts (deploy notes)

- Dialog workers jump 2 → ~18–22 at first boot after deploy (the point of
  the feature); `PERSISTENT_QUEUE_DIALOG_WORKERS_CAP` is the rollback lever.
- Admin virtual dialogs now contend for the `aifarm-dialog` pool with
  production dialog turns — the pool reflects the service's real capacity.
- Before deploying, dry-run `SELECT name FROM llm_providers` against prod to
  confirm the pool backfill's provider-name classification matches the rows.
