# AI Farm Embedder — Design Spec

Date: 2026-06-16

## Goal

Move OpenPlotva's text-embedding workload off the CPU service that ships inside
the bot deployment (`tools/embedder`, `jinaai/jina-embeddings-v5-text-nano`,
device `cpu`) onto a new GPU service on the AI Farm server, where free VRAM sits
next to the Qwen model. Register the new service in AI Farm Discovery, consume it
from OpenPlotva through Discovery (same path as the LLM), then delete the old CPU
embedder and all its remnants once the pipeline is verified.

Same model, same dimension (512), same `/encode` contract → **no re-index** of
existing pgvector data.

## Context (verified)

Two hosts:

- **geta.moe** runs OpenPlotva (bot + Postgres + Dragonfly + the current CPU
  embedder built from `./tools/embedder`, reached at `http://embedder:12500`).
- **aifarm** runs Discovery (`:50051`) plus the GPU services: `llm-openai`
  (vLLM Gemma-4-26B, GPU 0), `llamacpp-qwen35b` (the **Qwen** model, GPU 2),
  `draw-api`, `privacy-filter`.

OpenPlotva already reaches aifarm **only through Discovery** (`DISCOVERY_BASE_URL`)
for LLM traffic. Discovery is a job-queue dispatcher, not a URL registry:
services register (`POST /v1/services/register`), clients submit jobs
(`POST /v1/jobs`, body base64-encoded) and poll `GET /v1/jobs/{id}`. A discovery
client already exists in `crates/openplotva-llm/src/aifarm.rs`
(`complete_json_discovery_with_job_id`, `poll_discovery_result`).

GPU inventory on aifarm (measured):

| GPU | Card | Total | Free | Tenant |
|-----|------|-------|------|--------|
| 0 | RTX 3090 | 24 GB | ~1 GB | `llm-openai` (vLLM Gemma) |
| 1 | RTX 4060 Ti | 16 GB | ~5 GB | privacy-filter / draw |
| 2 | RTX 3090 | 24 GB | ~7 GB | `llamacpp-qwen35b` (Qwen) — UUID `GPU-9b451689-3127-ac48-ef9a-91a5a52231e6` |

Target GPU for the embedder: **GPU 2** (next to Qwen, ~7 GB free). A nano
embedding model needs <1 GB, so it fits with wide headroom; the experiment
confirms the exact footprint.

### Embedder contract (preserved verbatim)

`POST /encode` request `{prompts: [str], dimension?: int, task_description?: str}`
→ response `{embeddings: [[float]], dimension: int, count: int}`. Plus
`GET /health`. Output vectors are L2-normalized and cropped/padded to `dimension`.
Rust types `EmbedderEncodeRequest` / `EmbedderEncodeResponse`
(`crates/openplotva-app/src/memory_runtime.rs`) stay byte-compatible.

### Consumers (verified, `crates/openplotva-app`)

`EmbeddingProvider` trait (`memory_runtime.rs:74`) exposes only `embed_one` →
`Result<Option<PgEmbeddingVector>, EmbedderClientError>`.

- **Retrieval / query-time** (memory query `dialog_jobs.rs:1517`, shield query
  `dialog_jobs.rs:1600`, guest shield `guest.rs:378`, admin shield embed
  `lib.rs:4536/4557`): already degrade to lexical-only on any error (warn →
  `None`).
- **Consolidation / write-time** (`embed_memory_cards` `memory_runtime.rs:2142`,
  `insert_memory_episode` `memory_runtime.rs:2097`): episode embed is optional,
  but `embed_memory_cards` **propagates** the error and fails the run.

Consolidation queue: `memory_runs` table (status `queued|processing|completed|
failed|skipped`, `lease_owner`, `leased_until`, `attempts` ≤ 5, exponential
backoff). Scheduler + worker pool
(`run_memory_consolidation_taskman_scheduler_with_trigger_until`,
`run_memory_consolidation_taskman_worker_until`). A run is claimed
(`claim_run`, `attempts += 1`), processed by `process_next_memory_run` →
`process_claimed_memory_run`, then `complete_run` or `fail_run` (backoff, drops
permanently after 5 attempts).

## Decisions (locked with the user)

- **Model**: keep `jinaai/jina-embeddings-v5-text-nano` @ dim 512. No re-index.
- **Runtime**: CUDA only, never CPU. Pin to GPU 2 (Qwen's card) via
  `AIFARM_GPU_DEVICE` UUID.
- **Integration**: through Discovery (job submit/poll), reusing the LLM client
  machinery. Service name `embedder`, endpoint `encode` (`POST /encode`).
- **Concurrency**: the service handles concurrent requests (bounded GPU
  semaphore) and batched prompts; Discovery `max_concurrent_jobs = 8`. OpenPlotva
  gets a batch path (`embed_batch`) used by consolidation.
- **Circuit breaker**: process-shared, cooling. 5 consecutive failures → open for
  5 minutes; while open, embed calls short-circuit (no AI Farm traffic). After
  cooldown a probe is allowed; success closes, failure re-opens.
- **Fallback — retrieval & shield**: lexical-only (existing behavior). Shield
  keeps its lexical safety path; the vector path is skipped. This is a
  user-approved, intentional degradation of an embedding-dependent security
  feature; the lexical safety net stays on.
- **Fallback — consolidation**: health-gated. Start a run only when the embedder
  is healthy. If the embedder dies mid-run, **abort without consuming an
  attempt** and leave the run queued for later (do not delete, do not mark
  failed). Do not start new runs while the embedder is down.

## New service: `aifarm-embedder`

New sibling repo `/Users/Shared/src/github.com/iamwavecut/aifarm-embedder`,
modeled on `aifarm-draw`.

```
aifarm-embedder/
├── Dockerfile            # pytorch/pytorch:*-cuda*-runtime + curl
├── compose.yaml          # GPU pin + discovery-register profile
├── requirements.txt      # torch, sentence-transformers, fastembed(optional), fastapi, uvicorn, numpy
├── api/app.py            # CUDA port of tools/embedder/app.py + concurrency
├── docker/entrypoint.sh  # uvicorn launcher
├── README.md
└── integrate_to_discovery.md
```

- `api/app.py`: port the current `/encode` + `/health` logic 1:1, but
  `EMBEDDER_DEVICE=cuda` and prefer the sentence-transformers backend on CUDA
  (`trust_remote_code`, task-aware via `default_task`). Keep L2-normalize + crop.
  Add a bounded `asyncio.Semaphore` around GPU inference so concurrent Discovery
  jobs don't oversubscribe the GPU; `/encode` already accepts batched `prompts`.
- `compose.yaml`: `NVIDIA_VISIBLE_DEVICES` / device reservation = GPU 2 UUID,
  `CUDA_VISIBLE_DEVICES=0`, `UVICORN_PORT=8080`, healthcheck `GET /health`,
  `discovery-net` alias `embedder`, `discovery-register` profile that POSTs:

  ```json
  {"service":{"name":"embedder","base_url":"http://embedder:8080",
   "execution_model":"SERVICE_EXECUTION_MODEL_SYNC",
   "endpoints":[{"name":"encode","method":"HTTP_METHOD_POST",
                 "path":"/encode","timeout_ms":30000}],
   "default_timeout_ms":30000,"max_concurrent_jobs":8,"enabled":true},
   "upsert":true}
  ```

### VRAM experiment (`ssh aifarm`, non-disruptive)

1. `nvidia-smi` baseline.
2. Bring up the container pinned to GPU 2, model on CUDA fp16. Measure the delta
   via the process `used_gpu_memory`.
3. Latency + peak VRAM for batch sizes 1 / 32 / 256.
4. Fit ladder if it ever crowds Qwen: fp16 → cap batch/seq → 8-bit
   (bitsandbytes) or ONNX int8 (optimum / fastembed-gpu) → as last resort move to
   GPU 1 (4060 Ti). **Never CPU.** Pick the smallest option that keeps the
   contract. Expectation: nano fp16 ≈ a few hundred MB; quantization most likely
   unnecessary but the ladder is ready.

Guardrail: only add the new container, never touch the live LLM/Qwen/draw
containers; watch free VRAM before/after so Qwen keeps headroom.

## OpenPlotva integration

### Circuit breaker

`EmbedderCircuitBreaker` (new), shared via `Arc`, state in atomics:
`consecutive_failures: AtomicUsize`, `open_until_unix_ms: AtomicI64`. No locks
held across `.await`.

- `record_success()` → failures = 0, `open_until = 0`.
- `record_failure()` → failures += 1; if `failures >= threshold (5)` set
  `open_until = now + cooldown (300s)`.
- `is_available()` → `now >= open_until` (cheap, shared; used as the
  consolidation gate). While open it returns false without any network call.
- After `open_until` passes, traffic flows again (cooling/half-open); renewed
  failures re-trip. Thresholds configurable
  (`EMBEDDER_BREAKER_FAILURE_THRESHOLD`, `EMBEDDER_BREAKER_COOLDOWN_SECONDS`).

### Discovery embedder client

`DiscoveryEmbedderClient` implements `EmbeddingProvider`, calls the embedder
through Discovery: base64-encode the `/encode` JSON body, submit a job for
service `embedder` / endpoint `encode`, poll the result, decode
`EmbedderEncodeResponse`. Reuse the discovery job machinery from
`openplotva-llm/aifarm.rs` (extract a small shared JSON-call helper if it reduces
duplication; otherwise call the existing path). Wrap the breaker: when open,
short-circuit to a new `EmbedderClientError::Unavailable` without contacting
Discovery; on transport failure record a breaker failure; on success record
success.

Add `embed_batch(texts, dimension, task) -> Result<Vec<Option<Vec<f32>>>, _>` to
the trait (one Discovery job carrying all `prompts`). Implement for both clients.
`embed_one` stays for query-time callers.

### Config + wiring

Replace the direct-URL config with Discovery names, keep dims/model:

- Memory: `MEMORY_EMBEDDER_DISCOVERY_SERVICE_NAME` (default `embedder`),
  `MEMORY_EMBEDDER_DISCOVERY_ENDPOINT_NAME` (default `encode`),
  `MEMORY_EMBEDDING_DIM` (512). Reuse `DISCOVERY_BASE_URL`.
- Shield: `SHIELD_EMBEDDER_DISCOVERY_SERVICE_NAME` / `_ENDPOINT_NAME`
  (default to the same `embedder` / `encode`), `SHIELD_EMBEDDING_DIM`,
  `SHIELD_RETRIEVAL_TIMEOUT_SECONDS`.

`MemoryConfig` / `ShieldConfig` fields and `RawConfig` parsing updated
accordingly. The three builders
(`memory_retrieval_embedder_from_config`, `memory_write_embedder_from_config`,
`shield_embedder_from_config`) construct a `DiscoveryEmbedderClient` sharing one
process-wide breaker `Arc`. Wiring sites updated: admin shield embedder
(`lib.rs:546`), memory write embedder (`lib.rs:8377` / `8850`), dialog
query/shield embedders (`lib.rs:9259-9265`). Update `.env.example`.

### Consolidation gate + abort-without-burn

- **Gate**: in the consolidation taskman job, before `claim_run`, check
  `breaker.is_available()`. If unhealthy, return early without claiming — no
  attempt consumed, runs stay `queued`, and the cheap atomic keeps AI Farm
  untouched while the breaker is open.
- **Abort**: `embed_memory_cards` now uses `embed_batch`; an
  `EmbedderClientError::Unavailable` maps to a new
  `MemoryRunProcessError::EmbedderUnavailable`. `process_next_memory_run` handles
  that variant via a new `store.release_run(run_id)`
  (`status → queued`, clear `lease_owner` / `leased_until`, `attempts` unchanged)
  instead of `fail_run`. Other errors keep `fail_run`. Because the gate blocks
  further claims while the breaker is open, a run is aborted at most once per
  outage transition, then consolidation pauses until the embedder recovers.

Retrieval and shield need no behavior change beyond pointing at the new client —
they already fall back to lexical-only, and the breaker makes that fallback cheap
during an outage.

## Removal of the old embedder (after verification)

Delete only once the new path is verified end-to-end:

- `tools/embedder/` (Dockerfile + app.py).
- `embedder` service, `embedder-cache` volume, `MEMORY_EMBEDDER_URL` in
  `deploy/production/compose.production.yml`.
- embedder tar-copy step and `ensure_service embedder` /
  `wait_for_service_health embedder` in `.github/workflows/deploy-production.yml`
  and `deploy/production/deploy-production.sh` (and the legacy volume import).
- Old `HttpEmbedderClient` (direct `/encode`) once `DiscoveryEmbedderClient`
  replaces it.
- `.env.example` embedder-URL keys; `skills/openplotva-deploy/SKILL.md`
  references.

## Verification

- Service: `curl /health`, `curl /encode` on aifarm; then the same through the
  Discovery job API; confirm 512-dim normalized vectors and concurrency under a
  small load.
- OpenPlotva: `cargo fmt --all`; `cargo test` for the breaker, discovery client,
  `release_run`, and the consolidation gate. End-to-end with config pointed at
  the Discovery embedder (ssh tunnel if 50051 isn't reachable from dev): exercise
  memory retrieval, shield, and one consolidation run; force the embedder down
  and confirm (a) retrieval/shield degrade to lexical-only, (b) consolidation
  pauses and runs stay queued, (c) the breaker opens once and stops AI Farm
  traffic for the cooldown.

## Sequencing

1. Scaffold `aifarm-embedder` (local).
2. Deploy + VRAM experiment on aifarm; finalize GPU/quant config.
3. Register in Discovery + smoke via job API.
4. OpenPlotva: breaker → discovery client + `embed_batch` → config/wiring →
   consolidation gate + `release_run`.
5. `cargo fmt`/tests + end-to-end verification.
6. Remove the old embedder and all remnants; (production redeploy is the user's
   call and confirmed before triggering).

## Risks / notes

- Discovery adds submit+poll overhead on the retrieval hot path (memory 15s,
  shield 6s budgets); a single short embed on GPU is single-digit ms, polls are
  kept tight. Acceptable and the only clean cross-host path.
- Shield degraded to lexical-only during embedder outages is an approved security
  trade-off; document it in operator notes.
- Production deploy / redeploy steps are outward-facing; prepare them but confirm
  before triggering.
