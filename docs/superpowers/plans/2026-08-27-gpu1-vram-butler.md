# GPU1 VRAM Butler — Hardware Arbiter Design & Migration Plan

**Status: PHASES 0-2 DEPLOYED 2026-08-28** (approved with every §5 recommendation; owner then granted full autonomous deploy authority). The arbiter is live on GPU1: butler deployed, ASR + draw + privacy migrated to arbiter mode, pairing proven in production (GigaAM transcription completed mid-boogu-generation — impossible under the flock). Remaining: Phase 3 (OpenPlotva admin card), the gated embedder restart, and the §3.5 late findings below.

## Late owner decisions (2026-08-28, night session)

- **boogu-edit retired entirely** ("не нужен"): endpoint returns 410, dropped from Discovery registration and the profile registry. Weights were never cached; zero production requests ever.
- **Image references capped at 2** — and the 2-ref measurement dissolved the OD11 tight spot differently than expected: the 11.9 GiB peak is the **first-inference-after-slot-load autotune** (same structure as boogu), not a refs cost — warm flux2 with 2 refs is 7587 (+240 over ref-less). Registry v3: flux2 split into first (12032) / warm (8704) profiles; reserve 512→256 (capacity 16120) so flux2-first + resident redactor — production reality with ~470 MiB physical slack — stays admissible; extra refs are clamped to the first 2, not rejected.
- **Full autonomy granted** — merges and deploys executed without per-step confirmation.

## Approval addendum — process identity (farm-wide)

Owner requirement: every uvicorn service on the farm launches as `uvicorn api.app:app`, so `ps`/`nvidia-smi` show indistinguishable `python3` processes. Fix across the board: rename the generic `app.py` entry modules to service-specific names AND set the process title via `setproctitle` (the mechanism behind vLLM's `VLLM::EngineCore` in nvidia-smi — the title is what fixes nvidia-smi; the module rename fixes `ps auxww`).

| service | module | process title | delivered as |
|---|---|---|---|
| asr-api | `api/app.py` → `api/asr_api.py` | `asr-api` | [aifarm-asr#6](https://github.com/iamwavecut/aifarm-asr/pull/6) (with the Phase-0 probe) |
| draw-api | `api/app.py` → `api/draw_api.py` | `draw-api` | [aifarm-draw#5](https://github.com/iamwavecut/aifarm-draw/pull/5) (with the probe; base `boogu-sdnq-image-pipelines`) |
| privacy-filter | `api/app.py` → `api/privacy_filter_api.py` | `privacy-filter` | [aifarm-privacy-filter#1](https://github.com/iamwavecut/aifarm-privacy-filter/pull/1) (with the probe) |
| discovery | `app/main.py` → `app/discovery_api.py` | `discovery` | [aifarm-discovery#1](https://github.com/iamwavecut/aifarm-discovery/pull/1) |
| embedder (uvicorn wrapper) | `api/app.py` → `api/embedder_api.py` | `embedder` | [aifarm-embedder#6](https://github.com/iamwavecut/aifarm-embedder/pull/6) |
| gpu-butler | born as `api/gpu_butler_api.py` | `gpu-butler` | [aifarm-gpu-butler](https://github.com/iamwavecut/aifarm-gpu-butler) |

Already distinct, no action: `VLLM::EngineCore` (llm-openai), `llama-server` (llamacpp/embedder engine), `ninfer-serve`. Deploy gating unchanged: GPU1 services ride the Phase-0/1/2 restart windows; **discovery** restart briefly interrupts job routing for the whole farm; **embedder** lives on GPU2 next to production ninfer — its restart happens only in an explicitly agreed window.

**Goal:** replace the single `4060ti.lock` flock with a byte-accounted lease arbiter (`gpu-butler`) for the three services already running on AI-farm GPU1 (RTX 4060 Ti 16 GB, UUID `GPU-cd077448-af05-0cf1-0a94-be55ba1195f5`): `draw-api`, `asr-api`, `privacy-filter`. On-demand load/offload, no VRAM races, minimal (not total) eviction, and concurrent admission of compatible workloads — while preserving every OpenPlotva-visible contract byte-for-byte.

**Non-goals:** GPU0 (`llm-openai`/vLLM) and GPU2 (`ninfer-3090`, `embedder`, `llamacpp`) are untouched. audio.cpp / TTS / music is a separate future task; the arbiter must accept it later as just another client with a profile, with no core rework. No secret/key rotation of any kind. Discovery internals unchanged. OpenPlotva routing (pools, Taskman arbitration) stays as the second echelon, unchanged.

**Spec:** the task brief (2026-08-27) plus the live survey below. All code citations were re-verified against working trees and the live farm on 2026-08-27.

---

## 0. Live survey (2026-08-27, verified) and corrections to the brief

### 0.1 GPU1 idle state (matches the brief)

| process | container | VRAM idle | role |
|---|---|---|---|
| python3 (pid 3628) | privacy-filter | **3212 MiB** | OPF redactor resident on CUDA |
| python3 (pid 1755824) | draw-api | 240 MiB | CUDA context only (offload leaves ~0 steady) |
| python3 (pid 3213108) | asr-api | 228 MiB | CUDA context only (per-request GPU excursion) |
| Xorg | host | 4 MiB | display server touches all three GPUs |
| **total** | | **3713 / 16380 MiB** | ~12.6 GiB free at idle |

Host: driver 610.43.02, CUDA UMD 13.3. RAM 62 GiB with **swap 8/8 GiB fully used** and 14 GiB available; root disk **97 % full (31 GB free)**. Both constrain the design: nothing in this plan may add RAM residents or large images.

### 0.2 Live traffic (72 h window unless noted)

- **ASR: 1469 of 2973 requests (49.4 %) fell back to Vosk** (`engine=vosk fallback_used=True`). GigaAM avg latency 1.7 s, max 40.6 s. This is the headline pain the arbiter removes: nearly every fallback is *false* contention — a ~2 GiB ASR burst losing a binary mutex to a small boogu job or to a draw cold load, while ~12 GiB sat free.
- **draw-api: 3239 generations / 72 h** — 3140 `/v1/boogu/turbo/generate` (97 %), 99 `/v1/generate` (flux2, ~1.4/h), 0 `/v1/boogu/edit/generate`.
- **privacy-filter: 60 470 `/v1/redact` / 24 h (~42/min)** — a hot path (Gradius ad redaction ×2 per eligible dialog reply + memory extraction), not an idle tenant.
- Restart counts: draw-api **5**, asr-api **7**, privacy-filter 0 — holder death/self-kill is a real recurring event, not a theoretical one.
- All three services are invoked through Discovery (caller IP = discovery container). Registered budgets: draw endpoints 600 s, asr 600 s + `max_concurrent_jobs: 1`, privacy 30 s. Discovery stores any upstream HTTP response **verbatim** (status + body) in the job result (`/home/wavecut/discovery/app/worker.py:122-140`), and itself refuses over-capacity submits with `429 "service capacity unavailable"` (`app/main.py:179`) — so service-emitted busy bodies survive the gateway unmodified.

### 0.3 Corrections to the brief (verified deltas)

- **D1 — privacy-filter is NOT flock-coordinated today.** Its container has **no `../gpu-coordination` bind mount** (`services/privacy-filter/compose.yaml` mounts only `./models`) and `PRIVACY_FILTER_DYNAMIC_CUDA_OFFLOAD=0` keeps the lock code path dead. If someone flips the flag to 1 without adding the mount, `redactor.py:111` silently creates a **container-private** lock file that coordinates with nothing. Its 3.2 GiB residency plus per-request inference already coexists, unarbitrated, with every draw generation — empirical proof that flux2-under-offload + privacy-resident fit together on this GPU.
- **D2 — per-request ping-pong for privacy-filter (Phase-0 hypothesis in the brief) is not viable.** At 42 redacts/min, two ~3.2 GiB PCIe transfers per request would consume a large fraction of GPU1 wall time and add ~0.5-1 s to every eligible dialog reply (Gradius redaction sits synchronously between model finish and Telegram send, `crates/openplotva-app/src/dialog_turn/session.rs:758-830`). The right model is **resident by default, revocable on demand** — the arbiter's eviction protocol, not a mode flag.
- **D3 — the busy dictionary is narrower than stated.** `crates/openplotva-llm/src/retry.rs:173-183` matches exactly five phrases, ASCII-case-insensitive substring over the whole error string: `"capacity unavailable"`, `"service capacity unavailable"`, `"no slots"`, `"no slot available"`, `"resource exhausted"`. The bare `"capacity"` / `"slot"` + 429/503 rule (`crates/openplotva-llm/src/aifarm.rs:4171-4175`, `is_capacity_unavailable`) is applied **only on chat/dialog Discovery submits — the draw path never calls it**. Draw errors flatten to `"generation request failed: status {code}: {body}"` and re-classify purely via the five phrases (`crates/openplotva-app/src/image_jobs.rs:852-870`, `1645-1656`). **A busy body must therefore literally contain `capacity unavailable`** (safest of the five). It must also avoid the veto phrases (`retry.rs:166-171`): `"telegram api error"`, `"validation failed"`, `"user cancelled"`, `"user canceled"`.
- **D4 — CapacityUnavailable is NOT breaker-free.** `crates/openplotva-app/src/routed_attempts.rs:318-344`: every retryable failure, CapacityUnavailable included, calls `record_failure` (5 consecutive → 30 s cooldown, `crates/openplotva-llm/src/router/breaker.rs:15-31`) and additionally arms the capacity-cooldown trigger (`routed_attempts.rs:345-365`). What it *does* buy: immediate requeue with no backoff (`crates/openplotva-taskman/src/lib.rs:1430-1456` never touches `available_at`) and no retry-budget penalty. The only truly cost-free busy path is a client-side pool-busy skip that never starts an attempt (`routed_attempts.rs:211-214`). Image jobs carry `DEFAULT_LLM_JOB_MAX_ATTEMPTS = 5` — five terminal busy answers exhaust the job. Consequence: **draw must keep waiting in-request as its primary busy behavior** (as the blocking flock does today) and refuse only after a long in-request wait.
- **D5 — `asr_fallback_used` comes from the JSON boolean `fallback_used` only.** `crates/openplotva-app/src/asr.rs:601-613, 743-755, 1182-1189`. The `primary_failed:gigaam:...` warning string is **never parsed anywhere** (repo-wide, only test fixtures reference it); `warnings` is stored opaquely to `telegram_files.asr_warnings` and never read back. The compatibility surface is the field name/type, not the text. We will still keep the literal text unchanged (OD4) because it costs nothing.
- **D6 — the draw client sends `wait_for_capacity_ms: 0`** (`crates/openplotva-app/src/image_jobs.rs:822-851`) with a **300 s shared budget** for submit + all polls (`AIFARM_DRAW_API_DEFAULT_TIMEOUT = 5*60`, deadline at `image_jobs.rs:745,791`, watchdog string `"draw-api provider timeout: inference watchdog expired..."` → ProviderTimeout). Discovery will not park draw jobs on capacity; any queueing must happen inside draw-api's request handling, under ~240 s to leave polling headroom.
- **D7 — the ASR client never yields CapacityUnavailable.** Its classifier is typed (`crates/openplotva-app/src/asr.rs:665-680`): asr-api 5xx → ProviderUnavailable (charges the breaker). Fine — asr-api answers 200 even on fallback; 503 only on double-failure (`aifarm-asr/api/app.py:96-98`).
- **D8 — draw startup holds the shared flock for the entire eager `primary` load** (`aifarm-draw/api/app.py:1067-1072` → `generation_critical_section("startup")`), up to ~10 min (healthcheck `start_period: 10m`), and the lock is also held across every slot switch's snapshot download + teardown (`app.py:1014-1015, 1109-1115, 790-827`). With 5 lifetime restarts and ~2.8 slot switches/h (each flux2 request forces flux2-in and later boogu-back), this is a major share of today's 49 % ASR degradation.
- **D9 — allocator config diverges.** asr: `expandable_segments:True,max_split_size_mb:32` (compose + Dockerfile); privacy: `expandable_segments:True` only; draw: nothing in compose — set in-process by `api/app.py:25-27` `os.environ.setdefault(...)`.
- **D10 — draw's "three models" are three named slots + two alternate primary backends.** Slots: `primary` (backend `flux2_sdnq` → `WaveCut/FLUX.2-klein-9B-SDNQ-...uint4-static`, `enable_model_cpu_offload`), `boogu_turbo` and `boogu_edit` (both `enable_sequential_cpu_offload`). Alternate `primary` backends selected by `DRAW_MODEL_BACKEND`: `hidream_o1_sdnq` (`device_map="cuda"`, **full VRAM residency**) and `quanto` (full `.to(device)`). Exactly **one slot resident at a time**; every switch is full teardown + reload (`app.py:769-827`).
- **D11 — ASR self-contention exists in-process.** asr-api has no internal lock; two in-process transcribes race for the flock and the loser silently degrades to Vosk (`service_engines.py:80-103` + `app.py:90-95` thread offload). Discovery's `max_concurrent_jobs: 1` masks this for gateway traffic only. The migration must add an in-process gate so byte-admission does not un-mask it.

---

## 1. Target architecture & arbiter API

### 1.1 Principles

1. **Bytes, not a mutex.** Admission = does the requested profile envelope fit into `capacity − committed`. Compatible pairs (ASR burst over a long draw job) admit naturally; a 300 MiB job never queues behind a 15 GiB one unless bytes genuinely lack.
2. **Leases, not flocks.** Every grant has an owner, a size, a deadline, and a heartbeat. Holder death (draw's CUDA-poison SIGTERM self-kill, `aifarm-draw/api/app.py:120-157`) is survived by TTL + NVML reconciliation, not by kernel lock cleanup semantics.
3. **Residency is explicit and revocable.** privacy-filter's 3.2 GiB is a first-class *resident* grant the butler can (policy permitting) ask back via a callback — the "evict the necessary minimum" requirement. Burst services (draw under offload, ASR excursions) hold nothing between requests and need no eviction.
4. **The word contract is sacred.** Every refusal the business layer can see is `429` + a body containing the literal phrase `capacity unavailable` (plus `no slot available` for belt-and-suspenders with the chat-path classifier), never a veto phrase (D3).
5. **Fail toward today.** If the butler is unreachable, every client falls back to the exact legacy flock behavior (ASR fail-fast → Vosk; draw blocking). The butler holds the legacy flock while burst leases are active, so mixed migrated/unmigrated states are always mutually exclusive.
6. **Extensible by registry.** A future TTS/music service = one profile entry + the vendored client. No core changes.

### 1.2 Deployment

New repo **`aifarm-gpu-butler`**; deployed as `/home/wavecut/services/gpu-butler/` (fourth compose project — no merging of the existing three; see OD1):

```
aifarm-gpu-butler/
  api/app.py          # FastAPI: lease/resident/incident/status endpoints
  api/ledger.py       # byte ledger, admission, queue, eviction planner
  api/bridge.py       # legacy flock bridge (holds /gpu-coordination/4060ti.lock)
  api/nvmlwatch.py    # NVML reconciliation loop (100 ms sampling on demand, 2 s steady)
  api/state.py        # lease journal snapshot/restore (./state/leases.json)
  client/gpu_butler_client.py   # stdlib-only, vendored into each service repo
  profiles.yaml       # the VRAM profile registry (§2)
  tests/
  Dockerfile          # python:3.12-slim + fastapi + uvicorn + nvidia-ml-py (~80 MB image)
  compose.yaml
```

compose essentials: `restart: unless-stopped`, network `discovery-net` (alias `gpu-butler`), **no published host port**, `NVIDIA_VISIBLE_DEVICES=GPU-cd077448-af05-0cf1-0a94-be55ba1195f5`, `NVIDIA_DRIVER_CAPABILITIES=utility` (NVML without CUDA), device reservation same as the three services, bind mounts `../gpu-coordination:/gpu-coordination` and `./state:/state`, healthcheck `GET /health`. Single uvicorn worker; all state in-process + journal.

### 1.3 Ledger model

**Capacity:** `capacity_mib = 16380 − 4 (Xorg) − 512 (fragmentation/driver reserve) = 15864`.

**Grant kinds:**
- `burst` — per-request; fields `{service, profile, request_id, priority, wait_ms, ttl_ms}`; client heartbeats every 10 s, TTL 30 s (survives ~20 s of GIL starvation during inference); reclaim on 2 missed heartbeats, or immediately when NVML shows the owning PID gone.
- `resident` — long-lived; fields `{service, profile, callback_url, evictable}`; no TTL, revoked only via the vacate protocol; re-verified against NVML every watchdog tick.

**Static context entries:** per-service CUDA context (asr 228, draw 240 MiB) is carried in the registry as `context_mib` and permanently committed once the service is seen alive — contexts never free short of container restart.

**Admission rule** for a burst of profile `p`:

```
committed = Σ context_mib(live services) + Σ resident envelopes + Σ active burst peaks
fits      = (committed + p.peak_mib ≤ capacity_mib)
         && (nvml_free_now ≥ p.peak_mib + 256)   # reality clamp against unknown PIDs / drift, R10
```

The NVML clamp protects against unknown PIDs and profile drift; it uses the instantaneous free reading with a 256 MiB cushion and never *grants* on NVML alone.

**Queue:** two classes — `0 interactive` (asr, privacy operations) > `1 generation` (draw, future media). FIFO within class. No aging in v1 (OD10): ASR never queues (wait_ms=0 preserved as a feature) and OpenPlotva's `boogu-gpu` pool (max_concurrency 1, DB row re-asserted by `crates/openplotva-app/src/model_routing.rs:1568-1582`) already serializes draw client-side, so the butler queue is nearly always depth ≤ 1. A queued waiter re-evaluates on every release/vacate; on `wait_ms` expiry it gets the refusal body of §1.5.

**Eviction planner** (the "unload the necessary minimum"): when a burst does not fit but would fit after reclaiming evictable residents, pick the smallest sufficient set ordered by (`idle_seconds` desc, `bytes` desc), send each a vacate callback with a deadline, wait for acks, then grant. If a resident misses its deadline, the waiting lease is refused normally (never force-freed) and an incident is raised. With current policy (privacy `evictable: false`, OD3) the planner is a safety valve for future tenants and for the `hidream`/`quanto` backends, not a routine path — §2.4 shows all production pairings fit without eviction.

**Legacy flock bridge:** the butler holds `flock(LOCK_EX)` on `/gpu-coordination/4060ti.lock` **iff ≥ 1 burst lease is active** (residents do not hold it — privacy never participated in the flock, D1, and must not start blocking legacy users). Transition 0→1 bursts: acquire with `LOCK_NB` retry loop (250 ms); while a legacy holder (un-migrated draw) owns the flock, the butler grants no bursts — waiters queue, fail-fast callers get the refusal instantly. This makes any partial-migration state exactly as safe as today. The bridge stays after full migration (OD7) as insurance against stray scripts and env-flip rollbacks.

**Restart recovery:** journal every grant to `/state/leases.json`. On boot: restore journal entries as `suspect`; for 15 s grant only adoptions (heartbeat with a known lease id re-attaches) and refuse new bursts with the standard busy body; residents are re-confirmed by one NVML pass + registry. After the grace window, drop unconfirmed suspects and resume. Clients treat `404 unknown lease` on heartbeat as "finish the current GPU work, then re-acquire next time" — never abort in-flight inference.

**Watchdog:** NVML per-PID sweep every 2 s (100 ms burst sampling only while a measurement flag is set): unknown CUDA PID > 300 MiB → alarm; a service exceeding its envelope ×1.15 for >10 s → alarm + profile flagged; ledger/NVML drift > 1 GiB for 30 s → alarm. Alarms = ERROR log + `/v1/status.alarms[]` + incident counter. No auto-remediation in v1.

**Incidents:** `POST /v1/incidents {service, profile, kind: "oom"|"envelope_exceeded", observed_mib}` from service `except torch.OutOfMemoryError` handlers. The butler quarantines the profile: effective envelope becomes `max(envelope, observed_mib × 1.15)` until an operator resets it via registry reload, and `/v1/status` shows the quarantine.

### 1.4 Intra-service serialization invariants (the flock's hidden second job)

Byte-admission removes the accidental cross- *and intra*-service mutual exclusion the flock provided. Each client keeps its own in-process guard:

- **asr-api:** new `threading.Lock` with **non-blocking** `acquire` around the GigaAM excursion — a second in-process transcribe fails fast to Vosk exactly as a busy lease does (closes D11 without changing semantics).
- **draw-api:** `GENERATE_LOCK` (`api/app.py:118-119, 188-192`) stays — one generation at a time per process is a model-object constraint, not an arbiter concern.
- **privacy-filter:** the event loop already serializes (`api/app.py:124-134` runs the model synchronously); unchanged.

### 1.5 Arbiter HTTP API

All bodies JSON. The refusal body is engineered for both OpenPlotva classifiers (D3): `error` is the first key `parse_response_error` reads, and the text carries `capacity unavailable` (message-rule hit) + `no slot available` (redundant hit, and `slot` for the chat-path status classifier).

```
POST /v1/leases
  {"service":"asr-api","profile":"asr-gigaam-burst","request_id":"media-...",
   "priority":0,"wait_ms":0,"ttl_ms":30000}
  201 {"lease_id":"L-000041","granted_mib":2048,"ttl_ms":30000,"heartbeat_ms":10000}
  429 {"error":"capacity unavailable","detail":"no slot available on gpu1: committed 14020 MiB of 15864 MiB, queued 1","retry_after_ms":1500}
      + header Retry-After: 2

POST /v1/leases/{id}/heartbeat        -> 200 {"ttl_ms":30000} | 404 {"error":"unknown lease"}
DELETE /v1/leases/{id}                -> 204

POST /v1/residents
  {"service":"privacy-filter","profile":"privacy-redactor-resident",
   "callback_url":"http://privacy-filter:8080/admin/gpu","evictable":false}
  201 {"resident_id":"R-1","granted_mib":3724}
  409 {"error":"capacity unavailable","detail":"no slot available for resident privacy-redactor-resident","retry_after_ms":5000}   # client retries with backoff
POST /v1/residents/{id}/vacated       -> 200   # service ack after .to(cpu)+empty_cache
DELETE /v1/residents/{id}             -> 204

# butler -> service callbacks (service-side admin endpoints, §3 Phase 2):
POST {callback_url}/vacate   {"resident_id":"R-1","deadline_ms":15000,"reason":"lease L-77 needs 11264 MiB"}
POST {callback_url}/restore  {"resident_id":"R-1"}

POST /v1/incidents  {"service":"draw-api","profile":"draw-flux2-burst","kind":"oom","observed_mib":11930} -> 202
GET  /v1/status   # ledger, per-grant table, queue, NVML per-PID, bridge state, alarms, client versions, profile quarantines
GET  /v1/profiles ; POST /v1/profiles/reload   # re-read profiles.yaml from disk
GET  /health
```

No authentication (internal `discovery-net` only, no published port; consistent with the three services' own unauthenticated `/v1/*`). No new secrets are introduced anywhere in this plan.

### 1.6 Client library

`client/gpu_butler_client.py`, stdlib-only (`urllib.request` + `threading`), vendored as one file into each service repo (OD2), ~300 lines:

```python
lease = GpuButler.acquire(profile="asr-gigaam-burst", request_id=rid,
                          wait_ms=0, priority=0)      # raises GpuBusy on refusal
with lease:                                            # heartbeat thread inside
    ...  # GPU excursion
```

Behavior matrix, controlled per service by `<SVC>_GPU_ARBITER_MODE` (`arbiter` | `flock` | `off`) + `<SVC>_GPU_ARBITER_URL` (default `http://gpu-butler:8080`):

- `arbiter`: HTTP lease; on **transport error/timeout (750 ms)** to the butler → automatic per-call fallback to `flock` semantics with a loud `WARNING` (availability first; the bridge keeps this safe).
- `flock`: the exact legacy code path (fail-fast for asr, blocking for draw) — the rollback switch.
- `off`: no coordination (matches privacy-filter's current live reality; used only during Phase-0 measurement).

The client logs its version string at startup; the butler records versions seen in `/v1/status`.

### 1.7 What OpenPlotva keeps doing (second echelon, unchanged)

- `boogu-gpu` capacity pool `max_concurrency=1` on draw models (`model_routing.rs:1568-1582`) — client-side serialization stays.
- Taskman accelerator arbitration over `Asr | ImageGen | ImageEdit` with ASR_PRIORITY=6, VIP image 5→ceiling 7, +1/300 s aging (`crates/openplotva-taskman/src/lib.rs:18-29, 3290-3349`) — stays.
- ASR unpooled bypass (`model_routing.rs:1585-1616`) — stays.
- VIP draw's two parallel legs (`tokio::join!` at `crates/openplotva-app/src/image_jobs.rs:1221-1233`; flux `generate` + boogu `boogu_turbo_generate`, both `service_name="draw-api"`) arrive at draw-api already serialized by the pool; the butler sees them as two ordinary sequential burst requests. No special handling.

**Phases 0-2 require zero OpenPlotva code changes.** Phase 3 (visibility) is additive only.

---

## 2. VRAM profile registry & measurement methodology

### 2.1 Registry (`profiles.yaml` in `aifarm-gpu-butler`, hot-reloadable)

```yaml
capacity_mib: 15864          # 16380 - 4 xorg - 512 reserve
services:
  asr-api:        {context_mib: 228}    # measured 2026-08-27 nvidia-smi
  draw-api:       {context_mib: 240}    # measured 2026-08-27
  privacy-filter: {context_mib: 0}      # included in resident envelope
profiles:
  - key: asr-gigaam-burst
    service: asr-api
    kind: burst
    peak_mib: 2048            # ESTIMATE (fp32 GigaAM-v3 ~1 GiB weights + 22 s-chunk activations); MEASURE in Phase 0
    status: estimate
  - key: draw-boogu-turbo-burst
    service: draw-api
    kind: burst
    peak_mib: 2560            # ESTIMATE (sequential offload: largest submodule + 1024x1024 activations); MEASURE
    status: estimate
  - key: draw-boogu-edit-burst
    service: draw-api
    kind: burst
    peak_mib: 3072            # ESTIMATE (adds 4 Mpx reference conditioning, seq len 1280); MEASURE
    status: estimate
  - key: draw-flux2-burst
    service: draw-api
    kind: burst
    peak_mib: 9216            # ESTIMATE range 7-11 GiB (model offload: uint4 9B transformer ~4.7 GiB + activations + VAE decode); MEASURE — this number decides all pairing policy
    status: estimate
  - key: draw-hidream-exclusive
    service: draw-api
    kind: burst
    peak_mib: 15100           # ESTIMATE from 14.8 GiB full-resident lab figure; requires privacy eviction; not the active backend
    status: estimate
  - key: privacy-redactor-resident
    service: privacy-filter
    kind: resident
    steady_mib: 3212          # measured 2026-08-27 nvidia-smi (post-warmup)
    burst_extra_mib: 512      # ESTIMATE (n_ctx 4096 MoE activations above steady); MEASURE
    evictable: false          # OD3
    status: partial
```

Envelope math: burst `effective = ceil(measured_peak × 1.15 / 128) × 128`; resident `effective = ceil((steady + burst_extra) × 1.05 / 128) × 128`. Fields per entry after Phase 0: `measured_mib`, `measured_at`, `method`, `image_digest`, `status: measured`.

**Re-measurement triggers** (any → profile back to `estimate`, butler logs a warning on mismatch with the running image digest): model id/revision change, torch/diffusers/base-image change, offload-mode change, `PRIVACY_FILTER_N_CTX` change, driver major upgrade.

### 2.2 Measurement tooling

Two independent probes; the profile takes the **max NVML per-process peak** across runs (that is what the GPU actually loses), with torch allocator peaks as cross-check (>800 MiB discrepancy → investigate before trusting).

1. **Host-side sampler — no service changes, runnable immediately.** `tools/vram-probe.py` (lives in `aifarm-gpu-butler`) run in a disposable container:
   `docker run --rm --network none --gpus '"device=GPU-cd077448-..."' -e NVIDIA_DRIVER_CAPABILITIES=utility -v $PWD:/w python:3.12-slim sh -c "pip -q install nvidia-ml-py && python /w/vram-probe.py --gpu-uuid GPU-cd077448-... --interval-ms 100 --out /w/probe.csv"`
   Samples per-PID used memory at 100 ms; the driver's process accounting catches bursts ≥ ~100 ms, which covers every phase here except sub-100 ms allocator spikes — those are covered by probe 2.
2. **In-service torch peaks — Phase-0 log-only PRs.** Behind env flag `<SVC>_VRAM_PROBE=1`: `torch.cuda.reset_peak_memory_stats()` at request start, then one log line at request end: `vram_probe profile=<key> peak_alloc_mib=<max_memory_allocated> peak_reserved_mib=<max_memory_reserved> elapsed_ms=<n>`. Insertion points: `aifarm-asr/api/service_engines.py` GigaAM `transcribe` finally-block; `aifarm-draw/api/app.py` both generators' finally (`:883-884`, `:991-994`) plus one line after each `load_named_pipeline` (to verify the load-phase near-zero claim per backend); `aifarm-privacy-filter/api/redactor.py` `_redact_with_runtime` + after warmup.

### 2.3 Measurement campaign (workload matrix)

Run against live services during a quiet window, 3 runs per cell, plus the organic traffic already flowing (probe 1 needs nothing synthetic for boogu/privacy — 44 gens/h and 42 redacts/min provide samples for free):

| profile | workload | notes |
|---|---|---|
| draw-flux2-burst | 3× t2i 1024×1024 (prod default, steps 4); 1× aspect `1:2` → 640×1344 (the largest shape OpenPlotva's `draw_api_dimensions` can emit, `image_jobs.rs:4327-4348`); 1× with 4 `image_url` refs | decides all pairing; also record load-phase VRAM during the slot switch |
| draw-boogu-turbo-burst | 3× t2i 1024×1024 steps 4 | organic traffic suffices |
| draw-boogu-edit-burst | 2× edit with 4 Mpx input, `max_sequence_length` 1280 | endpoint currently unused in prod — synthetic required |
| asr-gigaam-burst | 3× 22 s single-chunk voice; 1× ~60 s multi-chunk | organic traffic suffices |
| privacy-redactor-resident | 3× ~4096-token redact (`typed_placeholders`); steady re-read; 1× CPU-mode timing run (`PRIVACY_FILTER_DEVICE=cpu` on a throwaway container) | CPU timing feeds OD3's degraded-mode option |

Results land in `profiles.yaml` (fields of §2.1) in the butler repo; the raw CSVs are task artifacts, deleted after the numbers are committed (per repo hygiene rules).

### 2.4 Ledger arithmetic — worked example with current estimates

```
capacity                                   15864
static contexts        asr 228 + draw 240    468
privacy resident envelope (3212+512)×1.05  3910   (rounded 3968 to 128-step)
----------------------------------------------------
free for bursts at idle                   ~11428

boogu-turbo 2560 + asr 2048 = 4608        fits (headroom ~6.8 GiB)  -> the 49 % fallback collapses
flux2 9216                                 fits (headroom ~2.2 GiB)
flux2 9216 + asr 2048 = 11264              fits marginally           -> measurement decides; if not, ASR falls back only during flux2 (~1.4/h × ~1-2 min ≈ 2-4 % of time, vs 49 % today)
boogu-edit 3072 + asr 2048                 fits
hidream-exclusive 15100                    does NOT fit with privacy resident (needs eviction => blocked under evictable:false; documented, not the active backend)
```

Every production pairing fits even on conservative estimates; eviction is genuinely a rare-path valve. If Phase-0 measurement moves flux2 above ~9.4 GiB, the flux2+ASR pairing drops out and ASR degrades to Vosk only during flux2 generations — still a ~15-25× improvement over today.

### 2.5 Phase-0 measurement results (2026-08-28) — pending OD11 sign-off

Method: in-service torch peak probes (`*_VRAM_PROBE=1`, live in prod) cross-checked against a host NVML sampler at 100-200 ms; 5 synthetic flux2 runs (3× 1024², 1× 640×1344, 1× with 4 refs), 2 long privacy redacts (10.7k chars), organic traffic for boogu/GigaAM/privacy. Registry committed as `aifarm-gpu-butler` `profiles.yaml` @ `7498767`; butler tests re-derived from these numbers (27 passing).

| profile | measured | chosen envelope | note |
|---|---|---|---|
| asr-gigaam-burst | NVML peak **1540** | **1792** | below the 2048 estimate |
| draw-boogu-turbo-burst (warm) | reserved 2486-2488, NVML **2682** | **3200** | estimate was 2560 — low |
| draw-boogu-turbo-**first**-burst | **9427-9488** reserved (~9.9 GiB NVML) | **11392** | **discovery #1:** the first inference after every slot (re)load runs quantized-matmul autotune with big scratch, then settles at 2.5 GiB. Draw will request this profile for the first post-load generation (Phase-2 wiring) |
| draw-flux2-burst (t2i, no refs) | hard alloc 7348-**9606**; elastic reserved 12318 (NVML 12738) | **11136** | **discovery #2:** the allocator reserves opportunistically up to ~12.3 GiB when free memory allows, but hard allocations peak at 9.6 GiB — admission budgets hard allocations, the elastic reserve shrinks under pressure (empirically: flux2 has coexisted with the 3.2 GiB resident redactor in prod all along) |
| draw-flux2-**ref**-burst (1-4 refs) | hard alloc **11899** | **12288** | **the one tight spot:** 12288 + privacy 3456 + contexts 468 = 16212 > 15864 — formally does not fit, yet production runs exactly this today with ~400 MiB real headroom. OD11 must pick: (a) codify the tightrope (dedicated profile + shrink reserve 512→256), (b) refuse refs-flux2 next to the resident redactor (behavior change), or (c) make privacy evictable for this profile only. Recommendation: **(a)** — it matches observed prod behavior and adds the watchdog + OOM-incident safety net that today doesn't exist |
| privacy-redactor-resident | steady **3208**, burst above residency **0** (reserved pinned at 3050-3052 even on 10.7k-char redacts, 20-240 ms) | **3456** | tighter than the 3968 estimate |
| draw slot loads (all backends) | **0-48 MiB**, 2.8-6.2 s | — | the Phase-2 premise "cold loads need no lease" is now measured fact; note the eager startup load took 6.2 s with warm page cache (the 10-min figure applies to first-ever downloads) |
| draw-boogu-edit-burst | not measured | 3072 (estimate) | endpoint unused; weights absent from the local HF cache — measuring means a multi-GB download, deferred to an owner window |

Updated pairing math (committed baseline = contexts 468 + privacy 3456 = **3924**, free for bursts ≈ **11 940**): boogu-warm + ASR = 4992 — fits with ~7 GiB headroom (the 49 % fallback fix stands); boogu-**first** + privacy = 15 316 — fits, but not with ASR on top; flux2-t2i + privacy = 15 060 — fits, ASR degrades to Vosk only during flux2 windows (~2-4 % of time); flux2-refs — the OD11 question above. Timings for lease TTLs: flux2 22-101 s (173 s first-after-restart), boogu warm 35-41 s / first-run 100-147 s, redact 20-240 ms.

---

## 3. Phased migration plan

Every phase ships as its own PR(s) in the owning repos, green locally first, deployed only with explicit owner go-ahead per deployment. Rollback for every service change is an env flip + `docker compose up -d <svc>` (no image rebuild), because both code paths ship side by side.

### Phase 0 — instrumentation, measurement, butler scaffold (no behavior change)

**Progress 2026-08-28 — Phase 0 substantially complete.** All five PRs merged; asr-api, privacy-filter, draw-api rebuilt and redeployed with probes enabled (`*_VRAM_PROBE=1` in each farm `.env`) and the new process titles live in ps/nvidia-smi; Discovery redeployed (seconds of downtime) and retitled. One extra fix shipped on the way: [aifarm-draw#6](https://github.com/iamwavecut/aifarm-draw/pull/6) pins diffusers/accelerate to the deployed SHAs (`26ec30e8`/`2278ebbf`) after upstream diffusers main moved to `huggingface-hub>=1.0` and broke every rebuild — the §6 unpinned-deps risk materialized on the first rebuild and is now closed. `docker system prune -af` (owner-requested) reclaimed **128.9 GB**; disk went 97 % → 72 % (245 GB free), retiring the disk-pressure risk. Measurement campaign ran the same night (§2.5). Remaining: organic GigaAM torch-probe confirmation (NVML number already captured), boogu-edit synthetic (deferred: weights not in the local HF cache — a multi-GB download needs an owner window), embedder redeploy (merged but gated: GPU2), **OD11 sign-off on §2.5**.

Changes:
- **aifarm-gpu-butler (new repo):** daemon skeleton + client + `profiles.yaml` v1 (estimates) + `tools/vram-probe.py` + tests. Nothing deployed yet.
- **aifarm-asr PR:** probe log line behind `ASR_VRAM_PROBE` (off by default) in `api/service_engines.py`.
- **aifarm-draw PR:** probe log lines behind `DRAW_VRAM_PROBE` in `api/app.py` (both generators + slot-load).
- **aifarm-privacy-filter PR:** probe log line behind `PRIVACY_FILTER_VRAM_PROBE` in `api/redactor.py`; **compose fix for D1's footgun documented in the PR description** (no mount added — the lock stays dead; the arbiter replaces it in Phase 2).
- Run the §2.3 campaign; commit measured `profiles.yaml`.

Deployment note: enabling the probes requires one rebuild+restart per service. **A draw-api restart re-enters the ~10-min flock-held startup load and degrades ASR to Vosk for that window** — schedule off-peak; asr/privacy restarts are ~1 min.

Success check: `profiles.yaml` fully `measured`; ASR fallback ratio unchanged (±5 pp) vs the 49.4 % baseline (`docker logs asr-api --since 72h | grep -c "fallback_used=True"` vs `=False`); no new warnings in service logs.
Rollback: unset probe env vars (flags off = dead code); revert PRs if desired.

**Checkpoint (OD11): owner approves the measured profile table before Phase 1 flips anything.**

### Phase 1 — butler live, flock bridge, ASR migrated

Changes:
- **Deploy gpu-butler** (`/home/wavecut/services/gpu-butler/compose.yaml`); verify `/health`, `/v1/status`, bridge idle (flock unheld), NVML view matches nvidia-smi.
- **aifarm-asr PR:** vendor `gpu_butler_client.py`; in `api/service_engines.py` route the GigaAM excursion through the client (`wait_ms=0`, profile `asr-gigaam-burst`, ttl 90 s to cover the 40 s max excursion) behind `ASR_GPU_ARBITER_MODE`; add the in-process non-blocking gate (§1.4); on lease refusal raise `RuntimeError("GPU lock is busy: gigaam transcribe")` — byte-identical text (OD4), so the warning remains `primary_failed:gigaam:GPU lock is busy: gigaam transcribe`; keep the `flock` branch intact for rollback. compose: `ASR_GPU_ARBITER_MODE=flock` at merge; flip to `arbiter` via env once the butler is verified healthy.
- Note: ASR's existing "any exception → Vosk" (`api/runtime.py:91-109`) already covers butler bugs — worst case is today's behavior.

Success check (72 h): fallback ratio **< 10 %** (from 49.4 %) with draw traffic unchanged; butler `/v1/status` shows overlapping asr+draw-legacy exclusion working (bridge held during draw flock holds); zero OOM (watchdog alarms 0, `dmesg | grep -i xid` clean); GigaAM avg latency unchanged ±10 %.
Rollback: `ASR_GPU_ARBITER_MODE=flock`, `docker compose up -d asr-api`. Butler stays (harmless, holds no locks when unused).

### Phase 2 — draw migrated (lease = inference only), privacy managed residency, eviction valve

Changes:
- **aifarm-draw PR:**
  - Split `generation_critical_section`: keep `GENERATE_LOCK`; in `arbiter` mode replace the flock with a lease acquired **after** `load_named_pipeline` and around inference only. Per-backend `load_requires_lease`: `false` for `flux2_sdnq`/`boogu_*` (loads are CPU-targeted, `app.py:558-576, 651-658` — verified in Phase 0 probes), `true` for `quanto`/`hidream_o1_sdnq` (they allocate VRAM during load, `app.py:510, 538, 732`).
  - Startup: in `arbiter` mode `startup()` loads `primary` **without any lock** (CPU-side) — the 10-minute ASR-starving startup window disappears (D8).
  - Busy behavior: in-request wait `wait_ms = 240_000` (OD5), then `HTTPException(status_code=429, detail="capacity unavailable: no slot available on gpu1 for draw")` + `Retry-After` — classified CapacityUnavailable by OpenPlotva (D3), requeued without backoff, attempt-counted and breaker-counted exactly like today's 300 s ProviderTimeout, but ~4× cheaper in wall-clock (D4).
  - Profiles per endpoint: `primary`→`draw-flux2-burst` (or `draw-hidream-exclusive` when that backend is active), `boogu_turbo`→`draw-boogu-turbo-burst`, `boogu_edit`→`draw-boogu-edit-burst`. Heartbeat thread during generation; lease released in the same `finally` that runs `flush_cuda_cache` (`app.py:883-884, 991-994`) — ahead of the poison SIGTERM path, which fires outside the critical section (`app.py:120-157`).
  - Management endpoints: `GET /admin/gpu/status` (resident slot, backend, last-used, poisoned flag, loading state), `POST /admin/gpu/unload` (wraps `unload_pipeline_states()` under `GENERATE_LOCK`), `POST /admin/gpu/prewarm {"slot": "boogu_turbo"}` (wraps `load_named_pipeline`). These are the "management endpoints" hole-closure; the butler does not call them in v1 policy, operators and OD9 automation do.
  - `DRAW_GPU_ARBITER_MODE` env, default `flock` at merge, flipped after smoke.
- **aifarm-privacy-filter PR:**
  - Vendor client; on startup acquire `privacy-redactor-resident` (retry loop with 5 s backoff; if the butler is unreachable, proceed resident anyway and keep retrying registration in background — availability first, ledger self-heals on adoption).
  - `POST /admin/gpu/vacate`: `_move_runtime("cpu")` + `flush_cuda_cache()` (reuses `api/redactor.py:201-207`), ack `/v1/residents/{id}/vacated`, enter degraded mode; `POST /admin/gpu/restore`: `_move_runtime("cuda")`. Degraded mode per redact: burst lease `wait_ms=5000` + ping-pong; on refusal → `503 {"error":"capacity unavailable: no slot available for redaction"}` — memory path classifies CapacityUnavailable and retries its background job (`memory_runtime.rs:3826-3831`), Gradius path is fail-closed and skips the ad, the user reply is unaffected (`session.rs:789-826`).
  - With `evictable: false` (OD3 default) vacate is never invoked by the planner; the endpoints exist for operators and future policy.
  - compose: add `../gpu-coordination:/gpu-coordination` mount (kills the D1 footgun) + `PRIVACY_FILTER_GPU_ARBITER_MODE`.
- **gpu-butler:** enable the eviction planner (inert under current policy) and resident restore pushes.

Success check: curl smoke of the refusal body from draw (temporarily set `wait_ms=1` on a test env var — verify literal `capacity unavailable` and 429); flux2 generation succeeds with privacy resident and ASR mid-flight (watch `/v1/status` show 3 concurrent grants); 72 h: ASR fallback **< 5 %**; boogu p95 latency ±10 % of baseline; OpenPlotva `admin_routing_status` (`crates/openplotva-app/src/lib.rs:8761-8826`) shows no new breaker opens for `aifarm-draw`; no watchdog alarms.
Rollback: per-service `<SVC>_GPU_ARBITER_MODE=flock` env flips (the bridge keeps mixed mode mutually exclusive); privacy reverts to unmanaged-resident (today's exact state) via mode `off`.

### 3.5 Phase 1-2 deployment record (2026-08-28, night)

Shipped and verified live, in order: butler deployed (`/home/wavecut/services/gpu-butler`, `pid: host` for NVML name resolution — [commits](https://github.com/iamwavecut/aifarm-gpu-butler)); privacy residency seeded in the ledger; ASR migrated ([aifarm-asr#7](https://github.com/iamwavecut/aifarm-asr/pull/7)) — first production lease `L-000001`: 201 → GigaAM (probe 1285 MiB) → 204; draw migrated ([aifarm-draw#7](https://github.com/iamwavecut/aifarm-draw/pull/7), warm-fix [#8](https://github.com/iamwavecut/aifarm-draw/pull/8)) — startup now takes **zero** flock acquisitions; privacy managed residency ([aifarm-privacy-filter#2](https://github.com/iamwavecut/aifarm-privacy-filter/pull/2)).

**The pairing proof:** `PAIRING engine=gigaam fallback=False latency_ms=1365` returned mid-flight of a 31 s boogu generation (butler log shows the ASR lease granted and released inside draw's lease window) — the exact event the flock made impossible and the reason 49.4 % of voice messages degraded to Vosk.

Also exercised live: queue-wait (a held 12 GiB lease parked an organic-style boogu request 180 s, inside the 240 s budget, then granted); orphaned-lease reclaim via 3×TTL; butler restart adoption (journal restored `R-0001` across a redeploy); vacate → redact-under-ping-pong-lease → restore roundtrip.

**Late findings:**
1. **Warm profile needs TWO runs** — the second post-load inference peaks as high as the first (~9.4 GiB autotune); `warm_runs >= 2` gates the warm profile now (draw#8).
2. **Vacate frees only ~224 MiB in practice** — `runtime.model.to("cpu")` + `empty_cache` leaves ~2.8 GiB NVML-resident (OPF holds CUDA memory beyond `runtime.model`, or the allocator keeps expandable segments). Harmless under `evictable: false` (the planner never fires), but revocable residency needs deeper OPF surgery before it can back hidream-class exclusives. Redacts under the degraded lease worked (12.6 s first ping-pong; organic redacts back to 13-15 ms after restore).
3. The transient `unknown_pid (unnamed)` watchdog alarm during a draw redeploy is the watchdog correctly seeing a dying process whose /proc entry vanished mid-lookup — expected noise during container swaps.

**Production DB verification (morning after, `plotva` on geta.moe):** ASR fallback per hour went 45.5 % / 50.0 % / 40.0 % (pre-arbiter hours) → **0.0 % across the 01:00-05:00 UTC morning ramp (165+ transcriptions)**; `asr_status='failed'` = 0 in 8 h (the `unavailable` rows are Telegram's own 20 MB "file is too big" limit — pre-existing product behavior); dialog ledger healthy (2 `terminal_failed` / 8 h); `aifarm-vllm-gpu0` error rate 8.6 % vs 7.0 % 72 h baseline (pre-existing broken-video vision traffic on GPU0 — not this program's doing). Draw served zero 5xx and Discovery zero upstream errors through every deploy and smoke.

**Deliberate deferrals (owner follow-up, reasons on record):**
- **embedder redeploy** — merged and ready, but its deploy contract is bespoke and hand-managed (non-git deploy copy, immutable `EMBEDDER_IMAGE` sha tag, `EMBEDDER_SERVICE_REVISION`, `GPU_LOCK_EXPECTED_DEVICE`/`GPU_LOCK_EXPECTED_INODE` guards from the GPU2-lock era, deterministic `Dockerfile.lock` build). The only benefit is the uvicorn wrapper's ps title (the llama-server engine is already distinct in nvidia-smi), and the build cache was pruned so a rebuild means a full llama.cpp compile on the production host. Risk ≫ benefit for an unattended 6 a.m. change next to production ninfer — needs a 15-minute owner window.
- **dependabot alerts** (draw: 3, asr: 6) — reviewed individually, all require major bumps (`transformers` 4.57.1→5.5.0, `torch` 2.8→2.9.1+/2.13). The vulnerable paths are untrusted-model/checkpoint loading and untrusted serialized-tensor APIs; both services load only owner-controlled model repos pinned by env (GigaAM additionally revision-pinned), so the exploit preconditions are absent. A transformers-5 major on the GPU stack is near-certain breakage (sdnq, trust_remote_code GigaAM, diffusers-pin compat) for a theoretical gain — deferred deliberately; alerts left open as the reminder.

### Phase 3 — SHIPPED 2026-08-28: [#116](https://github.com/iamwavecut/openplotva/pull/116) (panel + this plan) and [#117](https://github.com/iamwavecut/openplotva/pull/117) (poll fix), both deployed and verified on production (image `13a315f19`, live arbiter data over the gateway, real fallback trendline). Two post-deploy findings closed the loop:

1. **Discovery's blocking submit waits for capacity, not completion** — the first deploy showed the arbiter card permanently `JOB_STATE_QUEUED`; #117 follows the envelope with up to five 300 ms polls, mirroring the ASR client.
2. **The NVML reality clamp starved flux2-first** (butler `aeb3ba0`): draw's post-generation allocator cache (~480 MiB NVML-resident) pushed free memory 49 MiB under envelope+margin, so every flux2 waited 240 s and got 429 with ~12 GiB genuinely available. The clamp now credits the requesting service its own NVML residency (matched by process title — the identity sweep paying off again). Damage window: 6× 429 on `/v1/generate` over ~2 h (1-2 organic flux2 jobs; boogu untouched; VIP requests still delivered the boogu leg's image). Verified after the fix: flux2 200 in 167 s.

### Phase 3 — visibility & OpenPlotva admin (additive; the only OpenPlotva-touching phase)

- Register `gpu-butler` in Discovery (sidecar pattern like the other services) with a `GET status` endpoint, so OpenPlotva reaches it through the already-configured gateway — no new network surface, no new base URL config.
- **openplotva PR:** admin card "GPU1 arbiter" (Routing Ops or Analytics tab): committed/free bytes, grant table, queue depth, alarms, ASR-fallback trendline; server-side fetch through Discovery; `pl-*` components per the design system, `sha256` guard + `cargo test -p openplotva-web` + design-system review per repo rules.
- Optional (OD8): cold-start ETA surfacing from draw `/admin/gpu/status.loading` — deferred by default.
- Alerting hook: butler alarms → existing admin report channel (reuse the operator diagnostics path; exact wiring decided in the PR).

Success: card renders live data; no behavior change anywhere.
Rollback: revert the openplotva PR; deregister the Discovery entry.

---

## 4. Failure matrix

"Words" below always means a body containing the literal phrase `capacity unavailable` (+ `no slot available` in `detail`). OpenPlotva classification cites D3/D4 semantics.

| # | Scenario | asr-api | draw-api | privacy-filter | OpenPlotva sees |
|---|---|---|---|---|---|
| R1 | Normal concurrency (bytes fit) | lease granted ≤10 ms, GigaAM runs | lease granted, generates | resident, redacts | nothing new — success paths |
| R2 | Bytes genuinely short for ASR (e.g. during flux2 if measurement lands high) | butler 429 (words) → instant Vosk; HTTP **200**, `fallback_used: true`, warning `primary_failed:gigaam:GPU lock is busy: gigaam transcribe` | — | — | `asr_fallback_used=true` column; no error, no breaker (D5) |
| R3 | Bytes short for draw > 240 s | — | in-request wait, then **429** `"capacity unavailable: no slot available on gpu1 for draw"` | — | job error `generation request failed: status 429: ...capacity unavailable...` → **CapacityUnavailable** → immediate requeue (no backoff), attempt++ (of 5), breaker++ and capacity-cooldown trigger (D4) — same charging as today's 300 s ProviderTimeout, faster |
| R4 | Draw holder dies mid-generation (CUDA-poison SIGTERM, restart) | **unaffected** — in arbiter mode draw's restart loads on CPU without any lock (vs today's 10-min Vosk window, D8) | `/health` 503 until restart; lease reclaimed on NVML pid-gone (≤2 s) or TTL (≤30 s) | unaffected | in-flight job: Discovery `UPSTREAM_ERROR`/timeout → ProviderUnavailable/ProviderTimeout → attempt++/breaker++ — identical to today |
| R5 | Privacy vacated (only if policy becomes `evictable: true`) | normal | normal (it requested the bytes) | redact → burst lease wait ≤5 s, else **503** (words) | memory path: CapacityUnavailable → background retry (after retries, fail-open stores unredacted — documented privacy cost of enabling eviction, OD3); Gradius: fail-closed, ad skipped, reply unaffected |
| R6 | Butler restarts | new acquires refused with words for ≤15 s → brief Vosk blips | in-request wait absorbs the grace window | resident untouched; registration re-adopted | at most a few R2/R3-style events during 15 s |
| R7 | Butler unreachable / crashed | client auto-falls back to **flock fail-fast** → today's exact semantics (fallback rate returns toward 49 % until butler is back) | client falls back to **blocking flock** | unmanaged resident (today's state) | today's exact behavior; loud WARNINGs in service logs, butler `/health` red |
| R8 | Profile wrong → CUDA OOM | any-exception→Vosk (200, fallback) + `POST /v1/incidents` | OOM excluded from self-kill (`test_memory.py:395-399`); **500** `status 500: ...out of memory...` + incident | 500 + incident | draw 500 → ProviderUnavailable → breaker (correct: real fault); butler quarantines the profile (envelope inflated ×1.15) |
| R9 | Un-migrated draw still holds the legacy flock (mid-migration) | butler refuses bursts while bridge is blocked → Vosk (exactly today) | legacy blocking behavior | resident (never locked anyway) | today's behavior until draw migrates |
| R10 | Unknown PID appears on GPU1 / ledger drift | admissions clamped by live NVML free (§1.3) — degrades to R2/R3 refusals instead of OOM | same | unaffected | alarms in `/v1/status`; Phase-3 card surfaces them |
| R11 | Discovery down | — | — | — | transport errors → ProviderUnavailable (unchanged today); butler independent of Discovery |

---

## 5. Open decisions — RESOLVED 2026-08-28: every recommendation below was accepted as proposed

(Kept verbatim for the record; OD3 and OD6 get their final numbers re-confirmed at the OD11 checkpoint.)

**OD1 — Butler deployment form.**
Options: (a) container in its own compose project on `discovery-net`, NVML via `NVIDIA_DRIVER_CAPABILITIES=utility`; (b) host systemd service.
**Recommendation: (a)** — uniform ops with the rest of the farm, no host mutations, driver libs injected by the container toolkit; restart policy and healthcheck for free. (b) only wins if we ever want the butler to outlive Docker itself, which nothing here needs.

**OD2 — Client library distribution.**
Options: (a) one vendored stdlib-only file per service repo + version string reported to the butler; (b) proper package installed from the butler repo at image build; (c) shared wheel on a bind mount.
**Recommendation: (a)** — three consumers, zero dependency machinery, drift is visible in `/v1/status.client_versions`. Revisit (b) when a fourth+ consumer lands.

**OD3 — Privacy-filter eviction policy.**
Options: (a) `evictable: false` — flux2/boogu must fit alongside the resident redactor (per §2.4 they do, pending measurement); oversized backends (`hidream`) are simply refused; (b) `evictable: true` + degraded 503-capacity mode during vacate windows; (c) `evictable: true` + CPU-inference degraded mode (needs the Phase-0 CPU timing run).
**Recommendation: (a) now, revisit after Phase-0 numbers.** The redactor serves 42 req/min including a fail-closed hot path; evicting it trades ads + memory-redaction integrity for a backend we do not run. If flux2 measures above ~9.4 GiB, choose between (a)+ASR-falls-back-during-flux2 (~2-4 % of time) and (b) — with the measured numbers in hand.

**OD4 — ASR busy warning text under the arbiter.**
Options: (a) keep byte-identical `GPU lock is busy: gigaam transcribe`; (b) honest `GPU lease is busy: gigaam transcribe`.
**Recommendation: (a).** D5 proves the tail is unparsed, but identical bytes cost nothing and keep log-based tooling/greps stable across the migration boundary.

**OD5 — Draw in-request wait budget W.**
Options: (a) 240 s; (b) 60 s; (c) 290 s.
**Recommendation: (a) 240 s** — inside the 300 s shared client budget with polling headroom (D6); (b) burns the 5-attempt job budget four times faster under sustained pressure; (c) races the client watchdog and turns clean CapacityUnavailable into ProviderTimeout.

**OD6 — Draw request envelope vs profiles.**
The wire schema allows up to 2048×2048 and 4 reference images; OpenPlotva's own dimension formula caps at 1024-class shapes (max emitted 640×1344).
Options: (a) profile at the OpenPlotva envelope and clamp larger direct requests down to it (a service-side contract change for hypothetical non-OpenPlotva callers); (b) per-size profile tiers with bytes chosen by request shape; (c) profile at the absolute schema max.
**Recommendation: (a)** — honest admission needs the profile to bound reality; nothing in production sends >1344, and (c) would waste ~2-4 GiB of admission headroom on every draw job. If you want zero contract drift, (b) with two tiers (≤1344², ≤2048²) is the fallback.

**OD7 — Legacy flock bridge lifetime.**
Options: (a) keep forever; (b) retire after Phase 2 settles.
**Recommendation: (a)** — it is ~40 lines, makes every env-flip rollback instantly safe, and guards against any stray script that still opens `4060ti.lock`.

**OD8 — Cold-start ETA channel to OpenPlotva.**
Options: (a) defer entirely (cold start stays a long first request — today's behavior, now without the flock starvation); (b) Phase-3 admin-only visibility via draw `/admin/gpu/status.loading`; (c) user-visible progress in the bot.
**Recommendation: (b)** — operators get the signal cheaply; (c) is product work outside this plan's scope.

**OD9 — Auto-prewarm boogu after flux2.**
Today the first boogu request after a flux2 generation pays the full slot switch (~minutes) because slots are exclusive (D10). With loads happening outside leases, draw can switch back proactively.
Options: (a) yes — after `primary` has been idle N=120 s and `boogu_turbo` was the previous slot, prewarm `boogu_turbo` off-request via the new `/admin/gpu/prewarm` logic; (b) no.
**Recommendation: (a)** — removes a recurring multi-minute latency cliff from the 97 %-traffic path for one internal timer; it rides entirely on Phase-2 primitives.

**OD10 — Butler queue aging.**
Options: (a) none in v1; (b) mirror Taskman's +1/300 s.
**Recommendation: (a)** — with ASR fail-fast and OpenPlotva's pool serializing draw, butler queue depth is ~≤1; Taskman already does cross-class fairness upstream. Add aging only if `/v1/status` ever shows real starvation.

**OD11 — Measurement sign-off gate.**
Options: (a) hard checkpoint — you approve the measured `profiles.yaml` between Phase 0 and Phase 1; (b) roll through.
**Recommendation: (a)** — every admission decision downstream is only as good as this table.

---

## 6. Risks

- **Profile error → OOM.** Mitigations: ×1.15 envelopes + 128 MiB rounding, 512 MiB capacity reserve, NVML reality clamp on every grant, incident quarantine, and per-service OOM behavior that is already survivable (ASR→Vosk; draw 500 without poison-kill; privacy 500 fail-open/fail-closed by consumer).
- **NVML vs allocator lag.** `expandable_segments:True` means freed-but-cached memory still shows as used; the ledger (not NVML) is the source of truth for admission, NVML only clamps downward and alarms on drift >1 GiB/30 s.
- **Heartbeat starvation under GIL.** Python-side inference loops can starve the heartbeat thread; TTL 30 s tolerates ~20 s gaps, and NVML pid-liveness prevents false reclaims of a живой process (reclaim requires missed heartbeats **and** pid absence, or TTL ×3).
- **Sustained full-GPU pressure opens the draw breaker** (D4): five 429s in a row → 30 s cooldown on `(aifarm-draw, model)`. Identical charging exists today via 300 s timeouts; the butler only makes it faster and cheaper. Half-open probes recover automatically.
- **RAM/swap pressure (swap 8/8 full).** The plan adds no RAM residents: privacy stays on GPU, draw keeps one slot, butler ~60 MB RSS. OD9's prewarm swaps slots but never holds two.
- **Disk 97 %.** Butler image is slim (~80 MB); no new model downloads; probe containers are `--rm` and campaign CSVs are deleted after registry commit.
- **Event-loop coupling in privacy-filter.** Vacate/restore handlers share the loop with redacts (existing design, `api/app.py:124-134`); a vacate during a long redact waits for it — bounded by the 30 s Discovery budget. No change to the existing (imperfect) concurrency model in this plan.
- **Version skew (torch 2.8/cu12.9 vs 2.10/cu13) and unpinned draw deps** (torch/diffusers/accelerate track git main — build-time drift re-triggers §2.1 re-measurement). The butler is out-of-process HTTP and immune to the skew itself.
- **Client drift across three vendored copies** — surfaced in `/v1/status.client_versions`; the file carries a single-source version constant.
- **Xorg growth on GPU1** — currently 4 MiB; watchdog's unknown-PID alarm fires if the display server ever starts compositing seriously.

---

## Appendix — verification commands used for this survey

```
ssh aifarm nvidia-smi --query-gpu=index,uuid,name,memory.total,memory.used --format=csv
ssh aifarm docker ps / docker inspect {draw-api,asr-api,privacy-filter}   # env, mounts, restart counts
ssh aifarm cat /home/wavecut/services/{draw,asr,privacy-filter}/compose.yaml
ssh aifarm grep -rIl 4060ti.lock /home/wavecut/services                   # lock users: exactly asr, draw, privacy(+tests)
ssh aifarm docker logs asr-api --since 72h | grep -c fallback_used=True   # 1469 (vs 1504 False)
ssh aifarm docker logs draw-api --since 72h | grep -c "POST /v1/..."      # 99 / 3140 / 0
ssh aifarm docker logs privacy-filter --since 24h | grep -c "POST /v1/redact"  # 60470
ssh aifarm sed -n 100,152p /home/wavecut/discovery/app/worker.py          # verbatim upstream passthrough
```

Local: `aifarm-asr`, `aifarm-draw`, `aifarm-privacy-filter` working trees and the OpenPlotva workspace, cited inline as `path:line`.
