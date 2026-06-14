# LLM trace coverage — restoring `llm_request_events` parity with go-plotva

Date: 2026-06-15
Status: approved (design), pending implementation
Branch: `llm-trace-coverage`

## Problem

The "LLM requests per day" dashboard (per-model stacked series) dropped ~2x at the
go-plotva (Go) → openplotva (Rust) cutover. Root-cause investigation (23-agent workflow +
manual verification) established the drop is a **measurement artifact**, not a workflow
reduction: the bot still makes the same model calls, but the new code records ~half the rows.

The dashboard is `COUNT(*)` of rows in the `llm_request_events` table, bucketed by time and
grouped by `model` (`crates/openplotva-app/src/runtime_llm_analytics.rs` `SQL_LLM_MODEL_SERIES_HOUR`
/ `SQL_LLM_MODELS`). The analytics SQL is byte-identical between Go and Rust, so the regression is
entirely on the **write/instrumentation** side. Two compounding causes:

1. **Auxiliary flows no longer counted.** Go traced at the low-level shared clients —
   `internal/geminiadapter/adapter.go` `Generate` (`AddAndEmit` at :136/:142/:290) and
   `internal/dialog/aifarm/client.go` `Client.Complete` (`finishChatCompletionTrace` at :1010) —
   so every model call counted, including memory extraction, history summary, and image/song
   prompt optimizers. Rust records only via `TracingChatProvider` wrapping the dialog provider
   (`crates/openplotva-app/src/runtime_llm.rs:237`; wired `lib.rs:8988`/`:9064`). The auxiliary
   flows were re-implemented as standalone untraced `reqwest` clients (`openplotva-memory`,
   `openplotva-history`, `GeminiMediaPromptOptimizer`/`AifarmStructuredJsonGenerator`) → 0 rows
   although they still run.

2. **Per-attempt → per-iteration granularity collapse on the main path.** Go traced *below* the
   aifarm priority pool: `PooledClient.Complete` calls `Client.Complete` once per backend
   (primary `pool.go:211` + each secondary `:264` + primary-wait `:283`), each emitting one row
   with that backend's source/model. Rust traces *above* the pool: `TracingChatProvider` wraps the
   whole `AifarmDialogProvider`; the entire pool resolution is one `self.client.complete()`
   (`aifarm.rs:2518`) → one trace per agentic iteration, recorded with `source=aifarm` and
   `model=primary` (`aifarm.rs:2987/2991`). The `vram.cloud/qwen3.6-27b` & `qwen3.6-35b-a3b`
   *secondary* series therefore exist only in Go-era data; in Rust secondary-served responses are
   folded into the primary model and vanish.

Not contributing (verified): the analytics query itself (identical); agent tool-iteration
granularity (preserved 1:1 in both, caps aifarm=8 / gemini=5); embeddings, vision captioning,
translation (t8r), shield/WhiteCircle moderation, AceStep music — all uncounted in **both** eras
(or written to separate tables such as `whitecircle_check_events`). Genuine workflow reduction is
negligible (only Go's thought-channel parse-failure text-only retry, an error path, is gone).

## Goal

Restore full Go-era parity of `llm_request_events` (decision: "both mechanisms, full fidelity"):
- Auxiliary flows (memory extraction, history summary, image/song prompt optimization, youtube
  summary) emit rows again.
- The aifarm pool is counted per attempt with the real per-backend model, so the
  `vram.cloud/qwen3.6-*` secondary series reappear and per-attempt rows return.
- Total rows/day return to the pre-migration level.

Forward-only: no DB migration, no backfill; the historical cliff in the chart remains. Deploy via
the standard production workflow once verification passes.

## Approach (chosen: A — single low-level trace sink, faithful to Go)

Emit at the lowest layer where a model round-trip actually happens — exactly like Go, which has no
provider-level trace and emits from `adapter.Generate` / `Client.Complete`. This yields per-attempt
+ per-backend + auxiliary coverage uniformly, with no double-count, because there is exactly one
emission layer.

Rejected alternatives:
- **B (additive: keep `TracingChatProvider`, bubble pool attempts up via `CompletionResult`, add a
  separate observer for aux).** Lower regression risk to the working dialog path, but leaves two
  emission mechanisms (provider-level for dialog, observer for aux), risks divergence, and loses
  failed-attempt usage (the pool discards non-winning bodies). Does not match the Go single-layer
  model the existing `*_like_go` tests are written against.
- **C (aux observer only + relabel served model, no per-attempt dialog rows).** Cheapest, but does
  not restore the per-attempt/qwen contribution — fails the chosen "full fidelity" requirement.

### Crate-boundary constraint

`openplotva-llm` is a domain crate and must not depend on `sqlx`/the recorder (AGENTS.md). The sink
is therefore a trait declared in `openplotva-llm` and implemented in `openplotva-app` — the direct
analogue of Go's `llmtrace.SetEventEnqueuer` / `EmitEvent` indirection.

## Design

### 1. New module `openplotva_llm::trace`

- `LlmCallContext { chat_id, thread_id, message_id, user_id, full_name, chat_title, flow, source,
  request_kind, mode, iteration }` — `Clone + Default`. The call-site identity Go carries via
  `llmtrace.WithMetadata(ctx, meta)`.
- `LlmCallRecord { context, provider, model, prompt_chars, prompt_messages, docs_chars,
  duration_ms, usage, timings, inference_params, raw_request, raw_response, error }` — neutral
  record; fields are a subset of `RuntimeLlmRequestData` / `DialogTraceArtifacts`.
- `trait LlmCallObserver: Send + Sync { fn observe(&self, record: LlmCallRecord); }`
- `set_observer(Arc<dyn LlmCallObserver>)` over a `OnceLock`, registered once at startup next to the
  recorder spawn. `observe()` is non-blocking (`try_send` into the existing mpsc) — no lock held
  across `.await`. No-op when unset, so `openplotva-llm` unit tests are unaffected.
- `record_completion(ctx, model, duration_ms, &result_or_error)` — internal helper that assembles an
  `LlmCallRecord` and forwards it to the registered observer.

  Decision/trade-off: a process-global observer (vs. constructor injection through ~5 clients) is
  chosen to mirror Go and keep the diff small. Documented as deliberate global state.

### 2. Context threading and emit points

Low-level completion methods take an explicit `ctx: &LlmCallContext` parameter (the json-discovery
variant takes a raw `Value`, so context cannot ride on a typed request struct — it must be a param):
- aifarm: the `complete*` family (`aifarm.rs:637/652/686/696/730`) and the `CompletionClient` trait
  (`aifarm.rs:3528`) + its impls and test mocks. Emit **once** at the terminal result (after
  `poll_discovery_result` / after the direct `transport.send` returns), on success **and** error.
  Status callbacks and per-poll ticks do **not** emit.
- gemini: the terminal `generateContent` / `send_request`. One emit per round-trip.

The pool (`aifarm.rs:1953`) passes the same `ctx` into each per-backend attempt; the model recorded
comes from the request actually sent to that backend (`direct_compatible_request(request,
&backend.model)`), so per-attempt + per-backend model labelling is automatic and the qwen secondary
series return.

`ctx` is populated where it is known: `run_with_status` (from `DialogInput`), `extract`,
`generate_document`, `optimize_image_prompt`, `optimize_song_prompt`.

Exactly-once invariant: one `LlmCallRecord` per model round-trip — never per poll tick, per status
callback, or per internal retry inside a single `complete*`.

### 3. Remove `TracingChatProvider` persistent emission (kill double-count)

Persistence moves to the low-level layer, so the provider-level emit must go:
- Remove `TracingChatProvider` and its two wrappers (`lib.rs:8988`, `:9064`); register the global
  observer at startup instead (near `lib.rs:8090`).
- New `RuntimeLlmObserver` (in `openplotva-app`) implements `LlmCallObserver`, converts
  `LlmCallRecord → RuntimeLlmRequestData` (generalise the existing `trace_from_dialog_*` into
  `trace_from_record`), and feeds **both** sinks: `RuntimeLlmTraceBuffer` (live preview — now sees
  all flows, not just dialog) and `PostgresRuntimeLlmEventRecorder`.
- `RuntimeLlmRequestData`, the recorder, the `llm_request_events` schema, and all analytics SQL are
  unchanged — only the source of the rows changes.

### 4. Tag parity

`flow` / `source` / `request_kind` must match the Go values, or the by-source/by-flow group-bys and
the provider normalization (`aifarm`, `aifarm+fallback:%`, `Gemini/GenKit`) in
`runtime_llm_analytics.rs` break:
- dialog → `flow=dialog`, `source=<provider>` (aifarm/nvidia/vmlx/genkit), `request_kind` =
  `openai.chat.completions` (aifarm) / `gemini.generateContent` (gemini).
- memory → `flow=memory_extraction`, `source=aifarm_memory` (aifarm) / genkit source (gemini).
- history → `flow=history_summary`.
- image/song optimizers → `flow=optimize_prompt` / `optimize_edit_prompt` / song-reprompt — exact Go
  strings to be confirmed from go-plotva during implementation.

### 5. Edge cases and the "do not touch" list

- Errors are counted (Go counts them). Fallback aifarm→genkit now emits both the failed primary
  attempts and the fallback success — this also fixes the small under-count noted in the RCA.
- **Do not** add emit for: embeddings, vision captioning, translation (t8r), WhiteCircle moderation
  (its own `whitecircle_check_events` table), AceStep music. These were uncounted in Go too; adding
  them would over-count.

## Testing (TDD)

- Pool of K backends → observer receives K records, each with its backend's model (key new test).
- aux `extract` / `generate_document` / `optimize_*` → one record each with correct `flow`/`source`.
- Dialog of N iterations → N records (per-iteration parity preserved, now via the observer).
- Error path → record with `error` set.
- `LlmCallRecord → RuntimeLlmRequestData` conversion shape (replaces current dialog-shape tests).
- Anti-double-count: a dialog turn does not emit twice.
- Commands: `cargo fmt --all`; `cargo test -p openplotva-llm`; `cargo test -p openplotva-app`;
  then `tools/update-queue-smoke.sh` / `tools/local-smoke.sh`, and verify via the runtime GraphQL
  `llmRequests` plus analytics by source/model.

## Rollout

Forward-only; no DB migration. Deploy with `gh workflow run deploy-production.yml --ref main` after
verification. Post-deploy check: `llm_analytics` again contains `flow=memory_extraction /
history_summary / optimize_*` and the `vram.cloud/qwen3.6-*` models; the daily COUNT returns to the
pre-migration level.

## Risks (accepted)

- Wide signature change to the `complete*` family / `CompletionClient` trait and removal of the
  tested `TracingChatProvider` — mitigated by TDD and the invariance of `RuntimeLlmRequestData` and
  the analytics SQL.
- Global observer (process state) instead of DI — accepted to match Go and minimise the diff.
