# LLM trace coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore Go-era `llm_request_events` coverage so the per-model dashboard returns to its pre-migration volume — count every model round-trip (dialog, auxiliary flows, and each aifarm pool attempt with its real backend model).

**Architecture:** Move trace emission from the high-level `TracingChatProvider` down to the low-level model clients in `openplotva-llm` (faithful to Go, which traced at `geminiadapter.Generate` / aifarm `Client.Complete`). A new `openplotva_llm::trace` module exposes an injectable observer trait; the low-level aifarm `complete*` family and the gemini terminal emit one `DialogTraceArtifacts`-backed record per round-trip, carrying an `LlmCallContext` for caller identity/flow. `openplotva-app` implements the observer, converts records to the unchanged `RuntimeLlmRequestData`, and feeds the existing ring buffer + Postgres recorder. `TracingChatProvider`'s persistence is removed to avoid double counting.

**Tech Stack:** Rust, tokio, sqlx (app only), existing `openplotva-dialog::DialogTraceArtifacts` as the record payload, existing `PostgresRuntimeLlmEventRecorder` + `RuntimeLlmTraceBuffer`.

**Reference spec:** `docs/superpowers/specs/2026-06-15-llm-trace-coverage-design.md`

**Key anchors (verify before editing):**
- `crates/openplotva-llm/src/aifarm.rs`: `CompletionResult` (:243), `AifarmHttpClient::complete` family (:637/:652/:686/:696/:730), `poll_discovery_result` (:987), `CompletionClient` trait (:3528), `PooledClient::complete` (:~3601), `AifarmHttpPoolClient::complete` (:1953) + `try_secondary_backends` (:2013), `run_with_status` (:2499), `aifarm_dialog_trace_artifacts` (:2977), trace helpers `aifarm_trace_usage/timings/inference_params/raw_response/transport` (:3035-3215), aux: `optimize_image_prompt` (:1715), `optimize_song_prompt` (:1783), `generate_document` (:2071/:2394), `extract` (:2143/:2270), `FakeCompletionClient` impl (:5866).
- `crates/openplotva-llm/src/gemini.rs`: dialog loop (:492-560), low-level `generateContent`/`send_request` (~:2576), aux `optimize_image_prompt` (:1361), `optimize_song_prompt` (:1401), `extract` (:1694), `generate_document` (:1902).
- `crates/openplotva-dialog/src/history.rs`: `DialogTraceArtifacts` (:286), `DialogTraceUsage`.
- `crates/openplotva-app/src/runtime_llm.rs`: `RuntimeLlmTraceBuffer` (:74), `PostgresRuntimeLlmEventRecorder` (:157), `TracingChatProvider` (:214), `trace_from_dialog_base` (:779), `apply_dialog_trace_artifact` (:695), conversion helpers.
- `crates/openplotva-app/src/lib.rs`: recorder spawn (:8090), `TracingChatProvider` wiring (:8988, :9064).
- `crates/openplotva-app/src/runtime_llm_analytics.rs`: analytics SQL (unchanged; used only for post-deploy verification).

---

## Task 0: Confirm exact Go flow/source/request_kind tag strings

**Files:** none (read-only investigation; record findings in this task's notes below).

- [ ] **Step 1: Extract the tag strings Go writes per flow**

Run:
```bash
cd /Users/Shared/src/github.com/iamwavecut/go-plotva
grep -rn 'Flow:\|Source:\|RequestKind:\|Flow =\|Source =' internal/dialog/aifarm internal/genkit | grep -iE 'memory|history|optimize|edit|song|dialog|aifarm'
```
Expected: the literal `flow`/`source`/`request_kind` values for memory extraction, history summary, image/edit/song optimizers, and dialog. Record them here as the authoritative tag table:

| flow | source (aifarm) | source (gemini/genkit) | request_kind |
|------|-----------------|------------------------|--------------|
| dialog | `aifarm` | `genkit` | `openai.chat.completions` / `gemini.generateContent` |
| memory_extraction | _fill from grep_ | _fill_ | _fill_ |
| history_summary | _fill_ | _fill_ | _fill_ |
| optimize_prompt | _fill_ | _fill_ | _fill_ |
| optimize_edit_prompt | _fill_ | _fill_ | _fill_ |
| song reprompt | _fill_ | _fill_ | _fill_ |

- [ ] **Step 2: Cross-check against Rust analytics normalization**

Run:
```bash
cd /Users/Shared/src/github.com/iamwavecut/openplotva
grep -n "aifarm+fallback\|chat_flow_\|genkit\|aifarm_memory\|aifarm'" crates/openplotva-app/src/runtime_llm_analytics.rs
```
Expected: confirm the `source` values chosen in Step 1 fall through the normalization `CASE` correctly (so providers render as expected). No code change; this gates the tag constants used in Task 2.

---

## Task 1: New `openplotva_llm::trace` module (observer + context + global)

**Files:**
- Create: `crates/openplotva-llm/src/trace.rs`
- Modify: `crates/openplotva-llm/src/lib.rs` (add `pub mod trace;` and re-exports)
- Test: inline `#[cfg(test)]` in `crates/openplotva-llm/src/trace.rs`

- [ ] **Step 1: Write the failing test**

In `crates/openplotva-llm/src/trace.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use openplotva_dialog::DialogTraceArtifacts;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct CollectingObserver(Arc<Mutex<Vec<LlmCallRecord>>>);
    impl LlmCallObserver for CollectingObserver {
        fn observe(&self, record: LlmCallRecord) {
            self.0.lock().unwrap().push(record);
        }
    }

    #[test]
    fn observe_forwards_to_registered_observer() {
        let sink = Arc::new(Mutex::new(Vec::new()));
        let observer = LlmCallTraceRegistry::new();
        observer.set(Arc::new(CollectingObserver(Arc::clone(&sink))));
        observer.observe(LlmCallRecord {
            context: LlmCallContext { chat_id: -100, user_id: 7, ..LlmCallContext::default() },
            artifact: DialogTraceArtifacts { flow: "memory_extraction".into(), model: "Gemma".into(), ..Default::default() },
        });
        let got = sink.lock().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].context.chat_id, -100);
        assert_eq!(got[0].artifact.flow, "memory_extraction");
    }

    #[test]
    fn observe_without_observer_is_noop() {
        let registry = LlmCallTraceRegistry::new();
        registry.observe(LlmCallRecord::default()); // must not panic
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p openplotva-llm trace::tests -v`
Expected: FAIL (compile error — `LlmCallTraceRegistry`, `LlmCallRecord`, `LlmCallContext`, `LlmCallObserver` undefined).

- [ ] **Step 3: Write the module**

In `crates/openplotva-llm/src/trace.rs`:
```rust
//! Low-level LLM call trace sink. Mirrors go-plotva `llmtrace`: an injectable observer
//! that every model round-trip reports to, so dialog, auxiliary flows, and each pool
//! attempt are counted at the layer where the call actually happens.

use std::sync::{Arc, OnceLock};

use openplotva_dialog::DialogTraceArtifacts;

/// Caller identity + routing for a single model round-trip. Supplies the fields the
/// low-level client cannot know (chat/user/message) plus flow/source overrides.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LlmCallContext {
    pub chat_id: i64,
    pub thread_id: Option<i32>,
    pub chat_title: String,
    pub user_id: i64,
    pub full_name: String,
    pub message_id: i32,
}

/// One model round-trip observation: identity context + the existing trace artifact
/// (provider/source/flow/model/usage/timings/inference_params/error/sizes).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LlmCallRecord {
    pub context: LlmCallContext,
    pub artifact: DialogTraceArtifacts,
}

/// Sink for low-level model-call observations. Implemented in `openplotva-app`.
pub trait LlmCallObserver: Send + Sync {
    fn observe(&self, record: LlmCallRecord);
}

/// Injectable registry holding the process-wide observer. A type (not free fns) so tests
/// get isolated instances; production uses the `global()` singleton.
#[derive(Default)]
pub struct LlmCallTraceRegistry {
    observer: OnceLock<Arc<dyn LlmCallObserver>>,
}

impl LlmCallTraceRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self { observer: OnceLock::new() }
    }

    /// Register the observer once. Subsequent calls are ignored (returns false).
    pub fn set(&self, observer: Arc<dyn LlmCallObserver>) -> bool {
        self.observer.set(observer).is_ok()
    }

    /// Forward a record if an observer is registered; no-op otherwise.
    pub fn observe(&self, record: LlmCallRecord) {
        if let Some(observer) = self.observer.get() {
            observer.observe(record);
        }
    }
}

static GLOBAL: OnceLock<LlmCallTraceRegistry> = OnceLock::new();

fn global() -> &'static LlmCallTraceRegistry {
    GLOBAL.get_or_init(LlmCallTraceRegistry::new)
}

/// Register the process-wide observer once (analogue of Go `llmtrace.SetEventEnqueuer`).
pub fn set_observer(observer: Arc<dyn LlmCallObserver>) -> bool {
    global().set(observer)
}

/// Report a model round-trip to the process-wide observer (analogue of Go `EmitEvent`).
pub fn observe(record: LlmCallRecord) {
    global().observe(record);
}
```

In `crates/openplotva-llm/src/lib.rs` add near the other `pub mod`/`pub use` lines:
```rust
pub mod trace;
pub use trace::{LlmCallContext, LlmCallObserver, LlmCallRecord};
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p openplotva-llm trace::tests -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/openplotva-llm/src/trace.rs crates/openplotva-llm/src/lib.rs
git commit -m "Add low-level LLM call trace sink to openplotva-llm"
```

---

## Task 2: Generalize the aifarm trace-artifact builder for any flow

**Files:**
- Modify: `crates/openplotva-llm/src/aifarm.rs` (`aifarm_dialog_trace_artifacts` :2977 and add `aifarm_call_trace_artifacts`)
- Test: inline `#[cfg(test)]` in `crates/openplotva-llm/src/aifarm.rs`

The existing `aifarm_dialog_trace_artifacts` hardcodes `flow="dialog"`, `mode="tools"`, `request_kind="openai.chat.completions"`, `source=provider`, and takes `&DialogInput` only for `docs_chars`. Introduce a flow-parameterized builder so memory/history/optimizer calls produce correctly-tagged artifacts; keep the dialog one as a thin wrapper.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn aifarm_call_trace_artifacts_tags_flow_and_model() {
    let request = ChatCompletionRequest { model: "vram.cloud/qwen3.6-27b".into(), ..Default::default() };
    let result = CompletionResult::default();
    let artifact = aifarm_call_trace_artifacts(
        &request, &result, 0,
        TraceTags { provider: "aifarm", source: "aifarm_memory", flow: "memory_extraction",
                    mode: "json", request_kind: "openai.chat.completions", iteration: 1 },
    );
    assert_eq!(artifact.flow, "memory_extraction");
    assert_eq!(artifact.source, "aifarm_memory");
    assert_eq!(artifact.model, "vram.cloud/qwen3.6-27b");
    assert_eq!(artifact.request_kind, "openai.chat.completions");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p openplotva-llm aifarm_call_trace_artifacts_tags_flow_and_model -v`
Expected: FAIL (compile error — `aifarm_call_trace_artifacts`, `TraceTags` undefined).

- [ ] **Step 3: Implement the generalized builder**

In `crates/openplotva-llm/src/aifarm.rs`, add:
```rust
/// Static routing tags for a low-level aifarm call trace.
#[derive(Clone, Copy)]
pub(crate) struct TraceTags<'a> {
    pub provider: &'a str,
    pub source: &'a str,
    pub flow: &'a str,
    pub mode: &'a str,
    pub request_kind: &'a str,
    pub iteration: usize,
}

/// Build a trace artifact for any aifarm completion (dialog or auxiliary).
/// `docs_chars` is passed explicitly (0 for non-dialog flows).
pub(crate) fn aifarm_call_trace_artifacts(
    request: &ChatCompletionRequest,
    result: &CompletionResult,
    docs_chars: i32,
    tags: TraceTags<'_>,
) -> DialogTraceArtifacts {
    DialogTraceArtifacts {
        provider: tags.provider.trim().to_owned(),
        request_kind: tags.request_kind.to_owned(),
        source: tags.source.trim().to_owned(),
        mode: tags.mode.to_owned(),
        flow: tags.flow.to_owned(),
        iteration: i32::try_from(tags.iteration).unwrap_or(i32::MAX),
        model: request.model.trim().to_owned(),
        raw_request: serde_json::to_value(request).ok(),
        raw_response: aifarm_trace_raw_response(result),
        transport: aifarm_trace_transport(result),
        inference_params: aifarm_trace_inference_params(request),
        usage: result.response.as_ref().and_then(aifarm_trace_usage),
        timings: result.response.as_ref().and_then(aifarm_trace_timings),
        prompt_chars: json_size(&request.messages),
        prompt_messages: i32::try_from(request.messages.len()).unwrap_or(i32::MAX),
        docs_chars,
        ..DialogTraceArtifacts::default()
    }
}
```
Then rewrite `aifarm_dialog_trace_artifacts` (:2977) to delegate:
```rust
fn aifarm_dialog_trace_artifacts(
    request: &ChatCompletionRequest,
    result: &CompletionResult,
    input: &DialogInput,
    provider: &str,
    iteration: usize,
) -> DialogTraceArtifacts {
    let docs_chars = input.reference_context.iter().map(String::len).sum::<usize>().min(i32::MAX as usize) as i32;
    aifarm_call_trace_artifacts(request, result, docs_chars, TraceTags {
        provider, source: provider, flow: "dialog", mode: "tools",
        request_kind: "openai.chat.completions", iteration,
    })
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p openplotva-llm aifarm_call_trace_artifacts_tags_flow_and_model -v`
Then: `cargo test -p openplotva-llm aifarm -v` (existing dialog-artifact tests still pass — delegation is behaviour-preserving).
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/openplotva-llm/src/aifarm.rs
git commit -m "Generalize aifarm trace-artifact builder for any flow"
```

---

## Task 3: Thread `LlmCallContext` and emit at the aifarm low-level

**Files:**
- Modify: `crates/openplotva-llm/src/aifarm.rs` (`CompletionClient` trait :3528 + impls; `AifarmHttpClient::complete*` :637-760 terminal; `PooledClient`/`AifarmHttpPoolClient` :1953/:3601; `FakeCompletionClient` :5866; `run_with_status` :2499 call sites)
- Test: inline `#[cfg(test)]`

This is the core change. The `complete*` family gains `ctx: &LlmCallContext`; each terminal (after `poll_discovery_result` / after the direct `transport.send`) emits exactly one record via `trace::observe`, on success and error. The pool passes the same `ctx` to each backend attempt, so per-attempt + per-backend model fall out automatically (the request handed to each backend already carries that backend's model).

> Atomicity note: changing the `CompletionClient` trait signature and all impls must land in one task to keep the crate compiling. TDD is applied via the observer-count test below; intermediate steps will not compile until Step 3 is complete.

- [ ] **Step 1: Write the failing test (per-attempt, per-backend emission)**

```rust
#[tokio::test]
async fn pool_emits_one_record_per_backend_attempt() {
    // primary fails retryably, secondary succeeds -> 2 records, distinct models
    let sink = std::sync::Arc::new(std::sync::Mutex::new(Vec::<LlmCallRecord>::new()));
    let registry = crate::trace::LlmCallTraceRegistry::new();
    registry.set(std::sync::Arc::new(super::tests_support::Collecting(sink.clone())));

    let primary = Box::new(FakeCompletionClient::failing(FailureReason::CapacityUnavailable));
    let secondary = PooledBackend::new("sec", "vram.cloud/qwen3.6-27b",
        Box::new(FakeCompletionClient::ok("vram.cloud/qwen3.6-27b")));
    let mut pool = PooledClient::new(primary, vec![secondary]);

    let ctx = LlmCallContext { chat_id: -100, user_id: 7, ..Default::default() };
    let mut noop = |_| {};
    let _ = pool.complete_traced(
        ChatCompletionRequest { model: "Gemma".into(), ..Default::default() },
        &ctx, &mut noop, &registry,
    );

    let got = sink.lock().unwrap();
    assert_eq!(got.len(), 2, "one row per attempt");
    assert!(got.iter().any(|r| r.artifact.model == "Gemma" && !r.artifact.error.is_empty()));
    assert!(got.iter().any(|r| r.artifact.model == "vram.cloud/qwen3.6-27b" && r.artifact.error.is_empty()));
}
```
(Implementation note: because the production global registry is awkward to assert against, the low-level clients take the registry via the threaded path. Production passes `crate::trace::global_registry()`; tests pass a local one. Add a `complete_traced` wrapper or thread `&LlmCallTraceRegistry`. Choose the minimal seam: thread `&'a LlmCallTraceRegistry` alongside `ctx`. Adjust `trace.rs` to expose `pub fn global_registry() -> &'static LlmCallTraceRegistry`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p openplotva-llm pool_emits_one_record_per_backend_attempt -v`
Expected: FAIL (compile error — new params/methods undefined).

- [ ] **Step 3: Implement threading + emit**

3a. In `trace.rs` expose the global registry: `pub fn global_registry() -> &'static LlmCallTraceRegistry { global() }`.

3b. Change the `CompletionClient` trait (:3528):
```rust
pub trait CompletionClient {
    fn complete(
        &mut self,
        request: ChatCompletionRequest,
        ctx: &crate::trace::LlmCallContext,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
        registry: &crate::trace::LlmCallTraceRegistry,
    ) -> Result<CompletionResult, CompletionError>;
}
```

3c. In `AifarmHttpClient`, thread `ctx` + `registry` through `complete`, `complete_discovery_with_job_id`, `complete_json_discovery*`, `complete_direct_with_job_id`. At each terminal — the success/error return of `poll_discovery_result` and of the direct `transport.send` — call:
```rust
let artifact = aifarm_call_trace_artifacts(&request, &result_or_default, 0, tags_from_ctx_or_default);
registry.observe(LlmCallRecord { context: ctx.clone(), artifact });
```
For the chat path the tags come from the caller (dialog/aux). For the json-discovery path (`Value` request), build the artifact via a `Value`-aware variant (model read from the JSON `model` field). Emit once per call; on error set `artifact.error = err.to_string()` and emit before returning `Err`.

3d. In `PooledClient::complete` and `AifarmHttpPoolClient::complete`, add `ctx` + `registry` params and pass them into every `self.primary.complete(...)`, `backend.client.complete(...)`, and `primary_wait.complete(...)`. Do NOT emit at the pool level — emission already happens inside each backend's `complete`.

3e. Update `run_with_status` (:2499): build `ctx` once from `state.input` (chat/user/message/title), pass into `self.client.complete(request, &ctx, on_status, crate::trace::global_registry())`. Keep building `state.trace_events` (still used for `DialogTraceError`), but these no longer drive recording.

3f. Update `FakeCompletionClient` (:5866) and any other `CompletionClient` impl/mocks to the new signature. Add `tests_support::Collecting` observer + `FakeCompletionClient::failing/ok` helpers used by the test.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p openplotva-llm pool_emits_one_record_per_backend_attempt -v`
Then: `cargo test -p openplotva-llm aifarm -v`
Expected: PASS; existing aifarm tests still pass.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/openplotva-llm/src/aifarm.rs crates/openplotva-llm/src/trace.rs
git commit -m "Emit per-attempt LLM trace records at the aifarm low-level"
```

---

## Task 4: Thread context + emit at the gemini low-level

**Files:**
- Modify: `crates/openplotva-llm/src/gemini.rs` (dialog loop :492-560, terminal `generateContent`/`send_request` ~:2576)
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn gemini_dialog_emits_one_record_per_iteration() {
    // 2 iterations (tool call then final) -> 2 records, flow=dialog, source=genkit/gemini
    // use the existing gemini test transport/mocks; register a Collecting observer registry
    // assert records.len()==2 and iteration fields 1,2.
}
```
(Use the same `Collecting` observer pattern; thread the test registry into the gemini client like Task 3.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p openplotva-llm gemini_dialog_emits_one_record_per_iteration -v`
Expected: FAIL.

- [ ] **Step 3: Implement**

Mirror Task 3 in gemini: thread `ctx: &LlmCallContext` + `registry` to the terminal `generateContent`/`send_request`; build a gemini trace artifact (reuse the existing gemini trace-artifact builder — locate it near the dialog loop; it already sets `request_kind="gemini.generateContent"`, `provider`/`source`) generalized for flow; emit once per round-trip (success + error). Build `ctx` in the gemini `run_dialog` from `DialogInput`. Stop relying on `trace_events` for recording.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p openplotva-llm gemini -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/openplotva-llm/src/gemini.rs
git commit -m "Emit per-round-trip LLM trace records at the gemini low-level"
```

---

## Task 5: Tag and context for auxiliary flows (memory, history, optimizers)

**Files:**
- Modify: `crates/openplotva-llm/src/aifarm.rs` aux methods `extract` (:2143), `generate_document` (:2071/:2394), `optimize_image_prompt` (:1715), `optimize_song_prompt` (:1783)
- Modify: `crates/openplotva-llm/src/gemini.rs` aux methods `extract` (:1694), `generate_document` (:1902), `optimize_image_prompt` (:1361), `optimize_song_prompt` (:1401)
- Test: inline `#[cfg(test)]`

These already call the low-level `complete*`/`generateContent` (now emitting). This task supplies the correct `LlmCallContext` (chat/user from the aux input + the Task-0 flow/source/request_kind tags) at each aux call site so the emitted rows are tagged like Go.

- [ ] **Step 1: Write the failing test (memory extraction emits a tagged record)**

```rust
#[tokio::test]
async fn aifarm_memory_extract_emits_tagged_record() {
    // run AifarmMemoryExtractor.extract against a fake transport with a Collecting registry
    // assert 1 record, flow == "memory_extraction", source == "aifarm_memory" (per Task 0 table)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p openplotva-llm aifarm_memory_extract_emits_tagged_record -v`
Expected: FAIL.

- [ ] **Step 3: Implement**

For each aux method, build an `LlmCallContext` from its input (chat/user/message where available; zeros otherwise) and pass the flow/source/mode/request_kind tags from the Task-0 table into the low-level call (chat path → `TraceTags`; json path → the `Value`-aware builder with the same tags). Each aux flow maps to: memory→`memory_extraction`, history→`history_summary`, image→`optimize_prompt`, edit→`optimize_edit_prompt`, song→song reprompt flow.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p openplotva-llm -v`
Expected: PASS (whole crate green).

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/openplotva-llm/src/aifarm.rs crates/openplotva-llm/src/gemini.rs
git commit -m "Tag auxiliary LLM flows for trace coverage parity"
```

---

## Task 6: App-side observer; remove `TracingChatProvider` persistence; wire startup

**Files:**
- Modify: `crates/openplotva-app/src/runtime_llm.rs` (add `RuntimeLlmObserver`; add `trace_from_record`; strip persistence from `TracingChatProvider` or delete it)
- Modify: `crates/openplotva-app/src/lib.rs` (register observer at :~8090; remove `TracingChatProvider` wrapping at :8988/:9064)
- Test: inline `#[cfg(test)]` in `runtime_llm.rs`

- [ ] **Step 1: Write the failing test (record → row conversion + dual sink)**

```rust
#[test]
fn runtime_observer_converts_record_to_row_and_buffers() {
    let buffer = RuntimeLlmTraceBuffer::new(8);
    let observer = RuntimeLlmObserver::new(buffer.clone(), None); // None recorder = buffer only
    observer.observe(openplotva_llm::LlmCallRecord {
        context: openplotva_llm::LlmCallContext { chat_id: -100, user_id: 7, message_id: 77, ..Default::default() },
        artifact: openplotva_dialog::DialogTraceArtifacts {
            provider: "aifarm".into(), source: "aifarm_memory".into(), flow: "memory_extraction".into(),
            model: "Gemma".into(), request_kind: "openai.chat.completions".into(), ..Default::default()
        },
    });
    let rows = buffer.llm_requests(RuntimeLlmRequestsFilter { limit: 10, ..Default::default() }).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].chat.chat_id, -100);
    assert_eq!(rows[0].source, "aifarm_memory");
    assert_eq!(rows[0].request_kind.as_deref(), Some("memory_extraction").map(|_| "openai.chat.completions").unwrap());
    assert_eq!(rows[0].model.as_deref(), Some("Gemma"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p openplotva-app runtime_observer_converts_record_to_row_and_buffers -v`
Expected: FAIL (`RuntimeLlmObserver` undefined).

- [ ] **Step 3: Implement**

3a. Add `trace_from_record(ctx, artifact) -> RuntimeLlmRequestData`: seed identity from `ctx` (reuse the field mapping in `trace_from_dialog_base` :779 — chat/user/message/title), then `apply_dialog_trace_artifact(&mut trace, &artifact)` (:695) to layer provider/source/flow/model/usage/timings/inference_params/error/sizes; set `result.response_text_preview`/`error` from the artifact.

3b. Add `RuntimeLlmObserver { buffer: RuntimeLlmTraceBuffer, recorder: Option<PostgresRuntimeLlmEventRecorder> }` implementing `openplotva_llm::LlmCallObserver`:
```rust
impl openplotva_llm::LlmCallObserver for RuntimeLlmObserver {
    fn observe(&self, record: openplotva_llm::LlmCallRecord) {
        let trace = trace_from_record(&record.context, &record.artifact);
        self.buffer.record(trace.clone());
        if let Some(recorder) = &self.recorder {
            recorder.enqueue(trace);
        }
    }
}
```
(Make `PostgresRuntimeLlmEventRecorder::enqueue` reachable — it already exists at :175; expose `pub(crate)` if needed.)

3c. Remove `TracingChatProvider`'s persistence: delete the struct + its `record_dialog_*` call path (or reduce it to a passthrough) and the two wrap sites at `lib.rs:8988`/`:9064` (use the inner `dialog_provider` directly). Keep `trace_from_dialog_*` only if still referenced; otherwise fold into `trace_from_record`.

3d. At `lib.rs:~8090`, after spawning the recorder + creating the buffer, register the observer:
```rust
let observer = std::sync::Arc::new(runtime_llm::RuntimeLlmObserver::new(
    llm_trace_buffer.clone(), Some(llm_event_recorder.clone()),
));
openplotva_llm::trace::set_observer(observer);
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p openplotva-app runtime_observer_converts_record_to_row_and_buffers -v`
Then: `cargo test -p openplotva-app runtime_llm -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/openplotva-app/src/runtime_llm.rs crates/openplotva-app/src/lib.rs
git commit -m "Route LLM trace records through app observer; drop provider-level persistence"
```

---

## Task 7: Anti-double-count + full-suite verification

**Files:** test-only + verification

- [ ] **Step 1: Anti-double-count test**

In `crates/openplotva-app` add a test (or extend an existing dialog test) asserting that running one dialog turn through the now-unwrapped provider produces rows ONLY via the observer (the buffer count equals the number of low-level round-trips, not 2×). If a focused integration test is impractical, assert structurally that `TracingChatProvider` no longer references the recorder/buffer (grep test or removal).

- [ ] **Step 2: Run the targeted crates**

Run:
```bash
cargo fmt --all
cargo test -p openplotva-llm
cargo test -p openplotva-app
```
Expected: all green.

- [ ] **Step 3: Clippy + workspace build**

Run: `cargo clippy --all-targets -- -D warnings && cargo build`
Expected: clean.

- [ ] **Step 4: Runtime smoke**

Run (whichever are wired locally): `tools/local-smoke.sh` and/or `tools/update-queue-smoke.sh`.
Expected: dialog produces rows; trigger a memory/history/optimizer path and confirm new rows appear (inspect via the runtime GraphQL `llmRequests` or the admin live buffer). Record results.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "Add anti-double-count test for LLM trace coverage"
```

---

## Task 8: Deploy and verify in production (only after the user confirms results)

- [ ] **Step 1:** Merge `llm-trace-coverage` into `main` (fast-forward or PR per the user's preference).
- [ ] **Step 2:** `gh workflow run deploy-production.yml --ref main`; watch the run to success.
- [ ] **Step 3:** Verify on prod via the runtime GraphQL API: `llm_analytics` again shows `flow=memory_extraction / history_summary / optimize_*` and models `vram.cloud/qwen3.6-27b` & `qwen3.6-35b-a3b`; the per-day COUNT returns toward the pre-migration level. Compare a 24h window before/after.

---

## Self-review notes

- **Spec coverage:** trace module (Task 1) ✓; per-attempt/per-backend pool (Task 3) ✓; aux flows (Task 5) ✓; remove double-count / move to low level (Task 6) ✓; tag parity (Task 0 + 5) ✓; do-not-touch list (no emit added to embeddings/vision/t8r/whitecircle/acestep — none of those code paths are modified) ✓; testing (Tasks 1-7) ✓; rollout (Task 8) ✓.
- **Open confirmations during execution:** exact gemini trace-artifact builder name (Task 4 Step 3) and exact Go tag strings (Task 0) are resolved by reading code at execution time; both have concrete anchors. The `complete_json_discovery` `Value`-request path needs a model-from-JSON artifact variant (noted in Task 3 Step 3c / Task 5 Step 3).
- **Type consistency:** `LlmCallContext`/`LlmCallRecord`/`LlmCallObserver`/`LlmCallTraceRegistry` names are used consistently across Tasks 1, 3, 6; `TraceTags`/`aifarm_call_trace_artifacts` across Tasks 2, 3, 5; `RuntimeLlmObserver`/`trace_from_record` across Task 6.
