# LLM Dialogs Context X-ray Implementation Plan

> **For agentic workers:** implement task-by-task; each task ends with an independently testable deliverable and a commit. Steps use `- [ ]` tracking.

**Goal:** When an admin opens a dialog run, show an in-memory "Context X-ray" of that turn — a weighted graph of the memories that were recalled, plus the active persona and applied chat settings.

**Architecture:** Capture a light `TurnContextArtifact` at `materialize` time (where the scored `RetrievedMemory` still exists, before it is flattened to `reference_context` text), carry it on `DialogInput` as skip-serialized metadata, and record it onto the open `RunRecord` in the existing in-memory `RuntimeLlmRunBuffer` via the run scope. Extend the detail endpoint to return it; render it in the LLM Dialogs drawer, reusing `pl-graph`, the memory one-hop endpoint, and the memory card drawer.

**Tech Stack:** Rust (openplotva-dialog, openplotva-app), sqlx-free (in-memory only), Chart-free graph via the existing `pl-graph` custom element, admin single-file JS.

## Global Constraints

- In-memory only. No DB table, no migration. Artifact lives on the 512-entry run ring and dies on restart.
- The artifact is capture-only and MUST NOT be serialized into the LLM request (skip-serialized on `DialogInput`).
- Plain-data artifact types live in `openplotva-dialog` (no `openplotva-memory`/app type coupling; the app maps `Card` → the plain snapshot).
- `web/admin/` design system: `pl-*` + tokens only; no raw controls / inline handlers / color literals / native dialogs; loading/empty/error states; update the `sha256` in `crates/openplotva-web/src/lib.rs` and run `cargo test -p openplotva-web`; run `openplotva-design-system-review`.
- Delivery per AGENTS.md: `feat/llm-dialogs-context-xray` branch → PR → watch CI + reviews → merge → deploy → verify.
- `cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace`.

---

### Task 1: Artifact type + capture in materialize

**Files:**
- Modify: `crates/openplotva-dialog/src/history.rs` (add `TurnContextArtifact`, `CapturedMemory`, `PersonaSnapshot`, `SettingKv`; add `#[serde(skip)] context_capture` to `DialogInput`).
- Modify: `crates/openplotva-app/src/dialog_jobs/input.rs` (build the artifact in `load_reference_context` + `materialize`).

**Interfaces — Produces:**
```rust
// openplotva-dialog
pub struct CapturedMemory { pub card_id: i64, pub salience: f64, pub confidence: f64,
    pub card_type: String, pub competing: bool, pub preview: String }
pub struct PersonaSnapshot { pub name: String, pub mood: String, pub custom: bool, pub profanity: bool }
pub struct SettingKv { pub label: String, pub value: String }
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TurnContextArtifact {
    pub memories: Vec<CapturedMemory>, pub persona: Option<PersonaSnapshot>,
    pub settings: Vec<SettingKv>, pub history_len: i32, pub tools_offered: Vec<String>,
    pub shield_on: bool, pub reference_context_chars: i32,
}
// DialogInput gains: #[serde(skip)] pub context_capture: Option<TurnContextArtifact>
```

- [ ] **Step 1: Failing test** — in `openplotva-app/src/dialog_jobs/input.rs` tests, assert that a materialized input over a stub memory store whose recall returns two `Card`s (one `competing`) yields `context_capture` with 2 `memories` (capped ≤40), `salience` copied, `preview` = trimmed `fact_text` (≤120 chars), and `persona`/`settings` populated. Run: `cargo test -p openplotva-app --lib dialog_jobs::input`. Expected: FAIL (field/mapping absent).
- [ ] **Step 2: Types** — add the structs + `context_capture` field (derive `Serialize, Deserialize` with `#[serde(skip)]` on the field so it never reaches the LLM JSON). `cargo build -p openplotva-dialog`.
- [ ] **Step 3: Capture** — change `load_reference_context` to also return the recalled `Vec<CapturedMemory>` (map `memory.cards`: `card_id=id`, `salience`, `confidence`, `card_type=format!("{:?}",card_type).to_lowercase()`, `competing = status==Competing` (or the dispute flag), `preview = cap_chars(fact_text,120)`, cap the list at 40). In `materialize`, assemble `TurnContextArtifact { memories, persona: PersonaSnapshot from input.persona, settings: label/value from the applied chat settings, history_len = input.history.len(), tools_offered from the turn's tool set, shield_on = !input.shield_context.is_empty(), reference_context_chars = reference text length }` and set `input.context_capture = Some(...)`.
- [ ] **Step 4: Pass** — `cargo test -p openplotva-app --lib dialog_jobs::input`. Expected: PASS. `cargo fmt --all`.
- [ ] **Step 5: Commit** — `feat: capture a per-turn context artifact during dialog materialization`.

### Task 2: Record onto the run + expose in the detail endpoint

**Files:**
- Modify: `crates/openplotva-app/src/runtime_llm_runs.rs` (`RunRecord.context: Option<openplotva_dialog::TurnContextArtifact>`; `record_context(run_id, artifact)`; include in `detail_json`).
- Modify: `crates/openplotva-app/src/dialog_turn/engine.rs` (after `materialize_dialog_input`, record the artifact via `ctx.llm_runs`).
- Modify: `crates/openplotva-app/src/runtime_virtual_dialog.rs` (console path records it too).

**Interfaces — Consumes:** `TurnContextArtifact` from Task 1.
**Produces:** `RuntimeLlmRunBuffer::record_context(&self, run_id: &str, artifact: TurnContextArtifact)`; detail JSON gains `"context"`.

- [ ] **Step 1: Failing test** — in `runtime_llm_runs.rs` tests: `begin_run`; `record_context("job-1", artifact)`; assert `get(id).context` is `Some` and `detail_json()["context"]["memories"]` has the expected length; a `record_context` for an unknown id is a no-op. Run: `cargo test -p openplotva-app --lib runtime_llm_runs`. Expected: FAIL.
- [ ] **Step 2: Buffer** — add the `context` field to `RunRecord`, `record_context` (locks, `open.get_mut(run_id)` → set; unknown id no-op), and `"context": self.context.as_ref().map(serialize)` in `base_json`/`detail_json` (skeleton omits it — detail only). `cargo test -p openplotva-app --lib runtime_llm_runs`. Expected: PASS.
- [ ] **Step 3: Wire engine** — in `execute_dialog_turn`, after `materialize`, `if let (Some(runs), Some(art)) = (ctx.llm_runs, base_input.context_capture.clone()) { runs.record_context(&format!("job-{}", ctx.item.id), art); }`. Console path: same with the `console-…` run id.
- [ ] **Step 4: Loop test** — extend the existing dialog-worker run-lifecycle test to assert the closed run's detail carries `context` with the stub's recalled memories. `cargo test -p openplotva-app --lib dialog_jobs`. Expected: PASS.
- [ ] **Step 5: Commit** — `feat: record the turn context artifact on the run and expose it in the dialog detail`.

### Task 3: Frontend — Context X-ray section

**Files:**
- Modify: `web/admin/index.html` (add `llmdContextXray(run)` rendered in `llmdRenderDetail`; graph + panels + chips; node-click → memory drawer).
- Modify: `web/admin/admin.css` (new `.llmd-xray*` classes, tokens only).
- Modify: `crates/openplotva-web/src/lib.rs` (index.html + admin.css `sha256`).
- Modify: `tools/service-smoke.web-ui.spec.js` (assert the X-ray graph + panels render from a mocked detail `context`).

**Interfaces — Consumes:** detail JSON `context` from Task 2; reuses `pl-graph` (`.data = {nodes, edges, center}`, `pl:node-click`), `/admin/api/memory/card?id=` for one-hop edges, and the memory card drawer opener.

- [ ] **Step 1: Renderer** — `llmdContextXray(run)` builds a section (only when `run.context`): a `pl-graph` whose `center` is a synthetic `turn` node, nodes = `context.memories` (label = preview, `salience`→size/accent, `competing`→disputed accent), then fetch one-hop edges among the recalled ids via `apiCall('/memory/card?id='+id)` and merge the relations into `graph.data.edges`; `pl:node-click` opens the memory card drawer for that id. Persona panel (name/mood/flags), settings panel (label/value list), chips (history depth, tools, shield). Empty state when `memories` is empty. All `pl-*`/`PL.el`/tokens.
- [ ] **Step 2: Wire** — call `llmdContextXray(run)` from `llmdRenderDetail` after the rounds; add `.llmd-xray*` CSS referencing tokens only.
- [ ] **Step 3: Hashes + guards** — recompute + update the `index.html`/`admin.css` `sha256`; `cargo test -p openplotva-web` (19+). Gate-1 greps clean; `node --check web/admin/components.js`.
- [ ] **Step 4: Live check** — local no-services run + Chrome with a mocked detail `context`: graph shows recalled nodes weighted by score, node-click opens the memory card, persona/settings/chips render, empty state; light theme + mobile width. Screenshot.
- [ ] **Step 5: Smoke + commit** — extend `service-smoke.web-ui.spec.js` to assert `#llmd-detail` shows the X-ray graph + a persona/setting value from a mocked `context`. Commit `feat: render the per-turn Context X-ray in the LLM Dialogs drawer`.

## Self-Review

- **Spec coverage:** capture (T1), in-memory storage + detail field (T2), X-ray graph + persona/settings + reuse (T3), design-system + delivery (constraints). Covered.
- **Placeholders:** none — types, seams, and test intents are concrete.
- **Type consistency:** `TurnContextArtifact`/`CapturedMemory` names match across T1→T2→T3; `record_context` and `context` field names consistent.

## Frontend taste

Task 3 is design-sensitive; apply the frontend-taste skill when building it (visual hierarchy of the X-ray, graph legibility, weight encoding, empty state).
