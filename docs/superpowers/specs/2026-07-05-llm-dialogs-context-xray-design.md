# LLM Dialogs — "Context X-ray" per turn

Repo: `openplotva`. Branch: TBD (`feat/llm-dialogs-context-xray`). Design agreed
with the owner 2026-07-05 via brainstorming; code facts verified against the tree
the same day (re-verify before editing).

## Context

The admin **LLM Dialogs** detail drawer shows a run's rounds, tool calls, outcome,
and raw request/response. But the raw request only holds the *flattened* prompt:
the structured context that shaped it — which memories were recalled and how
strongly, the active persona, the applied chat settings/customizations — is
assembled in `materialize_dialog_input` and then discarded (memories become
`DialogInput.reference_context: Vec<String>` text via `format_context`; the
`RetrievedMemory` salience/ids/relations are dropped). The owner wants to click a
dialog turn and see a beautiful "X-ray of the bot's mind" for that turn — a
weighted memory graph plus persona and settings.

## Owner decisions (binding)

1. **Purpose:** an X-ray/showcase of everything that fed the turn (memory +
   persona + settings + a few counts), for understanding the bot's context.
2. **Retention: in-memory only.** Capture into the existing `RuntimeLlmRunBuffer`
   (512 ring); restart = clean slate. **No new DB table, no migration.** This is
   the explicit non-overkill boundary — do not build a persisted context store.
3. **Mechanism:** collect at runtime — thread a light context artifact from
   `materialize` down to the in-memory run buffer.
4. **Memory graph:** recall-star (nodes = recalled memories, weighted by recall
   score) **plus inter-memory edges fetched live** from the memory store by id at
   view time (keeps the artifact tiny, the graph rich).

## Goals

- One "Context X-ray" section in the existing detail drawer: memory graph +
  persona panel + settings/customizations panel + context chips.
- Reuse what exists: `pl-graph` (`{nodes, edges, center}` + `pl:node-click`), the
  memory one-hop endpoint `/admin/api/memory/card`, the memory card drawer, and
  the run-buffer detail pattern.

## Non-goals

- No persistence of the artifact (in-memory only); no new admin endpoint beyond
  extending `/admin/api/llm/dialogs/detail`; no change to what is actually sent to
  the LLM (the artifact is capture-only, never serialized into the request).

## Data flow — capture (runtime)

`materialize_dialog_input` (app, `dialog_jobs/input.rs`) already computes the
scored recall (`RetrievedMemory` list) before flattening, and holds the resolved
`Persona` and applied chat settings. It builds a plain-data `TurnContextArtifact`
and surfaces it on `DialogInput` as skip-serialized metadata
(`#[serde(skip)] pub context_capture: Option<TurnContextArtifact>`, the struct
defined in `openplotva-dialog` as plain fields — no memory/app type coupling; the
app maps `RetrievedMemory` → the plain snapshot). `execute_dialog_turn`
(`dialog_turn/engine.rs`), which already carries `ctx.llm_runs` and runs inside
the run scope, records it once via a new `RuntimeLlmRunBuffer::record_context(
run_id, artifact)` right after materialization. The console path
(`run_captured_session`) can populate it the same way; optimizer runs skip it.

## Artifact shape (light — ids/scores/snapshots, never bodies)

```
TurnContextArtifact {
  memories: Vec<{ card_id, salience/score: f64, card_type, competing: bool,
                  preview: String (~120 chars) }>,   // the recalled set, scored
  persona:  { id/key, name, traits: Vec<String> } | None,
  settings: Vec<{ label, value }>,                    // applied chat customizations
  history_len: i32, tools_offered: Vec<String>, shield_on: bool,
  reference_context_chars: i32,                        // size of injected memory text
}
```

Capped (e.g. ≤ 40 memories, previews trimmed). Stored on `RunRecord` as
`context: Option<TurnContextArtifact>`.

## API

- Extend `GET /admin/api/llm/dialogs/detail?id=` to include `context` (the
  artifact as JSON) when present. No new route.
- The graph's inter-memory **edges** are fetched at view time by the frontend from
  the existing memory one-hop API (`/admin/api/memory/card` per recalled id, or a
  small batched variant) — reusing the Memory admin's node/edge contract.

## Frontend — "Context X-ray" section (detail drawer)

Rendered under the existing round list, only when `run.context` is present:
- **Memory graph** — a `pl-graph`: `center` = a synthetic "this turn" node;
  recalled memories as nodes sized/colored by `salience`; `competing` nodes get
  the disputed accent; edges = one-hop relations among the recalled ids pulled
  live from the memory store. `pl:node-click` → open that memory card in the
  existing memory drawer. Empty state when nothing was recalled.
- **Persona panel** — resolved persona + traits.
- **Settings / customizations panel** — the applied chat settings, as label/value.
- **Context chips** — history depth, tools offered, shield on/off, injected-memory
  size. All via `pl-*` + tokens (design-system rules apply; update asset hashes;
  run `openplotva-design-system-review`).

## Reuse

`pl-graph`, `pl:node-click`, memory card drawer, `/admin/api/memory/card`,
`RuntimeLlmRunBuffer` + its detail endpoint, the run scope threading — all already
built. New code is: the artifact struct + capture seam, one `record_context`
method, the detail-JSON field, and the frontend X-ray section.

## Verification

- Backend unit tests: artifact built from a scored recall (ids/salience/competing
  mapped, capped, previews trimmed), `record_context` attaches to the open run,
  merged/parked/optimizer paths don't populate it, detail JSON carries `context`.
- Frontend (local no-services, mocked detail): graph renders recalled nodes
  weighted by score, live-edge fetch wired, node-click opens the memory card,
  persona/settings panels, empty state, states/keyboard/light-theme; asset hashes
  + `cargo test -p openplotva-web` + design-system review.
- `cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings`;
  `cargo test --workspace`.

## Rollout

Standard delivery (AGENTS.md): `feat/llm-dialogs-context-xray` branch → PR → watch
CI + reviews → merge → deploy on request → verify. Frontend-first is fine (the
artifact can be mocked) but backend capture + detail field should land together so
the section has real data on deploy.
