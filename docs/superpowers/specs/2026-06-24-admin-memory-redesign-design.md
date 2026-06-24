# Admin Memory section redesign — design

| | |
|---|---|
| **Status** | Draft — awaiting review |
| **Date** | 2026-06-24 |
| **Scope** | `web/admin/` Memory & Memory Runs; supporting admin API in `openplotva-app`/`openplotva-storage`; design-system extensions in `web/admin/{tokens,components}.{css,js}` + `DESIGN.md` |
| **Out of scope** | Settings WebApp (`web/settings/`); changing the memory extraction/retrieval runtime; re-assigning a fact's visibility/scope from the UI |
| **Builds on** | the just-shipped memory features: `portable` (mig 126), `recorded_at`/`retracted_at` (mig 127), `competing`/`conflict_group` (mig 128), `memory_links` graph |

## 1. Context & problem

The admin Memory section is two plain `pl-table`s. Memory Cards shows only `id / visibility / card_type / fact_text / confidence / updated_at`; Memory Runs shows `status / chat / window / messages / tokens / cards / error`. The data model carries far more that is never surfaced:

- **Cards:** `status`, `subject/predicate/object`, `salience`, `observation_count`, `use_count`, `decay_score`, scope keys (`chat_id/thread_id/user_id`), origin keys, `valid_from/valid_until`, and the new `portable`, `recorded_at`, `retracted_at`, `conflict_group`.
- **Runs:** `lease_owner` (who ran it), `attempts`, `cards_superseded`, `episodes_inserted`, `prompt_version`, `started_at/completed_at`, derived chat type (private/public) and how many extraction parts ran.
- **Graph:** `memory_links` (`supports / contradicts / same_topic / supersedes / mentions_same_entity`, weighted by `confidence`) is populated at extraction time but has **no** admin API or UI at all.

So an operator cannot: see a fact's full metadata, structure facts by chat/user, see the relationship graph, reason about time (when a fact was true vs when the bot learned it), or correct the store (fix coefficients, soft-delete, resolve a competing pair). This redesign closes those gaps.

## 2. Goals / non-goals

**Goals**
- Surface every card and run field, organized by chat and user, not as flat tables.
- Make the graph navigable (one-hop neighbourhood with typed, weighted edges).
- Make the bitemporal model legible (valid time vs transaction time; `as of` replay).
- Allow safe admin edits: coefficients, status (soft-delete/restore), `portable`, competing-pair resolution, and removing a spurious link.
- Stay inside the existing design system: token-driven `pl-*` components, no build step, guard tests + sha256 hashes, `openplotva-design-system-review` before merge.

**Non-goals**
- A standalone force-directed graph canvas (prototype #2). The graph appears only as a one-hop neighbourhood inside detail views — one hop is the right depth for this product.
- Editing a fact's `visibility`/scope keys from the UI (re-scopes its audience; a separate, carefully-gated feature if ever needed).
- New front-end tooling (no bundler/framework), and no change to existing API endpoint shapes that other code depends on — additive only.

## 3. Information architecture

`Memory` becomes a section with three views and one detail surface, composed from prototypes #4/#1/#3/#5:

- **Overview (Cockpit, #4)** — the landing view. KPI tiles (total / active / competing / links / runs today), breakdowns (by visibility, top chats), and a recent-runs list showing `who` (`lease_owner`), chat type, messages, parts, cards, status. Clicking a run → Explorer filtered to that chat; clicking a breakdown row → Explorer with that facet pre-applied.
- **Explorer (Workbench, #1)** — faceted filters (chat, user, visibility, card_type, status, `as of` time) over a dense fact list, with a **List ⇄ Timeline** toggle. Selecting a fact opens the detail drawer.
- **Timeline (#3)** — the Explorer's alternate body: bitemporal swimlanes (lane per chat/user) with valid-time bars, `recorded_at` markers, supersession chains, competing pairs, and `now` + `as of` lines. Same filter set as the list.
- **Detail drawer / Fact sheet (#5)** — opens from the list/timeline/graph. Compact read mode shows full metadata, a bitemporal strip, connections, and the one-hop **graph neighbourhood**. An "expand/edit" toggle turns it into the full Fact sheet: editable coefficient sliders, status & `portable` toggles, competing-resolution actions, and link removal. Clicking a neighbour node re-centres the drawer on that fact.

`as of` is a single section-level control: it re-queries the list, retimes the timeline, and replays the graph/detail — answering "what did the bot know on date X".

## 4. Design-system extensions (`DESIGN.md` §5/§6 update)

All additive; existing token names preserved. New work documented in `DESIGN.md` and gated by the existing tests.

### 4.1 Tokens (`tokens.css`, extended tier — mirrors `--c-shield-*`)
- `--c-cardtype-{preference,identity,project,decision,relationship,recurring_topic,joke,warning,technical_fact,event}`
- `--c-relation-{supports,contradicts,same_topic,supersedes,mentions_same_entity}`
- `--c-visibility-{chat,chat_user,public_user,private_chat,thread}`
- extend `--c-status-*` with `competing`
- Each defined for dark `:root` and `[data-theme="light"]`. Primitives (`--p-*`) added only for genuinely new raw values; otherwise alias existing primitives.

### 4.2 CSS primitives (`components.css`)
- `.metric-card` — KPI tile (muted label + large number), used in 2–5 grids.
- `.bar-row` — labelled horizontal breakdown bar (label / token-filled track / value).
- `.meter` — inline confidence/salience bar for list rows and detail.
- `.facet-bar` + `.filter-chip` — the Explorer filter row (chip = label + value + remove).
- badge variants `.tag--cardtype`, `.tag--relation`, `.tag--visibility` driven by the categorical tokens.

### 4.3 New `pl-*` components (`components.js` + `components.css`)
Light DOM, token-only styling, ARIA + keyboard in `connectedCallback`, documented in `DESIGN.md` §6, hashes updated.
- **`pl-slider`** — accessible range with a live readout. Props `min/max/step/value/label`, `.value` get/set, emits `pl:input`/`pl:change`. Used for coefficient editing and the `as of` scrubber. (Replaces raw `<input type=range>`, which the guard forbids.)
- **`pl-graph`** — renders an SVG one-hop neighbourhood from `{nodes, edges}` JS props. Node fill = `--c-cardtype-*`, radius ∝ salience, `competing` nodes ringed in `--c-status-competing`; edges coloured by `--c-relation-*`, width ∝ `confidence`, with a weight label. Emits `pl:node-click` (re-centre) and `pl:edge-action` (e.g. remove link). Static layout (precomputed radial positions) — no physics engine, no external lib.
- **`pl-timeline`** — renders SVG bitemporal swimlanes from `{lanes, items}` JS props: valid-time bars, `recorded_at` markers, supersession links, competing brackets, `now` + draggable `as of` lines. Emits `pl:item-click` and `pl:asof-change`.
- **`pl-drawer`** — a normal-flow slide-in side panel (no `position: fixed`, per design-system rules) hosting the detail/fact-sheet content; Esc/backdrop close, focus trap (reuse `pl-modal` patterns). If `.split-pane` proves sufficient, fold the drawer into it instead of a new element.

## 5. Backend / API additions (additive)

In `openplotva-app` (handlers) + `openplotva-storage` (SQL) + `openplotva-memory` (response structs).

- **`GET /admin/api/memory/overview`** *(new)* → `{ totals:{cards,active,competing,superseded,deleted,links}, runs_today, by_visibility:[{visibility,count}], by_card_type:[{card_type,count}], top_chats:[{chat_id,title,chat_type,count}], recent_runs:[…] }`. Backed by aggregate `COUNT(...) GROUP BY` queries.
- **`GET /admin/api/memory/cards`** *(extend)* — return all columns (status, S-P-O, salience, observation_count, use_count, decay_score, scope+origin keys, valid/recorded/retracted, portable, conflict_group); add `as_of`, `card_type`, `visibility`, `portable` filters. Existing params/shape preserved; fields only added.
- **`GET /admin/api/memory/cards/:id`** *(new)* — one card with all fields + its `memory_sources` (provenance) + its `memory_links` neighbourhood (peer card id/fact_text/card_type/relation/confidence).
- **`PATCH /admin/api/memory/cards/:id`** *(new)* — body `{ confidence?, salience?, portable?, status? }` (status restricted to `active`/`deleted`, plus a competing-resolution action: promote one side / clear competing). Admin-gated; success toast; destructive (`status=deleted`) confirmed.
- **`DELETE /admin/api/memory/links/:id`** *(new, optional within phase 6)* — remove a spurious link.
- **`GET /admin/api/memory/runs`** *(extend)* — add `lease_owner`, `attempts`, `cards_superseded`, `episodes_inserted`, `prompt_version`, `started_at`, `completed_at`, derived `chat_type` (join `chats`), and `parts` (extraction-batch count for the run — sourced from a small counter; if not yet tracked, add it to `RunStats`/`memory_runs` or derive from message_count ÷ batch size and label it an estimate).

Existing `DELETE /admin/api/memory/cards`, `POST /admin/api/memory/runs` (retry), `POST /admin/api/memory/restart` stay unchanged.

## 6. Editing & mutation model

- Editable: `confidence`, `salience` (via `pl-slider`), `portable` (toggle), `status` active↔deleted (soft-delete/restore via existing soft-delete + a restore path), competing-pair resolution, link removal.
- Not editable here: `visibility`/scope keys, `fact_text`/S-P-O text (changing meaning), embeddings.
- Every mutation: optimistic-free — call API, then toast on success / error-toast on failure; destructive (`delete`, link removal, soft-delete) gates on `PL.confirm({danger:true})` with verb-matched copy. Re-fetch the affected card/list after mutation.

## 7. Phases (one spec, ordered build)

1. **Tokens + DESIGN.md** — categorical token families + doc/§6 entries (no behavior yet). Cheap foundation.
2. **API expansion** — overview aggregates; full card fields + `as_of`; card detail; links neighbourhood; runs detail (+`parts`); `PATCH` card; (link delete). Unit/handler tests for shapes.
3. **Components** — `pl-slider`, `pl-graph`, `pl-timeline`, `pl-drawer` + CSS primitives; guard + sha256 updates; render on mock data.
4. **Overview (Cockpit)** — wire the landing view to `/overview` with loading/empty/error states.
5. **Explorer** — facet bar + enriched list + detail drawer (metadata + connections + `pl-graph` neighbourhood); selection + re-centre.
6. **Timeline + as-of** — `pl-timeline` body, List⇄Timeline toggle, section-level `as of` scrubber across list/timeline/detail.
7. **Fact sheet editing** — expand/edit mode: `pl-slider` coefficients, status/`portable` toggles, competing resolution, link removal → `PATCH`/`DELETE`.

## 8. Enforcement & contracts

- All `web/admin/` edits route through `pl-*` + tokens; no raw `<button>/<input>/<select>/<textarea>/<table>`, no inline `style=`/`onclick=`, no `alert/confirm` — enforced by `admin_markup_routes_through_design_system`.
- No color literals outside `tokens.css` — `admin_styles_keep_colors_in_tokens`.
- Every edited `web/` asset's `sha256` updated in `crates/openplotva-web/src/lib.rs` — `embedded_web_assets_match_expected_hashes`.
- Run `openplotva-design-system-review` before merge.
- Additive API only: existing endpoints/methods/bodies, element IDs, routes, and the login/cookie flow unchanged.

## 9. Verification

- `cargo test -p openplotva-web` (guards + hashes), `cargo test -p openplotva-app`/`-p openplotva-storage` (new handler/SQL shapes), `cargo fmt --all`.
- `tools/local-smoke.sh` for empty/error states; admin auth locally per `DESIGN.md` §11.
- Playwright `tools/service-smoke.web-ui.spec.js` extended with Memory-section selectors/assertions.
- Manual: load Overview/Explorer/Timeline/Fact-sheet against seeded data; verify `as of` replay, graph re-centre, and each mutation's toast + re-fetch.

## 10. Risks & open points

- **`parts` counter** may not be tracked per run today; phase 2 either adds a counter or ships a labelled estimate.
- **Graph layout** is precomputed/radial for a one-hop set; if neighbourhoods get large, cap node count and show "+N more".
- **`pl-timeline`/`pl-graph`** are the most novel components; budget the most review/iteration there, and keep them pure renderers (data in, events out) so they stay testable.
- **Light theme** parity for the categorical tokens must be checked (the design-system-review covers this).
