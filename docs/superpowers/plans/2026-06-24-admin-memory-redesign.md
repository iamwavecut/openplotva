# Admin Memory redesign — implementation plan

> **For agentic workers:** Use superpowers:executing-plans / subagent-driven-development to implement task-by-task. Steps use `- [ ]` checkboxes. This plan is executed inline by the authoring agent continuously; UI-assembly tasks specify the deliverable, files, interfaces, states, and tests — final markup is produced via TDD during execution rather than pre-written verbatim.

**Goal:** Replace the two flat admin Memory tables with a token-driven Explorer (Overview cockpit → faceted list ⇄ bitemporal timeline → editable fact-sheet drawer with a one-hop graph neighbourhood), surfacing all card/run metadata and the link graph.

**Architecture:** Additive admin API in `openplotva-app`/`openplotva-storage`; new `pl-*` components + categorical tokens in `web/admin/`; four composed views in `index.html`. No build step; light-DOM web components; guard tests + sha256 hashes enforce the design system.

**Tech Stack:** Rust (axum handlers, SQLx), vanilla JS custom elements, CSS tokens, SVG. Tests: `cargo test -p openplotva-web/-app/-storage`, Playwright smoke.

## Global Constraints

- `web/admin/` routes through `pl-*` + token classes only — no raw `<button>/<input>/<select>/<textarea>/<table>`, no inline `style=`/`onclick=`/`onsubmit=`, no `alert()/confirm()` (use `PL.*`). Enforced by `admin_markup_routes_through_design_system`.
- Color literals only in `tokens.css`; `components.css`/`admin.css` use `var(--c-*)`. Enforced by `admin_styles_keep_colors_in_tokens`.
- Every edited `web/` asset → recompute its `sha256` in `crates/openplotva-web/src/lib.rs` (hasher `static_asset_sha256_hex`); `embedded_web_assets_match_expected_hashes` must pass.
- API additive only — never change existing endpoint shapes, element IDs, routes, login/cookie flow.
- After Rust edits: `cargo fmt --all`. Run `openplotva-design-system-review` before merge.
- Editable fields: `confidence`, `salience`, `portable`, `status` (active↔deleted), competing-resolution, link removal. NOT editable: `visibility`/scope, `fact_text`/S-P-O.

---

## Phase 1 — Categorical tokens + DESIGN.md

### Task 1: Add memory categorical tokens
**Files:** Modify `web/admin/tokens.css` (primitives + extended tier); `web/admin/DESIGN.md` (§5 families); `crates/openplotva-web/src/lib.rs:40` (tokens.css sha256).
- [ ] Add `--p-pink-400:#f472b6`, `--p-teal-400:#2dd4bf` to primitives.
- [ ] Add `--c-cardtype-*` (10), `--c-relation-*` (5), `--c-visibility-*` (5), `--c-status-competing` in the extended tier (defined once in `:root`, saturated hues like the `--c-shield-*`/`--c-log-*` precedent). Property names use the exact DB enum spellings (underscores) so JS can do `--c-cardtype-${card_type}`.
- [ ] Document the three new families in `DESIGN.md` §5 token list.
- [ ] Recompute `shasum -a 256 web/admin/tokens.css`; update the constant.
- [ ] `cargo test -p openplotva-web` → green (hash + guards; tokens.css may hold hex, the color-literal guard exempts it).

## Phase 2 — Backend / API expansion (additive)

Storage SQL in `openplotva-storage`, response structs in `openplotva-memory`, handlers in `openplotva-app/src/lib.rs`. TDD: unit tests for SQL string contracts + handler response shape where pure.

### Task 2: Expand card list/detail fields + `as_of`
- [ ] Extend `SQL_LIST_MEMORY_CARDS` SELECT to add `salience, observation_count, use_count, decay_score, thread_id, origin_thread_id, valid_from, recorded_at, retracted_at, portable, conflict_group` (already returns most; add the missing/new). Add optional `as_of` filter variant (`recorded_at<=$ AND (retracted_at IS NULL OR retracted_at>$)`).
- [ ] Extend the card JSON (`admin_memory_cards_list_response`) and `Card`/row-map to carry the new columns.
- [ ] Add `card_type`/`visibility`/`portable` filters to `CardFilter` + SQL `WHERE`.
- [ ] New `GET /admin/api/memory/cards/:id` → card + `memory_sources` + links neighbourhood (`SQL_LIST_CARD_LINKS`).
- [ ] Tests: storage SQL-contract asserts for new columns/filters; handler shape test.

### Task 3: Links neighbourhood + runs detail + overview aggregates
- [ ] `SQL_LIST_CARD_LINKS` (both directions, join peer card id/fact_text/card_type) + `GET /admin/api/memory/cards/:id/links`.
- [ ] Extend `SQL_LIST_MEMORY_RUNS` + RunRecord JSON with `lease_owner, attempts, cards_superseded, episodes_inserted, prompt_version, started_at, completed_at`; join `chats.type` for `chat_type`; add `parts` (extraction-batch count: add a `parts` column to `memory_runs` via a migration + populate from the batch loop, or expose a derived estimate labelled as such).
- [ ] `GET /admin/api/memory/overview` → totals (by status incl. competing), links count, runs_today, `by_visibility`, `by_card_type`, `top_chats` (with chat_type), `recent_runs`. Backed by `COUNT(...) GROUP BY` queries.
- [ ] Tests: SQL-contract + handler shape.

### Task 4: Card mutation (`PATCH`) + link delete
- [ ] `PATCH /admin/api/memory/cards/:id` body `{confidence?,salience?,portable?,status?}` → `SQL_UPDATE_MEMORY_CARD_FIELDS` (clamp 0..1; status restricted to active/deleted; deleted reuses soft-delete + sets `retracted_at`; restore sets active + clears `retracted_at`). Competing-resolution action (promote one side → supersede the other; clear competing → both active).
- [ ] `DELETE /admin/api/memory/links/:id` → `SQL_DELETE_MEMORY_LINK`.
- [ ] Tests: SQL-contract + clamp/guard unit tests.

## Phase 3 — New `pl-*` components + CSS primitives

Light DOM, token-only, ARIA+keyboard. Each: define in `components.js`, style in `components.css` (tokens only), document in `DESIGN.md` §6, recompute all three asset hashes, `cargo test -p openplotva-web`.

### Task 5: CSS primitives
- [ ] `.metric-card`, `.bar-row`, `.meter`, `.facet-bar`, `.filter-chip`, `.tag--cardtype/--relation/--visibility` in `components.css` (var(--c-*) only). Hashes + test.

### Task 6: `pl-slider`
- [ ] Custom element: props `min/max/step/value/label`, inner native `<input type=range>` (id moved onto it like `pl-input`), live readout, `role`/aria, emits `pl:input`/`pl:change`, `.value` get/set. Style + DESIGN.md §6. Hashes + test.

### Task 7: `pl-graph`
- [ ] Custom element: JS props `nodes:[{id,label,card_type,salience,competing}]`, `edges:[{from,to,relation,confidence}]`, `center`. Renders SVG: radial layout around `center`, node fill `--c-cardtype-*`, r∝salience, competing ring `--c-status-competing`; edges stroke `--c-relation-*`, width∝confidence, weight label. Emits `pl:node-click{id}`, `pl:edge-action{id,action}`. Caps at N nodes ("+k more"). Style + DESIGN.md §6. Hashes + test.

### Task 8: `pl-timeline`
- [ ] Custom element: JS props `lanes:[{key,label}]`, `items:[{lane,label,valid_from,valid_until,recorded_at,status,conflict_group}]`, `now`, `asOf`. Renders SVG swimlanes: valid bars, recorded ▲, superseded greyed, competing bracket, `now` + draggable `asOf` lines. Emits `pl:item-click{id}`, `pl:asof-change{date}`. Style + DESIGN.md §6. Hashes + test.

### Task 9: `pl-drawer` (or `.split-pane` decision)
- [ ] If `.split-pane` insufficient: `pl-drawer` normal-flow slide-in panel (no `position:fixed`), Esc/backdrop close, focus trap (reuse `pl-modal` internals), `open` prop, emits `pl:close`. Else document the `.split-pane` reuse. Hashes + test.

## Phase 4 — Overview (Cockpit) view

### Task 10: Overview tab
- [ ] In `index.html` Memory section, add an Overview sub-view: `.metric-card` grid + `.bar-row` breakdowns + a recent-runs list (`pl-table` or list) showing who/chat-type/msgs/parts/cards/status. Wire `loadMemoryOverview()` → `/overview` with loading/empty/error. Drill actions (`data-action`) → Explorer with facet. Recompute index.html + components hashes; `cargo test -p openplotva-web`; Playwright selector.

## Phase 5 — Explorer (faceted list + detail drawer + graph)

### Task 11: Facet bar + enriched list
- [ ] `.facet-bar` with `.filter-chip`s (chat/user/visibility/card_type/status/as-of) driving `memoryQueryParams()`; enriched `MEMORY_CARD_COLUMNS` (type tag, fact, scope chips, conf/salience `.meter`, status pill incl. competing). Row-click opens drawer. Hashes + test.

### Task 12: Detail drawer (metadata + connections + neighbourhood graph)
- [ ] Drawer renders full card metadata, bitemporal strip, connections list, and `pl-graph` neighbourhood from `/cards/:id` + `/cards/:id/links`; node-click re-centres (re-fetch). Loading/empty/error. Hashes + test.

## Phase 6 — Timeline view + as-of

### Task 13: Timeline body + List⇄Timeline toggle + as-of scrubber
- [ ] `pl-slider`/scrubber sets section `asOf`; List⇄Timeline `pl-button-group` toggle; `pl-timeline` fed from the same filtered query (lanes per chat/user). `asOf` re-queries list, retimes timeline, replays drawer. Hashes + test.

## Phase 7 — Fact-sheet editing

### Task 14: Edit mode (coefficients/status/portable/resolution/link delete)
- [ ] Drawer "expand/edit" mode: `pl-slider` confidence/salience, `pl-toggle` portable, status soft-delete/restore, competing-resolution buttons, link-delete (each `PL.confirm` for destructive) → `PATCH`/`DELETE`, success `PL.toast`, re-fetch. Hashes + test.

## Final verification
- [ ] `cargo fmt --all`; `cargo test -p openplotva-web -p openplotva-app -p openplotva-storage`; `cargo clippy` (touched crates) clean.
- [ ] `openplotva-design-system-review` skill (token/state/a11y/light-theme checklist).
- [ ] `tools/local-smoke.sh` empty/error paths; Playwright Memory-section smoke.
- [ ] Manual: Overview→Explorer→Timeline→Fact-sheet against seeded data; as-of replay; graph re-centre; each mutation's toast + re-fetch.

## Self-review notes
- Spec coverage: every spec §3–§7 item maps to a task above (IA→10-13, design-system→1,5-9, API→2-4, editing→14, enforcement/verification→per-task hashes + final).
- Open points carried from spec: `parts` counter (Task 3 adds column or labelled estimate); `pl-drawer` vs `.split-pane` (Task 9 decides).
