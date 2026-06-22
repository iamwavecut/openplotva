# Admin Panel — UX Audit & Design-System Remediation

Senior-UX review of the OpenPlotva runtime admin console (`web/admin/`), walking the real operator
path end to end: log in → switch across 13 tabs → search/inspect runtime state → run VIP, Redis,
Chat, Shield, and User mutations. Each finding is rated by severity with a concrete fix. All
**Fixed** items were implemented in the design-system refactor (token layer + `pl-*` component library);
the two **Pre-existing** items are flagged for follow-up (out of scope for this refactor).

Severity: **Blocker** (breaks trust/usability or risks data) · **High** · **Medium** · **Low**.

## Feedback & destructive actions

| # | Sev | Flow | Problem | Fix | Status |
|---|-----|------|---------|-----|--------|
| 1 | Blocker | Every mutation (save settings, grant/revoke VIP, block chat, flush DB, …) | Feedback was a native `alert()` — a modal that blocks the whole thread, can't be styled, can't be themed, and reads as a browser error. 43 `alert()`/`confirm()` calls. | Non-blocking `PL.toast` with success/error/warning/info tones, ARIA-live, auto-dismiss + pause-on-hover. | Fixed |
| 2 | Blocker | Flush Redis DB, Delete Redis key, Delete user, Revoke VIP | Destructive actions guarded only by native `confirm()` — no danger styling, no context, one mis-click from data loss; identical OK/Cancel for "flush everything" and "delete one key". | Accessible `PL.confirm` modal: focus-trapped, Esc/backdrop to cancel, danger-red primary, explicit copy ("This deletes every key… cannot be undone") and verb-matched labels ("Flush everything", "Revoke VIP"). | Fixed |
| 3 | Medium | All key actions | No optimistic/section feedback beyond the alert; the operator couldn't tell an action was in flight. | Submit/refresh buttons disable + the shared `apiCall` drives a loader during the request; success toasts confirm completion. | Fixed |

## Loading, empty & error states (never built)

| # | Sev | Flow | Problem | Fix | Status |
|---|-----|------|---------|-----|--------|
| 4 | High | VIP/Users/Chats/Memory/Shield/Safety/LLM lists & tables | No loading state — containers showed stale data or a bare placeholder while fetching, so a slow query looked like a hang. | `pl-table` `state="loading"` renders shimmer skeleton rows; list containers use `PL.skeleton(...)`. | Fixed |
| 5 | High | Same surfaces | No real empty state — just `"No VIP users loaded"` grey text, indistinguishable from "still loading" or "broken". | `PL.empty` / `pl-table` empty state: icon + title + context ("Nothing matches '<query>'."), built once, consistent everywhere. | Fixed |
| 6 | High | Same surfaces | No error/retry state — a failed load toasted once then left a blank container with no recovery path. | `PL.error` / `table.showError(...)`: error icon + message + **Retry** button that re-runs the load. | Fixed |

## Information hierarchy & consistency

| # | Sev | Flow | Problem | Fix | Status |
|---|-----|------|---------|-----|--------|
| 7 | Medium | Whole app | 78 one-off inline styles (`style="font-size:0.8rem"`, ad-hoc widths, `background:var(--c-bg-app)`) produced drifting spacing, type sizes, and surfaces between tabs. | A formal three-tier token system (`tokens.css`): primitives → semantic → extended; a type scale (`--fs-*`/`--fw-*`), spacing rhythm, elevation, and a z-index ladder replacing magic `10/9999`. Zero inline styles remain. | Fixed |
| 8 | Medium | Cards/tables/badges | Repeated hand-rolled markup (40+ cards, 15+ tables, 50+ badges) with subtle inconsistencies. | One component library (`pl-table`, `pl-button`, `pl-field-group`, `pl-badge`/`.status-pill`, cards) reused across all 13 tabs. | Fixed |
| 9 | Low | Colors | ~25 hardcoded hex/`rgba` literals (incl. JSON-syntax, log-level, status, shield palettes) scattered across `admin.css`. | All centralized as named tokens; `admin.css`/`components.css` are color-literal-free (enforced by a guard test). | Fixed |

## Accessibility

| # | Sev | Flow | Problem | Fix | Status |
|---|-----|------|---------|-----|--------|
| 10 | High | Keyboard users | No `:focus-visible` styling anywhere; sidebar nav was `<button onclick>` with no `aria-current`; toggles were bare checkboxes; tables/list rows weren't keyboard-reachable. | Global focus-ring token; nav uses `aria-current="page"`; `pl-toggle` is `role="switch"` + `aria-checked` + Space/Enter; `pl-table` sortable headers and clickable rows are focusable + Enter-activated; dialogs trap & restore focus; `prefers-reduced-motion` respected. | Fixed |
| 11 | Low | Forms | Labels not reliably associated with controls; no help/error affordance. | `pl-field-group` auto-wires `<label for>`, `aria-describedby` (help), `aria-invalid` (error). | Fixed |

## Pre-existing defects surfaced during the walk (follow-up, out of scope)

| # | Sev | Flow | Problem | Recommendation | Status |
|---|-----|------|---------|----------------|--------|
| 12 | Blocker | Tasks (Taskman) tab | All Taskman JS handlers (`loadTaskmanJobs`, `clearTaskmanJobsByFilter`, `taskmanPrevPage/NextPage`, `copy/cancel/restartTaskmanSelectedJob`, `loadTaskmanJobByInput`) are referenced but **never defined** (`git show HEAD` confirms zero definitions). Entering the tab threw `ReferenceError`. | Implement (or remove) the Taskman backend JS. The refactor migrated its markup to the library and made `switchTab` defensive (no longer throws); buttons now no-op with a console warning instead of crashing. | Flagged |
| 13 | Medium | Chats/Users/LLM detail panes | `toggleChatDetails` / `toggleUserDetails` / `toggleLLMDetails` / `toggleTaskmanDetails` are referenced but undefined — the "Close" buttons on detail panes never worked. | Define the missing toggles (or remove the buttons). Preserved as-is; the library wires the action but the function is absent. | Flagged |
| 14 | Low | Analytics tab | The text panels (Summary, tool-parser, capacity) remain "Loading…" on API failure rather than showing an error state. | Extend the analytics error path to `PL.error(...)` the three text panels (the data tables already use `pl-table` error states). | Flagged |

Also fixed incidentally: the committed `index.html` SHA-256 constant was stale (the VIP commit changed the file without updating it), so `cargo test -p openplotva-web` was red on `main`; the refactor's hash discipline corrects it.
