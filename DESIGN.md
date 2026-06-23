# Plotva Admin Design System

| | |
|---|---|
| **Status** | Adopted — shipped to production (`geta.moe`) |
| **Scope** | `web/admin/` (the runtime admin console). The Settings WebApp (`web/settings/`, Framework7 + Telegram theme) is explicitly out of scope. |
| **Owners** | OpenPlotva maintainers |
| **Last updated** | 2026-06-23 |
| **Source of truth** | `web/admin/tokens.css`, `web/admin/components.css`, `web/admin/components.js` |
| **Enforced by** | guard tests in `crates/openplotva-web/src/lib.rs`; the `openplotva-design-system-review` skill |
| **Related** | `AGENTS.md` (§ Web UI / Design System), `docs/admin-ux-audit.md`, `skills/openplotva-design-system-review/SKILL.md` |

A design document, not a tutorial: it states the problem, the goals, the architecture, the
component/token contracts, the decisions and their alternatives, and how the system is enforced.
Read it before touching any UI under `web/admin/`.

---

## 1. Context

The admin console is a single-page app (`web/admin/index.html`) embedded into the Rust binary and
served by `openplotva-web` / `openplotva-app`. It grew organically: every feature hand-rolled its own
markup. By mid-2026 it carried ~80 inline `style=` attributes, ~78 inline `onclick=` handlers, 43
native `alert()`/`confirm()` calls, dozens of near-duplicate cards/tables/badges, no loading/empty/error
states, no focus-visible styling, and no ARIA. Feedback blocked the UI thread and could not be themed
or tested; destructive actions were one mis-click away behind an unstyled `confirm()`. See
`docs/admin-ux-audit.md` for the severity-rated findings that motivated this work.

This system replaces that ad-hoc layer with one token-driven, accessible component library and makes
"every interactive element goes through the library" a build-failing invariant.

## 2. Goals / Non-goals

**Goals**

- One canonical set of design tokens (color, type, spacing, radius, elevation, motion, z-index) — the
  only place visual constants are defined.
- A small, reusable, accessible component library (`pl-*`) that every admin control routes through.
- First-class interaction states everywhere: loading (skeleton), empty, error+retry, hover, focus,
  disabled, and non-blocking feedback.
- Mechanical enforcement: bypassing the library fails the build, not just review.
- Zero new tooling: no bundler, no framework, no npm. Ships as static assets like the rest of `web/`.

**Non-goals**

- Re-skinning or re-architecting the Settings WebApp (`web/settings/`) — a separate Telegram-themed
  security boundary.
- A public/published component library for other apps. This is internal to the admin console.
- A visual reinvention. The console is an operator tool; the design language stays a calm slate/indigo
  dark dashboard. The "signature" is rigor — consistent states and feedback — not flourish.

## 3. Principles

1. **Tokens are the only source of visual constants.** Components reference semantic tokens; raw color
   literals live only in `tokens.css`.
2. **The component owns the behavior.** Keyboard handling, ARIA, focus management, and escaping live
   inside the component, defined once, not re-derived per call site.
3. **No bypassing.** Raw `<button>/<input>/<select>/<textarea>/<table>`, inline `onclick`/`onsubmit`/
   `style`, `el.onclick =`, and native `alert()/confirm()` are forbidden in `web/admin/index.html`.
4. **Every async path shows its states.** loading → (content | empty | error+retry). Mutations confirm
   with a toast; destructive ones gate on an accessible dialog.
5. **Smallest implementation that preserves the contract.** Native browser APIs over frameworks; the
   data layer (API calls, IDs, routes) is never changed by a UI migration.

## 4. Architecture

### 4.1 Files

```
web/admin/
  tokens.css       Design tokens (3 tiers). The ONLY color source.
  admin.css        Layout + legacy structural classes. References tokens; no color literals.
  components.css   pl-* component styles + CSS-class primitives + global interaction states.
  components.js    pl-* custom elements + the PL runtime + [data-action] event delegation.
  index.html       The SPA. Markup uses pl-* + token-backed classes only.
  login.html, favicon.svg, site.webmanifest
```

### 4.2 Load order & embedding

`index.html` `<head>` loads, in order: `tokens.css` → `admin.css` → `components.css` → Chart.js (CDN).
`components.js` is loaded as a **classic script at the end of `<body>`, immediately before the page's
inline script**, so the full DOM (including each component's light-DOM children, e.g. `<option>`s) is
parsed before any element upgrades, and the `PL` runtime + every `pl-*` definition exist before the app
script runs.

All five admin assets are embedded into the Rust binary via `include_bytes!` in
`crates/openplotva-web/src/lib.rs` and served by the existing `/admin/{*path}` handler. There is **no
build step**. Each asset carries an embedded `sha256` checked by a test (see §9). Non-`index.html`
assets are served unauthenticated (they contain no secrets); `index.html` stays behind the admin
session gate.

### 4.3 Component model — light-DOM Web Components

Components are native Custom Elements with **no Shadow DOM**. Light DOM is deliberate: the global
`admin.css`/token cascade and `[data-theme]` switching apply normally, and Chart.js canvas sizing is
unaffected. Encapsulation is *behavioral* (lifecycle, attributes, events), not stylistic — theming is
global via tokens, which is exactly what an operator console wants.

Two layers:

- **Web Components** (`pl-*`) for anything interactive or stateful.
- **CSS-class primitives** for static surfaces (`.card`, `.badge`, `.skeleton`, `.empty-state`,
  `.error-state`, `.status-pill`, grids, split-pane).

## 5. Design tokens (`tokens.css`)

Three tiers. Components reference **only** the semantic/extended tiers.

| Tier | Prefix | Role | Examples |
|---|---|---|---|
| Primitive | `--p-*` | Raw, theme-agnostic palette. Never referenced by components. | `--p-slate-900`, `--p-indigo-500`, `--p-cyan-400`, `--p-red-500` |
| Semantic | `--c-*`, `--sp-*`, `--rad-*`, `--fs-*`, `--fw-*`, `--lh-*`, `--elev-*`, `--dur-*`, `--ease-*`, `--z-*` | Meaning-based aliases — the vocabulary components use. | `--c-bg-card`, `--c-primary`, `--c-danger`, `--c-text-main`, `--c-focus-ring`, `--sp-4`, `--rad-md`, `--fs-base`, `--elev-2`, `--z-modal` |
| Extended | `--c-json-*`, `--c-log-*`, `--c-status-*`, `--c-shield-*`, `--grad-*` | Domain palettes (syntax highlighting, log levels, status dots, shield categories, brand gradients). | `--c-log-error`, `--c-status-ok`, `--c-shield-red`, `--grad-brand` |

Token families:

- **Color** — surfaces (`--c-bg-app/sidebar/card/card-hover/input/elevated`), borders, brand
  (`--c-primary`, `--c-primary-hover`, `--c-accent`), state (`--c-success/warning/danger/info`) with
  tinted-surface variants (`--c-*-bg`, `--c-*-bg-strong`, derived via `color-mix`), text
  (`--c-text-main/sec/muted/on-primary`), and interaction (`--c-focus-ring`, `--c-scrim`,
  `--c-row-hover-overlay`).
- **Spacing** — a 4px rhythm `--sp-1 … --sp-12`.
- **Radius** — `--rad-sm/md/lg/full`.
- **Type** — families `--font-sans` (Inter) / `--font-mono` (JetBrains Mono); scale `--fs-xs … --fs-2xl`;
  weights `--fw-normal … --fw-extrabold`; line-heights `--lh-tight/snug/normal`.
- **Elevation** — `--elev-0 … --elev-3`, `--elev-modal`, `--elev-glow-brand`.
- **Motion** — `--dur-instant/quick/base`, `--ease-out`; legacy aliases `--trans-fast/norm`.
- **Z-index** — a named ladder `--z-base/resizer/sidebar/mobile-sidebar/dropdown/overlay/modal/toast`
  (formalizes the old `5/10/100/9999` magic numbers).
- **Layout** — `--w-sidebar`, `--h-header`.

**Theming.** `:root` holds the dark theme (default). `[data-theme="light"]` overrides semantic *color*
only; scale tokens are theme-agnostic. The theme toggle sets `data-theme` and persists it in
`localStorage`. Existing token names were preserved verbatim during the migration so prior markup kept
working — tokens were only added, never renamed.

## 6. Components

### 6.1 `pl-*` custom elements (`components.js` / `components.css`)

| Element | Replaces | Key attributes / API | States & a11y |
|---|---|---|---|
| `pl-button` | `<button class="btn …">` | `variant=primary\|outline\|danger\|success\|ghost\|nav`, `size=sm\|md\|lg`, `block`, `type=submit`, `disabled`, `loading`, `data-action`, `data-args` | host is the button: `role=button`, `tabindex`, Enter/Space activate; hover/active/`:focus-visible`/disabled/loading (inline spinner, `aria-busy`) |
| `pl-button-group` | ad-hoc `.d-flex` button rows | `block` | `role=group` |
| `pl-input` / `pl-textarea` / `pl-select` | `.form-control` inputs/areas/selects | standard form attrs; the `id` is moved onto the inner native control so `getElementById(id).value` keeps working; emits `pl:input`/`pl:change`/`pl:enter` | native control inside (real validation + form participation); `invalid`, `disabled`, focus ring |
| `pl-field-group` | `.form-group` + `.form-label` | `label`, `help`, `required` | auto-wires `<label for>`, `aria-describedby` (help), `aria-invalid` (error) |
| `pl-toggle` | bare checkboxes | `checked`, `disabled`, `label`; `.checked` get/set; emits `pl:change` | `role=switch`, `aria-checked`, Space/Enter toggle |
| `pl-table` | JS string-built `<table>` | JS props `columns` (`{key,label,sortable,mono,num,render}`) and `rows`; `state=idle\|loading\|empty\|error`; `row-clickable`, `dense`; `emptyTitle`/`emptyDesc`/`onRetry`; `showError(msg)`; emits `pl:row-click`/`pl:sort` | internal text-node escaping (kills the manual `escapeHtml` bug class); skeleton rows / empty / error+retry; `aria-sort`, keyboard-reachable sortable headers + clickable rows |
| `pl-modal` | `.loader-overlay` + native `confirm()` | imperative via `PL.alert`/`PL.confirm`; declarative `open` | `role=dialog`, `aria-modal`, focus trap, Esc/backdrop close, focus restore |
| `pl-toast-host` | — | singleton container | `aria-live`; hosts toasts |

Actions are wired by **event delegation**: a single document listener maps `[data-action="fn"]`
(optionally `data-args='[...]'`, `data-confirm="…"`) to a global function, awaiting promises and
surfacing thrown errors as an error toast. There is no inline `onclick` anywhere. `<form data-action>`
gives Enter-to-submit/search.

### 6.2 CSS-class primitives (`components.css`)

Static surfaces stay as classes used on plain elements: `.card`/`.card-header`/`.card-body`, `.badge` +
`.badge-*`/`.status-pill[data-status]`, `.skeleton`/`.skeleton-row`/`.skeleton-text` (shimmer),
`.empty-state`/`.error-state`, `.spinner`/`.inline-spinner`, layout grids, `.split-pane`/`.pane-*`,
`.json-tree*`, plus token-backed content utilities (`.cell-strong`, `.detail-pre`, `.list-item-meta`,
`.w-narrow/.w-xs/.w-range`, `.card-inset`, …). Global interaction states (`:focus-visible` rings via
`--c-focus-ring`, `[disabled]`, hover transitions) and `prefers-reduced-motion` handling are defined
once here.

## 7. The `PL` runtime

A single global object exposed by `components.js`:

- `PL.toast(message, 'success'|'error'|'warning'|'info', duration?)` — non-blocking feedback (replaces `alert`).
- `PL.confirm({title, body, danger, okLabel}) → Promise<boolean>` — accessible, focus-trapped (replaces `confirm`).
- `PL.alert({title, body}) → Promise` — informational modal (rarely needed; prefer toast).
- `PL.skeleton(el, {rows})` / `PL.empty(el, {title, desc, icon})` / `PL.error(el, {message, onRetry})` — state renderers for non-table containers.
- `PL.badge(label, tone)` → a badge node; `PL.el(tag, props, children)` → an element builder whose
  string children become auto-escaped text nodes (use instead of `innerHTML` concatenation).

## 8. Interaction & state model

Every async load follows: set `loading` (skeleton or `pl-table` `state='loading'`) → on success render
content or an **empty state** → on failure render an **error state with Retry** (`PL.error` /
`table.showError`). Mutations give a success **toast**; destructive actions (`flush DB`, delete key,
delete user, revoke VIP) gate on `await PL.confirm({danger:true})` with explicit, verb-matched copy.
Hover, `:focus-visible`, active, disabled, and motion are uniform across components.

## 9. Enforcement & contracts

This is what keeps the system from eroding. All build-failing.

- **`admin_markup_routes_through_design_system`** (`crates/openplotva-web/src/lib.rs`) — fails if
  `index.html` contains `onclick=`/`onsubmit=`/`style="`/`el.onclick =`, any native `alert()`/`confirm()`
  (only `PL.*` allowed), or raw `<button>/<input>/<select>/<textarea>/<table>`; and asserts `tokens.css`/
  `components.css`/`components.js` stay linked.
- **`admin_styles_keep_colors_in_tokens`** — `components.css` and `admin.css` must contain no hex/`rgb()`
  color literals and must not redefine tokens; colors come from `tokens.css` via `var(--c-*)` / `color-mix`.
- **`embedded_web_assets_match_expected_hashes`** — every `web/` asset's bytes must match its embedded
  `sha256`. **Editing any asset requires updating its constant** in `crates/openplotva-web/src/lib.rs`
  (the canonical hasher is `static_asset_sha256_hex`).
- **`AGENTS.md` § Web UI / Design System** — the human-readable contract.
- **`openplotva-design-system-review` skill** — the review checklist (automated gates → live browser
  verification) to run before merging admin UI changes.

External contracts preserved by construction: a UI migration never changes API endpoints/methods/bodies,
element IDs, routes, the login/cookie flow, Chart.js analytics, or the Settings WebApp.

## 10. Authoring guide

- **Add UI to a tab:** use `pl-*` elements + token classes. Wire actions via `data-action`/`data-args`
  to a named global function (no inline handlers). Dynamically created controls must be built with
  `document.createElement('pl-…')` / `PL.el`, **not** raw control tags.
- **Show data:** drive a `pl-table` via `columns`/`rows` with loading/empty/error states; never build
  table HTML by string concatenation.
- **Add a token:** define it in `tokens.css` (primitive if a new raw value; semantic alias for use). Add
  a `[data-theme="light"]` override if it is a color that must differ per theme.
- **Add a component:** define the custom element in `components.js` (light DOM, ARIA + keyboard in
  `connectedCallback`), style it in `components.css` referencing tokens only, document it in §6.
- **After any asset edit:** recompute and update the `sha256` constant, then `cargo test -p openplotva-web`.

## 11. Testing & verification

- `cargo test -p openplotva-web` — guard tests + asset-hash integrity + auth/signature tests.
- `cargo fmt --all -- --check`, `cargo check --workspace --all-targets --all-features` — also the deploy gates.
- `tools/local-smoke.sh` (no services) for empty/error paths; admin auth locally via
  `ADMINS_ADMIN_IDS=1001 BOT_KEY=testtoken` + cookie `admin_session = "1001." +
  HMAC_SHA256(key=BOT_KEY, msg="1001")`. Assets serve from embedded bytes, so `index.html` edits need a
  rebuild to view (the runtime does not check the hash; only the test does).
- `tools/service-smoke.web-ui.spec.js` — Playwright regression (selectors + toast assertions),
  via `OPENPLOTVA_SERVICE_SMOKE_WEB_UI=1 tools/service-smoke.sh` (needs Docker).

## 12. Decisions & alternatives considered

- **Web Components (chosen) vs a JS render-helper library vs CSS-only.** Custom elements make
  "no bypassing" *mechanically* enforceable — a raw `<button>` is a one-line lint failure, whereas a
  factory function can be silently hand-rolled. The tag name *is* the enforcement token. Hybrid: custom
  elements for interactive controls, CSS classes for static surfaces.
- **Light DOM (chosen) vs Shadow DOM.** Shadow DOM would isolate styles from the global token cascade
  and `[data-theme]` switching and historically complicates Chart.js sizing. Light DOM keeps the
  lifecycle/event benefits without the cascade cost.
- **No build step (chosen) vs a bundler.** The rest of `web/` ships as embedded static assets with a
  hash gate; introducing npm/Vite would fight that model for no benefit at this scale.
- **`id` on the inner native control (chosen)** so the large body of existing `getElementById(id).value`
  code kept working unchanged through the migration.
- **`data-action` delegation (chosen) vs per-element `addEventListener` wiring.** Delegation needs no
  re-binding for dynamically rendered content and keeps handlers as named, testable functions.

## 13. Known limitations / future work

- The **Tasks (Taskman)** tab's backing JS (`loadTaskmanJobs`, …) was never defined upstream; its markup
  is on the library and `switchTab` is defensive, but the feature is non-functional until implemented.
- A few detail-pane `toggle*Details()` handlers are referenced but undefined upstream.
- The Analytics text panels remain "Loading…" on API error rather than rendering an error state.
- Pre-existing on `main` and unrelated to this system: `cargo clippy --workspace --all-targets -- -D
  warnings` is red (test-code `unwrap()` in `subscription_sync.rs`) and one `payments` unit test fails.

## 14. References

- Component/token source: `web/admin/{tokens,components}.css`, `web/admin/components.js`.
- Enforcement: `crates/openplotva-web/src/lib.rs`; `AGENTS.md`; `skills/openplotva-design-system-review/SKILL.md`.
- UX audit that motivated the work: `docs/admin-ux-audit.md`.
