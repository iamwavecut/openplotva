---
name: openplotva-design-system-review
description: Use this skill to review OpenPlotva admin web UI changes for design-system compliance — token-only colors, no raw controls/inline handlers/inline styles, no native alert/confirm, full interaction-state coverage (loading/empty/error), and accessibility — before merging any change under web/admin/.
---

# OpenPlotva Admin Design-System Review

The admin UI (`web/admin/`) is built on a single token-driven, accessible component library:

- `web/admin/tokens.css` — the only place color/spacing/type/motion/elevation/z-index tokens are defined (primitives `--p-*`, semantic `--c-*`/`--sp-*`/`--rad-*`/`--fs-*`/`--elev-*`/`--z-*`, dark default + `[data-theme="light"]`).
- `web/admin/components.css` + `web/admin/components.js` — the `pl-*` custom elements (`pl-button`, `pl-input`, `pl-select`, `pl-textarea`, `pl-field-group`, `pl-toggle`, `pl-table`) and the `PL` runtime (`PL.toast`, `PL.alert`, `PL.confirm`, `PL.skeleton`, `PL.empty`, `PL.error`, `PL.badge`, `PL.el`). Actions are wired by `[data-action]` delegation; never inline `onclick`.
- `web/admin/admin.css` — layout + legacy structural classes only; references tokens, no color literals.

Run this review on any branch that touches `web/admin/*`.

## Rules (the contract)

All admin UI must route through the library. The following are forbidden in `web/admin/index.html`:
raw `<button>/<input>/<select>/<textarea>/<table>`, `onclick=`/`onsubmit=`, JS `el.onclick =`, inline `style="..."`, native `alert()`/`confirm()`. Use `pl-*` elements, `data-action`, token-backed classes, `PL.toast`, and `PL.confirm`. Colors live only in `tokens.css`. Every asset edit must update the matching `sha256` in `crates/openplotva-web/src/lib.rs`.

## Gate 1 — automated enforcement (must pass)

```bash
# Guard tests (raw controls / inline handlers / native dialogs / token colors) + asset-hash integrity.
cargo test -p openplotva-web

# The shipped admin JS must parse.
node --check web/admin/components.js
python3 - <<'PY'
import re; html=open('web/admin/index.html').read()
open('/tmp/_inline.js','w').write(re.findall(r'<script>(.*?)</script>', html, re.DOTALL)[-1])
PY
node --check /tmp/_inline.js

# Lint/format for the web crate.
cargo clippy -p openplotva-web --all-targets
cargo fmt --all --check
```

Quick manual greps (each must print nothing):

```bash
grep -nE 'onclick=|onsubmit=|style="|\.onclick *=' web/admin/index.html
grep -nE '[^.]alert\(|[^.]confirm\(' web/admin/index.html | grep -v 'PL\.'
grep -nE '<button|<input|<select|<textarea|<table' web/admin/index.html
grep -nE '#[0-9a-fA-F]{3,8}\b|rgba?\(' web/admin/components.css web/admin/admin.css
```

## Gate 2 — interaction-state coverage

For each async load path (`grep -n 'async function' web/admin/index.html`), confirm it:
- shows a loading state first (`PL.skeleton(...)` or `pl-table` `state = 'loading'`),
- renders an empty state on no results (`PL.empty(...)` or `pl-table` `emptyTitle`),
- renders an error state on failure (`PL.error(...)` / `table.showError(...)`), and
- gives success feedback for mutations (`PL.toast(..., 'success')`); destructive actions gate on `await PL.confirm(...)`.

## Gate 3 — accessibility

- Interactive elements are keyboard-operable with a visible `:focus-visible` ring (`--c-focus-ring`).
- `pl-toggle` exposes `role="switch"` + `aria-checked`; `pl-table` sortable headers/clickable rows are keyboard-reachable; dialogs (`pl-modal`) trap focus and restore it; toasts use `role="status"`/`alert` via `pl-toast-host` `aria-live`.
- Motion respects `prefers-reduced-motion` (defined once in `components.css`).

## Gate 4 — live verification

```bash
ADMINS_ADMIN_IDS=1001 BOT_KEY=testtoken BOT_USERNAME=SmokePlotvaBot \
OPENPLOTVA_BIND_ADDR=127.0.0.1:18099 WEBAPP_HOST=127.0.0.1 WEBAPP_PORT=18099 \
OPENPLOTVA_CONNECT_SERVICES=false OPENPLOTVA_RUN_MIGRATIONS=false \
OPENPLOTVA_CONSUME_UPDATES=false RUNTIME_API_ENABLED=false \
cargo run -p openplotva-app
# admin_session cookie = "1001." + HMAC_SHA256(key=BOT_KEY, msg="1001")  (python: hmac.new(b'testtoken', b'1001', hashlib.sha256).hexdigest())
```

Drive a browser (Playwright/Chrome MCP) to `/admin/`: switch every tab (no JS exceptions), and on a sampled data tab confirm loading→empty/error states render (no-services mode naturally returns API errors), the confirm modal appears for a destructive action, a success toast appears for a mutation, and keyboard Tab shows focus rings. The full regression spec is `tools/service-smoke.web-ui.spec.js` (run via `OPENPLOTVA_SERVICE_SMOKE_WEB_UI=1 tools/service-smoke.sh`, needs Docker).

## Acceptance

Approve only when Gate 1 is fully green, Gates 2–3 hold for the changed tabs, and Gate 4 shows the designed states with zero native dialogs. Fail the review on any forbidden pattern, any unbuilt loading/empty/error state on a new async path, any hardcoded color, or any stale asset hash.
