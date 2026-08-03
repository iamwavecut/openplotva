# Taskman Admin UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the existing Taskman admin pane without changing its REST, GraphQL, authentication, or queue contracts.

**Architecture:** The admin browser continues to use the signed-session REST facade, while Runtime GraphQL remains the bearer-authenticated operator API. Both adapters retain the existing shared `RuntimeTaskmanInspector`; a bounded frontend state object supplies rendering, paging, selection, mutation orchestration, and stale-response protection.

**Tech Stack:** Rust asset-contract tests, embedded HTML/JavaScript, `pl-*` admin components, Playwright service smoke, Axum REST, async-graphql.

## Global Constraints

- Preserve every existing Taskman REST path/method/query/response and Runtime GraphQL field.
- Use only `pl-*` controls and `PL` helpers; no inline handlers/styles, raw controls, native dialogs, or hardcoded colors.
- Render provider/job content as text, never `innerHTML`.
- Keep loading, empty, retryable error, success, destructive confirmation, and keyboard states complete.
- Do not modify `docs/CODEBASE_MAP.md`.
- Do not commit, push, open a PR, merge, or deploy without separate authorization.

---

### Task 1: Lock the missing action and browser contracts

**Files:**
- Modify: `crates/openplotva-web/src/lib.rs`
- Modify: `tools/service-smoke.web-ui.spec.js`

**Interfaces:**
- Consumes: the eight existing Taskman `data-action` names and five REST routes.
- Produces: regression tests for handler resolution and the complete operator workflow.

- [x] **Step 1: Add a failing Rust asset-contract test**

Add `admin_taskman_actions_have_handlers`, iterating over the eight action names and
requiring `ADMIN_INDEX_HTML` to contain `function <name>(` or
`async function <name>(`.

- [x] **Step 2: Run the focused Rust test and verify RED**

Run: `cargo test -p openplotva-web admin_taskman_actions_have_handlers`

Expected: FAIL because `loadTaskmanJobs` has no handler.

- [x] **Step 3: Add a failing Playwright Taskman workflow**

Mock all five `/admin/api/taskman/*` routes. Assert filter serialization, two-page
navigation, list rendering, row and direct-ID details, clipboard JSON, POST cancel,
POST restart with returned selection, DELETE clear, confirmation copy, success toasts,
and no page errors.

- [x] **Step 4: Run the browser smoke and verify RED**

Run: `OPENPLOTVA_SERVICE_SMOKE_WEB_UI=1 tools/service-smoke.sh`

Expected: FAIL because the Taskman actions are unresolved and no list request occurs.

### Task 2: Implement the Taskman browser adapter

**Files:**
- Modify: `web/admin/index.html`
- Modify: `crates/openplotva-web/src/lib.rs`

**Interfaces:**
- Consumes: `apiCall`, `PL`, the existing Taskman controls, and REST JSON shapes.
- Produces: `loadTaskmanJobs`, `clearTaskmanJobsByFilter`, `taskmanPrevPage`,
  `taskmanNextPage`, `copyTaskmanSelectedJob`, `cancelTaskmanSelectedJob`,
  `restartTaskmanSelectedJob`, `loadTaskmanJobByInput`, and `toggleTaskmanDetails`.

- [x] **Step 1: Implement filter and state helpers**

Add a single state object, filter serialization, ISO time conversion, status tones,
safe date formatting, page clamping, and monotonic list/detail generations.

- [x] **Step 2: Implement list loading and rendering**

Configure a clickable `pl-table`, show loading/empty/error states, retain selection
where valid, render summary counts, and implement clamped previous/next navigation.

- [x] **Step 3: Implement detail loading and rendering**

Validate positive IDs, fetch details, open/close the drawer, render job JSON and
payload artifacts as text, and configure accessible event/message tables.

- [x] **Step 4: Implement mutations**

Use `PL.confirm` for cancel, restart, and clear; call the existing methods; refresh
list/detail state; select `new_job_id` after restart; and show precise success toasts.

- [x] **Step 5: Update the embedded index hash**

Run: `shasum -a 256 web/admin/index.html`

Replace only the `index.html` hash in `ADMIN_ASSETS`.

- [x] **Step 6: Run focused GREEN checks**

Run:

```bash
cargo test -p openplotva-web admin_taskman_actions_have_handlers
cargo test -p openplotva-web
```

Expected: all tests pass, including asset integrity.

### Task 3: Verify behavior and reconcile optimization records

**Files:**
- Modify: `plans/020-complete-taskman-admin-ui.md`
- Modify: `plans/README.md`
- Modify: `docs/admin-ux-audit.md`
- Modify: `DESIGN.md` only if it still presents Taskman retirement as an open choice.

**Interfaces:**
- Consumes: the implemented and tested UI behavior.
- Produces: accurate local status and verification evidence; no deployment claim.

- [ ] **Step 1: Run the browser workflow GREEN**

Run: `OPENPLOTVA_SERVICE_SMOKE_WEB_UI=1 tools/service-smoke.sh`

Expected: Taskman workflow and existing browser smoke pass with zero page errors.

Environment note: the full service smoke was attempted but timed out acquiring its
Postgres pool before the browser phase on the heavily loaded host. The isolated Taskman
Playwright workflow passes against the real admin asset and mocked REST contracts; the
full service smoke remains unchecked until the service dependencies can start reliably.

- [x] **Step 2: Run the design-system review gates**

Run the commands from `skills/openplotva-design-system-review/SKILL.md`: web tests,
JavaScript parse checks, forbidden-pattern greps, focused Clippy, and formatting.

- [x] **Step 3: Run backend compatibility checks**

Run:

```bash
cargo test -p openplotva-app admin_taskman
cargo test -p openplotva-server taskman
```

Expected: REST behavior and GraphQL Taskman tests remain green.

- [x] **Step 4: Update documentation**

Replace the retirement decision with the operator-approved requirement and record the
implemented local result, exact checks, and preserved GraphQL/REST boundaries.

- [x] **Step 5: Run final focused verification**

Run:

```bash
cargo fmt --all --check
cargo clippy -p openplotva-web -p openplotva-app -p openplotva-server --all-targets -- -D warnings
git diff --check
```

Expected: exit 0 with no warnings or whitespace errors.

Final evidence additionally includes
`cargo clippy --workspace --all-targets --all-features -- -D warnings` and the full
workspace test suite, both on the completed local implementation.
