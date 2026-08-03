# Plan 020: Complete the Taskman admin UI

## Status

- Priority: P1
- Effort: M
- Risk: MED
- Planned at: whole-repository audit, 2026-08-02
- Operator decision: the graphical admin surface is required
- Local status: implemented and validated locally, 2026-08-03; full service smoke is
  environment-blocked before the browser phase

## Why

The admin Tasks pane shipped with filters, paging controls, a details drawer, and
cancel/restart/clear buttons, but its eight `data-action` handlers had never existed in
the available Git history. The underlying five authenticated REST routes and Runtime
GraphQL Taskman queries were functional and already delegated to the same
`RuntimeTaskmanInspector`.

The original retirement proposal treated the nonfunctional REST/UI facade as removable
duplication. The operator decision resolves its STOP condition in the opposite direction:
Taskman is a required graphical operator workflow, so the facade must be completed rather
than removed.

## Implemented change

1. Keep the browser on the signed-session `/admin/api/taskman/*` REST facade. Keep the
   bearer-authenticated Runtime GraphQL API unchanged; do not expose its token to the
   browser or add a second browser auth path.
2. Implement filter serialization, safe ISO time conversion, result summaries, accessible
   `pl-table` rendering, offset paging, and stale-response suppression.
3. Implement row and direct-ID detail loading with safe text-only rendering for job JSON,
   events, messages, and payload/status badges. Keep direct-ID lookup accessible before a
   list row has been selected, and clear stale details when a lookup fails.
4. Implement clipboard copy plus confirmed cancel, restart, and filtered clear. Clear
   explicitly warns when the filter can delete pending or processing jobs and refuses to
   run unless the visible filters match the last successful list snapshot.
5. Preserve all REST paths, methods, query parameters, JSON fields, GraphQL schema, queue
   ordering, job lifecycle behavior, and persisted meaning.
6. Repair browser smoke authentication by passing the signed admin cookie that
   `tools/service-smoke.sh` already generates instead of the obsolete bare user ID.

## Verification evidence

- TDD RED: the new asset test failed on missing `loadTaskmanJobs`; targeted Playwright
  failed on the empty Taskman list before the handlers were added.
- `cargo test -p openplotva-web`: 20 passed.
- `cargo test -p openplotva-app admin_taskman`: 2 passed.
- `cargo test -p openplotva-server taskman`: 1 passed, preserving Runtime GraphQL shape.
- Targeted Taskman Playwright: 1 passed, covering filters, paging, initially accessible
  direct-ID lookup, row details, failed-detail cleanup, clipboard, cancel, restart, clear,
  HTTP methods, confirmations, toasts, stale-filter protection, and page errors.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed on
  the final local tree.
- JavaScript parse checks, design-system forbidden-pattern checks, `cargo fmt --all -- --check`,
  `bash -n tools/service-smoke.sh`, and `git diff --check`: passed.
- `tools/rust-fast-gate.sh --skip-clippy`: passed, including `cargo test --workspace`.
- The full `OPENPLOTVA_SERVICE_SMOKE_WEB_UI=1 tools/service-smoke.sh` was attempted after
  repairing its signed-cookie input, but the loaded host repeatedly timed out acquiring the
  Postgres pool before reaching the browser phase. The current Taskman browser workflow was
  therefore validated independently against its real static asset and mocked REST contracts;
  a full service smoke still needs a healthy local Postgres/host window.

## Compatibility boundary

Taskman browser REST and Runtime GraphQL are two authenticated adapters over one shared
inspector, not competing sources of truth. Future changes must keep their data semantics
aligned while allowing transport-specific casing and authentication to remain at their
own boundaries.
