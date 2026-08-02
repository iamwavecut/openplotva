# Plan 020: Retire the broken duplicate Taskman admin facade

## Status

- Priority: P1
- Effort: M
- Risk: MED
- Planned at: whole-repository audit, 2026-08-02
- Depends on: live REST-consumer proof and operator UX decision

## Why

The admin Tasks pane exposes eight `data-action` names with no implementation:
`loadTaskmanJobs`, clear, pagination, copy, cancel, restart, and direct load. Static
resolution across all 65 admin actions leaves exactly those eight unresolved. The pane
therefore cannot provide its advertised operator workflow.

Despite the dead frontend, `openplotva-app` keeps five `/admin/api/taskman/*` routes,
their auth/method shell, filter parser, JSON serializers, action handlers, parity tests,
and roughly 191 lines of Taskman pane markup. No tracked caller consumes those REST
routes. The Runtime GraphQL API already owns tested `taskmanJobs`, `taskmanJob`, and
`taskmanQueueDiagnostics` queries, and the runtime API skill documents them. Keeping a
second, nonfunctional facade adds about a thousand lines of maintenance and security
surface without a working business path.

## Change

1. Before editing, inspect a representative production access-log window for all five
   `/admin/api/taskman/*` paths. Identify any external client, not only the shipped UI.
2. Confirm with the operator that Runtime GraphQL is the supported Taskman diagnostic
   path. If a working graphical Taskman surface is required, stop and implement the eight
   handlers in a separate feature instead of deleting the facade.
3. Remove the Tasks navigation item, pane markup, Taskman-only CSS/state, and unresolved
   action references from `web/admin/index.html` and related admin assets.
4. Remove the five admin REST routes and parity-list entries, handler shell, filter/query
   parser, JSON adapters, and tests that exist only for this duplicate facade.
5. Preserve `openplotva-server` Taskman GraphQL contracts, inspectors, queue semantics,
   cancel/restart behavior exposed elsewhere, and the runtime API skill examples.
6. Update current admin/design documentation to stop advertising the broken tab; do not
   describe the deleted REST routes as supported contracts.

## Verification

- Production evidence shows zero consumers of the five REST paths over the agreed window.
- A static `data-action` check resolves every remaining admin action.
- `rg '/admin/api/taskman/'` finds no shipped route or asset reference.
- Runtime GraphQL Taskman tests and representative query JSON remain unchanged.
- `cargo test -p openplotva-server`, `cargo test -p openplotva-web`, app route-parity
  tests, admin UI smoke, formatting, and workspace Clippy pass.
- Target at least 700 net LOC removed; report markup, app, test, and CSS reductions
  separately.

## STOP conditions

- Any live or external client uses an admin Taskman REST route.
- The operator requires Taskman actions in the admin GUI rather than the Runtime API.
- Removing the facade changes queue ordering, cancellation/restart semantics, GraphQL
  shape, or persisted Taskman data.
