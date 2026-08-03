# Admin panel UX audit outcome

Status: reconciled with the current admin console on 2026-08-02.

The original audit found blocking native dialogs, unsafe destructive confirmations,
missing load/empty/error states, inconsistent one-off styling, inaccessible controls,
and duplicated tables and badges. The token layer, `pl-*` component library, action
delegation, accessible `PL.confirm`, non-blocking toasts, and build-failing asset guards
closed those systemic findings.

Current proof lives in `web/admin/tokens.css`, `web/admin/components.css`,
`web/admin/components.js`, and the guard tests in `crates/openplotva-web/src/lib.rs`.
The canonical authoring contract is [DESIGN.md](../DESIGN.md).

## Resolved product defect

The Tasks (Taskman) tab originally referenced eight actions that had no implementation.
The operator confirmed that the graphical surface is required, so Plan 020 completed the
existing browser adapter instead of retiring it. The tab now supports filters, offset
paging, list and direct-ID details, events/messages, clipboard JSON, cancel, restart,
and filtered clear with explicit active-job warnings. Loading, empty, retryable error,
success, destructive-confirmation, stale-response, and keyboard states route through
the existing `pl-*` design system.

The browser retains signed-session REST authentication while the bearer-authenticated
Runtime GraphQL API remains unchanged. Both adapters use the shared Taskman inspector;
no queue or persistence contract was duplicated.

The previously reported detail-close and Analytics error-state defects are closed: the
unified drawer supplies close handlers, and the External Requests dashboard renders
retryable `PL.error` states.
