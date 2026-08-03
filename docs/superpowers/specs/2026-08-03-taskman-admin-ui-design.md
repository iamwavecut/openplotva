# Taskman Admin UI Design

## Problem

The admin Tasks pane ships complete controls but its eight `data-action` handlers do
not exist. The authenticated REST facade and Runtime GraphQL API both already delegate
to the same `RuntimeTaskmanInspector`, so the missing product surface is browser-side
orchestration, not queue or API functionality.

## Decision

Keep both supported adapters over the shared inspector:

- the browser admin uses `/admin/api/taskman/*` with the existing signed admin session;
- the operator Runtime API keeps `taskmanJobs`, `taskmanJob`, and queue diagnostics over
  its existing bearer-authenticated GraphQL endpoint.

The browser must not call Runtime GraphQL directly. That would require exposing a
runtime bearer token or adding a second browser authentication path while providing no
new capability.

## Browser state and flow

One small `taskmanState` object owns the current list response, offset, limit, selected
job details, and request generations. A reset search returns to offset zero; refresh
preserves the current page. Previous and next navigation clamp to the valid result
window.

The list request serializes only non-empty controls into the existing filter contract:
`q`, `queue`, `status`, `chat_id`, `user_id`, `time_field`, `from`, `to`, `sort_by`,
`sort_dir`, `offset`, and `limit`. Local `datetime-local` values are converted to ISO
timestamps. The response renders as an accessible clickable `pl-table` with status,
queue, identity, preview/title, and creation time.

Selecting a row or loading a positive job ID fetches `/taskman/job?id=…`, opens the
existing split-pane drawer, and renders job JSON, events, messages, and payload
artifacts using text nodes or `pl-table`; provider data is never inserted as HTML.

## Mutations

- Copy writes the selected detail response as formatted JSON and reports success or
  failure with `PL.toast`.
- Cancel requires `PL.confirm`, calls `POST /taskman/job/cancel?job_id=…`, and reloads
  the current list and selected job.
- Restart requires `PL.confirm`, calls `POST /taskman/job/restart?job_id=…`, then selects
  the returned `new_job_id` and reloads the list.
- Clear Filtered requires a danger confirmation containing the current filter and
  matched total. If status is empty or active, the copy explicitly warns that pending
  or processing jobs can be deleted. It calls `DELETE /taskman/jobs/clear?...`, reports
  matched/deleted/active counts, resets to the first page, and reloads.

The UI preserves existing backend semantics; it does not silently restrict or rewrite
operator filters.

## Interaction states and concurrency

Every load shows a skeleton or `pl-table` loading state, an explicit empty state, and a
retryable error state. Mutations show success feedback. List and detail requests use
monotonic generation counters so a slow earlier response cannot overwrite a newer
filter, page, or selection.

Buttons that require a selected job are disabled until details are loaded. List rows
and drawer tables remain keyboard-accessible through the existing design system.

## Compatibility and non-goals

No REST path, method, query parameter, response field, GraphQL schema, queue ordering,
job lifecycle, persisted data, or authentication contract changes. No Taskman redesign,
new dependency, new backend endpoint, or direct database access is introduced.

## Verification

- A Rust asset-contract test fails if any Taskman `data-action` loses its handler.
- A Playwright flow covers list/filter/pagination, detail-by-row and ID, copy, cancel,
  restart, clear confirmation, mutation methods, and zero page errors using mocked
  Taskman responses.
- Existing app REST and Runtime GraphQL tests remain green.
- The admin design-system gates, asset hash, JavaScript parse check, formatting,
  focused Clippy, and relevant browser smoke pass.
