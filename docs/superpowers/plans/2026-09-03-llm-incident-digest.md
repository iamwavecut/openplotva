# LLM Incident Digest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace per-event LLM routing pages with a restart-safe rolling Telegram digest that sends at most once per hour and silently edits useful incident context between sends.

**Architecture:** PostgreSQL remains the source of routing events and gains a small per-admin delivery-state table. A pure formatter/state machine consumes aggregated one-hour snapshots; an ancillary app worker queues `sendMessage` or `editMessageText` through the existing dispatcher, whose send path records Telegram receipts back into the state table.

**Tech Stack:** Rust 1.95, Tokio, SQLx/PostgreSQL, existing OpenPlotva dispatcher and Telegram builders.

**Spec:** `docs/superpowers/specs/2026-09-03-llm-incident-digest-design.md`

## Global Constraints

- A successful new report message for one admin gates every later new report message for that admin for exactly 60 minutes; edits do not consume or bypass this gate.
- The report window is exactly 60 minutes, refresh cadence is one minute, event-writer settling delay is five seconds, delivery retry floor is five minutes, and stale in-flight timeout is ten minutes.
- Report text must stay below 4,096 Telegram bytes and must never include raw prompts, responses, provider payloads, Redis values, credentials, or arbitrary event detail.
- All report sends and edits use the existing dispatcher; no direct Telegram calls or new dependencies.
- Existing routing event, runtime GraphQL, and database contracts remain backward-compatible except for additive nullable fields/table state.
- Do not modify `docs/CODEBASE_MAP.md`.

---

### Task 1: Durable event identity and report-state storage

**Files:**
- Create: `migrations/184_llm_admin_incident_reports.up.sql`
- Create: `migrations/184_llm_admin_incident_reports.down.sql`
- Modify: `crates/openplotva-storage/src/llm_routing.rs`
- Modify: `crates/openplotva-server/src/runtime_graphql.rs`

**Interfaces:**
- Produces: `RoutingEventInput.user_id: Option<i64>` and `RoutingEventRecord.user_id: Option<i64>`.
- Produces: `PostgresRoutingAdminReportStore::snapshot(since)` returning `RoutingAdminIncidentSnapshot` groups with exact counts and bounded context samples.
- Produces: `PostgresRoutingAdminReportStore::{state, mark_pending, record_delivery}` for the app worker and dispatcher receipt path.

- [ ] **Step 1: Write failing storage tests**

Add tests that construct a `RoutingEventInput` with `user_id: Some(42)` and assert the generated insert SQL includes `user_id`; add pure row/state transition tests around a wished-for `next_admin_report_delivery_state` helper so send success records message ID/time, edit success preserves them, failures clear pending state, and terminal edit failure clears only the edit target.

```rust
#[test]
fn send_delivery_records_real_message_and_hourly_gate() {
    let next = next_admin_report_delivery_state(
        pending_state("routing-admin-send:42:1", "digest-a"),
        &AdminReportDeliveryResult::sent(Some(77), at(1_000)),
    );
    assert_eq!(next.telegram_message_id, Some(77));
    assert_eq!(next.last_new_message_at, Some(at(1_000)));
    assert_eq!(next.last_rendered_fingerprint.as_deref(), Some("digest-a"));
    assert!(next.pending_virtual_id.is_none());
}
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
env CARGO_TARGET_DIR=/private/tmp/openplotva-llm-report-55ac-target \
  rustup run 1.95.0 cargo test -p openplotva-storage llm_routing --lib
```

Expected: compilation fails because `user_id`, report types, and transition helper do not exist.

- [ ] **Step 3: Add migration 184**

The up migration adds nullable `user_id BIGINT` to `llm_routing_events` and creates:

```sql
CREATE TABLE llm_admin_report_state (
    admin_id BIGINT PRIMARY KEY,
    telegram_message_id BIGINT,
    last_new_message_at TIMESTAMPTZ,
    last_rendered_fingerprint TEXT,
    pending_virtual_id TEXT UNIQUE,
    pending_kind TEXT,
    pending_fingerprint TEXT,
    pending_started_at TIMESTAMPTZ,
    last_delivery_attempt_at TIMESTAMPTZ,
    last_delivery_error_class TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT llm_admin_report_pending_kind_check
        CHECK (pending_kind IS NULL OR pending_kind IN ('send', 'edit'))
);
```

The down migration drops the state table and then drops `llm_routing_events.user_id`. No historical routing rows are rewritten.

- [ ] **Step 4: Implement event binds/selects and aggregate snapshot query**

Thread `user_id` through single and batch inserts and routing-event reads. Add a null-safe actionable predicate:

```sql
AND COALESCE(detail->>'admin_actionable', 'true') <> 'false'
AND NOT (
    event_type = 'all_attempts_exhausted'
    AND COALESCE(detail->>'admin_actionable', '') <> 'true'
    AND COALESCE(detail->>'failed_attempts', '') = '1'
    AND COALESCE(detail->>'last_retryable_reason', '') <> ''
)
```

Aggregate by `dedupe_key`, join provider/model names, compute exact occurrence/distinct user/chat/job counts, and return at most three context-ranked samples using `row_number()` plus `jsonb_agg`. Resolve user and chat labels from `telegram_users_effective` and `telegram_chats_effective` in the same query.

- [ ] **Step 5: Implement state persistence and pure transition logic**

Use compare-by-`pending_virtual_id` updates so a stale dispatcher result cannot overwrite a newer operation. `record_delivery` loads the row, applies the tested pure transition, and persists it. A failed operation sets `last_delivery_attempt_at/error_class`; a successful one clears both.

- [ ] **Step 6: Expose additive runtime API identity**

Add nullable `user_id` to the runtime routing event GraphQL data/object conversion without renaming or removing existing fields.

- [ ] **Step 7: Run focused tests and commit**

Run the storage and server routing tests, then:

```bash
git add migrations/184_llm_admin_incident_reports.* \
  crates/openplotva-storage/src/llm_routing.rs \
  crates/openplotva-server/src/runtime_graphql.rs
git commit -m "feat: persist LLM incident digest state"
```

### Task 2: Route complete user context into actionable events

**Files:**
- Modify: `crates/openplotva-app/src/runtime_routing.rs`
- Modify: `crates/openplotva-app/src/routed_attempts.rs`
- Modify: `crates/openplotva-app/src/dialog_runtime.rs`
- Modify: `crates/openplotva-app/src/dialog_jobs/retry.rs`
- Modify: user-scoped routed media/agent call sites under `crates/openplotva-app/src/`

**Interfaces:**
- Consumes: storage `RoutingEventInput.user_id`.
- Produces: `RoutingEvent.user_id` / `RoutingEventData.user_id` and `RoutedRequestContext.user_id`.
- Produces: `RoutingEventReporter::new(buffer, recorder, report_trigger)`; `record` stores every event and wakes the worker only for actionable events.

- [ ] **Step 1: Replace notifier tests with failing aggregation-trigger tests**

Test that an actionable event wakes a `Notify`, is annotated as `admin_report.action = "aggregated"`, and retains `user_id`; test that explicit non-actionable and one-attempt retryable exhaustion events record `action = "none"` and do not wake.

- [ ] **Step 2: Run the focused app test and verify RED**

```bash
env CARGO_TARGET_DIR=/private/tmp/openplotva-llm-report-55ac-target \
  rustup run 1.95.0 cargo test -p openplotva-app runtime_routing --lib
```

Expected: compile/assertion failure against the current notifier/cooldown API.

- [ ] **Step 3: Implement the minimal recorder/trigger seam**

Remove per-dedupe suppression and inline admin formatting from
`runtime_routing.rs`. Keep the existing actionable policy as a public
`is_admin_actionable_event(&RoutingEvent) -> bool` helper used by annotations
and storage-query tests. Notify after enqueueing the durable event.

- [ ] **Step 4: Populate user identity at owned call sites**

Set `RoutedRequestContext.user_id` from `DialogInput.user.id`, image/music job
or request user IDs, and agent origins. Set the terminal dialog retry event
from `DialogJobParams.user_id`. Leave background-only contexts null rather than
inventing identity.

- [ ] **Step 5: Run focused tests and commit**

Run `runtime_routing`, `routed_attempts`, dialog runtime, and dialog retry tests;
commit with:

```bash
git commit -am "feat: add user context to routing incidents"
```

### Task 3: Pure adaptive formatter and hourly decision engine

**Files:**
- Create: `crates/openplotva-app/src/routing_admin_reports.rs`
- Modify: `crates/openplotva-app/src/lib.rs` (module declaration only in this task)

**Interfaces:**
- Consumes: `RoutingAdminIncidentSnapshot` and `RoutingAdminReportState`.
- Produces: `format_incident_digest(snapshot, now) -> FormattedDigest` with text/fingerprint/latest occurrence.
- Produces: `plan_admin_report_delivery(state, digest, now) -> AdminReportDeliveryPlan::{None, Send, Edit}`.

- [ ] **Step 1: Write failing formatter tests**

Use literal snapshots to assert user-impact groups rank before high-volume
background groups; the header, cause, provider/model, counts, identities,
pipeline, and timestamps are present; arbitrary detail is absent; an oversized
snapshot stays below 3,900 bytes and reports omitted groups.

- [ ] **Step 2: Write failing state-machine tests**

Cover first active send, same-fingerprint no-op, edit inside 60 minutes,
rotation at exactly 60 minutes only when a newer active occurrence exists,
lost edit target waiting for the send gate, five-minute failure backoff,
in-flight suppression, ten-minute stale pending recovery, recovered edit, and
empty snapshot/no-message no-op.

- [ ] **Step 3: Run the new module tests and verify RED**

```bash
env CARGO_TARGET_DIR=/private/tmp/openplotva-llm-report-55ac-target \
  rustup run 1.95.0 cargo test -p openplotva-app routing_admin_reports --lib
```

Expected: compilation fails because the module API is not implemented.

- [ ] **Step 4: Implement formatter and plan logic**

Use only standard-library string building plus existing `sha2` and
`openplotva_observability::secrets::redact`. Map common workflow/event/reason
codes to concise Russian labels while retaining exact codes. Append whole
sections until the 3,900-byte ceiling; truncate individual dynamic fields on a
UTF-8 boundary.

- [ ] **Step 5: Run tests and commit**

```bash
git add crates/openplotva-app/src/routing_admin_reports.rs crates/openplotva-app/src/lib.rs
git commit -m "feat: format adaptive LLM incident digests"
```

### Task 4: Worker, dispatcher receipts, and runtime wiring

**Files:**
- Modify: `crates/openplotva-app/src/routing_admin_reports.rs`
- Modify: `crates/openplotva-app/src/lib.rs`

**Interfaces:**
- Consumes: `PostgresRoutingAdminReportStore`, dispatcher queue, report `Notify`, configured admin IDs, and runtime stop signal.
- Produces: `run_routing_admin_report_worker_until(...)`.
- Produces: `record_routing_admin_dispatch_result(store, &DispatchSendReport)` called by dispatcher workers before returning their status.

- [ ] **Step 1: Write failing queue/receipt tests**

Use a real in-memory `DispatcherQueue` and fake store boundary to prove a first
refresh enqueues one `SendMessage`, a receipt records message ID 77, a changed
snapshot enqueues `EditMessageText` for ID 77, and repeated refreshes neither
send nor edit identical content.

- [ ] **Step 2: Run the focused tests and verify RED**

Run the `routing_admin_reports` and dispatcher-send test filters. Expected:
missing worker/store interfaces.

- [ ] **Step 3: Implement bounded refresh worker**

On startup refresh once. Thereafter select over a one-minute delayed interval,
actionable `Notify`, and stop. Coalesce repeated notifications during the
five-second writer settling delay. For each admin, clear stale pending state,
load state, calculate a pure plan, persist pending before queueing, then enqueue
one namespaced protected immediate dispatcher command with a unique debounce
key.

- [ ] **Step 4: Record report receipts in the dispatcher path**

Add the report store to `DispatcherWorkerGroupInputs`. After
`send_work_item_with_history_and_ephemeral` returns, inspect only virtual IDs
with the `routing-admin-report:` prefix and await the compare-by-ID state
transition. A state-write failure is logged but does not rewrite Telegram send
status.

- [ ] **Step 5: Wire startup and shutdown ordering**

Create one shared `Notify` before the early routing reporter. After the
dispatcher queue exists, register the report worker in the Processor phase,
pass the report store to both dispatcher workers, remove the old notifier
reconstruction, and add a readiness entry describing the 60-minute window.

- [ ] **Step 6: Run focused tests and commit**

Run all app report/routing/dispatcher tests plus storage report tests; commit:

```bash
git commit -am "feat: deliver restart-safe hourly LLM digests"
```

### Task 5: Verification and delivery

**Files:**
- Modify only files required by failures found in this task.

**Interfaces:**
- Consumes: all previous task outputs.
- Produces: one ready PR, merged `main`, production deployment, and affected-path proof.

- [ ] **Step 1: Run formatting and focused tests**

```bash
rustup run 1.95.0 cargo fmt --all -- --check
env CARGO_TARGET_DIR=/private/tmp/openplotva-llm-report-55ac-target \
  rustup run 1.95.0 cargo test -p openplotva-storage llm_routing --lib
env CARGO_TARGET_DIR=/private/tmp/openplotva-llm-report-55ac-target \
  rustup run 1.95.0 cargo test -p openplotva-app routing_admin_reports --lib
env CARGO_TARGET_DIR=/private/tmp/openplotva-llm-report-55ac-target \
  rustup run 1.95.0 cargo test -p openplotva-app runtime_routing --lib
```

- [ ] **Step 2: Run repository gates**

```bash
env CARGO_TARGET_DIR=/private/tmp/openplotva-llm-report-55ac-target \
  rustup run 1.95.0 cargo clippy --workspace --all-targets -- -D warnings
env CARGO_TARGET_DIR=/private/tmp/openplotva-llm-report-55ac-target \
  rustup run 1.95.0 cargo test --workspace
```

- [ ] **Step 3: Push and open a ready PR**

Push `feat/llm-incident-digest`, open a ready PR into `main`, and include the
pre-change production evidence (44,657 events / 237 sent reports in 24 hours),
the behavioral contract, migration compatibility, privacy limits, and exact
local commands/results.

- [ ] **Step 4: Complete the PR delivery loop**

Poll checks, issue comments, review summaries/decisions, full persistent bot
comment bodies, inline review comments, and unresolved threads. Fix or rebut
every finding, rerun affected checks, resolve every inline thread, then merge
only when the exact head is green and mergeable.

- [ ] **Step 5: Deploy and prove production behavior**

Dispatch `.github/workflows/deploy-production.yml` on merged `main`; wait for
terminal success. Verify exact running image, migration 184, restart/OOM state,
health/readiness, fresh logs, worker presence, and report state. During a
bounded natural-event soak, prove one successful send receipt followed by a
changed fingerprint with the same Telegram message ID and unchanged
`last_new_message_at`; do not inject a fake user-facing incident if production
is quiet.

- [ ] **Step 6: Clean task-owned artifacts**

After all checks finish, verify `/private/tmp/openplotva-llm-report-55ac-target`
is the task-owned target and remove it. Confirm the worktree is clean and the
remote `main` ref equals the merged commit.
