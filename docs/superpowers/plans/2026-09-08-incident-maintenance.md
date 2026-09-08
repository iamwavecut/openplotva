# Incident maintenance implementation plan

Implement the user-approved incident → OMP diagnosis → GitHub issue → isolated
OMP repair → reviewed PR workflow. The implementation remains disabled until an
operator provisions credentials and enables it. Merge and production deployment
remain manual. No production data or credentials belong in GitHub artifacts.

## Global constraints

- Repository: iamwavecut/openplotva; authorized GitHub user: iamwavecut (239034).
- OMP 18.1.14, GLM 5.3, Rust 1.95.0; no automatic paid-provider fallback.
- One active agent, 30 initial / 10 deep starts per rolling 24 hours.
- Initial diagnosis: 600 seconds; deep work: 14,400 active seconds, 5 feedback rounds.
- Runtime: geta.moe, GitHub-hosted dispatch over restricted SSH.
- Worker: non-root, 2 CPU, 4 GiB RAM, 8 GiB workspace, no host mounts or Docker socket.
- Before start: 6 GiB available RAM, 8 GiB disk reserve beyond workspace allocation.
- All GitHub and notification publication is performed by trusted controller code.
- Unknown and no-fix results leave issues open for the operator.
- Existing Telegram report behavior and current runtime GraphQL privileges are preserved.

### Task 1: Durable sanitized incident storage

Own only `crates/openplotva-storage/src/maintenance.rs`, its module export in
`crates/openplotva-storage/src/lib.rs`, and migration 185 up/down. Do not modify
app, server, configuration, Python, or workflows.

Implement `MaintenanceStore::new(PgPool)`, `capture(now: OffsetDateTime) ->
Result<u64, StorageError>`, `incidents(after: i64, limit: i64) ->
Result<Vec<MaintenanceIncident>, StorageError>`, and
`evidence(incident_id: i64) -> Result<Option<serde_json::Value>, StorageError>`.
`MaintenanceIncident` serializes `{id, signature, first_seen, last_seen,
snapshot}` with timestamps as Unix seconds. An immutable outbox entry represents
a terminal llm_routing_events row; a unique source-event constraint makes captures
idempotent. Signature groups technical event type, workflow, route, queue, and
known reason, excluding identities, counts, and timestamps. Store internal source
mapping privately. Capture fresh user-facing actionable terminal failures in the
last five minutes, preserving the existing single retryable-attempt suppression;
background and explanatory attempt events do not independently trigger runs.

Never project arbitrary summary/detail, user/chat/message IDs, names, prompts,
responses, provider payloads, URLs, or secrets. Use allowlisted reason codes and
bounded technical catalog model/provider names. Correlate explanatory attempts
using existing workflow/chat/message/time relationships internally. Return
opaque incident-scoped references for linked attempts/jobs if required.
Evidence includes bounded sanitized route attempts, task state/queue counters,
database pool/lock aggregate counters; unavailable sections are explicit, never
raw SQL or raw row output. Read existing schemas before querying.

Add meaningful pure redaction/signature tests and live Postgres capture, repeat,
pagination, evidence-scope tests using the existing test DSN pattern. Use a
test-first cycle; report commands and results. Root coordinates Cargo compilation.
Do not commit, publish, deploy, or spawn subagents. Report changed files, exact
interfaces, verification, and any genuine limitations to the controller.

### Task 2: Controller and GitHub lifecycle

Implement Python standard-library controller, durable SQLite state and effects,
safe diagnosis contracts, semantic history context, owner-gated dispatch,
GitHub publication/reconciliation, review handling, quotas, cancellation and
notification retries. Tests must cover real state transitions and crash windows.
Keep external command and HTTP boundaries injectable for representative offline
end-to-end tests. Container execution is Task 3's runner interface.

### Task 3: Isolated runner and operator packaging

Implement the OMP container runner, trusted prompts/configuration, resource and
time enforcement, restricted temporary diagnostic/provider access, fixed-size
workspace, systemd units, SSH ingress, GitHub dispatch workflow, installation
preflight and operator documentation. Implement and test actual executable
paths; do not substitute unchecked shell fragments or placeholder hooks.

### Task 4: Runtime integration and verification

Add validated opt-in maintenance settings, independently supervised incident
capture, dedicated authenticated maintenance HTTP routes, and notification
delivery through the existing dispatcher with durable delivery receipts.
Do not expose the broad runtime GraphQL token to the worker. Integrate all
contracts, run focused and broad checks, independently review security and
recovery boundaries, and document local versus not-yet-enabled production state.
