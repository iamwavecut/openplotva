# Declarative LLM Routing Acceptance Specification

Status: accepted for implementation

This document replaces the previous declarative routing handoff. It is the
acceptance gate for the implementation: the task is not complete until the
behavior, observability, cleanup, and verification criteria below are satisfied.

## Goal

Make declarative routing from the routing studio database the source of truth for
all LLM-using flows, while making routing failures visible and actionable before
behavior-changing cutovers.

The completed system must:

- Gate workflows, providers, and models through their `enabled` flags.
- Fail loudly when a route is missing, disabled, empty, exhausted, or capacity
  blocked.
- Record structured routing-layer events separately from provider round-trip
  request traces.
- Page admins only for actionable routing incidents through direct Telegram DMs.
- Route chat-family and non-chat LLM flows through the same routing policy and
  attempt semantics.
- Remove the bespoke ex-pool fallback stack after routed parity is verified.

## Locked Decisions

- Empty, disabled, or missing routes are hard routing failures. They must not
  fall back to legacy config, environment defaults, or hidden provider defaults.
- The chat-family flows use one shared routed attempt walker with flow-specific
  adapters.
- Non-chat flows get full per-kind routed adapters in this work, not a temporary
  partial migration.
- Studio edits use safe patch actions for toggles and config JSON. Full provider
  replacement must not be used for routine enablement changes.
- Provider capacity uses event-cooldown semantics.
- Admin paging is limited to actionable routing incidents.
- Routing pages are sent as direct Telegram DMs to every configured admin in
  `config.admins.admin_ids`.
- Routing events use a new additive table, `llm_routing_events`, separate from
  `llm_request_events`.

## Existing Constraints

- Provider request/response traces remain in `llm_request_events` through
  `LlmCallObserver`.
- Routing decisions, no-call failures, reload failures, and backfill failures use
  `llm_routing_events`.
- Admin Telegram reports must use the existing outbound dispatcher path. Do not
  introduce a second Telegram sending stack.
- No secrets, raw prompts, full request payloads, database dumps, Redis values,
  encrypted key material, or private file IDs may appear in routing event
  summaries or admin reports.
- Database migrations must be additive and compatible with already deployed data.
- Do not push, deploy, restart production services, or perform live cutovers
  without explicit operator approval.

## Routing Events

Add a new `llm_routing_events` table owned by storage. It must support insert,
list/query for runtime/admin diagnostics, and retention cleanup comparable to
`llm_request_events`.

Required fields:

- `id`
- `created_at`
- `severity`
- `event_type`
- `workflow_key`
- `provider_id` when known
- `model_id` when known
- queue, job, update, chat, and message context when known
- `dedupe_key`
- `summary`
- `detail` as JSONB

The schema may add extra operational fields when useful, but the required fields
above must exist or be represented without losing their meaning.

The runtime/admin API must expose enough routing-event data for operators to
answer:

- Which workflow failed?
- Why did routing fail?
- Which provider/model was involved when known?
- Was the event paged to admins or suppressed by dedupe?
- What queue/job/message context can be used to trace the incident?

## Routing Event Reporter

Add an app-owned reporter responsible for all routing-layer event reporting.

The reporter must:

- Accept typed routing event inputs from router reloads, backfills, and routed
  attempt walkers.
- Record every routing event into the in-memory diagnostics buffer.
- Persist every routing event through the Postgres recorder when storage is
  available.
- Apply admin-paging policy centrally.
- Apply dedupe suppression by `dedupe_key`.
- Use a default suppression cooldown of 10 minutes unless configuration or tests
  need a shorter value.
- Send follow-up counts when the same incident keeps firing after suppression.
- Never page admins for a single retryable provider round-trip error.

Actionable events that must page admins:

- `route_unavailable`: workflow route is missing, disabled, or unavailable.
- `no_candidates`: enabled route resolves to zero eligible candidates.
- `all_attempts_exhausted`: all routed attempts failed for a workflow.
- `circuit_open_exhaustion`: every eligible candidate was blocked by circuit
  state.
- `capacity_unavailable`: routing could not use eligible candidates because of
  capacity state.
- `router_reload_failed`: runtime router reload failed.
- `routing_backfill_failed`: seed or backfill failed.

Non-actionable events remain queryable but must not page admins unless they are
explicitly promoted by policy.

## Admin Report Shape

Admin Telegram DMs must be short and actionable. Each report should include:

- severity
- workflow key
- failure class
- provider and model when known
- affected queue, job, update, chat, or message context when known
- suppressed repeat count when applicable
- a hint to inspect runtime API `llmRequests` and routing events

Reports must not include:

- secrets or key references that could reveal secret material
- raw prompts or full LLM request payloads
- database dumps
- Redis values
- private Telegram file IDs
- unrelated user content

## Routing Gates

All declarative routing resolution must enforce:

- `model_workflows.enabled`
- `provider_registry.enabled`
- `provider_models.enabled`
- workflow kind/capability compatibility
- provider/model availability according to circuit and capacity state

Capabilities:

- `chat`
- `vision`
- `embedding`
- `image`
- `music`
- `privacy_filter`

Embedding routes additionally require `embedding_dim = 512` unless the consuming
flow is explicitly changed and tested for another dimension.

Disabled rows must behave exactly like absent rows from the caller's point of
view, except that routing events should preserve enough detail for operators to
diagnose the disabled source.

## Studio And Control Plane

Add safe patch actions for:

- `set_provider_enabled`
- `set_model_enabled`
- `set_workflow_enabled`
- `patch_provider_config`
- `patch_model_config`

Requirements:

- Toggle actions must not clear `api_key_ref`, encrypted keys, or unrelated
  provider/model fields.
- JSON patch actions must preserve valid existing config outside the patched
  keys.
- Runtime snapshots must include `provider_models.config`.
- Studio UI must expose provider, model, and workflow enabled toggles.
- Studio UI must expose compact JSON config editing for providers/models.
- Studio UI must understand the `privacy_filter` kind/capability.
- Web asset edits must update the matching `sha256` constants in
  `crates/openplotva-web/src/lib.rs`.

## Override Semantics

Expand routing overrides so declarative rows can express the same behavior as
the current bespoke paths.

Required override coverage:

- sampling parameters
- thinking/reasoning parameters
- capacity parameters
- flow-specific fields already needed by dialog/chat-family callers

Merge order:

1. `provider_models.config`
2. `workflow_assignments.inference_overrides`
3. per-request runtime overrides, if the flow already supports them

The merge is shallow unless a touched config type already has a tested deeper
merge convention.

DB-routed `model_name` values must be sent verbatim to the provider. The old
Gemini alias normalization path may remain only for environment/default
compatibility or seed/backfill compatibility. Declarative DB candidates must not
receive hidden model-name rewrites.

## Shared Routed Attempt Walker

Create one app-layer routed attempt walker for LLM routing. It should own:

- route resolution
- policy candidate selection
- retry budget handling
- circuit accounting
- capacity accounting
- routing event emission for no-call and exhausted-call failures

Flow-specific adapters should own request construction and response conversion.
They must not fork routing policy.

Chat-family flows that must use the shared walker:

- dialog
- history summary
- memory consolidation
- media prompt optimizer
- agentic text flows

Non-chat flows that must use routed adapters:

- vision
- image generation
- embeddings
- music
- redaction/privacy filtering

The shield/redaction path must use the embedding route instead of a bespoke
redaction provider path once parity is verified.

## Provider Capacity

Extend trigger support with `provider_capacity`.

Required trigger params:

```json
{
  "provider_id": 0,
  "model_id": 0,
  "cooldown_ms": 30000
}
```

`provider_id` and `model_id` must be real IDs in stored rows. The values above
are placeholders documenting the shape only.

When a routed attempt fails with `FailureReason::CapacityUnavailable`, the
walker must mark that provider/model capacity-constrained for the configured
cooldown. The trigger poller must engage overflow during that cooldown and stop
after the cooldown expires unless fresh events extend it.

## Seed And Backfill

Add an idempotent seed/backfill marker for this rollout, for example
`llm.routing.declarative_v2`.

The seed/backfill must create or update:

- ex-pool provider rows
- ex-pool model rows
- workflow assignments for ex-pool chat-family traffic
- redaction/privacy-filter provider, model, and assignment rows
- draw-api provider/model rows
- OpenRouter provider/model rows
- per-model config needed for request construction
- `provider_capacity` trigger rows

Backfill failures must emit actionable routing events and page admins through
the reporter.

Production rows must be verified before deleting bespoke fallback code.

## Cleanup After Parity

After routed parity is verified, remove the obsolete fallback stack:

- `POOL_*` runtime configuration
- `AifarmHttpPoolClient`
- `PooledClient`
- `AifarmPoolConfig`
- `PoolGatedFallbackChatProvider`
- `DialogAifarmFallbackGate`
- separate `dialog-aifarm` worker family and queue routing
- config-only resolver paths made obsolete by declarative routing
- dashboard `/admin/api/aifarm/pool*` endpoints and controls

Keep one dialog queue and one routing studio control surface.

Do not remove code only because it looks legacy. Remove it only after the new
declarative route is exercised by tests or an approved live verification path.

## Acceptance Criteria

The implementation is accepted only when all of these are true:

- A disabled workflow/provider/model cannot receive new routed traffic.
- A missing or empty route fails loudly and emits a routing event.
- Exhausted attempts emit routing events with enough context to diagnose the
  failing workflow.
- Actionable routing incidents send throttled admin DMs.
- Single retryable provider errors do not send admin DMs.
- Routing events are persisted, queryable, and cleaned up by retention.
- Router reload and seed/backfill failures emit actionable routing events.
- Studio toggles and JSON patches preserve secret/key fields.
- DB-routed model names are sent verbatim to providers.
- Chat-family flows use the shared routed attempt walker.
- Vision, image, embedding, music, and redaction/privacy-filter flows use
  declarative routed adapters.
- Provider capacity events engage overflow for the configured cooldown.
- Obsolete ex-pool fallback code is removed after parity.
- Verification commands below have been run or explicitly reported as skipped
  with the remaining risk.

## Required Tests

Add or update focused tests for:

- routing reporter dedupe and suppression behavior
- admin-DM formatting and sanitization
- route unavailable event emission
- zero-attempt/no-candidate event emission
- all-attempts-exhausted event emission
- circuit-open exhaustion event emission
- capacity-unavailable event emission
- single retryable provider errors not paging admins
- router reload failures emitting actionable routing events
- seed/backfill failures emitting actionable routing events
- `llm_routing_events` insert/list/cleanup
- enabled gates in routing resolution
- safe admin patch actions preserving key material
- provider/model config override merging
- verbatim DB-routed model names
- chat-family routed walker behavior
- per-kind non-chat routed fallback behavior
- `provider_capacity` cooldown and overflow engagement

Existing known unrelated reds should not be hidden:

- `payments::tests::successful_payment_update_builds_go_taskman_control_job`
  has failed on `main` before this routing work.
- Existing `subscription_sync.rs` unwrap clippy warnings are not part of this
  task unless touched by implementation.

If these are still present, report them as pre-existing rather than folding them
into the routing change.

## Required Verification

Run at minimum:

```bash
rtk cargo fmt --all
rtk cargo check -p openplotva-app
rtk cargo test -p openplotva-web
```

Also run focused tests for every touched routing, storage, app, and web module.
Use exact filters in the final implementation report.

`rtk cargo test -p openplotva-web` is mandatory after any `web/` asset edit.

Live verification is allowed only after explicit operator approval. When
approved, use a short-lived runtime token and verify:

- `llmRequests` still exposes provider request traces.
- New routing events expose no-call and routing-layer failures.
- Admin report delivery reaches configured admin DMs.
- Dedupe suppression prevents report storms.
- `enabled=false` stops routed traffic and fails loudly.
- `provider_capacity` engages overflow during cooldown.

## Out Of Scope Until This Spec Is Complete

- Replacing the outbound Telegram dispatcher.
- Changing prompt templates unrelated to route parity.
- Introducing a new provider SDK just for this migration.
- Building a second admin/reporting channel outside Telegram DMs.
- Deploying or restarting production without explicit approval.

