# Execution outcomes and durable decisions

Status: completed record. Last reconciled with the repository on 2026-08-02.

This record replaces dated execution transcripts that had become misleading as
current instructions. Git retains their full history. Source, migrations, tests, and
the active plans in `plans/` remain the authority for current behavior.

## Durable outcomes

- The fast Rust gate runs formatting, workspace Clippy with warnings denied, and the
  workspace test suite. Admin sessions are versioned, time-bounded, HMAC-authenticated
  secure cookies; Telegram Login data is freshness-checked.
- Settings WebApp identity comes from Telegram `initData`; the public URL signature is
  only a routing hint. The current contract is documented in
  [Settings WebApp authentication](../settings-webapp-auth-design.md).
- Runtime Postgres pools have explicit acquisition, idle, and lifetime bounds. Update
  ingress recovers poisoned state without failing open, and producer error samples are
  bounded while total dropped errors remain counted.
- LLM provider calls use the low-level trace registry and the app-owned observer.
  Routing decisions have a separate journal, a shared routed-attempt walker, typed
  providers, circuit state, and capacity pools.
- The admin Memory explorer, LLM Dialogs, Context X-ray, Routing Ops, and External
  Requests dashboard are implemented through the token-backed `pl-*` design system.
- Rich Messages remain the delivery contract for the supported dialog, music, rates,
  check-in, help, and payment surfaces. Image generation deliberately uses classic
  Telegram albums and progressive `editMessageMedia`: rich media depends on external
  HTTPS fetches and is not rendered reliably across deployed Telegram clients.
- Memory extraction sends a compact existing-card projection, supports semantic
  update/merge/reinforce/demote operations, expires short-lived cards softly, and runs
  the subject merge pass for old duplicate-heavy groups.
- Retention defaults are 8 days for chat-history partitions, 7 days for Telegram file
  metadata, and 30 days for WhiteCircle checks; non-positive values disable their
  workers. The old `message_id_map` and `message_ops_queue` tables were retired, but the
  current `virtual_messages` module is a different, live outbound planning boundary and
  must not be deleted based on the old transcript.

## Plan inventory

Every plan present when this cleanup began is classified below. “Shipped” means the
current tree contains the behavior and regression evidence; it is not a deployment
claim. Plans 012–020 stay in `plans/` as the current optimization program until that
program is delivered or explicitly retired.

| Document | Classification | Current proof or disposition |
|---|---|---|
| `plans/README.md` | active | Current optimization index; reduced to live status and links. |
| `plans/001-ci-run-tests-and-clippy.md` | shipped | `.github/workflows/ci.yml` invokes `tools/rust-fast-gate.sh`. |
| `plans/002-sign-admin-session-cookie.md` | shipped | `openplotva-web` signs and verifies versioned cookies; app tests reject forged values. |
| `plans/003-postgres-pool-timeouts.md` | shipped | Runtime and critical pool builders apply explicit acquire/idle bounds. |
| `plans/004-ingress-guard-fail-closed.md` | shipped | `UpdateIngressGuard::check_update_at` recovers poisoned state; regression test exists. |
| `plans/005-bound-enqueue-error-vec.md` | shipped | Producer retains at most `MAX_ENQUEUE_ERRORS` and counts dropped samples. |
| `plans/006-constant-time-telegram-hmac.md` | shipped | Telegram HMAC and session verification use the constant-time helper. |
| `plans/007-admin-auth-date-freshness.md` | shipped | Admin login rejects stale, future, missing, and malformed `auth_date`. |
| `plans/008-bump-sqlx-to-stable.md` | shipped | Workspace and lock resolve `sqlx` 0.9.0. |
| `plans/009-settings-webapp-auth-spike.md` | historical evidence | Its decision is retained in the current Settings WebApp auth record. |
| `plans/010-settings-webapp-initdata-auth.md` | shipped | All settings gates authenticate `X-Telegram-Init-Data`; valid and invalid paths are tested. |
| `plans/011-fix-preexisting-test-failures.md` | shipped | Named regressions are fixed and the workspace suite is a live gate. |
| `plans/012-cache-prompt-registry.md` | active | Implemented locally; prompt bytes, concurrency, startup failure, and benchmark validated. |
| `plans/013-remove-dead-token-estimator.md` | active | Implemented locally after live no-op proof; deploy and smoke wiring no longer depend on it. |
| `plans/014-collapse-runtime-graphql-dtos.md` | active | Implemented locally with exact SDL golden coverage. |
| `plans/015-typed-update-router.md` | active | Safe allocation-reducing stage implemented; deeper direct dispatch stopped at error-contract drift. |
| `plans/016-runtime-worker-supervisor.md` | active | One-deadline, named atomic worker groups implemented and lifecycle-tested locally. |
| `plans/017-canonical-redacted-llm-trace.md` | active | Borrowed redacted trace artifact implemented and benchmarked locally. |
| `plans/018-canonical-telegram-outbound-command.md` | active | Versioned command and byte-compatible legacy decoder implemented; decoder removal awaits expiry proof. |
| `plans/019-retire-stale-execution-docs.md` | active | Complete locally: this record, link validation, and the completed-record gate pass. |
| `plans/020-complete-taskman-admin-ui.md` | active | Operator required the GUI; browser handlers implemented over existing REST while Runtime GraphQL remains unchanged. |

## Dated Superpowers inventory

| Removed document | Classification | Current proof or retained decision |
|---|---|---|
| `docs/superpowers/plans/2026-06-15-llm-trace-coverage.md` | shipped | `openplotva-llm/src/trace.rs` plus `RuntimeLlmObserver` own per-call tracing. |
| `docs/superpowers/plans/2026-06-24-admin-memory-redesign.md` | shipped | Memory overview/detail/mutation routes and `pl-graph`/`pl-timeline` are present and tested. |
| `docs/superpowers/plans/2026-06-27-db-bloat-remediation.md` | partially shipped, superseded | Retention workers and migrations 132–141 shipped; current outbound code supersedes its deletion instructions. |
| `docs/superpowers/plans/2026-07-03-admin-llm-dialogs.md` | shipped | `runtime_llm_runs`, run correlation/raw-body migrations, REST/GraphQL, and the LLM Dialogs UI exist. |
| `docs/superpowers/plans/2026-07-05-llm-dialogs-context-xray.md` | shipped | `TurnContextArtifact`, `record_context`, detail JSON, and the X-ray renderer exist. |
| `docs/superpowers/specs/2026-06-15-llm-trace-coverage-design.md` | shipped | Low-level trace registry and app observer replaced provider-level persistence. |
| `docs/superpowers/specs/2026-06-15-rich-messages-design.md` | partially shipped, superseded | Supported rich surfaces remain; classic image-album rollback is retained above. |
| `docs/superpowers/specs/2026-06-15-rich-messages-integration-guide.md` | historical evidence | Scenario wiring is represented by current effects/tests; image guidance was superseded. |
| `docs/superpowers/specs/2026-06-16-aifarm-embedder-design.md` | shipped | Discovery embedder, shared breaker, batch path, routing, and consolidation gating exist. |
| `docs/superpowers/specs/2026-06-24-admin-memory-redesign-design.md` | shipped | Current admin Memory implementation and design-system guards are authoritative. |
| `docs/superpowers/specs/2026-06-27-declarative-llm-routing-acceptance-spec.md` | shipped | Migrations 121–131, routing journal/reporter, enabled gates, and shared walker exist. |
| `docs/superpowers/specs/2026-07-02-model-providers-capacity-pools-design.md` | shipped | Migrations 146–147, `PoolRegistry`, derived dialog workers, schemas, and client factory exist. |
| `docs/superpowers/specs/2026-07-03-admin-llm-dialogs-design.md` | shipped | Current run buffer, endpoints, GraphQL, migrations, and UI replace the handoff. |
| `docs/superpowers/specs/2026-07-04-admin-analytics-dashboard-design.md` | shipped | `runtime_analytics_overview`, route parity, and External Requests renderers exist. |
| `docs/superpowers/specs/2026-07-05-llm-dialogs-context-xray-design.md` | shipped | Capture, in-memory retention, detail exposure, and UI graph exist. |
| `docs/superpowers/specs/2026-07-05-memory-consolidation-design.md` | shipped, partly superseded | Compact projection, semantic ops, expiry, and archival shipped; dedicated subject pass replaced the proposed off-hours shape. |
| `docs/superpowers/specs/2026-07-06-memory-subject-merge-pass-design.md` | shipped | Migrations 153–154, prompt/schema, validation, storage operations, config, and worker exist. |

## Documentation rule

`plans/` may contain unchecked task boxes because it is the active execution surface.
Completed decision records live under `docs/decisions/` and must not contain unchecked
task boxes; `tools/rust-fast-gate.sh` enforces that distinction.
