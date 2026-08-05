# Memory Routing Cascades Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add independent dialog, memory-extraction, and subject-merge cascades with a serialized shared GPU2 capacity pool.

**Architecture:** Migration 179 defines the canonical workflow rows, assignments, and GPU2 pool for upgraded databases. The app routes extraction and subject merge through separate `RoutedAttemptWalker` contexts, while startup backfills use the new extraction key so fresh databases converge on the same state.

**Tech Stack:** Rust 1.95, tokio, SQLx/Postgres, OpenPlotva LLM router, AIFarm Discovery.

## Global Constraints

- Preserve Telegram, HTTP, GraphQL, prompt, taskman queue, environment-variable, and persisted-memory contracts.
- Do not modify `docs/CODEBASE_MAP.md`.
- Do not deploy production as part of this plan.
- Use migration number 179 with reversible up/down SQL.
- Implement each behavior test-first.

---

### Task 1: Canonical routing migration

**Files:**
- Create: `migrations/179_memory_routing_cascades.up.sql`
- Create: `migrations/179_memory_routing_cascades.down.sql`
- Modify: `crates/openplotva-storage/src/lib.rs`

**Interfaces:**
- Consumes: existing `workflows`, `workflow_assignments`, `llm_providers`, `provider_models`, and `llm_capacity_pools` schemas.
- Produces: `dialog`, `memory_extraction`, and `memory_subject_merge` routes plus pool `aifarm-gpu2-qwen27b`.

- [ ] **Step 1: Write a failing migration contract test**

Add a storage test that includes both migration files and asserts these exact settings:

```rust
assert!(UP.contains("('memory_extraction', 'chat', FALSE, 4, 900000, TRUE)"));
assert!(UP.contains("('memory_subject_merge', 'chat', FALSE, 2, 900000, TRUE)"));
assert!(UP.contains("retry_max_hops = 5"));
assert!(UP.contains("max_concurrency = 1"));
for model in ["vibethinker-3b", "ternary-bonsai-27b"] {
    assert!(UP.contains(model));
}
```

- [ ] **Step 2: Run the test and confirm failure**

Run: `CARGO_TARGET_DIR=/tmp/openplotva-memory-routing-cascades-target cargo test -p openplotva-storage memory_routing_cascades_migration_defines_independent_workflows_and_pool`

Expected: compilation fails because migration 179 does not exist.

- [ ] **Step 3: Implement migration 179**

The up migration must:

```sql
INSERT INTO workflows (key, kind, full_routing, retry_max_hops, retry_wall_ms, enabled)
VALUES
    ('memory_extraction', 'chat', FALSE, 4, 900000, TRUE),
    ('memory_subject_merge', 'chat', FALSE, 2, 900000, TRUE)
ON CONFLICT (key) DO UPDATE SET
    kind = EXCLUDED.kind,
    full_routing = EXCLUDED.full_routing,
    retry_max_hops = EXCLUDED.retry_max_hops,
    retry_wall_ms = EXCLUDED.retry_wall_ms,
    enabled = TRUE;

UPDATE workflows
SET retry_max_hops = 5, retry_wall_ms = 180000
WHERE key = 'dialog';
```

It then orders dialog fallbacks as Bonsai `0` and Gemini `99`, disables the historical Gemini/Genkit overflow edge, reconciles `aifarm-gpu2-qwen27b` at one slot without claiming an operator-owned same-name pool, records prior model pool assignments, inserts the exact ordered chains from the design, and removes the historical `memory_consolidation` workflow after the replacement route exists. The down migration restores only marker-owned state as described in the design.

- [ ] **Step 4: Run the focused storage tests**

Run: `CARGO_TARGET_DIR=/tmp/openplotva-memory-routing-cascades-target cargo test -p openplotva-storage memory_routing_cascades -- --nocapture`

Expected: all matching tests pass; the optional PostgreSQL test skips when its DSN is absent.

### Task 2: Separate extraction and subject-merge adapters

**Files:**
- Modify: `crates/openplotva-app/src/memory_runtime.rs`
- Modify: `crates/openplotva-app/src/lib.rs`

**Interfaces:**
- Consumes: `RoutedAttemptWalker`, `AifarmMemoryExtractor`, `MemoryExtractor`, and `SubjectMerger`.
- Produces: `MEMORY_EXTRACTION_WORKFLOW_KEY`, `MEMORY_SUBJECT_MERGE_WORKFLOW_KEY`, and `RoutedSubjectMerger`.

- [ ] **Step 1: Write failing adapter-boundary tests**

Add compile-time and key assertions:

```rust
fn assert_subject_merger<T: openplotva_memory::SubjectMerger>() {}

#[test]
fn routed_memory_adapters_use_independent_workflows() {
    assert_subject_merger::<RoutedSubjectMerger>();
    assert_eq!(MEMORY_EXTRACTION_WORKFLOW_KEY, "memory_extraction");
    assert_eq!(MEMORY_SUBJECT_MERGE_WORKFLOW_KEY, "memory_subject_merge");
}
```

- [ ] **Step 2: Run the test and confirm failure**

Run: `CARGO_TARGET_DIR=/tmp/openplotva-memory-routing-cascades-target cargo test -p openplotva-app routed_memory_adapters_use_independent_workflows`

Expected: compilation fails because `RoutedSubjectMerger` and the workflow constants do not exist.

- [ ] **Step 3: Implement the routed subject merger**

Add a cloneable adapter with the same walker/config ownership as extraction:

```rust
#[derive(Clone)]
pub struct RoutedSubjectMerger {
    walker: RoutedAttemptWalker,
    config: AppConfig,
}
```

Its `SubjectMerger::merge_subject` implementation must run `MEMORY_SUBJECT_MERGE_WORKFLOW_KEY`, use `MEMORY_CONSOLIDATION_QUEUE_NAME` for observability, call an AIFarm extractor configured from the selected attempt, and map attempt/routing errors through a dedicated typed error. Change `RoutedMemoryExtractor` to use `MEMORY_EXTRACTION_WORKFLOW_KEY`.

- [ ] **Step 4: Wire the composition root**

Replace the direct `subject_merger_from_app_config(config)` construction with `routed_subject_merger_from_app_config(config, walker)` using the same router handle, breakers, triggers, pools, OpenRouter gate, and event reporter pattern as extraction.

- [ ] **Step 5: Run focused app tests**

Run: `CARGO_TARGET_DIR=/tmp/openplotva-memory-routing-cascades-target cargo test -p openplotva-app routed_memory_adapters_use_independent_workflows`

Expected: pass.

### Task 3: Fresh-database convergence and operator documentation

**Files:**
- Modify: `crates/openplotva-app/src/model_routing.rs`
- Modify: `.env.example`

**Interfaces:**
- Consumes: startup model-routing seed/backfill functions and current memory environment configuration.
- Produces: fresh installations that independently converge ready workflow subsets in one transaction, and comments explaining that environment values are bootstrap defaults behind routed workflows.

- [ ] **Step 1: Write failing backfill-key tests**

Update/add model-routing tests so the memory backfill target is asserted as `memory_extraction`, subject merge resolves without a vram.cloud catalog, and a forced mid-backfill failure preserves the pre-existing route and leaves no guard.

- [ ] **Step 2: Run the focused test and confirm failure**

Run: `CARGO_TARGET_DIR=/tmp/openplotva-memory-routing-cascades-target cargo test -p openplotva-app memory_vram -- --nocapture`

Expected: the new key assertion fails against the historical constant.

- [ ] **Step 3: Update seed, backfill, and worker derivation keys**

Replace only LLM workflow references with `memory_extraction`. Keep taskman queue/job keys named `memory_consolidation`. Normalize dialog, extraction, and subject merge under independent setting guards inside one SQL transaction, including the shared-pool reconciliation. Update the `.env.example` memory comments to state that the configured provider/model bootstrap the routed extraction clients and do not collapse the two workflow rows.

- [ ] **Step 4: Run focused tests**

Run: `CARGO_TARGET_DIR=/tmp/openplotva-memory-routing-cascades-target cargo test -p openplotva-app memory_vram -- --nocapture`

Expected: pass.

### Task 4: Verification and ready PR

**Files:**
- Verify all changed files.

**Interfaces:**
- Consumes: Tasks 1-3.
- Produces: a reviewable branch and ready PR into `main`.

- [ ] **Step 1: Format and inspect the diff**

Run: `cargo fmt --all`

Run: `git diff --check`

Expected: both succeed with no formatting errors.

- [ ] **Step 2: Run focused tests**

Run: `CARGO_TARGET_DIR=/tmp/openplotva-memory-routing-cascades-target cargo test -p openplotva-storage memory_routing_cascades -- --nocapture`

Run: `CARGO_TARGET_DIR=/tmp/openplotva-memory-routing-cascades-target cargo test -p openplotva-app routed_memory_adapters_use_independent_workflows`

Expected: pass.

- [ ] **Step 3: Run repository-required checks**

Run: `CARGO_TARGET_DIR=/tmp/openplotva-memory-routing-cascades-target cargo clippy --workspace --all-targets -- -D warnings`

Run: `CARGO_TARGET_DIR=/tmp/openplotva-memory-routing-cascades-target cargo test --workspace`

Expected: pass.

- [ ] **Step 4: Commit and open the ready PR**

Commit message: `Route memory workflows through independent cascades`

Push `feat/memory-routing-cascades` and open a ready PR into `main` describing the migration, routed subject-merger boundary, shared GPU2 pool, and verification results.

- [ ] **Step 5: Complete the CI/review loop**

Poll checks, issue comments, review summaries, inline comments, and edited bot bodies. Fix or rebut every actionable finding, resolve every inline thread, and stop with a fully green ready PR. Do not deploy production.
