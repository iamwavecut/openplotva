# Memory Routing Cascades Design

> **Historical pre-Maple document.** This records the migrations 178-179 design
> truthfully. Migration 180 replaces active Bonsai routes with Maple; see
> [the Maple GPU2 cutover design](2026-08-07-maple-gpu2-cutover-design.md).


## Goal

Give dialog, memory extraction, and memory subject merge independent retry budgets and deterministic model cascades, while preventing VibeThinker and Ternary Bonsai from running concurrently on their shared GPU2 service.

## Current State

- `workflows.retry_max_hops` and `workflows.retry_wall_ms` are already stored and edited per workflow.
- Dialog has three weighted primaries, then Ternary Bonsai and Gemini fallbacks, but its three-hop budget can stop before either fallback.
- Memory extraction uses the historical `memory_consolidation` workflow and has two vram.cloud primaries followed by VibeThinker.
- Subject merge constructs an `AifarmMemoryExtractor` directly, so it bypasses workflow routing, circuit breakers, retry budgets, and capacity pools.
- `vibethinker-3b` and `ternary-bonsai-27b` resolve to the same `llm-openai-qwen27b-gguf` Discovery service and therefore share one physical inference slot.

## Routing Design

The database will expose three independently editable workflow rows:

| Workflow | Ordered candidates | Max hops | Retry wall |
| --- | --- | ---: | ---: |
| `dialog` | weighted dialog primaries, Ternary Bonsai, Gemini | 5 | 180,000 ms |
| `memory_extraction` | vram.cloud 35B, vram.cloud 27B, VibeThinker, Ternary Bonsai | 4 | 900,000 ms |
| `memory_subject_merge` | VibeThinker, Ternary Bonsai | 2 | 900,000 ms |

`memory_extraction` and `memory_subject_merge` remain config-only chat workflows (`full_routing = false`), so the router follows one primary and then `fallback_order` deterministically. Dialog remains a full-routing workflow, so its weighted primary permutation is exhausted before its ordered fallback tail. The historical Gemini/Genkit overflow assignment and its triggers are disabled with rollback markers; otherwise an engaged trigger could promote Gemini into the weighted pool ahead of Bonsai.

The historical `memory_consolidation` workflow key is replaced by `memory_extraction`. Queue names, taskman job names, environment variables, and operator-facing memory terminology stay unchanged because they describe the durable job rather than the LLM routing sub-step.

## Shared Capacity

Migration 179 creates or reconciles `aifarm-gpu2-qwen27b` with `max_concurrency = 1` and attaches both GPU2 model rows to it:

- `vibethinker-3b`
- `ternary-bonsai-27b`

The pool is shared across workflows. A busy model does not consume a hop; the routed walker can skip or wait for the same physical slot without allowing simultaneous requests to the two aliases.

If an operator already owns a pool with that name, its description and config remain operator-owned. The migration records its previous concurrency and each model's previous pool assignment under scoped rollback keys instead of stamping the pool as migration-owned.

## Runtime Design

`RoutedMemoryExtractor` will request `memory_extraction` instead of `memory_consolidation`.

A new `RoutedSubjectMerger` will implement `openplotva_memory::SubjectMerger`. It will use the existing `RoutedAttemptWalker`, build the AIFarm client from each selected attempt, invoke the existing subject-merge prompt/decoder, and classify retryability through the same memory error rules as extraction. The subject-merge worker will receive this routed adapter from the composition root.

Provider/model backfills and memory worker-count derivation use `memory_extraction`, ensuring both upgraded and fresh databases converge on the same route. Startup convergence has separate guards for dialog, extraction, and subject merge: the local-only subject route is installed as soon as VibeThinker and Bonsai exist, even when no vram.cloud catalog is configured. Each ready subset, its shared-pool changes, and its guard are committed in one SQL transaction before the router snapshot is loaded.

## Compatibility and Rollback

- No public HTTP, GraphQL, Telegram, prompt, Redis, or taskman contract changes.
- Existing model/provider rows are reused by name; no model identity is replaced.
- The up migration normalizes only the three workflows, the historical Gemini/Genkit overflow edge, and the two GPU2 pool assignments named above.
- The down migration recreates the previous `memory_consolidation` route, removes the two new workflows, restores only model/pool/overflow state carrying the migration's rollback markers, and restores the dialog defaults only when they still match the migration values.
- Production deployment is outside this change unless separately requested.

## Verification

- Storage tests inspect the migration contract and, when `OPENPLOTVA_TEST_POSTGRES_DSN` is available, execute the up/down SQL against isolated temporary routing tables with a pre-existing operator-owned pool.
- App tests verify that extraction and subject merge use distinct workflow keys and that the new adapter satisfies the `SubjectMerger` boundary.
- A live app DB test injects a mid-replacement constraint failure to prove atomic rollback, then proves subject merge converges without vram.cloud targets.
- Router tests remain the source of truth for full-routing weighted permutations, ordered fallback tails, hop accounting, and shared-pool serialization.
- Final branch verification: `cargo fmt --all`, relevant focused tests, `cargo clippy --workspace --all-targets -- -D warnings`, and the workspace test suite.
