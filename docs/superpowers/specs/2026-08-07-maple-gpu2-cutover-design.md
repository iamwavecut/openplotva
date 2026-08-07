# Maple GPU2 routing cutover design

**Date:** 2026-08-07  
**Status:** implementation target for migration 180; not a production deployment record

## Goal

Replace every active Ternary Bonsai route with Maple without changing the
canonical `llm-openai` dialog primary or vision service. VibeThinker remains on
the retained GPU2 llama.cpp service and shares the existing one-slot OpenPlotva
capacity pool with Maple.

## Target identities

| Purpose | Provider | Discovery service | Model |
| --- | --- | --- | --- |
| Maple text fallback/reasoner | `aifarm-maple` | `llm-openai-maple` | `maple-preview-2bit-mlx` |
| Retained VibeThinker | `aifarm-llamacpp-gpu2` | `llm-openai-qwen27b-gguf` | `vibethinker-3b` |
| Canonical dialog/vision | unchanged | `llm-openai` | unchanged |

The Maple provider uses `protocol = openai_compat` and `runtime_hint = mlx`.
Its model advertises `chat` and `tools`, plus truthful config flags for tools,
structured output, response format, and reasoning.

## Route cutover

Migration 180 locks the mutable routing catalog against concurrent writers, then
asserts the exact known upgraded topology: one retained GPU2 provider, one
Bonsai row, one VibeThinker row, one one-slot pool, no unrelated Maple or marker
collision, all five startup-convergence guards, and at least one enabled Bonsai
assignment. It then creates a distinct Maple provider/model row and updates every
enabled assignment that references the Bonsai model. This includes the observed
production assignments for:

- `agentic_search_reasoner` primary;
- `dialog` fallback order 0;
- `memory_extraction` fallback order 2;
- `memory_subject_merge` fallback order 0;
- any other enabled operator-added assignment referencing the same model row.

Assignment rows are updated in place, so assignment ids, trigger references,
weights, fallback order, circuit-breaker settings, and inference overrides stay
stable. A rollback marker records the prior model id inside
`inference_overrides`, while a migration-owned manifest records the exact moved
assignment ids. Inactive historical assignments may retain their truthful
Bonsai target, but the Bonsai model row is disabled after the active assignments
move.

A separately proven post-migration-179 fresh shape has only the disabled
OpenRouter-free catalog, its seven fallbacks, and the two expected pools. In that
shape migration 180 seeds the exact Maple provider/model metadata with
`origin = fresh` but moves no assignments. Any partial or non-fingerprint state
still fails closed. The compatibility config key `qwen-reasoner` remains stable,
but its default service and model are Maple.

## Shared GPU2 capacity

Maple and VibeThinker both attach to the existing
`aifarm-gpu2-qwen27b` pool. Migration 180 requires the existing pool to
already have `max_concurrency = 1`; startup convergence retains that invariant.
The legacy pool name is retained because it is an
existing persisted operator contract; no second pool or Discovery-level lock is
introduced.

## Historical attribution and rollback

`llm_routing_events.provider_id` and `.model_id` are foreign keys to the
provider/model registry. Renaming the Bonsai row in place would make historical
Bonsai events render as Maple, while deleting a used row would set those keys to
NULL. Migration 180 therefore keeps distinct identities permanently:

- up migration: preserve and disable the Bonsai model row;
- down migration: restore only assignments carrying the migration-owned prior
  Bonsai marker, re-enable Bonsai, and disable Maple;
- down migration: never delete either provider/model identity.

Disabled Maple rows after rollback are intentional one-way audit data. The
routing reversal is reversible; accumulated audit attribution is not erased. A
later SQLx re-up may re-enable only that exact migration-owned retired pair.
Unrelated or operator-prepared Maple collisions abort rather than being claimed.
The upgraded-origin down migration requires the marker ids to equal its stored
manifest before restoring them to Bonsai. New unmarked Maple assignments are
never rewritten to Bonsai: down disables and marks them in place, and an exact
owned re-up removes that marker and re-enables them. Fresh-origin rollback uses
the same disable/re-enable mechanism without fabricating a Bonsai identity.

## Maple request normalization

The Maple compatibility service does not claim DRY sampling support. When an
`AifarmClientConfig` has `runtime_hint = mlx`, OpenPlotva removes
`dry_multiplier`, `dry_base`, and `dry_allowed_length` before serializing the
Discovery payload. This prevents the dialog defaults (`0.8`, `1.75`, `2`) from
being silently ignored upstream. `repeat_penalty` and float `top_k` remain on
the wire as accepted compatibility-normalization inputs. A request-body test
pins the exact Maple JSON shape.

## Production database checks

Before applying migration 180, verify that the canonical Maple model name is
not duplicated under `aifarm-maple` and inspect all enabled Bonsai assignments:

```sql
SELECT assignment.id, assignment.workflow_key, assignment.scope,
       assignment.role, assignment.fallback_order
FROM workflow_assignments AS assignment
JOIN provider_models AS model ON model.id = assignment.provider_model_id
JOIN llm_providers AS provider ON provider.id = model.provider_id
WHERE assignment.enabled
  AND provider.name = 'aifarm-llamacpp-gpu2'
  AND model.model_name = 'ternary-bonsai-27b'
ORDER BY assignment.workflow_key, assignment.id;
```

After applying it, verify no enabled assignment references Bonsai, Maple and
Vibe share one pool id, that pool has one slot, and historical event joins still
show their original model names. The migration intentionally does not infer or
rewrite operator-owned inactive assignments.

This repository change does not register Discovery, start/stop containers,
restart OpenPlotva, run the migration against production, or deploy anything.
