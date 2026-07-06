# Memory Subject Merge-Pass — Design

**Goal:** A 24/7 LLM merge-pass that consolidates the OLD backlog of over-extracted
memory cards. New extractions already reconcile against related existing cards
(PR #15/#19/#20 op-set + related-card retrieval); the backlog of already-bloated
`(scope, subject)` groups never gets pulled into new windows, so it needs a dedicated
sweep.

## Why (prod evidence, 2026-07-06)

`memory_cards` on prod: **26,546** active cards in **10,912** `(visibility,user_id,chat_id,thread_id,subject)` groups.
- **Half** the active cards (13,248) live in groups of **≥5**; 40% (10,686) in groups **≥8**.
- Groups ≥5: **1,013**; ≥8: **570**; ≥10: **420**. Max group: **138** cards.
Systemic over-extraction (a card per opinion/message), not isolated. Real work for an
always-on worker while vram.cloud is up.

## Shape (decision)

A **continuous background worker** mirroring `run_memory_duplicate_collapse_worker_until`
(memory_runtime.rs:2508) and the decay worker — NOT a taskman `JobType`. Rationale: it is
a backlog sweep (like dup-collapse/decay), the pattern already exists, and it needs no
per-event payload. Gated on vram-cloud availability via the same embedder breaker check
the consolidation worker uses (`embedder.is_available()`, memory_runtime.rs:978). Records
activity via structured tracing + a worker report (dup-collapse precedent); no new admin
surface or ledger table in this pass (avoid speculative scaffolding).

## Candidate selection (deterministic)

Group active cards by the full scope key `(visibility, user_id, chat_id, thread_id, subject)`.
A group is a candidate when it has **≥ N** active cards that have not been merge-reviewed
within a cooldown, so once the LLM reviews a group it drops out until the cooldown expires
or N fresh cards accumulate:

```sql
SELECT visibility, user_id, chat_id, thread_id, subject,
       array_agg(id ORDER BY id) AS card_ids
FROM memory_cards
WHERE status = 'active' AND subject <> ''
GROUP BY visibility, user_id, chat_id, thread_id, subject
HAVING count(*) FILTER (
  WHERE last_merge_pass_at IS NULL OR last_merge_pass_at < now() - ($1 || ' hours')::interval
) >= $2                     -- $2 = min_cards N
ORDER BY count(*) DESC
LIMIT $3;                   -- batch size
```

Active predicate stays `status = 'active'` (superseded/expired/deleted excluded). A group
that the LLM leaves large (legitimately distinct facts) is skipped for the cooldown.

## LLM merge decision

New prompt `prompts/memory/subject_merge.prompt`. Input: the group's cards as a compact
projection `{id, type, subject, predicate, fact, salience, obs, age_days}` (mirrors the
`compact_existing_cards` projection). No messages — this is pure card-on-card
consolidation. Reuse the aifarm transport + `aifarm_memory_extractor_config_from_app_config_with_model`
config (model = `memory.consolidation_model`, penalties clamped, enable_thinking off),
its response_format/JSON-schema pattern, and `decode` + `salvage_truncated_json`.

Output schema (focused, not the full extraction schema):
```json
{
  "clusters": [
    { "survivor_id": <id kept>, "absorbed_ids": [<ids folded in>], "merged_fact_text": "<one compact stance/fact>" }
  ],
  "demote_ids": [<weak-but-keep ids>],
  "keep_ids":   [<distinct durable ids left untouched>]
}
```
Prompt rules: fold near-duplicate opinions/hot-takes into ONE compact stance card per
subject+stance; keep genuinely distinct durable facts; a fact about one person is never a
duplicate of a card about another; never invent a fact not supported by the inputs; every
input id appears exactly once across clusters/absorbed/demote/keep. Ban self-questioning
prose (same anti-runaway discipline as extraction.prompt); low max_tokens.

## Apply (reuse existing primitives)

Per cluster: for each `absorbed_id` → `supersede_card(absorbed_id, survivor_id)`; then
`update_card_text(survivor_id, merged_fact_text, "", <fresh embedding>)` — the merge
rewrites the text, so the worker re-embeds `merged_fact_text` (best-effort via the same
embedder; on failure the prior embedding stays) so vector retrieval matches the new card;
then set survivor `observation_count = sum(obs of survivor + absorbed)` via the existing
`SQL_SET_MEMORY_CARD_OBSERVATION_COUNT` (the dup-collapse summation pattern). `demote_ids`
→ `demote_card(id, delta)`. `keep_ids` → no-op. Finally
`mark_cards_merge_passed(all_group_ids, now)`. Validate LLM output against the group's real
ids (drop any hallucinated id; require survivor ∈ group and absorbed ⊂ group) before
applying — never trust ids the model invents.

## Touchpoints

- **Migration 153** `153_memory_cards_merge_pass`: `ALTER TABLE memory_cards ADD COLUMN
  last_merge_pass_at TIMESTAMPTZ;` (additive/nullable, metadata-only).
- **Migration 154** `154_create_idx_memory_cards_subject_merge`: partial index
  `(visibility,user_id,chat_id,thread_id,subject) WHERE status='active'` built
  `CONCURRENTLY` (no-transaction) to back the grouped candidate scan on the hot table.
- **openplotva-storage**: `select_subject_merge_candidates`, `load_active_cards_by_ids`,
  `mark_cards_merge_passed`, expose `set_card_observation_count` (reuse
  supersede/update/demote already public).
- **openplotva-memory**: compact merge projection + the merge-plan decode/validate types.
- **openplotva-llm/aifarm**: `subject_merge` request builder + response schema + decode
  (mirror `extract`).
- **openplotva-app/memory_runtime.rs**: `run_memory_subject_merge_worker_until` +
  `MemorySubjectMergeConfig` + report; **lib.rs** `start_runtime_workers` spawn (mirror the
  dup-collapse spawn at lib.rs:10252) gated by config.
- **openplotva-config**: env knobs `MEMORY_SUBJECT_MERGE_ENABLED` (default true),
  `_MIN_CARDS` (N, default 6), `_COOLDOWN_HOURS` (default 168 = 7d), `_INTERVAL_SECONDS`
  (default 45), `_BATCH` (groups/tick, default 4), `_MODEL` (default = consolidation model).
- **PROMPT_VERSION**: NOT bumped (avoids ensure_daily_runs re-enqueue).

## Verification

Unit: candidate SQL (respects N + cooldown + active), merge-plan decode + id validation
(hallucinated ids dropped, survivor∈group), apply (supersede+sum obs, demote, mark passed),
worker tick gated on embedder availability. `cargo fmt`, `cargo clippy -D warnings`,
`cargo test` on touched crates + a representative storage check for the migration. Ship on a
`feat/` branch → PR → review-first babysit → merge. No deploy (owner's call).
