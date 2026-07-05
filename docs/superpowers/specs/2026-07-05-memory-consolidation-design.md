# Memory: Compact Context + Subject-Centric Consolidation + Durability — Design Spec

**Date:** 2026-07-05
**Status:** Design (approved direction; awaiting spec review before planning)
**Owner branch:** `feat/memory-consolidation`

## 1. Problem

Two operator-reported problems with how remembered facts enrich the LLM context, plus one enabling gap found during investigation. All three verified against current code and production data (`llm_request_events` on prod).

### 1.1 Context bloat — the raw-Card-JSON dump (verified)

The dialog memory channels are already lean:
- `format_context` (`crates/openplotva-memory/src/lib.rs:2176`) → `- [card_type conf] fact_text`.
- Agent `memory_search` → `format_memory` (`crates/openplotva-app/src/agent_runtime.rs:582`) → `- fact (confidence 0.90)`.
- Prod: 300 recent `dialog` requests contain **zero** `subject`/`salience`/`observation_count` keys.

The bloat is in the **`memory_extraction`** flow (largest flow, ~60935/day). The whole `ExtractInput` — including `existing_cards: Vec<Card>` with ~20 fields each, up to `EXISTING_CARDS_LIMIT = 80` cards — is serialized verbatim via `serde_json::to_string_pretty(input)` at **`crates/openplotva-llm/src/aifarm.rs:2035-2037`** (mirror ~2139). Prod sample of one `existing_cards` entry:

```json
{ "id": 32785, "visibility":"chat", "card_type":"event", "status":"active",
  "subject":"…", "predicate":"написал", "object":"написал слово «Овцебык»",
  "fact_text":"Участник … написал слово «Овцебык».",
  "confidence":0.9, "salience":0.3, "observation_count":1,
  "origin_chat_id":-1003…, "chat_id":-1003…, "valid_from":"…",
  "last_observed_at":"…", "use_count":0, "created_at":"…",
  "updated_at":"…", "decay_score":0.0, "recorded_at":"…" }
```

Useless-and-repeating fields for the extractor's reasoning: `visibility/status/predicate/object/salience/observation_count/origin_chat_id/chat_id/valid_from/last_observed_at/use_count/updated_at/decay_score/recorded_at`. Useful payload: `id` (needed for resolutions), `card_type`, `subject`, `fact_text`, `confidence`, age. Compacting also lets **more** cards fit the 10k-token extraction budget (`estimate_extraction_tokens` truncates the tail today).

### 1.2 Duplicate cards — no consolidation around the subject (verified)

Real dialog context from prod:
```
- [recurring_topic 0.50] я спать        (×3, exact)
- [joke 0.90] …«Омежа горит»…Morozoff_D…грил…   (×2, exact)
- [event 0.90] Ada … 'roulette' … send 10 messages …
- [event 0.90] Ada … 'roulette' … 60 participants …  (near-dup of same event)
```

Root cause:
- Extraction is context-aware (`existing_cards_for_run`, `crates/openplotva-app/src/memory_runtime.rs:1923`, ≤80 total / ≤20 per participant) but the LLM uses existing cards **only** for `supersede`/`competing` resolutions — never merge/update.
- `dedup_hash` (`crates/openplotva-storage/src/lib.rs:491`) includes the **full `fact_text`** → distinct facts about the same subject are distinct rows; the `ON CONFLICT` reinforce path (`observation_count++`) only fires on an exact-text match.
- No subject/entity identity resolution. `subject` is a free-text string with no canonicalization or cross-run binding. The deferred inite "off-hours entity resolution / near-dup merge" was never built.

Result: ~12 cards accrete per person in an active chat.

### 1.3 Enabling gap — there is (almost) no forgetting (verified)

`decay_score` is **read** in ranking (`-0.10 *` in both lexical/vector SQL, `crates/openplotva-storage/src/lib.rs:1288/1290`) but **never written**. No TTL, no prune job, no age-based archival. Active cards live forever; retrieval only demotes them via a recency tier (floor 0.2 for >30d). Ephemeral noise ("я спать", "написал слово «Овцебык»") persists permanently. So the "forgetting of old facts" we must not degrade barely exists today — which makes durability an opportunity, not a risk.

## 2. Goals / Non-Goals

**Goals**
1. Compact the memory representation sent to the LLM to `{ id, card_type, subject, fact_text, confidence, age }`.
2. Consolidate facts around a subject: the pipeline modifies, merges, reinforces, and demotes existing cards, not only supersede/competing.
3. Introduce semantic durability so short-lived facts fade and long-lived facts persist, without degrading durable facts.

**Non-Goals**
- Cross-chat entity resolution / a global entity table (deferred; start with per-scope subject clustering).
- Changing the dialog/agent external contracts beyond the approved additive improvements.
- Rewriting the extraction pipeline; everything extends the existing machinery.

## 3. Constraints (binding)

- Consolidation must be **context-aware** (operate against existing cards), able to **modify** existing cards, and **reinforce/demote** on conflict.
- Consolidation must be **semantic**, not literal: never merge a long-lived fact with a short-lived one.
- Must **not degrade** existing forgetting/supersession/competing/visibility behavior.
- External contracts (Telegram payloads, DB schema meaning, Redis, prompts as artifacts, retrieval visibility/no-leak) preserved; changes are additive with up/down migrations and prompt-version bumps (AGENTS.md).
- Subjects are people **within** a chat in multi-user groups; keep boundaries between them (respect the existing visibility model: `chat` / `chat_user` / `public_user` / `thread`).

## 4. Prior art (informing the design)

- **Mem0** — Extraction → Update: for each new fact, retrieve similar existing memories and let the LLM pick ADD / UPDATE / DELETE / NOOP. Directly maps to the requested op-set. (Warns that premature consolidation loses information → keep merges conservative, keep lineage.)
- **Graphiti / Zep** — bi-temporal facts with validity intervals; on contradiction, **invalidate, not delete**, prioritizing new info. We already have the bi-temporal columns (`valid_from/until`, `recorded_at/retracted_at`, migrations 127) and SPO fields — half the scaffold exists.

## 5. Architecture — three pillars over the existing pipeline

```
                      ┌─────────────────────────── Pillar A ──────────────────────────┐
 extraction run ─────►│ compact existing_cards projection at the serialize boundary    │
 (per 1h window)      │  aifarm.rs:2035/2139 : to_string_pretty(CompactExtractInput)   │
                      └───────────────────────────────────────────────────────────────┘
                                     │  existing cards shown compact + clustered by subject
                                     ▼
                      ┌─────────────────────────── Pillar B (online) ─────────────────┐
 extractor LLM ──────►│ op-set: ADD / UPDATE / MERGE / REINFORCE / DEMOTE /            │
 (prompt v5)          │         SUPERSEDE / COMPETING / (NOOP)                          │
                      │ applied by memory_runtime → storage ops                         │
                      └───────────────────────────────────────────────────────────────┘
                                     │
 off-hours scheduler ─┼──► memory_runs(kind='consolidation') ──► consolidator LLM ──► same op-set
                      │        (subject clusters with > N cards; cross-run cleanup)      (Pillar B off-hours)
                      ▼
                      ┌─────────────────────────── Pillar C ──────────────────────────┐
 write path ─────────►│ durability → expires_at ; archival worker → status='expired'   │
 retrieval ──────────►│ filter (expires_at IS NULL OR expires_at > now())              │
                      └───────────────────────────────────────────────────────────────┘
```

### 5.1 Pillar A — compact representation

Replace `serde_json::to_string_pretty(&ExtractInput)` (aifarm.rs:2035-2037 and ~2139) with serialization of a purpose-built wire projection. `existing_cards` project to:

```rust
struct CompactExistingCard {
    id: i64,            // required: resolutions reference old_card_id
    card_type: String,
    subject: String,
    fact_text: String,
    confidence: f64,    // 2-decimal
    age: String,        // coarse relative bucket: "today" | "3d" | "2w" | "old"
    status: Option<String>, // present only when "competing" (so the model sees disputes)
}
```

`age` is derived from `created_at` relative to the run's `range_end_at`. Messages are left intact (legitimately needed). Existing cards are **grouped by normalized subject** (see §5.2) in the wire payload so the model sees "everything about this person" together.

Optional, same phase: add `subject` to the lean dialog `format_context` line (`- [event 0.90] (А.Лютик) …`) so the dialog model can attribute facts to people in multi-user chats. Purely additive to the internal prompt context.

### 5.2 Pillar B — subject-centric semantic consolidation

**Op-set.** Extend `ResolutionDecision` (`crates/openplotva-memory/src/lib.rs`) from `{ Supersede, Competing }` to add:
- `Update` — refine an existing card's `fact_text`/`subject` in place (re-embed). For corrections/refinements that keep the same fact identity.
- `Merge` — fold N existing cards (same subject, same durability class) into one consolidated card. Implemented by reusing supersession: the merged-away cards go `status='superseded'`, `superseded_by = kept_id`; the kept card's `fact_text` is rewritten to the consolidated text, `observation_count` summed, `confidence`/`salience` = max. Lineage preserved for audit/restore.
- `Reinforce` — explicit bump of `confidence`/`salience`/`observation_count`/`last_observed_at` (the ON-CONFLICT reinforce, but decided by the model for semantic — not exact-text — matches).
- `Demote` — lower `confidence` (and optionally `salience`) when a fact is weakly contradicted or losing relevance; feeds naturally into forgetting.
- Existing `Supersede`/`Competing` unchanged. Absent resolution = NOOP.

**Subject binding.** Presentation-only for online: normalize `subject` (trim, lowercase, strip trailing `(@username)`); for `scope_type=user` cards also key by `user_id`. Cluster existing cards by this key in the wire payload and in the prompt guidance. No schema change; no cross-chat merging (respects visibility boundaries).

**Semantic guardrail.** The prompt forbids MERGE across durability classes (a root preference must not merge with a one-day event) and across card types where identity differs. Merges must be conservative; when in doubt, keep separate (or COMPETING).

**Storage ops (new/changed, `crates/openplotva-storage`):**
- `update_card(id, fact_text, subject?, confidence?, embedding)` — in-place modify + re-embed; bumps `updated_at`.
- `merge_cards(keep_id, merged_ids[], fact_text, embedding)` — supersede merged_ids → keep_id, rewrite keep card, sum observation_count, max conf/salience.
- `reinforce_card(id, confidence?, salience?)` and `demote_card(id, confidence, salience?)`.
- `supersede_card` / `mark_cards_competing` already exist.

**Online application** extends `write_memory_extraction_cards` (`crates/openplotva-app/src/memory_runtime.rs:~2195-2237`) to route the new decisions.

**Off-hours pass (both-layers).** A scheduled maintenance step enqueues `memory_runs` rows with `kind='consolidation'` for `(scope)` subjects whose active-card count exceeds a threshold (e.g. > 6). The worker loads that subject's cards (compact), calls a **consolidator** prompt (`prompts/memory/consolidation.prompt`, reusing extractor transport) that returns the same op-set, and applies it. This catches cross-run accumulation a single 1h window cannot. Rides the existing `memory_runs` leasing/retry/telemetry.

### 5.3 Pillar C — durability & forgetting

**Durability is decided semantically.** The extractor/consolidator emits an optional `durability` per card: `permanent | long | short | ephemeral` (default derived from `card_type`: identity/preference/warning/project/decision/relationship/technical_fact → long/permanent; one-off event/joke → short/ephemeral; recurring_topic stays until it stops recurring). Durability maps to a TTL at write time:
- `permanent`/`long` → `expires_at = NULL` (durable; unaffected).
- `short` → `expires_at = observed_at + ~14d`.
- `ephemeral` → `expires_at = observed_at + ~2d`.

**Schema (migration, new numbers verified at plan time):**
- `memory_cards.expires_at TIMESTAMPTZ NULL`.
- Extend the `status` CHECK to include `'expired'` (pattern of migration 128 adding `'competing'`).
- `memory_runs.kind TEXT NOT NULL DEFAULT 'extraction'` (values: `extraction | consolidation`).

**Retrieval change.** Both lexical and vector SQL add `AND (expires_at IS NULL OR expires_at > now())`. All existing rows have `expires_at = NULL` → no behavior change for current data.

**Archival worker.** A cleanup step (modeled on the existing scrub/cleanup workers) periodically sets `status='expired'`, `retracted_at=now()` for active cards past `expires_at`, in batches. Expired is a **soft** state (restorable), not a hard delete; lineage preserved.

**Optional (may defer within the pillar):** finally wire `decay_score` from a maintenance job so ephemeral cards demote gracefully in ranking before hard expiry, instead of a cliff.

## 6. External-contract & compatibility impact

- Compact `existing_cards` is input to our own extractor — not an external contract. Safe.
- Prompt changes bump `PROMPT_VERSION` (`chat_memory_daily_v4` → `v5`); `memory_runs` uniqueness includes `prompt_version`. Verify the daily-window enqueuer does not re-backfill history beyond the 7d retention window on the version change.
- New card ops modify stored data but preserve the Card schema meaning; additions (`expires_at`, `status='expired'`, `memory_runs.kind`) ship as up/down migration pairs with compatibility notes; data effects (archival) are one-way and noted in the down migration.
- Retrieval visibility/no-leak filters are untouched; the only added predicate is TTL, which cannot widen visibility.
- Admin Memory UI already surfaces status/superseded lineage; surfacing `expired` in filters + merged lineage is an additive follow-up (requires the `web/` sha256 update + `cargo test -p openplotva-web` + design-system-review if touched).

## 7. Delivery phases (one program, phased)

1. **Phase A — compact projection.** Wire projection + subject grouping at aifarm.rs:2035/2139; optional dialog `format_context` subject. Tests + prod check (admin X-ray shows compact extraction request). Low risk, immediate value.
2. **Phase C-schema — durability data model.** Migration (`expires_at`, `status='expired'`, `memory_runs.kind`) + retrieval TTL filter + archival worker. No behavior change for existing rows.
3. **Phase B-online — op-set.** Extend `ResolutionDecision` + storage ops + prompt v5 (compact shape, op-set, durability output, subject clustering, tighter noise rejection) + application in `write_memory_extraction_cards`.
4. **Phase B-offhours — consolidation runs.** `memory_runs.kind='consolidation'` scheduler + `consolidation.prompt` + consolidator worker + apply.
5. **Phase C-polish (optional).** `decay_score` graceful demotion; admin surfacing of expired/merged.

Each phase is independently shippable, testable, and reversible.

## 8. Testing strategy

- **A:** unit tests for the compact projection (fields kept/dropped, age bucketing, competing surfaced, subject grouping); token-budget test that more cards fit.
- **C-schema:** migration up/down; retrieval excludes expired and keeps `expires_at IS NULL`; archival worker only touches past-due active rows (UPDATE not DELETE); `memory_runs.kind` default.
- **B-online:** storage op tests (update in place + re-embed; merge sets superseded_by + sums counts + max conf; reinforce/demote deltas); resolution routing; prompt-version bump; end-to-end extraction applying each op; guardrail (no cross-durability merge).
- **B-offhours:** clustering threshold; consolidation run lifecycle (lease/retry); consolidator apply; idempotency (re-run converges, no oscillation).
- **Prod verification:** admin X-ray shows compact extraction request; DB checks — per-subject card counts drop after off-hours pass; expired cards archived; dialog context no longer shows exact/near dups; durable facts (identity/preference) retained.

## 9. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Merge destroys information (Mem0's premature-consolidation warning) | Merge only within same durability/type; keep `superseded_by` lineage (restorable); conservative prompt; off-hours reviews small clusters |
| Over-aggressive forgetting drops useful facts | Durability decided semantically with conservative defaults; durable types never expire; expired = soft/restorable, not deleted |
| Prompt-version bump re-enqueues history | Verify `ensure_daily_runs` windowing; retention (7d) bounds any re-run |
| Embedding recompute cost on update/merge | Batch; re-embed only when text changes |
| Consolidation oscillation (merge → re-split) | Idempotency tests; off-hours convergence check; dedup vs already-consolidated |

## 10. Open questions / deferred

- Real cross-chat entity resolution (entity table) — deferred; subject-string clustering first.
- `decay_score` graceful demotion — optional within Pillar C.
- Consolidator model choice/routing and cost budget for the off-hours pass — decide at plan time (reuse the extraction model/pool).
- Exact TTL values per durability class — tune from prod after Phase C.
```
