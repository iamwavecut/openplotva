# Plan 019: Retire completed execution transcripts and keep short decision records

## Status

- Priority: P0
- Effort: M
- Risk: LOW-MED
- Planned at: whole-repository audit, 2026-08-02
- Depends on: per-document shipped-state proof

## Why

The repository tracks 8,737 lines of Markdown. Completed plans 001–011 alone occupy
2,251 lines and retain 70 unchecked boxes even though `plans/README.md` declares the
work integrated and green. The 17 dated `docs/superpowers/` handoffs/specifications add
3,988 lines and 136 unchecked boxes. Many embed mutable branch SHAs, line numbers,
migration numbers, toolchain failures, production snapshots, and implementation steps
that no longer describe the current tree.

This is an operational correctness problem as well as context size: completed runbooks
look actionable, stale snapshots compete with source-of-truth code, and agents spend
time reconciling contradictory instructions. Git already preserves the full historical
text. Durable product/security/compatibility decisions should remain in concise records;
execution choreography should not remain in the active documentation surface.

## Change

1. Build a table for every file under `plans/` and `docs/superpowers/`: active,
   partially shipped, shipped, superseded, or historical evidence. Prove shipped state
   from current symbols, migrations, tests, and contracts—not from the document's status.
2. Replace plans 001–011 with one concise outcome/decision record covering the durable
   auth, CI, timeout, ingress, sqlx, and settings invariants; then delete the eleven
   execution transcripts.
3. For each shipped superpowers spec/plan pair, preserve only decisions that are not
   evident from code/tests and delete duplicated handoff steps. Keep partially shipped
   or still-approved work as an active spec with an explicit current status.
4. Correct stale current-facing claims in `DESIGN.md`, `docs/admin-ux-audit.md`, and
   `docs/settings-webapp-auth-design.md` when live code disproves them. Do not add new
   mutable production or branch snapshots.
5. Reduce `plans/README.md` to the active plan index plus links to concise decision
   records. Completed work belongs in Git history, not an unchecked active queue.
6. Do not modify `docs/CODEBASE_MAP.md`; repository policy keeps it read-only. Remove
   current-facing reliance on it where necessary and validate facts directly from source.
7. Add the smallest existing-workflow check that distinguishes active plans from
   completed records and rejects unchecked task boxes in completed records. Do not add a
   documentation framework or generator unless a simpler check cannot enforce the rule.

## Verification

- Every removed document has a row proving shipped/superseded state and naming the
  retained decision record, source contract, or Git commit.
- All relative Markdown links resolve; no active document links to a deleted path.
- `rg -- '- \[ \]'` finds no unchecked tasks in completed decision records.
- Current docs contain no known-red test claim when the named gate is green and no
  branch/deploy assertion presented as timeless architecture.
- Target at least 3,500 net documentation lines removed while preserving active plans
  012–020 and unique product rationale.
- Docs-only diff validation plus the focused source checks named by any corrected claim.

## STOP conditions

- A document contains a user-approved decision not represented in source, tests, or the
  proposed concise record.
- Shipped state cannot be proven from the current repository or required live evidence.
- A deletion would break an external documentation link that cannot be redirected.
- The change would edit `docs/CODEBASE_MAP.md` or replace it with an equivalent generated
  codebase map.
