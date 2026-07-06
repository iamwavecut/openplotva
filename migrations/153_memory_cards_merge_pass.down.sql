-- One-way data note: the per-card merge-review timestamps are not recoverable
-- from this down migration. They are only cooldown bookkeeping (which groups the
-- merge worker already reviewed), so dropping them just makes the next pass
-- reconsider every group — no card content is lost.
ALTER TABLE memory_cards DROP COLUMN IF EXISTS last_merge_pass_at;
