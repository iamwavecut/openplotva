-- Conflict-aware memory. Two contradictory facts where neither clearly wins are
-- marked 'competing' (instead of one silently overwriting the other) and grouped
-- by conflict_group, so retrieval surfaces both and the bot hedges. The active
-- dedup index stays scoped to status='active', so competing cards coexist.
ALTER TABLE memory_cards DROP CONSTRAINT IF EXISTS memory_cards_status_check;

ALTER TABLE memory_cards
    ADD CONSTRAINT memory_cards_status_check
    CHECK (status IN ('active', 'superseded', 'deleted', 'competing'));

ALTER TABLE memory_cards ADD COLUMN conflict_group BIGINT;

CREATE INDEX memory_cards_conflict_group_idx
    ON memory_cards (conflict_group)
    WHERE status = 'competing';
