DROP INDEX IF EXISTS memory_cards_conflict_group_idx;

ALTER TABLE memory_cards DROP COLUMN IF EXISTS conflict_group;

ALTER TABLE memory_cards DROP CONSTRAINT IF EXISTS memory_cards_status_check;

ALTER TABLE memory_cards
    ADD CONSTRAINT memory_cards_status_check
    CHECK (status IN ('active', 'superseded', 'deleted'));
