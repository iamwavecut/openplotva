DROP INDEX IF EXISTS memory_cards_asof_idx;

ALTER TABLE memory_cards
    DROP COLUMN IF EXISTS retracted_at,
    DROP COLUMN IF EXISTS recorded_at;
