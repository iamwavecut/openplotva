DROP INDEX IF EXISTS memory_cards_expiry_idx;

-- One-way data note: cards already archived by TTL (status='expired') are folded
-- back to 'active' so the pre-durability CHECK accepts them again. The expiry
-- decision that retired them is not recoverable from this down migration.
UPDATE memory_cards SET status = 'active', retracted_at = NULL WHERE status = 'expired';

ALTER TABLE memory_cards DROP CONSTRAINT IF EXISTS memory_cards_status_check;

ALTER TABLE memory_cards
    ADD CONSTRAINT memory_cards_status_check
    CHECK (status IN ('active', 'superseded', 'deleted', 'competing'));

ALTER TABLE memory_cards DROP COLUMN IF EXISTS expires_at;
