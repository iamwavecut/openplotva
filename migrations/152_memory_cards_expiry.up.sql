-- Durability-based forgetting for memory cards. Ephemeral facts (one-off events,
-- throwaway jokes) get an expires_at at write time from the durability the
-- extractor assigns, and the maintenance worker archives them (status='expired')
-- once past due; durable facts (identity, preference, ...) keep expires_at NULL
-- and live on. Every existing row gets expires_at NULL, so retrieval behavior is
-- unchanged until new cards start carrying a TTL.
ALTER TABLE memory_cards ADD COLUMN expires_at TIMESTAMPTZ;

ALTER TABLE memory_cards DROP CONSTRAINT IF EXISTS memory_cards_status_check;

ALTER TABLE memory_cards
    ADD CONSTRAINT memory_cards_status_check
    CHECK (status IN ('active', 'superseded', 'deleted', 'competing', 'expired'));

-- Drives the archival worker: active/competing cards whose TTL has elapsed.
CREATE INDEX memory_cards_expiry_idx
    ON memory_cards (expires_at)
    WHERE status IN ('active', 'competing') AND expires_at IS NOT NULL;
