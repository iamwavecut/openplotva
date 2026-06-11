-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS memory_cards_updated_id_desc_idx
    ON memory_cards (updated_at DESC, id DESC);
