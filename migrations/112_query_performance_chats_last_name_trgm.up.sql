-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_chats_last_name_trgm
    ON chats USING gin (last_name gin_trgm_ops);
