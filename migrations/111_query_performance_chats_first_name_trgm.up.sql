-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_chats_first_name_trgm
    ON chats USING gin (first_name gin_trgm_ops);
