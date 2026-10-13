-- no-transaction

-- Episodes are retrieved by full-text search only; nothing queried this vector
-- index, yet every episode insert paid for an HNSW graph update.
DROP INDEX CONCURRENTLY IF EXISTS memory_episodes_embedding_hnsw_idx;
