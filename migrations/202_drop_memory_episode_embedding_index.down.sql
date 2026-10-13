CREATE INDEX IF NOT EXISTS memory_episodes_embedding_hnsw_idx
    ON memory_episodes USING hnsw (embedding vector_cosine_ops)
    WHERE embedding IS NOT NULL;
