-- Source SHA-256: 54a5f2abccba7ea05e85b221b539c879c3aebed6b66691c42163b02f17546bb2

DROP TRIGGER IF EXISTS shield_documents_updated_at ON shield_documents;
DROP FUNCTION IF EXISTS shield_documents_set_updated_at();
DROP INDEX IF EXISTS shield_documents_embedding_hnsw_idx;
DROP INDEX IF EXISTS shield_documents_title_search_idx;
DROP INDEX IF EXISTS shield_documents_enabled_priority_idx;
DROP TABLE IF EXISTS shield_documents;

CREATE TABLE IF NOT EXISTS documents (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    embedding vector(1024),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS documents_embedding_hnsw_idx ON documents
    USING hnsw (embedding vector_l2_ops)
    WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS documents_id_idx ON documents (id);
