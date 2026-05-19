-- Source SHA-256: b3871694921be377225659aa982ca19922a59aaefe7511805ff86181f4a121eb

-- Drop the simplified table
DROP TABLE IF EXISTS documents CASCADE;

-- Restore the complex structure
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    title TEXT,
    content TEXT NOT NULL,
    document_type VARCHAR(50) DEFAULT 'message',
    metadata JSONB,
    embedding vector(1024),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Recreate indexes
CREATE INDEX documents_chat_user_idx ON documents (chat_id, user_id);
CREATE INDEX documents_created_at_idx ON documents (created_at);
CREATE INDEX documents_type_idx ON documents (document_type);
CREATE INDEX documents_metadata_gin_idx ON documents USING gin (metadata);
CREATE INDEX documents_embedding_hnsw_idx ON documents
USING hnsw (embedding vector_cosine_ops)
WHERE embedding IS NOT NULL;

-- Recreate trigger function
-- +StatementBegin
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';
-- +StatementEnd

-- Recreate trigger
CREATE TRIGGER update_documents_updated_at
    BEFORE UPDATE ON documents
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
