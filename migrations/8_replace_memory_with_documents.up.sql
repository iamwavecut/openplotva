-- Source SHA-256: 5c38f2f0d38131231b5d028ae64c63aaedc3dfa17afa19929184966132caebd6

CREATE EXTENSION IF NOT EXISTS vector;

-- Drop the old memory table
DROP TABLE IF EXISTS memory CASCADE;

-- Create the new documents table
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    title TEXT,
    content TEXT NOT NULL,
    document_type VARCHAR(50) DEFAULT 'message',
    metadata JSONB,
    embedding vector(1024), -- Nullable vector field for 1024 dimensions
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create indexes for efficient querying
CREATE INDEX documents_chat_user_idx ON documents (chat_id, user_id);
CREATE INDEX documents_created_at_idx ON documents (created_at);
CREATE INDEX documents_type_idx ON documents (document_type);
CREATE INDEX documents_metadata_gin_idx ON documents USING gin (metadata);

-- Create HNSW index for vector similarity search when embeddings exist
-- This index will be created conditionally since not all documents may have embeddings initially
CREATE INDEX documents_embedding_hnsw_idx ON documents
USING hnsw (embedding vector_cosine_ops)
WHERE embedding IS NOT NULL;

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_documents_updated_at
    BEFORE UPDATE ON documents
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
