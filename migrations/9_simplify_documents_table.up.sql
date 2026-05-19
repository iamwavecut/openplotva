-- Source SHA-256: b3871694921be377225659aa982ca19922a59aaefe7511805ff86181f4a121eb

-- Drop the current documents table completely
DROP TABLE IF EXISTS documents CASCADE;

-- Create the simplified documents table
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    embedding vector(1024) -- Optional vector field for 1024 dimensions
);

-- Create HNSW index for vector similarity search when embeddings exist
CREATE INDEX documents_embedding_hnsw_idx ON documents
USING hnsw (embedding vector_cosine_ops)
WHERE embedding IS NOT NULL;

-- Create a basic index on id for performance
CREATE INDEX documents_id_idx ON documents (id);
