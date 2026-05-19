-- Source SHA-256: 5c38f2f0d38131231b5d028ae64c63aaedc3dfa17afa19929184966132caebd6

DROP TRIGGER IF EXISTS update_documents_updated_at ON documents;
DROP FUNCTION IF EXISTS update_updated_at_column();
DROP TABLE IF EXISTS documents CASCADE;

-- Recreate memory table for rollback
CREATE TABLE memory (
    id SERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    text TEXT NOT NULL,
    emb vector(128),
    timestamp TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX memory_chat_user_idx ON memory (chat_id, user_id);
CREATE INDEX memory_timestamp_idx ON memory (timestamp);
