-- Source SHA-256: a39f04860aed359bf8b78d8f7288c9fd70fe726231badec49796e5f8b31dcc67

CREATE TABLE IF NOT EXISTS runtime_api_tokens (
    id TEXT PRIMARY KEY,
    token_hash BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_api_tokens_token_hash
    ON runtime_api_tokens (token_hash);

CREATE INDEX IF NOT EXISTS idx_runtime_api_tokens_created_at
    ON runtime_api_tokens (created_at DESC);
