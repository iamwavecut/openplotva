-- Source SHA-256: 7876d9106187b3da5d529b53b2d107f3c40ceaffc8999bd64a48901e7283e616

CREATE EXTENSION IF NOT EXISTS vector;

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
