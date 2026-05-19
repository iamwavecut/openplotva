-- Source SHA-256: 511aa2a9778efeaffd198a7db497986a42e099764ef3024b6edca3bf30338607

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_users_username_trgm ON users USING gin (username gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_users_first_name_trgm ON users USING gin (first_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_users_last_name_trgm ON users USING gin (last_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_chats_title_trgm ON chats USING gin (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_chats_username_trgm ON chats USING gin (username gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_chat_members_user_id ON chat_members (user_id, chat_id);
