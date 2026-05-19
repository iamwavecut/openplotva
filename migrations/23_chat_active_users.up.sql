-- Source SHA-256: ddd534513d7301f7fa9817022bdba61d901ab49e3fe88532ffbfa131bd926521

CREATE TABLE IF NOT EXISTS chat_active_users (
	chat_id BIGINT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
	user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
	last_active_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (chat_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_chat_active_users_chat_id_last_active_at
	ON chat_active_users (chat_id, last_active_at DESC);

CREATE INDEX IF NOT EXISTS idx_chat_active_users_last_active_at
	ON chat_active_users (last_active_at DESC);
