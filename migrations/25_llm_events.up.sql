-- Source SHA-256: 7a19d9b1baf8def4eff4d1b063780c1fa13726e26a6652af0fa844095398bfc4

CREATE TABLE IF NOT EXISTS llm_request_events (
	id BIGSERIAL PRIMARY KEY,
	created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	source TEXT NOT NULL,
	flow TEXT,
	chat_id BIGINT REFERENCES chats(id) ON DELETE SET NULL,
	thread_id INTEGER,
	message_id INTEGER,
	user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
	model TEXT,
	iteration INTEGER NOT NULL DEFAULT 1,
	prompt_chars INTEGER NOT NULL,
	prompt_messages INTEGER NOT NULL,
	docs_chars INTEGER NOT NULL,
	duration_ms INTEGER NOT NULL,
	error TEXT
);

CREATE INDEX IF NOT EXISTS idx_llm_request_events_created_at
	ON llm_request_events (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_llm_request_events_chat_created_at
	ON llm_request_events (chat_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_llm_request_events_source_created_at
	ON llm_request_events (source, created_at DESC);
