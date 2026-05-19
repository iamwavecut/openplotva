-- Source SHA-256: 83a9beadaffdc319c97d54a1b096bdadc1af97e84bbd065e7e6932ebfdcf0158

CREATE TABLE IF NOT EXISTS whitecircle_checks (
	id BIGSERIAL PRIMARY KEY,
	created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	source TEXT NOT NULL,
	flow TEXT,
	mode TEXT,
	chat_id BIGINT,
	thread_id INTEGER,
	message_id INTEGER,
	user_id BIGINT,
	deployment_id TEXT NOT NULL,
	external_session_id TEXT,
	request_messages JSONB NOT NULL DEFAULT '[]'::jsonb,
	flagged BOOLEAN,
	internal_session_id TEXT,
	policies JSONB,
	response_json JSONB,
	duration_ms INTEGER NOT NULL DEFAULT 0,
	error TEXT
);

CREATE INDEX IF NOT EXISTS idx_whitecircle_checks_created_at
	ON whitecircle_checks (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_whitecircle_checks_chat_created_at
	ON whitecircle_checks (chat_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_whitecircle_checks_flagged_created_at
	ON whitecircle_checks (flagged, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_whitecircle_checks_external_session_created_at
	ON whitecircle_checks (external_session_id, created_at DESC);
