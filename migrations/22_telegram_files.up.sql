-- Source SHA-256: 63be40405a4d2bc609975be58af4a4d8d9370e88a125e3256e9e2d297407a8ca

CREATE TABLE IF NOT EXISTS telegram_files (
	file_unique_id TEXT PRIMARY KEY,
	latest_file_id TEXT NOT NULL,
	media_kind TEXT NOT NULL,
	mime_type TEXT,
	width INTEGER,
	height INTEGER,
	file_size BIGINT,
	first_seen_chat_id BIGINT,
	first_seen_message_id BIGINT,
	last_seen_chat_id BIGINT,
	last_seen_message_id BIGINT,
	last_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	vision_status TEXT NOT NULL DEFAULT 'pending',
	vision_caption TEXT,
	vision_model TEXT,
	vision_latency_ms INTEGER,
	recognition_requested_at TIMESTAMPTZ,
	recognition_completed_at TIMESTAMPTZ,
	extra JSONB DEFAULT '{}'::jsonb,
	created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_telegram_files_last_seen
	ON telegram_files (last_seen_chat_id, last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_telegram_files_pending_status
	ON telegram_files (vision_status)
	WHERE vision_status <> 'completed';

CREATE INDEX IF NOT EXISTS idx_telegram_files_requested
	ON telegram_files (recognition_requested_at)
	WHERE recognition_completed_at IS NULL;
