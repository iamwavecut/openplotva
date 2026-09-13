-- Material of every generated song: the compiled tag list, lyrics and the
-- director's brief, so a listener can ask for another take of the same song
-- (new seed, no LLM call) and operators can trace what the music model heard.
CREATE TABLE IF NOT EXISTS generated_songs (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT,
    retake_of BIGINT REFERENCES generated_songs(id) ON DELETE SET NULL,
    chat_id BIGINT NOT NULL,
    thread_id INTEGER,
    user_id BIGINT NOT NULL,
    user_full_name TEXT NOT NULL DEFAULT '',
    trigger_message_id INTEGER NOT NULL DEFAULT 0,
    result_message_id INTEGER,
    request_text TEXT NOT NULL DEFAULT '',
    topic TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    vocal_language TEXT NOT NULL DEFAULT '',
    vocals TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '',
    style_summary TEXT NOT NULL DEFAULT '',
    lyrics TEXT NOT NULL DEFAULT '',
    duration_seconds INTEGER NOT NULL DEFAULT 0,
    brief JSONB NOT NULL DEFAULT '{}'::jsonb,
    seed BIGINT,
    audio_seconds REAL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- New, empty table: a plain index is safe here.
CREATE INDEX IF NOT EXISTS idx_generated_songs_chat_created
    ON generated_songs (chat_id, created_at DESC);
