-- Source SHA-256: 11b8fe6b6a570dc0a090fd8be1f5d29befe897601970ed350eacabb5a4e8965b

CREATE TABLE IF NOT EXISTS chat_history_summaries (
    id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    thread_id INTEGER NOT NULL DEFAULT 0,
    scope TEXT NOT NULL CHECK (scope IN ('chat', 'thread')),
    requested_by_user_id BIGINT NOT NULL DEFAULT 0,
    range_start_at TIMESTAMPTZ NOT NULL,
    range_end_at TIMESTAMPTZ NOT NULL,
    first_message_id INTEGER NOT NULL DEFAULT 0,
    last_message_id INTEGER NOT NULL DEFAULT 0,
    first_entry_id TEXT NOT NULL DEFAULT '',
    last_entry_id TEXT NOT NULL DEFAULT '',
    raw_message_count INTEGER NOT NULL DEFAULT 0,
    covered_message_count INTEGER NOT NULL DEFAULT 0,
    source_summary_ids BIGINT[] NOT NULL DEFAULT '{}',
    summary_json JSONB NOT NULL,
    summary_html TEXT NOT NULL,
    model TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    input_hash TEXT NOT NULL DEFAULT '',
    prompt_hash TEXT NOT NULL DEFAULT '',
    input_token_estimate INTEGER NOT NULL DEFAULT 0,
    output_token_estimate INTEGER NOT NULL DEFAULT 0,
    cascade_depth INTEGER NOT NULL DEFAULT 0,
    quality_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    quality_notes TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_chat_history_summaries_range
    ON chat_history_summaries (chat_id, thread_id, range_start_at, range_end_at);

CREATE INDEX IF NOT EXISTS idx_chat_history_summaries_recent
    ON chat_history_summaries (chat_id, thread_id, created_at DESC);

CREATE TABLE IF NOT EXISTS chat_history_summary_sources (
    id BIGSERIAL PRIMARY KEY,
    summary_id BIGINT NOT NULL REFERENCES chat_history_summaries(id) ON DELETE CASCADE,
    source_order INTEGER NOT NULL,
    source_type TEXT NOT NULL CHECK (source_type IN ('summary', 'message_range')),
    source_summary_id BIGINT REFERENCES chat_history_summaries(id) ON DELETE SET NULL,
    range_start_at TIMESTAMPTZ NOT NULL,
    range_end_at TIMESTAMPTZ NOT NULL,
    first_message_id INTEGER NOT NULL DEFAULT 0,
    last_message_id INTEGER NOT NULL DEFAULT 0,
    first_entry_id TEXT NOT NULL DEFAULT '',
    last_entry_id TEXT NOT NULL DEFAULT '',
    raw_message_count INTEGER NOT NULL DEFAULT 0,
    covered_message_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_history_summary_sources_order
    ON chat_history_summary_sources (summary_id, source_order);

CREATE INDEX IF NOT EXISTS idx_chat_history_summary_sources_source_summary
    ON chat_history_summary_sources (source_summary_id);

CREATE TABLE IF NOT EXISTS chat_history_events (
    id BIGSERIAL PRIMARY KEY,
    summary_id BIGINT NOT NULL REFERENCES chat_history_summaries(id) ON DELETE CASCADE,
    chat_id BIGINT NOT NULL,
    thread_id INTEGER NOT NULL DEFAULT 0,
    scope TEXT NOT NULL CHECK (scope IN ('chat', 'thread')),
    event_order INTEGER NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    actors TEXT[] NOT NULL DEFAULT '{}',
    occurred_at TIMESTAMPTZ,
    range_start_at TIMESTAMPTZ NOT NULL,
    range_end_at TIMESTAMPTZ NOT NULL,
    source_summary_ids BIGINT[] NOT NULL DEFAULT '{}',
    confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_history_events_summary_order
    ON chat_history_events (summary_id, event_order);

CREATE INDEX IF NOT EXISTS idx_chat_history_events_chat_thread_time
    ON chat_history_events (chat_id, thread_id, (COALESCE(occurred_at, range_start_at)), id);
