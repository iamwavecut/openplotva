-- Source SHA-256: 25fe2f5dd01129a76dd45362caeda581a8dd8af1ce0e0cefd66f42dfd0473bc8

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS memory_cards (
    id BIGSERIAL PRIMARY KEY,
    visibility TEXT NOT NULL CHECK (visibility IN ('public_user', 'chat_user', 'private_chat', 'chat', 'thread')),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'superseded', 'deleted')),
    card_type TEXT NOT NULL DEFAULT 'preference' CHECK (card_type IN ('preference', 'identity', 'project', 'decision', 'relationship', 'recurring_topic', 'joke', 'warning', 'technical_fact', 'event')),
    subject TEXT NOT NULL DEFAULT '',
    predicate TEXT NOT NULL DEFAULT '',
    object TEXT NOT NULL DEFAULT '',
    fact_text TEXT NOT NULL,
    dedup_hash TEXT NOT NULL,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (confidence >= 0 AND confidence <= 1),
    salience DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (salience >= 0 AND salience <= 1),
    observation_count INTEGER NOT NULL DEFAULT 1,
    origin_chat_id BIGINT NOT NULL DEFAULT 0,
    origin_thread_id INTEGER NOT NULL DEFAULT 0,
    origin_user_id BIGINT NOT NULL DEFAULT 0,
    chat_id BIGINT NOT NULL DEFAULT 0,
    thread_id INTEGER NOT NULL DEFAULT 0,
    user_id BIGINT NOT NULL DEFAULT 0,
    valid_from TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_until TIMESTAMPTZ,
    superseded_by BIGINT REFERENCES memory_cards(id) ON DELETE SET NULL,
    last_observed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMPTZ,
    use_count INTEGER NOT NULL DEFAULT 0,
    decay_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    embedding vector(512),
    text_search tsvector GENERATED ALWAYS AS (
        to_tsvector('simple', subject || ' ' || predicate || ' ' || object || ' ' || fact_text)
    ) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMPTZ,
    deleted_by_user_id BIGINT NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS memory_cards_active_dedup_idx
    ON memory_cards (visibility, user_id, chat_id, thread_id, dedup_hash)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS memory_cards_scope_idx
    ON memory_cards (status, visibility, user_id, chat_id, thread_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS memory_cards_text_search_idx
    ON memory_cards USING gin (text_search);

CREATE INDEX IF NOT EXISTS memory_cards_embedding_hnsw_idx
    ON memory_cards USING hnsw (embedding vector_cosine_ops)
    WHERE embedding IS NOT NULL;

CREATE TABLE IF NOT EXISTS memory_episodes (
    id BIGSERIAL PRIMARY KEY,
    visibility TEXT NOT NULL CHECK (visibility IN ('chat', 'thread')),
    chat_id BIGINT NOT NULL,
    thread_id INTEGER NOT NULL DEFAULT 0,
    range_start_at TIMESTAMPTZ NOT NULL,
    range_end_at TIMESTAMPTZ NOT NULL,
    message_count INTEGER NOT NULL DEFAULT 0,
    summary_text TEXT NOT NULL,
    topics TEXT[] NOT NULL DEFAULT '{}',
    participants TEXT[] NOT NULL DEFAULT '{}',
    model TEXT NOT NULL DEFAULT '',
    prompt_version TEXT NOT NULL DEFAULT '',
    cursor_after_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00',
    cursor_after_message_id INTEGER NOT NULL DEFAULT 0,
    cursor_after_entry_id TEXT NOT NULL DEFAULT '',
    input_hash TEXT NOT NULL DEFAULT '',
    input_token_estimate INTEGER NOT NULL DEFAULT 0,
    output_token_estimate INTEGER NOT NULL DEFAULT 0,
    embedding vector(512),
    text_search tsvector GENERATED ALWAYS AS (
        to_tsvector('simple', summary_text)
    ) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (chat_id, thread_id, range_start_at, range_end_at, prompt_version, cursor_after_at, cursor_after_message_id, cursor_after_entry_id)
);

CREATE INDEX IF NOT EXISTS memory_episodes_scope_idx
    ON memory_episodes (chat_id, thread_id, range_end_at DESC);

CREATE INDEX IF NOT EXISTS memory_episodes_text_search_idx
    ON memory_episodes USING gin (text_search);

CREATE INDEX IF NOT EXISTS memory_episodes_embedding_hnsw_idx
    ON memory_episodes USING hnsw (embedding vector_cosine_ops)
    WHERE embedding IS NOT NULL;

CREATE TABLE IF NOT EXISTS memory_sources (
    id BIGSERIAL PRIMARY KEY,
    card_id BIGINT REFERENCES memory_cards(id) ON DELETE CASCADE,
    episode_id BIGINT REFERENCES memory_episodes(id) ON DELETE CASCADE,
    chat_id BIGINT NOT NULL,
    thread_id INTEGER NOT NULL DEFAULT 0,
    entry_id TEXT NOT NULL DEFAULT '',
    message_id INTEGER NOT NULL DEFAULT 0,
    occurred_at TIMESTAMPTZ NOT NULL,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (card_id IS NOT NULL OR episode_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS memory_sources_card_idx ON memory_sources (card_id);
CREATE INDEX IF NOT EXISTS memory_sources_episode_idx ON memory_sources (episode_id);
CREATE INDEX IF NOT EXISTS memory_sources_chat_time_idx ON memory_sources (chat_id, thread_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS memory_links (
    id BIGSERIAL PRIMARY KEY,
    from_card_id BIGINT NOT NULL REFERENCES memory_cards(id) ON DELETE CASCADE,
    to_card_id BIGINT NOT NULL REFERENCES memory_cards(id) ON DELETE CASCADE,
    relation TEXT NOT NULL CHECK (relation IN ('supports', 'contradicts', 'same_topic', 'supersedes', 'mentions_same_entity')),
    confidence DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (confidence >= 0 AND confidence <= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (from_card_id, to_card_id, relation)
);

CREATE INDEX IF NOT EXISTS memory_links_from_idx ON memory_links (from_card_id, relation);
CREATE INDEX IF NOT EXISTS memory_links_to_idx ON memory_links (to_card_id, relation);

CREATE TABLE IF NOT EXISTS memory_runs (
    id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    thread_id INTEGER NOT NULL DEFAULT 0,
    range_start_at TIMESTAMPTZ NOT NULL,
    range_end_at TIMESTAMPTZ NOT NULL,
    prompt_version TEXT NOT NULL,
    cursor_after_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00',
    cursor_after_message_id INTEGER NOT NULL DEFAULT 0,
    cursor_after_entry_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'processing', 'completed', 'failed', 'skipped')),
    lease_owner TEXT NOT NULL DEFAULT '',
    leased_until TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    message_count INTEGER NOT NULL DEFAULT 0,
    cards_inserted INTEGER NOT NULL DEFAULT 0,
    cards_updated INTEGER NOT NULL DEFAULT 0,
    cards_superseded INTEGER NOT NULL DEFAULT 0,
    episodes_inserted INTEGER NOT NULL DEFAULT 0,
    input_token_estimate INTEGER NOT NULL DEFAULT 0,
    output_token_estimate INTEGER NOT NULL DEFAULT 0,
    error TEXT NOT NULL DEFAULT '',
    error_log JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (chat_id, thread_id, range_start_at, range_end_at, prompt_version, cursor_after_at, cursor_after_message_id, cursor_after_entry_id)
);

CREATE INDEX IF NOT EXISTS memory_runs_claim_idx
    ON memory_runs (status, leased_until, created_at);

CREATE INDEX IF NOT EXISTS memory_runs_chat_range_idx
    ON memory_runs (chat_id, thread_id, range_start_at, range_end_at, cursor_after_at, cursor_after_message_id);
