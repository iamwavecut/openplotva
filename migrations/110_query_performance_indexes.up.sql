-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_lower_username
    ON users (lower(username))
    WHERE username IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_chats_first_name_trgm
    ON chats USING gin (first_name gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_chats_last_name_trgm
    ON chats USING gin (last_name gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_memory_runs_claim_processing_order
    ON memory_runs (
        prompt_version,
        range_end_at DESC,
        range_start_at DESC,
        cursor_after_at ASC,
        cursor_after_message_id ASC,
        id ASC
    )
    WHERE status = 'processing' AND attempts < 5;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_memory_runs_claim_failed_order
    ON memory_runs (
        prompt_version,
        range_end_at DESC,
        range_start_at DESC,
        cursor_after_at ASC,
        cursor_after_message_id ASC,
        id ASC
    )
    WHERE status = 'failed' AND attempts < 5;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_memory_runs_claim_queued_order
    ON memory_runs (
        prompt_version,
        range_end_at DESC,
        range_start_at DESC,
        cursor_after_at ASC,
        cursor_after_message_id ASC,
        id ASC
    )
    WHERE status = 'queued';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_memory_runs_range_start_id_desc
    ON memory_runs (range_start_at DESC, id DESC);
