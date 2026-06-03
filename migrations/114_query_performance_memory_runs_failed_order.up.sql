-- no-transaction

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
