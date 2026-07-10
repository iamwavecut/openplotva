-- no-transaction

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS taskman_jobs_pending_debounce_idx
    ON taskman_jobs (queue_name, debounce_key)
    WHERE deleted_at IS NULL AND status = 'pending' AND debounce_key IS NOT NULL;
