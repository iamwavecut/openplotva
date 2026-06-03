-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_memory_runs_range_start_id_desc
    ON memory_runs (range_start_at DESC, id DESC);
