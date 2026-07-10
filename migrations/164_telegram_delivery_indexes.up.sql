-- no-transaction

-- SQLx executes one no-transaction migration as one statement. Keep exactly
-- one concurrent index in each migration so Postgres does not wrap multiple
-- statements in an implicit transaction.
CREATE INDEX CONCURRENTLY IF NOT EXISTS taskman_jobs_available_order_idx
    ON taskman_jobs (queue_name, status, available_at, priority DESC, created_at, id)
    WHERE deleted_at IS NULL;
