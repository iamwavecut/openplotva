-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS taskman_jobs_lane_idx
    ON taskman_jobs (lane_key, status, available_at, id)
    WHERE deleted_at IS NULL AND lane_key IS NOT NULL;
