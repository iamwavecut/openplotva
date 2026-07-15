-- no-transaction

-- Bound runtime update-to-task correlation to indexed array containment.
CREATE INDEX CONCURRENTLY IF NOT EXISTS taskman_jobs_source_update_ids_idx
    ON taskman_jobs USING GIN (source_update_ids)
    WHERE deleted_at IS NULL AND cardinality(source_update_ids) > 0;
