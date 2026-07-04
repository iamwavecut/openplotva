-- Retire the taskman job telemetry_rollups aggregation. It was a delayed
-- (~2x the completed-job retention, so ~2 days) historical rollup produced at
-- hard-purge time; nothing reads it now — the admin analytics dashboard reads
-- taskman_jobs directly and in real time. Drop the stale rollup rows, and index
-- taskman_jobs on the dashboard's terminal-time window expression so the direct
-- read stays fast as the table grows (COALESCE of timestamptz is immutable, so it
-- is index-eligible, and it matches the dashboard's WHERE clause exactly).
DELETE FROM telemetry_rollups WHERE source = 'taskman' AND kind = 'job';

CREATE INDEX IF NOT EXISTS idx_taskman_jobs_terminal_time
    ON taskman_jobs (COALESCE(completed_at, updated_at, created_at));
