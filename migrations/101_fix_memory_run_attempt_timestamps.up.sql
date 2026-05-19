-- Source SHA-256: f7268a0404ab7b0f185a3de56821240df808823f11f4e5802410855331857b96

UPDATE memory_runs
SET started_at = completed_at
WHERE status = 'completed'
  AND started_at IS NOT NULL
  AND completed_at IS NOT NULL
  AND completed_at - started_at > interval '1 day';

UPDATE memory_runs
SET started_at = NULL,
    completed_at = NULL
WHERE status IN ('queued', 'failed')
  AND (started_at IS NOT NULL OR completed_at IS NOT NULL);
