-- Source SHA-256: d06bcb4770b6249ba9bd8f202d238f996506cd569c03d6f1f518ccdd2ae1e47c

ALTER TABLE memory_runs
    ADD COLUMN IF NOT EXISTS error_log JSONB NOT NULL DEFAULT '[]'::jsonb;

UPDATE memory_runs
SET error_log = error_log || jsonb_build_array(jsonb_build_object(
        'attempt', attempts,
        'failed_at', COALESCE(completed_at, updated_at, CURRENT_TIMESTAMP),
        'error', left(error, 4000)
    ))
WHERE status = 'failed'
  AND error <> ''
  AND jsonb_array_length(error_log) = 0;

UPDATE memory_runs
SET status = 'failed',
    lease_owner = '',
    leased_until = NULL,
    error = CASE
        WHEN error <> '' THEN error
        ELSE 'memory run exhausted after 5 attempts without a captured failure'
    END,
    error_log = CASE
        WHEN jsonb_array_length(error_log) > 0 THEN error_log
        ELSE error_log || jsonb_build_array(jsonb_build_object(
            'attempt', attempts,
            'failed_at', CURRENT_TIMESTAMP,
            'error', CASE
                WHEN error <> '' THEN left(error, 4000)
                ELSE 'memory run exhausted after 5 attempts without a captured failure'
            END
        ))
    END,
    completed_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
WHERE status = 'processing'
  AND attempts >= 5
  AND (leased_until IS NULL OR leased_until < CURRENT_TIMESTAMP);
