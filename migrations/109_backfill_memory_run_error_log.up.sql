-- Source SHA-256: 070269bf2b3ba0cae80e35a995b30e50f7efb0991803c7c74296061bedd8a311

UPDATE memory_runs
SET error_log = error_log || jsonb_build_array(jsonb_build_object(
        'attempt', attempts,
        'failed_at', COALESCE(completed_at, updated_at, CURRENT_TIMESTAMP),
        'error', left(error, 4000)
    ))
WHERE status = 'failed'
  AND error <> ''
  AND jsonb_array_length(error_log) = 0;
