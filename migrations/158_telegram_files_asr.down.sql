ALTER TABLE telegram_files
    DROP COLUMN IF EXISTS asr_completed_at,
    DROP COLUMN IF EXISTS asr_requested_at,
    DROP COLUMN IF EXISTS asr_error,
    DROP COLUMN IF EXISTS asr_latency_ms,
    DROP COLUMN IF EXISTS asr_model,
    DROP COLUMN IF EXISTS asr_provider,
    DROP COLUMN IF EXISTS asr_text,
    DROP COLUMN IF EXISTS asr_status;
