-- Rollback discards ASR diagnostics only; cached transcripts remain intact.
ALTER TABLE telegram_files
  DROP COLUMN IF EXISTS asr_warnings,
  DROP COLUMN IF EXISTS asr_chunks,
  DROP COLUMN IF EXISTS asr_fallback_used;
