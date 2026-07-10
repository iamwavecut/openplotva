-- Persist engine fallback diagnostics so production audits can distinguish GPU
-- contention, decode failures, and other primary-engine failures per voice file.
-- Nullable columns keep existing rows and older writers compatible.
ALTER TABLE telegram_files
  ADD COLUMN IF NOT EXISTS asr_fallback_used BOOLEAN,
  ADD COLUMN IF NOT EXISTS asr_chunks INTEGER,
  ADD COLUMN IF NOT EXISTS asr_warnings TEXT[];
