-- no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_telegram_files_requested ON telegram_files (recognition_requested_at) WHERE recognition_completed_at IS NULL;
