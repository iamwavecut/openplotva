-- no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_telegram_files_pending_status ON telegram_files (vision_status) WHERE vision_status <> 'completed';
