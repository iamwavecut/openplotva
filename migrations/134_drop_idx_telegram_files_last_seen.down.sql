-- no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_telegram_files_last_seen ON telegram_files (last_seen_chat_id, last_seen_at DESC);
