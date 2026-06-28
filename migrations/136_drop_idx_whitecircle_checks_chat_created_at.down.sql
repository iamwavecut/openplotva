-- no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_whitecircle_checks_chat_created_at ON whitecircle_checks (chat_id, created_at DESC);
