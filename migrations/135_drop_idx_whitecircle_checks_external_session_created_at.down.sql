-- no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_whitecircle_checks_external_session_created_at ON whitecircle_checks (external_session_id, created_at DESC);
