-- no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_whitecircle_checks_flagged_created_at ON whitecircle_checks (flagged, created_at DESC);
