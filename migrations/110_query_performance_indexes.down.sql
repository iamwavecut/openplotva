-- no-transaction

DROP INDEX CONCURRENTLY IF EXISTS idx_memory_runs_range_start_id_desc;
DROP INDEX CONCURRENTLY IF EXISTS idx_memory_runs_claim_queued_order;
DROP INDEX CONCURRENTLY IF EXISTS idx_memory_runs_claim_failed_order;
DROP INDEX CONCURRENTLY IF EXISTS idx_memory_runs_claim_processing_order;
DROP INDEX CONCURRENTLY IF EXISTS idx_chats_last_name_trgm;
DROP INDEX CONCURRENTLY IF EXISTS idx_chats_first_name_trgm;
DROP INDEX CONCURRENTLY IF EXISTS idx_users_lower_username;
