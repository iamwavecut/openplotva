-- Source SHA-256: 7d92e9d37b1f18e810c46a7d32da1fbd2d0757452df715353b367645bcf61317

DROP INDEX IF EXISTS idx_job_queue_duplicate_detection;
DROP INDEX IF EXISTS idx_job_queue_completed_by_queue;
DROP INDEX IF EXISTS idx_job_queue_active_user_chat;
DROP INDEX IF EXISTS idx_job_queue_completed_cleanup;
DROP INDEX IF EXISTS idx_job_queue_processing_worker_id_partial;
DROP INDEX IF EXISTS idx_job_queue_pending_queue_priority;
DROP INDEX IF EXISTS idx_job_queue_pending_queue_created_at;
DROP INDEX IF EXISTS idx_job_queue_pending_queue_prio_created_at;
