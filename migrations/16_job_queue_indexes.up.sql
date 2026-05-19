-- Source SHA-256: 7d92e9d37b1f18e810c46a7d32da1fbd2d0757452df715353b367645bcf61317

CREATE INDEX IF NOT EXISTS idx_job_queue_pending_queue_prio_created_at ON job_queue (queue_name, priority DESC, created_at ASC) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_job_queue_pending_queue_created_at ON job_queue (queue_name, created_at ASC) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_job_queue_pending_queue_priority ON job_queue (queue_name, priority) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_job_queue_processing_worker_id_partial ON job_queue (worker_id) WHERE status = 'processing' AND worker_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_job_queue_completed_cleanup ON job_queue (completed_at) WHERE status IN ('completed','cancelled','failed') AND completed_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_job_queue_active_user_chat ON job_queue (user_id, chat_id) WHERE status IN ('pending','processing');
CREATE INDEX IF NOT EXISTS idx_job_queue_completed_by_queue ON job_queue (queue_name, completed_at DESC) WHERE status = 'completed' AND completed_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_job_queue_duplicate_detection ON job_queue (chat_id, user_id, prompt_hash, created_at DESC) WHERE status IN ('pending', 'processing');
