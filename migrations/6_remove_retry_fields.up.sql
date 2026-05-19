-- Source SHA-256: a3da358b608318c767ddfd8c54e006e93eb81d68104acb4a4184b9d31fbdfc50

DROP INDEX IF EXISTS idx_job_queue_next_retry;
ALTER TABLE job_queue DROP COLUMN IF EXISTS retry_count;
ALTER TABLE job_queue DROP COLUMN IF EXISTS max_retries;
ALTER TABLE job_queue DROP COLUMN IF EXISTS next_retry_at;
ALTER TABLE job_queue DROP COLUMN IF EXISTS original_job_id;
