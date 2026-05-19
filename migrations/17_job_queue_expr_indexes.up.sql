-- Source SHA-256: ec15cd9027743adb616916d037854c8dd7c7382ca930874dcfa9252be0e032df

CREATE INDEX IF NOT EXISTS idx_job_queue_processing_timeout ON job_queue (started_at, processing_timeout_seconds) WHERE status = 'processing' AND started_at IS NOT NULL AND processing_timeout_seconds IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_job_queue_status_created_at_desc ON job_queue (status, created_at DESC);
