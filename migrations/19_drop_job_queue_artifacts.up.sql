-- Source SHA-256: 71db06f81a0d9345de2ce7b51ca85c48f25921eee48b6befcbf0b4f704690c6f

-- Drop indexes if they exist to avoid dependency errors
DO $$
BEGIN
	IF to_regclass('public.idx_job_queue_status_priority') IS NOT NULL THEN
		EXECUTE 'DROP INDEX IF EXISTS idx_job_queue_status_priority';
	END IF;
	IF to_regclass('public.idx_job_queue_queue_name') IS NOT NULL THEN
		EXECUTE 'DROP INDEX IF EXISTS idx_job_queue_queue_name';
	END IF;
	IF to_regclass('public.idx_job_queue_worker_id') IS NOT NULL THEN
		EXECUTE 'DROP INDEX IF EXISTS idx_job_queue_worker_id';
	END IF;
	IF to_regclass('public.idx_job_queue_next_retry') IS NOT NULL THEN
		EXECUTE 'DROP INDEX IF EXISTS idx_job_queue_next_retry';
	END IF;
	IF to_regclass('public.idx_job_queue_processing_timeout') IS NOT NULL THEN
		EXECUTE 'DROP INDEX IF EXISTS idx_job_queue_processing_timeout';
	END IF;
	IF to_regclass('public.idx_job_queue_user_status') IS NOT NULL THEN
		EXECUTE 'DROP INDEX IF EXISTS idx_job_queue_user_status';
	END IF;
	IF to_regclass('public.idx_job_queue_chat_user') IS NOT NULL THEN
		EXECUTE 'DROP INDEX IF EXISTS idx_job_queue_chat_user';
	END IF;
	IF to_regclass('public.idx_job_queue_trigger_message') IS NOT NULL THEN
		EXECUTE 'DROP INDEX IF EXISTS idx_job_queue_trigger_message';
	END IF;
	IF to_regclass('public.idx_job_queue_dedup') IS NOT NULL THEN
		EXECUTE 'DROP INDEX IF EXISTS idx_job_queue_dedup';
	END IF;
	IF to_regclass('public.idx_job_queue_progress_messages') IS NOT NULL THEN
		EXECUTE 'DROP INDEX IF EXISTS idx_job_queue_progress_messages';
	END IF;
END$$;
-- Drop job_messages indexes first
DO $$
BEGIN
	IF to_regclass('public.idx_job_messages_job_id') IS NOT NULL THEN
		EXECUTE 'DROP INDEX IF EXISTS idx_job_messages_job_id';
	END IF;
	IF to_regclass('public.idx_job_messages_expires_at') IS NOT NULL THEN
		EXECUTE 'DROP INDEX IF EXISTS idx_job_messages_expires_at';
	END IF;
	IF to_regclass('public.idx_job_messages_ephemeral') IS NOT NULL THEN
		EXECUTE 'DROP INDEX IF EXISTS idx_job_messages_ephemeral';
	END IF;
	IF to_regclass('public.idx_job_messages_chat_message') IS NOT NULL THEN
		EXECUTE 'DROP INDEX IF EXISTS idx_job_messages_chat_message';
	END IF;
END$$;
-- Drop tables if exist
DROP TABLE IF EXISTS job_messages CASCADE;
DROP TABLE IF EXISTS job_queue CASCADE;
