DROP INDEX IF EXISTS idx_llm_request_events_run_id;
ALTER TABLE llm_request_events
    DROP COLUMN IF EXISTS run_id,
    DROP COLUMN IF EXISTS run_seq;
