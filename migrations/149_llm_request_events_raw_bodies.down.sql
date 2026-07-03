DROP INDEX IF EXISTS idx_llm_request_events_raw_scrub;

ALTER TABLE llm_request_events
    DROP COLUMN IF EXISTS raw_request,
    DROP COLUMN IF EXISTS raw_response;
