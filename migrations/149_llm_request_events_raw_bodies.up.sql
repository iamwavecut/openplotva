-- Short-lived raw LLM request/response bodies for the admin "LLM Dialogs"
-- detail view. Written only for recent rows (capped size), scrubbed to NULL by
-- an hourly worker after LLM_RAW_BODY_RETENTION_HOURS.
ALTER TABLE llm_request_events
    ADD COLUMN raw_request JSONB,
    ADD COLUMN raw_response JSONB;

ALTER TABLE llm_request_events
    ALTER COLUMN raw_request SET COMPRESSION lz4,
    ALTER COLUMN raw_response SET COMPRESSION lz4;

-- The scrub worker scans by age over rows that still carry bodies.
CREATE INDEX idx_llm_request_events_raw_scrub
    ON llm_request_events (created_at)
    WHERE raw_request IS NOT NULL OR raw_response IS NOT NULL;
