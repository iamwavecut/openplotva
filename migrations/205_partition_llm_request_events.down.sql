-- Back to one plain table. Raw partitions already dropped by retention are gone.
ALTER TABLE llm_request_events RENAME TO llm_request_events_partitioned;

CREATE TABLE llm_request_events (
    LIKE llm_request_events_partitioned
    INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMPRESSION
);

INSERT INTO llm_request_events
SELECT * FROM llm_request_events_partitioned;

ALTER SEQUENCE llm_request_events_id_seq OWNED BY llm_request_events.id;

DROP TABLE llm_request_events_partitioned;

ALTER TABLE llm_request_events ADD CONSTRAINT llm_request_events_pkey PRIMARY KEY (id);
CREATE INDEX idx_llm_request_events_created_at ON llm_request_events (created_at DESC);
CREATE INDEX idx_llm_request_events_chat_created_at ON llm_request_events (chat_id, created_at DESC);
CREATE INDEX idx_llm_request_events_model_bucket ON llm_request_events (model, created_at DESC);
CREATE INDEX idx_llm_request_events_provider_created_at ON llm_request_events (provider, created_at DESC);
CREATE INDEX idx_llm_request_events_source_created_at ON llm_request_events (source, created_at DESC);
CREATE INDEX idx_llm_request_events_raw_cleanup ON llm_request_events (created_at)
    WHERE is_rollup = FALSE;
CREATE INDEX idx_llm_request_events_raw_scrub ON llm_request_events (created_at)
    WHERE raw_request IS NOT NULL OR raw_response IS NOT NULL;
CREATE INDEX idx_llm_request_events_run_id ON llm_request_events (run_id)
    WHERE run_id IS NOT NULL;
CREATE UNIQUE INDEX idx_llm_request_events_rollup_key
    ON llm_request_events (
        rollup_granularity,
        bucket_start,
        COALESCE(source, ''),
        COALESCE(provider, ''),
        COALESCE(model, ''),
        COALESCE(chat_id, 0),
        COALESCE(max_tokens, -1),
        COALESCE(temperature, -999999.0),
        COALESCE(top_p, -999999.0),
        COALESCE(top_k, -1),
        COALESCE(candidate_count, -1),
        COALESCE(tool_mode, ''),
        COALESCE(response_format, '')
    )
    WHERE is_rollup = TRUE;
