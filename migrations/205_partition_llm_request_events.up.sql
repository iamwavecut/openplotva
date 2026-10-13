-- Split llm_request_events by kind: long-lived rollups in one partition, raw
-- events in daily partitions that retention drops once they are rolled up.
-- Unique keys must contain the partition key on a partitioned table, so the
-- primary key on id becomes a plain index; ids still come from the sequence.
-- The rollup key stays unique on the rollup partition, where the rollup's
-- ON CONFLICT DO NOTHING checks it.
ALTER TABLE llm_request_events RENAME TO llm_request_events_legacy;

CREATE TABLE llm_request_events (
    LIKE llm_request_events_legacy
    INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMPRESSION
) PARTITION BY LIST (is_rollup);

CREATE TABLE llm_request_events_rollup
    PARTITION OF llm_request_events FOR VALUES IN (TRUE);

CREATE TABLE llm_request_events_raw
    PARTITION OF llm_request_events FOR VALUES IN (FALSE)
    PARTITION BY RANGE (created_at);

SELECT ensure_daily_partition('llm_request_events_raw', day::date)
FROM generate_series(
    LEAST(
        (SELECT min((created_at AT TIME ZONE 'UTC')::date)
         FROM llm_request_events_legacy
         WHERE NOT is_rollup),
        (now() AT TIME ZONE 'UTC')::date
    ),
    (now() AT TIME ZONE 'UTC')::date + 7,
    interval '1 day'
) AS day;

INSERT INTO llm_request_events
SELECT * FROM llm_request_events_legacy;

ALTER SEQUENCE llm_request_events_id_seq OWNED BY llm_request_events.id;

DROP TABLE llm_request_events_legacy;

CREATE INDEX idx_llm_request_events_id ON llm_request_events (id);
CREATE INDEX idx_llm_request_events_created_at ON llm_request_events (created_at DESC);
CREATE INDEX idx_llm_request_events_chat_created_at ON llm_request_events (chat_id, created_at DESC);
CREATE INDEX idx_llm_request_events_model_bucket ON llm_request_events (model, created_at DESC);
CREATE INDEX idx_llm_request_events_provider_created_at ON llm_request_events (provider, created_at DESC);
CREATE INDEX idx_llm_request_events_source_created_at ON llm_request_events (source, created_at DESC);
CREATE INDEX idx_llm_request_events_raw_scrub ON llm_request_events (created_at)
    WHERE raw_request IS NOT NULL OR raw_response IS NOT NULL;
CREATE INDEX idx_llm_request_events_run_id ON llm_request_events (run_id)
    WHERE run_id IS NOT NULL;

CREATE UNIQUE INDEX idx_llm_request_events_rollup_key
    ON llm_request_events_rollup (
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
