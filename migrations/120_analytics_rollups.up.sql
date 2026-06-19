ALTER TABLE llm_request_events
    ADD COLUMN IF NOT EXISTS is_rollup BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS rollup_granularity TEXT,
    ADD COLUMN IF NOT EXISTS bucket_start TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS bucket_end TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS request_count INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS error_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS duration_ms_sum BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS p50_duration_ms INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS p95_duration_ms INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS input_tokens_sum BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS output_tokens_sum BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS total_tokens_sum BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS generation_tps_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS generation_tps_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS effective_output_tps_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS effective_output_tps_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS p50_effective_output_tps DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS p95_effective_output_tps DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS iteration_sum BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS iteration_max INTEGER NOT NULL DEFAULT 0,
    ADD CONSTRAINT llm_request_events_rollup_granularity_check
        CHECK (rollup_granularity IS NULL OR rollup_granularity IN ('hour', 'day'));

CREATE INDEX IF NOT EXISTS idx_llm_request_events_raw_cleanup
    ON llm_request_events (created_at)
    WHERE is_rollup = FALSE;

CREATE UNIQUE INDEX IF NOT EXISTS idx_llm_request_events_rollup_key
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

CREATE TABLE IF NOT EXISTS telemetry_rollups (
    source TEXT NOT NULL,
    kind TEXT NOT NULL,
    granularity TEXT NOT NULL CHECK (granularity IN ('hour', 'day')),
    bucket_start TIMESTAMPTZ NOT NULL,
    bucket_end TIMESTAMPTZ NOT NULL,
    dimensions_hash TEXT NOT NULL,
    dimensions JSONB NOT NULL DEFAULT '{}'::jsonb,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source, kind, granularity, bucket_start, dimensions_hash)
);
