DROP TABLE IF EXISTS telemetry_rollups;

DROP INDEX IF EXISTS idx_llm_request_events_rollup_key;
DROP INDEX IF EXISTS idx_llm_request_events_raw_cleanup;

ALTER TABLE llm_request_events
    DROP CONSTRAINT IF EXISTS llm_request_events_rollup_granularity_check,
    DROP COLUMN IF EXISTS iteration_max,
    DROP COLUMN IF EXISTS iteration_sum,
    DROP COLUMN IF EXISTS p95_effective_output_tps,
    DROP COLUMN IF EXISTS p50_effective_output_tps,
    DROP COLUMN IF EXISTS effective_output_tps_count,
    DROP COLUMN IF EXISTS effective_output_tps_sum,
    DROP COLUMN IF EXISTS generation_tps_count,
    DROP COLUMN IF EXISTS generation_tps_sum,
    DROP COLUMN IF EXISTS total_tokens_sum,
    DROP COLUMN IF EXISTS output_tokens_sum,
    DROP COLUMN IF EXISTS input_tokens_sum,
    DROP COLUMN IF EXISTS p95_duration_ms,
    DROP COLUMN IF EXISTS p50_duration_ms,
    DROP COLUMN IF EXISTS duration_ms_sum,
    DROP COLUMN IF EXISTS error_count,
    DROP COLUMN IF EXISTS request_count,
    DROP COLUMN IF EXISTS bucket_end,
    DROP COLUMN IF EXISTS bucket_start,
    DROP COLUMN IF EXISTS rollup_granularity,
    DROP COLUMN IF EXISTS is_rollup;

