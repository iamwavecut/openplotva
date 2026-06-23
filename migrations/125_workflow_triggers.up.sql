-- Edge-case overflow rules layered on top of a workflow's normal routing. A
-- trigger engages an existing assignment (typically role='overflow') while its
-- condition holds, then disengages. The FK points at the assignment, not a bare
-- model, so the engaged target carries its own overrides and reliability settings.
--
-- queue_depth generalizes the hardcoded DialogAifarmFallbackGate (its 30/20
-- hysteresis): typed `queue_name`/`high_watermark`/`low_watermark` because it is
-- the hot path. error_rate and time_of_day keep their dimension-specific knobs in
-- `params` (error_rate: {threshold, window_s}; time_of_day: {cron, tz}).
CREATE TABLE workflow_triggers (
    id                   BIGSERIAL PRIMARY KEY,
    workflow_key         TEXT NOT NULL REFERENCES workflows(key) ON DELETE CASCADE,
    trigger_type         TEXT NOT NULL,
    engage_assignment_id BIGINT NOT NULL REFERENCES workflow_assignments(id) ON DELETE CASCADE,
    enabled              BOOLEAN NOT NULL DEFAULT TRUE,
    queue_name           TEXT,
    high_watermark       INTEGER,
    low_watermark        INTEGER,
    params               JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT wt_type_check
        CHECK (trigger_type IN ('queue_depth', 'error_rate', 'time_of_day')),
    CONSTRAINT wt_queue_depth_complete
        CHECK (
            trigger_type <> 'queue_depth'
            OR (queue_name IS NOT NULL AND high_watermark IS NOT NULL
                AND low_watermark IS NOT NULL AND high_watermark > low_watermark)
        )
);

CREATE INDEX workflow_triggers_lookup_idx
    ON workflow_triggers (workflow_key)
    WHERE enabled;
