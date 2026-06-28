ALTER TABLE workflow_triggers
    DROP CONSTRAINT IF EXISTS wt_type_check;

ALTER TABLE workflow_triggers
    ADD CONSTRAINT wt_type_check
    CHECK (trigger_type IN ('queue_depth', 'error_rate', 'time_of_day', 'provider_capacity'));
