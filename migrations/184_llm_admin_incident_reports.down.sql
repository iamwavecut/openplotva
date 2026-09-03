DROP TABLE IF EXISTS llm_admin_report_state;

ALTER TABLE llm_routing_events
    DROP COLUMN IF EXISTS user_id;
