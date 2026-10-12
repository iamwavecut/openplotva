LOCK TABLE llm_capacity_pools IN SHARE ROW EXCLUSIVE MODE;

UPDATE llm_capacity_pools
SET max_concurrency = (config->>'gpu2_eight_lanes_v1_previous_max_concurrency')::INTEGER,
    description = config->>'gpu2_eight_lanes_v1_previous_description',
    config = config
        - 'gpu2_eight_lanes_v1_previous_max_concurrency'
        - 'gpu2_eight_lanes_v1_previous_description',
    updated_at = now()
WHERE config ? 'gpu2_eight_lanes_v1_previous_max_concurrency';
