LOCK TABLE llm_capacity_pools, provider_models IN SHARE ROW EXCLUSIVE MODE;

UPDATE provider_models
SET pool_id = (config->>'backend_lanes_v1_previous_pool_id')::BIGINT,
    config = config - 'backend_lanes_v1_previous_pool_id'
WHERE config ? 'backend_lanes_v1_previous_pool_id';

DELETE FROM llm_capacity_pools WHERE config ? 'created_by_backend_lanes_v1';

UPDATE llm_capacity_pools
SET name = config->>'backend_lanes_v1_previous_name',
    description = config->>'backend_lanes_v1_previous_description',
    config = config - 'backend_lanes_v1_previous_name' - 'backend_lanes_v1_previous_description',
    updated_at = now()
WHERE config ? 'backend_lanes_v1_previous_name';
