LOCK TABLE llm_capacity_pools, provider_models, workflow_assignments
    IN SHARE ROW EXCLUSIVE MODE;

DELETE FROM workflow_assignments
WHERE inference_overrides ? 'created_by_gpu0_gemma_routing_v1';

UPDATE workflow_assignments
SET role = inference_overrides->'gpu0_gemma_routing_v1_previous'->>'role',
    weight = (inference_overrides->'gpu0_gemma_routing_v1_previous'->>'weight')::INTEGER,
    fallback_order =
        (inference_overrides->'gpu0_gemma_routing_v1_previous'->>'fallback_order')::INTEGER,
    enabled = (inference_overrides->'gpu0_gemma_routing_v1_previous'->>'enabled')::BOOLEAN,
    inference_overrides = inference_overrides - 'gpu0_gemma_routing_v1_previous'
WHERE inference_overrides ? 'gpu0_gemma_routing_v1_previous';

UPDATE provider_models
SET pool_id = (config->>'gpu0_gemma_routing_v1_previous_pool_id')::BIGINT,
    config = config - 'gpu0_gemma_routing_v1_previous_pool_id'
WHERE config ? 'gpu0_gemma_routing_v1_previous_pool_id';

UPDATE llm_capacity_pools
SET max_concurrency = (config->>'gpu0_gemma_routing_v1_previous_max_concurrency')::INTEGER,
    config = config - 'gpu0_gemma_routing_v1_previous_max_concurrency',
    updated_at = now()
WHERE config ? 'gpu0_gemma_routing_v1_previous_max_concurrency';
