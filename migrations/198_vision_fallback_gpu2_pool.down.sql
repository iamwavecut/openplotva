LOCK TABLE provider_models IN SHARE ROW EXCLUSIVE MODE;

UPDATE provider_models
SET pool_id = (config->>'vision_gpu2_pool_v1_previous_pool_id')::BIGINT,
    config = config - 'vision_gpu2_pool_v1_previous_pool_id'
WHERE config ? 'vision_gpu2_pool_v1_previous_pool_id';
