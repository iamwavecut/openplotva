-- The vision fallback reaches the GPU2 Qwen server through its own model row, which
-- had no capacity pool, so its requests took GPU2 lanes the bot's GPU2 pool did not
-- count. The row joins that pool; its previous pool is recorded for the down migration.

LOCK TABLE llm_capacity_pools, llm_providers, provider_models IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM provider_models
        WHERE config ? 'vision_gpu2_pool_v1_previous_pool_id'
    ) THEN
        RAISE EXCEPTION 'vision GPU2 pool marker collision';
    END IF;
END $$;

UPDATE provider_models AS model
SET config = model.config || jsonb_build_object(
        'vision_gpu2_pool_v1_previous_pool_id', model.pool_id
    ),
    pool_id = capacity_pool.id
FROM llm_providers AS provider,
     llm_capacity_pools AS capacity_pool
WHERE provider.id = model.provider_id
  AND provider.name = 'aifarm-vision-ninfer'
  AND model.model_name = 'qwen3.8-27b'
  AND capacity_pool.name = 'aifarm-gpu2-qwen27b'
  AND model.pool_id IS DISTINCT FROM capacity_pool.id;
