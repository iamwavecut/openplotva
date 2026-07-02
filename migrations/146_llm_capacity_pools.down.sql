DROP INDEX IF EXISTS provider_models_pool_idx;
ALTER TABLE provider_models DROP COLUMN IF EXISTS pool_id;
DROP TABLE IF EXISTS llm_capacity_pools;
