DELETE FROM workflow_assignments
WHERE inference_overrides ->> 'managed_by' = 'openrouter_free_pool';

DELETE FROM provider_models
WHERE config ->> 'managed_by' = 'openrouter_free_pool';

DELETE FROM llm_providers
WHERE name = 'openrouter-free'
  AND config ->> 'managed_by' = 'openrouter_free_pool';

DELETE FROM llm_capacity_pools
WHERE name = 'openrouter-free'
  AND config ->> 'managed_by' = 'openrouter_free_pool';

-- Preserve shared schema/data that may have pre-existed this migration. The up
-- migration uses IF NOT EXISTS / ON CONFLICT DO NOTHING for these objects, so
-- rollback only removes rows marked as OpenRouter Free managed data.
