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
-- migration uses IF NOT EXISTS for the config column, ON CONFLICT DO NOTHING for
-- youtube_summary, and ON CONFLICT DO UPDATE for OpenRouter-managed seed rows.
-- Rollback only removes rows explicitly marked as OpenRouter Free managed data.
