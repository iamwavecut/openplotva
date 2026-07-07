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

DELETE FROM workflows
WHERE key = 'youtube_summary'
  AND NOT EXISTS (
      SELECT 1
      FROM workflow_assignments
      WHERE workflow_assignments.workflow_key = workflows.key
  );

ALTER TABLE llm_capacity_pools
    DROP COLUMN IF EXISTS config;
