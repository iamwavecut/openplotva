UPDATE llm_capacity_pools
SET config = jsonb_set(
        config,
        '{target_workflows}',
        COALESCE(
            (
                SELECT jsonb_agg(workflow)
                FROM jsonb_array_elements_text(config -> 'target_workflows') AS target(workflow)
                WHERE workflow <> 'memory_consolidation'
            ),
            '[]'::jsonb
        ),
        TRUE
    ),
    updated_at = now()
WHERE name = 'openrouter-free'
  AND config ->> 'managed_by' = 'openrouter_free_pool'
  AND config ? 'target_workflows';

DELETE FROM workflow_assignments
WHERE workflow_key = 'memory_consolidation'
  AND inference_overrides ->> 'managed_by' = 'openrouter_free_pool';
