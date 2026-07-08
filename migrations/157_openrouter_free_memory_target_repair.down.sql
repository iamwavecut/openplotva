UPDATE llm_capacity_pools
SET config = jsonb_set(
        config,
        '{target_workflows}',
        CASE
            WHEN config -> 'target_workflows' ? 'memory_consolidation' THEN config -> 'target_workflows'
            ELSE COALESCE(config -> 'target_workflows', '[]'::jsonb) || '["memory_consolidation"]'::jsonb
        END,
        TRUE
    ),
    updated_at = now()
WHERE name = 'openrouter-free'
  AND config ->> 'managed_by' = 'openrouter_free_pool';

-- Managed memory assignments are recreated by the next openrouter-free pool refresh.
