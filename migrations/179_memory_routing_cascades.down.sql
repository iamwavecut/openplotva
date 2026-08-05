-- Restore the historical workflow before removing its replacements.
INSERT INTO workflows (key, kind, full_routing, retry_max_hops, retry_wall_ms, enabled)
VALUES ('memory_consolidation', 'chat', FALSE, 3, 60000, TRUE)
ON CONFLICT (key) DO NOTHING;

DELETE FROM workflow_assignments
WHERE workflow_key = 'memory_consolidation';

WITH targets (role, provider_name, model_name, weight, fallback_order) AS (
    VALUES
        ('primary', 'vram-cloud', 'vram.cloud/qwen3.6-35b-a3b', 50, NULL::INTEGER),
        ('primary', 'vram-cloud', 'vram.cloud/qwen3.6-27b', 50, NULL::INTEGER),
        ('fallback', 'aifarm-llamacpp-gpu2', 'vibethinker-3b', NULL::INTEGER, 0)
)
INSERT INTO workflow_assignments (
    workflow_key,
    scope,
    role,
    provider_model_id,
    weight,
    fallback_order,
    enabled,
    inference_overrides,
    cb_failure_threshold,
    cb_cooldown_ms
)
SELECT
    'memory_consolidation',
    'global',
    targets.role,
    model.id,
    targets.weight,
    targets.fallback_order,
    TRUE,
    '{}'::jsonb,
    5,
    30000
FROM targets
JOIN llm_providers AS provider
  ON provider.name = targets.provider_name
JOIN provider_models AS model
  ON model.provider_id = provider.id
 AND model.model_name = targets.model_name;

DELETE FROM workflows
WHERE key IN ('memory_extraction', 'memory_subject_merge');

-- Restore only models moved by the up migration. A missing historical pool is
-- treated as unpooled rather than violating the foreign key during rollback.
UPDATE provider_models AS model
SET pool_id = CASE
        WHEN model.config -> 'memory_routing_cascades_v1_previous_pool_id' = 'null'::jsonb
        THEN NULL
        WHEN EXISTS (
            SELECT 1
            FROM llm_capacity_pools AS previous_pool
            WHERE previous_pool.id = (
                model.config ->> 'memory_routing_cascades_v1_previous_pool_id'
            )::BIGINT
        )
        THEN (model.config ->> 'memory_routing_cascades_v1_previous_pool_id')::BIGINT
        ELSE NULL
    END,
    config = model.config - 'memory_routing_cascades_v1_previous_pool_id'
WHERE model.config ? 'memory_routing_cascades_v1_previous_pool_id';

-- A same-named operator pool is not migration-owned: preserve its description
-- and config, and restore the concurrency value captured on first application.
UPDATE llm_capacity_pools
SET max_concurrency = CASE
        WHEN config -> 'memory_routing_cascades_v1_previous_max_concurrency' = 'null'::jsonb
        THEN NULL
        ELSE (config ->> 'memory_routing_cascades_v1_previous_max_concurrency')::INTEGER
    END,
    config = config - 'memory_routing_cascades_v1_previous_max_concurrency',
    updated_at = now()
WHERE name = 'aifarm-gpu2-qwen27b'
  AND config ? 'memory_routing_cascades_v1_previous_max_concurrency';

DELETE FROM llm_capacity_pools
WHERE name = 'aifarm-gpu2-qwen27b'
  AND config ->> 'managed_by' = 'memory_routing_cascades_v1';

UPDATE workflows
SET retry_max_hops = 3,
    retry_wall_ms = 60000
WHERE key = 'dialog'
  AND retry_max_hops = 5
  AND retry_wall_ms = 180000;

UPDATE workflow_assignments AS assignment
SET fallback_order = 1
FROM provider_models AS model,
     llm_providers AS provider
WHERE assignment.provider_model_id = model.id
  AND model.provider_id = provider.id
  AND assignment.workflow_key = 'dialog'
  AND assignment.scope = 'global'
  AND assignment.role = 'fallback'
  AND assignment.fallback_order = 0
  AND provider.name = 'aifarm-llamacpp-gpu2'
  AND model.model_name = 'ternary-bonsai-27b';

UPDATE workflow_assignments AS assignment
SET fallback_order = 0
FROM provider_models AS model,
     llm_providers AS provider
WHERE assignment.provider_model_id = model.id
  AND model.provider_id = provider.id
  AND assignment.workflow_key = 'dialog'
  AND assignment.scope = 'global'
  AND assignment.role = 'fallback'
  AND assignment.fallback_order = 99
  AND provider.name IN ('genkit', 'gemini');

UPDATE workflow_assignments
SET enabled = TRUE,
    inference_overrides = inference_overrides
        - 'disabled_by_memory_routing_cascades_v1'
WHERE inference_overrides ->> 'disabled_by_memory_routing_cascades_v1' = 'true';

UPDATE workflow_triggers
SET enabled = TRUE,
    params = params - 'disabled_by_memory_routing_cascades_v1'
WHERE params ->> 'disabled_by_memory_routing_cascades_v1' = 'true';
