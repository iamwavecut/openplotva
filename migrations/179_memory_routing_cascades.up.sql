-- Give the two memory LLM stages independent, operator-editable retry budgets.
INSERT INTO workflows (key, kind, full_routing, retry_max_hops, retry_wall_ms, enabled)
VALUES
    ('memory_extraction', 'chat', FALSE, 4, 900000, TRUE),
    ('memory_subject_merge', 'chat', FALSE, 2, 900000, TRUE)
ON CONFLICT (key) DO UPDATE SET
    kind = EXCLUDED.kind,
    full_routing = EXCLUDED.full_routing,
    retry_max_hops = EXCLUDED.retry_max_hops,
    retry_wall_ms = EXCLUDED.retry_wall_ms,
    enabled = EXCLUDED.enabled;

-- Three weighted primaries plus Bonsai and Gemini require five started attempts
-- to make the complete dialog fallback chain reachable.
UPDATE workflows
SET retry_max_hops = 5,
    retry_wall_ms = 180000
WHERE key = 'dialog';

UPDATE workflow_assignments AS assignment
SET fallback_order = 0
FROM provider_models AS model,
     llm_providers AS provider
WHERE assignment.provider_model_id = model.id
  AND model.provider_id = provider.id
  AND assignment.workflow_key = 'dialog'
  AND assignment.scope = 'global'
  AND assignment.role = 'fallback'
  AND provider.name = 'aifarm-llamacpp-gpu2'
  AND model.model_name = 'ternary-bonsai-27b';

UPDATE workflow_assignments AS assignment
SET fallback_order = 99
FROM provider_models AS model,
     llm_providers AS provider
WHERE assignment.provider_model_id = model.id
  AND model.provider_id = provider.id
  AND assignment.workflow_key = 'dialog'
  AND assignment.scope = 'global'
  AND assignment.role = 'fallback'
  AND provider.name IN ('genkit', 'gemini');

-- Gemini remains the terminal fallback. Disable the historical overflow edge
-- and its triggers so an engaged queue/capacity trigger cannot promote Gemini
-- ahead of Bonsai. Mark only rows that were enabled, making rollback scoped.
UPDATE workflow_triggers AS trigger
SET enabled = FALSE,
    params = trigger.params
        || '{"disabled_by_memory_routing_cascades_v1":true}'::jsonb
FROM workflow_assignments AS assignment,
     provider_models AS model,
     llm_providers AS provider
WHERE trigger.engage_assignment_id = assignment.id
  AND assignment.provider_model_id = model.id
  AND model.provider_id = provider.id
  AND trigger.workflow_key = 'dialog'
  AND trigger.enabled
  AND assignment.workflow_key = 'dialog'
  AND assignment.scope = 'global'
  AND assignment.role = 'overflow'
  AND provider.name IN ('genkit', 'gemini');

UPDATE workflow_assignments AS assignment
SET enabled = FALSE,
    inference_overrides = assignment.inference_overrides
        || '{"disabled_by_memory_routing_cascades_v1":true}'::jsonb
FROM provider_models AS model,
     llm_providers AS provider
WHERE assignment.provider_model_id = model.id
  AND model.provider_id = provider.id
  AND assignment.workflow_key = 'dialog'
  AND assignment.scope = 'global'
  AND assignment.role = 'overflow'
  AND assignment.enabled
  AND provider.name IN ('genkit', 'gemini');

-- VibeThinker and Bonsai are two model aliases served by one physical GPU2
-- llama.cpp process, so they must draw from the same single-slot pool.
INSERT INTO llm_capacity_pools (name, max_concurrency, description, config)
VALUES (
    'aifarm-gpu2-qwen27b',
    1,
    'Shared single-slot GPU2 budget for VibeThinker and Ternary Bonsai.',
    '{"managed_by":"memory_routing_cascades_v1"}'::jsonb
)
ON CONFLICT (name) DO UPDATE SET
    max_concurrency = 1,
    config = CASE
        WHEN llm_capacity_pools.config
            ? 'memory_routing_cascades_v1_previous_max_concurrency'
        THEN llm_capacity_pools.config
        ELSE jsonb_set(
            llm_capacity_pools.config,
            '{memory_routing_cascades_v1_previous_max_concurrency}',
            COALESCE(to_jsonb(llm_capacity_pools.max_concurrency), 'null'::jsonb),
            TRUE
        )
    END,
    updated_at = now();

UPDATE provider_models AS model
SET config = CASE
        WHEN model.config ? 'memory_routing_cascades_v1_previous_pool_id'
        THEN model.config
        ELSE jsonb_set(
            model.config,
            '{memory_routing_cascades_v1_previous_pool_id}',
            COALESCE(to_jsonb(model.pool_id), 'null'::jsonb),
            TRUE
        )
    END,
    pool_id = capacity_pool.id
FROM llm_providers AS provider,
     llm_capacity_pools AS capacity_pool
WHERE provider.id = model.provider_id
  AND provider.name = 'aifarm-llamacpp-gpu2'
  AND model.model_name IN ('vibethinker-3b', 'ternary-bonsai-27b')
  AND capacity_pool.name = 'aifarm-gpu2-qwen27b';

-- Normalize the replacement workflows instead of inheriting historical or
-- managed fallbacks from memory_consolidation.
DELETE FROM workflow_assignments
WHERE workflow_key IN ('memory_extraction', 'memory_subject_merge');

WITH targets (
    workflow_key,
    role,
    provider_name,
    model_name,
    weight,
    fallback_order
) AS (
    VALUES
        ('memory_extraction', 'primary', 'vram-cloud', 'vram.cloud/qwen3.6-35b-a3b', 100, NULL::INTEGER),
        ('memory_extraction', 'fallback', 'vram-cloud', 'vram.cloud/qwen3.6-27b', NULL::INTEGER, 0),
        ('memory_extraction', 'fallback', 'aifarm-llamacpp-gpu2', 'vibethinker-3b', NULL::INTEGER, 1),
        ('memory_extraction', 'fallback', 'aifarm-llamacpp-gpu2', 'ternary-bonsai-27b', NULL::INTEGER, 2),
        ('memory_subject_merge', 'primary', 'aifarm-llamacpp-gpu2', 'vibethinker-3b', 100, NULL::INTEGER),
        ('memory_subject_merge', 'fallback', 'aifarm-llamacpp-gpu2', 'ternary-bonsai-27b', NULL::INTEGER, 0)
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
    targets.workflow_key,
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

-- The durable task/queue keeps its historical name; only the LLM workflow key
-- is replaced. Deleting the old row also removes its now-unused assignments.
DELETE FROM workflows
WHERE key = 'memory_consolidation';
