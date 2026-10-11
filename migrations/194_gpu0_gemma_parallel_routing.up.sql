-- AI Farm GPU0 serves Gemma on NInfer with eight parallel lanes (Discovery
-- max_concurrent_jobs = 8). Dialog, which also carries the agent turns, and the
-- other GPU0 text flows start there; GPU2 Qwen keeps memory and becomes the first
-- fallback; GPT Luna stays the last resort. Every touched row records what it was,
-- so the down migration restores the previous routing exactly. A database without
-- these identities (a fresh install seeds routing at boot) is left unchanged.

LOCK TABLE llm_capacity_pools, llm_providers, provider_models, workflows,
    workflow_assignments IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM workflow_assignments
        WHERE inference_overrides ?| ARRAY[
            'gpu0_gemma_routing_v1_previous',
            'created_by_gpu0_gemma_routing_v1'
        ]
    ) OR EXISTS (
        SELECT 1 FROM llm_capacity_pools
        WHERE config ? 'gpu0_gemma_routing_v1_previous_max_concurrency'
    ) OR EXISTS (
        SELECT 1 FROM provider_models
        WHERE config ? 'gpu0_gemma_routing_v1_previous_pool_id'
    ) THEN
        RAISE EXCEPTION 'GPU0 Gemma routing marker collision';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM provider_models AS model
        JOIN llm_providers AS provider ON provider.id = model.provider_id
        WHERE (provider.name, model.model_name) IN (
            ('aifarm-vllm-gpu0', 'Gemma 4 26B Heretic'),
            ('aifarm-vision', 'Gemma 4 26B Heretic'),
            ('aifarm-ninfer-gpu2', 'qwen3.8-27b'),
            ('openrouter', '~openai/gpt-luna-latest')
        )
        GROUP BY provider.name, model.model_name
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'GPU0 Gemma routing found ambiguous model identities';
    END IF;
END $$;

UPDATE llm_capacity_pools
SET config = config || jsonb_build_object(
        'gpu0_gemma_routing_v1_previous_max_concurrency', max_concurrency
    ),
    max_concurrency = 8,
    updated_at = now()
WHERE name = 'aifarm-vllm-gpu0';

-- Vision captions hit the same GPU0 server, so they draw from the same lanes.
UPDATE provider_models AS model
SET config = model.config || jsonb_build_object(
        'gpu0_gemma_routing_v1_previous_pool_id', model.pool_id
    ),
    pool_id = capacity_pool.id
FROM llm_providers AS provider,
     llm_capacity_pools AS capacity_pool
WHERE provider.id = model.provider_id
  AND provider.name = 'aifarm-vision'
  AND model.model_name = 'Gemma 4 26B Heretic'
  AND capacity_pool.name = 'aifarm-vllm-gpu0'
  AND model.pool_id IS DISTINCT FROM capacity_pool.id;

CREATE TEMPORARY TABLE gpu0_gemma_routing_targets ON COMMIT DROP AS
SELECT
    targets.workflow_key,
    targets.role,
    targets.weight,
    targets.fallback_order,
    model.id AS provider_model_id
FROM (
    VALUES
        ('dialog', 'primary', 'aifarm-vllm-gpu0', 'Gemma 4 26B Heretic', 100, NULL::INTEGER),
        ('dialog', 'fallback', 'aifarm-ninfer-gpu2', 'qwen3.8-27b', NULL::INTEGER, 0),
        ('dialog', 'fallback', 'openrouter', '~openai/gpt-luna-latest', NULL::INTEGER, 99),
        ('media_prompt_optimizer', 'primary', 'aifarm-vllm-gpu0', 'Gemma 4 26B Heretic', 100, NULL::INTEGER),
        ('media_prompt_optimizer', 'fallback', 'aifarm-ninfer-gpu2', 'qwen3.8-27b', NULL::INTEGER, 0),
        ('media_prompt_optimizer', 'fallback', 'openrouter', '~openai/gpt-luna-latest', NULL::INTEGER, 99),
        ('history_summary', 'primary', 'aifarm-vllm-gpu0', 'Gemma 4 26B Heretic', 100, NULL::INTEGER),
        ('history_summary', 'fallback', 'aifarm-ninfer-gpu2', 'qwen3.8-27b', NULL::INTEGER, 0),
        ('history_summary', 'fallback', 'openrouter', '~openai/gpt-luna-latest', NULL::INTEGER, 99),
        ('youtube_summary', 'primary', 'aifarm-vllm-gpu0', 'Gemma 4 26B Heretic', 100, NULL::INTEGER),
        ('youtube_summary', 'fallback', 'aifarm-ninfer-gpu2', 'qwen3.8-27b', NULL::INTEGER, 0),
        ('youtube_summary', 'fallback', 'openrouter', '~openai/gpt-luna-latest', NULL::INTEGER, 99),
        ('agentic_image', 'primary', 'aifarm-vllm-gpu0', 'Gemma 4 26B Heretic', 100, NULL::INTEGER),
        ('agentic_image', 'fallback', 'aifarm-ninfer-gpu2', 'qwen3.8-27b', NULL::INTEGER, 0),
        ('agentic_image', 'fallback', 'openrouter', '~openai/gpt-luna-latest', NULL::INTEGER, 99),
        ('agentic_song', 'primary', 'aifarm-vllm-gpu0', 'Gemma 4 26B Heretic', 100, NULL::INTEGER),
        ('agentic_song', 'fallback', 'aifarm-ninfer-gpu2', 'qwen3.8-27b', NULL::INTEGER, 0),
        ('agentic_song', 'fallback', 'openrouter', '~openai/gpt-luna-latest', NULL::INTEGER, 99),
        ('agentic_search_writer', 'primary', 'aifarm-vllm-gpu0', 'Gemma 4 26B Heretic', 100, NULL::INTEGER),
        ('agentic_search_writer', 'fallback', 'aifarm-ninfer-gpu2', 'qwen3.8-27b', NULL::INTEGER, 0),
        ('agentic_search_writer', 'fallback', 'openrouter', '~openai/gpt-luna-latest', NULL::INTEGER, 99),
        ('agentic_search_reasoner', 'primary', 'aifarm-vllm-gpu0', 'Gemma 4 26B Heretic', 100, NULL::INTEGER),
        ('agentic_search_reasoner', 'fallback', 'aifarm-ninfer-gpu2', 'qwen3.8-27b', NULL::INTEGER, 0),
        ('agentic_search_reasoner', 'fallback', 'openrouter', '~openai/gpt-luna-latest', NULL::INTEGER, 99)
) AS targets (workflow_key, role, provider_name, model_name, weight, fallback_order)
JOIN workflows AS workflow
  ON workflow.key = targets.workflow_key
JOIN llm_providers AS provider
  ON provider.name = targets.provider_name
JOIN provider_models AS model
  ON model.provider_id = provider.id
 AND model.model_name = targets.model_name;

-- A chain is rewritten only when its GPU0 primary exists.
DELETE FROM gpu0_gemma_routing_targets AS target
WHERE NOT EXISTS (
    SELECT 1 FROM gpu0_gemma_routing_targets AS primary_target
    WHERE primary_target.workflow_key = target.workflow_key
      AND primary_target.role = 'primary'
);

-- One existing global row per target model is reused; enabled rows win.
CREATE TEMPORARY TABLE gpu0_gemma_routing_keepers ON COMMIT DROP AS
SELECT DISTINCT ON (assignment.workflow_key, assignment.provider_model_id)
    assignment.id,
    assignment.workflow_key,
    assignment.provider_model_id,
    target.role,
    target.weight,
    target.fallback_order
FROM workflow_assignments AS assignment
JOIN gpu0_gemma_routing_targets AS target
  ON target.workflow_key = assignment.workflow_key
 AND target.provider_model_id = assignment.provider_model_id
WHERE assignment.scope = 'global'
ORDER BY assignment.workflow_key, assignment.provider_model_id,
    assignment.enabled DESC, assignment.id;

UPDATE workflow_assignments AS assignment
SET enabled = FALSE,
    inference_overrides = assignment.inference_overrides || jsonb_build_object(
        'gpu0_gemma_routing_v1_previous', jsonb_build_object(
            'role', assignment.role,
            'weight', assignment.weight,
            'fallback_order', assignment.fallback_order,
            'enabled', assignment.enabled
        )
    )
WHERE assignment.scope = 'global'
  AND assignment.enabled
  AND assignment.workflow_key IN (SELECT workflow_key FROM gpu0_gemma_routing_targets)
  AND assignment.id NOT IN (SELECT id FROM gpu0_gemma_routing_keepers);

UPDATE workflow_assignments AS assignment
SET role = keeper.role,
    weight = keeper.weight,
    fallback_order = keeper.fallback_order,
    enabled = TRUE,
    inference_overrides = assignment.inference_overrides || jsonb_build_object(
        'gpu0_gemma_routing_v1_previous', jsonb_build_object(
            'role', assignment.role,
            'weight', assignment.weight,
            'fallback_order', assignment.fallback_order,
            'enabled', assignment.enabled
        )
    )
FROM gpu0_gemma_routing_keepers AS keeper
WHERE assignment.id = keeper.id;

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
    target.workflow_key,
    'global',
    target.role,
    target.provider_model_id,
    target.weight,
    target.fallback_order,
    TRUE,
    '{"created_by_gpu0_gemma_routing_v1": true}'::jsonb,
    5,
    30000
FROM gpu0_gemma_routing_targets AS target
WHERE NOT EXISTS (
    SELECT 1 FROM gpu0_gemma_routing_keepers AS keeper
    WHERE keeper.workflow_key = target.workflow_key
      AND keeper.provider_model_id = target.provider_model_id
);
