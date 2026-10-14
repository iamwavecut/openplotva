-- The dialog translate_text tool translates with its own routed LLM request
-- instead of DeepL. `translation` is a user-facing tool (lane priority 1) that
-- starts on the GPU0 Gemma lanes, with GPU2 Qwen and GPT Luna as fallbacks. The
-- chain is created only when its primary model exists; otherwise the operator
-- assigns models in the admin routing page.

INSERT INTO workflows (key, kind, full_routing, lane_priority)
VALUES ('translation', 'chat', FALSE, 1)
ON CONFLICT (key) DO NOTHING;

WITH targets (role, provider_name, model_name, weight, fallback_order) AS (
    VALUES
        ('primary', 'aifarm-vllm-gpu0', 'Gemma 4 26B Heretic', 100, NULL::INTEGER),
        ('fallback', 'aifarm-ninfer-gpu2', 'qwen3.8-27b', NULL::INTEGER, 0),
        ('fallback', 'openrouter', '~openai/gpt-luna-latest', NULL::INTEGER, 99)
), resolved AS (
    SELECT DISTINCT ON (targets.provider_name, targets.model_name)
        targets.role,
        targets.weight,
        targets.fallback_order,
        model.id AS provider_model_id
    FROM targets
    JOIN llm_providers AS provider ON provider.name = targets.provider_name
    JOIN provider_models AS model
      ON model.provider_id = provider.id
     AND model.model_name = targets.model_name
    ORDER BY targets.provider_name, targets.model_name, model.enabled DESC, model.id
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
    'translation',
    'global',
    resolved.role,
    resolved.provider_model_id,
    resolved.weight,
    resolved.fallback_order,
    TRUE,
    '{"created_by_translation_workflow_v1": true}'::jsonb,
    5,
    30000
FROM resolved
WHERE EXISTS (SELECT 1 FROM resolved WHERE role = 'primary')
  AND NOT EXISTS (
      SELECT 1 FROM workflow_assignments WHERE workflow_key = 'translation'
  );
