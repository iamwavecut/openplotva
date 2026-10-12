-- History summaries leave GPU0: GPU2 Qwen serves them and they queue for its
-- capacity pool together with memory work. Gemma leaves the chain; GPT Luna stays
-- the last resort. Touched rows record what they were, so the down migration
-- restores the previous chain exactly. A database without these rows (a fresh
-- install seeds routing at boot) is left unchanged.

LOCK TABLE llm_providers, provider_models, workflow_assignments
    IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM workflow_assignments
        WHERE inference_overrides ? 'gpu2_history_summary_v1_previous'
    ) THEN
        RAISE EXCEPTION 'GPU2 history summary routing marker collision';
    END IF;
END $$;

CREATE TEMPORARY TABLE gpu2_history_summary_targets ON COMMIT DROP AS
SELECT DISTINCT ON (assignment.provider_model_id)
    assignment.id,
    targets.role,
    targets.weight,
    targets.fallback_order,
    targets.enabled
FROM (
    VALUES
        ('aifarm-ninfer-gpu2', 'qwen3.8-27b', 'primary', 100, NULL::INTEGER, TRUE),
        ('aifarm-vllm-gpu0', 'Gemma 4 26B Heretic', 'fallback', NULL::INTEGER, 0, FALSE)
) AS targets (provider_name, model_name, role, weight, fallback_order, enabled)
JOIN llm_providers AS provider
  ON provider.name = targets.provider_name
JOIN provider_models AS model
  ON model.provider_id = provider.id
 AND model.model_name = targets.model_name
JOIN workflow_assignments AS assignment
  ON assignment.provider_model_id = model.id
 AND assignment.workflow_key = 'history_summary'
 AND assignment.scope = 'global'
ORDER BY assignment.provider_model_id, assignment.enabled DESC, assignment.id;

-- Gemma is switched off only when the GPU2 primary exists.
UPDATE workflow_assignments AS assignment
SET role = target.role,
    weight = target.weight,
    fallback_order = target.fallback_order,
    enabled = target.enabled,
    inference_overrides = assignment.inference_overrides || jsonb_build_object(
        'gpu2_history_summary_v1_previous', jsonb_build_object(
            'role', assignment.role,
            'weight', assignment.weight,
            'fallback_order', assignment.fallback_order,
            'enabled', assignment.enabled
        )
    )
FROM gpu2_history_summary_targets AS target
WHERE assignment.id = target.id
  AND EXISTS (
      SELECT 1 FROM gpu2_history_summary_targets AS primary_target
      WHERE primary_target.role = 'primary'
  );
