WITH target_assignment AS (
    SELECT wa.id
    FROM workflow_assignments wa
    JOIN provider_models pm ON pm.id = wa.provider_model_id
    JOIN llm_providers lp ON lp.id = pm.provider_id
    WHERE wa.workflow_key = 'memory_consolidation'
      AND wa.scope = 'global'
      AND wa.role = 'fallback'
      AND wa.enabled = FALSE
      AND lp.name = 'aifarm-llamacpp-gpu2'
      AND pm.model_name = 'vibethinker-3b'
)
UPDATE workflow_assignments wa
SET enabled = TRUE,
    inference_overrides = wa.inference_overrides || jsonb_build_object(
        'memory_vram_fallback_repair_v1', TRUE
    )
FROM target_assignment
WHERE wa.id = target_assignment.id;
