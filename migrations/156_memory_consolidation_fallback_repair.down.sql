UPDATE workflow_assignments
SET enabled = FALSE,
    inference_overrides = inference_overrides - 'memory_vram_fallback_repair_v1'
WHERE workflow_key = 'memory_consolidation'
  AND scope = 'global'
  AND role = 'fallback'
  AND inference_overrides ->> 'memory_vram_fallback_repair_v1' = 'true';
