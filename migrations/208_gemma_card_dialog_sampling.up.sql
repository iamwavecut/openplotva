-- Dialog turns served by the GPU0 Gemma take the sampling its model card recommends:
-- temperature 0.6, top_k 64, top_p 0.9, min_p 0.05, repeat_penalty 1.1. The values sit on the
-- dialog assignment of that model, so the Qwen and GPT fallbacks, the history, optimizer and
-- other Gemma workflows keep their own sampling. NInfer clamps top_k to 20 candidates and the
-- farm proxy drops repeat_penalty; both stay in the config as the card states them.
-- Each replaced key is saved under one marker so the down migration restores it exactly.
UPDATE workflow_assignments AS assignment
SET inference_overrides = assignment.inference_overrides
    || jsonb_build_object(
        'gemma_card_sampling_v1_previous',
        jsonb_build_object(
            'temperature', COALESCE(assignment.inference_overrides->'temperature', 'null'::jsonb),
            'top_p', COALESCE(assignment.inference_overrides->'top_p', 'null'::jsonb),
            'top_k', COALESCE(assignment.inference_overrides->'top_k', 'null'::jsonb),
            'min_p', COALESCE(assignment.inference_overrides->'min_p', 'null'::jsonb),
            'repeat_penalty', COALESCE(assignment.inference_overrides->'repeat_penalty', 'null'::jsonb)
        )
    )
    || '{"temperature": 0.6, "top_p": 0.9, "top_k": 64, "min_p": 0.05, "repeat_penalty": 1.1}'::jsonb
FROM provider_models AS model,
     llm_providers AS provider
WHERE model.id = assignment.provider_model_id
  AND provider.id = model.provider_id
  AND assignment.workflow_key = 'dialog'
  AND provider.name = 'aifarm-vllm-gpu0'
  AND model.model_name = 'Gemma 4 26B Heretic'
  AND NOT assignment.inference_overrides ? 'gemma_card_sampling_v1_previous';
