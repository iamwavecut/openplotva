-- Gemma on GPU0 writes most tool calls as markup in its reply text instead of
-- native tool_calls. Its parsed calls execute instead of rejecting the sample.
UPDATE provider_models AS model
SET config = model.config
    || jsonb_build_object(
        'textual_tool_calls_v1_previous',
        COALESCE(model.config->'textual_tool_calls', 'null'::jsonb)
    )
    || '{"textual_tool_calls": true}'::jsonb
FROM llm_providers AS provider
WHERE provider.id = model.provider_id
  AND provider.name = 'aifarm-vllm-gpu0'
  AND model.model_name = 'Gemma 4 26B Heretic'
  AND NOT model.config ? 'textual_tool_calls_v1_previous';
