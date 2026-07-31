-- Rename the GPU2 reasoner model in place so workflow assignments, circuit
-- state, weights, fallback order, and model identity remain attached to the
-- existing provider_models row.
UPDATE provider_models AS model
SET model_name = 'ternary-bonsai-27b'
FROM llm_providers AS provider
WHERE provider.id = model.provider_id
  AND provider.name = 'aifarm-llamacpp-gpu2'
  AND provider.discovery_service_name = 'llm-openai-qwen27b-gguf'
  AND model.model_name = 'qwen3.6-27b-moq';
