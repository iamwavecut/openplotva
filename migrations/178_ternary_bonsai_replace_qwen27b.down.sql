-- Restore the legacy wire model id without replacing the provider_models row.
UPDATE provider_models AS model
SET model_name = 'qwen3.6-27b-moq'
FROM llm_providers AS provider
WHERE provider.id = model.provider_id
  AND provider.name = 'aifarm-llamacpp-gpu2'
  AND provider.discovery_service_name = 'llm-openai-qwen27b-gguf'
  AND model.model_name = 'ternary-bonsai-27b';
