UPDATE provider_models AS model
SET capabilities = array_remove(model.capabilities, 'vision')
FROM llm_providers AS provider
WHERE provider.id = model.provider_id
  AND provider.name = 'aifarm-llamacpp-gpu2'
  AND model.model_name = 'qwen3.6-27b-moq';
