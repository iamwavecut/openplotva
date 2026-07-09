-- ASR providers use a Discovery endpoint with ASR-specific payloads.
ALTER TABLE llm_providers
  DROP CONSTRAINT IF EXISTS llm_providers_protocol_check;

ALTER TABLE llm_providers
  ADD CONSTRAINT llm_providers_protocol_check
  CHECK (protocol IS NULL OR protocol IN ('openai_compat', 'genkit', 'acestep', 'discovery_jobs', 'discovery_draw', 'privacy_filter', 'discovery_asr')) NOT VALID;

ALTER TABLE llm_providers
  VALIDATE CONSTRAINT llm_providers_protocol_check;

UPDATE llm_providers
SET protocol = 'discovery_asr'
WHERE kind = 'asr'
  AND protocol IS NULL;
