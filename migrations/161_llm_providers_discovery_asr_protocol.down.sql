-- Rollback compatibility: the pre-ASR protocol schema cannot represent
-- discovery_asr, so keep provider rows but clear that protocol before
-- restoring the old constraint.
LOCK TABLE llm_providers IN SHARE ROW EXCLUSIVE MODE;

UPDATE llm_providers
SET protocol = NULL
WHERE protocol = 'discovery_asr';

ALTER TABLE llm_providers
  DROP CONSTRAINT IF EXISTS llm_providers_protocol_check;

ALTER TABLE llm_providers
  ADD CONSTRAINT llm_providers_protocol_check
  CHECK (protocol IS NULL OR protocol IN ('openai_compat', 'genkit', 'acestep', 'discovery_jobs', 'discovery_draw', 'privacy_filter')) NOT VALID;

ALTER TABLE llm_providers
  VALIDATE CONSTRAINT llm_providers_protocol_check;
