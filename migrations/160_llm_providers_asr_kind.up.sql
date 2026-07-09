-- Forward compatibility: ASR providers use their own routing kind.
-- Expand the provider kind constraint before startup backfill inserts aifarm-asr.
ALTER TABLE llm_providers
  DROP CONSTRAINT IF EXISTS llm_providers_kind_check;

ALTER TABLE llm_providers
  ADD CONSTRAINT llm_providers_kind_check
  CHECK (kind IN ('chat', 'vision', 'embedding', 'image', 'music', 'privacy_filter', 'asr')) NOT VALID;

ALTER TABLE llm_providers
  VALIDATE CONSTRAINT llm_providers_kind_check;

UPDATE llm_providers
SET kind = 'asr'
WHERE protocol = 'discovery_asr'
  AND kind <> 'asr';
