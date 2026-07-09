-- Rollback compatibility: the pre-ASR schema cannot represent provider kind
-- asr. Preserve related provider/model rows, but disable ASR providers before
-- recategorizing them so rollback cannot route ASR traffic through another kind.
LOCK TABLE llm_providers IN SHARE ROW EXCLUSIVE MODE;

UPDATE llm_providers
SET kind = 'image',
    enabled = FALSE
WHERE kind = 'asr';

ALTER TABLE llm_providers
  DROP CONSTRAINT IF EXISTS llm_providers_kind_check;

ALTER TABLE llm_providers
  ADD CONSTRAINT llm_providers_kind_check
  CHECK (kind IN ('chat', 'vision', 'embedding', 'image', 'music', 'privacy_filter')) NOT VALID;

ALTER TABLE llm_providers
  VALIDATE CONSTRAINT llm_providers_kind_check;
