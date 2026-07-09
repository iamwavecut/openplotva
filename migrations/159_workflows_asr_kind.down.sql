-- Rollback compatibility: the pre-ASR schema cannot represent the ASR kind.
-- Preserve related rows by disabling ASR workflows and recategorizing them as
-- chat before restoring the older kind constraint.
UPDATE workflows
SET kind = 'chat',
    enabled = FALSE
WHERE kind = 'asr';

ALTER TABLE workflows
  DROP CONSTRAINT IF EXISTS workflows_kind_check;

ALTER TABLE workflows
  ADD CONSTRAINT workflows_kind_check
  CHECK (kind IN ('chat', 'vision', 'embedding', 'image', 'music', 'privacy_filter')) NOT VALID;

ALTER TABLE workflows
  VALIDATE CONSTRAINT workflows_kind_check;
