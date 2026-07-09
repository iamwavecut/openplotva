-- Forward compatibility: ASR is a new routed workflow kind. Replacing the
-- check constraint first lets startup backfill insert the voice_transcription
-- workflow without weakening existing workflow rows.
ALTER TABLE workflows
  DROP CONSTRAINT IF EXISTS workflows_kind_check;

ALTER TABLE workflows
  ADD CONSTRAINT workflows_kind_check
  CHECK (kind IN ('chat', 'vision', 'embedding', 'image', 'music', 'privacy_filter', 'asr')) NOT VALID;

ALTER TABLE workflows
  VALIDATE CONSTRAINT workflows_kind_check;

UPDATE workflows
SET kind = 'asr'
WHERE key = 'voice_transcription'
  AND kind <> 'asr';
