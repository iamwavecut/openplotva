-- Source SHA-256: 755ced391e296704648d56ba9de03d535fa41abe003f6569549ce4da0dd120d9

-- Restore original state
ALTER TABLE job_messages
DROP CONSTRAINT IF EXISTS valid_message_type_simple;

ALTER TABLE job_messages
ADD CONSTRAINT valid_message_type CHECK (message_type IN ('progress', 'queue_position', 'result', 'error', 'sticker'));

-- Remove status field and constraint
ALTER TABLE job_messages
DROP CONSTRAINT IF EXISTS valid_message_status,
DROP COLUMN IF EXISTS status;

-- Restore old ephemeral fields
ALTER TABLE job_messages
ADD COLUMN is_ephemeral BOOLEAN DEFAULT FALSE,
ADD COLUMN expires_at TIMESTAMP WITH TIME ZONE;

-- Restore old indexes
CREATE INDEX idx_job_messages_expires_at ON job_messages(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX idx_job_messages_ephemeral ON job_messages(is_ephemeral, expires_at) WHERE is_ephemeral = TRUE;

-- Remove new index
DROP INDEX IF EXISTS idx_job_messages_status_created;
