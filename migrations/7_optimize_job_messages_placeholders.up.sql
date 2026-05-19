-- Source SHA-256: 755ced391e296704648d56ba9de03d535fa41abe003f6569549ce4da0dd120d9

-- Optimize job_messages table for placeholder management
-- Removes old ephemeral fields and adds status field for tracking message states

-- Add status field for placeholder management
ALTER TABLE job_messages
ADD COLUMN status VARCHAR(20) DEFAULT 'completed';

-- Update existing records: all current records are considered completed results
UPDATE job_messages
SET status = 'completed'
WHERE status IS NULL;

-- Create CHECK constraint for status values
ALTER TABLE job_messages
ADD CONSTRAINT valid_message_status CHECK (status IN ('placeholder', 'completed', 'failed'));

-- Remove old ephemeral fields that are no longer needed
ALTER TABLE job_messages
DROP COLUMN IF EXISTS is_ephemeral,
DROP COLUMN IF EXISTS expires_at;

-- Simplify message_type constraint to only allow 'result'
ALTER TABLE job_messages
DROP CONSTRAINT IF EXISTS valid_message_type;

ALTER TABLE job_messages
ADD CONSTRAINT valid_message_type_simple CHECK (message_type IN ('result'));

-- Remove old ephemeral-related indexes
DROP INDEX IF EXISTS idx_job_messages_expires_at;
DROP INDEX IF EXISTS idx_job_messages_ephemeral;

-- Create optimized index for placeholder cleanup queries
CREATE INDEX idx_job_messages_status_created
ON job_messages(status, created_at)
WHERE status = 'placeholder';
