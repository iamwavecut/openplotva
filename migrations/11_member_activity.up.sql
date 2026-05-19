-- Source SHA-256: 24d1b01b90271e27633f5c20036b202d6ca595c2194d9d38efb5bc7b33e4c9e1

ALTER TABLE chat_members ADD COLUMN IF NOT EXISTS last_message_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_chat_members_last_message_at
  ON chat_members (chat_id, last_message_at DESC)
  WHERE status IN ('administrator', 'member', 'creator');
