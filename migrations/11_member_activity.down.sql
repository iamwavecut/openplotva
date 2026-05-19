-- Source SHA-256: 24d1b01b90271e27633f5c20036b202d6ca595c2194d9d38efb5bc7b33e4c9e1

DROP INDEX IF EXISTS idx_chat_members_last_message_at;
ALTER TABLE chat_members DROP COLUMN IF EXISTS last_message_at;
