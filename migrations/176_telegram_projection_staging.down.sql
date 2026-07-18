DROP VIEW IF EXISTS telegram_files_effective;
DROP VIEW IF EXISTS telegram_chat_active_users_effective;
DROP VIEW IF EXISTS telegram_chat_members_effective;
DROP VIEW IF EXISTS telegram_chats_effective;
DROP VIEW IF EXISTS telegram_users_effective;
DROP VIEW IF EXISTS telegram_files_stage_latest;
DROP VIEW IF EXISTS telegram_activity_stage_latest;
DROP VIEW IF EXISTS telegram_chat_members_stage_latest;
DROP VIEW IF EXISTS telegram_chats_stage_latest;
DROP VIEW IF EXISTS telegram_users_stage_latest;

DROP TABLE IF EXISTS telegram_files_stage;
DROP TABLE IF EXISTS telegram_activity_stage;
DROP TABLE IF EXISTS telegram_chat_members_stage;
DROP TABLE IF EXISTS telegram_chats_stage;
DROP TABLE IF EXISTS telegram_users_stage;

ALTER TABLE telegram_files
    DROP COLUMN IF EXISTS telegram_observed_at;

ALTER TABLE chat_members
    DROP COLUMN IF EXISTS telegram_observed_at;

ALTER TABLE chats
    DROP COLUMN IF EXISTS telegram_observed_at;

ALTER TABLE users
    DROP COLUMN IF EXISTS telegram_observed_at;
