-- Source SHA-256: 34c4fa39b556b7ca4b523c185fdf0678983a8e38a8a728d39a05168d90df3973

-- Revert daily game to disabled by default
UPDATE chat_settings
SET enable_daily_game = FALSE;

-- Revert default value for new chats
ALTER TABLE chat_settings
ALTER COLUMN enable_daily_game SET DEFAULT FALSE;
