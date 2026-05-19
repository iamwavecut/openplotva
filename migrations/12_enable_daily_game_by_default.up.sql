-- Source SHA-256: 34c4fa39b556b7ca4b523c185fdf0678983a8e38a8a728d39a05168d90df3973

-- Enable daily game for all existing chats by default
UPDATE chat_settings
SET enable_daily_game = TRUE
WHERE enable_daily_game IS NULL OR enable_daily_game = FALSE;

-- Set default daily game theme for chats that don't have one
UPDATE chat_settings
SET daily_game_theme = 'king'
WHERE daily_game_theme IS NULL OR daily_game_theme = '';

-- Update the default value for new chats
ALTER TABLE chat_settings
ALTER COLUMN enable_daily_game SET DEFAULT TRUE;
