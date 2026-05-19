-- Source SHA-256: 0ea8015901fb79239bb9eab3935b6f929608b6f546bb2cc8773ab39b33f936e2

ALTER TABLE chat_settings ADD COLUMN IF NOT EXISTS enable_daily_game BOOLEAN DEFAULT TRUE;
ALTER TABLE chat_settings ALTER COLUMN enable_daily_game SET DEFAULT TRUE;

UPDATE chat_settings
SET enable_daily_game = TRUE
WHERE enable_daily_game IS DISTINCT FROM TRUE;
