-- Source SHA-256: c7af48f76a0714dfb5e16134831668e418e49c876b8560b9235744ef31b3a5c8

DROP TRIGGER IF EXISTS set_won_on_date_before_update ON chat_game_results;
DROP TRIGGER IF EXISTS set_won_on_date_before_insert ON chat_game_results;
DROP FUNCTION IF EXISTS set_won_on_date();
DROP INDEX IF EXISTS uidx_chat_game_results_daily;
DROP INDEX IF EXISTS idx_chat_game_results_user_id;
DROP INDEX IF EXISTS idx_chat_game_results_chat_id;
DROP TABLE IF EXISTS chat_game_results;
DROP TABLE IF EXISTS chat_game_stats;
ALTER TABLE chat_settings DROP COLUMN IF EXISTS daily_game_theme;
ALTER TABLE chat_settings DROP COLUMN IF EXISTS enable_daily_game;
