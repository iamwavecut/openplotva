-- Source SHA-256: ddbd904698a5a69d3dc0efd87e957fc11d805c9475f57971242b272dfa651d2f

ALTER TABLE chat_settings ALTER COLUMN daily_game_theme SET DEFAULT 'auto';
UPDATE chat_settings SET daily_game_theme = 'auto' WHERE daily_game_theme IS NULL OR daily_game_theme = '' OR daily_game_theme = 'king';
