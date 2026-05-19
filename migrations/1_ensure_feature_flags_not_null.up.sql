-- Source SHA-256: 7ec73ef3d54ca70465a8dce9c327a1d3d8ced261a0b2358a1829c4602e2b0cc7

-- Ensure all feature flags in chat_settings are not null and have TRUE as default
ALTER TABLE chat_settings
    ALTER COLUMN enable_global_text_reply SET DEFAULT TRUE,
    ALTER COLUMN enable_global_text_reply SET NOT NULL,
    ALTER COLUMN enable_global_draw_reply SET DEFAULT TRUE,
    ALTER COLUMN enable_global_draw_reply SET NOT NULL,
    ALTER COLUMN enable_obscenifier SET DEFAULT TRUE,
    ALTER COLUMN enable_obscenifier SET NOT NULL,
    ALTER COLUMN enable_profanity SET DEFAULT TRUE,
    ALTER COLUMN enable_profanity SET NOT NULL,
    ALTER COLUMN enable_greet_joiners SET DEFAULT FALSE,
    ALTER COLUMN enable_greet_joiners SET NOT NULL;

-- Update any existing NULL values to their default values
UPDATE chat_settings SET
    enable_global_text_reply = TRUE WHERE enable_global_text_reply IS NULL;
UPDATE chat_settings SET
    enable_global_draw_reply = TRUE WHERE enable_global_draw_reply IS NULL;
UPDATE chat_settings SET
    enable_obscenifier = TRUE WHERE enable_obscenifier IS NULL;
UPDATE chat_settings SET
    enable_profanity = TRUE WHERE enable_profanity IS NULL;
UPDATE chat_settings SET
    enable_greet_joiners = FALSE WHERE enable_greet_joiners IS NULL;
