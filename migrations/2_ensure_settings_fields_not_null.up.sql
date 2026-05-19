-- Source SHA-256: 191140a1070974f5fab3cd850c825c060c292615614debc724e0fc906a56a8fe

UPDATE chat_settings SET
    reactivity_percentage = 3 WHERE reactivity_percentage IS NULL;
UPDATE chat_settings SET
    proactivity_percentage = 0 WHERE proactivity_percentage IS NULL;

ALTER TABLE chat_settings
    ALTER COLUMN reactivity_percentage SET DEFAULT 3,
    ALTER COLUMN reactivity_percentage SET NOT NULL,
    ALTER COLUMN proactivity_percentage SET DEFAULT 0,
    ALTER COLUMN proactivity_percentage SET NOT NULL;
