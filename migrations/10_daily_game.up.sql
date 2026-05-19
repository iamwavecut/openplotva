-- Source SHA-256: c7af48f76a0714dfb5e16134831668e418e49c876b8560b9235744ef31b3a5c8

ALTER TABLE chat_settings ADD COLUMN IF NOT EXISTS enable_daily_game BOOLEAN DEFAULT FALSE;
ALTER TABLE chat_settings ADD COLUMN IF NOT EXISTS daily_game_theme TEXT DEFAULT 'king';

CREATE TABLE IF NOT EXISTS chat_game_stats (
    chat_id BIGINT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    wins_count INTEGER NOT NULL DEFAULT 0,
    last_win_at TIMESTAMPTZ,
    PRIMARY KEY (chat_id, user_id)
);

CREATE TABLE IF NOT EXISTS chat_game_results (
    id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    theme TEXT NOT NULL,
    won_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE chat_game_results
    ADD COLUMN IF NOT EXISTS won_on_date DATE;

-- Backfill existing rows if any
UPDATE chat_game_results
SET won_on_date = won_at::date
WHERE won_on_date IS NULL;

-- Ensure not null after backfill
ALTER TABLE chat_game_results
    ALTER COLUMN won_on_date SET NOT NULL;

-- Maintain won_on_date via triggers to avoid immutable expression restrictions
CREATE OR REPLACE FUNCTION set_won_on_date() RETURNS trigger AS $$
BEGIN
    NEW.won_on_date := NEW.won_at::date;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS set_won_on_date_before_insert ON chat_game_results;
CREATE TRIGGER set_won_on_date_before_insert
BEFORE INSERT ON chat_game_results
FOR EACH ROW
EXECUTE FUNCTION set_won_on_date();

DROP TRIGGER IF EXISTS set_won_on_date_before_update ON chat_game_results;
CREATE TRIGGER set_won_on_date_before_update
BEFORE UPDATE OF won_at ON chat_game_results
FOR EACH ROW
EXECUTE FUNCTION set_won_on_date();

CREATE INDEX IF NOT EXISTS idx_chat_game_results_chat_id ON chat_game_results(chat_id);
CREATE INDEX IF NOT EXISTS idx_chat_game_results_user_id ON chat_game_results(user_id);
CREATE UNIQUE INDEX IF NOT EXISTS uidx_chat_game_results_daily ON chat_game_results (chat_id, won_on_date);
