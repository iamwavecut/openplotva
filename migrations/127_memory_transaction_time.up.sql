-- Transaction-time axis for memory cards. valid_from/valid_until track when a
-- fact was true in the world; recorded_at/retracted_at track when the bot learned
-- and unlearned it. Together they make supersession non-destructive and allow
-- point-in-time replay ("what did we know on date X") without rewriting history.
ALTER TABLE memory_cards
    ADD COLUMN recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN retracted_at TIMESTAMPTZ;

UPDATE memory_cards SET recorded_at = created_at;

UPDATE memory_cards
    SET retracted_at = COALESCE(valid_until, updated_at)
    WHERE status IN ('superseded', 'deleted');

CREATE INDEX memory_cards_asof_idx
    ON memory_cards (chat_id, thread_id, user_id, recorded_at)
    WHERE retracted_at IS NULL;
