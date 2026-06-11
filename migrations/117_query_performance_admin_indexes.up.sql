-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS memory_cards_updated_id_desc_idx
    ON memory_cards (updated_at DESC, id DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_chat_members_user_id_chat_id
    ON chat_members (user_id, chat_id);
