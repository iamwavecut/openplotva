-- Source SHA-256: eed9f9168f35aef1344e09bf0697bf3a48956a0e7ca14068f7806047b8c50888

CREATE TABLE IF NOT EXISTS chat_deputies (
    chat_id BIGINT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chat_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_chat_deputies_user_id ON chat_deputies (user_id);
