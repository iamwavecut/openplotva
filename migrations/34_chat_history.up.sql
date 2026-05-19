-- Source SHA-256: 9f853adab40a9599b7ca46f0ee4848257a26bbbec93945dc0fb8e5d59d8ac265

CREATE TABLE IF NOT EXISTS chat_history_entries (
    bucket_day DATE NOT NULL,
    chat_id BIGINT NOT NULL,
    thread_id INTEGER NOT NULL DEFAULT 0,
    message_id INTEGER NOT NULL,
    entry_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    role TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    sender_id BIGINT NOT NULL DEFAULT 0,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bucket_day, chat_id, entry_id)
) PARTITION BY RANGE (bucket_day);

CREATE INDEX IF NOT EXISTS idx_chat_history_entries_chat_occurred_at
    ON chat_history_entries (chat_id, occurred_at DESC, message_id DESC);

CREATE INDEX IF NOT EXISTS idx_chat_history_entries_chat_thread_occurred_at
    ON chat_history_entries (chat_id, thread_id, occurred_at DESC, message_id DESC)
    WHERE thread_id <> 0;

CREATE INDEX IF NOT EXISTS idx_chat_history_entries_chat_message_id
    ON chat_history_entries (chat_id, message_id);

CREATE TABLE IF NOT EXISTS chat_history_resets (
    chat_id BIGINT NOT NULL,
    thread_id INTEGER NOT NULL DEFAULT 0,
    reset_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (chat_id, thread_id)
);
