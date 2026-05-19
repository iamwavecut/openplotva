-- Source SHA-256: 9f853adab40a9599b7ca46f0ee4848257a26bbbec93945dc0fb8e5d59d8ac265

DROP TABLE IF EXISTS chat_history_resets;
DROP INDEX IF EXISTS idx_chat_history_entries_chat_message_id;
DROP INDEX IF EXISTS idx_chat_history_entries_chat_thread_occurred_at;
DROP INDEX IF EXISTS idx_chat_history_entries_chat_occurred_at;
DROP TABLE IF EXISTS chat_history_entries;
