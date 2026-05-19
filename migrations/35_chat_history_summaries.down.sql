-- Source SHA-256: 11b8fe6b6a570dc0a090fd8be1f5d29befe897601970ed350eacabb5a4e8965b

DROP INDEX IF EXISTS idx_chat_history_events_chat_thread_time;
DROP INDEX IF EXISTS idx_chat_history_events_summary_order;
DROP TABLE IF EXISTS chat_history_events;
DROP INDEX IF EXISTS idx_chat_history_summary_sources_source_summary;
DROP INDEX IF EXISTS idx_chat_history_summary_sources_order;
DROP TABLE IF EXISTS chat_history_summary_sources;
DROP INDEX IF EXISTS idx_chat_history_summaries_recent;
DROP INDEX IF EXISTS idx_chat_history_summaries_range;
DROP TABLE IF EXISTS chat_history_summaries;
