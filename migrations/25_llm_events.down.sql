-- Source SHA-256: 7a19d9b1baf8def4eff4d1b063780c1fa13726e26a6652af0fa844095398bfc4

DROP INDEX IF EXISTS idx_llm_request_events_source_created_at;
DROP INDEX IF EXISTS idx_llm_request_events_chat_created_at;
DROP INDEX IF EXISTS idx_llm_request_events_created_at;
DROP TABLE IF EXISTS llm_request_events;
