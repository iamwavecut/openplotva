-- Source SHA-256: 82d96616be7bde607eac432b196d881a3cdd16702015d2cf2ab87dff84249cd7

ALTER TABLE llm_request_events DROP CONSTRAINT IF EXISTS llm_request_events_chat_id_fkey;
ALTER TABLE llm_request_events DROP CONSTRAINT IF EXISTS llm_request_events_user_id_fkey;
