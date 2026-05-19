-- Source SHA-256: 82d96616be7bde607eac432b196d881a3cdd16702015d2cf2ab87dff84249cd7

ALTER TABLE llm_request_events
	ADD CONSTRAINT llm_request_events_chat_id_fkey FOREIGN KEY (chat_id) REFERENCES chats(id) ON DELETE SET NULL,
	ADD CONSTRAINT llm_request_events_user_id_fkey FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL;
