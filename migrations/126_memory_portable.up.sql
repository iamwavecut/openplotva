-- Cross-audience travel control for user-scoped memory. A `public_user` card
-- crosses from its origin group into other group audiences only when it is
-- marked portable (stable identity facts: name, language, long-term role).
-- New non-portable user facts are written as `chat_user` (group-local); this
-- column additionally contains pre-existing `public_user` rows at read time,
-- so legacy facts surface only in the user's own DM and their origin chat.
ALTER TABLE memory_cards
    ADD COLUMN portable BOOLEAN NOT NULL DEFAULT false;
