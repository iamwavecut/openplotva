-- no-transaction
-- Backs the merge-pass candidate scan (SQL_SELECT_SUBJECT_MERGE_CANDIDATE_GROUPS),
-- which groups active cards by full scope + subject every ~45s. A partial index on
-- the group key lets Postgres stream the grouping over active rows instead of a
-- full-table hash aggregate as the table grows. Built CONCURRENTLY so the deploy
-- does not block writes on the hot memory_cards table.
CREATE INDEX CONCURRENTLY IF NOT EXISTS memory_cards_subject_merge_idx
    ON memory_cards (visibility, user_id, chat_id, thread_id, subject)
    WHERE status = 'active';
