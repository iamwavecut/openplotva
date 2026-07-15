-- no-transaction

-- Runtime diagnostics query this terminal subset on every explicit snapshot.
CREATE INDEX CONCURRENTLY IF NOT EXISTS telegram_update_inbox_dead_letter_idx
    ON telegram_update_inbox (id)
    WHERE status = 'dead_letter';
