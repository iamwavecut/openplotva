-- no-transaction
-- Existing receipt states and repair order are unchanged.
CREATE INDEX CONCURRENTLY IF NOT EXISTS telegram_outbox_pending_history_idx
    ON telegram_outbox (confirmed_at, id)
    WHERE state = 'delivered' AND last_error_class = 'history_pending';
