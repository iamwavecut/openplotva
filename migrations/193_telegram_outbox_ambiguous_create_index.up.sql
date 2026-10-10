-- no-transaction
-- Only the recovery scan changes; ambiguous creates still require manual replay approval.
CREATE INDEX CONCURRENTLY IF NOT EXISTS telegram_outbox_ambiguous_create_idx
    ON telegram_outbox (id)
    WHERE state = 'ambiguous' AND delivery_policy = 'create'
      AND chat_id IS NOT NULL AND trigger_message_id IS NOT NULL;
