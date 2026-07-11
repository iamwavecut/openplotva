-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS telegram_outbox_dialog_job_idx
    ON telegram_outbox (dialog_job_id, state, id)
    WHERE dialog_job_id IS NOT NULL;
