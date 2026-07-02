-- Silent-but-guaranteed side effects: every queued generation job records a
-- delivery obligation at schedule time (ON CONFLICT (ticket_job_id) DO NOTHING,
-- so scheduler retries stay idempotent). A watcher resolves each obligation
-- against the generation ticket and notifies the user on failure, orphaning, or
-- expiry. States: pending, extended_once (deadline pushed once with a single
-- "taking longer" notice), delivered, failed_notified, expired_notified,
-- orphaned_notified. dialog_job_id is 0 at insert time (the scheduler runs
-- inside the dialog turn and does not know its taskman job id); turn
-- finalization annotates it.
CREATE TABLE dialog_delivery_obligations (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    chat_id BIGINT NOT NULL,
    thread_id INT,
    user_id BIGINT NOT NULL,
    trigger_message_id INT NOT NULL,
    dialog_job_id BIGINT NOT NULL,
    kind TEXT NOT NULL,
    ticket_job_id BIGINT NOT NULL,
    deadline_at TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    result_message_id INT,
    resolved_at TIMESTAMPTZ,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX idx_delivery_obligations_ticket ON dialog_delivery_obligations (ticket_job_id);
CREATE INDEX idx_delivery_obligations_pending ON dialog_delivery_obligations (state, deadline_at) WHERE state IN ('pending', 'extended_once');
