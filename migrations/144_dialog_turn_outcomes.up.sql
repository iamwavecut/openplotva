-- Per-turn dialog reply outcome ledger: one row per worker tick that handled a
-- dialog job, recording what the user actually got (or didn't). Written by the
-- async recorder in openplotva-app; queried by runtime GraphQL and admin
-- diagnostics. Additive and behavior-neutral.
CREATE TABLE dialog_turn_outcomes (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    job_id BIGINT NOT NULL,
    queue_name TEXT NOT NULL,
    chat_id BIGINT,
    thread_id INT,
    user_id BIGINT,
    trigger_message_id INT,
    attempt INT NOT NULL DEFAULT 1,
    outcome TEXT NOT NULL,
    reason TEXT,
    provider TEXT,
    model TEXT,
    elapsed_ms INT,
    budget_ms INT,
    user_signal TEXT,
    sent_message_parts INT,
    side_effect_ticket_id BIGINT,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX idx_dialog_turn_outcomes_created ON dialog_turn_outcomes (created_at);
CREATE INDEX idx_dialog_turn_outcomes_chat ON dialog_turn_outcomes (chat_id, trigger_message_id, created_at);
CREATE INDEX idx_dialog_turn_outcomes_outcome ON dialog_turn_outcomes (outcome, created_at);
