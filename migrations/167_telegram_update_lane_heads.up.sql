-- Compact, durable lane heads replace the inbox-wide anti-join used by claims.
-- This migration is additive: older binaries ignore both tables. The new
-- binary checks for them at readiness and populates lane heads through a
-- versioned startup reconciliation before it starts update consumers.
CREATE TABLE telegram_update_lanes (
    bot_id BIGINT NOT NULL,
    ordering_key TEXT NOT NULL,
    head_inbox_id BIGINT REFERENCES telegram_update_inbox(id) ON DELETE SET NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bot_id, ordering_key),
    UNIQUE (head_inbox_id)
);

CREATE INDEX telegram_update_lanes_active_idx
    ON telegram_update_lanes (head_inbox_id)
    WHERE head_inbox_id IS NOT NULL;

-- Versioned startup jobs are committed only after their whole bulk repair.
-- Losing this marker during a later rollback is safe: a future upgrade simply
-- reruns the idempotent inbox reconciliation.
CREATE TABLE telegram_update_startup_jobs (
    job_name TEXT PRIMARY KEY,
    bot_id BIGINT NOT NULL,
    ignored_rows BIGINT NOT NULL DEFAULT 0 CHECK (ignored_rows >= 0),
    repaired_attempt_rows BIGINT NOT NULL DEFAULT 0 CHECK (repaired_attempt_rows >= 0),
    lane_rows BIGINT NOT NULL DEFAULT 0 CHECK (lane_rows >= 0),
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
