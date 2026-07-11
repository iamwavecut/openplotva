-- Compact, durable lane heads replace the inbox-wide anti-join used by claims.
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
CREATE TABLE telegram_update_startup_jobs (
    job_name TEXT PRIMARY KEY,
    bot_id BIGINT NOT NULL,
    ignored_rows BIGINT NOT NULL DEFAULT 0 CHECK (ignored_rows >= 0),
    repaired_attempt_rows BIGINT NOT NULL DEFAULT 0 CHECK (repaired_attempt_rows >= 0),
    lane_rows BIGINT NOT NULL DEFAULT 0 CHECK (lane_rows >= 0),
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
