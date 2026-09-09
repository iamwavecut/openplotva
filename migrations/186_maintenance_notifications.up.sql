CREATE TABLE maintenance_notifications (
    notification_key TEXT PRIMARY KEY,
    recipient_id BIGINT NOT NULL CHECK (recipient_id > 0),
    body TEXT NOT NULL CHECK (octet_length(body) <= 3900),
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'queued', 'sending', 'sent', 'failed', 'ambiguous')),
    attempts INTEGER NOT NULL DEFAULT 0,
    telegram_message_id BIGINT,
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX maintenance_notifications_pending_idx
    ON maintenance_notifications (available_at)
    WHERE state IN ('pending', 'queued', 'sending');
