ALTER TABLE llm_routing_events
    ADD COLUMN user_id BIGINT;

CREATE TABLE llm_admin_report_state (
    admin_id BIGINT PRIMARY KEY,
    telegram_message_id BIGINT,
    last_new_message_at TIMESTAMPTZ,
    last_new_message_attempt_at TIMESTAMPTZ,
    last_rendered_fingerprint TEXT,
    pending_virtual_id TEXT UNIQUE,
    pending_kind TEXT,
    pending_fingerprint TEXT,
    pending_started_at TIMESTAMPTZ,
    last_delivery_attempt_at TIMESTAMPTZ,
    last_delivery_error_class TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT llm_admin_report_pending_kind_check
        CHECK (pending_kind IS NULL OR pending_kind IN ('send', 'edit')),
    CONSTRAINT llm_admin_report_pending_fields_check CHECK (
        (
            pending_virtual_id IS NULL
            AND pending_kind IS NULL
            AND pending_fingerprint IS NULL
            AND pending_started_at IS NULL
        ) OR (
            pending_virtual_id IS NOT NULL
            AND pending_kind IS NOT NULL
            AND pending_fingerprint IS NOT NULL
            AND pending_started_at IS NOT NULL
        )
    )
);
