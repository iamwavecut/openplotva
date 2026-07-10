-- Durable Telegram ingress, processing, and outbound delivery state.
--
-- Redis Streams remain the webhook-facing fan-in. These tables become the
-- source of truth only after one whole Stream batch has been materialized in a
-- single Postgres transaction.

CREATE TABLE telegram_update_inbox (
    id BIGSERIAL PRIMARY KEY,
    bot_id BIGINT NOT NULL,
    update_id BIGINT NOT NULL,
    schema_version SMALLINT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('webhook', 'long_poll', 'legacy')),
    stream_ms BIGINT NOT NULL CHECK (stream_ms >= 0),
    stream_seq BIGINT NOT NULL CHECK (stream_seq >= 0),
    last_stream_ms BIGINT NOT NULL CHECK (last_stream_ms >= 0),
    last_stream_seq BIGINT NOT NULL CHECK (last_stream_seq >= 0),
    raw_payload BYTEA NOT NULL,
    payload_sha256 BYTEA NOT NULL CHECK (octet_length(payload_sha256) = 32),
    payload_conflict BOOLEAN NOT NULL DEFAULT FALSE,
    update_type TEXT,
    telegram_event_at TIMESTAMPTZ,
    first_received_at TIMESTAMPTZ NOT NULL,
    last_received_at TIMESTAMPTZ NOT NULL,
    materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivery_count BIGINT NOT NULL DEFAULT 1 CHECK (delivery_count > 0),
    ordering_key TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    chat_id BIGINT,
    thread_id INTEGER,
    user_id BIGINT,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'retry_wait', 'completed', 'ignored', 'dead_letter')),
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_owner TEXT,
    lease_token BIGINT NOT NULL DEFAULT 0 CHECK (lease_token >= 0),
    leased_until TIMESTAMPTZ,
    processing_started_at TIMESTAMPTZ,
    state_applied_at TIMESTAMPTZ,
    handler_completed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    outcome TEXT,
    ignored_reason TEXT CHECK (ignored_reason IS NULL OR char_length(ignored_reason) <= 512),
    last_error_class TEXT CHECK (last_error_class IS NULL OR char_length(last_error_class) <= 128),
    last_error TEXT CHECK (last_error IS NULL OR char_length(last_error) <= 2048),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((last_stream_ms, last_stream_seq) >= (stream_ms, stream_seq)),
    UNIQUE (bot_id, update_id)
);

CREATE INDEX telegram_update_inbox_claim_idx
    ON telegram_update_inbox (status, available_at, priority DESC, stream_ms, stream_seq, bot_id)
    WHERE status IN ('pending', 'retry_wait', 'processing');

CREATE INDEX telegram_update_inbox_ordering_idx
    ON telegram_update_inbox (bot_id, ordering_key, stream_ms, stream_seq)
    WHERE status IN ('pending', 'processing', 'retry_wait');

CREATE INDEX telegram_update_inbox_lease_idx
    ON telegram_update_inbox (leased_until)
    WHERE status = 'processing';

CREATE INDEX telegram_update_inbox_terminal_idx
    ON telegram_update_inbox (completed_at, id)
    WHERE status IN ('completed', 'ignored', 'dead_letter');

CREATE TABLE telegram_update_attempts (
    id BIGSERIAL PRIMARY KEY,
    inbox_id BIGINT NOT NULL REFERENCES telegram_update_inbox(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    lease_token BIGINT NOT NULL CHECK (lease_token > 0),
    worker_id TEXT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    state_started_at TIMESTAMPTZ,
    state_completed_at TIMESTAMPTZ,
    handler_started_at TIMESTAMPTZ,
    handler_completed_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    outcome TEXT,
    error_class TEXT CHECK (error_class IS NULL OR char_length(error_class) <= 128),
    error TEXT CHECK (error IS NULL OR char_length(error) <= 2048),
    UNIQUE (inbox_id, attempt)
);

CREATE INDEX telegram_update_attempts_inbox_idx
    ON telegram_update_attempts (inbox_id, attempt DESC);

CREATE TABLE telegram_update_quarantine (
    id BIGSERIAL PRIMARY KEY,
    bot_id BIGINT NOT NULL,
    stream_ms BIGINT NOT NULL CHECK (stream_ms >= 0),
    stream_seq BIGINT NOT NULL CHECK (stream_seq >= 0),
    schema_version SMALLINT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('webhook', 'long_poll', 'legacy')),
    raw_payload BYTEA NOT NULL,
    payload_sha256 BYTEA NOT NULL CHECK (octet_length(payload_sha256) = 32),
    first_received_at TIMESTAMPTZ NOT NULL,
    last_received_at TIMESTAMPTZ NOT NULL,
    delivery_count BIGINT NOT NULL DEFAULT 1 CHECK (delivery_count > 0),
    error_class TEXT NOT NULL CHECK (char_length(error_class) <= 128),
    error TEXT NOT NULL CHECK (char_length(error) <= 2048),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (bot_id, stream_ms, stream_seq)
);

CREATE INDEX telegram_update_quarantine_created_idx
    ON telegram_update_quarantine (created_at, id);

CREATE TABLE telegram_outbox_blobs (
    id BIGSERIAL PRIMARY KEY,
    sha256 BYTEA NOT NULL CHECK (octet_length(sha256) = 32),
    media_type TEXT,
    file_name TEXT,
    bytes BYTEA NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (sha256)
);

CREATE INDEX telegram_outbox_blobs_created_idx
    ON telegram_outbox_blobs (created_at, id);

CREATE TABLE telegram_outbox (
    id BIGSERIAL PRIMARY KEY,
    operation_id TEXT NOT NULL UNIQUE,
    batch_id TEXT NOT NULL,
    part_index INTEGER NOT NULL CHECK (part_index >= 0),
    bot_id BIGINT NOT NULL,
    chat_id BIGINT,
    thread_id INTEGER,
    ordering_key TEXT NOT NULL,
    causation_update_id BIGINT,
    dialog_job_id BIGINT,
    trigger_message_id BIGINT,
    method_kind TEXT NOT NULL,
    payload_version SMALLINT NOT NULL,
    original_payload JSONB NOT NULL,
    payload JSONB NOT NULL,
    blob_id BIGINT REFERENCES telegram_outbox_blobs(id),
    delivery_policy TEXT NOT NULL
        CHECK (delivery_policy IN ('create', 'target_idempotent', 'ephemeral', 'financial')),
    protected BOOLEAN NOT NULL DEFAULT FALSE,
    priority INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'leased', 'retry_wait', 'delivered', 'ambiguous', 'dead_letter', 'expired', 'cancelled')),
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_owner TEXT,
    lease_token BIGINT NOT NULL DEFAULT 0 CHECK (lease_token >= 0),
    leased_until TIMESTAMPTZ,
    last_error_class TEXT CHECK (last_error_class IS NULL OR char_length(last_error_class) <= 128),
    last_error TEXT CHECK (last_error IS NULL OR char_length(last_error) <= 2048),
    response_kind TEXT,
    telegram_message_ids BIGINT[] NOT NULL DEFAULT '{}',
    receipt JSONB,
    confirmed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, part_index)
);

CREATE INDEX telegram_outbox_claim_idx
    ON telegram_outbox (state, available_at, priority DESC, id)
    WHERE state IN ('pending', 'retry_wait', 'leased');

CREATE INDEX telegram_outbox_ordering_idx
    ON telegram_outbox (bot_id, ordering_key, batch_id, part_index)
    WHERE state IN ('pending', 'leased', 'retry_wait');

CREATE INDEX telegram_outbox_lease_idx
    ON telegram_outbox (leased_until)
    WHERE state = 'leased';

CREATE INDEX telegram_outbox_terminal_idx
    ON telegram_outbox (confirmed_at, id)
    WHERE state IN ('delivered', 'dead_letter', 'expired', 'cancelled');

CREATE INDEX telegram_outbox_protected_terminal_idx
    ON telegram_outbox (state, updated_at, id)
    WHERE protected AND state IN ('ambiguous', 'dead_letter');

CREATE TABLE telegram_outbox_attempts (
    id BIGSERIAL PRIMARY KEY,
    outbox_id BIGINT NOT NULL REFERENCES telegram_outbox(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    lease_token BIGINT NOT NULL CHECK (lease_token > 0),
    worker_id TEXT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    request_started_at TIMESTAMPTZ,
    response_received_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    outcome TEXT,
    http_status INTEGER,
    latency_ms BIGINT CHECK (latency_ms IS NULL OR latency_ms >= 0),
    error_class TEXT CHECK (error_class IS NULL OR char_length(error_class) <= 128),
    error TEXT CHECK (error IS NULL OR char_length(error) <= 2048),
    UNIQUE (outbox_id, attempt)
);

CREATE INDEX telegram_outbox_attempts_outbox_idx
    ON telegram_outbox_attempts (outbox_id, attempt DESC);

ALTER TABLE taskman_jobs
    ADD COLUMN available_at TIMESTAMPTZ,
    ADD COLUMN debounce_key TEXT,
    ADD COLUMN lane_key TEXT,
    ADD COLUMN source_update_ids BIGINT[] NOT NULL DEFAULT '{}',
    ADD COLUMN latest_update_id BIGINT,
    ADD COLUMN pending_dialog_inputs JSONB NOT NULL DEFAULT '[]'::jsonb;

UPDATE taskman_jobs SET available_at = created_at WHERE available_at IS NULL;

ALTER TABLE taskman_jobs
    ALTER COLUMN available_at SET DEFAULT now(),
    ALTER COLUMN available_at SET NOT NULL;

ALTER TABLE dialog_turn_outcomes
    ADD COLUMN delivery_state TEXT NOT NULL DEFAULT 'legacy_unverified'
        CHECK (delivery_state IN ('legacy_unverified', 'queued', 'delivered', 'partial', 'ambiguous', 'dead_letter')),
    ADD COLUMN outbox_operation_ids TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN telegram_message_ids BIGINT[] NOT NULL DEFAULT '{}',
    ADD COLUMN delivered_at TIMESTAMPTZ,
    ADD COLUMN delivery_error_class TEXT CHECK (delivery_error_class IS NULL OR char_length(delivery_error_class) <= 128),
    ADD COLUMN delivery_error TEXT CHECK (delivery_error IS NULL OR char_length(delivery_error) <= 2048);

CREATE INDEX dialog_turn_outcomes_delivery_idx
    ON dialog_turn_outcomes (delivery_state, created_at, id);
