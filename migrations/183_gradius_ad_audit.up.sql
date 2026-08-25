CREATE TABLE gradius_ad_opportunities (
    id BIGSERIAL PRIMARY KEY,
    opportunity_key TEXT NOT NULL UNIQUE CHECK (btrim(opportunity_key) <> ''),
    source_kind TEXT NOT NULL CHECK (btrim(source_kind) <> ''),
    dialog_job_id BIGINT,
    integration_kind TEXT NOT NULL CHECK (btrim(integration_kind) <> ''),
    user_id BIGINT NOT NULL,
    chat_id BIGINT NOT NULL,
    thread_id INTEGER NOT NULL DEFAULT 0,
    model_version TEXT,
    interaction_started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    completed_answers INTEGER NOT NULL CHECK (completed_answers > 0),
    outcome TEXT NOT NULL CHECK (
        outcome IN (
            'ineligible',
            'reserved',
            'no_ad',
            'provider_error',
            'privacy_error',
            'ad',
            'render_error',
            'unsupported_surface'
        )
    ),
    ineligibility_reason TEXT,
    attempt_reserved_at TIMESTAMPTZ,
    provider_outcome TEXT CHECK (
        provider_outcome IS NULL OR provider_outcome IN ('no_ad', 'error', 'ad')
    ),
    provider_completed_at TIMESTAMPTZ,
    selected_placement JSONB,
    ad_markdown TEXT,
    rendered_html TEXT,
    insert_index INTEGER CHECK (insert_index IS NULL OR insert_index >= 0),
    show_price DOUBLE PRECISION,
    click_price DOUBLE PRECISION,
    delivery_state TEXT CHECK (
        delivery_state IS NULL OR delivery_state IN ('prepared', 'queued', 'delivered', 'failed')
    ),
    outbox_batch_id TEXT,
    prepared_at TIMESTAMPTZ,
    queued_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    delivery_failed_at TIMESTAMPTZ,
    delivery_error TEXT,
    shown_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (integration_kind, dialog_job_id),
    CHECK (shown_at IS NULL OR delivery_state = 'delivered'),
    CHECK (delivery_state <> 'delivered' OR delivered_at IS NOT NULL),
    CHECK (outcome <> 'ad' OR (selected_placement IS NOT NULL AND ad_markdown IS NOT NULL AND rendered_html IS NOT NULL))
);

ALTER TABLE gradius_ad_opportunities
    ALTER COLUMN selected_placement SET COMPRESSION lz4,
    ALTER COLUMN ad_markdown SET COMPRESSION lz4,
    ALTER COLUMN rendered_html SET COMPRESSION lz4;

CREATE INDEX gradius_ad_opportunities_interaction_idx
    ON gradius_ad_opportunities (
        integration_kind,
        user_id,
        chat_id,
        thread_id,
        completed_at DESC,
        id DESC
    );

CREATE INDEX gradius_ad_opportunities_user_shown_idx
    ON gradius_ad_opportunities (integration_kind, user_id, shown_at DESC)
    WHERE shown_at IS NOT NULL;

CREATE INDEX gradius_ad_opportunities_user_attempt_idx
    ON gradius_ad_opportunities (integration_kind, user_id, attempt_reserved_at DESC)
    WHERE attempt_reserved_at IS NOT NULL;

CREATE INDEX gradius_ad_opportunities_created_idx
    ON gradius_ad_opportunities (created_at DESC, id DESC);

CREATE INDEX gradius_ad_opportunities_reporting_idx
    ON gradius_ad_opportunities (integration_kind, completed_at DESC, id DESC)
    INCLUDE (outcome, provider_outcome, delivery_state, show_price);

CREATE TABLE gradius_api_calls (
    id BIGSERIAL PRIMARY KEY,
    opportunity_id BIGINT NOT NULL REFERENCES gradius_ad_opportunities(id) ON DELETE CASCADE,
    sequence SMALLINT NOT NULL CHECK (sequence > 0),
    role TEXT,
    synthetic_chat_id TEXT NOT NULL CHECK (btrim(synthetic_chat_id) <> ''),
    synthetic_user_id TEXT NOT NULL CHECK (btrim(synthetic_user_id) <> ''),
    endpoint TEXT NOT NULL CHECK (btrim(endpoint) <> ''),
    request_body JSONB NOT NULL,
    response_status INTEGER CHECK (response_status IS NULL OR response_status BETWEEN 100 AND 599),
    response_body TEXT,
    response_json JSONB,
    response_truncated BOOLEAN NOT NULL DEFAULT FALSE,
    duration_ms BIGINT NOT NULL CHECK (duration_ms >= 0),
    outcome TEXT NOT NULL CHECK (btrim(outcome) <> ''),
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (opportunity_id, sequence)
);

ALTER TABLE gradius_api_calls
    ALTER COLUMN request_body SET COMPRESSION lz4,
    ALTER COLUMN response_body SET COMPRESSION lz4,
    ALTER COLUMN response_json SET COMPRESSION lz4;

CREATE INDEX gradius_api_calls_opportunity_idx
    ON gradius_api_calls (opportunity_id, sequence);
