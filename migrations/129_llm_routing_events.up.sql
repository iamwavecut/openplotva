CREATE TABLE llm_routing_events (
    id           BIGSERIAL PRIMARY KEY,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    severity     TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    workflow_key TEXT NOT NULL,
    provider_id  BIGINT REFERENCES llm_providers(id) ON DELETE SET NULL,
    model_id     BIGINT REFERENCES provider_models(id) ON DELETE SET NULL,
    queue_name   TEXT,
    job_id       BIGINT,
    chat_id      BIGINT,
    thread_id    INTEGER,
    message_id   INTEGER,
    dedupe_key   TEXT NOT NULL,
    summary      TEXT NOT NULL,
    detail       JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT llm_routing_events_severity_check
        CHECK (severity IN ('debug', 'info', 'warn', 'error', 'critical'))
);

CREATE INDEX llm_routing_events_created_at_idx
    ON llm_routing_events (created_at DESC, id DESC);

CREATE INDEX llm_routing_events_workflow_created_at_idx
    ON llm_routing_events (workflow_key, created_at DESC);

CREATE INDEX llm_routing_events_dedupe_created_at_idx
    ON llm_routing_events (dedupe_key, created_at DESC);

CREATE INDEX llm_routing_events_provider_model_created_at_idx
    ON llm_routing_events (provider_id, model_id, created_at DESC)
    WHERE provider_id IS NOT NULL OR model_id IS NOT NULL;
