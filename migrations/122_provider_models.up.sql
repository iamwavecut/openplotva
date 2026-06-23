-- Model layer: a wire model offered by a provider. `base_url` lives on the model
-- row (not the provider) so the existing aifarm pool — one logical model served on
-- several physical endpoints for capacity — is expressed as N rows under one
-- provider, each with its own base_url.
--
-- `capabilities` tags what the model can do; the router validates a workflow gets a
-- compatible model. `embedding_dim` is hard-guarded against the schema-bound
-- vector(512) used by memory_cards / shield_documents (migrations 100 / 103): an
-- embedding-capable model with a mismatched dimension would silently corrupt those
-- vector writes, so it is rejected at insert time.
CREATE TABLE provider_models (
    id            BIGSERIAL PRIMARY KEY,
    provider_id   BIGINT NOT NULL REFERENCES llm_providers(id) ON DELETE CASCADE,
    model_name    TEXT NOT NULL,
    display_name  TEXT,
    base_url      TEXT,
    capabilities  TEXT[] NOT NULL DEFAULT '{}',
    embedding_dim INTEGER,
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    config        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (provider_id, model_name, base_url),
    CONSTRAINT provider_models_embedding_dim_guard
        CHECK (NOT ('embedding' = ANY(capabilities)) OR embedding_dim = 512)
);

CREATE INDEX provider_models_provider_idx
    ON provider_models (provider_id)
    WHERE enabled;

CREATE INDEX provider_models_capabilities_idx
    ON provider_models USING GIN (capabilities);
