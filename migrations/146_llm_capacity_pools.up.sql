-- Capacity pools: a shared concurrency budget over one physical resource
-- (a GPU, a gateway slot allocation). Models attached to the same pool contend
-- for its slots; a model is selectable while its pool has a free slot.
-- `max_concurrency` NULL means unlimited (gauge-only, never blocks).
-- `provider_models.pool_id` NULL means unpooled/unlimited; deleting a pool
-- degrades its models to unpooled instead of breaking routing.
CREATE TABLE llm_capacity_pools (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    max_concurrency INTEGER,
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT llm_capacity_pools_max_positive
        CHECK (max_concurrency IS NULL OR max_concurrency >= 1)
);

ALTER TABLE provider_models
    ADD COLUMN pool_id BIGINT REFERENCES llm_capacity_pools(id) ON DELETE SET NULL;

CREATE INDEX provider_models_pool_idx ON provider_models (pool_id)
    WHERE pool_id IS NOT NULL;
