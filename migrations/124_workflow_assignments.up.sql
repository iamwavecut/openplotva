-- Binds a provider model to a workflow with a routing policy. One table expresses
-- all requested semantics via `role`:
--   primary  : weighted per-request candidate (`weight`, percentages sum to 100;
--              an unavailable target's share is redistributed proportionally).
--   fallback : ordered tail tried after the whole weighted primary set is exhausted
--              (`fallback_order` ascending = tried first).
--   overflow : engaged only while a workflow_triggers rule is active.
--   shadow   : fire-and-forget duplicate; response discarded (experiments).
--   canary   : `canary_percent` of traffic routed to a new model (experiments).
-- `scope` layers a VIP-tier override over the global default (VIP beats global).
-- Inference overrides and reliability knobs are per-assignment; NULL = inherit.
CREATE TABLE workflow_assignments (
    id                   BIGSERIAL PRIMARY KEY,
    workflow_key         TEXT NOT NULL REFERENCES workflows(key) ON DELETE CASCADE,
    scope                TEXT NOT NULL DEFAULT 'global',
    role                 TEXT NOT NULL DEFAULT 'primary',
    provider_model_id    BIGINT NOT NULL REFERENCES provider_models(id) ON DELETE CASCADE,
    weight               INTEGER,
    fallback_order       INTEGER,
    canary_percent       INTEGER,
    enabled              BOOLEAN NOT NULL DEFAULT TRUE,
    inference_overrides  JSONB NOT NULL DEFAULT '{}'::jsonb,
    cb_failure_threshold INTEGER NOT NULL DEFAULT 5,
    cb_cooldown_ms       INTEGER NOT NULL DEFAULT 30000,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT wa_role_check
        CHECK (role IN ('primary', 'fallback', 'overflow', 'shadow', 'canary')),
    CONSTRAINT wa_scope_check
        CHECK (scope IN ('global', 'vip')),
    CONSTRAINT wa_weight_range
        CHECK (weight IS NULL OR weight BETWEEN 0 AND 100),
    CONSTRAINT wa_canary_range
        CHECK (canary_percent IS NULL OR canary_percent BETWEEN 0 AND 100)
);

CREATE INDEX workflow_assignments_lookup_idx
    ON workflow_assignments (workflow_key, scope, role)
    WHERE enabled;
