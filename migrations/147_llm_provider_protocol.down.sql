ALTER TABLE llm_providers
    DROP CONSTRAINT IF EXISTS llm_providers_runtime_hint_check,
    DROP CONSTRAINT IF EXISTS llm_providers_protocol_check;

ALTER TABLE llm_providers
    DROP COLUMN IF EXISTS runtime_hint,
    DROP COLUMN IF EXISTS protocol;
