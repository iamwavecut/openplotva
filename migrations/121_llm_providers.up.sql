-- Provider registry layer of the admin-configurable LLM routing system.
-- A provider is a credentialed inference endpoint (one inference engine / API).
-- `name` doubles as the observability provider label, so the analytics label
-- normalization in runtime_llm_analytics.rs keeps matching seeded names
-- (aifarm, genkit, nvidia, vmlx, gemini).
--
-- Key storage is mixed: existing providers reference an env/secret var name via
-- `api_key_ref` (the key never lands in the DB); admin-entered providers store an
-- AES-GCM ciphertext in `api_key_encrypted` under the MASTER_KEY env var. At most
-- one source is set; both NULL is valid for local discovery-only providers.
CREATE TABLE llm_providers (
    id                      BIGSERIAL PRIMARY KEY,
    name                    TEXT NOT NULL UNIQUE,
    kind                    TEXT NOT NULL,
    endpoint                TEXT,
    discovery_service_name  TEXT,
    discovery_endpoint_name TEXT,
    api_key_ref             TEXT,
    api_key_encrypted       BYTEA,
    enabled                 BOOLEAN NOT NULL DEFAULT TRUE,
    config                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT llm_providers_kind_check
        CHECK (kind IN ('chat', 'vision', 'embedding', 'image', 'music', 'privacy_filter')),
    CONSTRAINT llm_providers_single_key_source
        CHECK (NOT (api_key_ref IS NOT NULL AND api_key_encrypted IS NOT NULL))
);
