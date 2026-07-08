ALTER TABLE llm_capacity_pools
    ADD COLUMN IF NOT EXISTS config JSONB NOT NULL DEFAULT '{}'::jsonb;

INSERT INTO workflows (key, kind, full_routing)
VALUES ('youtube_summary', 'chat', FALSE)
ON CONFLICT (key) DO NOTHING;

WITH openrouter_provider AS (
    INSERT INTO llm_providers (
        name,
        kind,
        protocol,
        endpoint,
        api_key_ref,
        enabled,
        config
    )
    VALUES (
        'openrouter-free',
        'chat',
        'openai_compat',
        'https://openrouter.ai/api/v1/chat/completions',
        'OPENROUTER_KEY',
        FALSE,
        '{
          "managed_by": "openrouter_free_pool",
          "key_status_url": "https://openrouter.ai/api/v1/key",
          "models_url": "https://openrouter.ai/api/v1/models",
          "site_url": null,
          "app_title": "OpenPlotva",
          "timeout_ms": 300000,
          "connect_timeout_ms": 10000
        }'::jsonb
    )
    ON CONFLICT (name) DO UPDATE SET
        kind = EXCLUDED.kind,
        protocol = EXCLUDED.protocol,
        endpoint = EXCLUDED.endpoint,
        api_key_ref = COALESCE(llm_providers.api_key_ref, EXCLUDED.api_key_ref),
        config = llm_providers.config || EXCLUDED.config,
        updated_at = now()
    RETURNING id
),
openrouter_pool AS (
    INSERT INTO llm_capacity_pools (
        name,
        max_concurrency,
        description,
        config
    )
    VALUES (
        'openrouter-free',
        1,
        'Daily refreshed OpenRouter free-model pool for non-dialog background LLM routines.',
        '{
          "managed_by": "openrouter_free_pool",
          "auto_refresh_enabled": true,
          "source_url": "https://shir-man.com/api/free-llm/top-models",
          "refresh_interval_seconds": 86400,
          "refresh_on_startup": true,
          "max_models": 6,
          "rpm_limit": 16,
          "daily_request_limit": 950,
          "default_pool_cooldown_seconds": 3600,
          "model_cooldown_seconds": 900,
          "fallback_model": "openrouter/free",
          "target_workflows": [
            "history_summary",
            "agentic_search_reasoner",
            "agentic_search_writer",
            "agentic_song",
            "agentic_image",
            "media_prompt_optimizer",
            "youtube_summary"
          ]
        }'::jsonb
    )
    ON CONFLICT (name) DO UPDATE SET
        max_concurrency = COALESCE(llm_capacity_pools.max_concurrency, EXCLUDED.max_concurrency),
        description = COALESCE(llm_capacity_pools.description, EXCLUDED.description),
        config = llm_capacity_pools.config || EXCLUDED.config,
        updated_at = now()
    RETURNING id
),
fallback_model AS (
    INSERT INTO provider_models (
        provider_id,
        model_name,
        display_name,
        base_url,
        capabilities,
        pool_id,
        enabled,
        config
    )
    SELECT
        openrouter_provider.id,
        'openrouter/free',
        'OpenRouter Free Router',
        'https://openrouter.ai/api/v1/chat/completions',
        ARRAY['chat']::TEXT[],
        openrouter_pool.id,
        TRUE,
        '{
          "managed_by": "openrouter_free_pool",
          "source_rank": 999,
          "supports_tools": true,
          "supports_structured_outputs": true,
          "supports_response_format": true,
          "supports_reasoning": true
        }'::jsonb
    FROM openrouter_provider, openrouter_pool
    ON CONFLICT (provider_id, model_name, base_url) DO UPDATE SET
        display_name = EXCLUDED.display_name,
        capabilities = EXCLUDED.capabilities,
        pool_id = EXCLUDED.pool_id,
        enabled = TRUE,
        config = provider_models.config || EXCLUDED.config
    RETURNING id
),
target_workflows AS (
    SELECT key, row_number() OVER (ORDER BY key) AS fallback_order
    FROM workflows
    WHERE key IN (
        'history_summary',
        'agentic_search_reasoner',
        'agentic_search_writer',
        'agentic_song',
        'agentic_image',
        'media_prompt_optimizer',
        'youtube_summary'
    )
)
INSERT INTO workflow_assignments (
    workflow_key,
    scope,
    role,
    provider_model_id,
    weight,
    fallback_order,
    enabled,
    inference_overrides,
    cb_failure_threshold,
    cb_cooldown_ms
)
SELECT
    target_workflows.key,
    'global',
    'fallback',
    fallback_model.id,
    NULL,
    target_workflows.fallback_order,
    TRUE,
    jsonb_build_object(
        'managed_by', 'openrouter_free_pool',
        'fallback_model', true
    ),
    3,
    3600000
FROM target_workflows, fallback_model
WHERE NOT EXISTS (
    SELECT 1
    FROM workflow_assignments existing
    WHERE existing.workflow_key = target_workflows.key
      AND existing.scope = 'global'
      AND existing.role = 'fallback'
      AND existing.provider_model_id = fallback_model.id
      AND existing.inference_overrides ->> 'managed_by' = 'openrouter_free_pool'
);
