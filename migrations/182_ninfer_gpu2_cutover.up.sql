-- Replace the active GPU2 Maple routes with a distinct Qwen3.8 NInfer
-- identity. Historical provider/model rows and telemetry remain untouched.

LOCK TABLE app_settings, llm_capacity_pools, llm_providers, provider_models,
    workflow_assignments IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE llm_providers
    DROP CONSTRAINT llm_providers_runtime_hint_check;
ALTER TABLE llm_providers
    ADD CONSTRAINT llm_providers_runtime_hint_check CHECK (runtime_hint IS NULL OR runtime_hint IN
        ('llama_cpp', 'mlx', 'ninfer', 'vllm', 'sglang', 'ollama', 'tgi'));

DO $$
DECLARE
    shared_pool_id BIGINT;
    maple_provider_id BIGINT;
    maple_model_id BIGINT;
    vibethinker_provider_id BIGINT;
    vibethinker_model_id BIGINT;
    ninfer_provider_id BIGINT;
    ninfer_model_id BIGINT;
    identity_count BIGINT;
    expected_assignment_count BIGINT;
    moved_assignment_count BIGINT;
    expected_assignment_ids JSONB;
BEGIN
    IF EXISTS (
        SELECT 1 FROM app_settings
        WHERE key = 'llm.routing.ninfer_gpu2_cutover_v1_assignment_ids'
    ) THEN
        RAISE EXCEPTION 'NInfer cutover assignment manifest already exists';
    END IF;
    IF EXISTS (
        SELECT 1 FROM workflow_assignments
        WHERE inference_overrides ?| ARRAY[
            'ninfer_gpu2_cutover_v1_previous_model_id',
            'ninfer_gpu2_cutover_v1_previous_role',
            'ninfer_gpu2_cutover_v1_previous_weight',
            'ninfer_gpu2_cutover_v1_previous_fallback_order',
            'disabled_by_ninfer_gpu2_cutover_v1'
        ]
    ) THEN
        RAISE EXCEPTION 'NInfer cutover marker collision';
    END IF;

    SELECT count(*), min(id)
    INTO identity_count, shared_pool_id
    FROM llm_capacity_pools
    WHERE name = 'aifarm-gpu2-qwen27b';
    IF identity_count <> 1 THEN
        RAISE EXCEPTION
            'NInfer cutover requires exactly one shared GPU2 pool; found %',
            identity_count;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM llm_capacity_pools
        WHERE id = shared_pool_id
          AND max_concurrency IN (1, 2)
          AND (
              max_concurrency = 1
              OR config ? 'ninfer_gpu2_cutover_v1_previous_max_concurrency'
          )
    ) THEN
        RAISE EXCEPTION 'shared GPU2 pool concurrency/config drifted';
    END IF;

    SELECT count(*), min(provider.id), min(model.id)
    INTO identity_count, maple_provider_id, maple_model_id
    FROM llm_providers AS provider
    JOIN provider_models AS model ON model.provider_id = provider.id
    WHERE provider.name = 'aifarm-maple'
      AND provider.kind = 'chat'
      AND provider.protocol = 'openai_compat'
      AND provider.runtime_hint = 'mlx'
      AND provider.endpoint IS NULL
      AND provider.discovery_service_name = 'llm-openai-maple'
      AND provider.discovery_endpoint_name = 'chat_completions'
      AND provider.api_key_ref = 'DIALOG_API_KEY'
      AND provider.api_key_encrypted IS NULL
      AND provider.enabled
      AND model.model_name = 'maple-preview-2bit-mlx'
      AND model.pool_id = shared_pool_id
      AND model.enabled;
    IF identity_count <> 1 THEN
        RAISE EXCEPTION
            'NInfer cutover requires exactly one enabled Maple source identity; found %',
            identity_count;
    END IF;

    SELECT count(*) INTO identity_count
    FROM llm_providers
    WHERE name = 'aifarm-llamacpp-gpu2'
       OR discovery_service_name = 'llm-openai-qwen27b-gguf';
    IF identity_count > 1 THEN
        RAISE EXCEPTION
            'NInfer cutover found ambiguous VibeThinker source identities: %',
            identity_count;
    ELSIF identity_count = 1 THEN
        SELECT provider.id, model.id
        INTO vibethinker_provider_id, vibethinker_model_id
        FROM llm_providers AS provider
        JOIN provider_models AS model ON model.provider_id = provider.id
        WHERE provider.name = 'aifarm-llamacpp-gpu2'
          AND provider.kind = 'chat'
          AND provider.protocol = 'openai_compat'
          AND provider.runtime_hint = 'llama_cpp'
          AND provider.endpoint IS NULL
          AND provider.discovery_service_name = 'llm-openai-qwen27b-gguf'
          AND provider.discovery_endpoint_name = 'chat_completions'
          AND provider.api_key_ref = 'DIALOG_API_KEY'
          AND provider.api_key_encrypted IS NULL
          AND provider.enabled
          AND model.model_name = 'vibethinker-3b'
          AND model.pool_id = shared_pool_id
          AND model.enabled;
        IF vibethinker_provider_id IS NULL OR vibethinker_model_id IS NULL THEN
            RAISE EXCEPTION 'NInfer cutover VibeThinker source identity drift';
        END IF;
    END IF;

    SELECT count(*) INTO identity_count
    FROM llm_providers
    WHERE name = 'aifarm-ninfer-gpu2'
       OR discovery_service_name = 'llm-openai-qwen38-ninfer';
    IF identity_count = 0
       AND NOT EXISTS (SELECT 1 FROM provider_models WHERE model_name = 'qwen3.8-27b')
    THEN
        INSERT INTO llm_providers (
            name,
            kind,
            protocol,
            runtime_hint,
            endpoint,
            discovery_service_name,
            discovery_endpoint_name,
            api_key_ref,
            enabled,
            config
        )
        VALUES (
            'aifarm-ninfer-gpu2',
            'chat',
            'openai_compat',
            'ninfer',
            NULL,
            'llm-openai-qwen38-ninfer',
            'chat_completions',
            'DIALOG_API_KEY',
            TRUE,
            jsonb_build_object(
                'managed_by', 'ninfer_gpu2_cutover_v1',
                'origin', 'cutover',
                'supports_message_name', FALSE,
                'supports_responses_api', TRUE,
                'supports_anthropic_messages', TRUE
            )
        )
        RETURNING id INTO ninfer_provider_id;

        INSERT INTO provider_models (
            provider_id,
            model_name,
            display_name,
            base_url,
            capabilities,
            embedding_dim,
            pool_id,
            enabled,
            config
        )
        VALUES (
            ninfer_provider_id,
            'qwen3.8-27b',
            'Qwen3.8 27B NInfer',
            NULL,
            ARRAY['chat', 'tools']::TEXT[],
            NULL,
            shared_pool_id,
            TRUE,
            jsonb_build_object(
                'managed_by', 'ninfer_gpu2_cutover_v1',
                'origin', 'cutover',
                'supports_tools', TRUE,
                'supports_structured_outputs', TRUE,
                'supports_response_format', FALSE,
                'supports_reasoning', TRUE,
                'supports_usage_details', TRUE,
                'context_length', 16384
            )
        )
        RETURNING id INTO ninfer_model_id;
    ELSIF identity_count = 1
          AND (SELECT count(*) FROM provider_models WHERE model_name = 'qwen3.8-27b') = 1
    THEN
        SELECT provider.id, model.id
        INTO ninfer_provider_id, ninfer_model_id
        FROM llm_providers AS provider
        JOIN provider_models AS model ON model.provider_id = provider.id
        WHERE provider.name = 'aifarm-ninfer-gpu2'
          AND provider.kind = 'chat'
          AND provider.protocol = 'openai_compat'
          AND provider.runtime_hint IS NULL
          AND provider.endpoint IS NULL
          AND provider.discovery_service_name = 'llm-openai-qwen38-ninfer'
          AND provider.discovery_endpoint_name = 'chat_completions'
          AND provider.api_key_ref = 'DIALOG_API_KEY'
          AND provider.api_key_encrypted IS NULL
          AND NOT provider.enabled
          AND provider.config = jsonb_build_object(
              'managed_by', 'ninfer_gpu2_cutover_v1',
              'origin', 'cutover',
              'supports_message_name', FALSE,
              'supports_responses_api', TRUE,
              'supports_anthropic_messages', TRUE,
              'retired_by', 'ninfer_gpu2_cutover_v1_rollback'
          )
          AND model.model_name = 'qwen3.8-27b'
          AND model.display_name = 'Qwen3.8 27B NInfer'
          AND model.base_url IS NULL
          AND model.capabilities = ARRAY['chat', 'tools']::TEXT[]
          AND model.embedding_dim IS NULL
          AND model.pool_id = shared_pool_id
          AND NOT model.enabled
          AND model.config = jsonb_build_object(
              'managed_by', 'ninfer_gpu2_cutover_v1',
              'origin', 'cutover',
              'supports_tools', TRUE,
              'supports_structured_outputs', TRUE,
              'supports_response_format', FALSE,
              'supports_reasoning', TRUE,
              'supports_usage_details', TRUE,
              'context_length', 16384,
              'retired_by', 'ninfer_gpu2_cutover_v1_rollback'
          );
        IF ninfer_provider_id IS NULL OR ninfer_model_id IS NULL THEN
            RAISE EXCEPTION
                'NInfer identity collision: retained rows are not exact migration-owned rollback rows';
        END IF;
        UPDATE llm_providers
        SET runtime_hint = 'ninfer',
            enabled = TRUE,
            config = config - 'retired_by',
            updated_at = now()
        WHERE id = ninfer_provider_id;
        UPDATE provider_models
        SET enabled = TRUE,
            config = config - 'retired_by'
        WHERE id = ninfer_model_id;
        UPDATE workflow_assignments
        SET enabled = TRUE,
            inference_overrides = inference_overrides
                - 'disabled_by_ninfer_gpu2_cutover_v1_rollback'
        WHERE provider_model_id = ninfer_model_id
          AND inference_overrides
                -> 'disabled_by_ninfer_gpu2_cutover_v1_rollback' = 'true'::jsonb;
    ELSE
        RAISE EXCEPTION
            'NInfer identity collision: expected zero target rows or one exact migration-owned retired pair';
    END IF;

    UPDATE llm_capacity_pools
    SET max_concurrency = 2,
        config = CASE
            WHEN config ? 'ninfer_gpu2_cutover_v1_previous_max_concurrency' THEN config
            ELSE jsonb_set(
                config,
                '{ninfer_gpu2_cutover_v1_previous_max_concurrency}',
                to_jsonb(max_concurrency),
                TRUE
            )
        END
    WHERE id = shared_pool_id;

    SELECT count(*), COALESCE(jsonb_agg(id ORDER BY id), '[]'::jsonb)
    INTO expected_assignment_count, expected_assignment_ids
    FROM workflow_assignments
    WHERE provider_model_id = maple_model_id
      AND enabled;

    UPDATE workflow_assignments
    SET enabled = FALSE,
        inference_overrides = jsonb_set(
            inference_overrides,
            '{disabled_by_ninfer_gpu2_cutover_v1}',
            'true'::jsonb,
            TRUE
        )
    WHERE provider_model_id = vibethinker_model_id
      AND enabled;

    UPDATE workflow_assignments
    SET provider_model_id = ninfer_model_id,
        role = CASE
            WHEN workflow_key = 'memory_subject_merge' THEN 'primary'
            ELSE role
        END,
        weight = CASE
            WHEN workflow_key = 'memory_subject_merge' THEN 100
            ELSE weight
        END,
        fallback_order = CASE
            WHEN workflow_key = 'memory_extraction' THEN 1
            WHEN workflow_key = 'memory_subject_merge' THEN NULL
            ELSE fallback_order
        END,
        inference_overrides = inference_overrides || jsonb_build_object(
            'ninfer_gpu2_cutover_v1_previous_model_id', maple_model_id,
            'ninfer_gpu2_cutover_v1_previous_role', role,
            'ninfer_gpu2_cutover_v1_previous_weight', weight,
            'ninfer_gpu2_cutover_v1_previous_fallback_order', fallback_order
        )
    WHERE provider_model_id = maple_model_id
      AND enabled;
    GET DIAGNOSTICS moved_assignment_count = ROW_COUNT;
    IF moved_assignment_count <> expected_assignment_count THEN
        RAISE EXCEPTION
            'NInfer cutover moved % assignments, expected %',
            moved_assignment_count,
            expected_assignment_count;
    END IF;

    INSERT INTO app_settings (key, value)
    VALUES (
        'llm.routing.ninfer_gpu2_cutover_v1_assignment_ids',
        expected_assignment_ids::TEXT
    );

    UPDATE provider_models
    SET enabled = FALSE
    WHERE id IN (maple_model_id, vibethinker_model_id);
    UPDATE llm_providers
    SET enabled = FALSE,
        updated_at = now()
    WHERE id IN (maple_provider_id, vibethinker_provider_id);

    IF EXISTS (
        SELECT 1 FROM workflow_assignments
        WHERE provider_model_id = maple_model_id AND enabled
    ) OR EXISTS (
        SELECT 1 FROM workflow_assignments
        WHERE provider_model_id = vibethinker_model_id AND enabled
    ) OR (
        SELECT count(*) FROM workflow_assignments
        WHERE provider_model_id = ninfer_model_id
          AND inference_overrides
              -> 'ninfer_gpu2_cutover_v1_previous_model_id' = to_jsonb(maple_model_id)
    ) <> expected_assignment_count THEN
        RAISE EXCEPTION 'NInfer cutover assignment verification failed';
    END IF;
END
$$;
