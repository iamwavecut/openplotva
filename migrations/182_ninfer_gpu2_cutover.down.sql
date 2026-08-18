-- Restore the Maple routes moved by migration 182 while retaining the NInfer
-- rows for historical telemetry attribution and future re-application.

LOCK TABLE app_settings, llm_capacity_pools, llm_providers, provider_models,
    workflow_assignments IN SHARE ROW EXCLUSIVE MODE;

DO $$
DECLARE
    shared_pool_id BIGINT;
    maple_provider_id BIGINT;
    maple_model_id BIGINT;
    vibethinker_provider_id BIGINT;
    vibethinker_model_id BIGINT;
    ninfer_provider_id BIGINT;
    ninfer_model_id BIGINT;
    manifest JSONB;
    expected_assignment_count BIGINT;
    restored_assignment_count BIGINT;
    previous_max_concurrency INTEGER;
BEGIN
    SELECT id INTO shared_pool_id
    FROM llm_capacity_pools
    WHERE name = 'aifarm-gpu2-qwen27b';
    IF shared_pool_id IS NULL OR NOT EXISTS (
        SELECT 1 FROM llm_capacity_pools
        WHERE id = shared_pool_id
          AND max_concurrency = 2
          AND config ? 'ninfer_gpu2_cutover_v1_previous_max_concurrency'
    ) THEN
        RAISE EXCEPTION 'NInfer rollback shared GPU2 pool drift';
    END IF;

    SELECT provider.id, model.id
    INTO maple_provider_id, maple_model_id
    FROM llm_providers AS provider
    JOIN provider_models AS model ON model.provider_id = provider.id
    WHERE provider.name = 'aifarm-maple'
      AND provider.runtime_hint = 'mlx'
      AND provider.discovery_service_name = 'llm-openai-maple'
      AND NOT provider.enabled
      AND model.model_name = 'maple-preview-2bit-mlx'
      AND model.pool_id = shared_pool_id
      AND NOT model.enabled;
    IF maple_provider_id IS NULL OR maple_model_id IS NULL THEN
        RAISE EXCEPTION 'NInfer rollback Maple source identity drift';
    END IF;

    IF EXISTS (
        SELECT 1 FROM llm_providers
        WHERE name = 'aifarm-llamacpp-gpu2'
           OR discovery_service_name = 'llm-openai-qwen27b-gguf'
    ) THEN
        SELECT provider.id, model.id
        INTO vibethinker_provider_id, vibethinker_model_id
        FROM llm_providers AS provider
        JOIN provider_models AS model ON model.provider_id = provider.id
        WHERE provider.name = 'aifarm-llamacpp-gpu2'
          AND provider.runtime_hint = 'llama_cpp'
          AND provider.discovery_service_name = 'llm-openai-qwen27b-gguf'
          AND NOT provider.enabled
          AND model.model_name = 'vibethinker-3b'
          AND model.pool_id = shared_pool_id
          AND NOT model.enabled;
        IF vibethinker_provider_id IS NULL OR vibethinker_model_id IS NULL THEN
            RAISE EXCEPTION 'NInfer rollback VibeThinker source identity drift';
        END IF;
    END IF;

    SELECT provider.id, model.id
    INTO ninfer_provider_id, ninfer_model_id
    FROM llm_providers AS provider
    JOIN provider_models AS model ON model.provider_id = provider.id
    WHERE provider.name = 'aifarm-ninfer-gpu2'
      AND provider.kind = 'chat'
      AND provider.protocol = 'openai_compat'
      AND provider.runtime_hint = 'ninfer'
      AND provider.discovery_service_name = 'llm-openai-qwen38-ninfer'
      AND provider.discovery_endpoint_name = 'chat_completions'
      AND provider.api_key_ref = 'DIALOG_API_KEY'
      AND provider.enabled
      AND provider.config ->> 'managed_by' = 'ninfer_gpu2_cutover_v1'
      AND model.model_name = 'qwen3.8-27b'
      AND model.pool_id = shared_pool_id
      AND model.enabled
      AND model.config ->> 'managed_by' = 'ninfer_gpu2_cutover_v1';
    IF ninfer_provider_id IS NULL OR ninfer_model_id IS NULL THEN
        RAISE EXCEPTION 'NInfer rollback target identity drift';
    END IF;

    IF EXISTS (
        SELECT 1 FROM workflow_assignments
        WHERE inference_overrides ? 'disabled_by_ninfer_gpu2_cutover_v1_rollback'
    ) THEN
        RAISE EXCEPTION 'NInfer rollback disable-marker collision';
    END IF;

    SELECT value::JSONB INTO manifest
    FROM app_settings
    WHERE key = 'llm.routing.ninfer_gpu2_cutover_v1_assignment_ids';
    IF manifest IS NULL OR jsonb_typeof(manifest) <> 'array' THEN
        RAISE EXCEPTION 'NInfer rollback assignment manifest missing or invalid';
    END IF;
    SELECT jsonb_array_length(manifest) INTO expected_assignment_count;

    IF EXISTS (
        SELECT 1 FROM workflow_assignments
        WHERE inference_overrides ?| ARRAY[
            'ninfer_gpu2_cutover_v1_previous_model_id',
            'ninfer_gpu2_cutover_v1_previous_role',
            'ninfer_gpu2_cutover_v1_previous_weight',
            'ninfer_gpu2_cutover_v1_previous_fallback_order'
        ]
          AND NOT inference_overrides ?& ARRAY[
              'ninfer_gpu2_cutover_v1_previous_model_id',
              'ninfer_gpu2_cutover_v1_previous_role',
              'ninfer_gpu2_cutover_v1_previous_weight',
              'ninfer_gpu2_cutover_v1_previous_fallback_order'
          ]
    ) OR EXISTS (
        SELECT 1 FROM workflow_assignments
        WHERE inference_overrides ? 'ninfer_gpu2_cutover_v1_previous_model_id'
          AND (
              provider_model_id <> ninfer_model_id
              OR inference_overrides
                  -> 'ninfer_gpu2_cutover_v1_previous_model_id' <> to_jsonb(maple_model_id)
              OR NOT (to_jsonb(id) <@ manifest)
          )
    ) OR (
        SELECT count(*) FROM workflow_assignments
        WHERE provider_model_id = ninfer_model_id
          AND inference_overrides
              -> 'ninfer_gpu2_cutover_v1_previous_model_id' = to_jsonb(maple_model_id)
    ) <> expected_assignment_count THEN
        RAISE EXCEPTION 'NInfer rollback marker drift';
    END IF;

    UPDATE workflow_assignments
    SET enabled = FALSE,
        inference_overrides = jsonb_set(
            inference_overrides,
            '{disabled_by_ninfer_gpu2_cutover_v1_rollback}',
            'true'::jsonb,
            TRUE
        )
    WHERE provider_model_id = ninfer_model_id
      AND enabled
      AND NOT inference_overrides ? 'ninfer_gpu2_cutover_v1_previous_model_id';

    UPDATE workflow_assignments
    SET provider_model_id = maple_model_id,
        role = inference_overrides
            ->> 'ninfer_gpu2_cutover_v1_previous_role',
        weight = CASE
            WHEN inference_overrides
                    -> 'ninfer_gpu2_cutover_v1_previous_weight' = 'null'::jsonb
            THEN NULL
            ELSE (inference_overrides
                    ->> 'ninfer_gpu2_cutover_v1_previous_weight')::INTEGER
        END,
        fallback_order = CASE
            WHEN inference_overrides
                    -> 'ninfer_gpu2_cutover_v1_previous_fallback_order' = 'null'::jsonb
            THEN NULL
            ELSE (inference_overrides
                    ->> 'ninfer_gpu2_cutover_v1_previous_fallback_order')::INTEGER
        END,
        inference_overrides = inference_overrides
            - 'ninfer_gpu2_cutover_v1_previous_model_id'
            - 'ninfer_gpu2_cutover_v1_previous_role'
            - 'ninfer_gpu2_cutover_v1_previous_weight'
            - 'ninfer_gpu2_cutover_v1_previous_fallback_order'
    WHERE provider_model_id = ninfer_model_id
      AND inference_overrides
          -> 'ninfer_gpu2_cutover_v1_previous_model_id' = to_jsonb(maple_model_id)
      AND to_jsonb(id) <@ manifest;
    GET DIAGNOSTICS restored_assignment_count = ROW_COUNT;
    IF restored_assignment_count <> expected_assignment_count THEN
        RAISE EXCEPTION
            'NInfer rollback restored % assignments, expected %',
            restored_assignment_count,
            expected_assignment_count;
    END IF;

    UPDATE workflow_assignments
    SET enabled = TRUE,
        inference_overrides = inference_overrides
            - 'disabled_by_ninfer_gpu2_cutover_v1'
    WHERE provider_model_id = vibethinker_model_id
      AND inference_overrides
            -> 'disabled_by_ninfer_gpu2_cutover_v1' = 'true'::jsonb;

    UPDATE llm_providers
    SET enabled = TRUE,
        updated_at = now()
    WHERE id IN (maple_provider_id, vibethinker_provider_id);
    UPDATE provider_models
    SET enabled = TRUE
    WHERE id IN (maple_model_id, vibethinker_model_id);

    UPDATE provider_models
    SET enabled = FALSE,
        config = config || jsonb_build_object(
            'retired_by', 'ninfer_gpu2_cutover_v1_rollback'
        )
    WHERE id = ninfer_model_id;
    UPDATE llm_providers
    SET runtime_hint = NULL,
        enabled = FALSE,
        config = config || jsonb_build_object(
            'retired_by', 'ninfer_gpu2_cutover_v1_rollback'
        ),
        updated_at = now()
    WHERE id = ninfer_provider_id;

    SELECT (config ->> 'ninfer_gpu2_cutover_v1_previous_max_concurrency')::INTEGER
    INTO previous_max_concurrency
    FROM llm_capacity_pools
    WHERE id = shared_pool_id;
    IF previous_max_concurrency IS NULL OR previous_max_concurrency <= 0 THEN
        RAISE EXCEPTION 'NInfer rollback previous pool concurrency is invalid';
    END IF;
    UPDATE llm_capacity_pools
    SET max_concurrency = previous_max_concurrency,
        config = config - 'ninfer_gpu2_cutover_v1_previous_max_concurrency'
    WHERE id = shared_pool_id;

    DELETE FROM app_settings
    WHERE key = 'llm.routing.ninfer_gpu2_cutover_v1_assignment_ids';

    IF EXISTS (
        SELECT 1 FROM workflow_assignments
        WHERE provider_model_id = ninfer_model_id AND enabled
    ) OR EXISTS (
        SELECT 1 FROM workflow_assignments
        WHERE inference_overrides ?| ARRAY[
            'ninfer_gpu2_cutover_v1_previous_model_id',
            'ninfer_gpu2_cutover_v1_previous_role',
            'ninfer_gpu2_cutover_v1_previous_weight',
            'ninfer_gpu2_cutover_v1_previous_fallback_order',
            'disabled_by_ninfer_gpu2_cutover_v1'
        ]
    ) THEN
        RAISE EXCEPTION 'NInfer rollback assignment verification failed';
    END IF;
END
$$;

ALTER TABLE llm_providers
    DROP CONSTRAINT llm_providers_runtime_hint_check;
ALTER TABLE llm_providers
    ADD CONSTRAINT llm_providers_runtime_hint_check CHECK (runtime_hint IS NULL OR runtime_hint IN
        ('llama_cpp', 'mlx', 'vllm', 'sglang', 'ollama', 'tgi'));
