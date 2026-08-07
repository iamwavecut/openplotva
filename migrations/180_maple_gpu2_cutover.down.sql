-- Reverse only migration-marked assignments while retaining the Maple identity
-- for immutable routing-event attribution. Missing source rows, unrelated Maple
-- collisions, or assignment-marker drift abort instead of fabricating history.

LOCK TABLE app_settings, llm_capacity_pools, llm_providers, provider_models,
    workflow_assignments IN SHARE ROW EXCLUSIVE MODE;

DO $$
DECLARE
    old_provider_id BIGINT;
    bonsai_model_id BIGINT;
    vibe_model_id BIGINT;
    shared_pool_id BIGINT;
    maple_provider_id BIGINT;
    maple_model_id BIGINT;
    identity_count BIGINT;
    marked_assignment_count BIGINT;
    restored_assignment_count BIGINT;
    expected_assignment_ids JSONB;
    marked_assignment_ids JSONB;
    fresh_install BOOLEAN := FALSE;
BEGIN
    IF EXISTS (
        SELECT 1
        FROM workflow_assignments
        WHERE inference_overrides
            ? 'disabled_by_maple_gpu2_cutover_v1_rollback'
    ) THEN
        RAISE EXCEPTION
            'Maple rollback disable-marker collision';
    END IF;

    SELECT count(*), min(id)
    INTO identity_count, shared_pool_id
    FROM llm_capacity_pools
    WHERE name = 'aifarm-gpu2-qwen27b';
    IF identity_count <> 1 THEN
        RAISE EXCEPTION
            'Maple rollback requires exactly one shared GPU2 pool; found %',
            identity_count;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM llm_capacity_pools
        WHERE id = shared_pool_id
          AND max_concurrency = 1
    ) THEN
        RAISE EXCEPTION 'shared GPU2 pool must retain max_concurrency = 1';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM llm_providers AS provider
        JOIN provider_models AS model
          ON model.provider_id = provider.id
        WHERE provider.name = 'aifarm-maple'
          AND provider.discovery_service_name = 'llm-openai-maple'
          AND provider.config ->> 'managed_by' = 'maple_gpu2_cutover_v1'
          AND provider.config ->> 'origin' = 'fresh'
          AND NOT (provider.config ? 'retired_by')
          AND model.model_name = 'maple-preview-2bit-mlx'
          AND model.config ->> 'managed_by' = 'maple_gpu2_cutover_v1'
          AND model.config ->> 'origin' = 'fresh'
          AND NOT (model.config ? 'retired_by')
    ) THEN
        fresh_install := TRUE;
    END IF;

    SELECT count(*)
    INTO identity_count
    FROM llm_providers
    WHERE name = 'aifarm-llamacpp-gpu2'
       OR discovery_service_name = 'llm-openai-qwen27b-gguf';

    IF fresh_install THEN
        IF (SELECT count(*) FROM provider_models
            WHERE model_name = 'ternary-bonsai-27b') <> 0
           OR identity_count > 1
           OR (
                identity_count = 0
                AND (SELECT count(*) FROM provider_models
                     WHERE model_name = 'vibethinker-3b') <> 0
           )
        THEN
            RAISE EXCEPTION
                'fresh-origin Maple rollback has conflicting retained source identities';
        END IF;
        IF identity_count = 1 THEN
            SELECT id
            INTO old_provider_id
            FROM llm_providers
            WHERE name = 'aifarm-llamacpp-gpu2'
              AND discovery_service_name = 'llm-openai-qwen27b-gguf'
              AND kind = 'chat'
              AND protocol = 'openai_compat'
              AND runtime_hint = 'llama_cpp'
              AND discovery_endpoint_name = 'chat_completions'
              AND enabled;
            IF old_provider_id IS NULL
               OR (SELECT count(*) FROM provider_models
                   WHERE model_name = 'vibethinker-3b') <> 1
               OR (
                    SELECT count(*)
                    FROM provider_models
                    WHERE model_name = 'vibethinker-3b'
                      AND provider_id = old_provider_id
                      AND pool_id = shared_pool_id
                      AND enabled
               ) <> 1 THEN
                RAISE EXCEPTION
                    'fresh-origin Maple rollback has drifted VibeThinker identity';
            END IF;
        END IF;
    ELSIF identity_count = 0 THEN
        fresh_install := TRUE;
        IF (SELECT count(*) FROM provider_models
            WHERE model_name IN ('ternary-bonsai-27b', 'vibethinker-3b')) <> 0
           OR (SELECT count(*) FROM llm_providers
               WHERE NOT (
                   name = 'aifarm-maple'
                   AND discovery_service_name = 'llm-openai-maple'
               )) <> 1
           OR (SELECT count(*) FROM provider_models
               WHERE model_name <> 'maple-preview-2bit-mlx') <> 1
           OR (SELECT count(*) FROM llm_capacity_pools) <> 2
           OR (SELECT count(*) FROM workflow_assignments) <> 7
           OR (SELECT count(*) FROM llm_routing_events) <> 0
           OR EXISTS (
                SELECT 1
                FROM app_settings
                WHERE key LIKE 'llm.routing.%'
           )
        THEN
            RAISE EXCEPTION
                'fresh Maple rollback shape does not match migration 179';
        END IF;
        IF NOT EXISTS (
            SELECT 1
            FROM provider_models AS model
            JOIN llm_providers AS provider
              ON provider.id = model.provider_id
            JOIN llm_capacity_pools AS pool
              ON pool.id = model.pool_id
            WHERE provider.name = 'openrouter-free'
              AND provider.kind = 'chat'
              AND provider.protocol = 'openai_compat'
              AND NOT provider.enabled
              AND model.model_name = 'openrouter/free'
              AND model.enabled
              AND pool.name = 'openrouter-free'
              AND pool.max_concurrency = 1
        ) OR EXISTS (
            SELECT 1
            FROM (
                VALUES
                    ('agentic_image', 1),
                    ('agentic_search_reasoner', 2),
                    ('agentic_search_writer', 3),
                    ('agentic_song', 4),
                    ('history_summary', 5),
                    ('media_prompt_optimizer', 6),
                    ('youtube_summary', 8)
            ) AS expected(workflow_key, fallback_order)
            WHERE (
                SELECT count(*)
                FROM workflow_assignments AS assignment
                JOIN provider_models AS model
                  ON model.id = assignment.provider_model_id
                JOIN llm_providers AS provider
                  ON provider.id = model.provider_id
                WHERE assignment.workflow_key = expected.workflow_key
                  AND assignment.fallback_order = expected.fallback_order
                  AND provider.name = 'openrouter-free'
                  AND model.model_name = 'openrouter/free'
                  AND assignment.scope = 'global'
                  AND assignment.role = 'fallback'
                  AND assignment.enabled
            ) <> 1
        ) THEN
            RAISE EXCEPTION
                'fresh Maple rollback registry does not match migration 179';
        END IF;
    ELSE
        IF identity_count <> 1 THEN
            RAISE EXCEPTION
                'Maple rollback requires exactly one retained GPU2 provider; found %',
                identity_count;
        END IF;
        SELECT id
        INTO old_provider_id
        FROM llm_providers
        WHERE name = 'aifarm-llamacpp-gpu2'
          AND discovery_service_name = 'llm-openai-qwen27b-gguf';
        IF old_provider_id IS NULL OR NOT EXISTS (
            SELECT 1
            FROM llm_providers
            WHERE id = old_provider_id
              AND kind = 'chat'
              AND protocol = 'openai_compat'
              AND runtime_hint = 'llama_cpp'
              AND discovery_endpoint_name = 'chat_completions'
              AND enabled
        ) THEN
            RAISE EXCEPTION 'retained GPU2 provider identity/config drifted';
        END IF;

        SELECT count(*), min(CASE WHEN provider_id = old_provider_id THEN id END)
        INTO identity_count, bonsai_model_id
        FROM provider_models
        WHERE model_name = 'ternary-bonsai-27b';
        IF identity_count <> 1 OR bonsai_model_id IS NULL THEN
            RAISE EXCEPTION
                'Maple rollback requires exactly one Bonsai model row; found %',
                identity_count;
        END IF;
        IF NOT EXISTS (
            SELECT 1
            FROM provider_models
            WHERE id = bonsai_model_id
              AND pool_id = shared_pool_id
              AND NOT enabled
        ) THEN
            RAISE EXCEPTION 'preserved Bonsai model identity/config drifted';
        END IF;

        SELECT count(*), min(CASE WHEN provider_id = old_provider_id THEN id END)
        INTO identity_count, vibe_model_id
        FROM provider_models
        WHERE model_name = 'vibethinker-3b';
        IF identity_count <> 1 OR vibe_model_id IS NULL THEN
            RAISE EXCEPTION
                'Maple rollback requires exactly one VibeThinker model row; found %',
                identity_count;
        END IF;
        IF NOT EXISTS (
            SELECT 1
            FROM provider_models
            WHERE id = vibe_model_id
              AND pool_id = shared_pool_id
              AND enabled
        ) THEN
            RAISE EXCEPTION 'VibeThinker identity/config drifted';
        END IF;

        IF (
            SELECT count(*)
            FROM app_settings
            WHERE key IN (
                'llm.routing.gpu_backfilled',
                'llm.routing.dialog_qwen_fallback',
                'llm.routing.memory_extraction_cascade_v1',
                'llm.routing.memory_subject_merge_cascade_v1',
                'llm.routing.dialog_fallback_cascade_v1'
            )
        ) <> 5 THEN
            RAISE EXCEPTION
                'Maple rollback requires all routing-cascade convergence guards';
        END IF;
    END IF;

    SELECT count(*)
    INTO identity_count
    FROM llm_providers
    WHERE name = 'aifarm-maple'
       OR discovery_service_name = 'llm-openai-maple';
    IF identity_count <> 1 THEN
        RAISE EXCEPTION
            'Maple rollback requires exactly one Maple provider identity; found %',
            identity_count;
    END IF;
    SELECT count(*)
    INTO identity_count
    FROM provider_models
    WHERE model_name = 'maple-preview-2bit-mlx';
    IF identity_count <> 1 THEN
        RAISE EXCEPTION
            'Maple rollback requires exactly one Maple model identity; found %',
            identity_count;
    END IF;

    SELECT provider.id, model.id
    INTO maple_provider_id, maple_model_id
    FROM llm_providers AS provider
    JOIN provider_models AS model
      ON model.provider_id = provider.id
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
      AND provider.config = jsonb_build_object(
          'managed_by', 'maple_gpu2_cutover_v1',
          'origin', CASE WHEN fresh_install THEN 'fresh' ELSE 'upgrade' END
      )
      AND model.model_name = 'maple-preview-2bit-mlx'
      AND model.display_name = 'Maple Preview 2-bit MLX'
      AND model.base_url IS NULL
      AND model.capabilities = ARRAY['chat', 'tools']::TEXT[]
      AND model.embedding_dim IS NULL
      AND model.pool_id = shared_pool_id
      AND model.enabled
      AND model.config = jsonb_build_object(
          'managed_by', 'maple_gpu2_cutover_v1',
          'origin', CASE WHEN fresh_install THEN 'fresh' ELSE 'upgrade' END,
          'supports_tools', TRUE,
          'supports_structured_outputs', TRUE,
          'supports_response_format', TRUE,
          'supports_reasoning', TRUE
      );
    IF maple_provider_id IS NULL OR maple_model_id IS NULL THEN
        RAISE EXCEPTION
            'Maple rollback refused identity/config drift on the migration-owned target';
    END IF;

    SELECT count(*)
    INTO marked_assignment_count
    FROM workflow_assignments
    WHERE inference_overrides
        ? 'maple_gpu2_cutover_v1_previous_model_id';
    IF fresh_install THEN
        IF marked_assignment_count <> 0 OR EXISTS (
            SELECT 1
            FROM app_settings
            WHERE key = 'llm.routing.maple_gpu2_cutover_v1_assignment_ids'
        ) THEN
            RAISE EXCEPTION
                'fresh-origin Maple rollback cannot contain upgraded-origin markers';
        END IF;
    ELSE
        SELECT value::jsonb
        INTO expected_assignment_ids
        FROM app_settings
        WHERE key = 'llm.routing.maple_gpu2_cutover_v1_assignment_ids';
        IF expected_assignment_ids IS NULL
           OR jsonb_typeof(expected_assignment_ids) <> 'array'
        THEN
            RAISE EXCEPTION
                'Maple rollback assignment manifest is missing or invalid';
        END IF;
        SELECT COALESCE(jsonb_agg(id ORDER BY id), '[]'::jsonb)
        INTO marked_assignment_ids
        FROM workflow_assignments
        WHERE inference_overrides
            ? 'maple_gpu2_cutover_v1_previous_model_id';
        IF marked_assignment_ids IS DISTINCT FROM expected_assignment_ids THEN
            RAISE EXCEPTION
                'Maple rollback assignment manifest drift';
        END IF;
        IF marked_assignment_count < 1 THEN
            RAISE EXCEPTION 'Maple rollback found no migration-marked assignments';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM workflow_assignments
            WHERE inference_overrides
                    ? 'maple_gpu2_cutover_v1_previous_model_id'
              AND (
                  provider_model_id <> maple_model_id
                  OR inference_overrides
                        -> 'maple_gpu2_cutover_v1_previous_model_id'
                        IS DISTINCT FROM to_jsonb(bonsai_model_id)
              )
        ) THEN
            RAISE EXCEPTION
                'Maple rollback marker drift: marked assignments must point from Maple to canonical Bonsai';
        END IF;

        UPDATE workflow_assignments
        SET provider_model_id = bonsai_model_id,
            inference_overrides = inference_overrides
                - 'maple_gpu2_cutover_v1_previous_model_id'
        WHERE inference_overrides
            ? 'maple_gpu2_cutover_v1_previous_model_id';
        GET DIAGNOSTICS restored_assignment_count = ROW_COUNT;
        IF restored_assignment_count <> marked_assignment_count THEN
            RAISE EXCEPTION
                'Maple rollback restored % assignments, expected %',
                restored_assignment_count,
                marked_assignment_count;
        END IF;
        DELETE FROM app_settings
        WHERE key = 'llm.routing.maple_gpu2_cutover_v1_assignment_ids';
        GET DIAGNOSTICS identity_count = ROW_COUNT;
        IF identity_count <> 1 THEN
            RAISE EXCEPTION
                'Maple rollback failed to consume the assignment manifest';
        END IF;
    END IF;

    -- A post-cutover Maple assignment has no Bonsai origin marker. Preserve its
    -- model identity, but make it ineligible before retiring the Maple model.
    UPDATE workflow_assignments
    SET enabled = FALSE,
        inference_overrides = jsonb_set(
            inference_overrides,
            '{disabled_by_maple_gpu2_cutover_v1_rollback}',
            'true'::jsonb,
            TRUE
        )
    WHERE provider_model_id = maple_model_id
      AND enabled;

    IF NOT fresh_install THEN
        UPDATE provider_models
        SET enabled = TRUE
        WHERE id = bonsai_model_id;
        GET DIAGNOSTICS identity_count = ROW_COUNT;
        IF identity_count <> 1 THEN
            RAISE EXCEPTION 'Maple rollback failed to re-enable Bonsai';
        END IF;
    END IF;

    UPDATE provider_models
    SET enabled = FALSE,
        config = config || '{
            "retired_by": "maple_gpu2_cutover_v1_rollback"
        }'::jsonb
    WHERE id = maple_model_id;
    GET DIAGNOSTICS identity_count = ROW_COUNT;
    IF identity_count <> 1 THEN
        RAISE EXCEPTION 'Maple rollback failed to retire the Maple model';
    END IF;

    UPDATE llm_providers
    SET enabled = FALSE,
        runtime_hint = NULL,
        config = config || '{
            "retired_by": "maple_gpu2_cutover_v1_rollback"
        }'::jsonb,
        updated_at = now()
    WHERE id = maple_provider_id;
    GET DIAGNOSTICS identity_count = ROW_COUNT;
    IF identity_count <> 1 THEN
        RAISE EXCEPTION 'Maple rollback failed to retire the Maple provider';
    END IF;
END
$$;

ALTER TABLE llm_providers
    DROP CONSTRAINT llm_providers_runtime_hint_check;
ALTER TABLE llm_providers
    ADD CONSTRAINT llm_providers_runtime_hint_check CHECK (runtime_hint IS NULL OR runtime_hint IN
        ('llama_cpp', 'vllm', 'sglang', 'ollama', 'tgi'));
