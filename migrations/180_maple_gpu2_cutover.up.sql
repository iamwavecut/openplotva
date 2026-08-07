-- Cut over the known GPU2 Bonsai routes to a distinct Maple identity. This
-- migration is deliberately strict: it may create the target identity once, or
-- re-enable only the exact migration-owned rows retained by its own rollback.
-- Any missing source identity or unrelated target collision aborts the migration.

LOCK TABLE app_settings, llm_capacity_pools, llm_providers, provider_models,
    workflow_assignments IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE llm_providers
    DROP CONSTRAINT llm_providers_runtime_hint_check;
ALTER TABLE llm_providers
    ADD CONSTRAINT llm_providers_runtime_hint_check CHECK (runtime_hint IS NULL OR runtime_hint IN
        ('llama_cpp', 'mlx', 'vllm', 'sglang', 'ollama', 'tgi'));

DO $$
DECLARE
    old_provider_id BIGINT;
    bonsai_model_id BIGINT;
    vibe_model_id BIGINT;
    shared_pool_id BIGINT;
    maple_provider_id BIGINT;
    maple_model_id BIGINT;
    identity_count BIGINT;
    expected_assignment_count BIGINT;
    moved_assignment_count BIGINT;
    verified_assignment_count BIGINT;
    expected_assignment_ids JSONB;
    fresh_install BOOLEAN := FALSE;
BEGIN
    IF EXISTS (
        SELECT 1
        FROM app_settings
        WHERE key = 'llm.routing.maple_gpu2_cutover_v1_assignment_ids'
    ) THEN
        RAISE EXCEPTION
            'Maple cutover assignment manifest already exists';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM workflow_assignments
        WHERE inference_overrides
            ? 'maple_gpu2_cutover_v1_previous_model_id'
    ) THEN
        RAISE EXCEPTION
            'Maple cutover marker collision: prior-model marker already exists';
    END IF;

    SELECT count(*), min(id)
    INTO identity_count, shared_pool_id
    FROM llm_capacity_pools
    WHERE name = 'aifarm-gpu2-qwen27b';
    IF identity_count <> 1 THEN
        RAISE EXCEPTION
            'Maple cutover requires exactly one shared GPU2 pool; found %',
            identity_count;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM llm_capacity_pools
        WHERE id = shared_pool_id
          AND max_concurrency = 1
    ) THEN
        RAISE EXCEPTION 'shared GPU2 pool must already have max_concurrency = 1';
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
          AND provider.config ->> 'retired_by'
                = 'maple_gpu2_cutover_v1_rollback'
          AND model.model_name = 'maple-preview-2bit-mlx'
          AND model.config ->> 'managed_by' = 'maple_gpu2_cutover_v1'
          AND model.config ->> 'origin' = 'fresh'
          AND model.config ->> 'retired_by'
                = 'maple_gpu2_cutover_v1_rollback'
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
                'fresh-origin Maple re-up has conflicting retained source identities';
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
                    'fresh-origin Maple re-up has drifted VibeThinker identity';
            END IF;
        END IF;
    ELSIF identity_count = 0 THEN
        fresh_install := TRUE;

        IF (SELECT count(*) FROM provider_models
            WHERE model_name IN ('ternary-bonsai-27b', 'vibethinker-3b')) <> 0
        THEN
            RAISE EXCEPTION
                'fresh Maple cutover shape has orphaned Bonsai/Vibe model rows';
        END IF;
        IF (SELECT count(*) FROM llm_providers
            WHERE NOT (
                name = 'aifarm-maple'
                AND discovery_service_name = 'llm-openai-maple'
            )) <> 1
           OR NOT EXISTS (
                SELECT 1
                FROM llm_providers
                WHERE name = 'openrouter-free'
                  AND kind = 'chat'
                  AND protocol = 'openai_compat'
                  AND NOT enabled
           )
        THEN
            RAISE EXCEPTION
                'fresh Maple cutover provider registry does not match migration 179';
        END IF;
        IF (SELECT count(*) FROM provider_models
            WHERE model_name <> 'maple-preview-2bit-mlx') <> 1
           OR NOT EXISTS (
                SELECT 1
                FROM provider_models AS model
                JOIN llm_providers AS provider
                  ON provider.id = model.provider_id
                JOIN llm_capacity_pools AS pool
                  ON pool.id = model.pool_id
                WHERE provider.name = 'openrouter-free'
                  AND model.model_name = 'openrouter/free'
                  AND model.enabled
                  AND pool.name = 'openrouter-free'
                  AND pool.max_concurrency = 1
           )
        THEN
            RAISE EXCEPTION
                'fresh Maple cutover model registry does not match migration 179';
        END IF;
        IF (SELECT count(*) FROM llm_capacity_pools) <> 2
           OR NOT EXISTS (
                SELECT 1
                FROM llm_capacity_pools
                WHERE name = 'openrouter-free'
                  AND max_concurrency = 1
           )
        THEN
            RAISE EXCEPTION
                'fresh Maple cutover pool registry does not match migration 179';
        END IF;
        IF (SELECT count(*) FROM workflow_assignments) <> 7
           OR EXISTS (
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
           )
        THEN
            RAISE EXCEPTION
                'fresh Maple cutover assignments do not match migration 179';
        END IF;
        IF (SELECT count(*) FROM llm_routing_events) <> 0
           OR EXISTS (
                SELECT 1
                FROM app_settings
                WHERE key LIKE 'llm.routing.%'
           )
        THEN
            RAISE EXCEPTION
                'fresh Maple cutover requires no routing events or convergence guards';
        END IF;
    ELSE
        IF identity_count <> 1 THEN
            RAISE EXCEPTION
                'Maple cutover requires exactly one retained GPU2 provider; found %',
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
                'Maple cutover requires exactly one Bonsai model row; found %',
                identity_count;
        END IF;
        IF NOT EXISTS (
            SELECT 1
            FROM provider_models
            WHERE id = bonsai_model_id
              AND pool_id = shared_pool_id
              AND enabled
        ) THEN
            RAISE EXCEPTION 'Bonsai model must be enabled in the shared GPU2 pool';
        END IF;

        SELECT count(*), min(CASE WHEN provider_id = old_provider_id THEN id END)
        INTO identity_count, vibe_model_id
        FROM provider_models
        WHERE model_name = 'vibethinker-3b';
        IF identity_count <> 1 OR vibe_model_id IS NULL THEN
            RAISE EXCEPTION
                'Maple cutover requires exactly one VibeThinker model row; found %',
                identity_count;
        END IF;
        IF NOT EXISTS (
            SELECT 1
            FROM provider_models
            WHERE id = vibe_model_id
              AND pool_id = shared_pool_id
              AND enabled
        ) THEN
            RAISE EXCEPTION 'VibeThinker must be enabled in the shared GPU2 pool';
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
                'Maple cutover requires all routing-cascade convergence guards';
        END IF;

        SELECT count(*)
        INTO expected_assignment_count
        FROM workflow_assignments
        WHERE provider_model_id = bonsai_model_id
          AND enabled;
        IF expected_assignment_count < 1 THEN
            RAISE EXCEPTION
                'Maple cutover requires at least one enabled Bonsai assignment';
        END IF;
        SELECT jsonb_agg(id ORDER BY id)
        INTO expected_assignment_ids
        FROM workflow_assignments
        WHERE provider_model_id = bonsai_model_id
          AND enabled;
    END IF;

    SELECT count(*)
    INTO identity_count
    FROM llm_providers
    WHERE name = 'aifarm-maple'
       OR discovery_service_name = 'llm-openai-maple';
    SELECT count(*)
    INTO verified_assignment_count
    FROM provider_models
    WHERE model_name = 'maple-preview-2bit-mlx';

    IF identity_count = 0 AND verified_assignment_count = 0 THEN
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
            'aifarm-maple',
            'chat',
            'openai_compat',
            'mlx',
            NULL,
            'llm-openai-maple',
            'chat_completions',
            'DIALOG_API_KEY',
            TRUE,
            jsonb_build_object(
                'managed_by', 'maple_gpu2_cutover_v1',
                'origin', CASE WHEN fresh_install THEN 'fresh' ELSE 'upgrade' END
            )
        )
        RETURNING id INTO maple_provider_id;

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
            maple_provider_id,
            'maple-preview-2bit-mlx',
            'Maple Preview 2-bit MLX',
            NULL,
            ARRAY['chat', 'tools']::TEXT[],
            NULL,
            shared_pool_id,
            TRUE,
            jsonb_build_object(
                'managed_by', 'maple_gpu2_cutover_v1',
                'origin', CASE WHEN fresh_install THEN 'fresh' ELSE 'upgrade' END,
                'supports_tools', TRUE,
                'supports_structured_outputs', TRUE,
                'supports_response_format', TRUE,
                'supports_reasoning', TRUE
            )
        )
        RETURNING id INTO maple_model_id;
    ELSIF identity_count = 1 AND verified_assignment_count = 1 THEN
        SELECT provider.id, model.id
        INTO maple_provider_id, maple_model_id
        FROM llm_providers AS provider
        JOIN provider_models AS model
          ON model.provider_id = provider.id
        WHERE provider.name = 'aifarm-maple'
          AND provider.kind = 'chat'
          AND provider.protocol = 'openai_compat'
          AND provider.runtime_hint IS NULL
          AND provider.endpoint IS NULL
          AND provider.discovery_service_name = 'llm-openai-maple'
          AND provider.discovery_endpoint_name = 'chat_completions'
          AND provider.api_key_ref = 'DIALOG_API_KEY'
          AND provider.api_key_encrypted IS NULL
          AND NOT provider.enabled
          AND provider.config = jsonb_build_object(
              'managed_by', 'maple_gpu2_cutover_v1',
              'origin', CASE WHEN fresh_install THEN 'fresh' ELSE 'upgrade' END,
              'retired_by', 'maple_gpu2_cutover_v1_rollback'
          )
          AND model.model_name = 'maple-preview-2bit-mlx'
          AND model.display_name = 'Maple Preview 2-bit MLX'
          AND model.base_url IS NULL
          AND model.capabilities = ARRAY['chat', 'tools']::TEXT[]
          AND model.embedding_dim IS NULL
          AND model.pool_id = shared_pool_id
          AND NOT model.enabled
          AND model.config = jsonb_build_object(
              'managed_by', 'maple_gpu2_cutover_v1',
              'origin', CASE WHEN fresh_install THEN 'fresh' ELSE 'upgrade' END,
              'retired_by', 'maple_gpu2_cutover_v1_rollback',
              'supports_tools', TRUE,
              'supports_structured_outputs', TRUE,
              'supports_response_format', TRUE,
              'supports_reasoning', TRUE
          );
        IF maple_provider_id IS NULL OR maple_model_id IS NULL THEN
            RAISE EXCEPTION
                'Maple identity collision: retained rows are not exact migration-owned rollback rows';
        END IF;

        UPDATE llm_providers
        SET runtime_hint = 'mlx',
            enabled = TRUE,
            config = config - 'retired_by',
            updated_at = now()
        WHERE id = maple_provider_id;
        UPDATE provider_models
        SET enabled = TRUE,
            config = config - 'retired_by'
        WHERE id = maple_model_id;
    ELSE
        RAISE EXCEPTION
            'Maple identity collision: expected zero target rows or one exact migration-owned retired pair';
    END IF;

    SELECT count(*)
    INTO identity_count
    FROM provider_models AS model
    JOIN llm_providers AS provider
      ON provider.id = model.provider_id
    WHERE provider.id = maple_provider_id
      AND provider.name = 'aifarm-maple'
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
      AND model.id = maple_model_id
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
    IF identity_count <> 1 THEN
        RAISE EXCEPTION 'Maple cutover failed to establish the exact target identity';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM workflow_assignments
        WHERE inference_overrides
                ? 'disabled_by_maple_gpu2_cutover_v1_rollback'
          AND (
              provider_model_id <> maple_model_id
              OR enabled
              OR inference_overrides
                    -> 'disabled_by_maple_gpu2_cutover_v1_rollback'
                    IS DISTINCT FROM 'true'::jsonb
          )
    ) THEN
        RAISE EXCEPTION
            'Maple cutover rollback-disable marker drift';
    END IF;
    UPDATE workflow_assignments
    SET enabled = TRUE,
        inference_overrides = inference_overrides
            - 'disabled_by_maple_gpu2_cutover_v1_rollback'
    WHERE provider_model_id = maple_model_id
      AND inference_overrides
            -> 'disabled_by_maple_gpu2_cutover_v1_rollback' = 'true'::jsonb;

    IF NOT fresh_install THEN
        UPDATE workflow_assignments
        SET provider_model_id = maple_model_id,
            inference_overrides = jsonb_set(
                inference_overrides,
                '{maple_gpu2_cutover_v1_previous_model_id}',
                to_jsonb(bonsai_model_id),
                TRUE
            )
        WHERE provider_model_id = bonsai_model_id
          AND enabled;
        GET DIAGNOSTICS moved_assignment_count = ROW_COUNT;
        IF moved_assignment_count <> expected_assignment_count THEN
            RAISE EXCEPTION
                'Maple cutover moved % assignments, expected %',
                moved_assignment_count,
                expected_assignment_count;
        END IF;

        SELECT count(*)
        INTO verified_assignment_count
        FROM workflow_assignments
        WHERE provider_model_id = maple_model_id
          AND inference_overrides
              -> 'maple_gpu2_cutover_v1_previous_model_id' = to_jsonb(bonsai_model_id);
        IF verified_assignment_count <> expected_assignment_count THEN
            RAISE EXCEPTION
                'Maple cutover verified % assignments, expected %',
                verified_assignment_count,
                expected_assignment_count;
        END IF;
        IF EXISTS (
            SELECT 1
            FROM workflow_assignments
            WHERE provider_model_id = bonsai_model_id
              AND enabled
        ) THEN
            RAISE EXCEPTION 'enabled Bonsai assignments remain after Maple cutover';
        END IF;

        INSERT INTO app_settings (key, value)
        VALUES (
            'llm.routing.maple_gpu2_cutover_v1_assignment_ids',
            expected_assignment_ids::TEXT
        );

        UPDATE provider_models
        SET enabled = FALSE
        WHERE id = bonsai_model_id;
        GET DIAGNOSTICS identity_count = ROW_COUNT;
        IF identity_count <> 1 THEN
            RAISE EXCEPTION 'Maple cutover failed to disable the preserved Bonsai row';
        END IF;
    END IF;
END
$$;
