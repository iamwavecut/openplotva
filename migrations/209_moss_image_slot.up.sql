-- VIP draws get a third picture from MOSS-Image (draw-api `moss_generate` on the AI Farm
-- GPU1 worker). `image_generation_moss` is a user-facing tool (lane priority 1) like the
-- other VIP slots. Its model shares the GPU1 media pool with the other draw-api models,
-- and FLUX.3 is its fallback, as it is for the Krea 2 slot. A MOSS draw takes 40 s (30
-- steps), and the first one after a worker load also builds the fused caches (about 90
-- s), so the retry wall is 120 s: the fallback can still start after a MOSS attempt that
-- fails late, where the 60 s of the other slots would end the request. The model and the
-- assignments are created only when the aifarm-draw provider, the GPU1 media pool and the
-- FLUX.3 model exist; otherwise the operator assigns models in the admin routing page.
-- Every created row carries a marker, so the down migration removes exactly these rows.

INSERT INTO workflows (key, kind, full_routing, retry_max_hops, retry_wall_ms, enabled, lane_priority)
VALUES ('image_generation_moss', 'image', TRUE, 3, 120000, TRUE, 1)
ON CONFLICT (key) DO NOTHING;

INSERT INTO provider_models (provider_id, model_name, display_name, capabilities, enabled, config, pool_id)
SELECT
    provider.id,
    'moss-image-sft-heretic',
    'MOSS-Image SFT Heretic',
    ARRAY['image'],
    TRUE,
    jsonb_build_object(
        'service_name', 'draw-api',
        'endpoint_name', 'moss_generate',
        'prompt_target', 'moss',
        'created_by_moss_image_slot_v1', TRUE
    ),
    capacity_pool.id
FROM llm_providers AS provider
JOIN llm_capacity_pools AS capacity_pool ON capacity_pool.name = 'aifarm-gpu1-media'
WHERE provider.name = 'aifarm-draw'
  AND NOT EXISTS (
      SELECT 1 FROM provider_models AS model
      WHERE model.provider_id = provider.id
        AND model.config ->> 'endpoint_name' = 'moss_generate'
  );

WITH draw_models AS (
    SELECT DISTINCT ON (model.config ->> 'endpoint_name')
        model.config ->> 'endpoint_name' AS endpoint_name,
        model.id AS provider_model_id
    FROM provider_models AS model
    JOIN llm_providers AS provider ON provider.id = model.provider_id
    WHERE provider.name = 'aifarm-draw'
      AND model.enabled
      AND model.config ->> 'endpoint_name' IN ('moss_generate', 'flux3_generate')
    ORDER BY model.config ->> 'endpoint_name', model.id
), targets (role, endpoint_name, weight, fallback_order) AS (
    VALUES
        ('primary', 'moss_generate', 100, NULL::INTEGER),
        ('fallback', 'flux3_generate', NULL::INTEGER, 0)
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
    'image_generation_moss',
    'global',
    targets.role,
    draw_models.provider_model_id,
    targets.weight,
    targets.fallback_order,
    TRUE,
    '{"created_by_moss_image_slot_v1": true}'::jsonb,
    5,
    30000
FROM targets
JOIN draw_models USING (endpoint_name)
WHERE (SELECT count(*) FROM draw_models) = 2
  AND NOT EXISTS (
      SELECT 1 FROM workflow_assignments WHERE workflow_key = 'image_generation_moss'
  );
