-- Fibo 2.0 replaces FLUX.3 on aifarm-draw. Fibo 2.0 Edit Turbo (`fibo_edit`) takes every
-- FLUX.3 edit assignment, and Fibo 2.0 Dev (`fibo_generate`) every FLUX.3 text-to-image
-- assignment, the fallbacks of the Krea 2 and MOSS-Image slots among them. VIP draws get
-- a third picture from Fibo 2.0 Dev in `image_generation_fibo`, with Krea 2 as its
-- fallback. Both models share the GPU1 media pool with the other draw-api models. A Fibo
-- 2.0 Dev draw takes 100 to 120 s (50 steps), so its retry wall is 240 s: the Krea 2
-- fallback can still start after an attempt that waited for the lane and failed late.
-- Nothing changes unless the aifarm-draw provider, the GPU1 media pool and the FLUX.3 and
-- Krea 2 models exist. Every switched row records its previous model, every created row
-- carries a marker, and FLUX.3 is disabled with a marker, so the down migration restores
-- the previous routing exactly.

LOCK TABLE llm_providers, provider_models, workflows, workflow_assignments
    IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM workflow_assignments
        WHERE inference_overrides ? 'fibo2_v1_previous_model_id'
           OR inference_overrides ? 'created_by_fibo2_v1'
    ) OR EXISTS (
        SELECT 1 FROM provider_models
        WHERE config ? 'created_by_fibo2_v1' OR config ? 'disabled_by_fibo2_v1'
    ) THEN
        RAISE EXCEPTION 'Fibo 2.0 routing marker collision';
    END IF;
END $$;

CREATE TEMPORARY TABLE fibo2_draw ON COMMIT DROP AS
SELECT
    provider.id AS provider_id,
    capacity_pool.id AS pool_id,
    (SELECT max(model.id) FROM provider_models AS model
     WHERE model.provider_id = provider.id AND model.enabled
       AND model.config ->> 'endpoint_name' = 'flux3_generate') AS flux3_id,
    (SELECT max(model.id) FROM provider_models AS model
     WHERE model.provider_id = provider.id AND model.enabled
       AND model.config ->> 'endpoint_name' = 'krea2_generate') AS krea2_id
FROM llm_providers AS provider
JOIN llm_capacity_pools AS capacity_pool ON capacity_pool.name = 'aifarm-gpu1-media'
WHERE provider.name = 'aifarm-draw';

DELETE FROM fibo2_draw WHERE flux3_id IS NULL OR krea2_id IS NULL;

INSERT INTO provider_models (provider_id, model_name, display_name, capabilities, enabled, config, pool_id)
SELECT
    draw.provider_id,
    models.model_name,
    models.display_name,
    ARRAY['image'],
    TRUE,
    jsonb_build_object(
        'service_name', 'draw-api',
        'endpoint_name', models.endpoint_name,
        'prompt_target', 'fibo',
        'created_by_fibo2_v1', TRUE
    ),
    draw.pool_id
FROM fibo2_draw AS draw
CROSS JOIN (VALUES
    ('WaveCut/Fibo-2.0-dev-OrbitQuant-W4A8-Heretic',
     'Fibo 2.0 Dev OrbitQuant W4A8 Heretic', 'fibo_generate'),
    ('WaveCut/Fibo-2.0-edit-turbo-OrbitQuant-W4A8-Heretic',
     'Fibo 2.0 Edit Turbo OrbitQuant W4A8 Heretic', 'fibo_edit')
) AS models (model_name, display_name, endpoint_name);

CREATE TEMPORARY TABLE fibo2_models ON COMMIT DROP AS
SELECT
    draw.flux3_id,
    draw.krea2_id,
    max(model.id) FILTER (WHERE model.config ->> 'endpoint_name' = 'fibo_generate') AS dev_id,
    max(model.id) FILTER (WHERE model.config ->> 'endpoint_name' = 'fibo_edit') AS edit_id
FROM fibo2_draw AS draw
JOIN provider_models AS model
  ON model.provider_id = draw.provider_id AND model.config ? 'created_by_fibo2_v1'
GROUP BY draw.flux3_id, draw.krea2_id;

UPDATE workflow_assignments AS assignment
SET provider_model_id = CASE
        WHEN assignment.workflow_key LIKE 'image\_edit%' THEN models.edit_id
        ELSE models.dev_id
    END,
    inference_overrides = assignment.inference_overrides || jsonb_build_object(
        'fibo2_v1_previous_model_id', assignment.provider_model_id
    )
FROM fibo2_models AS models
WHERE assignment.provider_model_id = models.flux3_id
  AND assignment.workflow_key LIKE 'image\_%';

INSERT INTO workflows (key, kind, full_routing, retry_max_hops, retry_wall_ms, enabled, lane_priority)
SELECT 'image_generation_fibo', 'image', TRUE, 3, 240000, TRUE, 1
FROM fibo2_models
ON CONFLICT (key) DO NOTHING;

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
    'image_generation_fibo',
    'global',
    targets.role,
    CASE targets.role WHEN 'primary' THEN models.dev_id ELSE models.krea2_id END,
    targets.weight,
    targets.fallback_order,
    TRUE,
    '{"created_by_fibo2_v1": true}'::jsonb,
    5,
    30000
FROM fibo2_models AS models
CROSS JOIN (VALUES
    ('primary', 100, NULL::INTEGER),
    ('fallback', NULL::INTEGER, 0)
) AS targets (role, weight, fallback_order)
WHERE NOT EXISTS (
    SELECT 1 FROM workflow_assignments WHERE workflow_key = 'image_generation_fibo'
);

UPDATE provider_models AS model
SET enabled = FALSE,
    config = model.config || '{"disabled_by_fibo2_v1": true}'::jsonb
FROM fibo2_models AS models
WHERE model.id = models.flux3_id;
