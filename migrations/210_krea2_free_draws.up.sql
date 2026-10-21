-- Free users' draws (`image_generation_boogu_turbo`) move from FLUX.3 to Krea 2 Turbo, with
-- FLUX.3 as their fallback, as it is for the VIP Krea 2 slot. VIP draws drop their FLUX.3
-- slot in code and keep Krea 2 and MOSS-Image; FLUX.3 keeps the VIP edits. Only a FLUX.3
-- primary is switched, and only when an enabled Krea 2 model exists on aifarm-draw; any other
-- database is left unchanged. The switched primary records its previous model and the created
-- fallback carries a marker, so the down migration restores the previous routing exactly.

LOCK TABLE llm_providers, provider_models, workflow_assignments
    IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM workflow_assignments
        WHERE inference_overrides ? 'krea2_free_draws_v1_previous_model_id'
           OR inference_overrides ? 'created_by_krea2_free_draws_v1'
    ) THEN
        RAISE EXCEPTION 'Krea 2 free draws routing marker collision';
    END IF;
END $$;

CREATE TEMPORARY TABLE krea2_free_draw_models ON COMMIT DROP AS
SELECT
    max(model.id) FILTER (WHERE model.config ->> 'endpoint_name' = 'krea2_generate') AS krea2_id,
    max(model.id) FILTER (WHERE model.config ->> 'endpoint_name' = 'flux3_generate') AS flux3_id
FROM provider_models AS model
JOIN llm_providers AS provider ON provider.id = model.provider_id
WHERE provider.name = 'aifarm-draw'
  AND model.enabled;

UPDATE workflow_assignments AS assignment
SET provider_model_id = models.krea2_id,
    inference_overrides = assignment.inference_overrides || jsonb_build_object(
        'krea2_free_draws_v1_previous_model_id', assignment.provider_model_id
    )
FROM krea2_free_draw_models AS models
WHERE assignment.workflow_key = 'image_generation_boogu_turbo'
  AND assignment.scope = 'global'
  AND assignment.role = 'primary'
  AND assignment.enabled
  AND assignment.provider_model_id = models.flux3_id
  AND models.krea2_id IS NOT NULL;

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
    'image_generation_boogu_turbo',
    'global',
    'fallback',
    models.flux3_id,
    NULL,
    0,
    TRUE,
    '{"created_by_krea2_free_draws_v1": true}'::jsonb,
    5,
    30000
FROM krea2_free_draw_models AS models
WHERE EXISTS (
      SELECT 1 FROM workflow_assignments
      WHERE workflow_key = 'image_generation_boogu_turbo'
        AND inference_overrides ? 'krea2_free_draws_v1_previous_model_id'
  )
  AND NOT EXISTS (
      SELECT 1 FROM workflow_assignments
      WHERE workflow_key = 'image_generation_boogu_turbo'
        AND scope = 'global'
        AND role = 'fallback'
        AND enabled
        AND provider_model_id = models.flux3_id
  );
