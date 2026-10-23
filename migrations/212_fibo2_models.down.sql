LOCK TABLE provider_models, workflows, workflow_assignments IN SHARE ROW EXCLUSIVE MODE;

UPDATE provider_models
SET enabled = TRUE,
    config = config - 'disabled_by_fibo2_v1'
WHERE config ? 'disabled_by_fibo2_v1';

UPDATE workflow_assignments
SET provider_model_id = (inference_overrides ->> 'fibo2_v1_previous_model_id')::BIGINT,
    inference_overrides = inference_overrides - 'fibo2_v1_previous_model_id'
WHERE inference_overrides ? 'fibo2_v1_previous_model_id';

DELETE FROM workflow_assignments
WHERE inference_overrides ? 'created_by_fibo2_v1';

DELETE FROM provider_models
WHERE config ? 'created_by_fibo2_v1'
  AND NOT EXISTS (
      SELECT 1 FROM workflow_assignments
      WHERE workflow_assignments.provider_model_id = provider_models.id
  );

DELETE FROM workflows
WHERE key = 'image_generation_fibo'
  AND NOT EXISTS (
      SELECT 1 FROM workflow_assignments WHERE workflow_key = 'image_generation_fibo'
  );
