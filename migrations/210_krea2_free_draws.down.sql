LOCK TABLE workflow_assignments IN SHARE ROW EXCLUSIVE MODE;

DELETE FROM workflow_assignments
WHERE inference_overrides ? 'created_by_krea2_free_draws_v1';

UPDATE workflow_assignments
SET provider_model_id = (inference_overrides ->> 'krea2_free_draws_v1_previous_model_id')::BIGINT,
    inference_overrides = inference_overrides - 'krea2_free_draws_v1_previous_model_id'
WHERE inference_overrides ? 'krea2_free_draws_v1_previous_model_id';
