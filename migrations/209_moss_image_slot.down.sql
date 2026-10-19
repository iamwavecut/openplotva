DELETE FROM workflow_assignments
WHERE workflow_key = 'image_generation_moss'
  AND inference_overrides ? 'created_by_moss_image_slot_v1';

DELETE FROM provider_models
WHERE config ? 'created_by_moss_image_slot_v1'
  AND NOT EXISTS (
      SELECT 1 FROM workflow_assignments
      WHERE workflow_assignments.provider_model_id = provider_models.id
  );

DELETE FROM workflows
WHERE key = 'image_generation_moss'
  AND NOT EXISTS (
      SELECT 1 FROM workflow_assignments WHERE workflow_key = 'image_generation_moss'
  );
