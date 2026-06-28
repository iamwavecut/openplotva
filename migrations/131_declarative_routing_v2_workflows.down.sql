DELETE FROM workflows
WHERE key = 'image_edit'
  AND NOT EXISTS (
      SELECT 1
      FROM workflow_assignments
      WHERE workflow_key = 'image_edit'
  );
