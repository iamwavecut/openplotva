LOCK TABLE workflow_assignments IN SHARE ROW EXCLUSIVE MODE;

UPDATE workflow_assignments
SET role = inference_overrides->'gpu2_history_summary_v1_previous'->>'role',
    weight = (inference_overrides->'gpu2_history_summary_v1_previous'->>'weight')::INTEGER,
    fallback_order =
        (inference_overrides->'gpu2_history_summary_v1_previous'->>'fallback_order')::INTEGER,
    enabled = (inference_overrides->'gpu2_history_summary_v1_previous'->>'enabled')::BOOLEAN,
    inference_overrides = inference_overrides - 'gpu2_history_summary_v1_previous'
WHERE inference_overrides ? 'gpu2_history_summary_v1_previous';
