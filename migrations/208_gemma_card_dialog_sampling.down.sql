-- Restore each sampling key from the marker the up migration saved: a key that was absent
-- before is removed, a key that had a value gets it back.
UPDATE workflow_assignments AS assignment
SET inference_overrides = (
    SELECT COALESCE(jsonb_object_agg(key, value) FILTER (WHERE value <> 'null'::jsonb), '{}'::jsonb)
    FROM jsonb_each(assignment.inference_overrides->'gemma_card_sampling_v1_previous')
) || (
    assignment.inference_overrides
        - 'gemma_card_sampling_v1_previous'
        - 'temperature'
        - 'top_p'
        - 'top_k'
        - 'min_p'
        - 'repeat_penalty'
)
WHERE assignment.inference_overrides ? 'gemma_card_sampling_v1_previous';
