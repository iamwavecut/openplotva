-- GPU2 NInfer serves Qwen with eight lanes and Discovery admits eight jobs, so the
-- bot's GPU2 pool admits eight requests. The previous budget and description are
-- recorded for the down migration.

LOCK TABLE llm_capacity_pools IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM llm_capacity_pools
        WHERE config ? 'gpu2_eight_lanes_v1_previous_max_concurrency'
    ) THEN
        RAISE EXCEPTION 'GPU2 eight-lane pool marker collision';
    END IF;
END $$;

UPDATE llm_capacity_pools
SET config = config || jsonb_build_object(
        'gpu2_eight_lanes_v1_previous_max_concurrency', max_concurrency,
        'gpu2_eight_lanes_v1_previous_description', description
    ),
    max_concurrency = 8,
    description = 'GPU2 budget: Qwen3.8-27B GSQ-RCO IQ3_S on ninfer with eight lanes.',
    updated_at = now()
WHERE name = 'aifarm-gpu2-qwen27b';
