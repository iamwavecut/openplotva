-- Canonicalize llm_request_events.provider to the routing-registry provider that
-- actually owns the model (provider_models). Historically the non-dialog paths
-- (memory extractor and friends) stamped the generic client name 'aifarm' — which
-- is not even a registered provider — instead of the model's true provider, so the
-- same model (e.g. vram.cloud/qwen3.6-35b-a3b, which belongs to vram-cloud) showed
-- up under both 'aifarm' and 'vram-cloud' in analytics. The dialog path already
-- tags the canonical provider via the model router.
--
-- The mapping mirrors provider_models as of 2026-07-04. "Gemma 4 26B Heretic" is
-- registered under two providers, so it is disambiguated by call flow: vision
-- calls -> aifarm-vision, everything else -> the vLLM chat provider. Raw rows only
-- (rollup rows carry aggregated keys and age out on their own). Models not in the
-- map are left untouched. One-way data fix: the down migration is a no-op because
-- the pre-canonicalization per-row value is not recoverable.

WITH canon AS (
    SELECT
        id,
        CASE
            WHEN model ILIKE 'vram.cloud/%' THEN 'vram-cloud'
            WHEN model IN ('vibethinker-3b', 'qwen3.6-27b-moq') THEN 'aifarm-llamacpp-gpu2'
            WHEN model = 'Gemma 4 26B Heretic' AND flow = 'vision' THEN 'aifarm-vision'
            WHEN model = 'Gemma 4 26B Heretic' THEN 'aifarm-vllm-gpu0'
            ELSE NULL
        END AS canonical_provider
    FROM llm_request_events
    WHERE NOT is_rollup
)
UPDATE llm_request_events e
SET provider = canon.canonical_provider
FROM canon
WHERE e.id = canon.id
  AND canon.canonical_provider IS NOT NULL
  AND e.provider IS DISTINCT FROM canon.canonical_provider;
