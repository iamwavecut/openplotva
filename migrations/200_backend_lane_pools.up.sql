-- Lane pools describe physical resources and are named aifarm-<device>-<purpose>.
-- GPU1's draw, music and speech recognition share one sequential lane under the
-- VRAM arbiter; its privacy filter runs beside that lane. Renamed pools keep their
-- ids, so model rows keep their pool_id. Every change records what it replaced.

LOCK TABLE llm_capacity_pools, llm_providers, provider_models IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM llm_capacity_pools
        WHERE config ?| ARRAY['backend_lanes_v1_previous_name', 'created_by_backend_lanes_v1']
    ) OR EXISTS (
        SELECT 1 FROM provider_models WHERE config ? 'backend_lanes_v1_previous_pool_id'
    ) THEN
        RAISE EXCEPTION 'backend lanes marker collision';
    END IF;
END $$;

UPDATE llm_capacity_pools AS pool
SET config = pool.config || jsonb_build_object(
        'backend_lanes_v1_previous_name', pool.name,
        'backend_lanes_v1_previous_description', pool.description
    ),
    name = renamed.new_name,
    description = renamed.description,
    updated_at = now()
FROM (
    VALUES
        ('aifarm-vllm-gpu0', 'aifarm-gpu0-dialog', 'GPU0 dialog model: eight lanes.'),
        ('aifarm-gpu2-qwen27b', 'aifarm-gpu2-utility', 'GPU2 utility model: eight lanes.'),
        ('aifarm-draw-gpu1', 'aifarm-gpu1-media',
         'GPU1 draw, music and speech recognition: one lane under the VRAM arbiter.')
) AS renamed (old_name, new_name, description)
WHERE pool.name = renamed.old_name
  AND NOT EXISTS (SELECT 1 FROM llm_capacity_pools WHERE name = renamed.new_name);

INSERT INTO llm_capacity_pools (name, max_concurrency, description, config)
SELECT new_pool.name, new_pool.width, new_pool.description,
       '{"created_by_backend_lanes_v1": true}'::jsonb
FROM (
    VALUES
        ('aifarm-embedder', 8, 'Embedder: eight lanes.'),
        ('aifarm-gpu1-privacy', 4, 'GPU1 privacy filter: four lanes beside the media lane.')
) AS new_pool (name, width, description)
WHERE NOT EXISTS (SELECT 1 FROM llm_capacity_pools WHERE name = new_pool.name);

UPDATE provider_models AS model
SET config = model.config || jsonb_build_object(
        'backend_lanes_v1_previous_pool_id', model.pool_id
    ),
    pool_id = target_pool.id
FROM llm_providers AS provider,
     (
         VALUES
             ('aifarm-asr', 'aifarm-gpu1-media'),
             ('aifarm-embed', 'aifarm-embedder'),
             ('privacy-filter', 'aifarm-gpu1-privacy')
     ) AS mapping (provider_name, pool_name),
     llm_capacity_pools AS target_pool
WHERE provider.id = model.provider_id
  AND provider.name = mapping.provider_name
  AND target_pool.name = mapping.pool_name
  AND model.pool_id IS DISTINCT FROM target_pool.id;
