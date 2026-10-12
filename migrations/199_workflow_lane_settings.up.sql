-- Each workflow's place in its lane queues and the time it keeps for a fallback.
-- Priority 0 is interactive, 1 a user-facing tool, 2 background work; a lane pool
-- serves lower values first. Background work keeps no fallback reserve: without a
-- deadline it waits for its own lane instead of moving to another backend.

ALTER TABLE workflows
    ADD COLUMN lane_priority SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN fallback_reserve_ms INTEGER NOT NULL DEFAULT 30000;

UPDATE workflows SET lane_priority = 1
WHERE key IN ('history_summary', 'youtube_summary', 'media_prompt_optimizer',
              'image_generation', 'image_generation_flux', 'image_generation_boogu_turbo',
              'image_edit', 'image_edit_flux', 'image_edit_boogu_turbo', 'music');

UPDATE workflows SET lane_priority = 2, fallback_reserve_ms = 0
WHERE key IN ('memory_extraction', 'memory_subject_merge');
