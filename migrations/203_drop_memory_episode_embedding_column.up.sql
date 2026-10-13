-- The episode embedding only fed the dropped HNSW index. Its TOAST space is
-- reclaimed when the table is next rewritten (VACUUM FULL).
ALTER TABLE memory_episodes DROP COLUMN IF EXISTS embedding;
