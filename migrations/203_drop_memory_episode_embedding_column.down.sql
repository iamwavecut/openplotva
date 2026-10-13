-- One-way for data: dropped episode embeddings are not restored, and the writer
-- no longer produces them. Only the empty column comes back.
ALTER TABLE memory_episodes ADD COLUMN IF NOT EXISTS embedding vector(512);
