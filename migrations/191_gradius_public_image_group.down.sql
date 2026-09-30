-- no-transaction
-- Removing the index preserves all advertisement records and visibility metadata.
DROP INDEX CONCURRENTLY IF EXISTS gradius_public_image_group_idx;
