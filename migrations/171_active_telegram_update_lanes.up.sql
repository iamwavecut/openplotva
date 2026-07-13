-- Lane heads are derived scheduling state, not historical state. Keeping only
-- active heads prevents terminal lane rows from dominating claim planning and
-- keeps statistics representative of the rows the claim query can actually use.
DELETE FROM telegram_update_lanes
WHERE head_inbox_id IS NULL;

-- This table has a high update/delete rate while updates are flowing. Analyze
-- and vacuum it after 1% churn so claim plans and dead tuples recover promptly.
ALTER TABLE telegram_update_lanes SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 100,
    autovacuum_analyze_threshold = 100
);

ANALYZE telegram_update_lanes;
