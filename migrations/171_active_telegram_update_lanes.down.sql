ALTER TABLE telegram_update_lanes RESET (
    autovacuum_vacuum_scale_factor,
    autovacuum_analyze_scale_factor,
    autovacuum_vacuum_threshold,
    autovacuum_analyze_threshold
);

-- The removed NULL lane rows were derived inactive cache entries and are not
-- restored. Older binaries recreate a lane row when a new update arrives.
SELECT 1;
