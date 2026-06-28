-- Catalog-only per-table autovacuum tuning. These hot/large tables accumulated
-- dead tuples between rare passes under the global autovacuum_vacuum_scale_factor
-- (0.2). A small scale factor + fixed threshold makes autovacuum trigger on
-- absolute churn instead of a fraction of the (large) table. No rewrite, no lock
-- of significance -- a catalog update only.
ALTER TABLE IF EXISTS telegram_files
    SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 50000);
ALTER TABLE IF EXISTS users
    SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 50000);
ALTER TABLE IF EXISTS chat_members
    SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 50000);
