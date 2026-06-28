ALTER TABLE IF EXISTS telegram_files
    RESET (autovacuum_vacuum_scale_factor, autovacuum_vacuum_threshold);
ALTER TABLE IF EXISTS users
    RESET (autovacuum_vacuum_scale_factor, autovacuum_vacuum_threshold);
ALTER TABLE IF EXISTS chat_members
    RESET (autovacuum_vacuum_scale_factor, autovacuum_vacuum_threshold);
