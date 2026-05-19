-- Source SHA-256: 63be40405a4d2bc609975be58af4a4d8d9370e88a125e3256e9e2d297407a8ca

DROP INDEX IF EXISTS idx_telegram_files_requested;
DROP INDEX IF EXISTS idx_telegram_files_pending_status;
DROP INDEX IF EXISTS idx_telegram_files_last_seen;
DROP TABLE IF EXISTS telegram_files;
