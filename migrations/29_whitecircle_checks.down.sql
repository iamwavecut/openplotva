-- Source SHA-256: 83a9beadaffdc319c97d54a1b096bdadc1af97e84bbd065e7e6932ebfdcf0158

DROP INDEX IF EXISTS idx_whitecircle_checks_external_session_created_at;
DROP INDEX IF EXISTS idx_whitecircle_checks_flagged_created_at;
DROP INDEX IF EXISTS idx_whitecircle_checks_chat_created_at;
DROP INDEX IF EXISTS idx_whitecircle_checks_created_at;
DROP TABLE IF EXISTS whitecircle_checks;
