-- Source SHA-256: 5874834e1022368dc97780123162fe47d46e2be0a6622e95767d620eb9714a41

DROP INDEX IF EXISTS idx_whitecircle_checks_external_session_created_at;
ALTER TABLE whitecircle_checks
	DROP COLUMN IF EXISTS external_session_id;
