-- Source SHA-256: 5874834e1022368dc97780123162fe47d46e2be0a6622e95767d620eb9714a41

ALTER TABLE whitecircle_checks
	ADD COLUMN IF NOT EXISTS external_session_id TEXT;

CREATE INDEX IF NOT EXISTS idx_whitecircle_checks_external_session_created_at
	ON whitecircle_checks (external_session_id, created_at DESC);
