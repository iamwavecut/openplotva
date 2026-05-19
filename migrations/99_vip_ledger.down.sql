-- Source SHA-256: bccf7edd5c6711f08c9cfbf52289b00b9967f784f36a2f3cfbb198c312473e01

DROP INDEX IF EXISTS idx_vip_events_refund_subscription_unique;
DROP INDEX IF EXISTS idx_vip_events_payment_subscription_unique;
DROP INDEX IF EXISTS idx_vip_events_subscription_id;
DROP INDEX IF EXISTS idx_vip_events_user_effective_expires_desc;
DROP INDEX IF EXISTS idx_vip_events_user_id_id_desc;
DROP TABLE IF EXISTS vip_events;
DROP FUNCTION IF EXISTS vip_create_event(BIGINT, TEXT, BIGINT, BIGINT, BIGINT, TEXT);
DROP FUNCTION IF EXISTS vip_compute_effective_expires(TIMESTAMPTZ, BIGINT, TIMESTAMPTZ);
DROP INDEX IF EXISTS idx_subscriptions_user_active_created_at;
DROP INDEX IF EXISTS idx_subscriptions_user_created_at;
ALTER TABLE subscriptions
	DROP COLUMN IF EXISTS refunded_at,
	DROP COLUMN IF EXISTS canceled_at;
