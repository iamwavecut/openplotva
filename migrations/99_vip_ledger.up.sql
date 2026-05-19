-- Source SHA-256: bccf7edd5c6711f08c9cfbf52289b00b9967f784f36a2f3cfbb198c312473e01

ALTER TABLE subscriptions
	ADD COLUMN canceled_at TIMESTAMPTZ,
	ADD COLUMN refunded_at TIMESTAMPTZ;

CREATE INDEX idx_subscriptions_user_created_at
	ON subscriptions (user_id, created_at DESC, id DESC);

CREATE INDEX idx_subscriptions_user_active_created_at
	ON subscriptions (user_id, created_at DESC, id DESC)
	WHERE canceled_at IS NULL
		AND refunded_at IS NULL
		AND telegram_payment_charge_id NOT LIKE 'admin_grant_%';

CREATE OR REPLACE FUNCTION vip_compute_effective_expires(
	last_effective TIMESTAMPTZ,
	delta_seconds BIGINT,
	event_time TIMESTAMPTZ
) RETURNS TIMESTAMPTZ AS $$
	SELECT GREATEST(
		event_time,
		GREATEST(event_time, COALESCE(last_effective, event_time)) + (delta_seconds::double precision * INTERVAL '1 second')
	);
$$ LANGUAGE SQL IMMUTABLE;


CREATE TABLE vip_events (
	id BIGSERIAL PRIMARY KEY,
	user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
	event_type TEXT NOT NULL,
	delta_seconds BIGINT NOT NULL,
	effective_expires_at TIMESTAMPTZ NOT NULL,
	subscription_id BIGINT REFERENCES subscriptions(id) ON DELETE SET NULL,
	actor_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
	reason TEXT NOT NULL DEFAULT '',
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	CONSTRAINT chk_vip_events_type
		CHECK (
			event_type IN (
				'payment',
				'admin_adjustment',
				'admin_revoke',
				'refund_reversal',
				'legacy_subscription_backfill',
				'legacy_vip_cache_backfill'
			)
		)
);

CREATE INDEX idx_vip_events_user_id_id_desc
	ON vip_events (user_id, id DESC);

CREATE INDEX idx_vip_events_user_effective_expires_desc
	ON vip_events (user_id, effective_expires_at DESC, id DESC);

CREATE INDEX idx_vip_events_subscription_id
	ON vip_events (subscription_id)
	WHERE subscription_id IS NOT NULL;

CREATE UNIQUE INDEX idx_vip_events_payment_subscription_unique
	ON vip_events (subscription_id)
	WHERE event_type = 'payment'
		AND subscription_id IS NOT NULL;

CREATE UNIQUE INDEX idx_vip_events_refund_subscription_unique
	ON vip_events (subscription_id)
	WHERE event_type = 'refund_reversal'
		AND subscription_id IS NOT NULL;

CREATE OR REPLACE FUNCTION vip_create_event(
	p_user_id BIGINT,
	p_event_type TEXT,
	p_delta_seconds BIGINT,
	p_subscription_id BIGINT,
	p_actor_user_id BIGINT,
	p_reason TEXT
) RETURNS vip_events AS $$
DECLARE
	last_effective TIMESTAMPTZ;
	result vip_events;
BEGIN
	PERFORM pg_advisory_xact_lock(p_user_id);

	SELECT effective_expires_at
	INTO last_effective
	FROM vip_events
	WHERE user_id = p_user_id
	ORDER BY id DESC
	LIMIT 1;

	INSERT INTO vip_events (
		user_id,
		event_type,
		delta_seconds,
		effective_expires_at,
		subscription_id,
		actor_user_id,
		reason
	)
	VALUES (
		p_user_id,
		p_event_type,
		p_delta_seconds,
		vip_compute_effective_expires(last_effective, p_delta_seconds, CURRENT_TIMESTAMP),
		p_subscription_id,
		p_actor_user_id,
		COALESCE(p_reason, '')
	)
	ON CONFLICT DO NOTHING
	RETURNING *
	INTO result;

	IF result.id IS NOT NULL THEN
		RETURN result;
	END IF;

	IF p_subscription_id IS NOT NULL THEN
		SELECT *
		INTO result
		FROM vip_events
		WHERE subscription_id = p_subscription_id
			AND event_type = p_event_type
		ORDER BY id DESC
		LIMIT 1;

		IF result.id IS NOT NULL THEN
			RETURN result;
		END IF;
	END IF;

	RAISE EXCEPTION 'vip event insert failed for user % and type %', p_user_id, p_event_type;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
	source_row RECORD;
	last_effective TIMESTAMPTZ;
	delta BIGINT;
	reason_text TEXT;
BEGIN
	FOR source_row IN
		SELECT *
		FROM subscriptions
		ORDER BY user_id, created_at, id
	LOOP
		SELECT effective_expires_at
		INTO last_effective
		FROM vip_events
		WHERE user_id = source_row.user_id
		ORDER BY id DESC
		LIMIT 1;

		IF source_row.telegram_payment_charge_id LIKE 'admin_grant_%' THEN
			delta := GREATEST(
				0,
				FLOOR(EXTRACT(EPOCH FROM (source_row.expires_at - source_row.created_at)))::BIGINT
			);
			reason_text := 'backfill legacy admin grant artifact ' || source_row.telegram_payment_charge_id;
		ELSE
			delta := GREATEST(
				0,
				FLOOR(EXTRACT(EPOCH FROM (source_row.expires_at - source_row.created_at)))::BIGINT
			);
			reason_text := 'backfill legacy payment artifact ' || source_row.telegram_payment_charge_id;
		END IF;

		INSERT INTO vip_events (
			user_id,
			event_type,
			delta_seconds,
			effective_expires_at,
			subscription_id,
			reason,
			created_at
		)
		VALUES (
			source_row.user_id,
			'legacy_subscription_backfill',
			delta,
			vip_compute_effective_expires(last_effective, delta, source_row.created_at),
			source_row.id,
			reason_text,
			source_row.created_at
		);
	END LOOP;
END$$;

DO $$
DECLARE
	cache_row RECORD;
	last_effective TIMESTAMPTZ;
	base_time TIMESTAMPTZ := CURRENT_TIMESTAMP;
	delta BIGINT;
BEGIN
	FOR cache_row IN
		SELECT *
		FROM vip_cache
		WHERE is_vip = TRUE
			AND expires_at > CURRENT_TIMESTAMP
		ORDER BY user_id
	LOOP
		SELECT effective_expires_at
		INTO last_effective
		FROM vip_events
		WHERE user_id = cache_row.user_id
		ORDER BY id DESC
		LIMIT 1;

		delta := GREATEST(
			0,
			FLOOR(
				EXTRACT(
					EPOCH FROM (
						cache_row.expires_at
						- GREATEST(base_time, COALESCE(last_effective, base_time))
					)
				)
			)::BIGINT
		);

		IF delta > 0 THEN
			INSERT INTO vip_events (
				user_id,
				event_type,
				delta_seconds,
				effective_expires_at,
				reason,
				created_at
			)
			VALUES (
				cache_row.user_id,
				'legacy_vip_cache_backfill',
				delta,
				vip_compute_effective_expires(last_effective, delta, base_time),
				'backfill active vip_cache residual',
				base_time
			);
		END IF;
	END LOOP;
END$$;
