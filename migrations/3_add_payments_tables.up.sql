-- Source SHA-256: 03aa9a3c5fead8e9dd8c1486d466a315386dce22ead5ba17a6c3ce2a3a670051

CREATE TABLE subscriptions (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    telegram_payment_charge_id VARCHAR(255) NOT NULL UNIQUE,
    provider_payment_charge_id VARCHAR(255) NOT NULL DEFAULT '',
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_subscriptions_user_id_expires_at ON subscriptions (user_id, expires_at DESC);

CREATE TABLE donations (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    telegram_payment_charge_id VARCHAR(255) NOT NULL UNIQUE,
    provider_payment_charge_id VARCHAR(255) NOT NULL DEFAULT '',
    amount_stars BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_donations_user_id ON donations (user_id);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = NOW();
   RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_subscriptions_updated_at
BEFORE UPDATE ON subscriptions
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();
