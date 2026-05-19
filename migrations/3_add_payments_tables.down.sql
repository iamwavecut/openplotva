-- Source SHA-256: 03aa9a3c5fead8e9dd8c1486d466a315386dce22ead5ba17a6c3ce2a3a670051

DROP TRIGGER IF EXISTS update_subscriptions_updated_at ON subscriptions;
DROP FUNCTION IF EXISTS update_updated_at_column();
DROP TABLE IF EXISTS donations;
DROP TABLE IF EXISTS subscriptions;
