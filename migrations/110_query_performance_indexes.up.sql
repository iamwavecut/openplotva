-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_lower_username
    ON users (lower(username))
    WHERE username IS NOT NULL;
