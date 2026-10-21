-- No-op: the up migration removes the bot token from stored Telegram file URLs,
-- and the token must not come back. One-way for data.
SELECT 1;
