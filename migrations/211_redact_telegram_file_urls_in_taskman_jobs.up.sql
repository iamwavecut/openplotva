-- Image edit jobs stored Telegram file URLs, which carry the bot token. New jobs
-- store file IDs instead; this removes the token from the URLs already stored.
-- The `bot<redacted>/<file path>` form still resolves for jobs that are queued.
UPDATE taskman_jobs
SET record = regexp_replace(
        record::text,
        '(api\.telegram\.org/file/bot)[0-9]+:[A-Za-z0-9_-]+',
        '\1<redacted>',
        'g'
    )::jsonb
WHERE record::text ~ 'api\.telegram\.org/file/bot[0-9]+:';
