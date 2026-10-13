-- Daily partitions for the Telegram outbox and its attempts, so retention drops
-- whole days instead of deleting rows. Unique keys must contain the partition
-- key, so operation_id and (batch_id, part_index) become plain indexes: enqueue
-- serializes each batch with an advisory lock and reuses existing parts, and
-- attempts lose the foreign key to the outbox (their own partitions age out on
-- the same schedule).
--
-- One-way for data: only the last 7 days of history and every row still in
-- flight are copied; older delivered history is dropped with the old tables.
ALTER TABLE telegram_outbox_attempts RENAME TO telegram_outbox_attempts_legacy;
ALTER TABLE telegram_outbox RENAME TO telegram_outbox_legacy;

CREATE TABLE telegram_outbox (
    LIKE telegram_outbox_legacy
    INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMPRESSION
) PARTITION BY RANGE (created_at);

CREATE TABLE telegram_outbox_attempts (
    LIKE telegram_outbox_attempts_legacy
    INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMPRESSION
) PARTITION BY RANGE (claimed_at);

CREATE TEMP TABLE kept_telegram_outbox ON COMMIT DROP AS
SELECT id
FROM telegram_outbox_legacy
WHERE created_at >= ((now() AT TIME ZONE 'UTC')::date - 7)::timestamp AT TIME ZONE 'UTC'
   OR state IN ('pending', 'leased', 'retry_wait');

SELECT ensure_daily_partition('telegram_outbox', day::date)
FROM generate_series(
    LEAST(
        (SELECT min((outbox.created_at AT TIME ZONE 'UTC')::date)
         FROM telegram_outbox_legacy AS outbox
         JOIN kept_telegram_outbox AS kept ON kept.id = outbox.id),
        (now() AT TIME ZONE 'UTC')::date
    ),
    (now() AT TIME ZONE 'UTC')::date + 7,
    interval '1 day'
) AS day;

SELECT ensure_daily_partition('telegram_outbox_attempts', day::date)
FROM generate_series(
    LEAST(
        (SELECT min((attempt.claimed_at AT TIME ZONE 'UTC')::date)
         FROM telegram_outbox_attempts_legacy AS attempt
         JOIN kept_telegram_outbox AS kept ON kept.id = attempt.outbox_id),
        (now() AT TIME ZONE 'UTC')::date
    ),
    (now() AT TIME ZONE 'UTC')::date + 7,
    interval '1 day'
) AS day;

INSERT INTO telegram_outbox
SELECT outbox.*
FROM telegram_outbox_legacy AS outbox
JOIN kept_telegram_outbox AS kept ON kept.id = outbox.id;

INSERT INTO telegram_outbox_attempts
SELECT attempt.*
FROM telegram_outbox_attempts_legacy AS attempt
JOIN kept_telegram_outbox AS kept ON kept.id = attempt.outbox_id;

ALTER SEQUENCE telegram_outbox_id_seq OWNED BY telegram_outbox.id;
ALTER SEQUENCE telegram_outbox_attempts_id_seq OWNED BY telegram_outbox_attempts.id;

DROP TABLE telegram_outbox_attempts_legacy;
DROP TABLE telegram_outbox_legacy;

ALTER TABLE telegram_outbox
    ADD CONSTRAINT telegram_outbox_blob_id_fkey
    FOREIGN KEY (blob_id) REFERENCES telegram_outbox_blobs(id);

CREATE INDEX telegram_outbox_id_idx ON telegram_outbox (id);
CREATE INDEX telegram_outbox_operation_idx ON telegram_outbox (operation_id);
CREATE INDEX telegram_outbox_batch_part_idx ON telegram_outbox (batch_id, part_index);
CREATE INDEX telegram_outbox_claim_idx
    ON telegram_outbox (state, available_at, priority DESC, id)
    WHERE state IN ('pending', 'retry_wait', 'leased');
CREATE INDEX telegram_outbox_ordering_idx
    ON telegram_outbox (bot_id, ordering_key, batch_id, part_index)
    WHERE state IN ('pending', 'leased', 'retry_wait');
CREATE INDEX telegram_outbox_lease_idx
    ON telegram_outbox (leased_until)
    WHERE state = 'leased';
CREATE INDEX telegram_outbox_terminal_idx
    ON telegram_outbox (confirmed_at, id)
    WHERE state IN ('delivered', 'dead_letter', 'expired', 'cancelled');
CREATE INDEX telegram_outbox_protected_terminal_idx
    ON telegram_outbox (state, updated_at, id)
    WHERE protected AND state IN ('ambiguous', 'dead_letter');
CREATE INDEX telegram_outbox_dialog_job_idx
    ON telegram_outbox (dialog_job_id, state, id)
    WHERE dialog_job_id IS NOT NULL;
CREATE INDEX telegram_outbox_pending_history_idx
    ON telegram_outbox (confirmed_at, id)
    WHERE state = 'delivered' AND last_error_class = 'history_pending';
CREATE INDEX telegram_outbox_ambiguous_create_idx
    ON telegram_outbox (id)
    WHERE state = 'ambiguous' AND delivery_policy = 'create'
      AND chat_id IS NOT NULL AND trigger_message_id IS NOT NULL;

CREATE INDEX telegram_outbox_attempts_id_idx ON telegram_outbox_attempts (id);
CREATE INDEX telegram_outbox_attempts_outbox_idx
    ON telegram_outbox_attempts (outbox_id, attempt DESC);
