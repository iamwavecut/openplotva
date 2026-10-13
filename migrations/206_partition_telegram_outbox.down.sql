-- Back to plain tables with the original keys. History dropped by the up
-- migration and by retention is not restored.
ALTER TABLE telegram_outbox_attempts RENAME TO telegram_outbox_attempts_partitioned;
ALTER TABLE telegram_outbox RENAME TO telegram_outbox_partitioned;

CREATE TABLE telegram_outbox (
    LIKE telegram_outbox_partitioned
    INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMPRESSION
);
CREATE TABLE telegram_outbox_attempts (
    LIKE telegram_outbox_attempts_partitioned
    INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMPRESSION
);

INSERT INTO telegram_outbox SELECT * FROM telegram_outbox_partitioned;
INSERT INTO telegram_outbox_attempts
SELECT attempt.*
FROM telegram_outbox_attempts_partitioned AS attempt
WHERE EXISTS (SELECT 1 FROM telegram_outbox AS outbox WHERE outbox.id = attempt.outbox_id);

ALTER SEQUENCE telegram_outbox_id_seq OWNED BY telegram_outbox.id;
ALTER SEQUENCE telegram_outbox_attempts_id_seq OWNED BY telegram_outbox_attempts.id;

DROP TABLE telegram_outbox_attempts_partitioned;
DROP TABLE telegram_outbox_partitioned;

ALTER TABLE telegram_outbox
    ADD CONSTRAINT telegram_outbox_pkey PRIMARY KEY (id),
    ADD CONSTRAINT telegram_outbox_operation_id_key UNIQUE (operation_id),
    ADD CONSTRAINT telegram_outbox_batch_id_part_index_key UNIQUE (batch_id, part_index),
    ADD CONSTRAINT telegram_outbox_blob_id_fkey
        FOREIGN KEY (blob_id) REFERENCES telegram_outbox_blobs(id);
ALTER TABLE telegram_outbox_attempts
    ADD CONSTRAINT telegram_outbox_attempts_pkey PRIMARY KEY (id),
    ADD CONSTRAINT telegram_outbox_attempts_outbox_id_attempt_key UNIQUE (outbox_id, attempt),
    ADD CONSTRAINT telegram_outbox_attempts_outbox_id_fkey
        FOREIGN KEY (outbox_id) REFERENCES telegram_outbox(id) ON DELETE CASCADE;

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
CREATE INDEX telegram_outbox_attempts_outbox_idx
    ON telegram_outbox_attempts (outbox_id, attempt DESC);
