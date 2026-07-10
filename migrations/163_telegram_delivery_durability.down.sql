-- Refuse a destructive downgrade while the new delivery plane still owns work.
DO $$
BEGIN
    IF to_regclass('public.telegram_update_inbox') IS NOT NULL
       AND EXISTS (
           SELECT 1
           FROM telegram_update_inbox
           WHERE status NOT IN ('completed', 'ignored', 'dead_letter')
       ) THEN
        RAISE EXCEPTION 'cannot remove Telegram update inbox with nonterminal rows';
    END IF;

    IF to_regclass('public.telegram_outbox') IS NOT NULL
       AND EXISTS (
           SELECT 1
           FROM telegram_outbox
           WHERE state NOT IN ('delivered', 'dead_letter', 'expired', 'cancelled')
              OR (protected AND state = 'dead_letter')
       ) THEN
        RAISE EXCEPTION 'cannot remove Telegram outbox with unresolved rows';
    END IF;

    IF to_regclass('public.taskman_jobs') IS NOT NULL
       AND EXISTS (
           SELECT 1
           FROM taskman_jobs
           WHERE deleted_at IS NULL
             AND status NOT IN ('completed', 'failed', 'cancelled')
             AND (
                 debounce_key IS NOT NULL
                 OR lane_key IS NOT NULL
                 OR cardinality(source_update_ids) > 0
                 OR pending_dialog_inputs <> '[]'::jsonb
             )
       ) THEN
        RAISE EXCEPTION 'cannot remove durable taskman columns while jobs depend on them';
    END IF;

    IF to_regclass('public.dialog_turn_outcomes') IS NOT NULL
       AND EXISTS (
           SELECT 1
           FROM dialog_turn_outcomes
           WHERE delivery_state IN ('queued', 'partial', 'ambiguous')
       ) THEN
        RAISE EXCEPTION 'cannot remove dialog delivery fields with unresolved outcomes';
    END IF;
END
$$;

ALTER TABLE dialog_turn_outcomes
    DROP COLUMN IF EXISTS delivery_error,
    DROP COLUMN IF EXISTS delivery_error_class,
    DROP COLUMN IF EXISTS delivered_at,
    DROP COLUMN IF EXISTS telegram_message_ids,
    DROP COLUMN IF EXISTS outbox_operation_ids,
    DROP COLUMN IF EXISTS delivery_state;

ALTER TABLE taskman_jobs
    DROP COLUMN IF EXISTS pending_dialog_inputs,
    DROP COLUMN IF EXISTS latest_update_id,
    DROP COLUMN IF EXISTS source_update_ids,
    DROP COLUMN IF EXISTS lane_key,
    DROP COLUMN IF EXISTS debounce_key,
    DROP COLUMN IF EXISTS available_at;

DROP TABLE IF EXISTS telegram_outbox_attempts;
DROP TABLE IF EXISTS telegram_outbox;
DROP TABLE IF EXISTS telegram_outbox_blobs;
DROP TABLE IF EXISTS telegram_update_attempts;
DROP TABLE IF EXISTS telegram_update_quarantine;
DROP TABLE IF EXISTS telegram_update_inbox;
