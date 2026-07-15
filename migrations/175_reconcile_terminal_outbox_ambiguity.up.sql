-- A deterministic Telegram 4xx means the create request was rejected, so it
-- cannot have produced a duplicate message. Older workers conservatively
-- classified every started create failure as ambiguous; reclassify only the
-- two deterministic terminal classes and leave terminal_other untouched.
WITH reclassified AS (
    UPDATE telegram_outbox
    SET state = 'dead_letter',
        lease_owner = NULL,
        leased_until = NULL,
        updated_at = statement_timestamp()
    WHERE state = 'ambiguous'
      AND last_error_class IN ('terminal_permission', 'terminal_bad_request')
    RETURNING id
)
UPDATE telegram_outbox_attempts AS attempt
SET outcome = 'dead_letter',
    finished_at = COALESCE(attempt.finished_at, statement_timestamp())
FROM reclassified
WHERE attempt.outbox_id = reclassified.id
  AND attempt.outcome = 'ambiguous';

-- These outcomes were already reconciled to ambiguous and are outside the
-- worker's one-day queued scan. Roll them forward when no unresolved operation
-- remains for the dialog job.
WITH resolved AS (
    SELECT outcome.id,
           bool_or(operation.state = 'delivered') AS any_delivered,
           string_agg(DISTINCT operation.state, ',' ORDER BY operation.state) AS states
    FROM dialog_turn_outcomes AS outcome
    JOIN telegram_outbox AS operation ON operation.dialog_job_id = outcome.job_id
    WHERE outcome.outcome = 'queued_for_delivery'
      AND outcome.delivery_state = 'ambiguous'
      AND EXISTS (
          SELECT 1
          FROM telegram_outbox AS reclassified
          WHERE reclassified.dialog_job_id = outcome.job_id
            AND reclassified.state = 'dead_letter'
            AND reclassified.last_error_class IN (
                'terminal_permission',
                'terminal_bad_request'
            )
      )
      AND NOT EXISTS (
          SELECT 1
          FROM telegram_outbox AS unresolved
          WHERE unresolved.dialog_job_id = outcome.job_id
            AND (
                unresolved.state IN ('pending', 'leased', 'retry_wait', 'ambiguous')
                OR unresolved.last_error_class = 'history_pending'
            )
      )
    GROUP BY outcome.id
)
UPDATE dialog_turn_outcomes AS outcome
SET delivery_state = CASE
        WHEN resolved.any_delivered THEN 'partial'
        ELSE 'dead_letter'
    END,
    delivery_error_class = CASE
        WHEN resolved.any_delivered THEN 'partial'
        ELSE 'dead_letter'
    END,
    delivery_error = 'Telegram outbox reached terminal state(s): ' || resolved.states
FROM resolved
WHERE outcome.id = resolved.id;
