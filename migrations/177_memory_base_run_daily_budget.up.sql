-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS memory_runs_base_created_idx
    ON memory_runs (prompt_version, created_at)
    WHERE cursor_after_at = '1970-01-01 00:00:00+00'::timestamptz
      AND cursor_after_message_id = 0
      AND cursor_after_entry_id = '';
