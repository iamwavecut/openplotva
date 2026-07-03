-- Correlate low-level LLM calls with the agent run (dialog session,
-- song/image optimizer run, console turn) that issued them.
ALTER TABLE llm_request_events
    ADD COLUMN IF NOT EXISTS run_id TEXT,
    ADD COLUMN IF NOT EXISTS run_seq INTEGER;

CREATE INDEX IF NOT EXISTS idx_llm_request_events_run_id
    ON llm_request_events (run_id)
    WHERE run_id IS NOT NULL;
