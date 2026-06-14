-- Durable high-water source for the in-memory ID allocator. The allocator hands
-- out ids in memory; on startup it is seeded from these sequences, and every WAL
-- flush advances them past the highest persisted id. Because a sequence is never
-- lowered by row deletion or purge, the high-water cannot be lost across restarts.
CREATE SEQUENCE taskman_job_id_seq AS bigint;
CREATE SEQUENCE taskman_message_id_seq AS bigint;

-- The full taskman record lives in `record` (JSONB); the typed columns are bound
-- explicitly from the record's typed Rust fields at upsert time (see
-- upsert_task_queue_record). Binding in Rust keeps the SQL projection compile-time
-- coupled to the record struct: a field rename breaks the build rather than silently
-- producing wrong/NULL columns, and timestamps (which time's serde encodes as a
-- component array, not an ISO string) project cleanly via sqlx's native encoding.
CREATE TABLE taskman_jobs (
    id           BIGINT PRIMARY KEY,
    record       JSONB NOT NULL,
    deleted_at   TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    queue_name   TEXT NOT NULL,
    status       TEXT NOT NULL,
    job_type     TEXT NOT NULL,
    priority     INTEGER NOT NULL,
    chat_id      BIGINT,
    user_id      BIGINT,
    created_at   TIMESTAMPTZ NOT NULL,
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE INDEX taskman_jobs_active_order_idx
    ON taskman_jobs (queue_name, status, priority DESC, created_at ASC, id ASC)
    WHERE deleted_at IS NULL;

CREATE INDEX taskman_jobs_chat_user_idx
    ON taskman_jobs (chat_id, user_id, status)
    WHERE deleted_at IS NULL;

-- Backs the retention purge of soft-deleted rows.
CREATE INDEX taskman_jobs_purge_idx
    ON taskman_jobs (deleted_at)
    WHERE deleted_at IS NOT NULL;

CREATE TABLE taskman_job_history (
    id     BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL,
    op     TEXT NOT NULL,
    at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    record JSONB
);

-- Backs the retention purge of old history rows.
CREATE INDEX taskman_job_history_at_idx
    ON taskman_job_history (at);

CREATE INDEX taskman_job_history_job_id_idx
    ON taskman_job_history (job_id, at DESC);
