-- The dropped taskman/job rollup rows are not restorable (the writer that
-- produced them was removed with this change); only the index is reversible.
DROP INDEX IF EXISTS idx_taskman_jobs_terminal_time;
