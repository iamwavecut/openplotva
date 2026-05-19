-- Source SHA-256: d06bcb4770b6249ba9bd8f202d238f996506cd569c03d6f1f518ccdd2ae1e47c

ALTER TABLE memory_runs
    DROP COLUMN IF EXISTS error_log;
