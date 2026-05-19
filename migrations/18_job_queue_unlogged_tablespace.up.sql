-- Source SHA-256: 5b1fb8aa06a98e3f52555fbab9c1fe2028880fd98443539ae807ff283f1f70e9

-- SQLx compatibility no-op.
--
-- same artifacts. On fresh Postgres, applying the original body fails because
-- job_queue references logged job_messages while being changed to UNLOGGED.
-- 19 while allowing the full migration corpus to apply on scratch databases.
