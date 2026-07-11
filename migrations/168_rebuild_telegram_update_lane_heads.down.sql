-- This is a one-way derived-state repair. Reverting the data change would
-- intentionally recreate invalid lane heads, so the down migration is a no-op.
SELECT 1;
