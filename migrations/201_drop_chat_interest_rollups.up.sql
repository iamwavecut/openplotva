-- Retire the chat_history/interest telemetry rollup. Nothing reads it: dialog
-- memory context comes from memory_episodes directly. It re-aggregated every
-- memory episode on each analytics pass, so it held most of telemetry_rollups
-- and rewrote it daily. Keep the other rollups and rebuild the table without the
-- interest rows; TRUNCATE frees the space at once instead of leaving dead tuples
-- for vacuum.
CREATE TEMP TABLE kept_telemetry_rollups ON COMMIT DROP AS
SELECT *
FROM telemetry_rollups
WHERE NOT (source = 'chat_history' AND kind = 'interest');

TRUNCATE telemetry_rollups;

INSERT INTO telemetry_rollups
SELECT *
FROM kept_telemetry_rollups;
