-- LZ4 compression for the large archival JSONB columns. Affects future inserts
-- and rows rewritten by a later pg_repack; existing rows are unchanged until then.
ALTER TABLE whitecircle_checks
    ALTER COLUMN request_messages SET COMPRESSION lz4,
    ALTER COLUMN policies SET COMPRESSION lz4,
    ALTER COLUMN response_json SET COMPRESSION lz4;
