ALTER TABLE whitecircle_checks
    ALTER COLUMN request_messages SET COMPRESSION pglz,
    ALTER COLUMN policies SET COMPRESSION pglz,
    ALTER COLUMN response_json SET COMPRESSION pglz;
