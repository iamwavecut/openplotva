-- Source context is audit data and cannot be reconstructed after rollback.
ALTER TABLE gradius_ad_opportunities
    DROP CONSTRAINT gradius_source_context_object_check,
    DROP COLUMN source_context;
