ALTER TABLE gradius_ad_opportunities
    ADD COLUMN source_context JSONB;

ALTER TABLE gradius_ad_opportunities
    ADD CONSTRAINT gradius_source_context_object_check
    CHECK (source_context IS NULL OR jsonb_typeof(source_context) = 'object');

ALTER TABLE gradius_ad_opportunities
    ALTER COLUMN source_context SET COMPRESSION lz4;
