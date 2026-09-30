-- no-transaction
-- Adds an index only; compatible with existing readers and advertisement records.
CREATE INDEX CONCURRENTLY gradius_public_image_group_idx
    ON gradius_ad_opportunities (chat_id, shown_at DESC)
    WHERE integration_kind IN ('native_utility', 'native_generation')
      AND source_kind = 'image-job'
      AND source_context->>'image_ad_visibility' = 'public';
