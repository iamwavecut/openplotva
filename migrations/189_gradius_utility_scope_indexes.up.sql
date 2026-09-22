-- no-transaction
CREATE INDEX CONCURRENTLY gradius_utility_image_scope_idx
    ON gradius_ad_opportunities (user_id, shown_at DESC)
    WHERE integration_kind = 'native_utility' AND source_kind = 'image-job';
