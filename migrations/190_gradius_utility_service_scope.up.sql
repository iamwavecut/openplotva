-- no-transaction
CREATE INDEX CONCURRENTLY gradius_utility_service_scope_idx
    ON gradius_ad_opportunities (chat_id, shown_at DESC)
    WHERE integration_kind = 'native_utility' AND source_kind IN ('rates-command', 'checkin-final');
