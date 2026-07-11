-- One-time repair for lane heads that may have been cleared by the former
-- materializer/terminal-transition snapshot race. The inbox remains the source
-- of truth; this rebuild changes no update status or payload.
DELETE FROM telegram_update_lanes;

INSERT INTO telegram_update_lanes (bot_id, ordering_key, head_inbox_id)
SELECT DISTINCT ON (bot_id, ordering_key)
    bot_id,
    ordering_key,
    id
FROM telegram_update_inbox
WHERE status IN ('pending', 'processing', 'retry_wait')
ORDER BY bot_id, ordering_key, stream_ms, stream_seq, id;
