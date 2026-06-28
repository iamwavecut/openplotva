-- Retire the virtual-message async-state persistence. The deferred edit/delete
-- queue (message_ops_queue) processed zero operations and the vmsg->real map
-- (message_id_map) is never consumed; outbound rate limiting and the dispatcher
-- queue live in openplotva-telegram (Redis), independent of these tables. The
-- storage methods that touched these tables are no-ops; dropping is safe.
-- FK message_ops_queue.vmsg_id -> message_id_map is ON DELETE CASCADE, so order
-- only matters for the explicit drops below.
DROP TABLE IF EXISTS message_ops_queue;
DROP TABLE IF EXISTS message_id_map;
