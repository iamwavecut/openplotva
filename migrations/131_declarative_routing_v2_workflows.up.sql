INSERT INTO workflows (key, kind, full_routing, retry_max_hops, retry_wall_ms, enabled)
VALUES ('image_edit', 'image', TRUE, 3, 60000, TRUE)
ON CONFLICT (key) DO NOTHING;
