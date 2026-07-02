INSERT INTO workflows (key, kind, full_routing, retry_max_hops, retry_wall_ms, enabled)
VALUES
    ('image_generation_flux', 'image', TRUE, 3, 60000, TRUE),
    ('image_generation_boogu_turbo', 'image', TRUE, 3, 60000, TRUE),
    ('image_edit_flux', 'image', TRUE, 3, 60000, TRUE),
    ('image_edit_boogu_turbo', 'image', TRUE, 3, 60000, TRUE)
ON CONFLICT (key) DO NOTHING;
