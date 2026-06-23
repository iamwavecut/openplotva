-- The configurable app functions the admin assigns models to. The key set is
-- static (every LLM/VLM/embedding/media touchpoint in the app), so it is seeded
-- here without reading env. `full_routing` distinguishes the two control-plane
-- treatments: chat/vision/image get weights + triggers + fallback chains;
-- embedding/music/redaction are config-only (primary + ordered fallback, the
-- router ignores weights/triggers for them). `redaction` is a non-LLM
-- privacy-filter and is a dispatcher no-op, kept here for registry completeness.
CREATE TABLE workflows (
    key            TEXT PRIMARY KEY,
    kind           TEXT NOT NULL,
    full_routing   BOOLEAN NOT NULL,
    retry_max_hops INTEGER NOT NULL DEFAULT 3,
    retry_wall_ms  INTEGER NOT NULL DEFAULT 60000,
    enabled        BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT workflows_kind_check
        CHECK (kind IN ('chat', 'vision', 'embedding', 'image', 'music', 'privacy_filter'))
);

INSERT INTO workflows (key, kind, full_routing) VALUES
    ('dialog',                  'chat',           TRUE),
    ('vision',                  'vision',         TRUE),
    ('image_generation',        'image',          TRUE),
    ('memory_consolidation',    'chat',           FALSE),
    ('history_summary',         'chat',           FALSE),
    ('embedding',               'embedding',      FALSE),
    ('agentic_search_reasoner', 'chat',           FALSE),
    ('agentic_search_writer',   'chat',           FALSE),
    ('agentic_song',            'chat',           FALSE),
    ('agentic_image',           'chat',           FALSE),
    ('media_prompt_optimizer',  'chat',           FALSE),
    ('redaction',               'privacy_filter', FALSE),
    ('music',                   'music',          FALSE);
