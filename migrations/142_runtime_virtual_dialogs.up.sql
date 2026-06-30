-- Source SHA-256: dd259b35af3e3f6f2bd630ed667b20460098a229e93ee12b4d51f6172177d0fe
-- Source: runtime-only Rust schema addition for runtime virtual dialogs; no frozen Go migration source.

CREATE SEQUENCE IF NOT EXISTS runtime_virtual_dialog_id_seq AS bigint START WITH 1;

CREATE TABLE IF NOT EXISTS runtime_virtual_dialogs (
    session_id TEXT PRIMARY KEY,
    chat_id BIGINT NOT NULL UNIQUE,
    user_id BIGINT NOT NULL UNIQUE,
    next_message_id INTEGER NOT NULL DEFAULT 1,
    last_activity_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT runtime_virtual_dialogs_session_id_nonempty CHECK (btrim(session_id) <> ''),
    CONSTRAINT runtime_virtual_dialogs_session_id_length CHECK (char_length(session_id) <= 128),
    CONSTRAINT runtime_virtual_dialogs_next_message_id_positive CHECK (next_message_id > 0)
);

CREATE INDEX IF NOT EXISTS idx_runtime_virtual_dialogs_expires_at
    ON runtime_virtual_dialogs (expires_at)
    WHERE deleted_at IS NULL;
