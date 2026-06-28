CREATE TABLE IF NOT EXISTS message_id_map (
    vmsg_id VARCHAR(32) PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    thread_id INTEGER NULL,
    real_message_id INTEGER NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mim_chat_real
ON message_id_map (chat_id, real_message_id)
WHERE real_message_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS message_ops_queue (
    id BIGSERIAL PRIMARY KEY,
    vmsg_id VARCHAR(32) NOT NULL REFERENCES message_id_map(vmsg_id) ON DELETE CASCADE,
    chat_id BIGINT NOT NULL,
    op VARCHAR(16) NOT NULL,
    payload JSONB NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    executed_at TIMESTAMPTZ NULL,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_moq_vmsg_status ON message_ops_queue (vmsg_id, status);
CREATE INDEX IF NOT EXISTS idx_moq_status_created ON message_ops_queue (status, created_at);
