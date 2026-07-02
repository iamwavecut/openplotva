-- Typed provider transport. `protocol` names the wire payload shape (how the
-- runtime talks to the endpoint); discovery resolution stays orthogonal
-- (presence of discovery_service_name). `runtime_hint` names the serving
-- engine behind the endpoint and only drives which parameter schemas the admin
-- UI offers. Both nullable: NULL rows are classified by the loader-side
-- derivation until the protocol backfill fills them in.
ALTER TABLE llm_providers
    ADD COLUMN protocol TEXT,
    ADD COLUMN runtime_hint TEXT;

ALTER TABLE llm_providers
    ADD CONSTRAINT llm_providers_protocol_check CHECK (protocol IS NULL OR protocol IN
        ('openai_compat', 'genkit', 'acestep', 'discovery_jobs', 'discovery_draw', 'privacy_filter')),
    ADD CONSTRAINT llm_providers_runtime_hint_check CHECK (runtime_hint IS NULL OR runtime_hint IN
        ('llama_cpp', 'vllm', 'sglang', 'ollama', 'tgi'));
