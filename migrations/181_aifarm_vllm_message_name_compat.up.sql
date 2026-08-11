UPDATE llm_providers
SET config = jsonb_set(
    config || jsonb_build_object(
        '_migration_181_previous_supports_message_name',
        config->'supports_message_name'
    ),
    '{supports_message_name}',
    'false'::jsonb,
    true
)
WHERE name = 'aifarm-vllm-gpu0'
  AND NOT config ? '_migration_181_previous_supports_message_name';
