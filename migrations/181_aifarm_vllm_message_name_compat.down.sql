UPDATE llm_providers
SET config = CASE
    WHEN config->'_migration_181_previous_supports_message_name' = 'null'::jsonb
        THEN config
            - 'supports_message_name'
            - '_migration_181_previous_supports_message_name'
    ELSE jsonb_set(
        config - '_migration_181_previous_supports_message_name',
        '{supports_message_name}',
        config->'_migration_181_previous_supports_message_name',
        true
    )
END
WHERE name = 'aifarm-vllm-gpu0'
  AND config ? '_migration_181_previous_supports_message_name';
