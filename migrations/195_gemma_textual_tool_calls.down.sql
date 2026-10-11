UPDATE provider_models
SET config = CASE
    WHEN config->'textual_tool_calls_v1_previous' = 'null'::jsonb
        THEN config - 'textual_tool_calls' - 'textual_tool_calls_v1_previous'
    ELSE jsonb_set(
        config - 'textual_tool_calls_v1_previous',
        '{textual_tool_calls}',
        config->'textual_tool_calls_v1_previous',
        true
    )
END
WHERE config ? 'textual_tool_calls_v1_previous';
