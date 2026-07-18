ALTER TABLE users
    ADD COLUMN telegram_observed_at TIMESTAMPTZ;

ALTER TABLE chats
    ADD COLUMN telegram_observed_at TIMESTAMPTZ;

ALTER TABLE chat_members
    ADD COLUMN telegram_observed_at TIMESTAMPTZ;

ALTER TABLE telegram_files
    ADD COLUMN telegram_observed_at TIMESTAMPTZ;

CREATE UNLOGGED TABLE telegram_users_stage (
    bot_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT,
    username TEXT,
    language_code TEXT,
    is_premium BOOLEAN,
    observed_at TIMESTAMPTZ NOT NULL,
    stream_ms BIGINT NOT NULL,
    stream_seq BIGINT NOT NULL,
    PRIMARY KEY (bot_id, user_id)
);

CREATE INDEX telegram_users_stage_observed_idx
    ON telegram_users_stage (observed_at);

CREATE INDEX telegram_users_stage_latest_idx
    ON telegram_users_stage (
        user_id,
        observed_at DESC,
        stream_ms DESC,
        stream_seq DESC,
        bot_id DESC
    );

CREATE UNLOGGED TABLE telegram_chats_stage (
    bot_id BIGINT NOT NULL,
    chat_id BIGINT NOT NULL,
    type TEXT NOT NULL,
    title TEXT,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    is_forum BOOLEAN,
    observed_at TIMESTAMPTZ NOT NULL,
    stream_ms BIGINT NOT NULL,
    stream_seq BIGINT NOT NULL,
    PRIMARY KEY (bot_id, chat_id)
);

CREATE INDEX telegram_chats_stage_observed_idx
    ON telegram_chats_stage (observed_at);

CREATE INDEX telegram_chats_stage_latest_idx
    ON telegram_chats_stage (
        chat_id,
        observed_at DESC,
        stream_ms DESC,
        stream_seq DESC,
        bot_id DESC
    );

CREATE UNLOGGED TABLE telegram_chat_members_stage (
    bot_id BIGINT NOT NULL,
    chat_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    status TEXT NOT NULL,
    is_member BOOLEAN,
    is_anonymous BOOLEAN,
    custom_title TEXT,
    can_be_edited BOOLEAN,
    can_manage_chat BOOLEAN,
    can_delete_messages BOOLEAN,
    can_manage_video_chats BOOLEAN,
    can_restrict_members BOOLEAN,
    can_promote_members BOOLEAN,
    can_change_info BOOLEAN,
    can_invite_users BOOLEAN,
    can_post_messages BOOLEAN,
    can_edit_messages BOOLEAN,
    can_pin_messages BOOLEAN,
    can_manage_topics BOOLEAN,
    can_send_messages BOOLEAN,
    can_send_media_messages BOOLEAN,
    can_send_polls BOOLEAN,
    can_send_other_messages BOOLEAN,
    can_add_web_page_previews BOOLEAN,
    until_date TIMESTAMPTZ,
    observed_at TIMESTAMPTZ NOT NULL,
    stream_ms BIGINT NOT NULL,
    stream_seq BIGINT NOT NULL,
    PRIMARY KEY (bot_id, chat_id, user_id)
);

CREATE INDEX telegram_chat_members_stage_observed_idx
    ON telegram_chat_members_stage (observed_at);

CREATE INDEX telegram_chat_members_stage_latest_idx
    ON telegram_chat_members_stage (
        chat_id,
        user_id,
        observed_at DESC,
        stream_ms DESC,
        stream_seq DESC,
        bot_id DESC
    );

CREATE UNLOGGED TABLE telegram_activity_stage (
    bot_id BIGINT NOT NULL,
    chat_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    last_message_at TIMESTAMPTZ,
    last_active_at TIMESTAMPTZ,
    observed_at TIMESTAMPTZ NOT NULL,
    stream_ms BIGINT NOT NULL,
    stream_seq BIGINT NOT NULL,
    PRIMARY KEY (bot_id, chat_id, user_id)
);

CREATE INDEX telegram_activity_stage_observed_idx
    ON telegram_activity_stage (observed_at);

CREATE INDEX telegram_activity_stage_latest_idx
    ON telegram_activity_stage (
        chat_id,
        user_id,
        observed_at DESC,
        stream_ms DESC,
        stream_seq DESC,
        bot_id DESC
    );

CREATE UNLOGGED TABLE telegram_files_stage (
    bot_id BIGINT NOT NULL,
    file_unique_id TEXT NOT NULL,
    latest_file_id TEXT NOT NULL,
    media_kind TEXT NOT NULL,
    mime_type TEXT,
    width INTEGER,
    height INTEGER,
    file_size BIGINT,
    first_seen_chat_id BIGINT,
    first_seen_message_id BIGINT,
    last_seen_chat_id BIGINT,
    last_seen_message_id BIGINT,
    observed_at TIMESTAMPTZ NOT NULL,
    stream_ms BIGINT NOT NULL,
    stream_seq BIGINT NOT NULL,
    PRIMARY KEY (bot_id, file_unique_id)
);

CREATE INDEX telegram_files_stage_observed_idx
    ON telegram_files_stage (observed_at);

CREATE INDEX telegram_files_stage_latest_idx
    ON telegram_files_stage (
        file_unique_id,
        observed_at DESC,
        stream_ms DESC,
        stream_seq DESC,
        bot_id DESC
    );

CREATE VIEW telegram_users_stage_latest AS
SELECT DISTINCT ON (user_id) *
FROM telegram_users_stage
ORDER BY user_id, observed_at DESC, stream_ms DESC, stream_seq DESC, bot_id DESC;

CREATE VIEW telegram_users_effective AS
SELECT
    durable.id,
    CASE
        WHEN staged.observed_at >= COALESCE(durable.telegram_observed_at, '-infinity')
            THEN COALESCE(staged.is_premium, durable.is_premium)
        ELSE durable.is_premium
    END
        AS is_premium,
    CASE
        WHEN staged.observed_at >= COALESCE(durable.telegram_observed_at, '-infinity')
            THEN COALESCE(staged.first_name, durable.first_name)
        ELSE durable.first_name
    END
        AS first_name,
    CASE
        WHEN staged.observed_at >= COALESCE(durable.telegram_observed_at, '-infinity')
            THEN COALESCE(staged.last_name, durable.last_name)
        ELSE durable.last_name
    END
        AS last_name,
    CASE
        WHEN staged.observed_at >= COALESCE(durable.telegram_observed_at, '-infinity')
            THEN COALESCE(staged.username, durable.username)
        ELSE durable.username
    END
        AS username,
    CASE
        WHEN staged.observed_at >= COALESCE(durable.telegram_observed_at, '-infinity')
            THEN COALESCE(staged.language_code, durable.language_code)
        ELSE durable.language_code
    END
        AS language_code,
    COALESCE(durable.is_vip, FALSE) AS is_vip,
    durable.settings,
    durable.discovered,
    CASE
        WHEN staged.observed_at >= COALESCE(durable.telegram_observed_at, '-infinity')
            THEN GREATEST(durable.updated, staged.observed_at)
        ELSE durable.updated
    END AS updated
FROM users AS durable
LEFT JOIN telegram_users_stage_latest AS staged
    ON staged.user_id = durable.id
UNION ALL
SELECT
    staged.user_id,
    COALESCE(staged.is_premium, FALSE),
    staged.first_name,
    staged.last_name,
    staged.username,
    staged.language_code,
    FALSE,
    NULL,
    staged.observed_at,
    staged.observed_at
FROM telegram_users_stage_latest AS staged
WHERE NOT EXISTS (
    SELECT 1
    FROM users AS durable
    WHERE durable.id = staged.user_id
);

CREATE VIEW telegram_chats_stage_latest AS
SELECT DISTINCT ON (chat_id) *
FROM telegram_chats_stage
ORDER BY chat_id, observed_at DESC, stream_ms DESC, stream_seq DESC, bot_id DESC;

CREATE VIEW telegram_chats_effective AS
SELECT
    durable.id,
    CASE
        WHEN staged.observed_at >= COALESCE(durable.telegram_observed_at, '-infinity')
            THEN COALESCE(staged.type, durable.type)
        ELSE durable.type
    END AS type,
    CASE
        WHEN staged.observed_at >= COALESCE(durable.telegram_observed_at, '-infinity')
            THEN COALESCE(staged.title, durable.title)
        ELSE durable.title
    END AS title,
    CASE
        WHEN staged.observed_at >= COALESCE(durable.telegram_observed_at, '-infinity')
            THEN COALESCE(staged.username, durable.username)
        ELSE durable.username
    END AS username,
    CASE
        WHEN staged.observed_at >= COALESCE(durable.telegram_observed_at, '-infinity')
            THEN COALESCE(staged.first_name, durable.first_name)
        ELSE durable.first_name
    END
        AS first_name,
    CASE
        WHEN staged.observed_at >= COALESCE(durable.telegram_observed_at, '-infinity')
            THEN COALESCE(staged.last_name, durable.last_name)
        ELSE durable.last_name
    END AS last_name,
    CASE
        WHEN staged.observed_at >= COALESCE(durable.telegram_observed_at, '-infinity')
            THEN COALESCE(staged.is_forum, durable.is_forum)
        ELSE durable.is_forum
    END AS is_forum,
    durable.active_usernames,
    durable.available_reactions,
    durable.bio,
    COALESCE(durable.has_private_forwards, FALSE) AS has_private_forwards,
    COALESCE(durable.has_restricted_voice_and_video_messages, FALSE)
        AS has_restricted_voice_and_video_messages,
    COALESCE(durable.join_to_send_messages, FALSE) AS join_to_send_messages,
    COALESCE(durable.join_by_request, FALSE) AS join_by_request,
    durable.description,
    durable.invite_link,
    durable.pinned_message,
    durable.permissions,
    durable.slow_mode_delay,
    durable.message_auto_delete_time,
    COALESCE(durable.has_aggressive_anti_spam_enabled, FALSE)
        AS has_aggressive_anti_spam_enabled,
    COALESCE(durable.has_hidden_members, FALSE) AS has_hidden_members,
    COALESCE(durable.has_protected_content, FALSE) AS has_protected_content,
    COALESCE(durable.has_visible_history, FALSE) AS has_visible_history,
    durable.sticker_set_name,
    COALESCE(durable.can_set_sticker_set, FALSE) AS can_set_sticker_set,
    durable.linked_chat_id,
    durable.location,
    durable.discovered,
    CASE
        WHEN staged.observed_at >= COALESCE(durable.telegram_observed_at, '-infinity')
            THEN GREATEST(durable.updated, staged.observed_at)
        ELSE durable.updated
    END AS updated
FROM chats AS durable
LEFT JOIN telegram_chats_stage_latest AS staged
    ON staged.chat_id = durable.id
UNION ALL
SELECT
    staged.chat_id,
    staged.type,
    staged.title,
    staged.username,
    staged.first_name,
    staged.last_name,
    staged.is_forum,
    NULL,
    NULL,
    NULL,
    FALSE,
    FALSE,
    FALSE,
    FALSE,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    FALSE,
    FALSE,
    FALSE,
    FALSE,
    NULL,
    FALSE,
    NULL,
    NULL,
    staged.observed_at,
    staged.observed_at
FROM telegram_chats_stage_latest AS staged
WHERE NOT EXISTS (
    SELECT 1
    FROM chats AS durable
    WHERE durable.id = staged.chat_id
);

CREATE VIEW telegram_chat_members_stage_latest AS
SELECT DISTINCT ON (chat_id, user_id) *
FROM telegram_chat_members_stage
ORDER BY
    chat_id,
    user_id,
    observed_at DESC,
    stream_ms DESC,
    stream_seq DESC,
    bot_id DESC;

CREATE VIEW telegram_activity_stage_latest AS
SELECT DISTINCT ON (chat_id, user_id) *
FROM telegram_activity_stage
ORDER BY
    chat_id,
    user_id,
    observed_at DESC,
    stream_ms DESC,
    stream_seq DESC,
    bot_id DESC;

CREATE VIEW telegram_chat_members_effective AS
WITH
combined AS (
    SELECT
        durable.*,
        staged.chat_id AS staged_chat_id,
        staged.user_id AS staged_user_id,
        staged.status AS staged_status,
        staged.is_member AS staged_is_member,
        staged.is_anonymous AS staged_is_anonymous,
        staged.custom_title AS staged_custom_title,
        staged.can_be_edited AS staged_can_be_edited,
        staged.can_manage_chat AS staged_can_manage_chat,
        staged.can_delete_messages AS staged_can_delete_messages,
        staged.can_manage_video_chats AS staged_can_manage_video_chats,
        staged.can_restrict_members AS staged_can_restrict_members,
        staged.can_promote_members AS staged_can_promote_members,
        staged.can_change_info AS staged_can_change_info,
        staged.can_invite_users AS staged_can_invite_users,
        staged.can_post_messages AS staged_can_post_messages,
        staged.can_edit_messages AS staged_can_edit_messages,
        staged.can_pin_messages AS staged_can_pin_messages,
        staged.can_manage_topics AS staged_can_manage_topics,
        staged.can_send_messages AS staged_can_send_messages,
        staged.can_send_media_messages AS staged_can_send_media_messages,
        staged.can_send_polls AS staged_can_send_polls,
        staged.can_send_other_messages AS staged_can_send_other_messages,
        staged.can_add_web_page_previews AS staged_can_add_web_page_previews,
        staged.until_date AS staged_until_date,
        staged.observed_at AS staged_observed_at,
        activity.last_message_at AS staged_last_message_at,
        durable.chat_id IS NULL
            OR staged.observed_at >= COALESCE(durable.telegram_observed_at, '-infinity')
            AS stage_wins
    FROM chat_members AS durable
    LEFT JOIN telegram_chat_members_stage_latest AS staged
        ON staged.chat_id = durable.chat_id
        AND staged.user_id = durable.user_id
    LEFT JOIN telegram_activity_stage_latest AS activity
        ON activity.chat_id = durable.chat_id
        AND activity.user_id = durable.user_id
)
SELECT
    chat_id,
    user_id,
    CASE WHEN stage_wins THEN COALESCE(staged_status, status) ELSE status END AS status,
    CASE WHEN stage_wins THEN COALESCE(staged_is_anonymous, is_anonymous) ELSE is_anonymous END
        AS is_anonymous,
    CASE WHEN stage_wins THEN COALESCE(staged_custom_title, custom_title) ELSE custom_title END
        AS custom_title,
    CASE WHEN stage_wins THEN COALESCE(staged_can_be_edited, can_be_edited) ELSE can_be_edited END
        AS can_be_edited,
    CASE WHEN stage_wins THEN COALESCE(staged_can_manage_chat, can_manage_chat) ELSE can_manage_chat END
        AS can_manage_chat,
    CASE
        WHEN stage_wins THEN COALESCE(staged_can_delete_messages, can_delete_messages)
        ELSE can_delete_messages
    END AS can_delete_messages,
    CASE
        WHEN stage_wins THEN COALESCE(staged_can_manage_video_chats, can_manage_video_chats)
        ELSE can_manage_video_chats
    END AS can_manage_video_chats,
    CASE
        WHEN stage_wins THEN COALESCE(staged_can_restrict_members, can_restrict_members)
        ELSE can_restrict_members
    END AS can_restrict_members,
    CASE
        WHEN stage_wins THEN COALESCE(staged_can_promote_members, can_promote_members)
        ELSE can_promote_members
    END AS can_promote_members,
    CASE WHEN stage_wins THEN COALESCE(staged_can_change_info, can_change_info) ELSE can_change_info END
        AS can_change_info,
    CASE WHEN stage_wins THEN COALESCE(staged_can_invite_users, can_invite_users) ELSE can_invite_users END
        AS can_invite_users,
    CASE WHEN stage_wins THEN COALESCE(staged_can_post_messages, can_post_messages) ELSE can_post_messages END
        AS can_post_messages,
    CASE WHEN stage_wins THEN COALESCE(staged_can_edit_messages, can_edit_messages) ELSE can_edit_messages END
        AS can_edit_messages,
    CASE WHEN stage_wins THEN COALESCE(staged_can_pin_messages, can_pin_messages) ELSE can_pin_messages END
        AS can_pin_messages,
    CASE WHEN stage_wins THEN COALESCE(staged_can_manage_topics, can_manage_topics) ELSE can_manage_topics END
        AS can_manage_topics,
    CASE WHEN stage_wins THEN COALESCE(staged_can_send_messages, can_send_messages) ELSE can_send_messages END
        AS can_send_messages,
    CASE
        WHEN stage_wins THEN COALESCE(staged_can_send_media_messages, can_send_media_messages)
        ELSE can_send_media_messages
    END AS can_send_media_messages,
    CASE WHEN stage_wins THEN COALESCE(staged_can_send_polls, can_send_polls) ELSE can_send_polls END
        AS can_send_polls,
    CASE
        WHEN stage_wins THEN COALESCE(staged_can_send_other_messages, can_send_other_messages)
        ELSE can_send_other_messages
    END AS can_send_other_messages,
    CASE
        WHEN stage_wins THEN COALESCE(staged_can_add_web_page_previews, can_add_web_page_previews)
        ELSE can_add_web_page_previews
    END AS can_add_web_page_previews,
    CASE WHEN stage_wins THEN COALESCE(staged_until_date, until_date) ELSE until_date END
        AS until_date,
    COALESCE(created_at, staged_observed_at) AS created_at,
    CASE
        WHEN stage_wins THEN GREATEST(COALESCE(updated_at, '-infinity'), staged_observed_at)
        ELSE updated_at
    END AS updated_at,
    GREATEST(last_message_at, staged_last_message_at) AS last_message_at,
    CASE WHEN stage_wins THEN COALESCE(staged_is_member, is_member) ELSE is_member END
        AS is_member
FROM combined
UNION ALL
SELECT
    staged.chat_id,
    staged.user_id,
    staged.status,
    staged.is_anonymous,
    staged.custom_title,
    staged.can_be_edited,
    staged.can_manage_chat,
    staged.can_delete_messages,
    staged.can_manage_video_chats,
    staged.can_restrict_members,
    staged.can_promote_members,
    staged.can_change_info,
    staged.can_invite_users,
    staged.can_post_messages,
    staged.can_edit_messages,
    staged.can_pin_messages,
    staged.can_manage_topics,
    staged.can_send_messages,
    staged.can_send_media_messages,
    staged.can_send_polls,
    staged.can_send_other_messages,
    staged.can_add_web_page_previews,
    staged.until_date,
    staged.observed_at,
    staged.observed_at,
    activity.last_message_at,
    staged.is_member
FROM telegram_chat_members_stage_latest AS staged
LEFT JOIN telegram_activity_stage_latest AS activity
    ON activity.chat_id = staged.chat_id
    AND activity.user_id = staged.user_id
WHERE NOT EXISTS (
    SELECT 1
    FROM chat_members AS durable
    WHERE durable.chat_id = staged.chat_id
      AND durable.user_id = staged.user_id
);

CREATE VIEW telegram_chat_active_users_effective AS
SELECT
    durable.chat_id,
    durable.user_id,
    GREATEST(durable.last_active_at, staged.last_active_at) AS last_active_at
FROM chat_active_users AS durable
LEFT JOIN telegram_activity_stage_latest AS staged
    ON staged.chat_id = durable.chat_id
    AND staged.user_id = durable.user_id
    AND staged.last_active_at IS NOT NULL
UNION ALL
SELECT
    staged.chat_id,
    staged.user_id,
    staged.last_active_at
FROM telegram_activity_stage_latest AS staged
WHERE staged.last_active_at IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM chat_active_users AS durable
      WHERE durable.chat_id = staged.chat_id
        AND durable.user_id = staged.user_id
  );

CREATE VIEW telegram_files_stage_latest AS
SELECT DISTINCT ON (file_unique_id) *
FROM telegram_files_stage
ORDER BY
    file_unique_id,
    observed_at DESC,
    stream_ms DESC,
    stream_seq DESC,
    bot_id DESC;

CREATE VIEW telegram_files_effective AS
WITH
combined AS (
    SELECT
        durable.*,
        staged.file_unique_id AS staged_file_unique_id,
        staged.latest_file_id AS staged_latest_file_id,
        staged.media_kind AS staged_media_kind,
        staged.mime_type AS staged_mime_type,
        staged.width AS staged_width,
        staged.height AS staged_height,
        staged.file_size AS staged_file_size,
        staged.first_seen_chat_id AS staged_first_seen_chat_id,
        staged.first_seen_message_id AS staged_first_seen_message_id,
        staged.last_seen_chat_id AS staged_last_seen_chat_id,
        staged.last_seen_message_id AS staged_last_seen_message_id,
        staged.observed_at AS staged_observed_at,
        durable.file_unique_id IS NULL
            OR staged.observed_at >= COALESCE(durable.telegram_observed_at, '-infinity')
            AS stage_wins
    FROM telegram_files AS durable
    LEFT JOIN telegram_files_stage_latest AS staged
        ON staged.file_unique_id = durable.file_unique_id
)
SELECT
    file_unique_id,
    CASE WHEN stage_wins THEN COALESCE(staged_latest_file_id, latest_file_id) ELSE latest_file_id END
        AS latest_file_id,
    CASE WHEN stage_wins THEN COALESCE(staged_media_kind, media_kind) ELSE media_kind END
        AS media_kind,
    CASE WHEN stage_wins THEN COALESCE(staged_mime_type, mime_type) ELSE mime_type END AS mime_type,
    CASE WHEN stage_wins THEN COALESCE(staged_width, width) ELSE width END AS width,
    CASE WHEN stage_wins THEN COALESCE(staged_height, height) ELSE height END AS height,
    CASE WHEN stage_wins THEN COALESCE(staged_file_size, file_size) ELSE file_size END AS file_size,
    COALESCE(first_seen_chat_id, staged_first_seen_chat_id) AS first_seen_chat_id,
    COALESCE(first_seen_message_id, staged_first_seen_message_id) AS first_seen_message_id,
    CASE
        WHEN stage_wins THEN COALESCE(staged_last_seen_chat_id, last_seen_chat_id)
        ELSE last_seen_chat_id
    END AS last_seen_chat_id,
    CASE
        WHEN stage_wins THEN COALESCE(staged_last_seen_message_id, last_seen_message_id)
        ELSE last_seen_message_id
    END AS last_seen_message_id,
    GREATEST(last_seen_at, staged_observed_at) AS last_seen_at,
    COALESCE(vision_status, 'pending') AS vision_status,
    vision_caption,
    vision_model,
    vision_latency_ms,
    recognition_requested_at,
    recognition_completed_at,
    COALESCE(extra, '{}'::jsonb) AS extra,
    COALESCE(created_at, staged_observed_at) AS created_at,
    CASE
        WHEN stage_wins THEN GREATEST(COALESCE(updated_at, '-infinity'), staged_observed_at)
        ELSE updated_at
    END AS updated_at,
    COALESCE(asr_status, 'pending') AS asr_status,
    asr_text,
    asr_provider,
    asr_model,
    asr_latency_ms,
    asr_error,
    asr_requested_at,
    asr_completed_at,
    asr_fallback_used,
    asr_chunks,
    asr_warnings
FROM combined
UNION ALL
SELECT
    staged.file_unique_id,
    staged.latest_file_id,
    staged.media_kind,
    staged.mime_type,
    staged.width,
    staged.height,
    staged.file_size,
    staged.first_seen_chat_id,
    staged.first_seen_message_id,
    staged.last_seen_chat_id,
    staged.last_seen_message_id,
    staged.observed_at,
    'pending',
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    '{}'::jsonb,
    staged.observed_at,
    staged.observed_at,
    'pending',
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
FROM telegram_files_stage_latest AS staged
WHERE NOT EXISTS (
    SELECT 1
    FROM telegram_files AS durable
    WHERE durable.file_unique_id = staged.file_unique_id
);
