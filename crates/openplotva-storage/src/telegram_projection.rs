//! Bulk Telegram state projections backed by UNLOGGED coalescing tables.

use openplotva_core::{ChatState, UserState};
use sqlx::{PgPool, Postgres, Transaction};
use time::OffsetDateTime;

use crate::{ChatMemberUpsert, StorageError, TelegramFileMetadataUpsert};

const SQL_STAGE_USERS: &str = r#"
INSERT INTO telegram_users_stage (
    bot_id, user_id, first_name, last_name, username, language_code, is_premium,
    observed_at, stream_ms, stream_seq
)
SELECT *
FROM unnest(
    $1::bigint[], $2::bigint[], $3::text[], $4::text[], $5::text[],
    $6::text[], $7::boolean[], $8::timestamptz[], $9::bigint[], $10::bigint[]
) AS input(
    bot_id, user_id, first_name, last_name, username, language_code, is_premium,
    observed_at, stream_ms, stream_seq
)
ON CONFLICT (bot_id, user_id) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = COALESCE(EXCLUDED.last_name, telegram_users_stage.last_name),
    username = COALESCE(EXCLUDED.username, telegram_users_stage.username),
    language_code = COALESCE(EXCLUDED.language_code, telegram_users_stage.language_code),
    is_premium = COALESCE(EXCLUDED.is_premium, telegram_users_stage.is_premium),
    observed_at = EXCLUDED.observed_at,
    stream_ms = EXCLUDED.stream_ms,
    stream_seq = EXCLUDED.stream_seq
WHERE (EXCLUDED.stream_ms, EXCLUDED.stream_seq)
    > (telegram_users_stage.stream_ms, telegram_users_stage.stream_seq)
"#;

const SQL_STAGE_CHATS: &str = r#"
INSERT INTO telegram_chats_stage (
    bot_id, chat_id, type, title, username, first_name, last_name, is_forum,
    observed_at, stream_ms, stream_seq
)
SELECT *
FROM unnest(
    $1::bigint[], $2::bigint[], $3::text[], $4::text[], $5::text[],
    $6::text[], $7::text[], $8::boolean[], $9::timestamptz[],
    $10::bigint[], $11::bigint[]
) AS input(
    bot_id, chat_id, type, title, username, first_name, last_name, is_forum,
    observed_at, stream_ms, stream_seq
)
ON CONFLICT (bot_id, chat_id) DO UPDATE SET
    type = EXCLUDED.type,
    title = COALESCE(EXCLUDED.title, telegram_chats_stage.title),
    username = COALESCE(EXCLUDED.username, telegram_chats_stage.username),
    first_name = COALESCE(EXCLUDED.first_name, telegram_chats_stage.first_name),
    last_name = COALESCE(EXCLUDED.last_name, telegram_chats_stage.last_name),
    is_forum = COALESCE(EXCLUDED.is_forum, telegram_chats_stage.is_forum),
    observed_at = EXCLUDED.observed_at,
    stream_ms = EXCLUDED.stream_ms,
    stream_seq = EXCLUDED.stream_seq
WHERE (EXCLUDED.stream_ms, EXCLUDED.stream_seq)
    > (telegram_chats_stage.stream_ms, telegram_chats_stage.stream_seq)
"#;

const SQL_STAGE_MEMBERS: &str = r#"
INSERT INTO telegram_chat_members_stage (
    bot_id, chat_id, user_id, status, is_member, is_anonymous, custom_title,
    can_be_edited, can_manage_chat, can_delete_messages, can_manage_video_chats,
    can_restrict_members, can_promote_members, can_change_info, can_invite_users,
    can_post_messages, can_edit_messages, can_pin_messages, can_manage_topics,
    can_send_messages, can_send_media_messages, can_send_polls,
    can_send_other_messages, can_add_web_page_previews, until_date,
    observed_at, stream_ms, stream_seq
)
SELECT *
FROM unnest(
    $1::bigint[], $2::bigint[], $3::bigint[], $4::text[], $5::boolean[],
    $6::boolean[], $7::text[], $8::boolean[], $9::boolean[], $10::boolean[],
    $11::boolean[], $12::boolean[], $13::boolean[], $14::boolean[],
    $15::boolean[], $16::boolean[], $17::boolean[], $18::boolean[],
    $19::boolean[], $20::boolean[], $21::boolean[], $22::boolean[],
    $23::boolean[], $24::boolean[], $25::timestamptz[], $26::timestamptz[],
    $27::bigint[], $28::bigint[]
) AS input(
    bot_id, chat_id, user_id, status, is_member, is_anonymous, custom_title,
    can_be_edited, can_manage_chat, can_delete_messages, can_manage_video_chats,
    can_restrict_members, can_promote_members, can_change_info, can_invite_users,
    can_post_messages, can_edit_messages, can_pin_messages, can_manage_topics,
    can_send_messages, can_send_media_messages, can_send_polls,
    can_send_other_messages, can_add_web_page_previews, until_date,
    observed_at, stream_ms, stream_seq
)
ON CONFLICT (bot_id, chat_id, user_id) DO UPDATE SET
    status = EXCLUDED.status,
    is_member = COALESCE(EXCLUDED.is_member, telegram_chat_members_stage.is_member),
    is_anonymous = COALESCE(EXCLUDED.is_anonymous, telegram_chat_members_stage.is_anonymous),
    custom_title = COALESCE(EXCLUDED.custom_title, telegram_chat_members_stage.custom_title),
    can_be_edited = COALESCE(EXCLUDED.can_be_edited, telegram_chat_members_stage.can_be_edited),
    can_manage_chat = COALESCE(EXCLUDED.can_manage_chat, telegram_chat_members_stage.can_manage_chat),
    can_delete_messages = COALESCE(
        EXCLUDED.can_delete_messages,
        telegram_chat_members_stage.can_delete_messages
    ),
    can_manage_video_chats = COALESCE(
        EXCLUDED.can_manage_video_chats,
        telegram_chat_members_stage.can_manage_video_chats
    ),
    can_restrict_members = COALESCE(
        EXCLUDED.can_restrict_members,
        telegram_chat_members_stage.can_restrict_members
    ),
    can_promote_members = COALESCE(
        EXCLUDED.can_promote_members,
        telegram_chat_members_stage.can_promote_members
    ),
    can_change_info = COALESCE(EXCLUDED.can_change_info, telegram_chat_members_stage.can_change_info),
    can_invite_users = COALESCE(
        EXCLUDED.can_invite_users,
        telegram_chat_members_stage.can_invite_users
    ),
    can_post_messages = COALESCE(
        EXCLUDED.can_post_messages,
        telegram_chat_members_stage.can_post_messages
    ),
    can_edit_messages = COALESCE(
        EXCLUDED.can_edit_messages,
        telegram_chat_members_stage.can_edit_messages
    ),
    can_pin_messages = COALESCE(
        EXCLUDED.can_pin_messages,
        telegram_chat_members_stage.can_pin_messages
    ),
    can_manage_topics = COALESCE(
        EXCLUDED.can_manage_topics,
        telegram_chat_members_stage.can_manage_topics
    ),
    can_send_messages = COALESCE(
        EXCLUDED.can_send_messages,
        telegram_chat_members_stage.can_send_messages
    ),
    can_send_media_messages = COALESCE(
        EXCLUDED.can_send_media_messages,
        telegram_chat_members_stage.can_send_media_messages
    ),
    can_send_polls = COALESCE(EXCLUDED.can_send_polls, telegram_chat_members_stage.can_send_polls),
    can_send_other_messages = COALESCE(
        EXCLUDED.can_send_other_messages,
        telegram_chat_members_stage.can_send_other_messages
    ),
    can_add_web_page_previews = COALESCE(
        EXCLUDED.can_add_web_page_previews,
        telegram_chat_members_stage.can_add_web_page_previews
    ),
    until_date = COALESCE(EXCLUDED.until_date, telegram_chat_members_stage.until_date),
    observed_at = EXCLUDED.observed_at,
    stream_ms = EXCLUDED.stream_ms,
    stream_seq = EXCLUDED.stream_seq
WHERE (EXCLUDED.stream_ms, EXCLUDED.stream_seq)
    > (telegram_chat_members_stage.stream_ms, telegram_chat_members_stage.stream_seq)
"#;

const SQL_STAGE_ACTIVITY: &str = r#"
INSERT INTO telegram_activity_stage (
    bot_id, chat_id, user_id, last_message_at, last_active_at,
    observed_at, stream_ms, stream_seq
)
SELECT *
FROM unnest(
    $1::bigint[], $2::bigint[], $3::bigint[], $4::timestamptz[],
    $5::timestamptz[], $6::timestamptz[], $7::bigint[], $8::bigint[]
) AS input(
    bot_id, chat_id, user_id, last_message_at, last_active_at,
    observed_at, stream_ms, stream_seq
)
ON CONFLICT (bot_id, chat_id, user_id) DO UPDATE SET
    last_message_at = GREATEST(
        telegram_activity_stage.last_message_at,
        EXCLUDED.last_message_at
    ),
    last_active_at = GREATEST(
        telegram_activity_stage.last_active_at,
        EXCLUDED.last_active_at
    ),
    observed_at = GREATEST(telegram_activity_stage.observed_at, EXCLUDED.observed_at),
    stream_ms = CASE
        WHEN (EXCLUDED.stream_ms, EXCLUDED.stream_seq)
            > (telegram_activity_stage.stream_ms, telegram_activity_stage.stream_seq)
        THEN EXCLUDED.stream_ms ELSE telegram_activity_stage.stream_ms
    END,
    stream_seq = CASE
        WHEN (EXCLUDED.stream_ms, EXCLUDED.stream_seq)
            > (telegram_activity_stage.stream_ms, telegram_activity_stage.stream_seq)
        THEN EXCLUDED.stream_seq ELSE telegram_activity_stage.stream_seq
    END
WHERE ROW(
    telegram_activity_stage.last_message_at,
    telegram_activity_stage.last_active_at
) IS DISTINCT FROM ROW(
    GREATEST(telegram_activity_stage.last_message_at, EXCLUDED.last_message_at),
    GREATEST(telegram_activity_stage.last_active_at, EXCLUDED.last_active_at)
)
"#;

const SQL_STAGE_FILES: &str = r#"
INSERT INTO telegram_files_stage (
    bot_id, file_unique_id, latest_file_id, media_kind, mime_type, width, height,
    file_size, first_seen_chat_id, first_seen_message_id, last_seen_chat_id,
    last_seen_message_id, observed_at, stream_ms, stream_seq
)
SELECT *
FROM unnest(
    $1::bigint[], $2::text[], $3::text[], $4::text[], $5::text[],
    $6::integer[], $7::integer[], $8::bigint[], $9::bigint[], $10::bigint[],
    $11::bigint[], $12::bigint[], $13::timestamptz[], $14::bigint[], $15::bigint[]
) AS input(
    bot_id, file_unique_id, latest_file_id, media_kind, mime_type, width, height,
    file_size, first_seen_chat_id, first_seen_message_id, last_seen_chat_id,
    last_seen_message_id, observed_at, stream_ms, stream_seq
)
ON CONFLICT (bot_id, file_unique_id) DO UPDATE SET
    latest_file_id = EXCLUDED.latest_file_id,
    media_kind = EXCLUDED.media_kind,
    mime_type = COALESCE(EXCLUDED.mime_type, telegram_files_stage.mime_type),
    width = COALESCE(EXCLUDED.width, telegram_files_stage.width),
    height = COALESCE(EXCLUDED.height, telegram_files_stage.height),
    file_size = COALESCE(EXCLUDED.file_size, telegram_files_stage.file_size),
    first_seen_chat_id = COALESCE(
        telegram_files_stage.first_seen_chat_id,
        EXCLUDED.first_seen_chat_id
    ),
    first_seen_message_id = COALESCE(
        telegram_files_stage.first_seen_message_id,
        EXCLUDED.first_seen_message_id
    ),
    last_seen_chat_id = EXCLUDED.last_seen_chat_id,
    last_seen_message_id = EXCLUDED.last_seen_message_id,
    observed_at = EXCLUDED.observed_at,
    stream_ms = EXCLUDED.stream_ms,
    stream_seq = EXCLUDED.stream_seq
WHERE (EXCLUDED.stream_ms, EXCLUDED.stream_seq)
    > (telegram_files_stage.stream_ms, telegram_files_stage.stream_seq)
"#;

const SQL_APPLY_USERS_BATCH: &str = r#"
INSERT INTO users (
    id, first_name, last_name, username, language_code, is_premium,
    telegram_observed_at
)
SELECT
    user_id, first_name, last_name, username, language_code, is_premium,
    observed_at
FROM unnest(
    $1::bigint[], $2::bigint[], $3::text[], $4::text[], $5::text[],
    $6::text[], $7::boolean[], $8::timestamptz[], $9::bigint[], $10::bigint[]
) AS input(
    bot_id, user_id, first_name, last_name, username, language_code, is_premium,
    observed_at, stream_ms, stream_seq
)
ON CONFLICT (id) DO UPDATE SET
    first_name = COALESCE(EXCLUDED.first_name, users.first_name),
    last_name = COALESCE(EXCLUDED.last_name, users.last_name),
    username = COALESCE(EXCLUDED.username, users.username),
    language_code = COALESCE(EXCLUDED.language_code, users.language_code),
    is_premium = COALESCE(EXCLUDED.is_premium, users.is_premium),
    telegram_observed_at = EXCLUDED.telegram_observed_at,
    updated = CURRENT_TIMESTAMP
WHERE EXCLUDED.telegram_observed_at >= COALESCE(users.telegram_observed_at, '-infinity')
  AND ROW(
    users.first_name, users.last_name, users.username, users.language_code, users.is_premium
) IS DISTINCT FROM ROW(
    COALESCE(EXCLUDED.first_name, users.first_name),
    COALESCE(EXCLUDED.last_name, users.last_name),
    COALESCE(EXCLUDED.username, users.username),
    COALESCE(EXCLUDED.language_code, users.language_code),
    COALESCE(EXCLUDED.is_premium, users.is_premium)
)
"#;

const SQL_APPLY_CHATS_BATCH: &str = r#"
INSERT INTO chats (
    id, type, title, username, first_name, last_name, is_forum,
    telegram_observed_at
)
SELECT
    chat_id, type, title, username, first_name, last_name, is_forum,
    observed_at
FROM unnest(
    $1::bigint[], $2::bigint[], $3::text[], $4::text[], $5::text[],
    $6::text[], $7::text[], $8::boolean[], $9::timestamptz[],
    $10::bigint[], $11::bigint[]
) AS input(
    bot_id, chat_id, type, title, username, first_name, last_name, is_forum,
    observed_at, stream_ms, stream_seq
)
ON CONFLICT (id) DO UPDATE SET
    type = COALESCE(EXCLUDED.type, chats.type),
    title = COALESCE(EXCLUDED.title, chats.title),
    username = COALESCE(EXCLUDED.username, chats.username),
    first_name = COALESCE(EXCLUDED.first_name, chats.first_name),
    last_name = COALESCE(EXCLUDED.last_name, chats.last_name),
    is_forum = COALESCE(EXCLUDED.is_forum, chats.is_forum),
    telegram_observed_at = EXCLUDED.telegram_observed_at,
    updated = CURRENT_TIMESTAMP
WHERE EXCLUDED.telegram_observed_at >= COALESCE(chats.telegram_observed_at, '-infinity')
  AND ROW(
    chats.type, chats.title, chats.username, chats.first_name, chats.last_name, chats.is_forum
) IS DISTINCT FROM ROW(
    COALESCE(EXCLUDED.type, chats.type),
    COALESCE(EXCLUDED.title, chats.title),
    COALESCE(EXCLUDED.username, chats.username),
    COALESCE(EXCLUDED.first_name, chats.first_name),
    COALESCE(EXCLUDED.last_name, chats.last_name),
    COALESCE(EXCLUDED.is_forum, chats.is_forum)
)
"#;

const SQL_APPLY_MEMBERS_BATCH: &str = r#"
INSERT INTO chat_members (
    chat_id, user_id, status, is_member, is_anonymous, custom_title,
    can_be_edited, can_manage_chat, can_delete_messages, can_manage_video_chats,
    can_restrict_members, can_promote_members, can_change_info, can_invite_users,
    can_post_messages, can_edit_messages, can_pin_messages, can_manage_topics,
    can_send_messages, can_send_media_messages, can_send_polls,
    can_send_other_messages, can_add_web_page_previews, until_date,
    telegram_observed_at
)
SELECT
    chat_id, user_id, status, is_member, is_anonymous, custom_title,
    can_be_edited, can_manage_chat, can_delete_messages, can_manage_video_chats,
    can_restrict_members, can_promote_members, can_change_info, can_invite_users,
    can_post_messages, can_edit_messages, can_pin_messages, can_manage_topics,
    can_send_messages, can_send_media_messages, can_send_polls,
    can_send_other_messages, can_add_web_page_previews, until_date,
    observed_at
FROM unnest(
    $1::bigint[], $2::bigint[], $3::bigint[], $4::text[], $5::boolean[],
    $6::boolean[], $7::text[], $8::boolean[], $9::boolean[], $10::boolean[],
    $11::boolean[], $12::boolean[], $13::boolean[], $14::boolean[],
    $15::boolean[], $16::boolean[], $17::boolean[], $18::boolean[],
    $19::boolean[], $20::boolean[], $21::boolean[], $22::boolean[],
    $23::boolean[], $24::boolean[], $25::timestamptz[], $26::timestamptz[],
    $27::bigint[], $28::bigint[]
) AS input(
    bot_id, chat_id, user_id, status, is_member, is_anonymous, custom_title,
    can_be_edited, can_manage_chat, can_delete_messages, can_manage_video_chats,
    can_restrict_members, can_promote_members, can_change_info, can_invite_users,
    can_post_messages, can_edit_messages, can_pin_messages, can_manage_topics,
    can_send_messages, can_send_media_messages, can_send_polls,
    can_send_other_messages, can_add_web_page_previews, until_date,
    observed_at, stream_ms, stream_seq
)
ON CONFLICT (chat_id, user_id) DO UPDATE SET
    status = COALESCE(EXCLUDED.status, chat_members.status),
    is_member = COALESCE(EXCLUDED.is_member, chat_members.is_member),
    is_anonymous = COALESCE(EXCLUDED.is_anonymous, chat_members.is_anonymous),
    custom_title = COALESCE(EXCLUDED.custom_title, chat_members.custom_title),
    can_be_edited = COALESCE(EXCLUDED.can_be_edited, chat_members.can_be_edited),
    can_manage_chat = COALESCE(EXCLUDED.can_manage_chat, chat_members.can_manage_chat),
    can_delete_messages = COALESCE(EXCLUDED.can_delete_messages, chat_members.can_delete_messages),
    can_manage_video_chats = COALESCE(
        EXCLUDED.can_manage_video_chats,
        chat_members.can_manage_video_chats
    ),
    can_restrict_members = COALESCE(EXCLUDED.can_restrict_members, chat_members.can_restrict_members),
    can_promote_members = COALESCE(EXCLUDED.can_promote_members, chat_members.can_promote_members),
    can_change_info = COALESCE(EXCLUDED.can_change_info, chat_members.can_change_info),
    can_invite_users = COALESCE(EXCLUDED.can_invite_users, chat_members.can_invite_users),
    can_post_messages = COALESCE(EXCLUDED.can_post_messages, chat_members.can_post_messages),
    can_edit_messages = COALESCE(EXCLUDED.can_edit_messages, chat_members.can_edit_messages),
    can_pin_messages = COALESCE(EXCLUDED.can_pin_messages, chat_members.can_pin_messages),
    can_manage_topics = COALESCE(EXCLUDED.can_manage_topics, chat_members.can_manage_topics),
    can_send_messages = COALESCE(EXCLUDED.can_send_messages, chat_members.can_send_messages),
    can_send_media_messages = COALESCE(
        EXCLUDED.can_send_media_messages,
        chat_members.can_send_media_messages
    ),
    can_send_polls = COALESCE(EXCLUDED.can_send_polls, chat_members.can_send_polls),
    can_send_other_messages = COALESCE(
        EXCLUDED.can_send_other_messages,
        chat_members.can_send_other_messages
    ),
    can_add_web_page_previews = COALESCE(
        EXCLUDED.can_add_web_page_previews,
        chat_members.can_add_web_page_previews
    ),
    until_date = COALESCE(EXCLUDED.until_date, chat_members.until_date),
    telegram_observed_at = EXCLUDED.telegram_observed_at,
    updated_at = CURRENT_TIMESTAMP
WHERE EXCLUDED.telegram_observed_at
        >= COALESCE(chat_members.telegram_observed_at, '-infinity')
  AND ROW(
    chat_members.status, chat_members.is_member, chat_members.is_anonymous,
    chat_members.custom_title, chat_members.can_be_edited, chat_members.can_manage_chat,
    chat_members.can_delete_messages, chat_members.can_manage_video_chats,
    chat_members.can_restrict_members, chat_members.can_promote_members,
    chat_members.can_change_info, chat_members.can_invite_users,
    chat_members.can_post_messages, chat_members.can_edit_messages,
    chat_members.can_pin_messages, chat_members.can_manage_topics,
    chat_members.can_send_messages, chat_members.can_send_media_messages,
    chat_members.can_send_polls, chat_members.can_send_other_messages,
    chat_members.can_add_web_page_previews, chat_members.until_date
) IS DISTINCT FROM ROW(
    COALESCE(EXCLUDED.status, chat_members.status),
    COALESCE(EXCLUDED.is_member, chat_members.is_member),
    COALESCE(EXCLUDED.is_anonymous, chat_members.is_anonymous),
    COALESCE(EXCLUDED.custom_title, chat_members.custom_title),
    COALESCE(EXCLUDED.can_be_edited, chat_members.can_be_edited),
    COALESCE(EXCLUDED.can_manage_chat, chat_members.can_manage_chat),
    COALESCE(EXCLUDED.can_delete_messages, chat_members.can_delete_messages),
    COALESCE(EXCLUDED.can_manage_video_chats, chat_members.can_manage_video_chats),
    COALESCE(EXCLUDED.can_restrict_members, chat_members.can_restrict_members),
    COALESCE(EXCLUDED.can_promote_members, chat_members.can_promote_members),
    COALESCE(EXCLUDED.can_change_info, chat_members.can_change_info),
    COALESCE(EXCLUDED.can_invite_users, chat_members.can_invite_users),
    COALESCE(EXCLUDED.can_post_messages, chat_members.can_post_messages),
    COALESCE(EXCLUDED.can_edit_messages, chat_members.can_edit_messages),
    COALESCE(EXCLUDED.can_pin_messages, chat_members.can_pin_messages),
    COALESCE(EXCLUDED.can_manage_topics, chat_members.can_manage_topics),
    COALESCE(EXCLUDED.can_send_messages, chat_members.can_send_messages),
    COALESCE(EXCLUDED.can_send_media_messages, chat_members.can_send_media_messages),
    COALESCE(EXCLUDED.can_send_polls, chat_members.can_send_polls),
    COALESCE(EXCLUDED.can_send_other_messages, chat_members.can_send_other_messages),
    COALESCE(EXCLUDED.can_add_web_page_previews, chat_members.can_add_web_page_previews),
    COALESCE(EXCLUDED.until_date, chat_members.until_date)
)
"#;

const SQL_APPLY_ACTIVITY_BATCH: &str = r#"
WITH input AS MATERIALIZED (
    SELECT *
    FROM unnest(
        $1::bigint[], $2::bigint[], $3::bigint[], $4::timestamptz[],
        $5::timestamptz[], $6::timestamptz[], $7::bigint[], $8::bigint[]
    ) AS input(
        bot_id, chat_id, user_id, last_message_at, last_active_at,
        observed_at, stream_ms, stream_seq
    )
), member_activity AS (
    UPDATE chat_members AS member
    SET last_message_at = GREATEST(member.last_message_at, input.last_message_at),
        updated_at = CURRENT_TIMESTAMP
    FROM input
    WHERE input.last_message_at IS NOT NULL
      AND member.chat_id = input.chat_id
      AND member.user_id = input.user_id
      AND member.last_message_at IS DISTINCT FROM
          GREATEST(member.last_message_at, input.last_message_at)
    RETURNING member.chat_id
), active_users AS (
    INSERT INTO chat_active_users (chat_id, user_id, last_active_at)
    SELECT chat_id, user_id, last_active_at
    FROM input
    WHERE last_active_at IS NOT NULL
    ON CONFLICT (chat_id, user_id) DO UPDATE SET
        last_active_at = GREATEST(chat_active_users.last_active_at, EXCLUDED.last_active_at)
    WHERE chat_active_users.last_active_at IS DISTINCT FROM
        GREATEST(chat_active_users.last_active_at, EXCLUDED.last_active_at)
    RETURNING chat_id
)
SELECT
    (SELECT count(*) FROM member_activity)
    + (SELECT count(*) FROM active_users) AS affected
"#;

const SQL_APPLY_FILES_BATCH: &str = r#"
INSERT INTO telegram_files (
    file_unique_id, latest_file_id, media_kind, mime_type, width, height, file_size,
    first_seen_chat_id, first_seen_message_id, last_seen_chat_id, last_seen_message_id,
    last_seen_at, telegram_observed_at
)
SELECT
    file_unique_id, latest_file_id, media_kind, mime_type, width, height, file_size,
    first_seen_chat_id, first_seen_message_id, last_seen_chat_id, last_seen_message_id,
    observed_at, observed_at
FROM unnest(
    $1::bigint[], $2::text[], $3::text[], $4::text[], $5::text[],
    $6::integer[], $7::integer[], $8::bigint[], $9::bigint[], $10::bigint[],
    $11::bigint[], $12::bigint[], $13::timestamptz[], $14::bigint[], $15::bigint[]
) AS input(
    bot_id, file_unique_id, latest_file_id, media_kind, mime_type, width, height,
    file_size, first_seen_chat_id, first_seen_message_id, last_seen_chat_id,
    last_seen_message_id, observed_at, stream_ms, stream_seq
)
ON CONFLICT (file_unique_id) DO UPDATE SET
    latest_file_id = EXCLUDED.latest_file_id,
    media_kind = EXCLUDED.media_kind,
    mime_type = COALESCE(EXCLUDED.mime_type, telegram_files.mime_type),
    width = COALESCE(EXCLUDED.width, telegram_files.width),
    height = COALESCE(EXCLUDED.height, telegram_files.height),
    file_size = COALESCE(EXCLUDED.file_size, telegram_files.file_size),
    first_seen_chat_id = COALESCE(telegram_files.first_seen_chat_id, EXCLUDED.first_seen_chat_id),
    first_seen_message_id = COALESCE(
        telegram_files.first_seen_message_id,
        EXCLUDED.first_seen_message_id
    ),
    last_seen_chat_id = EXCLUDED.last_seen_chat_id,
    last_seen_message_id = EXCLUDED.last_seen_message_id,
    last_seen_at = GREATEST(telegram_files.last_seen_at, EXCLUDED.last_seen_at),
    telegram_observed_at = EXCLUDED.telegram_observed_at,
    updated_at = CURRENT_TIMESTAMP
WHERE EXCLUDED.telegram_observed_at
        >= COALESCE(telegram_files.telegram_observed_at, '-infinity')
  AND ROW(
    telegram_files.latest_file_id, telegram_files.media_kind, telegram_files.mime_type,
    telegram_files.width, telegram_files.height, telegram_files.file_size,
    telegram_files.last_seen_chat_id, telegram_files.last_seen_message_id,
    telegram_files.last_seen_at
) IS DISTINCT FROM ROW(
    EXCLUDED.latest_file_id,
    EXCLUDED.media_kind,
    COALESCE(EXCLUDED.mime_type, telegram_files.mime_type),
    COALESCE(EXCLUDED.width, telegram_files.width),
    COALESCE(EXCLUDED.height, telegram_files.height),
    COALESCE(EXCLUDED.file_size, telegram_files.file_size),
    EXCLUDED.last_seen_chat_id,
    EXCLUDED.last_seen_message_id,
    GREATEST(telegram_files.last_seen_at, EXCLUDED.last_seen_at)
)
"#;

const SQL_APPLY_USERS_FROM_STAGE: &str = r#"
INSERT INTO users (
    id, first_name, last_name, username, language_code, is_premium,
    telegram_observed_at
)
SELECT
    user_id, first_name, last_name, username, language_code, is_premium,
    observed_at
FROM telegram_users_stage
WHERE bot_id = $1
ON CONFLICT (id) DO UPDATE SET
    first_name = COALESCE(EXCLUDED.first_name, users.first_name),
    last_name = COALESCE(EXCLUDED.last_name, users.last_name),
    username = COALESCE(EXCLUDED.username, users.username),
    language_code = COALESCE(EXCLUDED.language_code, users.language_code),
    is_premium = COALESCE(EXCLUDED.is_premium, users.is_premium),
    telegram_observed_at = EXCLUDED.telegram_observed_at,
    updated = CURRENT_TIMESTAMP
WHERE EXCLUDED.telegram_observed_at >= COALESCE(users.telegram_observed_at, '-infinity')
  AND ROW(
    users.first_name, users.last_name, users.username, users.language_code, users.is_premium
) IS DISTINCT FROM ROW(
    COALESCE(EXCLUDED.first_name, users.first_name),
    COALESCE(EXCLUDED.last_name, users.last_name),
    COALESCE(EXCLUDED.username, users.username),
    COALESCE(EXCLUDED.language_code, users.language_code),
    COALESCE(EXCLUDED.is_premium, users.is_premium)
)
"#;

const SQL_APPLY_CHATS_FROM_STAGE: &str = r#"
INSERT INTO chats (
    id, type, title, username, first_name, last_name, is_forum,
    telegram_observed_at
)
SELECT
    chat_id, type, title, username, first_name, last_name, is_forum,
    observed_at
FROM telegram_chats_stage
WHERE bot_id = $1
ON CONFLICT (id) DO UPDATE SET
    type = COALESCE(EXCLUDED.type, chats.type),
    title = COALESCE(EXCLUDED.title, chats.title),
    username = COALESCE(EXCLUDED.username, chats.username),
    first_name = COALESCE(EXCLUDED.first_name, chats.first_name),
    last_name = COALESCE(EXCLUDED.last_name, chats.last_name),
    is_forum = COALESCE(EXCLUDED.is_forum, chats.is_forum),
    telegram_observed_at = EXCLUDED.telegram_observed_at,
    updated = CURRENT_TIMESTAMP
WHERE EXCLUDED.telegram_observed_at >= COALESCE(chats.telegram_observed_at, '-infinity')
  AND ROW(
    chats.type, chats.title, chats.username, chats.first_name, chats.last_name, chats.is_forum
) IS DISTINCT FROM ROW(
    COALESCE(EXCLUDED.type, chats.type),
    COALESCE(EXCLUDED.title, chats.title),
    COALESCE(EXCLUDED.username, chats.username),
    COALESCE(EXCLUDED.first_name, chats.first_name),
    COALESCE(EXCLUDED.last_name, chats.last_name),
    COALESCE(EXCLUDED.is_forum, chats.is_forum)
)
"#;

const SQL_APPLY_MEMBERS_FROM_STAGE: &str = r#"
INSERT INTO chat_members (
    chat_id, user_id, status, is_member, is_anonymous, custom_title,
    can_be_edited, can_manage_chat, can_delete_messages, can_manage_video_chats,
    can_restrict_members, can_promote_members, can_change_info, can_invite_users,
    can_post_messages, can_edit_messages, can_pin_messages, can_manage_topics,
    can_send_messages, can_send_media_messages, can_send_polls,
    can_send_other_messages, can_add_web_page_previews, until_date,
    telegram_observed_at
)
SELECT
    chat_id, user_id, status, is_member, is_anonymous, custom_title,
    can_be_edited, can_manage_chat, can_delete_messages, can_manage_video_chats,
    can_restrict_members, can_promote_members, can_change_info, can_invite_users,
    can_post_messages, can_edit_messages, can_pin_messages, can_manage_topics,
    can_send_messages, can_send_media_messages, can_send_polls,
    can_send_other_messages, can_add_web_page_previews, until_date,
    observed_at
FROM telegram_chat_members_stage
WHERE bot_id = $1
ON CONFLICT (chat_id, user_id) DO UPDATE SET
    status = COALESCE(EXCLUDED.status, chat_members.status),
    is_member = COALESCE(EXCLUDED.is_member, chat_members.is_member),
    is_anonymous = COALESCE(EXCLUDED.is_anonymous, chat_members.is_anonymous),
    custom_title = COALESCE(EXCLUDED.custom_title, chat_members.custom_title),
    can_be_edited = COALESCE(EXCLUDED.can_be_edited, chat_members.can_be_edited),
    can_manage_chat = COALESCE(EXCLUDED.can_manage_chat, chat_members.can_manage_chat),
    can_delete_messages = COALESCE(EXCLUDED.can_delete_messages, chat_members.can_delete_messages),
    can_manage_video_chats = COALESCE(
        EXCLUDED.can_manage_video_chats,
        chat_members.can_manage_video_chats
    ),
    can_restrict_members = COALESCE(EXCLUDED.can_restrict_members, chat_members.can_restrict_members),
    can_promote_members = COALESCE(EXCLUDED.can_promote_members, chat_members.can_promote_members),
    can_change_info = COALESCE(EXCLUDED.can_change_info, chat_members.can_change_info),
    can_invite_users = COALESCE(EXCLUDED.can_invite_users, chat_members.can_invite_users),
    can_post_messages = COALESCE(EXCLUDED.can_post_messages, chat_members.can_post_messages),
    can_edit_messages = COALESCE(EXCLUDED.can_edit_messages, chat_members.can_edit_messages),
    can_pin_messages = COALESCE(EXCLUDED.can_pin_messages, chat_members.can_pin_messages),
    can_manage_topics = COALESCE(EXCLUDED.can_manage_topics, chat_members.can_manage_topics),
    can_send_messages = COALESCE(EXCLUDED.can_send_messages, chat_members.can_send_messages),
    can_send_media_messages = COALESCE(
        EXCLUDED.can_send_media_messages,
        chat_members.can_send_media_messages
    ),
    can_send_polls = COALESCE(EXCLUDED.can_send_polls, chat_members.can_send_polls),
    can_send_other_messages = COALESCE(
        EXCLUDED.can_send_other_messages,
        chat_members.can_send_other_messages
    ),
    can_add_web_page_previews = COALESCE(
        EXCLUDED.can_add_web_page_previews,
        chat_members.can_add_web_page_previews
    ),
    until_date = COALESCE(EXCLUDED.until_date, chat_members.until_date),
    telegram_observed_at = EXCLUDED.telegram_observed_at,
    updated_at = CURRENT_TIMESTAMP
WHERE EXCLUDED.telegram_observed_at
        >= COALESCE(chat_members.telegram_observed_at, '-infinity')
  AND ROW(
    chat_members.status,
    chat_members.is_member,
    chat_members.is_anonymous,
    chat_members.custom_title,
    chat_members.can_be_edited,
    chat_members.can_manage_chat,
    chat_members.can_delete_messages,
    chat_members.can_manage_video_chats,
    chat_members.can_restrict_members,
    chat_members.can_promote_members,
    chat_members.can_change_info,
    chat_members.can_invite_users,
    chat_members.can_post_messages,
    chat_members.can_edit_messages,
    chat_members.can_pin_messages,
    chat_members.can_manage_topics,
    chat_members.can_send_messages,
    chat_members.can_send_media_messages,
    chat_members.can_send_polls,
    chat_members.can_send_other_messages,
    chat_members.can_add_web_page_previews,
    chat_members.until_date
) IS DISTINCT FROM ROW(
    COALESCE(EXCLUDED.status, chat_members.status),
    COALESCE(EXCLUDED.is_member, chat_members.is_member),
    COALESCE(EXCLUDED.is_anonymous, chat_members.is_anonymous),
    COALESCE(EXCLUDED.custom_title, chat_members.custom_title),
    COALESCE(EXCLUDED.can_be_edited, chat_members.can_be_edited),
    COALESCE(EXCLUDED.can_manage_chat, chat_members.can_manage_chat),
    COALESCE(EXCLUDED.can_delete_messages, chat_members.can_delete_messages),
    COALESCE(EXCLUDED.can_manage_video_chats, chat_members.can_manage_video_chats),
    COALESCE(EXCLUDED.can_restrict_members, chat_members.can_restrict_members),
    COALESCE(EXCLUDED.can_promote_members, chat_members.can_promote_members),
    COALESCE(EXCLUDED.can_change_info, chat_members.can_change_info),
    COALESCE(EXCLUDED.can_invite_users, chat_members.can_invite_users),
    COALESCE(EXCLUDED.can_post_messages, chat_members.can_post_messages),
    COALESCE(EXCLUDED.can_edit_messages, chat_members.can_edit_messages),
    COALESCE(EXCLUDED.can_pin_messages, chat_members.can_pin_messages),
    COALESCE(EXCLUDED.can_manage_topics, chat_members.can_manage_topics),
    COALESCE(EXCLUDED.can_send_messages, chat_members.can_send_messages),
    COALESCE(EXCLUDED.can_send_media_messages, chat_members.can_send_media_messages),
    COALESCE(EXCLUDED.can_send_polls, chat_members.can_send_polls),
    COALESCE(EXCLUDED.can_send_other_messages, chat_members.can_send_other_messages),
    COALESCE(EXCLUDED.can_add_web_page_previews, chat_members.can_add_web_page_previews),
    COALESCE(EXCLUDED.until_date, chat_members.until_date)
)
"#;

const SQL_APPLY_MEMBER_ACTIVITY_FROM_STAGE: &str = r#"
UPDATE chat_members AS member
SET last_message_at = GREATEST(member.last_message_at, staged.last_message_at),
    updated_at = CURRENT_TIMESTAMP
FROM telegram_activity_stage AS staged
WHERE staged.bot_id = $1
  AND staged.last_message_at IS NOT NULL
  AND member.chat_id = staged.chat_id
  AND member.user_id = staged.user_id
  AND member.last_message_at IS DISTINCT FROM
      GREATEST(member.last_message_at, staged.last_message_at)
"#;

const SQL_APPLY_ACTIVE_USERS_FROM_STAGE: &str = r#"
INSERT INTO chat_active_users (chat_id, user_id, last_active_at)
SELECT staged.chat_id, staged.user_id, staged.last_active_at
FROM telegram_activity_stage AS staged
JOIN chats ON chats.id = staged.chat_id
JOIN users ON users.id = staged.user_id
WHERE staged.bot_id = $1
  AND staged.last_active_at IS NOT NULL
ON CONFLICT (chat_id, user_id) DO UPDATE SET
    last_active_at = GREATEST(chat_active_users.last_active_at, EXCLUDED.last_active_at)
WHERE chat_active_users.last_active_at IS DISTINCT FROM
    GREATEST(chat_active_users.last_active_at, EXCLUDED.last_active_at)
"#;

const SQL_APPLY_FILES_FROM_STAGE: &str = r#"
INSERT INTO telegram_files (
    file_unique_id, latest_file_id, media_kind, mime_type, width, height, file_size,
    first_seen_chat_id, first_seen_message_id, last_seen_chat_id, last_seen_message_id,
    last_seen_at, telegram_observed_at
)
SELECT
    file_unique_id, latest_file_id, media_kind, mime_type, width, height, file_size,
    first_seen_chat_id, first_seen_message_id, last_seen_chat_id, last_seen_message_id,
    observed_at, observed_at
FROM telegram_files_stage
WHERE bot_id = $1
ON CONFLICT (file_unique_id) DO UPDATE SET
    latest_file_id = EXCLUDED.latest_file_id,
    media_kind = EXCLUDED.media_kind,
    mime_type = COALESCE(EXCLUDED.mime_type, telegram_files.mime_type),
    width = COALESCE(EXCLUDED.width, telegram_files.width),
    height = COALESCE(EXCLUDED.height, telegram_files.height),
    file_size = COALESCE(EXCLUDED.file_size, telegram_files.file_size),
    first_seen_chat_id = COALESCE(telegram_files.first_seen_chat_id, EXCLUDED.first_seen_chat_id),
    first_seen_message_id = COALESCE(
        telegram_files.first_seen_message_id,
        EXCLUDED.first_seen_message_id
    ),
    last_seen_chat_id = EXCLUDED.last_seen_chat_id,
    last_seen_message_id = EXCLUDED.last_seen_message_id,
    last_seen_at = GREATEST(telegram_files.last_seen_at, EXCLUDED.last_seen_at),
    telegram_observed_at = EXCLUDED.telegram_observed_at,
    updated_at = CURRENT_TIMESTAMP
WHERE EXCLUDED.telegram_observed_at
        >= COALESCE(telegram_files.telegram_observed_at, '-infinity')
  AND ROW(
    telegram_files.latest_file_id,
    telegram_files.media_kind,
    telegram_files.mime_type,
    telegram_files.width,
    telegram_files.height,
    telegram_files.file_size,
    telegram_files.last_seen_chat_id,
    telegram_files.last_seen_message_id,
    telegram_files.last_seen_at
) IS DISTINCT FROM ROW(
    EXCLUDED.latest_file_id,
    EXCLUDED.media_kind,
    COALESCE(EXCLUDED.mime_type, telegram_files.mime_type),
    COALESCE(EXCLUDED.width, telegram_files.width),
    COALESCE(EXCLUDED.height, telegram_files.height),
    COALESCE(EXCLUDED.file_size, telegram_files.file_size),
    EXCLUDED.last_seen_chat_id,
    EXCLUDED.last_seen_message_id,
    GREATEST(telegram_files.last_seen_at, EXCLUDED.last_seen_at)
)
"#;

const SQL_DELETE_USERS_STAGE: &str = "DELETE FROM telegram_users_stage WHERE bot_id = $1";
const SQL_DELETE_CHATS_STAGE: &str = "DELETE FROM telegram_chats_stage WHERE bot_id = $1";
const SQL_DELETE_MEMBERS_STAGE: &str = "DELETE FROM telegram_chat_members_stage WHERE bot_id = $1";
const SQL_DELETE_ACTIVITY_STAGE: &str = "DELETE FROM telegram_activity_stage WHERE bot_id = $1";
const SQL_DELETE_FILES_STAGE: &str = "DELETE FROM telegram_files_stage WHERE bot_id = $1";

const SQL_STAGE_STATS: &str = r#"
SELECT
    (SELECT count(*) FROM telegram_users_stage WHERE bot_id = $1)
    + (SELECT count(*) FROM telegram_chats_stage WHERE bot_id = $1)
    + (SELECT count(*) FROM telegram_chat_members_stage WHERE bot_id = $1)
    + (SELECT count(*) FROM telegram_activity_stage WHERE bot_id = $1)
    + (SELECT count(*) FROM telegram_files_stage WHERE bot_id = $1) AS rows,
    LEAST(
        (SELECT min(observed_at) FROM telegram_users_stage WHERE bot_id = $1),
        (SELECT min(observed_at) FROM telegram_chats_stage WHERE bot_id = $1),
        (SELECT min(observed_at) FROM telegram_chat_members_stage WHERE bot_id = $1),
        (SELECT min(observed_at) FROM telegram_activity_stage WHERE bot_id = $1),
        (SELECT min(observed_at) FROM telegram_files_stage WHERE bot_id = $1)
    ) AS oldest_observed_at
"#;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TelegramProjectionVersion {
    pub bot_id: i64,
    pub observed_at: OffsetDateTime,
    pub stream_ms: i64,
    pub stream_seq: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramUserProjection {
    pub version: TelegramProjectionVersion,
    pub state: UserState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramChatProjection {
    pub version: TelegramProjectionVersion,
    pub state: ChatState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramChatMemberProjection {
    pub version: TelegramProjectionVersion,
    pub state: ChatMemberUpsert,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramActivityProjection {
    pub version: TelegramProjectionVersion,
    pub chat_id: i64,
    pub user_id: i64,
    pub last_message_at: Option<OffsetDateTime>,
    pub last_active_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramFileProjection {
    pub version: TelegramProjectionVersion,
    pub state: TelegramFileMetadataUpsert,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TelegramProjectionBatch {
    pub users: Vec<TelegramUserProjection>,
    pub chats: Vec<TelegramChatProjection>,
    pub members: Vec<TelegramChatMemberProjection>,
    pub activity: Vec<TelegramActivityProjection>,
    pub files: Vec<TelegramFileProjection>,
}

impl TelegramProjectionBatch {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mutation_count() == 0
    }

    #[must_use]
    pub fn mutation_count(&self) -> usize {
        self.users.len()
            + self.chats.len()
            + self.members.len()
            + self.activity.len()
            + self.files.len()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TelegramProjectionStageStats {
    pub rows: i64,
    pub oldest_observed_at: Option<OffsetDateTime>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TelegramProjectionFlushReport {
    pub users: u64,
    pub chats: u64,
    pub members: u64,
    pub member_activity: u64,
    pub active_users: u64,
    pub files: u64,
    pub deleted_stage_rows: u64,
}

#[derive(Clone, Debug)]
pub struct PostgresTelegramProjectionStore {
    pool: PgPool,
}

impl PostgresTelegramProjectionStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn stage_projection_batch(
        &self,
        batch: &TelegramProjectionBatch,
    ) -> Result<u64, StorageError> {
        if batch.is_empty() {
            return Ok(0);
        }
        let mut tx = self.pool.begin().await?;
        let staged = stage_projection_batch_in_transaction(&mut tx, batch).await?;
        tx.commit().await?;
        Ok(staged)
    }

    pub async fn apply_projection_batch(
        &self,
        batch: &TelegramProjectionBatch,
    ) -> Result<u64, StorageError> {
        if batch.is_empty() {
            return Ok(0);
        }
        let mut tx = self.pool.begin().await?;
        let applied = apply_projection_batch_in_transaction(&mut tx, batch).await?;
        tx.commit().await?;
        Ok(applied)
    }

    pub async fn flush_staged_projections(
        &self,
        bot_id: i64,
    ) -> Result<TelegramProjectionFlushReport, StorageError> {
        let mut tx = self.pool.begin().await?;
        // Every apply and delete must see the same staging snapshot. Otherwise a
        // concurrent stage insert can commit after an apply statement and then
        // be removed by a later DELETE without ever reaching durable storage.
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *tx)
            .await?;
        let users = sqlx::query(SQL_APPLY_USERS_FROM_STAGE)
            .bind(bot_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        let chats = sqlx::query(SQL_APPLY_CHATS_FROM_STAGE)
            .bind(bot_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        let members = sqlx::query(SQL_APPLY_MEMBERS_FROM_STAGE)
            .bind(bot_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        let member_activity = sqlx::query(SQL_APPLY_MEMBER_ACTIVITY_FROM_STAGE)
            .bind(bot_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        let active_users = sqlx::query(SQL_APPLY_ACTIVE_USERS_FROM_STAGE)
            .bind(bot_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        let files = sqlx::query(SQL_APPLY_FILES_FROM_STAGE)
            .bind(bot_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();

        let mut deleted_stage_rows = 0_u64;
        for statement in [
            SQL_DELETE_USERS_STAGE,
            SQL_DELETE_CHATS_STAGE,
            SQL_DELETE_MEMBERS_STAGE,
            SQL_DELETE_ACTIVITY_STAGE,
            SQL_DELETE_FILES_STAGE,
        ] {
            deleted_stage_rows = deleted_stage_rows.saturating_add(
                sqlx::query(statement)
                    .bind(bot_id)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected(),
            );
        }
        tx.commit().await?;
        Ok(TelegramProjectionFlushReport {
            users,
            chats,
            members,
            member_activity,
            active_users,
            files,
            deleted_stage_rows,
        })
    }

    pub async fn stage_stats(
        &self,
        bot_id: i64,
    ) -> Result<TelegramProjectionStageStats, StorageError> {
        use sqlx::Row;

        let row = sqlx::query(SQL_STAGE_STATS)
            .bind(bot_id)
            .fetch_one(&self.pool)
            .await?;
        Ok(TelegramProjectionStageStats {
            rows: row.try_get("rows")?,
            oldest_observed_at: row.try_get("oldest_observed_at")?,
        })
    }
}

pub(crate) async fn stage_projection_batch_in_transaction(
    tx: &mut Transaction<'_, Postgres>,
    batch: &TelegramProjectionBatch,
) -> Result<u64, StorageError> {
    let mut staged = 0_u64;
    if !batch.users.is_empty() {
        staged = staged.saturating_add(write_users(tx, &batch.users, SQL_STAGE_USERS).await?);
    }
    if !batch.chats.is_empty() {
        staged = staged.saturating_add(write_chats(tx, &batch.chats, SQL_STAGE_CHATS).await?);
    }
    if !batch.members.is_empty() {
        staged = staged.saturating_add(write_members(tx, &batch.members, SQL_STAGE_MEMBERS).await?);
    }
    if !batch.activity.is_empty() {
        staged =
            staged.saturating_add(write_activity(tx, &batch.activity, SQL_STAGE_ACTIVITY).await?);
    }
    if !batch.files.is_empty() {
        staged = staged.saturating_add(write_files(tx, &batch.files, SQL_STAGE_FILES).await?);
    }
    Ok(staged)
}

pub(crate) async fn apply_projection_batch_in_transaction(
    tx: &mut Transaction<'_, Postgres>,
    batch: &TelegramProjectionBatch,
) -> Result<u64, StorageError> {
    let mut applied = 0_u64;
    if !batch.users.is_empty() {
        applied =
            applied.saturating_add(write_users(tx, &batch.users, SQL_APPLY_USERS_BATCH).await?);
    }
    if !batch.chats.is_empty() {
        applied =
            applied.saturating_add(write_chats(tx, &batch.chats, SQL_APPLY_CHATS_BATCH).await?);
    }
    if !batch.members.is_empty() {
        applied = applied
            .saturating_add(write_members(tx, &batch.members, SQL_APPLY_MEMBERS_BATCH).await?);
    }
    if !batch.activity.is_empty() {
        applied = applied
            .saturating_add(write_activity(tx, &batch.activity, SQL_APPLY_ACTIVITY_BATCH).await?);
    }
    if !batch.files.is_empty() {
        applied =
            applied.saturating_add(write_files(tx, &batch.files, SQL_APPLY_FILES_BATCH).await?);
    }
    Ok(applied)
}

async fn write_users(
    tx: &mut Transaction<'_, Postgres>,
    rows: &[TelegramUserProjection],
    statement: &'static str,
) -> Result<u64, StorageError> {
    let result = sqlx::query(statement)
        .bind(
            rows.iter()
                .map(|row| row.version.bot_id)
                .collect::<Vec<_>>(),
        )
        .bind(rows.iter().map(|row| row.state.id).collect::<Vec<_>>())
        .bind(
            rows.iter()
                .map(|row| row.state.first_name.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.last_name.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.username.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.language_code.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.is_premium)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.version.observed_at)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.version.stream_ms)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.version.stream_seq)
                .collect::<Vec<_>>(),
        )
        .execute(&mut **tx)
        .await?;
    Ok(result.rows_affected())
}

async fn write_chats(
    tx: &mut Transaction<'_, Postgres>,
    rows: &[TelegramChatProjection],
    statement: &'static str,
) -> Result<u64, StorageError> {
    let result = sqlx::query(statement)
        .bind(
            rows.iter()
                .map(|row| row.version.bot_id)
                .collect::<Vec<_>>(),
        )
        .bind(rows.iter().map(|row| row.state.id).collect::<Vec<_>>())
        .bind(
            rows.iter()
                .map(|row| row.state.chat_type.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.title.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.username.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.first_name.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.last_name.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.is_forum)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.version.observed_at)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.version.stream_ms)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.version.stream_seq)
                .collect::<Vec<_>>(),
        )
        .execute(&mut **tx)
        .await?;
    Ok(result.rows_affected())
}

async fn write_members(
    tx: &mut Transaction<'_, Postgres>,
    rows: &[TelegramChatMemberProjection],
    statement: &'static str,
) -> Result<u64, StorageError> {
    let result = sqlx::query(statement)
        .bind(
            rows.iter()
                .map(|row| row.version.bot_id)
                .collect::<Vec<_>>(),
        )
        .bind(rows.iter().map(|row| row.state.chat_id).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.state.user_id).collect::<Vec<_>>())
        .bind(
            rows.iter()
                .map(|row| row.state.status.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.is_member)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.is_anonymous)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.custom_title.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.can_be_edited)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.can_manage_chat)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.can_delete_messages)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.can_manage_video_chats)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.can_restrict_members)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.can_promote_members)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.can_change_info)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.can_invite_users)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.can_post_messages)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.can_edit_messages)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.can_pin_messages)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.can_manage_topics)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.can_send_messages)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.can_send_media_messages)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.can_send_polls)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.can_send_other_messages)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.can_add_web_page_previews)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.until_date)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.version.observed_at)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.version.stream_ms)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.version.stream_seq)
                .collect::<Vec<_>>(),
        )
        .execute(&mut **tx)
        .await?;
    Ok(result.rows_affected())
}

async fn write_activity(
    tx: &mut Transaction<'_, Postgres>,
    rows: &[TelegramActivityProjection],
    statement: &'static str,
) -> Result<u64, StorageError> {
    let result = sqlx::query(statement)
        .bind(
            rows.iter()
                .map(|row| row.version.bot_id)
                .collect::<Vec<_>>(),
        )
        .bind(rows.iter().map(|row| row.chat_id).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.user_id).collect::<Vec<_>>())
        .bind(
            rows.iter()
                .map(|row| row.last_message_at)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.last_active_at)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.version.observed_at)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.version.stream_ms)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.version.stream_seq)
                .collect::<Vec<_>>(),
        )
        .execute(&mut **tx)
        .await?;
    Ok(result.rows_affected())
}

async fn write_files(
    tx: &mut Transaction<'_, Postgres>,
    rows: &[TelegramFileProjection],
    statement: &'static str,
) -> Result<u64, StorageError> {
    let result = sqlx::query(statement)
        .bind(
            rows.iter()
                .map(|row| row.version.bot_id)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.file_unique_id.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.latest_file_id.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.media_kind.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.mime_type.clone())
                .collect::<Vec<_>>(),
        )
        .bind(rows.iter().map(|row| row.state.width).collect::<Vec<_>>())
        .bind(rows.iter().map(|row| row.state.height).collect::<Vec<_>>())
        .bind(
            rows.iter()
                .map(|row| row.state.file_size)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.first_seen_chat_id)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.first_seen_message_id)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.last_seen_chat_id)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.state.last_seen_message_id)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.version.observed_at)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.version.stream_ms)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .map(|row| row.version.stream_seq)
                .collect::<Vec<_>>(),
        )
        .execute(&mut **tx)
        .await?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use std::{env, error::Error};

    use openplotva_core::{ChatState, UserState};
    use sqlx::{PgPool, postgres::PgPoolOptions};
    use time::{Duration, OffsetDateTime};

    use crate::{ChatMemberUpsert, TelegramFileMetadataUpsert};

    use super::{
        PostgresTelegramProjectionStore, TelegramActivityProjection, TelegramChatMemberProjection,
        TelegramChatProjection, TelegramFileProjection, TelegramProjectionBatch,
        TelegramProjectionVersion, TelegramUserProjection,
    };

    #[test]
    fn empty_projection_batch_has_no_mutations() {
        let batch = TelegramProjectionBatch::default();
        assert!(batch.is_empty());
        assert_eq!(batch.mutation_count(), 0);
    }

    #[tokio::test]
    async fn staged_state_overlays_durable_reads_and_survives_flush() -> Result<(), Box<dyn Error>>
    {
        let Some(pool) = migrated_test_pool().await? else {
            return Ok(());
        };
        let suffix = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let bot_id = i64::try_from(suffix.rem_euclid(1_000_000_000))? + 10_000;
        let user_id = bot_id + 1_000_000_000;
        let chat_id = -(bot_id + 2_000_000_000);
        let file_unique_id = format!("projection-overlay-{suffix}");
        cleanup(&pool, bot_id, user_id, chat_id, &file_unique_id).await?;

        let observed_at = OffsetDateTime::now_utc();
        let batch = sample_batch(
            bot_id,
            user_id,
            chat_id,
            file_unique_id.clone(),
            observed_at,
            100,
            "staged",
        );
        let store = PostgresTelegramProjectionStore::new(pool.clone());
        store.stage_projection_batch(&batch).await?;

        assert!(
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM telegram_users_effective WHERE id = $1 AND first_name = 'staged')",
            )
            .bind(user_id)
            .fetch_one(&pool)
            .await?
        );
        assert!(
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM telegram_chat_members_effective WHERE chat_id = $1 AND user_id = $2 AND status = 'member')",
            )
            .bind(chat_id)
            .bind(user_id)
            .fetch_one(&pool)
            .await?
        );
        assert!(
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM telegram_files_effective WHERE file_unique_id = $1 AND latest_file_id = 'file-staged')",
            )
            .bind(&file_unique_id)
            .fetch_one(&pool)
            .await?
        );
        assert!(
            !sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)")
                .bind(user_id)
                .fetch_one(&pool)
                .await?
        );

        let flushed = store.flush_staged_projections(bot_id).await?;
        assert_eq!(flushed.deleted_stage_rows, 5);
        assert_eq!(store.stage_stats(bot_id).await?.rows, 0);
        assert!(
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM telegram_users_effective WHERE id = $1 AND first_name = 'staged')",
            )
            .bind(user_id)
            .fetch_one(&pool)
            .await?
        );

        cleanup(&pool, bot_id, user_id, chat_id, &file_unique_id).await?;
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_stage_insert_survives_the_flush_snapshot() -> Result<(), Box<dyn Error>> {
        let Some(pool) = migrated_test_pool().await? else {
            return Ok(());
        };
        let suffix = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let bot_id = i64::try_from(suffix.rem_euclid(1_000_000_000))? + 15_000;
        let first_user_id = bot_id + 1_500_000_000;
        let concurrent_user_id = bot_id + 1_600_000_000;
        let chat_id = -(bot_id + 1_700_000_000);
        let file_unique_id = format!("projection-concurrent-flush-{suffix}");
        cleanup(&pool, bot_id, first_user_id, chat_id, &file_unique_id).await?;
        cleanup(&pool, bot_id, concurrent_user_id, chat_id, &file_unique_id).await?;

        let store = PostgresTelegramProjectionStore::new(pool.clone());
        let observed_at = OffsetDateTime::now_utc();
        store
            .stage_projection_batch(&user_only_batch(sample_batch(
                bot_id,
                first_user_id,
                chat_id,
                file_unique_id.clone(),
                observed_at,
                150,
                "initial",
            )))
            .await?;
        store.flush_staged_projections(bot_id).await?;
        store
            .stage_projection_batch(&user_only_batch(sample_batch(
                bot_id,
                first_user_id,
                chat_id,
                file_unique_id.clone(),
                observed_at + Duration::seconds(1),
                151,
                "before-flush",
            )))
            .await?;

        let mut blocker = pool.begin().await?;
        sqlx::query("SELECT id FROM users WHERE id = $1 FOR UPDATE")
            .bind(first_user_id)
            .execute(&mut *blocker)
            .await?;

        let flushing_store = store.clone();
        let flush =
            tokio::spawn(async move { flushing_store.flush_staged_projections(bot_id).await });
        let mut flush_is_waiting = false;
        for _ in 0..100 {
            flush_is_waiting = sqlx::query_scalar(
                "SELECT EXISTS (\
                 SELECT 1 FROM pg_stat_activity \
                 WHERE datname = current_database() \
                   AND pid <> pg_backend_pid() \
                   AND wait_event_type = 'Lock' \
                   AND query LIKE '%INSERT INTO users%')",
            )
            .fetch_one(&pool)
            .await?;
            if flush_is_waiting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            flush_is_waiting,
            "flush must reach the blocked durable user merge before staging the concurrent row"
        );

        store
            .stage_projection_batch(&user_only_batch(sample_batch(
                bot_id,
                concurrent_user_id,
                chat_id,
                file_unique_id.clone(),
                observed_at + Duration::seconds(2),
                152,
                "concurrent",
            )))
            .await?;
        blocker.commit().await?;
        flush.await??;

        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM telegram_users_stage \
                 WHERE bot_id = $1 AND user_id = $2",
            )
            .bind(bot_id)
            .bind(concurrent_user_id)
            .fetch_one(&pool)
            .await?,
            1
        );
        assert!(
            !sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)")
                .bind(concurrent_user_id)
                .fetch_one(&pool)
                .await?
        );

        store.flush_staged_projections(bot_id).await?;
        assert!(
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1 AND first_name = 'concurrent')",
            )
            .bind(concurrent_user_id)
            .fetch_one(&pool)
            .await?
        );

        cleanup(&pool, bot_id, first_user_id, chat_id, &file_unique_id).await?;
        cleanup(&pool, bot_id, concurrent_user_id, chat_id, &file_unique_id).await?;
        Ok(())
    }

    #[tokio::test]
    async fn orphan_activity_does_not_block_valid_projection_flush() -> Result<(), Box<dyn Error>> {
        let Some(pool) = migrated_test_pool().await? else {
            return Ok(());
        };
        let suffix = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let bot_id = i64::try_from(suffix.rem_euclid(1_000_000_000))? + 17_000;
        let valid_user_id = bot_id + 1_800_000_000;
        let valid_chat_id = -(bot_id + 1_900_000_000);
        let orphan_user_id = bot_id + 2_000_000_000;
        let orphan_chat_id = -(bot_id + 2_100_000_000);
        let file_unique_id = format!("projection-orphan-activity-{suffix}");
        cleanup(&pool, bot_id, valid_user_id, valid_chat_id, &file_unique_id).await?;

        let store = PostgresTelegramProjectionStore::new(pool.clone());
        let observed_at = OffsetDateTime::now_utc();
        store
            .stage_projection_batch(&activity_only_batch(sample_batch(
                bot_id,
                orphan_user_id,
                orphan_chat_id,
                format!("orphan-{file_unique_id}"),
                observed_at,
                170,
                "orphan",
            )))
            .await?;
        store
            .stage_projection_batch(&sample_batch(
                bot_id,
                valid_user_id,
                valid_chat_id,
                file_unique_id.clone(),
                observed_at + Duration::seconds(1),
                171,
                "valid",
            ))
            .await?;

        let flushed = store.flush_staged_projections(bot_id).await?;
        assert_eq!(flushed.active_users, 1);
        assert_eq!(store.stage_stats(bot_id).await?.rows, 0);
        assert!(
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM chat_active_users \
                 WHERE chat_id = $1 AND user_id = $2)",
            )
            .bind(valid_chat_id)
            .bind(valid_user_id)
            .fetch_one(&pool)
            .await?
        );
        assert!(
            !sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM chat_active_users \
                 WHERE chat_id = $1 AND user_id = $2)",
            )
            .bind(orphan_chat_id)
            .bind(orphan_user_id)
            .fetch_one(&pool)
            .await?
        );

        cleanup(&pool, bot_id, valid_user_id, valid_chat_id, &file_unique_id).await?;
        Ok(())
    }

    #[tokio::test]
    async fn immediate_bulk_apply_skips_unchanged_durable_rows() -> Result<(), Box<dyn Error>> {
        let Some(pool) = migrated_test_pool().await? else {
            return Ok(());
        };
        let suffix = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let bot_id = i64::try_from(suffix.rem_euclid(1_000_000_000))? + 20_000;
        let user_id = bot_id + 3_000_000_000;
        let chat_id = -(bot_id + 4_000_000_000);
        let file_unique_id = format!("projection-online-{suffix}");
        cleanup(&pool, bot_id, user_id, chat_id, &file_unique_id).await?;

        let store = PostgresTelegramProjectionStore::new(pool.clone());
        let observed_at = OffsetDateTime::now_utc();
        let first = sample_batch(
            bot_id,
            user_id,
            chat_id,
            file_unique_id.clone(),
            observed_at,
            200,
            "online",
        );
        store.apply_projection_batch(&first).await?;
        let updated: OffsetDateTime = sqlx::query_scalar("SELECT updated FROM users WHERE id = $1")
            .bind(user_id)
            .fetch_one(&pool)
            .await?;

        let same = sample_batch(
            bot_id,
            user_id,
            chat_id,
            file_unique_id.clone(),
            observed_at + Duration::seconds(1),
            201,
            "online",
        );
        store.apply_projection_batch(&same).await?;
        let updated_after: OffsetDateTime =
            sqlx::query_scalar("SELECT updated FROM users WHERE id = $1")
                .bind(user_id)
                .fetch_one(&pool)
                .await?;
        assert_eq!(updated_after, updated);

        cleanup(&pool, bot_id, user_id, chat_id, &file_unique_id).await?;
        Ok(())
    }

    #[tokio::test]
    async fn older_staged_observation_neither_overlays_nor_overwrites_durable_state()
    -> Result<(), Box<dyn Error>> {
        let Some(pool) = migrated_test_pool().await? else {
            return Ok(());
        };
        let suffix = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let bot_id = i64::try_from(suffix.rem_euclid(1_000_000_000))? + 30_000;
        let user_id = bot_id + 5_000_000_000;
        let chat_id = -(bot_id + 6_000_000_000);
        let file_unique_id = format!("projection-ordering-{suffix}");
        cleanup(&pool, bot_id, user_id, chat_id, &file_unique_id).await?;

        let store = PostgresTelegramProjectionStore::new(pool.clone());
        let newer_observed_at = OffsetDateTime::now_utc();
        let newer = sample_batch(
            bot_id,
            user_id,
            chat_id,
            file_unique_id.clone(),
            newer_observed_at,
            300,
            "newer",
        );
        store.apply_projection_batch(&newer).await?;

        let older = sample_batch(
            bot_id,
            user_id,
            chat_id,
            file_unique_id.clone(),
            newer_observed_at - Duration::seconds(1),
            301,
            "older",
        );
        store.stage_projection_batch(&older).await?;

        let effective_name: String =
            sqlx::query_scalar("SELECT first_name FROM telegram_users_effective WHERE id = $1")
                .bind(user_id)
                .fetch_one(&pool)
                .await?;
        assert_eq!(effective_name, "newer");

        let flushed = store.flush_staged_projections(bot_id).await?;
        assert_eq!(flushed.users, 0);
        assert_eq!(flushed.chats, 0);
        assert_eq!(flushed.members, 0);
        assert_eq!(flushed.files, 0);

        let durable_name: String = sqlx::query_scalar("SELECT first_name FROM users WHERE id = $1")
            .bind(user_id)
            .fetch_one(&pool)
            .await?;
        assert_eq!(durable_name, "newer");

        cleanup(&pool, bot_id, user_id, chat_id, &file_unique_id).await?;
        Ok(())
    }

    #[tokio::test]
    async fn effective_point_reads_push_keys_into_durable_and_stage_indexes()
    -> Result<(), Box<dyn Error>> {
        let Some(pool) = migrated_test_pool().await? else {
            return Ok(());
        };
        for (query, expected_stage_index) in [
            (
                "EXPLAIN (COSTS OFF) SELECT * FROM telegram_users_effective WHERE id = 42",
                "telegram_users_stage_latest_idx",
            ),
            (
                "EXPLAIN (COSTS OFF) SELECT * FROM telegram_chat_members_effective \
                 WHERE chat_id = -10042 AND user_id = 77",
                "telegram_chat_members_stage_latest_idx",
            ),
            (
                "EXPLAIN (COSTS OFF) SELECT * FROM telegram_files_effective \
                 WHERE file_unique_id = 'projection-plan'",
                "telegram_files_stage_latest_idx",
            ),
        ] {
            let plan = sqlx::query_scalar::<_, String>(query)
                .fetch_all(&pool)
                .await?
                .join("\n");
            assert!(
                plan.contains(expected_stage_index),
                "effective point lookup missed {expected_stage_index}:\n{plan}"
            );
            assert!(
                !plan.contains("Full Join"),
                "effective point lookup scanned a full overlay relation:\n{plan}"
            );
        }
        Ok(())
    }

    async fn migrated_test_pool() -> Result<Option<PgPool>, Box<dyn Error>> {
        let Ok(dsn) = env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(None);
        };
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&dsn)
            .await?;
        crate::run_migrations_on(&pool).await?;
        Ok(Some(pool))
    }

    fn sample_batch(
        bot_id: i64,
        user_id: i64,
        chat_id: i64,
        file_unique_id: String,
        observed_at: OffsetDateTime,
        stream_ms: i64,
        label: &str,
    ) -> TelegramProjectionBatch {
        let version = TelegramProjectionVersion {
            bot_id,
            observed_at,
            stream_ms,
            stream_seq: 0,
        };
        TelegramProjectionBatch {
            users: vec![TelegramUserProjection {
                version,
                state: UserState::new(
                    user_id,
                    label,
                    Some("User".to_owned()),
                    Some(format!("{label}_user")),
                    Some("en".to_owned()),
                    Some(true),
                ),
            }],
            chats: vec![TelegramChatProjection {
                version,
                state: ChatState::new(
                    chat_id,
                    "supergroup",
                    Some(format!("{label} chat")),
                    None,
                    None,
                    None,
                    Some(true),
                ),
            }],
            members: vec![TelegramChatMemberProjection {
                version,
                state: ChatMemberUpsert {
                    chat_id,
                    user_id,
                    status: "member".to_owned(),
                    is_member: Some(true),
                    ..ChatMemberUpsert::default()
                },
            }],
            activity: vec![TelegramActivityProjection {
                version,
                chat_id,
                user_id,
                last_message_at: Some(observed_at),
                last_active_at: Some(observed_at),
            }],
            files: vec![TelegramFileProjection {
                version,
                state: TelegramFileMetadataUpsert {
                    file_unique_id,
                    latest_file_id: format!("file-{label}"),
                    media_kind: "photo".to_owned(),
                    first_seen_chat_id: Some(chat_id),
                    first_seen_message_id: Some(1),
                    last_seen_chat_id: Some(chat_id),
                    last_seen_message_id: Some(1),
                    ..TelegramFileMetadataUpsert::default()
                },
            }],
        }
    }

    fn user_only_batch(mut batch: TelegramProjectionBatch) -> TelegramProjectionBatch {
        batch.chats.clear();
        batch.members.clear();
        batch.activity.clear();
        batch.files.clear();
        batch
    }

    fn activity_only_batch(mut batch: TelegramProjectionBatch) -> TelegramProjectionBatch {
        batch.users.clear();
        batch.chats.clear();
        batch.members.clear();
        batch.files.clear();
        batch
    }

    async fn cleanup(
        pool: &PgPool,
        bot_id: i64,
        user_id: i64,
        chat_id: i64,
        file_unique_id: &str,
    ) -> Result<(), sqlx::Error> {
        let mut tx = pool.begin().await?;
        sqlx::query("DELETE FROM telegram_users_stage WHERE bot_id = $1")
            .bind(bot_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM telegram_chats_stage WHERE bot_id = $1")
            .bind(bot_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM telegram_chat_members_stage WHERE bot_id = $1")
            .bind(bot_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM telegram_activity_stage WHERE bot_id = $1")
            .bind(bot_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM telegram_files_stage WHERE bot_id = $1")
            .bind(bot_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM telegram_files WHERE file_unique_id = $1")
            .bind(file_unique_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM chat_active_users WHERE chat_id = $1 AND user_id = $2")
            .bind(chat_id)
            .bind(user_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM chat_members WHERE chat_id = $1 AND user_id = $2")
            .bind(chat_id)
            .bind(user_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM chats WHERE id = $1")
            .bind(chat_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await
    }
}
