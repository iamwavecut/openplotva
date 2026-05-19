-- Source SHA-256: a2027bb5b8b50753adeda1faa39dc450fbc843afc4e5060f399244b5c71a8600

CREATE TABLE chat_permissions (
    chat_id BIGINT PRIMARY KEY,
    status TEXT NOT NULL,
    can_manage_chat BOOLEAN DEFAULT FALSE,
    can_delete_messages BOOLEAN DEFAULT FALSE,
    can_manage_video_chats BOOLEAN DEFAULT FALSE,
    can_restrict_members BOOLEAN DEFAULT FALSE,
    can_promote_members BOOLEAN DEFAULT FALSE,
    can_change_info BOOLEAN DEFAULT FALSE,
    can_invite_users BOOLEAN DEFAULT FALSE,
    can_post_messages BOOLEAN DEFAULT FALSE,
    can_edit_messages BOOLEAN DEFAULT FALSE,
    can_pin_messages BOOLEAN DEFAULT FALSE,
    can_manage_topics BOOLEAN DEFAULT FALSE,
    can_send_messages BOOLEAN DEFAULT FALSE,
    can_send_media_messages BOOLEAN DEFAULT FALSE,
    can_send_polls BOOLEAN DEFAULT FALSE,
    can_send_other_messages BOOLEAN DEFAULT FALSE,
    can_add_web_page_previews BOOLEAN DEFAULT FALSE,
    last_checked_at TIMESTAMPTZ NOT NULL,
    last_error_at TIMESTAMPTZ,
    error_count INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE chat_settings (
    chat_id BIGINT PRIMARY KEY,
    mood_alignment TEXT DEFAULT 'neutral',
    custom_persona TEXT DEFAULT NULL,
    updated TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    reactivity_percentage INTEGER DEFAULT 3,
    proactivity_percentage INTEGER DEFAULT 0,
    enable_global_text_reply BOOLEAN DEFAULT TRUE,
    enable_global_draw_reply BOOLEAN DEFAULT TRUE,
    enable_obscenifier BOOLEAN DEFAULT TRUE,
    enable_profanity BOOLEAN DEFAULT TRUE,
    enable_greet_joiners BOOLEAN DEFAULT FALSE
);

CREATE TABLE chats (
    id BIGINT PRIMARY KEY,
    type TEXT NOT NULL,
    title TEXT,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    is_forum BOOLEAN DEFAULT FALSE,
    active_usernames JSONB DEFAULT NULL,
    available_reactions JSONB DEFAULT NULL,
    bio TEXT,
    has_private_forwards BOOLEAN DEFAULT FALSE,
    has_restricted_voice_and_video_messages BOOLEAN DEFAULT FALSE,
    join_to_send_messages BOOLEAN DEFAULT FALSE,
    join_by_request BOOLEAN DEFAULT FALSE,
    description TEXT,
    invite_link TEXT,
    pinned_message JSONB DEFAULT NULL,
    permissions JSONB DEFAULT NULL,
    slow_mode_delay BIGINT,
    message_auto_delete_time BIGINT,
    has_aggressive_anti_spam_enabled BOOLEAN DEFAULT FALSE,
    has_hidden_members BOOLEAN DEFAULT FALSE,
    has_protected_content BOOLEAN DEFAULT FALSE,
    has_visible_history BOOLEAN DEFAULT FALSE,
    sticker_set_name TEXT,
    can_set_sticker_set BOOLEAN DEFAULT FALSE,
    linked_chat_id BIGINT,
    location JSONB DEFAULT NULL,
    discovered TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE users (
    id BIGINT PRIMARY KEY,
    is_premium BOOLEAN DEFAULT FALSE,
    first_name TEXT NOT NULL,
    last_name TEXT,
    username TEXT,
    language_code TEXT,
    is_vip BOOLEAN DEFAULT FALSE,
    settings JSONB DEFAULT NULL,
    discovered TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE chat_members (
    chat_id BIGINT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    is_anonymous BOOLEAN DEFAULT FALSE,
    custom_title TEXT,
    can_be_edited BOOLEAN DEFAULT FALSE,
    can_manage_chat BOOLEAN DEFAULT FALSE,
    can_delete_messages BOOLEAN DEFAULT FALSE,
    can_manage_video_chats BOOLEAN DEFAULT FALSE,
    can_restrict_members BOOLEAN DEFAULT FALSE,
    can_promote_members BOOLEAN DEFAULT FALSE,
    can_change_info BOOLEAN DEFAULT FALSE,
    can_invite_users BOOLEAN DEFAULT FALSE,
    can_post_messages BOOLEAN DEFAULT FALSE,
    can_edit_messages BOOLEAN DEFAULT FALSE,
    can_pin_messages BOOLEAN DEFAULT FALSE,
    can_manage_topics BOOLEAN DEFAULT FALSE,
    can_send_messages BOOLEAN DEFAULT FALSE,
    can_send_media_messages BOOLEAN DEFAULT FALSE,
    can_send_polls BOOLEAN DEFAULT FALSE,
    can_send_other_messages BOOLEAN DEFAULT FALSE,
    can_add_web_page_previews BOOLEAN DEFAULT FALSE,
    until_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (chat_id, user_id)
);

CREATE INDEX idx_chat_members_status ON chat_members (status);
CREATE INDEX idx_chat_members_user_id ON chat_members (user_id);
CREATE INDEX idx_users_username ON users (username);

CREATE TABLE vip_cache (
    user_id BIGINT PRIMARY KEY,
    is_vip BOOLEAN NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX idx_vip_cache_expires_at ON vip_cache (expires_at);

-- Add foreign key constraint to chat_permissions
ALTER TABLE chat_permissions
    ADD CONSTRAINT fk_chat_permissions_chat_id
    FOREIGN KEY (chat_id) REFERENCES chats(id) ON DELETE CASCADE;

-- Add foreign key constraint to chat_settings
ALTER TABLE chat_settings
    ADD CONSTRAINT fk_chat_settings_chat_id
    FOREIGN KEY (chat_id) REFERENCES chats(id) ON DELETE CASCADE;

-- Additional optimized indexes
CREATE INDEX idx_chats_type ON chats (type);
CREATE INDEX idx_users_is_vip ON users (is_vip) WHERE is_vip = TRUE;
CREATE INDEX idx_chat_members_active ON chat_members (chat_id, user_id)
    WHERE status IN ('administrator', 'member', 'creator');
