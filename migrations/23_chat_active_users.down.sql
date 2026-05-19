-- Source SHA-256: ddd534513d7301f7fa9817022bdba61d901ab49e3fe88532ffbfa131bd926521

DROP INDEX IF EXISTS idx_chat_active_users_last_active_at;
DROP INDEX IF EXISTS idx_chat_active_users_chat_id_last_active_at;
DROP TABLE IF EXISTS chat_active_users;
