-- Source SHA-256: 511aa2a9778efeaffd198a7db497986a42e099764ef3024b6edca3bf30338607

DROP INDEX IF EXISTS idx_chats_username_trgm;
DROP INDEX IF EXISTS idx_chats_title_trgm;

DROP INDEX IF EXISTS idx_users_last_name_trgm;
DROP INDEX IF EXISTS idx_users_first_name_trgm;
DROP INDEX IF EXISTS idx_users_username_trgm;
