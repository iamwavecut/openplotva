-- Source SHA-256: 0ab5e127e274b3d2a5c77e7e524985c29d35bc1ad56b108321e733f3938ec038

DROP FUNCTION IF EXISTS drop_expired_chat_history_partitions(date);
DROP FUNCTION IF EXISTS ensure_chat_history_partition(date);
