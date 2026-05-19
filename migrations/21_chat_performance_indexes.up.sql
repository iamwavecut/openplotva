-- Source SHA-256: 65ad135c49c955874b50dcf2adbe85399a1edd1599493105bfe0ffe437885890

CREATE INDEX IF NOT EXISTS idx_chat_game_stats_top_winners
	ON chat_game_stats (chat_id, wins_count DESC, last_win_at DESC);

CREATE INDEX IF NOT EXISTS idx_chat_game_results_chat_id_won_at_desc
	ON chat_game_results (chat_id, won_at DESC);

CREATE INDEX IF NOT EXISTS idx_chat_game_results_chat_id_won_date
	ON chat_game_results (chat_id, won_on_date, won_at DESC);

CREATE INDEX IF NOT EXISTS idx_chat_permissions_retry_window
	ON chat_permissions (last_checked_at)
	WHERE last_error_at IS NOT NULL AND (error_count IS NULL OR error_count > 0);

CREATE INDEX IF NOT EXISTS idx_documents_without_embedding
	ON documents (id)
	WHERE embedding IS NULL;
