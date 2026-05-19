-- Source SHA-256: 685a8477172815b0edca6968a3295cfeedc550686bf7be084d2d1607f5da8316

CREATE INDEX IF NOT EXISTS idx_telegram_files_latest_file_id
	ON telegram_files (latest_file_id);
