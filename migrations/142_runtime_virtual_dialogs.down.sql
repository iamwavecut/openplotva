-- Source SHA-256: dd259b35af3e3f6f2bd630ed667b20460098a229e93ee12b4d51f6172177d0fe
-- Source: runtime-only Rust schema addition for runtime virtual dialogs; no frozen Go migration source.

DROP INDEX IF EXISTS idx_runtime_virtual_dialogs_expires_at;
DROP TABLE IF EXISTS runtime_virtual_dialogs;
DROP SEQUENCE IF EXISTS runtime_virtual_dialog_id_seq;
