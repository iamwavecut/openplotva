-- source: runtime-only/openplotva-rust/runtime_virtual_dialogs @ dd259b35af3e3f6f2bd630ed667b20460098a229e93ee12b4d51f6172177d0fe
-- source note: runtime-only Rust schema addition for runtime virtual dialogs; no frozen Go migration source exists.

DROP INDEX IF EXISTS idx_runtime_virtual_dialogs_expires_at;
DROP TABLE IF EXISTS runtime_virtual_dialogs;
DROP SEQUENCE IF EXISTS runtime_virtual_dialog_id_seq;
