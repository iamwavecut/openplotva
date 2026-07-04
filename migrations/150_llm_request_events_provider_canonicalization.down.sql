-- No-op: the up migration canonicalizes provider values in place and the
-- pre-canonicalization per-row provider is not recorded anywhere, so there is
-- nothing to restore. The canonical values remain valid regardless.
SELECT 1;
