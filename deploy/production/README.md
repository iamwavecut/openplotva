# Production operations

Production deployment and backup are independent operations. The deployment
workflow never starts, waits for, or checks a backup. It pulls the selected image,
recreates the application, and succeeds after `/api/health` and `/api/ready` pass.

The normal fast path reuses the release image built by the successful same-repository
PR CI run when the merged `main` Git tree exactly matches that PR head. A missing or
non-matching candidate falls back to building exact `main`, so artifact reuse cannot
silently deploy different source content.
After a successful deploy, local Docker and GHCR cleanup keep only the current
main-SHA image; they do not maintain rollback image history.

`.github/workflows/backup-production.yml` runs separately on a daily schedule or by
manual dispatch. Its backup is stored under
`${OPENPLOTVA_BACKUP_ROOT:-/home/wavecut/openplotva/backups}` and contains:

- `postgres.dump` — PostgreSQL custom-format logical dump;
- `dragonfly-snapshot.tar.gz` — native Dragonfly DFS snapshot set;
- `redis-ingress.rdb` — durable Valkey ingress snapshot;
- `openplotva-state.tar.gz` — runtime TLS/application state when the volume exists;
- `SHA256SUMS` — checksums verified before the backup is accepted.

The default retention is the newest 14 complete `scheduled-*` directories.
Override it with `OPENPLOTVA_BACKUP_KEEP`.

Before restore, stop OpenPlotva and the affected dependency. Validate the
backup with `sha256sum --check SHA256SUMS`. Restore PostgreSQL with
`pg_restore --clean --if-exists --no-owner`. For Dragonfly, empty its data
volume and extract the complete DFS snapshot set into `/data`. For Valkey,
remove the existing `appendonlydir`, place the snapshot at `/data/dump.rdb`,
start Valkey once with `--appendonly no`, verify the recovered Stream, then
enable AOF with `CONFIG SET appendonly yes` and wait for
`aof_rewrite_in_progress:0`. Stop it and start again with the production
`appendonly yes`, `appendfsync always`, and `noeviction` configuration.
