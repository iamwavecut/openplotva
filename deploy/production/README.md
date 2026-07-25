# Production operations

`deploy-production.sh` creates a verified pre-deploy backup before pulling or
recreating runtime containers. The backup is stored under
`${OPENPLOTVA_BACKUP_ROOT:-/home/wavecut/openplotva/backups}` and contains:

- `postgres.dump` — PostgreSQL custom-format logical dump;
- `dragonfly-snapshot.tar.gz` — native Dragonfly DFS snapshot set;
- `redis-ingress.rdb` — durable Valkey ingress snapshot;
- `openplotva-state.tar.gz` — runtime TLS/application state when the volume exists;
- `SHA256SUMS` — checksums verified before the backup is accepted.

The default retention is the newest 14 complete `predeploy-*` directories.
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
