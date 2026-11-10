# Production operations

Production deployment and backup are independent operations. The deployment
workflow never starts, waits for, or checks a backup. It pulls the selected image,
recreates the application, and succeeds after `/api/health` and `/api/ready` pass.

Every deploy builds the release binary and runtime image from the exact dispatched
`main` commit and pushes it to GHCR tagged with that commit SHA. The existing
`ghcr.io/iamwavecut/openplotva` application package is public, so the host pulls it
without registry credentials. Pull request CI does not build images. The release build cache is scoped to `main`, so a deploy with an
unchanged `Cargo.lock` recompiles only the workspace crates.
Before touching the host, the deploy waits for the CI run of the same commit after its
push to `main` and stops unless that run succeeded.
The runner sends the immutable application digest and a short-lived GitHub OIDC
identity to the host's deployment broker. Its SSH key has no arbitrary shell,
sudo, or Docker socket access. Compose settings, runtime secrets and bind mounts
are fixed in an administrator-owned profile. CI cannot upload replacement host
scripts or production environment files.

The broker preserves the previous pinned application image for rollback. Do not
delete current/previous registry versions or their OCI child manifests as a
post-deploy cleanup step. The protected release contract in `deploy/geta-deploy.json`
includes runtime templates, database migrations and sandbox protocol/code inputs.
A changed contract requires administrator review before another release.

The privileged Python sandbox is an administrator-managed dependency. Application
CI does not build, publish or activate its replacement. An administrator must
review and apply sandbox updates separately, using personal host access. Ordinary
application releases leave the existing sandbox, databases, Caddy and host
configuration unchanged.

The nightly backup runs from the administrator's crontab at 01:00 UTC, the quietest
hour of dialog traffic. The installed command invokes the fixed broker operation:

```cron
0 1 * * * sudo -n /usr/local/libexec/geta-deploy admin --project openplotva --operation backup >> <deploy-root>/backup-cron.log 2>&1
```

GitHub's scheduled workflows start hours late, so `.github/workflows/backup-production.yml`
is for manual backups only. It requests `backup` over the restricted SSH identity
with OIDC from the separately authorized backup workflow. It cannot upload or
modify the installed backup helper. The broker serializes backup and application
rollout operations. New backups are root-only under
`/var/lib/geta-deploy/backups/openplotva` and contain:

- `postgres.dump`: PostgreSQL custom-format logical dump;
- `dragonfly-snapshot.tar.gz`: native Dragonfly DFS snapshot set;
- `redis-ingress.rdb`: durable Valkey ingress snapshot;
- `openplotva-state.tar.gz`: runtime TLS/application state;
- `deployment-control.tar.gz` and `broker-project.json`: this project's protected
  runtime profile and exact image history;
- `SHA256SUMS`: checksums verified before the backup is accepted.

The previous complete backup is retained until its replacement is verified. Only
the newest verified backup is then kept in the new backup root. Existing legacy
backups are not deleted during migration.

The uploader's media volume has a separate daily retention job on the production host:

```sh
sudo install -o root -g root -m 0644 tools/uploader-retention.cron \
  /etc/cron.d/openplotva-uploader-retention
```

This is a system crontab with a user field; do not install it with `crontab`.
At 04:30 UTC it deletes regular files at least eight complete days old, preserving
the existing `find -mtime +7` policy. Old message media URLs expire with their
files. The job does not follow symlinks or cross filesystems, uses idle I/O
priority, and has a five-minute deadline and a lock against overlapping runs.

LLM event retention runs at startup and daily. Each pass archives complete UTC
days older than `LLM_REQUEST_EVENTS_RETENTION_DAYS` once, then drains expired raw
events in committed batches of 10,000 with 200 ms pauses. The cutoff stays fixed
throughout the pass; the boundary day remains raw until it can be archived in
full. Archive failures preserve the raw events and retry after five minutes.

To exercise backlog draining, archive rollback, restart, and retention boundaries,
use an empty PostgreSQL database whose name starts with `openplotva_retention_test_`:

```sh
OPENPLOTVA_RETENTION_TEST_DATABASE_URL=postgres://localhost/openplotva_retention_test_run \
  cargo test -p openplotva-app \
  cleanup_drains_multiple_batches_without_losing_rollups_or_recent_events -- --ignored
```

Before restore, stop OpenPlotva and the affected dependency. Validate the
backup with `sha256sum --check SHA256SUMS`. Restore PostgreSQL with
`pg_restore --clean --if-exists --no-owner`. For Dragonfly, empty its data
volume and extract the complete DFS snapshot set into `/data`. For Valkey,
remove the existing `appendonlydir`, place the snapshot at `/data/dump.rdb`,
start Valkey once with `--appendonly no`, verify the recovered Stream, then
enable AOF with `CONFIG SET appendonly yes` and wait for
`aof_rewrite_in_progress:0`. Stop it and start again with the production
`appendonly yes`, `appendfsync always`, and `noeviction` configuration.

## Image generation advertising

Create a separate Gradius project of type **Generation model** for `@PlotvoBot`.
An administrator stores its `GRADIUS_GENERATION_API_KEY` in the protected runtime
profile and enables `GRADIUS_UTILITY_IMAGE_ENABLED`. Application CI does not
provision or overwrite these credentials. Changes to a legacy `.env.production`
do not update an already frozen broker profile. Preserve the existing dialogue
key and other runtime settings when changing the profile. The Gradius master
switch (`GRADIUS_ENABLED`) must also be enabled. If the generation key is
missing, image ads stay disabled; the dialogue key is never used as a fallback.

Images use `POST /v1/native/generation_model/chat` with a redacted prompt and
stable synthetic user/chat IDs. Ads are offered only after a successful image
result, with a verified non-VIP initiator. Privacy, VIP lookup, provider, or
rendering failures skip the ad without affecting the image.

The ad is a separate reply to the resulting image in the initiating user's
private chat with the bot. All Gradius integrations, including dialogue text,
media banners, rates and check-in placements, reject groups and channels before
VIP lookups, privacy redaction or advertising requests. Group membership and
administrator privileges do not permit advertising. The enable switches cannot
override this restriction.

Image ad limits are per user: one accepted impression per hour and at most ten
in a rolling 24 hours. Historical utility image impressions retain these limits.

Dialogue media banners preserve the provider's sanitized rich HTML, including
HTTPS images and collages, and URL buttons. They are sent as a separate
`sendRichMessage` after the answer. The provider selects whether to return a
text placement or a media banner. A text placement does not imply a missing
image. Generation and utility placements use their separate text-ad contracts.
