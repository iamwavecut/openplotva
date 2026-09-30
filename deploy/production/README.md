# Production operations

Production deployment and backup are independent operations. The deployment
workflow never starts, waits for, or checks a backup. It pulls the selected image,
recreates the application, and succeeds after `/api/health` and `/api/ready` pass.

Every deploy builds the release binary and runtime image from the exact dispatched
`main` commit and pushes it to GHCR tagged with that commit SHA. Pull request CI does
not build images. The release build cache is scoped to `main`, so a deploy with an
unchanged `Cargo.lock` recompiles only the workspace crates.
Before touching the host, the deploy waits for the CI run of the same commit after its
push to `main` and stops unless that run succeeded.
After a successful deploy, local Docker and GHCR cleanup keep only the current
main-SHA image; they do not maintain rollback image history.

`.github/workflows/backup-production.yml` runs separately on a daily schedule or by
manual dispatch. Its backup is stored under
`${OPENPLOTVA_BACKUP_ROOT:-<deploy-root>/backups}` and contains:

- `postgres.dump` — PostgreSQL custom-format logical dump;
- `dragonfly-snapshot.tar.gz` — native Dragonfly DFS snapshot set;
- `redis-ingress.rdb` — durable Valkey ingress snapshot;
- `openplotva-state.tar.gz` — runtime TLS/application state when the volume exists;
- `SHA256SUMS` — checksums verified before the backup is accepted.

The default retention is the newest 14 complete `scheduled-*` directories.
Override it with `OPENPLOTVA_BACKUP_KEEP`.

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
Store its key in the repository Actions secret `GRADIUS_GENERATION_API_KEY`.
The deployment workflow streams this key over SSH on stdin, installs it into
`.env.production` with mode 0600, and enables `GRADIUS_UTILITY_IMAGE_ENABLED`.
It preserves the existing dialogue key and other runtime settings. The Gradius
master switch (`GRADIUS_ENABLED`) must also be enabled. If the generation key is
missing, image ads stay disabled; the dialogue key is never used as a fallback.

Images use `POST /v1/native/generation_model/chat` with a redacted prompt and
stable synthetic user/chat IDs. Ads are offered only after a successful image
result, with a verified non-VIP initiator. Privacy, VIP lookup, provider, or
rendering failures skip the ad without affecting the image.

The ad is a separate reply to the resulting image. Private-chat ads remain.
For group ads, the bot's current membership is fetched from Telegram before
requesting an advertisement. Administrators use native Bot API ephemeral messages, with
`ephemeral_message_parameters.receiver_user_id` set to the image initiator.
Only that user and the bot can see the advertisement. If the bot is a regular
group member, it sends an ordinary public reply that remains in the chat.
Neither mode schedules deletion. A failed membership lookup skips advertising;
a rejected ephemeral send is not retried as a public message. Telegram controls
expiry and does not guarantee delivery to offline users. API acknowledgement
is the available receipt, not proof that the user viewed the advertisement.

Image ad limits are per user: one accepted impression per hour and at most ten
in a rolling 24 hours across private chats, groups, and topics. Historical
utility image impressions retain these limits. Other users in the same group
have independent eligibility. Ephemeral replies retain the receiver during
outbox replay and do not enter shared conversation history.

Public image ads additionally share a 15-minute gap across all users and topics
in the group. Pending public deliveries reserve the group slot atomically;
confirmed deliveries start the gap, and an ambiguous send conservatively holds
the slot for 15 minutes. Failed sends free the slot. Native ephemeral and private
ads do not consume it. Ads blocked by this limit are skipped, not deferred.
The group limit is checked before calling Gradius and checked again atomically
when enqueueing to prevent simultaneous requests from different users sending
multiple public advertisements. Migration 191 adds a concurrent index for this
lookup and preserves existing records and readers.

References: [Telegram Bot Features](https://core.telegram.org/bots/features#ephemeral-messages)
and [Bot API Reference](https://core.telegram.org/bots/api#ephemeral-messages-and-commands).
