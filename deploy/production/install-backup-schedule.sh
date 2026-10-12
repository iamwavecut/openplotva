#!/usr/bin/env bash
# Install the nightly production backup into the deploy user's crontab.
#
# The backup runs at 01:00 UTC (04:00 Moscow), the quietest hour of dialog traffic: a
# full pg_dump evicts the database page cache for one to three hours, so it must not
# overlap daytime load. GitHub's scheduled workflows start hours late, which is why the
# schedule lives on the host. The block is replaced on every run, so the installer is
# idempotent.
set -euo pipefail

deploy_root="${OPENPLOTVA_DEPLOY_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
schedule="${OPENPLOTVA_BACKUP_CRON_SCHEDULE:-0 1 * * *}"
begin_marker="# BEGIN openplotva-backup (managed by install-backup-schedule.sh)"
end_marker="# END openplotva-backup"

[[ -x "${deploy_root}/backup-production.sh" ]] || {
  printf 'openplotva backup schedule error: %s/backup-production.sh is missing or not executable\n' "$deploy_root" >&2
  exit 1
}

entry="${schedule} flock -n ${deploy_root}/.backup.lock ${deploy_root}/backup-production.sh > ${deploy_root}/backup-cron.log 2>&1"

current="$(crontab -l 2>/dev/null || true)"
without_block="$(printf '%s\n' "$current" | awk -v begin="$begin_marker" -v end="$end_marker" '
  $0 == begin { skip = 1; next }
  $0 == end { skip = 0; next }
  !skip { print }
')"

{
  [[ -n "${without_block//[[:space:]]/}" ]] && printf '%s\n' "$without_block"
  printf '%s\n%s\n%s\n' "$begin_marker" "$entry" "$end_marker"
} | crontab -

printf '+ backup schedule: %s (host time %s)\n' "$schedule" "$(date +%Z)"
