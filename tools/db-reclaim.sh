#!/usr/bin/env bash
# One-time post-deploy disk reclaim for the OpenPlotva production Postgres.
#
# NOT run automatically. Run by an operator AFTER:
#   1. the retention migrations (132-141) have deployed (index drops + LZ4 +
#      message_id_map/message_ops_queue drop are instant on deploy), AND
#   2. the retention WORKERS have drained their backlogs (telegram_files and
#      whitecircle_checks delete aged rows over the first hours/days), AND
#   3. a fresh pg_dump / snapshot exists.
#
# Index drops, the LZ4 column setting, and the dropped virtual-message tables
# reclaim space immediately on deploy. This script reclaims the HEAP/INDEX
# high-water space left behind by the bulk retention DELETEs.
#
# Strategy: prefer pg_repack (online, no ACCESS EXCLUSIVE) when installed; else
# fall back to online VACUUM + REINDEX CONCURRENTLY. Never VACUUM FULL on the hot
# tables. Each step prints before/after total size.
set -euo pipefail

CONTAINER="${PG_CONTAINER:-openplotva-postgresql-1}"
DB_USER="${PG_USER:-plotva}"
DB_NAME="${PG_DB:-plotva}"
TABLES=(telegram_files whitecircle_checks users llm_request_events)

psql() { docker exec -i "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" "$@"; }
size_of() { psql -At -c "SELECT pg_size_pretty(pg_total_relation_size('$1'));"; }

echo "== OpenPlotva DB reclaim =="
echo "container=$CONTAINER db=$DB_NAME"
echo "Confirm a current backup exists before continuing. Ctrl-C to abort; Enter to proceed."
read -r _

if docker exec -i "$CONTAINER" sh -lc 'command -v pg_repack >/dev/null 2>&1'; then
  echo "pg_repack found -> online repack."
  for t in "${TABLES[@]}"; do
    echo "--- $t (before: $(size_of "$t")) ---"
    docker exec -i "$CONTAINER" pg_repack -U "$DB_USER" -d "$DB_NAME" -t "$t" || echo "  pg_repack failed for $t (continuing)"
    echo "    after: $(size_of "$t")"
  done
else
  echo "pg_repack NOT installed in the image. Falling back to online VACUUM + REINDEX CONCURRENTLY."
  echo "(For a full file-size shrink, install pg_repack or schedule a maintenance window for VACUUM FULL.)"
  for t in "${TABLES[@]}"; do
    echo "--- $t (before: $(size_of "$t")) ---"
    psql -c "VACUUM (ANALYZE) $t;"
    psql -c "REINDEX TABLE CONCURRENTLY $t;" || echo "  REINDEX CONCURRENTLY failed for $t (continuing)"
    echo "    after: $(size_of "$t")"
  done
fi

echo "== done =="
