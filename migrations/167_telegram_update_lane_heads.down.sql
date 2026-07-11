-- Roll back the application binary before applying this migration: the new
-- claim path requires telegram_update_lanes at readiness. Lane heads are
-- derived entirely from telegram_update_inbox, so dropping them does not drop
-- ingress data and the previous binary can resume its inbox anti-join claim.
DROP TABLE IF EXISTS telegram_update_startup_jobs;
DROP TABLE IF EXISTS telegram_update_lanes;
