# Inference Watchdog and Accelerator Aging Fairness

## Goal

Keep OpenPlotva's image workers recoverable when an AI Farm request stops making progress, and prevent sustained ASR or VIP traffic from starving older image work forever.

## Scope

This is one OpenPlotva PR. It changes the Taskman accelerator claim policy and the OpenPlotva-to-Discovery draw client. It does not restart or kill the separate AI Farm `draw-api` process; that process is owned by another repository. The watchdog instead guarantees that a stuck HTTP future cannot retain an OpenPlotva image worker indefinitely.

## Inference watchdog

Every Discovery draw attempt receives one absolute deadline covering submission and all status polls. The existing routed provider timeout remains the source of the deadline; the default is 600 seconds. Each transport future is polled through `tokio::time::timeout_at`, so a stalled connect, request body, or status response is cancelled when the deadline expires.

The failure is reported as a provider timeout containing the job ID and phase. Existing image retry classification and attempt limits remain authoritative. No independent requeue loop is introduced, avoiding duplicate ownership of a still-running Taskman job.

## Aging fairness

ASR, VIP image, and regular image jobs participate in one deterministic accelerator ranking at claim time:

- Base priorities stay unchanged: ASR `6`, VIP `4`, regular `0`.
- A ready or processing accelerator job gains one effective-priority point per five minutes since creation.
- Effective priority is capped at `6`.
- Ties use the existing oldest-created, then lowest-ID order.

Therefore an old VIP job reaches parity with fresh ASR after 10 minutes, and an old regular job reaches parity after 30 minutes. These are eligibility guarantees, not generation-completion guarantees. Future-dated timestamps receive no boost.

The ranking applies to ASR claims as well as image claims. Fresh ASR still preempts fresh image work. Once an aged image ranks first, new ASR claims wait until that aged work has been claimed and no longer ranks ahead. Existing already-running work is never cancelled.

## Compatibility

Queue names, persisted priorities, job payloads, WAL format, provider routing, retries, Telegram delivery, and database schemas remain unchanged. Aging is computed at claim time and requires no migration or new persisted fields.

## Verification

Regression tests must prove:

- a hanging draw submission returns through the watchdog instead of pinning the caller;
- a hanging draw status poll returns through the same deadline;
- fresh ASR still precedes fresh VIP and regular work;
- VIP reaches ASR parity after 10 minutes;
- regular reaches ASR parity after 30 minutes but not before;
- an aged processing draw prevents fresh ASR from extending starvation;
- all existing Taskman and image-job tests remain green.
