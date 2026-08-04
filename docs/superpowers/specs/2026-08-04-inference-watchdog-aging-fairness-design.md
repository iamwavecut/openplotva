# Inference Watchdog and Accelerator Aging Fairness

## Goal

Keep OpenPlotva's image workers recoverable when an AI Farm request stops making progress, prevent sustained accelerator traffic from starving older work forever, and preserve a ten-minute VIP escape above every non-VIP class.

## Scope

This is one OpenPlotva PR. It changes the Taskman accelerator claim policy and the OpenPlotva-to-Discovery draw client. It does not restart or kill the separate AI Farm `draw-api` process; that process is owned by another repository. The watchdog instead guarantees that a stuck HTTP future cannot retain an OpenPlotva image worker indefinitely.

## Inference watchdog

Every Discovery draw attempt receives one absolute deadline covering submission and all status polls. The existing routed provider timeout remains the source of the deadline; the default is 300 seconds. Each transport future is polled through `tokio::time::timeout_at`, so a stalled connect, request body, or status response is cancelled when the deadline expires.

The failure is reported as a provider timeout containing the job ID and phase. Existing image retry classification and attempt limits remain authoritative. No independent requeue loop is introduced, avoiding duplicate ownership of a still-running Taskman job.

## Aging fairness

ASR, VIP image, and regular image jobs participate in one deterministic accelerator ranking at claim time:

- Base priorities are ASR `6`, VIP image `5`, and regular image `0`.
- A ready or processing accelerator job gains one effective-priority point per five minutes since creation.
- ASR and regular image work are capped at effective priority `6`.
- VIP image work reaches `6` after five minutes and its dedicated ceiling `7` after ten minutes.
- Legacy persisted `image-vip` jobs with base priority `4` are treated as base priority `5` during claim arbitration, so deployment does not delay jobs already waiting.
- Ties use the existing oldest-created, then lowest-ID order.

Therefore an old regular job reaches parity with ASR after 30 minutes but can never reach VIP's ten-minute priority `7`. A VIP job may remain behind an older priority-6 job before ten minutes; at ten minutes it outranks every ASR and regular job. These are eligibility guarantees, not generation-completion guarantees. Future-dated timestamps receive no boost.

The ranking applies to ASR claims as well as image claims. Fresh ASR still preempts fresh image work. Once an aged image ranks first, new ASR claims wait until that aged work has been claimed and no longer ranks ahead. Existing already-running work is never cancelled.

## Compatibility

Queue names, job payloads, WAL format, provider routing, retries, Telegram delivery, and database schemas remain unchanged. Newly scheduled VIP image jobs persist priority `5`; existing priority-`4` VIP image jobs are normalized only for claim ranking. Aging is computed at claim time and requires no migration or new persisted fields.

## Verification

Regression tests must prove:

- a hanging draw submission returns through the watchdog instead of pinning the caller;
- a hanging draw status poll returns through the same deadline;
- fresh ASR still precedes fresh VIP and regular work;
- new VIP image jobs persist base priority `5`;
- VIP remains at most priority `6` at `09:59` and reaches priority `7` at `10:00`;
- legacy priority-`4` VIP image jobs receive the same `10:00` priority-`7` claim behavior;
- regular reaches ASR parity at priority `6` after 30 minutes but never exceeds it;
- an aged processing draw prevents fresh ASR from extending starvation;
- all existing Taskman and image-job tests remain green.
