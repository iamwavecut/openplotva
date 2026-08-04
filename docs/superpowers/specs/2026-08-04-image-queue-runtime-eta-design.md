# Runtime Image Queue ETA

## Goal

Replace the user-visible image queue ETA with a runtime estimate derived from recent sustained drawing throughput instead of multiplying historical queue age by the current queue depth.

## Root cause and evidence

The existing `clean_queue_stats` sample is `completed_at - created_at`. For image jobs, `completed_at` currently represents the poll timestamp at which execution started, so this sample is almost entirely time already spent waiting in the queue. `estimate_queue_time_for_depth` then treats that queue age as one job's service time and multiplies it by the number of jobs ahead. A large backlog therefore counts prior waiting again for every queued job and quickly reaches the four-hour display cap.

Production evidence on 2026-08-04 showed the mismatch directly:

- the latest 50 completed image jobs averaged 8,805 seconds from enqueue to execution start;
- their observed execution cycle averaged 49 seconds, with a 46-second median;
- the latest 20 continuously backlogged regular-image cycles produced an outlier-trimmed 45-second average;
- at a depth of 68, the current estimator displayed four hours while the recent throughput implied roughly 51 minutes.

## Estimator

Image queue statistics use the persisted `execution_started_at` timestamps already restored into the in-memory Taskman snapshot. For one queue:

1. Sort completed jobs by execution start time.
2. Form consecutive start-to-start intervals.
3. Keep an interval only when the later job was already created by the earlier start time. This proves the queue had work available and prevents idle gaps from being interpreted as slow drawing.
4. Keep the most recent 20 eligible intervals.
5. Compute continuous p5 and p95 thresholds, discard values outside them, and require at least five clean intervals.
6. Use the clean mean as the observed seconds per completed queue slot.

The existing estimate formula remains `clean mean × jobs ahead ÷ active workers`, with the existing 30-second floor and four-hour safety cap. If runtime history is insufficient, the existing static fallback remains authoritative.

Start-to-start throughput is preferred over a Postgres query on every notice because it keeps the Telegram scheduling path available during storage degradation. It is preferred over raw provider duration because it captures the effective cycle users observe, including result delivery, polling cadence, and competing shared-accelerator work. The backlog predicate prevents ordinary idle periods from polluting that rate.

## Scope and compatibility

This is one OpenPlotva PR. It changes only the image/text clean-stat sampling implementation and its regression tests; the queue-notice caller continues using `estimate_queue_time_for_depth` and therefore receives the corrected value without a new integration contract.

No database migration, persisted-field change, new dependency, queue-name change, Telegram payload change, or provider request change is required. Text queues without execution-start samples continue to use their existing enqueue-to-completion samples, preserving their current behavior.

## Verification

Regression tests must prove:

- long historical queue age does not inflate a backlogged image cycle;
- consecutive image starts yield the expected ETA for a literal queue depth;
- idle gaps are excluded from image throughput;
- p5/p95 trimming still removes recent outliers;
- fewer than five clean image intervals retain the existing fallback;
- the existing queue-notice integration renders the corrected runtime estimate;
- all Taskman, focused app, workspace clippy, and workspace tests remain green.
