---
name: openplotva-period-audit
description: Produce reproducible OpenPlotva interaction, generation, or advertising statistics for a fixed time window, including follow-ups since a previous audit. Define cohorts, deduplicate attempts, and distinguish completed work from confirmed Telegram delivery.
---

# OpenPlotva Period Audit

Answer the requested period question from a consistent read-only slice. Keep the metric definitions and ending timestamp reusable for the next audit.

## Define the slice

- Resolve the topic, target chats/cohort, requested breakdowns, exact UTC start/end, and display timezone. Default display to Europe/Warsaw when the user has not chosen another. Record whether each endpoint is inclusive; use the previous report's exact end for a follow-up and avoid counting that boundary twice.
- Distinguish calendar months, rolling durations, and calendar-day windows. Flag an incomplete final day and exclude it from full-day averages unless explicitly included. Handle timezone/DST day boundaries explicitly.
- Define the unit before writing queries: user interaction, persisted job, provider attempt, advertising opportunity, or confirmed delivery. Identify the source key and timestamp for each count. Retries and multiple provider calls do not create additional user interactions.
- Verify the deployed revision and schema. Read migrations and the owning query code in `openplotva-storage`; do not reuse historical report SQL as a current schema contract.

## Retrieve consistently

- Use the project-local [openplotva-runtime-api](../openplotva-runtime-api/SKILL.md) for the supported connection and read-only inspection. Load it before runtime API access; tokens, TLS pins, and runtime exposure must be resolved for the current deployment.
- If one SQL result can contain the required aggregates, prefer that bounded read. `sqlRead` calls are independent; they do not establish a shared multi-query snapshot. Check `truncated` and row limits before treating results as complete.
- For an authorized direct Postgres read requiring several dependent queries, use a read-only repeatable-read transaction, bounded statement timeouts, and one fixed observation end. Do not use writes, temporary production tables, migrations, or service restarts to collect statistics.
- Query aggregates and minimal correlation fields. Keep payloads, tokens, private file IDs, full dumps, and user messages out of saved/public outputs. Save sanitized SQL with parameter meanings so the same calculation can be repeated.

## Choose evidence for the question

| Question | Counting and verification |
|---|---|
| Dialog interactions | Count the interaction key once. Classify text delivery, generation handoff, intentional suppression/deferment, unresolved delivery, and failures using current lifecycle definitions. An unconfirmed answer is not automatically an error. |
| Generated media | Separate requests, distinct jobs, retries, provider results, and delivered outputs. A completed image/music job does not prove Telegram received its output. |
| Advertising | Count opportunities once, provider attempts separately, then no-ad/error/received-ad outcomes and Telegram receipts. Keep private, public-group, and targeted ephemeral surfaces distinct. |
| Post-release health | Separate work created before release from work created in the observation window; label backlog recovery separately. Verify the affected user path alongside the runtime revision. |

For implementation discovery, inspect current `dialog_delivery_obligations`, taskman/job lifecycle, Telegram outbox, Gradius opportunity/API-call records, and their owning modules/migrations. Treat those names as discovery leads; verify exact tables, keys, status values, and retention in the deployed schema.

## Validate the interpretation

- Preaggregate or deduplicate one-to-many joins before summing. Reconcile overall totals with daily, chat-type, and surface breakdowns; explain overlapping categories rather than forcing them to sum.
- Separate queued, completed, delivered, ambiguous, retrying, and failed states where supported. Telegram acknowledgement proves accepted delivery, not views, clicks, or revenue. Ledger fields and receipts must agree before calling an ad delivered.
- For targeted ephemeral delivery, check receiver correlation and the correct receipt type. Do not treat a sentinel ordinary message ID as a normal Telegram message.
- Check the current VIP/eligibility, per-user, rolling-day, and public-group limits only when requested or material to the audit. Include reservations and pending/ambiguous recovery state; inspect earlier rows needed for a cross-window limit without adding them to the reported period totals.
- Compare provider outcomes and latency only across the same cohort. An error occurring near a timeout threshold is not proof of a timeout; use the recorded transport error. State denominators and missing observations.

## Result and stopping condition

Return the window, deployed revision/schema evidence, metric definitions, totals and requested breakdowns, violations/errors, and unmeasured outcomes. Include the exact end timestamp and counting convention for follow-ups. Store a report and sanitized queries in the task's permitted artifact directory when useful; do not write private audit data into the repository.

Stop after the totals reconcile and the requested distinctions are supported, or identify the retention/access gap that prevents them. This workflow does not create a recurring schedule, send test messages, replay provider calls, repair data, or deploy changes.
