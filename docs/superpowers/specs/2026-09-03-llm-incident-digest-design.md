# LLM Incident Digest Design

## Problem

The current routing reporter emits one fixed English Telegram message per
actionable dedupe key every ten minutes. Production recorded 44,657 routing
events and 237 sent admin reports in the 24 hours observed on 2026-09-03. The
messages identify numeric provider/model IDs but do not aggregate impact,
resolve human identities, explain the pipeline, or update when an incident
becomes stale.

## Goals

- Give an operator enough context to decide whether users are affected and
  where to investigate: failure class, likely cause, pipeline, operation,
  provider, model, counts, first/last occurrence, and bounded user/chat/job
  samples.
- Send at most one new digest message per admin in any rolling 60-minute
  interval. Update that message silently between rotations.
- Remove incident groups automatically when their last occurrence leaves the
  rolling 60-minute window. Turn the existing message into a concise recovered
  state when the window becomes empty.
- Preserve routing events as the durable diagnostic source and preserve the
  existing actionable-event policy, including suppression of a single
  retryable attempt that remains inside a job retry budget.
- Survive process restarts without forgetting the Telegram message ID or the
  new-message rate limit.
- Keep raw prompts, model responses, provider payloads, Redis values, keys, and
  tokens out of Telegram reports.

## Non-goals

- Replacing the runtime routing dashboard or the underlying event store.
- Sending raw provider bodies or arbitrary `detail` JSON to Telegram.
- Adding a separate alerting service, queue, or dependency.
- Automatically diagnosing a root cause beyond the stable failure classes the
  runtime has actually observed.

## Chosen Architecture

The routing event recorder remains the source of truth. Each event gains an
optional typed `user_id`; existing `chat_id`, job, thread, message, provider,
model, workflow, and detail fields remain compatible. The reporter only marks
an event as `aggregated` or `none` and wakes a dedicated report worker. It no
longer sends Telegram messages inline.

The report worker queries one rolling hour from PostgreSQL and aggregates by
the existing routing dedupe key. PostgreSQL computes exact occurrence and
distinct-impact counts while returning at most three recent context samples
per group. Provider/model names come from the routing catalog; user/chat names
come from the effective Telegram identity views. This query runs on startup,
on a one-minute tick, and after an actionable-event wake-up delayed long enough
for the five-second routing-event writer flush.

One small `llm_admin_report_state` row per admin stores the current Telegram
message ID, the last successful new-message time, the last rendered
fingerprint, the latest new-message claim, and any in-flight dispatcher operation. This state prevents
restart-driven duplicates. A stale in-flight marker expires after ten minutes
so a crash cannot suppress reporting forever.

The pending-operation write is an atomic conditional claim. It checks the
stale-pending rule, five-minute delivery retry floor, 60-minute new-message
gate, and expected edit target in PostgreSQL before returning ownership. This
also prevents duplicate sends if two application revisions overlap during a
rolling deployment.

A send claim itself reserves the hourly slot until its outcome is known. A
failure that proves Telegram did not accept the request releases that
reservation; an ambiguous transport result, a successful response without a
recoverable message ID, or a lost database receipt keeps it for 60 minutes.
This closes the failure window between Telegram acceptance and receipt
persistence without weakening the anti-spam contract.

Both sends and edits continue through the existing dispatcher. Report
operations carry a namespaced virtual ID. The dispatcher persists the result
of those operations back to `llm_admin_report_state` before declaring its work
item complete, so a successful `sendMessage` supplies the real message ID used
by later `editMessageText` calls.

## Delivery State Machine

For each admin:

1. If the snapshot is empty and there is no current report message, do
   nothing; recovery never creates a notification by itself.
2. If an operation is in flight, do nothing until its dispatcher result is
   recorded or the ten-minute stale timeout passes.
3. If the freshly rendered text matches the last successful fingerprint, do
   nothing.
4. If there is no current message and a new message was successfully sent less
   than 60 minutes ago, wait; this preserves the hard send-rate limit even when
   an edit target was lost.
5. While incidents are active, send a new message when there is no editable
   message and the send interval permits it, or when the snapshot contains an
   occurrence at least 60 minutes newer than the last successful new message.
6. Otherwise edit the current message. Edits are non-notifying and happen at
   most once per one-minute refresh because identical fingerprints are skipped.
7. On a successful send, persist its Telegram message ID and send time. On a
   successful edit, persist only the new fingerprint. On a failed operation,
   clear the in-flight marker and wait five minutes before retrying. A terminal
   edit failure also clears the unusable message ID, but it does not bypass the
   60-minute new-message limit.

## Report Content

The digest is plain Telegram text and is capped below 4,096 bytes. It has:

- an adaptive status: user-facing failures continuing, background degradation,
  quieting down, or recovered;
- the rolling-window totals and distinct affected users, chats, and jobs;
- up to five ranked groups, with user-linked groups first, then severity,
  count, and recency;
- for each group: a readable operation and exact workflow key, stable cause
  label and code, provider/model names when known, occurrence and impact
  counts, first/last timestamps, and up to three bounded identity samples;
- an omitted-group count and a pointer to Runtime API routing events when the
  digest has more data than Telegram can safely carry.

Example shape:

```text
🔴 LLM: сбои затрагивают пользователей
За 60 мин: 46 событий · 12 пользователей · 9 чатов · 14 задач
Последний сбой: 16:21 UTC

1. Ответ в диалоге · dialog · 31 раз
Причина: провайдер перегружен (provider_overloaded)
Маршрут: vram-cloud → vram.cloud/qwen3.6-27b
Затронуто: 8 пользователей · 6 чатов · 10 задач
Контекст: @alice (42), «Plotva Lab» (-100…), job 3501765
Период: 15:43–16:21 UTC

Ещё 2 группы — Runtime API → routingEvents
```

When no actionable group remains, the same message becomes:

```text
🟢 LLM: за последние 60 минут сбоев нет
Отчёт обновлён после восстановления. Новых сообщений не будет до следующего сбоя.
```

## Event and Privacy Rules

- `user_id` is optional and additive in the database and runtime GraphQL
  contracts. Dialog and media call sites populate it when they already own the
  identity; background workflows leave it null.
- The query includes only the existing actionable event types. Explicit
  `admin_actionable=false` stays excluded. A one-attempt retryable exhaustion
  stays excluded unless the producer explicitly sets `admin_actionable=true`.
- Human names are resolved only from local PostgreSQL identity views and are
  delivered only to configured admins.
- Causes come from the allowlisted stable fields `last_retryable_reason`,
  `retryable_reason`, `reason`, and the sanitized infrastructure `error` used
  by routing reload/backfill events. Other detail keys never enter the report.
- Every dynamic value is compacted and bounded. Registered secrets are
  redacted before formatting.

## Failure Handling

- A routing-event database write failure remains logged and does not block the
  user request.
- A report snapshot query or state write failure is logged and retried on the
  next tick; it cannot affect LLM routing.
- Dispatcher enqueue and delivery failures leave the event data intact. The
  five-minute retry floor prevents a broken Telegram destination from becoming
  its own loop.
- The worker shuts down before the dispatcher drain. Dispatcher result writes
  are performed in the dispatcher path itself, so a late send receipt is not
  lost during shutdown.

## Verification

- Unit tests prove actionable filtering, aggregation ranking, adaptive status,
  safe bounded formatting, stale-group removal, the 60-minute send gate, edit
  reuse, identical-render suppression, and failed-delivery recovery.
- Storage tests prove the new `user_id` bind/select contract and report-state
  transition SQL. A representative PostgreSQL integration test exercises the
  migration and aggregate query where the repository test harness permits it.
- Dispatcher tests prove a report virtual ID records the real Telegram message
  ID and that an edit result advances the fingerprint without rotating the
  message.
- Local completion requires `cargo fmt --all`, workspace Clippy with warnings
  denied, focused storage/app tests, and the workspace test suite.
- Production completion requires the merged exact-tree image, migration 184,
  healthy/readiness checks, no restart or fresh error regression, one real
  report send receipt, and a later refresh that changes the fingerprint while
  retaining the same Telegram message ID and `last_new_message_at`. If no
  natural actionable event occurs during the bounded soak, exact-tree proof
  plus the targeted red/green tests is reported instead of injecting a fake
  user-facing incident.
