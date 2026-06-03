---
name: openplotva-runtime-api
description: Use this skill to inspect a deployed OpenPlotva Rust bot through the runtime GraphQL API over pinned self-signed HTTPS with a bearer token.
---

# OpenPlotva Runtime API

Use this skill when inspecting the deployed Rust bot through the runtime debug API.

## Connection

Default base URL:

```text
https://100.77.77.51:9091
```

Production may publish the runtime API on another host, for example
`https://geta.moe:9091`, when `OPENPLOTVA_RUNTIME_API_PUBLISH_HOST` is set in the
server-local env. Verify the live published address before concluding the
runtime API is down.

Required inputs:

- `token`: bearer token issued by `/admin_runtime_token`.
- `tls_pin`: TLS public key pin shown by `/admin_runtime_token`.

The Rust service may generate a fresh self-signed certificate on restart. Never use a hard-coded pin from this skill; always use the current pin returned in chat.

## Production Routing Signatures

Preserve this dialog routing shape when debugging or changing config:

- Primary dialog LLM path is AI Farm Discovery: `DIALOG_PROVIDER=aifarm`,
  `DISCOVERY_BASE_URL=<AI Farm Discovery>`, and
  `DIALOG_DISCOVERY_SERVICE_NAME=llm-openai`.
- `DIALOG_AIFARM_POOL_*` is a separate AI Farm overflow pool, not the Discovery
  base URL. Do not substitute pool base URLs for `DISCOVERY_BASE_URL`.
- The AI Farm pool is the first fallback layer for primary capacity pressure.
- `dialog_aifarm_fallback_jobs` is the second fallback layer: a GenKit drainer
  that activates only when the `dialog-aifarm` queue crosses its configured
  high watermark and drains toward the low watermark.
- Seeing both `dialog_jobs` and `dialog_aifarm_fallback_jobs` in `/api/ready` is
  expected. Do not set `PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_WORKERS=0`
  unless the user explicitly asks to disable that emergency drainer.

For image incidents, do not stop at queue depth. A job can be assigned and even
completed while the user still receives no picture. Check all boundaries:

- `taskmanJobs` for `image-regular` and `image-vip`: status, error, messages,
  events, `resultMessageID`, and processing timestamps.
- runtime logs filtered for `image`, `draw`, `photo`, `media`, `telegram`,
  `outbound`, `upload`, and `download`.
- `message_ops_queue` only for persisted outbound sends; an empty table does
  not prove direct Telegram sends succeeded.
- provider reachability for the configured draw path before changing code.

## Transport

Use pinned self-signed HTTPS:

```bash
BASE_URL="${BASE_URL:-https://100.77.77.51:9091}"
TOKEN="${TOKEN:?TOKEN is required}"
TLS_PIN="${TLS_PIN:?TLS_PIN is required}"

curl --silent --show-error \
  --insecure \
  --pinnedpubkey "$TLS_PIN" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  "$BASE_URL/graphql" \
  -d '{"query":"query { healthSnapshot { db { status error latencyMs } redis { status error latencyMs } updatesQueueLength } }"}'
```

## Useful Queries

Health and dependency reachability:

```graphql
query {
  healthSnapshot {
    db { status error latencyMs }
    redis { status error latencyMs }
    dispatcher { status error details }
    updatesQueueLength
  }
}
```

Runtime config snapshot:

```graphql
query {
  configSnapshot {
    runtimeApiEnabled
    webHost
    webPort
    dialogProvider
    dialogFallbackProvider
    persistentQueueEnabled
    shieldEnabled
    whiteCircleEnabled
    aceStepEnabled
  }
}
```

Inbound update runtime:

```graphql
query {
  updatesRuntime {
    active
    stateActive
    handleActive
    queueLen
    started1m
    completed1m
    timeouts1m
    oldestActiveMs
    lastStallAt
    tasks {
      stage
      startedAt
      ageMs
      chatID
      userID
      update
    }
  }
}
```

Queue diagnostics:

```graphql
query {
  taskmanQueueDiagnostics(queues: ["text", "image-regular", "image-vip", "music-vip"], priority: -4) {
    running
    active
    workerCount
    queues {
      queueName
      pending
      pendingOrHigher
      active
      workerCount
      etaSeconds
    }
  }
}
```

Recent image jobs:

```graphql
query {
  taskmanJobs(filter: { queue: ["image-regular", "image-vip"], limit: 20 }) {
    total
    summary { byStatus byQueue }
    items {
      id
      queueName
      status
      jobType
      workerID
      createdAt
      startedAt
      completedAt
      errorMessage
      progressMessageID
      queuePositionMessageID
      resultMessageID
    }
  }
}
```

One image job:

```graphql
query {
  taskmanJob(id: "JOB_ID") {
    job {
      id
      queueName
      status
      jobType
      createdAt
      startedAt
      completedAt
      errorMessage
      progressMessageID
      queuePositionMessageID
      resultMessageID
    }
    messages { messageType status messageID createdAt }
    events
  }
}
```

Recent errors:

```graphql
query {
  logs(limit: 80, level: "error") {
    count
    lastSeq
    items {
      seq
      time
      level
      message
      attrs
    }
  }
}
```

LLM request traces:

```graphql
query {
  llmRequests(filter: { limit: 20 }) {
    id
    at
    provider
    requestKind
    source
    flow
    model
    rawRequest
    resolvedCacheContent
    usage
    timings
    result {
      durationMs
      error
      responseTextPreview
    }
  }
}
```

Read-only SQL:

```graphql
query($sql: String!) {
  sqlRead(input: { sql: $sql, timeoutMs: 3000 }) {
    columns
    rowCount
    elapsedMs
    truncated
    rows
  }
}
```

## Operational Mutations

Use only when intentionally operating production.

Restart memory retries:

```graphql
mutation {
  restartMemory {
    ok
    retriedFailedRuns
    started
  }
}
```

Purge managed Gemini explicit caches after prompt/tool changes:

```graphql
mutation {
  purgeGeminiExplicitCaches {
    ok
    scanned
    matched
    deleted
    failed
  }
}
```

## Notes

- Rust OpenPlotva does not expose Go pprof endpoints.
- Treat the API as read-only unless the user asks for an operational mutation.
- Poll logs with `afterSeq` when monitoring a live incident.
