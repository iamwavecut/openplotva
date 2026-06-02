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

Required inputs:

- `token`: bearer token issued by `/admin_runtime_token`.
- `tls_pin`: TLS public key pin shown by `/admin_runtime_token`.

The Rust service may generate a fresh self-signed certificate on restart. Never use a hard-coded pin from this skill; always use the current pin returned in chat.

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
