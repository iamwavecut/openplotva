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

## Virtual Dialogs

Runtime virtual dialogs are disposable dialog sessions addressed only by
caller-chosen `sessionID: String!`. There is no default owner, no global active
dialog, and no list endpoint. Use a unique session ID per experiment, for
example `codex-intent-tools-20260630-01`.

Start or replace a session:

```graphql
mutation {
  startVirtualDialog(input: { sessionID: "codex-test-session", replaceExisting: true }) {
    sessionID
    chatID
    userID
    nextMessageID
    expiresAt
  }
}
```

Send a cleanup-friendly tool-calling message. `SAFE` mode uses normal routing,
history, memory, shield, and tool-calling, but side-effect tools such as drawing
and music return synthetic queued results instead of starting real jobs:

```graphql
mutation {
  sendVirtualDialogMessage(input: {
    sessionID: "codex-test-session"
    text: "Нарисуй маленькую плотву в стиле стикера"
    toolMode: SAFE
  }) {
    messageID
    role
    text
    provider
    toolMode
    toolCalls
  }
}
```

Use `REAL` only when real taskman/tool side effects are intended. Those effects
run under generated negative virtual `chatID`/`userID`; deleting the virtual
dialog cleans local metadata/history/taskman rows/traces but cannot recall an
external provider request that has already started.

Inspect the session history without extending its lifetime:

```graphql
query {
  virtualDialog(sessionID: "codex-test-session") {
    sessionID
    chatID
    userID
    lastActivityAt
    expiresAt
    messages {
      messageID
      role
      text
      provider
      toolMode
      toolCalls
    }
  }
}
```

Inspect LLM traces for the returned virtual `chatID`:

```graphql
query {
  llmRequests(filter: { chatID: -9100000000001, limit: 20 }) {
    id
    at
    provider
    source
    flow
    model
    message { messageID }
    result { durationMs error responseTextPreview }
  }
}
```

Delete the session and its cleanup-friendly artifacts:

```graphql
mutation {
  deleteVirtualDialog(sessionID: "codex-test-session") {
    found
    deleted
    historyDeleted
    taskmanDeleted
    llmTracesDeleted
  }
}
```

Expiration behavior:

- `sessionID` is required, trimmed, non-empty, and capped by the runtime API.
- `startVirtualDialog` fails if a non-expired session exists unless
  `replaceExisting: true`.
- Successful `startVirtualDialog` and `sendVirtualDialogMessage` refresh
  `lastActivityAt`/`expiresAt`; reads do not refresh TTL.
- Sessions expire after 24 hours of inactivity. Runtime cleanup removes expired
  sessions periodically, and each access lazily expires the requested session
  before continuing.

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
