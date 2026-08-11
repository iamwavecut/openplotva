# LLM routing resilience design

## Scope

This change addresses four observed production failure modes without changing
the routing order or internal dialog transcript:

- Keep `message.name` by default, but let an OpenAI-compatible provider declare
  `supports_message_name: false`; only the serialized wire copy then drops it.
- Let the small GPU embedder coexist with GPU 2 generation services through its
  own bounded pool of eight concurrent requests, advertising the same capacity
  to Discovery. The coarse generation lock remains between the large LLM services.
- Guarantee forward progress in tolerant JSON parsing and bound each analytics
  insert so one malformed response or stuck database operation cannot pin a
  runtime worker indefinitely.
- Record retry attempts and capacity-pool waits as informational events; terminal
  exhaustion, deadline, and circuit events retain warning/error severity.

## Compatibility and rollout

The provider capability defaults to enabled, so existing providers keep their
current request shape. Migration 181 disables it only for `aifarm-vllm-gpu0`,
whose live endpoint rejects the field. Tool results retain `tool_call_id` and
content.

The embedder and Discovery share `EMBEDDER_MAX_CONCURRENCY`, defaulting to eight,
so admission control agrees across both layers while the CUDA driver schedules it
alongside resident LLM services. Rollout verification must compare concurrent
embedder and LLM latency, GPU memory, OOM/Xid errors, and request outcomes before
raising the limit further.

Telemetry insert timeouts are best-effort loss boundaries: a timed-out batch is
dropped rather than retried because an ambiguous commit could otherwise create
duplicates. The next batch is still accepted and flushed.
