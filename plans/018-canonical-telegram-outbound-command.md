# Plan 018: Make OutboundCommand the Telegram send source of truth

## Status

- Priority: P1
- Effort: XL, staged
- Risk: HIGH
- Planned at: `ed2d8c1`, 2026-08-02
- Depends on: persisted queue compatibility fixtures

## Why

Telegram outbound operations are represented repeatedly as dispatcher methods, optional persistence bytes, replay-time JSON reconstruction, transport methods/kinds, and response handling. The same operation changes in three or four places. A canonical serializable command can remove an estimated 600–900 LOC, but Redis queue bytes and replay semantics are public persistence contracts.

## Change

1. Capture fixtures for every persisted operation and replay outcome from the current encoder.
2. Introduce a versioned `OutboundCommand` enum with typed payloads and a compatibility decoder for current bytes.
3. Make dispatcher enqueue, persistence, replay, and transport execution consume that command.
4. Preserve debounce/dedupe keys, ordering, TTL, retry classification, operation IDs, and Telegram response decoding.
5. Migrate one operation family at a time; keep old decode until all existing queue items have expired or migrated.

## Verification

- Byte fixtures decode identically; new encoding is deterministic.
- Round-trip and replay tests for every command, including malformed/unknown version.
- Queue ordering, debounce, dedupe, retry, and crash-recovery smokes.
- Target at least 500 net LOC removed after compatibility code expires.

## STOP conditions

- A command cannot preserve current wire bytes or requires a destructive queue migration.
- Idempotency, retry class, or response semantics differ.
