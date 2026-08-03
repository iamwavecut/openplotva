# Plan 016: Supervise workers with named phases and one shutdown deadline

## Status

- Priority: P0
- Effort: L
- Risk: HIGH
- Planned at: `ed2d8c1`, 2026-08-02
- Depends on: lifecycle characterization tests

## Why

`start_runtime_workers` spans 3,489 lines, starts 43 tasks, stores anonymous handles, and contains 375 explicit clones. Shutdown waits sequentially with a full timeout per handle, so the bound grows with worker count. It also snapshots the outbound dispatcher before send workers have terminated, leaving an in-flight persistence window.

## Change

1. Define named worker groups: ingress producers, processors, outbound consumers, persistence/telemetry, and independent servers.
2. Add a supervisor that records name, readiness, early exit/panic, cancellation handle, and group.
3. Extract subsystem builders returning owned worker groups; keep composition in app.
4. Shutdown in explicit phases: stop ingress, drain processors, stop outbound, persist after drain, then ancillary tasks.
5. Apply one absolute deadline to the whole phase sequence; abort remaining tasks at expiry.

## Verification

- Paused-time tests with 48 hung tasks prove shutdown stays within one deadline.
- Mid-send dispatcher test proves every item is delivered, terminally failed, or persisted.
- Panic/early-exit makes readiness unhealthy with worker name.
- Queue drain/persistence ordering tests; service/container smoke; deploy termination timing.

## STOP conditions

- A worker's existing drain or at-least-once semantics cannot be assigned to a phase.
- Persistence can run while a producer/consumer still mutates the same queue.
