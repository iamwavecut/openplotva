# Plan 013: Remove the dead token-estimator service

## Status

- Priority: P0
- Effort: S-M
- Risk: LOW-MED
- Planned at: `ed2d8c1`, 2026-08-02
- Depends on: live no-op confirmation

## Why

`HttpTokenEstimator` is defined but has no constructor call in the workspace. `openplotva-app` only fabricates its source label, while active extraction uses the heuristic estimator. Production nevertheless builds a Python/FastAPI/Transformers image, uploads it, waits for health, provisions a model cache volume, and makes app startup depend on it. Removing the path deletes about 140 Python/Docker lines plus Rust config/client and deploy wiring, and removes model download, memory, disk, and a deploy failure gate.

## Change

1. Before editing, query the running deployment for requests, logs, container network traffic, and estimator metrics over a representative window. Confirm zero callers.
2. Remove `HttpTokenEstimator`, its DTO/config/error/cooldown code, and misleading diagnostic label.
3. Remove the memory estimator URL/timeout configuration and tests.
4. Remove compose service/dependency, workflow upload, deploy health wait, cache volume, and legacy volume import for this service.
5. Keep the current heuristic source explicit in diagnostics.

## Verification

- `rg` proves no token-estimator URL/type/service references remain.
- Memory extraction token-budget tests retain current results.
- Compose config renders; container and service smokes pass without the service.
- A staging deploy starts faster and shows no connection attempts to port 12600.

## STOP conditions

- Any live request or external consumer reaches the estimator.
- Removing it changes extraction admission/batching results rather than only dead configuration.
