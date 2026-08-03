# Plan 015: Replace the recursive update chain with a typed router

## Status

- Priority: P0
- Effort: XL, staged
- Risk: MED-HIGH
- Planned at: `ed2d8c1`, 2026-08-02
- Depends on: route characterization matrix

## Why

The app assembles 34 nested update-handler wrappers. A fall-through dialog update allocates a boxed future at each wrapper and repeats classification, command parsing, and downstream error wrapping. The app contains 157 future aliases, 185 `Pin<Box<dyn Future>>` sites, and 995 `Box::pin` sites; 154 local traits show no dynamic use. The goal is not a `Vec<dyn Handler>`, which would retain allocations, but parse-once typed routing.

## Change

1. Record a route matrix for every Telegram update kind, command target, callback, payment, permission gate, and terminal/fall-through outcome.
2. Add one `ParsedUpdate`/`ParsedBotCommand` classification close to `openplotva-updates`, preserving raw payload access.
3. Route by typed category to cohesive command/callback/message modules; make ordering explicit in one table/match.
4. Preserve history/state stages around the router.
5. Convert generic-only async ports to RPITIT in small batches; keep boxed futures at real object-safe boundaries.
6. Delete wrappers and duplicate UTF-16 command parsers only after parity tests pass.

## Verification

- Golden route matrix covers private/group, bot targets, UTF-16 entities, callbacks, payments, edited/service messages, and unauthorized paths.
- Allocation-count and p50/p95 benchmark for fall-through dialog and handled command.
- Compare clean-check time, peak compiler RSS, and release binary size.
- Full app/workspace tests and update queue smokes.

## STOP conditions

- Handler ordering, error visibility, idempotency, or history scheduling differs.
- Allocation reduction causes unacceptable binary size or compile-time regression.
