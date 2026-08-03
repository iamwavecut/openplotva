# Plan 014: Use one runtime GraphQL output model

## Status

- Priority: P0
- Effort: L
- Risk: MED
- Planned at: `ed2d8c1`, 2026-08-02
- Depends on: schema golden fixture

## Why

`runtime_graphql.rs` maintains 59 public `Runtime*Data` boundary structs and a second private GraphQL output graph, connected by 57 `From` implementations. Most pairs express the same fields and ownership. The duplication is roughly 1.3–1.7k LOC and makes every field change a two-model migration.

## Change

1. Export the current GraphQL SDL/introspection result as a golden contract.
2. Classify each pair: identical, GraphQL scalar conversion (`ID`, JSON), or intentionally different nullability/name.
3. Derive `SimpleObject` directly on identical boundary outputs.
4. Retain tiny explicit wrappers only where scalar/nullability semantics differ; centralize repeated ID conversion helpers.
5. Remove corresponding private structs and `From` implementations incrementally by query family.

## Verification

- SDL/introspection byte or semantic diff is empty.
- Snapshot representative query JSON including nulls, IDs, lists, pagination, and errors.
- Existing `openplotva-server` tests, app runtime API tests, clippy.
- Measure LOC and clean-check time; target at least 1,000 net LOC removed.

## STOP conditions

- A proposed merge changes GraphQL name, nullability, scalar encoding, or error visibility.
- The public data type would gain GraphQL-only ownership that leaks into a non-GraphQL consumer.
