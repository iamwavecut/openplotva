# Plan 012: Compile prompts once and inject a PromptStore

## Status

- Priority: P0
- Effort: S
- Risk: LOW-MED
- Planned at: `ed2d8c1`, 2026-08-02
- Depends on: none

## Why

`openplotva_prompts::render` and `render_messages` recursively read and compile all 21 prompt files (124,610 bytes) on every call. A release benchmark using identical data and byte-equality assertions measured 4.06–5.28 ms per current render versus 0.45–0.54 us with a reused registry: 8,894–10,435x faster locally.

## Change

1. Introduce an immutable `PromptStore` that resolves `OPENPLOTVA_PROMPTS_DIR`, registers helpers in role-marker mode, and compiles the tree once.
2. Preserve `read` and metadata APIs for explicit filesystem access.
3. Inject one `Arc<PromptStore>` from `openplotva-app`; do not add a mutable global.
4. Replace production per-call `render`/`render_messages` use. Keep compatibility functions only if external callers require them.
5. Fail startup with the exact prompt name/path when compilation fails. Reload semantics are restart-only unless an explicit operator requirement proves otherwise.

## Verification

- Snapshot every rendered prompt and role sequence before/after, including custom root and partials.
- Concurrent render test; invalid-template startup test.
- Repeat the lab benchmark and record p50/p95 plus allocations.
- `cargo test -p openplotva-prompts` and affected LLM/media/app tests.

## STOP conditions

- Any supported workflow relies on live prompt changes without restart.
- Role markers, front matter, partial resolution, or rendered bytes differ.
