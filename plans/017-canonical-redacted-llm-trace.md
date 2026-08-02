# Plan 017: Build one redacted LLM trace artifact without cloning requests

## Status

- Priority: P0
- Effort: M
- Risk: MED-HIGH
- Planned at: `ed2d8c1`, 2026-08-02
- Depends on: trace contract and retention review

## Why

AIFarm clones `ChatCompletionRequest` before selecting direct/discovery transport and later clones it again for redaction. Gemini trace builders serialize raw requests, including possible inline media. This wastes memory on multimodal calls and creates two trace authorities with different redaction behavior, increasing sensitive-payload retention risk.

## Change

1. Define a provider-neutral `RedactedTraceArtifact` builder that accepts borrowed request data.
2. Redact/replace inline data URLs and Gemini `inline_data` before any JSON value is constructed.
3. Move request ownership into the selected transport; borrow only the small metadata needed after completion.
4. Emit exactly one trace record per round-trip on success and error.
5. Preserve model, usage, timing, routing tags, semantic error, docs chars, and operator-visible JSON shape.

## Verification

- Golden traces for AIFarm direct/discovery and Gemini, success/transport/semantic failure.
- Tests assert no base64/data URL/user secret survives in artifacts or persistence.
- Peak RSS/allocation benchmark with large image/video inputs.
- Existing analytics queries and retention cleanup continue to decode records.

## STOP conditions

- Redaction removes a field required for current operator diagnosis without an approved safe replacement.
- Trace event count, attribution, or persisted schema changes unexpectedly.
