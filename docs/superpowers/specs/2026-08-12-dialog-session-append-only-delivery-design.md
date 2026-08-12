# Dialog Session Append-Only Delivery Design

**Date:** 2026-08-12  
**Status:** approved implementation target  
**Scope:** dialog session text delivery and post-tool continuation in the Telegram runtime and captured admin session

## Problem

The dialog session engine currently interprets every assistant text emitted next to tool calls as an intermediate message, sends it, executes the calls, and then always asks the model for another step unless a queued image or song terminates the session. This is incorrect for sidecar tools such as `react_to_message`: a provider can return a complete reply together with reactions, after which the unnecessary second model step can replay the already delivered reply as a separate final Telegram message.

Telegram ingress, task execution, and outbound retries are not the cause. One dialog job can intentionally enqueue both `dialog-intermediate` and `dialog-answer` operations for the same visible reply.

## Product Invariants

1. One accepted user update owns one logical append-only response stream.
2. Every non-empty user-visible assistant text segment returned by the model is eligible for delivery, including text returned next to one or more tool calls.
3. Each segment is delivered at most once per dialog session. Formatting-only HTML differences, whitespace differences, and a replay formed by concatenating already delivered segments do not create a new delivery.
4. Tool calls are always executed even when adjacent text is suppressed as a duplicate.
5. Tool semantics and execution results determine whether the session needs another model step. The mere presence of a tool call does not imply that another step is required.
6. `send_message` remains the explicit way to emit a distinct intermediate message. It shares the same session delivery ledger as ordinary assistant text.
7. Successfully queued image or song generation may end the session, but it never discards adjacent assistant text. A queued generation ends only a batch that has no result-dependent tool requiring a follow-up.
8. A failed image or song scheduling call feeds its result back to the model and requires a follow-up.
9. Production Telegram sessions and captured admin sessions use the same disposition rules.
10. Existing durable outbox idempotency and the sent-text ledger remain defense in depth; they are not substitutes for correct session transitions.

## Model

### Visible output

`ChatStepOutput.text` and `send_message.text` are both visible response segments. Before an accepted delivery, the engine derives a canonical visible-text key by sanitizing the outbound HTML, decoding entities, removing markup, and normalizing whitespace. The session ledger records both the exact sanitized payload and the canonical visible key.

A candidate is a replay when its canonical key matches:

- an individual delivered segment; or
- the whitespace-joined canonical keys of all delivered segments in order.

The comparison is deterministic. The engine does not suppress merely similar or paraphrased text.

### Tool continuation semantics

Every tool specification carries one mandatory continuation class:

| Class | Tools | Continuation rule |
| --- | --- | --- |
| `RequiresFollowup` | `understand_media`, `currency_rates`, `web_search`, `crawl_url`, `youtube_summary`, `queue_status`, `cancel_drawing`, `translate_text`, `chat_history_summary`, `history_search` | Always feed the result to another model step. |
| `Sidecar` | `react_to_message` | If the assistant step also has visible text, that text completes the response; without text, request another step. Sidecar failure does not invalidate adjacent text. |
| `MayTerminateOnSuccess` | `draw_image`, `generate_song` | A queued result may finish the session. A failed/no-op result requires a follow-up. |
| `ExplicitIntermediate` | `send_message` | Deliver the tool's text once. By itself it does not complete the response. |

The continuation class is required in `ToolSpec`, so adding a new tool requires an explicit decision at compile time.

### Batch disposition

For one assistant step, the engine records the assistant message, attempts delivery of its adjacent text, executes every tool call in order, records every result, and folds the batch into one disposition:

1. `ContinueForResults` when any `RequiresFollowup` tool ran or any `MayTerminateOnSuccess` tool did not produce a queued terminal side effect.
2. `CompleteWithSideEffect` when at least one `MayTerminateOnSuccess` tool queued work and no result-dependent follow-up remains.
3. `CompleteAfterSidecars` when the batch contains only `Sidecar` tools and the assistant step supplied non-empty visible text, including text suppressed because the identical segment was already delivered in this session.
4. `ContinueWithoutFinal` for tool-only sidecar batches, batches containing only `ExplicitIntermediate`, or any other nonterminal batch without assistant text.

`CompleteWithSideEffect` is optional terminal behavior: adjacent text is committed before completion. A batch such as `draw_image + web_search` continues because the search result still creates an answer obligation.

## Data Flow

For each model step:

1. Decode native and salvaged tool calls plus residual assistant text.
2. Sanitize `step.text` and pass it through one `commit_visible_segment` boundary.
3. Append the assistant message and tool calls to the provider transcript.
4. Execute every call, including calls accompanying duplicate text.
5. Append all tool results to the transcript and durable tool history.
6. Compute the batch disposition from tool continuation classes, outcomes, side effects, and whether the step supplied visible text.
7. Complete the dialog job or begin the next model step accordingly.

Later model steps read their earlier assistant text in the transcript and are explicitly instructed that it was already delivered. If a provider still replays it, the shared delivery ledger suppresses it.

## Prompt Contract

The AIFarm and shared chat prompts must say:

- assistant text next to tool calls is delivered immediately and exactly once;
- after a tool result, continue only with new user-visible content;
- do not restate or wrap an already emitted segment;
- use `send_message` for an additional distinct progress message;
- a successfully queued image or song requires no later confirmation, but adjacent text is still delivered.

The prompt guides the model, while the engine enforces the invariant independently.

## Failure Handling

- Failure to enqueue adjacent text follows the existing partial-delivery failure rules and must not be recorded as delivered.
- Tool failures remain transcript results. Result-dependent and failed scheduling tools continue so the model can respond honestly.
- Sidecar failures do not trigger an extra model step when adjacent text already completes the response.
- If a final model step only replays delivered content after result-dependent work, existing final-only regeneration and `repeated_final_after_partial` exhaustion behavior remains.
- A session that emits only explicit intermediate messages and never produces a final or terminal side effect remains a terminal failure.

## Observability

Each completed tool batch records a deterministic disposition value:

- `continue_for_results`
- `complete_with_side_effect`
- `complete_after_sidecars`
- `continue_without_final`

Duplicate suppression records whether the match was an individual segment or the ordered aggregate. Existing LLM-round sent markers distinguish intermediate, final, and terminal-side-effect completion.

## Verification Matrix

Focused regression coverage must prove:

- text-only response: one LLM step, one final delivery;
- reactions plus text: one LLM step, one text delivery, all reactions attempted;
- failed reaction plus text: one LLM step, one text delivery;
- reactions without text: a second LLM step supplies one final delivery;
- search plus text: adjacent text delivered once, search result drives a later novel final;
- a repeated adjacent message on a later tool step is suppressed while that later tool still executes;
- `send_message` and adjacent assistant text share duplicate detection;
- queued draw/song without text: one LLM step and silent delegated completion;
- queued draw/song with text: one LLM step and one text delivery;
- failed draw/song with text: the text remains delivered and the failure drives a later response;
- queued draw plus search: all tools execute and the search result drives a later response;
- HTML-equivalent replay and ordered concatenated replay are not delivered twice;
- captured sessions return the same message sequence and stop/continue decisions as production sessions.

Broader verification is `cargo fmt --all`, focused app/dialog tests, `cargo test -p openplotva-app --lib`, `cargo test -p openplotva-dialog --lib`, and `cargo clippy --workspace --all-targets -- -D warnings` before the ready PR.

## Delivery

Ship through a dedicated fix branch and ready PR into `main`. After all CI and review artifacts are handled, merge with a merge commit, run `deploy-production.yml` for the exact merge SHA, and verify the running image, health/readiness, restart count, fresh logs, dialog queues, and post-deploy Telegram outbox behavior.
