# Dialog Session Append-Only Delivery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver every assistant text segment emitted next to dialog tool calls exactly once while making tool outcomes, rather than tool presence, decide whether another model step is required.

**Architecture:** Add mandatory continuation semantics to `ToolSpec`, use one pure batch-disposition fold in both production and captured sessions, and strengthen the existing per-session `SentLog` with canonical visible-text comparison. Preserve the current durable outbox, tool execution order, retry behavior, and final-only replay repair.

**Tech Stack:** Rust 1.95, tokio, openplotva-dialog, openplotva-app, openplotva-telegram HTML helpers, sqlx-backed task/outbox abstractions, cargo test/clippy.

## Global Constraints

- One accepted Telegram update owns one logical append-only response stream.
- Every non-empty assistant text segment returned next to tool calls is eligible for delivery.
- Each visible segment is delivered at most once per session; tool calls still execute when adjacent text is a replay.
- `send_message` shares the same delivery ledger as adjacent assistant text.
- Successfully queued image or song generation may end the session only when no result-dependent follow-up remains, and must never discard adjacent text.
- Production and captured sessions use the same disposition rules.
- Do not modify `docs/CODEBASE_MAP.md`.
- Ship through a ready PR and deploy only the exact merged `main` revision.

---

### Task 1: Make Tool Continuation Semantics Explicit

**Files:**
- Modify: `crates/openplotva-dialog/src/lib.rs:486-945`
- Test: `crates/openplotva-dialog/src/lib.rs` unit-test module

**Interfaces:**
- Produces: `pub enum ToolContinuation { RequiresFollowup, Sidecar, MayTerminateOnSuccess, ExplicitIntermediate }`
- Produces: `pub struct ToolSpec { ..., pub continuation: ToolContinuation, ... }`
- Produces: `pub fn dialog_tool_continuation(name: &str) -> Option<ToolContinuation>`
- Consumed by: production and captured session disposition logic in Task 2.

- [ ] **Step 1: Add a failing catalog coverage test**

```rust
#[test]
fn every_session_tool_has_explicit_continuation_semantics() {
    for spec in alternative_dialog_tools()
        .into_iter()
        .chain([SESSION_SEND_MESSAGE_SPEC, SESSION_REACT_TO_MESSAGE_SPEC])
    {
        assert_eq!(dialog_tool_continuation(spec.name), Some(spec.continuation));
    }
}
```

- [ ] **Step 2: Run the focused test and observe the missing API failure**

Run: `cargo test -p openplotva-dialog every_session_tool_has_explicit_continuation_semantics`

Expected: compilation fails because `ToolContinuation`, `ToolSpec.continuation`, and `dialog_tool_continuation` do not exist.

- [ ] **Step 3: Add the enum, mandatory field, and lookup**

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolContinuation {
    RequiresFollowup,
    Sidecar,
    MayTerminateOnSuccess,
    ExplicitIntermediate,
}

#[must_use]
pub fn dialog_tool_continuation(name: &str) -> Option<ToolContinuation> {
    alternative_dialog_tools()
        .into_iter()
        .chain([SESSION_SEND_MESSAGE_SPEC, SESSION_REACT_TO_MESSAGE_SPEC])
        .find(|spec| spec.name.eq_ignore_ascii_case(name.trim()))
        .map(|spec| spec.continuation)
}
```

Assign `MayTerminateOnSuccess` to draw/song, `Sidecar` to reactions, `ExplicitIntermediate` to `send_message`, and `RequiresFollowup` to every other catalog tool.

- [ ] **Step 4: Run the dialog crate tests**

Run: `cargo test -p openplotva-dialog --lib`

Expected: all tests pass.

- [ ] **Step 5: Commit the typed catalog contract**

```bash
git add crates/openplotva-dialog/src/lib.rs
git commit -m "refactor(dialog): type tool continuation semantics"
```

### Task 2: Add Shared Delivery and Batch-Disposition Primitives

**Files:**
- Modify: `crates/openplotva-app/src/dialog_turn/session.rs:141-179`
- Modify: `crates/openplotva-app/src/dialog_turn/session.rs:1045-1167`
- Test: `crates/openplotva-app/src/dialog_turn/session.rs` unit-test module

**Interfaces:**
- Consumes: `dialog_tool_continuation` and `ToolContinuation` from Task 1.
- Produces: `enum SessionBatchDisposition { ContinueForResults, CompleteWithSideEffect, CompleteAfterSidecars, ContinueWithoutFinal }`
- Produces: `fn session_batch_disposition(calls: &[ChatStepToolCall], results: &[ToolResult], step_had_text: bool) -> SessionBatchDisposition`
- Produces: canonical `SentLog::matches_delivery` shared by ordinary adjacent text and `send_message`.

- [ ] **Step 1: Add failing canonical-delivery tests**

```rust
#[test]
fn sent_log_matches_html_equivalent_and_aggregate_replays() {
    let mut sent = SentLog::new();
    sent.record("Ну и <b>юмор</b>", true);
    assert!(sent.matches_delivery("<p>Ну и юмор</p>"));
    sent.record("Ещё реплика", true);
    assert!(sent.matches_delivery("Ну и юмор\n\nЕщё реплика"));
}
```

- [ ] **Step 2: Run the focused test and observe the HTML-equivalence failure**

Run: `cargo test -p openplotva-app sent_log_matches_html_equivalent_and_aggregate_replays`

Expected: the `<b>` versus `<p>` assertion fails with the current whitespace-only normalization.

- [ ] **Step 3: Canonicalize the visible text**

Use `openplotva_telegram::strip_telegram_html` after `prepare_dialog_chat_response`, then normalize whitespace. Store canonical keys in `SentLog`; retain the original sanitized outbound payload only where required for delivery.

```rust
fn canonical_visible_text(text: &str) -> String {
    openplotva_telegram::strip_telegram_html(text)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}
```

- [ ] **Step 4: Add failing table tests for batch disposition**

Cover sidecar+text completion, sidecar-only continuation, successful queued draw completion, failed draw continuation, draw+search continuation, and `send_message`-only continuation.

```rust
assert_eq!(
    session_batch_disposition(&reaction_calls, &reaction_results, true),
    SessionBatchDisposition::CompleteAfterSidecars,
);
assert_eq!(
    session_batch_disposition(&draw_and_search, &queued_and_ok, true),
    SessionBatchDisposition::ContinueForResults,
);
```

- [ ] **Step 5: Implement the pure fold**

The fold must inspect every call/result pair, give `RequiresFollowup` precedence, treat an unsuccessful `MayTerminateOnSuccess` as requiring follow-up, complete a queued generation only when no follow-up remains, and use `step_had_text` only for a sidecar-only batch.

- [ ] **Step 6: Run the session unit tests**

Run: `cargo test -p openplotva-app dialog_turn::session::tests --lib`

Expected: all focused primitive tests pass.

- [ ] **Step 7: Commit the shared state-machine primitives**

```bash
git add crates/openplotva-app/src/dialog_turn/session.rs
git commit -m "refactor(dialog): centralize session batch disposition"
```

### Task 3: Enforce Append-Only Delivery in the Production Session

**Files:**
- Modify: `crates/openplotva-app/src/dialog_turn/session.rs:527-984`
- Modify: `crates/openplotva-app/src/dialog_turn/session.rs:1397-1458`
- Test: `crates/openplotva-app/src/dialog_jobs/tests.rs:3661-5025`

**Interfaces:**
- Consumes: `SessionBatchDisposition` and canonical `SentLog` from Task 2.
- Preserves: `try_send_intermediate`, `send_dialog_answer`, queued-side-effect tickets, tool history, session events, and partial-delivery failure behavior.

- [ ] **Step 1: Add the incident regression test**

Create a provider sequence whose first and only step contains the incident reply plus two `react_to_message` calls, one successful and one rejected. Assert one provider call, one adjacent-text delivery, no final-answer delivery, and both reactions attempted.

```rust
assert_eq!(provider.calls(), 1);
assert_eq!(effects.intermediates().len(), 1);
assert!(effects.sent().is_empty());
assert_eq!(reactor.reactions.lock().expect("reactions").len(), 1);
```

- [ ] **Step 2: Add result-dependent and optional-terminal regressions**

Add four named tests with these exact assertions:

```rust
assert_eq!(effects.intermediates().iter().filter(|row| row.0 == repeated).count(), 1);
assert_eq!(toolbox.web_search_calls(), 1, "a replay must not suppress its tool");

assert_eq!(provider.calls(), 1, "queued draw plus text is complete");
assert_eq!(effects.intermediates(), vec![("Уже рисую".to_owned(), 1, true)]);

assert_eq!(provider.calls(), 2, "failed draw must feed back to the model");
assert_eq!(effects.sent()[0].1, "Не запустилось, попробуем позже");

assert_eq!(provider.calls(), 2, "search result remains an answer obligation");
assert_eq!(effects.sent()[0].1, "Вот что нашлось");
```

- [ ] **Step 3: Run the new tests and verify current behavior fails**

Run: `cargo test -p openplotva-app --lib session_`

Expected: sidecar+text performs an extra provider call; HTML-equivalent replay can create an extra delivery; draw+search terminates before the search answer.

- [ ] **Step 4: Route every adjacent segment through the shared ledger**

Keep adjacent text delivery before tool execution, but record whether the step supplied a valid visible segment. Suppress a replay without skipping transcript recording or tool execution. Reuse the same `SentLog` from `STEP_SEND_MESSAGE` execution.

- [ ] **Step 5: Apply the batch disposition after all tools execute**

Replace unconditional loop continuation and the current `if !batch_side_effects.is_empty()` terminal check with an exhaustive match:

```rust
match session_batch_disposition(&step.tool_calls, &batch_results, step_had_text) {
    SessionBatchDisposition::ContinueForResults
    | SessionBatchDisposition::ContinueWithoutFinal => continue,
    SessionBatchDisposition::CompleteWithSideEffect => {
        side_effect_tickets.extend(batch_side_effects);
        return session_delegated(&sent, &side_effect_tickets);
    }
    SessionBatchDisposition::CompleteAfterSidecars => {
        report.sent_answer = true;
        if let Some(runs) = ctx.llm_runs {
            runs.mark_round_sent(&run_id, crate::runtime_llm_runs::RunRoundSent::Final);
        }
        append_session_sent_marker(queue, ctx.item_id, failure_now).await;
        return TurnResolution {
            outcome: TurnOutcome::Sent {
                parts: sent.total_count,
                side_effect_tickets: ticket_ids(&side_effect_tickets),
            },
            disposition: JobDisposition::Complete,
        };
    }
}
```

The completion helper must append `SESSION_MESSAGE_SENT_STAGE`, mark the last LLM round final, set `report.sent_answer`, and return `TurnOutcome::Sent` without invoking `send_dialog_answer` again.

- [ ] **Step 6: Record batch disposition telemetry**

Append the selected disposition to the last tool-batch job event without logging private text or tool results.

- [ ] **Step 7: Run focused production-session tests**

Run: `cargo test -p openplotva-app --lib session_`

Expected: all session tests pass, including previous replay, citation, side-effect, retry, and re-entry tests.

- [ ] **Step 8: Commit production behavior**

```bash
git add crates/openplotva-app/src/dialog_turn/session.rs crates/openplotva-app/src/dialog_jobs/tests.rs
git commit -m "fix(dialog): deliver inline tool text once"
```

### Task 4: Keep Captured Sessions and Prompts in Lockstep

**Files:**
- Modify: `crates/openplotva-app/src/dialog_turn/session.rs:1645-1804`
- Modify: `prompts/aifarm/system.prompt:4-18`
- Modify: `prompts/chat/_shared_core.prompt:61-67`
- Test: `crates/openplotva-app/src/dialog_turn/session.rs` captured-session tests
- Test: prompt snapshot/contract tests reached through `cargo test -p openplotva-app --lib`

**Interfaces:**
- Consumes: `session_batch_disposition` and canonical session ledger.
- Preserves: `CapturedSessionOutput { messages, tool_calls, provider }`.

- [ ] **Step 1: Add captured-session parity tests**

Prove reactions+text stop after one provider call, queued draw+text captures the text then stops, and search+text captures the first segment then continues for a novel answer without replay.

- [ ] **Step 2: Run captured tests and verify divergence**

Run: `cargo test -p openplotva-app --lib captured_session`

Expected: current captured loop always continues after sidecars and has no shared replay ledger.

- [ ] **Step 3: Reuse the production disposition and delivery ledger**

Collect tool results in call order, fold them with `session_batch_disposition`, and stop/continue exactly as the production session does. Capturing a duplicate must not append it to `CapturedSessionOutput.messages`.

- [ ] **Step 4: Rewrite the prompt contract**

State that text next to calls is delivered exactly once, only new content may be emitted after results, `send_message` creates an additional distinct intermediate message, and successfully queued generation needs no later confirmation while adjacent text is preserved.

- [ ] **Step 5: Run prompt and captured-session tests**

Run: `cargo test -p openplotva-app --lib captured_session`

Run: `cargo test -p openplotva-prompts --lib`

Expected: all tests pass.

- [ ] **Step 6: Commit parity and prompt changes**

```bash
git add crates/openplotva-app/src/dialog_turn/session.rs prompts/aifarm/system.prompt prompts/chat/_shared_core.prompt
git commit -m "fix(dialog): align tool continuation prompt"
```

### Task 5: Verify, Review, Merge, and Deploy

**Files:**
- Verify all changed files and the focused diff.
- Do not modify production configuration or migrations.

**Interfaces:**
- Produces: ready PR into `main`, merged SHA, exact production image, and live delivery evidence.

- [ ] **Step 1: Run formatting and focused tests**

Run: `cargo fmt --all`

Run: `cargo test -p openplotva-dialog --lib`

Run: `cargo test -p openplotva-app --lib session_`

- [ ] **Step 2: Run required broad local gates**

Run: `cargo test -p openplotva-app --lib`

Run: `cargo clippy --workspace --all-targets -- -D warnings`

- [ ] **Step 3: Inspect the final diff and push the branch**

Run: `git diff --check origin/main...HEAD`

Run: `git status --short --branch`

Push `fix/dialog-session-append-only-delivery` and open a ready PR into `main` with the incident evidence, state-machine table, verification commands, and no private message identifiers.

- [ ] **Step 4: Complete the PR review loop**

Poll checks, full persistent bot-comment bodies, issue comments, reviews, inline comments, and unresolved review threads. Fix or rebut every current finding, rerun affected local checks, resolve every inline thread, and merge only when all required checks are terminal green against the exact head SHA.

- [ ] **Step 5: Deploy exact merged main**

Dispatch `deploy-production.yml` against `main`, wait for terminal success, and verify that the running image label/revision equals the merge SHA.

- [ ] **Step 6: Verify production behavior**

Check health/readiness, restart count, fresh logs, dialog queue state, and post-deploy `telegram_outbox` operations. Run a safe representative dialog path that returns assistant text with inline sidecar tools when practical; otherwise prove the new disposition through durable LLM/tool/outbox records without exposing private payloads. Confirm no single session creates a second delivery for an already committed visible segment during the soak.
