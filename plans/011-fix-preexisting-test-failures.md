# Plan 011: Fix the pre-existing failing tests so `cargo test --workspace` is green

> **Executor instructions**: Follow each sub-task. Run the named test before and
> after your change to confirm. If a STOP condition occurs, stop and report —
> two of these three may be a real regression rather than a stale test, and that
> distinction must be a human decision, not an executor guess.
>
> **Drift check (run first)**: `git diff --stat 9f32c4b..HEAD -- crates/openplotva-web/src/lib.rs crates/openplotva-llm/src/aifarm.rs crates/openplotva-updates/src/lib.rs web/admin/index.html prompts/`

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED (one trivial; two require a regression-vs-stale-test judgment)
- **Depends on**: none
- **Blocks**: plan 001 — once CI runs `cargo test --workspace`, these three failures turn CI red until fixed.
- **Category**: bug
- **Planned at**: commit `9f32c4b`, 2026-06-15

## Why this matters

`cargo test --workspace` is currently **RED on `main`** at commit `9f32c4b` — three tests fail,
and nobody noticed because CI never runs the test suite (the exact gap plan 001 closes). Before
(or together with) plan 001 lands, these must be fixed, or enabling the CI test gate will block
every PR. Each was independently reproduced during the `improve` execution run.

The three failures (each runs without external services):

1. `openplotva-web :: embedded_web_assets_match_expected_hashes`
2. `openplotva-llm :: aifarm::tests::system_prompt_includes_tool_catalog`
3. `openplotva-updates :: update_consumer_skips_handle_at_go_stale_boundary`

## Commands you will need

| Purpose | Command |
|---------|---------|
| Web test    | `cargo test -p openplotva-web embedded_web_assets_match_expected_hashes` |
| LLM test    | `cargo test -p openplotva-llm system_prompt_includes_tool_catalog` |
| Updates test| `cargo test -p openplotva-updates update_consumer_skips_handle_at_go_stale_boundary` |
| Asset hash  | `shasum -a 256 web/admin/index.html` |
| Clippy/fmt  | `cargo fmt --all`; `cargo clippy --workspace --all-targets --all-features -- -D warnings` |

## Scope

**In scope** (depends on the diagnosis per sub-task; do not exceed without reporting):
- Sub-task 1: `crates/openplotva-web/src/lib.rs` (one sha256 constant).
- Sub-task 2: `crates/openplotva-llm/src/aifarm.rs` and/or a file under `prompts/`.
- Sub-task 3: `crates/openplotva-updates/src/lib.rs`.

**Out of scope**: anything not required by the specific failing assertion.

## Sub-task 1 — embedded asset hash drift (trivial, deterministic)

`web/admin/index.html` was edited but its `sha256` constant in `web/src/lib.rs` (in the
`ADMIN_ASSETS` array, the `index.html` entry near `web/src/lib.rs:47-52`) was not regenerated.
Diagnosis confirmed: actual `shasum -a 256 web/admin/index.html` begins `d898eb994099…`; the
declared constant begins `30af2f0d0494…`.

Steps:
1. Confirm the `web/admin/index.html` change was intentional: `git log --oneline -5 -- web/admin/index.html`. If the file looks accidentally corrupted (not a deliberate admin-UI edit), STOP and report — do not paper over a bad asset by bumping the hash.
2. If intentional: run `shasum -a 256 web/admin/index.html`, copy the full 64-char hash, and replace the `sha256: "30af2f0d0494…"` value for the `index.html` entry in `ADMIN_ASSETS`.

**Verify**: `cargo test -p openplotva-web embedded_web_assets_match_expected_hashes` passes.

## Sub-task 2 — aifarm system-prompt assertion drift

The test `aifarm::tests::system_prompt_includes_tool_catalog` (`crates/openplotva-llm/src/aifarm.rs:7962`)
asserts the rendered system prompt contains specific Russian substrings (e.g.
`"ведёшь персонажа в живом Telegram-чате"`, `"Большинство реплик не требуют tool"`,
`"Никогда не используй translate_text"`, `"<system_contract>"`, `"<tools>"`, and per-tool
`name="…"` + summary). One of these no longer matches the rendered prompt.

Steps:
1. Run the test, read which `assert!` fails and what the actual prompt contains around that text.
2. Determine the source of truth: the prompt is built from `.prompt` templates under `prompts/`
   (cache-sensitive system text — a public contract per AGENTS.md). If the **template** was
   intentionally reworded and the test is now stale, update the test's expected substring to match
   the current prompt. If the **template** lost required content (a real regression in the system
   contract), STOP and report — restoring prompt content is a product decision, not an executor
   edit.

**Verify**: `cargo test -p openplotva-llm system_prompt_includes_tool_catalog` passes; if you
changed a `.prompt` file or its consumer, also run `cargo test -p openplotva-llm` to confirm no
sibling prompt test regressed.

## Sub-task 3 — update-consumer stale-boundary behavior

`update_consumer_skips_handle_at_go_stale_boundary` (`crates/openplotva-updates/src/lib.rs`)
feeds an update dated `1_710_000_000` with `now = +60s` and asserts the consumer calls only the
`state` handler, NOT the `handle` handler (i.e. updates at/over the staleness boundary are
recorded but not handled — Go parity). It currently fails with observed calls `["state","handle"]`
vs expected `["state"]` — meaning **stale updates are being handled when they should be skipped.**

Steps:
1. Run the test; read `process_update_at` and the staleness-boundary logic it exercises.
2. Decide: is this a **code regression** (the stale-skip threshold/logic broke, so stale updates
   leak into handling — a real behavioral bug to fix in the consumer) or an **intentional behavior
   change** (the project decided to handle boundary updates, so the test is stale)?
   - If a regression: fix the consumer so updates at/over the boundary skip the `handle` call, and
     keep the test as-is. This is the likely case given the test name encodes intended parity.
   - If intentional: STOP and report — changing the test to accept handling of stale updates is a
     behavior decision that needs maintainer sign-off (it affects which Telegram updates the bot
     acts on), not an executor call.

**Verify**: `cargo test -p openplotva-updates update_consumer_skips_handle_at_go_stale_boundary`
passes, and `cargo test -p openplotva-updates` shows no new failures.

## Done criteria

- [ ] All three named tests pass individually.
- [ ] `cargo test -p openplotva-web`, `-p openplotva-llm`, `-p openplotva-updates` each pass (no new failures introduced).
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings` exits 0.
- [ ] Each sub-task's change is minimal and confined to its in-scope file(s); any regression-vs-stale-test judgment that went to STOP is reported, not guessed.
- [ ] `plans/README.md` status row updated.

## STOP conditions

- Sub-task 1: `web/admin/index.html` looks corrupted rather than deliberately edited.
- Sub-task 2: the prompt template lost required system-contract content (regression, not test drift).
- Sub-task 3: the stale-skip change appears intentional (test is stale) — confirm with the maintainer before weakening the test.
- Any fix would touch files beyond the sub-task's in-scope list.

## Maintenance notes

- These three are the concrete evidence for plan 001: they landed on `main` precisely because CI
  never ran the test suite. Land this plan with (or just before) 001 so enabling the CI test gate
  does not immediately block all PRs.
- After this lands, `tools/rust-fast-gate.sh` should exit 0 on a machine with adequate disk; if it
  still fails, re-check for additional latent failures the same way these were found.
