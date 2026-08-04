# Runtime Image Queue ETA Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Estimate user-visible drawing wait time from recent continuously backlogged image throughput instead of historical queue age.

**Architecture:** `openplotva-taskman` derives clean per-queue cycle samples from consecutive persisted `execution_started_at` timestamps. The existing `TaskmanDialogToolAdapter` continues calling `estimate_queue_time_for_depth`, so the Telegram notice receives the corrected estimate without a new dependency or storage call.

**Tech Stack:** Rust 1.95, OpenPlotva Taskman, `time::OffsetDateTime`, existing in-memory queue and dialog-tool tests.

## Global Constraints

- Deliver one ready PR against `main` and leave merge/deploy for a separate request.
- Preserve queue names, priorities, payloads, WAL shape, Telegram text shape, and provider routing.
- Use the latest 20 eligible backlog cycle intervals, continuous p5/p95 trimming, and a minimum clean sample count of 5.
- Preserve the current static fallback, 30-second floor, four-hour safety cap, and worker-count divisor.
- Preserve current text-queue sampling behavior.
- Do not add dependencies or database migrations.

---

### Task 1: Backlog-only image throughput samples

**Files:**
- Modify: `crates/openplotva-taskman/src/lib.rs`
- Test: `crates/openplotva-taskman/src/lib.rs`
- Test: `crates/openplotva-app/src/dialog_tools.rs`

**Interfaces:**
- Consumes: completed `TaskQueueRecord` values with `job.created` and `execution_started_at`.
- Produces: unchanged `CleanQueueStats` and `estimate_queue_time_for_depth(queue_name, depth)` APIs with corrected image samples.

- [ ] **Step 1: Write the failing queue-age regression test**

Add a helper that seeds completed image jobs whose creation times are three hours old but whose execution starts are 45 seconds apart. Add `image_queue_eta_uses_backlogged_start_cadence_not_queue_age`, asserting that ten jobs ahead estimate to the literal 450 seconds.

- [ ] **Step 2: Write the user-visible notice regression**

Seed six or more completed, continuously backlogged regular-image cycles at 45 seconds in the dialog-tool test queue, then seed ten pending jobs and schedule the next draw. Assert the notice contains `7 мин. 30 сек.` for ten jobs ahead rather than the eight-minute static fallback or a queue-age-derived value.

- [ ] **Step 3: Run both regressions and verify RED**

Run: `CARGO_TARGET_DIR=/tmp/openplotva-eta.Vn8dcC cargo test -p openplotva-taskman image_queue_eta_uses_backlogged_start_cadence_not_queue_age` and `CARGO_TARGET_DIR=/tmp/openplotva-eta.Vn8dcC cargo test -p openplotva-app schedule_image_notice_uses_runtime_backlog_cadence`

Expected: both FAIL because the current sample treats the three-hour queue age as one job duration; the direct estimate clamps to four hours and the rendered notice does not contain the expected runtime-derived duration.

- [ ] **Step 4: Add backlog and outlier boundary tests**

Add literal tests proving that a long interval is excluded when the later job did not yet exist at the earlier start, p5/p95 trimming removes a 10,000-second cycle outlier, and fewer than five eligible cycles retain `fallback_queue_time_estimate`.

- [ ] **Step 5: Implement minimal image cycle sampling**

For image queues, sort completed records by `execution_started_at`, form newest-to-previous pairs, require `newer.job.created <= previous_start`, keep positive intervals, and truncate to `sample_size`. Feed those seconds through the existing percentile and clean-mean code. Keep the enqueue-to-completion sample path for `TEXT_QUEUE_NAME`.

- [ ] **Step 6: Run focused tests and verify GREEN**

Run: `CARGO_TARGET_DIR=/tmp/openplotva-eta.Vn8dcC cargo test -p openplotva-taskman` and `CARGO_TARGET_DIR=/tmp/openplotva-eta.Vn8dcC cargo test -p openplotva-app dialog_tools`

Expected: all Taskman unit/doc tests and focused dialog-tool tests pass.

- [ ] **Step 7: Commit the estimator and notice coverage**

Commit message: `fix: estimate image queue time from runtime throughput`

### Task 2: Full verification and PR delivery

**Files:**
- Verify all changed source, tests, spec, and plan files.

**Interfaces:**
- Consumes: completed estimator and notice coverage.
- Produces: one ready PR with green local checks and handled review artifacts.

- [ ] **Step 1: Format and inspect the diff**

Run: `cargo fmt --all -- --check` and `git diff --check origin/main...HEAD`.

- [ ] **Step 2: Run repository gates**

Run: `CARGO_TARGET_DIR=/tmp/openplotva-eta.Vn8dcC cargo clippy --workspace --all-targets -- -D warnings` and `CARGO_TARGET_DIR=/tmp/openplotva-eta.Vn8dcC cargo test --workspace`.

- [ ] **Step 3: Push and open a ready PR**

Push `fix/image-queue-runtime-eta` and open a non-draft PR into `main` with the production evidence, estimator definition, fallback behavior, and verification commands.

- [ ] **Step 4: Complete the PR delivery loop**

Poll CI, issue comments, reviews, full bot-comment bodies, and inline review threads. Fix or rebut every finding, resolve every inline thread, and leave the PR unmerged unless the user separately requests merge.
