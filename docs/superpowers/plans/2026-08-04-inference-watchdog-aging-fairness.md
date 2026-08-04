# Inference Watchdog and Aging Fairness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound every OpenPlotva draw attempt and make accelerator scheduling starvation-resistant without changing persisted job contracts.

**Architecture:** `openplotva-app` owns the absolute Discovery draw deadline because it owns the client transport. `openplotva-taskman` computes an ephemeral effective priority for ASR and image records and uses one global rank when separate queue workers contend.

**Tech Stack:** Rust 1.95, Tokio time, OpenPlotva Taskman, existing in-memory queue tests.

## Global Constraints

- Deliver one ready PR against `main`.
- Preserve queue names, stored priorities, payloads, WAL shape, retries, and routing.
- Use a 300-second aging step and cap effective accelerator priority at `ASR_PRIORITY`.
- Use the configured draw timeout as one absolute submit-plus-poll watchdog deadline.
- Do not add dependencies or database migrations.

---

### Task 1: Deterministic accelerator aging

**Files:**
- Modify: `crates/openplotva-taskman/src/lib.rs`
- Test: `crates/openplotva-taskman/src/lib.rs`

**Interfaces:**
- Consumes: `TaskQueueRecord`, `JobType`, `OffsetDateTime`, existing queue priorities.
- Produces: claim arbitration used transparently by `dequeue` and `dequeue_matching`.

- [ ] **Step 1: Write failing fairness tests**

Add literal boundary cases showing that fresh ASR wins, a 29:59 regular job still waits, a 30:00 regular job wins the created-time tie, a 10:00 VIP job wins its tie, and an aged processing draw blocks a fresh ASR claim.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `cargo test -p openplotva-taskman aging`

Expected: the new aged-image assertions fail because current arbitration compares only stored priorities and excludes ASR candidates.

- [ ] **Step 3: Implement effective ranking**

Add a five-minute aging interval, compute a saturating non-negative boost capped at `ASR_PRIORITY`, and replace the image-only blocker with a global accelerator blocker. Compare effective priority first, then `created`, then `id`.

- [ ] **Step 4: Run Taskman tests and verify GREEN**

Run: `cargo test -p openplotva-taskman`

Expected: all Taskman unit and doc tests pass.

- [ ] **Step 5: Commit Taskman behavior**

Commit message: `fix: prevent accelerator queue starvation`

### Task 2: Absolute draw inference watchdog

**Files:**
- Modify: `crates/openplotva-app/src/image_jobs.rs`
- Test: `crates/openplotva-app/src/image_jobs.rs`

**Interfaces:**
- Consumes: `AifarmDrawApiConfig::timeout`, `AifarmHttpTransport::send`.
- Produces: one absolute `tokio::time::Instant` deadline shared by submit and poll requests.

- [ ] **Step 1: Write failing hanging-transport tests**

Add a transport double whose future never resolves. Cover both submission and status polling, with a longer outer safety timeout proving that the configured inner watchdog returns first.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `cargo test -p openplotva-app image_jobs::tests::aifarm_draw_api_watchdog`

Expected: the outer safety timeout wins because transport sends are currently unbounded.

- [ ] **Step 3: Implement one absolute deadline**

Create the deadline before submission, pass it through submit and wait, and wrap every transport send with `tokio::time::timeout_at`. Return a retryable timeout message naming the job and request phase.

- [ ] **Step 4: Run focused image tests and verify GREEN**

Run: `cargo test -p openplotva-app image_jobs`

Expected: watchdog and existing image tests pass.

- [ ] **Step 5: Commit watchdog behavior**

Commit message: `fix: bound draw inference requests`

### Task 3: Full verification and delivery

**Files:**
- Verify all changed files and the focused diff.

**Interfaces:**
- Consumes: completed Taskman and draw-client changes.
- Produces: ready PR with green local checks and handled review findings.

- [ ] **Step 1: Format and inspect the diff**

Run: `cargo fmt --all -- --check` and `git diff --check`.

- [ ] **Step 2: Run repository gates**

Run: `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace` with the task-specific Cargo target.

- [ ] **Step 3: Push and open a ready PR**

Push `fix/inference-watchdog-aging-fairness` and open a non-draft PR into `main` describing the watchdog boundary and exact aging thresholds.

- [ ] **Step 4: Complete the PR delivery loop**

Poll CI, issue comments, reviews, inline threads, PR-Agent persistent bodies, Qodo, and Danger. Fix or rebut every finding, resolve every inline thread, and leave the PR unmerged unless the user separately requests merge.
