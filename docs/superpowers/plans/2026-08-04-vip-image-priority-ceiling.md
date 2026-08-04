# VIP Image Priority Ceiling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give VIP image jobs persisted base priority `5` and effective priority `7` after ten minutes while every non-VIP accelerator job remains capped at `6`.

**Architecture:** `openplotva-taskman` owns accelerator claim ranking and will select a ceiling from the job's queue and type. `openplotva-app` will persist the new VIP image base priority through the existing queue plan. Legacy priority-`4` `image-vip` records will receive a semantic floor of `5` at claim time, avoiding a migration and preserving FIFO arbitration with new jobs.

**Tech Stack:** Rust 1.95, OpenPlotva Taskman, existing in-memory queue and dialog-tool tests.

## Global Constraints

- Deliver one ready PR against `main`; do not merge or deploy without a separate request.
- VIP image base priority is exactly `5`.
- VIP image effective priority is `6` after five minutes and `7` after ten minutes.
- ASR and regular image effective priorities never exceed `6`.
- Preserve queue names, payloads, WAL shape, retries, and database schema.
- Add no dependency or migration.

---

### Task 1: Lock the corrected accelerator contract with failing tests

**Files:**
- Modify: `crates/openplotva-taskman/src/lib.rs`
- Modify: `crates/openplotva-app/src/dialog_tools.rs`

**Interfaces:**
- Consumes: `TaskQueueRecord`, `JobType`, `IMAGE_VIP_QUEUE_NAME`, `OffsetDateTime`, and the existing five-minute aging interval.
- Produces: behavioral coverage for VIP base priority `5`, the `09:59`/`10:00` boundary, legacy priority `4`, and non-VIP ceiling `6`.

- [ ] **Step 1: Write Taskman boundary tests**

Replace the old parity-only VIP test with literal cases proving that a 30-minute regular job blocks a `09:59` VIP job at effective priority `6`, while a `10:00` VIP job claims first at effective priority `7`. Add a legacy priority-`4` VIP case and assert that a one-hour regular job remains at effective priority `6`.

- [ ] **Step 2: Write the scheduling test**

Change the existing VIP scheduling assertion to require persisted priority `5` rather than the broad `HIGHEST_PRIORITY` constant.

- [ ] **Step 3: Verify RED**

Run:

```bash
CARGO_TARGET_DIR=/tmp/openplotva-vip-priority.RsuBLJ cargo test -p openplotva-taskman vip_draw_reaches_priority_seven_at_ten_minutes
CARGO_TARGET_DIR=/tmp/openplotva-vip-priority.RsuBLJ cargo test -p openplotva-app taskman_dialog_tool_adapter_uses_vip_queue_and_user_limit
```

Expected: Taskman still caps VIP at `6`, and the dialog tool still persists priority `4`.

### Task 2: Implement VIP-specific base and ceiling

**Files:**
- Modify: `crates/openplotva-taskman/src/lib.rs`
- Modify: `crates/openplotva-app/src/dialog_tools.rs`

**Interfaces:**
- Produces: public `IMAGE_VIP_PRIORITY: Priority = 5`; `accelerator_effective_priority` returns at most `7` only for image jobs in `image-vip`, and at most `6` otherwise.

- [ ] **Step 1: Add the VIP image priority constant**

Define `IMAGE_VIP_PRIORITY` next to the existing priority constants without changing `HIGHEST_PRIORITY`, because music and unrelated queues retain their current contracts.

- [ ] **Step 2: Apply semantic base and ceiling during arbitration**

For `ImageGen` or `ImageEdit` records in `image-vip`, compute from `max(stored_priority, IMAGE_VIP_PRIORITY)` and cap at `ASR_PRIORITY + 1`. Keep the stored priority and `ASR_PRIORITY` ceiling for every other accelerator record.

- [ ] **Step 3: Persist priority 5 for new VIP image work**

Use `IMAGE_VIP_PRIORITY` in `image_gen_queue_plan(true)`; both image generation and image editing already consume this plan.

- [ ] **Step 4: Verify GREEN and affected boundaries**

Run:

```bash
CARGO_TARGET_DIR=/tmp/openplotva-vip-priority.RsuBLJ cargo test -p openplotva-taskman
CARGO_TARGET_DIR=/tmp/openplotva-vip-priority.RsuBLJ cargo test -p openplotva-app dialog_tools
```

Expected: all Taskman and dialog-tool tests pass.

### Task 3: Verify and deliver

**Files:**
- Verify all changed files and the focused diff.

**Interfaces:**
- Produces: one ready PR with local and remote gates green and all review findings handled.

- [ ] **Step 1: Run local repository gates**

```bash
cargo fmt --all -- --check
git diff --check origin/main...HEAD
CARGO_TARGET_DIR=/tmp/openplotva-vip-priority.RsuBLJ cargo clippy --workspace --all-targets -- -D warnings
CARGO_TARGET_DIR=/tmp/openplotva-vip-priority.RsuBLJ cargo test --workspace
```

- [ ] **Step 2: Commit, push, and open a ready PR**

Use a neutral commit message and a non-draft PR explaining the exact `5 → 6 → 7` VIP behavior, the non-VIP ceiling `6`, and legacy-record normalization.

- [ ] **Step 3: Complete the PR delivery loop**

Poll checks plus full issue comments, reviews, inline comments, and review threads. Fix or rebut every finding and leave the fully green PR open for the separately authorized merge/deploy step.
