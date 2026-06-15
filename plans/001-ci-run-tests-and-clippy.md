# Plan 001: CI runs the existing fast quality gate (clippy + tests) on every push and PR

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 9f32c4b..HEAD -- .github/workflows/ci.yml tools/rust-fast-gate.sh`
> If either file changed since this plan was written, compare the "Current
> state" excerpts against the live files before proceeding; on a mismatch,
> treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: dx
- **Planned at**: commit `9f32c4b`, 2026-06-15

## Why this matters

The workspace has ~1450 test functions and three workspace-level clippy deny-lints
(`unwrap_used`, `todo`, `dbg_macro` in `Cargo.toml`), but **no push/PR CI job runs
`cargo test` or `cargo clippy`**. `ci.yml` runs only `cargo fmt --check`,
`cargo check`, a release build, and a dependency gate. So a PR can land code that
fails tests or violates the deny-lints, and nobody finds out until runtime. The
repo already contains the exact gate that should run — `tools/rust-fast-gate.sh`,
whose own header says it is "used by CI and local development" — but no workflow
invokes it. This plan wires that existing script into CI. It is also a prerequisite
for safely doing dependency bumps and refactors (e.g. plan 008), because those rely
on CI actually running the test suite.

## Current state

- `.github/workflows/ci.yml` — CI workflow. The `rust-workspace` job (lines 21–61)
  installs Rust with only the `rustfmt` component and runs fmt + check + build:

  ```yaml
  # ci.yml:36-53
        - name: Install Rust
          run: |
            rustup toolchain install "$RUST_VERSION" --profile minimal --component rustfmt
            rustup default "$RUST_VERSION"
        - name: Cache Rust build artifacts
          uses: Swatinem/rust-cache@v2
          with:
            shared-key: rust-${{ runner.os }}-ubuntu-22.04-${{ env.RUST_VERSION }}-workspace
        - name: Check Rust formatting
          run: cargo fmt --all -- --check
        - name: Check Rust workspace
          run: cargo check --workspace --all-targets --all-features
        - name: Build release binary
          run: cargo build --locked --release -p openplotva-app
  ```

  Note: the toolchain is installed with `--component rustfmt` only. `cargo clippy`
  needs the `clippy` component, so the install step must also request it.

- `tools/rust-fast-gate.sh` — the existing gate script (NOT referenced by any
  workflow today). Its body runs exactly these four commands in order:

  ```sh
  # tools/rust-fast-gate.sh:43-46
  run cargo fmt --all -- --check
  run cargo check --workspace --all-targets --all-features
  run cargo clippy --workspace --all-targets --all-features -- -D warnings
  run cargo test --workspace
  ```

- `Cargo.toml:78-81` — the deny-lints that only `cargo clippy` enforces:

  ```toml
  [workspace.lints.clippy]
  dbg_macro = "deny"
  todo = "deny"
  unwrap_used = "deny"
  ```

Convention: this repo keeps reusable gate logic in `tools/*.sh` and calls those
scripts from workflows (e.g. `ci.yml:115` runs `tools/rust-dependency-gate.sh`,
`rust-deep.yml:76` runs `tools/rust-deep-gate.sh`). Match that — call the script,
don't inline a second copy of the commands.

The test suite is runnable without external services: the 5 DSN-gated live tests are
`#[ignore]`d and skipped by a default `cargo test` run, so no Postgres/Redis is needed
in CI (consistent with the script being described as a CI gate).

## Commands you will need

| Purpose            | Command                                                                 | Expected on success |
|--------------------|-------------------------------------------------------------------------|---------------------|
| Run the gate local | `tools/rust-fast-gate.sh`                                               | exit 0, all 4 steps pass |
| Lint workflow YAML | `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml'))"` | exit 0, no exception |
| Clippy only        | `cargo clippy --workspace --all-targets --all-features -- -D warnings`  | exit 0, no warnings |
| Tests only         | `cargo test --workspace`                                                | all pass (ignored ones skipped) |

## Scope

**In scope** (the only files you should modify):
- `.github/workflows/ci.yml`

**Out of scope** (do NOT touch):
- `tools/rust-fast-gate.sh` — it already does the right thing; do not edit it.
- `.github/workflows/security.yml`, `rust-deep.yml`, `deploy-production.yml`.
- Any source code or test — if a test or clippy lint actually fails when you run
  the gate locally, that is a real pre-existing failure: STOP and report it
  (see STOP conditions). Do NOT "fix" source to make CI green.

## Git workflow

- Branch: `advisor/001-ci-fast-gate`
- One commit. Message style: imperative, capitalized (match `git log`, e.g.
  "Run clippy and tests in CI via the fast gate").
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Confirm the gate passes locally before changing CI

Run the existing gate exactly as CI will run it. This establishes that wiring it in
will not break the build for a reason unrelated to this plan.

```sh
tools/rust-fast-gate.sh
```

**Verify**: exit 0; the script prints all four steps (fmt, check, clippy, test) and
the test step reports passes with no failures.

If this fails, STOP (see STOP conditions) — the failure is pre-existing and must be
triaged separately, not hidden by skipping CI.

### Step 2: Add the `clippy` component to the toolchain install

In `ci.yml`, the `rust-workspace` job's "Install Rust" step (around line 37–39),
add `clippy` to the components so the gate's clippy step can run:

```yaml
      - name: Install Rust
        run: |
          rustup toolchain install "$RUST_VERSION" --profile minimal --component rustfmt --component clippy
          rustup default "$RUST_VERSION"
```

**Verify**: `git diff .github/workflows/ci.yml` shows only `--component clippy`
added to that one line.

### Step 3: Replace the inline fmt/check steps with the fast gate

In the `rust-workspace` job, replace the two steps "Check Rust formatting" and
"Check Rust workspace" (the `cargo fmt --all -- --check` and
`cargo check --workspace --all-targets --all-features` steps, ci.yml lines 46–50)
with a single step that runs the gate. Keep the "Build release binary" step and the
artifact upload that follow it unchanged.

```yaml
      - name: Run fast Rust quality gate
        run: tools/rust-fast-gate.sh
```

The gate already covers fmt + check + clippy + test, so the two removed steps are
fully subsumed. The release build step stays because the gate does not build the
release binary.

**Verify**: `git diff .github/workflows/ci.yml` shows the two old steps removed and
the single gate step added; the "Build release binary" and "Upload release binary"
steps are still present and unchanged.

### Step 4: Validate the workflow YAML parses

```sh
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml')); print('ok')"
```

**Verify**: prints `ok`, exit 0.

## Test plan

- No new Rust tests (this is a CI configuration change).
- The verification IS the gate run in Step 1 plus the YAML parse in Step 4.
- Optional sanity check that the script is now wired: `grep -n "rust-fast-gate.sh" .github/workflows/ci.yml` returns one match in the `rust-workspace` job.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `tools/rust-fast-gate.sh` exits 0 locally.
- [ ] `grep -c "rust-fast-gate.sh" .github/workflows/ci.yml` returns `1`.
- [ ] `grep -c "component clippy" .github/workflows/ci.yml` returns `1`.
- [ ] The inline `cargo fmt --all -- --check` and `cargo check --workspace --all-targets --all-features` steps are gone from `ci.yml` (`grep -c "cargo fmt --all -- --check" .github/workflows/ci.yml` returns `0`).
- [ ] `cargo build --locked --release -p openplotva-app` is still present in `ci.yml`.
- [ ] The YAML parses (Step 4).
- [ ] No files outside `.github/workflows/ci.yml` are modified (`git status`).
- [ ] `plans/README.md` status row updated.

## STOP conditions

Stop and report back (do not improvise) if:

- `tools/rust-fast-gate.sh` fails in Step 1 (a real clippy or test failure exists on
  the base commit). Report which step failed and the output — do NOT modify source
  code to make it pass; that is out of scope and a separate decision.
- `ci.yml` no longer matches the "Current state" excerpt (it was restructured since
  this plan was written).
- The fast-gate script no longer contains the four expected commands.

## Maintenance notes

- After this lands, every PR runs clippy + the full test suite. Expect the first few
  PRs from other contributors to surface latent clippy/test failures that were never
  gated before — that is the point.
- If CI time becomes a concern, the gate can later be split into parallel jobs
  (clippy, test) or switched to `cargo nextest`; that is a follow-up, not part of this
  plan.
- A reviewer should confirm the release-build step and artifact upload still run
  (the container-image job depends on that artifact).
