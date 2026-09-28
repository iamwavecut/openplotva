# Test builds and artifact lifetime

`tools/rust-fast-gate.sh` checks formatting, runs Clippy with all targets/features,
then runs the complete workspace test suite and doctests. It does not select a
smaller test subset. PostgreSQL checks remain a separate serial CI step using the
same compiled test binaries. Cargo commands use `--locked`.

Normal runs keep Cargo's build cache. `--ephemeral` overrides `CARGO_TARGET_DIR`
with a newly created directory and removes it on exit, including failure and
handled SIGINT/SIGTERM. Both modes put test temporary files under a directory
owned by that invocation and remove it on exit. Existing target directories are
never cleaned. SIGKILL and host crashes cannot execute the cleanup handler;
Cargo's shared download cache is intentionally retained.

Local builds use `line-tables-only` debug data. CI and ephemeral builds default to
no debug data and no incremental state. Assertions and overflow checks remain
active. CI panic backtraces lose source lines; assertion diagnostics retain their
call-site location. For local variable inspection, set
`CARGO_PROFILE_DEV_DEBUG=2`. Explicit debug/incremental environment overrides are
respected by the ephemeral gate. Release settings are unchanged.

These choices follow the [Cargo profile documentation](https://doc.rust-lang.org/cargo/reference/profiles.html).
The existing [Rust cache action](https://github.com/Swatinem/rust-cache#cache-details)
already disables incremental compilation and caches dependencies; the change does
not add caches or expand their retention. The first CI run with the new profile
must rebuild dependencies whose compiler settings changed.

## Measurements, 2026-09-28

Source baseline: `51e9af13832132e70632166456e38e1964336a79`.
Host: Mac14,6, 12 logical CPUs, Rust/Cargo 1.95.0, aarch64-apple-darwin.
Both configurations used `CARGO_INCREMENTAL=0`, the same lockfile and test set,
and separate task-owned target directories. This is a local measurement, not a
GitHub-hosted Linux CI benchmark.

With dependencies already built and only the 18 workspace packages cleaned:

| Workspace rebuild and complete test run | Previous CI settings | New CI settings |
| --- | ---: | ---: |
| Wall time | 259.43 s | 148.83 s |
| Aggregate child-process CPU time | 386.44 s | 299.40 s |
| Tests passed / ignored | 2,407 / 9 | 2,407 / 9 |

This single sequential pair took 42.6% less wall time and 22.5% less CPU time.
It models a restored dependency cache, not a no-change rerun. Background load on
the shared host varied, so these figures are observations rather than a CI speed
guarantee.

| Cold build and complete test run | Previous CI settings | New CI settings |
| --- | ---: | ---: |
| Debug data | `line-tables-only` | `0` |
| Target disk allocation (`du -sk`) | 2,814,276 KiB | 1,585,476 KiB |
| Total wall time | 158.58 s | 282.52 s |
| Tests passed / ignored | 2,407 / 9 | 2,407 / 9 |

The target allocation fell by 43.7%. The cold runs had varying background load
and **do not establish a build-speed improvement**. Test names and outcomes were
identical. Tests requiring optional service credentials return early without
those credentials; these runs do not establish live PostgreSQL/Redis/provider
coverage. The existing CI PostgreSQL service and serial checks remain enabled.

To reproduce a cold comparison with rustup on macOS:

```bash
export PATH="$HOME/.cargo/bin:$PATH"
bench_root="$(mktemp -d "${TMPDIR:-/tmp}/openplotva-build-comparison.XXXXXX")"
trap 'rm -rf -- "$bench_root"' EXIT
for debug in line-tables-only 0; do
  mkdir -p "$bench_root/$debug/tmp"
  CARGO_TARGET_DIR="$bench_root/$debug/target" \
    TMPDIR="$bench_root/$debug/tmp" CARGO_INCREMENTAL=0 \
    CARGO_PROFILE_DEV_DEBUG="$debug" \
    /usr/bin/time -p cargo +1.95.0 test --locked --workspace --timings
  du -sk "$bench_root/$debug/target"
done
```

For a restored-dependency comparison, clean only the 18 workspace packages with
`cargo clean -p <package>` inside each disposable target directory, then repeat
its test command. Do not clean a shared or user-owned target directory to run this
measurement. Preserve Rust 1.95.0 at the front of `PATH` for both Cargo and rustc;
selecting a Cargo version alone can still invoke a system rustc.

## Verification

With Rust 1.95.0, `CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0`, and a
task-owned `CARGO_TARGET_DIR`:

- `tools/rust-fast-gate.sh`: passed, including `cargo fmt --all -- --check`,
  `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`,
  and `cargo test --locked --workspace` (2,407 passed, 9 ignored).
  The test step reused compiled binaries after Clippy; it emitted no compilation
  steps. Its temporary directory was empty after the gate exited.
- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tools/tests -v`:
  6 tests passed, covering retained caches, disposable builds, profile overrides,
  failure propagation, SIGINT/SIGTERM cleanup, and invalid arguments.
- `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tools/maintenance/tests -p test_required_checks.py -v`:
  2 tests passed; required CI jobs remain present on pull requests.
- `bash -n tools/rust-fast-gate.sh`, `shellcheck tools/rust-fast-gate.sh`,
  `actionlint .github/workflows/ci.yml`, and `git diff --check`: passed.

GitHub Actions and live service integrations were not run for this local change.
