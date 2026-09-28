"""Exercise gate failure/cleanup behavior without compiling the workspace."""

import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import unittest


GATE = Path(__file__).resolve().parents[1] / "rust-fast-gate.sh"


class RustFastGateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="fast gate test ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.scratch = self.root / "scratch"
        self.scratch.mkdir()
        self.target = self.root / "caller-target"
        self.target.mkdir()
        (self.target / "keep").write_text("caller data")
        self.log = self.root / "calls.jsonl"
        cargo = self.bin / "cargo"
        cargo.write_text("""#!/usr/bin/env python3
import json, os, pathlib, sys, time
scratch = pathlib.Path(os.environ['TMPDIR'])
(scratch / 'test-leftover').write_text('fixture')
target = pathlib.Path(os.environ['CARGO_TARGET_DIR'])
target.mkdir(parents=True, exist_ok=True)
(target / 'build-leftover').write_text('artifact')
with open(os.environ['GATE_CALLS'], 'a') as log:
    log.write(json.dumps({'args': sys.argv[1:], 'tmp': str(scratch),
                          'target': str(target),
                          'debug': os.environ.get('CARGO_PROFILE_DEV_DEBUG'),
                          'incremental': os.environ.get('CARGO_INCREMENTAL')}) + '\\n')
if os.environ.get('BLOCK_COMMAND') == sys.argv[1]:
    time.sleep(30)
if os.environ.get('FAIL_COMMAND') == sys.argv[1]:
    sys.exit(42)
""")
        cargo.chmod(0o755)
        self.env = {
            **os.environ,
            "PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
            "TMPDIR": str(self.scratch),
            "CARGO_TARGET_DIR": str(self.target),
            "GATE_CALLS": str(self.log),
            "CARGO_PROFILE_DEV_DEBUG": "",
            "CARGO_INCREMENTAL": "",
        }

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def run_gate(self, *args, **env):
        return subprocess.run(
            ["bash", str(GATE), *args], cwd=GATE.parents[1],
            env={**self.env, **env}, text=True, capture_output=True, timeout=10,
        )

    def assert_clean(self):
        self.assertEqual(list(self.scratch.iterdir()), [])
        self.assertEqual((self.target / "keep").read_text(), "caller data")

    def test_normal_run_keeps_build_cache_but_removes_test_files(self):
        result = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        self.assertEqual([call['args'][0] for call in calls], ['fmt', 'clippy', 'test'])
        self.assertIn('--locked', calls[-1]['args'])
        self.assertEqual(calls[-1]['target'], str(self.target))
        self.assertTrue((self.target / "build-leftover").exists())
        self.assert_clean()

    def test_ephemeral_run_removes_only_its_own_artifacts(self):
        result = self.run_gate('--ephemeral', '--skip-clippy')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([call['args'][0] for call in self.calls()], ['fmt', 'test'])
        for call in self.calls():
            self.assertFalse(Path(call['target']).exists())
            self.assertEqual(call['debug'], '0')
            self.assertEqual(call['incremental'], '0')
        self.assertFalse((self.target / "build-leftover").exists())
        self.assert_clean()

    def test_ephemeral_run_respects_explicit_profile_overrides(self):
        result = self.run_gate('--ephemeral', CARGO_PROFILE_DEV_DEBUG='2', CARGO_INCREMENTAL='1')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.calls()[-1]['debug'], '2')
        self.assertEqual(self.calls()[-1]['incremental'], '1')
        self.assert_clean()

    def test_failure_propagates_and_cleans_up_at_every_stage(self):
        for command in ('fmt', 'clippy', 'test'):
            with self.subTest(command=command):
                self.log.unlink(missing_ok=True)
                result = self.run_gate('--ephemeral', FAIL_COMMAND=command)
                self.assertEqual(result.returncode, 42, result.stderr)
                self.assertEqual(self.calls()[-1]['args'][0], command)
                self.assert_clean()

    def test_termination_cleans_up(self):
        for stop_signal, expected_status in ((signal.SIGTERM, 143), (signal.SIGINT, 130)):
            with self.subTest(signal=stop_signal):
                self.log.unlink(missing_ok=True)
                process = subprocess.Popen(
                    ['bash', str(GATE), '--ephemeral'], cwd=GATE.parents[1],
                    env={**self.env, 'BLOCK_COMMAND': 'test'},
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
                )
                try:
                    deadline = time.monotonic() + 5
                    while not (self.log.exists() and len(self.calls()) == 3):
                        if time.monotonic() > deadline:
                            self.fail('gate did not reach the blocking test command')
                        time.sleep(0.01)
                    os.killpg(process.pid, stop_signal)
                    self.assertEqual(process.wait(timeout=5), expected_status)
                    self.assert_clean()
                finally:
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()

    def test_invalid_extra_argument_does_not_run_cargo(self):
        result = self.run_gate('--skip-clippy', '--typo')
        self.assertEqual(result.returncode, 2)
        self.assertFalse(self.log.exists())
        self.assert_clean()


if __name__ == '__main__':
    unittest.main()
