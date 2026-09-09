import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from contracts import InvalidResult, Deferred, QuotaUnavailable, diagnosis
from runner import Runner, validate_patch, secret_file, verification_receipt


def patch_for(path, extra=""):
    return (f"diff --git a/{path} b/{path}\n{extra}--- a/{path}\n+++ b/{path}\n@@ -1 +1 @@\n-before\n+after\n").encode()


class RunnerTests(unittest.TestCase):
    def test_runtime_revision_tracks_promoted_deployment_not_pr_build(self):
        build = "b" * 40
        deployed = "d" * 40
        cases = [
            ("ghcr.io/iamwavecut/openplotva:" + deployed, build, deployed, build),
            ("ghcr.io/iamwavecut/openplotva@sha256:" + "a" * 64, build, build, build),
            ("unrelated/image:" + deployed, build, build, build),
            ("ghcr.io/iamwavecut/openplotva:latest", "invalid", None, None),
            ("unrelated/image:" + deployed, None, None, None),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            runner = Runner({"state_dir": tmp, "source_dir": tmp + "/source",
                             "image": "sha256:" + "a" * 64, "production_container": "production"}, None)
            for reference, label, expected, expected_build in cases:
                with self.subTest(reference=reference):
                    inspected = [{"Config": {"Image": reference,
                        "Labels": {"org.opencontainers.image.revision": label}},
                        "Image": "sha256:" + "a" * 64, "RestartCount": 0,
                        "State": {"OOMKilled": False, "Running": True}}]
                    responses = [SimpleNamespace(returncode=0, stdout=json.dumps(inspected).encode()),
                                 SimpleNamespace(returncode=1, stdout=b"")]
                    with patch("runner.command", side_effect=responses), \
                         patch("runner.memory_available", return_value=8 * 1024 ** 3):
                        snapshot = runner.host_snapshot()
                    self.assertEqual(snapshot["revision"], expected)
                    self.assertEqual(snapshot["runtime"]["revision"], expected)
                    self.assertEqual(snapshot["runtime"]["build_revision"], expected_build)

    def test_failed_run_charges_elapsed_time_without_losing_quota_usage(self):
        for error in (Deferred("resource boundary"), QuotaUnavailable(usage={"total_tokens": 20})):
            with self.subTest(error=type(error).__name__), tempfile.TemporaryDirectory() as tmp:
                runner = Runner({"state_dir": tmp, "source_dir": tmp + "/source",
                                 "image": "sha256:" + "a" * 64}, None)
                job = {"id": "elapsed-test", "base_sha": "b" * 40, "stage": "deep",
                       "incident_id": 1, "remaining_seconds": 14400}
                with patch.object(runner, "preflight"), patch.object(runner, "refresh_source"), \
                     patch.object(runner, "_checkout", side_effect=error), \
                     patch("runner.command", return_value=SimpleNamespace(stdout=b"", returncode=0)), \
                     patch("runner.time.monotonic", side_effect=[100, 125]), \
                     self.assertRaises(type(error)) as caught:
                    runner.run(job, {})
                self.assertEqual(getattr(caught.exception, "active_seconds", 0), 25)
                if isinstance(error, QuotaUnavailable):
                    self.assertEqual(caught.exception.usage, {"total_tokens": 20})

    def test_cleanup_failure_preserves_quota_backoff_accounting_and_recovery_journal(self):
        with tempfile.TemporaryDirectory() as tmp:
            runner = Runner({"state_dir": tmp, "source_dir": tmp + "/source",
                             "image": "sha256:" + "a" * 64}, None)
            job = {"id": "cleanup-test", "base_sha": "b" * 40, "stage": "deep",
                   "incident_id": 1, "remaining_seconds": 14400}
            error = QuotaUnavailable(usage={"total_tokens": 20}, retry_after_seconds=900)

            def host_command(args, **kwargs):
                if args[0] == "umount":
                    raise Deferred("unmount failed")
                return SimpleNamespace(stdout=b"", returncode=0)

            with patch.object(runner, "preflight"), patch.object(runner, "refresh_source"), \
                 patch.object(runner, "_checkout", side_effect=error), \
                 patch("runner.command", side_effect=host_command), \
                 patch("runner.time.monotonic", side_effect=[100, 130]), \
                 self.assertRaises(QuotaUnavailable) as caught:
                runner.run(job, {})
            self.assertIs(caught.exception, error)
            self.assertEqual(caught.exception.active_seconds, 30)
            self.assertEqual(caught.exception.usage, {"total_tokens": 20})
            self.assertEqual(caught.exception.retry_after_seconds, 900)
            self.assertEqual(len(list(Path(tmp).glob("workspaces/*/manifest.json"))), 1)
            self.assertEqual(len(list(Path(tmp).glob("artifacts/*/*/cleanup-failed.json"))), 1)

    def test_provider_quota_stops_only_current_container_and_preserves_counters(self):
        with tempfile.TemporaryDirectory() as tmp:
            runner = Runner({"state_dir": tmp, "source_dir": tmp + "/source", "image": "sha256:" + "a" * 64}, None)
            event = threading.Event()
            event.set()
            quota = SimpleNamespace(quota_unavailable=event, usage={"rate_limited": 1}, retry_after_seconds=7200)
            with patch("runner.subprocess.Popen") as process, patch("runner.command") as command:
                process.return_value.poll.return_value = None
                process.return_value.wait.return_value = 143
                with self.assertRaises(QuotaUnavailable) as caught:
                    runner._exec("opm-synthetic", ["agent", "600"], "run-1", time.monotonic() + 600, quota=quota)
                self.assertEqual(caught.exception.usage, {"rate_limited": 1})
                self.assertEqual(caught.exception.retry_after_seconds, 7200)
                command.assert_called_once_with(["docker", "stop", "--time", "1", "opm-synthetic"], check=False, timeout=15)

    def test_verified_receipt_requires_matching_exit_status_and_exact_checks(self):
        passed = [{"name": name, "passed": True} for name in ("fmt", "clippy", "tests")]
        self.assertEqual(verification_receipt(json.dumps(passed), 0), passed)
        failed = [{"name": "fmt", "passed": True}, {"name": "clippy", "passed": False}]
        self.assertEqual(verification_receipt(json.dumps(failed), 1), failed)
        for receipt, status in ((passed, 137), (failed, 0), (passed[:1], 0),
                                ([{"name": "../../private", "passed": False}], 1),
                                ([{"name": "fmt", "passed": "true"}], 0)):
            with self.subTest(receipt=receipt, status=status), self.assertRaises(InvalidResult):
                verification_receipt(json.dumps(receipt), status)

    def test_worker_fifo_cannot_block_result_collection(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.mkfifo(Path(tmp) / "result.json")
            probe = """from pathlib import Path
from runner import Runner, InvalidResult
import sys
try:
    Runner._read_worker_file(None, Path(sys.argv[1]), 'result.json', 4096)
except InvalidResult:
    sys.exit(0)
sys.exit(1)
"""
            result = subprocess.run([sys.executable, "-c", probe, tmp],
                cwd=Path(__file__).resolve().parents[1], capture_output=True, timeout=2)
            self.assertEqual(result.returncode, 0, result.stderr.decode())

    def test_patch_rejects_automation_escape_and_symlink_changes(self):
        self.assertEqual(validate_patch(patch_for("crates/openplotva-core/src/lib.rs")),
                         ["crates/openplotva-core/src/lib.rs"])
        for path in (".github/workflows/ci.yml", "../escape", "/absolute", "tools/maintenance/runner.py",
                     "AGENTS.md", "crates/AGENTS.md", ".cargo/config.toml", "deploy/production/a", ".env",
                     "tools/reviewdog-cargo-diagnostics.jq", "tools/rust-fast-gate.sh", "deny.toml",
                     "tools/rust-dependency-gate.sh", "Dockerfile", ".pr_agent.toml"):
            with self.subTest(path=path), self.assertRaises(InvalidResult):
                validate_patch(patch_for(path))
        for extra in ("new file mode 120000\n", "old mode 100644\nnew mode 120000\n", "GIT binary patch\n"):
            with self.assertRaises(InvalidResult):
                validate_patch(patch_for("crates/a", extra))
        with self.assertRaises(InvalidResult):
            validate_patch(patch_for("crates/a").replace(b"+after", b"+github_pat_canary_control_secret"))

    def test_container_has_only_job_mount_and_revocable_capability(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = {"state_dir": tmp, "source_dir": tmp + "/source", "image": "sha256:" + "a" * 64}
            runner = Runner(config, None)
            args = runner.container_args("job", Path(tmp) / "work", "jobnet")
            joined = " ".join(args)
            self.assertIn("--user 1000:1000", joined)
            self.assertIn("--cap-drop ALL", joined)
            self.assertIn("--memory 4g --memory-swap 4g", joined)
            self.assertIn("--cpus 2", joined)
            self.assertEqual(args.count("--mount"), 1)
            self.assertNotIn("docker.sock", joined)
            self.assertNotIn("--privileged", args)
            self.assertNotIn("GH_TOKEN", joined)
            self.assertNotIn("TELEGRAM", joined)
            self.assertNotIn("--network host", joined)

    def test_secret_permissions_and_symlinks_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            value = Path(tmp) / "token"
            value.write_text("synthetic-private-token-for-test")
            value.chmod(0o600)
            self.assertEqual(secret_file(value), "synthetic-private-token-for-test")
            value.chmod(0o644)
            with self.assertRaises(Deferred):
                secret_file(value)
            link = Path(tmp) / "link"
            link.symlink_to(value)
            with self.assertRaises(OSError):
                secret_file(link)

    def test_one_compute_slot_even_between_process_instances(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = {"state_dir": tmp, "source_dir": tmp + "/source", "image": "sha256:" + "a" * 64}
            first, second = Runner(config, None), Runner(config, None)
            with first.slot(), self.assertRaises(Deferred):
                with second.slot():
                    self.fail("second agent entered")
            with second.slot():
                pass


if __name__ == "__main__":
    unittest.main()
