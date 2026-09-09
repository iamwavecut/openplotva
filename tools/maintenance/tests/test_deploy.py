import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "deploy/production/deploy-production.sh"
OVERLAY = ROOT / "deploy/maintenance/compose.maintenance.yml"


class DeploymentIntegrationTests(unittest.TestCase):
    def write_command(self, directory, name, body):
        path = directory / name
        path.write_text("#!/bin/sh\nset -eu\n" + body)
        path.chmod(0o755)
        return path

    def run_configure(self, *, overlay_present, existing_enabled=False):
        with tempfile.TemporaryDirectory(prefix="openplotva-deploy-test-") as directory:
            test_root = Path(directory)
            deploy_root = test_root / "deploy"
            deploy_root.mkdir()
            (deploy_root / ".env.production").write_text(
                "ADMINS_ADMIN_IDS=239034\n"
                "BOT_KEY=synthetic-bot-key\n"
                "WEBAPP_URL=https://example.test\n"
                "DB_POSTGRES_PASSWORD=synthetic-db-password\n"
            )
            if overlay_present:
                shutil.copy(OVERLAY, deploy_root / "compose.maintenance.yml")
            runtime = test_root / "runtime.env"
            runtime.write_text(
                "MAINTENANCE_ENABLED=false\n"
                "MAINTENANCE_TOKEN=synthetic-maintenance-token-012345678901234567890123\n"
                "MAINTENANCE_NOTIFY_USER_ID=239034\n"
            )
            bin_dir = test_root / "bin"
            bin_dir.mkdir()
            self.write_command(
                bin_dir,
                "sudo",
                """
                if [ "$1" = "-n" ]; then shift; fi
                case "$1" in
                  test)
                    shift
                    case "$1" in
                      -f) test -f "$2" ;;
                      -L) test -L "$2" ;;
                      *) exit 2 ;;
                    esac
                    ;;
                  stat) printf '%s\\n' '0:0:600:regular file' ;;
                  cat) shift; [ "$1" = "--" ]; cat "$2" ;;
                  *) exit 2 ;;
                esac
                """,
            )
            docker_output = test_root / "docker.args"
            docker_body = f"""
            if [ "$1" = "container" ] && [ "$2" = "inspect" ]; then
              {'exit 0' if existing_enabled else 'exit 1'}
            fi
            if [ "$1" = "inspect" ]; then
              {'printf \'true\\n\'' if existing_enabled else 'exit 1'}
            fi
            if [ "$1" = "compose" ]; then
              printf '%s\\n' "$@" > {docker_output}
              exit 0
            fi
            exit 2
            """
            self.write_command(bin_dir, "docker", docker_body)
            environment = {
                **os.environ,
                "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"],
                "OPENPLOTVA_DEPLOY_ROOT": str(deploy_root),
                "OPENPLOTVA_DEPLOY_IMAGE": "ghcr.io/example/openplotva:synthetic",
                "OPENPLOTVA_MAINTENANCE_ENV_FILE": str(runtime),
            }
            command = [
                "bash",
                "-c",
                f"source {SCRIPT}; configure_maintenance_overlay; "
                "if [ \"${1:-}\" = compose ]; then compose config --quiet; fi",
                "deploy-test",
                "compose" if overlay_present else "",
            ]
            result = subprocess.run(command, cwd=ROOT, env=environment, text=True, capture_output=True)
            docker_arguments = docker_output.read_text() if docker_output.exists() else ""
            snapshot_exists = any(deploy_root.glob(".maintenance-runtime.env.*"))
            return result, docker_arguments, snapshot_exists, deploy_root

    def test_missing_overlay_keeps_legacy_deploy_compatible(self):
        result, _, snapshot_exists, _ = self.run_configure(overlay_present=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(snapshot_exists)

    def test_missing_overlay_cannot_disable_existing_enabled_app(self):
        result, _, _, _ = self.run_configure(overlay_present=False, existing_enabled=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("maintenance overlay is missing while the existing app is enabled", result.stderr)

    def test_overlay_uses_protected_snapshot_and_keeps_loopback_binding(self):
        result, docker_output, snapshot_exists, deploy_root = self.run_configure(overlay_present=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        arguments = docker_output.splitlines()
        self.assertEqual(arguments.count("--env-file"), 2)
        self.assertEqual(arguments.count("-f"), 2)
        self.assertIn(str(deploy_root / ".env.production"), arguments)
        self.assertIn(str(deploy_root / "compose.maintenance.yml"), arguments)
        self.assertNotIn("synthetic-maintenance-token", docker_output)
        self.assertFalse(snapshot_exists)
        overlay = OVERLAY.read_text()
        self.assertIn('"127.0.0.1:9092:9092"', overlay)

    def test_workflow_uploads_overlay_without_credentials(self):
        workflow = (ROOT / ".github/workflows/deploy-production.yml").read_text()
        script = SCRIPT.read_text()
        self.assertIn(
            'scp deploy/maintenance/compose.maintenance.yml "$GETA_SSH_TARGET:$GETA_DEPLOY_ROOT/compose.maintenance.yml"',
            workflow,
        )
        self.assertNotIn("MAINTENANCE_TOKEN", workflow)
        self.assertNotIn("runtime.env", workflow)
        self.assertIn(
            'maintenance_env_file="${OPENPLOTVA_MAINTENANCE_ENV_FILE:-/etc/openplotva-maintenance/runtime.env}"',
            script,
        )


if __name__ == "__main__":
    unittest.main()
