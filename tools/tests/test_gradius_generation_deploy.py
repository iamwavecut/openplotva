"""Generation secret deployment preserves other runtime settings and stays private."""

import base64
import importlib.util
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[2] / "deploy/production/configure-gradius-generation.py"
SPEC = importlib.util.spec_from_file_location("gradius_generation_deploy", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
KEY = "ad-" + "fixture_key_" * 4


class GenerationDeployTests(unittest.TestCase):
    def test_update_preserves_other_keys_and_restricts_file_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env.production"
            path.write_text("# existing config\nGRADIUS_API_KEY=dialogue\nGRADIUS_ENABLED=true\nGRADIUS_GENERATION_API_KEY=old\nGRADIUS_GENERATION_API_KEY=duplicate\nGRADIUS_UTILITY_IMAGE_ENABLED=false\n")
            path.chmod(0o644)
            MODULE.configure(path, {"key": KEY})
            self.assertEqual(path.read_text(), "# existing config\nGRADIUS_API_KEY=dialogue\nGRADIUS_ENABLED=true\nGRADIUS_GENERATION_API_KEY=" + KEY + "\nGRADIUS_UTILITY_IMAGE_ENABLED=true\n")
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual([p.name for p in Path(directory).iterdir()], [path.name])

    def test_bootstrap_uses_existing_production_secret(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env.production"
            MODULE.configure(path, {"key": KEY, "production_env_b64": base64.b64encode(b"BOT_KEY=fixture\n").decode()})
            self.assertTrue(path.read_text().startswith("BOT_KEY=fixture\n"))

    def test_invalid_key_and_symlink_preserve_existing_data(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env.production"
            path.write_text("unchanged\n")
            with self.assertRaises(ValueError):
                MODULE.configure(path, {"key": "ad-invalid\nINJECTED=true"})
            link = Path(directory) / "link"
            link.symlink_to(path)
            with self.assertRaises(ValueError):
                MODULE.configure(link, {"key": KEY})
            self.assertEqual(path.read_text(), "unchanged\n")

    def test_missing_bootstrap_configuration_does_not_create_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env.production"
            with self.assertRaises(ValueError):
                MODULE.configure(path, {"key": KEY, "production_env_b64": ""})
            self.assertFalse(path.exists())

    def test_cli_does_not_print_secret_on_failure(self):
        result = subprocess.run([sys.executable, str(SCRIPT), "/missing/runtime/env"], input='{"key":"' + KEY + '","production_env_b64":"invalid"}', text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn(KEY, result.stdout + result.stderr)
        self.assertNotIn("Traceback", result.stderr)


if __name__ == "__main__":
    unittest.main()
