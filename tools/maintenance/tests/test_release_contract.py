import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location('release_request', ROOT / 'scripts/geta-deploy-request.py')
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


class ReleaseContractTests(unittest.TestCase):
    def test_schema_and_sandbox_changes_require_profile_review(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'deploy').mkdir()
            (root / 'migrations').mkdir()
            (root / 'sandbox').mkdir()
            contract = {'data_version': '1', 'files': [], 'trees': ['migrations', 'sandbox'], 'images': {'openplotva': 'IMAGE'}}
            (root / release.CONTRACT).write_text(json.dumps(contract))
            (root / 'migrations/001.sql').write_text('SELECT 1;')
            before, _ = release.contract_digest(root)
            (root / 'sandbox/protocol.rs').write_text('changed protocol')
            after, _ = release.contract_digest(root)
            self.assertNotEqual(before, after)
            (root / 'migrations/002.sql').write_text('SELECT 2;')
            final, _ = release.contract_digest(root)
            self.assertNotEqual(after, final)

    def test_request_cannot_select_a_host_path_or_sandbox_image(self):
        environment = {'DEPLOY_OPERATION': 'deploy', 'DEPLOY_SHA': 'a' * 40, 'OPENPLOTVA_IMAGE_DIGEST': 'sha256:' + 'b' * 64}
        payload = release.payload(ROOT, environment, lambda: 'synthetic-identity')
        self.assertEqual(set(payload['images']), {'openplotva'})
        self.assertNotIn('path', payload)
        self.assertNotIn('registry_token', payload)
        self.assertNotIn('sandbox', payload['images'])

    def test_backup_uses_a_fixed_server_operation(self):
        workflow = (ROOT / '.github/workflows/backup-production.yml').read_text()
        self.assertIn('deploy@geta.moe backup', workflow)
        self.assertNotIn('scp ', workflow)
        self.assertNotIn('backup-production.sh', workflow)
        payload = release.payload(ROOT, {'DEPLOY_OPERATION': 'backup'}, lambda: 'synthetic-identity')
        self.assertEqual(payload, {'token': 'synthetic-identity'})


if __name__ == '__main__':
    unittest.main()
