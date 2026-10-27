#!/usr/bin/env python3
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

HELPER = next(path for parent in Path(__file__).resolve().parents
              for name in ('scripts/geta-deploy-request.py', 'workflow-tools/release-request.py')
              if (path := parent / name).is_file())
spec = importlib.util.spec_from_file_location('identity_request', HELPER)
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)
ENDPOINT = 'https://vstoken.actions.githubusercontent.com/oidc?api-version=2.0&audience=old'
ENVIRONMENT = {'ACTIONS_ID_TOKEN_REQUEST_URL': ENDPOINT,
               'ACTIONS_ID_TOKEN_REQUEST_TOKEN': 'synthetic-request-token'}


class IdentityTransportTests(unittest.TestCase):
    def response(self, factory, status, body):
        response = io.BytesIO(body)
        response.status = status
        connection = Mock()
        connection.getresponse.return_value = response
        factory.return_value = connection
        return connection

    def test_untrusted_origins_never_receive_credentials(self):
        for address in ('file:///tmp/data', 'http://vstoken.actions.githubusercontent.com/oidc',
                        'https://example.invalid/oidc', 'https://actions.githubusercontent.com.example.invalid/',
                        'https://user@vstoken.actions.githubusercontent.com/oidc',
                        'https://vstoken.actions.githubusercontent.com:444/oidc',
                        ENDPOINT + '#fragment'):
            with self.subTest(address=address), patch.dict(release.os.environ, {
                    **ENVIRONMENT, 'ACTIONS_ID_TOKEN_REQUEST_URL': address}, clear=True), \
                    patch.object(release.http.client, 'HTTPSConnection') as factory:
                with self.assertRaises(ValueError):
                    release.identity()
                factory.assert_not_called()

    def test_https_request_pins_audience_and_closes_connection(self):
        with patch.dict(release.os.environ, ENVIRONMENT, clear=True), \
                patch.object(release.http.client, 'HTTPSConnection') as factory:
            connection = self.response(factory, 200, b'{"value":"synthetic-identity"}')
            self.assertEqual(release.identity(), 'synthetic-identity')
            factory.assert_called_once_with('vstoken.actions.githubusercontent.com', timeout=15)
            connection.request.assert_called_once_with(
                'GET', '/oidc?api-version=2.0&audience=geta-deploy',
                headers={'Authorization': 'Bearer synthetic-request-token'})
            connection.close.assert_called_once()

    def test_redirects_are_not_followed(self):
        for status in (301, 302, 403, 500):
            with self.subTest(status=status), patch.dict(release.os.environ, ENVIRONMENT, clear=True), \
                    patch.object(release.http.client, 'HTTPSConnection') as factory:
                connection = self.response(factory, status, b'synthetic-private-response')
                with self.assertRaisesRegex(ValueError, 'identity request failed'):
                    release.identity()
                factory.assert_called_once()
                connection.request.assert_called_once()
                connection.close.assert_called_once()

    def test_response_size_is_bounded(self):
        with patch.dict(release.os.environ, ENVIRONMENT, clear=True), \
                patch.object(release.http.client, 'HTTPSConnection') as factory:
            connection = self.response(factory, 200, b'x' * 32769)
            with self.assertRaisesRegex(ValueError, 'response is too large'):
                release.identity()
            connection.close.assert_called_once()

    def test_invalid_identities_are_rejected(self):
        for value in (None, 123, '', 'x' * 16001):
            with self.subTest(value_type=type(value).__name__), \
                    patch.dict(release.os.environ, ENVIRONMENT, clear=True), \
                    patch.object(release.http.client, 'HTTPSConnection') as factory:
                connection = self.response(factory, 200, json.dumps({'value': value}).encode())
                with self.assertRaisesRegex(ValueError, 'invalid identity'):
                    release.identity()
                connection.close.assert_called_once()

    def test_schema_mode_requires_an_explicit_schema_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'deploy').mkdir()
            (root / release.CONTRACT).write_text('{"files":[],"trees":[]}')
            result = subprocess.run([sys.executable, str(HELPER), '--root', str(root),
                                     '--check-schema', str(root)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            self.assertIn('--check-schema requires approved_schema', result.stderr)


if __name__ == '__main__':
    unittest.main()
