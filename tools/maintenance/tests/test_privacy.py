import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from tools.maintenance.contracts import InvalidResult
from tools.maintenance.github import GitHub
from tools.maintenance.privacy import PublicationPrivacy, public_issue_body, public_title, public_feedback
from tools.maintenance.tests.test_controller import diagnosis
from tools.maintenance.tests.test_github import owner


class PublicationPrivacyTests(unittest.TestCase):
    def test_functional_facts_survive_redaction_and_spelling_variants_do_not(self):
        boundary = PublicationPrivacy(['Acme-Gateway', 'lab/model-27', 'gpu-17.internal.invalid'])
        value = 'Three attempts on Acme Gateway using LAB/MODEL 27 at gpu-17.internal.invalid fail after 15s with HTTP 503.'
        cleaned = boundary.redact(value)
        self.assertIn('Three attempts', cleaned)
        self.assertIn('15s with HTTP 503', cleaned)
        for identity in ('Acme', 'MODEL', 'gpu-17'):
            self.assertNotIn(identity, cleaned)
        self.assertEqual(boundary.assert_public(cleaned), cleaned)

    def test_unlisted_endpoints_ids_and_addresses_are_rejected_but_public_refs_work(self):
        boundary = PublicationPrivacy()
        for value in ('https://new-provider.example.net/v1', 'gpu-2.internal', '10.24.1.2:8080',
                      '2001:db8::42', 'provider_id=125', 'request ID: rq-123', 'incident:12:task',
                      '78bb9862-5ab9-44dc-9296-5dbec2bb4345'):
            with self.subTest(value=value), self.assertRaises(InvalidResult):
                boundary.assert_public('Observed '+value)
        for value in ('crates/openplotva-app/src/llm_routing.rs:123; HTTP 503; 15s; PR #98',
                      'https://github.com/iamwavecut/openplotva/pull/98',
                      'https://fixture.invalid/v1', '<!-- maintenance:origin:abc123 -->'):
            self.assertEqual(boundary.assert_public(value), value)

    def test_public_sections_retain_evidence_and_do_not_export_private_context(self):
        value = diagnosis()
        value.update(summary='Acme-Gateway fails before fallback.',
                     observations=['Three attempts fail with HTTP 503.'],
                     hypotheses=['The candidate filter may discard the fallback.'],
                     related_changes=['https://github.com/iamwavecut/openplotva/pull/98'],
                     acceptance=['A synthetic five-candidate route reaches the fallback.'])
        job = {'context': {'incident': {'count': 3, 'snapshot': {'route': {'provider': 'Acme-Gateway'}}},
                           'private': {'initial_diagnosis': {'summary': 'unrelated private prose'}}}}
        body = public_issue_body(value, job, '<!-- maintenance:origin:opaque -->')
        for fact in ('Three attempts fail with HTTP 503.', 'candidate filter', 'pull/98', 'five-candidate', '3 captured terminal events'):
            self.assertIn(fact, body)
        self.assertNotIn('Acme-Gateway', body)
        self.assertNotIn('unrelated private prose', body)
        # Classifications in the private diagnosis are not identifier aliases.
        self.assertIn('possible', body)
        self.assertNotIn('Acme', public_title('Acme-Gateway drops fallback', job))
        self.assertNotIn('Acme', public_feedback('Acme Gateway drops fallback', job))

    def test_every_github_text_surface_checks_before_public_transport(self):
        publisher = GitHub({'private_inventory': {'identifiers': ['Acme-Gateway']}})
        calls = []
        def api(path, method='GET', payload=None):
            if path == 'user': return owner()
            calls.append((path, payload))
            return {}
        publisher.api = api
        actions = [lambda: publisher.create_issue('Acme Gateway fails', 'Safe body.', []),
                   lambda: publisher.create_issue('Functional failure', '<!-- Acme-Gateway -->', []),
                   lambda: publisher.create_pr('fix/issue-7', 'Functional failure', 'Acme-Gateway'),
                   lambda: publisher.comment(7, 'Acme-Gateway'),
                   lambda: publisher.comment(7, 'Acme-Gateway', comment_id=9),
                   lambda: publisher.reply_thread('THREAD_1', 'Acme-Gateway')]
        for action in actions:
            with self.assertRaises(InvalidResult): action()
        self.assertEqual(calls, [])
        publisher.create_issue('Fallback is skipped', 'Three attempts exhaust before fallback.', [])
        self.assertEqual(len(calls), 1)

    def test_encoded_and_invisible_identifier_spellings_fail_closed(self):
        boundary = PublicationPrivacy(['Acme-Gateway'])
        for value in ('Acme%2DGateway', 'Acme&#45;Gateway', 'Acme\u200b-Gateway', 'Ａｃｍｅ-Gateway'):
            with self.subTest(value=value), self.assertRaises(InvalidResult):
                boundary.assert_public(value)
        self.assertEqual(boundary.assert_public('Count &lt; 5.'), 'Count &lt; 5.')

    def test_public_url_cannot_hide_identifiers_in_path_or_fragment(self):
        boundary = PublicationPrivacy()
        for value in ('https://github.com/iamwavecut/openplotva/issues/7#10.24.1.2',
                      'https://fixture.invalid/gpu-2.internal',
                      'https://github.com/iamwavecut/openplotva/attachments/private-report'):
            with self.subTest(value=value), self.assertRaises(InvalidResult): boundary.assert_public(value)

    def test_scoped_model_and_image_shorthand_stay_private_without_global_catalog(self):
        boundary = PublicationPrivacy().with_context({'context': {
            'evidence': {'model': 'synthetic-lab/private-model-27'},
            'deployed': {'runtime': {'image': 'sha256:'+'ab12'*16}},
        }})
        for value in ('private-model-27', 'ab12'*16, 'ab12'*3):
            with self.subTest(value=value), self.assertRaises(InvalidResult): boundary.assert_public(value)
        self.assertEqual(boundary.assert_public('Source revision '+'a'*40), 'Source revision '+'a'*40)

    def test_private_inventory_rejects_missing_symlink_and_fifo_and_accepts_private_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(InvalidResult): PublicationPrivacy.from_config({'private_inventory_file': str(root/'missing')})
            path = root/'inventory.json'
            path.write_text(json.dumps({'identifiers': ['Acme-Gateway']})); path.chmod(0o600)
            with self.assertRaises(InvalidResult):
                PublicationPrivacy.from_config({'private_inventory_file': str(path)}).assert_public('Acme Gateway')
            link = root/'link'; link.symlink_to(path)
            fifo = root/'fifo'; os.mkfifo(fifo, 0o600)
            for unsafe in (link, fifo):
                with self.assertRaises(InvalidResult): PublicationPrivacy.from_config({'private_inventory_file': str(unsafe)})

    def test_patch_headers_removed_context_and_commit_messages_are_private_surfaces(self):
        boundary = PublicationPrivacy(['Acme-Gateway'])
        for patch in (b'diff --git a/Acme-Gateway.rs b/Acme-Gateway.rs\n+safe\n',
                      b' context Acme-Gateway\n+safe\n', b'-Acme-Gateway\n+safe\n'):
            with self.assertRaises(InvalidResult): boundary.assert_patch_public(patch)
        publisher = GitHub({'private_inventory': {'identifiers': ['Acme-Gateway']}})
        prepared = {'sha': 'b'*40, 'directory': '/unused', 'branch': 'fix/issue-7'}
        for diff, message in ((b'+Acme-Gateway\n', b'Fix fallback\n'), (b'+safe\n', b'Fix Acme Gateway\n')):
            publisher.command = lambda args, **kwargs: diff if 'diff' in args else message
            with self.assertRaises(InvalidResult): publisher.validate_prepared({'base_sha': 'a'*40}, prepared)

    def test_private_inventory_file_requires_mode_0600(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'private-inventory.json'
            path.write_text(json.dumps({'models': ['glm-private-27']}))
            path.chmod(0o644)
            with self.assertRaises((InvalidResult, PermissionError)):
                GitHub({'private_inventory_file': str(path)})

    def test_authenticated_publisher_requires_private_inventory(self):
        with self.assertRaises(InvalidResult):
            GitHub({'github_token_file': '/private/operator-token'})

    def test_prepare_patch_rejects_confidential_additions_before_checkout(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'source'
            source.mkdir()

            def git(*args):
                return subprocess.check_output(['git', '-C', str(source), *args], stderr=subprocess.DEVNULL).decode().strip()

            git('init')
            git('config', 'user.name', 'Synthetic')
            git('config', 'user.email', 'synthetic@example.invalid')
            (source / 'code.rs').write_text('before\n')
            git('add', '.')
            git('commit', '-m', 'base')
            base = git('rev-parse', 'HEAD')
            patch = root / 'patch.diff'
            patch.write_text(
                'diff --git a/code.rs b/code.rs\n'
                '--- a/code.rs\n'
                '+++ b/code.rs\n'
                '@@ -1 +1 @@\n'
                '-before\n'
                '+model: glm-private-27\n'
            )
            publisher = GitHub({
                'state_dir': str(root / 'state'),
                'source_dir': str(source),
                'git_name': 'Synthetic',
                'git_email': 'synthetic@example.invalid',
                'private_inventory': {'models': ['glm-private-27']},
            })
            with self.assertRaises(InvalidResult):
                publisher.prepare_patch(
                    {'id': 'job', 'issue_number': 7, 'base_sha': base},
                    {'patch_path': str(patch), 'base_sha': base},
                    0,
                )
            self.assertFalse((root / 'state').exists())


if __name__ == '__main__':
    unittest.main()
