"""Execute the review-target resolver without GitHub access or credentials."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


class ReviewWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        source = Path(__file__).resolve().parents[3] / '.github/workflows/pr-automation.yml'
        block = source.read_text().split('        id: target\n')[1].split('        run: |\n')[1]
        lines = []
        for line in block.splitlines():
            if line.strip() and not line.startswith('          '):
                break
            lines.append(line)
        self.script = textwrap.dedent('\n'.join(lines))
        self.base = 'a' * 40
        self.head = 'b' * 40
        self.repo = 'iamwavecut/openplotva'
        self.fixture = {
            'html_url': 'https://github.com/iamwavecut/openplotva/pull/123',
            'head': {'sha': self.head, 'repo': {'full_name': self.repo}},
            'base': {'sha': self.base, 'repo': {'full_name': self.repo}},
            'additions': 5, 'deletions': 2, 'changed_files': 1,
        }
        fake = self.directory / 'gh'
        fake.write_text('#!/bin/sh\n: > "$GH_CALLED"\ncat "$GH_FIXTURE"\n')
        fake.chmod(0o755)

    def resolve(self, **overrides):
        fixture = self.directory / 'fixture.json'
        fixture.write_text(json.dumps(self.fixture))
        output = self.directory / 'output'
        output.write_text('')
        called = self.directory / 'called'
        called.unlink(missing_ok=True)
        env = {
            'PATH': str(self.directory) + os.pathsep + os.environ['PATH'],
            'GH_REPO': self.repo, 'PR_NUMBER': '123',
            'REVIEW_EVENT': 'workflow_dispatch', 'REVIEW_ACTOR': 'iamwavecut',
            'REVIEW_EXECUTION_SHA': self.head, 'GITHUB_OUTPUT': str(output),
            'GH_CALLED': str(called), 'GH_FIXTURE': str(fixture),
        }
        env.update(overrides)
        result = subprocess.run(['bash', '-c', self.script], env=env, capture_output=True, timeout=5)
        values = dict(line.split('=', 1) for line in output.read_text().splitlines())
        return result.returncode, values, called.exists()

    def test_owner_can_review_exact_head_while_configuration_stays_on_base(self):
        code, values, called = self.resolve()
        self.assertEqual(code, 0)
        self.assertTrue(called)
        self.assertEqual(values['wrapper_sha'], self.head)
        self.assertEqual(values['base_sha'], self.base)
        self.assertEqual(values['changed_lines'], '7')

    def test_owner_can_review_from_current_base(self):
        code, values, _ = self.resolve(REVIEW_EXECUTION_SHA=self.base)
        self.assertEqual(code, 0)
        self.assertEqual(values['wrapper_sha'], self.base)

    def test_other_actor_repository_and_non_numeric_issue_are_rejected_before_lookup(self):
        for overrides in ({'REVIEW_ACTOR': 'someone-else'}, {'GH_REPO': 'someone/repo'},
                          {'PR_NUMBER': '123; exit 0'}, {'PR_NUMBER': '--help'}, {'PR_NUMBER': '0'}):
            with self.subTest(overrides=overrides):
                code, values, called = self.resolve(**overrides)
                self.assertNotEqual(code, 0)
                self.assertFalse(called)
                self.assertEqual(values, {})

    def test_unrelated_or_malformed_execution_revision_cannot_select_wrapper(self):
        for value in ('c' * 40, 'main', self.head + '; exit 0'):
            with self.subTest(value=value):
                code, values, _ = self.resolve(REVIEW_EXECUTION_SHA=value)
                self.assertNotEqual(code, 0)
                self.assertEqual(values, {})

    def test_fork_target_cannot_select_wrapper(self):
        self.fixture['head']['repo']['full_name'] = 'someone/openplotva'
        code, values, _ = self.resolve()
        self.assertNotEqual(code, 0)
        self.assertEqual(values, {})

    def test_automatic_review_always_uses_base_without_manual_lookup(self):
        code, values, called = self.resolve(
            REVIEW_EVENT='pull_request', REVIEW_ACTOR='contributor',
            PULL_REQUEST_URL=self.fixture['html_url'], PULL_REQUEST_BASE_SHA=self.base,
            PULL_REQUEST_ADDITIONS='5', PULL_REQUEST_DELETIONS='2', PULL_REQUEST_CHANGED_FILES='1',
        )
        self.assertEqual(code, 0)
        self.assertFalse(called)
        self.assertEqual(values['wrapper_sha'], self.base)
        self.assertEqual(values['base_sha'], self.base)


if __name__ == '__main__':
    unittest.main()
