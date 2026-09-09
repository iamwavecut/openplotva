import copy
import importlib
import json
import tempfile
import unittest
from pathlib import Path

from tools.maintenance.contracts import Deferred
from tools.maintenance.github import GitHub
from tools.maintenance.state import State
from tools.maintenance.tests.test_github import owner
from tools.maintenance.tests.test_review_receipt import HEAD, check, execution

try:
    queue_module = importlib.import_module('tools.maintenance.review_queue')
except ModuleNotFoundError:
    queue_module = None


class ReviewAPI(GitHub):
    def __init__(self):
        super().__init__({})
        repo = {'full_name': 'iamwavecut/openplotva', 'owner': owner(), 'fork': False}
        self.pull = {'number': 8, 'user': owner(), 'state': 'open', 'draft': False,
                     'head': {'sha': HEAD, 'repo': repo}, 'base': {'repo': repo}}
        self.rows = [execution(), check()]
        self.workflow = {'id': 123, 'run_attempt': 1, 'status': 'completed', 'conclusion': 'failure',
                         'path': '.github/workflows/pr-automation.yml', 'event': 'pull_request',
                         'repository': repo, 'head_repository': repo, 'actor': owner(), 'triggering_actor': owner()}
        self.posted = []
        self.ambiguous = False

    def api(self, path, method='GET', payload=None):
        if method == 'POST':
            self.posted.append(path)
            self.workflow.update(run_attempt=2, status='in_progress', conclusion=None)
            self.rows[1].update(status='in_progress', conclusion=None)
            if self.ambiguous:
                raise Deferred('lost acknowledgement')
            return {}
        if path.endswith('/pulls/8'):
            return copy.deepcopy(self.pull)
        if '/pulls?' in path:
            return [copy.deepcopy(self.pull)] if self.pull['state'] == 'open' else []
        if '/check-runs?' in path:
            return {'check_runs': copy.deepcopy(self.rows)}
        if path.endswith('/actions/runs/123'):
            return copy.deepcopy(self.workflow)
        if path.endswith('/actions/jobs/456'):
            return {'id': 456, 'run_id': 123, 'run_attempt': 1, 'name': 'PR-Agent review and suggestions',
                    'head_sha': HEAD, 'status': 'completed', 'conclusion': 'failure'}
        if path == 'user':
            return owner()
        raise AssertionError(path)


class ReviewQueueTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(queue_module, 'The controller must durably resume quota-deferred reviews')
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.now = 1788912060.0  # 2026-09-09T00:01:00Z
        self.state = State(Path(self.temp.name) / 'state.sqlite3', clock=lambda: self.now)
        self.addCleanup(self.state.close)
        self.state.set_enabled(True)
        self.gh = ReviewAPI()
        self.queue = queue_module.ReviewQueue(self.state, self.gh)

    def test_quota_wait_frees_runner_and_resumes_once_after_restart(self):
        self.queue.poll()
        self.assertFalse(self.state.provider_available())
        self.assertEqual(self.gh.posted, [])
        self.now += 121
        self.queue = queue_module.ReviewQueue(self.state, self.gh)
        self.queue.poll()
        self.assertEqual(self.gh.posted, ['repos/iamwavecut/openplotva/actions/jobs/456/rerun'])
        self.assertFalse(self.state.provider_available())
        self.queue.poll()
        self.assertEqual(len(self.gh.posted), 1)
        self.gh.workflow.update(status='completed', conclusion='success')
        completed = execution('complete')
        completed.update(id=11, external_id='pr-agent:123:2', started_at='2026-09-09T00:03:01Z', completed_at='2026-09-09T00:04:00Z')
        value = json.loads(completed['output']['summary']); value.update(run_attempt=2, retry_after_seconds=None)
        completed['output']['summary'] = json.dumps(value)
        self.gh.rows = [completed, {**check(), 'id': 12, 'conclusion': 'success',
            'started_at': '2026-09-09T00:03:00Z', 'completed_at': '2026-09-09T00:05:00Z'}]
        self.queue.poll()
        self.assertTrue(self.state.provider_available())
        self.assertEqual(self.state.setting('provider_quota'), {})

    def test_lost_ack_is_reconciled_without_duplicate_review_or_new_repair(self):
        self.queue.poll(); self.now += 121; self.gh.ambiguous = True
        self.queue.poll()
        self.queue = queue_module.ReviewQueue(self.state, self.gh)
        self.queue.poll()
        self.assertEqual(len(self.gh.posted), 1)
        self.assertEqual(self.state.jobs(), [])

    def test_new_quota_receipt_releases_previous_retry_slot_even_with_stale_run_status(self):
        self.queue.poll(); self.now += 121; self.queue.poll()
        newer = execution()
        newer.update(id=22, external_id='pr-agent:123:2',
                     started_at='2026-09-09T00:03:01Z', completed_at='2026-09-09T00:04:00Z')
        value = json.loads(newer['output']['summary']); value['run_attempt'] = 2
        newer['output']['summary'] = json.dumps(value)
        self.gh.rows = [newer, {**check(), 'id': 23, 'started_at': '2026-09-09T00:03:00Z',
                              'completed_at': '2026-09-09T00:05:00Z'}]
        self.now += 200; self.queue.poll()
        self.assertIsNone(self.state.setting('review_slot'))
        self.assertEqual(self.state.setting('review_waits')['8']['receipt']['check_id'], 22)
        self.now += 121
        self.assertTrue(self.state.provider_available())

    def test_completed_retry_without_new_receipt_requires_human_after_bounded_grace(self):
        self.queue.poll(); self.now += 121; self.queue.poll()
        self.gh.workflow.update(status='completed', conclusion='cancelled')
        self.gh.rows = [execution(), check()]
        self.queue.poll()
        self.assertIsNone(self.state.setting('review_slot'))
        self.assertEqual(self.state.setting('review_waits')['8']['phase'], 'awaiting_receipt')
        self.now += 181
        self.queue = queue_module.ReviewQueue(self.state, self.gh)
        self.queue.poll()
        self.assertEqual(self.state.setting('review_waits')['8']['phase'], 'needs_human')
        self.assertEqual(len(self.gh.posted), 1)

    def test_failed_retry_receipt_requires_human_and_releases_slot(self):
        self.queue.poll(); self.now += 121; self.queue.poll()
        failed = execution('failed')
        failed.update(id=22, conclusion='failure', external_id='pr-agent:123:2')
        value = json.loads(failed['output']['summary']); value['run_attempt'] = 2
        failed['output']['summary'] = json.dumps(value)
        self.gh.rows = [failed, {**check(), 'id': 23}]
        self.queue.poll()
        self.assertIsNone(self.state.setting('review_slot'))
        self.assertEqual(self.state.setting('review_waits')['8']['phase'], 'needs_human')

    def test_no_retry_when_disabled_changed_head_closed_or_foreign_owner(self):
        for change in ('disabled', 'head', 'closed', 'owner', 'workflow', 'actor'):
            with self.subTest(change=change):
                self.now = 1788912060.0
                self.gh = ReviewAPI(); self.queue = queue_module.ReviewQueue(self.state, self.gh)
                self.queue.poll(); self.now += 121
                if change == 'disabled': self.state.set_enabled(False)
                if change == 'head': self.gh.pull['head']['sha'] = 'b'*40
                if change == 'closed': self.gh.pull['state'] = 'closed'
                if change == 'owner': self.gh.pull['user']['id'] = 1
                if change == 'workflow': self.gh.workflow['path'] = '.github/workflows/untrusted.yml'
                if change == 'actor': self.gh.workflow['triggering_actor']['id'] = 1
                self.queue.poll()
                self.assertEqual(self.gh.posted, [])
                self.state.set_enabled(True)
                self.state.set_setting('review_waits', {})
                self.state.set_setting('review_slot', None)
                self.state.provider_recovered()

    def test_running_worker_keeps_review_retry_queued(self):
        job = self.state.new_job('deep', 'sig', 1)
        self.state.claim(job['id'])
        self.queue.poll(); self.now += 121; self.queue.poll()
        self.assertEqual(self.gh.posted, [])
