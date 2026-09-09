import copy
import importlib
import json
import unittest

from tools.maintenance.contracts import Deferred

try:
    receipt = importlib.import_module('tools.maintenance.review_receipt')
except ModuleNotFoundError:
    receipt = None

HEAD = 'a' * 40
URL = 'https://github.com/iamwavecut/openplotva/actions/runs/123'


def execution(state='quota_wait'):
    return {'id': 9, 'name': 'PR-Agent execution', 'head_sha': HEAD,
            'app': {'id': 15368, 'slug': 'github-actions'}, 'status': 'completed',
            'conclusion': 'success' if state == 'complete' else 'neutral',
            'started_at': '2026-09-09T00:00:00Z', 'completed_at': '2026-09-09T00:01:00Z',
            'external_id': 'pr-agent:123:1', 'details_url': URL,
            'output': {'summary': json.dumps({'version': 1, 'state': state,
                'pr_number': 8, 'head_sha': HEAD, 'run_id': 123, 'run_attempt': 1,
                'retry_after_seconds': 120})}}


def check():
    return {'id': 10, 'name': 'PR-Agent review and suggestions', 'head_sha': HEAD,
            'app': {'id': 15368, 'slug': 'github-actions'}, 'status': 'completed',
            'started_at': '2026-09-09T00:00:00Z', 'completed_at': '2026-09-09T00:02:00Z',
            'conclusion': 'failure', 'details_url': URL + '/job/456'}


class ReviewReceiptTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(receipt, 'PR reviews need a structured execution receipt')

    def test_only_matching_trusted_head_and_execution_can_schedule_retry(self):
        value = receipt.execution_receipt([execution(), check()], HEAD, 8)
        self.assertEqual(value['state'], 'quota_wait')
        self.assertEqual(value['job_id'], 456)
        self.assertEqual(value['run_attempt'], 1)
        for change in ({'head_sha': 'b'*40}, {'app': {'id': 1, 'slug': 'github-actions'}},
                       {'details_url': URL + '4'}, {'external_id': 'pr-agent:123:2'}):
            with self.subTest(change=change):
                self.assertIsNone(receipt.execution_receipt([{**execution(), **change}, check()], HEAD, 8))

    def test_newer_running_or_failed_execution_invalidates_old_success(self):
        old = execution('complete')
        for change in ({'status': 'in_progress', 'conclusion': None, 'output': {}},
                       {'conclusion': 'failure', 'output': {'summary': '{}'}}):
            self.assertIsNone(receipt.execution_receipt([old, {**old, 'id': 11, **change}, check()], HEAD, 8))

    def test_mismatched_pr_run_or_unbounded_delay_cannot_be_a_receipt(self):
        for change in ({'pr_number': 7}, {'run_id': True}, {'run_attempt': 0},
                       {'retry_after_seconds': -1}, {'retry_after_seconds': 604801},
                       {'state': 'please run shell'}, {'private': 'canary'}):
            row = execution()
            value = json.loads(row['output']['summary']); value.update(change)
            row['output']['summary'] = json.dumps(value)
            self.assertIsNone(receipt.execution_receipt([row, check()], HEAD, 8))

    def test_other_run_and_successful_job_do_not_authorize_quota_retry(self):
        for changed in ({**check(), 'details_url': URL+'4/job/456'}, {**check(), 'conclusion': 'success'},
                        {**check(), 'started_at': '2026-09-09T00:01:30Z'}):
            self.assertIsNone(receipt.execution_receipt([execution(), changed], HEAD, 8))
