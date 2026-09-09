import tempfile
import unittest
from pathlib import Path

from tools.maintenance.api import MaintenanceAPI
from tools.maintenance.contracts import InvalidResult
from tools.maintenance.controller import Controller
from tools.maintenance.state import State


class NotificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)/'state.sqlite3'
        self.state = State(self.path); self.addCleanup(self.state.close)
        self.controller = Controller({}, self.state, None, None, None)

    def test_hundred_repeated_infrastructure_failures_send_one_actionable_notice_across_restart(self):
        for number in range(100):
            job = self.state.new_job('initial', str(number), number+1,
                reason='source or context dependency unavailable; retry budget exhausted')
            self.controller.notify(job, 'needs_human')
        other = State(self.path)
        try:
            Controller({}, other, None, None, None).notify(job, 'needs_human')
            records = other.records('notifications')
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]['payload']['reason_code'], 'dependency_unavailable')
            self.assertNotIn('issue_number', records[0]['payload'])
        finally:
            other.close()

    def test_repeat_initial_failure_recovers_known_issue_link_and_deduplicates_by_issue(self):
        self.state.record_origin('known', 7, 'same', 1)
        for _ in range(2):
            job = self.state.new_job('initial', 'same', 1, reason='invalid isolated result')
            self.controller.notify(job, 'needs_human')
        records = self.state.records('notifications')
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['payload']['issue_number'], 7)
        self.assertEqual(records[0]['payload']['reason_code'], 'invalid_result')
        job = self.state.new_job('deep', 'other', 2, issue_number=8, reason='invalid isolated result')
        self.controller.notify(job, 'needs_human')
        self.assertEqual(len(self.state.records('notifications')), 2)

    def test_changed_reason_or_verified_pr_head_is_a_new_actionable_notice(self):
        job = self.state.new_job('deep', 'same', 1, issue_number=7,
            reason='deep investigation produced no verified fix; issue remains open')
        self.controller.notify(job, 'needs_human')
        changed = self.state.update_job(job['id'], reason='review repair budget exhausted')
        self.controller.notify(changed, 'needs_human')
        for head in ('a'*40, 'a'*40, 'b'*40):
            changed = self.state.update_job(job['id'], pr_number=8, published_sha=head)
            self.controller.notify(changed, 'pr_ready')
        self.assertEqual(len(self.state.records('notifications')), 4)

    def test_api_accepts_only_bounded_reason_codes(self):
        api = MaintenanceAPI({'maintenance_url': 'https://localhost/internal/maintenance/v1'})
        api.request = lambda method, path, payload: {'key': payload['key'], 'state': 'pending'}
        payload = {'key': 'a'*64, 'run_id': 'test', 'status': 'needs_human', 'reason_code': 'dependency_unavailable'}
        self.assertEqual(api.notify(payload)['state'], 'pending')
        for value in ('private-canary', [], None):
            with self.assertRaises(InvalidResult): api.notify({**payload, 'reason_code': value})
