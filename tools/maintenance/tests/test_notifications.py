import tempfile
import unittest
from pathlib import Path

from tools.maintenance.api import MaintenanceAPI
from tools.maintenance.contracts import InvalidResult
from tools.maintenance.controller import Controller
from tools.maintenance.state import State
from tools.maintenance.notifications import reason_code
from tools.maintenance.tests.test_controller import API


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

    def test_pending_legacy_notices_coalesce_without_replacing_delivery_keys(self):
        api = API(); self.controller.api = api
        for number in range(3):
            job = self.state.new_job('initial', str(number), number+1,
                reason='source or context dependency unavailable; retry budget exhausted')
            key = str(number)*64
            payload = {'key': key, 'run_id': job['id'], 'status': 'needs_human'}
            if number == 1: api.notify(payload)
            self.state.put_record('notifications', key,
                {'key': key, 'payload': payload, 'posted': number == 1, 'state': 'pending'})
        self.controller.notify(job, 'needs_human')
        self.controller.poll_notifications()
        self.assertEqual(set(api.receipts), {'1'*64})
        self.assertEqual(self.state.status()['pending_notifications'], 1)
        api.delivery = 'sent'
        self.controller.poll_notifications()
        other = State(self.path)
        try:
            controller = Controller({}, other, api, None, None)
            controller.notify(job, 'needs_human'); controller.poll_notifications()
            self.assertEqual(set(api.receipts), {'1'*64})
            self.assertEqual(other.status()['pending_notifications'], 0)
        finally:
            other.close()

    def test_missing_execution_receipt_has_review_guidance(self):
        self.assertEqual(reason_code({'reason': 'required PR-Agent execution proof unavailable'},
                                     'needs_human'), 'review_incomplete')

    def test_legacy_sent_or_ambiguous_delivery_fences_replacements_across_restart(self):
        for delivered in ('sent', 'ambiguous'):
            with self.subTest(delivered=delivered):
                job = self.state.new_job('deep', delivered, 1, issue_number=7 if delivered == 'sent' else 8,
                    reason='deep investigation produced no verified fix; issue remains open')
                for number, status in enumerate((delivered, 'pending')):
                    key = (('a' if delivered == 'sent' else 'b')+str(number))*32
                    payload = {'key': key, 'run_id': job['id'], 'status': 'needs_human',
                               'issue_number': job['issue_number']}
                    self.state.put_record('notifications', key,
                        {'key': key, 'payload': payload, 'posted': number == 0, 'state': status})
                other = State(self.path)
                try:
                    api = API(); controller = Controller({}, other, api, None, None)
                    controller.notify(job, 'needs_human'); controller.poll_notifications()
                    self.assertEqual(api.receipts, {})
                finally:
                    other.close()

    def test_unacknowledged_legacy_post_reuses_the_same_transport_key(self):
        api = API(); self.controller.api = api
        for number in range(2):
            job = self.state.new_job('initial', str(number), number+1, reason='invalid isolated result')
            key = str(number)*64
            payload = {'key': key, 'run_id': job['id'], 'status': 'needs_human'}
            if number == 0: api.notify(payload)  # Dispatcher accepted it before the process stopped.
            self.state.put_record('notifications', key,
                {'key': key, 'payload': payload, 'posted': False, 'state': 'pending'})
        api.notify = lambda *_: self.fail('An existing dispatcher receipt must not be posted again')
        self.controller.poll_notifications()
        self.controller.notify(job, 'needs_human')
        self.assertEqual(set(api.receipts), {'0'*64})
        self.assertEqual(self.state.status()['pending_notifications'], 1)
