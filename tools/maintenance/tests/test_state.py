import concurrent.futures
import tempfile
import unittest
from pathlib import Path
from tools.maintenance.state import State
from tools.maintenance.contracts import Deferred, DAY


class StateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.now = 100000.0
        self.state = State(Path(self.temp.name) / 'state.sqlite3', clock=lambda: self.now)
        self.addCleanup(self.state.close)

    def test_result_update_cannot_overwrite_concurrent_cli_cancellation(self):
        job=self.state.new_job('deep','sig',1)
        other=State(Path(self.temp.name)/'state.sqlite3',clock=lambda:self.now)
        original=self.state.job
        future=None
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                def interleaved_read(job_id):
                    nonlocal future
                    snapshot=original(job_id)
                    if future is None:
                        future=executor.submit(other.cancel,job_id)
                        try: future.result(timeout=0.2)
                        except concurrent.futures.TimeoutError: pass
                    return snapshot
                self.state.job=interleaved_read
                self.state.update_job(job['id'],result={'outcome':'no_fix'})
                future.result(timeout=5)
            self.assertTrue(other.cancelled(job['id']))
            self.assertEqual(other.job(job['id'])['result'],{'outcome':'no_fix'})
            self.assertEqual(other.job(job['id'])['status'],'cancelled')
        finally:
            self.state.job=original
            other.close()

    def test_cancellation_is_monotonic_even_for_stale_save_and_nested_update(self):
        job=self.state.new_job('deep','sig',1)
        self.state.cancel(job['id'])
        with self.state.transaction(): self.state.update_job(job['id'],cancelled=False,status='result')
        self.assertTrue(self.state.cancelled(job['id']))
        self.state.save_job(job)
        self.assertTrue(self.state.cancelled(job['id']))
        self.assertEqual(self.state.job(job['id'])['status'],'cancelled')

    def test_replayed_outbox_is_atomic_and_debounced(self):
        events = [{'id': n, 'signature': 'same', 'first_seen': n, 'last_seen': n,
                   'snapshot': {'reason': 'timeout'}} for n in range(1, 101)]
        self.state.ingest(events, 100, 60)
        self.state.ingest(events, 100, 60)
        self.assertEqual(self.state.cursor(), 100)
        self.assertEqual(len(self.state.incidents()), 1)
        self.assertEqual(self.state.incidents()[0]['count'], 100)
        self.assertEqual(self.state.due_incidents(), [])
        self.now += 61
        self.assertEqual(len(self.state.due_incidents()), 1)

    def test_quota_slot_resume_and_crash_accounting(self):
        self.state.set_enabled(True)
        a = self.state.new_job('deep', 'sig', 1, issue_number=2)
        b = self.state.new_job('deep', 'other', 2, issue_number=3)
        self.state.claim(a['id'])
        with self.assertRaises(Deferred):
            self.state.claim(b['id'])
        self.now += 40
        self.state.recover()
        a = self.state.job(a['id'])
        self.assertEqual(a['active_seconds'], 40)
        self.state.claim(a['id'])
        self.state.finish_run(a['id'], 2, {'tokens': 9})
        self.assertEqual(self.state.status()['starts']['deep'], 1)
        self.assertEqual(self.state.job(a['id'])['active_seconds'], 42)
        self.assertEqual(self.state.status()['usage']['tokens'], 9)
        self.state.cancel(a['id'])
        self.assertTrue(self.state.cancelled(a['id']))

    def test_crash_after_result_receipt_does_not_restart_agent(self):
        self.state.set_enabled(True); job=self.state.new_job('deep','sig',1)
        self.state.claim(job['id']); self.state.update_job(job['id'],result={'outcome':'no_fix'})
        self.now+=4; self.state.recover()
        self.assertEqual(self.state.job(job['id'])['status'],'result')
        self.assertEqual(self.state.job(job['id'])['active_seconds'],4)

    def test_deep_budget_survives_new_manual_job_and_global_slot_connections(self):
        self.state.set_enabled(True)
        old=self.state.new_job('deep','sig',1,issue_number=7,active_seconds=14390,status='done')
        job=self.state.new_job('deep','sig',1,issue_number=7)
        claimed=self.state.claim(job['id']); self.assertEqual(claimed['remaining_seconds'],10)
        other=State(Path(self.temp.name)/'state.sqlite3',clock=lambda:self.now)
        try:
            second=other.new_job('deep','sig2',2,issue_number=8)
            with self.assertRaises(Deferred): other.claim(second['id'])
        finally: other.close()

    def test_cancelled_running_time_is_recovered(self):
        self.state.set_enabled(True)
        job=self.state.new_job('deep','sig',1)
        self.state.claim(job['id']); self.now+=50; self.state.cancel(job['id']); self.state.recover()
        self.assertEqual(self.state.job(job['id'])['active_seconds'],50)
        self.assertEqual(self.state.job(job['id'])['status'],'cancelled')

    def test_disabled_and_rolling_limits(self):
        a = self.state.new_job('initial', 'sig', 1)
        with self.assertRaises(Deferred): self.state.claim(a['id'])
        self.state.set_enabled(True)
        for n in range(30):
            job = a if n == 0 else self.state.new_job('initial', str(n), n+1)
            self.state.claim(job['id'])
            self.state.finish_run(job['id'], 0, {})
            self.state.update_job(job['id'], status='done')
        b = self.state.new_job('initial', '31', 31)
        with self.assertRaises(Deferred): self.state.claim(b['id'])
        self.now += DAY + 1
        self.state.claim(b['id'])

    def test_launch_ledger_migrates_legacy_initial_starts_only_once(self):
        self.state.db.execute('INSERT INTO starts VALUES(?,?,?)',('legacy','initial',self.now))
        self.state.db.execute('DROP TABLE initial_launches')
        for _ in range(2):
            other=State(Path(self.temp.name)/'state.sqlite3',clock=lambda:self.now)
            try: self.assertEqual(other.status()['starts']['initial'],1)
            finally: other.close()

    def test_initial_retries_each_charge_daily_launch_limit_across_restart(self):
        self.state.set_enabled(True)
        job=self.state.new_job('initial','sig',1)
        for _ in range(30):
            self.state.claim(job['id'])
            self.state.finish_run(job['id'],0,{})
            self.state.update_job(job['id'],status='queued')
        self.assertEqual(self.state.status()['starts']['initial'],30)
        with self.assertRaises(Deferred): self.state.claim(job['id'])
        other=State(Path(self.temp.name)/'state.sqlite3',clock=lambda:self.now)
        try:
            self.assertEqual(other.status()['starts']['initial'],30)
            with self.assertRaises(Deferred): other.claim(job['id'])
            self.now+=DAY+1
            other.claim(job['id'])
            self.assertEqual(other.status()['starts']['initial'],1)
        finally: other.close()

    def test_ten_deep_starts_per_rolling_day_but_existing_job_can_resume(self):
        self.state.set_enabled(True)
        first=None
        for n in range(10):
            job=self.state.new_job('deep',str(n),n+1,issue_number=n+1)
            first=first or job
            self.state.claim(job['id']); self.state.finish_run(job['id'],0,{}); self.state.update_job(job['id'],status='done')
        blocked=self.state.new_job('deep','extra',11,issue_number=11)
        with self.assertRaises(Deferred): self.state.claim(blocked['id'])
        self.state.update_job(first['id'],status='queued'); self.state.claim(first['id']); self.state.finish_run(first['id'],0,{})
        self.assertEqual(self.state.status()['starts']['deep'],10)
        self.now+=DAY+1; self.state.claim(blocked['id'])

    def test_late_duplicate_label_event_never_restarts_finished_generation(self):
        self.state.record_origin('marker',7,'sig',1)
        job=self.state.enqueue(7,'123',generation='label_1')
        self.state.update_job(job['id'],status='done')
        self.assertEqual(self.state.enqueue(7,'124',generation='label_1')['id'],job['id'])
        self.assertNotEqual(self.state.enqueue(7,'125',generation='label_2')['id'],job['id'])

    def test_dispatch_reservation_is_unique_and_requires_local_origin(self):
        with self.assertRaises(ValueError): self.state.enqueue(7, '123')
        self.state.record_origin('marker', 7, 'sig', 1)
        a = self.state.enqueue(7, '123')
        self.assertEqual(self.state.enqueue(7, '123')['id'], a['id'])
        self.assertEqual(self.state.enqueue(7, '124')['id'], a['id'])
        self.state.update_job(a['id'], status='done')
        self.assertNotEqual(self.state.enqueue(7, '125')['id'], a['id'])

    def test_legacy_review_stage_migrates_without_losing_progress(self):
        job = self.state.new_job('review', 'sig', 1, issue_number=7, active_seconds=80, rounds=2,
                                 previous_attempt={'checks': ['retained']})
        other = State(self.state.path, clock=lambda: self.now)
        try:
            migrated = other.job(job['id'])
            self.assertEqual(migrated['stage'], 'revise')
            self.assertEqual(migrated['active_seconds'], 80)
            self.assertEqual(migrated['rounds'], 2)
            self.assertEqual(migrated['previous_attempt'], {'checks': ['retained']})
        finally:
            other.close()


if __name__ == '__main__': unittest.main()
