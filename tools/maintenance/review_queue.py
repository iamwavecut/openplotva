"""The controller owns resumption of quota-deferred GitHub PR reviews."""
from __future__ import annotations

from .contracts import Deferred, InvalidResult, REPOSITORY
from .github import is_owner


class ReviewQueue:
    def __init__(self, state, github):
        self.state, self.github = state, github

    def release(self, record):
        slot = self.state.setting('review_slot')
        if slot and slot['check_id'] == record['receipt']['check_id']:
            self.state.set_setting('review_slot', None)

    def poll(self):
        waiting = self.state.setting('review_waits', {})
        pulls = self.github.pages('repos/'+REPOSITORY+'/pulls?state=open')
        eligible = {str(p['number']): p for p in pulls if is_owner(p.get('user')) and not p.get('draft')
                    and all(p.get(s, {}).get('repo', {}).get('full_name') == REPOSITORY for s in ('base', 'head'))}
        for number, record in waiting.items():
            pr = eligible.get(number)
            if not pr or pr['head']['sha'] != record['receipt']['head_sha']:
                record['phase'] = 'obsolete'
                self.release(record)
                continue
            if record['phase'] in {'uncertain', 'running'}:
                run = self.github.run(record['receipt']['run_id'])
                if run.get('run_attempt', 0) > record['receipt']['run_attempt']:
                    record['phase'] = 'running'
                    if run.get('status') == 'completed':
                        record['phase'] = 'finished'
                        self.release(record)
                timeout = 180 if record['phase'] == 'uncertain' else 1800
                if record['phase'] in {'uncertain', 'running'} and self.state.clock() - record['requested_at'] > timeout:
                    # No second POST after an ambiguous result. An operator can
                    # inspect/rerun the workflow without duplicate automatic work.
                    record['phase'] = 'needs_human'
                    self.release(record)
        for number, pr in eligible.items():
            execution = self.github.review_execution(pr)
            if not execution:
                continue
            record = waiting.get(number)
            if execution['state'] == 'complete':
                if record and record['phase'] != 'complete':
                    self.release(record)
                    record['phase'] = 'complete'
                    self.state.provider_recovered(execution['started_at'])
                continue
            if execution['state'] != 'quota_wait':
                continue
            if not record or record['receipt']['check_id'] != execution['check_id']:
                quota = self.state.defer_provider('review:'+str(execution['check_id']),
                    execution['retry_after_seconds'], at=execution['completed_at'])
                record = {'receipt': execution, 'phase': 'waiting', 'next_at': quota['until']}
                waiting[number] = record
        self.state.set_setting('review_waits', waiting)
        if not self.state.enabled() or not self.state.provider_available() or self.state.jobs({'running'}):
            return
        for record in waiting.values():
            if record['phase'] != 'waiting' or record['next_at'] > self.state.clock():
                continue
            try:
                self.github.validate_review_retry(record['receipt'])
            except InvalidResult:
                record['phase'] = 'needs_human'
                self.state.set_setting('review_waits', waiting)
                continue
            with self.state.transaction():
                record.update(phase='uncertain', requested_at=self.state.clock())
                self.state.set_setting('review_waits', waiting)
                self.state.set_setting('review_slot', record['receipt'])
            try:
                self.github.rerun_review(record['receipt'])
            except (Deferred, InvalidResult):
                pass  # Reconcile the actual workflow attempt on the next poll.
            return
