"""Owner-only issue conversations, with durable comment versions and bounded actions."""
from __future__ import annotations

import datetime

from .contracts import DEEP_SECONDS, MAX_ROUNDS, REPOSITORY, Deferred, InvalidResult, fingerprint, text, validate_output
from .github import is_owner, validate_issue
from .privacy import public_feedback


class Conversation:
    def __init__(self, controller):
        self.controller = controller
        self.state, self.github = controller.state, controller.github

    def blocked(self, job):
        return job['stage'] != 'triage' and self.state.owner_hold(job.get('issue_number'))

    def repair_jobs(self, number):
        return [job for job in self.state.jobs() if job.get('issue_number') == number
                and job['stage'] in ('deep', 'revise') and not job['cancelled']]

    def managed_pr(self, number, include_closed=False):
        candidates = [job for job in self.repair_jobs(number) if job.get('pr_number')]
        for job in reversed(candidates):
            effect = self.state.record('effects', fingerprint({'pr': job['id']}))
            if not effect or effect.get('state') != 'done' or (effect.get('result') or {}).get('number') != job['pr_number']:
                continue
            pr = self.github.pr(job['pr_number'])
            if (not is_owner(pr.get('user')) or pr.get('base', {}).get('repo', {}).get('full_name') != REPOSITORY
                    or pr.get('head', {}).get('repo', {}).get('full_name') != REPOSITORY
                    or pr.get('head', {}).get('ref') != job.get('branch')
                    or effect['payload']['marker'] not in (pr.get('body') or '')):
                raise InvalidResult('managed pull request provenance changed')
            if pr['state'] == 'open' or include_closed: return job, pr
        return None

    def automated(self, number, comment):
        for effect in self.state.records('effects'):
            payload = effect.get('payload', {})
            if (effect['kind'] in ('comment', 'reply') and payload.get('number') == number
                    and payload.get('body') == comment.get('body')):
                result = effect.get('result') or {}
                if str(result.get('id')) == str(comment['id']) or effect['state'] == 'uncertain': return True
        return bool(comment.get('performed_via_github_app'))

    def comments(self, number):
        since = self.state.setting('owner_feedback_since')
        values = []
        for comment in self.github.comments(number):
            if (not is_owner(comment.get('user')) or type(comment.get('id')) is not int
                    or comment['id'] <= 0 or self.automated(number, comment)
                    or comment.get('issue_url') != 'https://api.github.com/repos/'+REPOSITORY+'/issues/'+str(number)):
                continue
            updated = comment.get('updated_at', '')
            try:
                at = datetime.datetime.fromisoformat(updated.replace('Z', '+00:00'))
                cutoff = datetime.datetime.fromisoformat(since.replace('Z', '+00:00'))
                if at < cutoff: continue
                body = text(comment.get('body'), 12000)
            except (ValueError, TypeError, InvalidResult):
                continue
            value = {'number': number, 'id': comment['id'], 'updated_at': updated, 'body': body}
            values.append({**value, 'version': fingerprint(value)})
        return values

    def poll(self, issue_number=None):
        if self.state.setting('owner_feedback_since') is None:
            self.state.set_setting('owner_feedback_since', datetime.datetime.fromtimestamp(
                self.controller.clock(), datetime.timezone.utc).isoformat())
        numbers = [row[0] for row in self.state.db.execute('SELECT issue_number FROM origins WHERE issue_number IS NOT NULL')]
        if issue_number is not None: numbers = [number for number in numbers if number == issue_number]
        for number in numbers:
            try:
                validate_issue(self.github.issue(number), number)
                managed = self.managed_pr(number)
                comments = self.comments(number)
                if managed: comments += self.comments(managed[1]['number'])
                with self.state.transaction():
                    previous = [job for job in self.state.jobs() if job['stage'] == 'triage' and job['issue_number'] == number]
                    seen = {comment['version'] for job in previous for comment in job['owner_comments']}
                    fresh = sorted((comment for comment in comments if comment['version'] not in seen),
                                   key=lambda comment: (comment['updated_at'], comment['id']))
                    if not fresh: continue
                    # A polling retry and a crash reuse the same SQLite reservation.
                    waiting = next((job for job in previous if job['status'] == 'queued' and not job.get('context')), None)
                    if waiting:
                        combined = {(comment['number'], comment['id']): comment for comment in waiting['owner_comments']+fresh}
                        self.state.update_job(waiting['id'], owner_comments=sorted(combined.values(),
                            key=lambda comment: (comment['updated_at'], comment['id'])))
                    else:
                        origin = self.state.origin(number)
                        self.state.new_job('triage', origin['signature'], origin['incident_id'],
                                           issue_number=number, owner_comments=fresh)
                    self.state.set_setting('owner_hold_'+str(number), True)
            except InvalidResult:
                # A removed label or changed provenance revokes conversation access.
                continue

    def guidance(self, number):
        decisions = [job for job in self.state.jobs() if job['stage'] == 'triage'
                     and job['issue_number'] == number and job['status'] == 'done' and job.get('result')]
        return [{'comments': job['owner_comments'], 'decision': job['result']} for job in decisions[-10:]]

    def context(self, job):
        self.current(job)
        managed = self.managed_pr(job['issue_number'])
        target = None
        if managed:
            _, pr = managed
            target = {key: pr.get(key) for key in ('number', 'state', 'title', 'body')}
            target['head'] = pr['head']['sha']
        prior = [{'action': candidate['result']['action'], 'reply': candidate['result']['reply']}
                 for candidate in self.state.jobs() if candidate['stage'] == 'triage'
                 and candidate['issue_number'] == job['issue_number'] and candidate['status'] == 'done'
                 and candidate.get('result') and candidate['id'] != job['id']][-20:]
        return {'owner_comments': job['owner_comments'], 'managed_pr': target,
                'previous_replies': prior, 'repair_budget': self.state.issue_usage(job['issue_number'])}

    def current(self, job):
        validate_issue(self.github.issue(job['issue_number']), job['issue_number'])
        closing = self.state.record('effects', fingerprint({'close_pr': job['id']}))
        managed = self.managed_pr(job['issue_number'], include_closed=bool(closing))
        if closing and (not managed or managed[1].get('merged_at')
                        or managed[1]['number'] != closing['payload']['number']
                        or managed[1]['head']['sha'] != closing['payload']['head']):
            raise InvalidResult('closure target changed before publication')
        allowed = {job['issue_number']}
        if managed: allowed.add(managed[1]['number'])
        current = {c['version'] for number in allowed for c in self.comments(number)}
        # New comments also supersede a decision already being computed.
        known = {c['version'] for candidate in self.state.jobs() if candidate['stage'] == 'triage'
                 and candidate['issue_number'] == job['issue_number']
                 for c in candidate['owner_comments']}
        if not {c['version'] for c in job['owner_comments']} <= current or current - known:
            raise InvalidResult('owner feedback changed during triage')
        newer = [candidate for candidate in self.state.jobs() if candidate['stage'] == 'triage'
                 and candidate['issue_number'] == job['issue_number'] and candidate['id'] != job['id']
                 and candidate['status'] == 'queued']
        if newer and job.get('context'): raise InvalidResult('new owner feedback supersedes this decision')

    def process(self, job):
        value = validate_output({key: job['result'][key] for key in ('action', 'reply', 'reason')}, 'triage')
        self.current(job)
        controller = self.controller
        action = value['action']
        reply = public_feedback(value['reply'], job, controller.config)
        marker = '<!-- maintenance:conversation:'+job['id']+' -->'
        body = reply+'\n\n'+marker
        target = job['owner_comments'][-1]['number']
        repair = None
        feedback = []
        if action == 'continue':
            budget = self.state.issue_usage(job['issue_number'])
            if budget['active_seconds'] >= DEEP_SECONDS or budget['cycles'] >= MAX_ROUNDS:
                action = 'reply'
                body += '\n\nThe repair budget for this issue is exhausted; work remains paused for the owner.'
            managed = self.managed_pr(job['issue_number'])
            jobs = [candidate for candidate in self.repair_jobs(job['issue_number']) if not candidate.get('pr_number')]
            repair = managed[0] if managed else (jobs[-1] if jobs else None)
            if managed and managed[1]['head']['sha'] != repair['published_sha']:
                raise InvalidResult('managed pull request changed outside the controller')
            if managed:
                snapshot = self.filter_review(self.github.review_snapshot(managed[1]['number']), job['issue_number'])
                if snapshot['head'] != repair['published_sha']: raise InvalidResult('review revision changed')
                feedback = snapshot['artifacts'] + [
                    {'kind': 'check', 'id': str(check.get('id', check['name'])),
                     'body': text(check['name']+': '+str(check.get('conclusion'))+'\n'
                                  +str((check.get('output') or {}).get('summary') or '')[:12000]), 'head': snapshot['head']}
                    for check in snapshot['checks'] if check.get('status') == 'completed'
                    and check.get('conclusion') not in ('success', 'neutral', 'skipped')]
        if action == 'close_pr':
            managed = self.managed_pr(job['issue_number'])
            expected = job['context']['conversation'].get('managed_pr')
            # Reconciliation is possible after a crash immediately after close.
            key = fingerprint({'close_pr': job['id']})
            existing = self.state.record('effects', key)
            if not existing:
                if not managed or not expected or managed[1]['number'] != expected['number']:
                    raise InvalidResult('no managed pull request authorized for closure')
                repair, pr = managed
                if pr['head']['sha'] != expected['head'] or pr['head']['sha'] != repair['published_sha']:
                    raise InvalidResult('pull request changed after owner feedback')
            if not expected: raise InvalidResult('missing closure target')
            def reconcile():
                pr = self.github.pr(expected['number'])
                if pr.get('merged_at') or pr['head']['sha'] != expected['head']:
                    raise InvalidResult('closure target changed')
                return {'number': pr['number']} if pr['state'] == 'closed' else None
            def close():
                receipt = self.github.close_pr(expected['number'])
                if (not isinstance(receipt, dict) or receipt.get('number') != expected['number']
                        or receipt.get('state') != 'closed' or receipt.get('merged_at')
                        or receipt.get('head', {}).get('sha') != expected['head']):
                    raise Deferred('pull request closure receipt is unconfirmed')
                return {'number': receipt['number'], 'head': expected['head'], 'state': 'closed'}
            controller.effect(key, 'close_pr', {'number': expected['number'], 'head': expected['head']},
                              reconcile, close, job)
            if reconcile() is None: raise Deferred('pull request closure is not yet observable')
            body += '\n\nClosed PR #'+str(expected['number'])+'.'
        controller.effect(fingerprint({'conversation_reply': job['id']}), 'reply',
            {'number': target, 'body': body},
            lambda: self.github.find_comment(target, marker),
            lambda: self.github.comment(target, body), job)
        with self.state.transaction():
            if action == 'continue':
                if repair:
                    started = self.state.db.execute('SELECT 1 FROM starts WHERE job_id=?', (repair['id'],)).fetchone()
                    self.state.update_job(repair['id'], stage='revise' if repair.get('pr_number') else 'deep',
                        status='queued', result=None, prepared=None, next_at=0, attempts=0,
                        rounds=repair['rounds']+(1 if started else 0), feedback=feedback)
                else:
                    self.state.new_job('deep', job['signature'], job['incident_id'], issue_number=job['issue_number'])
                self.state.set_setting('owner_hold_'+str(job['issue_number']), False)
            elif action == 'close_pr':
                for repair in self.repair_jobs(job['issue_number']):
                    self.state.update_job(repair['id'], status='done', reason='closed after owner feedback')
            self.state.update_job(job['id'], status='done')
        if action != 'continue': controller.notify(job, 'needs_human')

    def filter_review(self, snapshot, number):
        handled = {(str(c['id']), c['body']) for job in self.state.jobs() if job['stage'] == 'triage'
                   and job['issue_number'] == number for c in job['owner_comments']}
        return {**snapshot, 'artifacts': [item for item in snapshot['artifacts']
                if not (item['kind'] == 'comment' and (str(item['id']), item['body']) in handled)]}
