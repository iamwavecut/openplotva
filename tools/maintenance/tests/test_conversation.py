import copy
import unittest
import tempfile
import json
import io
from pathlib import Path
from contextlib import redirect_stdout
from unittest.mock import patch

from tools.maintenance.contracts import Deferred, InvalidResult, fingerprint, validate_output
from tools.maintenance.state import State
from tools.maintenance import worker
from tools.maintenance.tests import test_controller as fixtures
from tools.maintenance.tests.test_github import owner


BASE = fixtures.BASE


class ConversationTests(unittest.TestCase):
    incident = fixtures.ControllerTests.incident

    def setUp(self):
        fixtures.ControllerTests.setUp(self)
        self.incident(); self.controller.run_next()
        self.comments = {7: []}
        self.gh.comments = lambda number: copy.deepcopy(self.comments.get(number, []))
        self.gh.discussion = lambda item: {**item, 'comments': self.gh.comments(item['number']), 'linked_prs': []}
        self.state.set_setting('owner_feedback_since', '1970-01-02T00:00:00Z')

    def comment(self, body='Please explain which behavior is incorrect.', number=7, author=None):
        values = self.comments.setdefault(number, [])
        value = {'id': 100 + sum(map(len, self.comments.values())), 'body': body,
                 'user': author or owner(), 'issue_url': 'https://api.github.com/repos/iamwavecut/openplotva/issues/'+str(number),
                 'created_at': '1970-01-02T01:00:00Z', 'updated_at': '1970-01-02T01:00:00Z'}
        values.append(value)
        return value

    def poll(self):
        self.controller.conversation.poll()
        return [job for job in self.state.jobs() if job['stage'] == 'triage']

    def complete(self, job, action='reply'):
        prepared = self.controller.prepare_run()
        self.assertEqual(prepared['id'], job['id'])
        result = {'action': action, 'reply': 'Please describe a reproducible sequence.',
                  'reason': 'The expected behavior needs clarification.'}
        self.controller.accept_result(prepared, {**result, 'active_seconds': 1, 'usage': {}, 'checks': [], 'base_sha': BASE})
        self.controller.process_results()
        return self.state.job(job['id'])

    def managed_pr(self):
        job = self.controller.enqueue(7, '123')
        marker = '<!-- maintenance:pr:'+job['id']+' -->'
        self.state.update_job(job['id'], status='ready', pr_number=8, branch='fix/issue-7', published_sha=BASE)
        self.state.put_record('effects', fingerprint({'pr': job['id']}), {'kind': 'pr', 'state': 'done',
            'payload': {'marker': marker}, 'result': {'number': 8}})
        self.gh.prs[8] = {'kind': 'pr', 'title': 'Queue fix', 'number': 8, 'state': 'open', 'merged_at': None, 'user': owner(), 'body': marker,
            'base': {'repo': {'full_name': 'iamwavecut/openplotva'}},
            'head': {'sha': BASE, 'ref': 'fix/issue-7', 'repo': {'full_name': 'iamwavecut/openplotva'}}}
        self.gh.close_pr = lambda number: self.gh.prs[number].update(state='closed') or {'number': number}
        return job

    def test_only_owner_comments_create_one_durable_triage_and_edits_are_new_feedback(self):
        self.assertTrue(hasattr(self.controller, 'conversation'), 'owner comment triage is absent')
        self.comment(author={'login': 'iamwavecut', 'id': 1})
        original = self.comment()
        jobs = self.poll()
        self.assertEqual(len(jobs), 1)
        self.assertEqual([c['id'] for c in jobs[0]['owner_comments']], [original['id']])
        for _ in range(100): self.poll()
        self.assertEqual(len(self.poll()), 1)
        self.complete(jobs[0])
        self.assertEqual(len(self.poll()), 1)
        original.update(body='The failure also loses the queued task.', updated_at='1970-01-02T01:01:00Z')
        self.assertEqual(len(self.poll()), 2)

    def test_agent_reply_is_not_owner_feedback_and_legacy_comments_are_not_replayed(self):
        self.assertTrue(hasattr(self.controller, 'conversation'), 'owner comment triage is absent')
        legacy = self.comment(); legacy['updated_at'] = '1970-01-01T00:00:00Z'
        automated = self.comment('Recorded response. <!-- maintenance:feedback:test -->')
        self.state.put_record('effects', 'reply', {'kind': 'reply', 'state': 'done',
            'payload': {'number': 7, 'body': automated['body']}, 'result': {'id': automated['id']}})
        self.assertEqual(self.poll(), [])
        automated.update(body='I edited this response with new facts.', updated_at='1970-01-02T01:02:00Z')
        self.assertEqual(len(self.poll()), 1)

    def test_comment_holds_old_fix_and_reply_waits_without_consuming_deep_budget(self):
        self.assertTrue(hasattr(self.controller, 'conversation'), 'owner comment triage is absent')
        deep = self.controller.enqueue(7, '123')
        self.comment(); triage = self.poll()[0]
        self.assertTrue(self.state.stop_requested(deep['id']))
        finished = self.complete(triage)
        self.assertEqual(finished['status'], 'done')
        self.assertEqual(self.state.issue_usage(7)['cycles'], 0)
        self.assertEqual(self.state.status()['starts']['initial'], 2)
        self.assertTrue(self.state.stop_requested(deep['id']))
        self.assertIsNone(self.controller.prepare_run())
        self.assertEqual(len(self.gh.comment_values), 1)

    def test_continue_reuses_existing_job_and_retains_budget(self):
        self.assertTrue(hasattr(self.controller, 'conversation'), 'owner comment triage is absent')
        deep = self.controller.enqueue(7, '123')
        self.state.claim(deep['id']); self.state.finish_run(deep['id'], 71, {})
        self.state.update_job(deep['id'], status='needs_human')
        self.comment('Please investigate the task loss and add a regression check.')
        self.complete(self.poll()[0], 'continue')
        resumed = self.state.job(deep['id'])
        self.assertEqual(resumed['status'], 'queued')
        self.assertEqual(resumed['active_seconds'], 71)
        self.assertEqual(resumed['rounds'], 1)
        self.assertFalse(self.state.stop_requested(deep['id']))
        self.assertEqual(len([j for j in self.state.jobs() if j['stage'] == 'deep']), 1)

    def test_edit_during_triage_prevents_stale_publication(self):
        self.assertTrue(hasattr(self.controller, 'conversation'), 'owner comment triage is absent')
        comment = self.comment(); job = self.poll()[0]
        prepared = self.controller.prepare_run()
        comment.update(body='Wait, please do not change the code.', updated_at='1970-01-02T01:03:00Z')
        self.controller.accept_result(prepared, {'action': 'continue', 'reply': 'Investigation requested.',
            'reason': 'New evidence warrants investigation.', 'active_seconds': 1, 'usage': {}, 'checks': [], 'base_sha': BASE})
        self.controller.process_results()
        self.assertEqual(self.gh.comment_values, {})
        self.assertFalse(any(j['stage'] == 'deep' for j in self.state.jobs()))

    def test_removed_label_and_other_incident_comment_never_start_triage(self):
        self.assertTrue(hasattr(self.controller, 'conversation'), 'owner comment triage is absent')
        comment = self.comment(); comment['issue_url'] += '0'
        self.assertEqual(self.poll(), [])
        comment['issue_url'] = comment['issue_url'][:-1]
        self.gh.items[7]['labels'] = [{'name': 'agent:created'}]
        self.assertEqual(self.poll(), [])

    def test_triage_contract_rejects_arbitrary_targets_patches_and_empty_decisions(self):
        value = {'action': 'reply', 'reply': 'Which behavior should change?', 'reason': 'Requirements are unclear.'}
        try: validated = validate_output(value, 'triage')
        except InvalidResult: self.fail('valid owner feedback decision was rejected')
        self.assertEqual(validated, value)
        for fields in ({'action': 'merge'}, {'target_pr': 999}, {'action': 'patch'}, {'reply': ''}):
            with self.assertRaises(InvalidResult): validate_output({**value, **fields}, 'triage')

    def test_close_managed_pr_preserves_issue_and_recovers_after_lost_close_ack(self):
        repair = self.managed_pr()
        self.comment('This change is no longer needed; close the pull request.', number=8)
        triage = self.poll()[0]
        close = self.gh.close_pr
        def lost_ack(number):
            close(number)
            raise Deferred('response lost')
        self.gh.close_pr = lost_ack
        self.complete(triage, 'close_pr')
        self.assertEqual(self.gh.prs[8]['state'], 'closed')
        self.now += 60
        self.controller.process_results()
        self.assertEqual(self.state.job(triage['id'])['status'], 'done')
        self.assertEqual(self.state.job(repair['id'])['status'], 'done')
        self.assertEqual(self.gh.items[7]['state'], 'open')
        self.assertEqual(len(self.gh.comment_values), 1)

    def test_closed_or_unmanaged_pr_cannot_be_selected_by_model(self):
        self.comment('Close the unrelated PR too.')
        result = self.complete(self.poll()[0], 'close_pr')
        self.assertEqual(result['status'], 'needs_human')
        self.assertEqual(self.gh.comment_values, {})

    def test_changed_pr_head_cannot_be_closed(self):
        self.managed_pr()
        self.comment('Close this PR.')
        self.poll(); prepared = self.controller.prepare_run()
        self.gh.prs[8]['head']['sha'] = 'c'*40
        self.controller.accept_result(prepared, {'action': 'close_pr', 'reply': 'Closing the obsolete fix.',
            'reason': 'The owner no longer wants this change.', 'active_seconds': 1, 'usage': {}, 'checks': [], 'base_sha': BASE})
        self.controller.process_results()
        self.assertEqual(self.gh.prs[8]['state'], 'open')
        self.assertEqual(self.gh.comment_values, {})

    def test_continuation_after_closed_pr_creates_new_job_without_reopening_old_pr(self):
        old = self.managed_pr()
        self.comment('Close the PR.')
        self.complete(self.poll()[0], 'close_pr')
        self.comment('New evidence warrants a different fix.')
        triage = self.poll()[-1]
        self.complete(triage, 'continue')
        self.assertEqual(self.gh.prs[8]['state'], 'closed')
        self.assertEqual(self.state.job(old['id'])['status'], 'done')
        pending = [j for j in self.state.jobs({'queued'}) if j['stage'] == 'deep']
        self.assertEqual(len(pending), 1)
        self.assertIsNone(pending[0]['pr_number'])

    def test_restart_after_reply_does_not_duplicate_effect_or_deep_resume(self):
        self.comment('Please continue.')
        job = self.poll()[0]
        original = self.state.update_job
        def crash(job_id, **fields):
            if job_id == job['id'] and fields.get('status') == 'done': raise RuntimeError('crash')
            return original(job_id, **fields)
        self.state.update_job = crash
        with self.assertRaises(RuntimeError): self.complete(job, 'continue')
        self.state.update_job = original
        self.state = State(self.state.path, clock=lambda: self.now)
        self.addCleanup(self.state.close)
        self.controller = fixtures.Controller(self.controller.config, self.state, self.api, self.gh, self.runner)
        self.state.recover()
        self.controller.process_results()
        self.assertEqual(len(self.gh.comment_values), 1)
        self.assertEqual(len([j for j in self.state.jobs() if j['stage'] == 'deep']), 1)
        self.assertEqual(len(self.poll()), 1)

    def test_new_feedback_after_close_does_not_replay_old_pr_comments(self):
        self.managed_pr(); self.comment('Close this PR.', number=8)
        self.complete(self.poll()[0], 'close_pr')
        self.assertEqual(len(self.poll()), 1)
        self.comment('What should I check next?')
        self.assertEqual(len(self.poll()), 2)

    def test_worker_accepts_bounded_triage_and_rejects_patch_shape(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            (work/'context.json').write_text('{"stage":"triage"}')
            (work/'result.json').write_text(json.dumps({'action': 'reply', 'reply': 'Which behavior should change?',
                                                       'reason': 'Requirements need clarification.'}))
            with patch.object(worker, 'WORK', work), redirect_stdout(io.StringIO()) as output:
                status = worker.validate_result()
            self.assertEqual(status, 0)
            with patch.object(worker, 'WORK', work):
                instruction = worker.launch_instruction(600)
                with self.assertRaises(ValueError): worker.launch_instruction(601)
            self.assertIn('triage', instruction)
            self.assertTrue(json.loads(output.getvalue())['valid'])
            (work/'result.json').write_text('{"outcome":"patch"}')
            with patch.object(worker, 'WORK', work), redirect_stdout(io.StringIO()) as output:
                self.assertEqual(worker.validate_result(), 1)
            self.assertEqual(json.loads(output.getvalue())['expected_root_fields'], ['action', 'reply', 'reason'])

    def test_latest_owner_guidance_is_in_resumed_fix_context(self):
        self.comment('Preserve accepted tasks and test retries.')
        self.complete(self.poll()[0], 'continue')
        repair = self.controller.prepare_run()
        self.assertEqual(repair['context']['owner_guidance'][0]['comments'][0]['body'],
                         'Preserve accepted tasks and test retries.')

    def test_exhausted_repair_budget_still_answers_but_cannot_start_fix(self):
        self.state.new_job('deep', 'signature', 1, issue_number=7, active_seconds=14400, status='needs_human')
        self.comment('Continue fixing this.')
        self.complete(self.poll()[0], 'continue')
        self.assertTrue(self.state.owner_hold(7))
        self.assertIsNone(self.controller.prepare_run())
        self.assertIn('budget', next(iter(self.gh.comment_values.values()))['body'])

    def test_edits_before_start_are_coalesced_and_latest_text_can_be_answered(self):
        comment = self.comment('First version.')
        self.poll()
        comment.update(body='Updated question.', updated_at='1970-01-02T01:02:00Z')
        jobs = self.poll()
        self.assertEqual(len(jobs), 1)
        self.assertEqual([c['body'] for c in jobs[0]['owner_comments']], ['Updated question.'])
        self.assertEqual(self.complete(jobs[0])['status'], 'done')

    def test_reply_to_pr_comment_stays_in_pr_discussion(self):
        self.managed_pr(); self.comment('Explain this proposed change.', number=8)
        self.complete(self.poll()[0])
        self.assertEqual(next(iter(self.gh.comment_values.values()))['number'], 8)

    def test_short_triage_quota_failure_keeps_its_own_budget(self):
        from tools.maintenance.contracts import QuotaUnavailable
        self.state.new_job('deep', 'signature', 1, issue_number=7, active_seconds=14400, status='needs_human')
        self.comment(); job = self.poll()[0]
        prepared = self.controller.prepare_run()
        self.assertEqual(prepared['id'], job['id'])
        self.controller.defer_quota(prepared, QuotaUnavailable(active_seconds=10))
        self.assertEqual(self.state.job(job['id'])['status'], 'queued')
        self.assertEqual(self.state.job(job['id'])['active_seconds'], 10)

    def test_close_privacy_failure_cannot_close_pr_before_reply_is_checked(self):
        self.managed_pr(); self.comment('Close this PR.')
        self.poll(); prepared = self.controller.prepare_run()
        with patch('tools.maintenance.conversation.public_feedback', side_effect=InvalidResult('private output')):
            self.controller.accept_result(prepared, {'action': 'close_pr', 'reply': 'Invalid public response.',
                'reason': 'Owner feedback.', 'active_seconds': 1, 'usage': {}, 'checks': [], 'base_sha': BASE})
            self.controller.process_results()
        self.assertEqual(self.gh.prs[8]['state'], 'open')

    def test_fresh_manual_queue_generation_resumes_hold_but_replayed_event_cannot(self):
        self.gh.run = lambda event: {**fixtures.dispatch(), 'id': int(event)}
        repair = self.controller.enqueue(7, '123')
        self.comment(); triage = self.poll()[0]
        self.controller.enqueue(7, '124')
        self.assertTrue(self.state.owner_hold(7))
        self.gh.queue_generation = lambda number, run: 'label_2'
        resumed = self.controller.enqueue(7, '125')
        self.assertEqual(resumed['id'], repair['id'])
        self.assertFalse(self.state.owner_hold(7))
        self.assertEqual(self.state.job(triage['id'])['status'], 'cancelled')

    def test_comment_edit_at_last_publication_check_prevents_stale_reply(self):
        comment = self.comment(); self.poll(); prepared = self.controller.prepare_run()
        original = self.gh.find_comment
        def edit_before_write(number, marker):
            comment.update(body='A different question.', updated_at='1970-01-02T01:04:00Z')
            return original(number, marker)
        self.gh.find_comment = edit_before_write
        self.controller.accept_result(prepared, {'action': 'reply', 'reply': 'Previous answer.', 'reason': 'Earlier question.',
            'active_seconds': 1, 'usage': {}, 'checks': [], 'base_sha': BASE})
        self.controller.process_results()
        self.assertEqual(self.gh.comment_values, {})

    def test_owner_feedback_is_not_replayed_as_ordinary_pr_review(self):
        self.managed_pr(); comment = self.comment('What does this change do?', number=8)
        self.complete(self.poll()[0])
        snapshot = {'artifacts': [
            {'kind': 'comment', 'id': str(comment['id']), 'body': comment['body']},
            {'kind': 'comment', 'id': '900', 'body': 'Reviewer found a concrete race.'}]}
        result = self.controller.conversation.filter_review(snapshot, 7)
        self.assertEqual([a['id'] for a in result['artifacts']], ['900'])

    def test_pr_continuation_reads_latest_review_bodies(self):
        repair = self.managed_pr()
        self.state.update_job(repair['id'], feedback=[{'kind': 'comment', 'id': '900', 'body': 'Old finding.'}])
        current = {'kind': 'comment', 'id': '900', 'body': 'Edited finding: task state is still lost.', 'hash': 'new', 'head': BASE}
        self.gh.review_snapshot = lambda number: {'head': BASE, 'artifacts': [current], 'checks': []}
        self.comment('Please revise the PR using my feedback.')
        self.complete(self.poll()[0], 'continue')
        self.assertEqual(self.state.job(repair['id'])['feedback'], [current])

    def test_closure_final_scope_check_rejects_changed_head(self):
        self.managed_pr(); self.comment('Close this PR.')
        self.poll(); prepared = self.controller.prepare_run()
        self.state.put_record('effects', fingerprint({'close_pr': prepared['id']}),
            {'kind': 'close_pr', 'state': 'intent', 'payload': {'number': 8, 'head': BASE}})
        self.gh.prs[8]['head']['sha'] = 'c'*40
        with self.assertRaises(InvalidResult): self.controller.conversation.current(prepared)

    def test_close_adapter_requires_owner_and_only_changes_pr_state(self):
        from tools.maintenance.github import GitHub
        github = GitHub({})
        writes = []
        def api(path, method='GET', payload=None):
            if path == 'user': return owner()
            writes.append((path, method, payload))
            return {'number': 8, 'state': 'closed'}
        github.api = api
        github.close_pr(8)
        self.assertEqual(writes, [('repos/iamwavecut/openplotva/pulls/8', 'PATCH', {'state': 'closed'})])
        github.api = lambda *args: {'login': 'other', 'id': 123}
        with self.assertRaises(InvalidResult): github.close_pr(8)
