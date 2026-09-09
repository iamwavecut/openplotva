import concurrent.futures
import contextlib
import io
import json
import copy
import tempfile
import unittest
from pathlib import Path
from tools.maintenance.controller import Controller, issue_body, review_ready, main
from tools.maintenance.state import State
from tools.maintenance.contracts import Deferred, InvalidResult, QuotaUnavailable, fingerprint
from tools.maintenance.tests.test_github import issue, dispatch, ReviewGitHub

BASE = 'a'*40

def diagnosis(action='investigate', external='possible', code='possible', matches=None):
    return {'external_cause': external, 'code_defect': code, 'observations': ['Queue timed out.'], 'hypotheses': ['Lock contention.'],
            'supporting': ['Bounded timeout counter increased.'], 'contradicting': [], 'related_changes': [],
            'missing': [] if action in ('observe', 'fix') else ['Lock ownership unknown.'], 'next_action': action,
            'title': 'Queue timeout', 'summary': 'Requests failed after queue timeout.', 'matches': matches or [], 'acceptance': ['No timeout under bounded contention.']}

class API:
    def __init__(self): self.events = []; self.receipts = {}; self.delivery = 'queued'
    def incidents(self, after):
        events = [e for e in self.events if e['id'] > after]
        return {'incidents': events, 'next_cursor': events[-1]['id'] if events else after}
    def evidence(self, _): return {'queue': {'timeouts': 1}}
    def notify(self, payload):
        self.receipts[payload['key']] = {'key': payload['key'], 'state': self.delivery, 'telegram_message_id': 1 if self.delivery == 'sent' else None}
        return self.receipts[payload['key']]
    def notification(self, key):
        self.receipts[key]['state'] = self.delivery
        self.receipts[key]['telegram_message_id'] = 1 if self.delivery == 'sent' else None
        return self.receipts[key]

class GH:
    def __init__(self):
        self.items = {}; self.created = 0; self.comment_values = {}; self.fail_create = False; self.prs = {}; self.remote = {}; self.fail_push = False; self.fail_pr = False
    def index(self): return list(self.items.values())+list(self.prs.values())
    def discussion(self, item): return {**item, 'comments': [], 'linked_prs': item.get('linked_prs', [])}
    def issue(self, number): return self.items[number]
    def comments(self, number): return [value for value in self.comment_values.values() if value['number'] == number]
    def run(self, _): return dispatch()
    def ensure_labels(self): return {}
    def labels_ready(self): return True
    def queue_generation(self, number, run): return "label_1"
    def assert_owner(self): pass
    def create_issue(self, title, body, labels):
        number=max([6,*self.items,*self.prs])+1
        self.created += 1; value = {**issue(), 'number':number, 'kind': 'issue', 'title': title, 'body': body, 'labels': [{'name': v} for v in labels]}; self.items[number] = value
        if self.fail_create: self.fail_create = False; raise Deferred('ambiguous network')
        return value
    def find_issue(self, marker): return next((v for v in self.items.values() if marker in v.get('body','')), None)
    def find_comment(self, number, marker): return next((v for v in self.comment_values.values() if v['number']==number and marker in v['body']), None)
    def comment(self, number, body, comment_id=None):
        value = {'id': comment_id or len(self.comment_values)+1, 'number':number, 'body': body}; self.comment_values[value['id']] = value; return value
    def add_label(self, *args): return {}
    def pr(self, number): return self.prs[number]
    def prepare_patch(self, job, result, round_number): return {'sha': 'b'*40, 'branch': 'fix/issue-7-'+job['id'][:12], 'directory': '/local'}
    def validate_prepared(self, job, prepared): pass
    def remote_sha(self, branch): return self.remote.get(branch)
    def push(self, prepared, previous=None):
        self.remote[prepared['branch']] = prepared['sha']
        if self.fail_push: self.fail_push = False; raise Deferred('ambiguous push')
        return {'sha': prepared['sha']}
    def find_pr(self, branch, marker): return next((p for p in self.prs.values() if marker in p['body']), None)
    def create_pr(self, branch, title, body):
        p = {'number': 8, 'body': body, 'state': 'open', 'head': {'sha':'b'*40,'ref':branch},'base': {'ref':'main'},'kind':'pr'}; self.prs[8] = p
        if self.fail_pr: self.fail_pr = False; raise Deferred('ambiguous PR')
        return p

class Runner:
    def __init__(self): self.value = diagnosis(); self.calls = 0; self.failure = False; self.ancestry = None
    def preflight(self): pass
    def refresh_source(self): return BASE
    def host_snapshot(self): return {'revision': BASE}
    def contains(self, *_): return self.ancestry
    def run(self, job, context):
        self.calls += 1
        if self.failure: raise Deferred('provider unavailable')
        return {'diagnosis': self.value, 'outcome':'no_fix', 'active_seconds': 1, 'usage': {'tokens': 4}, 'feedback': [], 'checks': [], 'base_sha': BASE}

class ControllerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.now = 100000.
        self.state = State(Path(self.temp.name)/'state.sqlite3', clock=lambda:self.now); self.addCleanup(self.state.close)
        self.api = API(); self.gh = GH(); self.runner = Runner()
        self.controller = Controller({'state_dir':self.temp.name, 'debounce_seconds':0}, self.state, self.api, self.gh, self.runner)
        self.state.set_enabled(True)
    def incident(self):
        self.api.events = [{'id':n,'signature':'signature','first_seen':1,'last_seen':n,'snapshot':{'reason':'timeout'}} for n in range(1,101)]
        self.controller.poll_incidents(); self.controller.schedule_incidents()
        return self.state.jobs()[0]

    def test_exhausted_launch_limit_waits_without_context_io_or_failure_attempts(self):
        for stage, count in (('initial', 30), ('deep', 10)):
            with self.subTest(stage=stage):
                self.state.db.execute('DELETE FROM jobs')
                self.state.db.execute('DELETE FROM starts')
                self.state.db.execute('DELETE FROM initial_launches')
                for number in range(count):
                    prior = self.state.new_job(stage, str(number), number+1)
                    self.state.claim(prior['id'])
                    self.state.finish_run(prior['id'], 0, {})
                    self.state.update_job(prior['id'], status='done')
                job = self.state.new_job(stage, 'waiting', 100)
                def forbidden(*args): raise AssertionError('No external context IO before launch admission')
                self.runner.refresh_source = forbidden
                self.gh.issue = forbidden
                self.controller.prepare_run()
                current = self.state.job(job['id'])
                self.assertEqual(current['status'], 'queued')
                self.assertEqual(current['attempts'], 0)
                self.assertEqual(current['next_at'], self.now+86400)
                self.assertNotIn('context', current)

    def known_ready_incident(self):
        self.incident(); self.controller.run_next()
        deep=self.controller.enqueue(7,'123')
        self.state.claim(deep['id']); self.state.finish_run(deep['id'],1,{})
        self.state.update_job(deep['id'],status='ready',pr_number=8,branch='fix/issue-7',published_sha='b'*40)
        self.gh.prs[8]={'kind':'pr','number':8,'state':'open','title':'Timeout fix','body':'Closes #7',
                       'head':{'sha':'b'*40,'ref':'fix/issue-7'},'base':{'ref':'main'}}
        # The timeline can lag publication; the persisted job still names the PR.
        self.gh.items[7]['linked_prs']=[]
        self.controller.config['history_limit']=1
        self.gh.index=lambda: [{'kind':'issue','number':1,'title':'queue timeout','body':'Older unrelated report'}]
        return deep

    def test_review_loads_linked_pr_discussion_when_rest_comments_is_a_count(self):
        deep = self.known_ready_incident()
        self.state.update_job(deep['id'], stage='revise', status='queued')
        self.gh.prs[8]['comments'] = 2
        comments = [{'id': 20, 'body': 'Clarify the affected route.'},
                    {'id': 21, 'body': 'The existing patch needs no behavioral change.'}]
        original_discussion = self.gh.discussion
        self.gh.discussion = lambda item: {
            **original_discussion(item),
            'comments': comments if item['kind'] == 'pr' and item['number'] == 8 else [],
        }
        previous_calls = self.runner.calls
        claimed = self.controller.prepare_run()
        self.assertEqual(claimed['id'], deep['id'])
        self.assertEqual(claimed['status'], 'running')
        related = next(item for item in claimed['context']['history']
                       if item['kind'] == 'pr' and item['number'] == 8)
        self.assertEqual(related['comments'], comments)
        self.assertEqual(self.runner.calls, previous_calls)

    def repeat_known_incident(self):
        self.api.events.append({'id':101,'signature':'signature','first_seen':1,'last_seen':101,'snapshot':{'reason':'timeout'}})
        self.controller.poll_incidents(); self.controller.schedule_incidents()
        self.runner.value=diagnosis(matches=[])
        self.controller.run_next()

    def test_same_signature_ready_pr_is_reused_when_model_omits_match_outside_shortlist(self):
        deep=self.known_ready_incident()
        self.repeat_known_incident()
        self.assertEqual(sorted(self.gh.items),[7])
        self.assertEqual(sorted(self.gh.prs),[8])
        self.assertEqual(self.state.status()['starts']['deep'],1)
        self.assertEqual(len([j for j in self.state.jobs() if j['stage']=='deep']),1)
        self.assertEqual(self.state.job(deep['id'])['status'],'ready')
        facts=next(iter(self.gh.comment_values.values()))
        self.assertEqual(facts['number'],7)
        self.assertIn('101 captured terminal events',facts['body'])
        initial=[j for j in self.state.jobs() if j['stage']=='initial'][-1]
        self.assertIn(7,[item['number'] for item in initial['context']['history']])
        # Replaying the immutable event preserves the same editable fact comment.
        self.controller.poll_incidents(); self.controller.schedule_incidents(); self.controller.process_results()
        self.assertEqual(len(self.gh.comment_values),1)

    def test_closed_known_pr_preserves_no_fix_and_material_new_evidence_routes(self):
        self.known_ready_incident()
        self.gh.prs[8].update(state='closed',merged_at=None)
        self.repeat_known_incident()
        self.assertEqual(sorted(self.gh.items),[7])
        self.assertEqual(self.state.status()['starts']['deep'],1)
        # A real new fact can still requeue the open issue after an unmerged PR closes.
        self.state.new_job('initial','signature',101)
        self.runner.value=diagnosis('fix','not_observed','confirmed',[{'kind':'issue','number':7,
            'relationship':'new_evidence','reason':'A new reproducer establishes the missing interleaving.'}])
        self.controller.run_next()
        queued=[j for j in self.state.jobs({'queued'}) if j['stage']=='deep']
        self.assertEqual(len(queued),1)
        self.assertEqual(queued[0]['issue_number'],7)
        self.assertEqual(self.controller.prepare_run()['id'],queued[0]['id'])
        self.assertEqual(self.state.status()['starts']['deep'],2)

    def test_duplicate_origin_cannot_start_deep_while_known_signature_pr_is_open(self):
        self.known_ready_incident()
        self.gh.items[9]={**issue(),'number':9,'title':'Accidental duplicate','body':'Repeated timeout'}
        self.state.record_origin('duplicate',9,'signature',101)
        duplicate=self.state.enqueue(9,'duplicate_event','duplicate_generation')
        self.assertIsNone(self.controller.prepare_run())
        self.assertEqual(self.state.job(duplicate['id'])['status'],'done')
        self.assertEqual(self.state.status()['starts']['deep'],1)

    def test_duplicate_origin_cannot_bypass_known_open_issue_after_pr_closes(self):
        self.known_ready_incident()
        self.gh.prs[8].update(state='closed',merged_at=None)
        self.gh.items[9]={**issue(),'number':9,'title':'Accidental duplicate','body':'Repeated timeout'}
        self.state.record_origin('duplicate',9,'signature',101)
        duplicate=self.state.enqueue(9,'duplicate_event','duplicate_generation')
        self.assertIsNone(self.controller.prepare_run())
        self.assertEqual(self.state.job(duplicate['id'])['reused_issue_number'],7)
        self.assertEqual(self.state.status()['starts']['deep'],1)

    def test_duplicate_origin_cannot_publish_after_known_signature_pr_appears(self):
        self.known_ready_incident()
        self.gh.items[9]={**issue(),'number':9,'title':'Accidental duplicate','body':'Repeated timeout'}
        self.state.record_origin('duplicate',9,'signature',101)
        result={'diagnosis':diagnosis('fix','not_observed','confirmed'),'outcome':'patch','base_sha':BASE,
                'patch_path':'/offline','checks':[{'name':name,'passed':True} for name in ('fmt','clippy','tests')],'feedback':[]}
        duplicate=self.state.new_job('deep','signature',101,issue_number=9,status='result',base_sha=BASE,result=result)
        self.controller.process_results()
        self.assertEqual(self.state.job(duplicate['id'])['status'],'done')
        self.assertEqual(self.gh.remote,{})
        self.assertEqual(sorted(self.gh.prs),[8])
        self.assertEqual(self.state.status()['starts']['deep'],1)

    def test_known_merged_pr_still_requires_deployment_ancestry_with_empty_matches(self):
        self.known_ready_incident()
        self.gh.prs[8].update(state='closed',merged_at='now',merge_commit_sha='b'*40)
        self.gh.items[7]['state']='closed'
        self.runner.ancestry=False
        self.repeat_known_incident()
        latest=[j for j in self.state.jobs() if j['stage']=='initial'][-1]
        self.assertEqual(latest['status'],'wait_deploy')
        self.assertEqual(sorted(self.gh.items),[7])
        self.runner.ancestry=True
        self.state.new_job('initial','signature',101)
        self.controller.run_next()
        self.assertEqual(sorted(self.gh.items),[7,9])
        self.assertIn('Regression after deployed fix in PR #8',self.gh.items[9]['body'])

    def test_own_uncertain_pr_is_reconciled_before_binding_and_notification(self):
        self.incident(); self.controller.run_next(); job=self.controller.enqueue(7,'123')
        result={'diagnosis':diagnosis('fix','not_observed','confirmed'),'outcome':'patch','base_sha':BASE,
                'patch_path':'/offline','checks':[{'name':name,'passed':True} for name in ('fmt','clippy','tests')],'feedback':[]}
        self.state.update_job(job['id'],status='result',base_sha=BASE,result=result)
        original=self.gh.create_pr
        def create_then_disconnect(branch,title,body):
            receipt=original(branch,title,body)
            self.gh.items[7]['linked_prs']=[receipt['number']]
            raise Deferred('response lost after remote PR creation')
        self.gh.create_pr=create_then_disconnect
        self.controller.process_results()
        self.assertIsNone(self.state.job(job['id'])['pr_number'])
        self.assertEqual(self.gh.items[7]['linked_prs'],[8])
        self.now+=1000
        self.controller.process_results()
        current=self.state.job(job['id'])
        self.assertEqual(current['status'],'waiting_ci')
        self.assertEqual(current['pr_number'],8)
        self.assertEqual(sorted(self.gh.prs),[8])
        notifications=self.state.records('notifications')
        self.assertEqual([n['payload']['status'] for n in notifications],['pr_created'])
        self.assertEqual(notifications[0]['payload']['pr_number'],8)

    def test_own_uncertain_regression_issue_keeps_effect_body_stable_on_recovery(self):
        self.known_ready_incident()
        self.gh.prs[8].update(state='closed',merged_at='now',merge_commit_sha='b'*40)
        self.gh.items[7]['state']='closed'
        self.runner.ancestry=True; self.gh.fail_create=True
        self.repeat_known_incident()
        job=[j for j in self.state.jobs() if j['stage']=='initial'][-1]
        self.assertEqual(job['status'],'result')
        self.assertEqual(sorted(self.gh.items),[7,9])
        self.now+=1000; self.controller.process_results()
        current=self.state.job(job['id'])
        self.assertEqual(current['status'],'done')
        self.assertEqual(current['issue_number'],9)
        self.assertEqual(sorted(self.gh.items),[7,9])
        self.assertEqual(self.gh.items[9]['body'].count('Regression after deployed fix in PR #8'),1)

    def test_unresolved_other_origin_cannot_authorize_another_issue(self):
        self.state.record_origin('prior_uncertain',None,'signature',1)
        job=self.incident()
        for _ in range(3): self.controller.run_next(); self.now+=1000
        self.assertEqual(self.gh.created,0)
        self.assertEqual(self.runner.calls,0)
        self.assertEqual(self.state.job(job['id'])['status'],'needs_human')
    def test_new_repeat_after_no_fix_reassesses_and_updates_facts_without_another_deep_job(self):
        self.incident(); self.controller.run_next()
        deep=self.controller.enqueue(7,'123'); self.controller.run_next()
        self.assertEqual(self.state.job(deep['id'])['status'],'needs_human')
        self.assertEqual(self.state.incident('signature')['status'],'needs_human')
        self.controller.config['debounce_seconds']=60
        self.api.events.append({'id':101,'signature':'signature','first_seen':1,'last_seen':101,'snapshot':{'reason':'timeout'}})
        self.controller.poll_incidents(); self.controller.schedule_incidents()
        self.assertEqual(len(self.state.jobs()),2)
        self.now+=61; self.controller.schedule_incidents()
        fresh=[j for j in self.state.jobs({'queued'}) if j['stage']=='initial']
        self.assertEqual(len(fresh),1)
        self.runner.value=diagnosis(matches=[{'kind':'issue','number':7,'relationship':'same_cause','reason':'The same timeout recurred without a new causal fact.'}])
        self.controller.run_next()
        self.assertEqual(self.gh.created,1)
        self.assertEqual(len([j for j in self.state.jobs() if j['stage']=='deep']),1)
        self.assertEqual(self.state.job(deep['id'])['status'],'needs_human')
        self.assertEqual(len(self.gh.comment_values),1)
        self.assertIn('101 captured terminal events',next(iter(self.gh.comment_values.values()))['body'])

    def test_new_event_after_no_fix_can_start_deep_only_with_material_new_evidence(self):
        self.incident(); self.controller.run_next(); old=self.controller.enqueue(7,'123'); self.controller.run_next()
        self.api.events.append({'id':101,'signature':'signature','first_seen':1,'last_seen':101,'snapshot':{'reason':'timeout'}})
        self.controller.poll_incidents(); self.controller.schedule_incidents()
        self.runner.value=diagnosis('fix','not_observed','confirmed',[{'kind':'issue','number':7,'relationship':'new_evidence','reason':'A new reproducer proves the previously unknown lock inversion.'}])
        self.runner.value['supporting']=['A bounded reproducer now proves the lock inversion.']
        self.controller.run_next()
        deep=[j for j in self.state.jobs() if j['stage']=='deep']
        self.assertEqual(len(deep),2)
        self.assertEqual(self.state.job(old['id'])['status'],'needs_human')
        self.assertEqual(len([j for j in deep if j['status']=='queued']),1)
        self.assertEqual(self.gh.created,1)

    def test_published_patch_keeps_unchanged_review_handled_at_new_head(self):
        github=ReviewGitHub(); github.resolved=False; github.issue=lambda number: issue()
        artifact=github.review_snapshot(8)['artifacts'][0]
        prepared={'sha':'b'*40,'branch':'fix/issue-7','directory':'/offline'}
        github.prepare_patch=lambda *args: prepared
        github.validate_prepared=lambda *args: None
        github.remote_sha=lambda branch: github.head
        def push(prepared,previous=None): github.head=prepared['sha']; return {'sha':github.head}
        github.push=push
        config={**self.controller.config,'required_checks':['Rust workspace']}
        controller=Controller(config,self.state,self.api,github,self.runner)
        result={'diagnosis':diagnosis('fix','not_observed','confirmed'),'outcome':'patch','base_sha':BASE,'patch_path':'/offline',
            'checks':[{'name':name,'passed':True} for name in ('fmt','clippy','tests')],
            'feedback':[{'kind':'thread','id':'THREAD_1','action':'fixed','body':'The reproduced lock inversion is fixed and checked.'}]}
        job=self.state.new_job('revise','sig',1,issue_number=7,pr_number=8,base_sha=BASE,published_sha=BASE,
            branch='fix/issue-7',rounds=4,feedback=[artifact],result=result,status='result')
        controller.process_results(); controller.poll_reviews()
        current=self.state.job(job['id'])
        self.assertEqual(current['published_sha'],'b'*40)
        self.assertEqual(current['status'],'ready')
        self.assertEqual(current['rounds'],4)
        self.assertEqual(github.reply_calls,1); self.assertEqual(github.resolve_calls,1)
        github.external_body='The new edge case still deadlocks despite that change.'
        self.now+=61; controller.poll_reviews()
        self.assertEqual(self.state.job(job['id'])['status'],'queued')
        self.assertEqual(self.state.job(job['id'])['rounds'],5)

    def test_review_quota_wait_preserves_rounds_and_failed_resumption_requests_human(self):
        self.gh.items[7] = issue()
        execution = {'state': 'quota_wait', 'check_id': 19, 'head_sha': BASE}
        self.gh.review_snapshot = lambda _: {'head': BASE, 'pr': {'state': 'open'},
            'review_execution': execution, 'checks': [], 'threads': [], 'artifacts': []}
        job = self.state.new_job('revise', 'sig', 1, issue_number=7, pr_number=8,
            published_sha=BASE, status='waiting_ci', rounds=3)
        self.controller.poll_reviews()
        self.assertEqual(self.state.job(job['id'])['status'], 'waiting_ci')
        self.assertEqual(self.state.job(job['id'])['rounds'], 3)
        self.state.set_setting('review_waits', {'8': {'phase': 'needs_human', 'receipt': execution}})
        self.now += 61
        self.controller.poll_reviews()
        self.assertEqual(self.state.job(job['id'])['status'], 'needs_human')
        self.assertEqual(self.state.job(job['id'])['rounds'], 3)
        self.assertEqual([n['payload']['status'] for n in self.state.records('notifications')], ['paused', 'needs_human'])

    def test_diagnostic_fix_acknowledges_previous_check_after_push_then_reviews_new_head(self):
        github=self.gh; github.items[7]=issue(); github.prs[8]={'state':'open','head':{'sha':BASE}}
        artifact={'kind':'comment','id':'check_2','body':'Old finding','hash':'old','head':BASE}
        def snapshot(number):
            head=github.prs[number]['head']['sha']
            return {'head':head,'artifacts':[artifact] if head==BASE else [],'threads':[],'checks':[],'pr':github.prs[number]}
        github.review_snapshot=snapshot
        github.previous_diagnostic=lambda reference,head: artifact if (reference,head)==('check_2',BASE) else None
        original_push=github.push
        def push(prepared,previous=None):
            result=original_push(prepared,previous); github.prs[8]['head']['sha']=prepared['sha']; return result
        github.push=push
        result={'diagnosis':diagnosis('fix','not_observed','confirmed'),'outcome':'patch','base_sha':BASE,'patch_path':'/offline',
            'checks':[{'name':name,'passed':True} for name in ('fmt','clippy','tests')],
            'feedback':[{'kind':'comment','id':'check_2','action':'fixed','body':'The failing case is fixed and tested.'}]}
        job=self.state.new_job('revise','sig',1,issue_number=7,pr_number=8,base_sha=BASE,published_sha=BASE,
            branch='fix/issue-7',feedback=[artifact],result=result,status='result')
        self.controller.process_results()
        current=self.state.job(job['id'])
        self.assertEqual(current['status'],'waiting_ci')
        self.assertEqual(current['published_sha'],'b'*40)
        self.assertEqual(current['handled']['comment:check_2'],{'hash':'old','head':'b'*40})
        self.assertIn('Fixed in '+'b'*40,next(iter(github.comment_values.values()))['body'])


    def test_feedback_does_not_resolve_if_body_or_head_changes_after_reply(self):
        for changed in ('body','head'):
            with self.subTest(changed=changed):
                github=ReviewGitHub(); github.resolved=False
                artifact=github.review_snapshot(8)['artifacts'][0]
                controller=Controller(self.controller.config,self.state,self.api,github,self.runner)
                job=self.state.new_job('revise','sig',1,issue_number=7,pr_number=8,base_sha=BASE,published_sha=BASE,
                    feedback=[artifact],result={'outcome':'no_fix','feedback':[{'kind':'thread','id':'THREAD_1','action':'rebuttal','body':'The reproduction disproves this finding.'}]})
                original=github.reply_thread
                def concurrent_change(thread_id,body):
                    receipt=original(thread_id,body)
                    if changed=='body': github.external_body='New evidence changes the finding.'
                    else: github.head='c'*40
                    return receipt
                github.reply_thread=concurrent_change
                with self.assertRaises(InvalidResult): controller.handle_feedback(job)
                self.assertEqual(github.reply_calls,1)
                self.assertEqual(github.resolve_calls,0)
                self.assertFalse(self.state.job(job['id']).get('handled'))

    def test_feedback_rejects_concurrent_head_change_even_when_body_matches(self):
        github=ReviewGitHub(); artifact=github.review_snapshot(8)['artifacts'][0]
        controller=Controller(self.controller.config,self.state,self.api,github,self.runner)
        job=self.state.new_job('revise','sig',1,issue_number=7,pr_number=8,base_sha=BASE,published_sha=BASE,
            feedback=[artifact],result={'outcome':'no_fix','feedback':[{'kind':'thread','id':'THREAD_1','action':'rebuttal','body':'The original interleaving was disproved.'}]})
        github.head='c'*40
        with self.assertRaises(InvalidResult): controller.handle_feedback(job)
        self.assertEqual(github.reply_calls,0); self.assertEqual(github.resolve_calls,0)

    def test_new_event_recovers_after_initial_dependency_failure_before_issue_creation(self):
        failed=self.incident(); self.runner.failure=True
        for _ in range(3): self.controller.run_next(); self.now+=1000
        self.assertEqual(self.state.job(failed['id'])['status'],'needs_human')
        self.assertEqual(self.gh.created,0)
        self.api.events.append({'id':101,'signature':'signature','first_seen':1,'last_seen':101,'snapshot':{'reason':'timeout'}})
        self.controller.poll_incidents(); self.controller.schedule_incidents()
        self.runner.failure=False
        self.state.set_enabled(False); self.controller.run_next()
        self.assertEqual(self.gh.created,0)
        self.state.set_enabled(True); self.controller.run_next()
        self.assertEqual(self.gh.created,1)
        self.assertEqual(self.state.job(failed['id'])['status'],'needs_human')
        self.assertEqual(len([j for j in self.state.jobs() if j['stage']=='deep']),0)

    def test_new_event_after_cancellation_creates_new_initial_job_without_resurrection(self):
        cancelled=self.incident(); self.state.cancel(cancelled['id'])
        self.controller.poll_incidents(); self.controller.schedule_incidents()
        self.assertEqual(len(self.state.jobs()),1)
        self.api.events.append({'id':101,'signature':'signature','first_seen':1,'last_seen':101,'snapshot':{'reason':'timeout'}})
        self.controller.poll_incidents(); self.controller.schedule_incidents()
        fresh=[j for j in self.state.jobs({'queued'})]
        self.assertEqual(len(fresh),1)
        self.assertNotEqual(fresh[0]['id'],cancelled['id'])
        self.assertEqual(fresh[0]['stage'],'initial')
        self.controller.run_next()
        self.assertTrue(self.state.cancelled(cancelled['id']))
        self.assertEqual(self.state.job(cancelled['id'])['status'],'cancelled')
        self.assertEqual(self.gh.created,1)

    def test_repeated_quota_exhaustion_defers_without_dependency_retries_or_lost_usage(self):
        job=self.incident(); self.state.update_job(job['id'],attempts=2)
        def quota(job,context):
            self.runner.calls+=1
            raise QuotaUnavailable(usage={'tokens':7},active_seconds=3,retry_after_seconds=900)
        self.runner.run=quota
        for _ in range(4):
            self.controller.run_next()
            current=self.state.job(job['id'])
            self.assertEqual(current['status'],'queued')
            self.assertEqual(current['attempts'],2)
            self.assertEqual(current['next_at'],self.now+900)
            self.now+=901
        self.assertEqual(current['active_seconds'],12)
        self.assertEqual(current['usage'],{'tokens':28})
        self.assertEqual(self.state.status()['starts']['initial'],1)
        self.state.set_enabled(False); self.controller.run_next(); self.assertEqual(self.runner.calls,4)
        self.state.set_enabled(True); self.state.cancel(job['id']); self.controller.run_next(); self.assertEqual(self.runner.calls,4)

    def test_ordinary_retry_after_quota_recovery_counts_as_another_short_launch(self):
        job = self.incident()
        self.runner.run = lambda *_: (_ for _ in ()).throw(QuotaUnavailable(retry_after_seconds=120))
        self.controller.run_next(); self.now += 121
        self.runner.run = lambda *_: (_ for _ in ()).throw(Deferred('ordinary dependency failure'))
        self.controller.run_next()
        self.assertFalse(self.state.job(job['id']).get('quota_resume'))
        self.assertEqual(self.state.status()['starts']['initial'], 1)
        self.now += 31
        self.controller.run_next()
        self.assertEqual(self.state.status()['starts']['initial'], 2)

    def test_quota_blocks_other_jobs_and_restart_does_not_lose_cooldown(self):
        job = self.incident()
        self.runner.run = lambda *_: (_ for _ in ()).throw(QuotaUnavailable(retry_after_seconds=120))
        self.controller.run_next()
        second = self.state.new_job('initial', 'other', 102)
        self.state.recover()
        self.assertIsNone(self.controller.prepare_run())
        self.assertEqual(self.state.job(second['id'])['status'], 'queued')
        self.assertEqual(self.state.status()['starts']['initial'], 1)
        self.now += 121
        self.assertEqual(self.controller.prepare_run()['id'], job['id'])

    def test_service_future_quota_backoff_survives_recovery(self):
        job=self.state.new_job('initial','sig',1,attempts=2)
        claimed=self.state.claim(job['id'])
        future=concurrent.futures.Future()
        future.set_exception(QuotaUnavailable(usage={'tokens':9},active_seconds=2,retry_after_seconds=120))
        self.controller.future=future; self.controller.future_job=claimed
        self.controller.complete_future(); self.state.recover()
        current=self.state.job(job['id'])
        self.assertEqual(current['status'],'queued')
        self.assertEqual(current['next_at'],self.now+120)
        self.assertEqual(current['attempts'],2)
        self.assertEqual(current['usage'],{'tokens':9})
        self.assertEqual(current['active_seconds'],2)

    def test_service_future_quota_deferral_honors_cancel_and_active_budget(self):
        for cancelled in (False,True):
            self.state.provider_recovered()
            job=self.state.new_job('deep','sig',1,issue_number=7,active_seconds=14398)
            self.gh.items[7]=issue()
            # Use distinct issue budgets for the second case.
            if cancelled: self.state.update_job(job['id'],issue_number=8,active_seconds=0)
            claimed=self.state.claim(job['id'])
            if cancelled: self.state.cancel(job['id'])
            future=concurrent.futures.Future()
            future.set_exception(QuotaUnavailable(usage={'tokens':5},active_seconds=5))
            self.controller.future=future; self.controller.future_job=claimed
            self.controller.complete_future()
            current=self.state.job(job['id'])
            self.assertEqual(current['status'],'cancelled' if cancelled else 'needs_human')
            self.assertEqual(current['usage'],{'tokens':5})
            self.assertIsNone(self.controller.future)

    def test_generic_dependency_failures_retain_usage_in_sync_and_future_paths(self):
        for asynchronous in (False,True):
            job=self.state.new_job('initial','sig',1)
            error=Deferred('provider failure'); error.usage={'tokens':11}; error.active_seconds=4
            if asynchronous:
                claimed=self.state.claim(job['id']); future=concurrent.futures.Future(); future.set_exception(error)
                self.controller.future=future; self.controller.future_job=claimed
                self.controller.complete_future()
            else:
                def failed(job,context): raise error
                self.runner.run=failed; self.controller.run_next()
            current=self.state.job(job['id'])
            self.assertEqual(current['usage'],{'tokens':11})
            self.assertEqual(current['active_seconds'],4)
            self.assertEqual(current['attempts'],1)

    def test_graphql_reply_error_never_resolves_and_ambiguous_creation_reconciles(self):
        github=ReviewGitHub(); github.resolved=False; github.reply_error=True
        controller=Controller(self.controller.config,self.state,self.api,github,self.runner)
        # Snapshot comes from the real GitHub adapter, including the external thread body.
        artifact=github.review_snapshot(8)['artifacts'][0]
        job=self.state.new_job('revise','sig',1,issue_number=7,pr_number=8,base_sha=BASE,published_sha=BASE,
            feedback=[artifact],result={'diagnosis':diagnosis(),'outcome':'no_fix','feedback':[{'kind':'thread','id':'THREAD_1','action':'rebuttal','body':'The tested lock ordering excludes this interleaving.'}]})
        with self.assertRaises(Deferred): controller.handle_feedback(job)
        self.assertEqual(github.resolve_calls,0)
        self.assertFalse(self.state.job(job['id']).get('handled'))
        self.assertEqual(self.state.records('effects')[0]['state'],'uncertain')
        # A separately simulated HTTP-200 error can follow an actual remote creation.
        self.state.db.execute('DELETE FROM effects')
        github.persist_ambiguous=True
        with self.assertRaises(Deferred): controller.handle_feedback(job)
        self.assertEqual(github.resolve_calls,0)
        github.reply_error=False
        controller.handle_feedback(job)
        self.assertEqual(len(github.replies),1)
        self.assertEqual(github.reply_calls,2)
        self.assertEqual(github.resolve_calls,1)
        latest=github.review_snapshot(8)
        self.assertTrue(review_ready(latest,['Rust workspace'],self.state.job(job['id'])['handled']))

    def test_deep_and_review_context_pin_exact_target_and_current_acceptance_outside_shortlist(self):
        target={**issue(),'number':99,'kind':'issue','title':'Specific target','body':'Current owner requirements.\n\n## Acceptance\n\n- Cancellation survives concurrent result writes.\n\n## Impact\n\nOne worker.'}
        self.gh.items[99]=target
        self.gh.index=lambda: [{'kind':'issue','number':n,'title':'queue timeout','body':'Older investigation'} for n in range(1,31)]
        for stage in ('deep','revise'):
            job=self.state.new_job(stage,'sig',1,issue_number=99)
            context=self.controller.context(job)
            self.assertNotIn(99,[i['number'] for i in context['history']])
            self.assertEqual(context['target_issue']['number'],99)
            self.assertEqual(context['target_issue']['body'],target['body'])
            self.assertEqual(context['target_issue']['acceptance_criteria'],'- Cancellation survives concurrent result writes.')

    def test_cli_status_pause_and_cancel_use_only_durable_state(self):
        config=Path(self.temp.name)/'config.json'; config.write_text(json.dumps({'state_dir':self.temp.name}))
        job=self.state.new_job('initial','sig',1)
        for args in (['disable'],['status'],['cancel',job['id']]):
            output=io.StringIO()
            with contextlib.redirect_stdout(output): result=main(['--config',str(config),*args])
            self.assertEqual(result,0); self.assertIsInstance(json.loads(output.getvalue()),dict)
        self.assertFalse(self.state.enabled()); self.assertTrue(self.state.cancelled(job['id']))

    def test_hundred_events_create_one_issue_and_duplicate_dispatch_one_deep_job(self):
        self.incident(); self.controller.run_next()
        self.assertEqual(self.gh.created,1)
        a = self.controller.enqueue(7,'123'); b = self.controller.enqueue(7,'123')
        self.assertEqual(a['id'],b['id'])
        self.assertEqual(len(self.state.jobs()),2)
    def test_issue_creation_crash_reconciles_without_duplicate(self):
        self.incident(); self.gh.fail_create = True; self.controller.run_next()
        self.now += 1000; self.controller.process_results()
        self.assertEqual(self.gh.created,1)
        self.assertIsNotNone(self.state.origin(7))
    def test_external_only_observes_but_mixed_opens_issue(self):
        job = self.incident(); self.runner.value = diagnosis('observe','confirmed','not_observed'); self.controller.run_next()
        self.assertEqual(self.gh.created,0); self.assertEqual(self.state.job(job['id'])['status'],'observing')
        self.runner.value = diagnosis('fix','confirmed','confirmed'); self.state.new_job('initial','other',1); self.controller.run_next()
        self.assertEqual(self.gh.created,1)
    def test_secret_output_fails_closed_without_issue(self):
        job = self.incident(); self.runner.value['summary'] = 'api_key=secret_canary'; self.controller.run_next()
        self.assertEqual(self.gh.created,0); self.assertEqual(self.state.job(job['id'])['status'],'needs_human')
        self.assertNotIn('result',self.state.job(job['id']))
    def test_no_fix_leaves_issue_open_and_notifies_actual_receipt(self):
        self.incident(); self.controller.run_next(); deep = self.controller.enqueue(7,'123'); self.controller.run_next()
        self.assertEqual(self.gh.issue(7)['state'],'open'); self.assertEqual(self.state.job(deep['id'])['status'],'needs_human')
        self.controller.poll_notifications(); self.assertEqual(self.state.status()['pending_notifications'],1)
        self.api.delivery = 'sent'; self.controller.poll_notifications(); self.assertEqual(self.state.status()['pending_notifications'],0)
    def test_pause_cancel_and_provider_failures_are_bounded(self):
        job = self.incident(); self.state.set_enabled(False); self.controller.run_next(); self.assertEqual(self.runner.calls,0)
        self.state.set_enabled(True); self.runner.failure=True
        for _ in range(4): self.controller.run_next(); self.now+=10000
        self.assertEqual(self.runner.calls,3); self.assertEqual(self.state.job(job['id'])['status'],'needs_human')
        job = self.state.new_job('deep','x',1,issue_number=7); self.state.cancel(job['id']); self.controller.run_next(); self.assertEqual(self.runner.calls,3)
    def test_material_new_evidence_closed_issue_gets_linked_followup(self):
        self.gh.items[6]={**issue(),'number':6,'state':'closed','kind':'issue','title':'queue timeout','body':'Prior inconclusive investigation'}
        self.incident(); self.runner.value=diagnosis('fix','not_observed','confirmed',[{'kind':'issue','number':6,'relationship':'new_evidence','reason':'A reproducible lock inversion now identifies the previously unknown owner.'}])
        self.controller.run_next()
        self.assertEqual(self.gh.created,1); self.assertIn('closed issue #6',self.gh.issue(7)['body']); self.assertEqual(self.gh.issue(6)['state'],'closed')

    def test_material_new_evidence_open_origin_requeues_once(self):
        self.gh.items[7]={**issue(),'kind':'issue','title':'queue timeout','body':'Inconclusive'}
        self.state.record_origin('prior',7,'signature',1)
        self.incident(); self.runner.value=diagnosis('fix','not_observed','confirmed',[{'kind':'issue','number':7,'relationship':'new_evidence','reason':'The new reproducer proves a previously unknown inversion.'}])
        self.controller.run_next(); jobs=[j for j in self.state.jobs() if j['stage']=='deep']
        self.assertEqual(len(jobs),1); self.assertEqual(self.gh.created,0)

    def test_arbitrary_semantic_reference_rejected(self):
        job=self.incident(); self.runner.value['matches']=[{'kind':'issue','number':999,'relationship':'same_cause','reason':'same'}]; self.controller.run_next()
        self.assertEqual(self.state.job(job['id'])['status'],'needs_human'); self.assertEqual(self.gh.created,0)
    def test_patch_push_and_pr_crash_recovery(self):
        self.incident(); self.controller.run_next(); job=self.controller.enqueue(7,'123')
        value={'diagnosis':diagnosis('fix','not_observed','confirmed'),'outcome':'patch','base_sha':BASE,'patch_path':'/local','checks':[{'name':n,'passed':True} for n in ('fmt','clippy','tests')],'feedback':[]}
        self.state.update_job(job['id'],base_sha=BASE,result=value,status='result'); self.gh.fail_push=True
        self.controller.process_results(); self.now+=1000; self.gh.fail_pr=True; self.controller.process_results(); self.now+=1000; self.controller.process_results()
        self.assertEqual(len(self.gh.prs),1); self.assertEqual(self.state.job(job['id'])['status'],'waiting_ci')
    def test_merged_fix_ancestry_unknown_cannot_be_regression(self):
        self.gh.prs[9]={'kind':'pr','number':9,'title':'queue timeout','body':'','state':'closed','merged_at':'now','merge_commit_sha':'b'*40}
        job=self.incident(); self.runner.value['matches']=[{'kind':'pr','number':9,'relationship':'same_cause','reason':'same'}]; self.controller.run_next()
        self.assertEqual(self.gh.created,0); self.assertEqual(self.state.job(job['id'])['status'],'needs_human')
    def test_resource_preflight_does_not_charge_start_quota(self):
        job=self.incident()
        def unavailable(): raise Deferred('resources unavailable')
        self.runner.preflight=unavailable
        self.controller.run_next()
        self.assertEqual(self.runner.calls,0)
        self.assertEqual(self.state.status()['starts']['initial'],0)
        self.assertEqual(self.state.job(job['id'])['status'],'queued')

    def test_successful_pr_agent_check_requires_execution_proof(self):
        snapshot={'head':BASE,'checks':[{'name':'PR-Agent review and suggestions','head_sha':BASE,'status':'completed','conclusion':'success'}], 'statuses':[], 'artifacts':[], 'threads':[]}
        self.assertFalse(review_ready(snapshot,['PR-Agent review and suggestions'],{}))
        snapshot['review_completed']=True
        self.assertTrue(review_ready(snapshot,['PR-Agent review and suggestions'],{}))

    def test_neutral_semgrep_report_requires_handling_without_weakening_required_checks(self):
        required={'id':1,'name':'Semgrep CE','head_sha':BASE,'status':'completed','conclusion':'success'}
        report={'id':2,'name':'semgrep','app':{'id':15368,'slug':'github-actions'},
                'head_sha':BASE,'status':'completed','conclusion':'neutral'}
        artifact={'kind':'comment','id':'check_2','hash':'findings'}
        snapshot={'head':BASE,'checks':[required,report],'statuses':[], 'threads':[], 'artifacts':[artifact]}
        handled={'comment:check_2':{'hash':'findings','head':BASE}}
        self.assertFalse(review_ready(snapshot,['Semgrep CE'],{}))
        self.assertTrue(review_ready(snapshot,['Semgrep CE'],handled))
        for fields in ({'conclusion':'failure'},{'status':'in_progress'},{'head_sha':'b'*40},
                       {'name':'Unknown report'},{'app':{'id':123,'slug':'github-actions'}}):
            snapshot['checks']=[required,{**report,**fields}]
            self.assertFalse(review_ready(snapshot,['Semgrep CE'],handled))
        snapshot['checks']=[required,report]
        self.assertFalse(review_ready(snapshot,['Semgrep CE','semgrep'],handled))
        snapshot['checks']=[{**required,'conclusion':'neutral'},report]
        self.assertFalse(review_ready(snapshot,['Semgrep CE'],handled))

    def test_context_failure_after_previous_attempt_is_bounded(self):
        job=self.incident(); self.state.update_job(job['id'],base_sha=BASE)
        def unavailable(): raise Deferred('index unavailable')
        self.gh.index=unavailable
        for _ in range(4): self.controller.run_next(); self.now+=1000
        self.assertEqual(self.state.job(job['id'])['status'],'needs_human')

    def test_absent_merged_fix_waits_but_present_fix_opens_regression(self):
        self.gh.prs[9]={'kind':'pr','number':9,'title':'queue timeout','body':'','state':'closed','merged_at':'now','merge_commit_sha':'b'*40}
        job=self.incident(); self.runner.value['matches']=[{'kind':'pr','number':9,'relationship':'same_cause','reason':'same'}]
        self.runner.ancestry=False; self.controller.run_next()
        self.assertEqual(self.state.job(job['id'])['status'],'wait_deploy'); self.assertEqual(self.gh.created,0)
        self.runner.ancestry=True; self.state.new_job('initial','signature',100); self.controller.run_next()
        self.assertEqual(self.gh.created,1); self.assertIn('Regression after deployed fix',self.gh.issue(10)['body'])

    def test_same_open_or_closed_issue_updates_facts_without_new_issue(self):
        self.gh.items[7]={**issue(),'kind':'issue','title':'queue timeout','body':'known'}
        job=self.incident(); self.runner.value['matches']=[{'kind':'issue','number':7,'relationship':'same_cause','reason':'same'}]
        self.controller.run_next(); self.assertEqual(self.gh.created,0); self.assertEqual(len(self.gh.comment_values),1)
        self.gh.items[7]['state']='closed'; self.state.new_job('initial','signature',100); self.controller.run_next()
        self.assertEqual(self.gh.created,0); self.assertEqual(len(self.gh.comment_values),1)

    def test_unconfirmed_create_never_retries_a_mutation(self):
        job=self.incident()
        def uncertain(*args): self.gh.created+=1; raise Deferred('unknown network outcome')
        self.gh.create_issue=uncertain
        for _ in range(5): self.controller.run_next(); self.now+=1000
        self.assertEqual(self.gh.created,1); self.assertEqual(self.state.job(job['id'])['status'],'needs_human')

    def test_no_patch_check_receipt_prevents_publication(self):
        self.incident(); self.controller.run_next(); job=self.controller.enqueue(7,'123')
        result={'diagnosis':diagnosis('fix','not_observed','confirmed'),'outcome':'patch','checks':[],'base_sha':BASE,'feedback':[]}
        self.state.update_job(job['id'],status='result',base_sha=BASE,result=result); self.controller.process_results()
        self.assertEqual(self.gh.prs,{}); self.assertEqual(self.state.job(job['id'])['status'],'needs_human')

    def test_ambiguous_notification_is_never_replaced(self):
        job=self.incident(); self.controller.needs_human(job,'test'); self.api.delivery='ambiguous'
        self.controller.poll_notifications(); self.controller.poll_notifications()
        self.assertEqual(len(self.api.receipts),1); self.assertEqual(self.state.records('notifications')[0]['state'],'ambiguous')

    def test_review_requires_exact_head_checks_and_rechecks_edited_body(self):
        snapshot={'head':BASE,'checks':[{'name':'Rust workspace','head_sha':BASE,'status':'completed','conclusion':'success'}], 'statuses':[], 'artifacts':[{'kind':'comment','id':'1','body':'finding','head':BASE,'hash':'old'}], 'threads':[]}
        self.assertFalse(review_ready(snapshot, ['Rust workspace'],{}))
        self.assertTrue(review_ready(snapshot, ['Rust workspace'],{'comment:1':{'hash':'old','head':BASE}}))
        snapshot['artifacts'][0]['hash']='edited'; self.assertFalse(review_ready(snapshot,['Rust workspace'],{'comment:1':{'hash':'old','head':BASE}}))
        snapshot['artifacts']=[]; snapshot['head']='b'*40; self.assertFalse(review_ready(snapshot,['Rust workspace'],{}))

    def test_public_issue_body_contains_functional_sections_without_private_inventory(self):
        value = diagnosis('fix', 'not_observed', 'confirmed')
        value.update(
            title='Timeout on glm-private-27 at gpu-01.internal.invalid',
            summary='Requests fail while provider-alpha.invalid serves model glm-private-27 at https://gpu-01.internal.invalid:8443.',
            observations=['worker_id=worker-7f91c2 observed a queue timeout.'],
            acceptance=['The request path recovers without provider-alpha.invalid or model glm-private-27.'],
        )
        job = {
            'base_sha': BASE,
            'signature': 'synthetic-signature',
            'context': {
                'deployed': {'revision': 'd' * 40, 'host': 'gpu-01.internal.invalid'},
                'private': {'identifiers': ['provider-alpha.invalid', 'glm-private-27', 'gpu-01.internal.invalid', 'worker-7f91c2']},
            },
        }
        body = issue_body(value, job, '<!-- maintenance:origin:opaque -->')
        self.assertIn('## Problem', body)
        self.assertIn('## Impact', body)
        self.assertIn('## Acceptance', body)
        self.assertNotIn('Versions:', body)
        self.assertNotIn('Independent causes:', body)
        for secret in ('provider-alpha.invalid', 'glm-private-27', 'gpu-01.internal.invalid', 'worker-7f91c2', 'd' * 40):
            self.assertNotIn(secret, body)
        self.assertIn('Requests fail', body)
        self.assertIn('maintenance:origin:opaque', body)

    def test_restart_hydrates_private_initial_diagnosis_without_global_inventory(self):
        self.incident()
        self.api.evidence = lambda _: {'route': {'provider': 'Acme-Gateway', 'model': 'model-27'}}
        self.runner.value['summary'] = 'Acme-Gateway stops before fallback.'
        self.controller.run_next()
        deep = self.controller.enqueue(7, '123')
        self.assertNotIn('Acme-Gateway', self.gh.items[7]['body'])
        self.api.evidence = lambda _: {'available': False}
        unrelated = self.state.new_job('initial', 'unrelated', 800, result={'diagnosis': diagnosis()})
        self.state.update_job(unrelated['id'], result={'diagnosis': {'summary': 'Unrelated confidential diagnosis'}})
        self.state.close()
        self.state = State(Path(self.temp.name)/'state.sqlite3', clock=lambda:self.now)
        self.addCleanup(self.state.close)
        config = {**self.controller.config, 'private_inventory': {'identifiers': ['unrelated-private-provider']}}
        controller = Controller(config, self.state, self.api, self.gh, self.runner)
        context = controller.context(self.state.job(deep['id']))
        self.assertEqual(context['private']['initial_diagnosis']['summary'], 'Acme-Gateway stops before fallback.')
        self.assertFalse(context['evidence']['available'])
        self.assertEqual(context['private']['initial_evidence']['evidence']['route']['model'], 'model-27')
        self.assertNotIn('Acme', issue_body(self.runner.value, {'context': context}, '<!-- maintenance:origin:opaque -->'))
        self.assertNotIn('unrelated-private-provider', json.dumps(context))
        self.assertNotIn('Unrelated confidential diagnosis', json.dumps(context))

    def test_recovered_prepared_commit_is_checked_before_push_or_pr(self):
        self.incident(); self.controller.run_next()
        deep = self.controller.enqueue(7, '123')
        prepared = {'sha': 'b'*40, 'branch': 'fix/issue-7', 'directory': '/offline'}
        value = diagnosis('fix', 'not_observed', 'confirmed')
        result = {'diagnosis': value, 'outcome': 'patch', 'feedback': [],
                  'checks': [{'name': name, 'passed': True} for name in ('fmt', 'clippy', 'tests')]}
        self.state.update_job(deep['id'], status='result', base_sha=BASE, prepared=prepared, result=result)
        checks = []
        def reject(job, commit):
            checks.append(commit['sha'])
            raise InvalidResult('publication contains private infrastructure identifiers')
        self.gh.validate_prepared = reject
        self.controller.process_results()
        self.assertEqual(checks, ['b'*40])
        self.assertEqual(self.gh.remote, {})
        self.assertEqual(self.gh.prs, {})
        self.assertEqual(self.state.job(deep['id'])['status'], 'needs_human')
        self.assertEqual(self.state.job(deep['id'])['prepared'], prepared)
