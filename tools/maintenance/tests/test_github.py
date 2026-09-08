import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from tools.maintenance.github import GitHub, validate_dispatch, select_candidates
from tools.maintenance.api import MaintenanceAPI
from tools.maintenance.contracts import InvalidResult, Deferred


def owner(): return {'login': 'iamwavecut', 'id': 239034}
def issue(): return {'number': 7, 'state': 'open', 'user': owner(), 'labels': [{'name': 'agent:created'}, {'name': 'agent:queued'}], 'repository_url': 'https://api.github.com/repos/iamwavecut/openplotva'}
def dispatch():
    repo = {'full_name': 'iamwavecut/openplotva', 'owner': owner(), 'fork': False}
    return {'id': 123, 'event': 'issues', 'path': '.github/workflows/incident-maintenance.yml', 'head_branch': 'main', 'repository': repo, 'head_repository': repo,
            'actor': owner(), 'triggering_actor': owner(), 'display_title': 'Incident maintenance issue #7'}


class GitHubTests(unittest.TestCase):
    def test_dispatch_gates(self):
        validate_dispatch(issue(), dispatch(), 7, '123')
        for field in ('actor', 'triggering_actor'):
            bad = dispatch(); bad[field] = {'login': 'iamwavecut', 'id': 1}
            with self.assertRaises(InvalidResult): validate_dispatch(issue(), bad, 7, '123')
        for change in ({'path': '.github/workflows/evil.yml'}, {'head_branch': 'fork'}, {'display_title': 'Incident maintenance issue #8'}):
            bad = dispatch(); bad.update(change)
            with self.assertRaises(InvalidResult): validate_dispatch(issue(), bad, 7, '123')
        bad = issue(); bad['labels'] = []
        with self.assertRaises(InvalidResult): validate_dispatch(bad, dispatch(), 7, '123')
        bad = issue(); bad['user'] = {'login': 'iamwavecut', 'id': 2}
        with self.assertRaises(InvalidResult): validate_dispatch(bad, dispatch(), 7, '123')

    def test_dispatch_generation_uses_owner_label_event_not_workflow_id(self):
        github=GitHub({})
        github.pages=lambda _: [
            {'id':11,'event':'labeled','actor':owner(),'label':{'name':'agent:queued'},'created_at':'2026-09-08T01:00:00Z'},
            {'id':12,'event':'labeled','actor':owner(),'label':{'name':'agent:queued'},'created_at':'2026-09-08T03:00:00Z'}]
        self.assertEqual(github.queue_generation(7,{'created_at':'2026-09-08T02:00:00Z'}),'label_11')
        self.assertEqual(github.queue_generation(7,{'created_at':'2026-09-08T04:00:00Z'}),'label_12')

    def test_candidate_search_is_deterministic_and_bounded(self):
        items = [{'kind': 'issue', 'number': n, 'title': 'queue timeout', 'body': 'locks'} for n in range(1,101)]
        self.assertEqual(select_candidates(items, {'reason': 'timeout'}, 3), items[:3])

    def test_publication_checks_patch_in_real_git_and_preserves_commit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); source = root/'source'; source.mkdir()
            def git(*args): return subprocess.check_output(['git', '-C', str(source), *args], stderr=subprocess.DEVNULL).decode().strip()
            git('init'); git('config', 'user.name', 'Owner'); git('config', 'user.email', 'owner@example.test')
            (source/'code.txt').write_text('old\n'); git('add', '.'); git('commit', '-m', 'base')
            base = git('rev-parse', 'HEAD'); (source/'code.txt').write_text('new\n')
            patch = root/'patch.diff'; patch.write_text(git('diff')+'\n'); git('checkout', '--', '.')
            publisher = GitHub({'state_dir': temp, 'source_dir': str(source), 'git_name': 'Owner', 'git_email': 'owner@example.test'})
            prepared = publisher.prepare_patch({'id': 'job', 'issue_number': 7, 'base_sha': base}, {'patch_path': str(patch), 'base_sha': base}, 0)
            self.assertEqual(len(prepared['sha']), 40)
            self.assertEqual(publisher.prepare_patch({'id': 'job', 'issue_number': 7, 'base_sha': base}, {'patch_path': str(patch), 'base_sha': base}, 0)['sha'], prepared['sha'])
            patch.write_text('diff --git a/.github/test b/.github/test\nnew file mode 100644\n--- /dev/null\n+++ b/.github/test\n@@ -0,0 +1 @@\n+bad\n')
            with self.assertRaises(InvalidResult): publisher.prepare_patch({'id': 'bad', 'issue_number': 7, 'base_sha': base}, {'patch_path': str(patch), 'base_sha': base}, 0)

    def test_api_transmits_only_identifiers_and_requires_confirmed_message(self):
        with tempfile.TemporaryDirectory() as temp:
            token=Path(temp)/'token'; token.write_text('fixture-private-token'); token.chmod(0o600)
            calls=[]
            def transport(request):
                calls.append(json.loads(request.data))
                return json.dumps({'key':'0'*64,'state':'sent','telegram_message_id':42}).encode()
            api=MaintenanceAPI({'maintenance_url':'https://example.test/internal/maintenance/v1','maintenance_token_file':str(token)},transport=transport)
            payload={'key':'0'*64,'run_id':'job','status':'pr_ready','pr_number':8,'issue_number':7}
            self.assertEqual(api.notify(payload)['telegram_message_id'],42); self.assertEqual(calls,[payload])
            with self.assertRaises(Deferred): api.receipt({'key':'0'*64,'state':'sent','telegram_message_id':None},'0'*64)

    def test_paginated_history_retains_closed_issues_and_merged_prs(self):
        github=GitHub({})
        def api(path):
            if '/issues?' in path:
                if 'page=2' in path: return [{'number':101,'state':'closed','body':'late'}]
                return [{'number':n,'state':'closed','body':'body'} for n in range(1,101)]
            return [{'number':102,'state':'closed','merged_at':'now'}, {'number':103,'state':'closed','merged_at':None}, {'number':104,'state':'open'}]
        github.api=api
        index=github.index()
        self.assertEqual(len(index),103)
        self.assertIn(101,{i['number'] for i in index}); self.assertIn(102,{i['number'] for i in index}); self.assertNotIn(103,{i['number'] for i in index})

    def test_api_enforces_tls_and_fixed_notification_contract(self):
        with self.assertRaises(ValueError): MaintenanceAPI({'maintenance_url': 'http://example.test'})
        with tempfile.TemporaryDirectory() as temp:
            token = Path(temp)/'token'; token.write_text('private'); token.chmod(0o600)
            api = MaintenanceAPI({'maintenance_url': 'https://example.test/internal/maintenance/v1', 'maintenance_token_file': str(token)})
            with self.assertRaises(InvalidResult): api.notify({'key': '0'*64, 'run_id': 'job', 'status': 'failed', 'text': 'secret'})


class ReviewGitHub(GitHub):
    """Offline transport fixture exercising the real snapshot and mutation adapters."""
    def __init__(self):
        super().__init__({})
        self.head='a'*40
        self.external_body='The lock order still permits deadlock.'
        self.resolved=True
        self.replies=[]
        self.reply_error=False
        self.persist_ambiguous=False
        self.resolve_calls=0
        self.reply_calls=0

    def api(self,path,method='GET',payload=None):
        if path=='user': return owner()
        if '/pulls/8/comments?' in path:
            return [{'id':11,'body':self.external_body,'user':{'login':'reviewer','id':2}},
                    *[{'id':r['databaseId'],'body':r['body'],'user':owner()} for r in self.replies]]
        if path.endswith('/pulls/8'): return {'number':8,'state':'open','head':{'sha':self.head,'ref':'fix/issue-7'},'draft':False}
        if '/check-runs?' in path: return {'check_runs':[{'id':1,'name':'Rust workspace','head_sha':self.head,'status':'completed','conclusion':'success'}]}
        if path=='graphql':
            query=payload['query']
            if 'addPullRequestReviewThreadReply' in query:
                self.reply_calls+=1
                if not self.reply_error or self.persist_ambiguous:
                    self.replies.append({'databaseId':12+len(self.replies),'id':'COMMENT_'+str(len(self.replies)), 'body':payload['variables']['body'], 'author':{'login':'iamwavecut','databaseId':239034}})
                if self.reply_error: return {'errors':[{'message':'Reply not permitted'}]}
                return {'data':{'addPullRequestReviewThreadReply':{'comment':{'id':self.replies[-1]['id']}}}}
            if 'resolveReviewThread' in query:
                self.resolve_calls+=1; self.resolved=True
                return {'data':{'resolveReviewThread':{'thread':{'id':'THREAD_1','isResolved':True}}}}
            return {'data':{'repository':{'pullRequest':{'reviewThreads':{'nodes':[{'id':'THREAD_1','isResolved':self.resolved,
                'comments':{'nodes':[{'databaseId':11,'body':self.external_body,'author':{'login':'reviewer','databaseId':2}},*self.replies],
                            'pageInfo':{'hasNextPage':False}}}], 'pageInfo':{'hasNextPage':False}}}}}}
        if '?' in path: return []
        raise AssertionError('unexpected offline request: '+path)


class ReviewSnapshotTests(unittest.TestCase):
    def test_resolved_thread_original_edit_invalidates_readiness_and_own_reply_does_not(self):
        from tools.maintenance.controller import review_ready
        github=ReviewGitHub()
        snapshot=github.review_snapshot(8)
        self.assertEqual(len(snapshot['artifacts']),1)
        artifact=snapshot['artifacts'][0]
        handled={'thread:THREAD_1':{'hash':artifact['hash'],'head':snapshot['head']}}
        self.assertTrue(review_ready(snapshot,['Rust workspace'],handled))
        github.replies.append({'databaseId':12,'body':'Explained. <!-- maintenance:feedback:fixture -->','author':{'login':'iamwavecut','databaseId':239034}})
        self.assertEqual(github.review_snapshot(8)['artifacts'][0]['hash'],artifact['hash'])
        github.external_body='The new interleaving still deadlocks after the claimed fix.'
        latest=github.review_snapshot(8)
        self.assertEqual(latest['artifacts'][0]['id'],artifact['id'])
        self.assertFalse(review_ready(latest,['Rust workspace'],handled))

    def test_graphql_errors_or_missing_reply_identity_do_not_confirm_reply(self):
        github=ReviewGitHub(); github.reply_error=True
        with self.assertRaises(Deferred): github.reply_thread('THREAD_1','Reasoned reply.')
        github.graphql=lambda *args,**kwargs: {'data':{'addPullRequestReviewThreadReply':{'comment':{}}}}
        with self.assertRaises(Deferred): github.reply_thread('THREAD_1','Reasoned reply.')
