"""Owner-authenticated GitHub adapter and safe local patch publisher."""
from __future__ import annotations
import datetime
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path

from .api import secret_file
from .contracts import Deferred, InvalidResult, OWNER, OWNER_ID, REPOSITORY, fingerprint, identifier, sha, text
from .privacy import PublicationPrivacy


def is_owner(value): return isinstance(value, dict) and value.get('login') == OWNER and value.get('id') == OWNER_ID


def validate_issue(value, number):
    if (value.get('number') != number or value.get('state') != 'open' or not is_owner(value.get('user'))
            or value.get('repository_url') != 'https://api.github.com/repos/'+REPOSITORY
            or not {'agent:created', 'agent:queued'} <= {label['name'] for label in value.get('labels', [])}):
        raise InvalidResult('issue no longer meets owner and label scope')


def validate_dispatch(issue, run, number, event_id):
    validate_issue(issue, number)
    for field in ('repository', 'head_repository'):
        repo = run.get(field) or {}
        if repo.get('full_name') != REPOSITORY or repo.get('fork') or not is_owner(repo.get('owner')):
            raise InvalidResult('workflow repository is outside scope')
    if (str(run.get('id')) != str(event_id) or run.get('event') != 'issues'
            or run.get('path') not in ('.github/workflows/incident-maintenance.yml', '.github/workflows/incident-maintenance.yml@refs/heads/main')
            or run.get('head_branch') != 'main' or not is_owner(run.get('actor'))
            or not is_owner(run.get('triggering_actor')) or run.get('display_title') != 'Incident maintenance issue #'+str(number)):
        raise InvalidResult('workflow dispatch is not the trusted owner issue event')


def select_candidates(items, incident, limit=20):
    tokens = set(re.findall(r'[a-z0-9_]{3,}', json.dumps(incident).lower()))
    def rank(item):
        words = set(re.findall(r'[a-z0-9_]{3,}', (item.get('title', '')+' '+(item.get('body') or '')).lower()))
        return (-len(words & tokens), item['kind'], item['number'])
    return sorted(items, key=rank)[:max(1, min(30, limit))]


class GitHub:
    def __init__(self, config, execute=None):
        self.config = config
        self.execute = execute
        # Snapshot private identifiers once per trusted controller process. The
        # inventory is never passed to the worker or written to GitHub.
        self.privacy = PublicationPrivacy.from_config(config)

    def command(self, args, *, data=None, authenticated=False, cwd=None):
        env = {'PATH': os.environ.get('PATH', '/usr/local/bin:/usr/bin:/bin'), 'HOME': str(Path(self.config['state_dir']).resolve()),
               'LANG': 'C.UTF-8', 'GIT_TERMINAL_PROMPT': '0', 'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': '/dev/null'}
        if authenticated: env['GH_TOKEN'] = secret_file(self.config['github_token_file'])
        try:
            result = (self.execute or subprocess.run)(args, input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=cwd, timeout=180)
            if result.returncode: raise Deferred('GitHub or git operation failed')
            return result.stdout
        except (OSError, subprocess.SubprocessError): raise Deferred('GitHub or git operation unavailable') from None

    def api(self, path, method='GET', payload=None):
        args = ['gh', 'api', '--method', method, path]
        data = None
        if payload is not None:
            args += ['--input', '-']; data = json.dumps(payload).encode()
        try: return json.loads(self.command(args, data=data, authenticated=True) or b'{}')
        except (ValueError, UnicodeError): raise Deferred('invalid GitHub response') from None

    def pages(self, path):
        results = []
        for page in range(1, 10001):
            values = self.api(path+('&' if '?' in path else '?')+'per_page=100&page='+str(page))
            if not isinstance(values, list): raise Deferred('invalid GitHub page')
            results.extend(values)
            if len(values) < 100: return results
        raise Deferred('GitHub index exceeds bounded pagination')

    def issue(self, number): return self.api('repos/'+REPOSITORY+'/issues/'+str(number))
    def pr(self, number): return self.api('repos/'+REPOSITORY+'/pulls/'+str(number))
    def run(self, event_id): return self.api('repos/'+REPOSITORY+'/actions/runs/'+str(event_id))

    def queue_generation(self, number, run):
        try: started=datetime.datetime.fromisoformat(run['created_at'].replace('Z','+00:00'))
        except (KeyError,ValueError): raise InvalidResult('workflow start timestamp unavailable') from None
        labels=[]
        for event in self.pages('repos/'+REPOSITORY+'/issues/'+str(number)+'/timeline'):
            if event.get('event')=='labeled' and event.get('label',{}).get('name')=='agent:queued' and is_owner(event.get('actor')):
                at=datetime.datetime.fromisoformat(event['created_at'].replace('Z','+00:00'))
                if at<=started: labels.append((at,event['id']))
        if not labels: raise Deferred('queued label generation is not yet available')
        return 'label_'+str(max(labels)[1])

    def index(self):
        issues = self.pages('repos/'+REPOSITORY+'/issues?state=all')
        pulls = self.pages('repos/'+REPOSITORY+'/pulls?state=all')
        return [{**i, 'kind': 'issue'} for i in issues if 'pull_request' not in i] + [
            {**p, 'kind': 'pr'} for p in pulls if p['state'] == 'open' or p.get('merged_at')]

    def discussion(self, item):
        number = item['number']
        comments = self.pages('repos/'+REPOSITORY+'/issues/'+str(number)+'/comments')
        linked = []
        for event in self.pages('repos/'+REPOSITORY+'/issues/'+str(number)+'/timeline'):
            source = event.get('source', {}).get('issue', {})
            pull_url=(source.get('pull_request') or {}).get('url','')
            if re.fullmatch(r'https://api.github.com/repos/iamwavecut/openplotva/pulls/[0-9]+',pull_url):
                linked.append(int(pull_url.rsplit('/',1)[-1]))
            elif source.get('repository_url') == 'https://api.github.com/repos/'+REPOSITORY and source.get('pull_request'):
                linked.append(source['number'])
        return {**item, 'comments': comments, 'linked_prs': sorted(set(linked))}

    def labels_ready(self):
        from .contracts import LABELS
        return set(LABELS) <= {v['name'] for v in self.pages('repos/'+REPOSITORY+'/labels')}

    def ensure_labels(self):
        self.assert_owner()
        from .contracts import LABELS
        known = {v['name'] for v in self.pages('repos/'+REPOSITORY+'/labels')}
        for name, color in LABELS.items():
            if name not in known: self.api('repos/'+REPOSITORY+'/labels', 'POST', {'name': name, 'color': color})
        return {}

    def create_issue(self, title, body, labels):
        self.assert_owner()
        return self.api('repos/'+REPOSITORY+'/issues', 'POST', {'title': self.privacy.assert_public(title, 180), 'body': self.privacy.assert_public(body, 60000), 'labels': labels})

    def assert_owner(self):
        if not is_owner(self.api('user')): raise InvalidResult('publishing credential is not the authorized owner')

    def find_issue(self, marker):
        items = [i for i in self.pages('repos/'+REPOSITORY+'/issues?state=all') if 'pull_request' not in i and marker in (i.get('body') or '') and is_owner(i.get('user'))]
        if len(items) > 1: raise InvalidResult('ambiguous issue provenance')
        return items[0] if items else None

    def comments(self, number): return self.pages('repos/'+REPOSITORY+'/issues/'+str(number)+'/comments')

    def find_comment(self, number, marker):
        values = [c for c in self.comments(number) if marker in (c.get('body') or '') and is_owner(c.get('user'))]
        if len(values) > 1: raise InvalidResult('ambiguous comment provenance')
        return values[0] if values else None

    def comment(self, number, body, comment_id=None):
        self.assert_owner()
        return self.api('repos/'+REPOSITORY+('/issues/comments/'+str(comment_id) if comment_id else '/issues/'+str(number)+'/comments'),
                        'PATCH' if comment_id else 'POST', {'body': self.privacy.assert_public(body, 60000)})

    def add_label(self, number, label):
        self.assert_owner()
        return self.api('repos/'+REPOSITORY+'/issues/'+str(number)+'/labels', 'POST', {'labels': [label]})

    def prepare_patch(self, job, result, round_number):
        from .runner import validate_patch
        patch_path = Path(result['patch_path'])
        if patch_path.is_symlink() or patch_path.stat().st_size > 2*1024*1024: raise InvalidResult('unsafe patch artifact')
        patch = patch_path.read_bytes()
        validate_patch(patch)
        # Check additions before creating a checkout or applying executable
        # changes, so a confidential patch fails closed at the publication
        # boundary and cannot affect a recovery attempt.
        self.privacy.with_context(job).assert_patch_public(patch)
        text(patch.decode(), 2*1024*1024)
        base = sha(result['base_sha'])
        if base != job['base_sha']: raise InvalidResult('patch starting revision mismatch')
        directory = Path(self.config['state_dir'])/'publications'/identifier(job['id'])/str(round_number)
        receipt = directory/'receipt.json'
        digest = fingerprint({'base': base, 'patch': hashlib.sha256(patch).hexdigest()})
        if receipt.exists():
            value = json.loads(receipt.read_text())
            if value['digest'] != digest: raise InvalidResult('publication artifact changed')
            return value
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        checkout = directory/'repo'
        if not checkout.exists(): self.command(['git', 'clone', '--no-hardlinks', '--no-checkout', self.config['source_dir'], str(checkout)])
        prefix = ['git', '-C', str(checkout), '-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgsign=false']
        # This checkout is exclusively task-owned and may be recreated after a crash.
        self.command(prefix+['reset', '--hard', base])
        self.command(prefix+['clean', '-fd'])
        self.command(prefix+['apply', '--index', '--whitespace=error', '-'], data=patch)
        self.command(prefix+['-c', 'user.name='+self.config['git_name'], '-c', 'user.email='+self.config['git_email'],
                             'commit', '-m', 'Fix incident reported in issue #'+str(job['issue_number'])])
        revision = sha(self.command(prefix+['rev-parse', 'HEAD']).decode().strip())
        value = {'sha': revision, 'directory': str(checkout), 'digest': digest,
                 'branch': job.get('branch') or 'fix/issue-'+str(job['issue_number'])+'-'+job['id'][:12]}
        temporary = receipt.with_suffix('.tmp'); temporary.write_text(json.dumps(value)); os.replace(temporary, receipt)
        return value

    def remote_sha(self, branch):
        identifier(branch.replace('/', '_'))
        raw = self.command(['git', '-c', 'credential.helper=', '-c', 'credential.helper=!gh auth git-credential',
                            'ls-remote', 'https://github.com/'+REPOSITORY+'.git', 'refs/heads/'+branch], authenticated=True)
        return sha(raw.decode().split()[0]) if raw.strip() else None

    def validate_prepared(self, job, prepared):
        boundary = self.privacy.with_context(job)
        prefix = ['git', '-C', prepared['directory'], '-c', 'core.hooksPath=/dev/null']
        revision = sha(prepared['sha'])
        base = sha(job['base_sha'])
        boundary.assert_public(prepared['branch'], 180)
        boundary.assert_patch_public(self.command(prefix+['diff', '--no-ext-diff', '--no-textconv', base, revision, '--']))
        boundary.assert_public(self.command(prefix+['log', '--format=%B', base+'..'+revision]).decode(), 60000)

    def push(self, prepared, previous=None):
        self.assert_owner()
        args = ['git', '-C', prepared['directory'], '-c', 'core.hooksPath=/dev/null', '-c', 'credential.helper=',
                '-c', 'credential.helper=!gh auth git-credential', 'push', 'https://github.com/'+REPOSITORY+'.git',
                sha(prepared['sha'])+':refs/heads/'+prepared['branch']]
        # No force push: concurrent owner updates are never overwritten.
        self.command(args, authenticated=True)
        if self.remote_sha(prepared['branch']) != prepared['sha']: raise Deferred('published branch revision is not confirmed')
        return {'sha': prepared['sha']}

    def find_pr(self, branch, marker):
        values = self.pages('repos/'+REPOSITORY+'/pulls?state=all&head='+OWNER+':'+branch+'&base=main')
        values = [p for p in values if p.get('head', {}).get('ref') == branch and p.get('base', {}).get('ref') == 'main'
                  and marker in (p.get('body') or '') and is_owner(p.get('user'))]
        if len(values) > 1: raise InvalidResult('ambiguous pull request provenance')
        return values[0] if values else None

    def create_pr(self, branch, title, body):
        self.assert_owner()
        return self.api('repos/'+REPOSITORY+'/pulls', 'POST', {'head': branch, 'base': 'main', 'title': self.privacy.assert_public(title, 180), 'body': self.privacy.assert_public(body, 60000), 'draft': False})

    def graphql(self, query, **variables):
        value=self.api('graphql', 'POST', {'query': query, 'variables': variables})
        if not isinstance(value,dict) or value.get('errors') or not isinstance(value.get('data'),dict):
            raise Deferred('GitHub GraphQL operation is not confirmed')
        return value

    def diagnostic_artifact(self, report):
        check_id=report['id']
        if type(check_id) is not int or check_id<=0: raise Deferred('invalid diagnostic check identity')
        output=report.get('output') or {}
        body='\n\n'.join(str(output.get(field) or '') for field in ('summary','text')).strip()
        if output.get('annotations_count'):
            annotations=self.pages('repos/'+REPOSITORY+'/check-runs/'+str(check_id)+'/annotations')
            if len(annotations)!=output['annotations_count']: raise Deferred('incomplete diagnostic annotations')
            body+='\n\n'+json.dumps([{key:a.get(key) for key in ('path','start_line','end_line','annotation_level','title','message','raw_details')} for a in annotations])
        artifact={'kind':'comment','id':'check_'+str(check_id),'body':body,
                  'head':report.get('head_sha'),'author':(report.get('app') or {}).get('slug','')}
        artifact['hash']=fingerprint({key:value for key,value in artifact.items() if key!='head'})
        return artifact

    def previous_diagnostic(self, reference, expected_head):
        match=re.fullmatch(r'check_([1-9][0-9]*)',reference)
        if not match: raise InvalidResult('invalid diagnostic reference')
        report=self.api('repos/'+REPOSITORY+'/check-runs/'+match[1])
        if (report.get('id')!=int(match[1]) or report.get('head_sha')!=sha(expected_head)
                or report.get('name')!='semgrep' or report.get('status')!='completed'
                or (report.get('app') or {}).get('id')!=15368
                or (report.get('app') or {}).get('slug')!='github-actions'):
            raise InvalidResult('diagnostic report provenance changed')
        return self.diagnostic_artifact(report)

    def review_snapshot(self, number):
        pr = self.pr(number); head = sha(pr['head']['sha'])
        checks = []
        for page in range(1, 1001):
            data = self.api('repos/'+REPOSITORY+'/commits/'+head+'/check-runs?per_page=100&page='+str(page))
            checks.extend(data['check_runs'])
            if len(data['check_runs']) < 100: break
        statuses = self.pages('repos/'+REPOSITORY+'/commits/'+head+'/statuses')
        comments = self.comments(number)
        reviews = self.pages('repos/'+REPOSITORY+'/pulls/'+str(number)+'/reviews')
        inline = self.pages('repos/'+REPOSITORY+'/pulls/'+str(number)+'/comments')
        threads = []; cursor = None
        while True:
            response = self.graphql('''query($owner:String!,$repo:String!,$number:Int!,$cursor:String){repository(owner:$owner,name:$repo){pullRequest(number:$number){reviewThreads(first:100,after:$cursor){nodes{id isResolved comments(first:100){nodes{databaseId body author{login ... on User{databaseId}}} pageInfo{hasNextPage endCursor}}} pageInfo{hasNextPage endCursor}}}}}''', owner=OWNER, repo='openplotva', number=number, cursor=cursor)
            page = response['data']['repository']['pullRequest']['reviewThreads']; threads.extend(page['nodes'])
            if not page['pageInfo']['hasNextPage']: break
            cursor = page['pageInfo']['endCursor']
        for thread in threads:
            while thread['comments'].get('pageInfo',{}).get('hasNextPage'):
                response=self.graphql('''query($id:ID!,$cursor:String){node(id:$id){... on PullRequestReviewThread{comments(first:100,after:$cursor){nodes{databaseId body author{login ... on User{databaseId}}} pageInfo{hasNextPage endCursor}}}}}''',
                    id=thread['id'],cursor=thread['comments']['pageInfo']['endCursor'])
                page=response['data']['node']['comments']
                thread['comments']['nodes'].extend(page['nodes'])
                thread['comments']['pageInfo']=page['pageInfo']
        artifacts = []
        # reviewdog's report is intentionally neutral; its full current findings
        # still require an explicit response before readiness can be confirmed.
        diagnostic_checks={c['name']:c for c in sorted(checks,key=lambda c:c.get('id',0))}
        report=diagnostic_checks.get('semgrep')
        if report and report.get('conclusion')=='neutral':
            artifacts.append(self.diagnostic_artifact(report))
        for comment in comments:
            if is_owner(comment.get('user')) and '<!-- maintenance:' in (comment.get('body') or ''): continue
            artifacts.append({'kind': 'comment', 'id': str(comment['id']), 'body': comment.get('body') or '', 'head': head, 'author': comment.get('user', {}).get('login', '')})
        for review in reviews:
            if review.get('body') or review.get('state') == 'CHANGES_REQUESTED':
                artifacts.append({'kind': 'comment', 'id': 'review_'+str(review['id']), 'body': review.get('body') or 'Review requests changes.',
                                  'head': review.get('commit_id'), 'author': review.get('user', {}).get('login', '')})
        inline_by_id={comment['id']:comment for comment in inline}
        for thread in threads:
            external=[]
            for comment in thread['comments']['nodes']:
                author=comment.get('author') or {}
                actor={'login':author.get('login'),'id':author.get('databaseId')} if author else inline_by_id.get(comment['databaseId'],{}).get('user')
                if is_owner(actor) and '<!-- maintenance:' in comment['body']: continue
                external.append(comment['body'])
            # Resolution is independent of content: edits to resolved findings still invalidate handling.
            if external:
                artifacts.append({'kind':'thread','id':thread['id'],'body':'\n\n'.join(external),'head':head})
        # Content identity survives a fix push; readiness separately binds handling to exact HEAD.
        for artifact in artifacts: artifact['hash'] = fingerprint({key:value for key,value in artifact.items() if key not in ('head','hash')})
        review_checks=[c for c in checks if c['name']=='PR-Agent review and suggestions']
        completed=False
        if review_checks:
            check=max(review_checks,key=lambda c:c.get('id',0))
            if check.get('head_sha')==head and check.get('status')=='completed' and check.get('conclusion')=='success':
                match=re.fullmatch(r'https://github.com/iamwavecut/openplotva/actions/runs/[0-9]+/job/([0-9]+)',check.get('details_url',''))
                if match:
                    logs=self.command(['gh','api','repos/'+REPOSITORY+'/actions/jobs/'+match[1]+'/logs'],authenticated=True)
                    if len(logs)>16*1024*1024: raise Deferred('PR-Agent execution log exceeds limit')
                    completed=(b'Generating PR review' in logs and
                        any(marker in logs for marker in (b'Published PR-Agent review comment',b'No actionable review findings to publish')) and
                        any(marker in logs for marker in (b'Published PR-Agent inline code suggestions',b'No actionable code suggestions to publish')))
        return {'pr': pr, 'head': head, 'checks': checks, 'statuses': statuses, 'artifacts': artifacts, 'threads': threads,'review_completed':completed}

    def reply_thread(self, thread_id, body):
        self.assert_owner()
        value=self.graphql('mutation($id:ID!,$body:String!){addPullRequestReviewThreadReply(input:{pullRequestReviewThreadId:$id,body:$body}){comment{id}}}', id=identifier(thread_id), body=self.privacy.assert_public(body))
        try:
            reply_id=value['data']['addPullRequestReviewThreadReply']['comment']['id']
            identifier(reply_id)
        except (KeyError,TypeError,InvalidResult):
            raise Deferred('review reply identity is not confirmed') from None
        return {'id':reply_id}

    def resolve_thread(self, thread_id):
        self.assert_owner()
        value = self.graphql('mutation($id:ID!){resolveReviewThread(input:{threadId:$id}){thread{id isResolved}}}', id=identifier(thread_id))
        thread=value.get('data', {}).get('resolveReviewThread', {}).get('thread') or {}
        if thread.get('id')!=thread_id or thread.get('isResolved') is not True:
            raise Deferred('review thread resolution not confirmed')
        return {'resolved': True}
