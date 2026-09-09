#!/usr/bin/env python3
"""Durable incident diagnosis and repair controller. Disabled until explicitly enabled."""
from __future__ import annotations

# Installed as a standalone directory as well as importable from the repository.
if not __package__:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    __package__ = Path(__file__).resolve().parent.name

import argparse
import concurrent.futures
import fcntl
import json
import math
import os
import re
import time
from pathlib import Path

from .api import MaintenanceAPI
from .contracts import (DEEP_SECONDS, INITIAL_SECONDS, MAX_ROUNDS, MODEL, OMP_VERSION, REPOSITORY,
                        Deferred, InvalidResult, QuotaUnavailable, diagnosis, fingerprint, identifier, sha, text)
from .github import GitHub, is_owner, select_candidates, validate_dispatch, validate_issue
from .state import State

DEFAULT_CHECKS = ['Rust workspace', 'Release candidate image', 'PostgreSQL integration', 'Rust dependencies',
                  'Danger PR rules', 'PR-Agent review and suggestions', 'CodeQL Rust', 'Semgrep CE', 'Maintenance automation']


def review_ready(snapshot, required_checks, handled):
    head = snapshot['head']
    if snapshot.get('pr',{}).get('draft') or snapshot.get('pr',{}).get('mergeable') is False: return False
    if 'PR-Agent review and suggestions' in required_checks and not snapshot.get('review_completed'): return False
    # Only the newest execution for a check name can satisfy the current HEAD.
    checks = {}
    for check in sorted(snapshot['checks'], key=lambda c: c.get('id', 0)):
        checks[check['name']] = check
    if not required_checks or not set(required_checks) <= checks.keys(): return False
    for check in checks.values():
        diagnostic = (check['name']=='semgrep' and check['name'] not in required_checks
                      and (check.get('app') or {}).get('id')==15368
                      and (check.get('app') or {}).get('slug')=='github-actions'
                      and check.get('conclusion')=='neutral')
        if check.get('head_sha') != head or check.get('status') != 'completed': return False
        if check.get('conclusion') != 'success' and not diagnostic: return False
    statuses = {}
    for status in snapshot.get('statuses', []): statuses.setdefault(status['context'], status)
    if any(s['state'] != 'success' for s in statuses.values()): return False
    for artifact in snapshot['artifacts']:
        record = handled.get(artifact['kind']+':'+str(artifact['id']), {})
        if record.get('hash') != artifact['hash'] or record.get('head') != head: return False
    return not any(not t['isResolved'] for t in snapshot.get('threads', []))


def issue_body(value, job, marker):
    sections = [value['summary'], 'Repository: '+REPOSITORY,
                'Versions: main `'+job['base_sha']+'`; deployed `'+str(job.get('context', {}).get('deployed', {}).get('revision') or 'unknown')+'`; OMP '+OMP_VERSION+'; '+MODEL+'.',
                'Independent causes: external '+value['external_cause']+'; code '+value['code_defect']+'.']
    for key in ('observations', 'hypotheses', 'supporting', 'contradicting', 'related_changes', 'missing', 'acceptance'):
        sections.append('## '+key.replace('_',' ').capitalize()+'\n\n'+('\n'.join('- '+item for item in value[key]) or 'None identified.'))
    sections.append('## Impact\n\nSanitized incident aggregate: '+str(job.get('context', {}).get('incident', {}).get('count', 1))+' captured terminal events.')
    sections.append(marker)
    return text('\n\n'.join(sections), 60000)


class Controller:
    def __init__(self, config, state, api, github, runner):
        self.config, self.state, self.api, self.github, self.runner = config, state, api, github, runner
        self.clock = state.clock
        self.last_incidents = self.last_review = self.last_history = 0
        self.future = None
        self.future_job = None

    def effect(self, key, kind, payload, reconcile, mutate, job=None):
        if job and self.state.cancelled(job['id']): raise InvalidResult('cancelled job cannot publish')
        record = self.state.record('effects', key)
        if record and record['payload'] != payload: raise InvalidResult('durable effect intent changed')
        if record and record['state'] == 'done': return record['result']
        if record is None:
            record = {'key': key, 'kind': kind, 'payload': payload, 'state': 'intent', 'attempts': 0}
            self.state.put_record('effects', key, record)
        found = reconcile()
        if found is not None:
            record.update(state='done', result=found); self.state.put_record('effects', key, record); return found
        if record['state'] == 'uncertain':
            record['attempts'] += 1; self.state.put_record('effects', key, record)
            if record['attempts'] >= self.config.get('dependency_retries', 3): raise InvalidResult('unconfirmed external write needs operator reconciliation')
            raise Deferred('external effect awaiting reconciliation')
        # Persist the crash boundary before sending even the first mutation.
        record.update(state='uncertain'); self.state.put_record('effects', key, record)
        if job and self.state.cancelled(job['id']): raise InvalidResult('cancelled job cannot publish')
        value = mutate()
        record.update(state='done', result=value); self.state.put_record('effects', key, record)
        return value

    def reconcile_origins(self):
        for record in self.state.records('effects'):
            if record['kind'] != 'issue': continue
            payload = record['payload']
            result = record.get('result') if record['state'] == 'done' else self.github.find_issue(payload['marker'])
            if result:
                self.state.record_origin(payload['marker'], result['number'], payload['signature'], payload['incident_id'])
                record.update(state='done', result=result); self.state.put_record('effects', record['key'], record)

    def enqueue(self, issue_number, event_id):
        if type(issue_number) is not int or issue_number <= 0 or not str(event_id).isdigit(): raise InvalidResult('invalid dispatch identifiers')
        identifier(str(event_id))
        run=self.github.run(event_id)
        validate_dispatch(self.github.issue(issue_number), run, issue_number, event_id)
        self.reconcile_origins()
        if not self.state.origin(issue_number): raise InvalidResult('untrusted issue provenance')
        return self.state.enqueue(issue_number, str(event_id), self.github.queue_generation(issue_number,run))

    def poll_incidents(self):
        page = self.api.incidents(self.state.cursor())
        # Revalidate the runtime allowlisted projection before persisting context.
        text(json.dumps(page), 2*1024*1024)
        self.state.ingest(page['incidents'], page['next_cursor'], self.config.get('debounce_seconds', 60))

    def schedule_incidents(self):
        for incident in self.state.due_incidents():
            with self.state.transaction():
                active = [j for j in self.state.jobs() if j['signature'] == incident['signature'] and j['status'] in ('queued', 'running', 'result', 'processing', 'waiting_ci')]
                if not active: self.state.new_job('initial', incident['signature'], incident['incident_id'])
                incident['status'] = 'diagnosing'; self.state.put_incident(incident)

    def refresh_history(self):
        index = self.github.index()
        for item in index: text(json.dumps(item), 1024*1024)
        self.state.replace_history(index)
        self.last_history = self.clock()

    def context(self, job):
        self.refresh_history()
        incident = self.state.incident(job['signature']) or {'signature': job['signature'], 'incident_id': job['incident_id'], 'snapshot': {}}
        candidates = select_candidates(self.state.history(), incident['snapshot'], self.config.get('history_limit', 20))
        history = [self.github.discussion(item) for item in candidates]
        # Discussion is untrusted evidence and cannot authorize publication by itself.
        safe_history = []
        for item in history:
            projected = {k: item.get(k) for k in ('kind', 'number', 'title', 'body', 'state', 'merged_at', 'merge_commit_sha', 'linked_prs')}
            projected['comments'] = [{'id': c['id'], 'body': c.get('body') or ''} for c in item.get('comments', [])][-100:]
            text(json.dumps(projected), 256*1024)
            safe_history.append(projected)
        context = {'incident': incident, 'history': safe_history, 'evidence': self.api.evidence(job['incident_id']),
                   'deployed': self.runner.host_snapshot(), 'feedback': job.get('feedback', [])}
        if job['stage'] in ('deep','review'):
            target=self.github.issue(job['issue_number'])
            validate_issue(target,job['issue_number'])
            body=target.get('body') or ''
            sections=re.findall(r'^#{1,6} +Acceptance(?: criteria)?[ \t]*\n(.*?)(?=^#{1,6} +|\Z)',body,re.IGNORECASE|re.MULTILINE|re.DOTALL)
            context['target_issue']={'repository':REPOSITORY,'number':target['number'],'title':target.get('title') or '',
                'state':target['state'],'body':body,'acceptance_criteria':'\n\n'.join(section.strip() for section in sections).strip() or None}
        if job.get('previous_attempt'): context['previous_attempt'] = job['previous_attempt']
        text(json.dumps(context), 1024*1024)
        return context

    def prepare_run(self):
        if not self.state.enabled(): return None
        for job in self.state.jobs({'queued'}):
            if job['next_at'] > self.clock(): continue
            budget = INITIAL_SECONDS if job['stage'] == 'initial' else DEEP_SECONDS
            spent=self.state.issue_usage(job['issue_number']) if job['stage']!='initial' and job.get('issue_number') else {'active_seconds':job['active_seconds'],'cycles':job['rounds']}
            already_started=self.state.db.execute('SELECT 1 FROM starts WHERE job_id=?',(job['id'],)).fetchone() is not None
            if spent['active_seconds'] >= budget or spent['cycles']+(0 if already_started else 1)>MAX_ROUNDS:
                self.needs_human(job, 'active time or repair cycle budget exhausted'); continue
            try:
                if job['stage'] != 'initial':
                    if not self.state.origin(job['issue_number']): raise InvalidResult('job has no durable issue origin')
                    validate_issue(self.github.issue(job['issue_number']), job['issue_number'])
                    if job.get('pr_number'):
                        pr = self.github.pr(job['pr_number'])
                        if pr['state'] != 'open' or pr.get('head', {}).get('ref') != job.get('branch'):
                            raise InvalidResult('pull request changed outside controller scope')
                        base = sha(pr['head']['sha'])
                        self.runner.refresh_source()
                    else:
                        linked = self.github.discussion({**self.github.issue(job['issue_number']), 'kind': 'issue'}).get('linked_prs', [])
                        if any(self.github.pr(n)['state'] == 'open' for n in linked):
                            self.state.update_job(job['id'], status='done', reason='existing open pull request'); continue
                        base = self.runner.refresh_source()
                else: base = self.runner.refresh_source()
                context = self.context(job)
                self.state.update_job(job['id'], base_sha=sha(base), context=context)
            except Deferred:
                self.retry(job, 'source or context dependency unavailable')
                continue
            except (InvalidResult, ValueError):
                self.needs_human(job, 'job scope or context validation failed'); continue
            try:
                self.runner.preflight()
                return self.state.claim(job['id'])
            except Deferred:
                # Resource/quota deferrals do not consume failure attempts or worker budget.
                self.state.update_job(job['id'], next_at=self.clock()+60)
                return None
        return None

    def run_next(self):
        job = self.prepare_run()
        if job:
            try: result = self.runner.run(job, job['context'])
            except QuotaUnavailable as error:
                self.defer_quota(job,error)
            except Deferred as error:
                current=self.finish_failed_run(job,error)
                if not current['cancelled']: self.retry(current, 'agent dependency unavailable')
            except (InvalidResult, ValueError, OSError) as error:
                self.finish_failed_run(job, error); self.needs_human(self.state.job(job['id']), 'invalid isolated result')
            else: self.accept_result(job, result)
        self.process_results()

    def finish_failed_run(self, job, error):
        active=getattr(error,'active_seconds',0)
        if not isinstance(active,(int,float)) or isinstance(active,bool) or not math.isfinite(active) or active<0: active=0
        usage=getattr(error,'usage',{})
        usage={key:value for key,value in usage.items() if isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value) and value>=0} if isinstance(usage,dict) else {}
        return self.state.finish_run(job['id'],active,usage)

    def defer_quota(self, job, error):
        delay=getattr(error,'retry_after_seconds',3600)
        if not isinstance(delay,(int,float)) or isinstance(delay,bool) or not math.isfinite(delay): delay=3600
        delay=max(60,min(86400,delay))
        # Accounting and rescheduling share a transaction so recovery cannot skip the quota delay.
        with self.state.transaction():
            current=self.finish_failed_run(job,error)
            if current['cancelled']: return
            current=self.state.update_job(job['id'],status='queued',next_at=self.clock()+delay,
                reason='GLM Coding Plan quota unavailable; waiting for quota recovery')
        spent=self.state.issue_usage(current['issue_number'])['active_seconds'] if current['stage']!='initial' and current.get('issue_number') else current['active_seconds']
        budget=INITIAL_SECONDS if current['stage']=='initial' else DEEP_SECONDS
        if spent>=budget: self.needs_human(current,'active time budget exhausted while waiting for provider quota')

    def complete_future(self):
        job=self.future_job
        try:
            try: result=self.future.result()
            except QuotaUnavailable as error:
                self.defer_quota(job,error)
            except Deferred as error:
                current=self.finish_failed_run(job,error)
                if not current['cancelled']: self.retry(current,'agent dependency unavailable')
            except Exception as error:
                self.finish_failed_run(job,error)
                self.needs_human(job,'isolated worker failed')
            else: self.accept_result(job,result)
        finally:
            self.future=None
            self.future_job=None

    def accept_result(self, job, result):
        try:
            diagnosis(result['diagnosis'])
            if set(result)-{'diagnosis','outcome','patch_path','base_sha','checks','usage','active_seconds','feedback','artifact_dir'}:
                raise InvalidResult('unexpected result receipt fields')
            if result['outcome'] not in ('patch','no_fix','needs_human'): raise InvalidResult('invalid outcome')
            for feedback in result.get('feedback',[]): text(feedback['body'])
            text(json.dumps(result),3*1024*1024)
        except (InvalidResult,ValueError,KeyError,TypeError):
            self.state.finish_run(job['id'],0,{})
            self.needs_human(job,'isolated result failed validation before persistence')
            return
        # Save before releasing lease, so restart can continue publication without another agent start.
        self.state.update_job(job['id'], result=result)
        active = result.get('active_seconds', 0)
        if not isinstance(active, (int, float)) or not math.isfinite(active) or active < 0: active = 0
        current = self.state.finish_run(job['id'], active, result.get('usage', {}))
        if not current['cancelled']: self.state.update_job(job['id'], status='result', attempts=0, next_at=0)

    def retry(self, job, reason):
        current = self.state.job(job['id'])
        attempts = current['attempts']+1
        if attempts >= self.config.get('dependency_retries', 3): self.needs_human(current, reason+'; retry budget exhausted')
        else: self.state.update_job(job['id'], status='result' if current.get('result') else 'queued', attempts=attempts,
                                   next_at=self.clock()+min(900, 30*2**(attempts-1)), reason=reason)

    def incident_status(self, job, status):
        incident = self.state.incident(job['signature'])
        if incident:
            incident.update(status=status, last_snapshot=fingerprint(incident['snapshot']))
            self.state.put_incident(incident)

    def notify(self, job, status):
        key = fingerprint({'run_id': job['id'], 'status': status})
        if self.state.record('notifications', key): return
        payload = {'key': key, 'run_id': job['id'], 'status': status}
        for field in ('issue_number', 'pr_number'):
            if job.get(field): payload[field] = job[field]
        self.state.put_record('notifications', key, {'key': key, 'payload': payload, 'state': 'pending', 'posted': False})

    def needs_human(self, job, reason):
        if self.state.cancelled(job['id']): return
        job = self.state.update_job(job['id'], status='needs_human', reason=reason)
        self.incident_status(job, 'needs_human')
        self.notify(job, 'needs_human')
        # Label additions are idempotent; persist/reconcile them through the same effect journal.
        if job.get('issue_number'):
            try:
                key = fingerprint({'label': job['issue_number'], 'name': 'needs-human'})
                self.effect(key, 'label', {'number': job['issue_number'], 'label': 'needs-human'},
                    lambda: {} if 'needs-human' in {v['name'] for v in self.github.issue(job['issue_number'])['labels']} else None,
                    lambda: self.github.add_label(job['issue_number'], 'needs-human'), job)
            except (Deferred, InvalidResult): pass

    def facts_comment(self, job, number, value):
        marker = '<!-- maintenance:facts:'+fingerprint({'signature':job['signature'],'issue':number})+' -->'
        body = issue_body(value, job, marker)
        key = fingerprint({'comment':marker, 'body':body})
        existing = self.github.find_comment(number, marker)
        self.effect(key, 'comment', {'number':number,'marker':marker,'body':body},
            lambda: (found if (found := self.github.find_comment(number, marker)) and found['body'] == body else None),
            lambda: self.github.comment(number, body, existing['id'] if existing else None), job)

    def handle_matches(self, job, value):
        candidates = {(c['kind'], c['number']): c for c in job['context']['history']}
        matches=[]
        for match in value['matches']:
            key=(match['kind'],match['number'])
            if key not in candidates: raise InvalidResult('semantic reference is outside supplied history candidates')
            if match['relationship']=='related': continue
            remote=self.github.issue(key[1]) if key[0]=='issue' else self.github.pr(key[1])
            linked=candidates[key].get('linked_prs') or []
            if key[0]=='issue': linked=self.github.discussion({**remote,'kind':'issue'}).get('linked_prs',linked)
            prs=[remote] if key[0]=='pr' else [self.github.pr(n) for n in linked]
            matches.append((match,remote,prs))
        # Examine all related PRs before any issue can authorize another deep run.
        for match,remote,prs in matches:
            if any(p['state']=='open' for p in prs):
                self.facts_comment(job,match['number'],value)
                self.state.update_job(job['id'],status='done',reason='existing open pull request')
                self.incident_status(job,'done'); return True
        links=[]
        for match,remote,prs in matches:
            merged=[p for p in prs if p.get('merged_at')]
            for pr in merged:
                present=self.runner.contains(pr.get('merge_commit_sha'),job['context']['deployed'].get('revision'))
                if present is None:
                    self.needs_human(job,'cannot establish merged fix deployment ancestry'); return True
                if not present:
                    self.state.update_job(job['id'],status='wait_deploy',reason='related fix has not reached deployed revision')
                    self.incident_status(job,'wait_deploy'); return True
                links.append('Regression after deployed fix in PR #'+str(pr['number'])+'.')
        for match,remote,prs in matches:
            if match['kind']!='issue' or any(p.get('merged_at') for p in prs): continue
            self.facts_comment(job,remote['number'],value)
            if match['relationship']=='new_evidence':
                if value['next_action']!='fix' or value['code_defect']!='confirmed' or not value['supporting'] or not value['acceptance']:
                    raise InvalidResult('material new evidence lacks justified fix')
                if remote['state']=='closed':
                    links.append('Follow-up to closed issue #'+str(remote['number'])+': '+match['reason'])
                    continue
                if not self.state.origin(remote['number']):
                    self.needs_human(job,'existing issue lacks controller origin for automatic repair'); return True
                validate_issue(remote,remote['number'])
                evidence_key='evidence_'+fingerprint({'issue':remote['number'],'supporting':value['supporting'],'acceptance':value['acceptance']})
                self.state.enqueue(remote['number'],evidence_key,evidence_key)
            self.state.update_job(job['id'],status='done',reason='existing issue records this cause')
            self.incident_status(job,'done'); return True
        if links:
            value={**value,'related_changes':value['related_changes']+links}
            self.state.update_job(job['id'],result={**job['result'],'diagnosis':value})
        return False

    def initial_result(self, job, value):
        if self.handle_matches(job, value): return
        job = self.state.job(job['id']); value = job['result']['diagnosis']
        if value['next_action'] == 'observe':
            self.state.update_job(job['id'], status='observing'); self.incident_status(job,'observing'); return
        marker = '<!-- maintenance:origin:'+job['id']+' -->'
        body = issue_body(value, job, marker)
        labels = ['agent:created','agent:queued','bug' if value['code_defect']=='confirmed' else 'needs-triage']
        payload = {'marker':marker,'signature':job['signature'],'incident_id':job['incident_id'],'title':value['title'],'body':body,'labels':labels}
        self.effect(fingerprint({'labels':'maintenance-v1'}),'labels',{'version':1},
            lambda: {} if self.github.labels_ready() else None,self.github.ensure_labels,job)
        self.state.record_origin(marker, None, job['signature'], job['incident_id'])
        issue = self.effect(fingerprint({'issue':job['id']}), 'issue', payload,
                            lambda:self.github.find_issue(marker), lambda:self.github.create_issue(value['title'],body,labels), job)
        self.state.record_origin(marker,issue['number'],job['signature'],job['incident_id'])
        self.state.update_job(job['id'],status='done',issue_number=issue['number']); self.incident_status(job,'done')

    def publish_patch(self, job, value):
        self.validate_feedback(job)
        result = job['result']
        if value['next_action'] != 'fix' or {c['name'] for c in result.get('checks',[]) if c.get('passed') is True} != {'fmt','clippy','tests'}:
            raise InvalidResult('patch has no complete verification receipt')
        validate_issue(self.github.issue(job['issue_number']),job['issue_number'])
        prepared = job.get('prepared')
        if prepared is None:
            prepared = self.github.prepare_patch(job,result,job['rounds'])
            # Commit revision and local checkout exist durably BEFORE any push.
            job = self.state.update_job(job['id'],prepared=prepared,branch=prepared['branch'])
        self.effect(fingerprint({'push':job['id'],'sha':prepared['sha']}),'push',prepared,
                    lambda: {'sha':prepared['sha']} if self.github.remote_sha(prepared['branch']) == prepared['sha'] else None,
                    lambda:self.github.push(prepared,job.get('published_sha')),job)
        job = self.state.update_job(job['id'],published_sha=prepared['sha'])
        if not job.get('pr_number'):
            marker = '<!-- maintenance:pr:'+job['id']+' -->'
            body = issue_body(value,job,marker)+'\n\nCloses #'+str(job['issue_number'])+'\n\nValidation: cargo fmt; workspace clippy with warnings denied; affected tests passed.'
            payload = {'branch':prepared['branch'],'marker':marker,'title':value['title'],'body':body}
            pr = self.effect(fingerprint({'pr':job['id']}),'pr',payload,
                lambda:self.github.find_pr(prepared['branch'],marker),lambda:self.github.create_pr(prepared['branch'],value['title'],body),job)
            job = self.state.update_job(job['id'],pr_number=pr['number'])
            self.notify(job,'pr_created')
        self.handle_feedback(job)
        self.state.update_job(job['id'],status='waiting_ci',last_poll=0,prepared=None,result=None,attempts=0)

    def validate_feedback(self, job):
        responses=job['result'].get('feedback',[])
        if not isinstance(responses,list): raise InvalidResult('invalid feedback responses')
        artifacts={(a['kind'],str(a['id'])):a for a in job.get('feedback',[]) if a.get('kind') in ('comment','thread')}
        seen=set()
        for response in responses:
            reference=(response.get('kind'),str(response.get('id')))
            if reference not in artifacts or reference in seen or response.get('action') not in ('fixed','rebuttal'):
                raise InvalidResult('invalid feedback reference or action')
            if response['action']=='fixed' and job['result']['outcome']!='patch': raise InvalidResult('fixed response requires verified patch')
            text(response.get('body'))
            seen.add(reference)
        if responses and job.get('pr_number'):
            snapshot=self.github.review_snapshot(job['pr_number'])
            if snapshot['head']!=job.get('published_sha',job['base_sha']):
                raise InvalidResult('pull request HEAD changed during feedback handling')
            current={(a['kind'],str(a['id'])):a for a in snapshot['artifacts']}
            for reference in seen:
                old=artifacts[reference]
                new=current.get(reference)
                if (new is None and reference[0]=='comment' and reference[1].startswith('check_')
                        and job['result']['outcome']=='patch' and old.get('head')==job['base_sha']
                        and (job.get('prepared') or {}).get('sha')==snapshot['head']):
                    new=self.github.previous_diagnostic(reference[1],job['base_sha'])
                if not new or new['body']!=old['body'] or new['hash']!=old['hash']:
                    raise InvalidResult('review artifact changed during repair')

    def handle_feedback(self, job):
        self.validate_feedback(job)
        responses = job['result'].get('feedback',[])
        artifacts = {(a['kind'],str(a['id'])):a for a in job.get('feedback',[]) if a.get('kind') in ('comment','thread')}
        handled = dict(job.get('handled',{}))
        for response in responses:
            if (response.get('kind'),str(response.get('id'))) not in artifacts or response.get('action') not in ('fixed','rebuttal'):
                raise InvalidResult('feedback response references unknown review artifact')
            if response['action']=='fixed' and job['result']['outcome'] != 'patch': raise InvalidResult('fixed feedback has no published patch')
            artifact = artifacts[(response['kind'],str(response['id']))]
            body = text(response['body'])
            marker = '<!-- maintenance:feedback:'+fingerprint({'job':job['id'],'hash':artifact['hash'],'head':job.get('published_sha')})+' -->'
            body += '\n\n'+('Fixed in '+job['published_sha']+'.\n\n' if response['action']=='fixed' else '')+marker
            key = fingerprint({'reply':marker})
            if response['kind']=='thread':
                def reconcile_reply():
                    snapshot=self.github.review_snapshot(job['pr_number'])
                    replies=[c for t in snapshot['threads'] if t['id']==response['id'] for c in t['comments']['nodes']
                             if c['body']==body and is_owner({'login':(c.get('author') or {}).get('login'),'id':(c.get('author') or {}).get('databaseId')})]
                    if len(replies)>1: raise InvalidResult('ambiguous review reply provenance')
                    if not replies: return None
                    if type(replies[0].get('databaseId')) is not int or replies[0]['databaseId']<=0:
                        raise Deferred('reconciled review reply identity is unavailable')
                    return {'id':str(replies[0]['databaseId'])}
                self.effect(key,'thread_reply',{'thread':response['id'],'body':body},reconcile_reply,lambda:self.github.reply_thread(response['id'],body),job)
                self.validate_feedback(job)
                self.effect(fingerprint({'resolve':marker}),'resolve',{'thread':response['id']},
                    lambda: {} if any(t['id']==response['id'] and t['isResolved'] for t in self.github.review_snapshot(job['pr_number'])['threads']) else None,
                    lambda:self.github.resolve_thread(response['id']),job)
            else:
                self.effect(key,'reply',{'number':job['pr_number'],'body':body},lambda:self.github.find_comment(job['pr_number'],marker),lambda:self.github.comment(job['pr_number'],body),job)
            handled[response['kind']+':'+str(response['id'])]={'hash':artifact['hash'],'head':job.get('published_sha',job['base_sha'])}
        self.validate_feedback(job)
        self.state.update_job(job['id'],handled=handled)

    def process_results(self):
        for job in self.state.jobs({'result'}):
            if job['next_at'] > self.clock() or job['cancelled']: continue
            try:
                value = diagnosis(job['result']['diagnosis'])
                if job['stage']=='initial': self.initial_result(job,value)
                elif job['result']['outcome']=='patch': self.publish_patch(job,value)
                elif job['stage']=='review' and job['result'].get('feedback') and not any(not c.get('passed') for c in job['result'].get('checks',[])):
                    self.handle_feedback(job); self.state.update_job(job['id'],status='waiting_ci',result=None,last_poll=0)
                elif job['result'].get('patch_path') and any(not c.get('passed') for c in job['result'].get('checks',[])) and job['rounds']+1 < MAX_ROUNDS:
                    self.state.update_job(job['id'],stage='review',status='queued',rounds=job['rounds']+1,result=None,
                        previous_attempt={'diagnosis':value,'checks':job['result']['checks']},
                        feedback=[{'kind':'check','id':c['name'],'body':'Isolated verification failed: '+c['name']} for c in job['result']['checks'] if not c.get('passed')])
                else: self.needs_human(job,'deep investigation produced no verified fix; issue remains open')
            except Deferred: self.retry(job,'GitHub publication awaiting reconciliation')
            except (InvalidResult,ValueError,KeyError,TypeError): self.needs_human(job,'result or publication contract failed validation')

    def poll_reviews(self):
        for job in self.state.jobs({'waiting_ci','ready'}):
            if job.get('last_poll',0)+self.config.get('review_poll_seconds',60)>self.clock(): continue
            try:
                snapshot=self.github.review_snapshot(job['pr_number'])
                self.state.update_job(job['id'],last_poll=self.clock())
                if snapshot['pr']['state']!='open':
                    self.state.update_job(job['id'],status='done',reason='pull request closed externally'); continue
                if snapshot['head'] != job['published_sha']:
                    self.needs_human(job,'pull request HEAD changed outside controller'); continue
                self.state.update_job(job['id'],poll_failures=0)
                handled=job.get('handled',{})
                if review_ready(snapshot,self.config.get('required_checks',DEFAULT_CHECKS),handled):
                    self.state.update_job(job['id'],status='ready'); self.notify(job,'pr_ready'); continue
                feedback=[a for a in snapshot['artifacts'] if handled.get(a['kind']+':'+str(a['id'])) != {'hash':a['hash'],'head':snapshot['head']}]
                failures=[c for c in snapshot['checks'] if c.get('status')=='completed' and c.get('conclusion') not in ('success','neutral','skipped')]
                feedback += [{'kind':'check','id':str(c.get('id',c['name'])),'body':text(c['name']+': '+str(c.get('conclusion'))+'\n'+str((c.get('output') or {}).get('summary') or '')[:12000]),'head':snapshot['head']} for c in failures]
                if not feedback and all(c.get('status')=='completed' for c in snapshot['checks']) and not snapshot.get('review_completed'):
                    missing=job.get('missing_review_polls',0)+1
                    self.state.update_job(job['id'],missing_review_polls=missing)
                    if missing>=self.config.get('dependency_retries',3): self.needs_human(job,'required PR-Agent execution proof unavailable')
                if feedback:
                    if job['rounds']>=MAX_ROUNDS or job['active_seconds']>=DEEP_SECONDS:
                        self.needs_human(job,'review repair budget exhausted'); continue
                    self.state.update_job(job['id'],status='queued',stage='review',feedback=feedback,rounds=job['rounds']+1,
                                          result=None,prepared=None,next_at=0,attempts=0)
            except (Deferred,InvalidResult,ValueError,KeyError,TypeError):
                failures=job.get('poll_failures',0)+1
                self.state.update_job(job['id'],poll_failures=failures,last_poll=self.clock())
                if failures>=self.config.get('dependency_retries',3): self.needs_human(job,'review polling unavailable; readiness is unconfirmed')

    def poll_notifications(self):
        for record in self.state.records('notifications'):
            if record['state'] in ('sent','ambiguous'): continue
            try:
                if record['posted']:
                    receipt=self.api.notification(record['key'])
                else:
                    receipt=self.api.notify(record['payload']); record['posted']=True
                if receipt.get('key') != record['key']: raise Deferred('notification receipt key mismatch')
                if receipt['state']=='sent' and not receipt.get('telegram_message_id'): raise Deferred('notification not confirmed by dispatcher')
                record.update(state=receipt['state'],telegram_message_id=receipt.get('telegram_message_id'))
                self.state.put_record('notifications',record['key'],record)
                if record['state']=='ambiguous':
                    self.state.update_job(record['payload']['run_id'],status='needs_human',reason='notification delivery ambiguous; no replacement notification sent')
            except (Deferred,ValueError,KeyError): continue

    def serve(self):
        directory=Path(self.config['state_dir']); directory.mkdir(parents=True,exist_ok=True,mode=0o700)
        with (directory/'service.lock').open('a') as lock:
            try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError: raise Deferred('maintenance service already running') from None
            self.runner.recover()
            self.state.recover()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                while True:
                    try:
                        if self.future and self.future.done(): self.complete_future()
                        if self.clock()-self.last_incidents>=self.config.get('incident_poll_seconds',30):
                            self.last_incidents=self.clock(); self.poll_incidents(); self.schedule_incidents()
                        self.process_results(); self.poll_reviews(); self.poll_notifications()
                        if not self.future:
                            job=self.prepare_run()
                            if job: self.future_job=job; self.future=pool.submit(self.runner.run,job,job['context'])
                    except Exception:
                        # Errors deliberately omit exception text, subprocess output and model context.
                        self.state.set_setting('last_error',{'at':self.clock(),'status':'dependency unavailable'})
                    time.sleep(1)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',default='/etc/openplotva-maintenance/config.json')
    sub=parser.add_subparsers(dest='command',required=True)
    for name in ('status','enable','disable','serve'): sub.add_parser(name)
    cancel=sub.add_parser('cancel'); cancel.add_argument('job_id')
    enqueue=sub.add_parser('enqueue'); enqueue.add_argument('issue_number',type=int); enqueue.add_argument('event_run_id')
    args=parser.parse_args(argv)
    state=None
    try:
        config=json.loads(Path(args.config).read_text()); state=State(Path(config['state_dir'])/'state.sqlite3')
        if args.command=='status': print(json.dumps(state.status(),sort_keys=True)); return 0
        if args.command in ('enable','disable'):
            state.set_enabled(args.command=='enable'); print(json.dumps({'enabled':state.enabled()})); return 0
        if args.command=='cancel':
            state.cancel(identifier(args.job_id)); print(json.dumps({'cancelled':args.job_id})); return 0
        from .runner import Runner
        api=MaintenanceAPI(config); github=GitHub(config)
        runner=Runner(config,api,cancelled=state.cancelled)
        controller=Controller(config,state,api,github,runner)
        if args.command=='enqueue': print(json.dumps({'job_id':controller.enqueue(args.issue_number,args.event_run_id)['id']})); return 0
        controller.serve()
    except (Deferred,InvalidResult,ValueError,OSError,KeyError):
        print(json.dumps({'error':'maintenance command failed; inspect configuration or safe service status'})); return 1
    finally:
        if state is not None: state.close()
    return 0


if __name__=='__main__': raise SystemExit(main())
