"""Actionable notification identities; private diagnostics never become message text."""
from .contracts import fingerprint

REASONS = {'usage_quota', 'work_budget', 'dependency_unavailable', 'unverified_fix',
           'scope_changed', 'invalid_result', 'review_incomplete', 'unspecified'}


def reason_code(job, status):
    reason = job.get('reason', '')
    if status == 'paused': return 'usage_quota'
    if 'budget' in reason and 'exhausted' in reason and 'retry budget' not in reason: return 'work_budget'
    if 'produced no verified fix' in reason: return 'unverified_fix'
    if 'PR-Agent execution proof' in reason: return 'review_incomplete'
    if 'review' in reason and any(word in reason for word in ('unavailable', 'unconfirmed', 'resumption')): return 'review_incomplete'
    if any(word in reason for word in ('dependency', 'GitHub publication', 'source or context')): return 'dependency_unavailable'
    if any(word in reason for word in ('scope', 'HEAD changed', 'changed outside')): return 'scope_changed'
    if any(word in reason for word in ('invalid', 'validation', 'isolated worker failed')): return 'invalid_result'
    return 'unspecified'


def notification_payload(state, job, status):
    references = {field: job[field] for field in ('issue_number', 'pr_number') if job.get(field)}
    if 'issue_number' not in references:
        origins = {r['issue_number'] for r in state.origins_for_signature(job['signature']) if r['issue_number']}
        if len(origins) == 1: references['issue_number'] = origins.pop()
    identity = {'status': status, 'scope': references or 'automation'}
    payload = {'run_id': job['id'], 'status': status, **references}
    if status in {'needs_human', 'paused', 'failed'}:
        identity['reason_code'] = payload['reason_code'] = reason_code(job, status)
    if status == 'paused': identity['episode'] = state.setting('provider_quota', {}).get('since')
    if references.get('pr_number') and status != 'pr_created': identity['head'] = job.get('published_sha')
    payload['key'] = fingerprint({'notification_v2': identity})
    return payload


def coalesce_pending(state):
    # Preserve transport keys/payloads: a lost POST acknowledgement may already
    # have queued the old message in the dispatcher. Identity is a separate key.
    with state.transaction():
        records = state.records('notifications')
        groups = {}
        for record in records:
            if 'identity_key' not in record:
                if record['state'] in ('sent', 'ambiguous'): continue
                job = state.job(record['payload']['run_id'])
                record['identity_key'] = notification_payload(state, job, record['payload']['status'])['key']
                state.put_record('notifications', record['key'], record)
            groups.setdefault(record['identity_key'], []).append(record)
        for group in groups.values():
            primary = min(group, key=lambda r: (r['state'] not in ('sent', 'ambiguous'),
                                                not r['posted'], r['state'] == 'superseded'))
            for record in group:
                if record is not primary and not record['posted'] and record['state'] != 'superseded':
                    record['state'] = 'superseded'
                    state.put_record('notifications', record['key'], record)
        return records
