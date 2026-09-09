"""Versioned public execution metadata; never contains prompts or provider text."""
from __future__ import annotations

import datetime
import json
import re

from .contracts import REPOSITORY

REVIEW_CHECK = 'PR-Agent review and suggestions'
EXECUTION_CHECK = 'PR-Agent execution'


def execution_receipt(checks, head, number):
    latest = {c['name']: c for c in sorted(checks, key=lambda c: c.get('id', 0))}
    report, job = latest.get(EXECUTION_CHECK), latest.get(REVIEW_CHECK)
    if not report or not job:
        return None
    for check in (report, job):
        if (check.get('head_sha') != head or check.get('status') != 'completed'
                or (check.get('app') or {}).get('id') != 15368
                or (check.get('app') or {}).get('slug') != 'github-actions'):
            return None
    try:
        summary = report['output']['summary']
        if len(summary) > 4096:
            return None
        value = json.loads(summary)
        if set(value) != {'version', 'state', 'pr_number', 'head_sha', 'run_id', 'run_attempt', 'retry_after_seconds'}:
            return None
        if (type(value['version']) is not int or value['version'] != 1
                or type(value['pr_number']) is not int or value['pr_number'] != number
                or value['head_sha'] != head or value['state'] not in {'complete', 'quota_wait', 'failed'}):
            return None
        for field in ('run_id', 'run_attempt'):
            if type(value[field]) is not int or value[field] <= 0:
                return None
        delay = value['retry_after_seconds']
        if delay is not None and (type(delay) is not int or not 60 <= delay <= 604800):
            return None
        url = 'https://github.com/'+REPOSITORY+'/actions/runs/'+str(value['run_id'])
        if (report.get('details_url') != url
                or report.get('external_id') != f"pr-agent:{value['run_id']}:{value['run_attempt']}"):
            return None
        match = re.fullmatch(re.escape(url) + r'/job/([1-9][0-9]*)', job.get('details_url', ''))
        expected = {'complete': 'success', 'quota_wait': 'neutral', 'failed': 'failure'}[value['state']]
        if not match or report.get('conclusion') != expected:
            return None
        if job.get('conclusion') != ('success' if value['state'] == 'complete' else 'failure'):
            return None
        times = [datetime.datetime.fromisoformat(report[k].replace('Z', '+00:00'))
                 for k in ('started_at', 'completed_at')]
        job_times = [datetime.datetime.fromisoformat(job[k].replace('Z', '+00:00'))
                     for k in ('started_at', 'completed_at')]
        if (any(t.tzinfo is None for t in times + job_times)
                or not job_times[0] <= times[0] <= times[1] <= job_times[1]):
            return None
        return {**value, 'check_id': report['id'], 'job_id': int(match[1]),
                'started_at': times[0].timestamp(), 'completed_at': times[1].timestamp()}
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
        return None
