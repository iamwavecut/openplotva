"""Publish bounded review receipts using the workflow's GitHub identity."""
import json
import re

import httpx

from maintenance.contracts import REPOSITORY
from maintenance.review_receipt import EXECUTION_CHECK


class ReviewExecutionError(RuntimeError):
    pass


class ReviewExecution:
    def __init__(self, env):
        try:
            self.number = int(env['PR_NUMBER'])
            self.head = env['PR_HEAD_SHA']
            self.run_id = int(env['GITHUB_RUN_ID'])
            self.attempt = int(env['GITHUB_RUN_ATTEMPT'])
            self.token = env['GITHUB_TOKEN']
            if (min(self.number, self.run_id, self.attempt) <= 0 or not self.token
                    or not re.fullmatch('[0-9a-f]{40}', self.head)
                    or env['PR_URL'] != f'https://github.com/{REPOSITORY}/pull/{self.number}'):
                raise ValueError()
        except (KeyError, ValueError, TypeError):
            raise ReviewExecutionError('invalid review target') from None
        self.url = f'https://github.com/{REPOSITORY}/actions/runs/{self.run_id}'
        self.external_id = f'pr-agent:{self.run_id}:{self.attempt}'
        self.check_id = None

    async def request(self, method, path, payload=None):
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=False, trust_env=False) as client:
                response = await client.request(method, 'https://api.github.com/repos/'+REPOSITORY+path,
                    json=payload, headers={'Authorization': 'Bearer '+self.token,
                    'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28'})
                if response.status_code not in (200, 201):
                    raise ReviewExecutionError('review receipt unavailable')
                return response.json()
        except (httpx.HTTPError, ValueError):
            raise ReviewExecutionError('review receipt unavailable') from None

    async def start(self):
        pr = await self.request('GET', '/pulls/'+str(self.number))
        if (pr.get('state') != 'open' or pr.get('draft')
                or pr.get('head', {}).get('sha') != self.head
                or any(pr.get(side, {}).get('repo', {}).get('full_name') != REPOSITORY for side in ('base', 'head'))):
            raise ReviewExecutionError('review target changed or outside owner scope')
        result = await self.request('POST', '/check-runs', {
            'name': EXECUTION_CHECK, 'head_sha': self.head, 'status': 'in_progress',
            'external_id': self.external_id, 'details_url': self.url})
        if type(result.get('id')) is not int or result['id'] <= 0:
            raise ReviewExecutionError('review receipt identity unavailable')
        self.check_id = result['id']

    async def finish(self, state, delay=None):
        conclusion = {'complete': 'success', 'quota_wait': 'neutral', 'failed': 'failure'}[state]
        value = {'version': 1, 'state': state, 'pr_number': self.number, 'head_sha': self.head,
                 'run_id': self.run_id, 'run_attempt': self.attempt, 'retry_after_seconds': delay}
        title = {'complete': 'Review completed', 'quota_wait': 'Review waits for usage quota',
                 'failed': 'Review did not complete'}[state]
        result = await self.request('PATCH', '/check-runs/'+str(self.check_id), {
            'status': 'completed', 'conclusion': conclusion,
            'output': {'title': title, 'summary': json.dumps(value, sort_keys=True)}})
        if (result.get('id') != self.check_id or result.get('head_sha') != self.head
                or result.get('external_id') != self.external_id or result.get('conclusion') != conclusion):
            raise ReviewExecutionError('review completion receipt unconfirmed')
