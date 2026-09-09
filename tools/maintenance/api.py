"""Narrow HTTPS maintenance API; credentials never enter worker context."""
from __future__ import annotations
import json
import os
import re
import ssl
import stat
import urllib.error
import urllib.parse
import urllib.request

from .contracts import Deferred, InvalidResult, identifier
from .notifications import REASONS


def secret_file(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor) as stream:
        mode = os.fstat(stream.fileno())
        if not stat.S_ISREG(mode.st_mode) or mode.st_mode & 0o077:
            raise Deferred('credential file must be private and regular')
        value = stream.read(16385).strip()
    if not value or len(value) > 16384 or '\n' in value or '\r' in value:
        raise Deferred('invalid credential file')
    return value


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl): return None


class MaintenanceAPI:
    def __init__(self, config, transport=None):
        self.config = config
        self.base = config['maintenance_url'].rstrip('/')
        parsed = urllib.parse.urlsplit(self.base)
        if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError('maintenance endpoint must be an HTTPS URL without credentials')
        self.transport = transport

    def request(self, method, path, payload=None):
        try:
            token = secret_file(self.config['maintenance_token_file'])
            data = json.dumps(payload).encode() if payload is not None else None
            request = urllib.request.Request(self.base+path, data=data, method=method,
                                            headers={'Authorization': 'Bearer '+token, 'Content-Type': 'application/json'})
            if self.transport:
                raw = self.transport(request)
            else:
                context = ssl.create_default_context(cafile=self.config.get('maintenance_ca'))
                opener = urllib.request.build_opener(NoRedirect, urllib.request.HTTPSHandler(context=context))
                with opener.open(request, timeout=20) as response: raw = response.read(2*1024*1024+1)
            if len(raw) > 2*1024*1024: raise Deferred('maintenance response exceeds limit')
            return json.loads(raw)
        except (OSError, ValueError, urllib.error.URLError) as error:
            raise Deferred('maintenance API unavailable or invalid response') from None

    def incidents(self, after):
        if type(after) is not int or after < 0: raise InvalidResult('invalid incident cursor')
        value = self.request('GET', '/incidents?after='+str(after))
        if not isinstance(value, dict) or not isinstance(value.get('incidents'), list) or type(value.get('next_cursor')) is not int:
            raise Deferred('invalid maintenance outbox response')
        return value

    def evidence(self, incident_id):
        if type(incident_id) is not int or incident_id <= 0: raise InvalidResult('invalid incident ID')
        return self.request('GET', '/incidents/'+str(incident_id)+'/evidence')

    @staticmethod
    def receipt(value, key):
        if not isinstance(value, dict) or value.get('key') != key or value.get('state') not in ('pending', 'queued', 'sending', 'sent', 'ambiguous', 'failed'):
            raise Deferred('invalid notification receipt')
        if value['state'] == 'sent' and not value.get('telegram_message_id'):
            raise Deferred('notification lacks confirmed delivery receipt')
        return value

    def notify(self, payload):
        required = {'key', 'run_id', 'status'}
        if not required <= payload.keys() or payload.keys()-required-{'issue_number', 'pr_number', 'reason_code'}:
            raise InvalidResult('invalid notification fields')
        if not re.fullmatch('[a-f0-9]{64}', payload['key']) or payload['status'] not in ('pr_created', 'pr_ready', 'needs_human', 'paused', 'failed'):
            raise InvalidResult('invalid notification scope')
        identifier(payload['run_id'])
        if 'reason_code' in payload and (not isinstance(payload['reason_code'], str) or payload['reason_code'] not in REASONS):
            raise InvalidResult('invalid notification reason')
        for field in ('issue_number', 'pr_number'):
            if field in payload and (type(payload[field]) is not int or payload[field] <= 0): raise InvalidResult('invalid notification reference')
        return self.receipt(self.request('POST', '/notifications', payload), payload['key'])

    def notification(self, key):
        if not re.fullmatch('[a-f0-9]{64}', key): raise InvalidResult('invalid notification key')
        return self.receipt(self.request('GET', '/notifications/'+key), key)
