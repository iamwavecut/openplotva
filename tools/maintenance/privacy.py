"""Private diagnostic context and the functional-only GitHub publication boundary."""
from __future__ import annotations

import copy
import html
import ipaddress
import json
import os
import re
import stat
import unicodedata
from urllib.parse import unquote, urlsplit

from .contracts import InvalidResult, REPOSITORY, text


IDENTITY_KEYS = {
    'identifiers', 'provider', 'providers', 'provider_name', 'model', 'models',
    'model_name', 'host', 'hostname', 'endpoint', 'base_url', 'image',
    'worker_id', 'container_id', 'request_id', 'job_id', 'reference',
}
URL = re.compile(r'https?://[^\s<>`"\)\]]+', re.I)
HOST = re.compile(r'(?<!\w)(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+(?:internal|local|lan|cloud|com|net|org|io|ai|moe|dev)(?::[0-9]+)?\b', re.I)
IPV4 = re.compile(r'(?<![\w.])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?::[0-9]+)?(?![\w.])')
IPV6 = re.compile(r'(?<![\w:])(?:[a-f0-9]{0,4}:){2,}[a-f0-9:.]+(?:%[a-z0-9]+)?', re.I)
UUID = re.compile(r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b', re.I)
INTERNAL_ID = re.compile(r'\b(?:provider|model|worker|container|request|job|user|chat|message|incident)[ _-]?(?:id|reference)\s*[:=#]\s*[`"\']?[-\w:.]+', re.I)
INCIDENT_REF = re.compile(r'\bincident:[0-9]+(?::[\w-]+)*', re.I)


def identity_values(value, selected=False):
    """Collect typed identifiers, never arbitrary prose under a private object."""
    if isinstance(value, dict):
        for key, child in value.items():
            yield from identity_values(child, key.lower() in IDENTITY_KEYS)
    elif isinstance(value, list):
        for child in value:
            yield from identity_values(child, selected)
    elif selected and isinstance(value, str) and 3 <= len(value) <= 512 and not value.isdecimal():
        yield value


class PublicationPrivacy:
    def __init__(self, values=()):
        self.values = frozenset(values)
        expressions = []
        for value in sorted(self.values, key=lambda item: (-len(item), item)):
            # Models and provider names are often written with spaces instead of
            # the separators in configuration. Match both, without substrings.
            parts = re.split(r'[\s._/-]+', value)
            expression = r'[\s._/-]+'.join(re.escape(part) for part in parts)
            expressions.append(r'(?<!\w)' + expression + r'(?!\w)')
        self.pattern = re.compile('|'.join(expressions), re.I) if expressions else None

    @classmethod
    def from_config(cls, config):
        inventory = config.get('private_inventory', {})
        path = config.get('private_inventory_file')
        if config.get('github_token_file') and not path:
            raise InvalidResult('authenticated publication requires a private identity inventory')
        if path:
            try:
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(fd, 'r') as handle:
                    info = os.fstat(handle.fileno())
                    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077 or info.st_size > 256*1024:
                        raise InvalidResult('private identity inventory must be a bounded owner-only regular file')
                    inventory = json.load(handle)
            except (OSError, ValueError):
                raise InvalidResult('private identity inventory is unavailable or invalid') from None
            if not isinstance(inventory, dict) or set(inventory) != {'identifiers'} or not isinstance(inventory['identifiers'], list) or not all(isinstance(item, str) for item in inventory['identifiers']):
                raise InvalidResult('private identity inventory must contain an identifiers array')
        return cls(identity_values(inventory))

    def with_context(self, job):
        context = job.get('context', {})
        scoped = {key: context.get(key, {}) for key in ('incident', 'evidence', 'deployed')}
        # Explicit identity references are accepted; private diagnoses and
        # public history are prose, not an identifier dictionary.
        scoped['identifiers'] = context.get('private', {}).get('identifiers', [])
        scoped['initial_evidence'] = context.get('private', {}).get('initial_evidence', {})
        return PublicationPrivacy(self.values | set(identity_values(scoped)))

    def redact(self, value, limit=60000):
        value = text(value, limit)
        normalized = value
        for _ in range(4):
            decoded = unicodedata.normalize('NFKC', html.unescape(unquote(normalized)))
            decoded = ''.join(char for char in decoded if unicodedata.category(char) != 'Cf')
            if decoded == normalized: break
            normalized = decoded
        else:
            raise InvalidResult('nested encoded publication text rejected')
        if normalized != value and self._redact(normalized, limit) != normalized:
            raise InvalidResult('encoded private identity rejected')
        return self._redact(value, limit)

    def _redact(self, value, limit):
        value = text(value, limit)
        def public_url(match):
            raw = match.group().rstrip('.,;')
            if self.pattern and self.pattern.search(raw): return '[private endpoint]'
            try: parsed = urlsplit(raw)
            except ValueError: return '[private endpoint]'
            tail = parsed.path+'#'+parsed.fragment
            if any(pattern.search(tail) for pattern in (HOST, IPV4, IPV6, UUID, INTERNAL_ID, INCIDENT_REF)):
                return '[private endpoint]'
            synthetic = parsed.hostname and (parsed.hostname.endswith(('.invalid', '.test')) or parsed.hostname == 'localhost')
            repo = (parsed.scheme == 'https' and parsed.netloc == 'github.com'
                    and re.fullmatch('/'+REPOSITORY+r'/(?:pull/[1-9][0-9]*|issues/[1-9][0-9]*|commit/[a-f0-9]{40}|blob/(?:main|[a-f0-9]{40})/[\w./-]+)', parsed.path)
                    and (not parsed.fragment or re.fullmatch(r'(?:L[0-9]+(?:-L[0-9]+)?|issuecomment-[0-9]+|discussion_r[0-9]+)', parsed.fragment)))
            if (synthetic or repo) and not parsed.username and not parsed.password and not parsed.query:
                return match.group()
            return '[private endpoint]'

        # Protect verified public repository links from the generic host guard.
        urls = []
        def protect_url(match):
            replacement = public_url(match)
            if replacement != match.group(): return replacement
            urls.append(replacement)
            return '\x01'+str(len(urls)-1)+'\x02'
        value = URL.sub(protect_url, value)
        if self.pattern:
            value = self.pattern.sub('[configured resource]', value)
        value = HOST.sub('[private host]', value)
        value = IPV4.sub('[private address]', value)
        def ipv6(match):
            try: ipaddress.IPv6Address(match.group().split('%')[0])
            except ValueError: return match.group()
            return '[private address]'
        value = IPV6.sub(ipv6, value)
        value = UUID.sub('[private reference]', value)
        value = INTERNAL_ID.sub('[private reference]', value)
        value = INCIDENT_REF.sub('[private reference]', value)
        for index, url in enumerate(urls):
            value = value.replace('\x01'+str(index)+'\x02', url)
        return text(value, limit)

    def assert_public(self, value, limit=60000):
        original = text(value, limit)
        if self.redact(original, limit) != original:
            raise InvalidResult('publication contains private infrastructure identifiers')
        return original

    def assert_patch_public(self, patch):
        try: value = patch.decode('utf-8')
        except UnicodeError: raise InvalidResult('patch is not UTF-8') from None
        # Filenames, removed lines and context are public too. Never silently
        # rewrite executable code or synthetic tests at publication time.
        self.assert_public(value, 2*1024*1024)


def boundary_for_job(config, job):
    return PublicationPrivacy.from_config(config or {}).with_context(job)


def hydrate_private_context(context, initial_job):
    context = copy.deepcopy(context)
    if initial_job is not None:
        private = context.setdefault('private', {})
        private['initial_diagnosis'] = copy.deepcopy(initial_job['result']['diagnosis'])
        private['initial_evidence'] = {key: copy.deepcopy(initial_job.get('context', {}).get(key, {}))
                                       for key in ('incident', 'evidence', 'deployed')}
    # The supervisor's complete inventory must never expand a worker's incident
    # scope. Exact current evidence and this same-incident diagnosis suffice.
    return context


def public_title(value, job, config=None):
    return boundary_for_job(config, job).redact(value, 180)


def public_feedback(value, job, config=None):
    return boundary_for_job(config, job).redact(value, 12000)


def public_issue_body(value, job, marker, config=None):
    boundary = boundary_for_job(config, job)
    sections = ['## Problem\n\n'+boundary.redact(value['summary'], 12000),
                'External cause: '+value['external_cause']+'. Code defect: '+value['code_defect']+'.']
    for key in ('observations', 'hypotheses', 'supporting', 'contradicting', 'related_changes', 'missing', 'acceptance'):
        items = [boundary.redact(item, 4000) for item in value[key]]
        sections.append('## '+key.replace('_', ' ').capitalize()+'\n\n'+('\n'.join('- '+item for item in items) or 'None identified.'))
    count = job.get('context', {}).get('incident', {}).get('count')
    impact = str(count)+' captured terminal events.' if type(count) is int and count > 0 else 'The number of affected terminal events is unavailable.'
    sections.append('## Impact\n\n'+impact)
    sections.append('Exact infrastructure identities and operational evidence remain in the private incident context available to the repair worker.')
    sections.append(marker)
    return boundary.assert_public('\n\n'.join(sections))
