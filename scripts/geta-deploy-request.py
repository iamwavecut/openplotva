#!/usr/bin/env python3
"""Prepare a bounded deployment request. Credentials go to SSH stdin only."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import urllib.parse
import urllib.request

CONTRACT = 'deploy/geta-deploy.json'


def schema_hashes(directory):
    root = Path(directory)
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob('*.sql')) if p.is_file() and not p.is_symlink()}


def contract_digest(root):
    root = Path(root).resolve()
    contract = json.loads((root / CONTRACT).read_text())
    names = {CONTRACT, *contract.get('files', [])}
    for tree in contract.get('trees', []):
        path = (root / tree).resolve()
        if not path.is_relative_to(root) or not path.is_dir():
            raise ValueError('invalid contract tree')
        names.update(str(p.relative_to(root)) for p in path.rglob('*') if p.is_file())
    digest = hashlib.sha256()
    for name in sorted(names):
        path = root / name
        if path.is_symlink() or not path.resolve().is_relative_to(root) or not path.is_file():
            raise ValueError('invalid contract file')
        digest.update(name.encode() + b'\0' + path.read_bytes() + b'\0')
    return digest.hexdigest(), contract


def identity():
    address = os.environ['ACTIONS_ID_TOKEN_REQUEST_URL']
    if urllib.parse.urlsplit(address).scheme != 'https':
        raise ValueError('identity endpoint must use HTTPS')
    separator = '&' if '?' in address else '?'
    request = urllib.request.Request(
        address + separator + urllib.parse.urlencode({'audience': 'geta-deploy'}),
        headers={'Authorization': 'Bearer ' + os.environ['ACTIONS_ID_TOKEN_REQUEST_TOKEN']})
    with urllib.request.urlopen(request, timeout=15) as response:
        value = json.load(response).get('value')
    if not isinstance(value, str) or not value or len(value) > 16000:
        raise ValueError('invalid identity')
    return value


def payload(root, environment, get_identity):
    operation = environment.get('DEPLOY_OPERATION', 'deploy')
    if operation in ('rollback', 'backup'):
        return {'token': get_identity()}
    if operation != 'deploy' or not re.fullmatch(r'[0-9a-f]{40}', environment.get('DEPLOY_SHA', '')):
        raise ValueError('a deploy operation and immutable source commit are required')
    digest, contract = contract_digest(root)
    images = {service: environment.get(variable, '') for service, variable in contract['images'].items()}
    if not images or any(not re.fullmatch(r'sha256:[0-9a-f]{64}', value) for value in images.values()):
        raise ValueError('immutable image digests are required')
    result = {'token': get_identity(), 'images': images,
              'data_version': contract['data_version'], 'profile_digest': digest}
    if contract.get('registry_auth'):
        credential = environment.get('GETA_REGISTRY_TOKEN', '')
        if not credential:
            raise ValueError('scoped workflow registry credential required')
        result['registry_token'] = credential
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--digest', action='store_true')
    parser.add_argument('--check-schema', type=Path)
    args = parser.parse_args()
    if args.check_schema:
        _, contract = contract_digest(args.root)
        approved = json.loads((args.root / contract['approved_schema']).read_text())
        if not approved or approved != schema_hashes(args.check_schema):
            raise ValueError('database schema changed; coordinated administrator migration required')
        print('Approved database migration set verified')
    elif args.digest:
        print(contract_digest(args.root)[0])
    else:
        json.dump(payload(args.root, os.environ, identity), sys.stdout)
        sys.stdout.write('\n')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print('deployment contract or authenticated request validation failed', file=sys.stderr)
        raise SystemExit(1)
