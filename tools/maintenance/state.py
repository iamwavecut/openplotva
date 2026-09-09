"""Transactional, restart-safe controller state; no external IO inside transactions."""
from __future__ import annotations

import contextlib
import json
import sqlite3
import time
import uuid
from pathlib import Path

from .contracts import DAY, DEEP_LIMIT, DEEP_SECONDS, INITIAL_LIMIT, INITIAL_SECONDS, Deferred
from .quota import retry_after

TERMINAL = {'done', 'observing', 'needs_human', 'cancelled', 'ready', 'wait_deploy'}


class State:
    def __init__(self, path, clock=time.time):
        Path(path).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = str(path)
        self.db = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.clock = clock
        self.db.executescript('''
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        INSERT OR IGNORE INTO settings VALUES('enabled','false');
        INSERT OR IGNORE INTO settings VALUES('cursor','0');
        CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY,signature TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS incidents(signature TEXT PRIMARY KEY,data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY,issue_number INTEGER,status TEXT NOT NULL,data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS starts(job_id TEXT PRIMARY KEY,kind TEXT NOT NULL,at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS initial_launches(id INTEGER PRIMARY KEY,job_id TEXT NOT NULL,at REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS initial_launches_at ON initial_launches(at);
        INSERT INTO initial_launches(job_id,at) SELECT job_id,at FROM starts AS prior
          WHERE prior.kind='initial' AND NOT EXISTS(SELECT 1 FROM initial_launches WHERE job_id=prior.job_id);
        CREATE TABLE IF NOT EXISTS origins(marker TEXT PRIMARY KEY,issue_number INTEGER UNIQUE,signature TEXT NOT NULL,incident_id INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS dispatches(event_id TEXT PRIMARY KEY,issue_number INTEGER NOT NULL,job_id TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS queue_reservations(issue_number INTEGER NOT NULL,generation TEXT NOT NULL,job_id TEXT NOT NULL,PRIMARY KEY(issue_number,generation));
        CREATE TABLE IF NOT EXISTS effects(key TEXT PRIMARY KEY,data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS notifications(key TEXT PRIMARY KEY,data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS history(kind TEXT NOT NULL,number INTEGER NOT NULL,data TEXT NOT NULL,PRIMARY KEY(kind,number));
        ''')
        self.db.execute("UPDATE jobs SET data=json_set(data,'$.stage','revise') WHERE json_extract(data,'$.stage')='review'")

    def close(self): self.db.close()

    @contextlib.contextmanager
    def transaction(self):
        if self.db.in_transaction:
            yield
            return
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    def setting(self, key, default=None):
        row = self.db.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_setting(self, key, value):
        self.db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)', (key, json.dumps(value)))

    def enabled(self): return self.setting('enabled', False)
    def set_enabled(self, value): self.set_setting('enabled', bool(value))
    def cursor(self): return self.setting('cursor', 0)

    def provider_available(self):
        return not self.setting('review_slot') and self.setting('provider_quota', {}).get('until', 0) <= self.clock()

    def defer_provider(self, source, delay=None, at=None):
        with self.transaction():
            previous = self.setting('provider_quota', {})
            if previous.get('source') == source:
                return previous
            failures = min(16, previous.get('failures', 0) + 1)
            delay = retry_after(delay) or min(3600, 600 * 2 ** (failures - 1))
            at = self.clock() if at is None else at
            value = {'source': source, 'failures': failures, 'since': previous.get('since', at),
                     'observed_at': max(previous.get('observed_at', at), at),
                     'until': max(previous.get('until', 0), at + delay), 'retry_after_seconds': delay}
            self.set_setting('provider_quota', value)
            return value

    def provider_recovered(self, started_at=None):
        if started_at is None or started_at >= self.setting('provider_quota', {}).get('observed_at', 0):
            self.set_setting('provider_quota', {})

    def ingest(self, events, cursor, debounce):
        with self.transaction():
            for event in events:
                if type(event.get('id')) is not int or event['id'] <= 0 or not isinstance(event.get('signature'), str):
                    raise ValueError('invalid outbox event')
                existing = self.db.execute('SELECT signature FROM events WHERE id=?', (event['id'],)).fetchone()
                if existing:
                    if existing[0] != event['signature']: raise ValueError('immutable outbox ID changed')
                    continue
                self.db.execute('INSERT INTO events VALUES(?,?)', (event['id'], event['signature']))
                previous = self.incident(event['signature'])
                data = previous or {'signature': event['signature'], 'first_seen': event['first_seen'], 'count': 0,
                                    'status': 'pending', 'due': self.clock() + debounce}
                data.update(incident_id=event['id'], last_seen=event['last_seen'], snapshot=event['snapshot'], count=data['count']+1)
                if previous and (
                    previous['status'] in ('observing','done','wait_deploy','needs_human')
                    or previous['status']=='diagnosing' and not any(
                        job['signature']==event['signature'] and job['status'] not in TERMINAL for job in self.jobs())
                ):
                    # A new immutable event can reassess a terminal job; replayed IDs returned above.
                    data.update(status='pending', due=self.clock()+debounce)
                self.put_incident(data)
            if type(cursor) is not int or cursor < self.cursor() or (events and cursor < max(e['id'] for e in events)):
                raise ValueError('invalid outbox cursor')
            self.set_setting('cursor', cursor)

    def put_incident(self, data):
        self.db.execute('INSERT OR REPLACE INTO incidents VALUES(?,?)', (data['signature'], json.dumps(data)))

    def incident(self, signature):
        row = self.db.execute('SELECT data FROM incidents WHERE signature=?', (signature,)).fetchone()
        return json.loads(row[0]) if row else None

    def incidents(self): return [json.loads(r[0]) for r in self.db.execute('SELECT data FROM incidents')]
    def due_incidents(self): return [i for i in self.incidents() if i['status'] == 'pending' and i['due'] <= self.clock()]

    def new_job(self, stage, signature, incident_id, **fields):
        job = {'id': uuid.uuid4().hex, 'stage': stage, 'signature': signature, 'incident_id': incident_id,
               'status': 'queued', 'issue_number': None, 'pr_number': None, 'active_seconds': 0,
               'rounds': 0, 'attempts': 0, 'next_at': 0, 'cancelled': False, 'usage': {}, **fields}
        self.save_job(job)
        return job

    def save_job(self, job):
        with self.transaction():
            existing=self.db.execute('SELECT data FROM jobs WHERE id=?',(job['id'],)).fetchone()
            # Cancellation cannot be cleared by a stale service result or saved snapshot.
            if job.get('cancelled') or existing and json.loads(existing[0])['cancelled']:
                job.update(cancelled=True,status='cancelled')
            self.db.execute('INSERT OR REPLACE INTO jobs VALUES(?,?,?,?)',
                            (job['id'], job.get('issue_number'), job['status'], json.dumps(job)))

    def job(self, job_id):
        row = self.db.execute('SELECT data FROM jobs WHERE id=?', (job_id,)).fetchone()
        if not row: raise ValueError('unknown job')
        return json.loads(row[0])

    def jobs(self, statuses=None):
        rows = [json.loads(r[0]) for r in self.db.execute('SELECT data FROM jobs')]
        return [j for j in rows if statuses is None or j['status'] in statuses]

    def update_job(self, job_id, **fields):
        with self.transaction():
            job = self.job(job_id)
            job.update(fields)
            self.save_job(job)
            return job

    def cancelled(self, job_id):
        # The runner's polling thread never shares the service transaction connection.
        with contextlib.closing(sqlite3.connect(self.path,timeout=30)) as connection:
            row=connection.execute('SELECT data FROM jobs WHERE id=?',(job_id,)).fetchone()
        return not row or json.loads(row[0])['cancelled']

    def stop_requested(self, job_id):
        with contextlib.closing(sqlite3.connect(self.path, timeout=30)) as connection:
            row = connection.execute('SELECT data FROM jobs WHERE id=?', (job_id,)).fetchone()
            if not row: return True
            job = json.loads(row[0])
            hold = connection.execute('SELECT value FROM settings WHERE key=?',
                                      ('owner_hold_'+str(job.get('issue_number')),)).fetchone()
            return job['cancelled'] or (job['stage'] != 'triage' and bool(hold and json.loads(hold[0])))

    def owner_hold(self, issue_number):
        return self.setting('owner_hold_'+str(issue_number), False)

    def cancel(self, job_id):
        with self.transaction(): self.update_job(job_id, cancelled=True, status='cancelled')

    def claim(self, job_id):
        with self.transaction():
            job = self.job(job_id)
            if not self.enabled(): raise Deferred('new agent starts are disabled')
            if not self.provider_available(): raise Deferred('waiting for shared Coding Plan quota')
            if job['cancelled'] or job['status'] != 'queued': raise Deferred('job cannot start')
            if self.jobs({'running'}): raise Deferred('another agent owns the durable lease')
            short = job['stage'] in ('initial', 'triage')
            limit = INITIAL_SECONDS if short else DEEP_SECONDS
            if not short and job.get('issue_number'):
                limit-=sum(j['active_seconds'] for j in self.jobs() if j['id']!=job_id and j['stage'] in ('deep', 'revise') and j.get('issue_number')==job['issue_number'])
            if job['active_seconds'] >= limit: raise Deferred('active budget exhausted')
            if short and not job.get('quota_resume'):
                count=self.db.execute('SELECT count(*) FROM initial_launches WHERE at>?',(self.clock()-DAY,)).fetchone()[0]
                if count>=INITIAL_LIMIT: raise Deferred('rolling daily quota exhausted')
                self.db.execute('INSERT INTO initial_launches(job_id,at) VALUES(?,?)',(job_id,self.clock()))
            if not self.db.execute('SELECT 1 FROM starts WHERE job_id=?', (job_id,)).fetchone():
                kind = 'initial' if short else 'deep'
                if kind=='deep':
                    count=self.db.execute("SELECT count(*) FROM starts WHERE kind='deep' AND at>?",(self.clock()-DAY,)).fetchone()[0]
                    if count>=DEEP_LIMIT: raise Deferred('rolling daily quota exhausted')
                self.db.execute('INSERT INTO starts VALUES(?,?,?)', (job_id, kind, self.clock()))
            return self.update_job(job_id, status='running', lease_started=self.clock(), remaining_seconds=limit-job['active_seconds'])

    def issue_usage(self, issue_number):
        jobs=[j for j in self.jobs() if j['stage'] in ('deep', 'revise') and j.get('issue_number')==issue_number]
        started={r[0] for r in self.db.execute('SELECT job_id FROM starts')}
        return {'active_seconds':sum(j['active_seconds'] for j in jobs),
                'cycles':sum(1+j['rounds'] for j in jobs if j['id'] in started)}

    def finish_run(self, job_id, active_seconds, usage):
        with self.transaction():
            job = self.job(job_id)
            elapsed = max(0, self.clock()-job.get('lease_started', self.clock()), float(active_seconds))
            limit = INITIAL_SECONDS if job['stage'] in ('initial', 'triage') else DEEP_SECONDS
            total = dict(job['usage'])
            for key, value in usage.items():
                if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
                    total[key] = total.get(key, 0) + value
            return self.update_job(job_id, active_seconds=min(limit, job['active_seconds']+elapsed),
                                   usage=total, lease_started=None, status='cancelled' if job['cancelled'] else 'processing')

    def recover(self):
        for job in [j for j in self.jobs() if j.get('lease_started') is not None]:
            self.finish_run(job['id'], 0, {})
            if not job['cancelled']: self.update_job(job['id'], status='result' if job.get('result') else 'queued')
        for job in self.jobs({'processing'}):
            self.update_job(job['id'], status='result' if job.get('result') else 'queued')

    def record_origin(self, marker, issue_number, signature, incident_id):
        self.db.execute('INSERT INTO origins VALUES(?,?,?,?) ON CONFLICT(marker) DO UPDATE SET issue_number=excluded.issue_number',
                        (marker, issue_number, signature, incident_id))

    def origin(self, issue_number):
        row = self.db.execute('SELECT * FROM origins WHERE issue_number=?', (issue_number,)).fetchone()
        return dict(row) if row else None

    def origins_for_signature(self, signature):
        return [dict(row) for row in self.db.execute(
            'SELECT * FROM origins WHERE signature=? ORDER BY issue_number', (signature,))]

    def enqueue(self, issue_number, event_id, generation=None):
        with self.transaction():
            row = self.db.execute('SELECT job_id,issue_number FROM dispatches WHERE event_id=?', (event_id,)).fetchone()
            if row:
                if row['issue_number'] != issue_number: raise ValueError('dispatch reused for another issue')
                return self.job(row['job_id'])
            origin = self.origin(issue_number)
            if not origin: raise ValueError('issue has no durable controller origin')
            reserved = self.db.execute('SELECT job_id FROM queue_reservations WHERE issue_number=? AND generation=?', (issue_number,generation)).fetchone() if generation else None
            if reserved:
                self.db.execute('INSERT INTO dispatches VALUES(?,?,?)', (event_id,issue_number,reserved[0]))
                return self.job(reserved[0])
            manual_resume = str(event_id).isdigit() and self.owner_hold(issue_number)
            if manual_resume:
                for previous in self.jobs():
                    if previous['stage'] == 'triage' and previous['issue_number'] == issue_number and previous['status'] not in TERMINAL:
                        self.cancel(previous['id'])
                self.set_setting('owner_hold_'+str(issue_number), False)
            active = [j for j in self.jobs() if j.get('issue_number') == issue_number
                      and j['stage'] in ('deep', 'revise') and j['status'] not in TERMINAL]
            job = active[0] if active else self.new_job('deep', origin['signature'], origin['incident_id'], issue_number=issue_number)
            if manual_resume and active and job['status'] != 'running':
                started = self.db.execute('SELECT 1 FROM starts WHERE job_id=?', (job['id'],)).fetchone()
                job = self.update_job(job['id'], status='queued', result=None, prepared=None, next_at=0, attempts=0,
                                      rounds=job['rounds']+(1 if started else 0))
            self.db.execute('INSERT INTO dispatches VALUES(?,?,?)', (event_id, issue_number, job['id']))
            if generation: self.db.execute('INSERT INTO queue_reservations VALUES(?,?,?)', (issue_number,generation,job['id']))
            return job

    def record(self, table, key):
        if table not in ('effects', 'notifications'): raise ValueError('invalid record table')
        row = self.db.execute('SELECT data FROM '+table+' WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def put_record(self, table, key, value):
        if table not in ('effects', 'notifications'): raise ValueError('invalid record table')
        self.db.execute('INSERT OR REPLACE INTO '+table+' VALUES(?,?)', (key, json.dumps(value)))

    def records(self, table):
        if table not in ('effects', 'notifications'): raise ValueError('invalid record table')
        return [json.loads(row[0]) for row in self.db.execute('SELECT data FROM '+table)]

    def replace_history(self, items):
        with self.transaction():
            self.db.execute('DELETE FROM history')
            self.db.executemany('INSERT INTO history VALUES(?,?,?)', [(i['kind'], i['number'], json.dumps(i)) for i in items])

    def history(self): return [json.loads(r[0]) for r in self.db.execute('SELECT data FROM history')]

    def status(self):
        counts, usage = {}, {}
        jobs = self.jobs()
        for job in jobs:
            counts[job['status']] = counts.get(job['status'], 0)+1
            for key, value in job['usage'].items(): usage[key] = usage.get(key, 0)+value
        return {'enabled': self.enabled(), 'cursor': self.cursor(), 'jobs': counts,
                'owner_holds': [int(row['key'].removeprefix('owner_hold_')) for row in
                    self.db.execute("SELECT key,value FROM settings WHERE key LIKE 'owner_hold_%'")
                    if row['key'].removeprefix('owner_hold_').isdigit() and json.loads(row['value'])],
                'active_seconds': sum(j['active_seconds'] for j in jobs), 'usage': usage,
                'provider_quota': self.setting('provider_quota', {}),
                'review_waits': self.setting('review_waits', {}),
                'starts': {'initial':self.db.execute('SELECT count(*) FROM initial_launches WHERE at>?',(self.clock()-DAY,)).fetchone()[0],
                           'deep':self.db.execute("SELECT count(*) FROM starts WHERE kind='deep' AND at>?",(self.clock()-DAY,)).fetchone()[0]},
                'job_status': [{'id':j['id'],'stage':j['stage'],'status':j['status'],'issue_number':j.get('issue_number'),
                                'pr_number':j.get('pr_number'),'active_seconds':j['active_seconds'],'rounds':j['rounds']} for j in jobs],
                'pending_notifications': sum(n['state'] != 'sent' for n in self.records('notifications'))}
