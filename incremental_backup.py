"""Single-writer, daily snapshots + durable per-change outbox in Supabase Storage.
Never delete database history. Publish the recovery manifest before pruning objects.
"""
import gzip
import hashlib
import json
import os
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from memory_io import write_checkpoint, restore_checkpoint, validate_checkpoint

TRACKED = ('draws', 'compact_forecasts', 'wave_forecasts', 'parity_forecasts')


def setup(c, enable=False):
    c.execute('CREATE TABLE IF NOT EXISTS backup_outbox(seq INTEGER PRIMARY KEY AUTOINCREMENT, tbl TEXT NOT NULL, rid INTEGER NOT NULL)')
    c.execute('CREATE TABLE IF NOT EXISTS backup_pending_objects(key TEXT PRIMARY KEY)')
    for table in TRACKED:
        for action in ('insert', 'update'):
            name = 'backup_' + table + '_' + action
            c.execute('DROP TRIGGER IF EXISTS ' + name)
            if enable:
                c.execute(f"CREATE TRIGGER {name} AFTER {action.upper()} ON {table} BEGIN INSERT INTO backup_outbox(tbl,rid) VALUES ('{table}',NEW.rowid); END")
    c.commit()


def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(65536), b''): h.update(block)
    return h.hexdigest()


class Storage:
    def __init__(self, a):
        self.a = a
        self.base = a.SUPABASE_URL + '/storage/v1/object/' + quote(a.SUPABASE_BUCKET, safe='')

    def get_json(self, key):
        with self.a.requests.get(self.base + '/' + quote(key, safe='/'), headers=self.a._supabase_headers(), params={'fresh': str(time.time_ns())}, timeout=30) as r:
            if r.status_code == 404: return None
            if r.status_code == 400:
                try:error=r.json()
                except ValueError:error={}
                if str(error.get('statusCode')) == '404' or error.get('error') == 'not_found':return None
            r.raise_for_status()
            return r.json()

    def put(self, key, path, content_type='application/gzip'):
        headers = self.a._supabase_headers(content_type)
        headers.update({'x-upsert': 'true', 'cache-control': 'no-store'})
        with open(path, 'rb') as source:
            with self.a.requests.post(self.base + '/' + quote(key, safe='/'), headers=headers, data=source, timeout=60) as r:
                r.raise_for_status()

    def download(self, key, path):
        with self.a.requests.get(self.base + '/' + quote(key, safe='/'), headers=self.a._supabase_headers(), stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(path, 'wb') as output:
                for chunk in r.iter_content(65536): output.write(chunk)

    def delete(self, keys):
        for start in range(0, len(keys), 100):
            with self.a.requests.delete(self.base, headers=self.a._supabase_headers('application/json'), json={'prefixes': keys[start:start+100]}, timeout=30) as r:
                r.raise_for_status()


class Backup:
    def __init__(self, a, app_version, core_version, storage=None):
        self.a, self.app_version, self.core_version = a, app_version, core_version
        self.storage = storage or Storage(a)
        self.prefix = a.SUPABASE_OBJECT + '.incremental-v1/'
        self.manifest_key = self.prefix + 'manifest.json'
        self.manifest = None
        self.lock = threading.RLock()
        self.remote_restored = False

    def owned(self, key):
        return isinstance(key, str) and key.startswith(self.prefix) and '..' not in key and key != self.manifest_key

    def check_manifest(self, m):
        if m.get('format') != 1 or not m.get('snapshots'): raise ValueError('备份目录无效')
        for item in m['snapshots'] + m.get('deltas', []):
            if not self.owned(item['key']): raise ValueError('备份文件路径无效')
        seq = int(m['snapshots'][-1]['seq'])
        for item in m.get('deltas', []):
            if int(item['start']) != seq + 1 or int(item['seq']) < seq + 1:
                raise ValueError('增量备份序号不连续，停止恢复')
            seq = int(item['seq'])
        if seq != int(m['seq']): raise ValueError('备份进度不一致')

    def fetch(self, item, path):
        self.storage.download(item['key'], path)
        if digest(path) != item['sha256']: raise ValueError('备份校验失败')

    def restore(self):
        if not self.a.REMOTE_BACKUP_ENABLED: return False
        m = self.storage.get_json(self.manifest_key)
        if m is None: return False  # First upgrade uses the untouched legacy checkpoint.
        self.check_manifest(m)
        path = self.a.CHECKPOINT_PATH + '.recover'
        try:
            self.fetch(m['snapshots'][-1], path)
            validate_checkpoint(path)
            restore_checkpoint(self.a, path)
            os.replace(path, self.a.CHECKPOINT_PATH)
            for item in m.get('deltas', []):
                self.fetch(item, path)
                self.apply_delta(path)
            c = self.a.connect()
            try:
                history=c.execute('SELECT COUNT(*) AS n,MAX(issue) AS latest FROM draws').fetchone()
                if int(history['n']) < int(m['history_count']) or str(history['latest'] or '') < m['history_latest']:
                    raise ValueError('恢复后历史期号/总数核对失败')
                # Already published local events need not be uploaded again.
                c.execute('DELETE FROM backup_outbox WHERE seq<=?', (m['seq'],))
                row = c.execute("SELECT seq FROM sqlite_sequence WHERE name='backup_outbox'").fetchone()
                if row is None: c.execute("INSERT INTO sqlite_sequence(name,seq) VALUES ('backup_outbox',?)", (m['seq'],))
                else: c.execute("UPDATE sqlite_sequence SET seq=MAX(seq,?) WHERE name='backup_outbox'", (m['seq'],))
                c.commit()
            finally: c.close()
            self.manifest, self.remote_restored = m, True
            self.a.auto_state.update(remote_restore_at=time.strftime('%Y-%m-%d %H:%M:%S'), remote_backup_error='')
            return True
        finally:
            if os.path.exists(path): os.unlink(path)

    def apply_delta(self, path):
        c = self.a.connect()
        try:
            c.execute('BEGIN')
            allowed = {t: {r[1] for r in c.execute('PRAGMA table_info(' + t + ')')} for t in TRACKED}
            with gzip.open(path, 'rt', encoding='utf-8') as source:
                for line in source:
                    item = json.loads(line); table = item['table']; row = item['row']
                    if table not in allowed or not set(row).issubset(allowed[table]): raise ValueError('未知增量字段')
                    keys = list(row)
                    c.execute('INSERT OR IGNORE INTO ' + table + '(' + ','.join(keys) + ') VALUES (' + ','.join('?' for _ in keys) + ')', [row[k] for k in keys])
                    # Restore settlements without ever overwriting a previously locked recommendation.
                    actual = 'actual_wave' if table == 'wave_forecasts' else 'actual'
                    if table != 'draws' and row.get(actual) is not None:
                        c.execute(f'UPDATE {table} SET {actual}=? WHERE target=? AND version=? AND {actual} IS NULL', (row[actual], row['target'], row['version']))
            c.commit()
        except Exception:
            c.rollback(); raise
        finally: c.close()

    def remember(self, key):
        c=self.a.connect()
        try:
            c.execute('INSERT OR IGNORE INTO backup_pending_objects(key) VALUES (?)',(key,));c.commit()
        finally:c.close()

    def publish(self, m):
        path = self.a.CHECKPOINT_PATH + '.manifest.tmp'
        live={x['key'] for x in m['snapshots']+m.get('deltas',[])}
        c=self.a.connect()
        try:pending={x[0] for x in c.execute('SELECT key FROM backup_pending_objects')}
        finally:c.close()
        m=dict(m,garbage=sorted((set(m.get('garbage',[]))|pending)-live))
        try:
            with open(path, 'w', encoding='utf-8') as out: json.dump(m, out, separators=(',', ':'))
            self.storage.put(self.manifest_key, path, 'application/json')
            self.manifest = m
            c=self.a.connect()
            try:
                c.executemany('DELETE FROM backup_pending_objects WHERE key=?',[(key,) for key in live]);c.commit()
            finally:c.close()
        finally:
            if os.path.exists(path): os.unlink(path)

    def collect(self):
        garbage = self.manifest.get('garbage', [])
        if not garbage: return
        if not all(self.owned(key) for key in garbage): raise ValueError('清理范围无效')
        self.storage.delete(garbage)
        c=self.a.connect()
        try:
            c.executemany('DELETE FROM backup_pending_objects WHERE key=?',[(key,) for key in garbage]);c.commit()
        finally:c.close()
        self.publish(dict(self.manifest, garbage=[]))

    def sync(self, now=None):
        with self.lock:
            if not self.a.REMOTE_BACKUP_ENABLED:
                return write_checkpoint(self.a, self.app_version, self.core_version, remote_upload=False)
            try:
                day = (now or datetime.now(timezone(timedelta(hours=8)))).strftime('%Y-%m-%d')
                m = self.manifest
                if m is None or m['snapshots'][-1]['day'] != day:
                    self.snapshot(day)
                else:
                    self.delta()
                # Cleanup errors never invalidate a successfully published recovery chain.
                try:
                    self.collect(); self.a.auto_state['backup_cleanup_error'] = ''
                except Exception as exc:
                    self.a.auto_state['backup_cleanup_error'] = type(exc).__name__ + ': ' + str(exc)
                self.a.auto_state.update(remote_backup_at=time.strftime('%Y-%m-%d %H:%M:%S'), remote_backup_error='', backup_mode='daily_full_plus_delta', remote_backup_seq=self.manifest['seq'])
                return True
            except Exception as exc:
                self.a.auto_state['remote_backup_error'] = type(exc).__name__ + ': ' + str(exc)
                print('[BACKUP] incremental failed: ' + self.a.auto_state['remote_backup_error'], flush=True)
                return False

    def ack(self, seq):
        c = self.a.connect()
        try:
            c.execute('DELETE FROM backup_outbox WHERE seq<=?', (seq,)); c.commit()
        finally: c.close()

    def snapshot(self, day):
        if not write_checkpoint(self.a, self.app_version, self.core_version, remote_upload=False):
            raise RuntimeError('完整备份生成失败')
        path = self.a.CHECKPOINT_PATH
        validate_checkpoint(path)
        seq = int(self.a.auto_state['checkpoint_seq'])
        key = self.prefix + 'full-' + day + '-' + uuid.uuid4().hex + '.json.gz'
        item = {'day': day, 'seq': seq, 'key': key, 'sha256': digest(path)}
        self.remember(key)
        self.storage.put(key, path)
        old = self.manifest or {'snapshots': [], 'deltas': [], 'garbage': []}
        snapshots = old['snapshots'] + [item]
        garbage = old.get('garbage', []) + [x['key'] for x in snapshots[:-3]] + [x['key'] for x in old.get('deltas', [])]
        self.publish({'format': 1, 'seq': seq, 'snapshots': snapshots[-3:], 'deltas': [], 'garbage': garbage, 'history_count':self.a.auto_state['checkpoint_history_count'], 'history_latest':self.a.auto_state['checkpoint_history_latest']})
        self.ack(seq)
        self.a.auto_state['last_upload_bytes'] = os.path.getsize(path)

    def delta(self):
        start = int(self.manifest['seq'])
        path = self.a.CHECKPOINT_PATH + '.delta.tmp'
        c = self.a.connect()
        try:
            c.execute('BEGIN')
            end = c.execute('SELECT MAX(seq) FROM backup_outbox').fetchone()[0]
            if end is None or end <= start: return
            history=c.execute('SELECT COUNT(*) AS n,MAX(issue) AS latest FROM draws').fetchone()
            with gzip.open(path, 'wt', encoding='utf-8') as output:
                for table in TRACKED:
                    for row in c.execute(f'SELECT * FROM {table} WHERE rowid IN (SELECT rid FROM backup_outbox WHERE tbl=? AND seq>? AND seq<=?)', (table, start, end)):
                        output.write(json.dumps({'table': table, 'row': dict(row)}, ensure_ascii=False, separators=(',', ':')) + '\n')
        finally: c.close()
        try:
            key = self.prefix + f'delta-{start+1}-{end}-' + uuid.uuid4().hex + '.jsonl.gz'
            item = {'key': key, 'start': start + 1, 'seq': end, 'sha256': digest(path)}
            self.remember(key)
            self.storage.put(key, path)
            m = dict(self.manifest, seq=end, deltas=self.manifest.get('deltas', []) + [item], history_count=int(history['n']),history_latest=str(history['latest'] or ''))
            self.publish(m)
            self.ack(end)
            self.a.auto_state['last_upload_bytes'] = os.path.getsize(path)
        finally:
            if os.path.exists(path): os.unlink(path)
