"""Bounded-memory checkpoints, compatible with existing gzip JSON backups."""
import gzip
import json
import os
import time
import resource
import sys
import ijson

def peak_rss_mib():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(value / (1024 * 1024 if sys.platform == 'darwin' else 1024), 1)


TABLES = ('draws', 'compact_forecasts', 'wave_forecasts', 'parity_forecasts', 'learner_scores')


def write_checkpoint(a, app_version, core_version, remote_upload=True):
    started = time.perf_counter()
    tmp = a.CHECKPOINT_PATH + '.tmp'
    try:
        os.makedirs(os.path.dirname(os.path.abspath(tmp)), exist_ok=True)
        header = {'version': app_version, 'core_version': core_version,
                  'created_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                  'persistent_mode': a.PERSISTENT_MODE,
                  'learning': a.export_learning_payload(1500)}
        c = a.connect()
        try:
            c.execute('BEGIN')  # Consistent SQLite read snapshot across all tables.
            exists=c.execute("SELECT 1 FROM sqlite_master WHERE name='backup_outbox'").fetchone()
            seq=c.execute("SELECT seq FROM sqlite_sequence WHERE name='backup_outbox'").fetchone() if exists else None
            header['backup_seq']=int(seq[0]) if seq else 0
            history=c.execute('SELECT COUNT(*) AS n,MAX(issue) AS latest FROM draws').fetchone()
            header['history_count']=int(history['n']);header['history_latest']=str(history['latest'] or '')
            with gzip.open(tmp, 'wt', encoding='utf-8') as output:
                output.write('{')
                first = True
                for key, value in header.items():
                    if not first: output.write(',')
                    first = False
                    output.write(json.dumps(key) + ':')
                    json.dump(value, output, ensure_ascii=False, separators=(',', ':'))
                for table in TABLES:
                    output.write(',' + json.dumps(table) + ':[')
                    first_row = True
                    for row in c.execute('SELECT * FROM ' + table):
                        if not first_row: output.write(',')
                        first_row = False
                        json.dump(dict(row), output, ensure_ascii=False, separators=(',', ':'))
                    output.write(']')
                output.write('}')
        finally:
            c.close()
        os.replace(tmp, a.CHECKPOINT_PATH)
        a.auto_state.update(checkpoint_at=time.strftime('%Y-%m-%d %H:%M:%S'), checkpoint_error='', checkpoint_seq=header['backup_seq'], checkpoint_history_count=header['history_count'], checkpoint_history_latest=header['history_latest'])
        if remote_upload and a.REMOTE_BACKUP_ENABLED:
            a.upload_checkpoint_to_supabase()
        print(f'[PERF] checkpoint seconds={time.perf_counter()-started:.3f} process_peak_rss_mib={peak_rss_mib()}', flush=True)
        return True
    except Exception as exc:
        a.auto_state['checkpoint_error'] = f'{type(exc).__name__}: {exc}'
        print('[BACKUP] checkpoint failed: ' + a.auto_state['checkpoint_error'], flush=True)
        return False
    finally:
        if os.path.exists(tmp): os.unlink(tmp)


def objects(path):
    """Yield a single row at a time; never materialize the root JSON object."""
    paths = {table + '.item' for table in TABLES}
    paths.update(('learning.logs.item', 'learning.audits.item', 'learning.correction.item', 'learning.ai'))
    with gzip.open(path, 'rb') as source:
        builder = None
        depth = 0
        for prefix, event, value in ijson.parse(source, use_float=True):
            if builder is None:
                if prefix not in paths or event != 'start_map': continue
                builder = ijson.ObjectBuilder()
                active = prefix
                depth = 0
            builder.event(event, value)
            if event in ('start_map', 'start_array'): depth += 1
            elif event in ('end_map', 'end_array'): depth -= 1
            if depth == 0:
                yield active, builder.value
                builder = None


def restore_checkpoint(a, path, include_learning=True):
    if not os.path.isfile(path): return 0
    learning = {'logs': [], 'audits': [], 'correction': [], 'ai': None}
    with a.db_lock:
        c = a.connect()
        try:
            columns = {table: [row[1] for row in c.execute('PRAGMA table_info(' + table + ')')] for table in TABLES}
            inserted = 0
            c.execute('BEGIN')
            for prefix, row in objects(path):
                if prefix.startswith('learning.'):
                    if not include_learning: continue
                    key = prefix.split('.')[1]
                    if key == 'ai': learning[key] = row
                    else: learning[key].append(row)
                    continue
                table = prefix.split('.')[0]
                keys = [key for key in columns[table] if key in row]
                if not keys: continue
                before = c.total_changes
                c.execute('INSERT OR IGNORE INTO ' + table + '(' + ','.join(keys) + ') VALUES (' + ','.join('?' for _ in keys) + ')', [row[key] for key in keys])
                inserted += c.total_changes - before
            c.commit()
        except Exception:
            c.rollback()
            raise
        finally:
            c.close()
        if include_learning:
            a.import_learning_payload(learning)
            a.load_ai_state()
            a.load_correction_state()
    return inserted


def validate_checkpoint(path):
    learning = False
    with gzip.open(path, 'rb') as source:
        for prefix, event, value in ijson.parse(source):
            if prefix == 'learning' and event == 'start_map': learning = True
    if not learning: raise ValueError('invalid remote checkpoint: missing learning object')


def download_file(a, url, path, headers, timeout=25):
    with a.requests.get(url, headers=headers, timeout=timeout, stream=True) as response:
        response.raise_for_status()
        with open(path, 'wb') as output:
            for chunk in response.iter_content(chunk_size=64 * 1024):
                output.write(chunk)
    validate_checkpoint(path)
