import os, re, csv, sqlite3, threading, math, time, hashlib, json, gzip, itertools
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from flask import Flask, jsonify, render_template_string, request, send_file
import requests
PERSIST_DIR = os.getenv('PERSIST_DIR', '').strip()
if not PERSIST_DIR:
    for _candidate in ('/var/data', '/data'):
        if os.path.isdir(_candidate) and os.access(_candidate, os.W_OK):
            PERSIST_DIR = _candidate
            break
DB = os.getenv('DB_PATH', '').strip()
if not DB:
    DB = os.path.join(PERSIST_DIR, 'history.db') if PERSIST_DIR else 'history.db'
CHECKPOINT_PATH = os.getenv('CHECKPOINT_PATH', '').strip()
if not CHECKPOINT_PATH:
    CHECKPOINT_PATH = os.path.join(PERSIST_DIR, 'sanfen_ai_checkpoint.json.gz') if PERSIST_DIR else 'sanfen_ai_checkpoint.json.gz'
PERSISTENT_MODE = bool(PERSIST_DIR and os.path.abspath(DB).startswith(os.path.abspath(PERSIST_DIR)) and os.path.abspath(CHECKPOINT_PATH).startswith(os.path.abspath(PERSIST_DIR)))
SUPABASE_URL = os.getenv('SUPABASE_URL', '').strip().rstrip('/')
SUPABASE_SERVICE_ROLE_KEY = ''.join(os.getenv('SUPABASE_SERVICE_ROLE_KEY', '').split())
SUPABASE_BUCKET = os.getenv('SUPABASE_BUCKET', 'sanfen-backup').strip() or 'sanfen-backup'
SUPABASE_OBJECT = os.getenv('SUPABASE_OBJECT', 'sanfen_ai_checkpoint.json.gz').strip() or 'sanfen_ai_checkpoint.json.gz'
REMOTE_BACKUP_ENABLED = bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY and SUPABASE_BUCKET and SUPABASE_OBJECT)
BOT_TOKEN = os.getenv('BOT_TOKEN', '').strip()
ALLOWED_CHAT_ID = os.getenv('ALLOWED_CHAT_ID', '').strip()
PORT = int(os.getenv('PORT', '10000'))
RENDER_SERVICE_NAME = os.getenv('RENDER_SERVICE_NAME', '').strip()
RENDER_EXTERNAL_URL = os.getenv('RENDER_EXTERNAL_URL', '').rstrip('/')
WEBHOOK_BASE_URL = os.getenv('WEBHOOK_BASE_URL', '').rstrip('/')
if not WEBHOOK_BASE_URL:
    WEBHOOK_BASE_URL = RENDER_EXTERNAL_URL
if not WEBHOOK_BASE_URL and RENDER_SERVICE_NAME:
    WEBHOOK_BASE_URL = f'https://{RENDER_SERVICE_NAME}.onrender.com'
WEBHOOK_PATH = '/telegram/webhook'
WEBHOOK_SECRET = hashlib.sha256(BOT_TOKEN.encode('utf-8')).hexdigest()[:48] if BOT_TOKEN else ''
TG_API = f'https://api.telegram.org/bot{BOT_TOKEN}' if BOT_TOKEN else ''

def _issue_add(issue, steps=1):
    """Add periods for YYYYMMDDNNN ids, 001..480 per day."""
    raw = str(issue or '').strip()
    try:
        if re.fullmatch('\\d{11}', raw):
            day = raw[:8]
            seq = int(raw[8:])
            if 1 <= seq <= 480:
                dt = datetime.strptime(day, '%Y%m%d')
                zero = seq - 1 + int(steps)
                day_shift, idx = divmod(zero, 480)
                dt += timedelta(days=day_shift)
                return dt.strftime('%Y%m%d') + f'{idx + 1:03d}'
        return str(int(raw) + int(steps))
    except Exception:
        return raw

def _next_issue_id(issue):
    return _issue_add(issue, 1)
DEFAULT_HISTORY_SOURCE = 'https://sanfencai.onrender.com/api/export?limit=30000'
HISTORY_SOURCE_URL = os.getenv('HISTORY_SOURCE_URL', DEFAULT_HISTORY_SOURCE).strip()
background_state = {'profile_calibrating': False, 'stats_building': False, 'boot_ready': False}
history_cache = {'total': 0, 'items': [], 'loaded_at': ''}
history_cache_lock = threading.RLock()
sync_state = {'source': HISTORY_SOURCE_URL, 'imported': 0, 'last_error': '', 'last_sync': ''}
app = Flask(__name__)
db_lock = threading.RLock()
learner_lock = threading.RLock()
learner_cache = {'ready': False, 'best_profile': '平衡', 'profiles': {}, 'settled': 0, 'window': 60, 'updated_at': ''}
AI_FEATURES = ['bias', 'sp6', 'sp12', 'sp24', 'sp60', 'all12', 'all36', 'gap', 'transition', 'tail', 'longprior', 'wave', 'size', 'parity', 'head', 'cold', 'repeat', 'zodiac_pair', 'wave_parity_8', 'wave_parity_16', 'wave_parity_36', 'wave_parity_80']
ai_lock = threading.RLock()
ai_state = {'weights': [0.0] * len(AI_FEATURES), 'steps': 0, 'trained': 0, 'last_issue': '', 'lr': 0.08, 'ready': False, 'bootstrapping': False, 'historical_validation_n': 0, 'historical_hit24': 0.0, 'rolling_window': 100, 'rolling_trained': 0, 'refit_count': 0, 'last_refit_seconds': 0.0, 'updated_at': ''}
correction_lock = threading.RLock()
correction_state = {'20': {'weights': [0.0] * len(AI_FEATURES), 'steps': 0, 'trained': 0, 'updated_at': ''}, '27': {'weights': [0.0] * len(AI_FEATURES), 'steps': 0, 'trained': 0, 'updated_at': ''}}
auto_state = {'last_draw_issue': '', 'last_prediction_issue': '', 'last_learning_issue': '', 'last_stats_issue': '', 'last_refresh_at': '', 'last_error': '', 'updating': False, 'checkpoint_at': '', 'checkpoint_error': '', 'remote_backup_at': '', 'remote_restore_at': '', 'remote_backup_error': ''}

def ensure_db_dir():
    parent = os.path.dirname(os.path.abspath(DB))
    os.makedirs(parent, exist_ok=True)

def connect():
    ensure_db_dir()
    c = sqlite3.connect(DB, timeout=20, check_same_thread=False)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA busy_timeout=20000')
    c.execute('PRAGMA synchronous=NORMAL')
    c.execute('PRAGMA temp_store=MEMORY')
    return c

def _db_retry(fn, attempts=8):
    last = None
    for i in range(attempts):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            last = e
            if 'locked' not in str(e).lower() and 'busy' not in str(e).lower():
                raise
            time.sleep(min(0.08 * 2 ** i, 1.2))
    raise last

def init_db():
    with db_lock:
        c = connect()
        _db_retry(lambda: c.execute('PRAGMA journal_mode=WAL').fetchone())
        c.execute('PRAGMA wal_autocheckpoint=800')
        c.execute('CREATE TABLE IF NOT EXISTS draws(\n          issue TEXT PRIMARY KEY,\n          n1 INTEGER,n2 INTEGER,n3 INTEGER,n4 INTEGER,n5 INTEGER,n6 INTEGER,special INTEGER,\n          z1 TEXT,z2 TEXT,z3 TEXT,z4 TEXT,z5 TEXT,z6 TEXT,z7 TEXT,\n          c1 TEXT,c2 TEXT,c3 TEXT,c4 TEXT,c5 TEXT,c6 TEXT,c7 TEXT,\n          raw TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)')
        c.execute('CREATE TABLE IF NOT EXISTS prediction_log(\n          target_issue TEXT NOT NULL,\n          profile TEXT NOT NULL,\n          special24 TEXT NOT NULL,\n          main4 TEXT NOT NULL,\n          zodiac4 TEXT NOT NULL,\n          pingte TEXT,\n          created_at TEXT DEFAULT CURRENT_TIMESTAMP,\n          settled INTEGER DEFAULT 0,\n          hit24 INTEGER,\n          hitmain INTEGER,\n          hitz INTEGER,\n          hitping INTEGER,\n          actual_special INTEGER,\n          actual_zodiac TEXT,\n          PRIMARY KEY(target_issue,profile)\n        )')
        c.execute('CREATE TABLE IF NOT EXISTS rule_counterfactual(\n          draw_id TEXT NOT NULL,\n          group_id TEXT NOT NULL,\n          model_version TEXT NOT NULL,\n          rule_id TEXT NOT NULL,\n          rule_version TEXT NOT NULL,\n          experiment_id TEXT NOT NULL,\n          cohort_start_draw TEXT,\n          history_cutoff_id TEXT NOT NULL,\n          locked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,\n          locked_before_draw INTEGER NOT NULL DEFAULT 1,\n          base_code_count INTEGER NOT NULL,\n          rule_code_count INTEGER NOT NULL,\n          base_codes TEXT NOT NULL,\n          rule_codes TEXT NOT NULL,\n          result_special INTEGER,\n          base_hit INTEGER,\n          rule_hit INTEGER,\n          rescue INTEGER,\n          harm INTEGER,\n          same_win INTEGER,\n          same_lose INTEGER,\n          net_contribution INTEGER,\n          created_at TEXT DEFAULT CURRENT_TIMESTAMP,\n          settled INTEGER DEFAULT 0,\n          PRIMARY KEY(draw_id,group_id,rule_id,rule_version)\n        )')
        c.execute('CREATE INDEX IF NOT EXISTS idx_rule_cf_rule\n                     ON rule_counterfactual(group_id,rule_id,rule_version,settled,draw_id)')
        c.execute('CREATE TABLE IF NOT EXISTS v68_forecast(\n          target_issue TEXT NOT NULL,\n          ranker_id TEXT NOT NULL,\n          model_version TEXT NOT NULL,\n          history_cutoff_id TEXT NOT NULL,\n          locked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,\n          probs_json TEXT NOT NULL,\n          ranked49 TEXT NOT NULL,\n          top16 TEXT NOT NULL,\n          top22 TEXT NOT NULL,\n          top27 TEXT NOT NULL,\n          settled INTEGER DEFAULT 0,\n          actual_special INTEGER,\n          actual_rank INTEGER,\n          log_loss REAL,\n          brier REAL,\n          hit16 INTEGER,\n          hit22 INTEGER,\n          hit27 INTEGER,\n          PRIMARY KEY(target_issue,ranker_id,model_version)\n        )')
        c.execute('CREATE INDEX IF NOT EXISTS idx_v68_forecast_ranker\n                     ON v68_forecast(ranker_id,model_version,settled,target_issue)')
        c.execute('CREATE TABLE IF NOT EXISTS learner_scores(\n          profile TEXT PRIMARY KEY,\n          weight REAL DEFAULT 1.0,\n          n INTEGER DEFAULT 0,\n          hit24 INTEGER DEFAULT 0,\n          hitmain INTEGER DEFAULT 0,\n          hitz INTEGER DEFAULT 0,\n          hitping INTEGER DEFAULT 0,\n          score REAL DEFAULT 0.0,\n          updated_at TEXT DEFAULT CURRENT_TIMESTAMP\n        )')
        c.execute('CREATE TABLE IF NOT EXISTS ai_model(\n          id INTEGER PRIMARY KEY CHECK(id=1),\n          weights TEXT NOT NULL,\n          steps INTEGER DEFAULT 0,\n          trained INTEGER DEFAULT 0,\n          last_issue TEXT,\n          lr REAL DEFAULT 0.08,\n          historical_validation_n INTEGER DEFAULT 0,\n          historical_hit24 REAL DEFAULT 0.0,\n          updated_at TEXT DEFAULT CURRENT_TIMESTAMP\n        )')
        c.execute('CREATE TABLE IF NOT EXISTS correction_model(\n          strategy TEXT PRIMARY KEY,\n          weights TEXT NOT NULL,\n          steps INTEGER DEFAULT 0,\n          trained INTEGER DEFAULT 0,\n          updated_at TEXT DEFAULT CURRENT_TIMESTAMP\n        )')
        for _strategy in ('20', '27'):
            c.execute('INSERT OR IGNORE INTO correction_model\n              (strategy,weights,steps,trained)\n              VALUES (?,?,0,0)', (_strategy, json.dumps([0.0] * len(AI_FEATURES))))
        c.execute('CREATE TABLE IF NOT EXISTS strategy_audit(\n          target_issue TEXT NOT NULL,\n          profile TEXT NOT NULL,\n          ranked49 TEXT,\n          selected_codes TEXT,\n          excluded_zodiacs TEXT,\n          killed_head TEXT,\n          regime TEXT,\n          created_at TEXT DEFAULT CURRENT_TIMESTAMP,\n          settled INTEGER DEFAULT 0,\n          actual_special INTEGER,\n          actual_zodiac TEXT,\n          actual_rank INTEGER,\n          selected_hit INTEGER,\n          cold_zodiac_error INTEGER,\n          head_kill_success INTEGER,\n          failure_reason TEXT,\n          PRIMARY KEY(target_issue,profile)\n        )')
        c.execute("INSERT OR IGNORE INTO ai_model\n          (id,weights,steps,trained,last_issue,lr)\n          VALUES (1,?,0,0,'',0.08)", (json.dumps([0.0] * len(AI_FEATURES)),))
        for _sql in ['ALTER TABLE ai_model ADD COLUMN rolling_window INTEGER DEFAULT 100', 'ALTER TABLE ai_model ADD COLUMN rolling_trained INTEGER DEFAULT 0', 'ALTER TABLE ai_model ADD COLUMN refit_count INTEGER DEFAULT 0', 'ALTER TABLE ai_model ADD COLUMN last_refit_seconds REAL DEFAULT 0.0', 'ALTER TABLE strategy_audit ADD COLUMN snapshot_json TEXT']:
            try:
                c.execute(_sql)
            except sqlite3.OperationalError:
                pass
        for _p in ['趋势快', '平衡', '热码', '结构', '均衡覆盖']:
            c.execute('INSERT OR IGNORE INTO learner_scores(profile,weight) VALUES (?,1.0)', (_p,))
        c.commit()
        c.close()

def normalize_z(z):
    return {'馬': '马', '龍': '龙', '雞': '鸡', '豬': '猪'}.get(z, z)

def import_history_once():
    if not os.path.exists('history.csv'):
        return
    with db_lock:
        c = connect()
        existing = c.execute('SELECT COUNT(*) FROM draws').fetchone()[0]
        if existing:
            c.close()
            return
        with open('history.csv', 'r', encoding='utf-8-sig', newline='') as f:
            for r in csv.DictReader(f):
                issue = (r.get('期号') or '').strip()
                try:
                    nums = [int(r[k]) for k in ['正码1', '正码2', '正码3', '正码4', '正码5', '正码6', '特码']]
                except Exception:
                    continue
                if not issue or not all((1 <= n <= 49 for n in nums)):
                    continue
                zs = [normalize_z((r.get(k) or '').strip()) for k in ['正码1生肖', '正码2生肖', '正码3生肖', '正码4生肖', '正码5生肖', '正码6生肖']]
                zs.append(normalize_z((r.get('生肖') or '').strip()))
                c.execute('INSERT OR IGNORE INTO draws\n                (issue,n1,n2,n3,n4,n5,n6,special,z1,z2,z3,z4,z5,z6,z7,raw)\n                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', [issue, *nums, *zs, 'history.csv'])
        c.commit()
        c.close()

def _insert_migrated_draw(c, item):
    """Accept v12 /api/history item or v13 /api/export full row."""
    issue = str(item.get('issue') or item.get('期号') or '').strip()
    if not issue:
        return 0
    if all((k in item for k in ['n1', 'n2', 'n3', 'n4', 'n5', 'n6', 'special'])):
        try:
            nums = [int(item[f'n{i}']) for i in range(1, 7)] + [int(item['special'])]
        except Exception:
            return 0
        zs = [normalize_z(str(item.get(f'z{i}') or '')) for i in range(1, 8)]
        cs = [str(item.get(f'c{i}') or '') for i in range(1, 8)]
        raw = str(item.get('raw') or 'remote-export')
        created = str(item.get('created_at') or '')
    else:
        try:
            nums = [int(x) for x in item.get('numbers', [])]
        except Exception:
            return 0
        if len(nums) != 7:
            return 0
        zs = ['', '', '', '', '', '', normalize_z(str(item.get('zodiac') or ''))]
        cs = [''] * 7
        raw = 'remote-history-migration'
        created = str(item.get('created_at') or '')
    if len(set(nums)) != 7 or not all((1 <= n <= 49 for n in nums)):
        return 0
    before = c.total_changes
    c.execute("INSERT OR IGNORE INTO draws\n      (issue,n1,n2,n3,n4,n5,n6,special,z1,z2,z3,z4,z5,z6,z7,\n       c1,c2,c3,c4,c5,c6,c7,raw,created_at)\n      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP))", [issue, *nums, *zs, *cs, raw, created])
    return 1 if c.total_changes > before else 0

def sync_remote_history(url=None):
    """Merge history from the currently-live old service before switching versions."""
    url = (url or HISTORY_SOURCE_URL or '').strip()
    if not url:
        return 0
    try:
        r = requests.get(url, timeout=25, headers={'User-Agent': 'SanfenMigration/13'})
        r.raise_for_status()
        payload = r.json()
        items = payload.get('items') if isinstance(payload, dict) else payload
        if not isinstance(items, list):
            raise ValueError('remote history response has no items list')
        imported = 0
        with db_lock:
            c = connect()
            for item in items:
                if isinstance(item, dict):
                    imported += _insert_migrated_draw(c, item)
            c.commit()
            c.close()
        sync_state['remote_total'] = int(payload.get('total', 0)) if isinstance(payload, dict) else 0
        sync_state['remote_latest'] = max((str(x.get('issue') or x.get('期号') or '') for x in items if isinstance(x, dict)), default='')
        sync_state['source'] = url
        sync_state['imported'] = imported
        sync_state['last_error'] = ''
        sync_state['last_sync'] = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f'[SYNC] imported={imported} from {url}', flush=True)
        return imported
    except Exception as e:
        sync_state['source'] = url
        sync_state['last_error'] = f'{type(e).__name__}: {e}'
        sync_state['last_sync'] = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f'[SYNC] failed from {url}: {type(e).__name__}: {e}', flush=True)
        return 0

def sync_best_available_history():
    """Migrate from the deployment that is live right now, then configured fallbacks.
    On Render zero-downtime deploys, our public URL normally still serves the old
    deployment while this new worker is booting. This keeps bot-collected history
    across v13 -> v14 and later same-service upgrades."""
    urls = []
    if WEBHOOK_BASE_URL:
        urls += [WEBHOOK_BASE_URL + '/api/export?limit=30000', WEBHOOK_BASE_URL + '/api/history?limit=500']
    if HISTORY_SOURCE_URL:
        if '/api/' in HISTORY_SOURCE_URL:
            base = HISTORY_SOURCE_URL.split('/api/', 1)[0]
            urls += [base + '/api/export?limit=30000']
        urls += [HISTORY_SOURCE_URL]
    seen = set()
    total = 0
    for u in urls:
        if not u or u in seen:
            continue
        seen.add(u)
        n = sync_remote_history(u)
        total += n
        if not sync_state['last_error']:
            break
    return total

def parse_draw(text):
    m = re.search('第\\s*[:：]?\\s*(\\d{8,})\\s*期', text)
    if not m:
        return None
    issue = m.group(1)
    nums = None
    for line in [x.strip() for x in text.splitlines() if x.strip()]:
        vals = re.findall('(?<!\\d)(?:0?[1-9]|[1-4]\\d)(?!\\d)', line)
        if len(vals) == 7:
            cand = [int(x) for x in vals]
            if len(set(cand)) == 7 and all((1 <= n <= 49 for n in cand)):
                nums = cand
                break
    if not nums:
        return None
    zs = re.findall('[鼠牛虎兔龙龍蛇马馬羊猴鸡雞狗猪豬]', text)
    zs = [normalize_z(x) for x in zs[-7:]] if len(zs) >= 7 else [''] * 7
    colors = re.findall('[🔴🟢🔵]', text)
    colors = colors[-7:] if len(colors) >= 7 else [''] * 7
    return (issue, nums, zs, colors)

def add_draw(*args):
    return False

def latest_row():

    def _read():
        c = connect()
        try:
            return c.execute('SELECT * FROM draws\n                                ORDER BY CAST(issue AS INTEGER) DESC LIMIT 1').fetchone()
        finally:
            c.close()
    return _db_retry(_read)

def recent_rows(limit=1200):
    """Read only the recent history needed by the live predictor."""

    def _read():
        c = connect()
        try:
            return c.execute('SELECT * FROM draws\n                                ORDER BY CAST(issue AS INTEGER) DESC\n                                LIMIT ?', (int(limit),)).fetchall()
        finally:
            c.close()
    return _db_retry(_read)

def refresh_history_cache(limit=500):

    def _read():
        c = connect()
        try:
            total = c.execute('SELECT COUNT(*) FROM draws').fetchone()[0]
            rr = c.execute('SELECT issue,n1,n2,n3,n4,n5,n6,special,z7,created_at\n                            FROM draws ORDER BY CAST(issue AS INTEGER) DESC LIMIT ?', (limit,)).fetchall()
            return (total, rr)
        finally:
            c.close()
    total, rr = _db_retry(_read)
    items = []
    for x in rr:
        items.append({'issue': x['issue'], 'numbers': [x[f'n{i}'] for i in range(1, 7)] + [x['special']], 'zodiac': normalize_z(x['z7'] or ''), 'created_at': x['created_at'] or ''})
    with history_cache_lock:
        history_cache['total'] = total
        history_cache['items'] = items
        history_cache['loaded_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
    return total
PROFILE_LIBRARY = {'趋势快': {'recent': 1.3, 'accel': 1.35, 'wave': 1.2, 'size': 1.05, 'parity': 1.0, 'zodiac': 1.1, 'long': 0.55, 'omit': 0.45}, '平衡': {'recent': 1.0, 'accel': 1.0, 'wave': 1.0, 'size': 1.0, 'parity': 1.0, 'zodiac': 1.0, 'long': 1.0, 'omit': 0.75}, '热码': {'recent': 1.45, 'accel': 0.9, 'wave': 0.7, 'size': 0.65, 'parity': 0.65, 'zodiac': 0.8, 'long': 0.45, 'omit': 0.25}, '结构': {'recent': 0.88, 'accel': 1.05, 'wave': 1.45, 'size': 1.3, 'parity': 1.25, 'zodiac': 1.35, 'long': 0.7, 'omit': 0.45}, '均衡覆盖': {'recent': 0.82, 'accel': 0.82, 'wave': 1.1, 'size': 1.1, 'parity': 1.1, 'zodiac': 1.4, 'long': 1.1, 'omit': 0.9}}

def load_ai_state():
    with db_lock:
        c = connect()
        try:
            row = c.execute('SELECT * FROM ai_model WHERE id=1').fetchone()
        finally:
            c.close()
    if not row:
        return
    try:
        w = json.loads(row['weights'] or '[]')
        if len(w) < len(AI_FEATURES):
            w = list(w) + [0.0] * (len(AI_FEATURES) - len(w))
        elif len(w) > len(AI_FEATURES):
            w = list(w)[:len(AI_FEATURES)]
    except Exception:
        w = [0.0] * len(AI_FEATURES)
    with ai_lock:
        ai_state['weights'] = [float(x) for x in w]
        ai_state['steps'] = int(row['steps'] or 0)
        ai_state['trained'] = int(row['trained'] or 0)
        ai_state['last_issue'] = str(row['last_issue'] or '')
        ai_state['lr'] = float(row['lr'] or 0.08)
        ai_state['ready'] = ai_state['trained'] > 0
        ai_state['historical_validation_n'] = int(row['historical_validation_n'] or 0)
        ai_state['historical_hit24'] = float(row['historical_hit24'] or 0.0)
        ai_state['rolling_window'] = int(row['rolling_window'] or 100) if 'rolling_window' in row.keys() else 100
        ai_state['rolling_trained'] = int(row['rolling_trained'] or 0) if 'rolling_trained' in row.keys() else 0
        ai_state['refit_count'] = int(row['refit_count'] or 0) if 'refit_count' in row.keys() else 0
        ai_state['last_refit_seconds'] = float(row['last_refit_seconds'] or 0.0) if 'last_refit_seconds' in row.keys() else 0.0
        ai_state['updated_at'] = str(row['updated_at'] or '')

def load_correction_state():
    with db_lock:
        c = connect()
        try:
            rows = c.execute('SELECT * FROM correction_model').fetchall()
        finally:
            c.close()
    with correction_lock:
        for row in rows:
            strategy = str(row['strategy'])
            if strategy not in correction_state:
                continue
            try:
                w = json.loads(row['weights'] or '[]')
            except Exception:
                w = []
            if len(w) < len(AI_FEATURES):
                w = list(w) + [0.0] * (len(AI_FEATURES) - len(w))
            elif len(w) > len(AI_FEATURES):
                w = list(w)[:len(AI_FEATURES)]
            correction_state[strategy] = {'weights': [float(x) for x in w], 'steps': int(row['steps'] or 0), 'trained': int(row['trained'] or 0), 'updated_at': str(row['updated_at'] or '')}

def refresh_learner_cache(window=60):
    """Use only forecasts that were saved before their outcomes were known."""
    result = {}
    with db_lock:
        c = connect()
        try:
            for profile in PROFILE_LIBRARY:
                rows = c.execute('SELECT hit24,hitmain,hitz,hitping\n                                  FROM prediction_log\n                                  WHERE profile=? AND settled=1\n                                  ORDER BY CAST(target_issue AS INTEGER) DESC\n                                  LIMIT ?', (profile, window)).fetchall()
                n = len(rows)
                h24 = sum((int(x['hit24'] or 0) for x in rows))
                hm = sum((int(x['hitmain'] or 0) for x in rows))
                hz = sum((int(x['hitz'] or 0) for x in rows))
                hp = sum((int(x['hitping'] or 0) for x in rows))
                r24 = (h24 + 4 * (24 / 49)) / (n + 4)
                rm = (hm + 4 * (4 / 49)) / (n + 4)
                rz = (hz + 4 * (4 / 12)) / (n + 4)
                rp = (hp + 4 * 0.5) / (n + 4)
                score = 0.88 * r24 + 0.03 * rm + 0.05 * rz + 0.04 * rp
                weight = math.exp(5.0 * (score - 0.49))
                result[profile] = {'n': n, 'hit24': h24, 'hitmain': hm, 'hitz': hz, 'hitping': hp, 'rate24': round(h24 / n * 100, 1) if n else 0.0, 'rateMain': round(hm / n * 100, 1) if n else 0.0, 'rateZ': round(hz / n * 100, 1) if n else 0.0, 'ratePing': round(hp / n * 100, 1) if n else 0.0, 'score': score, 'weight': weight}
                c.execute('INSERT INTO learner_scores\n                  (profile,weight,n,hit24,hitmain,hitz,hitping,score,updated_at)\n                  VALUES (?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)\n                  ON CONFLICT(profile) DO UPDATE SET\n                    weight=excluded.weight,n=excluded.n,hit24=excluded.hit24,\n                    hitmain=excluded.hitmain,hitz=excluded.hitz,hitping=excluded.hitping,\n                    score=excluded.score,updated_at=CURRENT_TIMESTAMP', (profile, weight, n, h24, hm, hz, hp, score))
            c.commit()
        finally:
            c.close()
    best = max(PROFILE_LIBRARY.keys(), key=lambda p: (result[p]['score'], result[p]['rate24'], p)) if result else '平衡'
    settled = max((v['n'] for v in result.values()), default=0)
    with learner_lock:
        learner_cache.update({'ready': settled > 0, 'best_profile': best, 'profiles': result, 'settled': settled, 'window': window, 'updated_at': time.strftime('%Y-%m-%d %H:%M:%S')})
    return (best, result)

def export_learning_payload(limit=1000):
    with db_lock:
        c = connect()
        try:
            rows = c.execute('SELECT * FROM prediction_log\n                              ORDER BY CAST(target_issue AS INTEGER) DESC, profile\n                              LIMIT ?', (int(limit),)).fetchall()
            airow = c.execute('SELECT * FROM ai_model WHERE id=1').fetchone()
            audits = c.execute('SELECT * FROM strategy_audit\n                                ORDER BY CAST(target_issue AS INTEGER) DESC,profile\n                                LIMIT 1000').fetchall()
            corrections = c.execute('SELECT * FROM correction_model ORDER BY strategy').fetchall()
            return {'logs': [{k: x[k] for k in x.keys()} for x in rows], 'ai': {k: airow[k] for k in airow.keys()} if airow else None, 'audits': [{k: x[k] for k in x.keys()} for x in audits], 'correction': [{k: x[k] for k in x.keys()} for x in corrections]}
        finally:
            c.close()

def _supabase_headers(content_type=None):
    key = SUPABASE_SERVICE_ROLE_KEY
    h = {'apikey': key, 'Cache-Control': 'no-cache'}
    if key and (not key.startswith('sb_secret_')):
        h['Authorization'] = f'Bearer {key}'
    if content_type:
        h['Content-Type'] = content_type
    return h

def upload_checkpoint_to_supabase():
    if not REMOTE_BACKUP_ENABLED or not os.path.exists(CHECKPOINT_PATH):
        return False
    try:
        size = os.path.getsize(CHECKPOINT_PATH)
        url = f'{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{SUPABASE_OBJECT}'
        h = _supabase_headers('application/gzip')
        h['x-upsert'] = 'true'
        with open(CHECKPOINT_PATH, 'rb') as f:
            r = requests.post(url, headers=h, data=f, timeout=25)
        if r.status_code not in (200, 201):
            raise RuntimeError(f'HTTP {r.status_code}: {r.text[:220]}')
        auto_state['remote_backup_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
        auto_state['remote_backup_error'] = ''
        print(f'[REMOTE] Supabase backup OK bytes={size}', flush=True)
        return True
    except Exception as e:
        auto_state['remote_backup_error'] = f'{type(e).__name__}: {e}'
        print(f"[REMOTE] backup failed: {auto_state['remote_backup_error']}", flush=True)
        return False

def download_checkpoint_from_supabase():
    if not REMOTE_BACKUP_ENABLED:
        return False
    try:
        url = f'{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{SUPABASE_OBJECT}'
        from memory_io import download_file
        parent = os.path.dirname(os.path.abspath(CHECKPOINT_PATH))
        os.makedirs(parent, exist_ok=True)
        tmp = CHECKPOINT_PATH + '.remote.tmp'
        download_file(sys.modules[__name__], url, tmp, _supabase_headers())
        os.replace(tmp, CHECKPOINT_PATH)
        auto_state['remote_restore_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
        auto_state['remote_backup_error'] = ''
        print(f'[REMOTE] Supabase restore OK bytes={os.path.getsize(CHECKPOINT_PATH)}', flush=True)
        return True
    except Exception as e:
        auto_state['remote_backup_error'] = f'restore {type(e).__name__}: {e}'
        print(f"[REMOTE] restore failed: {auto_state['remote_backup_error']}", flush=True)
        return False

def _checkpoint_payload():
    learning = export_learning_payload(1500)
    with db_lock:
        c = connect()
        try:
            rows = c.execute('SELECT * FROM draws\n                              ORDER BY CAST(issue AS INTEGER) DESC\n                              LIMIT 1400').fetchall()
            scores = c.execute('SELECT * FROM learner_scores ORDER BY profile').fetchall()
        finally:
            c.close()
    return {'version': 'v67', 'created_at': time.strftime('%Y-%m-%d %H:%M:%S'), 'persistent_mode': PERSISTENT_MODE, 'learning': learning, 'draws': [{k: r[k] for k in r.keys()} for r in rows], 'learner_scores': [{k: r[k] for k in r.keys()} for r in scores]}

def write_checkpoint_atomic():
    """Atomic learning checkpoint. On a persistent disk this survives deploy/restart."""
    try:
        parent = os.path.dirname(os.path.abspath(CHECKPOINT_PATH))
        os.makedirs(parent, exist_ok=True)
        payload = _checkpoint_payload()
        tmp = CHECKPOINT_PATH + '.tmp'
        with gzip.open(tmp, 'wt', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))
        os.replace(tmp, CHECKPOINT_PATH)
        auto_state['checkpoint_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
        auto_state['checkpoint_error'] = ''
        if REMOTE_BACKUP_ENABLED:
            upload_checkpoint_to_supabase()
        return True
    except Exception as e:
        auto_state['checkpoint_error'] = f'{type(e).__name__}: {e}'
        print(f"[BACKUP] checkpoint failed: {auto_state['checkpoint_error']}", flush=True)
        return False

def _restore_draw_dicts(items):
    if not items:
        return 0
    cols = ['issue', 'n1', 'n2', 'n3', 'n4', 'n5', 'n6', 'special', 'z1', 'z2', 'z3', 'z4', 'z5', 'z6', 'z7', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'raw', 'created_at']
    sql = f"INSERT OR IGNORE INTO draws({','.join(cols)})\n             VALUES ({','.join(['?'] * len(cols))})"
    inserted = 0
    with db_lock:
        c = connect()
        try:
            for x in items:
                before = c.total_changes
                vals = [x.get(col) for col in cols]
                c.execute(sql, vals)
                inserted += int(c.total_changes > before)
            c.commit()
        finally:
            c.close()
    return inserted

def restore_checkpoint_if_better():
    """Restore automatically when checkpoint contains more learning history than DB."""
    if not os.path.exists(CHECKPOINT_PATH):
        return 0
    try:
        with gzip.open(CHECKPOINT_PATH, 'rt', encoding='utf-8') as f:
            payload = json.load(f)
        learning = payload.get('learning', {}) or {}
        ck_logs = learning.get('logs', []) or []
        with db_lock:
            c = connect()
            try:
                current_logs = int(c.execute('SELECT COUNT(*) AS n FROM prediction_log').fetchone()['n'])
            finally:
                c.close()
        restored = 0
        restored += import_learning_payload(learning)
        restored += _restore_draw_dicts(payload.get('draws', []) or [])
        load_ai_state()
        load_correction_state()
        refresh_learner_cache(60)
        print(f'[BACKUP] restored checkpoint logs={len(ck_logs)} inserted={restored}', flush=True)
        return restored
    except Exception as e:
        print(f'[BACKUP] restore failed: {type(e).__name__}: {e}', flush=True)
        return 0

def import_learning_payload(payload):
    logs = payload.get('logs', []) if isinstance(payload, dict) else []
    ai_payload = payload.get('ai') if isinstance(payload, dict) else None
    audits = payload.get('audits', []) if isinstance(payload, dict) else []
    correction_payload = payload.get('correction', []) if isinstance(payload, dict) else []
    inserted = 0
    with db_lock:
        c = connect()
        try:
            for x in logs:
                before = c.total_changes
                c.execute("INSERT OR IGNORE INTO prediction_log\n                  (target_issue,profile,special24,main4,zodiac4,pingte,created_at,\n                   settled,hit24,hitmain,hitz,hitping,actual_special,actual_zodiac)\n                  VALUES (?,?,?,?,?,?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP),\n                          ?,?,?,?,?,?,?)", (str(x.get('target_issue', '')), str(x.get('profile', '')), str(x.get('special24', '')), str(x.get('main4', '')), str(x.get('zodiac4', '')), str(x.get('pingte', '')), str(x.get('created_at', '')), int(x.get('settled') or 0), x.get('hit24'), x.get('hitmain'), x.get('hitz'), x.get('hitping'), x.get('actual_special'), str(x.get('actual_zodiac') or '')))
                inserted += int(c.total_changes > before)
            c.commit()
        finally:
            c.close()
    if ai_payload:
        try:
            weights = str(ai_payload.get('weights') or '[]')
            parsed = json.loads(weights)
            if len(parsed) < len(AI_FEATURES):
                parsed = list(parsed) + [0.0] * (len(AI_FEATURES) - len(parsed))
                weights = json.dumps(parsed, separators=(',', ':'))
            elif len(parsed) > len(AI_FEATURES):
                parsed = list(parsed)[:len(AI_FEATURES)]
                weights = json.dumps(parsed, separators=(',', ':'))
            if len(parsed) == len(AI_FEATURES):
                with db_lock:
                    c = connect()
                    try:
                        c.execute('UPDATE ai_model SET weights=?,steps=?,trained=?,\n                                     last_issue=?,lr=?,historical_validation_n=?,\n                                     historical_hit24=?,rolling_window=?,rolling_trained=?,\n                                     refit_count=?,last_refit_seconds=?,\n                                     updated_at=CURRENT_TIMESTAMP WHERE id=1', (weights, int(ai_payload.get('steps') or 0), int(ai_payload.get('trained') or 0), str(ai_payload.get('last_issue') or ''), float(ai_payload.get('lr') or 0.08), int(ai_payload.get('historical_validation_n') or 0), float(ai_payload.get('historical_hit24') or 0.0), int(ai_payload.get('rolling_window') or 100), int(ai_payload.get('rolling_trained') or 0), int(ai_payload.get('refit_count') or 0), float(ai_payload.get('last_refit_seconds') or 0.0)))
                        c.commit()
                    finally:
                        c.close()
                load_ai_state()
        except Exception as e:
            print(f'[AI] inherited state failed: {e}', flush=True)
    if audits:
        with db_lock:
            c = connect()
            try:
                for x in audits:
                    c.execute("INSERT INTO strategy_audit\n                      (target_issue,profile,ranked49,selected_codes,excluded_zodiacs,\n                       killed_head,regime,created_at,settled,actual_special,actual_zodiac,\n                       actual_rank,selected_hit,cold_zodiac_error,head_kill_success,failure_reason)\n                      VALUES (?,?,?,?,?,?,?,COALESCE(NULLIF(?,''),CURRENT_TIMESTAMP),\n                              ?,?,?,?,?,?,?,?)\n                      ON CONFLICT(target_issue,profile) DO UPDATE SET\n                        ranked49=excluded.ranked49,\n                        selected_codes=excluded.selected_codes,\n                        excluded_zodiacs=excluded.excluded_zodiacs,\n                        killed_head=excluded.killed_head,\n                        regime=excluded.regime,\n                        settled=MAX(strategy_audit.settled,excluded.settled),\n                        actual_special=COALESCE(excluded.actual_special,strategy_audit.actual_special),\n                        actual_zodiac=COALESCE(NULLIF(excluded.actual_zodiac,''),strategy_audit.actual_zodiac),\n                        actual_rank=COALESCE(excluded.actual_rank,strategy_audit.actual_rank),\n                        selected_hit=COALESCE(excluded.selected_hit,strategy_audit.selected_hit),\n                        cold_zodiac_error=COALESCE(excluded.cold_zodiac_error,strategy_audit.cold_zodiac_error),\n                        head_kill_success=COALESCE(excluded.head_kill_success,strategy_audit.head_kill_success),\n                        failure_reason=COALESCE(NULLIF(excluded.failure_reason,''),strategy_audit.failure_reason)", (str(x.get('target_issue', '')), str(x.get('profile', '')), str(x.get('ranked49', '')), str(x.get('selected_codes', '')), str(x.get('excluded_zodiacs', '')), str(x.get('killed_head', '')), str(x.get('regime', '')), str(x.get('created_at', '')), int(x.get('settled') or 0), x.get('actual_special'), str(x.get('actual_zodiac') or ''), x.get('actual_rank'), x.get('selected_hit'), x.get('cold_zodiac_error'), x.get('head_kill_success'), str(x.get('failure_reason') or '')))
                c.commit()
            finally:
                c.close()
    if correction_payload:
        with db_lock:
            c = connect()
            try:
                for x in correction_payload:
                    strategy = str(x.get('strategy') or '')
                    if strategy not in ('20', '27'):
                        continue
                    incoming = int(x.get('trained') or 0)
                    cur = c.execute('SELECT trained FROM correction_model WHERE strategy=?', (strategy,)).fetchone()
                    current = int(cur['trained'] or 0) if cur else 0
                    if incoming >= current:
                        weights = str(x.get('weights') or '[]')
                        try:
                            parsed = json.loads(weights)
                        except Exception:
                            parsed = []
                        if len(parsed) < len(AI_FEATURES):
                            parsed = list(parsed) + [0.0] * (len(AI_FEATURES) - len(parsed))
                        elif len(parsed) > len(AI_FEATURES):
                            parsed = list(parsed)[:len(AI_FEATURES)]
                        c.execute('INSERT INTO correction_model(strategy,weights,steps,trained,updated_at)\n                                     VALUES (?,?,?,?,CURRENT_TIMESTAMP)\n                                     ON CONFLICT(strategy) DO UPDATE SET\n                                       weights=excluded.weights,steps=excluded.steps,\n                                       trained=excluded.trained,updated_at=CURRENT_TIMESTAMP', (strategy, json.dumps(parsed, separators=(',', ':')), int(x.get('steps') or 0), incoming))
                c.commit()
            finally:
                c.close()
        load_correction_state()
    refresh_learner_cache(60)
    return inserted

def model():
    c = connect()
    try:
        r = c.execute('SELECT COUNT(*) n,MAX(issue) issue FROM draws').fetchone()
        return {'count': r['n'], 'issue': r['issue']}
    finally:
        c.close()

@app.get('/')
def home():
    return ''

@app.get('/api/health')
def health():
    return jsonify(ok=True)

@app.get('/api/prediction')
def prediction():
    return jsonify({})

@app.get('/api/backup-status')
def backup_status():
    exists = os.path.exists(CHECKPOINT_PATH)
    try:
        size = os.path.getsize(CHECKPOINT_PATH) if exists else 0
    except Exception:
        size = 0
    return jsonify({'persistent': PERSISTENT_MODE, 'persist_dir': PERSIST_DIR, 'db_path': DB, 'checkpoint_path': CHECKPOINT_PATH, 'checkpoint_exists': exists, 'checkpoint_bytes': size, 'checkpoint_at': auto_state.get('checkpoint_at', ''), 'checkpoint_error': auto_state.get('checkpoint_error', ''), 'remote_backup_enabled': REMOTE_BACKUP_ENABLED, 'remote_provider': 'Supabase Storage' if REMOTE_BACKUP_ENABLED else '', 'remote_bucket': SUPABASE_BUCKET if REMOTE_BACKUP_ENABLED else '', 'remote_object': SUPABASE_OBJECT if REMOTE_BACKUP_ENABLED else '', 'remote_backup_at': auto_state.get('remote_backup_at', ''), 'remote_restore_at': auto_state.get('remote_restore_at', ''), 'remote_backup_error': auto_state.get('remote_backup_error', ''), 'backup_mode': auto_state.get('backup_mode',''), 'remote_backup_seq': auto_state.get('remote_backup_seq'), 'last_upload_bytes': auto_state.get('last_upload_bytes'), 'cleanup_error': auto_state.get('backup_cleanup_error','')})

@app.get('/api/full-backup')
def full_backup():
    return jsonify({})

@app.get('/api/history')
def history():
    try:
        limit = max(20, min(int(request.args.get('limit', '200')), 500))
    except Exception:
        limit = 200
    with history_cache_lock:
        total = history_cache.get('total', 0)
        items = list(history_cache.get('items', []))[:limit]
        loaded_at = history_cache.get('loaded_at', '')
    if not items:
        try:
            refresh_history_cache()
            with history_cache_lock:
                total = history_cache.get('total', 0)
                items = list(history_cache.get('items', []))[:limit]
                loaded_at = history_cache.get('loaded_at', '')
        except Exception as e:
            return (jsonify({'total': 0, 'items': [], 'error': str(e)}), 503)
    resp = jsonify({'total': total, 'items': items, 'cache_time': loaded_at})
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    return resp

@app.get('/api/export')
def export_history():
    try:
        limit = max(20, min(int(request.args.get('limit', '20000')), 30000))
    except Exception:
        limit = 20000

    def _read():
        c = connect()
        try:
            rr = c.execute('SELECT * FROM draws\n                            ORDER BY CAST(issue AS INTEGER) DESC LIMIT ?', (limit,)).fetchall()
            total = c.execute('SELECT COUNT(*) FROM draws').fetchone()[0]
            return (total, rr)
        finally:
            c.close()
    total, rr = _db_retry(_read)
    items = [{k: x[k] for k in x.keys()} for x in rr]
    return jsonify({'total': total, 'items': items})

@app.get('/api/sync-status')
def sync_status():
    with history_cache_lock:
        count = history_cache.get('total', 0)
    return jsonify({**sync_state, 'history_count': count})

@app.post('/api/sync-history')
def sync_history_now():
    body = request.get_json(silent=True) or {}
    n = sync_remote_history(str(body.get('url') or HISTORY_SOURCE_URL or ''))
    refresh_history_cache()
    return jsonify(ok=not bool(sync_state['last_error']), imported=n, **sync_state)

def tg_send(chat_id, text, reply_to_message_id=None):
    if not BOT_TOKEN:
        return False
    payload = {'chat_id': chat_id, 'text': text}
    if reply_to_message_id:
        payload['reply_to_message_id'] = reply_to_message_id
    try:
        r = requests.post(f'{TG_API}/sendMessage', json=payload, timeout=12)
        if not r.ok:
            print(f'[TG] sendMessage failed {r.status_code}: {r.text[:300]}', flush=True)
        return r.ok
    except Exception as e:
        print(f'[TG] sendMessage exception: {type(e).__name__}: {e}', flush=True)
        return False

def set_telegram_webhook():
    if not BOT_TOKEN:
        print('[TG] BOT_TOKEN not configured; webhook disabled', flush=True)
        return False
    if not WEBHOOK_BASE_URL:
        print('[TG] no public webhook URL detected; set WEBHOOK_BASE_URL manually', flush=True)
        return False
    url = f'{WEBHOOK_BASE_URL}{WEBHOOK_PATH}'
    payload = {'url': url, 'secret_token': WEBHOOK_SECRET, 'allowed_updates': ['message'], 'drop_pending_updates': False, 'max_connections': 20}
    try:
        r = requests.post(f'{TG_API}/setWebhook', json=payload, timeout=15)
        print(f'[TG] setWebhook url={url} -> {r.status_code} {r.text[:400]}', flush=True)
        return r.ok
    except Exception as e:
        print(f'[TG] setWebhook exception: {type(e).__name__}: {e}', flush=True)
        return False

def webhook_keeper():
    for delay in (0, 8, 20, 40):
        if delay:
            time.sleep(delay)
        set_telegram_webhook()
    while True:
        time.sleep(600)
        set_telegram_webhook()

def process_telegram_message(msg):
    text = msg.get('text') or ''
    chat = msg.get('chat') or {}
    sender = msg.get('from') or {}
    chat_id = chat.get('id')
    message_id = msg.get('message_id')
    sender_name = sender.get('username') or ' '.join((x for x in [sender.get('first_name'), sender.get('last_name')] if x)) or 'unknown'
    sender_is_bot = bool(sender.get('is_bot'))
    print(f'[TG-WEBHOOK] chat={chat_id} sender={sender_name} is_bot={sender_is_bot} text={text[:160]!r}', flush=True)
    cmd = text.strip().split()[0].split('@')[0].lower() if text.strip().startswith('/') else ''
    if cmd == '/id':
        tg_send(chat_id, f'Chat ID: {chat_id}', message_id)
        return
    if cmd == '/status':
        m = model()
        tg_send(chat_id, f"运行正常\\n历史期数: {m['count']}\\n最新期号: {m['issue']}", message_id)
        return
    if ALLOWED_CHAT_ID and str(chat_id) != ALLOWED_CHAT_ID:
        print(f'[TG-WEBHOOK] ignored: chat id does not match ALLOWED_CHAT_ID={ALLOWED_CHAT_ID}', flush=True)
        return
    p = parse_draw(text)
    if not p:
        print('[TG-WEBHOOK] message received but parser did not recognize a complete draw', flush=True)
        return
    issue, nums, zs, colors = p
    print(f'[TG-WEBHOOK] parsed issue={issue} nums={nums} zodiac={zs} colors={colors}', flush=True)
    if add_draw(issue, nums, zs, colors, text):
        print(f'[TG-WEBHOOK] inserted issue={issue}', flush=True)
        if not sender_is_bot:
            tg_send(chat_id, f'已入库 {issue}，统计结果已更新。', message_id)
    else:
        print(f'[TG-WEBHOOK] duplicate/invalid issue={issue}, not inserted', flush=True)

@app.post(WEBHOOK_PATH)
def telegram_webhook():
    if WEBHOOK_SECRET:
        got = request.headers.get('X-Telegram-Bot-Api-Secret-Token', '')
        if got != WEBHOOK_SECRET:
            return (jsonify({'ok': False, 'error': 'bad secret'}), 403)
    data = request.get_json(silent=True) or {}
    msg = data.get('message')
    if msg:
        process_telegram_message(msg)
    return jsonify({'ok': True})

@app.get('/api/webhook')
def webhook_status():
    if not BOT_TOKEN:
        return jsonify({'ok': False, 'configured': False})
    try:
        r = requests.get(f'{TG_API}/getWebhookInfo', timeout=10)
        payload = r.json()
        payload['_detected_base_url'] = WEBHOOK_BASE_URL
        return jsonify(payload)
    except Exception as e:
        return (jsonify({'ok': False, 'error': str(e)}), 500)

def boot():
    pass
import sys
from compact import install
install(sys.modules[__name__])
boot()
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT)
