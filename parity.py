"""Wave-conditioned special-number odd/even probabilities and pre-draw ledger."""
import json
from engine import NUMS, period_ordinal, wave
from module_models import categorical, formal_mix
from rounds import summarize

VERSION = 'v84_parity_probability'
LABELS = ('单', '双')
BASE = {'单': 25 / 49, '双': 24 / 49}


def parity(number):
    return '单' if int(number) % 2 else '双'


def forecast(history, records, wave_info):
    # P(parity) = sum P(wave) * P(parity | wave). Preserve the full wave
    # distribution instead of treating the selected colour as certain.
    wave_probs = wave_info['probs']
    def aggregate(probabilities):
        mass = {color: sum(probabilities[n] for n in NUMS if wave(n) == color) for color in wave_probs}
        return {label: sum(wave_probs[color] * sum(probabilities[n] for n in NUMS if wave(n) == color and parity(n) == label) / mass[color] for color in wave_probs) for label in LABELS}
    joint = lambda n: (wave(n), parity(n))
    models = {f'wave_window{w}': aggregate(categorical(history, joint, w, max(2, w / 3), prior=20)) for w in (6, 20, 60)}
    models['wave_transition'] = aggregate(categorical(history, joint, 120, 40, conditional=True))
    models['wave_baseline'] = aggregate({n: 1 / 49 for n in NUMS})
    cutoff = period_ordinal(str(history[0]['issue']))
    eligible = {}
    for row in records:
        if row['actual'] not in LABELS or row['version'] != VERSION:
            continue
        if not period_ordinal(str(row['cutoff'])) < period_ordinal(str(row['target'])) <= cutoff:
            continue
        info = json.loads(row['info_json'])
        if info.get('probability_revision') == VERSION:
            eligible[row['target']] = (info.get('probability_candidates', {}), row['actual'])
    observations = [eligible[key] for key in sorted(eligible, key=period_ordinal, reverse=True)]
    probs, weights, evidence = formal_mix(models, observations)
    choice = max(LABELS, key=lambda label: (probs[label], BASE[label]))
    return {'choice': choice, 'probs': probs, 'estimated_probability': probs[choice],
            'history_cutoff': str(history[0]['issue']), 'probability_revision': VERSION,
            'probability_candidates': models, 'weights': weights, 'evidence': evidence,
            'wave_probs': wave_probs, 'wave_source': 'single_wave',
            'source': '基于单波三色完整概率与各波色单双条件分布融合'}


def create_table(connection):
    connection.execute('CREATE TABLE IF NOT EXISTS parity_forecasts(target TEXT, version TEXT, cutoff TEXT, choice TEXT, info_json TEXT, actual TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(target,version))')


def refresh(connection, history, target, ready, wave_info):
    cutoff = str(history[0]['issue'])
    connection.execute("UPDATE parity_forecasts SET actual=(SELECT CASE WHEN special % 2=1 THEN '单' ELSE '双' END FROM draws WHERE issue=target) WHERE actual IS NULL AND EXISTS(SELECT 1 FROM draws WHERE issue=target)")
    locked = connection.execute('SELECT * FROM parity_forecasts WHERE target=? AND version=?', (target, VERSION)).fetchone()
    rows = [dict(x) for x in connection.execute('SELECT * FROM parity_forecasts WHERE version=? ORDER BY target', (VERSION,))]
    if ready and locked is None and wave_info.get('probs'):
        info = forecast(history, rows, wave_info)
        connection.execute('INSERT OR IGNORE INTO parity_forecasts(target,version,cutoff,choice,info_json) VALUES (?,?,?,?,?)', (target, VERSION, cutoff, info['choice'], json.dumps(info, ensure_ascii=False)))
        locked = connection.execute('SELECT * FROM parity_forecasts WHERE target=? AND version=?', (target, VERSION)).fetchone()
        rows = [dict(x) for x in connection.execute('SELECT * FROM parity_forecasts WHERE version=? ORDER BY target', (VERSION,))]
    settled = [x for x in rows if x['actual'] is not None]
    hits = sum(x['choice'] == x['actual'] for x in settled)
    pseudo = [dict(x, groups_json=json.dumps({'parity': [x['choice']]}), meta_json='{}') for x in rows]
    current_round, past_rounds = summarize(pseudo, {'parity': ('特码单双', 1)}, target, VERSION, {'parity': VERSION})
    previous = next((x for x in reversed(settled) if x['target'] == cutoff), None)
    return {'next_issue': target, 'choice': locked['choice'] if locked else None,
            'info': json.loads(locked['info_json']) if locked else {},
            'current_draw': {'issue': cutoff, 'special': int(history[0]['special']), 'parity': parity(history[0]['special'])},
            'current_prediction': {'choice': previous['choice'], 'hit': previous['choice'] == previous['actual']} if previous else None,
            'n': len(settled), 'hit': hits, 'miss': len(settled) - hits,
            'rate': round(100 * hits / len(settled), 1) if settled else None,
            'round': current_round['parity'], 'round_history': past_rounds['parity'],
            'recent': [{'issue': x['target'], 'choice': x['choice'], 'actual': x['actual'], 'hit': x['choice'] == x['actual']} for x in settled[-10:][::-1]]}


def restore(connection, payload):
    for row in payload.get('parity_forecasts', []):
        connection.execute('INSERT OR IGNORE INTO parity_forecasts(target,version,cutoff,choice,info_json,actual,created_at) VALUES (?,?,?,?,?,?,?)', tuple(row.get(k) for k in ('target', 'version', 'cutoff', 'choice', 'info_json', 'actual', 'created_at')))
