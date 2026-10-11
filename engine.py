"""Deterministic pre-draw scoring; no dependence on the target result."""
import hashlib, math
from collections import Counter
NUMS = tuple(range(1, 50))
RED = {1, 2, 7, 8, 12, 13, 18, 19, 23, 24, 29, 30, 34, 35, 40, 45, 46}
BLUE = {3, 4, 9, 10, 14, 15, 20, 25, 26, 31, 36, 37, 41, 42, 47, 48}

def wave(n):
    return '红' if n in RED else '蓝' if n in BLUE else '绿'

def seed(r):
    return '|'.join((str(x['issue']) + ':' + str(x['special']) for x in r[:20]))

def rank(p, r):
    sig = seed(r)
    ties = {n: hashlib.blake2s((sig + ':' + str(n)).encode(), digest_size=8).digest() for n in NUMS}
    return sorted(NUMS, key=lambda n: (-round(p.get(n, 0), 14), ties[n]))

def normalize(p):
    total = sum(p.values()) or 1
    return {n: p.get(n, 0) / total for n in NUMS}

def posterior(r, window, half, alpha=3):
    c = Counter()
    for i, x in enumerate(r[:window]):
        c[int(x['special'])] += 0.5 ** (i / half)
    return normalize({n: c[n] + alpha for n in NUMS})

def mix(*parts):
    return normalize({n: sum((w * p[n] for w, p in parts)) for n in NUMS})

def short16(r):
    r = r[:20]
    hot = posterior(r, 20, 7, 3)
    return sorted(rank(hot, r)[:16])

def baseline_algo(r):
    uniform = {n: 1 / 49 for n in NUMS}
    return mix((0.35, posterior(r, 80, 25, 3)), (0.35, posterior(r, 500, 170, 4)), (0.3, uniform))

def fractional_votes(p, k=22):
    ranked = sorted(NUMS, key=lambda n: -p[n])
    cut = p[ranked[k - 1]]
    greater = [n for n in NUMS if p[n] > cut + 1e-12]
    equal = [n for n in NUMS if abs(p[n] - cut) <= 1e-12]
    fraction = (k - len(greater)) / max(1, len(equal))
    return {n: 1.0 if n in greater else fraction if n in equal else 0.0 for n in NUMS}

def legacy_pool_raw(r):
    short = r[:60]
    trend = posterior(short, 60, 15, 2)
    bynum = {n: Counter() for n in NUMS}
    for x in short:
        for j in range(1, 7):
            if x['z' + str(j)]:
                bynum[int(x['n' + str(j)])][x['z' + str(j)]] += 1
        if x['z7']:
            bynum[int(x['special'])][x['z7']] += 2
    zm = {n: c.most_common(1)[0][0] for n, c in bynum.items() if c}
    heat = Counter((int(x['special']) for x in short))
    zh = Counter((x['z7'] for x in short))
    waves = Counter((wave(int(x['special'])) for x in short[:20]))
    parity = Counter((int(x['special']) % 2 for x in short[:20]))
    return {'趋势': trend, '生肖': {n: zh[zm.get(n, '')] / max(1, sum((zm.get(k) == zm.get(n) for k in NUMS))) for n in NUMS}, '冷热': {n: 1 / (2 + heat[n]) for n in NUMS}, '波色': {n: waves[wave(n)] / sum((wave(k) == wave(n) for k in NUMS)) for n in NUMS}, '单双': {n: parity[n % 2] / sum((k % 2 == n % 2 for k in NUMS)) for n in NUMS}}

def corrected_predict(r):
    raw = legacy_pool_raw(r)
    votes = {k: fractional_votes(p) for k, p in raw.items()}
    norms = {}
    for k, p in raw.items():
        low = min(p.values())
        high = max(p.values())
        norms[k] = {n: (p[n] - low) / (high - low) if high - low > 1e-09 else 0.5 for n in NUMS}
    score = {n: sum((v[n] for v in votes.values())) + 0.2 * sum((p[n] for p in norms.values())) / 5 for n in NUMS}
    return {'algo22': sorted(rank(baseline_algo(r), r)[:22]), 'algo16': short16(r), 'pool22': sorted(rank(score, r)[:22])}


def consecutive_history(r):
    """Stop at a missing period; disconnected old records are not a recent trend window."""
    from datetime import datetime,timedelta
    out=[]
    def following(raw):
        raw=str(raw)
        if len(raw)==11 and raw.isdigit() and 1<=int(raw[8:])<=480:
            day=datetime.strptime(raw[:8],'%Y%m%d');seq=int(raw[8:])+1
            if seq>480:day+=timedelta(days=1);seq=1
            return day.strftime('%Y%m%d')+f'{seq:03d}'
        try:return str(int(raw)+1)
        except (TypeError,ValueError):return ''
    for x in r:
        if out and following(x['issue'])!=str(out[-1]['issue']):break
        out.append(x)
    return out
