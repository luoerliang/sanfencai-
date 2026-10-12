"""Keep revised algorithm evidence separate from archived locks."""
import json

ALGO16_REVISION = 'v82_fixed_blend20'

def revision(row, group):
    meta = json.loads(row.get('meta_json') or '{}')
    return meta.get('algorithm_revisions', {}).get(group, 'legacy:' + row['version'])

def totals(rows, group):
    unique = {}
    for row in sorted(rows, key=lambda x: (x['target'], x.get('created_at') or '', x['version'])):
        if row['actual'] is not None and json.loads(row['groups_json']).get(group):
            unique.setdefault(row['target'], row)
    values = list(unique.values())
    n = len(values)
    hit = sum(x['actual'] in json.loads(x['groups_json'])[group] for x in values)
    return {'n': n, 'hit': hit, 'miss': n-hit, 'rate': round(100*hit/n, 1) if n else None}

def group_stats(rows, group):
    lifetime = totals(rows, group)
    if group != 'algo16':
        return lifetime
    current = [x for x in rows if revision(x, group) == ALGO16_REVISION]
    archived = sorted({revision(x, group) for x in rows if json.loads(x['groups_json']).get(group)} - {ALGO16_REVISION})
    return dict(totals(current, group), revision=ALGO16_REVISION, lifetime=lifetime,
                archived=[dict(revision=r, **totals([x for x in rows if revision(x, group)==r], group)) for r in archived])
