"""Offline chronological screening; no automatic promotion or lock backfill."""
import json, time
from engine import period_ordinal, rank, normalize
from enhanced import split16
from validation import summarize, paired

METHODS=('blend20','trend20','shape20','gap20')

def candidates(prior):
    out={m:split16(prior,m)[0] for m in METHODS[:-1]}
    gaps={n:20 for n in range(1,50)}
    for i,row in enumerate(prior[:20]):
        n=int(row['special'])
        gaps[n]=min(gaps[n],i)
    score=normalize({n:1+gaps[n]/20 for n in gaps})
    out['gap20']=rank(score,prior)[:16]
    return out

def run(rows):
    started=time.perf_counter()
    rows=sorted({x['issue']:x for x in rows}.values(),key=lambda x:x['issue'])
    observations=[];skipped=0
    for i in range(20,len(rows)):
        # Require target + prior 20 calendar periods without gaps.
        if period_ordinal(rows[i]['issue'])-period_ordinal(rows[i-20]['issue'])!=20:
            skipped+=1;continue
        observations.append((rows[i]['issue'],int(rows[i]['special']),list(reversed(rows[i-20:i]))))
    if len(observations)<600:raise ValueError('完整连续样本不足600期')
    n=len(observations);ends=(n//2,3*n//4,n);lo=0;report={};choice=None
    for name,hi in zip(('development','validation','test'),ends):
        bits={m:[] for m in METHODS}
        for issue,actual,prior in observations[lo:hi]:
            sets=candidates(prior)
            for m in METHODS:bits[m].append(actual in sets[m])
        if choice is None:choice=max(METHODS[1:],key=lambda m:(sum(bits[m]),-METHODS.index(m)))
        stats={m:summarize(v,16) for m,v in bits.items()}
        stats[choice].update(paired(bits[choice],bits['blend20']))
        half=(hi-lo)//2
        stats[choice]['half_gains']=[sum(bits[choice][:half])-sum(bits['blend20'][:half]),sum(bits[choice][half:])-sum(bits['blend20'][half:])]
        report[name]={'start':observations[lo][0],'end':observations[hi-1][0],'methods':stats}
        lo=hi
    threshold=.05/3
    passed=all(report[s]['methods'][choice]['p_above_random']<threshold and report[s]['methods'][choice]['p_better_than_current']<threshold for s in ('validation','test')) and all(x>0 for x in report['test']['methods'][choice]['half_gains'])
    return {'source_count':len(rows),'source_latest':rows[-1]['issue'],'eligible_targets':n,'skipped_gaps':skipped,'prior_periods':20,
            'segments':report,'chosen_from_development':choice,'screening_passed':passed,'automatic_promotion':False,
            'active_method':'blend20','multiplicity_threshold':threshold,'seconds':round(time.perf_counter()-started,3),
            'limitation':'Retrospective screening on previously available history; not proof of an untouched test or future advantage. Forward pre-draw locks must independently confirm any advantage.'}

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('backup');p.add_argument('output');args=p.parse_args()
    source=json.load(open(args.backup,encoding='utf-8'));report=run(source['draws'])
    with open(args.output,'w',encoding='utf-8') as f:json.dump(report,f,ensure_ascii=False,indent=2)
    print(json.dumps({k:report[k] for k in ('eligible_targets','chosen_from_development','screening_passed','seconds')},ensure_ascii=False))
