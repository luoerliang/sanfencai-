"""Retrospective chronological audit. NOT live forward evidence.

Uses v68/v71 fixed algorithms, no fitting to evaluation period, but the
algorithms may have been selected after observing the same dataset. The output
MUST NOT be presented as independent prospective validation.
"""
import csv, json, math, sys
from collections import Counter
from pathlib import Path
from v72_evidence import random_ranking, _wilson


def posterior(h, window, half, alpha):
    a=[alpha]*49; total=49*alpha
    for i,n in enumerate(h[-window:][::-1]):
        w=0.5**(i/half)
        a[n-1]+=w; total+=w
    return [v/total for v in a]


def transition(h, window, half, alpha):
    # h is oldest -> newest; uses older->newer transitions entirely before prediction.
    recent=h[-(window+1):][::-1]
    if len(recent)<3:return [1/49]*49
    current=recent[0]; a=[alpha]*49; total=49*alpha
    for i in range(len(recent)-1):
        newer,older=recent[i],recent[i+1]
        if older==current:
            w=0.5**(i/half)
            a[newer-1]+=w; total+=w
    return [v/total for v in a]


def gap(h,window,scale):
    recent=h[-window:][::-1]
    pos={}
    for i,n in enumerate(recent):pos.setdefault(n,i+1)
    raw=[.65+.35*(1-math.exp(-pos.get(n,len(recent)+1)/scale)) for n in range(1,50)]
    z=sum(raw)
    return [v/z for v in raw]


def mix(*pairs):
    return [sum(w*p[i] for w,p in pairs) for i in range(49)]


def ranked(p):
    return sorted(range(1,50),key=lambda n:(-p[n-1],n))


def run(path, start=8000):
    with open(path,encoding='utf-8-sig',newline='') as f:rows=list(csv.DictReader(f))
    rows.sort(key=lambda x:int(x['期号']))
    issues=[str(r['期号']) for r in rows]
    nums=[int(r['特码']) for r in rows]
    assert len(set(issues))==len(issues)
    result={k:{'n':0,'hits':{16:0,22:0,27:0},'ll':0.,'rank':0.} for k in ['R1','R2','Q1','Q2','Q3','RAND']}
    for i in range(start,len(nums)):
        h=nums[:i]; actual=nums[i]
        p60=posterior(h,60,20,1.35);p240=posterior(h,240,80,1.60)
        p10=posterior(h,10,4,1.50);p40=posterior(h,40,14,1.55)
        probs={
          'R1':mix((.55,p60),(.45,p240)),
          'R2':mix((.70,p10),(.30,p40)),
          'Q1':mix((.40,posterior(h,60,18,1.55)),(.40,posterior(h,360,120,1.85)),(.20,transition(h,420,140,2.20))),
          'Q2':mix((.42,posterior(h,18,6,1.70)),(.38,posterior(h,90,30,1.75)),(.20,transition(h,260,85,2.35))),
          'Q3':mix((.28,posterior(h,30,10,1.70)),(.34,posterior(h,150,50,1.85)),(.22,transition(h,500,160,2.40)),(.16,gap(h,260,40))),
        }
        for name,p in probs.items():
            rank=ranked(p).index(actual)+1
            d=result[name];d['n']+=1;d['rank']+=rank;d['ll']-=math.log(p[actual-1])
            for k in (16,22,27):d['hits'][k]+=rank<=k
        rank=random_ranking(issues[i]).index(actual)+1
        d=result['RAND'];d['n']+=1;d['rank']+=rank;d['ll']+=math.log(49)
        for k in (16,22,27):d['hits'][k]+=rank<=k
    summary={}
    for name,d in result.items():
        n=d['n'];summary[name]={'n':n,'mean_rank':round(d['rank']/n,3),'logloss':round(d['ll']/n,6),
           'rates':{str(k):{'hit':d['hits'][k],'rate_pct':round(d['hits'][k]/n*100,3),
              'random_pct':round(k/49*100,3),
              'ci95_pct':[round(v*100,3) for v in _wilson(d['hits'][k],n)]} for k in (16,22,27)}}
    return {'type':'RETROSPECTIVE_WALK_FORWARD_NOT_LIVE','start_index':start,'history_count':len(nums),
            'first_test_issue':issues[start],'last_test_issue':issues[-1],
            'warning':'Model configurations may have been chosen after inspecting this historical series; this is NOT independent evidence of prediction advantage.',
            'summary':summary}


if __name__=='__main__':
    src=Path(sys.argv[1]) if len(sys.argv)>1 else Path(__file__).with_name('history.csv')
    report=run(src)
    out=Path(__file__).with_name('v72_historical_audit.json')
    out.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(report,ensure_ascii=False,indent=2))
