"""Chronological, pre-draw candidate evaluation and baseline checks. Standard library only."""
import math,time,json,random,hashlib
from collections import Counter
from engine import NUMS,rank,posterior,normalize,mix,wave,corrected_predict,fractional_votes,seed,legacy_pool_raw
CANDIDATES=('current','hot20','hot60','hot180','cold','structure','balanced')
def inverse(r,w):
    c=Counter(int(x['special']) for x in r[:w]);return normalize({n:1/(3+c[n]) for n in NUMS})
def structure(r,w):
    r=r[:w];parts=[]
    for mapper in (wave,lambda n:n%2,lambda n:n>=25,lambda n:n%10):
        counts=Counter(mapper(int(x['special'])) for x in r);sizes=Counter(mapper(n) for n in NUMS)
        parts.append((1,normalize({n:(counts[mapper(n)]+25*sizes[mapper(n)]/49)/sizes[mapper(n)] for n in NUMS})))
    return mix(*parts)
def candidate_groups(r):
    current=corrected_predict(r);out={k:{'current':v} for k,v in current.items()}
    # Every algo16 candidate is restricted to the same twenty pre-draw records, including tie handling.
    for group,k in (('algo22',22),('algo16',16),('pool22',22)):
        rr=r[:20] if group=='algo16' else r
        ps={'hot20':posterior(rr,20,7,3),'hot60':posterior(rr,60,20,3),'hot180':posterior(rr,180,60,3),'cold':inverse(rr,20 if k==16 else 60),'structure':structure(rr,20 if k==16 else 60)}
        # hot60/hot180 intentionally collapse to <=20 input for algo16; no hidden wider window.
        for name,p in ps.items():out[group][name]=sorted(rank(p,rr)[:k])
        h=rank(ps['hot20'],rr)[:k//2];cold=rank(ps['cold'],rr)
        out[group]['balanced']=sorted(h+[n for n in cold if n not in h][:k-len(h)])
    return out

def binomial_tail(n,h,p):
    if n==0 or h==0:return 1.
    terms=[math.lgamma(n+1)-math.lgamma(j+1)-math.lgamma(n-j+1)+j*math.log(p)+(n-j)*math.log1p(-p) for j in range(h,n+1)]
    m=max(terms)
    return min(1.,math.exp(m)*sum(math.exp(x-m) for x in terms))
def wilson(h,n):
    if not n:return [0.,100.]
    z=1.95996398454;p=h/n;d=1+z*z/n;mid=(p+z*z/(2*n))/d;half=z*math.sqrt(p*(1-p)/n+z*z/(4*n*n))/d
    return [round((mid-half)*100,2),round((mid+half)*100,2)]
def summarize(bits,k):
    n=len(bits);h=sum(bits)
    return {'n':n,'hit':h,'miss':n-h,'rate':round(h/n*100,2) if n else None,'random_expected_rate':round(k/49*100,2),'ci95':wilson(h,n),'p_above_random':binomial_tail(n,h,k/49)}
def paired(challenger,current):
    better=sum(a and not b for a,b in zip(challenger,current));worse=sum(b and not a for a,b in zip(challenger,current))
    return {'rescued':better,'lost':worse,'p_better_than_current':binomial_tail(better+worse,better,.5)}
def run(rows,exclude_latest=2000):
    """Oldest 500 warmup; 50% development, 25% validation, 25% untouched test.
    Last 2000 supplied CSV draws are excluded because earlier versions already inspected them.
    Candidate choice is fixed from development; validation and test cannot change the choice.
    """
    started=time.perf_counter();rows=sorted(rows,key=lambda x:str(x['issue']))
    use=rows[:-exclude_latest] if exclude_latest else rows
    warmup=500;available=len(use)-warmup
    if available<600:raise ValueError('至少需要1100期数据进行三个时间段的验证')
    dev=warmup+available//2;val=dev+available//4
    boundaries={'development':(warmup,dev),'validation':(dev,val),'test':(val,len(use))}
    chosen={};report={'source_total':len(rows),'source_latest':str(rows[-1]['issue']),'excluded_latest':exclude_latest,'warmup':warmup,'candidate_count':len(CANDIDATES),'selection_rule':'development chooses one; validation and untouched test must both exceed same-size random baseline and current model at p < 0.05/3, with positive gains in both test halves','segments':{}}
    for label,(lo,hi) in boundaries.items():
        bits={g:{name:[] for name in CANDIDATES+('random',)} for g in ('algo22','algo16','pool22')}
        for i in range(lo,hi):
            prior=list(reversed(use[max(0,i-500):i]));actual=int(use[i]['special']);groups=candidate_groups(prior)
            # A reproducible equal-size random baseline; seed is pre-draw history only.
            rng=random.Random(hashlib.sha256(seed(prior).encode()).digest());random49=rng.sample(list(NUMS),49)
            for g,k in (('algo22',22),('algo16',16),('pool22',22)):
                for name in CANDIDATES:bits[g][name].append(actual in groups[g][name])
                bits[g]['random'].append(actual in random49[:k])
        seg={'n':hi-lo,'start_issue':str(use[lo]['issue']),'end_issue':str(use[hi-1]['issue']),'groups':{}}
        for g,k in (('algo22',22),('algo16',16),('pool22',22)):
            stats={name:summarize(bs,k) for name,bs in bits[g].items()}
            if label=='development':chosen[g]=max(CANDIDATES[1:],key=lambda name:(sum(bits[g][name]),-CANDIDATES.index(name)))
            pick=chosen[g];stats[pick].update(paired(bits[g][pick],bits[g]['current']))
            half=(hi-lo)//2
            stats[pick]['half_gains']=[sum(bits[g][pick][:half])-sum(bits[g]['current'][:half]),sum(bits[g][pick][half:])-sum(bits[g]['current'][half:])]
            seg['groups'][g]=stats
        report['segments'][label]=seg
    report['chosen']=chosen;report['promote']={}
    for g,pick in chosen.items():
        evidence=[report['segments'][s]['groups'][g][pick] for s in ('validation','test')]
        report['promote'][g]=bool(all(x['p_above_random']<.05/3 and x['p_better_than_current']<.05/3 for x in evidence) and all(x>0 for x in evidence[-1]['half_gains']))
    report['elapsed_seconds']=round(time.perf_counter()-started,3)
    return report

def load_csv(path):
    import csv
    rows=[]
    normal=lambda z:{'馬':'马','龍':'龙','雞':'鸡','豬':'猪'}.get(z,z)
    with open(path,encoding='utf-8-sig') as f:
        for x in csv.DictReader(f):
            r={'issue':x['期号'],'special':int(x['特码']),'z7':normal(x['生肖'])}
            for i in range(1,7):r['n'+str(i)]=int(x['正码'+str(i)]);r['z'+str(i)]=normal(x['正码'+str(i)+'生肖'])
            rows.append(r)
    return rows
if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--history',default='history.csv');p.add_argument('--output',default='validation_report.json');p.add_argument('--exclude-latest',type=int,default=2000);args=p.parse_args()
    report=run(load_csv(args.history),args.exclude_latest)
    with open(args.output,'w',encoding='utf-8') as f:json.dump(report,f,ensure_ascii=False,indent=2)
    print(json.dumps({'chosen':report['chosen'],'promote':report['promote'],'seconds':report['elapsed_seconds']},ensure_ascii=False))
