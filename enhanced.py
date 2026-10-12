"""Pre-draw, evidence-gated 22-code fusion plus six-zodiac/four-codes selection."""
import math,hashlib
from collections import Counter
from engine import wave,NUMS,seed,rank,normalize,baseline_algo,legacy_pool_raw,fractional_votes,short16,posterior,period_window,period_ordinal
from validation import binomial_tail
ZODIACS=('鼠','牛','虎','兔','龙','蛇','马','羊','猴','鸡','狗','猪')
def norm_z(z):return {'馬':'马','龍':'龙','雞':'鸡','豬':'猪'}.get(z,z)
def zodiac_map(r):
    # Infer the residue/year anchor from recent actual labelled draws, rather than guessing zodiac labels.
    anchors=Counter()
    for x in period_window(r,120):
        for n,z in [(x['special'],x['z7'])]+[(x['n'+str(i)],x['z'+str(i)]) for i in range(1,7)]:
            z=norm_z(z or '')
            if z in ZODIACS:anchors[(ZODIACS.index(z)+int(n)-1)%12]+=1
    if not anchors:raise ValueError('缺少开奖生肖，不能生成6肖24码')
    anchor=anchors.most_common(1)[0][0]
    return {n:ZODIACS[(anchor-(n-1))%12] for n in NUMS}
def minmax(p):
    lo=min(p.values());hi=max(p.values())
    return {n:(p[n]-lo)/(hi-lo) if hi-lo>1e-12 else .5 for n in NUMS}
class Book:
    def __init__(self,r):self.r=r;self.features={};self.candidates={}
    def at(self,i):
        if i not in self.features:
            rr=self.r[i:];raw=legacy_pool_raw(rr)
            self.features[i]=(rr,raw,{k:fractional_votes(p) for k,p in raw.items()},{k:minmax(p) for k,p in raw.items()})
        return self.features[i]
    def weights(self,i):
        rr,raw,votes,norms=self.at(i);names=list(raw);n=min(60,max(0,len(rr)-20));hits=Counter()
        for j in range(i,i+n):
            prior,models,_,_=self.at(j+1);actual=int(self.r[j]['special'])
            for k,p in models.items():hits[k]+=actual in rank(p,prior)[:22]
        baseline=22/49
        w={k:math.exp(4*((hits[k]+20*baseline)/(n+20)-baseline)) for k in names};total=sum(w.values())
        return {k:w[k]/total for k in names},n
    def candidate(self,i=0):
        if i not in self.candidates:
            rr,raw,votes,norms=self.at(i);weights,n=self.weights(i)
            pool={x:sum(weights[k]*(votes[k][x]+.2*norms[k][x]) for k in raw) for x in NUMS}
            ppool=normalize({x:pool[x]+1 for x in NUMS});base=baseline_algo(rr)
            fusion={x:.55*base[x]+.45*ppool[x] for x in NUMS}
            self.candidates[i]=({'algo22':sorted(rank(fusion,rr)[:22]),'pool22':sorted(rank(pool,rr)[:22])},weights,n)
        return self.candidates[i]
    def current(self,i=0):
        rr,raw,votes,norms=self.at(i)
        p={x:sum(v[x] for v in votes.values())+.2*sum(v[x] for v in norms.values())/5 for x in NUMS}
        return {'algo22':sorted(rank(baseline_algo(rr),rr)[:22]),'pool22':sorted(rank(p,rr)[:22])}
    def evaluate(self):
        # This test uses only past targets; every reconstructed forecast excludes its target result.
        n=min(40,max(0,len(self.r)-80));hits=Counter();wins=Counter();losses=Counter()
        for j in range(n):
            actual=int(self.r[j]['special']);candidate,_,_=self.candidate(j+1);old=self.current(j+1)
            for k in candidate:
                a=actual in candidate[k];b=actual in old[k];hits[k]+=a;wins[k]+=a and not b;losses[k]+=b and not a
        result={}
        for k in ('algo22','pool22'):
            paired_p=binomial_tail(wins[k]+losses[k],wins[k],.5);random_p=binomial_tail(n,hits[k],22/49)
            result[k]={'validation_n':n,'candidate_hit':hits[k],'rescued':wins[k],'lost':losses[k],'paired_p':round(paired_p,6),'random_p':round(random_p,6),'enabled':bool(n==40 and wins[k]>losses[k] and paired_p<.025 and random_p<.025),'reason':'前40期逐期检验，仅充分证据才启用融合'}
        return result

def six_zodiac(r,number_scores=None,method='blend'):
    zm=zodiac_map(r);sizes=Counter(zm.values());uniform={z:sizes[z]/49 for z in ZODIACS}
    freq=Counter();transition=Counter();last=zm[int(r[0]['special'])]
    for i,x in enumerate(period_window(r,60)):freq[zm[int(x['special'])]]+=.5**(i/20)
    for i in range(min(120,len(r)-1)):
        if period_ordinal(str(r[i]['issue']))-period_ordinal(str(r[i+1]['issue']))==1 and zm[int(r[i+1]['special'])]==last:transition[zm[int(r[i]['special'])]]+=.5**(i/40)
    fden=sum(freq.values())+30;tden=sum(transition.values())+30
    f={z:(freq[z]+30*uniform[z])/fden for z in ZODIACS};t={z:(transition[z]+30*uniform[z])/tden for z in ZODIACS}
    ns=number_scores or baseline_algo(r);support={z:sum(ns[n] for n in NUMS if zm[n]==z) for z in ZODIACS}
    # Exactly one defensive cold zodiac; the other five are selected by actual recent heat.
    cold_counts=Counter(zm[int(x['special'])] for x in period_window(r,60));sig=seed(r)
    gap={z:999 for z in ZODIACS}
    for i,x in enumerate(period_window(r,60)):
        z=zm[int(x['special'])]
        if gap[z]==999:gap[z]=i
    coldest=min(ZODIACS,key=lambda z:(cold_counts[z]/sizes[z],-gap[z],hashlib.blake2s((sig+z).encode(),digest_size=8).digest()))
    heat_window=20 if method=='transition' else 60 if method=='blend' else 90
    recent=period_window(r,heat_window);heat=Counter()
    for i,x in enumerate(recent):heat[zm[int(x['special'])]]+=.5**(i/(7 if heat_window==20 else 20 if heat_window==60 else 30))
    heat_score={z:heat[z]/sizes[z] for z in ZODIACS}
    hot_five=sorted((z for z in ZODIACS if z!=coldest),key=lambda z:(-round(heat_score[z],14),-t[z],hashlib.blake2s((sig+z).encode(),digest_size=8).digest()))[:5]
    chosen=hot_five+[coldest];number_rank=rank(ns,r);by_z={z:sorted([n for n in number_rank if zm[n]==z][:4]) for z in chosen}
    codes=sorted(n for z in chosen for n in by_z[z])
    assert len(codes)==24 and len(set(codes))==24 and all(len(ns)==4 for ns in by_z.values())
    return codes,{'zodiacs':chosen,'by_zodiac':by_z,'number_zodiac':{str(n):zm[n] for n in codes},'all_number_zodiac':{str(n):zm[n] for n in NUMS},'hot_zodiacs':hot_five,'cold_zodiac':coldest,'heat_window':heat_window,'heat_scores':dict(heat_score),'cold_counts':{z:cold_counts[z] for z in ZODIACS},'rule':'5热肖+1最冷肖，每肖4码；生肖中与24码中分开统计','recipe':method}
METHODS={
'algo22':('base','fusion','cold_balanced'),
'pool22':('vote','weighted','structure'),
'algo16':('hot20','balanced20','structure20'),
'zodiac24':('blend','transition','balanced')}
METHOD_LABELS={'base':'中长窗评分','fusion':'多维融合','cold_balanced':'冷热平衡','vote':'均衡投票','weighted':'表现加权投票','structure':'波色单双结构','hot20':'8热8冷短窗','balanced20':'8热8冷均衡','structure20':'8热8冷结构','blend':'5热1冷中窗','transition':'5热1冷短窗','balanced':'5热1冷慢窗'}
def _structure(rr):
    from validation import structure
    rr=period_window(rr,60)
    return structure(rr,len(rr))
def split16(r,method='hot20'):
    rr=period_window(r,20);counts=Counter(int(x['special']) for x in rr);hotprob=posterior(rr,20,7 if method=='hot20' else 20,3);structure=_structure(rr)
    if method=='structure20':hotprob={n:.6*hotprob[n]+.4*structure[n] for n in NUMS}
    observed=[n for n in rank(hotprob,rr) if counts[n]>0]
    hot=observed[:8]
    if len(hot)<8:hot += [n for n in rank(hotprob,rr) if n not in hot][:8-len(hot)]
    coldprob=structure
    if method=='balanced20':coldprob={n:1/max(1e-9,structure[n]) for n in NUMS}
    if method=='structure20':
        tails=Counter(int(x['special'])%10 for x in rr)
        coldprob={n:.5*structure[n]+.5*(tails[n%10]+1)/(len(rr)+10) for n in NUMS}
    pool=[n for n in NUMS if counts[n]<=1 and n not in hot];cold=[];wv=Counter();tails=Counter();zs=Counter();zm=zodiac_map(rr)
    scores=minmax(coldprob)
    while len(cold)<8:
        eligible=[n for n in pool if n not in cold and wv[(wave(n),n%2)]<2]
        if not eligible:eligible=[n for n in pool if n not in cold]
        adjusted={n:scores[n]-.18*tails[n%10]-.18*zs[zm[n]] for n in eligible}
        chosen=rank({n:adjusted.get(n,-100) for n in NUMS},rr)[0]
        cold.append(chosen);wv[(wave(chosen),chosen%2)]+=1;tails[chosen%10]+=1;zs[zm[chosen]]+=1
    assert len(hot)==8 and len(cold)==8 and not(set(hot)&set(cold))
    return sorted(hot+cold),{'hot':sorted(hot),'cold':sorted(cold),'window_used':len(rr),'cold_rule':'近20期未开或仅开1次、排除热8；分散波色单双、生肖与尾数','hot_observed_count':sum(counts[n]>0 for n in hot),'insufficient_hot_history':len(observed)<8}
def method_group(r,k,method,book=None,index=0):
    rr=r[index:];book=book or Book(r)
    if k=='algo16':return split16(rr,method)[0]
    if k=='zodiac24':return six_zodiac(rr,method=method)[0]
    if k=='algo22':
        if method=='base':return book.current(index)[k]
        if method=='fusion':return book.candidate(index)[0][k]
        c=Counter(int(x['special']) for x in period_window(rr,60));pcold=normalize({n:1/(3+c[n]) for n in NUMS});base=baseline_algo(rr)
        return sorted(rank({n:.5*base[n]+.5*pcold[n] for n in NUMS},rr)[:22])
    if method=='vote':return book.current(index)[k]
    if method=='weighted':return book.candidate(index)[0][k]
    return sorted(rank(_structure(rr),rr)[:22])
def predict(r,controls=None):
    controls=controls or {};book=Book(r);gates=book.evaluate();candidate,weights,n=book.candidate();groups={};chosen={};zmeta=None
    for k,options in METHODS.items():
        ctl=controls.get(k,{})
        current=ctl.get('current_key',options[0]);current=current if current in options else options[0]
        method=current;switch=bool(ctl.get('force_switch'))
        if switch:
            alternate=[x for x in options if x!=current]
            rr=period_window(r,20) if k=='algo16' else r;folds=min(8,len(rr)-5) if k=='algo16' else min(20,max(0,len(rr)-60));performance={x:0 for x in alternate}
            for j in range(max(0,folds)):
                for x in alternate:
                    past=rr[j+1:];codes=method_group(past,k,x) if k=='algo16' else method_group(r,k,x,book,j+1)
                    performance[x]+=int(rr[j]['special']) in codes
            current_codes=set(method_group(r,k,current,book));method=max(alternate,key=lambda x:(performance[x],len(set(method_group(r,k,x,book))-current_codes),-options.index(x)))
        elif current==options[0] and k in gates and gates[k]['enabled']:
            method='fusion' if k=='algo22' else 'weighted'
        groups[k]=method_group(r,k,method,book)
        if k=='zodiac24':groups[k],zmeta=six_zodiac(r,method=method)
        chosen[k]={'key':method,'label':METHOD_LABELS[method],'previous':current,'switched':method!=current,'trigger':'连续2期未中' if switch else '逐期比较通过' if method!=current else '继续当前方法','miss_run':int(ctl.get('miss_run',0))}
    meta={'algo16':split16(r,chosen['algo16']['key'])[1],'gates':gates,'methods':chosen,'expert_weights':{k:round(v,4) for k,v in weights.items()},'weight_validation_n':n,'zodiac24':zmeta}
    return groups,meta
