"""Small, prequential probability ensembles. No replay fitting on the ingestion path."""
import json,math
from collections import Counter
from engine import NUMS,wave,period_window,period_ordinal,posterior,normalize,rank
REVISION='v83_prequential_probability'
COLORS=('红','蓝','绿')
UNIFORM={n:1/49 for n in NUMS}
BASE={z:sum(wave(n)==z for n in NUMS)/49 for z in COLORS}

def blend(parts):
    keys=list(parts[0][1]);return {n:sum(w*p[n] for w,p in parts)/sum(w for w,p in parts) for n in keys}

def categorical(r,mapper,window,half,prior=25,conditional=False):
    rr=period_window(r,window);sizes=Counter(mapper(n) for n in NUMS);base={z:s/49 for z,s in sizes.items()};c=Counter();latest=period_ordinal(str(r[0]['issue']))
    for i,row in enumerate(rr):
        if conditional and (i+1>=len(rr) or period_ordinal(str(row['issue']))-period_ordinal(str(rr[i+1]['issue']))!=1 or mapper(int(rr[i+1]['special']))!=mapper(int(r[0]['special']))):continue
        age=latest-period_ordinal(str(row['issue']));c[mapper(int(row['special']))]+=.5**(age/half)
    total=sum(c.values())+prior
    return {n:(c[mapper(n)]+prior*base[mapper(n)])/(total*sizes[mapper(n)]) for n in NUMS}

def structure(r,window,half,conditional=False):
    mappers=(wave,lambda n:n%2,lambda n:n>=25,lambda n:n%10)
    return blend([(1,categorical(r,m,window,half,conditional=conditional)) for m in mappers])

def number_models(r,group):
    if group=='algo16':
        rr=period_window(r,20);return {'short20':posterior(rr,20,6,3),'smooth20':posterior(rr,20,20,3),'structure20':structure(rr,20,10),'transition20':structure(rr,20,10,True),'uniform':UNIFORM}
    if group=='algo22':return {'medium80':posterior(r,80,25,3),'stable500':posterior(r,500,170,4),'structure80':structure(r,80,25),'transition200':structure(r,200,60,True),'uniform':UNIFORM}
    if group=='pool22':return {'recent20':posterior(r,20,7,3),'trend60':posterior(r,60,20,3),'structure60':structure(r,60,20),'transition120':structure(r,120,40,True),'uniform':UNIFORM}
    return {'zodiac20':zodiac_prob(r,20),'zodiac60':zodiac_prob(r,60),'zodiac120':zodiac_prob(r,120),'structure60':structure(r,60,20),'uniform':UNIFORM}

def zodiac_prob(r,window):
    from enhanced import zodiac_map
    zm=zodiac_map(r)
    return categorical(r,lambda n:zm[n],window,max(7,window/3))

def formal_mix(models,observations):
    # Use common pre-recorded observations; compare proper log loss, not a noisy few hits.
    names=list(models);loss={k:0. for k in names};mass=0.;count=0
    for age,(saved,actual) in enumerate(observations[:120]):
        if not all(k in saved for k in names):continue
        weight=.5**(age/40);count+=1;mass+=weight
        for key in names:loss[key]+=weight*(-math.log(max(1e-9,float(saved[key].get(str(actual),saved[key].get(actual,1e-9))))))
    base_loss=loss[names[-1]]
    gain={key:(base_loss-loss[key])/(mass+20) for key in names}
    raw={key:math.exp(max(-4,min(4,8*gain[key]))) for key in names};den=sum(raw.values())
    weights={key:.2/len(names)+.8*raw[key]/den for key in names}
    probs=blend([(weights[k],models[k]) for k in names])
    return probs,weights,{'n':count,'log_loss':{k:loss[k]/mass if mass else None for k in names},'criterion':'开奖前候选概率的正式对数损失，先验收缩与权重下限'}

def number_forecast(r,records):
    from enhanced import six_zodiac
    groups={};meta={'probability_revision':REVISION,'probability_models':{},'algorithm_revisions':{},'methods':{},'gates':{},'online_historical_evaluation':False}
    for group,size in [('algo22',22),('algo16',16),('pool22',22),('zodiac24',24)]:
        models=number_models(r,group);eligible={}
        for row in records:
            if row['actual'] is None or group not in json.loads(row['groups_json']) or period_ordinal(str(row['target']))>period_ordinal(str(r[0]['issue'])) or period_ordinal(str(row['cutoff']))>=period_ordinal(str(row['target'])):continue
            info=json.loads(row.get('meta_json') or '{}')
            if info.get('probability_revision')==REVISION and group in info.get('probability_models',{}):eligible[row['target']]=(info['probability_models'][group]['candidates'],int(row['actual']))
        observations=[eligible[k] for k in sorted(eligible,reverse=True)]
        probs,weights,evidence=formal_mix(models,observations)
        rr=period_window(r,20) if group=='algo16' else r
        if group=='zodiac24':groups[group],meta[group]=six_zodiac(r,probs,'blend')
        else:groups[group]=sorted(rank(probs,rr)[:size])
        meta['probability_models'][group]={'candidates':models,'weights':weights,'evidence':evidence,'probs':probs}
        meta['algorithm_revisions'][group]=REVISION
        meta['methods'][group]={'key':REVISION,'label':'正式概率反馈融合','switched':False,'trigger':'每期开奖更新独立权重','miss_run':0}
        if group=='algo16':meta[group]={'window_used':len(rr),'rule':'近20期概率融合，无冷热配额','recipe':REVISION}
    return groups,meta

def wave_models(r,single=False):
    def aggregate(p):return {z:sum(p[n] for n in NUMS if wave(n)==z) for z in COLORS}
    windows=(6,20,60) if single else (20,60,120)
    out={f'window{w}':aggregate(categorical(r,wave,w,max(2,w/3),prior=20)) for w in windows}
    out['transition']=aggregate(categorical(r,wave,120,40,conditional=True))
    # State conditioned on the current colour run; adjacent pairs only, shrink sparse samples.
    def run_at(i):
        length=1
        while i+length<len(r) and length<3 and period_ordinal(str(r[i+length-1]['issue']))-period_ordinal(str(r[i+length]['issue']))==1 and wave(int(r[i+length]['special']))==wave(int(r[i]['special'])):length+=1
        return length
    counts=Counter();current_run=run_at(0)
    for i in range(min(119,len(r)-1)):
        if period_ordinal(str(r[i]['issue']))-period_ordinal(str(r[i+1]['issue']))==1 and wave(int(r[i+1]['special']))==wave(int(r[0]['special'])) and run_at(i+1)==current_run:counts[wave(int(r[i]['special']))]+=.5**(i/40)
    total=sum(counts.values())+25;out['run_state']={z:(counts[z]+25*BASE[z])/total for z in COLORS};out['uniform']=BASE
    return out

def wave_forecast(r,records,single=False):
    models=wave_models(r,single);observations=[]
    for row in sorted(records,key=lambda x:x['target'],reverse=True):
        if row['actual_wave'] is None or period_ordinal(str(row['target']))>period_ordinal(str(r[0]['issue'])) or period_ordinal(str(row['cutoff']))>=period_ordinal(str(row['target'])):continue
        info=json.loads(row['info_json'])
        if info.get('probability_revision')==REVISION:observations.append((info.get('probability_candidates',{}),row['actual_wave']))
    probs,weights,evidence=formal_mix(models,observations)
    colors=sorted(COLORS,key=lambda z:(-probs[z],-BASE[z],COLORS.index(z)))[:1 if single else 2]
    result={'colors':colors,'color':colors[0],'estimated_probability':probs[colors[0]],'probs':probs,'method':REVISION,'history_cutoff':str(r[0]['issue']),'based_on_wave':wave(int(r[0]['special'])),'probability_revision':REVISION,'probability_candidates':models,'weights':weights,'evidence':evidence,'switched':False,'recipe':'独立波色概率与正式反馈融合','candidates':{},'source':'独立单波概率融合' if single else '双波覆盖概率融合'}
    return result
