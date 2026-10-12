"""Frozen dual-colour rule: 35%20-window,35%60-window,30%last-colour transition."""
import hashlib
from collections import Counter
from engine import NUMS,wave,period_window,period_ordinal,recent_history,seed
COLORS=('红','蓝','绿')
BASE={z:sum(wave(n)==z for n in NUMS)/49 for z in COLORS}
def freq(r,window,half):
    c=Counter()
    for i,x in enumerate(period_window(r,window)):c[wave(int(x['special']))]+=.5**(i/half)
    den=sum(c.values())+20
    return {z:(c[z]+20*BASE[z])/den for z in COLORS}
def forecast(r,feedback=None):
    if not r:raise ValueError('没有开奖历史')
    feedback=feedback or {};r=recent_history(r);p20=freq(r,20,7);p60=freq(r,60,20);last=wave(int(r[0]['special']));counts=Counter();rr=period_window(r,121);pairs=0
    for i in range(len(rr)-1):
        if period_ordinal(str(rr[i]['issue']))-period_ordinal(str(rr[i+1]['issue']))!=1:continue
        if wave(int(rr[i+1]['special']))==last:counts[wave(int(rr[i]['special']))]+=.5**(i/40);pairs+=1
    den=sum(counts.values())+20;pt={z:(counts[z]+20*BASE[z])/den for z in COLORS}
    models={'mixed':{z:.35*p20[z]+.35*p60[z]+.30*pt[z] for z in COLORS},'trend':{z:.30*p20[z]+.70*p60[z] for z in COLORS},'transition':{z:.20*p60[z]+.80*pt[z] for z in COLORS}}
    sig=seed(r);recommendations={}
    for key,prob in models.items():recommendations[key]=sorted(COLORS,key=lambda z:(-round(prob[z],14),hashlib.blake2s((sig+z).encode(),digest_size=8).digest()))[:2]
    current=feedback.get('current_method','mixed');current=current if current in models else 'mixed';method=current;forced=bool(feedback.get('force_switch'));perf=feedback.get('performance',{})
    if forced:
        options=[k for k in models if k!=current]
        method=max(options,key=lambda k:(perf.get(k,{}).get('smoothed_rate',2/3),len(set(recommendations[k])-set(recommendations[current])),models[k][last]))
    evidence_switch=False
    if not forced and all(perf.get(k,{}).get('n',0)>=30 for k in models):
        best=max(models,key=lambda k:perf[k]['smoothed_rate'])
        if perf[best]['smoothed_rate']>perf[current]['smoothed_rate']+.08:method=best;evidence_switch=True
    return {'colors':recommendations[method],'probs':models[method],'based_on_wave':last,'history_cutoff':str(r[0]['issue']),'transition_pairs':pairs,'recipe':'走势+波色转移，正式结果反馈，两错切法','baseline_coverage':sum(BASE[z] for z in recommendations[method]),'method':method,'switched':method!=current,'switch_reason':'同一方法相邻两期未中' if forced else '正式近60期表现更好' if evidence_switch else '', 'candidates':{k:{'colors':recommendations[k],'probs':models[k]} for k in models},'performance':perf}

def streaks(bits,targets=None):
    max_hit=max_miss=cur_hit=cur_miss=0
    for i,h in enumerate(bits):
        if targets is not None and i and period_ordinal(str(targets[i]))-period_ordinal(str(targets[i-1]))!=1:cur_hit=cur_miss=0
        if h:cur_hit+=1;cur_miss=0;max_hit=max(max_hit,cur_hit)
        else:cur_miss+=1;cur_hit=0;max_miss=max(max_miss,cur_miss)
    return {'current_hit':cur_hit,'current_miss':cur_miss,'max_hit':max_hit,'max_miss':max_miss}
