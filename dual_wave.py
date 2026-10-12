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
def forecast(r):
    if not r:raise ValueError('没有开奖历史')
    r=recent_history(r);p20=freq(r,20,7);p60=freq(r,60,20);last=wave(int(r[0]['special']));counts=Counter();rr=period_window(r,121)
    pairs=0
    for i in range(len(rr)-1):
        if period_ordinal(str(rr[i]['issue']))-period_ordinal(str(rr[i+1]['issue']))!=1:continue
        if wave(int(rr[i+1]['special']))==last:counts[wave(int(rr[i]['special']))]+=.5**(i/40);pairs+=1
    den=sum(counts.values())+20;pt={z:(counts[z]+20*BASE[z])/den for z in COLORS}
    probs={z:.35*p20[z]+.35*p60[z]+.30*pt[z] for z in COLORS}
    sig=seed(r);ranked=sorted(COLORS,key=lambda z:(-round(probs[z],14),hashlib.blake2s((sig+z).encode(),digest_size=8).digest()))
    return {'colors':ranked[:2],'probs':probs,'based_on_wave':last,'history_cutoff':str(r[0]['issue']),'transition_pairs':pairs,'recipe':'20期35%+60期35%+本期波色条件转移30%，均按号码覆盖比例平滑','baseline_coverage':sum(BASE[z] for z in ranked[:2])}
def streaks(bits,targets=None):
    max_hit=max_miss=cur_hit=cur_miss=0
    for i,h in enumerate(bits):
        if targets is not None and i and period_ordinal(str(targets[i]))-period_ordinal(str(targets[i-1]))!=1:cur_hit=cur_miss=0
        if h:cur_hit+=1;cur_miss=0;max_hit=max(max_hit,cur_hit)
        else:cur_miss+=1;cur_hit=0;max_miss=max(max_miss,cur_miss)
    return {'current_hit':cur_hit,'current_miss':cur_miss,'max_hit':max_hit,'max_miss':max_miss}
def backtest(rows,n=100):
    rows=sorted(rows,key=lambda x:str(x['issue']),reverse=True)
    if len(rows)<=n:raise ValueError('历史不足')
    records=[];eligible=[];excluded=[]
    for i in range(len(rows)-1):
        if period_ordinal(str(rows[i]['issue']))-period_ordinal(str(rows[i+1]['issue']))!=1:
            excluded.append(str(rows[i]['issue']));continue
        eligible.append(i)
        if len(eligible)==n:break
    if len(eligible)<n:raise ValueError('可还原开奖前锁定的连续相邻目标不足100')
    for i in reversed(eligible):
        target=rows[i];prior=rows[i+1:i+501];f=forecast(prior);actual=wave(int(target['special']))
        records.append({'target':str(target['issue']),'history_cutoff':str(prior[0]['issue']),'recommended':f['colors'],'actual':actual,'special':int(target['special']),'hit':actual in f['colors'],'baseline_coverage':f['baseline_coverage'],'transition_pairs':f['transition_pairs']})
    bits=[x['hit'] for x in records];hit=sum(bits);fixed={}
    for a,b in (('红','蓝'),('红','绿'),('蓝','绿')):
        h=sum(x['actual'] in (a,b) for x in records);fixed[a+'+'+b]={'hit':h,'miss':n-h,'rate':round(h/n*100,1),'theoretical_rate':round(100*(BASE[a]+BASE[b]),2)}
    comparison={}
    for pair in fixed:
        colors=pair.split('+');comparison[pair]={'rescued':sum(x['hit'] and x['actual'] not in colors for x in records),'lost':sum(not x['hit'] and x['actual'] in colors for x in records)}
    return {'fixed_pair_comparison':comparison,'version':'v81_dual_wave','n':n,'start_issue':records[0]['target'],'end_issue':records[-1]['target'],'hit':hit,'miss':n-hit,'rate':round(hit/n*100,1),'streaks':streaks(bits,[x['target'] for x in records]),'fixed_baselines':fixed,'mean_theoretical_coverage':round(sum(x['baseline_coverage'] for x in records)/n*100,2),'no_target_result_used':True,'parameters_fixed_before_test':True,'historical_not_formal':True,'excluded_missing_predecessor':excluded,'records':records}
