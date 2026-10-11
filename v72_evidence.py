"""v72 preregistered, prospective-only evidence and matched issue audit.

No model is automatically promoted. RAND is a cryptographic-hash-based fair
control, locked before the draw. Historical CSV is NEVER backfilled here.
"""
import hashlib, json, math

VERSION='v72-evidence-001'
MIN_N=1000
PAYOUT=47.5
SPECS=(('F22','Q1','R1',22),('A27','Q1','R1',27),('C22','Q2','R2',22),('B27','Q2','R2',27),('D16','Q3','R1',16))


def random_ranking(target):
    # Domain-separated deterministic random ranking; target issue is public before draw.
    return sorted(range(1,50),key=lambda n:hashlib.sha256(f'v72-control-001|{target}|{n}'.encode()).digest())


def lock_random(connect, db_lock, target, cutoff):
    rank=random_ranking(target)
    with db_lock:
        c=connect()
        try:
            c.execute('''INSERT OR IGNORE INTO v68_forecast
                (target_issue,ranker_id,model_version,history_cutoff_id,probs_json,ranked49,top16,top22,top27)
                VALUES (?,?,?,?,?,?,?,?,?)''',
                (str(target),'RAND',VERSION,str(cutoff),
                 json.dumps({str(i):1/49 for i in range(1,50)},separators=(',',':')),
                 ','.join(map(str,rank)),','.join(map(str,rank[:16])),
                 ','.join(map(str,rank[:22])),','.join(map(str,rank[:27]))))
            c.commit()
        finally:c.close()


def settle_random(connect,db_lock,target,actual):
    with db_lock:
        c=connect()
        try:
            row=c.execute('''SELECT ranked49 FROM v68_forecast
                WHERE target_issue=? AND ranker_id='RAND' AND model_version=? AND settled=0''',
                (str(target),VERSION)).fetchone()
            if row:
                rank=[int(x) for x in row['ranked49'].split(',')]
                pos=rank.index(int(actual))+1
                c.execute('''UPDATE v68_forecast SET settled=1,actual_special=?,actual_rank=?,
                    log_loss=?,brier=?,hit16=?,hit22=?,hit27=?
                    WHERE target_issue=? AND ranker_id='RAND' AND model_version=?''',
                    (int(actual),pos,math.log(49),48/49,int(pos<=16),int(pos<=22),int(pos<=27),str(target),VERSION))
                c.commit()
        finally:c.close()


def _wilson(h,n,z=1.96):
    if not n:return [0.,1.]
    p=h/n; a=1+z*z/n
    center=(p+z*z/(2*n))/a
    d=z*math.sqrt(p*(1-p)/n+z*z/(4*n*n))/a
    return [max(0.,center-d),min(1.,center+d)]


def _exact_one_sided(wins,losses):
    n=wins+losses
    if not n:return 1.
    if wins<=losses:return min(1.,sum(math.exp(math.lgamma(n+1)-math.lgamma(i+1)-math.lgamma(n-i+1)-n*math.log(2)) for i in range(wins,n+1)))
    # symmetry: upper tail of Bin(n,.5)
    return min(1.,sum(math.exp(math.lgamma(n+1)-math.lgamma(i+1)-math.lgamma(n-i+1)-n*math.log(2)) for i in range(wins,n+1)))


def _holm(pairs):
    out={}; m=len(pairs); last=0.
    for i,(name,p) in enumerate(sorted(pairs,key=lambda x:x[1])):
        last=max(last,min(1.,p*(m-i)))
        out[name]=last
    return out


def dashboard(connect,db_lock):
    with db_lock:
        c=connect()
        try:
            rows=c.execute('''SELECT target_issue,ranker_id,model_version,actual_special,
                   hit16,hit22,hit27,log_loss,history_cutoff_id
                   FROM v68_forecast WHERE settled=1 AND
                   ((model_version='v69-null-first-001' AND ranker_id IN ('R1','R2')) OR
                    (model_version='v71-forward-competition-001' AND ranker_id IN ('Q1','Q2','Q3')) OR
                    (model_version=? AND ranker_id='RAND'))''',(VERSION,)).fetchall()
        finally:c.close()
    by_issue={}
    for r in rows:
        issue=str(r['target_issue']); rid=str(r['ranker_id'])
        # Basic anti-leak integrity; old malformed rows excluded, not repaired.
        if not str(r['history_cutoff_id']).isdigit() or not issue.isdigit() or int(r['history_cutoff_id'])>=int(issue):continue
        by_issue.setdefault(issue,{})[rid]=r
    out={}; pvals=[]
    for name,chall,inc,k in SPECS:
        column={16:'hit16',22:'hit22',27:'hit27'}[k]
        matched=[(v[chall],v[inc],v.get('RAND')) for v in by_issue.values()
                 if chall in v and inc in v and v[chall]['actual_special']==v[inc]['actual_special']]
        n=len(matched); h=sum(int(a[column]) for a,b,r in matched)
        base=sum(int(b[column]) for a,b,r in matched)
        wins=sum(int(a[column])==1 and int(b[column])==0 for a,b,r in matched)
        losses=sum(int(a[column])==0 and int(b[column])==1 for a,b,r in matched)
        p=_exact_one_sided(wins,losses)
        ci=_wilson(h,n)
        ll=sum(float(a['log_loss']) for a,b,r in matched)/n if n else 0.
        inc_ll=sum(float(b['log_loss']) for a,b,r in matched)/n if n else 0.
        rand_matched=[(a,r) for a,b,r in matched if r is not None and a['actual_special']==r['actual_special']]
        rh=sum(int(r[column]) for a,r in rand_matched)
        rn=len(rand_matched)
        out[name]={
            'n':n,'k':k,'challenger':chall,'incumbent':inc,
            'hits':h,'rate_pct':round(100*h/n,3) if n else 0,
            'incumbent_hits':base,'incumbent_rate_pct':round(100*base/n,3) if n else 0,
            'random_expected_pct':round(100*k/49,3),
            'breakeven_pct':round(100*k/PAYOUT,3),
            'ci95_pct':[round(100*v,3) for v in ci],
            'rescue':wins,'harm':losses,'p_one_sided':round(p,8),
            'delta_logloss_vs_uniform':round(ll-math.log(49),6) if n else None,
            'delta_logloss_vs_incumbent':round(ll-inc_ll,6) if n else None,
            'random_control_n':rn,'random_control_hits':rh,
            'random_control_rate_pct':round(100*rh/rn,3) if rn else None,
            'roi_pct':round(100*((h/n)*PAYOUT/k-1),3) if n else None,
        }
        pvals.append((name,p))
    holm=_holm(pvals)
    for name,item in out.items():
        item['p_holm']=round(holm[name],8)
        n=item['n']; ci_low=item['ci95_pct'][0]/100
        is_edge=(n>=MIN_N and holm[name]<0.025 and ci_low>item['k']/49
                 and ci_low>item['k']/PAYOUT and item['delta_logloss_vs_uniform'] is not None
                 and item['delta_logloss_vs_uniform']<0 and item['delta_logloss_vs_incumbent']<0)
        item['verified_edge']=bool(is_edge)
        item['status']='仅满足人工复核门槛，未自动晋级' if is_edge else ('前瞻样本不足' if n<MIN_N else '未证明稳定优势 · 停止实盘')
    return {'version':VERSION,'minimum_matched_forward':MIN_N,
            'policy':'只统计同一期已锁单样本；Holm校正五组；收益95%下界与随机基线双门槛；不自动切Live',
            'bet_gate':'STOP', 'groups':out}
