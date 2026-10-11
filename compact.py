"""v76: compact three-group runtime; legacy functions are compatibility helpers only."""
import os, json, threading, time, gzip
from collections import Counter
from flask import jsonify

VERSION='v76_compact_3groups'
GROUPS={'algo22':('算法22码',22),'algo16':('算法16码 · 近20期',16),'pool22':('多策略22码',22)}

def install(a):
    lock=threading.RLock()
    state={'ready':False,'error':'','migration':'正在核对历史','latest':'','count':0}
    cache={}
    def dbmeta():
        c=a.connect()
        try:
            x=c.execute('SELECT COUNT(*) n, MAX(issue) latest FROM draws').fetchone()
            return int(x['n']),str(x['latest'] or '')
        finally:c.close()
    def table():
        c=a.connect()
        try:
            c.execute('CREATE TABLE IF NOT EXISTS compact_forecasts(target TEXT, version TEXT, cutoff TEXT, groups_json TEXT, actual INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(target,version))')
            c.commit()
        finally:c.close()
    def rank(p):return sorted(range(1,50),key=lambda n:(-p.get(n,0),n))
    def build(r):
        # Independent smoothed number frequency model: long and medium windows + uniform.
        pu={n:1/49 for n in range(1,50)}
        p=a._v68_mix_prob((.35,a._v68_weighted_posterior(r,80,25,3)),(.35,a._v68_weighted_posterior(r,500,170,4)),(.30,pu))
        # This model receives exactly the latest twenty special-number draws.
        p16=a._v68_weighted_posterior(r[:20],20,7,3)
        # Pool has independent trend, zodiac, hot/cold, colour and parity votes.
        short=r[:60]; zmap=a._number_zodiac_map(short)
        heat=Counter(int(x['special']) for x in short)
        zh=Counter(a.normalize_z(x['z7']) for x in short)
        waves=Counter(a.wave_of(int(x['special'])) for x in short[:20])
        parity=Counter(a.parity_of(int(x['special'])) for x in short[:20])
        trend=a._v68_weighted_posterior(short,60,15,2)
        raw={
          '趋势':trend,
          '生肖':{n:zh[zmap.get(n,'')]/max(1,sum(zmap.get(k)==zmap.get(n) for k in range(1,50))) for n in range(1,50)},
          '冷热':{n:1/(2+heat[n]) for n in range(1,50)},
          '波色':{n:waves[a.wave_of(n)]/sum(a.wave_of(k)==a.wave_of(n) for k in range(1,50)) for n in range(1,50)},
          '单双':{n:parity[a.parity_of(n)]/sum(a.parity_of(k)==a.parity_of(n) for k in range(1,50)) for n in range(1,50)}}
        votes=Counter(n for s in raw.values() for n in rank(s)[:22])
        normalized={k:a._norm_values(s,range(1,50)) for k,s in raw.items()}
        pooled={n:votes[n]+.2*sum(s[n] for s in normalized.values())/5 for n in range(1,50)}
        return {'algo22':rank(p)[:22],'algo16':rank(p16)[:16],'pool22':rank(pooled)[:22]}
    def refresh():
        with lock,a.db_lock:
            r=a.recent_rows(500)
            if not r:return
            cutoff=str(r[0]['issue']);target=a._next_issue_id(cutoff)
            c=a.connect()
            try:
                # Settle only predictions genuinely locked before their target draw existed.
                c.execute('UPDATE compact_forecasts SET actual=(SELECT special FROM draws WHERE issue=target) WHERE version=? AND actual IS NULL AND EXISTS(SELECT 1 FROM draws WHERE issue=target)',(VERSION,))
                if state['ready']:
                    existing=c.execute('SELECT groups_json FROM compact_forecasts WHERE target=? AND version=?',(target,VERSION)).fetchone()
                    groups=json.loads(existing[0]) if existing else build(r)
                    c.execute('INSERT OR IGNORE INTO compact_forecasts(target,version,cutoff,groups_json) VALUES (?,?,?,?)',(target,VERSION,cutoff,json.dumps(groups)))
                else:groups={k:[] for k in GROUPS}
                c.commit()
                rows=c.execute('SELECT target,actual,groups_json FROM compact_forecasts WHERE version=? AND actual IS NOT NULL ORDER BY target DESC LIMIT 10',(VERSION,)).fetchall()
                allstats=c.execute('SELECT actual,groups_json FROM compact_forecasts WHERE version=? AND actual IS NOT NULL',(VERSION,)).fetchall()
            finally:c.close()
            stats={}
            for k in GROUPS:
                n=len(allstats);h=sum(x['actual'] in json.loads(x['groups_json'])[k] for x in allstats)
                stats[k]={'n':n,'hit':h,'miss':n-h,'rate':round(h/n*100,1) if n else None}
            count,latest=dbmeta();state.update(count=count,latest=latest)
            cache.clear();cache.update(version=VERSION,**state,issue=cutoff,next_issue=target,latest_numbers=[r[0][f'n{i}'] for i in range(1,7)]+[r[0]['special']],groups=groups,stats=stats,recent=[{'issue':x['target'],'actual':x['actual'],'hits':{k:x['actual'] in json.loads(x['groups_json'])[k] for k in GROUPS}} for x in rows])
    old_payload=a._checkpoint_payload
    def payload():
        p=old_payload();c=a.connect()
        try:
            p['draws']=[dict(x) for x in c.execute('SELECT * FROM draws ORDER BY issue DESC')]
            p['compact_forecasts']=[dict(x) for x in c.execute('SELECT * FROM compact_forecasts')]
            p['version']=VERSION
        finally:c.close()
        return p
    a._checkpoint_payload=payload
    def restore_compact():
        if not os.path.exists(a.CHECKPOINT_PATH):return
        with gzip.open(a.CHECKPOINT_PATH,'rt',encoding='utf-8') as f:p=json.load(f)
        c=a.connect()
        try:
            for x in p.get('compact_forecasts',[]):
                c.execute('INSERT OR IGNORE INTO compact_forecasts(target,version,cutoff,groups_json,actual,created_at) VALUES (?,?,?,?,?,?)',tuple(x.get(k) for k in ('target','version','cutoff','groups_json','actual','created_at')))
            c.commit()
        finally:c.close()
    def initialize():
        try:
            initial_count,initial_latest=dbmeta()
            if a.REMOTE_BACKUP_ENABLED:a.download_checkpoint_from_supabase()
            a.restore_checkpoint_if_better();restore_compact()
            # Merge bundled baseline ONLY when no inherited database is available.
            a.import_history_once()
            a.sync_best_available_history()
            count,latest=dbmeta()
            expected_count=os.getenv('EXPECTED_HISTORY_COUNT','').strip()
            expected_latest=os.getenv('EXPECTED_LATEST_ISSUE','').strip()
            if expected_count or expected_latest:
                ok=bool(expected_count and expected_latest and count>=int(expected_count) and latest>=expected_latest)
            else:
                # Never treat an unverified bundled snapshot as a migrated live database.
                remote_count=int(a.sync_state.get('remote_total',0))
                remote_latest=a.sync_state.get('remote_latest','')
                ok=bool(not a.sync_state.get('last_error') and remote_count and remote_latest and count>=remote_count and latest>=remote_latest)
            state.update(ready=ok,count=count,latest=latest,migration='历史继承核对通过' if ok else '待核对历史：设置旧版最新期号和总条数后启用')
            a.auto_state['migration_latest_issue']=latest;a.auto_state['migration_history_count']=count
            a.background_state['boot_ready']=True
            refresh();a.refresh_history_cache()
            if a.BOT_TOKEN:threading.Thread(target=a.webhook_keeper,daemon=True).start()
        except Exception as e:
            state.update(error=f'{type(e).__name__}: {e}',migration='历史核对失败');cache.update(state)
    def boot():
        a.init_db();table()
        threading.Thread(target=initialize,daemon=True,name='history-inheritance').start()
    def add_draw(issue,nums,zs,colors,raw):
        if len(nums)!=7 or len(set(nums))!=7 or not all(1<=n<=49 for n in nums):return False
        with lock,a.db_lock:
            c=a.connect()
            try:
                before=c.total_changes
                c.execute('INSERT OR IGNORE INTO draws(issue,n1,n2,n3,n4,n5,n6,special,z1,z2,z3,z4,z5,z6,z7,c1,c2,c3,c4,c5,c6,c7,raw) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',[issue,*nums,*zs,*colors,raw[:4000]])
                c.commit();changed=c.total_changes>before
            finally:c.close()
            if changed:
                refresh();a.refresh_history_cache()
                threading.Thread(target=backup,daemon=True).start()
            return changed
    backup_lock=threading.RLock()
    original_write_checkpoint=a.write_checkpoint_atomic
    def serialized_checkpoint():
        with backup_lock:
            return original_write_checkpoint()
    a.write_checkpoint_atomic=serialized_checkpoint
    def backup():
        with backup_lock:a.write_checkpoint_atomic()
    def prediction():
        with lock:return jsonify(dict(cache) if cache else dict(state,groups={k:[] for k in GROUPS},recent=[],stats={}))
    a.boot=boot;a.add_draw=add_draw
    a.app.view_functions['prediction']=prediction
    a.app.view_functions['home']=lambda:HTML
    a.app.view_functions['health']=lambda:jsonify(ok=True,**state)
    for endpoint in ('stats_api','learning_status','auto_status','complement_status','strategy_diagnostics','model_pool_api'):
        if endpoint in a.app.view_functions:
            a.app.view_functions[endpoint]=lambda:jsonify(version=VERSION,message='旧算法已停用，请使用三组正式号码',ready=state['ready'])
    def full_backup():
        with backup_lock:
            if not a.write_checkpoint_atomic():return jsonify(error='备份失败'),500
            return a.send_file(a.CHECKPOINT_PATH,as_attachment=True,download_name='sanfen_full_backup.json.gz',mimetype='application/gzip')
    a.app.view_functions['full_backup']=full_backup

HTML='''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>三组号码 v76</title><style>
*{box-sizing:border-box}body{margin:0;background:#f2f5f8;color:#172738;font:13px -apple-system,BlinkMacSystemFont,sans-serif}main{max-width:560px;margin:auto;padding:10px}header{display:flex;justify-content:space-between;align-items:center}h1{font-size:17px;margin:4px 0}section{background:white;border-radius:10px;padding:10px;margin:8px 0}h2{font-size:13px;margin:0}small,.muted{color:#637386;font-size:11px}.head{display:flex;justify-content:space-between;align-items:center}.nums{display:flex;flex-wrap:wrap;gap:4px;margin:8px 0}.ball{width:25px;height:25px;border-radius:50%;display:grid;place-items:center;background:#eef2f7;font-size:12px;font-weight:600}.red{color:#be3341;background:#ffedf0}.blue{color:#255da9;background:#edf4ff}.green{color:#248250;background:#ecf8ee}button,a{font:11px inherit}button{border:0;border-radius:5px;background:#eaf0f7;padding:5px 9px;color:#304860}table{width:100%;border-collapse:collapse;font-size:11px}th,td{text-align:center;padding:5px 2px;border-bottom:1px solid #edf1f4}.yes{color:#198454}.no{color:#b75151}#status{line-height:1.6;margin-top:6px}footer{font-size:10px;color:#718093;line-height:1.7}a{color:#355c91}#toast{min-height:16px;color:#277055}</style><main><header><h1>三组号码 <small>v76</small></h1><button id="refresh">刷新</button></header><div id="status">正在连接…</div><section><div class="head"><h2>最新开奖</h2><small id="issue">--</small></div><div id="draw" class="nums"></div><small id="next">下一期 --</small></section><div id="groups"></div><div id="toast"></div><section><h2>最近10期正式记录</h2><p class="muted">仅统计开奖前已锁定的新版本号码</p><table><thead><tr><th>期号</th><th>特码</th><th>算法22</th><th>算法16</th><th>多策略22</th></tr></thead><tbody id="records"></tbody></table></section><footer><a href="/api/full-backup">下载完整备份</a> · <a href="/api/export?limit=30000">导出历史</a><br>历史统计不能保证预测未来开奖；命中率按正式锁单结果计算。</footer></main><script>
const labels={algo22:'算法22码',algo16:'算法16码 · 近20期',pool22:'多策略22码'};let data={},busy=false;
const red=new Set([1,2,7,8,12,13,18,19,23,24,29,30,34,35,40,45,46]),blue=new Set([3,4,9,10,14,15,20,25,26,31,36,37,41,42,47,48]);
function balls(ns){return ns.map(n=>`<span class="ball ${red.has(n)?'red':blue.has(n)?'blue':'green'}">${String(n).padStart(2,'0')}</span>`).join('')}
async function load(){if(busy)return;busy=true;try{let r=await fetch('/api/prediction',{cache:'no-store'});if(!r.ok)throw Error('HTTP '+r.status);data=await r.json();document.getElementById('status').textContent=`${data.migration||''} · ${data.count||0}期${data.error?' · '+data.error:''}`;document.getElementById('issue').textContent=data.issue||'--';document.getElementById('next').textContent='下一期 '+(data.next_issue||'--');document.getElementById('draw').innerHTML=balls(data.latest_numbers||[]);document.getElementById('groups').innerHTML=Object.keys(labels).map(k=>{let s=data.stats[k]||{};return `<section><div class="head"><h2>${labels[k]}</h2><button data-copy="${k}" ${data.ready?'':'disabled'}>复制</button></div><div class="nums">${balls(data.groups[k]||[])}</div><small>${data.ready?'开奖前锁定':'等待历史核对'} · 已开${s.n||0} · 中${s.hit||0} · 错${s.miss||0} · ${s.rate==null?'--':s.rate+'%'}</small></section>`}).join('');document.getElementById('records').innerHTML=(data.recent||[]).map(x=>`<tr><td>${x.issue}</td><td>${String(x.actual).padStart(2,'0')}</td>${Object.keys(labels).map(k=>`<td class="${x.hits[k]?'yes':'no'}">${x.hits[k]?'中':'错'}</td>`).join('')}</tr>`).join('')||'<tr><td colspan="5">等待正式开奖</td></tr>'}catch(e){document.getElementById('status').textContent='连接失败，稍后自动重试 · '+e.message}finally{busy=false}}
document.getElementById('refresh').onclick=load;document.getElementById('groups').onclick=async e=>{let k=e.target.dataset.copy;if(!k)return;let text=data.groups[k].map(n=>String(n).padStart(2,'0')).join(',');try{await navigator.clipboard.writeText(text);document.getElementById('toast').textContent='已复制'}catch(_){let t=document.createElement('textarea');t.value=text;document.body.append(t);t.select();document.execCommand('copy');t.remove();document.getElementById('toast').textContent='已复制'}};load();setInterval(()=>{if(!document.hidden)load()},10000);document.addEventListener('visibilitychange',()=>{if(!document.hidden)load()});</script></html>'''
