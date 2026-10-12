"""v79: four groups, ten-period rounds, preserved pre-draw locks."""
import os, json, threading, time, gzip
from collections import Counter
from flask import jsonify

VERSION='v80_hot5_cold1_spread16'
GROUPS={'algo22':('算法22码',22),'algo16':('算法16码 · 近20期',16),'pool22':('多策略22码',22),'zodiac24':('6肖24码',24)}

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
            if 'meta_json' not in {x[1] for x in c.execute('PRAGMA table_info(compact_forecasts)')}:
                c.execute("ALTER TABLE compact_forecasts ADD COLUMN meta_json TEXT NOT NULL DEFAULT '{}'")
            c.commit()
        finally:c.close()
    def build(r,records):
        from enhanced import predict,METHODS
        controls={}
        for k,options in METHODS.items():
            eligible={}
            for x in records:
                if x['actual'] is not None and json.loads(x['groups_json']).get(k):eligible[x['target']]=x
            recent=sorted(eligible.values(),key=lambda x:x['target'],reverse=True)[:2]
            def key(x):return json.loads(x.get('meta_json') or '{}').get('methods',{}).get(k,{}).get('key',options[0])
            current=key(recent[0]) if recent else options[0]
            misses=0;previous_target=None
            from rounds import ordinal
            for x in recent:
                if previous_target is not None and ordinal(previous_target)-ordinal(x['target'])!=1:break
                if key(x)!=current or x['actual'] in json.loads(x['groups_json'])[k]:break
                misses+=1;previous_target=x['target']
            controls[k]={'current_key':current,'miss_run':misses,'force_switch':misses>=2}
        return predict(r,controls)
    def refresh():
        started=time.perf_counter()
        with lock,a.db_lock:
            stored_r=a.recent_rows(500)
            if not stored_r:return
            from engine import consecutive_history,recent_history,period_window,period_ordinal
            r=recent_history(stored_r)
            continuous=consecutive_history(stored_r)
            quality={'recent_consecutive':len(continuous),'usable_recent':len(r),'missing_recent':max(0,period_ordinal(str(r[0]['issue']))-period_ordinal(str(r[-1]['issue']))+1-len(r)),'loaded_rows':len(stored_r),'gap_detected':len(r)<len(stored_r),'gap_older_issue':str(stored_r[len(r)]['issue']) if len(r)<len(stored_r) else '', 'algo16_window_used':len(period_window(r,20)), 'algo22_medium_used':len(period_window(r,80)), 'algo22_long_used':len(period_window(r,500)), 'pool_window_used':len(period_window(r,60))}
            cutoff=str(r[0]['issue']);target=a._next_issue_id(cutoff)
            c=a.connect()
            try:
                # Settle only predictions genuinely locked before their target draw existed.
                c.execute('UPDATE compact_forecasts SET actual=(SELECT special FROM draws WHERE issue=target) WHERE actual IS NULL AND EXISTS(SELECT 1 FROM draws WHERE issue=target)',())
                current_meta={};active_versions={};groups={k:[] for k in GROUPS}
                if state['ready']:
                    existing=c.execute('SELECT groups_json,meta_json,version FROM compact_forecasts WHERE target=? AND actual IS NULL ORDER BY created_at,version',(target,)).fetchall()
                    for x in existing:
                        for k,ns in json.loads(x['groups_json']).items():
                            if k in GROUPS and ns:groups[k]=ns;active_versions[k]=x['version']
                        current_meta.update(json.loads(x['meta_json'] or '{}'))
                    missing=[k for k,ns in groups.items() if not ns]
                    if missing:
                        ledger=[dict(x) for x in c.execute('SELECT * FROM compact_forecasts WHERE actual IS NOT NULL ORDER BY target,created_at,version')]
                        calculated,metadata=build(r,ledger);new_groups={k:calculated[k] for k in missing}
                        metadata['methods']={k:v for k,v in metadata.get('methods',{}).items() if k in missing}
                        if 'algo16' not in missing:metadata.pop('algo16',None)
                        # Preserve existing current-period locks; append only the new group when upgrading.
                        c.execute('INSERT OR IGNORE INTO compact_forecasts(target,version,cutoff,groups_json,meta_json) VALUES (?,?,?,?,?)',(target,VERSION,cutoff,json.dumps(new_groups),json.dumps(metadata,ensure_ascii=False)))
                        groups.update(new_groups);current_meta.update(metadata)
                        active_versions.update({k:VERSION for k in missing})
                    calculation_mode='新开奖重算' if missing else '保留本期锁单'
                    active_version=VERSION if all(v==VERSION for v in active_versions.values()) else '升级过渡锁单'
                    for k,gate in current_meta.get('gates',{}).items():gate['applied']=bool(gate.get('enabled') and active_versions.get(k)==VERSION)
                else:
                    active_version=VERSION;calculation_mode='等待历史核对'
                c.commit()
                records=[dict(x) for x in c.execute('SELECT * FROM compact_forecasts ORDER BY target,created_at,version')]
                rows=records
                allstats=[x for x in records if x['version']==VERSION and x['actual'] is not None]

            finally:c.close()
            from rounds import summarize
            round_stats,round_history=summarize(records,GROUPS,target,VERSION,active_versions)
            stats={}
            for k in GROUPS:
                latest_settled={}
                for x in records:
                    if x['actual'] is not None and json.loads(x['groups_json']).get(k):latest_settled[x['target']]=x
                eligible=list(latest_settled.values())
                n=len(eligible);h=sum(x['actual'] in json.loads(x['groups_json'])[k] for x in eligible)
                stats[k]={'n':n,'hit':h,'miss':n-h,'rate':round(h/n*100,1) if n else None}
            previous_by_group={}
            for x in records:
                if x['target']==cutoff and x['actual'] is not None:
                    previous_by_group.update({k:set(ns) for k,ns in json.loads(x['groups_json']).items() if ns})
            diagnostics={}
            for k in GROUPS:
                before=previous_by_group.get(k);after=set(groups[k])
                diagnostics[k]={'added':sorted(after-before) if before is not None else [],'removed':sorted(before-after) if before is not None else [],'changed_count':len(after-before) if before is not None else None,'latest_actual_preselected':r[0]['special'] in before if before is not None else None,'latest_actual_next_selected':r[0]['special'] in after,'comparison_available':before is not None}
            next_locks={};settled_by_target={}
            for x in records:
                next_locks.setdefault(x['target'],{}).update(json.loads(x['groups_json']))
                if x['actual'] is not None:
                    entry=settled_by_target.setdefault(x['target'],{'issue':x['target'],'actual':x['actual'],'hits':{},'versions':{}})
                    for k,ns in json.loads(x['groups_json']).items():
                        if ns:entry['hits'][k]=x['actual'] in ns;entry['versions'][k]=x['version'].split('_')[0]
            recent=[];chasing=[]
            for entry in sorted(settled_by_target.values(),key=lambda x:x['issue'],reverse=True)[:100]:
                nxt=next_locks.get(a._next_issue_id(entry['issue']),{})
                chasing.append({'issue':entry['issue'],'actual':entry['actual'],'pre_hits':entry['hits'],'added_after':{k:(not entry['hits'][k] and entry['actual'] in nxt[k]) if k in entry['hits'] and k in nxt else None for k in GROUPS}})
                if len(recent)<10:recent.append(entry)
            count,latest=dbmeta();state.update(count=count,latest=latest)
            calculation={'history_cutoff':cutoff,'target':target,'mode':calculation_mode,'completed_at':time.strftime('%Y-%m-%d %H:%M:%S'),'elapsed_ms':round((time.perf_counter()-started)*1000,2)}

            cache.clear();cache.update(version=VERSION,active_version=active_version,active_versions=active_versions,**state,issue=cutoff,next_issue=target,latest_numbers=[r[0][f'n{i}'] for i in range(1,7)]+[r[0]['special']],groups={k:sorted(ns) for k,ns in groups.items()},stats=stats,rounds=round_stats,round_history=round_history,group_meta=current_meta,data_quality=quality,calculation=calculation,diagnostics=diagnostics,chasing=chasing,stale_prediction=False,recent=recent)
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
    def restore_compact(path=None):
        path=path or a.CHECKPOINT_PATH
        if not os.path.exists(path):return
        with gzip.open(path,'rt',encoding='utf-8') as f:p=json.load(f)
        c=a.connect()
        try:
            for x in p.get('compact_forecasts',[]):
                c.execute('INSERT OR IGNORE INTO compact_forecasts(target,version,cutoff,groups_json,actual,created_at,meta_json) VALUES (?,?,?,?,?,?,?)',tuple(x.get(k) for k in ('target','version','cutoff','groups_json','actual','created_at'))+(x.get('meta_json') or '{}',))
            c.commit()
        finally:c.close()
    def sync_previous_compact():
        bases=[]
        if a.WEBHOOK_BASE_URL:bases.append(a.WEBHOOK_BASE_URL)
        if a.HISTORY_SOURCE_URL:bases.append(a.HISTORY_SOURCE_URL.split('/api/',1)[0])
        for base in dict.fromkeys(bases):
            try:
                resp=a.requests.get(base.rstrip('/')+'/api/full-backup',timeout=15)
                resp.raise_for_status()
                previous=json.loads(gzip.decompress(resp.content))
                if not isinstance(previous.get('compact_forecasts'),list):continue
                a._restore_draw_dicts(previous.get('draws',[]))
                temporary=a.CHECKPOINT_PATH+'.inherit.tmp'
                with gzip.open(temporary,'wt',encoding='utf-8') as f:json.dump(previous,f,ensure_ascii=False)
                try:restore_compact(temporary)
                finally:
                    if os.path.isfile(temporary):os.unlink(temporary)
                state['inherited_lock_count']=len(previous['compact_forecasts'])
                return
            except Exception as e:
                state['lock_sync_error']=f'{type(e).__name__}: {e}'
    def initialize():
        try:
            initial_count,initial_latest=dbmeta()
            if a.REMOTE_BACKUP_ENABLED:a.download_checkpoint_from_supabase()
            a.restore_checkpoint_if_better();restore_compact()
            inherited=os.path.join(os.path.dirname(__file__),'inherited_snapshot.json.gz')
            if os.path.isfile(inherited):
                with gzip.open(inherited,'rt',encoding='utf-8') as f:snapshot=json.load(f)
                a._restore_draw_dicts(snapshot.get('draws',[]))
                restore_compact(inherited)
            # Merge bundled baseline ONLY when no inherited database is available.
            a.import_history_once()
            a.sync_best_available_history()
            sync_previous_compact()
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
            state.update(error='',ready=ok,count=count,latest=latest,migration='历史继承核对通过' if ok else '待核对历史：设置旧版最新期号和总条数后启用')
            a.auto_state['migration_latest_issue']=latest;a.auto_state['migration_history_count']=count
            a.background_state['boot_ready']=True
            refresh();a.refresh_history_cache()
            if a.BOT_TOKEN:threading.Thread(target=a.webhook_keeper,daemon=True).start()
        except Exception as e:
            state.update(ready=False,error=f'{type(e).__name__}: {e}',migration='历史核对失败');cache.update(state)
    def boot():
        with lock:
            state['ready']=False;cache.clear()
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
            if changed or cache.get('issue')!=str(a.latest_row()['issue'] if a.latest_row() else ''):
                try:
                    refresh();a.refresh_history_cache();state['error']=''
                except Exception as e:
                    state['error']=f'重算失败，将自动重试: {type(e).__name__}: {e}'
                    cache.update(error=state['error'],stale_prediction=True)
                if changed:threading.Thread(target=backup,daemon=True).start()
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
        with lock:
            if state['ready']:
                latest=a.latest_row()
                if latest and (cache.get('issue')!=str(latest['issue']) or cache.get('stale_prediction')):
                    try:
                        refresh();state['error']='';cache['error']=''
                    except Exception as e:
                        cache.update(error=f'重算失败: {type(e).__name__}: {e}',stale_prediction=True)
            return jsonify(dict(cache) if cache else dict(state,ready=False,groups={k:[] for k in GROUPS},recent=[],stats={}))
    @a.app.get('/api/rounds')
    def rounds_api():
        with lock:return jsonify(version=VERSION,current=cache.get('rounds',{}),history=cache.get('round_history',{}))
    @a.app.get('/api/prediction-audit')
    def prediction_audit():
        with lock:
            return jsonify(version=VERSION,calculation=cache.get('calculation',{}),diagnostics=cache.get('diagnostics',{}),group_meta=cache.get('group_meta',{}),data_quality=cache.get('data_quality',{}),chasing=cache.get('chasing',[]),error=cache.get('error',''))
    @a.app.get('/api/live-audit')
    def live_audit():
        path=os.path.join(os.path.dirname(__file__),'live_audit_v79.json')
        with open(path,encoding='utf-8') as f:return jsonify(json.load(f))
    @a.app.get('/api/validation-report')
    def validation_report():
        path=os.path.join(os.path.dirname(__file__),'validation_v80_16.json')
        if not os.path.isfile(path):return jsonify(error='验证报告未生成'),404
        with open(path,encoding='utf-8') as f:return jsonify(json.load(f))
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

HTML=r'''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>四组号码 v80</title><style>
*{box-sizing:border-box}body{margin:0;background:#f2f5f8;color:#172738;font:13px -apple-system,BlinkMacSystemFont,sans-serif}main{max-width:560px;margin:auto;padding:10px}header,.head{display:flex;justify-content:space-between;align-items:center}h1{font-size:17px;margin:4px 0}section{background:#fff;border-radius:10px;padding:10px;margin:8px 0}h2{font-size:13px;margin:0}small,.muted{color:#637386;font-size:11px}.nums{display:flex;flex-wrap:wrap;gap:4px;margin:8px 0}.ball{width:25px;height:25px;border-radius:50%;display:grid;place-items:center;background:#eef2f7;font-size:12px;font-weight:600}.red{color:#be3341;background:#ffedf0}.blue{color:#255da9;background:#edf4ff}.green{color:#248250;background:#ecf8ee}button{font:11px inherit;border:0;border-radius:5px;background:#eaf0f7;padding:5px 9px;color:#304860}a{color:#355c91}table{width:100%;border-collapse:collapse;font-size:10px}th,td{text-align:center;padding:5px 1px;border-bottom:1px solid #edf1f4}.yes{color:#198454}.no{color:#b75151}#status{line-height:1.6;margin-top:6px}footer{font-size:10px;color:#718093;line-height:1.7}#toast{min-height:14px;color:#277055}.round{font-size:12px;line-height:1.8}.range{font-size:10px;color:#637386}.summary{font-size:10px;line-height:1.6;color:#637386}details{margin-top:4px}.zodiac{font-size:11px;color:#406281;margin-bottom:6px}strong{font-weight:600}</style>
<main><header><h1>四组号码 <small>v80</small></h1><button id="refresh">刷新</button></header><div id="status">正在连接…</div><section><div class="head"><h2>最新开奖</h2><small id="issue">--</small></div><div id="draw" class="nums"></div><small id="next">下一期 --</small><div id="calculation" class="muted"></div></section><div id="groups"></div><div id="toast"></div><section><h2>最近10期正式记录</h2><div class="summary">每期重算、开奖前锁单；每组独立按10期统计一轮</div><table><thead><tr><th>期号</th><th>特码</th><th>算法22</th><th>算法16</th><th>多策略22</th><th>6肖24</th></tr></thead><tbody id="records"></tbody></table></section><footer><a href="/api/full-backup">完整备份</a> · <a href="/api/rounds">轮次记录</a> · <a href="/api/prediction-audit">计算诊断</a><br>预测方案需要后续实盘验证，命中率不作保证。</footer></main>
<script>
const labels={algo22:'算法22码',algo16:'算法16码 · 8热8冷',pool22:'多策略22码',zodiac24:'24码 · 5热肖1冷肖'};let data={},busy=false;
const red=new Set([1,2,7,8,12,13,18,19,23,24,29,30,34,35,40,45,46]),blue=new Set([3,4,9,10,14,15,20,25,26,31,36,37,41,42,47,48]);
function balls(ns){return ns.map(n=>`<span class="ball ${red.has(n)?'red':blue.has(n)?'blue':'green'}">${String(n).padStart(2,'0')}</span>`).join('')}
function mark(x){return x==null?'—':x?'<span class="yes">中</span>':'<span class="no">错</span>'}
function roundtext(r){return `第${r.number||1}轮 · 已开${r.opened||0}/10 · <strong>中${r.hit||0} 错${r.miss||0}</strong>`}
async function load(){if(busy)return;busy=true;try{let res=await fetch('/api/prediction',{cache:'no-store'});if(!res.ok)throw Error('HTTP '+res.status);data=await res.json();document.getElementById('status').textContent=`${data.migration||''} · ${data.count||0}期${data.error?' · '+data.error:''}`;document.getElementById('issue').textContent=data.issue||'--';document.getElementById('next').textContent='下一期 '+(data.next_issue||'--');document.getElementById('draw').innerHTML=balls(data.latest_numbers||[]);let calc=data.calculation||{},q=data.data_quality||{};document.getElementById('calculation').textContent=`${calc.mode||''} · 可用${q.usable_recent??'--'}期，连${q.recent_consecutive??'--'}期，缺${q.missing_recent||0}期 · ${calc.elapsed_ms??'--'}ms`;
document.getElementById('groups').innerHTML=Object.keys(labels).map(k=>{let st=(data.stats||{})[k]||{},rd=(data.rounds||{})[k]||{},dg=(data.diagnostics||{})[k]||{},meta=data.group_meta||{},zm=meta.zodiac24||{},gate=(meta.gates||{})[k]||{},hist=(data.round_history||{})[k]||[],ver=((data.active_versions||{})[k]||'').split('_')[0];let hm=meta.algo16||{};let split=k==='algo16'?(hm.hot?`<details><summary class="summary">热8：${hm.hot.map(n=>String(n).padStart(2,'0')).join(',')} · 冷8：${hm.cold.map(n=>String(n).padStart(2,'0')).join(',')}</summary><div class="summary">冷码在近20期未开或只开1次，并排除热8；按波色单双、生肖和尾数分散。${hm.insufficient_hot_history?'热码样本不足，已按相对热度补足。':''}</div></details>`:'<div class="summary">旧版本期锁单保留，下个未锁期启用8热8冷</div>'):'';let z=k==='zodiac24'?`<div class="zodiac">${zm.hot_zodiacs?'热肖：'+zm.hot_zodiacs.join('、')+' · 防冷：'+zm.cold_zodiac:'旧版本期6肖：'+(zm.zodiacs||[]).join('、')} · 每肖4码</div>`:'';let method=(meta.methods||{})[k]||{};let rule=k==='zodiac24'?'6肖与24码分别结算':gate.applied?'融合已启用：通过开奖前逐期比较':k==='algo16'?'仅使用近20期':ver&&ver!=='v80'?'升级保留原锁单':gate.validation_n?'融合候选未通过，沿用原方案':'';
return `<section><div class="head"><h2>${labels[k]}</h2><button data-copy="${k}" ${data.ready?'':'disabled'}>复制</button></div><div class="nums">${balls([...(data.groups[k]||[])].sort((a,b)=>a-b))}</div>${z}${split}<div class="round">${roundtext(rd)}</div><div class="range">本轮 ${rd.start||'--'} — ${rd.end||'--'}${k==='zodiac24'&&rd.zodiac_opened?' · 6肖中'+rd.zodiac_hit+'错'+rd.zodiac_miss:''}</div><div class="summary">累计（含旧版）已开${st.n||0} · 中${st.hit||0}错${st.miss||0} · ${st.rate==null?'--':st.rate+'%'} · ${dg.changed_count==null?'无前期对照':'换入'+dg.changed_count+'码'} · ${ver}锁定</div><div class="summary">${method.label?method.label+' · '+(method.switched?method.trigger:'本方法已连错'+(method.miss_run||0)+'期')+' · ':''}${rule}</div>${hist.length?`<details><summary class="summary">之前${hist.length}轮</summary>${hist.slice().reverse().map(r=>`<div class="summary">${roundtext(r)} · ${r.start}—${r.end}</div>`).join('')}</details>`:''}${k==='zodiac24'?`<details><summary class="summary">各肖号码</summary><div class="summary">${(zm.zodiacs||[]).map(z=>z+'：'+(zm.by_zodiac?.[z]||[]).map(n=>String(n).padStart(2,'0')).join(',')).join(' · ')}</div></details>`:''}</section>`}).join('');
document.getElementById('records').innerHTML=(data.recent||[]).map(x=>`<tr><td>${x.issue}</td><td>${String(x.actual).padStart(2,'0')}</td>${Object.keys(labels).map(k=>`<td>${mark(x.hits[k])}</td>`).join('')}</tr>`).join('')||'<tr><td colspan="6">等待正式开奖</td></tr>';
}catch(e){document.getElementById('status').textContent='连接失败，自动重试 · '+e.message}finally{busy=false}}
document.getElementById('refresh').onclick=load;document.getElementById('groups').onclick=async e=>{let k=e.target.dataset.copy;if(!k)return;let text=[...(data.groups[k]||[])].sort((a,b)=>a-b).map(n=>String(n).padStart(2,'0')).join(',');try{await navigator.clipboard.writeText(text)}catch(_){let t=document.createElement('textarea');t.value=text;document.body.append(t);t.select();document.execCommand('copy');t.remove()}document.getElementById('toast').textContent='已复制'};load();setInterval(()=>{if(!document.hidden)load()},10000);document.addEventListener('visibilitychange',()=>{if(!document.hidden)load()});
</script></html>'''
