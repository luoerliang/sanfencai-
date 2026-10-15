"""v79: four groups, ten-period rounds, preserved pre-draw locks."""
import os, json, threading, time, gzip
from collections import Counter
from flask import jsonify

VERSION='v83_probability_modules'
APP_VERSION='v84_2_bandwidth_fix'
SINGLE_VERSION='v81_3_single_wave'
WAVE_VERSION='v81_dual_wave'
GROUPS={'algo22':('算法22码',22),'algo16':('算法16码 · 近20期',16),'pool22':('多策略22码',22),'zodiac24':('6肖24码',24)}

def install(a):
    lock=threading.RLock()
    state={'ready':False,'error':'','migration':'正在核对历史','latest':'','count':0}
    cache={}
    from incremental_backup import Backup, setup
    remote=Backup(a,APP_VERSION,VERSION)
    a.incremental_backup=remote
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
            c.execute('CREATE TABLE IF NOT EXISTS wave_forecasts(target TEXT,version TEXT,cutoff TEXT,colors_json TEXT,info_json TEXT,actual_wave TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(target,version))')
            if 'meta_json' not in {x[1] for x in c.execute('PRAGMA table_info(compact_forecasts)')}:
                c.execute("ALTER TABLE compact_forecasts ADD COLUMN meta_json TEXT NOT NULL DEFAULT '{}'")
            from parity import create_table
            create_table(c)
            setup(c,enable=False)
            c.commit()
        finally:c.close()
    def build(r,records):
        from module_models import number_forecast
        return number_forecast(r,records)
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
                        ledger=lambda: (dict(x) for x in c.execute('SELECT * FROM compact_forecasts WHERE actual IS NOT NULL ORDER BY target DESC,created_at DESC,version DESC'))
                        calculated,metadata=build(r,ledger);new_groups={k:calculated[k] for k in missing}
                        metadata['methods']={k:v for k,v in metadata.get('methods',{}).items() if k in missing}
                        metadata['probability_models']={k:v for k,v in metadata.get('probability_models',{}).items() if k in missing}
                        metadata['algorithm_revisions']={k:v for k,v in metadata.get('algorithm_revisions',{}).items() if k in missing}
                        if 'algo16' not in missing:
                            metadata.pop('algo16',None);metadata.pop('algo16_candidates',None)
                        # Preserve existing current-period locks; append only the new group when upgrading.
                        c.execute('INSERT OR IGNORE INTO compact_forecasts(target,version,cutoff,groups_json,meta_json) VALUES (?,?,?,?,?)',(target,VERSION,cutoff,json.dumps(new_groups),json.dumps(metadata,ensure_ascii=False)))
                        groups.update(new_groups);current_meta.update(metadata)
                        active_versions.update({k:VERSION for k in missing})
                    calculation_mode='新开奖重算' if missing else '保留本期锁单'
                    active_version=VERSION if all(v==VERSION for v in active_versions.values()) else '升级过渡锁单'
                    for k,gate in current_meta.get('gates',{}).items():gate['applied']=bool(gate.get('enabled') and active_versions.get(k)==VERSION)
                else:
                    active_version=VERSION;calculation_mode='等待历史核对'
                from dual_wave import forecast as wave_forecast
                for pending in c.execute('SELECT target,version FROM wave_forecasts WHERE actual_wave IS NULL').fetchall():
                    actual=c.execute('SELECT special FROM draws WHERE issue=?',(pending['target'],)).fetchone()
                    if actual is not None:
                        from engine import wave
                        c.execute('UPDATE wave_forecasts SET actual_wave=? WHERE target=? AND version=?',(wave(int(actual['special'])),pending['target'],pending['version']))
                locked_wave=c.execute('SELECT * FROM wave_forecasts WHERE target=? AND version=?',(target,WAVE_VERSION)).fetchone()
                if state['ready'] and locked_wave is None:
                    previous_wave=[dict(x) for x in c.execute('SELECT * FROM wave_forecasts WHERE version=? AND actual_wave IS NOT NULL ORDER BY target DESC LIMIT 120',(WAVE_VERSION,))]
                    from module_models import wave_forecast as probability_wave
                    wf=probability_wave(r,previous_wave)
                    c.execute('INSERT OR IGNORE INTO wave_forecasts(target,version,cutoff,colors_json,info_json) VALUES (?,?,?,?,?)',(target,WAVE_VERSION,cutoff,json.dumps(wf['colors'],ensure_ascii=False),json.dumps(wf,ensure_ascii=False)))
                    locked_wave=c.execute('SELECT * FROM wave_forecasts WHERE target=? AND version=?',(target,WAVE_VERSION)).fetchone()
                wave_rows=[dict(x) for x in c.execute('SELECT target,version,cutoff,colors_json,actual_wave,created_at FROM wave_forecasts WHERE version=? ORDER BY target',(WAVE_VERSION,))]
                locked_single=c.execute('SELECT * FROM wave_forecasts WHERE target=? AND version=?',(target,SINGLE_VERSION)).fetchone()
                if state['ready'] and locked_single is None and locked_wave is not None:
                    from module_models import wave_forecast as probability_wave
                    prior_single=(dict(x) for x in c.execute('SELECT * FROM wave_forecasts WHERE version=? ORDER BY target DESC',(SINGLE_VERSION,)))
                    si=probability_wave(r,prior_single,True);color=si['color']
                    c.execute('INSERT OR IGNORE INTO wave_forecasts(target,version,cutoff,colors_json,info_json) VALUES (?,?,?,?,?)',(target,SINGLE_VERSION,cutoff,json.dumps([color],ensure_ascii=False),json.dumps(si,ensure_ascii=False)))
                    locked_single=c.execute('SELECT * FROM wave_forecasts WHERE target=? AND version=?',(target,SINGLE_VERSION)).fetchone()
                single_rows=[dict(x) for x in c.execute('SELECT target,version,cutoff,colors_json,actual_wave,created_at FROM wave_forecasts WHERE version=? ORDER BY target',(SINGLE_VERSION,))]

                from parity import refresh as refresh_parity
                parity_data=refresh_parity(c,r,target,state['ready'],json.loads(locked_single['info_json']) if locked_single else {})
                c.commit()
                records=[dict(x) for x in c.execute("SELECT target,version,cutoff,groups_json,actual,created_at,json_object('algorithm_revisions',json(COALESCE(json_extract(meta_json,'$.algorithm_revisions'),'{}')),'zodiac24',json(COALESCE(json_extract(meta_json,'$.zodiac24'),'{}'))) AS meta_json FROM compact_forecasts ORDER BY target,created_at,version")]

            finally:c.close()
            from rounds import summarize
            round_stats,round_history=summarize(records,GROUPS,target,VERSION,active_versions)
            from audit_policy import group_stats,revision,ALGO16_REVISION
            stats={k:group_stats(records,k) for k in GROUPS}
            revised16=[x for x in records if revision(x,'algo16')==ALGO16_REVISION]
            new_round,new_history=summarize(revised16,{'algo16':GROUPS['algo16']},target,VERSION,{'algo16':VERSION})
            legacy16=[x for x in records if revision(x,'algo16')!=ALGO16_REVISION]
            old_round,old_history=summarize(legacy16,{'algo16':GROUPS['algo16']},target,VERSION,{'algo16':VERSION})
            archived16_rounds=[x for x in old_history['algo16']+[old_round['algo16']] if x['locked']]
            round_stats['algo16']=new_round['algo16'];round_history['algo16']=new_history['algo16']
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

            from memory_io import peak_rss_mib
            calculation['process_peak_rss_mib']=peak_rss_mib()
            print(f"[PERF] refresh issue={cutoff} ms={calculation['elapsed_ms']} process_peak_rss_mib={calculation['process_peak_rss_mib']}",flush=True)
            from dual_wave import streaks
            settled_wave=[x for x in wave_rows if x['actual_wave'] is not None]
            wh=sum(x['actual_wave'] in json.loads(x['colors_json']) for x in settled_wave)
            pseudo=[dict(target=x['target'],version=x['version'],created_at=x['created_at'],groups_json=json.dumps({'dual_wave':json.loads(x['colors_json'])}),actual=x['actual_wave'],meta_json='{}') for x in wave_rows]
            wr,wrh=summarize(pseudo,{'dual_wave':('双波色',2)},target,WAVE_VERSION,{'dual_wave':WAVE_VERSION})
            bits=[x['actual_wave'] in json.loads(x['colors_json']) for x in settled_wave]
            latest_wave_lock=next((x for x in reversed(wave_rows) if x['target']==cutoff and x['actual_wave'] is not None),None)
            from engine import wave
            wave_data={'current_draw':{'issue':cutoff,'special':int(r[0]['special']),'wave':wave(int(r[0]['special']))},'current_prediction':{'colors':json.loads(latest_wave_lock['colors_json']),'hit':latest_wave_lock['actual_wave'] in json.loads(latest_wave_lock['colors_json'])} if latest_wave_lock else None,'next_issue':target,'colors':json.loads(locked_wave['colors_json']) if locked_wave else [],'info':json.loads(locked_wave['info_json']) if locked_wave else {},'n':len(settled_wave),'hit':wh,'miss':len(settled_wave)-wh,'rate':round(100*wh/len(settled_wave),1) if settled_wave else None,'streaks':streaks(bits,[x['target'] for x in settled_wave]),'round':wr['dual_wave'],'round_history':wrh['dual_wave'],'recent':[{'issue':x['target'],'recommended':json.loads(x['colors_json']),'actual':x['actual_wave'],'hit':x['actual_wave'] in json.loads(x['colors_json'])} for x in settled_wave[-10:][::-1]]}
            single_settled=[x for x in single_rows if x['actual_wave'] is not None]
            single_hit=sum(x['actual_wave'] in json.loads(x['colors_json']) for x in single_settled)
            single_pseudo=[dict(target=x['target'],version=x['version'],created_at=x['created_at'],groups_json=json.dumps({'single_wave':json.loads(x['colors_json'])}),actual=x['actual_wave'],meta_json='{}') for x in single_rows]
            common_origin=min([x['target'] for x in wave_rows+single_rows],key=period_ordinal) if wave_rows or single_rows else target
            sr,srh=summarize(single_pseudo,{'single_wave':('单波推荐',1)},target,SINGLE_VERSION,{'single_wave':SINGLE_VERSION},{'single_wave':common_origin})
            current_single=next((x for x in reversed(single_settled) if x['target']==cutoff),None)
            single_data={'next_issue':target,'colors':json.loads(locked_single['colors_json']) if locked_single else [],'info':json.loads(locked_single['info_json']) if locked_single else {},'n':len(single_settled),'hit':single_hit,'miss':len(single_settled)-single_hit,'rate':round(100*single_hit/len(single_settled),1) if single_settled else None,'round':sr['single_wave'],'round_history':srh['single_wave'],'current_prediction':{'color':json.loads(current_single['colors_json'])[0],'hit':current_single['actual_wave'] in json.loads(current_single['colors_json'])} if current_single else None,'recent':[{'issue':x['target'],'color':json.loads(x['colors_json'])[0],'actual':x['actual_wave'],'hit':x['actual_wave'] in json.loads(x['colors_json'])} for x in single_settled[-10:][::-1]]}
            wave_by_target={x['target']:x for x in wave_rows};single_by_target={x['target']:x for x in single_rows}
            wave_history=[]
            for draw in r[:100]:
                issue=str(draw['issue']);actual_color=wave(int(draw['special']));dwlock=wave_by_target.get(issue);swlock=single_by_target.get(issue)
                pair=json.loads(dwlock['colors_json']) if dwlock else [];single=json.loads(swlock['colors_json']) if swlock else []
                wave_history.append({'issue':issue,'special':int(draw['special']),'waves':[wave(int(draw[f'n{i}'])) for i in range(1,7)]+[actual_color],'actual':actual_color,'dual':pair,'single':single,'dual_hit':actual_color in pair if pair else None,'single_hit':actual_color in single if single else None})
            cache.clear();cache.update(parity=parity_data,archived16_rounds=archived16_rounds,wave_history=wave_history,single_wave=single_data,dual_wave=wave_data,version=APP_VERSION,active_version=active_version,active_versions=active_versions,**state,issue=cutoff,next_issue=target,latest_numbers=[r[0][f'n{i}'] for i in range(1,7)]+[r[0]['special']],groups={k:sorted(ns) for k,ns in groups.items()},stats=stats,rounds=round_stats,round_history=round_history,group_meta=current_meta,data_quality=quality,calculation=calculation,diagnostics=diagnostics,chasing=chasing,stale_prediction=False,recent=recent)
    old_payload=a._checkpoint_payload
    def payload():
        p=old_payload();c=a.connect()
        try:
            p['draws']=[dict(x) for x in c.execute('SELECT * FROM draws ORDER BY issue DESC')]
            p['compact_forecasts']=[dict(x) for x in c.execute('SELECT * FROM compact_forecasts')]
            p['parity_forecasts']=[dict(x) for x in c.execute('SELECT * FROM parity_forecasts')]
            p['version']=APP_VERSION
            p['core_version']=VERSION
            p['wave_forecasts']=[dict(x) for x in c.execute('SELECT * FROM wave_forecasts')]
        finally:c.close()
        return p
    a._checkpoint_payload=payload
    def restore_compact(path=None,include_learning=False):
        from memory_io import restore_checkpoint
        return restore_checkpoint(a,path or a.CHECKPOINT_PATH,include_learning)
    def sync_previous_compact():
        bases=[]
        if a.WEBHOOK_BASE_URL:bases.append(a.WEBHOOK_BASE_URL)
        if a.HISTORY_SOURCE_URL:bases.append(a.HISTORY_SOURCE_URL.split('/api/',1)[0])
        for base in dict.fromkeys(bases):
            try:
                from memory_io import download_file
                temporary=a.CHECKPOINT_PATH+'.inherit.tmp'
                try:
                    download_file(a,base.rstrip('/')+'/api/full-backup',temporary,{},15)
                    state['inherited_lock_count']=restore_compact(temporary)
                finally:
                    if os.path.isfile(temporary):os.unlink(temporary)
                return
            except Exception as e:
                state['lock_sync_error']=f'{type(e).__name__}: {e}'
    def initialize():
        try:
            initial_count,initial_latest=dbmeta()
            restored=remote.restore()
            if not restored:
                if a.REMOTE_BACKUP_ENABLED:
                    downloaded=a.download_checkpoint_from_supabase()
                    if not downloaded and a.auto_state.get('remote_backup_error'):
                        raise RuntimeError('远程备份读取失败，停止启用以保护历史：'+a.auto_state['remote_backup_error'])
                restore_compact(include_learning=True)
            c=a.connect()
            try:setup(c,enable=True)
            finally:c.close()
            inherited=os.path.join(os.path.dirname(__file__),'inherited_snapshot.json.gz')
            if os.path.isfile(inherited):
                restore_compact(inherited)
            # Merge bundled baseline ONLY when no inherited database is available.
            a.import_history_once()
            a.sync_best_available_history()
            if not restored:sync_previous_compact()
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
            if state['ready']:request_backup()
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
                if changed:request_backup()
            return changed
    backup_lock=threading.RLock()
    from memory_io import write_checkpoint
    original_write_checkpoint=lambda: write_checkpoint(a,APP_VERSION,VERSION,remote_upload=False)
    def serialized_checkpoint():
        with backup_lock:
            if not state['ready']:
                a.auto_state['checkpoint_error']='历史核对未通过，保留原备份'
                return False
            return original_write_checkpoint()
    a.write_checkpoint_atomic=serialized_checkpoint
    # At most one pending backup; do not accumulate waiting threads per draw.
    backup_event=threading.Event()
    def backup_worker():
        while True:
            backup_event.wait(timeout=60);backup_event.clear()
            if state['ready']:
                with backup_lock:remote.sync()
    threading.Thread(target=backup_worker,daemon=True,name='checkpoint-worker').start()
    def request_backup():
        backup_event.set()
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
    from web_transfer import install as install_transfer
    install_transfer(a,cache,lock,prediction)
    @a.app.get('/api/parity')
    def parity_api():
        prediction()
        with lock:return jsonify(cache.get('parity',{}))
    @a.app.get('/api/dual-wave')
    def dual_wave_api():
        with lock:return jsonify(cache.get('dual_wave',{}))
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
        path=os.path.join(os.path.dirname(__file__),'validation_v82_16.json')
        if not os.path.isfile(path):return jsonify(error='验证报告未生成'),404
        with open(path,encoding='utf-8') as f:return jsonify(archived=True,report_revision='v82_fixed_blend20',active_revision=APP_VERSION,report=json.load(f))
    a.boot=boot;a.add_draw=add_draw
    a.app.view_functions['prediction']=prediction
    a.app.view_functions['home']=lambda:HTML
    a.app.view_functions['health']=lambda:jsonify(ok=True,**state)
    for endpoint in ('stats_api','learning_status','auto_status','complement_status','strategy_diagnostics','model_pool_api'):
        if endpoint in a.app.view_functions:
            a.app.view_functions[endpoint]=lambda:jsonify(version=VERSION,message='旧算法已停用，请使用四组正式号码',ready=state['ready'])
    def full_backup():
        with backup_lock:
            if not a.write_checkpoint_atomic():return jsonify(error='备份失败'),500
            return a.send_file(a.CHECKPOINT_PATH,as_attachment=True,download_name='sanfen_full_backup.json.gz',mimetype='application/gzip')
    a.app.view_functions['full_backup']=full_backup

HTML=r'''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>四组号码 v84.2</title><style>
*{box-sizing:border-box}body{margin:0;background:#f2f5f8;color:#172738;font:13px -apple-system,BlinkMacSystemFont,sans-serif}main{max-width:560px;margin:auto;padding:10px}header,.head{display:flex;justify-content:space-between;align-items:center}h1{font-size:17px;margin:4px 0}section{background:#fff;border-radius:10px;padding:10px;margin:8px 0}h2{font-size:13px;margin:0}small,.muted{color:#637386;font-size:11px}.nums{display:flex;flex-wrap:wrap;gap:4px;margin:8px 0}.ball{width:25px;height:25px;border-radius:50%;display:grid;place-items:center;background:#eef2f7;font-size:12px;font-weight:600}.red{color:#be3341;background:#ffedf0}.blue{color:#255da9;background:#edf4ff}.green{color:#248250;background:#ecf8ee}button{font:11px inherit;border:0;border-radius:5px;background:#eaf0f7;padding:5px 9px;color:#304860}a{color:#355c91}table{width:100%;border-collapse:collapse;font-size:10px}th,td{text-align:center;padding:5px 1px;border-bottom:1px solid #edf1f4}.yes{color:#198454}.no{color:#b75151}#status{line-height:1.6;margin-top:6px}footer{font-size:10px;color:#718093;line-height:1.7}#toast{min-height:14px;color:#277055}.round{font-size:12px;line-height:1.8}.range{font-size:10px;color:#637386}.total,#dualTotal,#singleTotal{font-size:10px;white-space:nowrap}.summary{font-size:10px;line-height:1.6;color:#637386}details{margin-top:4px}.zodiac{font-size:11px;color:#406281;margin-bottom:6px}strong{font-weight:600}.wave-panel{padding:10px;border:1px solid #dce4ef;border-radius:10px;margin:8px 0}.next-panel{border-color:#e0be69}.wave-line{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.wave-line .nums{margin:0}.wave-metrics{display:grid;grid-template-columns:1fr 1fr;gap:10px;text-align:center;background:#f3f6fb;border-radius:10px;padding:10px}.wave-metrics .round{font-size:15px}.history-scroll{max-height:280px;overflow:auto;overscroll-behavior:contain}.history-scroll thead{position:sticky;top:0;background:#fff}.history-scroll td{font-size:12px;white-space:nowrap;padding:8px 3px}.wave-dots{letter-spacing:1px}.dot{font-size:13px}.dot.red{color:#c43e50}.dot.blue{color:#3462b6}.dot.green{color:#448b5b}</style>
<main><header><h1>四组号码 <small>v84.2</small></h1><button id="refresh">刷新</button></header><div id="status">正在连接…</div><section><div class="head"><h2>最新开奖</h2><small id="issue">--</small></div><div id="draw" class="nums"></div><small id="next">下一期 --</small><div id="calculation" class="muted"></div></section><section id="dualWave"><h2>波色预测</h2><div class="wave-panel"><div class="summary">本期开奖 <span id="waveCurrent"></span></div><div id="wavePrevious" class="summary"></div></div><div class="wave-panel next-panel"><div id="waveBasis" class="summary"></div><div class="wave-line"><span>双波</span><div id="wavePair" class="nums"></div><span>单波</span><div id="singlePair" class="nums"></div></div></div><div id="singleMethod" class="summary"></div><div id="waveRound" class="range"></div><div class="wave-metrics"><div><b>双波</b><div id="dualRound" class="round"></div><div id="dualTotal" class="summary"></div></div><div><b>单波</b><div id="singleRound" class="round"></div><div id="singleTotal" class="summary"></div></div></div><details><summary class="summary">历史轮次</summary><div id="waveRoundHistory" class="history-scroll"></div></details><h2 style="margin-top:12px">波色开奖历史</h2><div class="history-scroll"><table><thead><tr><th>期号</th><th>7波色</th><th>双波</th><th>单波</th><th>结果</th></tr></thead><tbody id="waveHistory"></tbody></table></div><div id="parityPanel" class="wave-panel"><h2>波色单双预测 · 特码</h2><div id="parityCurrent" class="summary"></div><div class="wave-panel next-panel"><div id="parityNext" class="summary"></div><div id="parityChoice" class="nums"></div><div id="parityProbs" class="summary"></div></div><div id="parityRound" class="round"></div><div id="parityRange" class="range"></div><div id="parityTotal" class="summary"></div><details><summary class="summary">历史轮次</summary><div id="parityRounds" class="history-scroll"></div></details><details open><summary class="summary">最近10期正式单双记录</summary><table><thead><tr><th>期号</th><th>推荐</th><th>实际</th><th>结果</th></tr></thead><tbody id="parityRecords"></tbody></table></details></div></section><div id="groups"></div><div id="toast"></div><section><h2>最近10期正式记录</h2><div class="summary">每期重算、开奖前锁单；每组独立按10期统计一轮</div><table><thead><tr><th>期号</th><th>特码</th><th>算法22</th><th>算法16</th><th>多策略22</th><th>6肖24</th></tr></thead><tbody id="records"></tbody></table></section><footer><a href="/api/full-backup">完整备份</a> · <a href="/api/rounds">轮次记录</a> · <a href="/api/prediction-audit">计算诊断</a><br>预测方案需要后续实盘验证，命中率不作保证。</footer></main>
<script>
const labels={algo22:'算法22码',algo16:'算法16码',pool22:'多策略22码',zodiac24:'24码 · 5热肖1冷肖'};let data={},busy=false,etag='',showHistory=false;
const red=new Set([1,2,7,8,12,13,18,19,23,24,29,30,34,35,40,45,46]),blue=new Set([3,4,9,10,14,15,20,25,26,31,36,37,41,42,47,48]);
function balls(ns){return ns.map(n=>`<span class="ball ${red.has(n)?'red':blue.has(n)?'blue':'green'}">${String(n).padStart(2,'0')}</span>`).join('')}
function wavepair(colors){return colors.map(z=>`<span class="ball ${z==='红'?'red':z==='蓝'?'blue':'green'}">${z}</span>`).join('<span>＋</span>')}
function mark(x){return x==null?'—':x?'<span class="yes">中</span>':'<span class="no">错</span>'}
function roundtext(r){return `第${r.number||1}轮 · 已开${r.opened||0}/10 · <strong>中${r.hit||0} 错${r.miss||0}</strong>`}
async function load(){if(busy)return;busy=true;try{let res=await fetch('/api/display'+(showHistory?'?history=all':''),{cache:'no-cache',headers:etag?{'If-None-Match':etag}:{}});if(res.status===304)return;if(!res.ok)throw Error('HTTP '+res.status);etag=res.headers.get('ETag')||'';let opened=[...document.querySelectorAll('details')].map(x=>x.open);data=await res.json();document.getElementById('status').textContent=`${data.migration||''} · ${data.count||0}期${data.error?' · '+data.error:''}${data.backup_error?' · 备份待重试：'+data.backup_error:''}`;document.getElementById('issue').textContent=data.issue||'--';document.getElementById('next').textContent='下一期 '+(data.next_issue||'--');document.getElementById('draw').innerHTML=balls(data.latest_numbers||[]);let calc=data.calculation||{},q=data.data_quality||{};document.getElementById('calculation').textContent=`${calc.mode||''} · 可用${q.usable_recent??'--'}期，连${q.recent_consecutive??'--'}期，缺${q.missing_recent||0}期 · ${calc.elapsed_ms??'--'}ms`;
let pa=data.parity||{},pi=pa.info||{},pd=pa.current_draw||{},pp=pa.current_prediction,pr=pa.round||{};
document.getElementById('parityCurrent').textContent=pd.issue?`本期 ${pd.issue} · 特码${String(pd.special).padStart(2,'0')} ${pd.parity} · 本期推荐 ${pp?pp.choice+(pp.hit?' 中':' 错'):'—'}`:'等待开奖';
document.getElementById('parityNext').textContent='下期预测 '+(pa.next_issue||'--');
document.getElementById('parityChoice').innerHTML=pa.choice?`<span class="ball ${pa.choice==='单'?'red':'blue'}">${pa.choice}</span>`:'等待历史核对';
document.getElementById('parityProbs').textContent=pi.probs?['单','双'].map(z=>z+' '+(pi.probs[z]*100).toFixed(1)+'%').join(' · ')+` · 基于单波三色概率 · 正式反馈${pi.evidence?.n||0}期`:'';
document.getElementById('parityRound').innerHTML=roundtext(pr);
document.getElementById('parityRange').textContent=`${pr.start||'--'}—${pr.end||'--'} · 10期一轮`;
document.getElementById('parityTotal').textContent=`总开奖${pa.n||0}期 · 中${pa.hit||0} · 错${pa.miss||0} · 命中率${pa.rate==null?'—':pa.rate+'%'}`;
document.getElementById('parityRounds').innerHTML=(pa.round_history||[]).slice().reverse().map(x=>`<div class="summary">${roundtext(x)} · ${x.start}—${x.end}</div>`).join('')||'暂无已结束轮次';
document.getElementById('parityRecords').innerHTML=(pa.recent||[]).map(x=>`<tr><td>${x.issue}</td><td>${x.choice}</td><td>${x.actual}</td><td>${mark(x.hit)}</td></tr>`).join('')||'<tr><td colspan="4">等待正式开奖</td></tr>';
let dw=data.dual_wave||{},sw=data.single_wave||{},wd=dw.current_draw||{},wp=dw.current_prediction,sc=sw.current_prediction,dr=dw.round||{},sr=sw.round||{};
document.getElementById('waveCurrent').innerHTML=wd.special?`${wd.issue} · 特码${String(wd.special).padStart(2,'0')} · ${wd.wave}波`:'等待开奖';document.getElementById('wavePrevious').innerHTML=`本期推荐：双波 ${wp?wp.colors.join('+')+' '+mark(wp.hit):'—'} · 单波 ${sc?sc.color+' '+mark(sc.hit):'—'}`;document.getElementById('waveBasis').textContent='下期预测 '+(dw.next_issue||'--');document.getElementById('wavePair').innerHTML=wavepair(dw.colors||[]);document.getElementById('singlePair').innerHTML=wavepair(sw.colors||[]);document.getElementById('waveRound').textContent=`第${dr.number||1}轮 · ${dr.start||'--'}—${dr.end||'--'} · 10期一轮`;document.getElementById('dualRound').textContent=`已开${dr.opened||0}/10 · 中${dr.hit||0} 错${dr.miss||0}`;document.getElementById('singleRound').textContent=`已开${sr.opened||0}/10 · 中${sr.hit||0} 错${sr.miss||0}`;
document.getElementById('dualTotal').textContent=`总开奖${dw.n||0}期 · 中${dw.hit||0} · 错${dw.miss||0}`;document.getElementById('singleTotal').textContent=`总开奖${sw.n||0}期 · 中${sw.hit||0} · 错${sw.miss||0}`;
document.getElementById('singleMethod').textContent=`单波 ${({mixed:'融合',trend:'趋势',transition:'转移',short:'短窗'})[sw.info?.method]||'概率融合'} · ${['红','蓝','绿'].map(z=>z+((sw.info?.probs?.[z]||0)*100).toFixed(1)+'%').join(' ')}${sw.info?.switched?' · 两错切法':''}`;
let sh=new Map((sw.round_history||[]).map(x=>[x.number,x]));document.getElementById('waveRoundHistory').innerHTML=(dw.round_history||[]).slice().reverse().map(x=>{let y=sh.get(x.number)||{};return `<div class="summary">第${x.number}轮 · ${x.start}—${x.end}<br>双波 已开${x.opened} 中${x.hit}错${x.miss} · 单波 已开${y.opened||0} 中${y.hit||0}错${y.miss||0}</div>`}).join('')||'暂无已结束轮次';
function dots(zs){return zs.map(z=>`<span class="dot ${z==='红'?'red':z==='蓝'?'blue':'green'}" title="${z}">●</span>`).join('')}
document.getElementById('waveHistory').innerHTML=(data.wave_history||[]).map(x=>`<tr><td>${x.issue.slice(-6)}<br>特${String(x.special).padStart(2,'0')} ${x.actual}</td><td class="wave-dots">${dots(x.waves)}</td><td>${x.dual.join('+')||'—'}</td><td>${x.single.join('')||'—'}</td><td>双${mark(x.dual_hit)} 单${mark(x.single_hit)}</td></tr>`).join('')||'<tr><td colspan="5">等待开奖</td></tr>';
document.getElementById('groups').innerHTML=Object.keys(labels).map(k=>{let rd=(data.rounds||{})[k]||{},hist=(data.round_history||{})[k]||[],st=(data.stats||{})[k]||{},life=st.lifetime||st;let total=`<div class="summary total">总开奖${life.n||0}期 · 中${life.hit||0} · 错${life.miss||0}</div>`;let evidence=k==='algo16'?`<details><summary class="summary">本版已开${st.n||0} · 中${st.hit||0} 错${st.miss||0}</summary><details><summary class="summary">旧版记录与累计</summary><div class="summary">历版累计已开${life.n||0} · 中${life.hit||0} 错${life.miss||0}</div><div class="history-scroll">${(data.archived16_rounds||[]).slice().reverse().map(r=>`<div class="summary">${roundtext(r)} · ${r.start}—${r.end}</div>`).join('')}</div></details></details>`:'';return `<section><div class="head"><h2>${labels[k]}</h2><button data-copy="${k}" ${data.ready?'':'disabled'}>复制</button></div><div class="nums">${balls([...(data.groups[k]||[])].sort((a,b)=>a-b))}</div><div class="round">${roundtext(rd)}</div>${total}${evidence}<div class="range">${rd.start||'--'}—${rd.end||'--'}</div><details><summary class="summary">历史轮次</summary><div class="history-scroll">${hist.slice().reverse().map(r=>`<div class="summary">${roundtext(r)} · ${r.start}—${r.end}</div>`).join('')||'暂无已结束轮次'}</div></details></section>`}).join('');
document.getElementById('records').innerHTML=(data.recent||[]).map(x=>`<tr><td>${x.issue}</td><td>${String(x.actual).padStart(2,'0')}</td>${Object.keys(labels).map(k=>`<td>${mark(x.hits[k])}</td>`).join('')}</tr>`).join('')||'<tr><td colspan="6">等待正式开奖</td></tr>';
document.querySelectorAll('details').forEach((x,i)=>{if(opened[i])x.open=true});
}catch(e){document.getElementById('status').textContent='连接失败，自动重试 · '+e.message}finally{busy=false}}
document.addEventListener('toggle',e=>{if(e.target.tagName==='DETAILS'&&e.target.open&&!showHistory&&/历史轮次|本版已开|旧版/.test(e.target.querySelector('summary')?.textContent||'')){showHistory=true;etag='';if(!busy)load()}},true);
document.getElementById('refresh').onclick=load;document.getElementById('groups').onclick=async e=>{let k=e.target.dataset.copy;if(!k)return;let text=[...(data.groups[k]||[])].sort((a,b)=>a-b).map(n=>String(n).padStart(2,'0')).join(',');try{await navigator.clipboard.writeText(text)}catch(_){let t=document.createElement('textarea');t.value=text;document.body.append(t);t.select();document.execCommand('copy');t.remove()}document.getElementById('toast').textContent='已复制'};load();setInterval(()=>{if(!document.hidden)load()},10000);document.addEventListener('visibilitychange',()=>{if(!document.hidden)load()});
</script></html>'''
