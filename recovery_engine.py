"""Recover history without serving forecasts or replacing protected checkpoints."""
import csv, gzip, io, json, os, secrets, threading, time
from flask import jsonify, request, render_template_string

PAGE='''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>历史恢复</title><style>body{background:#0b1421;color:#e5eefc;font:14px -apple-system,sans-serif;margin:0;padding:20px}main{max-width:550px;margin:auto}h1{font-size:21px}p{line-height:1.6;color:#aebed5}section{border:1px solid #31445e;border-radius:10px;padding:15px;margin:12px 0}button,input{font:inherit;padding:9px;max-width:100%;margin:5px 0}button{background:#24578b;color:white;border:0;border-radius:6px}input{box-sizing:border-box;width:100%;background:#18273b;color:white;border:1px solid #456080}small{color:#9aafca}#result{white-space:pre-wrap;overflow-wrap:anywhere}</style><main><h1>正在恢复历史数据</h1><p>历史检查尚未通过，预测暂停。已有记录继续保留，不完整数据不会覆盖备份。</p><section><p id="numbers">读取中…</p><p id="reason"></p><p id="storage"></p><button id="retry" onclick="retry()">重试已配置的历史来源</button></section><section><b>导入原版历史备份</b><p>支持原版导出的JSON、完整JSON.GZ备份和历史CSV。只补入缺失期号，不生成过去的预测。</p><form id="upload"><input id="token" type="password" placeholder="恢复管理凭证" autocomplete="off"><input id="file" type="file" accept=".json,.gz,.csv" required><button>导入并核对</button></form><small id="admin"></small></section><p id="result"></p><small>能够打开本页只说明服务已运行，不代表历史恢复成功。缺少旧库或备份时，程序不能还原未知开奖。</small></main><script>
const $=id=>document.getElementById(id);let polling=false;async function status(){if(polling)return;polling=true;try{const r=await fetch('/api/recovery-status',{cache:'no-store'}),d=await r.json();if(d.ready){location.replace('/');return}$('numbers').textContent=`当前 ${d.count}期 · 最新 ${d.latest||'无'}\n要求至少 ${d.required_count}期 · 最新不早于 ${d.required_latest}`;$('reason').textContent=d.busy?'正在尝试恢复，请稍候':d.message;$('storage').textContent=`外部备份：${d.remote_backup_enabled?'已配置':'未配置'} · 持久存储：${d.persistent?'已配置':'未配置'}`;$('retry').disabled=d.busy;$('admin').textContent=d.admin_upload_enabled?'可用管理凭证导入备份。':'导入备份需先在服务环境变量设置 RECOVERY_ADMIN_TOKEN，作为你自己的恢复管理凭证。'}catch(e){$('reason').textContent='正在重新连接'}finally{polling=false}}
async function retry(){const r=await fetch('/api/retry-recovery',{method:'POST'}),d=await r.json();$('result').textContent=d.message||'';status()}
$('upload').onsubmit=async e=>{e.preventDefault();const f=$('file').files[0];if(!f)return;const form=new FormData();form.append('file',f);$('result').textContent='正在导入，请勿重复提交';try{const r=await fetch('/api/recovery-import',{method:'POST',headers:{'X-Recovery-Token':$('token').value},body:form}),d=await r.json();$('result').textContent=d.message||'';status()}catch(e){$('result').textContent='导入连接失败，请查看恢复状态后再试'}};status();setInterval(status,4000);
</script></html>'''

MAX_BYTES=50*1024*1024

def decode_backup(data,filename=''):
    if data[:2]==b'\x1f\x8b':
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:data=f.read(MAX_BYTES+1)
    if len(data)>MAX_BYTES:raise ValueError('备份解压后超过50MB限制')
    text=data.decode('utf-8-sig')
    if filename.lower().endswith('.csv') or text.lstrip().startswith('期号,'):
        items=[]
        for x in csv.DictReader(io.StringIO(text)):
            item={'issue':x.get('期号','')}
            for i in range(1,7):
                item[f'n{i}']=x.get(f'正码{i}')
                item[f'z{i}']=x.get(f'正码{i}生肖','')
            item['special']=x.get('特码');item['z7']=x.get('生肖','');items.append(item)
        payload={'draws':items}
    else:
        parsed=json.loads(text)
        payload={'draws':parsed} if isinstance(parsed,list) else parsed
    if not isinstance(payload,dict):raise ValueError('备份应是历史列表或导出对象')
    items=payload.get('draws',payload.get('items'))
    if not isinstance(items,list) or not items:raise ValueError('备份没有开奖记录')
    for x in items:
        if not isinstance(x,dict):raise ValueError('开奖记录格式错误')
        issue=str(x.get('issue') or x.get('期号') or '')
        if not issue.isdigit() or len(issue)!=11:raise ValueError('备份含无效期号')
        try:
            nums=[int(x[f'n{i}']) for i in range(1,7)]+[int(x['special'])] if 'n1' in x else [int(n) for n in x.get('numbers',[])]
        except (KeyError,ValueError,TypeError):raise ValueError('备份含无效开奖号码')
        if len(nums)!=7 or len(set(nums))!=7 or not all(1<=n<=49 for n in nums):raise ValueError('备份含无效开奖号码')
    return payload,items

class Recovery:
    def __init__(self,g):
        self.g=g;self.ready=False;self.busy=False;self.message='等待继承原版历史';self.lock=threading.Lock();self.wake=threading.Event();self.started=False;self.last_retry=0
        self.required_count=max(15764,int(os.getenv('V72_REQUIRED_HISTORY_COUNT','15764')))
        self.required_latest=max('20261011473',os.getenv('V72_REQUIRED_LATEST_ISSUE','20261011473'))
        self.admin=os.getenv('RECOVERY_ADMIN_TOKEN','').strip()
    def snapshot(self):
        g=self.g
        with g['db_lock']:
            c=g['connect']()
            try:
                n=c.execute('SELECT COUNT(*) FROM draws').fetchone()[0]
                row=c.execute('SELECT issue FROM draws ORDER BY CAST(issue AS INTEGER) DESC LIMIT 1').fetchone()
            finally:c.close()
        latest=str(row['issue']) if row else ''
        self.required_count=max(self.required_count,int(g['auto_state'].get('migration_required_count') or 0))
        known=str(g['auto_state'].get('migration_required_issue') or '')
        if known.isdigit():self.required_latest=max(self.required_latest,known)
        passed=n>=self.required_count and latest.isdigit() and int(latest)>=int(self.required_latest)
        return n,latest,passed
    def status(self):
        n,latest,passed=self.snapshot()
        return {'ready':self.ready,'busy':self.busy,'count':n,'latest':latest,'required_count':self.required_count,'required_latest':self.required_latest,'migration_pass':passed,'message':self.message,'remote_backup_enabled':self.g['REMOTE_BACKUP_ENABLED'],'persistent':self.g['PERSISTENT_MODE'],'admin_upload_enabled':bool(self.admin)}
    def merge(self,payload,items):
        g=self.g
        g['migration_high_water'](payload.get('history_total',payload.get('total',len({str(x.get('issue') or x.get('期号')) for x in items}))),max((str(x.get('issue') or x.get('期号')) for x in items),key=int))
        imported=0
        with g['db_lock']:
            c=g['connect']()
            try:
                c.execute('BEGIN IMMEDIATE')
                for x in items:imported+=g['_insert_migrated_draw'](c,x)
                c.commit()
            finally:c.close()
        if isinstance(payload.get('learning'),dict):g['import_learning_payload'](payload['learning'])
        g['refresh_history_cache']()
        return imported
    def merge_file(self,path):
        with open(path,'rb') as f:data=f.read(MAX_BYTES+1)
        payload,items=decode_backup(data,path)
        return self.merge(payload,items)
    def remote(self):
        g=self.g
        if not g['REMOTE_BACKUP_ENABLED']:return
        url=f"{g['SUPABASE_URL']}/storage/v1/object/{g['SUPABASE_BUCKET']}/{g['SUPABASE_OBJECT']}"
        r=g['requests'].get(url,headers=g['_supabase_headers'](),timeout=25)
        if r.status_code!=200:
            raise RuntimeError(f'外部备份读取失败 HTTP {r.status_code}')
        payload,items=decode_backup(r.content)
        self.merge(payload,items)
        # Incoming bytes never replace a possibly better local checkpoint.
        g['auto_state']['remote_restore_at']=time.strftime('%Y-%m-%d %H:%M:%S')
    def activate(self):
        g=self.g;n,latest,passed=self.snapshot()
        if not passed:
            self.message=f'历史未恢复完整：当前{n}期，最新{latest or "无"}；预测继续暂停'
            return False
        g['load_ai_state']();g['load_correction_state']();g['refresh_learner_cache'](60);g['refresh_history_cache']()
        g['auto_state'].update(migration_latest_issue=latest,migration_history_count=n,last_draw_issue=latest,last_prediction_issue=g['_next_issue_id'](latest))
        self.message='历史检查通过';self.ready=True;g['background_state']['boot_ready']=True
        print(f'[RECOVERY] migration PASS count={n} latest={latest}',flush=True)
        # New predictors cannot run or back up until history passes the check.
        for fn,name in [('rebuild_live_cache','recovered-model'),('build_long_prior','full-history-prior'),('bootstrap_ai_history','ai-bootstrap'),('bootstrap_correction_models','correction-bootstrap')]:
            threading.Thread(target=g[fn],daemon=True,name=name).start()
        return True
    def attempt(self):
        if not self.lock.acquire(False):return
        self.busy=True
        try:
            errors=[]
            path=self.g['CHECKPOINT_PATH']
            if os.path.exists(path):
                try:self.merge_file(path)
                except Exception as e:errors.append(f'本地备份读取失败：{type(e).__name__}')
            try:self.remote()
            except Exception as e:errors.append(str(e));self.g['auto_state']['remote_backup_error']=str(e)
            self.g['sync_best_available_history']()
            self.g['sync_previous_learning']()
            if not self.activate():
                if errors:self.message+='；'+'；'.join(errors)
                if not self.g['REMOTE_BACKUP_ENABLED']:self.message+='；未配置外部备份，请恢复原配置或导入完整历史备份'
                print('[RECOVERY] WAIT: '+self.message,flush=True)
        except Exception as e:
            self.message=f'历史恢复失败：{type(e).__name__}；预测仍暂停'
            print(f'[RECOVERY] {type(e).__name__}: {e}',flush=True)
        finally:self.busy=False;self.lock.release()
    def loop(self):
        while not self.ready:
            self.attempt()
            if not self.ready:self.wake.wait(60);self.wake.clear()
    def start(self):
        if self.started:return
        self.started=True
        threading.Thread(target=self.loop,daemon=True,name='history-recovery').start()
    def routes(self,app):
        app.config['MAX_CONTENT_LENGTH']=20*1024*1024
        @app.before_request
        def gate():
            if not self.ready and request.path in {'/api/prediction','/api/stats','/api/model-pool','/api/learning','/api/auto-status','/api/sync-history'}:
                return jsonify(self.status()),503
        @app.get('/api/recovery-status')
        def status():return jsonify(self.status())
        @app.post('/api/retry-recovery')
        def retry():
            if self.ready:return jsonify({'message':'历史检查已经通过'})
            if time.monotonic()-self.last_retry<30:return jsonify({'message':'恢复正在进行，请稍候'}),429
            self.last_retry=time.monotonic();self.wake.set();self.start()
            return jsonify({'message':'已请求重试配置中的历史来源'}),202
        @app.post('/api/recovery-import')
        def upload():
            if not self.admin:return jsonify({'message':'请先设置 RECOVERY_ADMIN_TOKEN 恢复管理凭证'}),503
            if not secrets.compare_digest(request.headers.get('X-Recovery-Token',''),self.admin):return jsonify({'message':'恢复管理凭证不正确'}),403
            if self.ready:return jsonify({'message':'已启用预测，不能从恢复入口修改历史'}),409
            if not self.lock.acquire(False):return jsonify({'message':'正在读取历史来源，稍后再导入'}),409
            self.busy=True
            try:
                f=request.files.get('file')
                if f is None:return jsonify({'message':'请选择原版备份文件'}),400
                payload,items=decode_backup(f.read(MAX_BYTES+1),f.filename or '')
                n=self.merge(payload,items)
                passed=self.activate()
                return jsonify({'message':f'补入{n}期；'+('历史检查通过' if passed else self.message),'imported':n,'ready':passed})
            except (ValueError,UnicodeError,KeyError,TypeError,OSError) as e:
                return jsonify({'message':'备份格式无效，未覆盖现有记录'}),400
            finally:self.busy=False;self.lock.release()
