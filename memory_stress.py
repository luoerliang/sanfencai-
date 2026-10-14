import os,sys,tempfile,time,types,json,gzip,resource,random
from pathlib import Path
source_dir=Path(sys.argv[1]).resolve();n=int(sys.argv[2]);sys.path.insert(0,str(source_dir));os.chdir(source_dir)
# Run every comparison in a fresh process. No network or Telegram calls.
with tempfile.TemporaryDirectory() as d:
 os.environ.update(DB_PATH=d+'/test.db',CHECKPOINT_PATH=d+'/backup.gz',HISTORY_SOURCE_URL='',BOT_TOKEN='',SUPABASE_URL='',SUPABASE_SERVICE_ROLE_KEY='',EXPECTED_HISTORY_COUNT='14987',EXPECTED_LATEST_ISSUE='20261013155')
 a=types.ModuleType('stress_app');a.__file__=str(source_dir/'app.py');sys.modules[a.__name__]=a
 exec(compile((source_dir/'app.py').read_text().replace('\nboot()\n','\n'),'app.py','exec'),a.__dict__)
 a.sync_best_available_history=lambda:None
 a.boot();client=a.app.test_client()
 for _ in range(1000):
  initial=client.get('/api/prediction').get_json()
  if initial.get('parity') or initial.get('error'):break
  time.sleep(.02)
 assert initial.get('ready') and not initial.get('error'),initial.get('error')
 from rounds import advance
 from engine import wave
 from module_models import number_forecast,wave_forecast
 import parity
 history=a.recent_rows(500);groups,meta=number_forecast(history,[]);wi=wave_forecast(history,[],True);pi=parity.forecast(history,[],wi)
 from compact import VERSION,WAVE_VERSION,SINGLE_VERSION
 c=a.connect();template=dict(history[0]);cols=list(template);start=initial['next_issue']
 for i in range(n):
  issue=advance(start,i);cutoff=advance(issue,-1);row=dict(template,issue=issue,special=(i*17)%49+1)
  c.execute('INSERT OR IGNORE INTO draws('+','.join(cols)+') VALUES ('+','.join('?' for _ in cols)+')',[row[k] for k in cols])
  c.execute('INSERT OR IGNORE INTO compact_forecasts(target,version,cutoff,groups_json,meta_json,actual) VALUES (?,?,?,?,?,?)',(issue,VERSION,cutoff,json.dumps(groups),json.dumps(meta,ensure_ascii=False),row['special']))
  for v in (WAVE_VERSION,SINGLE_VERSION):
   c.execute('INSERT OR IGNORE INTO wave_forecasts(target,version,cutoff,colors_json,info_json,actual_wave) VALUES (?,?,?,?,?,?)',(issue,v,cutoff,json.dumps(wi['colors']),json.dumps(wi,ensure_ascii=False),wave(row['special'])))
  c.execute('INSERT OR IGNORE INTO parity_forecasts(target,version,cutoff,choice,info_json,actual) VALUES (?,?,?,?,?,?)',(issue,parity.VERSION,cutoff,pi['choice'],json.dumps(pi,ensure_ascii=False),parity.parity(row['special'])))
 c.commit();c.close();del meta,groups,wi,pi,history,initial
 checkpoint=a.write_checkpoint_atomic;a.write_checkpoint_atomic=lambda:True
 started=time.perf_counter();out=client.get('/api/prediction').get_json();refresh_seconds=time.perf_counter()-started
 assert not out.get('error'),out.get('error')
 # Repeated genuine draws exercise settlement + model feedback and stable memory.
 for _ in range(5):
  assert a.add_draw(out['next_issue'],[1,2,3,4,5,6,49],['']*7,['']*7,'stress')
  out=client.get('/api/prediction').get_json();assert not out.get('error'),out.get('error')
 started=time.perf_counter();assert checkpoint();backup_seconds=time.perf_counter()-started
 if (source_dir/'memory_io.py').exists():
  from memory_io import restore_checkpoint
  started=time.perf_counter();restore_checkpoint(a,a.CHECKPOINT_PATH);restore_seconds=time.perf_counter()-started
 else:restore_seconds=None
 print(json.dumps(dict(version=out['version'],synthetic_records=n,refresh_seconds=round(refresh_seconds,3),backup_seconds=round(backup_seconds,3),restore_seconds=round(restore_seconds,3) if restore_seconds else None,peak_rss_mib=round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024,1),backup_bytes=os.path.getsize(a.CHECKPOINT_PATH),settled=out['parity']['n']),ensure_ascii=False))
