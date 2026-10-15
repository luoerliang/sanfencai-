"""Offline end-to-end transfer measurement using an in-memory Storage double."""
import os,sys,time,json,tempfile,types,gzip
from pathlib import Path
from unittest.mock import patch
from test_incremental import Store
with tempfile.TemporaryDirectory() as d:
 with patch.dict(os.environ,dict(DB_PATH=d+'/db',CHECKPOINT_PATH=d+'/backup.gz',HISTORY_SOURCE_URL='',BOT_TOKEN='',SUPABASE_URL='',SUPABASE_SERVICE_ROLE_KEY='',EXPECTED_HISTORY_COUNT='14987',EXPECTED_LATEST_ISSUE='20261013155')):
  a=types.ModuleType('bandwidth_app');a.__file__=str(Path('app.py').resolve());sys.modules[a.__name__]=a
  exec(compile(Path('app.py').read_text().replace('\nboot()\n','\n'),'app.py','exec'),a.__dict__)
  a.sync_best_available_history=lambda:None
  sync=a.incremental_backup.sync;a.incremental_backup.sync=lambda:True
  a.boot();client=a.app.test_client()
  for _ in range(500):
   data=client.get('/api/prediction').get_json()
   if data.get('parity') or data.get('error'):break
   time.sleep(.02)
  assert data['ready'] and not data.get('error'),data.get('error')
  store=Store();a.incremental_backup.storage=store;a.REMOTE_BACKUP_ENABLED=True
  assert sync();full_bytes=a.auto_state['last_upload_bytes'];initial=len(store.uploads)
  for i in range(20):
   assert a.add_draw(data['next_issue'],[1,2,3,4,5,6,49],['']*7,['']*7,'test')
   assert sync();data=client.get('/api/prediction').get_json()
  deltas=[n for k,n in store.uploads[initial:] if '/delta-' in k]
  page=client.get('/api/display',headers={'Accept-Encoding':'gzip'})
  unchanged=client.get('/api/display',headers={'If-None-Match':page.headers['ETag']})
  assert unchanged.status_code==304
  report={'synthetic_draws':20,'full_backup_bytes':full_bytes,'mean_incremental_object_bytes':round(sum(deltas)/len(deltas)), 'total_uploaded_bytes_for_20_draws_including_manifests':sum(n for k,n in store.uploads[initial:]),'page_gzip_bytes':len(page.data),'unchanged_response_body_bytes':len(unchanged.data),'preserved_history_count':data['count']}
  print('MEASURED',json.dumps(report));Path('bandwidth_test_result.json').write_text(json.dumps(report,indent=2))
