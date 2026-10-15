import copy, json, sqlite3, tempfile, threading, types, unittest
from pathlib import Path
from datetime import datetime, timezone, timedelta
from incremental_backup import Backup, setup
from memory_io import TABLES
class Store:
    def __init__(self):self.files={};self.uploads=[];self.fail_manifest=False;self.fail_delete=False
    def get_json(self,key):return json.loads(self.files[key]) if key in self.files else None
    def put(self,key,path,content_type='application/gzip'):
        if self.fail_manifest and key.endswith('manifest.json'):raise OSError('publish failed')
        value=Path(path).read_bytes();self.files[key]=value;self.uploads.append((key,len(value)))
    def download(self,key,path):Path(path).write_bytes(self.files[key])
    def delete(self,keys):
        if self.fail_delete:raise OSError('delete failed')
        for key in keys:self.files.pop(key,None)
def app(directory):
    a=types.SimpleNamespace(DB=str(Path(directory)/'history.db'),CHECKPOINT_PATH=str(Path(directory)/'backup.gz'),db_lock=threading.RLock(),PERSISTENT_MODE=False,REMOTE_BACKUP_ENABLED=True,SUPABASE_OBJECT='old.gz',auto_state={})
    def connect():
        c=sqlite3.connect(a.DB);c.row_factory=sqlite3.Row;return c
    a.connect=connect;a.export_learning_payload=lambda n:{};a.import_learning_payload=lambda p:None;a.load_ai_state=lambda:None;a.load_correction_state=lambda:None
    c=connect();c.executescript('''CREATE TABLE draws(issue TEXT PRIMARY KEY,special INTEGER);
CREATE TABLE compact_forecasts(target TEXT,version TEXT,cutoff TEXT,groups_json TEXT,meta_json TEXT,actual INTEGER,PRIMARY KEY(target,version));
CREATE TABLE wave_forecasts(target TEXT,version TEXT,cutoff TEXT,colors_json TEXT,info_json TEXT,actual_wave TEXT,PRIMARY KEY(target,version));
CREATE TABLE parity_forecasts(target TEXT,version TEXT,cutoff TEXT,choice TEXT,info_json TEXT,actual TEXT,PRIMARY KEY(target,version));
CREATE TABLE learner_scores(profile TEXT PRIMARY KEY);''');setup(c,True);c.close();return a
def contents(a):
    c=a.connect()
    try:return {t:sorted(json.dumps(dict(r),sort_keys=True) for r in c.execute('SELECT * FROM '+t)) for t in TABLES}
    finally:c.close()
class IncrementalTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.a=app(self.temp.name);self.store=Store();self.backup=Backup(self.a,'v84.2','v83',self.store);self.day=datetime(2026,10,15,12,tzinfo=timezone(timedelta(hours=8)))
    def tearDown(self):self.temp.cleanup()
    def insert(self,issue=100):
        c=self.a.connect();c.execute('INSERT INTO draws VALUES (?,?)',(str(issue),25))
        c.execute('INSERT INTO compact_forecasts VALUES (?,?,?,?,?,NULL)',(str(issue+1),'v83',str(issue),'{}','{}'))
        c.execute('INSERT INTO wave_forecasts VALUES (?,?,?,?,?,NULL)',(str(issue+1),'single',str(issue),'["红"]','{}'))
        c.execute('INSERT INTO parity_forecasts VALUES (?,?,?,?,?,NULL)',(str(issue+1),'parity',str(issue),'单','{}'));c.commit();c.close()
    def test_roundtrip_settlement_backfill_noop(self):
        self.insert();self.assertTrue(self.backup.sync(self.day));self.insert(101);c=self.a.connect()
        c.execute('UPDATE compact_forecasts SET actual=25 WHERE target="101"');c.execute('UPDATE wave_forecasts SET actual_wave="蓝" WHERE target="101"');c.execute('UPDATE parity_forecasts SET actual="单" WHERE target="101"');c.execute('INSERT INTO draws VALUES ("50",12)');c.commit();c.close()
        self.assertTrue(self.backup.sync(self.day));count=len(self.store.uploads);self.assertTrue(self.backup.sync(self.day));self.assertEqual(count,len(self.store.uploads))
        with tempfile.TemporaryDirectory() as folder:
            b=app(folder);c=b.connect();setup(c,False);c.close();restored=Backup(b,'v84.2','v83',self.store);self.assertTrue(restored.restore());self.assertEqual(contents(self.a),contents(b))
            c=b.connect();setup(c,True);c.execute('INSERT INTO draws VALUES ("103",4)');c.commit();c.close();self.assertTrue(restored.sync(self.day));self.assertGreater(restored.manifest['seq'],self.backup.manifest['seq'])
    def test_failed_publication_keeps_outbox_and_chain(self):
        self.insert();self.assertTrue(self.backup.sync(self.day));old=copy.deepcopy(self.backup.manifest);self.insert(101);self.store.fail_manifest=True
        self.assertFalse(self.backup.sync(self.day));self.assertEqual(old,self.backup.manifest);c=self.a.connect();self.assertGreater(c.execute('SELECT COUNT(*) FROM backup_outbox').fetchone()[0],0);c.close()
        self.store.fail_manifest=False;self.assertTrue(self.backup.sync(self.day));self.backup.check_manifest(self.backup.manifest)
    def test_daily_retention_and_failed_cleanup(self):
        self.insert();self.backup.sync(self.day);first=self.backup.manifest['snapshots'][0]['key']
        for d in (1,2):self.assertTrue(self.backup.sync(self.day+timedelta(days=d)))
        self.store.fail_delete=True;self.assertTrue(self.backup.sync(self.day+timedelta(days=3)));self.assertEqual(len(self.backup.manifest['snapshots']),3);self.assertIn(first,self.store.files)
        self.store.fail_delete=False;self.assertTrue(self.backup.sync(self.day+timedelta(days=3)));self.assertNotIn(first,self.store.files);self.assertEqual(len(contents(self.a)['draws']),1)
    def test_corruption_fails_restore(self):
        self.insert();self.backup.sync(self.day);key=self.backup.manifest['snapshots'][-1]['key'];self.store.files[key]=b'broken'
        with tempfile.TemporaryDirectory() as folder:
            b=app(folder)
            with self.assertRaises(ValueError):Backup(b,'v84.2','v83',self.store).restore()
            self.assertEqual(contents(b)['draws'],[])
    def test_failed_new_snapshot_keeps_old(self):
        self.insert();self.backup.sync(self.day);old=copy.deepcopy(self.backup.manifest);self.store.fail_manifest=True;self.assertFalse(self.backup.sync(self.day+timedelta(days=1)));self.assertEqual(old,self.backup.manifest);self.assertIn(old['snapshots'][0]['key'],self.store.files)
if __name__=='__main__':unittest.main()
