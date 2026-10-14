"""Offline regression tests: python -m unittest test_parity -v."""
import gzip
import json
import os
import sqlite3
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest.mock import patch
import parity


class ParityTests(unittest.TestCase):
    def test_wave_dependency_and_no_future_feedback(self):
        history = [{'issue': str(200-i), 'special': i % 49 + 1} for i in range(120)]
        red = parity.forecast(history, [], {'probs': {'红': 1., '蓝': 0., '绿': 0.}})
        blue = parity.forecast(history, [], {'probs': {'红': 0., '蓝': 1., '绿': 0.}})
        self.assertNotEqual(red['probs'], blue['probs'])
        for result in (red, blue):
            self.assertAlmostEqual(sum(result['probs'].values()), 1)
            for model in result['probability_candidates'].values():
                self.assertAlmostEqual(sum(model.values()), 1)
        self.assertAlmostEqual(red['probability_candidates']['wave_baseline']['单'], 8/17)
        self.assertAlmostEqual(blue['probability_candidates']['wave_baseline']['单'], 8/16)
        record = {'target': '201', 'cutoff': '200', 'version': parity.VERSION, 'actual': '单', 'info_json': json.dumps(red)}
        result = parity.forecast(history, [record], {'probs': red['wave_probs']})
        self.assertEqual(result['evidence']['n'], 0)
        record.update(target='200', cutoff='199')
        self.assertEqual(parity.forecast(history, [record], {'probs': red['wave_probs']})['evidence']['n'], 1)
        self.assertEqual(parity.parity(49), '单')

    def test_service_locks_settlement_backup_and_ui(self):
        with tempfile.TemporaryDirectory() as directory:
            env = dict(DB_PATH=directory+'/test.db', CHECKPOINT_PATH=directory+'/backup.gz', HISTORY_SOURCE_URL='', BOT_TOKEN='', SUPABASE_URL='', SUPABASE_SERVICE_ROLE_KEY='', EXPECTED_HISTORY_COUNT='14987', EXPECTED_LATEST_ISSUE='20261013155')
            with patch.dict(os.environ, env):
                module = types.ModuleType('parity_test_app')
                module.__file__ = str(Path('app.py').resolve())
                sys.modules[module.__name__] = module
                source = Path('app.py').read_text().replace('\nboot()\n', '\n')
                exec(compile(source, 'app.py', 'exec'), module.__dict__)
                module.sync_best_available_history = lambda: None
                module.boot()
                client = module.app.test_client()
                for _ in range(200):
                    data = client.get('/api/prediction').get_json()
                    if data.get('parity') or data.get('error'): break
                    time.sleep(.02)
                self.assertTrue(data.get('ready'), data)
                self.assertFalse(data.get('error'))
                self.assertEqual(data['parity']['n'], 0)
                self.assertEqual(data['parity']['info']['wave_probs'], data['single_wave']['info']['probs'])
                html = client.get('/').get_data(as_text=True)
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html, 'html.parser')
                self.assertIsNotNone(soup.select_one('#dualWave #parityPanel'))
                self.assertIsNotNone(soup.select_one('main > #groups'))
                target = data['next_issue']
                c = module.connect()
                before = dict(c.execute('SELECT * FROM parity_forecasts WHERE target=?', (target,)).fetchone())
                c.close()
                self.assertEqual(client.get('/api/parity').get_json()['choice'], before['choice'])
                module.write_checkpoint_atomic = lambda: True  # No background disk writer in test.
                actual = 1 if before['choice'] == '单' else 2
                nums = [3,4,5,6,7,8,actual]
                self.assertTrue(module.add_draw(target, nums, ['']*7, ['']*7, 'test'))
                self.assertFalse(module.add_draw(target, nums, ['']*7, ['']*7, 'test'))
                after = client.get('/api/prediction').get_json()
                self.assertFalse(after['error'])
                self.assertEqual(after['parity']['n'], 1)
                self.assertEqual(after['parity']['hit'], 1)
                self.assertEqual(after['parity']['info']['evidence']['n'], 1)
                payload = module._checkpoint_payload()
                from memory_io import write_checkpoint, restore_checkpoint
                self.assertTrue(write_checkpoint(module, 'v84_1_memory_fix', 'v83_probability_modules'))
                with gzip.open(module.CHECKPOINT_PATH, 'rt') as saved:
                    streamed = json.load(saved)
                for table in ('draws', 'compact_forecasts', 'wave_forecasts', 'parity_forecasts', 'learner_scores'):
                    canonical = lambda rows: sorted(json.dumps(x, sort_keys=True) for x in rows)
                    self.assertEqual(canonical(streamed[table]), canonical(payload[table]))
                c = module.connect()
                for table in ('compact_forecasts', 'wave_forecasts', 'parity_forecasts'):
                    c.execute('DELETE FROM ' + table)
                c.commit(); c.close()
                restore_checkpoint(module, module.CHECKPOINT_PATH)
                restored_payload = module._checkpoint_payload()
                for table in ('compact_forecasts', 'wave_forecasts', 'parity_forecasts'):
                    self.assertEqual(canonical(restored_payload[table]), canonical(payload[table]))

                old = next(x for x in payload['parity_forecasts'] if x['target'] == target)
                for key in ('choice', 'info_json', 'cutoff', 'created_at'):
                    self.assertEqual(old[key], before[key])
                restored = sqlite3.connect(':memory:'); restored.row_factory = sqlite3.Row
                parity.create_table(restored); parity.restore(restored, {}); parity.restore(restored, payload); parity.restore(restored, payload)
                self.assertEqual(restored.execute('SELECT COUNT(*) FROM parity_forecasts').fetchone()[0], 2)
                restored.close()
                Path(directory+'/ui.js').write_text(html.split('<script>')[1].split('</script>')[0])
                import subprocess
                subprocess.run(['node', '--check', directory+'/ui.js'], check=True, capture_output=True)

if __name__ == '__main__':
    unittest.main()
