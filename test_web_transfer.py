import gzip,json,threading,types,unittest
from flask import Flask,jsonify
from web_transfer import install
class WebTests(unittest.TestCase):
    def test_conditional_compression_and_lazy_history(self):
        app=Flask(__name__);a=types.SimpleNamespace(app=app)
        cache={'issue':'20261015001','groups':{},'group_meta':{'candidates':'s'*30000},'round_history':{'algo22':[{'number':i} for i in range(300)]},'archived16_rounds':[{}], 'wave_history':[{'issue':str(i),'waves':['红']*7} for i in range(100)],'single_wave':{'info':{'probs':{'红':.4},'probability_candidates':{'large':'a'*10000}},'round_history':[{'number':1}]}}
        install(a,cache,threading.RLock(),lambda:jsonify(cache));client=app.test_client()
        response=client.get('/api/display',headers={'Accept-Encoding':'gzip'});self.assertEqual(response.status_code,200);self.assertEqual(response.headers['Content-Encoding'],'gzip')
        value=json.loads(gzip.decompress(response.data));self.assertNotIn('group_meta',value);self.assertNotIn('probability_candidates',value['single_wave']['info']);self.assertEqual(value['round_history']['algo22'],[])
        again=client.get('/api/display',headers={'If-None-Match':response.headers['ETag']});self.assertEqual(again.status_code,304);self.assertEqual(again.data,b'')
        history=client.get('/api/display?history=all').get_json();self.assertEqual(len(history['round_history']['algo22']),300)
        cache['issue']='20261015002';changed=client.get('/api/display',headers={'If-None-Match':response.headers['ETag']});self.assertEqual(changed.status_code,200)
        print('transfer_bytes',json.dumps({'original':len(json.dumps(cache).encode()),'compressed_page':len(response.data),'unchanged_body':len(again.data)}))
if __name__=='__main__':unittest.main()
