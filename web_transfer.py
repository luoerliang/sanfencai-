"""Small page payloads, conditional responses and gzip; audits stay complete."""
import gzip
import hashlib
import json
from flask import request, jsonify


def install(a, cache, lock, prediction):
    @a.app.get('/api/display')
    def display():
        # Reuse the existing freshness/retry path, but never transmit model candidates.
        prediction()
        with lock:
            data = dict(cache)
            data['backup_error']=getattr(a,'auto_state',{}).get('remote_backup_error','')
            full_history = request.args.get('history') == 'all'
            for key in ('group_meta', 'diagnostics', 'chasing', 'active_versions'):
                data.pop(key, None)
            if not full_history:
                data['round_history'] = {k: [] for k in data.get('round_history', {})}
                data['archived16_rounds'] = []
            for key in ('dual_wave', 'single_wave', 'parity'):
                item = dict(data.get(key) or {})
                item['info'] = {k: v for k, v in (item.get('info') or {}).items() if k in ('probs', 'method', 'switched', 'source', 'evidence')}
                if not full_history: item['round_history'] = []
                data[key] = item
            body = json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode()
        etag = hashlib.sha256(body).hexdigest()
        if request.if_none_match.contains(etag):
            response = a.app.response_class(status=304)
        else:
            response = a.app.response_class(body, mimetype='application/json')
        response.set_etag(etag)
        response.headers['Cache-Control'] = 'private, no-cache'
        return response

    @a.app.after_request
    def compress(response):
        if response.direct_passthrough or response.status_code != 200 or response.headers.get('Content-Encoding'):
            return response
        if response.mimetype not in ('application/json', 'text/html'): return response
        response.vary.add('Accept-Encoding')
        if request.accept_encodings['gzip'] <= 0: return response
        body = response.get_data()
        if len(body) < 1024: return response
        packed = gzip.compress(body, compresslevel=5, mtime=0)
        if len(packed) < len(body):
            response.set_data(packed)
            response.headers['Content-Encoding'] = 'gzip'
        return response
