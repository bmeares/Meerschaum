"""API execution preserves Python connector identities without constructing DB clients."""

from tests.test_adhoc_connectors import _run_python


def test_python_aliases_use_rotating_credentials_and_ignore_local_source_sql(tmp_path):
    result = _run_python(r'''
import json
import pathlib
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

import meerschaum as mrsm
import meerschaum.connectors as connectors
from meerschaum.config import set_config

requests = []
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_GET(self):
        requests.append((self.path, self.headers.get('Authorization'), None))
        payload = [{'value': 7}]
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode())
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        requests.append((self.path, self.headers.get('Authorization'), body))
        payload = {'version': 1, 'dtypes': {'value': 'int64'}, 'rows': [{'value': 8}], 'complete': True}
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode())

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
try:
    import time
    credential = pathlib.Path('credential.json').resolve()
    credential.write_text(json.dumps({'token': 'first-grant', 'expires_at': int(time.time()) + 30}))
    credential.chmod(0o600)
    origin = f'http://127.0.0.1:{server.server_port}'
    def attrs(keys):
        return {'mrsm_api_alias': {'version': 1, 'logical_keys': keys,
            'callback_url': origin, 'credential_file': str(credential)}}
    config = {'meerschaum': {'connectors': {'sql': {}, 'valkey': {}, 'api': {}}}}
    for typ, label in [('sql', 'team'), ('valkey', 'team'), ('api', 'remote')]:
        config['meerschaum']['connectors'][typ][label] = attrs(f'{typ}:{label}')
    set_config(config)
    def forbidden_discovery():
        raise AssertionError('A logical API lookup must not discover plugins.')
    connectors.load_plugin_connectors = forbidden_discovery
    sql = mrsm.get_connector('sql:team')
    assert str(sql) == 'sql:team' and sql.type == 'sql' and sql.label == 'team'
    assert str(mrsm.get_connector('valkey:team')) == 'valkey:team'
    assert str(mrsm.get_connector('api:remote')) == 'api:remote'
    assert mrsm.get_connector('sql:team') is sql
    try:
        mrsm.get_connector('sql:team', flavor='sqlite', database=str(pathlib.Path('must-not-create.sqlite').resolve()))
    except ValueError: pass
    else: raise AssertionError('Native attributes overrode an API alias.')
    try:
        sql.engine
    except RuntimeError: pass
    else: raise AssertionError('A native SQL engine escaped the API transport.')
    pipe = mrsm.Pipe('sql:team', 'metric', instance=sql,
        parameters={'sql': 'SELECT must_not_send_local_sql'})
    docs = sql.get_pipe_docs(pipe)
    assert docs == [{'value': 7}], docs
    credential.write_text(json.dumps({'token': 'second-grant', 'expires_at': int(time.time()) + 30}))
    frame = sql.fetch(pipe, begin=1, limit=3)
    assert frame['value'].tolist() == [8], frame
    assert [request[1] for request in requests] == ['Bearer first-grant', 'Bearer second-grant'], requests
    assert parse_qs(urlsplit(requests[0][0]).query)['instance_keys'] == ['sql:team']
    assert unquote(urlsplit(requests[1][0]).path).endswith('/metric/[None]/fetch')
    body = requests[1][2]
    assert body['instance_keys'] == 'sql:team' and body['source_keys'] == 'sql:team'
    assert body['begin'] == {'kind': 'integer', 'value': 1} and body['limit'] == 3
    assert 'must_not_send_local_sql' not in json.dumps(body)
    assert 'mrsm_api_alias' in sql.meta and sql.meta['type'] == 'sql'
finally:
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)
''', tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr


def test_python_alias_cannot_fall_back_from_invalid_descriptor(tmp_path):
    result = _run_python(r'''
import pathlib
import meerschaum as mrsm
from meerschaum.config import set_config
path = pathlib.Path('forbidden.sqlite').resolve()
config = {'meerschaum': {'connectors': {'sql': {}}}}
config['meerschaum']['connectors']['sql']['broken'] = {
    'mrsm_api_alias': {'version': 999, 'logical_keys': 'sql:broken',
        'callback_url': 'http://127.0.0.1:1', 'credential_file': str(pathlib.Path('credential').resolve())},
    'flavor': 'sqlite', 'database': str(path),
}
set_config(config)
try:
    mrsm.get_connector('sql:broken')
except ValueError: pass
else: raise AssertionError('Invalid descriptor became a native connector.')
assert not path.exists()
''', tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
