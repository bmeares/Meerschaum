"""Copy and sync through a real Rust API whose response limit is three rows."""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import urllib.error
import urllib.parse
import urllib.request

import pytest

from tests.console.conftest import PASSWORD, USERNAME, Server

pytestmark = pytest.mark.console
SOURCE = 'sql:w7console'
METRIC = 'limited-readings'
TIES = 'unsplittable-readings'


@pytest.fixture
def capped_server():
    server = Server('api-source-chunks', native=False, args=['--private', '--no-dash'],
                    config={'api': {'data': {'max_response_row_limit': 3}}})
    config = json.loads(server.env['MRSM_CONFIG'])
    config['meerschaum']['connectors'] = {'api': {'capped': {
        'host': '127.0.0.1', 'port': server.port, 'username': USERNAME,
        'password': PASSWORD, 'max_response_row_limit': 3,
    }}}
    server.env['MRSM_CONFIG'] = json.dumps(config)
    for label in ('dest', 'filtered'):
        server.env['MRSM_SQL_' + label.upper()] = f'sqlite:///{server.root / (label + ".db")}'
    try:
        server.start()
        python_json(server, f'''
parameters = {{'columns': {{'datetime': 'n', 'id': 'id'}},
              'dtypes': {{'n': 'int', 'id': 'int', 'value': 'int', 'kind': 'str'}}}}
for metric, rows in [
    ({METRIC!r}, [{{'n': i // 2, 'id': i, 'value': i * 10,
                    'kind': 'even' if i % 2 == 0 else 'odd'}} for i in range(20)]),
    ({TIES!r}, [{{'n': 4, 'id': i, 'value': i, 'kind': 'tied'}} for i in range(4)]),
]:
    pipe = m.Pipe({SOURCE!r}, metric, instance='sql:w7console',
                  parameters=json.loads(json.dumps(parameters)))
    success, message = pipe.sync(rows)
    assert success, message
print('API_CHUNKS:' + json.dumps(True))
''')
        yield server
    finally:
        server.stop()


def python_json(server, code):
    result = server.python('import json\nimport meerschaum as m\n' + code)
    assert result.returncode == 0, result.stderr[-3000:]
    rows = [line[11:] for line in result.stdout.splitlines() if line.startswith('API_CHUNKS:')]
    assert len(rows) == 1, result.stdout[-3000:]
    return json.loads(rows[0])


def copy(server, metric, destination='sql:dest', *filters):
    return server.mrsm('copy', 'pipes', destination, '-i', 'api:capped',
        '-c', SOURCE, '-m', metric, '-l', 'None', '--sync-data', 'filter',
        '--begin', '0', '--end', '10', '-f', '-y', '--noask', *filters)


def ids(server, metric, destination='sql:dest'):
    return python_json(server, f'''
pipe = m.Pipe({SOURCE!r}, {metric!r}, instance={destination!r})
print('API_CHUNKS:' + json.dumps(sorted(row['id'] for row in pipe.get_data(as_docs=True))))
''')


def test_real_api_copy_and_source_sync_keep_all_rows_and_filters(capped_server):
    server = capped_server
    result = copy(server, METRIC)
    assert result.returncode == 0, result.stdout + result.stderr
    assert ids(server, METRIC) == list(range(20))

    result = copy(server, METRIC, 'sql:filtered', '--begin', '2', '--end', '7',
                  '--params', '{"kind":"even"}')
    assert result.returncode == 0, result.stdout + result.stderr
    assert ids(server, METRIC, 'sql:filtered') == [4, 6, 8, 10, 12]

    result = python_json(server, f'''
def destination(metric):
    return m.Pipe('api:capped', metric, instance='sql:dest', parameters={{
        'columns': {{'datetime': 'n', 'id': 'id'}},
        'dtypes': {{'n': 'int', 'id': 'int', 'value': 'int'}},
        'fetch': {{'pipe': {{'connector': {SOURCE!r}, 'metric': {METRIC!r}}},
                  'select_columns': ['n', 'id', 'value', 'kind'], 'omit_columns': ['kind']}},
    }})
all_rows = destination('complete-sync')
success, message = all_rows.sync(begin=0, end=10)
assert success, message
full = all_rows.get_data(as_docs=True)
filtered = destination('filtered-sync')
success, message = filtered.sync(begin=2, end=7, params={{'kind': 'even'}})
assert success, message
subset = filtered.get_data(as_docs=True)
assert all('kind' not in row for row in full + subset)
print('API_CHUNKS:' + json.dumps({{'all': sorted(row['id'] for row in full),
                                'filtered': sorted(row['id'] for row in subset)}}))
''')
    assert result == {'all': list(range(20)), 'filtered': [4, 6, 8, 10, 12]}


def test_real_api_unsplittable_ties_fail_instead_of_reporting_a_partial_success(capped_server):
    server = capped_server
    result = copy(server, TIES)
    assert result.returncode != 0, result.stdout + result.stderr
    result = python_json(server, f'''
copied = m.Pipe({SOURCE!r}, {TIES!r}, instance='sql:dest')
assert copied.get_rowcount() in (None, 0), 'A truncated tied timestamp was synced.'
pipe = m.Pipe('api:capped', 'tied-sync', instance='sql:dest', parameters={{
    'columns': {{'datetime': 'n', 'id': 'id'}},
    'dtypes': {{'n': 'int', 'id': 'int', 'value': 'int', 'kind': 'str'}},
    'fetch': {{'pipe': {{'connector': {SOURCE!r}, 'metric': {TIES!r}}}}},
}})
success, message = pipe.sync(begin=0, end=10)
assert not success, message
assert pipe.get_rowcount() in (None, 0), 'A truncated tied timestamp was synced.'
print('API_CHUNKS:' + json.dumps({{'failed': not success, 'message': message}}))
''')
    assert result['failed']


def test_real_api_applies_descending_order_before_its_row_limit(capped_server):
    server = capped_server
    result = python_json(server, f'''
pipe = m.Pipe({SOURCE!r}, {METRIC!r}, instance='api:capped')
ascending = pipe.get_data(order='asc', limit=2, as_pandas=True).to_dict(orient='records')
descending = pipe.get_data(order='desc', limit=2, as_pandas=True).to_dict(orient='records')
bounded = pipe.get_data(begin=2, end=7, order='desc', limit=2,
                        as_pandas=True).to_dict(orient='records')
backtrack = pipe.get_backtrack_data(limit=1, as_pandas=True).to_dict(orient='records')
assert [row['n'] for row in ascending] == [0, 0], ascending
assert [row['n'] for row in descending] == [9, 9], descending
assert [row['n'] for row in bounded] == [6, 6], bounded
assert [row['n'] for row in backtrack] == [9], backtrack
print('API_CHUNKS:' + json.dumps(True))
''')
    assert result


@contextmanager
def failed_existence_proxy(server, suffix='/exists'):
    """Forward the fixture's reads and login; fail one requested metadata probe."""
    failed = []
    requests = []
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def answer(self):
            path = urllib.parse.urlsplit(self.path).path
            requests.append((self.command, path))
            if path.endswith(suffix):
                failed.append(self.path)
                status, body = 503, b'{"detail":"Intentional source catalog failure."}'
                content_type = 'application/json'
            else:
                length = int(self.headers.get('Content-Length', 0))
                assert 0 <= length <= 65536
                data = self.rfile.read(length) if length else None
                headers = {key: self.headers[key] for key in ('Authorization', 'Content-Type')
                           if key in self.headers}
                req = urllib.request.Request(server.base_url + self.path, data=data,
                                             headers=headers, method=self.command)
                try:
                    response = opener.open(req, timeout=10)
                except urllib.error.HTTPError as error:
                    response = error
                with response:
                    status = response.status
                    content_type = response.headers.get('Content-Type', 'application/json')
                    body = response.read(512001)
                    assert len(body) <= 512000
            self.send_response(status)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_GET = answer
        do_POST = answer
        do_PATCH = answer
        do_PUT = answer
        do_DELETE = answer

    proxy = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    proxy.daemon_threads = True
    worker = threading.Thread(target=proxy.serve_forever, kwargs={'poll_interval': .05}, daemon=True)
    worker.start()
    try:
        yield proxy.server_port, failed, requests
    finally:
        proxy.shutdown()
        proxy.server_close()
        worker.join(timeout=10)
        assert not worker.is_alive()


def test_real_api_copy_reports_an_unreadable_source_instead_of_a_success(capped_server):
    server = capped_server
    missing = 'registered-without-data'
    python_json(server, f'''
pipe = m.Pipe({SOURCE!r}, {missing!r}, instance='sql:w7console', parameters={{
    'columns': {{'datetime': 'n', 'id': 'id'}}, 'dtypes': {{'n': 'int', 'id': 'int'}},
}})
success, message = pipe.register()
assert success, message
assert not pipe.exists()
print('API_CHUNKS:true')
''')
    # A proven missing table is a valid attributes-only copy, even when data was requested.
    result = copy(server, missing)
    assert result.returncode == 0, result.stdout + result.stderr

    config = json.loads(server.env['MRSM_CONFIG'])
    with failed_existence_proxy(server) as (port, failures, _requests):
        config['meerschaum']['connectors']['api']['capped']['port'] = port
        server.env['MRSM_CONFIG'] = json.dumps(config)
        result = copy(server, METRIC)
        assert failures, 'The intended existence-probe failure was not exercised.'
        assert result.returncode != 0, result.stdout + result.stderr
    python_json(server, f'''
pipe = m.Pipe({SOURCE!r}, {METRIC!r}, instance='sql:dest')
assert pipe.get_rowcount() in (None, 0)
print('API_CHUNKS:true')
''')


def test_real_api_copy_does_not_register_after_an_unreadable_destination_id(capped_server):
    server = capped_server
    config = json.loads(server.env['MRSM_CONFIG'])
    with failed_existence_proxy(server, '/id') as (port, failures, requests):
        config['meerschaum']['connectors']['api']['capped']['port'] = port
        server.env['MRSM_CONFIG'] = json.dumps(config)
        result = server.mrsm('copy', 'pipes', 'api:capped', '-i', 'sql:w7console',
            '-c', SOURCE, '-m', METRIC, '-l', 'None', '--sync-data', 'nothing', '-f', '-y', '--noask')
        assert failures, 'The intended destination-ID failure was not exercised.'
        assert result.returncode != 0, result.stdout + result.stderr
        writes = [(verb, path) for verb, path in requests
                  if verb in ('POST', 'PUT', 'PATCH', 'DELETE') and path.startswith('/pipes/')]
        assert not writes, 'A registration was attempted after an unreadable lookup: ' + str(writes)


def test_real_api_copy_keeps_destination_metadata_when_its_read_fails(capped_server):
    server = capped_server
    before = python_json(server, f'''
original = m.Pipe({SOURCE!r}, {METRIC!r}, instance='sql:w7console')
parameters = original.get_parameters(apply_symlinks=False)
different = m.Pipe({SOURCE!r}, {METRIC!r}, instance='sql:filtered',
                   parameters={{**parameters, 'copy_sentinel': 'from a different source'}})
success, message = different.register()
assert success, message
print('API_CHUNKS:' + json.dumps(parameters))
''')
    config = json.loads(server.env['MRSM_CONFIG'])
    with failed_existence_proxy(server, '/attributes') as (port, failures, requests):
        config['meerschaum']['connectors']['api']['capped']['port'] = port
        server.env['MRSM_CONFIG'] = json.dumps(config)
        result = server.mrsm('copy', 'pipes', 'api:capped', '-i', 'sql:filtered',
            '-c', SOURCE, '-m', METRIC, '-l', 'None', '--sync-data', 'nothing', '-f', '-y', '--noask')
        assert failures, 'The intended destination-metadata failure was not exercised.'
        assert result.returncode != 0, result.stdout + result.stderr
        writes = [(verb, path) for verb, path in requests
                  if verb in ('POST', 'PUT', 'PATCH', 'DELETE') and path.startswith('/pipes/')]
        assert not writes, 'Destination metadata was edited after an unreadable read: ' + str(writes)
    after = python_json(server, f'''
pipe = m.Pipe({SOURCE!r}, {METRIC!r}, instance='sql:w7console')
print('API_CHUNKS:' + json.dumps(pipe.get_parameters(apply_symlinks=False)))
''')
    assert after == before
