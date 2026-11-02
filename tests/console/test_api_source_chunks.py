"""Copy and sync through a real Rust API whose response limit is three rows."""

import json

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
