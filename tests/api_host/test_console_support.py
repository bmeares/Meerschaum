#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The pieces of the web console that need no browser: the line the dashboard's Exec button runs, the
engine views of the pipelines page, the helpers that replaced v4's deleted internals, the session
cache and the host's loopback-TCP transport. Everything runs in a subprocess in host mode with a
temporary root, plugins directory and SQLite instance.
"""

import json
import os
import sys
import time
import socket
import subprocess
import http.client

from tests.api_host._helpers import make_env, run_python, write_plugin, sandbox  # noqa: F401


def _json(result) -> dict:
    assert result.returncode == 0, result.stderr[-4000:]
    line = [l for l in result.stdout.splitlines() if l.startswith('JSON:')][-1]
    return json.loads(line[len('JSON:'):])


def test_exec_builds_the_line_the_terminal_used_to_get(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from meerschaum.api.dash.execute import build_sysargs, build_pipe_sysargs

out = {}
out['basic'] = build_sysargs(action='show', subaction='pipes')
out['keys'] = build_sysargs(
    connector_keys=['sql:main', 'plugin:x y'], metric_keys=['m'], location_keys=['a', None],
    tags=['t'], action='sync', subaction='pipes', instance='sql:local',
    flags=['--debug'], input_flags=['--begin', '--end', None], input_flags_texts=['2024-01-01 10:00', '', 'x'],
    allowed_flags=['--debug', '--begin', '--end'],
)
out['text'] = build_sysargs(action='show', subaction='logs', subaction_text="my-job --nopretty 'a b'")
out['omitted'] = build_sysargs(action='sh', subaction='')
out['no_action'] = build_sysargs(action='')
out['bad_flag'] = build_sysargs(action='show', subaction='pipes', flags=['--shell'], allowed_flags=['--debug'])
out['bad_input_flag'] = build_sysargs(
    action='show', subaction='pipes', input_flags=['--executor-keys'], input_flags_texts=['api:x'],
    allowed_flags=['--debug'],
)
out['bad_quote'] = build_sysargs(action='show', subaction='logs', subaction_text="'unterminated")
meta = {'connector_keys': 'sql:main', 'metric_key': 'm', 'location_key': None, 'instance_keys': 'sql:local'}
out['verify'] = build_pipe_sysargs(meta, 'verify')
out['clear_unconfirmed'] = build_pipe_sysargs(meta, 'clear')
out['clear'] = build_pipe_sysargs({**meta, 'location_key': 'l'}, 'clear', confirmed=True)
out['unknown'] = build_pipe_sysargs(meta, 'python')
out['bad_meta'] = build_pipe_sysargs({}, 'sync')
print('JSON:' + json.dumps(out))
'''))
    assert out['basic'] == [['show', 'pipes'], '']
    assert out['keys'] == [[
        'sync', 'pipes', '-c', 'sql:main', 'plugin:x y', '-m', 'm', '-l', 'a', 'None', '-t', 't',
        '-i', 'sql:local', '--debug', '--begin', '2024-01-01 10:00',
    ], '']
    assert out['text'] == [['show', 'logs', 'my-job', '--nopretty', 'a b'], '']
    assert out['omitted'][0] is None and "'sh'" in out['omitted'][1]
    assert out['no_action'][0] is None
    assert out['bad_flag'][0] is None and '--shell' in out['bad_flag'][1]
    assert out['bad_input_flag'][0] is None
    assert out['bad_quote'][0] is None
    assert out['verify'] == [
        ['verify', 'pipes', '-c', 'sql:main', '-m', 'm', '-l', 'None', '-i', 'sql:local'], '',
    ]
    ### A job cannot answer a prompt: a destructive action runs only after the modal, with `--yes`.
    assert out['clear_unconfirmed'][0] is None
    assert out['clear'] == [[
        'clear', 'pipes', '-c', 'sql:main', '-m', 'm', '-l', 'l', '-i', 'sql:local', '--yes',
    ], '']
    assert out['unknown'][0] is None and out['bad_meta'][0] is None


def test_exec_posts_to_the_server_with_a_user_bearer(sandbox):
    """`server_url` in the host config: `POST /execute` with a minted bearer, and its stream path."""
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json, threading, http.server
import jwt
from meerschaum.api._oauth2 import SECRET
from meerschaum.api.host import HOST_CONFIG_ENV
from meerschaum.core import User
from meerschaum.api import get_api_connector

instance = get_api_connector()
instance.register_user(User('execuser', 'correct-horse-1', type='admin', instance=instance))

seen = {}
class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['content-length'])))
        seen.update(path=self.path, body=body, auth=self.headers.get('authorization'))
        if body['sysargs'][0] == 'nope':
            payload = {'success': False, 'msg': 'refused'}
        else:
            payload = {'success': True, 'msg': 'started', 'job': 'execute-abc', 'stream': '/logs/execute-abc/stream'}
        data = json.dumps(payload).encode()
        self.send_response(200); self.send_header('content-type', 'application/json')
        self.send_header('content-length', str(len(data))); self.end_headers(); self.wfile.write(data)
    def log_message(self, *args): pass

server = http.server.HTTPServer(('127.0.0.1', 0), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
import os
os.environ[HOST_CONFIG_ENV] = json.dumps({'server_url': f'http://127.0.0.1:{server.server_port}/'})

from meerschaum.api.dash import execute
out = {'url': execute.server_url().startswith('http://127.0.0.1:')}
result = execute.run_action(['sync', 'pipes'], 'execuser')
out['result'] = result
out['seen_path'] = seen['path']
out['seen_body'] = seen['body']
claims = jwt.decode(seen['auth'].split(' ', 1)[1], SECRET, algorithms=['HS256'])
out['claims'] = {'sub': claims['sub'], 'scopes': claims['scopes'], 'ttl': claims['exp'] > 0}
out['refused'] = execute.run_action(['nope'], 'execuser')
server.shutdown()
print('JSON:' + json.dumps(out))
'''))
    assert out['url'] is True
    assert out['result'] == {
        'success': True, 'msg': 'started', 'job': 'execute-abc', 'stream': '/logs/execute-abc/stream',
    }
    assert out['seen_path'] == '/execute' and out['seen_body'] == {'sysargs': ['sync', 'pipes']}
    assert out['claims']['sub'] == 'execuser' and out['claims']['scopes'] == ['*']
    assert out['refused']['success'] is False and out['refused']['msg'] == 'refused'


def test_engine_views_of_the_pipelines_page(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from datetime import datetime, timedelta
import meerschaum as mrsm
from meerschaum.api import get_api_connector
from meerschaum.api.dash import engine
from meerschaum.core.Pipeline import Pipeline

instance = get_api_connector()
src = mrsm.Pipe('dp', 'src', instance=instance, columns={'datetime': 'dt'}, dtypes={'dt': 'datetime'})
success, msg = src.sync([{'dt': datetime(2026, 1, 1) + timedelta(hours=i), 'v': i} for i in range(5)])
assert success, msg
mid = mrsm.Pipe(
    'dp', 'mid', instance=instance, columns={'datetime': 'dt'}, dtypes={'dt': 'datetime'},
    parameters={'parents': [{'connector': 'dp', 'metric': 'src'}]},
)
mid.register()
leaf = mrsm.Pipe(
    'dp', 'leaf', 'loc', instance=instance, columns={'datetime': 'dt'}, dtypes={'dt': 'datetime'},
    parameters={'parents': [{'connector': 'dp', 'metric': 'mid'}]},
)
leaf.register()
run = Pipeline(leaf, parents=True).sync()

out = {'run_success': bool(run.success)}
graph = engine.plan_graph(instance)
out['nodes'] = sorted(n['label'] for n in graph['nodes'])
out['edges'] = sorted((engine.ref_label(e['parent']), engine.ref_label(e['child'])) for e in graph['edges'])
out['statuses'] = {n['label']: n['status'] for n in graph['nodes']}
figure = engine.graph_figure(graph)
out['figure_points'] = len(figure['data'][0]['x'])
out['figure_arrows'] = len(figure['layout']['annotations'])
out['x_by_label'] = dict(zip(figure['data'][0]['text'], figure['data'][0]['x']))

runs = engine.get_runs(instance)
out['runs'] = len(runs)
out['run_status'] = runs[0]['status']
out['filtered'] = len(engine.get_runs(instance, status='failed'))
out['by_pipe'] = len(engine.get_runs(instance, pipe='dp|leaf'))
out['by_id'] = len(engine.get_runs(instance, run_id=runs[0]['run']['run_id']))
detail = engine.get_run_detail(instance, runs[0]['run']['run_id'])
gantt = engine.gantt_figure(detail)
out['gantt_traces'] = [t['name'] for t in gantt['data']]
out['gantt_planned'] = len(gantt['data'][0]['y'])
out['gantt_actual'] = len(gantt['data'][1]['y'])
out['unknown_run'] = engine.get_run_detail(instance, 'no-such-run')
events = engine.get_run_events(instance, runs[0]['run']['run_id'])
lines = [engine.describe_event(e) for e in events]
out['event_lines'] = [l.split(' ', 1)[1] for l in lines][:3]

relations = engine.relations_of(mid)
out['mid_parents'] = [r['label'] for r in relations['parents']]
out['mid_children'] = [r['label'] for r in relations['children']]
out['href'] = relations['children'][0]['href']
out['unregistered'] = engine.relations_of(mrsm.Pipe('dp', 'ghost', instance=instance))
print('JSON:' + json.dumps(out))
'''))
    assert out['run_success'] is True
    assert out['nodes'] == ['dp/leaf/loc', 'dp/mid', 'dp/src']
    assert out['edges'] == [['dp/mid', 'dp/leaf/loc'], ['dp/src', 'dp/mid']]
    assert out['statuses']['dp/src'] == 'root'
    assert out['figure_points'] == 3 and out['figure_arrows'] == 2
    ### Columns by depth: a parent is left of its child.
    xs = out['x_by_label']
    assert xs['dp/src'] < xs['dp/mid'] < xs['dp/leaf/loc']
    assert out['runs'] >= 1 and out['run_status'] == 'succeeded'
    assert out['filtered'] == 0 and out['by_pipe'] >= 1 and out['by_id'] == 1
    assert out['gantt_traces'] == ['Planned', 'Actual']
    ### Every node has a planned bar; a node that did not run has no actual one.
    assert out['gantt_planned'] == 3 and 1 <= out['gantt_actual'] <= 3
    assert out['unknown_run'] is None
    assert out['event_lines'][0].startswith('run_started')
    assert out['mid_parents'] == ['dp/src'] and out['mid_children'] == ['dp/leaf/loc']
    assert out['href'] == '/dash/pipes/dp/leaf/loc?instance=sql%3Aapihosttest'
    assert out['unregistered'] == {'parents': [], 'children': []}


def test_layout_handles_cycles_and_isolated_nodes(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from meerschaum.api.dash.engine import layered_positions, ref_label, node_status, split_ref

positions = layered_positions(['a|c|m|None', 'b|c|n|None', 'c|c|o|None'], [
    ('a|c|m|None', 'b|c|n|None'), ('b|c|n|None', 'a|c|m|None'),
])
out = {
    'count': len(positions),
    'isolated_column': positions['c|c|o|None'][0],
    'label': ref_label('i|conn|metric|loc'),
    'label_no_loc': ref_label('i|conn|metric|None'),
    'split': list(split_ref('i|conn|metric|None')),
    'status': [node_status(a) for a in ('skip', 'incremental', 'backfill', 'rebuild', 'root', 'observe')],
}
print('JSON:' + json.dumps(out))
'''))
    assert out['count'] == 3 and out['isolated_column'] == 0.0
    assert out['label'] == 'conn/metric/loc' and out['label_no_loc'] == 'conn/metric'
    assert out['split'] == ['i', 'conn', 'metric', None]
    assert out['status'] == ['caught_up', 'pending', 'pending', 'rebuild', 'root', 'observe']


def test_pipe_query_helpers(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
import meerschaum as mrsm
from meerschaum.api import get_api_connector
from meerschaum.api.dash._support import get_pipe_query, set_pipe_query, get_shell_intro, authenticate_user

instance = get_api_connector()
pipe = mrsm.Pipe('sql:apihosttest', 'q', instance=instance, parameters={'fetch': {'definition': 'SELECT 1 AS a'}})
pipe.register()
out = {'before': get_pipe_query(pipe)}
success, _ = set_pipe_query(pipe, 'SELECT 2 AS a')
pipe = mrsm.Pipe('sql:apihosttest', 'q', instance=instance)
out['after'] = get_pipe_query(pipe)
out['fetch_keys'] = sorted(pipe.parameters['fetch'])
other = mrsm.Pipe('sql:apihosttest', 'plain', instance=instance, parameters={'sql': 'SELECT 3'})
set_pipe_query(other, 'SELECT 4')
out['sql_key'] = other.parameters['sql']
out['none'] = get_pipe_query(mrsm.Pipe('sql:apihosttest', 'empty', instance=instance), warn=False)
out['intro'] = 'v5' in get_shell_intro()

from meerschaum.core import User
instance.register_user(User('loginuser', 'correct-horse-1', instance=instance))
out['good'] = authenticate_user('loginuser', 'correct-horse-1')
out['bad'] = authenticate_user('loginuser', 'wrong-password')
out['unknown'] = authenticate_user('nobody-here', 'correct-horse-1')
out['empty'] = authenticate_user('', '')
print('JSON:' + json.dumps(out))
'''))
    assert out['before'] == 'SELECT 1 AS a'
    assert out['after'] == 'SELECT 2 AS a'
    ### v4 wrote `fetch: {defintion: ...}` here and lost the edit.
    assert out['fetch_keys'] == ['definition']
    assert out['sql_key'] == 'SELECT 4'
    assert out['none'] is None
    assert out['intro'] is True
    assert (out['good'], out['bad'], out['unknown'], out['empty']) == (True, False, False, False)


def test_session_lookups_are_cached_for_a_few_seconds(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from meerschaum.api.dash import sessions

calls = []
real = sessions._get_session_data
def counting(session_id):
    calls.append(session_id)
    return real(session_id)
sessions._get_session_data = counting

sessions.set_session('cached-1', {'username': 'ann'})
out = {}
out['first'] = sessions.get_username_from_session('cached-1')
out['second'] = sessions.get_username_from_session('cached-1')
out['third'] = sessions.is_session_active('cached-1')
out['store_reads'] = len(calls)
### Changing a session drops its cache entry in this process.
sessions.update_session('cached-1', {'admin': True})
out['updated'] = sessions.get_session_data('cached-1')
### Logging out is immediate here.
sessions.delete_session('cached-1')
out['after_delete'] = sessions.get_username_from_session('cached-1')
out['active_after_delete'] = sessions.is_session_active('cached-1')
### An unknown session is looked up every time (a login is visible at once).
before = len(calls)
sessions.get_username_from_session('nope'); sessions.get_username_from_session('nope')
out['unknown_reads'] = len(calls) - before
### The cache expires.
sessions.set_session('cached-2', {'username': 'bob'})
sessions.SESSION_CACHE_SECONDS = 0.0
sessions.get_username_from_session('cached-2'); n = len(calls)
sessions.get_username_from_session('cached-2')
out['expired_rereads'] = len(calls) - n
print('JSON:' + json.dumps(out))
'''))
    assert (out['first'], out['second'], out['third']) == ('ann', 'ann', True)
    assert out['store_reads'] == 1
    assert out['updated'] == {'username': 'ann', 'admin': True}
    assert out['after_delete'] is None and out['active_after_delete'] is False
    assert out['unknown_reads'] == 2
    assert out['expired_rereads'] == 1


def test_console_status_and_availability(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from meerschaum.api.host import console_available, dash_enabled
from meerschaum.api.dash import console_available as probe
from meerschaum.api.host.dash import console_status
out = {'available': console_available(), 'probe': probe(), 'before_build': console_status()}
from meerschaum.api.host import build_app
build_app()
out['after_build'] = console_status()
print('JSON:' + json.dumps(out))
'''))
    assert out['available'] is True and out['probe'] is True
    assert out['before_build']['mounted'] is False
    assert out['after_build']['mounted'] is True and out['after_build']['dash_version']


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        return probe.getsockname()[1]


def _request(port: int, path: str, headers: dict = None) -> int:
    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=10)
    try:
        connection.request('GET', path, headers=headers or {})
        return connection.getresponse().status
    finally:
        connection.close()


def test_the_tcp_transport_refuses_to_start_without_a_token(sandbox):
    """No token, no unauthenticated TCP port: the host exits and says why."""
    root, plugins = sandbox
    env = make_env(root, plugins)
    env.pop('MRSM_API_HOST_PROXY_TOKEN', None)
    result = subprocess.run(
        [sys.executable, '-m', 'meerschaum.api.host', '--tcp', f'127.0.0.1:{_free_port()}'],
        env=env, cwd=str(root), capture_output=True, text=True, timeout=180,
    )
    assert result.returncode != 0
    assert 'MRSM_API_HOST_PROXY_TOKEN' in (result.stdout + result.stderr)


def test_the_tcp_transport_requires_the_proxy_token(sandbox):
    """`--tcp` (the Windows transport): without the token every request is refused."""
    root, plugins = sandbox
    port = _free_port()
    env = make_env(root, plugins)
    env['MRSM_API_HOST_PROXY_TOKEN'] = 'proxy-token-for-the-test'
    proc = subprocess.Popen(
        [sys.executable, '-m', 'meerschaum.api.host', '--tcp', f'127.0.0.1:{port}'],
        env=env, cwd=str(root), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        start_new_session=True,
    )
    good = {'X-Mrsm-Host-Proxy-Token': 'proxy-token-for-the-test'}
    try:
        deadline = time.time() + 120
        status = None
        while time.time() < deadline:
            if proc.poll() is not None:
                raise AssertionError(f"The host exited:\n{proc.stdout.read()[-4000:]}")
            try:
                status = _request(port, '/openapi.json', good)
                break
            except OSError:
                time.sleep(0.3)
        assert status == 200
        assert _request(port, '/openapi.json') == 403
        assert _request(port, '/dash/', {'X-Mrsm-Host-Proxy-Token': 'wrong'}) == 403
        assert _request(port, '/_mrsm/host/status') == 403
        assert _request(port, '/dash/', good) == 200
        # The identity proof the proxy asks for before it forwards anything: answered without the
        # token (the nonce is the proxy's), and only a holder of the token can compute it.
        import hashlib
        import hmac
        connection = http.client.HTTPConnection('127.0.0.1', port, timeout=10)
        try:
            connection.request('GET', '/_mrsm/host/proof?nonce=abc123')
            response = connection.getresponse()
            assert response.status == 200
            expected = hmac.new(
                b'proxy-token-for-the-test', b'abc123', hashlib.sha256,
            ).hexdigest()
            assert response.read().decode('ascii') == expected
        finally:
            connection.close()
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def test_the_host_needs_one_transport(sandbox):
    root, plugins = sandbox
    for argv in ([], ['--socket', 'a.sock', '--tcp', '127.0.0.1:1'], ['--tcp', 'nope']):
        result = subprocess.run(
            [sys.executable, '-m', 'meerschaum.api.host'] + argv,
            env=make_env(root, plugins), cwd=str(root), capture_output=True, text=True, timeout=120,
        )
        assert result.returncode == 2, (argv, result.stderr[-500:])
