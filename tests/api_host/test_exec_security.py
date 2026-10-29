#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Wave 18 (FX7): the dashboard's Exec button never runs an action in the host.

The host-local fallback of the first draft started a hidden job in the host process whenever the
server's `POST /execute` could not be reached (no `server_url`, a connection error, a route the
allowlist left out), with no permission check: any logged-in user could run `python -c ...` as the
server's OS user. Now every action goes through the server with the user's bearer and the console
shows the server's answer; nothing runs locally. Everything runs in a subprocess in host mode
with a temporary root, plugins directory and SQLite instance.
"""

import json

from tests.api_host._helpers import make_env, run_python, write_plugin, sandbox  # noqa: F401
from tests.api_host.test_console_support import _json


FAKE_SERVER = '''
import json, os, threading, http.server
from meerschaum.api.host import HOST_CONFIG_ENV

seen = []
behavior = {'mode': 'ok'}

class Handler(http.server.BaseHTTPRequestHandler):
    def _send(self, code, payload):
        data = json.dumps(payload).encode()
        self.send_response(code); self.send_header('content-type', 'application/json')
        self.send_header('content-length', str(len(data))); self.end_headers(); self.wfile.write(data)
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['content-length'])))
        seen.append(('POST', self.path, body, self.headers.get('authorization')))
        mode = behavior['mode']
        if mode == '404':
            self._send(404, {'detail': 'Not Found'})
        elif mode == '403':
            self._send(403, {'detail': "The action 'python' cannot be run by non-administrators."})
        elif mode == 'tuple':
            self._send(200, [False, 'The administrator for this server has not allowed users to perform actions.'])
        else:
            self._send(200, {'success': True, 'msg': 'started', 'job': 'execute-abc', 'stream': '/logs/execute-abc/stream'})
    def do_GET(self):
        seen.append(('GET', self.path, None, self.headers.get('authorization')))
        if self.path == '/execute/policy' and behavior['mode'] != 'nopolicy':
            self._send(200, {'omit_actions': ['api', 'sh', 'python'], 'omit_flags': ['--shell']})
        else:
            self._send(404, {'detail': 'Not Found'})
    def log_message(self, *args): pass

server = http.server.HTTPServer(('127.0.0.1', 0), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
os.environ[HOST_CONFIG_ENV] = json.dumps({'server_url': f'http://127.0.0.1:{server.server_port}'})
'''


def test_exec_never_runs_an_action_in_the_host(sandbox):
    """RV2 #1: no `server_url` (a bare host) used to start the action as a local job, unchecked."""
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json, time, pathlib
from meerschaum.api.dash import execute
from meerschaum.jobs import get_jobs

marker = pathlib.Path('PWNED').resolve()
out = {'server_url': execute.server_url()}
result = execute.run_action(['python', '-c', f"open({str(marker)!r}, 'w').write('bob')"], 'bob')
out['result'] = result
time.sleep(3)
out['marker'] = marker.exists()
out['jobs'] = sorted(get_jobs(include_hidden=True))
out['local_runner'] = [n for n in ('start_exec_job', 'read_job_log', 'cleanup_exec_jobs') if hasattr(execute, n)]
print('JSON:' + json.dumps(out))
'''))
    assert out['server_url'] is None
    assert out['result']['success'] is False and out['result']['job'] is None
    assert 'no server' in out['result']['msg'].lower()
    assert out['marker'] is False
    assert out['jobs'] == []
    assert out['local_runner'] == []


def test_exec_shows_the_servers_error_and_runs_nothing_locally(sandbox):
    """A missing `/execute` route, a refusal, a refused permission and a dead server: all errors."""
    root, plugins = sandbox
    out = _json(run_python(root, plugins, FAKE_SERVER + '''
import time, pathlib
from meerschaum.api.dash import execute
from meerschaum.jobs import get_jobs

marker = pathlib.Path('PWNED').resolve()
line = ['python', '-c', f"open({str(marker)!r}, 'w').write('bob')"]
out = {}
for mode in ('404', '403', 'tuple'):
    behavior['mode'] = mode
    out[mode] = execute.run_action(line, 'bob')

# A server that is not there.
os.environ[HOST_CONFIG_ENV] = json.dumps({'server_url': 'http://127.0.0.1:9'})
out['down'] = execute.run_action(line, 'bob')
server.shutdown()
time.sleep(2)
out['marker'] = marker.exists()
out['jobs'] = sorted(get_jobs(include_hidden=True))
print('JSON:' + json.dumps(out))
'''))
    for key in ('404', '403', 'tuple', 'down'):
        assert out[key]['success'] is False and out[key]['job'] is None, key
    assert 'api:permissions:routes:allowlist' in out['404']['msg']
    assert out['403']['msg'] == (
        "The server refused the action: The action 'python' cannot be run by non-administrators."
    )
    assert out['tuple']['msg'] == (
        'The administrator for this server has not allowed users to perform actions.'
    )
    assert out['down']['msg'].startswith('Could not reach the server')
    assert out['marker'] is False
    assert out['jobs'] == []


def test_exec_sends_the_users_scopes_not_the_hosts(sandbox):
    """A non-admin's bearer carries the non-admin's own scopes (the server decides from them)."""
    root, plugins = sandbox
    out = _json(run_python(root, plugins, FAKE_SERVER + '''
import jwt
from meerschaum.api._oauth2 import SECRET
from meerschaum.api import get_api_connector
from meerschaum.core import User
from meerschaum.api.dash import execute

instance = get_api_connector()
instance.register_user(User('bob', 'correct-horse-1', instance=instance))
result = execute.run_action(['show', 'version'], 'bob')
claims = jwt.decode(seen[-1][3].split(' ', 1)[1], SECRET, algorithms=['HS256'])
out = {'result': result, 'sub': claims['sub'], 'type': claims.get('type'), 'scopes': claims['scopes']}
server.shutdown()
print('JSON:' + json.dumps(out))
'''))
    assert out['result']['success'] is True and out['result']['job'] == 'execute-abc'
    assert out['sub'] == 'bob' and out['type'] != 'admin' and out['scopes'] != ['*']


def test_the_console_reads_the_servers_policy_and_falls_back_to_its_own(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, FAKE_SERVER + '''
from meerschaum.api.dash import execute

out = {'builtin': sorted(execute.OMIT_ACTIONS)}
out['from_server'] = sorted(execute.omit_actions())
out['flags'] = sorted(execute.omit_flags())
out['python_refused'] = execute.build_sysargs(action='python', subaction='')[0]
out['show_ok'] = execute.build_sysargs(action='show', subaction='pipes')[0]
policy_calls = len([s for s in seen if s[1] == '/execute/policy'])
execute.omit_actions(); execute.omit_actions()
out['cached'] = len([s for s in seen if s[1] == '/execute/policy']) == policy_calls

# The server does not answer: the built-in copy, and the server is asked again later.
behavior['mode'] = 'nopolicy'
execute._policy_cache.update(policy=None, expires=0.0)
out['fallback'] = sorted(execute.omit_actions())
out['fallback_flags'] = sorted(execute.omit_flags())
server.shutdown()
print('JSON:' + json.dumps(out))
'''))
    assert out['from_server'] == ['api', 'python', 'sh']
    assert out['flags'] == ['--shell']
    assert out['python_refused'] is None and out['show_ok'] == ['show', 'pipes']
    assert out['cached'] is True
    assert out['fallback'] == out['builtin'] and out['fallback_flags'] == []


def test_the_dashboard_has_no_polling_of_job_logs(sandbox):
    """`poll_exec` read any job's log named in a client-held store, without checking a session."""
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from meerschaum.api.dash.callbacks import execute as callbacks
from meerschaum.api.dash.pages import dashboard

layout = repr(dashboard.layout)
print('JSON:' + json.dumps({
    'poll_callback': hasattr(callbacks, 'poll_exec'),
    'raw_pane': "exec-raw" in layout,
    'interval': "exec-interval" in layout,
    'store': "exec-store" in layout,
}))
'''))
    assert out == {'poll_callback': False, 'raw_pane': False, 'interval': False, 'store': True}
