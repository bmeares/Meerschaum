#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The console's client of the server's v5-only routes (`POST /execute`, `/execute/validate`,
`/execute/runs`) reads their `{"success", "message", ...}` shape with real status codes, and
still reads the shapes of older servers; the API host honors the public preset.
"""

import http.server
import json
import os
import subprocess
import sys
import threading

import pytest


class _Server:
    """A fake Rust server answering what `responses[(method, path)]` says."""

    def __init__(self, responses):
        self.responses = responses
        self.seen = []
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def _answer(self, method):
                length = int(self.headers.get('content-length') or 0)
                body = json.loads(self.rfile.read(length)) if length else None
                outer.seen.append((method, self.path, body))
                code, payload = outer.responses.get(
                    (method, self.path.split('?')[0]),
                    (404, {'success': False, 'message': 'Not Found'}),
                )
                data = json.dumps(payload).encode()
                self.send_response(code)
                self.send_header('content-type', 'application/json')
                self.send_header('content-length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                self._answer('GET')

            def do_POST(self):
                self._answer('POST')

            def log_message(self, *args):
                pass

        self.httpd = http.server.HTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        self.url = f'http://127.0.0.1:{self.httpd.server_port}'

    def close(self):
        self.httpd.shutdown()


@pytest.fixture
def server(monkeypatch):
    servers = []

    def _make(responses):
        srv = _Server(responses)
        servers.append(srv)
        monkeypatch.setenv('MRSM_API_HOST_CONFIG', json.dumps({'server_url': srv.url}))
        return srv

    yield _make
    for srv in servers:
        srv.close()


def test_execute_reads_the_v5_shape(server):
    from meerschaum.api.dash import execute
    server({
        ('POST', '/execute'): (200, {
            'success': True, 'message': 'Started.', 'job': 'execute-abc',
            'sysargs': ['show', 'version'], 'stream': None, 'logs': '/logs/execute-abc',
        }),
    })
    result = execute.run_action(['show', 'version'], None)
    assert result['success'] is True
    assert result['msg'] == 'Started.'
    assert result['job'] == 'execute-abc'
    ### The server names no log stream (it does not serve one): the console does not guess one.
    assert result['stream'] is None


def test_execute_refusals_carry_the_servers_message(server):
    from meerschaum.api.dash import execute
    server({
        ('POST', '/execute'): (403, {
            'success': False,
            'message': 'The administrator for this server has not allowed users to perform actions.',
        }),
    })
    result = execute.run_action(['show', 'version'], None)
    assert result['success'] is False and result['job'] is None
    assert result['msg'] == (
        'The server refused the action: '
        'The administrator for this server has not allowed users to perform actions.'
    )


def test_execution_requests_unwrap_the_v5_shape(server):
    from meerschaum.api.dash import execute
    server({
        ('GET', '/execute/runs'): (200, {
            'success': True, 'message': 'Found 1 retained execution.',
            'runs': [{'name': 'execute-old', 'ended': '2026-01-01T00:00:00Z'}],
        }),
        ('POST', '/execute/validate'): (200, {'success': True, 'message': 'You may run this command.'}),
        ('POST', '/execute/runs/execute-old/retain'): (409, {
            'success': False, 'message': 'Wait until the execution finishes before promoting it.',
        }),
    })
    assert execute.execution_request('/execute/runs', None) == [
        {'name': 'execute-old', 'ended': '2026-01-01T00:00:00Z'}
    ]
    assert execute.execution_request(
        '/execute/validate', None, method='POST', body={'sysargs': ['show', 'version']},
    ) == [True, 'You may run this command.']
    with pytest.raises(ValueError, match='Wait until the execution finishes'):
        execute.execution_request('/execute/runs/execute-old/retain', None, method='POST')


def test_a_refused_validation_is_a_tuple_not_an_exception(server):
    from meerschaum.api.dash import execute
    server({
        ('POST', '/execute/validate'): (403, {
            'success': False, 'message': "The action 'python' cannot be run by non-administrators.",
        }),
    })
    assert execute.execution_request(
        '/execute/validate', None, method='POST', body={'sysargs': ['python']},
    ) == [
        False,
        "The server refused this command: "
        "The action 'python' cannot be run by non-administrators.",
    ]


def test_older_servers_shapes_still_read(server):
    from meerschaum.api.dash import execute
    server({
        ('POST', '/execute'): (200, [False, 'Not allowed.']),
        ('GET', '/execute/runs'): (200, [{'name': 'execute-x'}]),
    })
    result = execute.run_action(['show', 'version'], None)
    assert result['success'] is False and result['msg'] == 'Not allowed.'
    assert execute.execution_request('/execute/runs', None) == [{'name': 'execute-x'}]


def _route_groups(root, plugins, host_config, config_patch=None):
    """Which route groups the API host serves, in a fresh interpreter."""
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_API_HOST': '1',
        'MRSM_API_HOST_CONFIG': json.dumps(host_config),
        'PYTHONPATH': os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    })
    if config_patch:
        env['MRSM_CONFIG'] = json.dumps(config_patch)
    code = (
        "import json\n"
        "from meerschaum.api import app, route_group_is_allowed, public_preset\n"
        "paths = sorted({getattr(route, 'path', '') for route in app.routes})\n"
        "print('JSON:' + json.dumps({'public': public_preset, 'groups': {g: route_group_is_allowed(g) "
        "for g in ('dash', 'version', 'misc', 'pipes', 'actions')}, "
        "'docs': {'openapi': app.openapi_url, 'docs': app.docs_url, 'redoc': app.redoc_url, "
        "'routed': [p for p in paths if p in ('/openapi.json', '/docs', '/redoc')]}}))\n"
    )
    result = subprocess.run(
        [sys.executable, '-c', code], env=env, capture_output=True, text=True, timeout=300,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    line = [line for line in result.stdout.splitlines() if line.startswith('JSON:')][-1]
    return json.loads(line[len('JSON:'):])


def test_the_api_host_honors_the_public_preset(tmp_path):
    pytest.importorskip('fastapi')
    root = tmp_path / 'root'
    root.mkdir()
    plugins = tmp_path / 'plugins'
    plugins.mkdir()
    default = _route_groups(root, plugins, {})
    assert default['public'] is False
    assert default['groups']['dash'] is True
    assert default['docs']['openapi'] == '/openapi.json'
    assert default['docs']['routed'] == ['/docs', '/openapi.json', '/redoc']
    public = _route_groups(root, plugins, {'public': True})
    assert public['public'] is True
    ### The console's own pages are off; the public groups stay.
    assert public['groups'] == {
        'dash': False, 'version': True, 'misc': True, 'pipes': False, 'actions': False,
    }
    ### The host serves no API docs and no schema either (an encoded `/%64ocs` that reached it
    ### would otherwise answer).
    assert public['docs'] == {'openapi': None, 'docs': None, 'redoc': None, 'routed': []}
    ### The config key works too, and an explicit allowlist adds groups on top of the preset.
    patched = _route_groups(root, plugins, {}, {
        'api': {'permissions': {'preset': 'public', 'routes': {'allowlist': ['*', 'pipes']}}},
    })
    assert patched['public'] is True
    assert patched['groups']['pipes'] is True and patched['groups']['dash'] is False
    assert patched['docs']['routed'] == []
    ### Allowlisting `docs` brings the docs back under the preset.
    documented = _route_groups(root, plugins, {'public': True}, {
        'api': {'permissions': {'routes': {'allowlist': ['docs']}}},
    })
    assert documented['docs']['routed'] == ['/docs', '/openapi.json', '/redoc']
