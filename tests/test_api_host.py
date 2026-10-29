#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test the Python API host (`python -m meerschaum.api.host`, v5 design §7.3, §8.4, §7.2).

Host mode must be set before `meerschaum.api` is imported, so the host runs in a subprocess (a
temporary root with the synthetic plugin `crates/meerschaum-server/tests/host_plugin/hostdemo`)
and is reached over its Unix socket. JWTs are minted the way the Rust server mints them (HS256
over the shared secret file), so a token from either side validates on the other.

Skipped when the host's dependencies (`fastapi`, `uvicorn`, `dash`, `a2wsgi`, `PyJWT`) are not
importable. The end-to-end test through the Rust server is
`crates/meerschaum-server/tests/host_e2e.rs`.
"""

import os
import sys
import json
import time
import hmac
import base64
import shutil
import hashlib
import pathlib
import tempfile
import subprocess

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
PLUGINS_DIR = REPO / 'crates' / 'meerschaum-server' / 'tests' / 'host_plugin'
INTERNAL_TOKEN_HEADER = 'x-mrsm-host-token'


def _env(root: pathlib.Path) -> dict:
    env = {
        key: val
        for key, val in os.environ.items()
        if not key.startswith('MRSM_') and key != 'PYTHONPATH'
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(PLUGINS_DIR),
        'MRSM_NO_AUTO_INSTALL': '1',
        'PYTHONDONTWRITEBYTECODE': '1',
        'MRSM_API_HOST_CONFIG': json.dumps({'mrsm_instance': 'sql:local'}),
    })
    return env


def _python(root: pathlib.Path, code: str, host_mode: bool = True) -> subprocess.CompletedProcess:
    env = _env(root)
    if host_mode:
        env['MRSM_API_HOST'] = '1'
    return subprocess.run(
        [sys.executable, '-c', code],
        env=env,
        cwd=str(root),
        capture_output=True,
        text=True,
        timeout=180,
    )


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode()


def mint_jwt(secret: bytes, claims: dict) -> str:
    """An HS256 JWT, as the Rust server's `jwt_encode` produces it."""
    header = _b64(json.dumps({'alg': 'HS256', 'typ': 'JWT'}, separators=(',', ':')).encode())
    payload = _b64(json.dumps(claims, separators=(',', ':')).encode())
    signing_input = f'{header}.{payload}'.encode()
    signature = hmac.new(secret, signing_input, hashlib.sha256).digest()
    return f'{header}.{payload}.{_b64(signature)}'


@pytest.fixture(scope='module')
def root():
    path = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-host-'))
    check = _python(
        path,
        "import meerschaum.utils.venv as v; v.activate_venv('mrsm'); "
        "import fastapi, uvicorn, dash, dash_bootstrap_components, dash_ace, a2wsgi, jwt",
    )
    if check.returncode != 0:
        shutil.rmtree(path, ignore_errors=True)
        pytest.skip(f"The API host's dependencies are missing:\n{check.stderr[-2000:]}")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope='module')
def host(root):
    """Serve the host on a Unix socket and yield an `httpx` client over it."""
    httpx = pytest.importorskip('httpx')
    ### A valid user JWT must name a principal in this host's own instance.
    setup = _python(root, """
from meerschaum import get_connector
from meerschaum.core import User
instance = get_connector('sql:local')
assert instance.register_user(User('admin', 'probe-password', instance=instance))[0]
""")
    assert setup.returncode == 0, setup.stderr[-4000:]
    sock_dir = pathlib.Path(tempfile.mkdtemp(prefix='mh'))
    if len(str(sock_dir)) > 90:
        shutil.rmtree(sock_dir, ignore_errors=True)
        sock_dir = pathlib.Path(tempfile.mkdtemp(prefix='mh', dir='/tmp'))
    socket_path = sock_dir / 'host.sock'
    log = open(root / 'host.log', 'w+', encoding='utf-8')
    proc = subprocess.Popen(
        [sys.executable, '-m', 'meerschaum.api.host', '--socket', str(socket_path)],
        env=_env(root),
        cwd=str(root),
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    try:
        deadline = time.time() + 120
        client = httpx.Client(
            transport=httpx.HTTPTransport(uds=str(socket_path)),
            base_url='http://localhost',
            timeout=30,
        )
        while True:
            if proc.poll() is not None:
                log.seek(0)
                pytest.fail(f"The API host exited:\n{log.read()[-4000:]}")
            if socket_path.exists():
                try:
                    client.get('/hostdemo/hello')
                    break
                except httpx.TransportError:
                    pass
            if time.time() > deadline:
                pytest.fail("The API host did not start in time.")
            time.sleep(0.2)
        secret = (root / 'api' / '.api_secret_key').read_bytes()
        yield client, secret
        client.close()
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        log.close()
        shutil.rmtree(sock_dir, ignore_errors=True)


def internal_token(secret: bytes, scopes, sub=None) -> str:
    claims = {'scopes': list(scopes), 'mrsm_host': 'mcp', 'exp': int(time.time()) + 300}
    if sub is not None:
        claims['sub'] = sub
    return mint_jwt(secret, claims)


def test_api_plugin_returns_function_unchanged():
    from meerschaum.plugins import api_plugin, _api_plugins

    def init(app):
        return app

    try:
        assert api_plugin(init) is init
        assert init('app') == 'app'
    finally:
        for functions in _api_plugins.values():
            if init in functions:
                functions.remove(init)


def test_plugin_registry_excludes_builtins():
    from meerschaum.mcp import tool, get_tools
    from meerschaum.mcp._registry import get_plugin_registry, is_builtin, _TOOLS

    def _handler(x):
        return {'x': x}

    try:
        tool(
            'test_api_host_tool', 'A test tool.', scopes=['test:read'], read_only=True,
            input_schema={'type': 'object', 'properties': {'x': {'type': 'integer'}}},
        )(_handler)
        registry = get_plugin_registry()
        names = [t['name'] for t in registry['tools']]
        assert names == ['test_api_host_tool']
        entry = registry['tools'][0]
        assert entry['scopes'] == ['test:read']
        assert entry['read_only'] is True
        assert entry['input_schema']['properties']['x'] == {'type': 'integer'}
        assert all(is_builtin(t) for name, t in get_tools().items() if name != 'test_api_host_tool')
        assert registry['resources'] == [] and registry['prompts'] == []
    finally:
        _TOOLS.pop('test_api_host_tool', None)


def test_host_mode_surface(root):
    """`meerschaum.api` keeps its surface in host mode, without the core routes."""
    code = """
import json
import meerschaum.api as api
from meerschaum.api import (
    app, endpoints, get_api_connector, get_cache_connector, production, debug,
    ScopedAuth, CurrentScopes, get_pipe, no_auth,
)
from meerschaum.api._oauth2 import SECRET
from meerschaum.api._tokens import get_token_from_authorization
import meerschaum.api.models
from meerschaum.api.host import build_app
from meerschaum.api.host.dash import plugin_path, redirect_target, console_status
built = build_app()
import meerschaum.api.dash as api_dash
from meerschaum.api.dash.sessions import (
    set_session, get_user_from_session, get_username_from_session, delete_session,
    is_session_active, session_is_admin, EXPIRES_SECONDS, set_session_cookie,
)
paths = sorted({getattr(r, 'path', '') for r in built.routes})
print(json.dumps({
    'host_mode': api._host_mode,
    'same_app': built is app,
    'paths': paths,
    'secret_len': len(SECRET),
    'instance': str(get_api_connector()),
    'console_built': api_dash.dash_app is not None,
    'console_status': console_status(),
    'plugin_paths': [
        plugin_path('demo', '/dash/demo/page'),
        plugin_path('demo', 'dash/other'),
        plugin_path('demo', '/dash/demo'),
        plugin_path('demo', '/dash/demonstration'),
    ],
    'redirects': [
        redirect_target('/dash/hostdemo/page'),
        redirect_target('/dash/hostdemo/pipes'),
        redirect_target('/dash/hostdemo/pipes', 'a=1'),
        redirect_target('/dash/pipes/demo/child'),
        redirect_target('/dash/unknownplugin/pipes'),
        redirect_target('/dash/hostdemo/nothing-like-it'),
    ],
}))
"""
    result = _python(root, code)
    assert result.returncode == 0, result.stderr[-4000:]
    out = json.loads(result.stdout.strip().splitlines()[-1])
    assert out['host_mode'] is True
    assert out['same_app'] is True
    assert out['secret_len'] > 0
    assert out['instance'] == 'sql:local'
    ### One Dash app is the console, mounted at `/dash`.
    assert out['console_built'] is True
    assert out['console_status']['mounted'] is True
    assert out['console_status']['reason'] is None
    assert out['console_status']['dash_version']
    assert '/hostdemo/hello' in out['paths']
    assert '/dash' in out['paths']
    assert '/_mrsm/host/registry' in out['paths'] and '/_mrsm/host/status' in out['paths']
    for core in ('/pipes/keys', '/login', '/mcp', '/version'):
        assert core not in out['paths'], core
    assert out['plugin_paths'] == [
        '/dash/demo/page', '/dash/demo/other', '/dash/demo', '/dash/demo/demonstration',
    ]
    ### rc1's per-plugin paths redirect to the v4 paths (the plugin's own pages keep theirs).
    assert out['redirects'] == [
        None, '/dash/pipes', '/dash/pipes?a=1', None, None, None,
    ]


def test_host_serves_plugin_routes(host):
    client, _ = host
    response = client.get('/hostdemo/hello')
    assert response.status_code == 200
    assert response.json() == {'message': 'Hello from hostdemo!'}
    assert response.headers['x-hostdemo'] == 'middleware'

    response = client.patch(
        '/hostdemo/echo?a=1', content=b'body', headers={'x-demo': 'y', 'cookie': 'flavor=mint'},
    )
    assert response.json() == {
        'method': 'PATCH', 'path': '/hostdemo/echo', 'query': 'a=1',
        'cookie': 'mint', 'header': 'y', 'body': 'body',
    }
    assert client.get('/hostdemo/static/hello.txt').text == 'hello from a static mount\n'
    assert client.get('/hostdemo/stream').text == 'chunk 0\nchunk 1\nchunk 2\n'


def test_jwts_from_the_server_validate_in_the_host(host):
    client, secret = host
    claims = {
        'sub': 'admin', 'scopes': ['hostdemo:read'], 'type': 'user',
        'exp': int(time.time()) + 300,
    }
    token = mint_jwt(secret, claims)
    response = client.get('/hostdemo/scoped', headers={'Authorization': f'Bearer {token}'})
    assert response.status_code == 200, response.text
    assert response.json() == {'scopes': ['hostdemo:read']}
    response = client.get('/hostdemo/whoami', headers={'Authorization': f'Bearer {token}'})
    assert response.json() == {'username': 'admin'}

    no_scope = mint_jwt(secret, {**claims, 'scopes': ['pipes:read']})
    response = client.get('/hostdemo/scoped', headers={'Authorization': f'Bearer {no_scope}'})
    assert response.status_code == 403

    forged = mint_jwt(b'not-the-secret', claims)
    response = client.get('/hostdemo/scoped', headers={'Authorization': f'Bearer {forged}'})
    assert response.status_code == 401


def test_internal_routes_require_the_host_token(host):
    client, secret = host
    assert client.get('/_mrsm/host/registry').status_code == 403
    bad = mint_jwt(b'wrong', {'mrsm_host': 'mcp', 'exp': int(time.time()) + 60})
    assert client.get('/_mrsm/host/registry', headers={INTERNAL_TOKEN_HEADER: bad}).status_code == 403
    # A user's JWT (no `mrsm_host` claim) is not an internal token.
    user = mint_jwt(secret, {'sub': 'admin', 'scopes': ['*'], 'exp': int(time.time()) + 60})
    assert client.get('/_mrsm/host/registry', headers={INTERNAL_TOKEN_HEADER: user}).status_code == 403
    response = client.post(
        '/_mrsm/host/mcp', json={'jsonrpc': '2.0', 'id': 1, 'method': 'tools/list'},
    )
    assert response.status_code == 403


def test_registry_lists_only_plugin_entries(host):
    client, secret = host
    response = client.get(
        '/_mrsm/host/registry', headers={INTERNAL_TOKEN_HEADER: internal_token(secret, [])},
    )
    assert response.status_code == 200
    registry = response.json()
    assert sorted(t['name'] for t in registry['tools']) == ['hostdemo_add', 'hostdemo_reset']
    add = next(t for t in registry['tools'] if t['name'] == 'hostdemo_add')
    assert add['scopes'] == ['hostdemo:read']
    assert add['output_schema'] == {'type': 'object', 'properties': {'sum': {'type': 'number'}}}
    assert [r['uri'] for r in registry['resources']] == ['hostdemo://readme']
    assert [p['name'] for p in registry['prompts']] == ['hostdemo_greet']
    assert '/hostdemo/hello' in registry['routes']
    assert not any(route.startswith('/_mrsm/') for route in registry['routes'])


def test_mcp_calls_run_with_the_forwarded_scopes(host):
    client, secret = host

    def call(scopes, message):
        return client.post(
            '/_mrsm/host/mcp',
            json=message,
            headers={INTERNAL_TOKEN_HEADER: internal_token(secret, scopes, sub='admin')},
        )

    add = {
        'jsonrpc': '2.0', 'id': 7, 'method': 'tools/call',
        'params': {'name': 'hostdemo_add', 'arguments': {'a': 2, 'b': 40}},
    }
    response = call(['hostdemo:read'], add)
    assert response.status_code == 200
    result = response.json()
    assert result['id'] == 7
    assert result['result']['structuredContent'] == {'sum': 42}

    response = call(['pipes:read'], add)
    assert response.json()['result']['isError'] is True
    assert 'Missing required scope' in response.json()['result']['content'][0]['text']

    response = call(['hostdemo:read'], {
        'jsonrpc': '2.0', 'id': 8, 'method': 'resources/read',
        'params': {'uri': 'hostdemo://readme'},
    })
    assert response.json()['result']['contents'][0]['text'] == 'Hostdemo is a synthetic plugin.'

    response = call(['hostdemo:read'], {'jsonrpc': '2.0', 'method': 'notifications/initialized'})
    assert response.status_code == 202


def _dependencies(client):
    return client.get('/dash/_dash-dependencies').json()


def _route(client, pathname):
    """
    Fire the console's page-router callback. It is found through `_dash-dependencies`, because the
    output id differs with the app class (`dash_extensions` multiplexes the stores).
    """
    dependency = next(d for d in _dependencies(client) if 'page-layout-div' in d['output'])
    outputs = []
    for part in dependency['output'].strip('.').split('...'):
        component_id, _, prop = part.partition('.')
        outputs.append({'id': component_id, 'property': prop})
    payload = {
        'output': dependency['output'],
        'outputs': outputs,
        'inputs': [
            {'id': 'mrsm-location', 'property': 'pathname', 'value': pathname},
            {'id': 'session-store', 'property': 'data', 'value': {}},
        ],
        'changedPropIds': ['mrsm-location.pathname'],
        'state': [{'id': 'mrsm-location', 'property': 'href', 'value': 'http://localhost' + pathname}],
    }
    response = client.post('/dash/_dash-update-component', json=payload)
    assert response.status_code == 200, response.text
    return response.json()['response']


def test_the_console_is_one_dash_app(host):
    client, _ = host
    html = client.get('/dash/').text
    assert 'id="mrsm-theme-dark"' in html and 'id="mrsm-theme-light"' in html
    assert '/dash/_dash-component-suites/' in html and 'Meerschaum Web' in html
    layout = client.get('/dash/_dash-layout').text
    for component_id in ('session-store', 'mrsm-location', 'dbc-dark-store', 'page-layout-div'):
        assert f'"{component_id}"' in layout
    ### The top navigation replaced v4's logo menu (each page carries it).
    assert '"pages-offcanvas"' not in layout
    ### v4's assets are served by Dash itself; a plugin's `assets/` directory is served beside them
    ### and its stylesheet is loaded by every page.
    assert client.get('/dash/assets/ansi_up.js').status_code == 200
    assert '/dash/_plugin-assets/hostdemo/hostdemo.css' in html
    assert client.get('/dash/_plugin-assets/hostdemo/hostdemo.css').status_code == 200
    assert client.get('/dash/_plugin-assets/hostdemo/missing.css').status_code == 404
    assert client.get('/dash/_plugin-assets/nosuchplugin/hostdemo.css').status_code == 404
    ### The plugin's `@dash_plugin` callback is registered on the core app.
    assert any('hostdemo-session.children' in d['output'] for d in _dependencies(client))

    ### A plugin's `@web_page` is a page of the console at its v4 path.
    response = _route(client, '/dash/hostdemo/page')
    page = json.dumps(response['page-layout-div'])
    assert 'Hostdemo' in page and 'hostdemo-session' in page
    assert 'Page not found' not in page
    ### An unknown path is the console's dashboard, not a plugin page (v4's router).
    assert 'Hostdemo' not in json.dumps(_route(client, '/dash/nope'))


def test_rc1_plugin_paths_redirect(host):
    client, _ = host
    response = client.get('/dash/hostdemo/pipes?a=1', follow_redirects=False)
    assert response.status_code == 307
    assert response.headers['location'] == '/dash/pipes?a=1'
    ### The plugin's own page keeps its path.
    assert client.get('/dash/hostdemo/page', follow_redirects=False).status_code == 200


def test_host_status_route(host):
    client, secret = host
    assert client.get('/_mrsm/host/status').status_code == 403
    response = client.get(
        '/_mrsm/host/status', headers={INTERNAL_TOKEN_HEADER: internal_token(secret, [])},
    )
    assert response.status_code == 200
    body = response.json()
    assert body['console']['mounted'] is True and body['console']['reason'] is None
    assert body['console']['dash_version'] and body['version']
    registry = client.get(
        '/_mrsm/host/registry', headers={INTERNAL_TOKEN_HEADER: internal_token(secret, [])},
    ).json()
    assert registry['console'] == body['console']
