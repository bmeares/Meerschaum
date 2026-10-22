#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The API host on a fresh install (transition bugs 3 and 4).

A user who runs `pip install meerschaum[api]` (or only `meerschaum`, letting Meerschaum install
what the host needs into `<root>/venvs/mrsm` on first use) must get a working `mrsm start api`
with their `@api_plugin` routes:

- bug 3: with uvicorn living only in the `mrsm` virtual environment, the host died in
  `uvicorn.run` with `ModuleNotFoundError: No module named 'h11'` because `attempt_import()`
  deactivated the environment before uvicorn imported its protocol modules lazily. The host now
  runs inside `Venv('mrsm')` for its whole life;
- bug 4: `meerschaum[api]` must list everything `meerschaum.api` and a plugin's routes need (uvicorn
  serves WebSocket routes only with `websockets` or `wsproto` installed). The scenarios below
  install exactly `meerschaum.utils.packages._packages.extras['api']` into a bare interpreter
  (no `fastapi-login`, no `python-jose`, no `aiofiles`) and run with `MRSM_NO_AUTO_INSTALL=1`, so a
  missing requirement fails the test instead of being installed behind its back.

The interpreter is a throwaway `venv` with no third-party packages; `PYTHONPATH` points at the
working tree (which holds the built extension), so the tree's code is what runs. The packages come
from the package index (the test is skipped, naming the reason, when there is none).
"""

import os
import sys
import json
import time
import hmac
import base64
import socket
import hashlib
import pathlib
import subprocess
import textwrap
import urllib.error
import urllib.request
import venv

import pytest

from tests._binary import skip_or_fail
from tests.api_host._helpers import REPO

pytestmark = pytest.mark.skipif(
    sys.platform == 'win32',
    reason="The host listens on a Unix socket.",
)

PLUGIN = '''
__version__ = '1.0.0'

from meerschaum.plugins import api_plugin

@api_plugin
def fresh_routes(app):
    ### As in the docs: the API imports live inside the function, so that importing the plugin
    ### (the worker does, before the host exists) never needs FastAPI.
    import fastapi
    from meerschaum.api import get_api_connector, manager
    from meerschaum.core import User

    ### A signed JWT authenticates an existing user, including in a fresh install.
    instance = get_api_connector()
    assert instance.register_user(User('bob', 'probe-password', instance=instance))[0]

    @app.get('/freshhost/ping')
    def ping():
        return {'pong': True}

    @app.websocket('/freshhost/ws')
    async def ws(websocket: fastapi.WebSocket):
        await websocket.accept()
        await websocket.send_text('hello')
        await websocket.close()

    @app.get('/freshhost/me')
    def me(curr_user=fastapi.Depends(manager)):
        return {'username': curr_user.username}
'''


DASH_PLUGIN = '''
__version__ = '1.0.0'

from meerschaum.plugins import dash_plugin, web_page

@dash_plugin
def init_dash(dash_app):
    from dash import Output
    from dash_extensions.enrich import Trigger

    @dash_app.callback(
        Output('freshdash-out', 'children'),
        Trigger('freshdash-button', 'n_clicks'),
    )
    def clicked():
        return 'clicked'

@web_page('freshdash/page', login_required=False)
def page():
    import dash_bootstrap_components as dbc
    from dash import html
    return html.Div([dbc.Button('Go', id='freshdash-button'), html.Div(id='freshdash-out')])
'''


def _api_requirements() -> list:
    from meerschaum.utils.packages._packages import extras
    return list(extras['api'])


def _dash_requirements() -> list:
    ### `meerschaum[dash]` alone is a working console: it brings the `api` extra (the host) and
    ### `pandas` (the pipe cards) with it.
    from meerschaum.utils.packages._packages import extras
    return list(extras['dash'])


def _python_of(env_dir: pathlib.Path) -> pathlib.Path:
    return env_dir / ('Scripts' if os.name == 'nt' else 'bin') / (
        'python.exe' if os.name == 'nt' else 'python'
    )


@pytest.fixture(scope='module')
def bare_python(tmp_path_factory) -> pathlib.Path:
    """An interpreter with pip and no other package."""
    env_dir = tmp_path_factory.mktemp('hostvenv') / 'bare'
    venv.EnvBuilder(with_pip=True, clear=True).create(env_dir)
    return _python_of(env_dir)


def _pip_install(python: pathlib.Path, requirements: list, target: pathlib.Path = None) -> None:
    command = [str(python), '-m', 'pip', 'install', '--quiet', '--disable-pip-version-check']
    if target is not None:
        command += ['--target', str(target)]
    result = subprocess.run(
        command + requirements, capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        skip_or_fail(f"Could not install the API extra from a package index:\n{result.stderr[-1500:]}")


def _scenario_env(
    tmp_path: pathlib.Path,
    plugin_name: str = 'fresh_host',
    plugin_source: str = None,
    **extra,
) -> dict:
    root = tmp_path / 'root'
    plugins = tmp_path / 'plugins'
    root.mkdir(exist_ok=True)
    plugins.mkdir(exist_ok=True)
    env = {
        key: val
        for key, val in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('PYTHONPATH', 'TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'PYTHONNOUSERSITE': '1',
        'PYTHONDONTWRITEBYTECODE': '1',
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_SQL_APIHOSTFRESH': f"sqlite:///{tmp_path / 'instance.db'}",
        'MRSM_CONFIG': json.dumps({'meerschaum': {'instance': 'sql:apihostfresh'}}),
        'HOME': str(tmp_path),
    })
    env.update(extra)
    plugin_dir = plugins / plugin_name
    plugin_dir.mkdir(exist_ok=True)
    (plugin_dir / '__init__.py').write_text(
        textwrap.dedent(plugin_source or PLUGIN), encoding='utf-8',
    )
    return env


def _mrsm_venv_site_packages(python: pathlib.Path, env: dict) -> pathlib.Path:
    """Where Meerschaum puts the `mrsm` environment's packages under this root."""
    result = subprocess.run(
        [
            str(python), '-c',
            "from meerschaum.utils.venv import venv_target_path; "
            "print(venv_target_path('mrsm', allow_nonexistent=True))",
        ],
        env=env, capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    return pathlib.Path(result.stdout.strip().splitlines()[-1])


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def _bearer(secret: bytes, sub: str) -> str:
    def _b64(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b'=').decode()
    signing_input = (
        _b64(json.dumps({'alg': 'HS256', 'typ': 'JWT'}).encode()) + '.'
        + _b64(json.dumps({'sub': sub, 'scopes': ['*'], 'exp': int(time.time()) + 300}).encode())
    )
    signature = hmac.new(secret, signing_input.encode(), hashlib.sha256).digest()
    return f'{signing_input}.{_b64(signature)}'


def _get(port: int, path: str, token: str = None, body: dict = None):
    """GET `path`, or POST `body` as JSON. Return `(status, text)`; `(None, '')` when unreachable."""
    request = urllib.request.Request(f'http://127.0.0.1:{port}{path}')
    if token:
        request.add_header('Authorization', f'Bearer {token}')
    if body is not None:
        request.data = json.dumps(body).encode()
        request.add_header('Content-Type', 'application/json')
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except (urllib.error.URLError, ConnectionError, TimeoutError):
        return None, ''


def _websocket_upgrade_status(port: int, path: str) -> str:
    """The status line of a raw WebSocket handshake (uvicorn needs `websockets` or `wsproto`)."""
    key = base64.b64encode(os.urandom(16)).decode()
    request = (
        f'GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\n'
        f'Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n'
    )
    with socket.create_connection(('127.0.0.1', port), timeout=10) as sock:
        sock.sendall(request.encode())
        return sock.recv(4096).decode('latin-1').split('\r\n', 1)[0]


def _probe_routes(port: int, tmp_path: pathlib.Path) -> dict:
    secret = (tmp_path / 'root' / 'api' / '.api_secret_key').read_bytes()
    return {
        'ping': _get(port, '/freshhost/ping'),
        'no_token': _get(port, '/freshhost/me')[0],
        'token': _get(port, '/freshhost/me', _bearer(secret, 'bob')),
        'websocket': _websocket_upgrade_status(port, '/freshhost/ws'),
    }


def _serve_and_probe(
    python: pathlib.Path,
    env: dict,
    tmp_path: pathlib.Path,
    ready_path: str = '/freshhost/ping',
    probe=_probe_routes,
) -> dict:
    """Run `mrsm start api` on a random port, wait for `ready_path` (200), then call `probe`."""
    port = _free_port()
    log_path = tmp_path / 'api.log'
    log = open(log_path, 'w+', encoding='utf-8')
    proc = subprocess.Popen(
        [
            str(python), '-m', 'meerschaum', 'start', 'api',
            '-i', 'sql:apihostfresh', '--host', '127.0.0.1', '--port', str(port),
        ],
        env=env,
        cwd=str(tmp_path),
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    try:
        deadline = time.time() + 240
        status = None
        while time.time() < deadline:
            if proc.poll() is not None:
                break
            status, _body = _get(port, ready_path)
            if status == 200:
                break
            time.sleep(0.5)
        if status != 200:
            log.seek(0)
            pytest.fail(
                f"{ready_path} answered {status} (the host never came up):\n{log.read()[-5000:]}"
            )
        return {**probe(port, tmp_path), 'log': log_path}
    finally:
        import signal
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait()
        log.close()


def _assert_probe(probe: dict) -> None:
    assert probe['ping'] == (200, '{"pong":true}')
    assert probe['no_token'] == 401
    assert probe['token'] == (200, '{"username":"bob"}')
    assert ' 101 ' in probe['websocket'], probe['websocket']


def test_api_extra_lists_what_the_host_imports():
    """`fastapi-login`, `python-jose` and `aiofiles` are in no extra; `PyJWT` is in `api`."""
    requirements = ' '.join(_api_requirements()).lower()
    for gone in ('fastapi-login', 'python-jose', 'aiofiles'):
        assert gone not in requirements
    for needed in ('fastapi', 'uvicorn', 'a2wsgi', 'python-multipart', 'pyjwt', 'websockets'):
        assert needed in requirements


def test_host_installs_what_it_needs_on_its_first_start(bare_python, tmp_path):
    """
    Bug 3 as users met it: only the Meerschaum wheel is installed, `MRSM_NO_AUTO_INSTALL` is unset,
    and the host installs FastAPI, uvicorn and the rest into `<root>/venvs/mrsm` while starting.
    """
    ### A throwaway install doubles as the "is there a package index" probe.
    _pip_install(bare_python, ['uvicorn'], target=tmp_path / 'probe')
    env = _scenario_env(tmp_path)
    probe = _serve_and_probe(bare_python, env, tmp_path)
    _assert_probe(probe)
    log = probe['log'].read_text(encoding='utf-8', errors='replace')
    assert 'ModuleNotFoundError' not in log and 'Traceback' not in log, log[-3000:]
    ### Only what the `api` extra lists was installed: not `fastapi-login`, `python-jose`, `aiofiles`.
    installed = ' '.join(
        p.name.lower()
        for p in _mrsm_venv_site_packages(bare_python, env).iterdir()
    )
    for gone in ('fastapi_login', 'jose', 'aiofiles'):
        assert gone not in installed


def test_host_serves_plugin_routes_when_uvicorn_is_only_in_the_mrsm_venv(bare_python, tmp_path):
    """Bug 3: the extra's packages live in `<root>/venvs/mrsm` only, as after an auto-install."""
    env = _scenario_env(tmp_path, MRSM_NO_AUTO_INSTALL='1')
    target = _mrsm_venv_site_packages(bare_python, env)
    target.mkdir(parents=True, exist_ok=True)
    _pip_install(bare_python, _api_requirements(), target=target)
    _assert_probe(_serve_and_probe(bare_python, env, tmp_path))


def test_host_serves_plugin_routes_with_the_api_extra_in_the_base_environment(
    bare_python, tmp_path,
):
    """Bug 4: `pip install meerschaum[api]` (no `mrsm` venv packages, no auto-install)."""
    base_env = tmp_path / 'base-env'
    venv.EnvBuilder(with_pip=True, clear=True).create(base_env)
    python = _python_of(base_env)
    _pip_install(python, _api_requirements())
    env = _scenario_env(tmp_path, MRSM_NO_AUTO_INSTALL='1')
    _assert_probe(_serve_and_probe(python, env, tmp_path))


def _run_host_module(python: pathlib.Path, env: dict, tmp_path: pathlib.Path):
    return subprocess.run(
        [str(python), '-m', 'meerschaum.api.host', '--socket', str(tmp_path / 'h.sock')],
        env=env, capture_output=True, text=True, timeout=240, cwd=str(tmp_path),
    )


def test_missing_api_extra_is_named_when_auto_install_is_off(bare_python, tmp_path):
    env = _scenario_env(tmp_path, MRSM_NO_AUTO_INSTALL='1')
    result = _run_host_module(bare_python, env, tmp_path)
    assert result.returncode != 0
    assert 'ImportError' in result.stderr
    assert 'pip install "meerschaum[api]"' in result.stderr
    assert 'Meerschaum is installing' not in result.stderr


def test_missing_uvicorn_is_named_when_auto_install_is_off(tmp_path):
    """Everything but uvicorn is installed: the host says which extra provides it."""
    base_env = tmp_path / 'no-uvicorn-env'
    venv.EnvBuilder(with_pip=True, clear=True).create(base_env)
    python = _python_of(base_env)
    _pip_install(python, [r for r in _api_requirements() if not r.lower().startswith('uvicorn')])
    env = _scenario_env(tmp_path, MRSM_NO_AUTO_INSTALL='1')
    result = _run_host_module(python, env, tmp_path)
    assert result.returncode != 0
    assert 'The API host needs uvicorn' in result.stderr
    assert 'pip install "meerschaum[api]"' in result.stderr


def _dash_outputs(output: str):
    """The `outputs` field Dash's client sends: a dict for one output, a list for several."""
    if not output.startswith('..'):
        component_id, _, prop = output.partition('.')
        return {'id': component_id, 'property': prop}
    return [
        {'id': part.partition('.')[0], 'property': part.partition('.')[2]}
        for part in output.strip('.').split('...')
    ]


def _probe_dash(port: int, tmp_path: pathlib.Path) -> dict:
    status, index = _get(port, '/dash/')
    _status, dependencies = _get(port, '/dash/_dash-dependencies')
    dependencies = json.loads(dependencies)

    def _fire(output_id: str, inputs: list, state: list = None):
        dependency = next(d for d in dependencies if output_id in d['output'])
        payload = {
            'output': dependency['output'],
            'outputs': _dash_outputs(dependency['output']),
            'inputs': inputs,
            'changedPropIds': [f"{i['id']}.{i['property']}" for i in inputs][:1],
            'state': state or [],
        }
        status, text = _get(port, '/dash/_dash-update-component', body=payload)
        return status, (json.loads(text)['response'] if status == 200 else text)

    return {
        'index': (status, 'mrsm-theme-dark' in index),
        'page': _fire(
            'page-layout-div',
            [
                {'id': 'mrsm-location', 'property': 'pathname', 'value': '/dash/freshdash/page'},
                {'id': 'session-store', 'property': 'data', 'value': {}},
            ],
            [{'id': 'mrsm-location', 'property': 'href',
              'value': 'http://localhost/dash/freshdash/page'}],
        ),
        'callback': _fire(
            'freshdash-out',
            [{'id': 'freshdash-button', 'property': 'n_clicks', 'value': 1}],
        ),
    }


def test_plugin_dash_app_works_with_the_dash_extra_only(tmp_path):
    """
    `pip install meerschaum[dash]` and `MRSM_NO_AUTO_INSTALL=1`: the web console renders, and a
    plugin's Dash page (a `@dash_plugin` callback using `dash_extensions.enrich.Trigger`, a
    `@web_page` layout built with `dash_bootstrap_components`) is a page of it whose callback fires
    through `mrsm start api`. The `dash` extra must carry everything the console imports, the
    `api` extra (the host) included: the console builds the app with `DashProxy` and `dash-ace`
    editors, and v4 plugin layouts use `dash_bootstrap_components`.
    """
    base_env = tmp_path / 'dash-env'
    venv.EnvBuilder(with_pip=True, clear=True).create(base_env)
    python = _python_of(base_env)
    _pip_install(python, _dash_requirements())
    env = _scenario_env(
        tmp_path, plugin_name='freshdash', plugin_source=DASH_PLUGIN, MRSM_NO_AUTO_INSTALL='1',
    )
    probe = _serve_and_probe(
        python, env, tmp_path, ready_path='/dash/', probe=_probe_dash,
    )
    assert probe['index'] == (200, True)
    status, page = probe['page']
    assert status == 200, page
    page_json = json.dumps(page['page-layout-div'])
    assert 'freshdash-button' in page_json and 'Button' in page_json
    assert probe['callback'] == (200, {'freshdash-out': {'children': 'clicked'}})
    log = probe['log'].read_text(encoding='utf-8', errors='replace')
    assert 'ModuleNotFoundError' not in log and 'Traceback' not in log, log[-3000:]
