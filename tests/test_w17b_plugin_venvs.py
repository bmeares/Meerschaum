#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Plugin code sees the shared `mrsm` virtual environment, as it did in v4 (wave 17b).

In v4 a default `pip install meerschaum` has no dependencies: pandas and friends are installed on
first use INTO `<root>/venvs/mrsm`, and every process that ran plugin code had put that venv on
`sys.path` long before the first plugin was imported. A plugin with its own `required` venv could
therefore still `import pandas`. The order v4 produced (measured with the released 4.1.4 from a
fresh root; see `meerschaum.utils.venv.ensure_default_venvs`) is

    <plugin venv(s)>, <base interpreter site-packages>, <root>/venvs/mrsm, <the rest>

so the plugin's version of a package wins over the `mrsm` venv's. The v5 worker (and the API host)
reproduce it at import time, in `fetch`, in actions, in sync hooks and in
`@api_plugin` routes.

No network: the packages are tiny pure-Python wheels built here and installed with
`--no-index --find-links`; the `mrsm` venv holds `vsynth_mrsm` and `vsynth_shared 1.0`, the plugin
venv holds `vsynth_b` and `vsynth_shared 2.0`. Every test runs in a temporary root, never in
`~/.config/meerschaum`.

Needs no `uv` and no network (`python -m venv` + `pip install --no-index`); in CI a venv that
cannot be built fails the test.

Run with:
    PYTHONPATH=$(pwd) MRSM_NO_AUTO_INSTALL=1 python -m pytest tests/test_w17b_plugin_venvs.py -p no:cacheprovider
"""

import base64
import hashlib
import json
import os
import pathlib
import signal
import socket
import subprocess
import sys
import time
import urllib.request
import zipfile

import pytest

from tests._binary import in_ci

REPO = pathlib.Path(__file__).resolve().parent.parent

pytestmark = pytest.mark.skipif(
    sys.platform == 'win32',
    reason="The `bin/python` layout and Unix process groups are assumed",
)

PLUGIN_SOURCE = '''
__version__ = '0.0.1'
required = {required!r}
import json, os, sys
from meerschaum.plugins import make_action, pre_sync_hook, post_sync_hook, api_plugin

NAMES = {names!r}
ROOT = os.environ['MRSM_ROOT_DIR']


def _rec(label):
    info = {{}}
    for name in NAMES:
        try:
            module = __import__(name)
            info[name] = [module.VERSION, module.__file__.replace(ROOT, '<root>')]
        except ImportError:
            info[name] = None
    with open(os.environ['VREC'], 'a') as f:
        f.write(json.dumps({{
            'plugin': {plugin!r},
            'label': label,
            'sys_path': [p.replace(ROOT, '<root>') for p in sys.path],
            'info': info,
        }}) + '\\n')


_rec('import')


def register(pipe, **kw):
    _rec('register')
    return {{'columns': {{'datetime': 'dt'}}}}


def fetch(pipe, **kw):
    _rec('fetch')
    import datetime
    return [{{'dt': datetime.datetime(2024, 1, 1), 'v': 1}}]


@make_action
def {action}(**kw):
    _rec('action')
    return True, 'ok'


@pre_sync_hook
def {plugin}_pre(pipe, **kw):
    _rec('pre_hook')


@post_sync_hook
def {plugin}_post(pipe, **kw):
    _rec('post_hook')


@api_plugin
def {plugin}_api(app):
    _rec('api_init')

    @app.get('/{plugin}/x')
    def x():
        _rec('api_route')
        return {{'ok': True}}
'''


def _make_wheel(out: pathlib.Path, name: str, version: str):
    dist = f'{name}-{version}'
    files = {
        f'{name}/__init__.py': f'VERSION = {version!r}\n',
        f'{dist}.dist-info/METADATA': f'Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n',
        f'{dist}.dist-info/WHEEL': (
            'Wheel-Version: 1.0\nGenerator: w17bv\nRoot-Is-Purelib: true\nTag: py3-none-any\n'
        ),
    }
    record = []
    for path, text in files.items():
        digest = base64.urlsafe_b64encode(
            hashlib.sha256(text.encode()).digest()
        ).rstrip(b'=').decode()
        record.append(f'{path},sha256={digest},{len(text.encode())}')
    record.append(f'{dist}.dist-info/RECORD,,')
    files[f'{dist}.dist-info/RECORD'] = '\n'.join(record) + '\n'
    with zipfile.ZipFile(out / f'{dist}-py3-none-any.whl', 'w') as z:
        for path, text in files.items():
            z.writestr(path, text)


def _venv_with(root: pathlib.Path, venv: str, wheels: pathlib.Path, *requirements: str):
    """
    Create `<root>/venvs/<venv>` with `python -m venv` (no `uv` needed: CI has none) and install
    the synthetic wheels with `pip install --no-index`. Fails in CI, skips elsewhere, when the
    environment cannot be built.
    """
    path = root / 'venvs' / venv
    try:
        subprocess.run(
            [sys.executable, '-m', 'venv', str(path)],
            capture_output=True, text=True, timeout=300, check=True,
        )
        subprocess.run(
            [
                str(path / 'bin' / 'python'), '-m', 'pip', 'install', '--quiet',
                '--disable-pip-version-check', '--no-index', '--find-links', str(wheels),
                *requirements,
            ],
            capture_output=True, text=True, timeout=300, check=True,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as e:
        message = f"Could not build the throwaway virtual environment '{venv}': {e}"
        if in_ci():
            pytest.fail(message)
        pytest.skip(message)


@pytest.fixture(scope='module')
def sandbox(tmp_path_factory):
    """
    A temporary root whose `mrsm` venv holds `vsynth_mrsm` and `vsynth_shared 1.0` and whose
    `vplug_req` venv holds `vsynth_b` and `vsynth_shared 2.0`, with two plugins that record what
    they can import: `vplug_req` (own `required`) and `vplug_plain` (none).
    """
    base = tmp_path_factory.mktemp('w17bv')
    root = base / 'root'
    plugins = base / 'plugins'
    wheels = base / 'wheels'
    for path in (root, plugins, wheels, root / 'venvs'):
        path.mkdir(parents=True, exist_ok=True)
    for name, version in (
        ('vsynth_mrsm', '1.0'), ('vsynth_b', '1.0'),
        ('vsynth_shared', '1.0'), ('vsynth_shared', '2.0'),
    ):
        _make_wheel(wheels, name, version)
    _venv_with(root, 'mrsm', wheels, 'vsynth_mrsm', 'vsynth_shared==1.0')
    _venv_with(root, 'vplug_req', wheels, 'vsynth_b', 'vsynth_shared==2.0')
    (plugins / 'vplug_req.py').write_text(PLUGIN_SOURCE.format(
        plugin='vplug_req',
        action='vplugreq',
        required=['vsynth_b', 'vsynth_shared==2.0'],
        names=('vsynth_mrsm', 'vsynth_b', 'vsynth_shared'),
    ))
    (plugins / 'vplug_plain.py').write_text(PLUGIN_SOURCE.format(
        plugin='vplug_plain',
        action='vplugplain',
        required=[],
        names=('vsynth_mrsm', 'vsynth_b'),
    ))
    return {'base': base, 'root': root, 'plugins': plugins}


def _env(sandbox, record: pathlib.Path) -> dict:
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('PYTHONPATH', 'TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(sandbox['root']),
        'MRSM_PLUGINS_DIR': str(sandbox['plugins']),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_SQL_W17BVT': f"sqlite:///{sandbox['base'] / 'instance.db'}",
        'MRSM_CONFIG': json.dumps({'meerschaum': {'instance': 'sql:w17bvt'}}),
        'PYTHONDONTWRITEBYTECODE': '1',
        'VREC': str(record),
    })
    return env


def _mrsm(sandbox, record, *args, timeout=300):
    return subprocess.run(
        [sys.executable, '-m', 'meerschaum', *args, '-i', 'sql:w17bvt'],
        env=_env(sandbox, record),
        cwd=str(sandbox['base']),
        capture_output=True, text=True, timeout=timeout,
    )


def _read(record: pathlib.Path):
    if not record.exists():
        return []
    return [json.loads(line) for line in record.read_text().splitlines() if line.strip()]


@pytest.fixture(scope='module')
def cli_records(sandbox):
    """
    Register and sync a `plugin:vplug_req` pipe, then run both plugins' actions, each through the
    CLI (the Rust host and its Python workers). Returns the records the plugins wrote.
    """
    record = sandbox['base'] / 'cli.rec'
    keys = ['-c', 'plugin:vplug_req', '-m', 'x', '-l', 'y']
    for args in (
        ('register', 'pipes', *keys, '--yes'),
        ('sync', 'pipes', *keys, '--yes'),
        ('vplugreq',),
        ('vplugplain',),
    ):
        result = _mrsm(sandbox, record, *args)
        assert result.returncode == 0, result.stdout + result.stderr
    return _read(record)


def _find(records, plugin, label, first=False):
    found = [r for r in records if r['plugin'] == plugin and r['label'] == label]
    assert found, f"no '{label}' record from {plugin}: {[(r['plugin'], r['label']) for r in records]}"
    return found[0] if first else found[-1]


def _version(record, name):
    entry = record['info'][name]
    return entry[0] if entry else None


def _index(record, fragment):
    for i, path in enumerate(record['sys_path']):
        if fragment in path:
            return i
    return None


def test_import_time_sees_the_mrsm_venv(cli_records):
    ### The plugin with its own venv and the plugin without one both import a package that lives
    ### only in the `mrsm` venv.
    assert _version(_find(cli_records, 'vplug_req', 'import'), 'vsynth_mrsm') == '1.0'
    assert _version(_find(cli_records, 'vplug_plain', 'import'), 'vsynth_mrsm') == '1.0'
    ### ... and the plugin's own packages are still found in its own venv.
    own = _find(cli_records, 'vplug_req', 'import')
    assert _version(own, 'vsynth_b') == '1.0'
    assert '/venvs/vplug_req/' in own['info']['vsynth_b'][1]


def test_fetch_sees_the_mrsm_venv(cli_records):
    record = _find(cli_records, 'vplug_req', 'fetch')
    assert _version(record, 'vsynth_mrsm') == '1.0'
    assert _version(record, 'vsynth_b') == '1.0'


def test_action_sees_the_mrsm_venv(cli_records):
    for plugin in ('vplug_req', 'vplug_plain'):
        record = _find(cli_records, plugin, 'action')
        assert _version(record, 'vsynth_mrsm') == '1.0', plugin


def test_sync_hooks_see_the_mrsm_venv(cli_records):
    for plugin in ('vplug_req', 'vplug_plain'):
        for label in ('pre_hook', 'post_hook'):
            assert _version(_find(cli_records, plugin, label), 'vsynth_mrsm') == '1.0', (plugin, label)


def test_the_plugin_venv_wins_over_the_mrsm_venv(cli_records):
    ### `vsynth_shared` is 1.0 in the `mrsm` venv and 2.0 in the plugin's venv: v4 puts the
    ### plugin's venv ahead (and so does v5), at every call site.
    for label in ('import', 'fetch', 'action', 'pre_hook', 'post_hook'):
        record = _find(cli_records, 'vplug_req', label)
        assert _version(record, 'vsynth_shared') == '2.0', label
    record = _find(cli_records, 'vplug_req', 'import')
    assert _index(record, '/venvs/vplug_req/') < _index(record, '/venvs/mrsm/')


def test_the_mrsm_venv_comes_before_the_rest_of_sys_path(cli_records, sandbox):
    sandbox_root = sandbox['root']
    ### v4's order: plugin venv, base interpreter site-packages, `mrsm` venv, then the rest
    ### (here: the stdlib entries).
    record = _find(cli_records, 'vplug_req', 'import')
    stdlib = record['sys_path'].index(os.path.dirname(os.__file__).replace(str(sandbox_root), '<root>'))
    assert _index(record, '/venvs/mrsm/') < stdlib


def test_no_mrsm_venv_is_created_without_one(tmp_path):
    ### `ensure_default_venvs()` exposes an existing `mrsm` venv; it never creates one and never
    ### installs anything (`MRSM_NO_AUTO_INSTALL` semantics are unchanged).
    code = (
        "import sys, meerschaum.utils.venv as v;"
        "ok = v.ensure_default_venvs();"
        "import pathlib, os;"
        "print(ok, (pathlib.Path(os.environ['MRSM_ROOT_DIR']) / 'venvs' / 'mrsm').exists())"
    )
    (tmp_path / 'root').mkdir()
    (tmp_path / 'plugins').mkdir()
    env = {
        key: value for key, value in os.environ.items() if not key.startswith('MRSM_')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_SQL_W17BVT': f"sqlite:///{tmp_path / 'i.db'}",
        'MRSM_CONFIG': json.dumps({'meerschaum': {'instance': 'sql:w17bvt'}}),
    })
    result = subprocess.run(
        [sys.executable, '-c', code], env=env, cwd=str(tmp_path),
        capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().splitlines()[-1] == 'False False'


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def _fastapi_available() -> bool:
    check = subprocess.run(
        [sys.executable, '-c', 'import fastapi, uvicorn'],
        capture_output=True, env={**os.environ, 'PYTHONPATH': str(REPO)},
    )
    return check.returncode == 0


@pytest.mark.skipif(sys.platform == 'win32', reason="Unix sockets and process groups")
def test_api_plugin_routes_see_the_mrsm_venv(sandbox):
    if not _fastapi_available():
        message = "The API host's dependencies (fastapi, uvicorn) are missing."
        if in_ci():
            pytest.fail(message)
        pytest.skip(message)
    record = sandbox['base'] / 'api.rec'
    log = sandbox['base'] / 'api.log'
    port = _free_port()
    with open(log, 'w') as log_file:
        proc = subprocess.Popen(
            [
                sys.executable, '-m', 'meerschaum', 'start', 'api',
                '-i', 'sql:w17bvt', '--host', '127.0.0.1', '--port', str(port), '--no-auth',
            ],
            env=_env(sandbox, record),
            cwd=str(sandbox['base']),
            stdout=log_file, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    try:
        deadline = time.time() + 120
        body = None
        while time.time() < deadline:
            if proc.poll() is not None:
                pytest.fail(f"`start api` exited early:\n{log.read_text()[-3000:]}")
            try:
                with urllib.request.urlopen(
                    f'http://127.0.0.1:{port}/vplug_req/x', timeout=5
                ) as response:
                    body = json.loads(response.read())
                break
            except Exception:
                time.sleep(1)
        assert body == {'ok': True}, log.read_text()[-3000:]
        with urllib.request.urlopen(f'http://127.0.0.1:{port}/vplug_plain/x', timeout=10) as r:
            assert json.loads(r.read()) == {'ok': True}
    finally:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=20)
        except Exception:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except Exception:
                pass
            proc.wait(timeout=10)

    records = _read(record)
    for plugin in ('vplug_req', 'vplug_plain'):
        for label in ('api_init', 'api_route'):
            assert _version(_find(records, plugin, label), 'vsynth_mrsm') == '1.0', (plugin, label)
    assert _version(_find(records, 'vplug_req', 'api_route'), 'vsynth_shared') == '2.0'
    route = _find(records, 'vplug_req', 'api_route')
    assert _index(route, '/venvs/vplug_req/') < _index(route, '/venvs/mrsm/')
