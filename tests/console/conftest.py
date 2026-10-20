#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fixtures for the console browser tests (v5 design §14 "Console tests", D21).

The console is the Plotly Dash app of v4 (`meerschaum.api.dash`), served by the Python API host
behind the Rust server. The suite starts real `mrsm start api` servers (the standalone binary:
`MRSM_TEST_BIN`, or `target/{release,debug}/mrsm` outside CI; its version and age are checked, see
`tests/_binary.py`) on free high ports, each with a temporary `MRSM_ROOT_DIR`, an
empty-but-for-the-test plugins directory, a SQLite instance inside that root and a synthetic user,
and drives them with Python Playwright's Chromium. It never touches the real configuration,
port 8989, 8000, 8998, 3000 or any container.

The servers' Python (the API host and the plugin workers, `MRSM_PYTHON`, see `server_python()`)
is the test interpreter when its own environment already has the `meerschaum[api,dash,sql,pandas]`
packages (the CI virtual environment), otherwise a cached virtual environment built from
`requirements/{api,dash,sql,pandas}.txt` under `~/.cache/mrsm-console-venvs/<hash of those files>`
(override the directory with `MRSM_TEST_CONSOLE_VENV_DIR`; the first run needs a package index).
It is never a developer's `test_root/venvs` or user site, so a Playwright-only `python3` works.

It is opt-in: set `MRSM_TEST_CONSOLE=1`, otherwise every test here is skipped (the default
sqlite pytest run starts no servers). Run it with the repo on the path:

    cargo build -p meerschaum --bin mrsm
    MRSM_TEST_CONSOLE=1 PYTHONPATH=$(pwd) python -m pytest tests/console -v
"""

from __future__ import annotations

import os
import sys
import json
import venv
import hashlib
import time
import shutil
import signal
import socket
import random
import pathlib
import tempfile
import subprocess
import urllib.request
import urllib.error

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
ENABLE_ENV = 'MRSM_TEST_CONSOLE'
BIN_ENV = 'MRSM_TEST_BIN'
USERNAME = 'alice'
PASSWORD = 'correct-horse-1'
PORT_RANGE = (18400, 18999)
FORBIDDEN_PORTS = {8989, 8000, 8998, 3000, 5432, 1883}
COPIED_PLUGINS = ('w7_pyfig', 'w7_dashapp')

#: Console noise that is not a failure: Dash's renderer logs this (as a `console.error`) whenever a
#: callback's inputs are re-rendered in the same round, which every page of v4's console does.
IGNORED_CONSOLE_ERRORS = (
    'Failed to load resource',
    'A nonexistent object was used in an `Input` of a Dash callback',
    '{index,type:',
    ### A page navigation cancels the callbacks in flight.
    'Callback failed: the server did not respond',
)


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        f"console: browser tests against a real `mrsm start api` (set {ENABLE_ENV}=1)",
    )


def pytest_collection_modifyitems(config, items):
    if os.environ.get(ENABLE_ENV, '').lower() not in ('', '0', 'false'):
        return
    skip = pytest.mark.skip(
        reason=f"The console browser tests start servers; set {ENABLE_ENV}=1 to run them.",
    )
    here = str(pathlib.Path(__file__).parent)
    for item in items:
        if str(item.fspath).startswith(here):
            item.add_marker(skip)


def mrsm_binary() -> pathlib.Path:
    """The verified standalone `mrsm` (`tests/_binary.py`: version and age are checked)."""
    from tests._binary import binary_or_skip
    return pathlib.Path(binary_or_skip())


def free_port() -> int:
    ports = list(range(*PORT_RANGE))
    random.shuffle(ports)
    for port in ports:
        if port in FORBIDDEN_PORTS:
            continue
        with socket.socket() as probe:
            try:
                probe.bind(('127.0.0.1', port))
            except OSError:
                continue
        return port
    raise RuntimeError("No free port for the console test server.")


SERVER_REQUIREMENTS = ('api', 'dash', 'sql', 'pandas')
### What those extras let the host import (checked with `python -s`: the user site never counts).
SERVER_IMPORTS = (
    'fastapi', 'uvicorn', 'multipart', 'jwt', 'a2wsgi', 'websockets', 'dash',
    'dash_bootstrap_components', 'dash_ace', 'dash_extensions', 'sqlalchemy', 'pandas',
)
_server_python: dict = {}


def _provides_server_packages(python: str) -> bool:
    probe = subprocess.run(
        [python, '-s', '-c', 'import ' + ', '.join(SERVER_IMPORTS)], capture_output=True,
    )
    return probe.returncode == 0


def server_python() -> str:
    """
    The interpreter the servers run Python in: the test interpreter itself when its own
    environment (user site excluded) already provides the `meerschaum[api,dash,sql,pandas]`
    packages, as the CI virtual environment does; otherwise a virtual environment holding exactly
    those packages, keyed by the hash of their requirement files and the Python version and built
    on first use. Fails (it never falls back to another interpreter) when the packages cannot be
    installed.
    """
    if 'python' in _server_python:
        return _server_python['python']
    if _provides_server_packages(sys.executable):
        _server_python['python'] = sys.executable
        return sys.executable
    files = [REPO / 'requirements' / f'{name}.txt' for name in SERVER_REQUIREMENTS]
    digest = hashlib.sha256()
    digest.update(f'{sys.version_info[0]}.{sys.version_info[1]}'.encode())
    for path in files:
        digest.update(path.read_bytes())
    base = pathlib.Path(
        os.environ.get('MRSM_TEST_CONSOLE_VENV_DIR')
        or (pathlib.Path.home() / '.cache' / 'mrsm-console-venvs')
    )
    env_dir = base / digest.hexdigest()[:12]
    python = env_dir / ('Scripts' if os.name == 'nt' else 'bin') / (
        'python.exe' if os.name == 'nt' else 'python'
    )
    ready = env_dir / '.ready'
    if not ready.exists():
        base.mkdir(parents=True, exist_ok=True)
        venv.EnvBuilder(with_pip=True, clear=True).create(env_dir)
        command = [
            str(python), '-m', 'pip', 'install', '--quiet', '--disable-pip-version-check',
        ]
        for path in files:
            command += ['-r', str(path)]
        result = subprocess.run(command, capture_output=True, text=True, timeout=1200)
        if result.returncode != 0:
            pytest.fail(
                f"The console suite builds its server interpreter ({env_dir}) from "
                f"requirements/{{{','.join(SERVER_REQUIREMENTS)}}}.txt and needs a package index; "
                f"pip failed:\n{result.stderr[-2000:]}"
            )
        ready.write_text('\n'.join(str(p.name) for p in files), encoding='utf-8')
    _server_python['python'] = str(python)
    return str(python)


def base_env(root: pathlib.Path, plugins: pathlib.Path, config: dict | None = None) -> dict:
    """
    The environment for every subprocess: isolated root and plugins, no inherited `MRSM_*`.
    `HOME` is the temporary root so nothing falls back to `~/.config/meerschaum`; the Python
    host and workers run in `server_python()` with no user site.
    """
    env = {
        key: val for key, val in os.environ.items()
        if not key.startswith('MRSM_')
        ### An inherited `TMUX` makes tmux ignore `TMUX_TMPDIR` and hit the caller's server.
        and key not in ('PYTHONPATH', 'HOME', 'TMUX', 'TMUX_PANE')
    }
    merged_config = {
        'meerschaum': {'instance': 'sql:w7console', 'api_instance': 'sql:w7console'},
    }
    for key, val in (config or {}).items():
        merged_config.setdefault(key, {})
        if isinstance(val, dict):
            merged_config[key].update(val)
        else:
            merged_config[key] = val
    env.update({
        'HOME': str(root),
        'PYTHONNOUSERSITE': '1',
        'PYTHONPATH': str(REPO),
        'PYTHONDONTWRITEBYTECODE': '1',
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_NO_UPDATE_CHECK': '1',
        'MRSM_PYTHON': server_python(),
        'MRSM_W7_CONSOLE': '1',
        'MRSM_SQL_W7CONSOLE': f"sqlite:///{root / 'w7console.db'}",
        'MRSM_CONFIG': json.dumps(merged_config),
    })
    return env


def run_python(env: dict, code: str, timeout: int = 180) -> subprocess.CompletedProcess:
    return subprocess.run(
        [env['MRSM_PYTHON'], '-c', code],
        env=env, cwd=env['MRSM_ROOT_DIR'], capture_output=True, text=True, timeout=timeout,
    )


def http(url: str, timeout: float = 5.0) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status, response.read().decode('utf-8', 'replace')
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8', 'replace')
    except Exception:
        return 0, ''


def _reap_by_root(root: pathlib.Path) -> None:
    """Kill any process still running with this root (the API host, the webterm)."""
    needle = f"MRSM_ROOT_DIR={root}".encode()
    for proc in pathlib.Path('/proc').iterdir():
        if not proc.name.isdigit() or int(proc.name) == os.getpid():
            continue
        try:
            if needle in (proc / 'environ').read_bytes().split(b'\0'):
                os.kill(int(proc.name), signal.SIGKILL)
        except (OSError, PermissionError):
            continue


class Server:
    """A running `mrsm start api` with its own root, plugins directory and user."""

    def __init__(
        self,
        name: str,
        config: dict | None = None,
        args: list[str] | None = None,
        native: bool = True,
        webterm: bool = False,
        env: dict | None = None,
    ):
        self.name = name
        self.root = pathlib.Path(tempfile.mkdtemp(prefix=f'mrsm-w7-{name}-'))
        self.plugins = pathlib.Path(tempfile.mkdtemp(prefix=f'mrsm-w7-plugins-{name}-'))
        self.port = free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.env = base_env(self.root, self.plugins, config)
        self.env.update(env or {})
        self.args = list(args or [])
        self.native = native
        self.webterm = webterm
        self.native_built = False
        self.proc: subprocess.Popen | None = None
        self.watch_ports: list[int] = []
        self.leaked_ports: list[int] = []
        self.log_path = self.root / 'server.log'

    def prepare(self) -> None:
        for plugin in COPIED_PLUGINS:
            shutil.copytree(
                REPO / 'tests' / 'plugins' / plugin,
                self.plugins / plugin,
                ignore=shutil.ignore_patterns('__pycache__'),
            )
        if self.native:
            result = subprocess.run(
                ['bash', str(REPO / 'tests' / 'console' / 'build_native.sh'), str(self.plugins)],
                capture_output=True, text=True,
            )
            self.native_built = result.returncode == 0
            if not self.native_built:
                (self.root / 'native_build.log').write_text(result.stdout + result.stderr)
        result = run_python(self.env, f"""
import meerschaum as mrsm
from meerschaum.core import User
conn = mrsm.get_connector('sql:w7console')
success, msg = conn.register_user(User({USERNAME!r}, {PASSWORD!r}, instance=conn, type='admin'))
assert success, msg
""")
        assert result.returncode == 0, result.stderr[-3000:]

    def start(self) -> None:
        self.prepare()
        binary = mrsm_binary()
        with open(self.log_path, 'wb') as log:
            self.proc = subprocess.Popen(
                [
                    str(binary), 'start', 'api',
                    '--host', '127.0.0.1', '--port', str(self.port),
                    *([] if self.webterm else ['--no-webterm']),
                    *self.args,
                ],
                env=self.env, cwd=str(self.root), stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        deadline = time.time() + 180
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"The server exited early:\n{self.log()}")
            if http(f"{self.base_url}/version")[0] == 200:
                break
            time.sleep(0.5)
        else:
            raise RuntimeError(f"The server did not start in time:\n{self.log()}")
        ### The Python host starts on the first `/dash` request (it imports Dash and builds the
        ### console, which takes a while): wait for it here, so no test pays for it.
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"The server exited early:\n{self.log()}")
            status, body = http(f"{self.base_url}/dash/_dash-layout", timeout=60)
            ### 200: the console; 404: its pointer page (no Dash installed, or `--no-dash`).
            if status and status < 500:
                return
            time.sleep(1)
        raise RuntimeError(f"The console did not start in time:\n{self.log()}")

    def log(self) -> str:
        try:
            return self.log_path.read_text(errors='replace')[-4000:]
        except OSError:
            return ''

    def port_is_open(self, port: int) -> bool:
        with socket.socket() as probe:
            probe.settimeout(1)
            return probe.connect_ex(('127.0.0.1', port)) == 0

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            try:
                os.killpg(self.proc.pid, signal.SIGTERM)
                self.proc.wait(timeout=20)
            except Exception:
                try:
                    os.killpg(self.proc.pid, signal.SIGKILL)
                except Exception:
                    pass
        ### Whatever a clean shutdown left behind (checked by tests before the reaping below).
        deadline = time.time() + 10
        self.leaked_ports = list(self.watch_ports)
        while self.leaked_ports and time.time() < deadline:
            self.leaked_ports = [p for p in self.leaked_ports if self.port_is_open(p)]
            time.sleep(0.5)
        _reap_by_root(self.root)
        shutil.rmtree(self.root, ignore_errors=True)
        shutil.rmtree(self.plugins, ignore_errors=True)

    def mrsm(self, *args: str, timeout: int = 180) -> subprocess.CompletedProcess:
        """Run the CLI against this server's root (e.g. `sync pipes`)."""
        return subprocess.run(
            [str(mrsm_binary()), *args],
            env=self.env, cwd=str(self.root), capture_output=True, text=True, timeout=timeout,
        )

    def python(self, code: str) -> subprocess.CompletedProcess:
        return run_python(self.env, code)


@pytest.fixture(scope='session')
def playwright_instance():
    sync_api = pytest.importorskip('playwright.sync_api')
    with sync_api.sync_playwright() as p:
        yield p


@pytest.fixture(scope='session')
def browser(playwright_instance):
    try:
        browser = playwright_instance.chromium.launch(headless=True)
    except Exception as e:
        pytest.skip(f"Chromium is not available to Playwright: {e}")
    yield browser
    browser.close()


@pytest.fixture(scope='session')
def server():
    """The main console server (every plugin, native one included)."""
    instance = Server('main')
    try:
        instance.start()
    except Exception:
        instance.stop()
        raise
    yield instance
    instance.stop()


@pytest.fixture(scope='session')
def gated_server():
    """A server whose route allowlist does not include the `dash` group."""
    instance = Server(
        'gated',
        config={'api': {'permissions': {'routes': {'allowlist': ['index', 'pipes', 'version', 'plugins']}}}},
        native=False,
    )
    try:
        instance.start()
    except Exception:
        instance.stop()
        raise
    yield instance
    instance.stop()


@pytest.fixture(scope='session')
def nodash_server():
    """A server started with `--no-dash`: API plugin routes only."""
    instance = Server('nodash', args=['--no-dash'], native=False)
    try:
        instance.start()
    except Exception:
        instance.stop()
        raise
    yield instance
    instance.stop()


@pytest.fixture(scope='session')
def webterm_server():
    """A server with the web terminal on; its terminal process must not outlive the server."""
    port = free_port()
    instance = Server(
        'webterm',
        config={'api': {'webterm': {'port': port, 'tmux': {'enabled': False}}}},
        native=False,
        webterm=True,
    )
    instance.watch_ports = [port]
    instance.webterm_port = port
    try:
        instance.start()
    except Exception:
        instance.stop()
        raise
    yield instance
    instance.stop()
    assert instance.leaked_ports == [], "the terminal process outlived the server"


@pytest.fixture(scope='session')
def noauth_server():
    """A server started with `--no-auth`: the console needs no login."""
    instance = Server('noauth', args=['--no-auth'], native=False)
    try:
        instance.start()
    except Exception:
        instance.stop()
        raise
    yield instance
    instance.stop()


@pytest.fixture(scope='session')
def nodashpkg_server():
    """
    A server whose Python cannot import Dash: a stub `dash` package that raises `ImportError` sits
    in the `mrsm` virtual environment, which the host puts first on its path for its whole life. The
    host starts, reports that the console is not mounted, and `/dash` shows the pointer page. The
    API plugins' routes still work.
    """
    instance = Server('nodashpkg', native=False)
    version = subprocess.run(
        [server_python(), '-c', 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")'],
        capture_output=True, text=True,
    ).stdout.strip()
    site = instance.root / 'venvs' / 'mrsm' / 'lib' / f"python{version}" / 'site-packages'
    (site / 'dash').mkdir(parents=True)
    (site / 'dash' / '__init__.py').write_text(
        "raise ImportError(\"No module named 'dash' (a stub for the tests)\")\n"
    )
    try:
        instance.start()
    except Exception:
        instance.stop()
        raise
    yield instance
    instance.stop()


@pytest.fixture
def context(browser):
    context = browser.new_context()
    yield context
    context.close()


class Tab:
    """A page that records its console errors, page errors and failed same-origin responses."""

    def __init__(self, page, base_url: str):
        self.page = page
        self.base_url = base_url
        self.errors: list[str] = []
        self.network_changed = False
        self.network_changed_ever = False
        page.on('console', self._on_console)
        page.on('pageerror', self._on_pageerror)
        page.on('response', self._on_response)
        page.on('requestfailed', self._on_requestfailed)

    def _on_requestfailed(self, request):
        if 'ERR_NETWORK_CHANGED' in str(request.failure):
            self.network_changed = True
            self.network_changed_ever = True

    def _on_pageerror(self, error):
        ### The scripts of that page never loaded (see `goto`).
        if self.network_changed_ever and 'is not defined' in str(error):
            return
        self.errors.append(f"pageerror: {error}")

    def _on_console(self, message):
        ### A script that never loaded (see `goto`) leaves its globals undefined.
        unloaded = self.network_changed_ever and 'is not defined' in message.text
        if message.type == 'error' and not unloaded and not any(
            ignored in message.text for ignored in IGNORED_CONSOLE_ERRORS
        ):
            self.errors.append(f"console.error: {message.text} ({message.location})")

    def _on_response(self, response):
        ### Browsers ask for a source map of each Dash bundle; the console answers `404` on purpose.
        if response.url.endswith('.map'):
            return
        if response.url.startswith(self.base_url) and response.status >= 400:
            ### Intentional 404/401 probes are made with `request`, not the page; any failure
            ### a page itself triggers is an error.
            self.errors.append(f"{response.status} {response.url}")

    def goto(self, path: str, selector: str | None = None) -> None:
        """
        Open a console path and wait for the Dash app to render (a selector, else the navbar).
        Chromium aborts every request of a page when the machine's network interfaces change
        (`ERR_NETWORK_CHANGED`: a container bridge coming up is enough), which leaves a Dash page
        without its scripts: that, and only that, is retried.
        """
        selector = selector or '#logo-img, #username-input'
        for attempt in range(3):
            self.network_changed = False
            self.page.goto(f"{self.base_url}{path}")
            try:
                self.page.wait_for_selector(selector, timeout=30000 if attempt == 2 else 15000)
                return
            except Exception:
                if not self.network_changed or attempt == 2:
                    raise
        return

    def login(self, username: str = USERNAME, password: str = PASSWORD) -> None:
        self.goto('/dash', '#username-input')
        self.page.fill('#username-input', username)
        self.page.fill('#password-input', password)
        self.page.click('#login-button')
        self.wait_for_dashboard()

    def wait_for_dashboard(self) -> None:
        """The dashboard is ready when its action form has its default (`show pipes`)."""
        self.page.wait_for_selector('#go-button', timeout=30000)
        self.page.wait_for_function(
            "document.getElementById('action-dropdown') && "
            "document.getElementById('action-dropdown').value === 'show'",
            timeout=30000,
        )


@pytest.fixture
def tab(context, server):
    page = context.new_page()
    return Tab(page, server.base_url)
