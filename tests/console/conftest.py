#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fixtures for the console browser tests (v5 design §14 "Console tests").

The suite starts real `mrsm start api` servers (the debug binary, `target/debug/mrsm`, or
`MRSM_BIN`) on free high ports, each with a temporary `MRSM_ROOT_DIR`, an empty-but-for-the-test
plugins directory, a SQLite instance inside that root and a synthetic user, and drives them with
Python Playwright's Chromium. It never touches the real configuration, port 8989, 8000, 8998,
3000 or any container.

It is opt-in: set `MRSM_TEST_CONSOLE=1`, otherwise every test here is skipped (the default
sqlite pytest run starts no servers). Run it with the repo on the path:

    cargo build -p meerschaum-cli --bin mrsm
    MRSM_TEST_CONSOLE=1 PYTHONPATH=$(pwd) python -m pytest tests/console -v
"""

from __future__ import annotations

import os
import sys
import json
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
BIN_ENV = 'MRSM_BIN'
USERNAME = 'alice'
PASSWORD = 'correct-horse-1'
PORT_RANGE = (18400, 18999)
FORBIDDEN_PORTS = {8989, 8000, 8998, 3000, 5432, 1883}
COPIED_PLUGINS = ('w7_pyfig', 'w7_dashapp')


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
    return pathlib.Path(os.environ.get(BIN_ENV) or REPO / 'target' / 'debug' / 'mrsm')


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


def base_env(root: pathlib.Path, plugins: pathlib.Path, config: dict | None = None) -> dict:
    """
    The environment for every subprocess: isolated root and plugins, no inherited `MRSM_*`.
    `HOME` is the temporary root so nothing falls back to `~/.config/meerschaum`; the user site is
    kept (`PYTHONUSERBASE`) so the Python host finds its dependencies.
    """
    import site
    env = {
        key: val for key, val in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('PYTHONPATH', 'HOME')
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
        'PYTHONUSERBASE': site.getuserbase(),
        'PYTHONPATH': str(REPO),
        'PYTHONDONTWRITEBYTECODE': '1',
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_NO_UPDATE_CHECK': '1',
        'MRSM_PYTHON': sys.executable,
        'MRSM_W7_CONSOLE': '1',
        'MRSM_SQL_W7CONSOLE': f"sqlite:///{root / 'w7console.db'}",
        'MRSM_CONFIG': json.dumps(merged_config),
    })
    venvs = REPO / 'test_root' / 'venvs'
    if (venvs / 'mrsm').is_dir():
        env['MRSM_VENVS_DIR'] = str(venvs)
    return env


def run_python(env: dict, code: str, timeout: int = 180) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, '-c', code],
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
    ):
        self.name = name
        self.root = pathlib.Path(tempfile.mkdtemp(prefix=f'mrsm-w7-{name}-'))
        self.plugins = pathlib.Path(tempfile.mkdtemp(prefix=f'mrsm-w7-plugins-{name}-'))
        self.port = free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.env = base_env(self.root, self.plugins, config)
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
        assert binary.exists(), f"{binary} is missing; run `cargo build -p meerschaum-cli --bin mrsm`."
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
                return
            time.sleep(0.5)
        raise RuntimeError(f"The server did not start in time:\n{self.log()}")

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
        page.on('console', self._on_console)
        page.on('pageerror', lambda e: self.errors.append(f"pageerror: {e}"))
        page.on('response', self._on_response)

    def _on_console(self, message):
        if message.type == 'error':
            self.errors.append(f"console.error: {message.text} ({message.location})")

    def _on_response(self, response):
        if response.url.startswith(self.base_url) and response.status >= 400:
            ### Intentional 404/401 probes are made with `request`, not the page; any failure
            ### a page itself triggers is an error.
            self.errors.append(f"{response.status} {response.url}")

    def login(self, username: str = USERNAME, password: str = PASSWORD) -> None:
        self.page.goto(f"{self.base_url}/dash/login")
        self.page.fill('input[name=username]', username)
        self.page.fill('input[name=password]', password)
        self.page.click('button:has-text("Log in")')
        self.page.wait_for_url(f"{self.base_url}/dash")


@pytest.fixture
def tab(context, server):
    page = context.new_page()
    return Tab(page, server.base_url)
