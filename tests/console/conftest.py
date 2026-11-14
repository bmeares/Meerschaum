#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fixtures for the console browser tests (v5 design §14 "Console tests", D21).

The console is the Plotly Dash app of v4 (`meerschaum.api.dash`), served by the Python API host
behind the Rust server. The suite starts real `mrsm start api` servers (the standalone binary:
`MRSM_TEST_BIN`, or `target/{release,debug}/mrsm` outside CI; its version and age are checked, see
`tests/_binary.py`, including artifact provenance in CI) on free high ports, each with a temporary `MRSM_ROOT_DIR`, an
empty-but-for-the-test plugins directory, a SQLite instance inside that root and a synthetic user,
and drives them with Python Playwright's Chromium. It never touches the real configuration,
port 8989, 8000, 8998, 3000 or any container.

The servers' Python (the API host and the plugin workers, `MRSM_PYTHON`, see `server_python()`)
is the test interpreter when its own environment already has the `meerschaum[api,dash,sql,pandas]`
packages (the CI virtual environment), otherwise a cached virtual environment built from
`requirements/{api,dash,sql,pandas}.txt` under `~/.cache/mrsm-console-venvs/<hash of those files>`
(override the directory with `MRSM_TEST_CONSOLE_VENV_DIR`; the first run needs a package index).
It is never a developer's `test_root/venvs` or user site, so a Playwright-only `python3` works.

On Linux, Chromium runs in a network namespace of its own (`browser_isolation`, which needs
`pasta`), so Docker networks other jobs create and remove never abort its requests; CI requires
it, `MRSM_TEST_CONSOLE_NETNS=0` turns it off. A failing test leaves its tabs' screenshots, DOM,
console and network timelines and the servers' logs under `MRSM_TEST_CONSOLE_DIAG_DIR` (default
`~/.cache/mrsm-console-tests/diagnostics`; CI uploads them as `console-diagnostics-<attempt>`).

It is opt-in: set `MRSM_TEST_CONSOLE=1`, otherwise every test here is skipped (the default
sqlite pytest run starts no servers). Run it with the repo on the path:

    cargo build -p meerschaum --bin mrsm
    MRSM_TEST_CONSOLE=1 PYTHONPATH=$(pwd) python -m pytest tests/console -v
"""

from __future__ import annotations

import os
import re
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
import collections
import urllib.request
import urllib.error
from datetime import datetime, timezone

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
    '{index,type:',
    ### A page navigation cancels the callbacks in flight.
    'Callback failed: the server did not respond',
)


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        f"console: browser tests against a real `mrsm start api` (set {ENABLE_ENV}=1)",
    )


#: Where a failing console test leaves its evidence (CI uploads it).
DIAG_ENV = 'MRSM_TEST_CONSOLE_DIAG_DIR'
#: The tabs opened and the servers running in this process, for a failing test's evidence.
_live_tabs: list = []
_live_servers: list = []
#: Tabs captured while their context closed during a failing test (see `_console_diagnostics`).
_captured: list[dict] = []


def diagnostics_dir() -> pathlib.Path:
    return pathlib.Path(
        os.environ.get(DIAG_ENV)
        or (pathlib.Path.home() / '.cache' / 'mrsm-console-tests' / 'diagnostics')
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    setattr(item, f'_console_report_{report.when}', report)


def _failed(item) -> bool:
    return any(
        getattr(getattr(item, f'_console_report_{when}', None), 'failed', False)
        for when in ('setup', 'call')
    )


@pytest.fixture(autouse=True)
def _console_diagnostics(request, monkeypatch):
    """
    When a console test fails, write what it saw under `diagnostics_dir()/<test>/`: each open tab's
    screenshot, URL, DOM, console messages, failed and still-pending requests and the timeline of
    its Dash callbacks, plus the full log of every server running in this process (the Rust API
    server and the Python API host write to the same file). A test that closes its own browser
    context in a `finally` is captured just before that close, while the page still shows the
    failure.
    """
    if os.environ.get(ENABLE_ENV, '').lower() in ('', '0', 'false'):
        yield
        return
    item = request.node
    _live_tabs.clear()
    _captured.clear()
    try:
        from playwright.sync_api import BrowserContext
    except ImportError:
        BrowserContext = None
    if BrowserContext is not None:
        original_close = BrowserContext.close

        def close(context, *args, **kwargs):
            ### A failure propagating through the test's `finally`, or a failed call before the
            ### `context` fixture's teardown.
            if sys.exc_info()[0] is not None or _failed(item):
                for tab in list(_live_tabs):
                    if tab.page.context is context or tab.page.context == context:
                        _captured.append(tab.snapshot())
                        _live_tabs.remove(tab)
            return original_close(context, *args, **kwargs)

        monkeypatch.setattr(BrowserContext, 'close', close)
    yield
    if not _failed(item):
        return
    for tab in list(_live_tabs):
        _captured.append(tab.snapshot())
    _live_tabs.clear()
    name = re.sub(r'[^A-Za-z0-9_.-]+', '_', item.nodeid)[-150:]
    dest = diagnostics_dir() / name
    try:
        dest.mkdir(parents=True, exist_ok=True)
        for index, snap in enumerate(_captured):
            png = snap.pop('screenshot', None)
            html = snap.pop('html', None)
            if png:
                (dest / f'tab{index}.png').write_bytes(png)
            if html:
                (dest / f'tab{index}.html').write_text(html, encoding='utf-8', errors='replace')
            (dest / f'tab{index}.json').write_text(
                json.dumps(snap, indent=2, default=str), encoding='utf-8'
            )
        for server in list(_live_servers):
            try:
                data = server.log_path.read_bytes()[-4_000_000:]
            except OSError:
                continue
            (dest / f'server-{server.name}-{server.port}.log').write_bytes(data)
            for log_file in (server.root / 'logs').glob('*.log*'):
                if log_file.is_file() and not log_file.name.startswith('.'):
                    (dest / f'job-{server.name}-{log_file.name}').write_bytes(
                        log_file.read_bytes()[-4_000_000:]
                    )
        (dest / 'failure.txt').write_text(
            f"{item.nodeid}\nwritten {_now()}\n\n"
            + '\n\n'.join(
                str(getattr(getattr(item, f'_console_report_{when}', None), 'longrepr', '') or '')
                for when in ('setup', 'call')
            ),
            encoding='utf-8',
        )
        sys.stderr.write(f"\nConsole diagnostics for {item.nodeid}: {dest}\n")
    except OSError as e:
        sys.stderr.write(f"\nCould not write console diagnostics to {dest}: {e}\n")
    finally:
        _captured.clear()


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
        ### pytest-xdist workers (and concurrent runs) share the directory: one builds it, the
        ### others wait on the lock and then find it complete (`.ready`, written last).
        with _exclusive(base / (env_dir.name + '.lock')):
            if not ready.exists():
                _build_server_venv(env_dir, python, ready, files)
    _server_python['python'] = str(python)
    return str(python)


def _exclusive(path: pathlib.Path):
    """An exclusive lock on `path` across processes (released when the block ends)."""
    import contextlib

    @contextlib.contextmanager
    def _lock():
        with open(path, 'a+b') as handle:
            if os.name == 'nt':
                import msvcrt
                while True:
                    try:
                        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                        break
                    except OSError:
                        time.sleep(0.5)
                try:
                    yield
                finally:
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    return _lock()


def _build_server_venv(env_dir: pathlib.Path, python: pathlib.Path, ready: pathlib.Path, files) -> None:
    """Create the server interpreter's virtual environment (called under `_exclusive`)."""
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
        ### `executor: local`: jobs a test creates (`-d`, the console's New job) are never
        ### systemd units of the host running the suite.
        'meerschaum': {'instance': 'sql:w7console', 'api_instance': 'sql:w7console', 'executor': 'local'},
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
        scratch = pathlib.Path.home() / '.cache' / 'mrsm-console-tests'
        scratch.mkdir(parents=True, exist_ok=True)
        self.root = pathlib.Path(tempfile.mkdtemp(prefix=f'mrsm-w7-{name}-', dir=scratch))
        self.plugins = pathlib.Path(tempfile.mkdtemp(prefix=f'mrsm-w7-plugins-{name}-', dir=scratch))
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
                from tests._binary import in_ci
                if in_ci():
                    pytest.fail('Required native console fixture is unavailable: '
                                + (result.stdout + result.stderr)[-1500:])
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
        _live_servers.append(self)
        with open(self.log_path, 'wb') as log:
            self.proc = subprocess.Popen(
                [
                    str(binary), 'start', 'api',
                    '--host', '127.0.0.1', '--port', str(self.port),
                    *([] if self.webterm else ['--no-webterm']),
                    *self.args,
                ],
                ### The proxy's warnings (a host that did not answer, a refused connection) are
                ### only printed with `RUST_LOG`: keep them, timestamped, for a failing test.
                env={'RUST_LOG': 'meerschaum_server=warn,meerschaum_web=warn', **self.env},
                cwd=str(self.root), stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        deadline = time.time() + 180
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"The server exited early:\n{self.log()}")
            ### 401: a `--private` server answers, asking for credentials.
            if http(f"{self.base_url}/version")[0] in (200, 401):
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
        if self in _live_servers:
            _live_servers.remove(self)
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


@pytest.fixture(scope='module')
def playwright_instance():
    # Playwright's sync API keeps an asyncio loop active in this thread. Close it
    # when leaving a browser test module so later SDK tests can use asyncio.run().
    from tests._binary import skip_or_fail
    try:
        import playwright.sync_api as sync_api
    except ImportError:
        skip_or_fail('The required console browser suite needs the pinned Playwright package.')
    with sync_api.sync_playwright() as p:
        yield p


#: The host ports the browser reaches test servers on (`PORT_RANGE` and the plugin SDK test's).
BROWSER_PORTS = f"{PORT_RANGE[0]}-19999"
#: `0` launches Chromium in the host's network namespace (see `browser_isolation`).
NETNS_ENV = 'MRSM_TEST_CONSOLE_NETNS'
NETNS_WRAPPER = REPO / 'tests' / 'console' / 'chromium_netns.sh'
_isolation: dict = {}


def _headless_shell(playwright_instance) -> pathlib.Path | None:
    """The headless shell `launch(headless=True)` runs, installed beside Playwright's Chromium."""
    try:
        chromium = pathlib.Path(playwright_instance.chromium.executable_path)
        ### `.../ms-playwright/chromium-<revision>/chrome-linux64/chrome`
        revision = chromium.parents[1].name.split('-', 1)[1]
    except Exception:
        return None
    shell = (
        chromium.parents[2] / f'chromium_headless_shell-{revision}'
        / 'chrome-headless-shell-linux64' / 'chrome-headless-shell'
    )
    return shell if shell.exists() else None


def browser_isolation(playwright_instance) -> dict | None:
    """
    The `launch()` arguments that run Chromium in a network namespace of its own
    (`chromium_netns.sh`), or `None` with the reason in `_isolation['reason']`.

    Chromium fails every request waiting for a connection with `ERR_NETWORK_CHANGED` when the
    host's network interfaces change, and Docker networks of the database lanes come and go on the
    same CI runners: a Dash page's callbacks beyond the six connections per host were dropped
    mid-test and their outputs never rendered (the console suite's intermittent 60 s timeouts).
    In its own namespace (loopback ports forwarded to the host's by pasta) it never sees them.
    Linux only; needs `unshare`, `nsenter`, `setpriv`, `pasta` and unprivileged user namespaces.
    """
    if 'result' in _isolation:
        return _isolation['result']
    reason = None
    shell = None
    if os.environ.get(NETNS_ENV, '').lower() in ('0', 'false', 'no'):
        reason = f"{NETNS_ENV}=0"
    elif not sys.platform.startswith('linux'):
        reason = f"not on Linux ({sys.platform})"
    elif missing := [tool for tool in ('unshare', 'nsenter', 'setpriv', 'pasta') if not shutil.which(tool)]:
        reason = f"missing {', '.join(missing)}"
    elif (shell := _headless_shell(playwright_instance)) is None:
        reason = "Playwright's headless shell was not found"
    elif subprocess.run(
        ['unshare', '--user', '--map-current-user', '--net', 'true'], capture_output=True,
    ).returncode != 0:
        reason = "unprivileged user and network namespaces are not available"
    _isolation['reason'] = reason
    _isolation['result'] = None if reason else {
        'executable_path': str(NETNS_WRAPPER),
        'env': {
            **os.environ,
            'MRSM_TEST_CHROMIUM': str(shell),
            'MRSM_TEST_CHROMIUM_PORTS': BROWSER_PORTS,
        },
    }
    return _isolation['result']


@pytest.fixture(scope='module')
def browser(playwright_instance):
    from tests._binary import in_ci, skip_or_fail
    isolation = browser_isolation(playwright_instance)
    if isolation is None and in_ci() and sys.platform.startswith('linux') and (
        os.environ.get(NETNS_ENV, '').lower() not in ('0', 'false', 'no')
    ):
        pytest.fail(
            "The console suite's browser must not see the runner's Docker networks change "
            f"(see `browser_isolation`): {_isolation['reason']}. Set {NETNS_ENV}=0 to run it "
            "in the host's network namespace anyway."
        )
    try:
        browser = playwright_instance.chromium.launch(headless=True, **(isolation or {}))
    except Exception as e:
        skip_or_fail(f"Chromium is not available to the required console suite: {e}")
    yield browser
    browser.close()


#: A synthetic password the Connectors page must never send to the browser.
CONNECTOR_SECRET = 'W7-s3cret-pw'


@pytest.fixture(scope='session')
def server():
    """
    The main console server (every plugin, native one included), with an unreachable connector
    holding `CONNECTOR_SECRET` (`sql:w7secret`) and an empty instance (`sql:w7empty`).
    """
    instance = Server('main')
    instance.env['MRSM_SQL_W7SECRET'] = f"postgresql://w7user:{CONNECTOR_SECRET}@127.0.0.1:1/w7"
    instance.env['MRSM_SQL_W7EMPTY'] = f"sqlite:///{instance.root / 'w7empty.db'}"
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
    ### The legacy terminal port must stay unused. Keep it bound without listening so another
    ### parallel fixture cannot claim it before the browser checks the shared API listener.
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        port = reservation.getsockname()[1]
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
        self.interrupted_components: set[str] = set()
        ### Evidence for a failing test (`snapshot`, `_console_diagnostics`).
        self.console_log: collections.deque = collections.deque(maxlen=500)
        self.failed_requests: collections.deque = collections.deque(maxlen=200)
        self.callbacks: collections.deque = collections.deque(maxlen=300)
        self.pending: dict = {}
        page.on('console', self._on_console)
        page.on('pageerror', self._on_pageerror)
        page.on('response', self._on_response)
        page.on('request', self._on_request)
        page.on('requestfinished', self._on_requestfinished)
        page.on('requestfailed', self._on_requestfailed)
        _live_tabs.append(self)

    @staticmethod
    def _callback_outputs(request) -> str:
        """The outputs a `/_dash-update-component` request is for (its `output` field)."""
        try:
            return str((json.loads(request.post_data or '{}') or {}).get('output', ''))[:300]
        except Exception:
            return ''

    def _on_request(self, request):
        self.pending[id(request)] = {
            'started': _now(), 't0': time.monotonic(), 'method': request.method, 'url': request.url,
            'outputs': self._callback_outputs(request) if '_dash-update-component' in request.url else '',
        }

    def _finish(self, request, outcome: str):
        entry = self.pending.pop(id(request), None)
        if entry is None:
            return None
        entry['seconds'] = round(time.monotonic() - entry.pop('t0'), 3)
        entry['outcome'] = outcome
        if '_dash-update-component' in entry['url'] or outcome != 'ok':
            self.callbacks.append(entry)
        return entry

    def _on_requestfinished(self, request):
        self._finish(request, 'ok')

    def _on_requestfailed(self, request):
        failure = str(getattr(request, 'failure', ''))
        entry = self._finish(request, failure) if hasattr(request, 'method') else None
        self.failed_requests.append(entry or {'at': _now(), 'url': request.url, 'outcome': failure})
        if 'ERR_NETWORK_CHANGED' in failure:
            self.network_changed = True
            self.network_changed_ever = True
            self.interrupted_components.update(
                name for name in ('dash_html_components', 'dash_core_components')
                if name in request.url
            )

    def _unloaded_script_error(self, text):
        return ((self.network_changed_ever and 'is not defined' in text)
                or any(name+' was not found' in text for name in self.interrupted_components))

    def _on_pageerror(self, error):
        ### The scripts of that page never loaded (see `goto`).
        if self._unloaded_script_error(str(error)):
            return
        self.errors.append(f"pageerror: {getattr(error, 'stack', None) or error}")

    def _on_console(self, message):
        self.console_log.append({'at': _now(), 'type': message.type, 'text': message.text[:2000]})
        ### A script that never loaded (see `goto`) leaves its globals undefined.
        unloaded = self._unloaded_script_error(message.text)
        if message.type == 'error' and not unloaded and not any(
            ignored in message.text for ignored in IGNORED_CONSOLE_ERRORS
        ):
            self.errors.append(f"console.error: {message.text} ({message.location})")

    def _on_response(self, response):
        if response.status == 200:
            self.interrupted_components.difference_update(
                name for name in tuple(self.interrupted_components) if name in response.url
            )
        ### Browsers ask for a source map of each Dash bundle; the console answers `404` on purpose.
        if response.url.endswith('.map'):
            return
        if response.url.startswith(self.base_url) and response.status >= 400:
            ### Intentional 404/401 probes are made with `request`, not the page; any failure
            ### a page itself triggers is an error.
            self.errors.append(f"{response.status} {response.url}")

    def snapshot(self) -> dict:
        """What this tab shows and saw (see `_console_diagnostics`); never raises."""
        now = time.monotonic()
        snap = {
            'captured': _now(),
            'errors': list(self.errors),
            'network_changed_ever': self.network_changed_ever,
            'pending_requests': [
                {**{k: v for k, v in entry.items() if k != 't0'}, 'seconds': round(now - entry['t0'], 3)}
                for entry in self.pending.values()
            ],
            'failed_requests': list(self.failed_requests),
            'callbacks': list(self.callbacks),
            'console': list(self.console_log),
        }
        try:
            snap['url'] = self.page.url
        except Exception as e:
            snap['url'] = f'unavailable: {e}'
        for key, call in (
            ('screenshot', lambda: self.page.screenshot(timeout=10000)),
            ('html', lambda: self.page.content()),
        ):
            try:
                snap[key] = call()
            except Exception as e:
                snap[key + '_error'] = str(e)
        return snap

    def goto(self, path: str, selector: str | None = None) -> None:
        """
        Open a console path and wait for the Dash app to render (a selector, else the navbar).
        Chromium fails the requests of a page still waiting for a connection when the machine's
        network interfaces change (`ERR_NETWORK_CHANGED`: a container bridge coming up is enough),
        which leaves a Dash page without its scripts: that, and only that, is retried. A browser
        in its own network namespace (`browser_isolation`) never sees such a change.
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

    def goto_until(self, path: str, selector: str, condition: str, timeout: int = 60000) -> None:
        """
        Open a console path (`goto`) and wait until the JavaScript `condition` holds. A page whose
        callbacks Chromium aborted after it loaded (`ERR_NETWORK_CHANGED`: another job bringing
        a container network up or down) never renders what they would have drawn: that page is
        loaded again (twice at most); any other wait that runs out fails as before.
        """
        for attempt in range(3):
            ### `goto` clears `network_changed` before it loads the page.
            self.goto(path, selector)
            try:
                self.page.wait_for_function(condition, timeout=timeout)
                return
            except Exception:
                if not self.network_changed or attempt == 2:
                    raise

    def login(
        self,
        username: str = USERNAME,
        password: str = PASSWORD,
        path: str = '/dash/terminal',
    ) -> None:
        """
        Sign in on the login page `path` shows, and wait for that page. By default it is the
        Terminal page (v4's dashboard: the action form beside the terminal), which most tests
        drive; `path='/dash'` lands on the Overview.
        """
        self.goto(path, '#username-input')
        self.page.fill('#username-input', username)
        self.page.fill('#password-input', password)
        self.page.click('#login-button')
        if path.rstrip('/') in ('/dash/terminal', '/dash/webterm'):
            self.wait_for_dashboard()
        elif path.rstrip('/') == '/dash':
            self.page.wait_for_selector('#overview-page', timeout=30000)
        else:
            self.page.wait_for_selector('#pages-navbar-div', timeout=30000)

    def wait_for_dashboard(self) -> None:
        """The Terminal page is ready when its action form has its default (`show pipes`)."""
        self.page.wait_for_selector('#go-button', timeout=30000)
        self.page.wait_for_function(
            "document.getElementById('action-dropdown') && "
            "document.getElementById('action-dropdown').value === 'show'",
            timeout=30000,
        )

    def goto_terminal(self) -> None:
        """Open the Terminal page (`/dash/terminal`) and wait for its action form."""
        self.goto('/dash/terminal', '#go-button')
        self.wait_for_dashboard()

    def sign_out(self) -> None:
        """Sign out: the user menu's Sign out on console pages, the bar's on a plugin's page."""
        menu = self.page.locator('#console-nav-user .dropdown-toggle')
        if menu.count():
            toggler = self.page.locator('#navbar-toggler')
            if toggler.is_visible() and not menu.is_visible():
                toggler.click()
            menu.click()
        self.page.click('#sign-out-button')


@pytest.fixture
def tab(context, server):
    page = context.new_page()
    return Tab(page, server.base_url)
