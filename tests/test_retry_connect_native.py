#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
5.1: `meerschaum.connectors.poll.retry_connect()` runs in this process, on the Rust core, for the
connectors the core implements (`sql`, `valkey`, `api`): no child Python process, no temporary
file. Any other connector keeps the child-process path (`tests/test_retry_connect_secret.py`).

The unreachable targets are closed loopback ports (a socket bound to port 0 and closed); the
reachable ones are a SQLite file and servers these tests start themselves.
"""

import http.server
import json
import os
import pathlib
import signal
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time

import pytest

import meerschaum as mrsm
from meerschaum.connectors.poll import retry_connect

SECRET = 'native-retry-s3cret-value'

pytestmark = pytest.mark.skipif(
    sys.platform == 'win32', reason="these tests read the fd 0 / signals of POSIX processes",
)


def _closed_port() -> int:
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class _NoChild:
    """Fail the test if anything starts a child process or a temporary file."""

    def __init__(self, monkeypatch):
        self.calls = []
        from meerschaum.utils import venv as venv_utils

        def forbidden(name):
            def _fail(*args, **kwargs):
                self.calls.append(name)
                raise AssertionError(f"`{name}` was called on the native path")
            return _fail

        monkeypatch.setattr(venv_utils, 'venv_exec', forbidden('venv_exec'))
        monkeypatch.setattr(tempfile, 'mkstemp', forbidden('tempfile.mkstemp'))
        monkeypatch.setattr(subprocess, 'Popen', forbidden('subprocess.Popen'))
        monkeypatch.setattr(subprocess, 'call', forbidden('subprocess.call'))
        monkeypatch.setattr(subprocess, 'run', forbidden('subprocess.run'))
        self.tmp_before = set(os.listdir(tempfile.gettempdir()))

    def assert_clean(self):
        assert not self.calls
        created = set(os.listdir(tempfile.gettempdir())) - self.tmp_before
        assert not [name for name in created if name.startswith('mrsm-retry-')], created


@pytest.fixture(scope='module', autouse=True)
def _warm_up(tmp_path_factory):
    """
    Load the connectors' modules and the plugins directory (which sets up the virtual
    environments of the plugins it holds, with child processes of their own) before any test
    forbids child processes.
    """
    database = tmp_path_factory.mktemp('warm') / 'warm.db'
    conn = mrsm.get_connector('sql', 'trcn_warm', flavor='sqlite', database=str(database))
    mrsm.get_connector('valkey', 'trcn_warm', host='127.0.0.1', port=1)
    mrsm.get_connector('api', 'trcn_warm', protocol='http', host='127.0.0.1', port=1)
    assert retry_connect(conn, max_retries=1, retry_wait=0, warn=False) is True


@pytest.fixture(autouse=True)
def _every_warning():
    """Print each warning (Python prints an identical one once per location by default)."""
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('always')
        yield


@pytest.fixture
def no_child(monkeypatch):
    guard = _NoChild(monkeypatch)
    yield guard
    guard.assert_clean()


def _sqlite(tmp_path, label='trcn_sqlite', **kw):
    return mrsm.get_connector(
        'sql', label, flavor='sqlite', database=str(tmp_path / f'{label}.db'), **kw,
    )


def _dead_sql(label='trcn_pg'):
    return mrsm.get_connector(
        'sql', label,
        flavor='postgresql', host='127.0.0.1', port=_closed_port(),
        username='trcn', password=SECRET, database='trcn',
    )


def _dead_valkey(label='trcn_valkey'):
    return mrsm.get_connector(
        'valkey', label, host='127.0.0.1', port=_closed_port(), password=SECRET,
    )


def _dead_api(label='trcn_api'):
    return mrsm.get_connector(
        'api', label,
        protocol='http', host='127.0.0.1', port=_closed_port(),
        username='trcn', password=SECRET,
    )


FAILED_WARNING = "Connection to '{keys}' failed."
PROMPT = "Press [Enter] to retry or wait 0 seconds."


def test_reachable_sqlite_connects_on_the_first_try(tmp_path, no_child, capsys):
    conn = _sqlite(tmp_path)
    assert retry_connect(conn, max_retries=2, retry_wait=0, print_on_connect=True) is True
    out, err = capsys.readouterr()
    assert "Connected to 'sql:trcn_sqlite'." in out
    assert "Failed connection attempt" not in out
    assert err == ''


def test_reachable_sqlite_is_silent_without_print_on_connect(tmp_path, no_child, capsys):
    conn = _sqlite(tmp_path, 'trcn_sqlite_quiet')
    assert retry_connect(conn, max_retries=1, retry_wait=0) is True
    out, err = capsys.readouterr()
    assert (out, err) == ('', '')


def test_sql_wait_connects_through_the_native_loop(tmp_path, no_child):
    from meerschaum.connectors.sql import SQLConnector
    conn = SQLConnector('trcn_sqlite_wait', flavor='sqlite', database=str(tmp_path / 'w.db'),
                        wait=True)
    assert conn.test_connection() is True


@pytest.mark.parametrize('make, keys', [
    (_dead_sql, 'sql:trcn_pg'),
    (_dead_valkey, 'valkey:trcn_valkey'),
    (_dead_api, 'api:trcn_api'),
])
def test_closed_port_fails_after_max_retries_with_v4_messages(make, keys, no_child, capsys):
    conn = make()
    started = time.monotonic()
    assert retry_connect(conn, max_retries=2, retry_wait=0) is False
    assert time.monotonic() - started < 30
    out, err = capsys.readouterr()
    ### One line per failed attempt, counted from 1, and no attempt past `max_retries`.
    assert "Failed connection attempt (1 / 2)" in out
    assert "Failed connection attempt (2 / 2)" in out
    assert "(3 / 2)" not in out
    assert err.count(FAILED_WARNING.format(keys=keys)) == 2, err
    assert err.count(PROMPT) == 2, err
    ### The connector's attributes never reach a message.
    assert SECRET not in out and SECRET not in err
    if keys != 'api:trcn_api':
        ### v4 printed the driver's error text on standard output, once per failed attempt.
        assert out.count('cannot connect to 127.0.0.1:') == 2, out


@pytest.mark.parametrize('make', [_dead_sql, _dead_valkey, _dead_api])
def test_closed_port_without_warn_prints_nothing(make, no_child, capsys):
    conn = make('trcn_quiet_' + make.__name__.strip('_'))
    assert retry_connect(conn, max_retries=2, retry_wait=0, warn=False) is False
    out, err = capsys.readouterr()
    assert out == ''
    ### An API connector's own login warning is the client's, not the loop's.
    assert FAILED_WARNING.format(keys=str(conn)) not in err


def test_print_on_connect_prints_nothing_for_a_failure(no_child, capsys):
    conn = _dead_valkey('trcn_valkey_pc')
    assert retry_connect(conn, max_retries=1, retry_wait=0, warn=False,
                         print_on_connect=True) is False
    out, _ = capsys.readouterr()
    assert 'Connected to' not in out


def test_zero_retries_make_no_attempt(no_child, capsys):
    conn = _dead_valkey('trcn_valkey_zero')
    assert retry_connect(conn, max_retries=0, retry_wait=0) is False
    assert capsys.readouterr() == ('', '')


def test_workers_adds_the_quit_hint(no_child, capsys):
    conn = _dead_valkey('trcn_valkey_workers')
    assert retry_connect(conn, max_retries=1, retry_wait=0, workers=3) is False
    out, _ = capsys.readouterr()
    assert "To quit, press CTRL-C, then 'q' + [Enter] for each worker (3)." in out
    assert out.index('To quit') < out.index('Failed connection attempt (1 / 1)')


def test_one_worker_has_no_quit_hint(no_child, capsys):
    conn = _dead_valkey('trcn_valkey_worker')
    assert retry_connect(conn, max_retries=1, retry_wait=0, workers=1) is False
    assert 'To quit' not in capsys.readouterr().out


def test_debug_prints_the_attempts(no_child, capsys):
    conn = _dead_valkey('trcn_valkey_debug')
    assert retry_connect(conn, max_retries=2, retry_wait=0, warn=False, debug=True) is False
    _, err = capsys.readouterr()
    assert "Trying to connect to 'valkey:trcn_valkey_debug'..." in err
    assert "Attempt (1 / 2)" in err and "Attempt (2 / 2)" in err


def test_waits_after_every_failure_including_the_last(no_child):
    conn = _dead_valkey('trcn_valkey_wait')
    started = time.monotonic()
    ### stdin is a terminal, a pipe or /dev/null here: the wait is the full 0.4 s each time
    ### (nothing is typed in a test run).
    assert retry_connect(conn, max_retries=2, retry_wait=0.4, warn=False) is False
    assert time.monotonic() - started >= 0.75


def test_no_attributes_on_a_command_line_or_in_a_file(tmp_path, no_child, capsys):
    ### The attributes never leave the process on the native path: nothing is spawned (the
    ### guard fails the test on a child) and no file named like the Python path's is written.
    conn = _sqlite(tmp_path, 'trcn_sqlite_secret', password=SECRET)
    assert conn.meta.get('password') == SECRET
    assert retry_connect(conn, max_retries=1, retry_wait=0, print_on_connect=True) is True
    out, err = capsys.readouterr()
    assert SECRET not in out and SECRET not in err
    for path in pathlib.Path(tempfile.gettempdir()).glob('mrsm-retry-*'):
        assert SECRET not in path.read_text(errors='ignore'), path


# ------------------------------------------------------------------------------------------------
# Reachable servers started here

class _Valkey(socketserver.StreamRequestHandler):
    def handle(self):
        while True:
            line = self.rfile.readline()
            if not line:
                return
            if not line.startswith(b'*'):
                continue
            parts = []
            for _ in range(int(line[1:])):
                size = int(self.rfile.readline()[1:])
                parts.append(self.rfile.read(size + 2)[:-2])
            name = parts[0].upper()
            if name == b'PING':
                self.wfile.write(b'+PONG\r\n')
            elif name == b'HELLO':
                self.wfile.write(b'-ERR unknown command\r\n')
            else:
                self.wfile.write(b'+OK\r\n')
            self.wfile.flush()


class _Server:
    def __init__(self, server):
        self.server = server
        self.port = server.server_address[1]
        self.thread = threading.Thread(target=server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class _ThreadingTCP(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True


class _ThreadingHTTP(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def _api_handler(chaining, login_ok):
    class Handler(http.server.BaseHTTPRequestHandler):
        requests = []

        def log_message(self, *args):
            pass

        def _send(self, code, obj):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = self.path.split('?')[0]
            Handler.requests.append(('GET', path))
            if path == '/chaining':
                return self._send(200, chaining)
            self._send(404, {'detail': 'not found'})

        def do_POST(self):
            self.rfile.read(int(self.headers.get('Content-Length', 0)))
            path = self.path.split('?')[0]
            Handler.requests.append(('POST', path))
            if path == '/login' and login_ok:
                return self._send(200, {
                    'access_token': 'jwt',
                    'expires': '2099-01-01T00:00:00.000000',
                })
            self._send(401, {'detail': 'Incorrect username or password'})

    return Handler


@pytest.fixture
def valkey_server():
    server = _Server(_ThreadingTCP(('127.0.0.1', 0), _Valkey))
    yield server
    server.close()


def _api_server(chaining=True, login_ok=True):
    handler = _api_handler(chaining, login_ok)
    return _Server(_ThreadingHTTP(('127.0.0.1', 0), handler)), handler


def test_reachable_valkey_connects(valkey_server, no_child, capsys):
    conn = mrsm.get_connector('valkey', 'trcn_valkey_ok', host='127.0.0.1', port=valkey_server.port)
    assert retry_connect(conn, max_retries=2, retry_wait=0, print_on_connect=True) is True
    out, err = capsys.readouterr()
    assert "Connected to 'valkey:trcn_valkey_ok'." in out
    assert err == ''
    ### The connection the loop made is the connector's.
    assert conn.test_connection() is True


def test_valkey_that_starts_late_is_picked_up(no_child, capsys):
    port = _closed_port()
    conn = mrsm.get_connector('valkey', 'trcn_valkey_late', host='127.0.0.1', port=port)
    holder = {}

    def start_later():
        time.sleep(0.5)
        holder['server'] = _Server(_ThreadingTCP(('127.0.0.1', port), _Valkey))

    thread = threading.Thread(target=start_later)
    thread.start()
    try:
        assert retry_connect(conn, max_retries=40, retry_wait=0.1, warn=False) is True
    finally:
        thread.join()
        if 'server' in holder:
            holder['server'].close()


def test_reachable_api_connects_and_logs_in(no_child, capsys):
    server, handler = _api_server()
    try:
        conn = mrsm.get_connector(
            'api', 'trcn_api_ok', protocol='http', host='127.0.0.1', port=server.port,
            username='trcn', password=SECRET,
        )
        assert retry_connect(conn, max_retries=2, retry_wait=0, print_on_connect=True) is True
    finally:
        server.close()
    out, err = capsys.readouterr()
    assert "Connected to 'api:trcn_api_ok'." in out
    assert err == ''
    assert ('POST', '/login') in handler.requests


def test_api_without_chaining_is_refused_at_once(no_child, capsys):
    server, handler = _api_server(chaining=False)
    try:
        conn = mrsm.get_connector(
            'api', 'trcn_api_nochain', protocol='http', host='127.0.0.1', port=server.port,
            username='trcn', password=SECRET,
        )
        started = time.monotonic()
        assert retry_connect(conn, max_retries=5, retry_wait=30) is False
        assert time.monotonic() - started < 10, "no wait after a refusal"
    finally:
        server.close()
    out, err = capsys.readouterr()
    assert "does not allow chaining" in err
    assert "Failed connection attempt" not in out
    ### The only login is the one `/chaining`'s token needed: the attempt never logs in itself.
    assert handler.requests.count(('POST', '/login')) == 1


def test_api_without_chaining_is_silent_without_warn(no_child, capsys):
    server, _ = _api_server(chaining=False)
    try:
        conn = mrsm.get_connector(
            'api', 'trcn_api_nochain_q', protocol='http', host='127.0.0.1', port=server.port,
            username='trcn', password=SECRET,
        )
        assert retry_connect(conn, max_retries=2, retry_wait=0, warn=False) is False
    finally:
        server.close()
    assert capsys.readouterr() == ('', '')


def test_enforce_chaining_false_ignores_the_status(no_child, capsys):
    server, handler = _api_server(chaining=False)
    try:
        conn = mrsm.get_connector(
            'api', 'trcn_api_nochain_ok', protocol='http', host='127.0.0.1', port=server.port,
            username='trcn', password=SECRET,
        )
        assert retry_connect(
            conn, max_retries=2, retry_wait=0, enforce_chaining=False, print_on_connect=True,
        ) is True
    finally:
        server.close()
    assert "Connected to 'api:trcn_api_nochain_ok'." in capsys.readouterr().out
    assert ('POST', '/login') in handler.requests


def test_a_failed_login_is_reported_each_attempt(no_child, capsys):
    server, _ = _api_server(login_ok=False)
    try:
        conn = mrsm.get_connector(
            'api', 'trcn_api_badlogin', protocol='http', host='127.0.0.1', port=server.port,
            username='trcn', password=SECRET,
        )
        assert retry_connect(conn, max_retries=2, retry_wait=0) is False
    finally:
        server.close()
    out, err = capsys.readouterr()
    assert err.count("Unable to login to 'api:trcn_api_badlogin'!") == 2, err
    assert err.count("Connection to 'api:trcn_api_badlogin' failed.") == 2, err
    assert "Failed connection attempt (2 / 2)" in out
    assert SECRET not in out and SECRET not in err


def test_enforce_login_false_does_not_log_in(no_child, capsys):
    server, handler = _api_server(login_ok=False)
    try:
        conn = mrsm.get_connector(
            'api', 'trcn_api_nologin', protocol='http', host='127.0.0.1', port=server.port,
            username='trcn', password=SECRET,
        )
        assert retry_connect(
            conn, max_retries=2, retry_wait=0, enforce_login=False, warn=False,
        ) is True
    finally:
        server.close()
    assert "Unable to login" not in capsys.readouterr().err


def test_a_forked_child_runs_its_own_loop(tmp_path, no_child):
    ### A job daemon forks after the parent used the extension's runtime: the child must not
    ### wait on the runtime it inherited (its worker threads are gone), on success or failure.
    conn = _sqlite(tmp_path, 'trcn_fork')
    assert retry_connect(conn, max_retries=1, retry_wait=0, warn=False) is True
    dead = _dead_valkey('trcn_valkey_fork')
    pid = os.fork()
    if pid == 0:
        status = 2
        try:
            signal.alarm(120)
            good = retry_connect(conn, max_retries=1, retry_wait=0, warn=False)
            bad = retry_connect(dead, max_retries=2, retry_wait=0, warn=False)
            status = 0 if (good is True and bad is False) else 1
        finally:
            os._exit(status)
    _, wait_status = os.waitpid(pid, 0)
    assert os.WIFEXITED(wait_status) and os.WEXITSTATUS(wait_status) == 0, wait_status


# ------------------------------------------------------------------------------------------------
# Which path a connector takes

def test_a_plugin_style_connector_keeps_the_child_process_path(monkeypatch):
    from meerschaum.connectors import Connector, make_connector
    from meerschaum.utils import venv as venv_utils

    @make_connector
    class TrcnplugConnector(Connector):
        REQUIRED_ATTRIBUTES = []

        def test_connection(self):
            return True

    conn = mrsm.get_connector('trcnplug', 'x')
    started = []

    def fake_venv_exec(code, venv=None, debug=False, **kw):
        started.append(code)
        return True

    monkeypatch.setattr(venv_utils, 'venv_exec', fake_venv_exec)
    from meerschaum import _core
    monkeypatch.setattr(
        _core, 'retry_connect_native',
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("the native loop was used")),
    )
    assert retry_connect(conn, max_retries=1, retry_wait=0) is True
    assert len(started) == 1


def test_a_driver_that_cannot_be_built_falls_back_with_a_debug_reason(
    tmp_path, monkeypatch, capsys,
):
    from meerschaum.utils import venv as venv_utils
    conn = _sqlite(tmp_path, 'trcn_nodriver')

    def no_driver(discover_schema=True):
        raise RuntimeError("the native library is missing")

    monkeypatch.setattr(conn, '_core_connector', no_driver)
    started = []
    monkeypatch.setattr(
        venv_utils, 'venv_exec', lambda code, venv=None, debug=False, **kw: started.append(1) or True,
    )
    assert retry_connect(conn, max_retries=1, retry_wait=0, debug=True) is True
    assert started == [1]
    _, err = capsys.readouterr()
    assert "keeps to a child process for 'sql:trcn_nodriver': the native library is missing" in err


def test_without_the_binding_the_child_process_path_is_used(tmp_path, monkeypatch):
    from meerschaum import _core
    from meerschaum.utils import venv as venv_utils
    conn = _sqlite(tmp_path, 'trcn_nobinding')
    monkeypatch.delattr(_core, 'retry_connect_native', raising=False)
    started = []
    monkeypatch.setattr(
        venv_utils, 'venv_exec', lambda code, venv=None, debug=False, **kw: started.append(1) or True,
    )
    assert retry_connect(conn, max_retries=1, retry_wait=0) is True
    assert started == [1]


def test_the_binding_refuses_other_objects():
    from meerschaum import _core
    with pytest.raises(TypeError):
        _core.retry_connect_native(
            object(), 'x:y', 1, 0.0, '0', 1, True, False, True, True, True, False,
        )


# ------------------------------------------------------------------------------------------------
# Standard input and Ctrl-C, in a process of their own

_CHILD = """
import sys, meerschaum as mrsm
from meerschaum.connectors.poll import retry_connect
conn = mrsm.get_connector('valkey', 'trcn_stdin', host='127.0.0.1', port={port})
result = retry_connect(conn, max_retries={max_retries}, retry_wait={retry_wait}, warn=True)
sys.stdout.write('RESULT=' + repr(result) + '\\n')
"""


def _run_child(stdin, max_retries, retry_wait, timeout=60):
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith('MRSM_') or key in ('MRSM_ROOT_DIR', 'MRSM_PLUGINS_DIR')
    }
    env['PYTHONPATH'] = os.pathsep.join(
        [str(pathlib.Path(mrsm.__file__).resolve().parent.parent)]
        + ([os.environ['PYTHONPATH']] if os.environ.get('PYTHONPATH') else [])
    )
    code = _CHILD.format(port=_closed_port(), max_retries=max_retries, retry_wait=retry_wait)
    started = time.monotonic()
    proc = subprocess.run(
        [sys.executable, '-c', code], input=stdin, capture_output=True, timeout=timeout,
        env=env,
    )
    return proc, time.monotonic() - started


def test_q_on_standard_input_quits_the_wait():
    proc, elapsed = _run_child(b'q\n', max_retries=3, retry_wait=30)
    out = proc.stdout.decode()
    assert 'RESULT=False' in out, (out, proc.stderr.decode())
    assert 'Failed connection attempt (1 / 3)' in out
    assert 'Failed connection attempt (2 / 3)' not in out
    assert elapsed < 25


def test_enter_on_standard_input_retries_at_once():
    proc, elapsed = _run_child(b'\n\n', max_retries=2, retry_wait=30)
    out = proc.stdout.decode()
    assert 'RESULT=False' in out, (out, proc.stderr.decode())
    assert 'Failed connection attempt (2 / 2)' in out
    assert elapsed < 25


def test_the_end_of_standard_input_prints_an_empty_line_and_sleeps():
    proc, elapsed = _run_child(b'', max_retries=2, retry_wait=1)
    out = proc.stdout.decode()
    assert 'RESULT=False' in out, (out, proc.stderr.decode())
    ### v4 printed the empty prompt of its timed input each wait, then slept the whole time.
    assert out.count('\n\n') >= 1 or '\n\n' in out.replace('\r\n', '\n')
    assert elapsed >= 2


def test_ctrl_c_interrupts_the_wait():
    env = dict(os.environ)
    env['PYTHONPATH'] = os.pathsep.join(
        [str(pathlib.Path(mrsm.__file__).resolve().parent.parent)]
        + ([os.environ['PYTHONPATH']] if os.environ.get('PYTHONPATH') else [])
    )
    code = _CHILD.format(port=_closed_port(), max_retries=5, retry_wait=60)
    proc = subprocess.Popen(
        [sys.executable, '-u', '-c', code],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
    )
    seen = []

    def read_stdout():
        for line in proc.stdout:
            seen.append(line.decode())

    reader = threading.Thread(target=read_stdout, daemon=True)
    reader.start()
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and not any('(1 / 5)' in line for line in seen):
            time.sleep(0.1)
        assert any('(1 / 5)' in line for line in seen), seen
        time.sleep(0.5)
        proc.send_signal(signal.SIGINT)
        started = time.monotonic()
        stderr = proc.stderr.read().decode()
        proc.wait(timeout=20)
        assert time.monotonic() - started < 15
        assert proc.returncode != 0
        assert 'KeyboardInterrupt' in stderr, stderr
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.stdin.close()
        proc.wait()
        reader.join(timeout=5)
        proc.stdout.close()
        proc.stderr.close()
