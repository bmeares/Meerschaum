#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The CLI progress spinner and the process's signals (wave 17b, PARITY W17T-003), in real PTYs:

- `mrsm.entry()` in an embedding Python process: Ctrl-C while the spinner is drawn raises
  `KeyboardInterrupt` and the interpreter keeps running (the spinner's signal guard belongs to the
  `mrsm` program only);
- `python -m meerschaum` and the standalone binary: Ctrl-C cancels the action (wave 18, decision 1):
  the spinner is cleared, `Interrupted.` is printed and the exit status is 130; SIGTERM still ends
  the process by the signal;
- `start api`: the server keeps its own graceful shutdown on SIGINT.
"""

from __future__ import annotations

import os
import pathlib
import pty
import select
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent
VIEW = (
    "CREATE VIEW slowv AS WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL "
    "SELECT x+1 FROM c WHERE x<300000000) SELECT x AS id FROM c"
)
MIDVIEW = (
    "CREATE VIEW midv AS WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL "
    "SELECT x+1 FROM c WHERE x<6000000) SELECT x AS id FROM c LIMIT 5"
)
CLEAR = b'\r\x1b[2K'
MID_ARGS = ['show', 'data', '-c', 'plugin:y', '-m', 'mid', '-l', 'None', '-i', 'sql:w17bp']
ARGS = ['show', 'data', '-c', 'plugin:x', '-m', 'slow', '-l', 'None', '-i', 'sql:w17bp']


@pytest.fixture
def home():
    base = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-w17bp-', dir=pathlib.Path.home() / '.cache'))
    root, plugins = base / 'root', base / 'plugins'
    root.mkdir()
    plugins.mkdir()
    db = base / 'w.db'
    con = sqlite3.connect(db)
    con.execute(VIEW)
    con.execute(MIDVIEW)
    con.commit()
    con.close()
    env = {
        key: val for key, val in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('PYTHONPATH', 'TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'HOME': str(base),
        'TERM': 'xterm-256color',
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_NO_UPDATE_CHECK': '1',
        'MRSM_SQL_W17BP': f"sqlite:///{db}",
    })
    result = subprocess.run(
        [sys.executable, '-m', 'meerschaum', 'register', 'pipes', '-c', 'plugin:x', '-m', 'slow',
         '-l', 'None', '-i', 'sql:w17bp', '--params', '{"target":"slowv","columns":{"id":"id"}}',
         '--noask'],
        env=env, cwd=str(root), capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    result = subprocess.run(
        [sys.executable, '-m', 'meerschaum', 'register', 'pipes', '-c', 'plugin:y', '-m', 'mid',
         '-l', 'None', '-i', 'sql:w17bp', '--params', '{"target":"midv","columns":{"id":"id"}}',
         '--noask'],
        env=env, cwd=str(root), capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    yield env, root
    shutil.rmtree(base, ignore_errors=True)


def _pty(argv, env, cwd, wait_for, signum=None, byte=b'\x03', after=4.0, timeout=60, quiet=0.0):
    """Run `argv` in a PTY until `wait_for` is in its output, interrupt it, collect the rest."""
    pid, fd = pty.fork()
    if pid == 0:
        os.chdir(cwd)
        os.execve(argv[0], argv, env)
    out = b''
    reaped = False
    deadline = time.time() + timeout

    def pump(seconds):
        nonlocal out
        end = time.time() + seconds
        while time.time() < end:
            ready, _, _ = select.select([fd], [], [], 0.1)
            if ready:
                try:
                    data = os.read(fd, 65536)
                except OSError:
                    return False
                if not data:
                    return False
                out += data
        return True

    try:
        def ready():
            return wait_for() if callable(wait_for) else wait_for in out

        while not ready() and time.time() < deadline:
            if not pump(0.2):
                break
        assert ready(), out.decode('utf-8', 'replace')[-1500:]
        if quiet:
            ### The spinner has ended and the process is in a later phase: wait for silence.
            last, seen = time.time(), len(out)
            while time.time() < deadline and time.time() - last < quiet:
                pump(0.2)
                if len(out) != seen:
                    last, seen = time.time(), len(out)
        if signum is None:
            os.write(fd, byte)
        else:
            os.kill(pid, signum)
        end = time.time() + after
        status = None
        while time.time() < end:
            pump(0.2)
            done, st = os.waitpid(pid, os.WNOHANG)
            if done:
                reaped = True
                status = st
                pump(0.3)
                break
        return out, status
    finally:
        if not reaped:
            try:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
            except (OSError, ChildProcessError):
                pass
        os.close(fd)


def _after_last_frame(out: bytes) -> bytes:
    return out[out.rfind(b'Reading data'):]


def test_entry_raises_keyboard_interrupt_and_the_interpreter_keeps_running(home):
    env, root = home
    code = (
        "import meerschaum as mrsm, sys\n"
        "try:\n"
        f"    mrsm.entry({ARGS!r})\n"
        "    print('FINISHED', flush=True)\n"
        "except KeyboardInterrupt:\n"
        "    print('KEYBOARDINTERRUPT', flush=True)\n"
        "print('ALIVE', flush=True)\n"
    )
    out, status = _pty([sys.executable, '-c', code], env, root, b'Reading data', after=20)
    text = out.decode('utf-8', 'replace')
    assert status is not None, text[-1500:]
    assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, (status, text[-1500:])
    assert 'KEYBOARDINTERRUPT' in text and 'ALIVE' in text, text[-1500:]


def _assert_interrupted(out, status):
    text = out.decode('utf-8', 'replace')
    assert status is not None, text[-1500:]
    assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 130, (status, text[-1500:])
    assert 'Interrupted.' in text, text[-1500:]
    assert 'Traceback' not in text and 'panicked' not in text, text[-1500:]
    assert CLEAR in _after_last_frame(out)


def test_the_python_console_script_clears_the_spinner_and_exits_130(home):
    env, root = home
    out, status = _pty(
        [sys.executable, '-m', 'meerschaum', *ARGS], env, root, b'Reading data',
    )
    _assert_interrupted(out, status)


def test_the_standalone_binary_clears_the_spinner_and_exits_130(home):
    from tests._binary import binary_or_skip
    binary = binary_or_skip()
    env, root = home
    out, status = _pty([str(binary), *ARGS], env, root, b'Reading data')
    _assert_interrupted(out, status)


@pytest.mark.parametrize('readiness', ['tcp', 'http'])
@pytest.mark.parametrize('signum', [signal.SIGINT, signal.SIGTERM], ids=['sigint', 'sigterm'])
def test_start_api_keeps_its_graceful_shutdown_on_signal(home, readiness, signum):
    from tests._binary import binary_or_skip
    from tests.console.conftest import free_port
    binary = binary_or_skip()
    env, root = home
    port = free_port()

    def listening():
        import socket
        with socket.socket() as probe:
            probe.settimeout(0.5)
            return probe.connect_ex(('127.0.0.1', port)) == 0

    def healthy():
        import http.client
        connection = http.client.HTTPConnection('127.0.0.1', port, timeout=0.5)
        try:
            connection.request('GET', '/healthcheck')
            response = connection.getresponse()
            response.read()
            return response.status == 200
        except (OSError, http.client.HTTPException):
            return False
        finally:
            connection.close()

    out, status = _pty(
        [str(binary), 'start', 'api', '-i', 'sql:w17bp', '--host', '127.0.0.1', '--port',
         str(port), '--no-webterm'],
        env, root, listening if readiness == 'tcp' else healthy, signum=signum, after=30,
    )
    text = out.decode('utf-8', 'replace')[-1500:]
    assert status is not None, text
    assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, (status, text)
    assert not listening(), "the owned API listener survived server shutdown"


def _sleeps_after_the_spinner(argv_prefix, env, root, signum):
    """A spinner is drawn and ENDS, then the process sleeps: the signal must still end it."""
    out, status = _pty(
        [*argv_prefix, *MID_ARGS, '+', 'sleep', '60'], env, root, b'Reading data',
        signum=signum, after=8, quiet=1.5,
    )
    text = out.decode('utf-8', 'replace')
    assert status is not None, "the process survived the signal:\n" + text[-1500:]
    if signum == signal.SIGINT:
        ### Sent to the process alone, SIGINT is forwarded to the program it waits for (`sleep`),
        ### which ends; the action is then reported as interrupted.
        assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 130, (status, text[-800:])
        assert 'Interrupted.' in text, text[-800:]
    else:
        assert os.WIFSIGNALED(status), (status, text[-800:])


@pytest.mark.parametrize('signum', [signal.SIGINT, signal.SIGTERM], ids=['sigint', 'sigterm'])
def test_a_signal_still_ends_the_console_script_after_a_spinner_ended(home, signum):
    env, root = home
    _sleeps_after_the_spinner([sys.executable, '-m', 'meerschaum'], env, root, signum)


@pytest.mark.parametrize('signum', [signal.SIGINT, signal.SIGTERM], ids=['sigint', 'sigterm'])
def test_a_signal_still_ends_the_binary_after_a_spinner_ended(home, signum):
    from tests._binary import binary_or_skip
    env, root = home
    _sleeps_after_the_spinner([str(binary_or_skip())], env, root, signum)
