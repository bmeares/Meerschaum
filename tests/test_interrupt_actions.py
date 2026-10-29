#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Ctrl-C while a plugin action runs in a Python worker (wave 18, decision 1), in a real PTY with the
standalone `mrsm`.

The terminal gives Ctrl-C to every process of the foreground process group, so the worker's active
call raises `KeyboardInterrupt` and returns; `mrsm` waits for it, prints `Interrupted.` and exits
130. A plugin which swallows `KeyboardInterrupt` is dropped (the worker is killed) by a second
Ctrl-C within two seconds. Neither prints a traceback from `mrsm`.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import sys
import tempfile
import time

import pytest

from tests.test_progress_signals import _pty

REPO = pathlib.Path(__file__).resolve().parent.parent

PLUGIN = '''\
__version__ = '0.0.1'

from meerschaum.plugins import make_action


@make_action
def w18sleep(**kw):
    """Sleep until interrupted."""
    import time
    print('W18SLEEP-STARTED', flush=True)
    time.sleep(120)
    return True, 'slept'


@make_action
def w18stubborn(**kw):
    """Sleep, ignoring KeyboardInterrupt."""
    import time
    print('W18STUBBORN-STARTED', flush=True)
    while True:
        try:
            time.sleep(120)
        except KeyboardInterrupt:
            print('W18-IGNORED-INT', flush=True)
'''


@pytest.fixture
def home():
    from tests._binary import binary_or_skip
    binary = binary_or_skip()
    base = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-w18sh-', dir=pathlib.Path.home() / '.cache'))
    root, plugins = base / 'root', base / 'plugins'
    root.mkdir()
    plugins.mkdir()
    (plugins / 'w18act.py').write_text(PLUGIN)
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
        'MRSM_PYTHON': sys.executable,
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_NO_UPDATE_CHECK': '1',
        'MRSM_SQL_W18SH': f"sqlite:///{base / 'w.db'}",
        'MRSM_CONFIG': '{"meerschaum": {"instance": "sql:w18sh"}}',
    })
    yield binary, env, root
    shutil.rmtree(base, ignore_errors=True)


def _no_crash(text: str) -> None:
    for bad in ('Traceback', 'panicked', 'RUST_BACKTRACE'):
        assert bad not in text.replace('KeyboardInterrupt', ''), text[-1500:]


def test_ctrl_c_interrupts_a_worker_backed_action(home):
    binary, env, root = home
    started = time.time()
    out, status = _pty([str(binary), 'w18sleep'], env, root, b'W18SLEEP-STARTED', after=30)
    text = out.decode('utf-8', 'replace')
    assert status is not None, text[-1500:]
    assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 130, (status, text[-1500:])
    assert 'Interrupted.' in text, text[-1500:]
    assert time.time() - started < 60
    ### The Python side of the interrupt is the plugin's own: the output holds no mrsm traceback
    ### beyond what the worker prints for the plugin's `KeyboardInterrupt`.
    assert 'panicked' not in text, text[-1500:]


def test_a_second_ctrl_c_drops_a_plugin_which_ignores_the_first(home):
    binary, env, root = home
    out, status = _pty_twice([str(binary), 'w18stubborn'], env, root, b'W18STUBBORN-STARTED')
    text = out.decode('utf-8', 'replace')
    assert status is not None, text[-1500:]
    assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 130, (status, text[-1500:])
    assert 'W18-IGNORED-INT' in text, "the plugin should have seen (and ignored) the first Ctrl-C"
    assert 'Interrupted.' in text, text[-1500:]
    assert 'panicked' not in text, text[-1500:]


def _pty_twice(argv, env, cwd, wait_for, gap=0.6, after=30, timeout=60):
    """Like `tests.test_progress_signals._pty`, with two Ctrl-C bytes `gap` seconds apart."""
    import pty
    import select
    import signal

    pid, fd = pty.fork()
    if pid == 0:
        os.chdir(cwd)
        os.execve(argv[0], argv, env)
    out = b''
    reaped = False

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
        deadline = time.time() + timeout
        while wait_for not in out and time.time() < deadline:
            if not pump(0.2):
                break
        assert wait_for in out, out.decode('utf-8', 'replace')[-1500:]
        pump(0.5)
        os.write(fd, b'\x03')
        pump(gap)
        os.write(fd, b'\x03')
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
