#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`meerschaum.entry()` decides colors and live progress from Python's streams, not from the
process's file descriptors: `show graph` prints ANSI only when `sys.stdout` is a terminal.

Run with:
    PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \\
        python -m pytest tests/test_entry_progress.py -p no:cacheprovider
"""

import os
import pathlib
import pty
import select
import subprocess
import sys
import time

import pytest

from meerschaum import _core

pytestmark = [
    pytest.mark.skipif(
        _core.load() is None,
        reason="The `_core` extension is not built (scripts/rust/build_py.sh).",
    ),
    pytest.mark.skipif(sys.platform == 'win32', reason="Needs a POSIX pseudo-terminal."),
]

REPO = pathlib.Path(__file__).resolve().parent.parent

SETUP = """
import meerschaum as mrsm
for metric, parent in (('a', None), ('b', 'a')):
    params = {'columns': {'datetime': 'dt'}}
    if parent:
        params['parents'] = [{'connector': 'sql:t', 'metric': parent}]
    success, msg = mrsm.entry(
        ['register', 'pipes', '-i', 'sql:t', '-c', 'sql:t', '-m', metric, '--noask'],
        _patch_args={'params': params},
    )
    assert success, msg
"""

REDIRECTED = """
import io, sys
import meerschaum as mrsm
real = sys.stdout
captured = io.StringIO()
sys.stdout = captured
mrsm.entry(['show', 'graph', '-i', 'sql:t', '-m', 'b', '--parents'])
sys.stdout = real
sys.stdout.write('CAPTURED-BEGIN\\n' + captured.getvalue() + 'CAPTURED-END\\n')
"""

DIRECT = """
import meerschaum as mrsm
mrsm.entry(['show', 'graph', '-i', 'sql:t', '-m', 'b', '--parents'])
"""


def _env(tmp_path: pathlib.Path) -> dict:
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE', 'NO_COLOR')
    }
    plugins = tmp_path / 'plugins'
    plugins.mkdir(exist_ok=True)
    (tmp_path / 'root').mkdir(exist_ok=True)
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_SQL_T': f"sqlite:///{tmp_path / 't.db'}",
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_NOASK': 'true',
        'TERM': 'xterm-256color',
    })
    return env


def _run_on_a_terminal(script: str, env: dict, timeout: float = 60.0) -> str:
    """Run `python -c script` with stdout and stderr on a pseudo-terminal; return its output."""
    pid, fd = pty.fork()
    if pid == 0:  # pragma: no cover (the child)
        os.execvpe(sys.executable, [sys.executable, '-c', script], env)
    chunks = []
    deadline = time.time() + timeout
    while time.time() < deadline:
        ready, _, _ = select.select([fd], [], [], 0.2)
        if not ready:
            continue
        try:
            data = os.read(fd, 65536)
        except OSError:
            break
        if not data:
            break
        chunks.append(data)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0, b''.join(chunks).decode(errors='replace')
    return b''.join(chunks).decode(errors='replace')


@pytest.fixture
def graph_env(tmp_path):
    env = _env(tmp_path)
    result = subprocess.run(
        [sys.executable, '-c', SETUP],
        env=env, capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stderr
    return env


def test_a_redirected_sys_stdout_gets_no_ansi_even_when_fd_1_is_a_terminal(graph_env):
    output = _run_on_a_terminal(REDIRECTED, graph_env)
    captured = output.split('CAPTURED-BEGIN', 1)[1].split('CAPTURED-END', 1)[0]
    assert 'sql:t' in captured and 'b' in captured, output
    assert '\x1b[' not in captured, repr(captured)


def test_a_terminal_sys_stdout_gets_the_colored_graph(graph_env):
    output = _run_on_a_terminal(DIRECT, graph_env)
    assert 'Pipe graph on sql:t' in output, output
    assert '\x1b[' in output, repr(output)
