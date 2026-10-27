#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Output styles in `meerschaum.utils.formatting` follow the CLI's rules (rc15, Q14): `NO_COLOR`
turns them off, a non-terminal stdout prints plain text unless the process is a job's or
`FORCE_COLOR` is set, and `formatting:ansi` still has the last word.
"""

import io
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

SCRIPT = (
    "from meerschaum.utils.formatting import ANSI, ansi_allowed, print_tuple\n"
    "from meerschaum.utils.warnings import info, warn\n"
    "print('ANSI=' + str(ANSI))\n"
    "print_tuple((True, 'Synced.'))\n"
    "info('Some information.')\n"
    "warn('A warning.', stack=False)\n"
)


def _run(tmp_path: Path, **env_vars: str) -> subprocess.CompletedProcess:
    """Run the script in a child with stdout and stderr piped (never a terminal)."""
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith('MRSM_')
        and key not in ('NO_COLOR', 'FORCE_COLOR', 'TMUX', 'TMUX_PANE')
    }
    root = tmp_path / 'root'
    root.mkdir(exist_ok=True)
    plugins = tmp_path / 'plugins'
    plugins.mkdir(exist_ok=True)
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
    })
    env.update(env_vars)
    return subprocess.run(
        [sys.executable, '-c', SCRIPT],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(tmp_path),
        timeout=120,
    )


def _has_escape(proc: subprocess.CompletedProcess) -> bool:
    return '\x1b[' in proc.stdout or '\x1b[' in proc.stderr


def _styles_available() -> bool:
    """
    Whether `rich` can be imported: the Python helpers style text with it, and since the base
    wheel installs nothing (and `attempt_import()` never installs `rich`), they print plain text
    without it even where styles are allowed (the CI environment has no `rich`).
    """
    import importlib.util
    return importlib.util.find_spec('rich') is not None


def test_piped_output_is_plain(tmp_path):
    proc = _run(tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert 'ANSI=False' in proc.stdout
    assert 'Synced.' in proc.stdout
    assert not _has_escape(proc), (proc.stdout, proc.stderr)


@pytest.mark.parametrize('variables', [
    {'FORCE_COLOR': '1'},
    {'FORCE_COLOR': 'true'},
    {'MRSM_DAEMON_ID': 'my-job'},
])
def test_styles_when_forced_or_in_a_job(tmp_path, variables):
    proc = _run(tmp_path, **variables)
    assert proc.returncode == 0, proc.stderr
    assert 'ANSI=True' in proc.stdout
    assert ('\x1b[' in proc.stdout) is _styles_available(), proc.stdout


@pytest.mark.parametrize('variables', [
    {'NO_COLOR': '1', 'FORCE_COLOR': '1'},
    {'NO_COLOR': '1', 'MRSM_DAEMON_ID': 'my-job'},
    {'FORCE_COLOR': '0'},
    {'FORCE_COLOR': 'false'},
    {'FORCE_COLOR': '1', 'MRSM_CONFIG': '{"formatting": {"ansi": false}}'},
])
def test_no_color_and_ansi_off_win(tmp_path, variables):
    proc = _run(tmp_path, **variables)
    assert proc.returncode == 0, proc.stderr
    assert 'ANSI=False' in proc.stdout
    assert not _has_escape(proc), (proc.stdout, proc.stderr)


def test_ansi_allowed_reads_the_environment_each_time(monkeypatch):
    from meerschaum.utils import formatting
    monkeypatch.delenv('NO_COLOR', raising=False)
    monkeypatch.delenv('FORCE_COLOR', raising=False)
    monkeypatch.delenv('MRSM_DAEMON_ID', raising=False)
    monkeypatch.setattr(sys, 'stdout', io.StringIO())
    assert formatting.ansi_allowed() is False
    monkeypatch.setenv('FORCE_COLOR', '1')
    assert formatting.ansi_allowed() is True
    monkeypatch.setenv('NO_COLOR', 'yes')
    assert formatting.ansi_allowed() is False
    monkeypatch.setenv('NO_COLOR', '')
    assert formatting.ansi_allowed() is True


def test_get_console_follows_ansi(monkeypatch):
    rich = pytest.importorskip('rich')
    from meerschaum.utils import formatting
    monkeypatch.delenv('NO_COLOR', raising=False)
    monkeypatch.setenv('FORCE_COLOR', '1')
    styled = formatting.get_console()
    assert styled is not None and styled.is_terminal
    monkeypatch.setenv('NO_COLOR', '1')
    plain = formatting.get_console()
    assert plain is not None and not plain.is_terminal
    with plain.capture() as captured:
        plain.print('[bold red]text[/bold red]')
    assert '\x1b[' not in captured.get()
    assert formatting.colored('text', 'red') == 'text'


class _Stream(io.StringIO):
    """A text stream that says whether it is a terminal."""

    def __init__(self, terminal: bool):
        super().__init__()
        self._terminal = terminal

    def isatty(self) -> bool:
        return self._terminal


def test_warnings_follow_stderr_not_stdout(monkeypatch):
    """`2> errors.log` gets plain warnings although stdout is a terminal, and the reverse."""
    from meerschaum.utils import formatting
    from meerschaum.utils.warnings import warn
    import warnings
    for key in ('NO_COLOR', 'FORCE_COLOR', 'MRSM_DAEMON_ID'):
        monkeypatch.delenv(key, raising=False)
    for stdout_terminal, stderr_terminal in ((True, False), (False, True)):
        out, err = _Stream(stdout_terminal), _Stream(stderr_terminal)
        monkeypatch.setattr(sys, 'stdout', out)
        monkeypatch.setattr(sys, 'stderr', err)
        assert formatting.ansi_allowed() is stdout_terminal
        assert formatting.ansi_allowed(sys.stderr) is stderr_terminal
        assert formatting.stderr_ansi() is stderr_terminal
        ### `meerschaum.utils.warnings` shows every `UserWarning` (`always`) from its import, but
        ### pytest drops filters set during an earlier test: the repeated warning would be shown
        ### once only.
        with warnings.catch_warnings():
            warnings.simplefilter('always', UserWarning)
            warn('A warning on stderr.', stack=False)
        text = err.getvalue()
        assert 'A warning on stderr.' in text
        assert ('\x1b[' in text) is (stderr_terminal and _styles_available()), repr(text)


def test_entry_in_a_notebook_is_styled(monkeypatch):
    """
    `mrsm.entry()` styles its output in a Jupyter notebook, whose streams are not terminals,
    as `meerschaum.utils.formatting` does; elsewhere off a terminal it prints plain text.
    """
    import meerschaum as mrsm
    from meerschaum.utils import formatting
    for key in ('NO_COLOR', 'FORCE_COLOR', 'MRSM_DAEMON_ID'):
        monkeypatch.delenv(key, raising=False)
    for notebook in (True, False):
        out = _Stream(False)
        monkeypatch.setattr(sys, 'stdout', out)
        monkeypatch.setattr(formatting, '_in_notebook', lambda notebook=notebook: notebook)
        mrsm.entry(['show', 'version'])
        text = out.getvalue()
        assert 'Meerschaum v' in text, text
        assert ('\x1b[' in text) is notebook, repr(text)
