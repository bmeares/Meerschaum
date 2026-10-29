"""
Wave 18 (SW): what a Python user of `python -m meerschaum` sees from `show data` (pandas-style
truncation, `--all`) and `--nopretty` (a failing command exits 1), against a SQLite instance.

The Rust integration tests (`crates/meerschaum-cli/tests/w18_show.rs`) cover the same behavior
through the standalone binary; these go through the console script path of the wheel.
"""

from __future__ import annotations

import os
import pathlib
import re
import sqlite3
import subprocess
import sys

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent
ANSI = re.compile(r'\x1b\[[0-9;]*[A-Za-z]')


def _mrsm(root: pathlib.Path, *args: str) -> subprocess.CompletedProcess:
    env = {
        **{k: v for k, v in os.environ.items() if not k.startswith('MRSM_')},
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(root / 'root'),
        'MRSM_PLUGINS_DIR': str(root / 'plugins'),
        'MRSM_SQL_W18SW': f"sqlite:///{root / 'show.db'}",
        'MRSM_CONFIG': '{"meerschaum": {"instance": "sql:w18sw"}, "shell": {"updates": {"check": false}}}',
        'COLUMNS': '120',
    }
    if os.environ.get('MRSM_BIN'):
        env['MRSM_BIN'] = os.environ['MRSM_BIN']
    result = subprocess.run(
        [sys.executable, '-P', '-m', 'meerschaum', *args],
        capture_output=True, text=True, env=env, timeout=180, cwd=str(root),
    )
    result.stdout = ANSI.sub('', result.stdout)
    result.stderr = ANSI.sub('', result.stderr)
    return result


@pytest.fixture
def long_pipe(tmp_path):
    (tmp_path / 'root').mkdir()
    (tmp_path / 'plugins').mkdir()
    connection = sqlite3.connect(tmp_path / 'show.db')
    connection.execute('CREATE TABLE src (id INTEGER PRIMARY KEY, dt TEXT, v REAL)')
    connection.executemany(
        'INSERT INTO src VALUES (?, datetime(?, ?), ?)',
        [(i, '2024-01-01', f'+{i} hours', i * 1.5) for i in range(150)],
    )
    connection.commit()
    connection.close()
    params = '{"columns": {"datetime": "dt", "primary": "id"}, "fetch": {"definition": "SELECT * FROM src"}}'
    registered = _mrsm(tmp_path, 'register', 'pipes', '-c', 'sql:w18sw', '-m', 'long', '--params', params)
    assert registered.returncode == 0, registered.stdout + registered.stderr
    synced = _mrsm(tmp_path, 'sync', 'pipes', '-c', 'sql:w18sw', '-m', 'long')
    assert synced.returncode == 0, synced.stdout + synced.stderr
    return tmp_path


def _table_lines(text: str) -> list[str]:
    return [line for line in text.splitlines() if line.startswith('|')]


def test_show_data_truncates_and_all_prints_everything(long_pipe):
    shown = _mrsm(long_pipe, 'show', 'data', 'all', '-m', 'long')
    assert shown.returncode == 0, shown.stdout + shown.stderr
    lines = _table_lines(shown.stdout)
    assert len(lines) == 13, shown.stdout
    assert '…' in lines[7], shown.stdout
    assert '[150 rows x 3 columns (showing 10 rows)]' in shown.stdout, shown.stdout

    everything = _mrsm(long_pipe, 'show', 'data', 'all', '-m', 'long', '--all')
    assert everything.returncode == 0, everything.stdout + everything.stderr
    assert len(_table_lines(everything.stdout)) == 152
    assert '[150 rows x 3 columns]' in everything.stdout


def test_nopretty_exits_nonzero_on_failure(long_pipe):
    ok = _mrsm(long_pipe, 'show', 'pipes', '-m', 'long', '--nopretty')
    assert ok.returncode == 0, ok.stdout + ok.stderr
    failed = _mrsm(long_pipe, 'show', 'sizes', '-m', 'nothing', '--nopretty')
    assert failed.returncode == 1, failed.stdout + failed.stderr
