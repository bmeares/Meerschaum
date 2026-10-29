"""
Wave 18 (DECISIONS items 23 and 24): connections fail fast with a clear message, and
`MRSM_<TYPE>_<LABEL>` variables of an unknown connector type are ignored.

Needs the built extension. No database: a closed local port and an unroutable address.
"""

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

import meerschaum as mrsm  # noqa: E402

REPO = Path(__file__).resolve().parent.parent


def _closed_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def test_a_refused_connection_fails_at_once_with_a_clear_message():
    port = _closed_port()
    conn = mrsm.get_connector(
        'sql', 'w18cn_refused', uri=f'postgresql://scratch:not-a-secret@127.0.0.1:{port}/scratch',
    )
    started = time.monotonic()
    with pytest.raises(Exception) as error:
        conn._rust_method('get_users')()
    elapsed = time.monotonic() - started
    assert (
        f"sql:w18cn_refused: cannot connect to 127.0.0.1:{port} (connection refused). "
        "Is the database running?"
    ) in str(error.value)
    assert elapsed < 3, elapsed
    assert 'not-a-secret' not in str(error.value)


def test_an_unresponsive_host_is_bounded_by_connect_timeout():
    conn = mrsm.get_connector(
        'sql', 'w18cn_unroutable',
        uri='postgresql://scratch:not-a-secret@10.255.255.1:5432/scratch?connect_timeout=1',
    )
    started = time.monotonic()
    with pytest.raises(Exception) as error:
        conn._rust_method('get_users')()
    elapsed = time.monotonic() - started
    message = str(error.value)
    assert "sql:w18cn_unroutable: cannot connect to 10.255.255.1:5432" in message, message
    assert 'no response within 1 s' in message or 'network unreachable' in message, message
    assert elapsed < 4, elapsed


CHILD = """
import meerschaum as mrsm  # noqa: E402
connectors = mrsm.get_config('meerschaum', 'connectors')
print('TYPES', sorted(connectors))
print('SQL', sorted(connectors.get('sql', {})))
"""


def test_variables_of_an_unknown_connector_type_are_ignored(tmp_path):
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_SQL_W18CN_REAL': f'sqlite:///{tmp_path / "real.db"}',
        'MRSM_W18CN_NOISE': '1',
        'MRSM_CI_MIN_FREE_GB': '10',
        'MRSM_DUCKDB_LIB_NOT': 'x',
    })
    (tmp_path / 'plugins').mkdir()
    (tmp_path / 'root').mkdir()
    result = subprocess.run(
        [sys.executable, '-c', CHILD],
        capture_output=True, text=True, env=env, cwd=str(tmp_path), timeout=120,
    )
    assert result.returncode == 0, result.stderr
    lines = dict(line.split(' ', 1) for line in result.stdout.splitlines() if ' ' in line)
    assert 'w18cn_real' in lines['SQL'], result.stdout
    for name in ('w18cn', 'ci', 'duckdb'):
        assert f"'{name}'" not in lines['TYPES'], result.stdout
