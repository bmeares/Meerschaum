"""
`MRSM_SQL_<LABEL>` URIs decide where the connection goes, like v4's engine (wave 17b).

A fresh root's default `sql:main` points at `localhost:5432` (a `timescaledb-ha` config) and
`sql:local` at SQLite. Setting `MRSM_SQL_MAIN=<uri>` merges only the `uri` into that config, so
the connector keeps the default's `host` / `port` / `flavor` as attributes. The connection (and,
for a URI of another driver family, the SQL dialect) must follow the URI.

Every test runs a child process with a scratch root and forces the default `sql:main` attributes to
a local trap socket, which counts connections: nothing may ever dial it (and never port 5432).
"""

import json
import os
import socket
import sqlite3
import subprocess
import sys
import threading
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

CHILD = r'''
import json
import meerschaum as mrsm

label = {label!r}
# Both URI-label cases may run concurrently against the same owned PostgreSQL
# database. Keep their registrations and cleanup targets independent.
pipe = mrsm.Pipe('test', 'w17b_env_uri', label, instance=f'sql:{{label}}')
out = {{}}
try:
    conn = pipe.instance_connector
    out['flavor'] = conn.flavor
    out['rust_flavor'] = conn._core_connector().flavor
    out['uri_set'] = bool(conn.__dict__.get('uri'))
    ok, msg = pipe.sync([{{'dt': '2024-01-01', 'v': 1}}, {{'dt': '2024-01-02', 'v': 2}}], debug=False)
    out['sync'] = [bool(ok), str(msg)[:200]]
    out['rowcount'] = int(pipe.get_rowcount())
    out['target'] = pipe.target
finally:
    if {cleanup!r}:
        pipe.delete()
print('RESULT ' + json.dumps(out))
'''


class Trap:
    """A local listener that counts the connections made to it."""

    def __init__(self):
        self.sock = socket.socket()
        self.sock.bind(('127.0.0.1', 0))
        self.sock.listen(16)
        self.port = self.sock.getsockname()[1]
        self.hits = 0
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            self.hits += 1
            conn.close()

    def close(self):
        self.sock.close()


@pytest.fixture
def trap():
    t = Trap()
    yield t
    t.close()


def run_child(tmp_path, trap, label, uri, cleanup=False):
    root = tmp_path / 'root'
    plugins = tmp_path / 'plugins'
    root.mkdir()
    plugins.mkdir()
    # The default `sql:main` (a PostgreSQL-family config) must never reach a real server.
    config = {
        'meerschaum': {
            'connectors': {'sql': {'main': {'host': '127.0.0.1', 'port': trap.port}}},
        },
    }
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_CONFIG': json.dumps(config),
        f'MRSM_SQL_{label.upper()}': uri,
    })
    proc = subprocess.run(
        [sys.executable, '-s', '-c', CHILD.format(label=label, cleanup=cleanup)],
        env=env, cwd=str(tmp_path), capture_output=True, text=True, timeout=300,
    )
    lines = [l for l in proc.stdout.splitlines() if l.startswith('RESULT ')]
    assert lines, f"child failed ({proc.returncode}):\n{proc.stdout[-2000:]}\n{proc.stderr[-2000:]}"
    return json.loads(lines[0][len('RESULT '):])


def test_main_with_a_sqlite_uri_writes_the_file_and_dials_nothing(tmp_path, trap):
    db = tmp_path / 'main.db'
    result = run_child(tmp_path, trap, 'main', f'sqlite:///{db}')
    assert result['sync'][0], result
    assert result['rowcount'] == 2
    # Another driver family than the default `timescaledb-ha`: the dialect follows the URI.
    assert result['flavor'] == 'sqlite'
    assert result['rust_flavor'] == 'sqlite'
    assert trap.hits == 0, "the default sql:main host was dialed"
    with sqlite3.connect(db) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master")}
        assert result['target'] in tables
        assert conn.execute(f'SELECT COUNT(*) FROM "{result["target"]}"').fetchone()[0] == 2


def test_local_with_a_sqlite_uri_uses_that_file(tmp_path, trap):
    db = tmp_path / 'local.db'
    result = run_child(tmp_path, trap, 'local', f'sqlite:///{db}')
    assert result['sync'][0], result
    assert result['rowcount'] == 2
    assert trap.hits == 0
    with sqlite3.connect(db) as conn:
        assert conn.execute(f'SELECT COUNT(*) FROM "{result["target"]}"').fetchone()[0] == 2


def test_custom_label_with_a_sqlite_uri(tmp_path, trap):
    db = tmp_path / 'custom.db'
    result = run_child(tmp_path, trap, 'w17bcust', f'sqlite:///{db}')
    assert result['sync'][0], result
    assert result['flavor'] == 'sqlite'
    assert trap.hits == 0
    with sqlite3.connect(db) as conn:
        assert conn.execute(f'SELECT COUNT(*) FROM "{result["target"]}"').fetchone()[0] == 2


def _server_uri():
    from tests.connectors import conns
    conn = conns['timescaledb']
    return (
        f"postgresql://{conn.username}:{conn.password}@{conn.host}:{conn.port}/{conn.database}"
    )


@pytest.mark.parametrize('label', ['main', 'w17bcust'])
def test_postgresql_uri_reaches_the_test_container(tmp_path, trap, label):
    if 'timescaledb' not in os.environ.get('MRSM_TEST_FLAVORS', 'sqlite').split(','):
        pytest.skip("set MRSM_TEST_FLAVORS to include timescaledb")
    result = run_child(tmp_path, trap, label, _server_uri(), cleanup=True)
    assert result['sync'][0], result
    assert result['rowcount'] == 2
    # The same driver family keeps the configured flavor on `main` (v4 parity).
    assert result['flavor'] == ('timescaledb-ha' if label == 'main' else 'postgresql')
    assert result['rust_flavor'] == result['flavor']
    assert trap.hits == 0, "the default sql:main host was dialed"
