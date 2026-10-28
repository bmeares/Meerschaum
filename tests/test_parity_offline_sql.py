"""SQL renderer fixtures must block hidden I/O even if the reference swallows errors."""

import json
import os
from pathlib import Path
import subprocess
import sys


def test_offline_guard_blocks_native_checkout_sqlite_and_sockets(tmp_path):
    fixture_dir = (
        Path(__file__).resolve().parents[1]
        / 'crates' / 'meerschaum-connector-sql' / 'tests' / 'fixtures'
    )
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('MRSM_') and key not in (
               'TMUX', 'TMUX_PANE', 'PYTHONPATH', 'PYTHONHOME')}
    plugins = tmp_path / 'plugins'
    plugins.mkdir()
    env.update(MRSM_ROOT_DIR=str(tmp_path), MRSM_PLUGINS_DIR=str(plugins))
    result = subprocess.run(
        [sys.executable, '-I', '-c', '''
import json, pathlib, socket, sqlite3, sys
sys.path.insert(0, sys.argv[1])
import _offline_sql as offline
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
database = pathlib.Path(sys.argv[2]) / 'must-not-exist.db'
marker = database.with_suffix('.creator')
engine = create_engine('sqlite:///' + str(database))
pool = StaticPool(creator=lambda: marker.write_text('unexpected checkout'))
sock = socket.socket()
operations = [
    lambda: sock.connect(('127.0.0.1', 1)),
    lambda: sock.connect_ex(('127.0.0.1', 1)),
    lambda: socket.create_connection(('127.0.0.1', 1)),
    lambda: socket.getaddrinfo('offline-fixture.invalid', 1),
    lambda: sqlite3.connect(database),
    lambda: sqlite3.dbapi2.connect(database),
    engine.connect, engine.raw_connection, pool.connect,
]
try:
    for operation in operations:
        try:
            operation()
        except BaseException as error:
            assert isinstance(error, offline.OfflineSQLIO), type(error)
        else:
            raise AssertionError('I/O guard was bypassed.')
    assert not database.exists() and not marker.exists()
    try:
        offline.assert_no_io()
    except AssertionError as error:
        assert all(name in str(error) for name in (
            'socket.connect', 'socket.connect_ex', 'socket.create_connection',
            'socket.getaddrinfo', 'sqlite3.connect', 'sqlite3.dbapi2.connect',
            'Engine.connect', 'Engine.raw_connection', 'Pool.connect'))
    else:
        raise AssertionError('Swallowed attempts must still fail oracle generation.')
    print(json.dumps({'blocked': len(operations), 'database_created': database.exists()}))
finally:
    sock.close()
''', str(fixture_dir), str(tmp_path)],
        env=env, capture_output=True, text=True, timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout) == {'blocked': 9, 'database_created': False}
