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


def test_instance_renderer_needs_no_database_driver_or_engine(tmp_path):
    from tests._binary import skip_or_fail

    repo = Path(__file__).resolve().parents[1]
    reference = Path(os.environ.get('MRSM_TEST_PYTHON', repo / '.venv-ref/bin/python'))
    if not reference.is_file():
        skip_or_fail('The released v4 reference is required; run scripts/parity/setup_reference.sh.')
    fixture_dir = repo / 'crates' / 'meerschaum-connector-sql' / 'tests' / 'fixtures'
    plugins = tmp_path / 'plugins'
    plugins.mkdir()
    venvs = tmp_path / 'venvs'
    venvs.mkdir()
    (venvs / 'mrsm').symlink_to(reference.parent.parent, target_is_directory=True)
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('MRSM_') and key not in (
               'TMUX', 'TMUX_PANE', 'PYTHONPATH', 'PYTHONHOME')}
    env.update(MRSM_ROOT_DIR=str(tmp_path), MRSM_PLUGINS_DIR=str(plugins),
               MRSM_VENVS_DIR=str(venvs), UV_PYTHON=str(reference),
               MRSM_NO_AUTO_INSTALL='1', MRSM_NO_UPDATE_CHECK='1')
    result = subprocess.run(
        [str(reference), '-I', '-c', '''
import importlib.abc, importlib.util, json, pathlib, sys
sys.path.insert(0, sys.argv[1])
import _offline_sql as offline
import sqlalchemy
attempts = []
def forbidden(name):
    attempts.append(name)
    raise offline.OfflineSQLIO('Unexpected engine or DBAPI: ' + name)
class NoDrivers(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if name.split('.')[0] in ('psycopg', 'psycopg2', 'pymysql', 'MySQLdb'):
            forbidden(name)
sqlalchemy.create_engine = lambda *args, **kwargs: forbidden('create_engine')
sys.meta_path.insert(0, NoDrivers())
import meerschaum as mrsm
assert mrsm.__version__ == '4.1.4', mrsm.__file__
source = pathlib.Path(sys.argv[1]) / 'gen_instance_queries.py'
spec = importlib.util.spec_from_file_location('instance_renderer', source)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
module.OUT = str(pathlib.Path(sys.argv[2]) / 'rendered.json')
module.main()
offline.assert_no_io()
assert not attempts, attempts
expected = json.loads(source.with_name('instance_queries.json').read_text())
actual = json.loads(pathlib.Path(module.OUT).read_text())
assert actual['cases'] == expected['cases'], 'The SQL oracle changed.'
print('DRIVER_FREE=' + json.dumps({'cases': len(actual['cases']), 'attempts': attempts}))
''', str(fixture_dir), str(tmp_path)],
        env=env, cwd=tmp_path, capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    output = next(line for line in result.stdout.splitlines() if line.startswith('DRIVER_FREE='))
    assert json.loads(output.removeprefix('DRIVER_FREE=')) == {'cases': 2742, 'attempts': []}
