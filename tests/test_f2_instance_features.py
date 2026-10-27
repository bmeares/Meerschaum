"""
rc15 instance features through the Python API, on throwaway SQLite instances:

- database errors raise (`SQLConnector.read/exec/value`, `pipe.get_data/get_rowcount/get_sync_time`)
  and `silent=True` keeps Meerschaum 4's `None`; a never-synced pipe still reads as empty;
- `pipe.get_rowcount(estimate=True)`;
- protected connectors (`protected: true`, `allow_protected()`);
- `pipe.verify_schema()`;
- Meerschaum's internal `mrsm` pipes stay out of `mrsm.get_pipes()`.
"""

import os
import socket
import sqlite3
import uuid
import warnings

import pytest

import meerschaum as mrsm
from meerschaum.connectors import allow_protected


def _label(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def conn(tmp_path):
    return mrsm.get_connector(
        'sql', _label('f2'),
        flavor='sqlite', database=str(tmp_path / 'f2.db'),
    )


@pytest.fixture(autouse=True)
def _no_inherited_allowance(monkeypatch):
    monkeypatch.delenv('MRSM_ALLOW_PROTECTED', raising=False)


def _closed_port() -> int:
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def _sqlalchemy_available() -> bool:
    try:
        import sqlalchemy  # noqa: F401
        return True
    except ImportError:
        return False


@pytest.mark.skipif(not _sqlalchemy_available(), reason="needs the `sql` extra")
def test_sql_helpers_raise_database_errors_unless_silent(conn):
    with pytest.raises(Exception, match='no such table'):
        conn.read('SELECT * FROM f2_missing_table')
    with pytest.raises(Exception, match='no such table'):
        conn.value('SELECT COUNT(*) FROM f2_missing_table')
    with pytest.raises(Exception, match='no such table'):
        conn.exec('DELETE FROM f2_missing_table')
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        assert conn.read('SELECT * FROM f2_missing_table', silent=True) is None
        assert conn.value('SELECT COUNT(*) FROM f2_missing_table', silent=True) is None
        assert conn.exec('DELETE FROM f2_missing_table', silent=True) is None
    assert conn.value('SELECT 1') == 1
    ### A query without rows has no value (not an error).
    conn.exec('CREATE TABLE f2_empty (a INTEGER)')
    assert conn.value('SELECT a FROM f2_empty') is None


def test_reads_of_a_never_synced_pipe_are_empty(conn):
    pipe = mrsm.Pipe('f2', 'never_synced', instance=conn, columns={'datetime': 'ts'})
    assert pipe.get_data() is None
    assert pipe.get_data(as_docs=True) == []
    assert pipe.get_rowcount() == 0
    assert pipe.get_rowcount(estimate=True) == 0
    assert pipe.get_sync_time() is None


def test_reads_of_an_unreachable_instance_raise_unless_silent():
    dead = mrsm.get_connector(
        'sql', _label('f2_dead'),
        flavor='postgresql', host='127.0.0.1', port=_closed_port(),
        username='nobody', password='not-a-secret', database='nodb', schema='public',
    )
    pipe = mrsm.Pipe('f2', 'dead', instance=dead, columns={'datetime': 'ts'})
    with pytest.raises(Exception):
        pipe.get_rowcount()
    with pytest.raises(Exception):
        pipe.get_sync_time()
    with pytest.raises(Exception):
        pipe.get_data()
    with pytest.raises(Exception):
        pipe.get_data(as_docs=True)
    assert pipe.get_rowcount(silent=True) == 0
    assert pipe.get_sync_time(silent=True) is None
    assert pipe.get_data(silent=True) is None


def test_row_estimates_are_exact_on_sqlite(conn):
    pipe = mrsm.Pipe('f2', 'estimate', instance=conn, columns={'datetime': 'ts'})
    success, msg = pipe.sync([{'ts': '2024-01-01', 'v': 1}, {'ts': '2024-01-02', 'v': 2}])
    assert success, msg
    assert pipe.get_rowcount(estimate=True) == 2
    assert pipe.get_rowcount(estimate=True, begin='2024-01-02') == 1


def test_protected_connectors_refuse_writes(tmp_path):
    database = str(tmp_path / 'protected.db')
    plain = mrsm.get_connector('sql', _label('f2_plain'), flavor='sqlite', database=database)
    seed = mrsm.Pipe('f2', 'protected', instance=plain, columns={'datetime': 'ts'})
    success, msg = seed.sync([{'ts': '2024-01-01', 'v': 1}])
    assert success, msg

    prot = mrsm.get_connector(
        'sql', _label('f2_prot'), flavor='sqlite', database=database, protected=True,
    )
    assert prot.protected is True
    assert plain.protected is False
    pipe = mrsm.Pipe('f2', 'protected', instance=prot, columns={'datetime': 'ts'})

    ### Reads are never blocked.
    assert pipe.get_rowcount() == 1

    for op, call in (
        ('sync', lambda: pipe.sync([{'ts': '2024-01-02', 'v': 2}])),
        ('clear', lambda: pipe.clear()),
        ('drop', lambda: pipe.drop()),
    ):
        success, msg = call()
        assert not success, op
        assert f"Refusing to {op} " in msg, msg
        assert f"on protected instance '{prot}'" in msg, msg
        assert msg.endswith('Pass --allow-protected (or set MRSM_ALLOW_PROTECTED=1).'), msg
    assert pipe.get_rowcount() == 1

    if _sqlalchemy_available():
        with pytest.raises(PermissionError, match='Refusing to execute SQL'):
            prot.exec('DELETE FROM "f2_protected"')
        assert prot.exec('DELETE FROM "f2_protected"', silent=True) is None
        assert prot.value('SELECT COUNT(*) FROM "f2_protected"') == 1

    with allow_protected():
        success, msg = pipe.sync([{'ts': '2024-01-02', 'v': 2}])
        assert success, msg
    assert pipe.get_rowcount() == 2
    success, msg = pipe.clear()
    assert not success


def test_protected_from_uri_and_environment(tmp_path, monkeypatch):
    database = tmp_path / 'uri.db'
    prot = mrsm.get_connector(
        'sql', _label('f2_uri'), uri=f"sqlite:///{database}?protected=true",
    )
    assert prot.protected is True
    assert not prot.__dict__['uri'].endswith('?protected=true')
    pipe = mrsm.Pipe('f2', 'uri', instance=prot)
    success, msg = pipe.sync([{'a': 1}])
    assert not success and 'protected instance' in msg
    monkeypatch.setenv('MRSM_ALLOW_PROTECTED', '1')
    success, msg = pipe.sync([{'a': 1}])
    assert success, msg


def test_verify_schema(conn):
    pipe = mrsm.Pipe(
        'f2', 'schema', instance=conn,
        columns={'datetime': 'ts', 'id': 'station'},
    )
    success, msg = pipe.sync([
        {'ts': '2024-01-01', 'station': 'A', 'temp': 10, 'code': '1'},
        {'ts': '2024-01-02', 'station': 'B', 'temp': 20, 'code': '2'},
    ])
    assert success, msg
    success, msg = pipe.verify_schema()
    assert success, msg
    assert 'matches its dtypes' in msg

    pipe.dtypes = {'temp': 'float', 'code': 'int', 'note': 'string'}
    pipe.edit()
    success, plan = pipe.verify_schema()
    assert success, plan
    assert 'note' in plan and 'add column' in plan
    assert 'rebuilds the table' in plan

    success, sql = pipe.verify_schema(dry_run=True)
    assert success
    assert 'CREATE TABLE' in sql and 'RENAME TO' in sql
    assert pipe.get_columns_types(refresh=True)['temp'] != 'FLOAT'

    success, msg = pipe.verify_schema(apply=True)
    assert success, msg
    types = pipe.get_columns_types(refresh=True)
    assert types['temp'] == 'FLOAT', types
    assert types['code'] == 'BIGINT', types
    assert 'note' in types
    df = pipe.get_data()
    assert len(df) == 2
    assert sorted(df['code'].tolist()) == [1, 2]
    success, msg = pipe.verify_schema()
    assert success and 'matches its dtypes' in msg


def test_internal_pipes_are_hidden_from_get_pipes(conn):
    user = mrsm.Pipe('f2', 'visible', instance=conn)
    user.register()
    registry = mrsm.Pipe('mrsm', 'pipeline', 'f2_daily', instance=conn)
    registry.register()
    listed = mrsm.get_pipes(instance=conn, as_list=True)
    assert user in listed
    assert all(pipe.connector_keys != 'mrsm' for pipe in listed)
    internal = mrsm.get_pipes(connector_keys=['mrsm'], instance=conn, as_list=True)
    assert any(pipe.location_key == 'f2_daily' for pipe in internal)


def test_protected_custom_instances_refuse_writes_in_the_worker():
    """The worker refuses the write methods of a protected custom instance connector."""
    from meerschaum.connectors import make_connector, Connector
    from meerschaum.worker import _ops

    calls = []

    @make_connector
    class F2protConnector(Connector):
        IS_INSTANCE = True

        def register_pipe(self, pipe, **kwargs):
            calls.append('register_pipe')
            return True, 'registered'

        def get_pipe_attributes(self, pipe, **kwargs):
            calls.append('get_pipe_attributes')
            return {}

    label = _label('w')
    mrsm.get_connector('f2prot', label, protected=True)
    keys = f'f2prot:{label}'
    pipe_state = {'connector_keys': 'plugin:x', 'metric_key': 'y', 'instance_keys': keys}

    value, _ = _ops.op_instance(None, 0, {
        'connector': keys, 'method': 'register_pipe', 'pipe': pipe_state,
    }, [])
    assert value[0] is False
    assert value[1].startswith("Refusing to register Pipe('plugin:x', 'y'"), value
    assert calls == []

    ### Reads are not refused, and the allowance lets writes through.
    _ops.op_instance(None, 0, {
        'connector': keys, 'method': 'get_pipe_attributes', 'pipe': pipe_state,
    }, [])
    with allow_protected():
        value, _ = _ops.op_instance(None, 0, {
            'connector': keys, 'method': 'register_pipe', 'pipe': pipe_state,
        }, [])
    assert value == [True, 'registered']
    assert calls == ['get_pipe_attributes', 'register_pipe']


def _protected_twin(conn, label: str):
    """A protected SQL connector to the same database as `conn`."""
    attributes = {
        key: conn.__dict__[key]
        for key in ('flavor', 'host', 'port', 'username', 'password', 'database', 'schema')
        if conn.__dict__.get(key, None) is not None
    }
    return mrsm.get_connector('sql', _label(label), protected=True, **attributes)


def _sql_flavors():
    from tests.connectors import get_flavors
    return [flavor for flavor in get_flavors() if flavor not in ('api', 'valkey')]


@pytest.mark.skipif(not _sqlalchemy_available(), reason="needs the `sql` extra")
@pytest.mark.parametrize("flavor", _sql_flavors())
def test_protected_raw_sql_reads_are_read_only(flavor):
    """
    `read()` and `value()` on a protected connector run one read statement, read-only, and
    refuse anything that writes (before it reaches the database).
    """
    from tests.connectors import conns
    from meerschaum.utils.sql import sql_item_name
    conn = conns[flavor]
    prot = _protected_twin(conn, 'f2_raw')
    table = f"f2_canary_{uuid.uuid4().hex[:8]}"
    name = sql_item_name(table, conn.flavor, conn.__dict__.get('schema', None))
    conn.exec(f"CREATE TABLE {name} (x INT)")
    try:
        conn.exec(f"INSERT INTO {name} (x) VALUES (1)")
        conn.exec(f"INSERT INTO {name} (x) VALUES (2)")
        assert prot.value(f"SELECT COUNT(*) FROM {name}") == 2
        assert len(prot.read(f"SELECT x FROM {name}")) == 2
        assert len(prot.read(table)) == 2
        chunks = list(prot.read(f"SELECT x FROM {name}", chunksize=1, as_iterator=True))
        assert sum(len(chunk) for chunk in chunks) == 2

        for statement in (
            f"DELETE FROM {name}",
            f"UPDATE {name} SET x = 3",
            f"DROP TABLE {name}",
            f"SELECT x FROM {name}; DELETE FROM {name}",
            f"WITH d AS (SELECT 1 AS y) DELETE FROM {name}",
        ):
            with pytest.raises(PermissionError, match='Refusing to execute SQL on protected'):
                prot.value(statement)
            with pytest.raises(PermissionError, match='Refusing to execute SQL on protected'):
                prot.read(statement)
            assert prot.read(statement, silent=True) is None
            assert prot.value(statement, silent=True) is None
        assert conn.value(f"SELECT COUNT(*) FROM {name}") == 2

        ### The allowance lets the same connector write: nothing it read with stays read-only.
        with allow_protected():
            prot.exec(f"INSERT INTO {name} (x) VALUES (3)")
        assert conn.value(f"SELECT COUNT(*) FROM {name}") == 3
    finally:
        conn.exec(f"DROP TABLE {name}", silent=True)


@pytest.mark.skipif(not _sqlalchemy_available(), reason="needs the `sql` extra")
def test_protected_sqlite_reads_give_back_writable_connections(tmp_path):
    """A read-only SQLite read (`PRAGMA query_only`) never leaves its pooled connection read-only."""
    database = str(tmp_path / 'query_only.db')
    plain = mrsm.get_connector('sql', _label('f2_qo'), flavor='sqlite', database=database)
    plain.exec('CREATE TABLE f2_qo (x INTEGER)')
    plain.exec('INSERT INTO f2_qo VALUES (1), (2)')
    prot = mrsm.get_connector(
        'sql', _label('f2_qop'), flavor='sqlite', database=database, protected=True,
    )

    def query_only() -> int:
        with prot.engine.connect() as connection:
            return connection.exec_driver_sql('PRAGMA query_only').scalar()

    assert len(prot.read('SELECT * FROM f2_qo')) == 2
    assert query_only() == 0
    ### An iterator abandoned after its first chunk.
    chunks = prot.read('SELECT * FROM f2_qo', chunksize=1, as_iterator=True)
    assert len(next(iter(chunks))) == 1
    del chunks
    assert query_only() == 0
    with allow_protected():
        prot.exec('INSERT INTO f2_qo VALUES (3)')
    assert plain.value('SELECT COUNT(*) FROM f2_qo') == 3


def test_protected_from_valkey_and_api_uris():
    """`?protected=true` on any connector URI marks it (`MRSM_VALKEY_<LABEL>`, `MRSM_API_<LABEL>`)."""
    valkey = mrsm.get_connector(
        'valkey', _label('f2_vk'), uri='redis://localhost:6399/13?protected=true',
    )
    assert valkey.protected is True
    assert valkey._rust_client().uri.endswith('?protected=true')
    assert mrsm.get_connector(
        'valkey', _label('f2_vk'), uri='redis://localhost:6399/13?protected=false',
    ).protected is False
    pipe = mrsm.Pipe('f2', 'valkey_uri', instance=valkey)
    success, msg = pipe.register()
    assert not success and 'protected instance' in msg, msg

    api = mrsm.get_connector(
        'api', _label('f2_api'), uri=f'http://u:p@127.0.0.1:{_closed_port()}?protected=true',
    )
    assert api.protected is True
    pipe = mrsm.Pipe('f2', 'api_uri', instance=api)
    success, msg = pipe.clear()
    assert not success and 'protected instance' in msg, msg
