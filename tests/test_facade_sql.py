"""
The `SQLConnector` façade: pipe, user and plugin methods route through
`meerschaum._core.SqlConnector`. These tests run against a throwaway SQLite instance.
"""

from tests._ports import service_port

import uuid
import pickle
from datetime import datetime, timezone

import pytest

import meerschaum as mrsm


@pytest.fixture
def conn(tmp_path):
    return mrsm.get_connector(
        'sql', 'facade_' + uuid.uuid4().hex,
        flavor='sqlite', database=str(tmp_path / 'facade.db'),
    )


def test_rust_connector_is_built_lazily_and_not_pickled(conn):
    pipe = mrsm.Pipe('facade', 'lazy', instance=conn)
    assert '_rust_connector' not in conn.__dict__
    pipe.exists()
    assert conn.__dict__.get('_rust_connector') not in (None, False)
    state = conn.__getstate__()
    assert '_rust_connector' not in state
    pickle.dumps(pipe)


def test_pipe_index_names_come_from_the_core(conn):
    pipe = mrsm.Pipe(
        'facade', 'index_names', instance=conn, columns={'datetime': 'dt'},
        parameters={'indices': {'datetime': ['dt']}},
    )
    assert conn.get_pipe_index_names(pipe) == {'datetime': f'IX_{pipe.target}_dt'}


def test_every_method_delegates_to_the_core_connector(conn):
    """The façade keeps no second implementation: every call goes through `_core_connector()`."""
    core_connector = conn._core_connector()
    assert core_connector is conn._core_connector()
    for name in ('register_pipe', 'get_pipe_data', 'sync_pipe', 'get_users', 'register_plugin'):
        assert conn._rust_method(name).__self__ is core_connector
    with pytest.raises(AttributeError, match="no method 'read'"):
        conn._rust_method('read')  # raw SQL goes through `_native` and the `sql_*` bindings


def test_every_delegation_names_a_bound_method():
    """Every `_rust_method('<name>')` in the connector modules is a `_core.SqlConnector` method."""
    import re
    from pathlib import Path
    from meerschaum import _core
    names = set()
    for path in Path(mrsm.__file__).parent.joinpath('connectors', 'sql').glob('_*.py'):
        names.update(re.findall(r"_rust_method\('(\w+)'\)", path.read_text(encoding='utf-8')))
    names.discard('read')
    assert len(names) > 40, names
    missing = sorted(name for name in names if not hasattr(_core.SqlConnector, name))
    assert not missing, missing


def test_get_pipe_data_moves_its_bounds_by_minutes(conn):
    """`begin_add_minutes` / `end_add_minutes` shift the bounds of the read, as in v4."""
    from datetime import timedelta
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    pipe = mrsm.Pipe('facade', 'add_minutes', instance=conn, columns={'datetime': 'dt'})
    pipe.delete()
    pipe = mrsm.Pipe('facade', 'add_minutes', instance=conn, columns={'datetime': 'dt'})
    success, msg = pipe.sync([{'dt': start + timedelta(hours=i), 'v': i} for i in range(10)])
    assert success, msg
    df = conn.get_pipe_data(
        pipe,
        begin=start + timedelta(hours=4), begin_add_minutes=-120,
        end=start + timedelta(hours=8), end_add_minutes=60,
    )
    assert df['v'].tolist() == [2, 3, 4, 5, 6, 7, 8]
    unshifted = conn.get_pipe_data(
        pipe, begin=start + timedelta(hours=4), end=start + timedelta(hours=8),
    )
    assert unshifted['v'].tolist() == [4, 5, 6, 7]


def test_set_integer_now_func_has_nothing_to_do_on_sqlite(conn):
    pipe = mrsm.Pipe('facade', 'integer_now', instance=conn, columns={'datetime': 'dt'})
    assert conn.set_integer_now_func(pipe) is True


def test_a_connector_without_a_driver_raises_a_clear_error(conn):
    conn.__dict__['_rust_connector'] = False
    conn.__dict__['_rust_error'] = 'no such driver'
    pipe = mrsm.Pipe('facade', 'no_driver', instance=conn)
    with pytest.raises(RuntimeError, match='no such driver'):
        conn.pipe_exists(pipe)
    with pytest.raises(RuntimeError, match=str(conn)):
        conn.get_users()


def test_the_oracle_client_requirement_is_named(monkeypatch):
    from meerschaum.connectors.sql import _SQLConnector
    monkeypatch.setattr(_SQLConnector, '_oracle_client_available', lambda: False)
    conn = mrsm.get_connector(
        'sql', 'facade_oracle_' + uuid.uuid4().hex,
        flavor='oracle', host='localhost', database='xe', username='u', password='p',
    )
    with pytest.raises(RuntimeError, match='Oracle client library'):
        conn.get_users()


def test_sync_get_data_round_trip(conn):
    pipe = mrsm.Pipe('facade', 'round_trip', instance=conn, columns={'datetime': 'dt', 'id': 'id'})
    docs = [
        {'dt': datetime(2024, 1, 1), 'id': 1, 'val': 1.5},
        {'dt': datetime(2024, 1, 2), 'id': 2, 'val': 2.5},
    ]
    success, msg = pipe.sync(docs)
    assert success, msg
    success, msg = pipe.sync([{'dt': datetime(2024, 1, 1), 'id': 1, 'val': 3.5}])
    assert success, msg
    assert msg == 'Inserted 0, updated 1 rows.'
    df = pipe.get_data()
    assert list(df['val']) == [3.5, 2.5]
    assert str(df['dt'].dtype) == 'datetime64[us, UTC]'
    assert pipe.get_sync_time() == datetime(2024, 1, 2, tzinfo=timezone.utc)
    assert pipe.get_sync_time(newest=False) == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert pipe.get_rowcount() == 2
    assert pipe.get_rowcount(begin=datetime(2024, 1, 2)) == 1
    success, msg = pipe.clear(begin=datetime(2024, 1, 2))
    assert success, msg
    assert pipe.get_rowcount() == 1
    docs = pipe.get_docs()
    assert len(docs) == 1
    assert docs[0]['id'] == 1
    assert docs[0]['val'] == 3.5
    success, msg = pipe.drop()
    assert success, msg
    assert not pipe.exists()
    success, msg = pipe.delete()
    assert success, msg


def test_partial_row_update_keeps_other_columns(conn):
    """Design §4.4: a partial-row sync on a non-upsert pipe does not null the other columns."""
    pipe = mrsm.Pipe('facade', 'partial', instance=conn, columns={'datetime': 'dt', 'id': 'id'})
    success, msg = pipe.sync([{'dt': datetime(2024, 1, 1), 'id': 1, 'a': 1, 'b': 'x'}])
    assert success, msg
    success, msg = pipe.sync([{'dt': datetime(2024, 1, 1), 'id': 1, 'b': 'y'}])
    assert success, msg
    doc = pipe.get_doc()
    assert doc['a'] == 1
    assert doc['b'] == 'y'


def test_attribute_aliases_persist(conn):
    pipe = mrsm.Pipe('facade', 'live', instance=conn, columns={'id': 'id'})
    success, msg = pipe.register()
    assert success, msg
    pipe.tags = ['a']
    success, msg = pipe.edit()
    assert success, msg
    assert conn.get_pipe_attributes(pipe)['parameters']['tags'] == ['a']
    fresh = mrsm.Pipe('facade', 'live', instance=conn, cache=False)
    assert fresh.tags == ['a']
    keys = conn.fetch_pipes_keys(tags=['a'])
    entries = keys.values() if isinstance(keys, dict) else keys
    assert [tuple(k[:3]) for k in entries] == [('facade', 'live', None)]
    pipe.delete()


def test_attribute_read_errors_propagate(conn):
    """
    A failed metadata read must raise, not report the pipe as unregistered
    (`tests/test_pipe_attributes.py::test_sql_attribute_errors_are_not_unregistered` mocks `exec`;
    this checks the routed path with a real failure).
    """
    pytest.importorskip('sqlalchemy')
    pipe = mrsm.Pipe('facade', 'errors', instance=conn, parameters={'target': 'x'})
    success, msg = pipe.register()
    assert success, msg
    conn.exec("ALTER TABLE mrsm_pipes RENAME COLUMN parameters TO parameters_renamed")
    with pytest.raises(RuntimeError):
        conn.get_pipe_attributes(pipe)


def test_users_and_plugins(conn):
    from meerschaum.core import User
    user = User('facade_user', 'password123', email='a@b.c', instance=conn)
    success, msg = conn.register_user(user)
    assert success, msg
    assert conn.get_user_id(user) is not None
    assert 'facade_user' in conn.get_users()
    assert conn.get_user_type(user) is None
    password_hash = conn.get_user_password_hash(user)
    from meerschaum.core.User import verify_password
    assert verify_password('password123', password_hash)
    user.type = 'admin'
    success, msg = conn.edit_user(user)
    assert success, msg
    assert conn.get_user_type(user) == 'admin'
    success, msg = conn.delete_user(user)
    assert success, msg
    assert conn.get_user_id(user) is None


def test_a_pipe_on_an_instance_that_cannot_be_built():
    """v4.1.4 raises `Unable to parse connector keys '...'` from every call that needs the instance."""
    pipe = mrsm.Pipe(
        'facade', 'no_instance', instance='sql:facade_missing_' + uuid.uuid4().hex,
        columns={'datetime': 'dt'},
    )
    calls = {
        'get_data': lambda: pipe.get_data(),
        'get_rowcount': lambda: pipe.get_rowcount(),
        'get_sync_time': lambda: pipe.get_sync_time(),
        'get_backtrack_data': lambda: pipe.get_backtrack_data(),
        'infer_dtypes': lambda: pipe.infer_dtypes(),
        'deduplicate': lambda: pipe.deduplicate(),
        'sync': lambda: pipe.sync([{'dt': '2024-01-01'}]),
        'verify': lambda: pipe.verify(),
        'enforce_dtypes': lambda: pipe.enforce_dtypes([{'dt': 1}]),
        'filter_existing': lambda: pipe.filter_existing([{'dt': 1}]),
        'exists': lambda: pipe.exists(),
    }
    for name, call in calls.items():
        with pytest.raises(Exception, match='Unable to parse connector keys'):
            call()
    ### The source is checked first: a source without `fetch()` answers `None` with a warning.
    with pytest.warns(UserWarning, match='No `fetch\\(\\)` function defined'):
        assert pipe.fetch() is None


def test_raw_sql_without_the_sql_extra(tmp_path):
    """
    rc15 Q53: `read`, `exec`, `value`, `exec_queries` and `to_sql` run on the Rust connector, so
    they work without SQLAlchemy; only `.engine` names `meerschaum[sql]` (D16).
    """
    import os
    import subprocess
    import sys
    code = """
import sys
sys.modules['sqlalchemy'] = None
import meerschaum as mrsm
import pandas as pd
conn = mrsm.get_connector('sql', 'noextra', flavor='sqlite', database=sys.argv[1])
assert conn.exec('SELECT 1').scalar() == 1
assert conn.read('SELECT 1 AS one')['one'].tolist() == [1]
assert conn.value('SELECT 1') == 1
assert conn.to_sql(pd.DataFrame({'a': [1]}), 't', as_tuple=True)[0]
assert conn.exec_queries(['SELECT 1'])[0].scalar() == 1
### A property raises `AttributeError`, so that `hasattr(conn, 'engine')` is `False`.
assert hasattr(conn, 'engine') is False
try:
    conn.engine
except AttributeError as e:
    assert 'meerschaum[sql]' in str(e), str(e)
else:
    raise AssertionError('engine did not raise')
print('D16_OK')
"""
    env = {key: value for key, value in os.environ.items() if not key.startswith('MRSM_')}
    env.update({
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_DATAFRAME': 'pandas',
        'PYTHONPATH': os.getcwd(),
    })
    (tmp_path / 'root').mkdir()
    (tmp_path / 'plugins').mkdir()
    completed = subprocess.run(
        [sys.executable, '-c', code, str(tmp_path / 'x.db')],
        env=env, capture_output=True, text=True, timeout=180,
    )
    assert 'D16_OK' in completed.stdout, completed.stdout + completed.stderr


@pytest.mark.parametrize(
    'flavor, port, username, password, database, expected_schema',
    [
        ('postgis', service_port('postgis'), 'test', 'test1234', 'testdb', 'public'),
        ('mssql', service_port('mssql'), 'sa', 'supersecureSECRETPASSWORD123!', 'master', 'dbo'),
    ],
    ids=['postgis', 'mssql'],
)
def test_default_schema_is_discovered_natively_without_sqlalchemy(
    tmp_path, flavor, port, username, password, database, expected_schema,
):
    """
    D20: a server flavor with no schema configured asks the database for its default schema
    through the Rust connector (`SELECT current_schema()` / `SELECT SCHEMA_NAME()`), so the base
    wheel (no `sql` extra) syncs and reads a pipe. The schema equals what v4 reports
    (`sqlalchemy.inspect(engine).default_schema_name`: `public` on PostgreSQL, `dbo` on MSSQL).
    """
    import os
    import socket
    import subprocess
    import sys
    try:
        socket.create_connection(('localhost', port), timeout=2).close()
    except OSError:
        pytest.skip(f"No {flavor} test database on port {port}.")
    code = """
import os, sys
sys.modules['sqlalchemy'] = None
import meerschaum as mrsm
conn = mrsm.get_connector(
    'sql', 'default_schema_' + os.environ['FLAVOR'],
    flavor=os.environ['FLAVOR'], host='localhost', port=int(os.environ['PORT']),
    username=os.environ['USERNAME_'], password=os.environ['PASSWORD'],
    database=os.environ['DATABASE'],
)
assert 'schema' not in conn.__dict__, 'no schema is configured'
assert conn.test_connection() is True
bad = mrsm.get_connector(
    'sql', 'unreachable_' + os.environ['FLAVOR'],
    flavor=os.environ['FLAVOR'], host='localhost', port=1,
    username=os.environ['USERNAME_'], password=os.environ['PASSWORD'],
    database=os.environ['DATABASE'],
)
assert bad.test_connection() is False
pipe = mrsm.Pipe('w16', 'default_schema', os.environ['FLAVOR'], instance=conn,
                 columns={'datetime': 'dt', 'id': 'id'})
pipe.delete()
pipe.drop()
assert pipe.sync([{'dt': '2024-01-01', 'id': 1, 'v': 'a'}, {'dt': '2024-01-02', 'id': 2, 'v': 'b'}])[0]
assert [d['v'] for d in pipe.get_data(as_docs=True)] == ['a', 'b']
assert pipe.get_rowcount() == 2
print('SCHEMA=' + str(conn.schema))
pipe.delete()
pipe.drop()
assert sys.modules['sqlalchemy'] is None
assert not any(m.startswith('sqlalchemy.') for m in sys.modules)
assert '_engine' not in conn.__dict__, 'the SQLAlchemy engine was created'
print('SCHEMA_OK')
"""
    env = {key: value for key, value in os.environ.items() if not key.startswith('MRSM_')}
    env.update({
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
        'PYTHONPATH': os.getcwd(),
        'FLAVOR': flavor,
        'PORT': str(port),
        'USERNAME_': username,
        'PASSWORD': password,
        'DATABASE': database,
    })
    (tmp_path / 'root').mkdir()
    (tmp_path / 'plugins').mkdir()
    completed = subprocess.run(
        [sys.executable, '-c', code], env=env, capture_output=True, text=True, timeout=300,
    )
    output = completed.stdout + completed.stderr
    assert 'SCHEMA_OK' in completed.stdout, output
    assert f'SCHEMA={expected_schema}' in completed.stdout, output


def test_a_roles_search_path_is_the_default_schema_without_sqlalchemy(tmp_path):
    """
    v4 reports `sqlalchemy.inspect(engine).default_schema_name`: a role with
    `ALTER ROLE ... SET search_path = <schema>` has that schema, so its pipes live in
    `<schema>.<target>`, not `public.<target>`. The Rust connector asks the database before the
    first query, on the base wheel too.
    """
    import os
    import socket
    import subprocess
    import sys
    import uuid
    pytest.importorskip('sqlalchemy')  # the admin connection uses `exec()`
    try:
        socket.create_connection(('localhost', service_port('postgis')), timeout=2).close()
    except OSError:
        pytest.skip("No postgis test database on port 5449.")
    admin = mrsm.get_connector(
        'sql', 'w16_schema_admin', flavor='postgis', host='localhost', port=service_port('postgis'),
        username='test', password='test1234', database='testdb',
    )
    tag = uuid.uuid4().hex[:8]
    role, schema = f'w16role_{tag}', f'w16schema_{tag}'
    for query in (
        f"CREATE ROLE {role} LOGIN PASSWORD 'w16pass'",
        f"GRANT CREATE ON DATABASE testdb TO {role}",
        f"CREATE SCHEMA {schema} AUTHORIZATION {role}",
        f"ALTER ROLE {role} SET search_path = {schema}",
    ):
        assert admin.exec(query) is not None, query
    code = """
import os, sys
sys.modules['sqlalchemy'] = None
import meerschaum as mrsm
conn = mrsm.get_connector(
    'sql', 'w16_schema_role', flavor='postgis', host='localhost', port=int(os.environ['PORT']),
    username=os.environ['ROLE'], password='w16pass', database='testdb',
)
assert 'schema' not in conn.__dict__
pipe = mrsm.Pipe('w16', 'role_schema', instance=conn, columns={'datetime': 'dt', 'id': 'id'})
assert pipe.sync([{'dt': '2024-01-01', 'id': 1}])[0]
assert pipe.get_rowcount() == 1
print('SCHEMA=' + str(conn.schema))
print('TARGET=' + pipe.target)
print('ROLE_OK')
"""
    env = {key: value for key, value in os.environ.items() if not key.startswith('MRSM_')}
    env.update({
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
        'PYTHONPATH': os.getcwd(),
        'ROLE': role,
        'PORT': str(service_port('postgis')),
    })
    (tmp_path / 'root').mkdir()
    (tmp_path / 'plugins').mkdir()
    try:
        completed = subprocess.run(
            [sys.executable, '-c', code], env=env, capture_output=True, text=True, timeout=300,
        )
        output = completed.stdout + completed.stderr
        assert 'ROLE_OK' in completed.stdout, output
        assert f'SCHEMA={schema}' in completed.stdout, output
        target = next(
            line.split('=', 1)[1] for line in completed.stdout.splitlines()
            if line.startswith('TARGET=')
        )
        in_schema = admin.value(
            "SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_name = '{target}'"
        )
        in_public = admin.value(
            "SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = 'public' AND table_name = '{target}'"
        )
        assert (in_schema, in_public) == (1, 0), output
    finally:
        for query in (
            f"DROP SCHEMA IF EXISTS {schema} CASCADE",
            f"DROP OWNED BY {role}",
            f"DROP ROLE IF EXISTS {role}",
        ):
            admin.exec(query)


def test_a_sqlite_instance_needs_no_sqlalchemy_for_its_attributes_and_pipes(tmp_path):
    """
    Without the `sql` extra `URI`, `DATABASE_URL` and `IS_THREAD_SAFE` come from the attributes,
    `get_pipes(as_tags_dict=True)` works, nothing warns about SQLAlchemy and `table_exists` asks
    the Rust connector.
    """
    import os
    import subprocess
    import sys
    code = """
import sys, warnings
sys.modules['sqlalchemy'] = None
warnings.simplefilter('error', ImportWarning)
import meerschaum as mrsm
from meerschaum.utils.sql import table_exists
db = sys.argv[1]
conn = mrsm.get_connector('sql', 'noextra_attrs', flavor='sqlite', database=db)
assert conn.URI == 'sqlite:///' + db, conn.URI
assert conn.DATABASE_URL == conn.URI
assert conn.IS_THREAD_SAFE is True
memory = mrsm.get_connector('sql', 'noextra_mem', flavor='sqlite', database=':memory:')
assert memory.IS_THREAD_SAFE is False
assert mrsm.get_connector('sql', 'noextra_duck', flavor='duckdb', database='x.duckdb').IS_THREAD_SAFE is False
pipe = mrsm.Pipe('noextra', 'tags', instance=conn, columns={'id': 'id'}, tags=['a', 'b'])
assert pipe.sync([{'id': 1}])[0]
tags = mrsm.get_pipes(mrsm_instance=conn, as_tags_dict=True)
assert sorted(tags) == ['a', 'b'], tags
### rc15 Q53: asked through the Rust connector.
assert table_exists('t', conn) is False
assert table_exists(pipe.target, conn) is True
print('ATTRS_OK')
"""
    env = {key: value for key, value in os.environ.items() if not key.startswith('MRSM_')}
    env.update({
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
        'PYTHONPATH': os.getcwd(),
    })
    (tmp_path / 'root').mkdir()
    (tmp_path / 'plugins').mkdir()
    completed = subprocess.run(
        [sys.executable, '-c', code, str(tmp_path / 'attrs.db')],
        env=env, capture_output=True, text=True, timeout=180,
    )
    assert 'ATTRS_OK' in completed.stdout, completed.stdout[-1500:] + completed.stderr[-3000:]
    assert 'sqlalchemy' not in completed.stderr.lower(), completed.stderr[-3000:]
