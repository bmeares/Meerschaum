"""
Raw SQL through the Rust connector (rc15 Q50 / Q53): `SQLConnector.read`, `exec`,
`exec_queries`, `value` and `to_sql` return what Meerschaum 4's SQLAlchemy path returns (still
there for `native_sql: false`), value for value and dtype for dtype, on every flavor; they need
no SQLAlchemy; and a process uses one SQLite library.
"""

from __future__ import annotations

import datetime
import decimal
import math
import os
import socket
import subprocess
import sys
import threading
import uuid

import pytest

import meerschaum as mrsm
from tests.connectors import get_flavors

#: The test databases (`tests/docker-compose.yaml`), by `MRSM_TEST_FLAVORS` key.
SERVERS = {
    'timescaledb': dict(
        flavor='timescaledb', username='test', password='test1234', database='testdb',
        host='localhost', port=5439,
    ),
    'timescaledb_ha': dict(
        flavor='timescaledb-ha', username='test', password='test1234', database='testdb',
        host='localhost', port=5459,
    ),
    'postgis': dict(
        flavor='postgis', username='test', password='test1234', database='testdb',
        host='localhost', port=5449,
    ),
    'mariadb': dict(
        flavor='mariadb', username='test', password='test1234', database='testdb',
        host='localhost', port=3309,
    ),
    'mysql': dict(
        flavor='mysql', username='root', password='my-secret-pw', database='mysql',
        host='localhost', port=3310,
    ),
    'mssql': dict(
        flavor='mssql', username='sa', password='supersecureSECRETPASSWORD123!',
        database='master', host='localhost', port=1439,
    ),
    'oracle': dict(
        flavor='oracle', username='system', password='oracle', database='xe',
        host='localhost', port=1529,
    ),
}

#: The DBAPI module SQLAlchemy needs for each flavor (the reference path).
DRIVERS = {
    'timescaledb': 'psycopg', 'timescaledb_ha': 'psycopg', 'postgis': 'psycopg',
    'mariadb': 'pymysql', 'mysql': 'pymysql', 'mssql': 'pyodbc', 'oracle': 'oracledb',
    'sqlite': 'sqlite3',
}

FLAVORS = [
    flavor for flavor in get_flavors()
    if flavor in SERVERS or flavor == 'sqlite'
]


def _reachable(port: int) -> bool:
    try:
        socket.create_connection(('localhost', port), timeout=2).close()
        return True
    except OSError:
        return False


@pytest.fixture(params=FLAVORS)
def pair(request, tmp_path):
    """A native connector and a SQLAlchemy (`native_sql=False`) one to the same database."""
    flavor = request.param
    pytest.importorskip('sqlalchemy')
    pytest.importorskip('pandas')
    pytest.importorskip(DRIVERS[flavor])
    if flavor == 'sqlite':
        attrs = dict(flavor='sqlite', database=str(tmp_path / 'native.db'))
    else:
        attrs = SERVERS[flavor]
        if not _reachable(attrs['port']):
            pytest.skip(f"No {flavor} test database on port {attrs['port']}.")
    tag = uuid.uuid4().hex[:8]
    native = mrsm.get_connector('sql', f'r1n_{flavor}_{tag}', **attrs)
    reference = mrsm.get_connector('sql', f'r1s_{flavor}_{tag}', native_sql=False, **attrs)
    return flavor, native, reference


def _table(flavor: str) -> str:
    return f"_mrsm_rc15_r1_{uuid.uuid4().hex[:10]}"


### Columns and rows per flavor family: every common type, NULLs, NaN / infinity, null-looking
### strings, bytes, JSON, naive and aware datetimes, dates, booleans.
DDL = {
    'postgresql': (
        "CREATE TABLE {t} (id INT, i INT, b BIGINT, f DOUBLE PRECISION, r REAL, n NUMERIC, "
        "n2 NUMERIC(10, 2), tx TEXT, c CHAR(3), bo BOOLEAN, ts TIMESTAMP, tstz TIMESTAMPTZ, "
        "d DATE, tm TIME, j JSON, jb JSONB, u UUID, bys BYTEA, iv INTERVAL, arr INT[])"
    ),
    'mysql': (
        "CREATE TABLE {t} (id INT, i INT, b BIGINT, f DOUBLE, r FLOAT, n2 DECIMAL(10, 2), "
        "tx TEXT, bt VARCHAR(10) COLLATE utf8mb4_bin, bo BOOLEAN, ts DATETIME(6), "
        "tsz TIMESTAMP(6) NULL, d DATE, tm TIME, j JSON, bys BLOB)"
    ),
    'mssql': (
        "CREATE TABLE {t} (id INT, i INT, b BIGINT, f FLOAT, r REAL, n2 DECIMAL(10, 2), "
        "tx NVARCHAR(MAX), bo BIT, ts DATETIME2, tsz DATETIMEOFFSET, d DATE, tm TIME, "
        "u UNIQUEIDENTIFIER, bys VARBINARY(MAX))"
    ),
    'oracle': (
        "CREATE TABLE {t} (id NUMBER(10), i NUMBER(10), b NUMBER(19), f BINARY_DOUBLE, "
        "r BINARY_FLOAT, n2 NUMBER(10, 2), nn NUMBER, tx NVARCHAR2(200), cl CLOB, "
        "ts TIMESTAMP, tsz TIMESTAMP WITH TIME ZONE, d DATE, bys BLOB)"
    ),
    'sqlite': (
        "CREATE TABLE {t} (id INTEGER, i INTEGER, f REAL, tx TEXT, bys BLOB, n NUMERIC, "
        "ts DATETIME, d DATE, bo BOOLEAN, mixed)"
    ),
}
UUID_TEXT = '0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d'
ROWS = {
    'postgresql': [
        "INSERT INTO {t} VALUES (1, 1, 9007199254740993, 1.5, 1.1, 1.50, 3.14, 'hello', 'xy', "
        "TRUE, '2024-01-02 03:04:05.123456', '2024-01-02 03:04:05.123456+00', '2024-01-02', "
        "'03:04:05.5', '{\"a\": 1}', '{\"b\": [1, 2]}', '" + UUID_TEXT + "', '\\x00ff', "
        "'1 day 02:00:00', ARRAY[1, NULL, 3])",
        "INSERT INTO {t} (id) VALUES (2)",
        "INSERT INTO {t} (id, f, r, n, tx, c) VALUES (3, 'NaN', 'Infinity', 'NaN', 'None', '')",
        "INSERT INTO {t} (id, tx) VALUES (4, 'nan')",
        "INSERT INTO {t} (id, tx) VALUES (5, '<NA>')",
    ],
    'mysql': [
        "INSERT INTO {t} VALUES (1, 1, 9007199254740993, 1.5, 1.1, 3.14, 'hello', 'Abc', TRUE, "
        "'2024-01-02 03:04:05.123456', '2024-01-02 03:04:05.123456', '2024-01-02', '03:04:05', "
        "'{\"a\": 1}', x'00ff')",
        "INSERT INTO {t} (id) VALUES (2)",
        "INSERT INTO {t} (id, tx, bt) VALUES (3, 'None', '')",
        "INSERT INTO {t} (id, tx) VALUES (4, 'NaT')",
    ],
    'mssql': [
        "INSERT INTO {t} VALUES (1, 1, 9007199254740993, 1.5, 1.1, 3.14, N'hello', 1, "
        "'2024-01-02 03:04:05.123456', '2024-01-02 03:04:05.123456 +00:00', '2024-01-02', "
        "'03:04:05.5', '" + UUID_TEXT + "', 0x00ff)",
        "INSERT INTO {t} (id) VALUES (2)",
        "INSERT INTO {t} (id, tx) VALUES (3, N'None')",
        "INSERT INTO {t} (id, tx) VALUES (4, N'')",
    ],
    'oracle': [
        "INSERT INTO {t} VALUES (1, 1, 9007199254740993, 1.5, 1.1, 3.14, 2.5, N'hello', "
        "'clob text', TIMESTAMP '2024-01-02 03:04:05.123456', "
        "TIMESTAMP '2024-01-02 03:04:05.123456 +00:00', DATE '2024-01-02', HEXTORAW('00FF'))",
        "INSERT INTO {t} (id) VALUES (2)",
        "INSERT INTO {t} (id, tx, nn) VALUES (3, N'None', 7)",
    ],
    'sqlite': [
        "INSERT INTO {t} VALUES (1, 1, 1.5, 'hello', x'00ff', 3.14, "
        "'2024-01-02 03:04:05.123456', '2024-01-02', 1, 5)",
        "INSERT INTO {t} (id) VALUES (2)",
        "INSERT INTO {t} (id, f, tx, mixed) VALUES (3, 9e999, 'None', 'text')",
        "INSERT INTO {t} (id, tx, mixed) VALUES (4, '', 2.5)",
        "INSERT INTO {t} (id, tx, mixed) VALUES (5, 'NA', x'01')",
    ],
}
FAMILY = {
    'timescaledb': 'postgresql', 'timescaledb_ha': 'postgresql', 'postgis': 'postgresql',
    'mariadb': 'mysql', 'mysql': 'mysql', 'mssql': 'mssql', 'oracle': 'oracle',
    'sqlite': 'sqlite',
}


def _norm(value):
    """A comparable description of one value: its type and its value (aware datetimes in UTC)."""
    import pandas as pd
    if value is None:
        return ('None',)
    if value is pd.NA:
        return ('NA',)
    if value is pd.NaT:
        return ('NaT',)
    if isinstance(value, float) and math.isnan(value):
        return ('float', 'nan')
    if isinstance(value, decimal.Decimal):
        return ('Decimal', str(value))
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            return (type(value).__name__, value.astimezone(datetime.timezone.utc).isoformat(), 'aware')
        return (type(value).__name__, value.isoformat())
    if isinstance(value, (list, tuple)):
        return (type(value).__name__, [_norm(v) for v in value])
    return (type(value).__name__, repr(value))


def _describe(df):
    return {
        str(col): (str(df[col].dtype), [_norm(v) for v in df[col].tolist()])
        for col in df.columns
    }


def _rows(result):
    return [tuple(_norm(v) for v in row) for row in result]


def _setup(flavor, native):
    family = FAMILY[flavor]
    from meerschaum.utils.sql import sql_item_name
    table = _table(flavor)
    name = sql_item_name(table, native.flavor, None)
    native.exec(DDL[family].replace('{t}', name))
    for row in ROWS[family]:
        native.exec(row.replace('{t}', name))
    return table, name


def test_reads_match_the_sqlalchemy_path(pair):
    flavor, native, reference = pair
    table, name = _setup(flavor, native)
    try:
        query = f"SELECT * FROM {name} ORDER BY id"
        for kwargs in ({}, {'coerce_float': False}):
            assert _describe(native.read(query, **kwargs)) == _describe(
                reference.read(query, **kwargs)
            ), kwargs
        ### The rows a cursor returns.
        native_result = native.exec(query)
        reference_result = reference.exec(query)
        assert list(native_result.keys()) == list(reference_result.keys())
        assert _rows(native_result.fetchall()) == _rows(reference_result.fetchall())
        assert _norm(native.value(f"SELECT COUNT(*) FROM {name}")) == _norm(
            reference.value(f"SELECT COUNT(*) FROM {name}")
        )
        assert native.value(f"SELECT id FROM {name} WHERE id < 0") is None
        ### Chunks are typed chunk by chunk, as `pd.read_sql_query(chunksize=...)` does.
        native_chunks = native.read(query, chunksize=2, as_chunks=True)
        reference_chunks = reference.read(query, chunksize=2, as_chunks=True)
        assert [_describe(c) for c in native_chunks] == [_describe(c) for c in reference_chunks]
        ### A table name reads the whole table.
        assert len(native.read(table)) == len(ROWS[FAMILY[flavor]])
    finally:
        native.exec(f"DROP TABLE {name}", silent=True)


def test_parameters_bind_in_every_style(pair):
    flavor, native, reference = pair
    table, name = _setup(flavor, native)
    try:
        named = f"SELECT id, tx FROM {name} WHERE id = :id"
        assert _describe(native.read(named, params={'id': 1})) == _describe(
            reference.read(named, params={'id': 1})
        )
        assert native.value(f"SELECT tx FROM {name} WHERE id = :id", {'id': 1}) == 'hello'
        ### Positional values with `?` or `%s` placeholders (SQLAlchemy refused lists).
        for marker in ('?', '%s'):
            df = native.read(f"SELECT id FROM {name} WHERE id > {marker} ORDER BY id", params=[2])
            assert df['id'].tolist() == [i for i in range(3, len(ROWS[FAMILY[flavor]]) + 1)]
        ### `%(name)s` placeholders with a dictionary.
        df = native.read(f"SELECT id FROM {name} WHERE id = %(x)s", params={'x': 3})
        assert df['id'].tolist() == [3]
        ### Text parameters compare with any type (psycopg sends them untyped).
        assert native.value(f"SELECT COUNT(*) FROM {name} WHERE id = :id", {'id': '1'}) == 1
        ### `executemany` with a list of dictionaries.
        result = native.exec(
            f"INSERT INTO {name} (id, tx) VALUES (:id, :tx)",
            [{'id': 100, 'tx': "O'Brien"}, {'id': 101, 'tx': 'a :b'}],
        )
        assert result.rowcount == 2
        assert native.value(f"SELECT tx FROM {name} WHERE id = :id", {'id': 100}) == "O'Brien"
        assert native.value(f"SELECT tx FROM {name} WHERE id = :id", {'id': 101}) == 'a :b'
        ### A missing value names the bind parameter, as SQLAlchemy does.
        with pytest.raises(mrsm.connectors.sql.ProgrammingError, match="bind parameter 'nope'"):
            native.read(f"SELECT * FROM {name} WHERE id = :nope", params={'x': 1})
    finally:
        native.exec(f"DROP TABLE {name}", silent=True)


def test_sqlalchemy_constructs_compile_to_text(pair):
    flavor, native, reference = pair
    sqlalchemy = pytest.importorskip('sqlalchemy')
    table, name = _setup(flavor, native)
    try:
        lite = sqlalchemy.table(table, sqlalchemy.column('id'), sqlalchemy.column('tx'))
        select = sqlalchemy.select(lite.c.id, lite.c.tx).where(lite.c.id.in_([1, 3])).order_by(lite.c.id)
        assert _describe(native.read(select)) == _describe(reference.read(select))
        text = sqlalchemy.text(f"SELECT tx FROM {name} WHERE id = :id").bindparams(id=1)
        assert native.value(text) == 'hello'
        result = native.exec(sqlalchemy.insert(lite).values(id=200, tx='inserted'))
        assert result.rowcount == 1
        assert native.value(f"SELECT tx FROM {name} WHERE id = 200") == 'inserted'
    finally:
        native.exec(f"DROP TABLE {name}", silent=True)


def test_results_have_the_cursor_result_api(pair):
    flavor, native, reference = pair
    table, name = _setup(flavor, native)
    try:
        query = f"SELECT id, tx FROM {name} ORDER BY id"
        row = native.exec(query).first()
        assert row == (1, 'hello') and row.id == 1 and row._mapping['tx'] == 'hello'
        assert row._asdict() == {'id': 1, 'tx': 'hello'}
        assert native.exec(query).scalar() == reference.exec(query).scalar()
        assert native.exec(query).mappings().all()[0] == {'id': 1, 'tx': 'hello'}
        result = native.exec(query)
        assert result.fetchone() == (1, 'hello')
        assert len(result.fetchmany(2)) == 2
        assert [r.id for r in native.exec(query)][:2] == [1, 2]
        assert native.exec(query).scalars().all()[:2] == [1, 2]
        update = native.exec(f"UPDATE {name} SET tx = 'x' WHERE id >= 4")
        assert update.returns_rows is False
        assert update.rowcount == reference.exec(f"UPDATE {name} SET tx = 'x' WHERE id >= 4").rowcount
        with pytest.raises(mrsm.connectors.sql.ResourceClosedError):
            update.fetchall()
    finally:
        native.exec(f"DROP TABLE {name}", silent=True)


def test_database_errors_raise_pep_249_exceptions(pair):
    flavor, native, reference = pair
    sqlalchemy = pytest.importorskip('sqlalchemy.exc')
    import pandas.errors
    query = "SELECT * FROM _mrsm_rc15_r1_missing_table"
    with pytest.raises(Exception) as reference_error:
        reference.exec(query)
    ### v4's `read()` raised pandas' `DatabaseError` (`pd.read_sql_query()` wraps the error).
    with pytest.raises(pandas.errors.DatabaseError):
        reference.read(query)
    with pytest.raises(pandas.errors.DatabaseError):
        native.read(query)
    for call in (native.read, native.exec, native.value):
        with pytest.raises(mrsm.connectors.sql.DatabaseError) as error:
            call(query)
        ### Also SQLAlchemy's class of the same name (SQLAlchemy is imported here).
        assert isinstance(error.value, sqlalchemy.DBAPIError)
        assert isinstance(error.value, sqlalchemy.DatabaseError)
        assert query in str(error.value)
        assert type(error.value).__name__ == type(reference_error.value).__name__
    assert native.read(query, silent=True) is None
    assert native.exec(query, silent=True) is None
    assert native.value(query, silent=True) is None


def test_to_sql_writes_the_tables_pandas_writes(pair):
    flavor, native, reference = pair
    import pandas as pd
    family = FAMILY[flavor]
    aware = family not in ('mysql', 'oracle')
    ### MySQL, MSSQL and Oracle store no infinity.
    df = pd.DataFrame({
        'i': [1, 2, 3],
        'f': (
            [1.5, float('nan'), float('inf')]
            if family in ('sqlite', 'postgresql')
            else [1.5, float('nan'), 1e300]
        ),
        's': ['a', None, 'None'],
        'b': [True, False, True],
        'dt': pd.to_datetime(['2024-01-01 00:00:01', '2024-01-02 00:00:00', '2024-01-03 12:00:00']),
    })
    if aware:
        df['dtz'] = pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03'], utc=True)
    a, b = _table(flavor), _table(flavor)
    try:
        from meerschaum.utils.sql import sql_item_name
        assert native.to_sql(df.copy(), a, if_exists='replace', as_tuple=True)[0]
        read_a = reference.read(f"SELECT * FROM {sql_item_name(a, native.flavor, None)}")
        if family == 'oracle':
            ### v4 could not write pandas' float columns on Oracle (SQLAlchemy 2 refuses to
            ### compile `Float(precision=53)` there): the Rust path writes them as `FLOAT`.
            assert read_a['f'].tolist()[0] == 1.5 and read_a['i'].tolist() == [1, 2, 3]
            return
        assert reference.to_sql(df.copy(), b, if_exists='replace', as_tuple=True)[0]
        read_b = reference.read(f"SELECT * FROM {sql_item_name(b, native.flavor, None)}")
        assert _describe(read_a) == _describe(read_b)
        if family in ('sqlite', 'postgresql', 'mysql'):
            from meerschaum.utils.sql import get_table_cols_types
            assert get_table_cols_types(a, native) == get_table_cols_types(b, native)
        ### `append` adds the rows; `fail` refuses an existing table; `replace` starts over.
        assert native.to_sql(df.copy(), a, if_exists='append')
        assert native.value(f"SELECT COUNT(*) FROM {sql_item_name(a, native.flavor, None)}") == 6
        success, message = native.to_sql(df.copy(), a, if_exists='fail', as_tuple=True, silent=True)
        assert not success and 'already exists' in message
        stats = native.to_sql(df.copy(), a, if_exists='replace', as_dict=True)
        assert stats['success'] and stats['num_rows'] == 3
        assert native.value(f"SELECT COUNT(*) FROM {sql_item_name(a, native.flavor, None)}") == 3
    finally:
        from meerschaum.utils.sql import sql_item_name
        for t in (a, b):
            native.exec(f"DROP TABLE {sql_item_name(t, native.flavor, None)}", silent=True)


def test_exec_queries_runs_in_one_transaction(pair):
    flavor, native, reference = pair
    table, name = _setup(flavor, native)
    try:
        seen = []

        def hook(session):
            seen.append(session.execute(f"SELECT COUNT(*) FROM {name}").scalar())
            return [f"INSERT INTO {name} (id) VALUES (301)"]

        results = native.exec_queries([
            f"INSERT INTO {name} (id) VALUES (300)",
            (f"DELETE FROM {name} WHERE id = 2", hook),
        ])
        assert results[0] is not None and results[0].rowcount == 1
        assert isinstance(results[1], tuple)
        assert seen == [len(ROWS[FAMILY[flavor]])]
        assert native.value(f"SELECT COUNT(*) FROM {name} WHERE id IN (300, 301)") == 2
        ### `break_on_error` rolls back everything before the failure.
        results = native.exec_queries(
            [f"INSERT INTO {name} (id) VALUES (400)", "SELECT * FROM _mrsm_rc15_r1_missing"],
            break_on_error=True,
            silent=True,
        )
        assert results[-1] is None
        assert native.value(f"SELECT COUNT(*) FROM {name} WHERE id = 400") == 0
    finally:
        native.exec(f"DROP TABLE {name}", silent=True)


def test_read_returns_polars_when_asked(tmp_path):
    pl = pytest.importorskip('polars')
    conn = mrsm.get_connector(
        'sql', 'r1_polars_' + uuid.uuid4().hex[:8], flavor='sqlite',
        database=str(tmp_path / 'polars.db'),
    )
    conn.exec("CREATE TABLE p (a INTEGER, b TEXT)")
    conn.exec("INSERT INTO p VALUES (1, 'x'), (2, NULL)")
    df = conn.read("SELECT * FROM p ORDER BY a", as_polars=True)
    assert isinstance(df, pl.DataFrame)
    assert df.to_dicts() == [{'a': 1, 'b': 'x'}, {'a': 2, 'b': None}]
    chunks = list(conn.read("SELECT * FROM p", chunksize=1, as_iterator=True, as_polars=True))
    assert all(isinstance(c, pl.DataFrame) for c in chunks) and len(chunks) == 2
    import pandas as pd
    assert isinstance(conn.read("SELECT * FROM p", as_pandas=True), pd.DataFrame)


def _clean_env(tmp_path):
    env = {key: value for key, value in os.environ.items() if not key.startswith('MRSM_')}
    env.pop('TMUX', None)
    env.pop('TMUX_PANE', None)
    env.update({
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_DATAFRAME': 'pandas',
        'PYTHONPATH': os.getcwd(),
    })
    (tmp_path / 'root').mkdir(exist_ok=True)
    (tmp_path / 'plugins').mkdir(exist_ok=True)
    return env


def test_raw_sql_needs_no_sqlalchemy_and_no_second_sqlite(tmp_path):
    """
    Without SQLAlchemy and with Python's own SQLite library unavailable, raw SQL on a SQLite
    instance works: the process uses the extension's SQLite only.
    """
    pytest.importorskip('pandas')
    code = """
import sys
for name in ('sqlalchemy', 'sqlite3', '_sqlite3'):
    sys.modules[name] = None
import meerschaum as mrsm
import pandas as pd
conn = mrsm.get_connector('sql', 'r1_nosa', flavor='sqlite', database=sys.argv[1])
assert conn.exec('CREATE TABLE t (a INTEGER, b TEXT)').rowcount == -1
assert conn.exec('INSERT INTO t VALUES (:a, :b)', [{'a': 1, 'b': 'x'}, {'a': 2, 'b': None}]).rowcount == 2
assert conn.read('SELECT * FROM t ORDER BY a')['a'].tolist() == [1, 2]
assert conn.value('SELECT COUNT(*) FROM t') == 2
assert conn.exec_queries(['INSERT INTO t VALUES (3, NULL)'])[0].rowcount == 1
assert conn.to_sql(pd.DataFrame({'c': [1.5]}), 'u', as_tuple=True)[0]
assert conn.read('u')['c'].tolist() == [1.5]
from meerschaum.utils.sql import table_exists, get_table_cols_types
assert table_exists('u', conn) and get_table_cols_types('u', conn) == {'c': 'FLOAT'}
pipe = mrsm.Pipe('r1', 'nosa', instance=conn, columns={'id': 'id'})
assert pipe.sync([{'id': 1, 'v': 1.5}])[0]
assert conn.value('SELECT COUNT(*) FROM ' + pipe.target) == 1
try:
    conn.engine
except AttributeError as e:
    assert 'meerschaum[sql]' in str(e), str(e)
else:
    raise AssertionError('engine did not raise')
assert not any(m.startswith('sqlalchemy.') for m in sys.modules)
print('NATIVE_OK')
"""
    completed = subprocess.run(
        [sys.executable, '-c', code, str(tmp_path / 'nosa.db')],
        env=_clean_env(tmp_path), capture_output=True, text=True, timeout=300,
    )
    assert 'NATIVE_OK' in completed.stdout, completed.stdout[-2000:] + completed.stderr[-4000:]


def test_threads_mixing_syncs_reads_and_writes_keep_sqlite_intact(tmp_path):
    """
    rc15 Q50: `pipe.sync()`, `conn.read()` and `conn.exec()` from threads on one SQLite file run
    through one SQLite library: no "disk I/O error", and the file passes `PRAGMA integrity_check`.
    """
    pytest.importorskip('pandas')
    code = """
import sys, threading, traceback
for name in ('sqlalchemy', 'sqlite3', '_sqlite3'):
    sys.modules[name] = None
import meerschaum as mrsm
conn = mrsm.get_connector('sql', 'r1_stress', flavor='sqlite', database=sys.argv[1])
conn.exec('CREATE TABLE side (k INTEGER, v TEXT)')
pipe = mrsm.Pipe('r1', 'stress', instance=conn, columns={'datetime': 'ts', 'id': 'id'})
errors = []
def syncs():
    for i in range(40):
        rows = [{'ts': '2024-01-01 00:00:%02d' % (i % 60), 'id': j, 'v': i} for j in range(25)]
        ok, msg = pipe.sync(rows)
        if not ok:
            errors.append(msg)
def reads():
    for _ in range(120):
        conn.read('SELECT * FROM side')
        if pipe.exists():
            conn.read('SELECT * FROM ' + pipe.target)
def writes(k):
    for i in range(120):
        conn.exec('INSERT INTO side VALUES (:k, :v)', {'k': k, 'v': str(i)})
        conn.value('SELECT COUNT(*) FROM side')
def guarded(fn, *args):
    try:
        fn(*args)
    except Exception:
        errors.append(traceback.format_exc())
threads = [threading.Thread(target=guarded, args=(syncs,)), threading.Thread(target=guarded, args=(reads,))]
threads += [threading.Thread(target=guarded, args=(writes, k)) for k in range(3)]
for t in threads:
    t.start()
for t in threads:
    t.join()
assert not errors, errors[:3]
assert conn.value('PRAGMA integrity_check') == 'ok'
assert conn.value('SELECT COUNT(*) FROM side') == 360
print('STRESS_OK')
"""
    completed = subprocess.run(
        [sys.executable, '-c', code, str(tmp_path / 'stress.db')],
        env=_clean_env(tmp_path), capture_output=True, text=True, timeout=600,
    )
    output = completed.stdout[-3000:] + completed.stderr[-6000:]
    assert 'disk I/O error' not in output, output
    assert 'STRESS_OK' in completed.stdout, output


def test_native_sql_false_keeps_the_sqlalchemy_path(tmp_path):
    pytest.importorskip('sqlalchemy')
    conn = mrsm.get_connector(
        'sql', 'r1_opt_out_' + uuid.uuid4().hex[:8], flavor='sqlite',
        database=str(tmp_path / 'opt_out.db'), native_sql=False,
    )
    result = conn.exec('SELECT 1 AS one')
    assert type(result).__module__.startswith('sqlalchemy')
    native = mrsm.get_connector(
        'sql', 'r1_opt_in_' + uuid.uuid4().hex[:8], flavor='sqlite',
        database=str(tmp_path / 'opt_in.db'),
    )
    assert type(native.exec('SELECT 1 AS one')).__name__ == 'NativeResult'
    ### `with_connection=True` hands out a SQLAlchemy connection.
    result, connection = native.exec('SELECT 1', with_connection=True, close=False)
    assert type(connection).__module__.startswith('sqlalchemy')
    connection.close()


def test_duckdb_reads_without_sqlalchemy(tmp_path):
    """DuckDB raw SQL runs on the Rust connector (both DuckDB transports)."""
    pytest.importorskip('pandas')
    conn = mrsm.get_connector(
        'sql', 'r1_duck_' + uuid.uuid4().hex[:8], flavor='duckdb',
        database=str(tmp_path / 'r1.duckdb'),
    )
    try:
        conn.exec("CREATE TABLE d (a BIGINT, b VARCHAR, c DOUBLE, ts TIMESTAMP, tz TIMESTAMPTZ)")
        conn.exec(
            "INSERT INTO d VALUES (1, 'x', 1.5, '2024-01-01 00:00:00', '2024-01-01 00:00:00+00'), "
            "(2, NULL, NULL, NULL, NULL)"
        )
    except Exception as e:
        pytest.skip(f"DuckDB is not available: {e}")
    df = conn.read("SELECT * FROM d ORDER BY a")
    assert df['a'].tolist() == [1, 2]
    assert str(df['ts'].dtype).startswith('datetime64')
    assert str(df['tz'].dtype).endswith('UTC]')
    ### v4 read DuckDB values through pandas: a NumPy scalar.
    assert int(conn.value("SELECT COUNT(*) FROM d")) == 2
    assert conn.exec("SELECT a, b FROM d ORDER BY a").fetchall() == [(1, 'x'), (2, None)]


class _Flavored:
    """A stand-in connector: `prepare()` only reads the flavor."""

    def __init__(self, flavor):
        self.flavor = flavor


@pytest.mark.parametrize('flavor, query, params, expected', [
    ('sqlite', "SELECT * FROM t WHERE a = :a AND b = :a", {'a': 1},
     ("SELECT * FROM t WHERE a = ? AND b = ?", [1, 1])),
    ('mssql', "SELECT * FROM t WHERE a = ? AND b = ?", (1, 'x'),
     ("SELECT * FROM t WHERE a = @P1 AND b = @P2", [1, 'x'])),
    ('oracle', "SELECT * FROM t WHERE a = :a", {'a': 1}, ("SELECT * FROM t WHERE a = :1", [1])),
    ('mysql', "SELECT %(a)s, %(b)s", {'a': 1, 'b': None}, ("SELECT ?, ?", [1, None])),
    ### psycopg's `%%` with `%s` / `%(name)s` markers is a literal `%`; inlined values are kept.
    ('postgresql', "SELECT * FROM t WHERE a LIKE 'x%%' AND b = %s", ['50%'],
     ("SELECT * FROM t WHERE a LIKE 'x%' AND b = $mrsm$50%$mrsm$", [])),
    ('postgresql', "SELECT * FROM t WHERE a LIKE 'x%%' AND b = %(b)s", {'b': 5},
     ("SELECT * FROM t WHERE a LIKE 'x%' AND b = $1", [5])),
    ### PostgreSQL text values are untyped literals; casts (`::date`) are not binds.
    ('postgresql', "SELECT * FROM t WHERE a = :a AND ts::date > :d", {'a': "it's", 'd': '2024-01-01'},
     ("SELECT * FROM t WHERE a = $mrsm$it's$mrsm$ AND ts::date > $mrsm$2024-01-01$mrsm$", [])),
    ('postgresql', "SELECT :v", {'v': 'a$mrsm$b'}, ("SELECT $mrsm_$a$mrsm$b$mrsm_$", [])),
    ('cockroachdb', "SELECT :v", {'v': "it's"}, ("SELECT 'it''s'", [])),
    ### `\:` is a literal colon (SQLAlchemy's escape); without parameters nothing is a bind.
    ('sqlite', "SELECT '\\:literal', :x", {'x': 2}, ("SELECT ':literal', ?", [2])),
    ('sqlite', "SELECT ':no_params'", None, ("SELECT ':no_params'", [])),
])
def test_statements_prepare_for_each_flavor(flavor, query, params, expected):
    from meerschaum.connectors.sql._native import prepare
    statement = prepare(_Flavored(flavor), query, params)
    assert (statement.sql, statement.params) == expected


def test_executemany_prepares_one_statement_per_set():
    from meerschaum.connectors.sql._native import prepare
    statement = prepare(_Flavored('sqlite'), "INSERT INTO t VALUES (:a)", [{'a': 1}, {'a': 2}])
    assert statement.many and statement.sql == "INSERT INTO t VALUES (?)"
    assert statement.params == [[1], [2]]
    ### PostgreSQL text inlines: one statement per set.
    statement = prepare(_Flavored('postgresql'), "INSERT INTO t VALUES (:a)", [{'a': 'x'}, {'a': 'y'}])
    assert statement.many and statement.params is None
    assert [sql for sql, _ in statement.sql] == [
        "INSERT INTO t VALUES ($mrsm$x$mrsm$)", "INSERT INTO t VALUES ($mrsm$y$mrsm$)",
    ]


def test_to_sql_takes_polars_and_arrow_frames(tmp_path):
    pl = pytest.importorskip('polars')
    pa = pytest.importorskip('pyarrow')
    conn = mrsm.get_connector(
        'sql', 'r1_frames_' + uuid.uuid4().hex[:8], flavor='sqlite',
        database=str(tmp_path / 'frames.db'),
    )
    frame = pl.DataFrame({
        'i': [1, 2], 'f': [1.5, None], 's': ['x', None], 'b': [True, False],
        'ts': [datetime.datetime(2024, 1, 1), None],
    })
    assert conn.to_sql(frame, 'from_polars', as_tuple=True)[0]
    assert conn.exec("SELECT name, type FROM pragma_table_info('from_polars')").fetchall() == [
        ('i', 'BIGINT'), ('f', 'FLOAT'), ('s', 'TEXT'), ('b', 'BOOLEAN'), ('ts', 'DATETIME'),
    ]
    assert conn.exec("SELECT * FROM from_polars ORDER BY i").fetchall() == [
        (1, 1.5, 'x', 1, '2024-01-01 00:00:00.000000'), (2, None, None, 0, None),
    ]
    assert conn.to_sql(pa.table({'a': [1, 2], 'b': ['x', None]}), 'from_arrow', as_tuple=True)[0]
    assert conn.read('SELECT * FROM from_arrow', as_polars=True).to_dicts() == [
        {'a': 1, 'b': 'x'}, {'a': 2, 'b': None},
    ]


def test_get_pipe_data_follows_the_dataframe_setting(tmp_path):
    pl = pytest.importorskip('polars')
    import pandas as pd
    conn = mrsm.get_connector(
        'sql', 'r1_pipe_data_' + uuid.uuid4().hex[:8], flavor='sqlite',
        database=str(tmp_path / 'pipe_data.db'),
    )
    pipe = mrsm.Pipe('r1', 'pipe_data', instance=conn, columns={'datetime': 'ts', 'id': 'id'})
    assert pipe.sync([{'ts': '2024-01-01', 'id': 1, 'v': 1.5}, {'ts': '2024-01-02', 'id': 2, 'v': 2.5}])[0]
    assert isinstance(conn.get_pipe_data(pipe, as_polars=True), pl.DataFrame)
    assert isinstance(conn.get_pipe_data(pipe, as_pandas=True), pd.DataFrame)
    ### The suite pins `MRSM_DATAFRAME=pandas`.
    assert isinstance(conn.get_pipe_data(pipe), pd.DataFrame)
    chunks = list(conn.get_pipe_data(pipe, as_iterator=True, chunksize=1, as_polars=True))
    assert len(chunks) == 2 and all(isinstance(c, pl.DataFrame) for c in chunks)


def test_polars_to_sql_on_every_flavor(pair):
    """A default-library read can be written back on every native SQL flavor."""
    pl = pytest.importorskip('polars')
    flavor, native, _ = pair
    from meerschaum.utils.sql import sql_item_name
    table = _table(flavor)
    name = sql_item_name(table, native.flavor, None)
    frame = pl.DataFrame({
        'id': [1, 2],
        'value': [1.5, None],
        'text': ['hello', None],
        'enabled': [True, False],
        'ts': [datetime.datetime(2024, 1, 1, 1, 2, 3, 456789), None],
    })
    try:
        success, message = native.to_sql(frame, table, as_tuple=True)
        assert success, message
        result = native.read(f'SELECT * FROM {name} ORDER BY id', as_polars=True)
        assert result['id'].to_list() == [1, 2]
        assert result['value'].to_list() == [1.5, None]
        assert result['text'].to_list() == ['hello', None]
        assert result['enabled'].to_list() == [True, False]
        assert result['ts'].to_list() == (
            ['2024-01-01 01:02:03.456789', None]
            if FAMILY[flavor] == 'sqlite'
            else [datetime.datetime(2024, 1, 1, 1, 2, 3, 456789), None]
        )
        assert native.to_sql(result, table, if_exists='append')
        assert native.value(f'SELECT COUNT(*) FROM {name}') == 4
    finally:
        native.exec(f'DROP TABLE {name}', silent=True)
