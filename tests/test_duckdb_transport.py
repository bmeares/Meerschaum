"""DuckDB Pipe and SQLAlchemy operations share one public driver in Python hosts.

Each scenario gets a fresh interpreter and an isolated root: backend choice is process-wide.
No external server, driver download, default root, or optional-package installation is used.
"""
import os
import signal
from pathlib import Path
import subprocess
import sys
import textwrap

import pytest

REPO = Path(__file__).resolve().parents[1]


def _run(tmp_path, program, *args, native_pin=False):
    for name in ('root', 'plugins'):
        (tmp_path / name).mkdir(exist_ok=True)
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(('MRSM_', 'PYTHON')) and k not in ('TMUX', 'TMUX_PANE')}
    env.update(
        PYTHONPATH=str(REPO), PYTHONDONTWRITEBYTECODE='1',
        MRSM_ROOT_DIR=str(tmp_path / 'root'), MRSM_PLUGINS_DIR=str(tmp_path / 'plugins'),
        MRSM_VENVS_DIR=str(tmp_path / 'venvs'), MRSM_NO_AUTO_INSTALL='1',
        MRSM_NO_UPDATE_CHECK='1',
        MRSM_SQL_W17CDUCKGUARD=f'sqlite:///{tmp_path}/guard.db',
        MRSM_CONFIG='{"meerschaum":{"instance":"sql:w17cduckguard"}}',
    )
    if native_pin:
        env['MRSM_DUCKDB_LIB'] = native_pin if isinstance(native_pin, str) else str(tmp_path / 'explicit-driver-never-opened.so')
    process = subprocess.Popen(
        [sys.executable, '-B', '-s', '-c', textwrap.dedent(program), *map(str, args)],
        env=env, cwd=tmp_path, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, start_new_session=(os.name != 'nt'),
    )
    try:
        output, _ = process.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        if os.name != 'nt':
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
        output, _ = process.communicate()
        pytest.fail('DuckDB scratch subprocess timed out:\n' + output)
    assert process.returncode == 0, output
    return output



@pytest.fixture(autouse=True)
def _drivers():
    pytest.importorskip('duckdb')
    pytest.importorskip('duckdb_engine')
    pytest.importorskip('sqlalchemy')
    # Missing/stale native code is an error, never an optional dependency skip.
    import meerschaum._core._native  # noqa: F401


@pytest.mark.parametrize('close_order', ['native-first', 'sqlalchemy-first'])
@pytest.mark.parametrize('open_order', ['pipe-first', 'sqlalchemy-first'])
def test_alternating_writes_are_visible_and_persist(tmp_path, close_order, open_order):
    database = tmp_path / 'alternating.duckdb'
    _run(tmp_path, '''
        import gc, sys
        import meerschaum as mrsm
        conn = mrsm.get_connector('sql', 'w17cduck', flavor='duckdb', database=sys.argv[1])
        if sys.argv[3] == 'sqlalchemy-first':
            assert conn.exec('CREATE TABLE raw_first (id INT)') is not None
        pipe = mrsm.Pipe('w17cduck', 'alternating', instance=conn, columns={'id': 'id'})
        assert pipe.sync([{'id': 1, 'value': 1}])[0]
        table = '"' + pipe.target + '"'
        held = conn.engine.connect()
        assert held.exec_driver_sql('SELECT value FROM ' + table).scalar() == 1
        held.commit()
        assert conn.exec('UPDATE ' + table + ' SET value=999') is not None
        assert pipe.get_data(as_docs=True) == [{'id': 1, 'value': 999}]
        result = pipe.sync([{'id': 2, 'value': 2}])
        assert result[0], result
        expected = [{'id': 1, 'value': 999}, {'id': 2, 'value': 2}]
        assert pipe.get_data(as_docs=True) == expected
        assert held.exec_driver_sql('SELECT id, value FROM ' + table + ' ORDER BY id').fetchall() == [(1, 999), (2, 2)]
        held.rollback()
        # User-held transactions remain independent and rollback without replacing the engine.
        held.exec_driver_sql('UPDATE ' + table + ' SET value=333 WHERE id=1')
        assert pipe.get_data(as_docs=True) == expected
        held.rollback()
        assert pipe.get_data(as_docs=True) == expected
        if sys.argv[2] == 'native-first':
            del pipe
            conn.__dict__.pop('_rust_connector', None)
            gc.collect()
        held.close()
        conn.engine.dispose()
        print(pipe.target if 'pipe' in locals() else 'w17cduck_alternating')
    ''', database, close_order, open_order)
    # A fresh process is essential: independent handles previously lost acknowledged native
    # inserts on close, even when both in-process reads had separately appeared successful.
    _run(tmp_path, '''
        import sys, duckdb
        db = duckdb.connect(sys.argv[1])
        rows = db.execute('SELECT id, value FROM w17cduck_alternating ORDER BY id').fetchall()
        assert rows == [(1, 999), (2, 2)], rows
        db.close()
    ''', database)


def test_memory_identity_and_global_hook(tmp_path):
    _run(tmp_path, '''
        import meerschaum as mrsm
        from meerschaum.connectors.sql import SQLConnector
        from meerschaum.connectors.sql import _duckdb_transport as transport
        a = SQLConnector('w17cduckmemory', flavor='duckdb', database=':memory:')
        b = SQLConnector('w17cduckmemory', flavor='duckdb', database=':memory:')
        p = mrsm.Pipe('w17cduck', 'memory', instance=a, columns={'id':'id'})
        assert p.sync([{'id':1, 'value':7}])[0]
        assert a.value('SELECT value FROM w17cduck_memory') == 7
        assert b.value("SELECT count(*) FROM information_schema.tables WHERE table_name='w17cduck_memory'") == 0
        cached = mrsm.get_connector('sql', 'w17cduckcached', flavor='duckdb', database=':memory:')
        q = mrsm.Pipe('w17cduck', 'cached', instance=cached, columns={'id':'id'})
        assert q.sync([{'id':1, 'value':8}])[0]
        # The embedded/native registry constructs without SDK injection. Its root+label alias
        # must resolve the canonical cached object, while direct objects remain isolated.
        from meerschaum import _core
        core = _core.SqlConnector(cached.label, {'flavor':'duckdb', 'database':':memory:'})
        assert core.get_pipe_rowcount(q) == 1
        anchor = transport.native_anchor(':memory:', cached.label, '', {})
        assert anchor.connect(native=True).execute('SELECT value FROM w17cduck_cached').fetchone() == (8,)
    ''')


def test_driver_scalar_and_complex_types(tmp_path):
    _run(tmp_path, r'''
        import datetime as dt
        from decimal import Decimal
        from uuid import UUID
        import meerschaum as mrsm
        from meerschaum.connectors.sql import _duckdb_transport as transport
        conn = mrsm.get_connector('sql', 'w17cducktypes', flavor='duckdb', database=':memory:')
        assert conn.exec("""CREATE TABLE w17cduck_types AS SELECT
            300::SMALLINT AS signed, 18446744073709551615::UBIGINT AS unsigned,
            '18446744073709551615'::DECIMAL(20,0) AS unsigned_decimal,
            123456789012345678901234567890::HUGEINT AS huge,
            123.45::DECIMAL(5,2) AS decimal_value,
            123456789012345678901234567890.123456::DECIMAL(38,6) AS wide_decimal,
            '0.12345678901234567890123456789012345678'::DECIMAL(38,38) AS fractional_decimal,
            '-0.12345678901234567890123456789012345678'::DECIMAL(38,38) AS negative_decimal,
            '0.00000000000000000000000000000000000001'::DECIMAL(38,38) AS tiny_decimal,
            '-0.00000000000000000000000000000000000001'::DECIMAL(38,38) AS negative_tiny_decimal,
            '\x01\x02'::BLOB AS bytes_value, uuid '12345678-1234-5678-1234-567812345678' AS uuid_value,
            DATE '2024-03-05' AS date_value, TIME '10:11:12.123456' AS time_value,
            TIMESTAMP '2024-01-02 03:04:05.123456' AS naive,
            TIMESTAMPTZ '2024-01-02 03:04:05+02' AS aware,
            TIMESTAMP 'infinity' AS positive_infinity, TIMESTAMPTZ '-infinity' AS negative_infinity,
            NULL::INTEGER AS null_value, '[1,null]'::JSON AS json_value,
            [NULL,'a'] AS nested_value, INTERVAL '1 year 2 months 3 days 4 hours' AS interval_value,
            TIMETZ '10:11:12+02' AS timetz_value
        """) is not None
        pipe = mrsm.Pipe('w17cduck', 'types', instance=conn)
        rows = pipe.get_data(as_docs=True)
        assert len(rows) == 1, rows
        row = rows[0]
        assert row['signed'] == 300 and row['unsigned'] == 18446744073709551615, repr(row)
        assert type(row['unsigned']) is int
        assert row['unsigned_decimal'] == Decimal('18446744073709551615')
        assert type(row['unsigned_decimal']) is Decimal
        direct = conn._core_connector().get_pipe_docs(pipe)
        assert type(direct[0]['unsigned']) is int and direct[0]['unsigned'] == row['unsigned']
        assert type(direct[0]['unsigned_decimal']) is Decimal
        assert Decimal(str(row['huge'])) == Decimal('123456789012345678901234567890')
        assert Decimal(str(row['decimal_value'])) == Decimal('123.45')
        assert type(row['decimal_value']) is Decimal
        assert Decimal(str(row['wide_decimal'])) == Decimal('123456789012345678901234567890.123456')
        assert Decimal(str(row['fractional_decimal'])) == Decimal('0.12345678901234567890123456789012345678')
        assert Decimal(str(row['negative_decimal'])) == Decimal('-0.12345678901234567890123456789012345678')
        assert Decimal(str(row['tiny_decimal'])) == Decimal('0.00000000000000000000000000000000000001')
        assert Decimal(str(row['negative_tiny_decimal'])) == Decimal('-0.00000000000000000000000000000000000001')
        assert row['bytes_value'] == b'\x01\x02'
        assert str(row['uuid_value']) == '12345678-1234-5678-1234-567812345678'
        assert str(row['date_value']) == '2024-03-05'
        assert str(row['time_value']) == '10:11:12.123456'
        assert str(row['naive']) == '2024-01-02 03:04:05.123456'
        assert str(row['aware']).startswith('2024-01-02 01:04:05')
        assert str(row['positive_infinity']).startswith('9999-12-31')
        assert str(row['negative_infinity']).startswith('0001-01-01')
        assert row['null_value'] is None
        assert row['json_value'] == [1, None]
        assert row['nested_value'] == '[NULL, a]'
        assert row['interval_value'] == '1 year 2 months 3 days 04:00:00'
        assert row['timetz_value'] == '10:11:12+02'
        anchor = transport.native_anchor(':memory:', conn.label, transport.memory_id(conn), {})
        db = anchor.connect(native=True)
        assert transport.execute(db, 'CREATE TABLE affected (id INT)', []) == 0
        assert transport.execute(db, 'INSERT INTO affected VALUES (1),(2)', []) == 2
        assert transport.execute(db, 'UPDATE affected SET id=id+1', []) == 2
        assert transport.execute(db, 'DELETE FROM affected', []) == 2
        # Casts preserve duplicate names and bound parameters in one execution plan.
        cursor, columns = transport.query(db, "SELECT [$1::INTEGER,NULL] AS x, INTERVAL '1 month' AS x", [3])
        assert cursor.fetchall() == [('[3, NULL]', '1 month')]
        assert [name for name, _ in columns] == ['x', 'x']
        cursor, columns = transport.query(db, 'SELECT $1 AS x, 2 AS x_1, 3 AS x', [1])
        assert cursor.fetchall() == [(1, 2, 3)]
        assert [name for name, _ in columns] == ['x', 'x_1', 'x']
        cursor, columns = transport.query(db, 'SELECT $1 AS x, 2 AS x WHERE FALSE', [1])
        assert cursor.fetchall() == [] and [name for name, _ in columns] == ['x', 'x']
        db.execute('CREATE SEQUENCE query_once')
        cursor, columns = transport.query(db, "SELECT nextval('query_once') AS n, $1 AS value", [9])
        assert cursor.fetchall() == [(1, 9)]
        assert [name for name, _ in columns] == ['n', 'value']
        assert db.execute("SELECT currval('query_once')").fetchone() == (1,)
        db.execute('CREATE TABLE query_side_effect (id INTEGER)')
        for sql in ('SELECT $1 AS x; INSERT INTO query_side_effect VALUES (1)',
                    'INSERT INTO query_side_effect VALUES ($1) RETURNING id'):
            try:
                transport.query(db, sql, [7])
            except RuntimeError as e:
                assert 'exactly one SELECT' in str(e), e
            else:
                raise AssertionError('unsafe bound read shape was executed')
        assert db.execute('SELECT count(*) FROM query_side_effect').fetchone() == (0,)
        assert conn.exec("""CREATE TABLE w17cduck_integer_edges AS
            SELECT 1 AS id, 7::UBIGINT AS value, 7::DECIMAL(20,0) AS numeric_value
            UNION ALL SELECT 2, 18446744073709551615::UBIGINT, '18446744073709551615'::DECIMAL(20,0)
            UNION ALL SELECT 3, NULL::UBIGINT, NULL::DECIMAL(20,0)
        """) is not None
        edges = mrsm.Pipe('w17cduck', 'integer_edges', instance=conn, columns={'id':'id'})
        expected = [
            {'id':1, 'value':7, 'numeric_value':Decimal('7')},
            {'id':2, 'value':18446744073709551615, 'numeric_value':Decimal('18446744073709551615')},
            {'id':3, 'value':None, 'numeric_value':None},
        ]
        for docs in (edges.get_data(as_docs=True), conn._core_connector().get_pipe_docs(edges)):
            assert docs == expected, docs
            assert type(docs[0]['value']) is int and type(docs[1]['value']) is int
            assert type(docs[0]['numeric_value']) is Decimal and type(docs[1]['numeric_value']) is Decimal
        chunks = list(edges.get_data(as_docs=True, as_chunks=True, chunksize=1))
        assert [row for chunk in chunks for row in chunk] == expected
    ''')


def test_native_pin_refuses_sqlalchemy_before_import(tmp_path):
    _run(tmp_path, '''
        import sys
        import meerschaum as mrsm
        c = mrsm.get_connector('sql', 'w17cduckpin', flavor='duckdb', database=':memory:')
        try:
            _ = c.engine
        except RuntimeError as e:
            assert 'Unset MRSM_DUCKDB_LIB' in str(e), e
        else:
            raise AssertionError('explicit native driver must refuse SQLAlchemy')
        assert 'duckdb' not in sys.modules
        assert 'duckdb_engine' not in sys.modules
    ''', native_pin=True)


def test_lazy_config_conflicts_and_session_settings(tmp_path):
    _run(tmp_path, '''
        import sys, duckdb
        from pathlib import Path
        import meerschaum as mrsm
        from meerschaum.connectors.sql import SQLConnector
        path = str(Path(sys.argv[1]) / 'config.duckdb')
        duckdb.connect(path).close()
        a = SQLConnector('w17cducka', flavor='duckdb', database=path)
        a._sys_config['create_engine']['connect_args'] = {'read_only': True}
        first = a.engine
        b = SQLConnector('w17cduckb', flavor='duckdb', database=path)
        try:
            _ = b.engine
        except RuntimeError as e:
            assert 'Conflicting DuckDB connection settings' in str(e), e
        else:
            raise AssertionError('unopened readonly engine configuration was replaced')
        with first.connect() as c:
            try:
                c.exec_driver_sql('CREATE TABLE forbidden (a INT)')
            except Exception as e:
                assert 'read-only' in str(e).lower() or 'read_only' in str(e).lower(), e
            else:
                raise AssertionError('readonly engine became writable')
        for key, value in [('creator', lambda: None), ('module', duckdb)]:
            c = SQLConnector('w17cduckoverride', flavor='duckdb', database=':memory:')
            try: _ = c.create_engine(**{key: value})
            except RuntimeError: pass
            else: raise AssertionError('unsafe custom driver override accepted')
        c = SQLConnector('w17cducksession', flavor='duckdb', database=':memory:')
        c._sys_config['create_engine']['connect_args'] = {'config': {'enable_progress_bar': True, 'TimeZone': 'Europe/Amsterdam'}}
        with c.engine.connect() as db:
            assert db.exec_driver_sql("SELECT current_setting('enable_progress_bar')").scalar() is True
            assert db.exec_driver_sql("SELECT current_setting('TimeZone')").scalar() == 'Europe/Amsterdam'
        from meerschaum.connectors.sql import _duckdb_transport as t
        native = t.native_anchor(':memory:', c.label, t.memory_id(c), {}).connect(native=True)
        assert native.execute("SELECT current_setting('enable_progress_bar')").fetchone() == (True,)
        assert native.execute("SELECT current_setting('TimeZone')").fetchone() == ('UTC',)
    ''', tmp_path)


def test_native_first_late_optional_driver_refuses_sdk(tmp_path):
    _run(tmp_path, '''
        import sys
        from meerschaum import _core
        from meerschaum.connectors.sql import _duckdb_transport as t
        original = t.importlib.util.find_spec
        t.importlib.util.find_spec = lambda name, *args, **kwargs: None if name == 'duckdb' else original(name, *args, **kwargs)
        core = _core.SqlConnector('w17cducklate', {'flavor':'duckdb', 'database':':memory:'})
        assert t.policy() == 'native'
        t.importlib.util.find_spec = original
        import meerschaum as mrsm
        c = mrsm.get_connector('sql', 'w17cducklate', flavor='duckdb', database=':memory:')
        try: _ = c.engine
        except RuntimeError as e: assert 'restart' in str(e), e
        else: raise AssertionError('late optional driver mixed with selected C API')
        assert 'duckdb' not in sys.modules
    ''')


def test_configured_memory_lifetime_and_reverse_sdk_adoption(tmp_path):
    _run(tmp_path, '''
        import gc
        from meerschaum import _core
        from meerschaum.connectors.sql import _duckdb_transport as t
        first = _core.SqlConnector('w17cduckconfigured', {'flavor':'duckdb', 'database':':memory:'})
        anchor = t.native_anchor(':memory:', 'w17cduckconfigured', '', {})
        db = anchor.connect(native=True)
        db.execute('CREATE TABLE configured_memory (value INT)')
        db.execute('INSERT INTO configured_memory VALUES (7)')
        db.close()
        del db, anchor, first
        gc.collect()
        second = _core.SqlConnector('w17cduckconfigured', {'flavor':'duckdb', 'database':':memory:'})
        import meerschaum as mrsm
        c = mrsm.get_connector('sql', 'w17cduckconfigured', flavor='duckdb', database=':memory:')
        assert c.value('SELECT value FROM configured_memory') == 7
        c._core_connector(discover_schema=False)
        assert c.value('SELECT value FROM configured_memory') == 7
    ''')


def test_relative_database_freezes_at_engine_registration(tmp_path):
    _run(tmp_path, '''
        import os, sys
        from pathlib import Path
        import meerschaum as mrsm
        from meerschaum.connectors.sql import SQLConnector
        start = Path.cwd()
        c = SQLConnector('w17cduckrelative', flavor='duckdb', database='relative.duckdb')
        engine = c.engine
        other = start / 'other'
        other.mkdir()
        os.chdir(other)
        assert c.exec('CREATE TABLE w17cduck_relative (id INT)') is not None
        pipe = mrsm.Pipe('w17cduck', 'relative', instance=c, columns={'id':'id'})
        assert pipe.sync([{'id':7}])[0]
        assert c.value('SELECT id FROM w17cduck_relative') == 7
        absolute = SQLConnector('w17cduckabsolute', flavor='duckdb', database=str(start / 'relative.duckdb'))
        assert absolute.value('SELECT id FROM w17cduck_relative') == 7
        assert not (other / 'relative.duckdb').exists()
    ''')


def test_opened_duckdb_refuses_fork_reuse(tmp_path):
    if not hasattr(os, 'fork'):
        pytest.skip('fork is unavailable')
    _run(tmp_path, '''
        import os
        import meerschaum as mrsm
        from meerschaum.connectors.sql import _duckdb_transport as t
        c = mrsm.get_connector('sql', 'w17cduckfork', flavor='duckdb', database=':memory:')
        assert c.value('SELECT 1') == 1
        pid = os.fork()
        if pid == 0:
            try: t.native_anchor(':memory:', c.label, t.memory_id(c), {})
            except RuntimeError as e: os._exit(0 if 'fresh interpreter' in str(e) else 2)
            except BaseException: os._exit(3)
            os._exit(4)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0, status
        assert c.value('SELECT 1') == 1
    ''')


def test_explicit_real_native_driver_pipe_then_sdk_refusal(tmp_path):
    library = os.environ.get('MRSM_TEST_DUCKDB_LIB') or os.environ.get('MRSM_DUCKDB_LIB')
    if not library or not Path(library).is_file():
        from tests._binary import in_ci
        if in_ci():
            pytest.fail('set MRSM_TEST_DUCKDB_LIB for the explicit native driver regression')
        pytest.skip('set MRSM_TEST_DUCKDB_LIB for the explicit native driver regression')
    database = tmp_path / 'native-pin.duckdb'
    _run(tmp_path, '''
        import sys
        import meerschaum as mrsm
        c = mrsm.get_connector('sql', 'w17cduckpinreal', flavor='duckdb', database=sys.argv[1])
        p = mrsm.Pipe('w17cduck', 'pinreal', instance=c, columns={'id':'id'})
        assert p.sync([{'id':1,'value':7}])[0]
        try: _ = c.engine
        except RuntimeError as e: assert 'Unset MRSM_DUCKDB_LIB' in str(e), e
        else: raise AssertionError('explicit native driver allowed mixed SQLAlchemy')
        assert p.get_data(as_docs=True) == [{'id':1,'value':7}]
        assert 'duckdb' not in sys.modules
    ''', database, native_pin=library)
    _run(tmp_path, '''
        import sys, duckdb
        db = duckdb.connect(sys.argv[1])
        assert db.execute('SELECT id, value FROM w17cduck_pinreal').fetchall() == [(1,7)]
        db.close()
    ''', database)


def test_embedded_uri_readonly_is_not_discarded(tmp_path):
    _run(tmp_path, '''
        import sys, duckdb
        from pathlib import Path
        import meerschaum as mrsm
        from meerschaum import _core
        path = str(Path(sys.argv[1]) / 'readonly.duckdb')
        duckdb.connect(path).close()
        core = _core.SqlConnector('w17cduckreadonly', {'uri':'duckdb:///' + path + '?access_mode=READ_ONLY'})
        # An unopened explicit native constraint cannot be replaced by a default SDK engine.
        from meerschaum.connectors.sql import SQLConnector
        attack = SQLConnector('w17cduckreadonlyattack', flavor='duckdb', database=path)
        try: _ = attack.engine
        except RuntimeError as e: assert 'Conflicting DuckDB connection settings' in str(e), e
        else: raise AssertionError('lazy SDK engine removed explicit native read-only constraint')
        # Registration invokes native DDL and must fail under the original URI access mode.
        conn = mrsm.get_connector('sql', 'w17cduckreadonly', uri='duckdb:///' + path + '?access_mode=READ_ONLY')
        p = mrsm.Pipe('w17cduck', 'readonly', instance=conn)
        result = core.register_pipe(p)
        assert not result[0], result
        from meerschaum.connectors.sql import _duckdb_transport as t
        anchor = t.native_anchor(path, conn.label, '', {'access_mode':'READ_ONLY'})
        db = anchor.connect(native=True)
        try: db.execute('CREATE TABLE forbidden (id INT)')
        except Exception as e: assert 'read-only' in str(e).lower() or 'read_only' in str(e).lower(), e
        else: raise AssertionError('URI read-only mode was discarded')
        assert db.execute("SELECT count(*) FROM information_schema.tables").fetchone() == (0,)
    ''', tmp_path)


def test_invalid_sdk_engine_configuration_never_opens_database(tmp_path):
    _run(tmp_path, '''
        import sys
        from pathlib import Path
        from meerschaum.connectors.sql import SQLConnector
        database = Path(sys.argv[1]) / 'must-not-exist.duckdb'
        c = SQLConnector('w17cduckinvalid', flavor='duckdb', database=str(database))
        c._sys_config['create_engine']['not_a_real_engine_argument'] = True
        try: c._core_connector(discover_schema=False)
        except RuntimeError as e: assert 'refusing native database access' in str(e), e
        else: raise AssertionError('invalid engine settings fell back to default native writes')
        assert not database.exists()
    ''', tmp_path)


@pytest.mark.parametrize('dependency', ['duckdb', 'duckdb_engine'])
def test_unsupported_driver_versions_refuse_before_open(tmp_path, dependency):
    _run(tmp_path, '''
        import sys, importlib
        from pathlib import Path
        from meerschaum.connectors.sql import SQLConnector
        module = importlib.import_module(sys.argv[1])
        module.__version__ = '1.4.0' if sys.argv[1] == 'duckdb' else '0.16.0'
        path = Path(sys.argv[2]) / 'unsupported-version.duckdb'
        c = SQLConnector('w17cduckold', flavor='duckdb', database=str(path))
        try: c._core_connector(discover_schema=False)
        except RuntimeError as e: assert 'upgrade and restart' in str(e), e
        else: raise AssertionError('unsupported installed driver fell back or opened a file')
        assert not path.exists()
    ''', dependency, tmp_path)


def test_driver_override_change_and_special_databases_refuse(tmp_path):
    _run(tmp_path, '''
        import os
        from pathlib import Path
        from meerschaum.connectors.sql import SQLConnector
        from meerschaum.connectors.sql import _duckdb_transport as t
        for database in (':default:', ':memory:named'):
            c = SQLConnector('w17cduckspecial', flavor='duckdb', database=database)
            try: _ = c.engine
            except RuntimeError as e: assert 'local file or :memory:' in str(e), e
            else: raise AssertionError('special database silently interpreted as file')
            assert not Path(database).exists()
        assert t.policy() == 'python'
        os.environ['MRSM_DUCKDB_LIB'] = '/not-opened/custom-driver.so'
        try: t.native_anchor(':memory:', 'w17cduckchange', '', {})
        except RuntimeError as e: assert 'changed' in str(e) and 'restart' in str(e), e
        else: raise AssertionError('driver selection changed inside an active process')
    ''')


def test_do_connect_refusal_and_standard_events(tmp_path):
    _run(tmp_path, '''
        from sqlalchemy import event
        from meerschaum.connectors.sql import SQLConnector
        c = SQLConnector('w17cduckevents', flavor='duckdb', database=':memory:')
        engine = c.engine
        calls = []
        event.listen(engine, 'connect', lambda *args: calls.append('connect'))
        event.listen(engine, 'checkout', lambda *args: calls.append('checkout'))
        event.listen(engine, 'begin', lambda *args: calls.append('begin'))
        with engine.connect() as db:
            assert db.exec_driver_sql('SELECT 1').scalar() == 1
        assert all(name in calls for name in ('connect','checkout','begin')), calls
        c = SQLConnector('w17cduckrouting', flavor='duckdb', database='routing-never-opened.duckdb')
        engine = c.engine
        def route(dialect, record, args, kwargs):
            kwargs['read_only'] = True
        event.listen(engine, 'do_connect', route)
        import meerschaum as mrsm
        p = mrsm.Pipe('w17cduck', 'routing', instance=c, columns={'id':'id'})
        try:
            result = p.sync([{'id':1}])
        except RuntimeError as e:
            assert 'do_connect' in str(e), e
        else:
            assert not result[0] and 'do_connect' in str(result), result
        try: engine.connect()
        except RuntimeError as e: assert 'do_connect' in str(e), e
        else: raise AssertionError('do_connect routing was silently bypassed')
        from pathlib import Path
        assert not Path('routing-never-opened.duckdb').exists()
    ''')


@pytest.mark.parametrize(('backend', 'child_exit'), [
    ('python', 'immediate'), ('native', 'immediate'),
    ('python', 'exec'), ('native', 'normal'),
])
def test_fork_child_gc_cannot_close_parent_database(tmp_path, backend, child_exit):
    if not hasattr(os, 'fork'):
        pytest.skip('fork is unavailable')
    library = os.environ.get('MRSM_TEST_DUCKDB_LIB') or os.environ.get('MRSM_DUCKDB_LIB')
    if backend == 'native' and (not library or not Path(library).is_file()):
        from tests._binary import in_ci
        if in_ci():
            pytest.fail('set MRSM_TEST_DUCKDB_LIB for native fork cleanup regression')
        pytest.skip('set MRSM_TEST_DUCKDB_LIB for native fork cleanup regression')
    database = tmp_path / 'fork-cleanup.duckdb'
    _run(tmp_path, '''
        import gc, os, sys
        import meerschaum as mrsm
        c = mrsm.get_connector('sql', 'w17cduckforkcleanup', flavor='duckdb', database=sys.argv[1])
        p = mrsm.Pipe('w17cduck', 'forkcleanup', instance=c, columns={'id':'id'})
        assert p.sync([{'id':1,'value':1}])[0]
        engine = c.engine if sys.argv[2] == 'python' else None
        held = engine.connect() if engine is not None else None
        if held is not None:
            assert held.exec_driver_sql('SELECT value FROM w17cduck_forkcleanup').scalar() == 1
            held.rollback()
        pid = os.fork()
        if pid == 0:
            try:
                # Explicitly remove inherited cached native handles and collect them. Python
                # raw wrappers may also be collected, but must never close inherited handles.
                cached = c.__dict__.pop('_rust_connector', None)
                del cached
                if engine is not None:
                    engine.dispose()
                del held
                gc.collect()
                try: _ = c.engine
                except RuntimeError as e:
                    assert 'fresh interpreter' in str(e)
                else: raise AssertionError('inherited engine access was allowed')
            except BaseException:
                os._exit(2)
            if sys.argv[3] == 'exec':
                # The public driver cannot safely finalize inherited worker threads. A
                # fresh interpreter is the supported workflow, with a new memory instance.
                fresh = (
                    "import meerschaum as m; "
                    "c=m.get_connector('sql','w17cduckfresh',flavor='duckdb',database=':memory:'); "
                    "p=m.Pipe('w17cduck','fresh',instance=c,columns={'id':'id'}); "
                    "assert p.sync([{'id':7}])[0]; assert p.get_data(as_docs=True)==[{'id':7}]"
                )
                os.execve(sys.executable, [sys.executable, '-B', '-s', '-c', fresh], os.environ.copy())
            if sys.argv[3] == 'normal':
                raise SystemExit(0)
            os._exit(0)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0, status
        result = p.sync([{'id':1,'value':999},{'id':2,'value':2}])
        assert result[0], result
        assert p.get_data(as_docs=True) == [{'id':1,'value':999},{'id':2,'value':2}]
        if held is not None: held.close()
        if engine is not None: engine.dispose()
    ''', database, backend, child_exit, native_pin=library if backend == 'native' else False)
    _run(tmp_path, '''
        import sys, duckdb
        c = duckdb.connect(sys.argv[1])
        assert c.execute('SELECT id,value FROM w17cduck_forkcleanup ORDER BY id').fetchall() == [(1,999),(2,2)]
        c.close()
    ''', database)


@pytest.mark.parametrize('reserved', ['_duckdb_database', '_duckdb_memory_id'])
def test_reserved_uri_transport_keys_never_override_identity(tmp_path, reserved):
    _run(tmp_path, '''
        import sys
        from pathlib import Path
        from urllib.parse import quote
        import meerschaum as mrsm
        from meerschaum import _core
        from meerschaum.connectors.sql import SQLConnector
        root = Path(sys.argv[1])
        key = sys.argv[2]
        expected = root / 'expected.duckdb'
        redirected = root / 'redirected.duckdb'
        value = str(redirected) if key == '_duckdb_database' else 'shared-memory-token'
        uri = 'duckdb:///' + str(expected) + '?' + quote(key) + '=' + quote(value)
        for constructor in (
            lambda: _core.SqlConnector('w17cduckreserved', {'uri':uri}),
            lambda: SQLConnector('w17cduckreserved', uri=uri)._core_connector(discover_schema=False),
            lambda: SQLConnector('w17cduckreserved', uri=uri).engine,
        ):
            try: constructor()
            except RuntimeError as e: assert 'Reserved DuckDB transport keys' in str(e), e
            else: raise AssertionError('URI transport identity key was accepted')
        assert not expected.exists() and not redirected.exists()
        canonical = mrsm.get_connector('sql','w17cduckreservedmemory',flavor='duckdb',database=':memory:')
        p = mrsm.Pipe('w17cduck','reservedmemory',instance=canonical,columns={'id':'id'})
        assert p.sync([{'id':7}])[0]
        token = canonical.__dict__['_duckdb_memory_id']
        try: _core.SqlConnector('w17cduckimpostor', {'uri':'duckdb:///:memory:?_duckdb_memory_id=' + token})
        except RuntimeError as e: assert 'Reserved DuckDB transport keys' in str(e), e
        else: raise AssertionError('URI joined a private memory anchor')
        for source in ('config', 'connect_args'):
            c = SQLConnector('w17cduckreservedconfig',flavor='duckdb',database=str(expected))
            options = {key:value}
            c._sys_config['create_engine']['connect_args'] = {'config':options} if source == 'config' else options
            try: c._core_connector(discover_schema=False)
            except RuntimeError as e: assert 'Reserved DuckDB transport keys' in str(e), e
            else: raise AssertionError('user settings accepted a reserved transport key')
        assert not expected.exists() and not redirected.exists()
    ''', tmp_path, reserved)


def test_native_lazy_relative_file_and_option_refusal(tmp_path):
    library = os.environ.get('MRSM_TEST_DUCKDB_LIB') or os.environ.get('MRSM_DUCKDB_LIB')
    if not library or not Path(library).is_file():
        from tests._binary import in_ci
        if in_ci():
            pytest.fail('set MRSM_TEST_DUCKDB_LIB for native lazy endpoint regression')
        pytest.skip('set MRSM_TEST_DUCKDB_LIB for native lazy endpoint regression')
    _run(tmp_path, '''
        import os
        from pathlib import Path
        from meerschaum import _core
        start = Path.cwd()
        native = _core.SqlConnector('w17cducknativelazy', {'flavor':'duckdb','database':'relative.duckdb'})
        other = start / 'other'
        other.mkdir()
        os.chdir(other)
        assert native.test_connection()
        assert (start / 'relative.duckdb').is_file()
        assert not (other / 'relative.duckdb').exists()
        forbidden = start / 'forbidden.duckdb'
        try: _core.SqlConnector('w17cducknativeoptions', {'uri':'duckdb:///' + str(forbidden) + '?access_mode=READ_ONLY'})
        except RuntimeError as e: assert 'connection options' in str(e), e
        else: raise AssertionError('native options silently discarded')
        assert not forbidden.exists()
    ''', native_pin=library)
