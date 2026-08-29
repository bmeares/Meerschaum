#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test SQL utility functions.
"""

import datetime
from types import SimpleNamespace
import pytest
import meerschaum as mrsm
from tests.connectors import conns, get_flavors
from meerschaum.connectors.sql import SQLConnector
from meerschaum.connectors.sql.tools import dateadd_str, table_exists, sql_item_name
import dateutil.parser

@pytest.mark.parametrize("flavor", get_flavors())
def test_dateadd_str(flavor: str):
    """
    Verify that the DATEADD function works for all flavors.
    """
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    td_margin = (
        datetime.timedelta(microseconds=1000)
        if conn.flavor != 'sqlite' else datetime.timedelta(days=1)
    )
    td_advance = datetime.timedelta(days=1)
    dt = datetime.datetime(2022, 1, 2, 3, 4, 5, 678000)
    dt_str = dateadd_str(conn.flavor, begin=dt, number=td_advance.days)
    q = f"SELECT {dt_str}" + ('' if conn.flavor != 'oracle' else ' FROM DUAL')
    dt_val = conn.value(q)
    assert dt_val is not None
    if conn.flavor == 'sqlite':
        dt_val = dateutil.parser.parse(dt_val)
    assert ((dt + td_advance) - dt_val) <= td_margin


@pytest.mark.parametrize("flavor", get_flavors())
def test_exists(flavor: str):
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    tbl = "foo"
    tbl_name = sql_item_name(tbl, conn.flavor)
    conn.exec(f"DROP TABLE {tbl_name}", silent=True)
    assert table_exists(tbl, conn) is False
    assert conn.exec(f"CREATE TABLE {tbl_name} (bar INT)", commit=True, debug=True) is not None
    assert table_exists(tbl, conn, debug=True) is True
    conn.exec(f"DROP TABLE {tbl_name}", silent=True)
    assert table_exists(tbl, conn, debug=True) is False
 

@pytest.mark.parametrize(
    "uri,expected_attributes", [
        (
            'postgresql://mrsm:mrsm@localhost:5432/meerschaum',
            {
                'flavor': 'postgresql',
                'username': 'mrsm',
                'password': 'mrsm',
                'host': 'localhost',
                'port': 5432,
                'database': 'meerschaum',
            }
        ),
        (
            'timescaledb://mrsm:mrsm@localhost:5432/meerschaum',
            {
                'flavor': 'timescaledb',
                'username': 'mrsm',
                'password': 'mrsm',
                'host': 'localhost',
                'port': 5432,
                'database': 'meerschaum',
            }
        ),
        (
            'sqlite:////home/foo/.config/meerschaum/sqlite/mrsm_local.db',
            {
                'flavor': 'sqlite',
                'database': '/home/foo/.config/meerschaum/sqlite/mrsm_local.db',
            }
        ),
        (
            'mssql+pyodbc://sa:supersecureSECRETPASSWORD123!'
            + '@localhost:1439/master?driver=ODBC+Driver+17+for+SQL+Server',
            {
                'flavor': 'mssql',
                'username': 'sa',
                'password': 'supersecureSECRETPASSWORD123!',
                'host': 'localhost',
                'port': 1439,
                'database': 'master',
                'driver': 'ODBC Driver 17 for SQL Server',
            },
        ),
        (
            'http://user:pass@localhost:8000',
            {
                'username': 'user',
                'password': 'pass',
                'host': 'localhost',
                'port': 8000,
                'flavor': 'http',
            }
        ),
    ],
)
def test_parse_uri(uri: str, expected_attributes):
    """
    Text that parsing a URI string returns the expected dictionary.
    """
    assert SQLConnector.parse_uri(uri) == expected_attributes


def test_get_pipe_data_uses_adbc_for_polars(monkeypatch):
    """Arrow-safe Polars reads use ADBC and strip SQLAlchemy's dialect from the URI."""
    import polars as pl
    import meerschaum.utils.packages as packages
    from meerschaum.connectors.sql._pipes import get_pipe_data

    expected = pl.DataFrame({'id': [1]})
    real_attempt_import = packages.attempt_import

    def attempt_import(*names, **kw):
        if names == ('adbc_driver_postgresql',):
            assert kw.get('install') is False
            return SimpleNamespace()
        return real_attempt_import(*names, **kw)

    monkeypatch.setattr(
        packages,
        'attempt_import',
        attempt_import,
    )
    monkeypatch.setattr(
        pl,
        'read_database_uri',
        lambda query, uri, engine: (
            expected
            if (
                query == 'SELECT id FROM test'
                and uri == 'postgresql://user:pass@localhost/db'
                and engine == 'adbc'
            )
            else None
        ),
    )
    connector = SimpleNamespace(
        flavor='postgresql',
        URI='postgresql+psycopg://user:pass@localhost/db',
        get_pipe_data_query=lambda *args, **kw: 'SELECT id FROM test',
    )
    pipe = SimpleNamespace(
        enforce=True,
        get_columns_types=lambda **kw: {'id': 'BIGINT'},
        get_dtypes=lambda **kw: {'id': 'int64'},
    )

    assert get_pipe_data(connector, pipe, as_polars=True) is expected


def test_duckdb_unchunked_read_iterator(tmp_path):
    """DuckDB's unchunked Pandas result is exposed as one iterator chunk."""
    if mrsm.attempt_import('duckdb', install=False, warn=False) is None:
        pytest.skip("DuckDB is not installed.")

    connector = SQLConnector(
        'test_duckdb_iterator',
        flavor='duckdb',
        database=str(tmp_path / 'iterator.duckdb'),
    )
    try:
        assert connector.exec('CREATE TABLE test (id BIGINT)', commit=True) is not None
        assert connector.exec('INSERT INTO test VALUES (1)', commit=True) is not None
        assert connector.read('SELECT * FROM test', chunksize=None)['id'].tolist() == [1]
        assert connector.read(
            'SELECT * FROM test',
            chunksize=None,
            chunk_hook=len,
            as_hook_results=True,
        ) == [1]
        chunks = list(connector.read('SELECT * FROM test', chunksize=None, as_iterator=True))
        assert len(chunks) == 1
        assert chunks[0]['id'].tolist() == [1]
    finally:
        if connector.engine is not None:
            connector.engine.dispose()


def test_wrap_query_with_cte_flattens_sub_query_ctes():
    """
    A sub-query with its own CTEs must be flattened into one `WITH` list.
    """
    from meerschaum.utils.sql import wrap_query_with_cte
    sub_query = "WITH foo AS (SELECT 1 AS val) SELECT (val * 2) AS newval FROM foo"
    parent_query = 'SELECT newval * 3 FROM "src"'
    query = wrap_query_with_cte(sub_query, parent_query, 'postgresql')
    assert query.lower().count('with') == 1
    assert '"src" AS (' in query
    assert query.rstrip().endswith(parent_query)


def test_wrap_query_with_cte_tolerates_comments():
    """
    `--` comments — including ones containing the word "select" — must not
    corrupt the flattened query.
    """
    from meerschaum.utils.sql import wrap_query_with_cte
    sub_query = (
        "WITH a AS (\n"
        "    SELECT 1 AS val\n"
        ")\n"
        "-- now select the final rows\n"
        "SELECT * FROM a"
    )
    parent_query = 'SELECT * FROM "src"'
    query = wrap_query_with_cte(sub_query, parent_query, 'postgresql')
    lines = query.splitlines()
    assert '-- now select the final rows' in lines
    ### The joining comma must be on its own line (not swallowed by the comment).
    comment_ix = lines.index('-- now select the final rows')
    assert lines[comment_ix + 1].lstrip().startswith(',')
    assert 'SELECT * FROM a' in query


def test_wrap_query_with_cte_tolerates_leading_comment_before_with():
    """
    A comment before the leading `WITH` must still trigger flattening
    (instead of illegally nesting a CTE inside a CTE body).
    """
    from meerschaum.utils.sql import wrap_query_with_cte
    sub_query = (
        "-- get the rows\n"
        "WITH a AS (SELECT 1 AS val)\n"
        "SELECT * FROM a"
    )
    parent_query = 'SELECT * FROM "src"'
    query = wrap_query_with_cte(sub_query, parent_query, 'mssql')
    assert query.lower().count('with') == 1


def test_wrap_query_with_cte_hoists_parent_ctes():
    """
    A parent query declaring its own CTEs (the pushdown path) must be
    hoisted into the enclosing `WITH` list, not emitted as a second `WITH`.
    """
    from meerschaum.utils.sql import wrap_query_with_cte
    sub_query = 'SELECT * FROM "parent_tbl" WHERE "ts" >= 1'
    parent_query = (
        'WITH a AS (SELECT * FROM "src")\n'
        "SELECT * FROM a"
    )
    query = wrap_query_with_cte(sub_query, parent_query, 'postgresql', cte_name='src')
    assert query.lower().count('with ') == 1
    assert query.lower().lstrip().startswith('with')
    assert ', a AS (' in query


def test_wrap_query_with_cte_raises_on_missing_top_level_select():
    """
    An unwrappable definition must fail loudly instead of emitting
    invalid SQL.
    """
    from meerschaum.utils.sql import wrap_query_with_cte
    sub_query = "WITH a AS (SELECT 1 AS val)"
    parent_query = 'SELECT * FROM "src"'
    with pytest.raises(Exception):
        wrap_query_with_cte(sub_query, parent_query, 'postgresql')


def test_find_top_level_select_index_skips_comments_and_strings():
    """
    The top-level `SELECT` scan must skip comments, string literals,
    and quoted identifiers.
    """
    from meerschaum.utils.sql import find_top_level_select_index
    query = (
        "-- select nothing here\n"
        "/* select nothing here either */\n"
        "WITH a AS (SELECT 'select' AS \"select\")\n"
        "SELECT * FROM a"
    )
    ix = find_top_level_select_index(query)
    assert query[ix:].startswith('SELECT * FROM a')
    last_ix = find_top_level_select_index(query, last=True)
    assert last_ix == ix
    assert find_top_level_select_index("WITH a AS (SELECT 1)") == -1
