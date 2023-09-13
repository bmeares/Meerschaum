#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test SQL utility functions.
"""

import datetime
import pytest
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
