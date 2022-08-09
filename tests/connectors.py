#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import pathlib
from meerschaum import get_connector
from meerschaum.config._paths import ROOT_DIR_PATH
data_path = ROOT_DIR_PATH / 'data'
data_path.mkdir(exist_ok=True)

conns = {
    'timescaledb': get_connector('sql', 'test_timescaledb',
        flavor='timescaledb', username='test', password='test1234', database='testdb',
        port=5439, host='localhost',
    ),
    'mariadb': get_connector('sql', 'test_mariadb',
        flavor='mariadb', username='test', password='test1234', database='testdb',
        port=3309, host='localhost',
    ),
    'mssql': get_connector('sql', 'test_mssql',
        flavor='mssql', username='sa', password='supersecureSECRETPASSWORD123!',
        database='master', port=1439, host='localhost',
    ),
    #  'cockroachdb': get_connector('sql', 'test_cockroachdb',
        #  flavor='cockroachdb', host='localhost', port=26259,
    #  ),
    'oracle': get_connector('sql', 'test_oracle',
        flavor='oracle', host='localhost', database='xe', username='system', password='oracle',
        port=1529,
    ),
    'sqlite':  get_connector('sql', 'test_sqlite',
        database=str(data_path / 'test_sqlite.db'),
        flavor='sqlite',
    ),
    'duckdb': get_connector('sql', 'test_duckdb',
        database=str(data_path / 'test_duckdb.db'),
        flavor='duckdb',
    ),
    'citus': get_connector('sql', 'test_citus',
        flavor='citus', username='test', password='test1234', database='testdb',
        port=5499, host='localhost',
    ),
    'api': get_connector('api', 'test_api',
        port=8989, username='test', password='test1234', host='localhost',
    ),
}

