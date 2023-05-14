#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

from typing import Dict
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
    'mysql': get_connector('sql', 'test_mysql',
        flavor='mysql', username='root', password='my-secret-pw', database='mysql',
        port=3310, host='localhost',
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
        #  database=str(data_path / 'test_duckdb.db'),
        database=str(data_path / 'test_duck.db'),
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

def get_dtypes(debug: bool = False) -> Dict[str, Dict[str, 'sqlalchemy.types.Type']]:
    """
    Print every pandas dtype and database dtype.
    """
    from meerschaum.utils.packages import attempt_import, import_pandas
    from meerschaum.utils.sql import get_sqlalchemy_table
    pd = import_pandas()
    sqlalchemy = attempt_import('sqlalchemy')
    result_dtypes = {}

    dtypes = {
        'datetime64[ns]': {
            'value': '2022-01-01',
        },
        'datetime64[ns, UTC]': {
            'value': '2022-01-01',
        },
        'float': {
            'value': 1.0,
        },
        'int64': {
            'value': 1,
        },
        'Int64': {
            'value': 1,
        },
        'bool': {
            'value': True,
        },
        'object': {
            'value': 'foo',
        },
        'json': {
            'value': '{"foo": "bar"}',
            'local': 'object',
            'to_sql': sqlalchemy.types.JSON,
        },
    }
    table_name_base = '_dtype_temp'
    for i, (dtype, params) in enumerate(dtypes.items()):
        df = pd.DataFrame([{'a': params['value']}], dtype=params.get('local', dtype))
        table_name = table_name_base + '_' + str(i)

        result_dtypes[dtype] = {}

        for flavor, conn in conns.items():
            if conn.type != 'sql':
                continue
            kw = {'debug': debug, 'if_exists': 'replace'}
            if 'to_sql' in params:
                kw['dtype'] = {'a': params['to_sql']}
            if not conn.to_sql(df, table_name, **kw):
                kw.pop('dtype', None)
                conn.to_sql(df, table_name, **kw)
            table_obj = get_sqlalchemy_table(table_name, conn, refresh=True, debug=debug)
            result_dtypes[dtype][flavor] = str(table_obj.columns['a'].type)

    return result_dtypes
