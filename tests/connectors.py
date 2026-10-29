#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import os
from typing import Dict, List
from meerschaum import get_connector
from meerschaum.config._paths import ROOT_DIR_PATH
from tests._ports import service_port
data_path = ROOT_DIR_PATH / 'data'
data_path.mkdir(exist_ok=True)

conns = {
    'timescaledb': get_connector(
        'sql', 'test_timescaledb',
        flavor='timescaledb',
        username='test',
        password='test1234',
        database='testdb',
        port=service_port('timescaledb'),
        host='localhost',
        schema='public',
    ),
    'timescaledb_ha': get_connector(
        'sql', 'test_timescaledb_ha',
        flavor='timescaledb-ha',
        username='test',
        password='test1234',
        database='testdb',
        port=service_port('timescaledb_ha'),
        host='localhost',
        schema='public',
    ),
    'postgis': get_connector(
        'sql', 'test_postgis',
        flavor='postgis',
        username='test',
        password='test1234',
        database='testdb',
        port=service_port('postgis'),
        host='localhost',
        schema='public',
    ),
    'mariadb': get_connector(
        'sql', 'test_mariadb',
        flavor='mariadb',
        username='test',
        password='test1234',
        database='testdb',
        port=service_port('mariadb'),
        host='localhost',
    ),
    'mysql': get_connector(
        'sql', 'test_mysql',
        flavor='mysql',
        username='root',
        password='my-secret-pw',
        database='mysql',
        port=service_port('mysql'),
        host='localhost',
    ),
    'mssql': get_connector(
        'sql', 'test_mssql',
        flavor='mssql',
        username='sa',
        password='supersecureSECRETPASSWORD123!',
        database='master',
        port=service_port('mssql'),
        host='localhost',
        schema='dbo',
    ),
    'oracle': get_connector(
        'sql', 'test_oracle',
        flavor='oracle',
        host='localhost',
        database='xe',
        username='system',
        password='oracle',
        port=service_port('oracle'),
    ),
    'sqlite':  get_connector(
        'sql', 'test_sqlite',
        database=str(data_path / 'test_sqlite.db'),
        flavor='sqlite',
    ),
    #  'duckdb': get_connector(
        #  'sql', 'test_duckdb',
        #  database=str(data_path / 'test_duck.db'),
        #  flavor='duckdb',
    #  ),
    'api': get_connector(
        'api', 'test_api',
        port=service_port('api'),
        username='test',
        password='test1234',
        host='localhost',
    ),
    'valkey': get_connector(
        'valkey', 'test_valkey',
        port=service_port('valkey'),
        host='localhost',
    ),
}

### Flavors whose tables support reclaiming disk space (VACUUM / OPTIMIZE / REBUILD).
VACUUMABLE_FLAVORS = {
    'timescaledb', 'timescaledb-ha', 'postgresql', 'postgis', 'citus', 'mysql', 'mariadb',
    'mssql', 'sqlite', 'geopackage',
}
### `VACUUM FULL` rewrites the table and reclaims space to the OS (PostgreSQL family only).
FULL_VACUUM_FLAVORS = {'postgresql', 'postgis', 'citus'}
### Flavors whose tables support refreshing planner statistics.
ANALYZABLE_FLAVORS = VACUUMABLE_FLAVORS | {'cockroachdb', 'duckdb'}
### Flavors whose tables support some form of compression.
COMPRESSIBLE_FLAVORS = {'timescaledb', 'timescaledb-ha', 'mysql', 'mariadb', 'mssql'}
### Flavors with declarative range partitioning on a datetime column (TimescaleDB uses chunks).
PG_PARTITION_FLAVORS = {'postgresql', 'postgis'}
MYSQL_PARTITION_FLAVORS = {'mysql', 'mariadb'}
MSSQL_PARTITION_FLAVORS = {'mssql'}
PARTITIONABLE_FLAVORS = PG_PARTITION_FLAVORS | MYSQL_PARTITION_FLAVORS | MSSQL_PARTITION_FLAVORS


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
            'value': '2022-01-01 00:00:00+00:00',
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


def get_flavors() -> List[str]:
    """
    Get the flavors against which to test in this current environment.
    If `MRSM_TEST_FLAVORS` is set, split on commas and return the value.
    Otherwise return all possible flavors.
    """
    flavors_str = os.environ.get('MRSM_TEST_COLLECTION_FLAVORS') or os.environ.get(
        'MRSM_TEST_FLAVORS', ','.join(list(conns.keys()))
    )
    return sorted(flavors_str.split(','))
