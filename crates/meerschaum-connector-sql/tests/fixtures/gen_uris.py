#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate `uris.json`: golden fixtures for `meerschaum_connector_sql::uri`.

Run from the repository root:

    PYTHONPATH=$(pwd) MRSM_ROOT_DIR="$(mktemp -d)" MRSM_PLUGINS_DIR="$(mktemp -d)" \
        ~/.config/meerschaum/venvs/mrsm/bin/python crates/meerschaum-connector-sql/tests/fixtures/gen_uris.py

Recorded per case:
- `parse_uri`: `SQLConnector.parse_uri(uri)` (or the exception text).
- `from_uri`: `SQLConnector.from_uri(uri, as_dict=True)` (or the exception text).
- `connector`: the attributes of `SQLConnector(uri=...)` / `SQLConnector(label, flavor=..., **attrs)`
  and the engine string `create_engine(include_uri=True)` builds.

No database is contacted. `create_engine` is made side-effect free by
- emptying `install_flavor_drivers` (so no driver is imported or pip-installed), and
- replacing `sqlalchemy.create_engine` with a stub returning `None`,
so only the engine string is captured.
"""

import json
import pathlib
import sys

from _offline_sql import assert_no_io

import meerschaum as mrsm
assert pathlib.Path(mrsm.__file__).resolve().is_relative_to(pathlib.Path.cwd().resolve()), mrsm.__file__

sqlalchemy = mrsm.attempt_import('sqlalchemy', lazy=False)
import meerschaum.connectors.sql._create_engine as _create_engine_module
from meerschaum.connectors.sql import SQLConnector

_create_engine_module.install_flavor_drivers.clear()
sqlalchemy.create_engine = lambda *args, **kwargs: None

URIS = [
    'postgresql+psycopg://test:test1234@localhost:5559/testdb',
    'postgresql://test:test1234@localhost:5559/testdb',
    'postgres://test:test1234@localhost/testdb',
    'timescaledb://test:test1234@localhost:5439/testdb',
    'timescaledb-ha://test:test1234@localhost:5459/testdb',
    'postgis://test:test1234@localhost:5449/testdb',
    'postgresql+psycopg://test:test1234@localhost:5449/postgis_db',
    'postgresql+psycopg://u:p%40ss%3Aw%2Frd@db.example.com:5432/mydb',
    'postgresql+psycopg://u:p+s%20s@[::1]:5432/mydb',
    'postgresql+psycopg://u:pw@localhost/mydb?options=--search_path%3Dfoo',
    'postgresql+psycopg://u:pw@localhost/mydb?sslmode=require&application_name=mrsm&empty=',
    'postgresql+psycopg://u@localhost/mydb',
    'postgresql+psycopg://localhost/mydb',
    'mysql+pymysql://test:test1234@localhost:3310/testdb',
    'mysql://test:test1234@localhost:3310/testdb?charset=utf8mb4',
    'mariadb://test:test1234@localhost:3309/testdb',
    'mariadb+pymysql://test:test1234@localhost:3309/testdb',
    'sqlite:////tmp/abs/Path.db',
    'sqlite:///rel.db',
    'sqlite:///:memory:',
    'sqlite://',
    'mssql+pyodbc://sa:supersecureSECRETPASSWORD123!@localhost:1439/master?driver=ODBC+Driver+17+for+SQL+Server',
    'mssql+pyodbc://sa:pw@localhost:1439/master?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes&driver=dup',
    'oracle+oracledb://system:oracle@localhost:1529/?service_name=XEPDB1',
    'oracle+oracledb://system:oracle@localhost:1529/xe',
    'duckdb:///x.duckdb',
    'duckdb:///:memory:',
    'cockroachdb://root@localhost:26257/defaultdb?sslmode=disable',
    'citus://u:p@localhost:5432/db',
    'postgresql+psycopg://user:pw@host:5432/db#frag?x=1',
]

ATTRIBUTE_CASES = [
    {'flavor': 'postgresql', 'host': 'localhost', 'username': 'mrsm', 'password': 'mrsm', 'database': 'meerschaum'},
    {'flavor': 'timescaledb', 'host': 'localhost', 'port': 5439, 'username': 'test', 'password': 'p@ss w/rd', 'database': 'testdb'},
    {'flavor': 'timescaledb-ha', 'host': 'localhost', 'port': 5459, 'username': 'test', 'password': 'test1234', 'database': 'testdb'},
    {'flavor': 'postgis', 'host': 'localhost', 'username': 'test', 'password': 'test1234', 'database': 'testdb', 'options': {'sslmode': 'disable'}},
    {'flavor': 'citus', 'host': 'localhost', 'username': 'test', 'password': 'test1234', 'database': 'testdb'},
    {'flavor': 'cockroachdb', 'host': 'localhost'},
    {'flavor': 'mysql', 'host': 'localhost', 'port': 3310, 'username': 'test', 'password': 'test1234', 'database': 'testdb'},
    {'flavor': 'mariadb', 'host': 'localhost', 'username': 'test', 'password': 'test1234', 'database': 'testdb', 'options': 'charset=utf8mb4&empty='},
    {'flavor': 'mssql', 'host': 'localhost', 'port': 1439, 'username': 'sa', 'password': 'supersecureSECRETPASSWORD123!', 'database': 'master'},
    {'flavor': 'oracle', 'host': 'localhost', 'port': 1529, 'username': 'system', 'password': 'oracle', 'database': 'xe'},
    {'flavor': 'sqlite', 'database': '/tmp/mrsm_test.db'},
    {'flavor': 'geopackage', 'database': '/tmp/mrsm_test.gpkg'},
    {'flavor': 'duckdb', 'database': '/tmp/mrsm_test.duckdb'},
    {'flavor': 'postgresql', 'host': 'localhost', 'username': 'u', 'database': 'db'},
    {'flavor': 'timescaledb', 'uri': 'postgresql+psycopg://test:test1234@localhost:5439/testdb'},
    {'flavor': 'postgis', 'uri': 'postgresql+psycopg://test:test1234@localhost:5449/postgis'},
]

_ATTR_KEYS = ('label', 'flavor', 'host', 'port', 'username', 'password', 'database', 'schema', 'uri')
_SKIP_KEYS = {'type', '_attributes', '_sys_config', '_original_dict', '_debug', '_pid',
              '_thread_ident', '_sessions', '_locks', '_engine', '_engine_str'}


def _jsonable(val):
    if val is None or isinstance(val, (bool, int, float, str)):
        return val
    if isinstance(val, dict):
        return {str(k): _jsonable(v) for k, v in val.items()}
    if isinstance(val, (list, tuple, set)):
        return [_jsonable(v) for v in val]
    return str(val)


def _capture(fn):
    try:
        return {'ok': _jsonable(fn())}
    except BaseException as e:
        return {'error': f"{type(e).__name__}: {e}"}


def _connector_attrs(conn):
    attrs = {}
    for key, val in conn.__dict__.items():
        if key in _SKIP_KEYS or key.startswith('_'):
            continue
        attrs[key] = _jsonable(val)
    return attrs


def _connector_case(make):
    try:
        conn = make()
    except BaseException as e:
        return {'error': f"{type(e).__name__}: {e}"}
    try:
        _, engine_str = conn.create_engine(include_uri=True)
    except BaseException as e:
        engine_str = f"ERROR {type(e).__name__}: {e}"
    return {'ok': {'attributes': _connector_attrs(conn), 'engine_str': engine_str}}


def main():
    cases = {'uris': [], 'attributes': []}
    for i, uri in enumerate(URIS):
        cases['uris'].append({
            'uri': uri,
            'parse_uri': _capture(lambda: SQLConnector.parse_uri(uri)),
            'from_uri': _capture(lambda: SQLConnector.from_uri(uri, as_dict=True)),
            'connector': _connector_case(lambda: SQLConnector(uri=uri)),
        })
    for i, attrs in enumerate(ATTRIBUTE_CASES):
        label = f"rs_fixture_{i}"
        attrs = dict(attrs)
        flavor = attrs.pop('flavor')
        cases['attributes'].append({
            'label': label,
            'flavor': flavor,
            'attributes': attrs,
            'connector': _connector_case(lambda: SQLConnector(label, flavor=flavor, **attrs)),
        })

    out_path = pathlib.Path(__file__).parent / 'uris.json'
    assert_no_io()
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(cases, f, indent=2)
        f.write('\n')
    print(f"Wrote {len(cases['uris'])} URI cases and {len(cases['attributes'])} attribute cases to {out_path}")


if __name__ == '__main__':
    sys.exit(main())
