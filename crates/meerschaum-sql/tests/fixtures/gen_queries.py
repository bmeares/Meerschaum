#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Generate the golden SQL fixtures for `meerschaum-sql` (design doc §10.2).

Calls the v4 Python query builders with synthetic inputs, for every flavor and every pipe shape,
and records the exact query text (or the error) as JSON. `tests/golden.rs` replays each case
through the Rust port and asserts byte-for-byte equality.

Run from the repository root:

    PYTHONPATH=$(pwd) MRSM_ROOT_DIR="$(mktemp -d)" MRSM_PLUGINS_DIR="$(mktemp -d)" \
        ~/.config/meerschaum/venvs/mrsm/bin/python crates/meerschaum-sql/tests/fixtures/gen_queries.py

Both `MRSM_ROOT_DIR` and `MRSM_PLUGINS_DIR` must be empty temporary directories so no plugins
(or real configuration) are ever loaded; every input below is synthetic.

How functions that need a live database were fixtured (no database is ever contacted):

- `SQLConnector` objects are built for every flavor with dummy credentials and an explicit
  `schema` (so `SQLConnector.schema` never inspects an engine). SQLite-family connectors point at
  a throwaway path that is never opened.
- Pipe-level methods receive a `StubPipe`: the real `Pipe` methods (`get_dtypes`,
  `get_precision`, `get_chunk_interval`, `get_backtrack_interval`, `guess_datetime`) are bound to
  it, while state that would come from the database (`get_columns_types`,
  `get_pipe_columns_indices`, `exists`, `get_sync_time`) is synthetic.
- `StubPipe.indices` reproduces `Pipe.indices` except that the `unique` list keeps a
  deterministic order (datetime first, then `columns` order): Python's `list(set(...))` depends
  on `PYTHONHASHSEED`. Likewise `get_create_index_queries` builds `indices_cols_str` from a set;
  a module-level `list` shim orders sets the same way while that function runs.
- `SQLConnector.value` / `.exec` / `.exec_queries` / `.read` / `.execute` and
  `meerschaum.utils.sql.table_exists` are monkeypatched per case to record the queries a method
  issues and to return synthetic results (e.g. a hypertable check, a max partition boundary).
"""

import json
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone

import meerschaum as mrsm
from meerschaum.connectors.sql import SQLConnector
from meerschaum.utils import sql as usql
from meerschaum.utils.dtypes import sql as dsql
from meerschaum.utils import dtypes as mdtypes
from meerschaum.utils.misc import truncate_string_sections
from meerschaum.core.Pipe import _attributes as pipe_attrs
from meerschaum.core.Pipe import _data as pipe_data
from meerschaum.core.Pipe import _fetch as pipe_fetch
from meerschaum.connectors.sql import _pipes as sql_pipes
from meerschaum.connectors.sql import _fetch as sql_fetch
from meerschaum.connectors.sql import _partition as sql_partition
from meerschaum.connectors.sql import _compress as sql_compress

assert mrsm.__file__.startswith(os.getcwd()), (
    f"Stale meerschaum imported from {mrsm.__file__}; set PYTHONPATH=$(pwd)."
)

pd = mrsm.attempt_import('pandas', lazy=False)

FLAVORS = [
    'timescaledb', 'timescaledb-ha', 'postgresql', 'postgis', 'citus', 'cockroachdb',
    'mssql', 'mysql', 'mariadb', 'oracle', 'sqlite', 'geopackage', 'duckdb',
]
OUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'queries.json')
TMP_DIR = tempfile.mkdtemp(prefix='mrsm_golden_')

CASES = []


def enc(x):
    """Encode Python values for JSON (datetimes and timedeltas are tagged)."""
    if isinstance(x, datetime):
        return {'$dt': x.isoformat()}
    if isinstance(x, timedelta):
        return {'$td_us': (x.days * 86400 + x.seconds) * 1_000_000 + x.microseconds}
    if isinstance(x, tuple):
        return [enc(v) for v in x]
    if isinstance(x, list):
        return [enc(v) for v in x]
    if isinstance(x, dict):
        return {str(k): enc(v) for k, v in x.items()}
    return x


def add(fn, args, func):
    """Run `func()` and record its result (or error)."""
    case = {'fn': fn, 'args': enc(args)}
    try:
        case['result'] = enc(func())
    except Exception as e:
        case['error'] = f"{type(e).__name__}: {e}"
    CASES.append(case)


### ------------------------------------------------------------------------------------------
### Connectors and stub pipes
### ------------------------------------------------------------------------------------------

_CONNS = {}


def get_conn(flavor):
    if flavor in _CONNS:
        return _CONNS[flavor]
    kw = {}
    if flavor in ('sqlite', 'geopackage', 'duckdb'):
        kw['database'] = os.path.join(TMP_DIR, f'{flavor}.db')
    else:
        kw.update({'host': 'localhost', 'username': 'u', 'password': 'p', 'database': 'db'})
    schema = None if flavor in usql.NO_SCHEMA_FLAVORS else usql.DEFAULT_SCHEMA_FLAVORS.get(flavor)
    conn = SQLConnector(f'golden_{flavor.replace("-", "_")}', flavor=flavor, schema=schema, **kw)
    conn.__dict__['schema'] = schema
    conn.__dict__['_db_version'] = '3.45.1' if flavor in ('sqlite', 'geopackage') else '16.0'
    _CONNS[flavor] = conn
    return conn


class StubPipe:
    """A duck-typed pipe exposing what the SQL builders read (see module docstring)."""

    def __init__(self, conn, parameters, target, ck='plugin:golden', mk='m', lk=None):
        self.connector_keys = ck
        self.metric_key = mk
        self.location_key = lk
        self.parameters = parameters
        self.target = target
        self.instance_connector = conn
        self.debug = False
        self.temporary = True
        self.parent = None
        self._columns_types = {}
        self._columns_indices = {}
        self._sync_time = None
        self._exists = True

    def __str__(self):
        return f"Pipe('{self.connector_keys}', '{self.metric_key}')"

    @property
    def instance_keys(self):
        return str(self.instance_connector)

    ### Real `Pipe` methods.
    get_dtypes = pipe_attrs.get_dtypes
    get_precision = pipe_attrs.get_precision
    guess_datetime = pipe_attrs.guess_datetime
    get_chunk_interval = pipe_data.get_chunk_interval
    _scale_delta_to_int_axis = pipe_data._scale_delta_to_int_axis
    get_backtrack_interval = pipe_fetch.get_backtrack_interval

    def get_parameters(self, apply_symlinks=True, refresh=False, debug=False, **kw):
        return self.parameters

    def infer_dtypes(self, *args, **kwargs):
        return {}

    def _get_cached_value(self, *args, **kwargs):
        return None

    def _cache_value(self, key, value, *args, **kwargs):
        self.__dict__['_' + key] = value

    def _clear_cache_key(self, *args, **kwargs):
        pass

    @property
    def columns(self):
        return self.parameters.get('columns', {})

    @property
    def dtypes(self):
        return self.get_dtypes(infer=False)

    @property
    def precision(self):
        return self.get_precision()

    @property
    def upsert(self):
        return self.parameters.get('upsert', False)

    @property
    def autotime(self):
        return self.parameters.get('autotime', False)

    @property
    def indices(self):
        _parameters = self.parameters
        indices_key = 'indexes' if 'indexes' in _parameters else 'indices'
        _indices = _parameters.get(indices_key, {})
        _columns = self.columns
        dt_col = _columns.get('datetime', None)
        if not isinstance(_indices, dict):
            _indices = {}
        unique_cols = []
        for col in ([dt_col] if dt_col else []) + [
            col for col_ix, col in _columns.items() if col and col_ix != 'datetime'
        ]:
            if col not in unique_cols:
                unique_cols.append(col)
        return {
            **({'unique': unique_cols} if len(unique_cols) > 1 else {}),
            **{col_ix: col for col_ix, col in _columns.items() if col},
            **_indices,
        }

    def get_columns(self, *args, error=False):
        if len(args) == 1:
            return self.columns.get(args[0], None)
        return tuple(self.columns.get(a, None) for a in args)

    def get_indices(self):
        return self.instance_connector.get_pipe_index_names(self)

    def get_columns_types(self, *args, **kwargs):
        return self._columns_types

    def get_columns_indices(self, *args, **kwargs):
        return self._columns_indices

    def exists(self, *args, **kwargs):
        return self._exists

    def get_sync_time(self, *args, **kwargs):
        return self._sync_time

    def get_size(self, *args, **kwargs):
        return None

    def update_parameters(self, *args, **kwargs):
        return True, "Success"


def pipe_json(pipe):
    return {
        'connector_keys': pipe.connector_keys,
        'metric_key': pipe.metric_key,
        'location_key': pipe.location_key,
        'target': pipe.target,
        'parameters': pipe.parameters,
    }


class Recorder:
    """Monkeypatch connector I/O to record queries."""

    def __init__(self, conn, value=None, read=None, table_exists=True):
        self.conn = conn
        self.queries = []
        self._value = value
        self._read = read
        self._table_exists = table_exists

    def __enter__(self):
        conn = self.conn
        rec = self

        def value(query, *args, **kwargs):
            rec.queries.append(str(query))
            v = rec._value
            return v(query) if callable(v) else v

        def exec_(query, *args, **kwargs):
            rec.queries.append(str(query))
            return True

        def exec_queries(queries, *args, **kwargs):
            rec.queries.extend([str(q) for q in queries])
            return [True for _ in queries]

        def read(query, *args, **kwargs):
            rec.queries.append(str(query))
            r = rec._read
            return r(query) if callable(r) else r

        conn.value = value
        conn.exec = exec_
        conn.exec_queries = exec_queries
        conn.read = read
        self._old_table_exists = usql.table_exists
        usql.table_exists = lambda *a, **k: rec._table_exists
        return self

    def __exit__(self, *exc):
        for attr in ('value', 'exec', 'exec_queries', 'read'):
            self.conn.__dict__.pop(attr, None)
        usql.table_exists = self._old_table_exists


### ------------------------------------------------------------------------------------------
### Static tables (re-check the generated Rust templates)
### ------------------------------------------------------------------------------------------

add('templates', {}, lambda: {
    'UPDATE_QUERIES': {k: (v if isinstance(v, list) else [v]) for k, v in usql.UPDATE_QUERIES.items()},
    'columns_types_queries': usql.columns_types_queries,
    'columns_indices_queries': usql.columns_indices_queries,
    'reset_autoincrement_queries': usql.reset_autoincrement_queries,
    'version_queries': usql.version_queries,
    'hypertable_queries': usql.hypertable_queries,
    'table_wrappers': {k: list(v) for k, v in usql.table_wrappers.items()},
    'max_name_lens': usql.max_name_lens,
})

add('flavor_sets', {}, lambda: {
    'SKIP_IF_EXISTS_FLAVORS': sorted(usql.SKIP_IF_EXISTS_FLAVORS),
    'DROP_IF_EXISTS_FLAVORS': sorted(usql.DROP_IF_EXISTS_FLAVORS),
    'DROP_INDEX_IF_EXISTS_FLAVORS': sorted(usql.DROP_INDEX_IF_EXISTS_FLAVORS),
    'SKIP_AUTO_INCREMENT_FLAVORS': sorted(usql.SKIP_AUTO_INCREMENT_FLAVORS),
    'COALESCE_UNIQUE_INDEX_FLAVORS': sorted(usql.COALESCE_UNIQUE_INDEX_FLAVORS),
    'json_flavors': sorted(usql.json_flavors),
    'NO_SCHEMA_FLAVORS': sorted(usql.NO_SCHEMA_FLAVORS),
    'DEFAULT_SCHEMA_FLAVORS': usql.DEFAULT_SCHEMA_FLAVORS,
    'OMIT_NULLSFIRST_FLAVORS': sorted(usql.OMIT_NULLSFIRST_FLAVORS),
    'SINGLE_ALTER_TABLE_FLAVORS': sorted(usql.SINGLE_ALTER_TABLE_FLAVORS),
    'NO_CTE_FLAVORS': sorted(usql.NO_CTE_FLAVORS),
    'NO_SELECT_INTO_FLAVORS': sorted(usql.NO_SELECT_INTO_FLAVORS),
    'NUMERIC_AS_TEXT_FLAVORS': sorted(dsql.NUMERIC_AS_TEXT_FLAVORS),
    'TIMEZONE_NAIVE_FLAVORS': sorted(dsql.TIMEZONE_NAIVE_FLAVORS),
    'NUMERIC_PRECISION_FLAVORS': {k: list(v) for k, v in dsql.NUMERIC_PRECISION_FLAVORS.items()},
    'AUTO_INCREMENT_COLUMN_FLAVORS': dsql.AUTO_INCREMENT_COLUMN_FLAVORS,
    'PG_PARTITION_FLAVORS': sorted(sql_partition.PG_PARTITION_FLAVORS),
    'MYSQL_PARTITION_FLAVORS': sorted(sql_partition.MYSQL_PARTITION_FLAVORS),
    'MSSQL_PARTITION_FLAVORS': sorted(sql_partition.MSSQL_PARTITION_FLAVORS),
    'PARTITIONABLE_FLAVORS': sorted(sql_partition.PARTITIONABLE_FLAVORS),
    'COMPRESSIBLE_FLAVORS': sorted(sql_compress.COMPRESSIBLE_FLAVORS),
    'POSTGRES_UNIQUE_CONSTRAINT_FLAVORS': sorted(sql_pipes._POSTGRES_UNIQUE_CONSTRAINT_FLAVORS),
    'flavor_configs': {
        f: {
            'engine': c.get('engine'),
            'defaults': {k: str(v) for k, v in c.get('defaults', {}).items()},
            'requirements': sorted(c.get('requirements', []) or []),
        }
        for f, c in SQLConnector.flavor_configs.items()
    },
})

add('pd_to_db_table', {}, lambda: dsql.PD_TO_DB_DTYPES_FLAVORS)
add('db_to_pd_table', {}, lambda: dsql.DB_TO_PD_DTYPES)
add('cast_dtypes_table', {}, lambda: dsql.DB_FLAVORS_CAST_DTYPES)


### ------------------------------------------------------------------------------------------
### Identifiers
### ------------------------------------------------------------------------------------------

LONG = 'a_very_long_table_name_section_' * 5
ITEMS = [
    'table', 'my_table', 'MyTable', 'Mixed Case', '_leading', 'date', 'float', 'row',
    'with"quote', "with'apostrophe", '#temp_table', 'plugin_noaa_weather_station',
    LONG, LONG.upper(), 'x' * 40 + '_' + 'y' * 40, 'ümlaut_col', 'a' * 64, 'b' * 30,
]
for flavor in FLAVORS:
    for item in ITEMS:
        for schema in (None, 'my_schema'):
            add('sql_item_name', {'item': item, 'flavor': flavor, 'schema': schema},
                lambda: usql.sql_item_name(item, flavor, schema))
        add('truncate_item_name', {'item': item, 'flavor': flavor},
            lambda: usql.truncate_item_name(item, flavor))

for s in ['abc', 'Abc', '_abc', 'a b', '"quoted"', 'a"b', 'abc123', 'ä', 'A_B', '']:
    for qc in (True, False):
        add('pg_capital', {'s': s, 'quote_capitals': qc}, lambda: usql.pg_capital(s, qc))

for s in ['ok', 'drop table x', 'x;', 'a -- comment', 'UNION select', "' OR '1'='1", 'created_at',
          'update_time', 'insert into', '/* c */']:
    add('clean', {'s': s}, lambda: usql.clean(s))

for item, max_len in [('abc_def_ghi', 10), ('abc', 10), ('a_b_c_d_e_f_g', 5), ('x' * 70, 64),
                      ('a' * 64, 64), ('foo_bar_baz_qux_quux', 12)]:
    add('truncate_string_sections', {'item': item, 'max_len': max_len},
        lambda: truncate_string_sections(item, max_len=max_len))


### ------------------------------------------------------------------------------------------
### build_where
### ------------------------------------------------------------------------------------------

PARAMS = [
    {'color': 'red'},
    {'color': ['red', 'blue']},
    {'color': '_red'},
    {'color': ['_red', '_blue']},
    {'color': ['red', '_blue']},
    {'color': None},
    {'color': 'None'},
    {'color': '_None'},
    {'color': '__None'},
    {'color': '_'},
    {'color': ''},
    {'color': ['red', None]},
    {'color': ['_None', 'blue']},
    {'color': [None, '_None']},
    {'color': ['_red', '_None']},
    {'meta': {'k': 'v', 'n': 1}},
    {'n': 1, 'f': 1.5, 'b': True, 'z': 0},
    {'quote': "it's"},
    {'quote': ["it's", "_isn't"]},
    {'a': 'x', 'b': ['y', 'z'], 'c': '_w'},
    {'bad': 'x; drop table y'},
    {'bad': 'x -- y'},
    {'list_nums': [1, 2, 3]},
    {'empty_list': []},
    {'MixedCase': 'v'},
    {'unicode': 'é'},
    {},
]
for flavor in FLAVORS:
    conn = get_conn(flavor)
    for params in PARAMS:
        for with_where in (True, False):
            add('build_where', {'params': params, 'flavor': flavor, 'with_where': with_where},
                lambda: usql.build_where(params, conn, with_where=with_where))


### ------------------------------------------------------------------------------------------
### dateadd_str
### ------------------------------------------------------------------------------------------

BEGINS = [
    datetime(2022, 1, 1),
    datetime(2022, 1, 1, 12, 30, 15, 123456),
    datetime(2022, 1, 1, tzinfo=timezone.utc),
    datetime(2022, 6, 1, 5, 4, 3, 21, tzinfo=timezone.utc),
    datetime(2022, 1, 1, 10, tzinfo=timezone(timedelta(hours=5))),
    datetime(2022, 1, 1, 10, tzinfo=timezone(timedelta(hours=-3, minutes=-30))),
    100,
    -5,
    'now',
    '2022-01-01',
    '2022-01-01 00:00:00',
    '2022-01-01T08:00:00+00:00',
    '2022-01-01 08:00:00.250000',
    '2022-01-01 08:00:00Z',
    'SELECT MAX(dt) FROM t',
    'CAST(x AS DATE)',
    'x; drop table y',
    '',
    None,
]
NUMBERS = [0, 1, -1440, 1.5]
DB_TYPES = [None, 'TIMESTAMPTZ', 'TIMESTAMP', 'DATETIME2', 'DATETIMEOFFSET', 'DATETIME']
for flavor in FLAVORS:
    for begin in BEGINS:
        if begin == 'now' and flavor == 'oracle':
            continue  # Uses the current time.
        for number in NUMBERS:
            for datepart in ('day', 'minute'):
                add('dateadd_str',
                    {'flavor': flavor, 'datepart': datepart, 'number': number, 'begin': begin, 'db_type': None},
                    lambda: usql.dateadd_str(flavor=flavor, datepart=datepart, number=number, begin=begin))
        for db_type in DB_TYPES[1:]:
            add('dateadd_str',
                {'flavor': flavor, 'datepart': 'minute', 'number': -30, 'begin': begin, 'db_type': db_type},
                lambda: usql.dateadd_str(flavor=flavor, datepart='minute', number=-30, begin=begin, db_type=db_type))
    add('dateadd_str',
        {'flavor': flavor, 'datepart': 'week', 'number': 1, 'begin': 'now', 'db_type': None},
        lambda: usql.dateadd_str(flavor=flavor, datepart='week', number=1, begin='now'))
    add('dateadd_str',
        {'flavor': flavor, 'datepart': None, 'number': 3, 'begin': 7, 'db_type': None},
        lambda: usql.dateadd_str(flavor=flavor, datepart=None, number=3, begin=7))


### ------------------------------------------------------------------------------------------
### dtypes
### ------------------------------------------------------------------------------------------

DB_TYPE_INPUTS = [
    'BIGINT', 'INT', 'INTEGER', 'SMALLINT', 'TINYINT', 'TINYINT(1)', 'BIT', 'BOOLEAN', 'BOOL',
    'FLOAT', 'REAL', 'DOUBLE PRECISION', 'DOUBLE', 'NUMERIC', 'NUMERIC(10,2)', 'NUMERIC(38, 20)',
    'DECIMAL(28,10)', 'NUMBER', 'NUMBER(10,0)', 'TEXT', 'VARCHAR', 'VARCHAR(255)', 'NVARCHAR(MAX)',
    'NVARCHAR2(2000)', 'CHAR(36)', 'CLOB', 'JSON', 'JSONB', 'UUID', 'UNIQUEIDENTIFIER', 'BYTEA',
    'BLOB', 'VARBINARY(MAX)', 'TIMESTAMP', 'TIMESTAMP(6)', 'TIMESTAMPTZ', 'TIMESTAMPTZ(9)',
    'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP WITHOUT TIME ZONE', 'DATETIME', 'DATETIME2',
    'DATETIME2(7)', 'DATETIMEOFFSET', 'DATETIMEOFFSET(3)', 'DATE', 'TIME', 'GEOMETRY',
    'GEOMETRY(POINT, 4326)', 'GEOMETRY(MULTIPOLYGON, 6570)', 'geography(Point,4326)',
    'GEOGRAPHY', 'USER-DEFINED', 'INTERVAL', 'MONEY', 'bigint', 'character varying',
    'timestamp with time zone', 'LONGBLOB', 'MEDIUMTEXT', 'ENUM', 'BINARY(16)',
]
for db_type in DB_TYPE_INPUTS:
    for allow in (True, False):
        add('get_pd_type_from_db_type', {'db_type': db_type, 'allow_custom_dtypes': allow},
            lambda: dsql.get_pd_type_from_db_type(db_type, allow_custom_dtypes=allow))

PD_TYPE_INPUTS = [
    'int', 'int64', 'Int64', 'int32', 'Int32', 'int16', 'int8', 'uint8', 'int64[pyarrow]',
    'int32[pyarrow]', 'float', 'float64', 'float64[pyarrow]', 'double[pyarrow]', 'float32',
    'str', 'string', 'string[pyarrow]', 'large_string[pyarrow]', 'object', 'bool', 'boolean',
    'bool[pyarrow]', 'datetime', 'datetimetz', 'datetime64[ns]', 'datetime64[ns, UTC]',
    'datetime64[us]', 'datetime64[us, UTC]', 'datetime64[ms]', 'datetime64[ms, UTC]',
    'datetime64[s]', 'datetime64[s, UTC]', 'datetime64[ns, America/New_York]',
    'timestamp[us][pyarrow]', 'date', 'date32[day][pyarrow]', 'json', 'JSON', 'jsonl',
    'numeric', 'numeric[10,2]', 'numeric[]', 'decimal', 'Decimal', 'uuid', 'UUID', 'guid',
    'bytes', 'binary[pyarrow]', 'blob', 'geometry', 'geometry[]', 'geometry[POINT]',
    'geometry[Point, 4326]', 'geometry[MULTIPOLYGON, 6570]', 'geometry[3361]',
    'geometry[EPSG:4326]', 'geometry[ESRI:102003]', 'geometry[POINT, ESRI:102003]',
    'geometry[LINESTRINGZ, 4326]', 'geography', 'geography[POINT, 4326]', 'geom', 'day',
    'unknown_type',
]
for flavor in FLAVORS + ['default']:
    for pd_type in PD_TYPE_INPUTS:
        add('get_db_type_from_pd_type', {'pd_type': pd_type, 'flavor': flavor},
            lambda: dsql.get_db_type_from_pd_type(pd_type, flavor=flavor))
    for dtype in [None, '', 'numeric', 'numeric[10,2]', 'numeric[ 10 , 2 ]', 'NUMERIC(28,10)',
                  'numeric[10]', 'DECIMAL(38, 20)', 'numeric[a,b]']:
        add('get_numeric_precision_scale', {'flavor': flavor, 'dtype': dtype},
            lambda: list(dsql.get_numeric_precision_scale(flavor, dtype)))

for dtype in ['geometry', 'geometry[]', 'geometry[Point, 0]', 'geometry[0, Point]', 'geometry[0]',
              'geometry[MULTILINESTRING, 4326]', 'geography', 'geography[POINT]',
              'geometry[POINT, ESRI:102003]', 'GEOMETRY(POINT, 4326)', 'geometry[srid=3857]',
              'geometry[MULTIPOLYGON, 6570]']:
    add('get_geometry_type_srid', {'dtype': dtype},
        lambda: list(mdtypes.get_geometry_type_srid(dtype)))

for l, r in [('int', 'Int64'), ('int64[pyarrow]', 'int'), ('float', 'double'), ('object', 'json'),
             ('numeric', 'decimal'), ('datetime64[ns, UTC]', 'datetime'), ('str', 'string'),
             ('bool', 'boolean'), ('geometry', 'object'), ('date', 'date32[day][pyarrow]'),
             ('int', 'float'), ('uuid', 'str'), ('bytes', 'binary'), ('timestamp', 'datetime64[us]')]:
    add('are_dtypes_equal', {'l': l, 'r': r}, lambda: mdtypes.are_dtypes_equal(l, r))


### ------------------------------------------------------------------------------------------
### Null replacements and update queries
### ------------------------------------------------------------------------------------------

NULL_TYPES = [
    'BIGINT', 'INT', 'NUMERIC', 'number', 'BOOLEAN', 'bool', 'BIT', 'TIMESTAMPTZ', 'TIMESTAMP(6)',
    'DATETIME2(7)', 'DATETIMEOFFSET', 'datetime', 'date', 'DATE', 'FLOAT', 'DOUBLE PRECISION',
    'decimal', 'char(36)', 'CHAR', 'blob', 'bytes', 'UUID', 'uniqueidentifier', 'guid', 'TEXT',
    'NVARCHAR(MAX)', 'GEOMETRY(POINT, 4326)', 'geometry', 'JSONB',
]
for flavor in FLAVORS:
    for typ in NULL_TYPES:
        add('get_null_replacement', {'typ': typ, 'flavor': flavor},
            lambda: usql.get_null_replacement(typ, flavor))


def flavor_cols_types(flavor, dtypes):
    return {col: dsql.get_db_type_from_pd_type(typ, flavor) for col, typ in dtypes.items()}


UPDATE_DTYPES = {
    'ts': 'datetime64[ns, UTC]',
    'station': 'str',
    'value': 'float',
    'flag': 'bool',
    'payload': 'json',
    'uid': 'uuid',
    'amount': 'numeric',
    'raw': 'bytes',
}
for flavor in FLAVORS:
    conn = get_conn(flavor)
    target_types = flavor_cols_types(flavor, UPDATE_DTYPES)
    variants = [
        ('full', target_types, target_types),
        ('index_only', {k: v for k, v in target_types.items() if k in ('ts', 'station')},
         {k: v for k, v in target_types.items() if k in ('ts', 'station')}),
        ('patch_subset', target_types, {k: v for k, v in target_types.items() if k in ('ts', 'station', 'value')}),
    ]
    for variant, ttypes, ptypes in variants:
        for upsert in (False, True):
            for dt_col in (None, 'ts'):
                for null_indices in (True, False):
                    for cast_columns in (True, False):
                        for identity_insert in ((False, True) if flavor == 'mssql' else (False,)):
                            args = {
                                'target': 'target_tbl', 'patch': 'patch_tbl', 'flavor': flavor,
                                'join_cols': ['ts', 'station'], 'upsert': upsert,
                                'datetime_col': dt_col, 'schema': conn.schema,
                                'patch_schema': None,
                                'target_cols_types': ttypes, 'patch_cols_types': ptypes,
                                'identity_insert': identity_insert, 'null_indices': null_indices,
                                'cast_columns': cast_columns, 'db_version': conn.db_version,
                                'variant': variant,
                            }
                            add('get_update_queries', args, lambda: usql.get_update_queries(
                                'target_tbl', 'patch_tbl', conn, ['ts', 'station'],
                                upsert=upsert, datetime_col=dt_col,
                                target_cols_types=ttypes, patch_cols_types=ptypes,
                                identity_insert=identity_insert, null_indices=null_indices,
                                cast_columns=cast_columns,
                            ))
    ### Explicit schemas and the old-SQLite DELETE/INSERT path.
    add('get_update_queries', {
        'target': 'target_tbl', 'patch': 'patch_tbl', 'flavor': flavor,
        'join_cols': ['station'], 'upsert': True, 'datetime_col': 'ts', 'schema': 'tgt_schema',
        'patch_schema': 'tmp_schema', 'target_cols_types': target_types,
        'patch_cols_types': target_types, 'identity_insert': False, 'null_indices': True,
        'cast_columns': True, 'db_version': conn.db_version, 'variant': 'schemas',
    }, lambda: usql.get_update_queries(
        'target_tbl', 'patch_tbl', conn, ['station'], upsert=True, datetime_col='ts',
        schema='tgt_schema', patch_schema='tmp_schema', target_cols_types=target_types,
        patch_cols_types=target_types,
    ))
for flavor in ('sqlite', 'geopackage'):
    conn = get_conn(flavor)
    target_types = flavor_cols_types(flavor, UPDATE_DTYPES)
    for version in ('3.20.0', '3.9.0'):
        for upsert in (False, True):
            conn.__dict__['_db_version'] = version
            add('get_update_queries', {
                'target': 'target_tbl', 'patch': 'patch_tbl', 'flavor': flavor,
                'join_cols': ['ts', 'station'], 'upsert': upsert, 'datetime_col': 'ts',
                'schema': None, 'patch_schema': None, 'target_cols_types': target_types,
                'patch_cols_types': target_types, 'identity_insert': False,
                'null_indices': True, 'cast_columns': True, 'db_version': version,
                'variant': 'old_sqlite',
            }, lambda: usql.get_update_queries(
                'target_tbl', 'patch_tbl', conn, ['ts', 'station'], upsert=upsert,
                datetime_col='ts', target_cols_types=target_types, patch_cols_types=target_types,
            ))
    conn.__dict__['_db_version'] = '3.45.1'


### ------------------------------------------------------------------------------------------
### CTEs
### ------------------------------------------------------------------------------------------

SUB_QUERIES = [
    "SELECT * FROM foo",
    "  SELECT a, b\n  FROM foo\n  WHERE a > 1",
    "WITH foo AS (SELECT 1 AS val) SELECT (val * 2) AS newval FROM foo",
    "WITH a AS (\n    SELECT 1 AS x\n), b AS (\n    SELECT 2 AS y\n) -- trailing comment\nSELECT * FROM a, b",
    "-- leading comment mentioning select\nSELECT 1 AS one",
    "/* block */ WITH q AS (SELECT 'select' AS s) SELECT s FROM q",
    "WITH x AS (SELECT 1),\nSELECT * FROM x",
    "SELECT \"select\" FROM [select] WHERE `select` = 'select'",
    "WITH incomplete AS (SELECT 1)",
]
PARENT_QUERIES = [
    "SELECT * FROM src",
    "SELECT newval * 3 FROM src",
    "WITH other AS (SELECT * FROM src) SELECT * FROM other",
    "  -- comment\nWITH other AS (SELECT * FROM \"src\") SELECT * FROM other",
]
for flavor in FLAVORS:
    for sub in SUB_QUERIES:
        for parent in PARENT_QUERIES:
            add('wrap_query_with_cte', {'sub_query': sub, 'parent_query': parent, 'flavor': flavor, 'cte_name': 'src'},
                lambda: usql.wrap_query_with_cte(sub, parent, flavor))
        for cols in ('*', ['a', 'b'], '"a", "b"'):
            add('format_cte_subquery', {'sub_query': sub, 'flavor': flavor, 'sub_name': 'definition', 'cols_to_select': cols},
                lambda: usql.format_cte_subquery(sub, flavor, 'definition', cols))
for q in SUB_QUERIES + PARENT_QUERIES + ['', 'select', 'Select', 'SELECTED', '(select 1) select 2',
                                       "'it''s' select", "with", "withdrawal", "  \n with x"]:
    for last in (False, True):
        add('find_top_level_select_index', {'query': q, 'last': last},
            lambda: usql.find_top_level_select_index(q, last=last))
    add('get_leading_with_index', {'query': q}, lambda: usql.get_leading_with_index(q))


### ------------------------------------------------------------------------------------------
### CREATE / RENAME / DROP / schema / introspection
### ------------------------------------------------------------------------------------------

CREATE_DTYPES = {
    'ts': 'datetime64[ns, UTC]',
    'station': 'str',
    'value': 'float',
    'count': 'int',
}
CREATE_SHAPES = [
    ('plain', CREATE_DTYPES, {}),
    ('pk', CREATE_DTYPES, {'primary_key': 'station'}),
    ('pk_dt', CREATE_DTYPES, {'primary_key': 'station', 'datetime_column': 'ts'}),
    ('pk_is_dt', CREATE_DTYPES, {'primary_key': 'ts', 'datetime_column': 'ts'}),
    ('autoincrement', {k: v for k, v in CREATE_DTYPES.items()}, {'primary_key': 'id', 'autoincrement': True, 'datetime_column': 'ts'}),
    ('autoincrement_pk_missing', CREATE_DTYPES, {'primary_key': 'id', 'datetime_column': 'ts'}),
    ('pk_db_type_unparsed', {'id': 'BIGINT', 'v': 'TEXT'}, {'primary_key': 'id', '_parse_dtypes': False}),
    ('int_axis', {'epoch': 'int', 'station': 'str', 'value': 'float'}, {'primary_key': 'pk', 'datetime_column': 'epoch'}),
    ('geometry', {'ts': 'datetime', 'geom': 'geometry[POINT, 4326]', 'shape': 'geometry[MULTIPOLYGON, ESRI:102003]'}, {'datetime_column': 'ts'}),
    ('special', {'payload': 'json', 'uid': 'uuid', 'amount': 'numeric', 'amount2': 'numeric[10,2]', 'raw': 'bytes', 'd': 'date', 'flag': 'bool'}, {}),
    ('hypertable', CREATE_DTYPES, {'primary_key': 'pk', 'datetime_column': 'ts', 'hypertable_chunk_interval': '43200 minutes',
                                   'hypertable_segmentby': ['"station"'], 'hypertable_orderby': ['"ts" DESC']}),
    ('hypertable_int', {'epoch': 'int', 'v': 'float'}, {'datetime_column': 'epoch', 'hypertable_chunk_interval': "100'000"}),
    ('partitioned', CREATE_DTYPES, {'primary_key': 'pk', 'datetime_column': 'ts', 'partition_by_column': 'ts',
                                    'partition_bounds': [('target_tbl_p_20240101000000', "'2024-01-31 00:00:00.000000'"),
                                                         ('target_tbl_p_20240131000000', "'2024-03-01 00:00:00.000000'")],
                                    'partition_scheme_name': 'ps_target_tbl'}),
    ('partitioned_no_pk', CREATE_DTYPES, {'datetime_column': 'ts', 'partition_by_column': 'ts',
                                          'partition_bounds': [('p0', '100')], 'partition_scheme_name': 'ps_target_tbl'}),
    ('partitioned_pk_is_dt', CREATE_DTYPES, {'primary_key': 'ts', 'datetime_column': 'ts', 'partition_by_column': 'ts',
                                             'partition_scheme_name': 'ps_target_tbl'}),
    ('long_names', {('col_' + 'z' * 140): 'int', 'ok': 'str'}, {'primary_key': 'ok'}),
]
for flavor in FLAVORS:
    for shape, dtypes, kw in CREATE_SHAPES:
        for table, schema in (('target_tbl', None), ('target_tbl', 'my_schema'), (LONG + 'tbl', None)):
            args = {'kind': 'dtypes', 'query_or_dtypes': dtypes, 'new_table': table, 'flavor': flavor,
                    'schema': schema, 'shape': shape, **{k: v for k, v in kw.items()}}
            add('get_create_table_queries', args,
                lambda: usql.get_create_table_queries(dtypes, table, flavor, schema=schema, **kw))
    add('get_create_table_queries', {'kind': 'dtypes', 'query_or_dtypes': {}, 'new_table': 't', 'flavor': flavor,
                                     'schema': None, 'shape': 'empty'},
        lambda: usql.get_create_table_queries({}, 't', flavor))
    for q in ["SELECT * FROM src_tbl", "WITH a AS (SELECT 1 AS x) SELECT * FROM a",
              "  -- note\nSELECT a, b FROM x WHERE a > 1"]:
        for kw in ({}, {'primary_key': 'id'}, {'primary_key': 'id', 'datetime_column': 'ts'},
                   {'primary_key': 'id', 'primary_key_db_type': 'BIGINT', 'datetime_column': 'id'}):
            for schema in (None, 'my_schema'):
                args = {'kind': 'query', 'query_or_dtypes': q, 'new_table': 'new_tbl', 'flavor': flavor,
                        'schema': schema, 'shape': 'query', **kw}
                add('get_create_table_queries', args,
                    lambda: usql.get_create_table_queries(q, 'new_tbl', flavor, schema=schema, **kw))

    for old, new, schema, new_schema in [('old_tbl', 'new_tbl', None, None),
                                         ('old_tbl', 'new_tbl', 's1', None),
                                         ('old_tbl', 'new_tbl', 's1', 's2'),
                                         ('old_tbl', 'old_tbl', 's1', 's2'),
                                         ('old_tbl', 'old_tbl', None, None)]:
        add('get_rename_table_queries', {'old_table': old, 'new_table': new, 'flavor': flavor,
                                         'schema': schema, 'new_schema': new_schema},
            lambda: usql.get_rename_table_queries(old, new, flavor, schema=schema, new_schema=new_schema))

    for schema in (None, '', 'public', 'dbo', 'mysql', 'custom_schema', 'bad; drop'):
        add('get_create_schema_if_not_exists_queries', {'schema': schema, 'flavor': flavor},
            lambda: usql.get_create_schema_if_not_exists_queries(schema, flavor))

    conn = get_conn(flavor)
    for table in ('target_tbl', "it's", '#temp_tbl', LONG + 'tbl', 'MixedCase'):
        def _cols_types_query():
            captured = []

            class FakeResult:
                def fetchall(self):
                    return []

            def execute(query, *a, **k):
                captured.append(query.text)
                return FakeResult()

            def read(query, *a, **k):
                captured.append(query.text)
                return pd.DataFrame()

            conn.execute = execute
            conn.read = read
            try:
                usql.get_table_cols_types(table, conn, flavor=flavor)
                usql.get_table_cols_indices(table, conn, flavor=flavor)
            finally:
                conn.__dict__.pop('execute', None)
                conn.__dict__.pop('read', None)
            return captured
        add('table_introspection_queries', {'table': table, 'flavor': flavor,
                                            'schema': usql.DEFAULT_SCHEMA_FLAVORS.get(flavor) if flavor not in usql.NO_SCHEMA_FLAVORS else None},
            _cols_types_query)

    def _version_query():
        with Recorder(conn, value='1') as rec:
            usql.get_db_version(conn)
        return rec.queries
    add('get_db_version_query', {'flavor': flavor}, _version_query)

    for schema in (None, 'my_schema'):
        def _reset_autoincrement():
            with Recorder(conn, value=41) as rec:
                queries = usql.get_reset_autoincrement_queries('target_tbl', 'id', conn, schema=schema)
            return {'max_id_query': rec.queries[0], 'queries': queries}
        add('get_reset_autoincrement_queries', {'table': 'target_tbl', 'column': 'id', 'flavor': flavor,
                                                'schema': schema or conn.schema, 'max_id': 41},
            _reset_autoincrement)

    def _distinct_count():
        with Recorder(conn, value=3) as rec:
            usql.get_distinct_col_count('station', 'SELECT * FROM t', conn)
        return rec.queries
    add('get_distinct_col_count_query', {'col': 'station', 'query': 'SELECT * FROM t', 'flavor': flavor},
        _distinct_count)

for table, schema in (('target_tbl', None), ('target_tbl', 'geo'), (LONG + 'tbl', 'public')):
    def _postgis():
        conn = get_conn('postgis')
        captured = []

        class FakeResult:
            def fetchall(self):
                return []

        def execute(query, *a, **k):
            captured.append(query.text)
            return FakeResult()
        conn.execute = execute
        try:
            usql.get_postgis_geo_columns_types(conn, table, schema=schema)
        finally:
            conn.__dict__.pop('execute', None)
        return captured
    add('get_postgis_geo_columns_types_query', {'table': table, 'schema': schema}, _postgis)


### ------------------------------------------------------------------------------------------
### Pipe shapes: index names, CREATE/DROP INDEX, partitions, compression, metadef
### ------------------------------------------------------------------------------------------

SHAPES = {
    'plain': {'columns': {'datetime': 'ts', 'id': 'station'}, 'dtypes': {'value': 'float'}},
    'upsert': {'columns': {'datetime': 'ts', 'id': 'station', 'primary': 'pk'}, 'upsert': True,
               'dtypes': {'pk': 'int', 'value': 'float'}},
    'upsert_no_pk': {'columns': {'datetime': 'ts', 'id': 'station'}, 'upsert': True,
                     'dtypes': {'value': 'float'}},
    'upsert_dt_only': {'columns': {'datetime': 'ts'}, 'upsert': True, 'dtypes': {'value': 'float'}},
    'no_hypertable': {'columns': {'datetime': 'ts', 'id': 'station'}, 'hypertable': False,
                      'dtypes': {'value': 'float'}},
    'int_axis': {'columns': {'datetime': 'epoch', 'id': 'station'}, 'dtypes': {'epoch': 'int', 'value': 'float'}},
    'int_axis_precision': {'columns': {'datetime': 'epoch', 'id': 'station'},
                           'dtypes': {'epoch': 'int', 'value': 'float'}, 'precision': 'ms',
                           'verify': {'chunk_days': 7}},
    'no_datetime': {'columns': {'id': 'station'}, 'dtypes': {'value': 'float'}},
    'geometry': {'columns': {'datetime': 'ts', 'id': 'station'},
                 'dtypes': {'geom': 'geometry[POINT, 4326]', 'value': 'float'},
                 'indices': {'geom': 'geom', 'combo': ['geom', 'station']}},
    'autoincrement': {'columns': {'datetime': 'ts', 'primary': 'id'}, 'autoincrement': True,
                      'dtypes': {'value': 'float'}},
    'autotime': {'columns': {'datetime': 'ts', 'id': 'station'}, 'autotime': True,
                 'dtypes': {'value': 'float'}},
    'null_indices_false': {'columns': {'datetime': 'ts', 'id': 'station'}, 'upsert': True,
                           'null_indices': False, 'dtypes': {'value': 'float'}},
    'static': {'columns': {'datetime': 'ts', 'id': 'station', 'primary': 'pk'}, 'static': True,
               'dtypes': {'pk': 'int'}},
    'schema': {'columns': {'datetime': 'ts', 'id': 'station'}, 'schema': 'custom',
               'dtypes': {'value': 'float'}},
    'multi_index': {'columns': {'datetime': 'ts', 'id': 'station', 'value': 'value'},
                    'indices': {'combo': ['station', 'value'], 'single': 'value', 'empty': []},
                    'dtypes': {'value': 'TEXT_VAL'}},
    'index_template': {'columns': {'datetime': 'ts', 'id': 'station'},
                       'index_template': 'idx_{metric_key}_{location_key}_{column_names}'},
    'id_eq_primary': {'columns': {'datetime': 'ts', 'id': 'id', 'primary': 'id'}, 'upsert': True,
                      'dtypes': {'id': 'int'}},
    'compress': {'columns': {'datetime': 'ts', 'id': 'station'},
                 'compress': {'segmentby': 'station', 'orderby': 'ts DESC NULLS LAST, station', 'after': '30 days'}},
    'compress_int': {'columns': {'datetime': 'epoch', 'id': 'station'}, 'dtypes': {'epoch': 'int64'},
                     'precision': 's', 'compress': {'after': 86400}},
    'compress_int_str': {'columns': {'datetime': 'epoch', 'id': 'station', 'primary': 'station'},
                         'dtypes': {'epoch': 'int'}, 'precision': 'minute', 'compress': {'after': '2 hours'}},
    'chunk_hours': {'columns': {'datetime': 'ts'}, 'verify': {'chunk_hours': 12}},
}
LONG_TARGET = 'plugin_noaa_' + 'weather_station_observations_' * 4 + 'final'


def shape_targets(flavor):
    yield 'plain_target', 'plugin_golden_m'
    yield 'long_target', usql.truncate_item_name(LONG_TARGET, flavor)


def existing_types_for(flavor, params, drop_pk=False):
    cols = list(params.get('columns', {}).values()) + list(params.get('dtypes', {}).keys())
    seen = []
    for c in cols:
        if c and c not in seen:
            seen.append(c)
    columns = params.get('columns', {})
    types = {}
    for c in seen:
        if drop_pk and c == columns.get('primary'):
            continue
        typ = params.get('dtypes', {}).get(c)
        if typ is None:
            typ = 'datetime' if c == columns.get('datetime') else ('int' if c == columns.get('primary') else 'str')
        if typ == 'TEXT_VAL':
            typ = 'str'
        types[c] = dsql.get_db_type_from_pd_type(typ, flavor)
    return types


def existing_indices_for(flavor, pipe, kind):
    """Synthetic `get_pipe_columns_indices` output."""
    if kind == 'none':
        return {}
    try:
        names = get_conn(flavor).get_pipe_index_names(pipe)
    except Exception:
        ### e.g. an index name that cannot be truncated (recorded as an error case elsewhere).
        return {}
    out = {}
    cols = pipe.columns
    if kind in ('some', 'all'):
        dt = cols.get('datetime')
        if dt and names.get('datetime'):
            doc = {'name': names['datetime'], 'type': 'INDEX'}
            if flavor == 'mssql':
                doc['clustered'] = True
            out.setdefault(dt, []).append(doc)
        pk = cols.get('primary')
        if pk:
            doc = {'name': f'PK_{pipe.target}', 'type': 'PRIMARY KEY'}
            if flavor == 'mssql':
                doc['clustered'] = False
            out.setdefault(pk, []).append(doc)
    if kind == 'all':
        for ix_key, name in names.items():
            if ix_key in ('datetime', 'primary'):
                continue
            ix_cols = pipe.indices.get(ix_key)
            col = ix_cols[0] if isinstance(ix_cols, list) and ix_cols else ix_cols
            if not col:
                continue
            out.setdefault(col, []).append({'name': name, 'type': 'INDEX', **({'clustered': False} if flavor == 'mssql' else {})})
            if ix_key == 'unique':
                out.setdefault(col, []).append({'name': name.replace('IX_', 'UQ_'), 'type': 'INDEX', **({'clustered': False} if flavor == 'mssql' else {})})
    return out


_real_list = list
_SET_ORDER = []


class _OrderedListMeta(type):
    """A stand-in for `list` that orders sets deterministically and still works in isinstance()."""

    def __instancecheck__(cls, inst):
        return isinstance(inst, _real_list)

    def __call__(cls, x=()):
        if isinstance(x, (set, frozenset)):
            order = {v: i for i, v in enumerate(_SET_ORDER)}
            return sorted(x, key=lambda v: (order.get(v, len(order)), v))
        return _real_list(x)


class _ordered_list(metaclass=_OrderedListMeta):
    pass


def create_index_queries(conn, pipe):
    _SET_ORDER[:] = [usql.sql_item_name(c, conn.flavor) for c in pipe.columns.values() if c]
    sql_pipes.list = _ordered_list
    try:
        return conn.get_create_index_queries(pipe)
    finally:
        del sql_pipes.list


for flavor in FLAVORS:
    conn = get_conn(flavor)
    for shape, params in SHAPES.items():
        for target_kind, target in shape_targets(flavor):
            lk = 'loc' if shape == 'index_template' else None
            pipe = StubPipe(conn, json.loads(json.dumps(params)), target, lk=lk)
            add('get_pipe_index_names', {'flavor': flavor, 'pipe': pipe_json(pipe)},
                lambda: conn.get_pipe_index_names(pipe))
            add('pipe_attributes', {'flavor': flavor, 'pipe': pipe_json(pipe)}, lambda: {
                'dtypes': pipe.dtypes,
                'precision_unit': pipe.precision['unit'],
                'chunk_interval': pipe.get_chunk_interval(),
                'chunk_interval_60': pipe.get_chunk_interval(timedelta(minutes=60)),
                'chunk_interval_int': pipe.get_chunk_interval(90),
                'backtrack': pipe.get_backtrack_interval(),
                'backtrack_no_existing': pipe.get_backtrack_interval(check_existing=False),
                'guess_datetime': pipe.guess_datetime(),
                'indices': pipe.indices,
                'schema': conn.get_pipe_schema(pipe),
            })
            for state_kind in ('fresh', 'pk_missing', 'some', 'all'):
                drop_states = ('fresh', 'all')
                pipe._columns_types = existing_types_for(flavor, params, drop_pk=(state_kind == 'pk_missing'))
                pipe._columns_indices = existing_indices_for(
                    flavor, pipe, {'fresh': 'none', 'pk_missing': 'none'}.get(state_kind, state_kind)
                )
                conn.get_pipe_columns_indices = (lambda p, debug=False, _ix=pipe._columns_indices: _ix)
                state = {
                    'existing_cols_types': pipe._columns_types,
                    'existing_cols_indices': pipe._columns_indices,
                }
                add('get_create_index_queries', {'flavor': flavor, 'pipe': pipe_json(pipe), 'state': state},
                    lambda: create_index_queries(conn, pipe))
                for is_hypertable in ((False, True) if flavor in usql.hypertable_queries else (False,)):
                    if state_kind not in drop_states:
                        break
                    for temp_exists in ((False, True) if is_hypertable else (False,)):
                        def _drop():
                            with Recorder(conn, value=(1 if is_hypertable else None), table_exists=temp_exists):
                                return conn.get_drop_index_queries(pipe)
                        add('get_drop_index_queries', {
                            'flavor': flavor, 'pipe': pipe_json(pipe),
                            'state': {'exists': True, 'cols_indices': pipe._columns_indices,
                                      'is_hypertable': is_hypertable, 'temp_table_exists': temp_exists},
                        }, _drop)
                conn.__dict__.pop('get_pipe_columns_indices', None)

            ### Drop table (as `drop_pipe` issues it, plus MSSQL partition cleanup).
            def _drop_pipe():
                with Recorder(conn) as rec:
                    conn.drop_pipe(pipe)
                return rec.queries
            add('drop_pipe_queries', {'flavor': flavor, 'pipe': pipe_json(pipe)}, _drop_pipe)

            if target_kind != 'plain_target':
                ### Long targets only matter for names (covered above and in `partition_misc`).
                def _partition_misc_long():
                    return {
                        'cleanup_queries': conn._get_partition_cleanup_queries(pipe),
                        'function_name': conn._partition_function_name(pipe),
                        'scheme_name': conn._partition_scheme_name(pipe),
                    }
                add('partition_names', {'flavor': flavor, 'pipe': pipe_json(pipe)}, _partition_misc_long)
                continue

            ### Maintenance.
            pipe_name = usql.sql_item_name(pipe.target, flavor, conn.get_pipe_schema(pipe))
            for full in (False, True):
                add('get_vacuum_queries', {'flavor': flavor, 'pipe_name': pipe_name, 'full': full},
                    lambda: conn._get_vacuum_queries(pipe, pipe_name, full=full))
            add('get_analyze_query', {'flavor': flavor, 'pipe_name': pipe_name},
                lambda: conn._get_analyze_query(pipe, pipe_name))

            ### Compression.
            add('compress_settings', {'flavor': flavor, 'pipe': pipe_json(pipe)}, lambda: {
                'settings': conn._get_compress_settings(pipe),
                'columnstore_settings_query': conn._get_columnstore_settings_query(pipe),
                'columnstore_policy_query': conn._get_columnstore_policy_query(pipe),
                'columnstore_remove_policy_query': conn._get_columnstore_remove_policy_query(pipe),
                'columnstore_disable_query': conn._get_columnstore_disable_query(pipe),
                'integer_now_func_queries': conn._get_integer_now_func_queries(pipe),
            })
            ts_flavor = flavor in ('timescaledb', 'timescaledb-ha')
            for is_hypertable in ((False, True) if ts_flavor else (True,)):
                for no_policy in ((False, True) if ts_flavor else (False,)):
                    for op in ('compress', 'decompress'):
                        def _compress():
                            with Recorder(conn, value=(1 if is_hypertable else None)) as rec:
                                method = conn.compress_pipe if op == 'compress' else conn.decompress_pipe
                                success, msg = method(pipe, no_policy=no_policy)
                            return {'success': success, 'queries': rec.queries if success else rec.queries[:1]}
                        add(f'{op}_pipe_queries', {'flavor': flavor, 'pipe': pipe_json(pipe),
                                                   'is_hypertable': is_hypertable, 'no_policy': no_policy}, _compress)

            def _size():
                with Recorder(conn, value=None) as rec:
                    conn.get_pipe_size(pipe)
                return rec.queries
            add('get_pipe_size_queries', {'flavor': flavor, 'pipe': pipe_json(pipe), 'database': 'db'}, _size)

            ### Partitions.
            def _partition_misc():
                out = {}
                with Recorder(conn, value=3) as rec:
                    conn._get_partition_count(pipe)
                out['partition_count_query'] = rec.queries[0] if rec.queries else None
                with Recorder(conn, value=3) as rec:
                    conn._get_chunk_count_timescaledb(pipe)
                out['chunk_count_query'] = rec.queries[0] if rec.queries else None
                out['should_partition'] = conn._should_partition(pipe)
                out['partition_column'] = conn._get_partition_column(pipe)
                out['cleanup_queries'] = conn._get_partition_cleanup_queries(pipe)
                out['function_name'] = conn._partition_function_name(pipe)
                out['scheme_name'] = conn._partition_scheme_name(pipe)
                if flavor in ('timescaledb', 'timescaledb-ha') and pipe.columns.get('datetime'):
                    with Recorder(conn) as rec:
                        conn.partition_pipe(pipe, chunk_minutes=1440)
                    out['set_chunk_time_interval_query'] = rec.queries[0] if rec.queries else None
                return out
            add('partition_misc', {'flavor': flavor, 'pipe': pipe_json(pipe), 'database': 'db'}, _partition_misc)

            dt_col = pipe.columns.get('datetime')
            if not dt_col:
                continue
            is_int = 'int' in str(pipe.dtypes.get(dt_col, 'datetime')).lower()
            frames = (
                [[5, 123456, -7], [259_201]]
                if is_int
                else [
                    [datetime(2024, 1, 15, 12, tzinfo=timezone.utc), datetime(2024, 3, 2, tzinfo=timezone.utc)],
                    [datetime(2023, 12, 31, 23, 59, 59)],
                ]
            )
            for values in frames:
                df = pd.DataFrame({dt_col: values})

                def _ranges():
                    ranges = conn._get_partition_ranges_for_df(pipe, df)
                    out = {
                        'min': min(values), 'max': max(values),
                        'interval': pipe.get_chunk_interval(),
                        'ranges': [list(r) for r in ranges],
                        'literals': [[conn._partition_literal(lo), conn._partition_literal(hi)] for lo, hi in ranges],
                        'names': [conn._partition_name(pipe, lo) for lo, _ in ranges],
                        'initial_bounds': [list(b) for b in conn._get_initial_partition_bounds(pipe, df)],
                    }
                    if flavor in sql_partition.PG_PARTITION_FLAVORS:
                        with Recorder(conn, value=1) as rec:
                            conn._create_missing_partitions_pg(pipe, df)
                        out['pg_queries'] = rec.queries
                    if flavor == 'mssql':
                        out['mssql_creation_queries'] = conn._get_mssql_partition_creation_queries(pipe, df)
                        max_text = '1000' if is_int else '2023-11-01T00:00:00.0000000+00:00'
                        with Recorder(conn, value=max_text) as rec:
                            conn._create_missing_partitions_mssql(pipe, df)
                        out['mssql_max_boundary_text'] = max_text
                        out['mssql_queries'] = rec.queries
                    if flavor in sql_partition.MYSQL_PARTITION_FLAVORS:
                        descs = ['1000', 'MAXVALUE'] if is_int else ["'2023-11-01 00:00:00.000000'", "'2023-10-02 00:00:00'"]
                        conn.__dict__['database'] = 'db'
                        with Recorder(conn, read=pd.DataFrame({'PARTITION_DESCRIPTION': [d for d in descs if d != 'MAXVALUE']})) as rec:
                            conn._create_missing_partitions_mysql(pipe, df)
                        out['mysql_descriptions'] = [d for d in descs if d != 'MAXVALUE']
                        out['mysql_queries'] = rec.queries
                    return out
                add('partition_ranges', {'flavor': flavor, 'pipe': pipe_json(pipe), 'values': values}, _ranges)


### Partition primitives across value types and offsets.
for flavor in ('postgresql', 'mysql', 'mssql'):
    conn = get_conn(flavor)
    for value in [datetime(2024, 1, 15, 12, tzinfo=timezone.utc), datetime(2024, 1, 15, 12),
                  datetime(1969, 12, 31, 23, tzinfo=timezone.utc),
                  datetime(2024, 1, 15, 12, tzinfo=timezone(timedelta(hours=5, minutes=30))),
                  datetime(2024, 2, 29, 0, 0, 0, 1)]:
        for interval in (timedelta(minutes=43200), timedelta(hours=1), timedelta(days=7), timedelta(seconds=90)):
            add('partition_bounds', {'flavor': flavor, 'value': value, 'interval': interval}, lambda: {
                'bounds': list(conn._partition_bounds(value, interval)),
                'literals': [conn._partition_literal(v) for v in conn._partition_bounds(value, interval)],
            })
    for value in (0, 1, -1, 99, 100, -100, 10**12):
        for interval in (1, 7, 100, 86400):
            add('partition_bounds', {'flavor': flavor, 'value': value, 'interval': interval}, lambda: {
                'bounds': list(conn._partition_bounds(value, interval)),
                'literals': [conn._partition_literal(v) for v in conn._partition_bounds(value, interval)],
            })


### ------------------------------------------------------------------------------------------
### Metadefinitions
### ------------------------------------------------------------------------------------------

DEFINITIONS = [
    "SELECT * FROM source_tbl",
    "\n    SELECT ts, station, value\n    FROM source_tbl\n    WHERE value > 0\n",
    "WITH src AS (SELECT * FROM source_tbl) SELECT * FROM src",
    "-- comment\nSELECT * FROM \"parent_tbl\"",
    "SELECT * FROM parent_tbl WHERE value IS NOT NULL",
]
METADEF_SHAPES = {
    'plain': {'columns': {'datetime': 'ts', 'id': 'station'}},
    'backtrack': {'columns': {'datetime': 'ts'}, 'fetch': {'backtrack_minutes': 30}},
    'int_axis': {'columns': {'datetime': 'epoch'}, 'dtypes': {'epoch': 'int'}},
    'int_axis_precision': {'columns': {'datetime': 'epoch'}, 'dtypes': {'epoch': 'int'}, 'precision': 's'},
    'guess': {'dtypes': {'when': 'datetime64[ns, UTC]', 'v': 'float'}},
    'no_datetime': {'columns': {'id': 'station'}},
    'naive_dt': {'columns': {'datetime': 'ts'}, 'dtypes': {'ts': 'datetime64[us]'}},
}
BOUNDS = [
    ('default', '', None, datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc), True),
    ('default_no_sync', '', None, None, True),
    ('default_no_check', '', None, datetime(2024, 1, 2, tzinfo=timezone.utc), False),
    ('explicit', datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 2, 1, tzinfo=timezone.utc), None, True),
    ('begin_after_end', datetime(2024, 3, 1, tzinfo=timezone.utc), datetime(2024, 2, 1, tzinfo=timezone.utc), None, True),
    ('unbounded', None, None, None, True),
    ('end_only', None, datetime(2024, 2, 1, 12, 30), None, True),
    ('naive', datetime(2024, 1, 1, 6), None, None, True),
    ('int', 1000, 2000, None, True),
    ('int_default', '', None, 1700000000, True),
]
for flavor in FLAVORS:
    conn = get_conn(flavor)
    for shape, base_params in METADEF_SHAPES.items():
        for definition in (DEFINITIONS[0], DEFINITIONS[2], DEFINITIONS[4]):
            for bound_name, begin, end, sync_time, check_existing in BOUNDS:
                if shape.startswith('int_axis') != bound_name.startswith('int'):
                    continue
                for params in ((None, {'station': ['a', '_b']}) if definition == DEFINITIONS[0] else (None,)):
                    p = json.loads(json.dumps(base_params))
                    p['sql'] = definition
                    pipe = StubPipe(conn, p, 'plugin_golden_m', ck='sql:src')
                    pipe._sync_time = sync_time
                    add('get_pipe_metadef', {
                        'flavor': flavor, 'pipe': pipe_json(pipe), 'params': params, 'begin': begin,
                        'end': end, 'check_existing': check_existing, 'sync_time': sync_time,
                        'parent': None,
                    }, lambda: conn.get_pipe_metadef(pipe, params=params, begin=begin, end=end,
                                                     check_existing=check_existing))
    ### Predicate pushdown onto a parent pipe.
    for parent_params, parent_target, parent_schema in [
        ({'columns': {'datetime': 'pts'}}, 'parent_tbl', conn.schema),
        ({'columns': {'datetime': 'pepoch'}, 'dtypes': {'pepoch': 'int'}, 'precision': 'ms'}, 'parent_tbl', conn.schema),
        ({'columns': {'id': 'x'}}, 'parent_tbl', conn.schema),
    ]:
        for definition in (DEFINITIONS[0], DEFINITIONS[3], DEFINITIONS[4]):
            for bound_name, begin, end, sync_time, check_existing in BOUNDS:
                if bound_name.startswith('int') and 'pepoch' not in json.dumps(parent_params):
                    continue
                parent = StubPipe(conn, json.loads(json.dumps(parent_params)), parent_target)
                p = {'columns': {'datetime': 'ts'}, 'sql': definition}
                if 'pepoch' in json.dumps(parent_params):
                    p['dtypes'] = {'ts': 'int'}
                pipe = StubPipe(conn, p, 'plugin_golden_child', ck='sql:src')
                pipe.parent = parent
                pipe._sync_time = sync_time
                parent_dt = parent.columns.get('datetime')
                add('get_pipe_metadef', {
                    'flavor': flavor, 'pipe': pipe_json(pipe), 'params': None, 'begin': begin,
                    'end': end, 'check_existing': check_existing, 'sync_time': sync_time,
                    'parent': {
                        'target': parent_target,
                        'schema': conn.get_pipe_schema(parent),
                        'dt_col': parent_dt,
                        'dt_typ': parent.dtypes.get(parent_dt, 'datetime') if parent_dt else None,
                        'precision_unit': parent.precision['unit'],
                    },
                }, lambda: conn.get_pipe_metadef(pipe, begin=begin, end=end, check_existing=check_existing))


### ------------------------------------------------------------------------------------------

meta = {
    'python': sys.version.split()[0],
    'meerschaum': mrsm.__version__,
    'generator': 'crates/meerschaum-sql/tests/fixtures/gen_queries.py',
    'num_cases': len(CASES),
}
with open(OUT_PATH, 'w', encoding='utf-8') as f:
    ### One case per line keeps diffs reviewable without inflating the file.
    f.write('{"meta": ' + json.dumps(meta) + ', "cases": [\n')
    f.write(',\n'.join(json.dumps(c, ensure_ascii=False, separators=(',', ':')) for c in CASES))
    f.write('\n]}')
    f.write('\n')
print(f"Wrote {len(CASES)} cases to {OUT_PATH}")
