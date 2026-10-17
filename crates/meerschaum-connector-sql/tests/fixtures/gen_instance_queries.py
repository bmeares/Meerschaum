#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Generate `instance_queries.json`: the exact query text the v4 SQL instance methods emit for
synthetic pipes, with every database read mocked (no connection is made):

- `get_pipe_data_query` (select / omit columns, begin / end, params, order, limit),
- the `get_sync_time` query (newest / oldest, params, MariaDB partitioned `MAX` / `MIN`),
- the `get_pipe_rowcount` query, the `clear_pipe` `DELETE`,
- `get_add_columns_queries` / `get_alter_columns_queries`,
- the instance tables' `CREATE TABLE` / `CREATE INDEX` DDL (`tables/__init__.py`).

Run from the repository root with empty temporary directories:

    PYTHONPATH=$(pwd) MRSM_ROOT_DIR="$(mktemp -d)" MRSM_PLUGINS_DIR="$(mktemp -d)" \\
        ~/.config/meerschaum/venvs/mrsm/bin/python crates/meerschaum-connector-sql/tests/fixtures/gen_instance_queries.py

or with `scripts/parity/run.sh crates/meerschaum-connector-sql/tests/fixtures/gen_instance_queries.py`.
"""

import json
import os
import sys
from datetime import datetime, timezone

import meerschaum as mrsm

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'instance_queries.json')

FLAVORS = [
    'postgresql', 'timescaledb', 'timescaledb-ha', 'postgis', 'citus', 'cockroachdb',
    'mysql', 'mariadb', 'sqlite',
]

### Pandas dtypes of each synthetic table; the database types come from
### `get_db_type_from_pd_type(dtype, flavor)` so both implementations see the same state.
SHAPES = {
    'dt_id': {
        'parameters': {'columns': {'datetime': 'dt', 'id': 'id'}},
        'pd_types': {'dt': 'datetime64[ns, UTC]', 'id': 'int64', 'val': 'float64', 's': 'string'},
    },
    'naive_dt': {
        'parameters': {
            'columns': {'datetime': 'dt', 'id': 'id'},
            'dtypes': {'dt': 'datetime64[ns]'},
        },
        'pd_types': {'dt': 'datetime64[ns]', 'id': 'int64', 'val': 'float64'},
    },
    'int_axis': {
        'parameters': {'columns': {'datetime': 'ts', 'id': 'id'}, 'dtypes': {'ts': 'int'}},
        'pd_types': {'ts': 'int64', 'id': 'int64', 'v': 'float64'},
    },
    'no_dt': {
        'parameters': {'columns': {'id': 'id'}},
        'pd_types': {'id': 'int64', 'a': 'string', 'b': 'float64'},
    },
    'guess_dt': {
        'parameters': {'columns': {'id': 'id'}},
        'pd_types': {'id': 'int64', 'when': 'datetime64[ns, UTC]', 'a': 'string'},
    },
    'upsert_primary': {
        'parameters': {'columns': {'datetime': 'dt', 'primary': 'pk'}, 'upsert': True},
        'pd_types': {'dt': 'datetime64[ns, UTC]', 'pk': 'int64', 'v': 'string'},
    },
    'schema_target': {
        'parameters': {
            'columns': {'datetime': 'dt', 'id': 'id'},
            'schema': 'other_schema',
            'target': 'Custom Target',
        },
        'pd_types': {'dt': 'datetime64[ns, UTC]', 'id': 'int64', 'val': 'float64'},
    },
    'no_partition': {
        'parameters': {'columns': {'datetime': 'dt', 'id': 'id'}, 'hypertable': False},
        'pd_types': {'dt': 'datetime64[ns, UTC]', 'id': 'int64', 'val': 'float64'},
    },
}

UTC_BEGIN = {'$dt': '2024-01-01T00:00:00+00:00'}
UTC_END = {'$dt': '2024-02-01T00:00:00+00:00'}

DATA_QUERIES = [
    {},
    {'select_columns': ['id']},
    {'select_columns': ['id', 'missing']},
    {'omit_columns': ['val', 'v', 'a']},
    {'begin': 'BEGIN'},
    {'end': 'END'},
    {'begin': 'BEGIN', 'end': 'END'},
    {'params': {'id': [1, 2]}},
    {'params': {'id': '_3', 'missing': 1}},
    {'params': {'id': ['_3', '_4', None]}},
    {'begin': 'BEGIN', 'params': {'id': 1}},
    {'order': 'desc'},
    {'order': None},
    {'limit': 5},
    {'begin': 'BEGIN', 'end': 'END', 'order': 'desc', 'limit': 10},
]

### NOTE: `get_pipe_rowcount` selects `set([dt_col] + list(params))`, whose order depends on
### `PYTHONHASHSEED`, so `params` only name the datetime column (or a single column when there
### is none) to keep the fixture deterministic.
ROWCOUNT_QUERIES = [
    {},
    {'begin': 'BEGIN'},
    {'begin': 'BEGIN', 'end': 'END'},
    {'params': 'DT_PARAMS'},
    {'end': 'END', 'params': 'DT_PARAMS'},
]

CLEAR_QUERIES = [
    {},
    {'begin': 'BEGIN'},
    {'begin': 'BEGIN', 'end': 'END'},
    {'params': {'id': [1, 2]}},
    {'end': 'END', 'params': {'id': '_1'}},
]

### NOTE: v4 iterates a `set` of new columns, so each case adds one column.
ADD_COLUMNS = [
    {'new_int': 'int64'},
    {'new_str': 'str'},
    {'new_json': 'json'},
    {'new_dt': 'datetime64[ns, UTC]'},
    {'dt': 'datetime64[ns, UTC]', 'id': 'int64'},
]

ALTER_COLUMNS = [
    {'val': 'object'},
    {'val': 'numeric', 'id': 'int64'},
    {'s': 'int64', 'id': 'float64'},
    {'v': 'string', 'a': 'float64'},
]


def decode(v):
    if isinstance(v, dict) and '$dt' in v:
        return datetime.fromisoformat(v['$dt'])
    return v


def bound_for(shape, which):
    if shape == 'int_axis':
        return 10 if which == 'BEGIN' else 20
    if shape == 'naive_dt':
        return {'$dt': ('2024-01-01T00:00:00' if which == 'BEGIN' else '2024-02-01T00:00:00')}
    return UTC_BEGIN if which == 'BEGIN' else UTC_END


def resolve_query(shape, query):
    out = {}
    dt_col = SHAPES[shape]['parameters'].get('columns', {}).get('datetime', None)
    if shape == 'guess_dt':
        dt_col = 'when'
    for k, v in query.items():
        if v in ('BEGIN', 'END'):
            out[k] = bound_for(shape, v)
        elif v == 'DT_PARAMS':
            out[k] = {dt_col: '_None'} if dt_col else {'id': [1, 2]}
        else:
            out[k] = v
    return out


def make_conn(flavor, prefix='gen_'):
    kw = (
        {'database': '/tmp/_mrsm_gen_instance_queries.db'}
        if flavor == 'sqlite'
        else {'username': 'u', 'password': 'p', 'host': 'localhost', 'database': 'd'}
    )
    conn = mrsm.get_connector('sql', prefix + flavor.replace('-', '_'), flavor=flavor, **kw)
    from meerschaum.utils.sql import NO_SCHEMA_FLAVORS
    conn.__dict__['schema'] = None if flavor in NO_SCHEMA_FLAVORS else 'public'
    return conn


def main():
    from meerschaum.utils.dtypes.sql import get_db_type_from_pd_type
    import meerschaum.utils.sql as sql_utils
    import meerschaum.utils.misc as misc

    current_cols_types = {}
    mrsm.Pipe.get_columns_types = lambda self, refresh=False, debug=False: dict(current_cols_types)
    mrsm.Pipe.exists = lambda self, debug=False: True
    mrsm.Pipe.edit = lambda self, *args, **kwargs: (True, 'Success')
    sql_utils.table_exists = lambda *args, **kwargs: True
    sql_utils.get_table_cols_types = lambda *args, **kwargs: dict(current_cols_types)
    misc.generate_password = lambda length=12: 'abc'

    cases = []
    for flavor in FLAVORS:
        conn = make_conn(flavor)
        captured = []

        def capture(query, *args, **kwargs):
            captured.append(str(query))
            return None

        conn.value = capture
        conn.exec = capture

        for shape, spec in SHAPES.items():
            cols_types = {
                col: get_db_type_from_pd_type(typ, flavor)
                for col, typ in spec['pd_types'].items()
            }
            current_cols_types.clear()
            current_cols_types.update(cols_types)
            pipe = mrsm.Pipe(
                'gen', shape, instance=conn,
                parameters=json.loads(json.dumps(spec['parameters'])),
            )
            base = {
                'flavor': flavor,
                'shape': shape,
                'parameters': spec['parameters'],
                'cols_types': cols_types,
            }

            for query in DATA_QUERIES:
                q = resolve_query(shape, query)
                kwargs = {k: decode(v) for k, v in q.items()}
                try:
                    result = conn.get_pipe_data_query(pipe, **kwargs)
                except Exception as e:
                    result = {'error': str(e)}
                cases.append({**base, 'fn': 'get_pipe_data_query', 'query': q, 'result': result})

            for newest in (True, False):
                for params in (None, {'id': 1}):
                    captured.clear()
                    conn.get_sync_time(pipe, params=params, newest=newest)
                    cases.append({
                        **base, 'fn': 'get_sync_time_query',
                        'query': {'newest': newest, 'params': params},
                        'result': captured[0] if captured else None,
                    })

            for query in ROWCOUNT_QUERIES:
                q = resolve_query(shape, query)
                captured.clear()
                conn.get_pipe_rowcount(pipe, **{k: decode(v) for k, v in q.items()})
                cases.append({
                    **base, 'fn': 'get_pipe_rowcount_query', 'query': q,
                    'result': captured[0] if captured else None,
                })

            for query in CLEAR_QUERIES:
                q = resolve_query(shape, query)
                captured.clear()
                conn.clear_pipe(pipe, **{k: decode(v) for k, v in q.items()})
                cases.append({
                    **base, 'fn': 'clear_pipe_query', 'query': q,
                    'result': captured[0] if captured else None,
                })

            for df_types in ADD_COLUMNS:
                result = conn.get_add_columns_queries(pipe, dict(df_types))
                cases.append({
                    **base, 'fn': 'get_add_columns_queries', 'query': {'df': df_types},
                    'result': result,
                })

            for df_types in ALTER_COLUMNS:
                result = conn.get_alter_columns_queries(pipe, dict(df_types))
                cases.append({
                    **base, 'fn': 'get_alter_columns_queries', 'query': {'df': df_types},
                    'result': result,
                })

    ### Instance tables (SQLAlchemy `CREATE TABLE` / `CREATE INDEX` as `create_all` emits them).
    from sqlalchemy.schema import CreateTable, CreateIndex
    import meerschaum.connectors.sql.tables as tables_module
    tables_module._check_create_cache = lambda c, debug=False: False
    for flavor in ('postgresql', 'timescaledb', 'postgis', 'mysql', 'mariadb', 'sqlite'):
        conn = make_conn(flavor, prefix='ddl_')
        _ = tables_module.get_tables(conn, create=False)
        dialect = conn.engine.dialect
        ddl = {}
        for table in conn.metadata.sorted_tables:
            stmts = [str(CreateTable(table).compile(dialect=dialect))]
            for ix in sorted(table.indexes, key=lambda i: i.name):
                stmts.append(str(CreateIndex(ix).compile(dialect=dialect)))
            ddl[table.name] = stmts
        cases.append({'fn': 'instance_tables', 'flavor': flavor, 'schema': conn.schema, 'result': ddl})

    meta = {
        'python': sys.version.split()[0],
        'meerschaum': mrsm.__version__,
        'generator': 'crates/meerschaum-connector-sql/tests/fixtures/gen_instance_queries.py',
        'num_cases': len(cases),
    }
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump({'meta': meta, 'cases': cases}, f, indent=1, sort_keys=False)
        f.write('\n')
    print(f"Wrote {len(cases)} cases to {OUT}", file=sys.stderr)


if __name__ == '__main__':
    main()
