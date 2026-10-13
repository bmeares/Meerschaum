#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Python half of the cross-implementation test (`tests/instance_cross_impl.rs`).

Run with `PYTHONPATH=<repo root>` and empty temporary `MRSM_ROOT_DIR` / `MRSM_PLUGINS_DIR`:

    python cross_impl.py sync <db> <scenario.json> <phase>   # sync one phase of every pipe
    python cross_impl.py read <db> <scenario.json>           # get_data() of every pipe
    python cross_impl.py dump <db>                           # schema + rows of every table

`<db>` is a SQLite file path, or a JSON object of connector attributes (`flavor`, `host`,
`port`, `username`, `password`, `database`, `schema`) for a server flavor such as MSSQL, whose
`dump` lists the tables of the connector's schema.

All output is JSON on the last line of stdout.
"""

import json
import sys
import uuid
import sqlite3
from datetime import datetime, date
from decimal import Decimal


def decode(value):
    """Decode the scenario's tagged values."""
    if isinstance(value, dict):
        if '$dt' in value:
            return datetime.fromisoformat(value['$dt'])
        if '$date' in value:
            return date.fromisoformat(value['$date'])
        if '$dec' in value:
            return Decimal(value['$dec'])
        if '$uuid' in value:
            return uuid.UUID(value['$uuid'])
        if '$bytes' in value:
            return bytes.fromhex(value['$bytes'])
        return {k: decode(v) for k, v in value.items()}
    if isinstance(value, list):
        return [decode(v) for v in value]
    return value


def normalize(value):
    """`str()` of a cell, with every null spelling as `None`."""
    import pandas as pd
    try:
        if value is None or pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


def get_conn(db_path):
    import meerschaum as mrsm
    ### NOTE: The label is fixed so `sql:xi` pipes are synced in place (connector == instance).
    if db_path.startswith('{'):
        return mrsm.get_connector('sql', 'xi', **json.loads(db_path))
    return mrsm.get_connector('sql', 'xi', flavor='sqlite', database=db_path)


def get_pipe(conn, spec):
    import meerschaum as mrsm
    return mrsm.Pipe(
        spec['connector_keys'], spec['metric_key'], spec['location_key'],
        instance=conn,
        parameters=json.loads(json.dumps(spec['parameters'])),
    )


def do_sync(db_path, scenario_path, phase):
    with open(scenario_path, 'r', encoding='utf-8') as f:
        scenario = json.load(f)
    conn = get_conn(db_path)
    results = []
    for spec in scenario['pipes']:
        pipe = get_pipe(conn, spec)
        if spec.get('inplace'):
            success, msg = pipe.sync()
        else:
            docs = decode(spec['phases'][phase])
            success, msg = pipe.sync(docs)
        results.append([success, msg])
    return results


def do_read(db_path, scenario_path):
    with open(scenario_path, 'r', encoding='utf-8') as f:
        scenario = json.load(f)
    conn = get_conn(db_path)
    out = {}
    for spec in scenario['pipes']:
        pipe = get_pipe(conn, spec)
        df = pipe.get_data()
        key = f"{spec['connector_keys']}:{spec['metric_key']}:{spec['location_key']}"
        out[key] = (
            [
                {col: normalize(val) for col, val in doc.items()}
                for doc in df.to_dict(orient='records')
            ]
            if df is not None
            else None
        )
        out[key + ':sync_time'] = normalize(pipe.get_sync_time())
        out[key + ':rowcount'] = pipe.get_rowcount()
        out[key + ':columns_types'] = conn.get_pipe_columns_types(pipe)
    return out


def encode_cell(value):
    if isinstance(value, bytes):
        return {'$bytes': value.hex()}
    if isinstance(value, float):
        return {'$float': repr(value)}
    return value


def do_dump_server(target):
    """Schema and rows of every table in a server connector's schema."""
    conn = get_conn(target)
    schema = conn.schema
    tables = conn.read(
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' ORDER BY table_name"
    )['table_name'].tolist()
    out = {'tables': {}}
    for table in tables:
        info = conn.read(
            "SELECT column_name, data_type, character_maximum_length, numeric_precision, "
            "numeric_scale, is_nullable FROM information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}' "
            "ORDER BY ordinal_position"
        )
        from meerschaum.utils.sql import sql_item_name
        df = conn.read(f"SELECT * FROM {sql_item_name(table, conn.flavor, schema)}")
        rows = sorted(
            json.dumps([normalize_dump(v) for v in row], default=str)
            for row in df.itertuples(index=False)
        )
        out['tables'][table] = {
            'info': [[normalize_dump(v) for v in row] for row in info.itertuples(index=False)],
            'count': len(df),
            'rows': rows,
        }
        out['tables'][table]['indices'] = _server_indices(conn, schema, table)
    return out


def _server_indices(conn, schema, table):
    from meerschaum.utils.sql import get_table_cols_indices
    cols_indices = get_table_cols_indices(table, conn, schema=schema)
    return sorted(
        f"{col}:{ix.get('type')}"
        for col, ixs in cols_indices.items()
        for ix in ixs
    )


def normalize_dump(value):
    """`str()` of a cell, with every null spelling as `None` and bytes as hex."""
    import pandas as pd
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    try:
        if value is None or pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


def do_dump(db_path):
    if db_path.startswith('{'):
        return do_dump_server(db_path)
    con = sqlite3.connect(db_path)
    out = {}
    objects = con.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
    ).fetchall()
    out['objects'] = [list(row) for row in objects]
    tables = [row[1] for row in objects if row[0] == 'table']
    out['tables'] = {}
    for table in tables:
        info = con.execute(f'PRAGMA table_info("{table}")').fetchall()
        cols = [row[1] for row in info]
        typed_cols = ', '.join(f'typeof("{c}"), "{c}"' for c in cols)
        rows = con.execute(f'SELECT {typed_cols} FROM "{table}" ORDER BY rowid').fetchall()
        out['tables'][table] = {
            'info': [list(row) for row in info],
            'count': len(rows),
            'rows': [[encode_cell(v) for v in row] for row in rows],
        }
    out['journal_mode'] = con.execute('PRAGMA journal_mode').fetchone()[0]
    con.close()
    return out


if __name__ == '__main__':
    mode = sys.argv[1]
    if mode == 'sync':
        result = do_sync(sys.argv[2], sys.argv[3], int(sys.argv[4]))
    elif mode == 'read':
        result = do_read(sys.argv[2], sys.argv[3])
    elif mode == 'dump':
        result = do_dump(sys.argv[2])
    else:
        raise ValueError(f"Unknown mode '{mode}'.")
    print(json.dumps(result, default=str))
