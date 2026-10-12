#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Python half of the cross-implementation test (`tests/instance_cross_impl.rs`).

Run with `PYTHONPATH=<repo root>` and empty temporary `MRSM_ROOT_DIR` / `MRSM_PLUGINS_DIR`:

    python cross_impl.py sync <db> <scenario.json> <phase>   # sync one phase of every pipe
    python cross_impl.py read <db> <scenario.json>           # get_data() of every pipe
    python cross_impl.py dump <db>                           # schema + rows of every table

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


def do_dump(db_path):
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
