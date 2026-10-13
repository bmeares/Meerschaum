#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generate `v4_sync.json`: a six-level chain and a diamond of `sql:` pipes on a SQLite instance,
synced with v4.1's `sync pipes` (one pass per phase, `-w 1`, registration order = dependency
order), with the target tables read back after each phase.

The Rust test `tests/v4_sync.rs` replays the same phases through the engine
(`--parents` over the leaves) and compares its tables with the ones recorded here.

Run from the repository root with v4.1.4 on the path and an empty plugins directory:

    PYTHONPATH=$(pwd) python crates/meerschaum-engine/tests/fixtures/v4_sync/gen_v4_sync.py

The data is synthetic. The generator writes only into a temporary directory and records no
paths, hostnames or environment.
"""

import json
import os
import pathlib
import sys
import tempfile
from datetime import datetime, timedelta, timezone

HERE = pathlib.Path(__file__).resolve().parent
TMP = tempfile.mkdtemp(prefix='mrsm_v4_sync_')
os.environ['MRSM_ROOT_DIR'] = os.path.join(TMP, 'root')
os.environ['MRSM_PLUGINS_DIR'] = os.path.join(TMP, 'plugins')
os.makedirs(os.environ['MRSM_ROOT_DIR'], exist_ok=True)
os.makedirs(os.environ['MRSM_PLUGINS_DIR'], exist_ok=True)
DB_PATH = os.path.join(TMP, 'eng.db')
os.environ['MRSM_SQL_ENG'] = 'sqlite:///' + DB_PATH

import meerschaum as mrsm  # noqa: E402
from meerschaum.actions import actions  # noqa: E402

INSTANCE = 'sql:eng'


def ref(metric: str) -> str:
    return "{{ Pipe('sql:eng', '" + metric + "', instance='sql:eng') }}"


GRID = {'chunk_minutes': 360}

PIPES = [
    {
        'metric': 'a_raw',
        'parameters': {
            'sql': 'SELECT ts, sensor_id, value FROM src_readings',
            'columns': {'datetime': 'ts', 'id': 'sensor_id'},
            'dtypes': {'ts': 'datetime', 'sensor_id': 'int', 'value': 'float'},
            'verify': GRID,
        },
    },
    {
        'metric': 'b_clean',
        'parameters': {
            'sql': f"SELECT ts, sensor_id, value FROM {ref('a_raw')} WHERE value >= 0",
            'columns': {'datetime': 'ts', 'id': 'sensor_id'},
            'dtypes': {'ts': 'datetime', 'sensor_id': 'int', 'value': 'float'},
            'verify': GRID,
        },
    },
    {
        'metric': 'c_hourly',
        'parameters': {
            'sql': (
                "SELECT strftime('%Y-%m-%d %H:00:00', ts) AS hour, sensor_id, "
                "SUM(value) AS total, COUNT(*) AS num "
                f"FROM {ref('b_clean')} "
                "GROUP BY strftime('%Y-%m-%d %H:00:00', ts), sensor_id"
            ),
            'columns': {'datetime': 'hour', 'id': 'sensor_id'},
            'dtypes': {'hour': 'datetime', 'sensor_id': 'int', 'total': 'float', 'num': 'int'},
            'verify': GRID,
        },
    },
    {
        'metric': 'd_daily',
        'parameters': {
            'sql': (
                "SELECT strftime('%Y-%m-%d 00:00:00', hour) AS day, sensor_id, "
                "SUM(total) AS total, SUM(num) AS num "
                f"FROM {ref('c_hourly')} "
                "GROUP BY strftime('%Y-%m-%d 00:00:00', hour), sensor_id"
            ),
            'columns': {'datetime': 'day', 'id': 'sensor_id'},
            'dtypes': {'day': 'datetime', 'sensor_id': 'int', 'total': 'float', 'num': 'int'},
            'verify': {'chunk_days': 1},
        },
    },
    {
        'metric': 'e_peak',
        'parameters': {
            'sql': f"SELECT day, MAX(total) AS peak FROM {ref('d_daily')} GROUP BY day",
            'columns': {'datetime': 'day'},
            'dtypes': {'day': 'datetime', 'peak': 'float'},
            'verify': {'chunk_days': 1},
        },
    },
    {
        'metric': 'f_report',
        'parameters': {
            'sql': f"SELECT day, peak * 2 AS score FROM {ref('e_peak')}",
            'columns': {'datetime': 'day'},
            'dtypes': {'day': 'datetime', 'score': 'float'},
            'parents': ["Pipe('sql:eng', 'e_peak', instance='sql:eng')"],
            'verify': {'chunk_days': 1},
        },
    },
    {
        'metric': 'g_scaled',
        'parameters': {
            'sql': f"SELECT ts, sensor_id, value * 10 AS scaled FROM {ref('a_raw')}",
            'columns': {'datetime': 'ts', 'id': 'sensor_id'},
            'dtypes': {'ts': 'datetime', 'sensor_id': 'int', 'scaled': 'float'},
            'verify': GRID,
        },
    },
    {
        'metric': 'h_counts',
        'parameters': {
            'sql': f"SELECT ts, COUNT(*) AS readings FROM {ref('a_raw')} GROUP BY ts",
            'columns': {'datetime': 'ts'},
            'dtypes': {'ts': 'datetime', 'readings': 'int'},
            'verify': GRID,
        },
    },
    {
        'metric': 'i_joined',
        'parameters': {
            'sql': (
                "SELECT g.ts AS ts, g.sensor_id AS sensor_id, g.scaled AS scaled, "
                "h.readings AS readings "
                f"FROM {ref('g_scaled')} AS g "
                f"INNER JOIN {ref('h_counts')} AS h ON g.ts = h.ts"
            ),
            'columns': {'datetime': 'ts', 'id': 'sensor_id'},
            'dtypes': {
                'ts': 'datetime', 'sensor_id': 'int', 'scaled': 'float', 'readings': 'int',
            },
            'verify': GRID,
        },
    },
]

CREATE = 'CREATE TABLE src_readings (ts TEXT, sensor_id INTEGER, value REAL)'


def readings(start: datetime, count: int, sensors, offset: int = 0):
    rows = []
    for i in range(count):
        ts = start + timedelta(minutes=15 * i)
        for s in sensors:
            value = float(((i + offset) * 7 + s * 5) % 23 - 3)
            rows.append((ts.strftime('%Y-%m-%d %H:%M:%S'), s, value))
    return rows


def insert_statement(rows) -> str:
    values = ', '.join(f"('{ts}', {s}, {v!r})" for ts, s, v in rows)
    return f'INSERT INTO src_readings (ts, sensor_id, value) VALUES {values}'


PHASES = [
    {
        'description': 'initial load: two days of 15-minute readings for two sensors',
        'statements': [insert_statement(readings(datetime(2026, 1, 1), 192, [1, 2]))],
    },
    {
        'description': (
            'six new hours, a late reading for a new sensor three hours back, '
            'and a corrected value two hours back'
        ),
        'statements': [
            insert_statement(readings(datetime(2026, 1, 3), 24, [1, 2], offset=192)),
            insert_statement([('2026-01-02 21:15:00', 3, 4.0)]),
            "UPDATE src_readings SET value = 100.0 "
            "WHERE ts = '2026-01-02 22:00:00' AND sensor_id = 1",
        ],
    },
    {
        'description': 'no source changes',
        'statements': [],
    },
]


def normalize(v):
    if v is None:
        return None
    if hasattr(v, 'isoformat'):
        if getattr(v, 'tzinfo', None) is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    if hasattr(v, 'item'):
        v = v.item()
    if isinstance(v, float):
        if v != v:
            return None
        return float(v)
    return v


def read_table(pipe):
    df = pipe.get_data()
    if df is None:
        return []
    cols = sorted(df.columns)
    rows = [
        [normalize(row[c]) for c in cols]
        for row in df.to_dict(orient='records')
    ]
    rows.sort(key=json.dumps)
    return {'columns': cols, 'rows': rows}


def main():
    conn = mrsm.get_connector(INSTANCE)
    assert conn.flavor == 'sqlite', conn.flavor
    conn.exec(CREATE)
    pipes = []
    for spec in PIPES:
        pipe = mrsm.Pipe('sql:eng', spec['metric'], instance=INSTANCE)
        pipe.parameters = spec['parameters']
        success, msg = pipe.register()
        assert success, msg
        pipes.append(pipe)

    phases_out = []
    for phase in PHASES:
        for stmt in phase['statements']:
            assert conn.exec(stmt) is not None, stmt
        success, msg = actions['sync'](
            ['pipes'],
            mrsm_instance=INSTANCE,
            workers=1,
            nopretty=True,
            shell=False,
        )
        assert success, msg
        expected = {}
        for pipe in pipes:
            pipe._invalidate_cache(hard=True)
            expected[pipe.metric_key] = read_table(pipe)
        phases_out.append({
            'description': phase['description'],
            'statements': phase['statements'],
            'expected': expected,
        })

    out = {
        'description': (
            'A six-level chain (a_raw → b_clean → c_hourly → d_daily → e_peak → f_report) and '
            'a diamond (a_raw → g_scaled, a_raw → h_counts → i_joined ← g_scaled) of sql: pipes '
            'on a SQLite instance, synced by v4.1 `sync pipes -w 1` once per phase. Generated '
            'by gen_v4_sync.py; each table is sorted columns plus rows (UTC ISO datetimes), '
            'rows sorted by their JSON text.'
        ),
        'meerschaum_version': mrsm.__version__,
        'instance': INSTANCE,
        'create': CREATE,
        'pipes': [
            {'connector': 'sql:eng', 'metric': p['metric'], 'parameters': p['parameters']}
            for p in PIPES
        ],
        'phases': phases_out,
    }
    with open(HERE / 'v4_sync.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps(out, separators=(',', ':')).replace('],[', '],\n['))
        f.write('\n')
    print('wrote', (HERE / 'v4_sync.json').relative_to(HERE.parents[3]), file=sys.stderr)


if __name__ == '__main__':
    main()
