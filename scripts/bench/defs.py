"""
Shared definitions for the sync benchmark (`scripts/bench/run.py`): pipe layouts, table names
and connector environment variables. Imported by the driver and by `worker.py`.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

import os

SIZES = {'10k': 10_000, '100k': 100_000, '1M': 1_000_000}
BASE_EPOCH = 1_704_067_200  # 2024-01-01T00:00:00Z
#: The driver assigns a fresh database and schema per run, and passes them to its workers.
SCHEMA = os.environ.get('MRSM_BENCH_PG_SCHEMA', 'w17cbench_unset')
PG_DATABASE = os.environ.get('MRSM_BENCH_PG_DATABASE', 'w17cbench_unset')
PG_BACKENDS = {'postgis': 5449, 'timescaledb': 5439}
#: Rows of the small pipe of the per-call latency cases.
LATENCY_ROWS = 50
SRC_INSTANCE = 'sql:a_src'
DEST_INSTANCE = 'sql:b_dest'

#: Implementations: how a case is executed. `py` runs `worker.py` inside the implementation's
#: interpreter (timed around the call); `cli` times the whole process, startup included.
IMPLS = ('v4py', 'v4cli', 'v5py', 'v5cli', 'v5bin')
PY_IMPLS = ('v4py', 'v5py')
#: Every implementation `--impls` accepts.
ALL_IMPLS = IMPLS
CLI_IMPLS = ('v4cli', 'v5cli', 'v5bin')

DTYPES = {'dt': 'datetime', 'id': 'int', 'value': 'float'}
COLUMNS = {'datetime': 'dt', 'id': 'id'}


def src_table(label: str, backend: str) -> str:
    name = f'src_{label}'
    return name if backend == 'sqlite' else f'{SCHEMA}.{name}'


def inc_src_table(label: str, impl: str, backend: str) -> str:
    name = f'src_inc_{label}_{impl}'
    return name if backend == 'sqlite' else f'{SCHEMA}.{name}'


def sync_metric(label: str, impl: str) -> str:
    return f'sync_{label}_{impl}'


def inc_metric(label: str, impl: str) -> str:
    return f'inc_{label}_{impl}'


def target_name(metric: str) -> str:
    return f'bench_{metric}'.lower()  # lowercase: PostgreSQL folds unquoted names


def latency_metric(impl: str) -> str:
    return f'lat_small_{impl}'


def chain_metrics(impl: str) -> List[str]:
    return [f'chain_1_raw_{impl}', f'chain_2_mid_{impl}', f'chain_3_leaf_{impl}']


def chain_targets(impl: str) -> List[str]:
    """Chain pipes keep v4's default target names (see the note in `run.py`)."""
    raw, mid, leaf = chain_metrics(impl)
    return [
        f'sql_a_src_{raw}', f'sql_b_dest_{mid}', f'sql_b_dest_{leaf}',
    ]


def chain_tag(impl: str) -> str:
    return f'w7chain_{impl}'


def pipe_definitions(backend: str, labels: List[str], impls=IMPLS) -> List[Dict[str, Any]]:
    """Every pipe the benchmark registers on the destination instance, as dicts."""
    out: List[Dict[str, Any]] = []

    def add(connector, metric, sql, tags=None, default_target=False):
        params = {
            'columns': dict(COLUMNS),
            'dtypes': dict(DTYPES),
            'sql': sql,
        }
        if not default_target:
            params['target'] = target_name(metric)
        if tags:
            params['tags'] = tags
        out.append({
            'connector': connector,
            'metric': metric,
            'instance': DEST_INSTANCE,
            'parameters': params,
        })

    for impl in impls:
        add(SRC_INSTANCE, sync_metric('warmup', impl),
            f'SELECT * FROM {src_table("10k", backend)}')
        add(SRC_INSTANCE, latency_metric(impl), f'SELECT * FROM {src_table("small", backend)}')
        for label in labels:
            add(SRC_INSTANCE, sync_metric(label, impl),
                f'SELECT * FROM {src_table(label, backend)}')
            add(SRC_INSTANCE, inc_metric(label, impl),
                f'SELECT * FROM {inc_src_table(label, impl, backend)}')
        raw, mid, leaf = chain_metrics(impl)
        tag = [chain_tag(impl)]
        add(SRC_INSTANCE, raw, f'SELECT * FROM {src_table("100k", backend)}', tag, True)
        add(DEST_INSTANCE, mid,
            "SELECT dt, id, value * 2.0 AS value FROM "
            "{{ Pipe('" + SRC_INSTANCE + "', '" + raw + "', instance='" + DEST_INSTANCE + "') }}",
            tag, True)
        add(DEST_INSTANCE, leaf,
            "SELECT dt, id, value + 1.0 AS value FROM "
            "{{ Pipe('" + DEST_INSTANCE + "', '" + mid + "', instance='" + DEST_INSTANCE + "') }}",
            tag, True)
    return out


def connector_env(backend: str, workdir: str) -> Dict[str, str]:
    """`MRSM_SQL_*` variables for the source and destination connectors."""
    if backend == 'sqlite':
        a = {'flavor': 'sqlite', 'database': f'{workdir}/src.db'}
        b = {'flavor': 'sqlite', 'database': f'{workdir}/dest.db'}
    elif backend in PG_BACKENDS:
        base = {
            'flavor': backend, 'host': 'localhost', 'port': PG_BACKENDS[backend],
            'username': 'test', 'password': 'test1234',
            'database': PG_DATABASE, 'schema': SCHEMA,
        }
        a, b = dict(base), dict(base)
    else:
        raise ValueError(f'Unknown backend: {backend}')
    return {'MRSM_SQL_A_SRC': json.dumps(a), 'MRSM_SQL_B_DEST': json.dumps(b)}
