#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`pipe.drop()`, `pipe.delete()` and a full `pipe.clear()` forget the pipe's Syncing Engine state
(ledger entries, edge cursors, definition hash; wave 9), so a pipe recreated under the same keys
starts fresh, as in v4. Partial clears keep it. Synthetic, on the SQLite test instance.
"""

import uuid

import pytest
import meerschaum as mrsm
from meerschaum import _core

from tests.connectors import conns

pytestmark = pytest.mark.skipif(
    _core.load() is None,
    reason="The Syncing Engine needs the Rust extension (scripts/rust/build_py.sh).",
)


def _ref(pipe):
    return '|'.join([str(pipe.instance_keys), pipe.connector_keys, pipe.metric_key, 'None'])


def _count(conn, metric, column, value):
    state = mrsm.Pipe(
        'mrsm', metric, instance=conn, temporary=True, static=True, cache=False,
        target=f'mrsm_{metric}',
    )
    if not state.exists():
        return 0
    df = state.get_data(params={column: value})
    return 0 if df is None else len(df)


@pytest.fixture
def chain():
    conn = conns['sqlite']
    inst = str(conn)
    tag = uuid.uuid4().hex[:8]
    table = f'w9_state_src_{tag}'
    conn.exec_queries([f"CREATE TABLE {table} (ts TIMESTAMP, id INTEGER, v INTEGER)"])
    conn.exec_queries([
        f"INSERT INTO {table} VALUES ('2024-01-01 00:00:00', 1, 1), ('2024-01-01 01:00:00', 2, 5)"
    ])
    raw_metric, child_metric = f'w9_state_raw_{tag}', f'w9_state_child_{tag}'
    params = lambda definition: {
        'columns': {'datetime': 'ts', 'id': 'id'},
        'dtypes': {'ts': 'datetime'},
        'fetch': {'definition': definition},
    }
    raw = mrsm.Pipe(inst, raw_metric, instance=conn, parameters=params(f"SELECT * FROM {table}"))
    child = mrsm.Pipe(inst, child_metric, instance=conn, parameters=params(
        f"SELECT ts, id, v FROM {{{{ Pipe('{inst}', '{raw_metric}', instance='{inst}') }}}}"
    ))
    for pipe in (raw, child):
        success, message = pipe.register()
        assert success, message
    yield conn, raw, child
    for pipe in (raw, child):
        pipe.drop()
        pipe.delete()
    conn.exec_queries([f"DROP TABLE IF EXISTS {table}"], silent=True)


def test_drop_delete_and_full_clear_forget_engine_state(chain):
    conn, raw, child = chain
    run = child.sync(parents=True)
    assert run.success, run.message
    raw_ref, child_ref = _ref(raw), _ref(child)
    assert _count(conn, 'engine_ledger', 'pipe_ref', raw_ref) > 0
    assert _count(conn, 'engine_cursors', 'child_ref', child_ref) == 1
    assert _count(conn, 'engine_state', 'pipe_ref', child_ref) == 1

    ### A partial clear keeps the state.
    success, message = child.clear(begin='2024-01-01 01:00:00')
    assert success, message
    assert _count(conn, 'engine_cursors', 'child_ref', child_ref) == 1

    ### A full clear forgets the child's cursor and hash.
    success, message = child.clear()
    assert success, message
    assert _count(conn, 'engine_cursors', 'child_ref', child_ref) == 0
    assert _count(conn, 'engine_state', 'pipe_ref', child_ref) == 0
    assert _count(conn, 'engine_ledger', 'pipe_ref', raw_ref) > 0

    ### Dropping the parent forgets its ledger and the cursors on it.
    assert child.sync(parents=True).success
    assert _count(conn, 'engine_cursors', 'parent_ref', raw_ref) == 1
    success, message = raw.drop()
    assert success, message
    assert _count(conn, 'engine_ledger', 'pipe_ref', raw_ref) == 0
    assert _count(conn, 'engine_cursors', 'parent_ref', raw_ref) == 0

    ### Deleting forgets too; the SuccessTuple is the instance's own.
    assert child.sync(parents=True).success
    result = child.delete()
    assert result[0], result[1]
    assert _count(conn, 'engine_state', 'pipe_ref', child_ref) == 0
    assert _count(conn, 'engine_cursors', 'child_ref', child_ref) == 0
