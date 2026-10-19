#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`pipe.sync(parents=, children=)` and `mrsm.Pipeline` (design §5.2, D4, D5, R1): the Python
entry points of the Rust Syncing Engine, over a three-level `{{ Pipe() }}` chain on SQLite.

The engine is Rust-only (D14), so these need the built extension (`scripts/rust/build_py.sh`).
Everything is synthetic and runs on a SQLite file of its own per worker (`_conn()`).
"""

import json
from datetime import datetime, timezone

import pytest
import meerschaum as mrsm
from meerschaum import _core
from meerschaum.core.Pipe._sync import SyncResult

from tests._pipeline_diag import why
from tests._worker_sqlite import worker_sqlite

pytestmark = pytest.mark.skipif(
    _core.load() is None,
    reason="The Syncing Engine needs the Rust extension (scripts/rust/build_py.sh).",
)

SOURCE_TABLE = 'w8_pipeline_readings'
TAG = 'w8-pipeline'


def _conn():
    """
    The SQLite instance of these tests: a file of their own per `pytest -n` worker
    (`tests/_worker_sqlite.py`), not the shared `sql:test_sqlite`, whose other writers made a
    chain sync fail with `database is locked` under `pytest -n 8`.
    """
    return worker_sqlite('w8_pipeline')


def _insert(conn, rows):
    for ts, site, temp in rows:
        queries = [f"INSERT INTO {SOURCE_TABLE} (ts, site, temp) VALUES ('{ts}', '{site}', {temp})"]
        assert conn.exec_queries(queries, silent=True, break_on_error=True) is not None


@pytest.fixture
def chain():
    """
    `readings table -> raw -> clean -> daily`, each level reading the previous one through
    `{{ Pipe() }}` (no `parents` key: the engine sees the edges from the SQL).

    Every test shares the one source table and the three pipes, so under `pytest -n` two
    workers running tests from this module at once would drop each other's table mid-run
    (CI saw `1 failed, 1 upstream_failed` on a chain sync). An inter-process lock keeps the
    chain tests serial across workers.
    """
    from meerschaum.config.paths import ROOT_DIR_PATH
    from meerschaum.utils.locks import InterProcessLock
    with InterProcessLock(ROOT_DIR_PATH / '.w8_pipeline_chain.lock'):
        yield from _build_chain()


def _build_chain():
    conn = _conn()
    inst = str(conn)
    conn.exec_queries([f"DROP TABLE IF EXISTS {SOURCE_TABLE}"], silent=True)
    conn.exec_queries(
        [f"CREATE TABLE {SOURCE_TABLE} (ts TIMESTAMP, site TEXT, temp INTEGER)"],
        silent=True,
        break_on_error=True,
    )
    _insert(conn, [
        ('2024-01-01 00:00:00', 'a', 1),
        ('2024-01-01 01:00:00', 'a', 2),
        ('2024-01-01 02:00:00', 'b', 3),
    ])
    columns = {'datetime': 'ts', 'id': 'site'}
    dtypes = {'ts': 'datetime'}
    definitions = {
        'w8_raw': f"SELECT * FROM {SOURCE_TABLE}",
        'w8_clean': (
            "SELECT ts, site, temp FROM "
            f"{{{{ Pipe('{inst}', 'w8_raw', instance='{inst}') }}}} WHERE temp IS NOT NULL"
        ),
        'w8_daily': (
            "SELECT ts, site, temp * 10 AS temp FROM "
            f"{{{{ Pipe('{inst}', 'w8_clean', instance='{inst}') }}}}"
        ),
    }

    def build():
        return tuple(
            mrsm.Pipe(
                inst, metric, instance=conn,
                parameters={
                    'columns': columns, 'dtypes': dtypes, 'tags': [TAG],
                    'fetch': {'definition': definition},
                },
            )
            for metric, definition in definitions.items()
        )

    for stale in build():
        stale.delete()
    pipes = build()
    for pipe in pipes:
        success, message = pipe.register()
        assert success, message
    yield pipes
    for pipe in pipes:
        pipe.drop()
        pipe.delete()
    conn.exec_queries([f"DROP TABLE IF EXISTS {SOURCE_TABLE}"], silent=True)


_why = why


def _statuses(run):
    return {
        node['pipe_ref'].split('|')[2]: node['status']
        for node in run.record['nodes']
    }


def _rowcounts(*pipes):
    return [pipe.get_rowcount() if pipe.exists() else 0 for pipe in pipes]


def test_children_true_runs_the_whole_chain(chain):
    raw, clean, daily = chain
    run = raw.sync(children=True)
    assert run[0] is True, _why(run)
    assert isinstance(run, tuple) and len(run) == 2
    assert run.success is True, _why(run)
    assert run.run_id
    assert [o['pipe_ref'].split('|')[2] for o in run.outcomes] == ['w8_raw', 'w8_clean', 'w8_daily']
    assert all(o['result']['success'] for o in run.outcomes)
    assert _rowcounts(raw, clean, daily) == [3, 3, 3]
    assert daily.get_data(order='asc')['temp'].tolist() == [10, 20, 30]

    ### Run history is on the instance.
    detail = mrsm.Pipeline(raw).get_run(run.run_id)
    assert detail['summary']['status'] == 'succeeded'
    events = [event['event'] for event in run.events()]
    assert events[0] == 'run_started' and events[-1] == 'run_finished'
    assert 'node_finished' in events


def test_second_run_skips_caught_up_children(chain):
    raw, clean, daily = chain
    run = raw.sync(children=True)
    assert run.success, _why(run)
    run = raw.sync(children=True)
    assert run.success, _why(run)
    statuses = _statuses(run)
    assert statuses['w8_clean'] == 'skipped'
    assert statuses['w8_daily'] == 'skipped'
    assert _rowcounts(raw, clean, daily) == [3, 3, 3]

    ### New source rows flow all the way down.
    _insert(_conn(), [('2024-01-01 03:00:00', 'b', 4)])
    run = raw.sync(children=True)
    assert run.success, _why(run)
    assert _rowcounts(raw, clean, daily) == [4, 4, 4]


def test_parents_true_syncs_the_ancestors_first(chain):
    raw, clean, daily = chain
    run = daily.sync(parents=True)
    assert run.success, _why(run)
    assert [o['pipe_ref'].split('|')[2] for o in run.outcomes] == ['w8_raw', 'w8_clean', 'w8_daily']
    assert _rowcounts(raw, clean, daily) == [3, 3, 3]


def test_depth_limits(chain):
    raw, clean, daily = chain
    run = raw.sync(children=True)
    assert run.success, _why(run)
    _insert(_conn(), [('2024-01-01 03:00:00', 'b', 4)])

    ### `children=1` stops at the direct child.
    run = raw.sync(children=1)
    assert run.success, _why(run)
    names = [o['pipe_ref'].split('|')[2] for o in run.outcomes]
    assert names == ['w8_raw', 'w8_clean']
    assert _rowcounts(raw, clean, daily) == [4, 4, 3]

    ### `parents=1` is the direct parent only.
    _insert(_conn(), [('2024-01-01 04:00:00', 'a', 5)])
    run = daily.sync(parents=1)
    assert run.success, _why(run)
    names = [o['pipe_ref'].split('|')[2] for o in run.outcomes]
    assert 'w8_raw' not in names
    assert names == ['w8_clean', 'w8_daily']

    ### `parents=0` / `children=False` are no traversal at all.
    assert type(raw.sync(parents=0, children=False)) is SyncResult


def test_bare_sync_is_v4(chain):
    raw, clean, daily = chain
    before = mrsm.Pipeline(raw).get_runs(all_runs=True, limit=100)
    result = raw.sync()
    ### A plain tuple subclass (`SyncResult`), never a `RunResult`.
    assert type(result) is SyncResult
    assert result[0] is True
    assert _rowcounts(raw, clean, daily) == [3, 0, 0]
    result = clean.sync()
    assert type(result) is SyncResult and result[0] is True
    assert _rowcounts(raw, clean, daily) == [3, 3, 0]
    after = mrsm.Pipeline(raw).get_runs(all_runs=True, limit=100)
    assert len(after) == len(before)


def test_sync_with_a_dataframe_and_a_traversal_is_rejected(chain):
    raw, _, _ = chain
    with pytest.raises(ValueError):
        raw.sync([{'ts': datetime(2024, 1, 1), 'site': 'a', 'temp': 1}], children=True)


def test_pipeline_plan_is_a_dry_run(chain):
    raw, clean, daily = chain
    pipeline = mrsm.Pipeline(daily, parents=True)
    runs_before = len(pipeline.get_runs(limit=1000))
    plan = pipeline.plan()
    assert [n.pipe.metric_key for n in plan.nodes] == ['w8_raw', 'w8_clean', 'w8_daily']
    assert [n.action for n in plan.nodes][0] == 'root'
    assert plan.edges
    text = str(plan)
    assert 'node' in text and 'action' in text and 'w8_clean' in text
    rows = plan.to_dicts()
    assert [r['node'].split('/')[1] for r in rows] == ['w8_raw', 'w8_clean', 'w8_daily']
    assert plan.to_dataframe()['action'].tolist()[0] == 'root'
    assert plan.node(clean).pipe_ref.endswith('|w8_clean|None')
    assert [p.metric_key for p in pipeline.pipes] == ['w8_raw', 'w8_clean', 'w8_daily']
    ### Nothing moved.
    assert _rowcounts(raw, clean, daily) == [0, 0, 0]
    assert len(pipeline.get_runs(limit=1000)) == runs_before


def test_pipeline_sync_and_plan_execute(chain):
    raw, clean, daily = chain
    pipeline = mrsm.Pipeline([raw, clean], children=True)
    run = pipeline.sync(workers=2)
    assert run.success, _why(run)
    assert run.plan is not None and len(run.plan) == 3
    assert _rowcounts(raw, clean, daily) == [3, 3, 3]
    runs = pipeline.get_runs()
    assert runs[0]['run']['run_id'] == run.run_id
    assert runs[0]['status'] == 'succeeded'
    assert pipeline.get_events(run.run_id)

    _insert(_conn(), [('2024-01-01 05:00:00', 'b', 6)])
    plan = mrsm.Pipeline(raw, children=True).plan()
    second = plan.execute()
    assert second.success, _why(second)
    assert _rowcounts(raw, clean, daily) == [4, 4, 4]
    assert second.run_id != run.run_id


def test_pipeline_from_tags_and_keys(chain):
    raw, clean, daily = chain
    pipeline = mrsm.Pipeline(tags=[TAG], instance=_conn())
    assert sorted(p.metric_key for p in pipeline.pipes) == ['w8_clean', 'w8_daily', 'w8_raw']
    by_keys = mrsm.Pipeline(
        [(raw.connector_keys, 'w8_clean')],
        parents=True,
        instance=_conn(),
    )
    assert [p.metric_key for p in by_keys.pipes] == ['w8_raw', 'w8_clean']
    run = pipeline.sync()
    assert run.success, _why(run)
    assert _rowcounts(raw, clean, daily) == [3, 3, 3]


def test_pipeline_needs_targets():
    with pytest.raises(ValueError):
        mrsm.Pipeline()
    with pytest.raises(ValueError):
        mrsm.Pipeline(mrsm.Pipe('sql:local', 'w8_x'), parents=-1)


def test_run_result_pickles_as_a_success_tuple(chain):
    import pickle
    raw, _, _ = chain
    run = raw.sync(children=1)
    assert pickle.loads(pickle.dumps(run)) == (run.success, run.message)


def test_cli_plan_is_pipeline_plan_expected(chain):
    """
    `mrsm plan pipes --nopretty` prints `Pipeline.plan(expected=True)`: provisional nodes
    re-planned as the executor will re-plan them once their parents have synced. The default
    `Pipeline.plan()` stays the static plan.
    """
    import os
    import subprocess
    import sys
    raw, clean, daily = chain
    pipeline = mrsm.Pipeline(daily, parents=True)
    first = pipeline.sync(yes=True)
    assert first.success, _why(first)
    ### A late reading before `raw`'s watermark: `raw` will write a ledger entry for its grid cell.
    _insert(_conn(), [('2024-01-01 01:30:00', 'c', 4)])

    static = pipeline.plan()
    expected = pipeline.plan(expected=True)
    assert static.node(clean).action == 'skip'
    assert expected.node(clean).action == 'backfill'
    assert expected.node(clean).raw['provisional'] is True

    conn = _conn()
    inst = str(conn)
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in ('MRSM_CONFIG', 'MRSM_PATCH')
    }
    env[f'MRSM_SQL_{conn.label.upper()}'] = f"sqlite:///{conn.database}"
    result = subprocess.run(
        [
            sys.executable, '-m', 'meerschaum', 'plan', 'pipes',
            '-c', inst, '-m', 'w8_daily', '--parents', '-i', inst, '--nopretty',
        ],
        capture_output=True,
        text=True,
        env=env,
        timeout=300,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    cli = json.loads(result.stdout.strip().splitlines()[-1])
    assert cli['nodes'] == expected.raw['plan']['nodes']
