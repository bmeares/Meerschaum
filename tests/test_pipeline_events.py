#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The live event stream of the Rust Syncing Engine in Python (design §5.7, wave 9):
`Pipeline.sync(on_event=...)` and `pipe.sync(parents=..., children=..., on_event=...)`.

Events are plain dicts delivered on the calling thread while the run executes on the engine's
threads (without the GIL); an `Exception` in the callback is warned about and the run
continues; `KeyboardInterrupt` cancels it. Everything is synthetic, on the SQLite test instance.
"""

import threading
import time
import uuid
from datetime import datetime, timezone

import pytest
import meerschaum as mrsm
from meerschaum import _core

from meerschaum.core.Pipeline import EVENT_KEYS, EVENT_NAMES
from tests.connectors import conns

pytestmark = pytest.mark.skipif(
    _core.load() is None,
    reason="The Syncing Engine needs the Rust extension (scripts/rust/build_py.sh).",
)


def _make_chain(suffix: str):
    conn = conns['sqlite']
    inst = str(conn)
    table = f"w9_events_readings_{suffix}"
    conn.exec_queries([f"DROP TABLE IF EXISTS {table}"], silent=True)
    conn.exec_queries(
        [f"CREATE TABLE {table} (ts TIMESTAMP, site TEXT, temp INTEGER)"],
        silent=True,
        break_on_error=True,
    )
    values = ', '.join(
        f"('2024-01-01 {hour:02d}:00:00', '{site}', {hour})"
        for hour in range(12)
        for site in ('a', 'b')
    )
    assert conn.exec_queries(
        [f"INSERT INTO {table} (ts, site, temp) VALUES {values}"],
        silent=True,
        break_on_error=True,
    ) is not None
    raw_metric, clean_metric = f"w9ev_raw_{suffix}", f"w9ev_clean_{suffix}"
    definitions = {
        raw_metric: f"SELECT * FROM {table}",
        clean_metric: (
            "SELECT ts, site, temp FROM "
            f"{{{{ Pipe('{inst}', '{raw_metric}', instance='{inst}') }}}} WHERE temp >= 0"
        ),
    }
    pipes = tuple(
        mrsm.Pipe(
            inst, metric, instance=conn,
            parameters={
                'columns': {'datetime': 'ts', 'id': 'site'},
                'dtypes': {'ts': 'datetime'},
                'fetch': {'definition': definition},
                ### Several window chunks for the child, so chunk events appear.
                'verify': {'chunk_minutes': 180},
            },
        )
        for metric, definition in definitions.items()
    )
    for pipe in pipes:
        pipe.delete()
        success, message = pipe.register()
        assert success, message

    def cleanup():
        for pipe in pipes:
            pipe.drop()
            pipe.delete()
        conn.exec_queries([f"DROP TABLE IF EXISTS {table}"], silent=True)

    return pipes, cleanup


@pytest.fixture
def chain():
    pipes, cleanup = _make_chain(uuid.uuid4().hex[:8])
    yield pipes
    cleanup()


def test_events_arrive_live_in_order_as_documented_dicts(chain):
    raw, clean = chain
    events = []
    threads = set()
    ticks = {'n': 0}
    stop = threading.Event()

    def spin():
        ### Needs the GIL to count: it only advances while the engine works without it.
        while not stop.is_set():
            ticks['n'] += 1
            time.sleep(0.005)

    spinner = threading.Thread(target=spin, daemon=True)
    spinner.start()
    seen_ticks = []

    def on_event(event):
        threads.add(threading.get_ident())
        seen_ticks.append(ticks['n'])
        events.append(event)

    try:
        run = mrsm.Pipeline(clean, parents=True).sync(on_event=on_event, workers=1)
    finally:
        stop.set()
        spinner.join()

    assert run.success, run.message
    assert run.callback_error is None
    assert threads == {threading.get_ident()}, "callbacks run on the calling thread"
    names = [e['event'] for e in events]
    assert names[0] == 'run_started'
    assert names[-1] == 'run_finished'
    assert [e['seq'] for e in events] == list(range(len(events)))
    for event in events:
        assert set(EVENT_KEYS) <= set(event), event
        assert event['event'] in EVENT_NAMES, event
        assert event['run_id'] == run.run_id
        assert isinstance(event['ts'], datetime) and event['ts'].tzinfo == timezone.utc
    raw_ref = '|'.join([str(raw.instance_keys), raw.connector_keys, raw.metric_key, 'None'])
    clean_ref = '|'.join([str(clean.instance_keys), clean.connector_keys, clean.metric_key, 'None'])
    for ref in (raw_ref, clean_ref):
        per_node = [e['event'] for e in events if e['pipe_ref'] == ref]
        assert 'node_planned' in per_node and 'node_started' in per_node, per_node
        assert per_node.index('node_started') < per_node.index('node_finished'), per_node
    finished = [e for e in events if e['event'] == 'node_finished']
    assert {e['pipe_ref'] for e in finished} == {raw_ref, clean_ref}
    assert all(e['result']['success'] for e in finished)
    assert sum(e['result']['inserted'] for e in finished if e['pipe_ref'] == raw_ref) == 24
    chunks = [e for e in events if e['event'] in ('chunk_started', 'chunk_finished')]
    assert chunks, names
    assert all('chunk' in e and 'source' in e for e in chunks)
    assert events[-1]['success'] is True and 'message' in events[-1]

    ### The run executed without the GIL: the spinner thread made progress during it.
    assert seen_ticks[-1] > seen_ticks[0], seen_ticks

    ### The live stream matches the result and the run history.
    assert run.run_id == events[0]['run_id']
    assert raw.get_rowcount() == 24 and clean.get_rowcount() == 24


def test_pipe_sync_with_a_traversal_accepts_on_event(chain):
    raw, clean = chain
    events = []
    run = raw.sync(children=True, on_event=events.append)
    assert run.success, run.message
    assert events and events[-1]['event'] == 'run_finished'
    assert {e['pipe_ref'] for e in events if e['event'] == 'node_finished'} == {
        '|'.join([str(p.instance_keys), p.connector_keys, p.metric_key, 'None'])
        for p in chain
    }


def test_a_failing_callback_is_reported_and_the_run_continues(chain):
    raw, clean = chain
    calls = []

    def on_event(event):
        calls.append(event['event'])
        if event['event'] == 'node_started':
            raise ValueError("progress bar exploded")

    with pytest.warns(UserWarning, match="progress bar exploded"):
        run = mrsm.Pipeline(clean, parents=True).sync(on_event=on_event)

    assert run.success, run.message
    assert isinstance(run.callback_error, ValueError)
    assert calls[-1] == 'node_started', "no event is delivered after the failure"
    assert calls.count('node_started') == 1
    assert any('on_event' in w and 'ValueError' in w for w in run.warnings), run.warnings
    assert raw.get_rowcount() == 24 and clean.get_rowcount() == 24


def test_keyboard_interrupt_in_the_callback_cancels_the_run():
    pipes, cleanup = _make_chain('kbd' + uuid.uuid4().hex[:6])
    raw, clean = pipes
    try:
        seen = []

        def on_event(event):
            seen.append(event['event'])
            if event['event'] == 'node_started':
                raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            mrsm.Pipeline(clean, parents=True).sync(on_event=on_event)
        assert 'run_finished' not in seen
        assert seen.count('node_started') == 1
    finally:
        cleanup()


def test_on_event_must_be_callable(chain):
    raw, clean = chain
    with pytest.raises(TypeError):
        mrsm.Pipeline(clean).sync(on_event='not a function')
