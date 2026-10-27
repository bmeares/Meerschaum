#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Python Pipeline API (rc15, Q11 / B3 / C7): `mrsm.Pipeline('name')` loads a saved Pipeline,
`save()` returns a `SuccessTuple` and refuses to overwrite a different saved Pipeline, `plan()`
and `.pipes` never register, the lifecycle methods (`exists`, `delete`, `rename`, `copy`,
`analyze`, `apply_analysis`, `schedule`), `mrsm.get_pipelines()`, the top-level exports and the
`RunResult` totals.

Everything is synthetic and runs on SQLite files of their own per test (`worker_sqlite`).
"""

import re
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import meerschaum as mrsm
from meerschaum.core.Pipeline import _pipe_ref

from tests._worker_sqlite import worker_sqlite


def _sqlite(monkeypatch, name):
    conn = worker_sqlite(name)
    monkeypatch.setenv(f'MRSM_SQL_{conn.label.upper()}', f'sqlite:///{conn.database}')
    return conn


@pytest.fixture
def instance(monkeypatch):
    """A SQLite instance and a unique token; drops every pipe and saved Pipeline afterwards."""
    token = uuid.uuid4().hex[:8]
    conn = _sqlite(monkeypatch, f'rc15_pipeline_api_{token}')
    yield conn, token
    for pipe in mrsm.get_pipes(instance=conn, as_list=True, connector_keys=['mrsm']):
        pipe.delete()
    for pipe in mrsm.get_pipes(instance=conn, as_list=True):
        pipe.drop()
        pipe.delete()


def _chain(conn, token):
    """`a -> b` (b reads a through `{{ Pipe() }}`), not registered yet."""
    inst = str(conn)
    a = mrsm.Pipe(
        inst, f'rc15_{token}_a', instance=conn,
        parameters={
            'columns': {'id': 'id'},
            'fetch': {'definition': 'SELECT 1 AS id, 2 AS value'},
        },
    )
    b = mrsm.Pipe(
        inst, f'rc15_{token}_b', instance=conn,
        parameters={
            'columns': {'id': 'id'},
            'fetch': {
                'definition': (
                    "SELECT id, value * 10 AS value FROM "
                    f"{{{{ Pipe('{inst}', 'rc15_{token}_a', instance='{inst}') }}}}"
                ),
            },
        },
    )
    return a, b


def _refs(pipes):
    return sorted(_pipe_ref(pipe) for pipe in pipes)


def test_a_name_loads_the_saved_pipeline(instance):
    conn, token = instance
    a, b = _chain(conn, token)
    name = f'rc15-{token}'
    success, message = mrsm.Pipeline([a, b], parents=1, instance=conn).save(name)
    assert success, message
    assert name in message

    for loaded in (
        mrsm.Pipeline(name, instance=conn),
        mrsm.Pipeline(name=name, instance=conn),
        mrsm.Pipeline([name], instance=conn),
        mrsm.Pipeline(name, name=name, instance=conn),
        mrsm.Pipeline.load(name, conn),
    ):
        assert loaded.name == name
        assert loaded.definition['name'] == name
        assert _refs(loaded.members) == _refs([a, b])
        assert loaded.parents == 1 and loaded.children is None
        assert loaded.graph is True
        assert loaded.exists()
        assert repr(loaded) == f"Pipeline({name!r}, instance={str(conn)!r})"

    ### Constructor depths override the saved ones for this object (`False` turns them off).
    assert mrsm.Pipeline(name, parents=True, instance=conn).parents is True
    assert mrsm.Pipeline(name, parents=False, instance=conn).parents is None


def test_a_missing_or_invalid_name_is_a_clear_error(instance):
    conn, token = instance
    with pytest.raises(ValueError, match="does not exist on"):
        mrsm.Pipeline('weather', instance=conn)
    with pytest.raises(ValueError, match="does not exist on"):
        mrsm.Pipeline(name='weather', instance=conn)
    with pytest.raises(ValueError, match="not the name of a saved Pipeline"):
        mrsm.Pipeline('sql:main', instance=conn)
    with pytest.raises(ValueError, match="Invalid pipe reference"):
        mrsm.Pipeline('sql:main|weather', instance=conn)
    with pytest.raises(ValueError, match="cannot be combined with a tags"):
        mrsm.Pipeline('weather', tags=['x'], instance=conn)
    with pytest.raises(ValueError, match="needs member pipes"):
        mrsm.Pipeline()
    with pytest.raises(TypeError):
        mrsm.Pipeline([object()], instance=conn)

    ### The error lists what is saved.
    a, _ = _chain(conn, token)
    assert mrsm.Pipeline(a, instance=conn).save(f'rc15-{token}')[0]
    with pytest.raises(ValueError, match=f"rc15-{token}"):
        mrsm.Pipeline('weather', instance=conn)
    ### The string is never split into characters.
    with pytest.raises(ValueError) as raised:
        mrsm.Pipeline('weather', instance=conn)
    assert "['w'" not in str(raised.value)


def test_pipe_refs_tuples_and_get_pipes_dicts_are_members(instance):
    conn, token = instance
    a, b = _chain(conn, token)
    by_ref = mrsm.Pipeline(_pipe_ref(a), instance=conn)
    assert _refs(by_ref.members) == _refs([a])
    assert by_ref.name is None and by_ref.definition is None

    mixed = mrsm.Pipeline([_pipe_ref(a), b], instance=conn)
    assert _refs(mixed.members) == _refs([a, b])

    keys = mrsm.Pipeline((a.connector_keys, a.metric_key), instance=conn)
    assert keys.members == [(a.connector_keys, a.metric_key)]

    assert a.register()[0] and b.register()[0]
    from_dict = mrsm.Pipeline(
        mrsm.get_pipes(metric_keys=[a.metric_key, b.metric_key], instance=conn),
        instance=conn,
    )
    assert _refs(from_dict.members) == _refs([a, b])


def test_combined_saved_pipelines(instance):
    conn, token = instance
    a, b = _chain(conn, token)
    for pipe in (a, b):
        assert pipe.register()[0]
    first, second = f'rc15-{token}-1', f'rc15-{token}-2'
    assert mrsm.Pipeline(a, instance=conn).save(first)[0]
    assert mrsm.Pipeline(b, instance=conn).save(second)[0]

    combined = mrsm.Pipeline([first, second], instance=conn)
    assert combined.name is None and combined.definition is None
    assert combined.graph is True
    assert _refs(combined.members) == _refs([a, b])
    plan = combined.plan()
    assert sorted(node.pipe_ref for node in plan.nodes) == _refs([a, b])
    assert (_pipe_ref(a), _pipe_ref(b)) in plan.edges
    run = combined.sync()
    assert run.success, run.message
    assert run.run_id.startswith('combined-')
    assert run.run_id in [row['run']['run_id'] for row in combined.get_runs()]
    ### The run belongs to both saved Pipelines' histories.
    for name in (first, second):
        assert run.run_id in [
            row['run']['run_id'] for row in mrsm.Pipeline(name, instance=conn).get_runs()
        ]

    ### Different saved traversal needs an explicit choice.
    assert mrsm.Pipeline(b, children=True, instance=conn).save(second, overwrite=True)[0]
    with pytest.raises(ValueError, match="different `children` settings"):
        mrsm.Pipeline([first, second], instance=conn)
    assert mrsm.Pipeline([first, second], children=False, instance=conn).children is None

    ### A combined selection saved under a new name.
    success, message = mrsm.Pipeline([first, a, b], name=f'rc15-{token}-3', instance=conn).save()
    assert success, message
    assert _refs(mrsm.Pipeline(f'rc15-{token}-3', instance=conn).members) == _refs([a, b])


def test_save_refuses_to_overwrite_a_different_pipeline(instance):
    conn, token = instance
    a, b = _chain(conn, token)
    name = f'rc15-{token}'
    success, message = mrsm.Pipeline([a, b], name=name, instance=conn).save()
    assert success, message
    revision = mrsm.Pipeline(name, instance=conn).definition['revision']

    ### The same selection again is a no-op success.
    success, message = mrsm.Pipeline([b, a], instance=conn).save(name)
    assert success, message
    assert mrsm.Pipeline(name, instance=conn).definition['revision'] == revision

    ### A different one under the same name is refused (B3: it used to overwrite silently).
    other = mrsm.Pipeline(a, name=name, instance=conn)
    success, message = other.save()
    assert not success
    assert 'already saved' in message and 'overwrite=True' in message
    assert _refs(mrsm.Pipeline(name, instance=conn).members) == _refs([a, b])
    assert other.definition is None

    success, message = other.save(overwrite=True)
    assert success, message
    saved = mrsm.Pipeline(name, instance=conn)
    assert _refs(saved.members) == _refs([a])
    assert saved.definition['revision'] == revision + 1
    assert other.definition['revision'] == revision + 1

    ### Saving without a name says how to name it.
    success, message = mrsm.Pipeline(a, instance=conn).save()
    assert not success and 'save(' in message
    success, message = mrsm.Pipeline(a, instance=conn).save('not a valid name')
    assert not success


def test_a_loaded_pipeline_saves_its_changes_with_a_revision_check(instance):
    conn, token = instance
    a, b = _chain(conn, token)
    name = f'rc15-{token}'
    assert mrsm.Pipeline(a, instance=conn).save(name)[0]

    first = mrsm.Pipeline(name, instance=conn)
    second = mrsm.Pipeline(name, instance=conn)
    first.members = first.members + [b]
    first.children = True
    success, message = first.save()
    assert success, message
    saved = mrsm.Pipeline(name, instance=conn)
    assert _refs(saved.members) == _refs([a, b]) and saved.children is True
    assert first.definition['revision'] == saved.definition['revision']

    ### Saving it again unchanged writes nothing (no new revision for other editors).
    success, message = saved.save()
    assert success, message
    assert mrsm.Pipeline(name, instance=conn).definition['revision'] == saved.definition['revision']

    ### The other copy is stale now.
    second.members = [b]
    success, message = second.save()
    assert not success and 'revision' in message
    assert _refs(mrsm.Pipeline(name, instance=conn).members) == _refs([a, b])

    ### Save as a new name carries the description and defaults.
    assert mrsm.Pipeline(name, instance=conn).save(
        description='Daily rollups', defaults={'workers': 2},
    )[0]
    loaded = mrsm.Pipeline(name, instance=conn)
    success, message = loaded.save(f'{name}-copy')
    assert success, message
    assert loaded.name == f'{name}-copy'
    copied = mrsm.Pipeline(f'{name}-copy', instance=conn)
    assert copied.definition['description'] == 'Daily rollups'
    assert copied.definition['defaults'] == {'workers': 2}
    assert copied.definition['id'] != mrsm.Pipeline(name, instance=conn).definition['id']

    with pytest.raises(ValueError, match="not the names"):
        loaded.members = [name]


def test_plan_and_pipes_never_register(instance):
    conn, token = instance
    a, b = _chain(conn, token)
    pipeline = mrsm.Pipeline([a, b], instance=conn)

    plan = pipeline.plan()
    assert a.get_id() is None and b.get_id() is None
    assert plan.unregistered == [a, b]
    assert plan.nodes == []
    assert sum('is not registered' in warning for warning in plan.warnings) == 2
    assert 'is not registered' in str(plan) and '`Pipeline.sync()` registers it' in str(plan)

    assert pipeline.pipes == [a, b]
    assert a.get_id() is None and b.get_id() is None

    ### Only `sync()` registers (as it always did).
    run = pipeline.sync()
    assert run.success, run.message
    assert a.get_id() is not None and b.get_id() is not None
    plan = pipeline.plan()
    assert plan.unregistered == []
    assert [node.pipe_ref for node in plan.nodes] == [_pipe_ref(a), _pipe_ref(b)]


def test_a_saved_member_that_is_not_registered_is_reported_not_registered(instance):
    conn, token = instance
    a, b = _chain(conn, token)
    name = f'rc15-{token}'
    pipeline = mrsm.Pipeline([a, b], instance=conn)
    success, message = pipeline.save(name)
    assert success, message
    assert 'not registered yet' in message

    ### The object that holds the pipes still registers them on `sync()`.
    run = pipeline.sync()
    assert run.success, run.message

    assert b.delete()[0]
    loaded = mrsm.Pipeline(name, instance=conn)
    plan = loaded.plan()
    assert [pipe.metric_key for pipe in plan.unregistered] == [b.metric_key]
    assert f"syncing Pipeline '{name}'" in str(plan)
    run = loaded.sync()
    assert not run.success
    assert 'not registered' in run.message
    assert mrsm.Pipe(b.connector_keys, b.metric_key, instance=conn).get_id() is None


def test_name_does_not_switch_graph_behavior(instance):
    conn, token = instance
    a, b = _chain(conn, token)
    for pipe in (a, b):
        assert pipe.register()[0]

    flat = mrsm.Pipeline([a, b], name=f'rc15-{token}', instance=conn)
    assert flat.graph is False
    plan = flat.plan()
    assert plan.flat
    assert {node.action for node in plan.nodes} == {'root'}

    graph = mrsm.Pipeline([a, b], name=f'rc15-{token}', graph=True, instance=conn)
    plan = graph.plan()
    assert not plan.flat
    assert plan.node(b).action != 'root'

    ### Saved, it respects the dependencies between its members (like `sync pipelines NAME`).
    assert flat.save()[0]
    assert flat.graph is True
    assert not mrsm.Pipeline(f'rc15-{token}', instance=conn).plan().flat
    assert mrsm.Pipeline(f'rc15-{token}', graph=False, instance=conn).plan().flat


def test_get_pipelines_exists_delete_rename_copy(instance, monkeypatch):
    conn, token = instance
    other = _sqlite(monkeypatch, f'rc15_pipeline_api_other_{token}')
    a, b = _chain(conn, token)
    for pipe in (a, b):
        assert pipe.register()[0]
    name = f'rc15-{token}'
    pipeline = mrsm.Pipeline([a, b], instance=conn)
    assert not pipeline.exists()
    assert pipeline.save(name)[0]
    assert pipeline.exists()

    pipelines = mrsm.get_pipelines(conn)
    assert [p.name for p in pipelines] == [name]
    assert isinstance(pipelines[0], mrsm.Pipeline)
    assert pipelines[0].definition['targets'] == _refs([a, b])
    assert mrsm.get_pipelines(other) == []

    success, message = pipeline.rename(f'{name}-renamed')
    assert success, message
    assert pipeline.name == f'{name}-renamed'
    assert [p.name for p in mrsm.get_pipelines(conn)] == [f'{name}-renamed']
    assert mrsm.Pipeline(f'{name}-renamed', instance=conn).definition['id'] == pipeline.definition['id']

    success, message = pipeline.copy(f'{name}-copy')
    assert success, message
    success, message = pipeline.copy(f'{name}-copy')
    assert not success and 'already exists' in message
    success, message = pipeline.copy(name, instance=other)
    assert success, message
    elsewhere = mrsm.Pipeline(name, instance=other)
    ### The members keep their own instance.
    assert _refs(elsewhere.members) == _refs([a, b])
    assert sorted(p.name for p in mrsm.get_pipelines(conn)) == [f'{name}-copy', f'{name}-renamed']

    success, message = pipeline.delete()
    assert success, message
    assert 'kept' in message
    assert not pipeline.exists() and pipeline.definition is None
    assert a.get_id() is not None and b.get_id() is not None
    success, message = pipeline.delete()
    assert not success
    success, message = pipeline.rename('anything')
    assert not success
    assert elsewhere.delete()[0]


def test_analyze_and_apply_analysis(instance):
    conn, token = instance
    a, b = _chain(conn, token)
    for pipe in (a, b):
        assert pipe.register()[0]
    name = f'rc15-{token}'
    pipeline = mrsm.Pipeline(a, children=True, instance=conn)
    assert pipeline.save(name)[0]

    proposal = pipeline.analyze()
    assert not proposal['errors'], proposal
    assert {row['pipe_ref'] for row in proposal['resolved']} == set(_refs([a, b]))
    assert {change['tag'] for change in proposal['changes']} == {f'pipeline-{name}'}
    ### Analysis changes nothing.
    assert f'pipeline-{name}' not in (mrsm.Pipe(a.connector_keys, a.metric_key, instance=conn).tags or [])

    success, message = pipeline.apply_analysis(proposal)
    assert success, message
    for pipe in (a, b):
        fresh = mrsm.Pipe(pipe.connector_keys, pipe.metric_key, instance=conn)
        assert f'pipeline-{name}' in fresh.tags
    assert pipeline.definition['tag_associations']

    ### Without a proposal: analyze and apply now (nothing left to change).
    success, message = pipeline.apply_analysis()
    assert success, message

    with pytest.raises(ValueError, match='Save the Pipeline'):
        mrsm.Pipeline(a, instance=conn).analyze()


def test_schedule_returns_the_job(instance, monkeypatch):
    conn, token = instance
    a, _ = _chain(conn, token)
    assert a.register()[0]
    name = f'rc15-{token}'
    pipeline = mrsm.Pipeline(a, instance=conn)
    with pytest.raises(ValueError, match='Save the Pipeline'):
        pipeline.schedule('every 10 minutes')
    assert pipeline.save(name)[0]

    jobs = {}
    started = []

    class FakeJob:
        def __init__(self, job_name, sysargs=None, executor_keys=None):
            existing = jobs.get(job_name, None)
            self.name = job_name
            self.sysargs = existing.sysargs if existing is not None else list(sysargs or [])
            self.executor_keys = executor_keys

        def exists(self):
            return self.name in jobs

        def is_running(self):
            return False

        def start(self):
            started.append(self.name)
            jobs[self.name] = self
            return True, 'started'

    monkeypatch.setattr(mrsm, 'Job', FakeJob)
    job = pipeline.schedule('every 10 minutes', workers=2)
    assert isinstance(job, FakeJob)
    assert job.name == f'pipeline-{name}'
    assert job.sysargs[:3] == ['sync', 'pipeline', name]
    assert job.sysargs[job.sysargs.index('-i') + 1] == str(conn)
    assert job.sysargs[job.sysargs.index('--schedule') + 1] == 'every 10 minutes'
    assert job.sysargs[job.sysargs.index('--workers') + 1] == '2'
    assert started == [f'pipeline-{name}']

    ### The same schedule again returns the existing job.
    assert pipeline.schedule('every 10 minutes', workers=2).name == job.name
    ### Another command under the same job name is refused.
    with pytest.raises(ValueError, match='already exists with a different command'):
        pipeline.schedule('every 5 minutes')
    other = pipeline.schedule('every 5 minutes', name=f'{name}-fast', start=False)
    assert other.name == f'{name}-fast' and f'{name}-fast' not in started
    with pytest.raises(ValueError):
        pipeline.schedule('', name=f'{name}-empty')
    with pytest.raises(ValueError):
        pipeline.schedule('whenever you like', name=f'{name}-bad')

    def fail(self):
        return False, 'executor unavailable'

    monkeypatch.setattr(FakeJob, 'start', fail)
    with pytest.raises(RuntimeError, match='executor unavailable'):
        pipeline.schedule('every 5 minutes', name=f'{name}-down')


def test_run_result_totals_and_events(instance):
    conn, token = instance
    a, b = _chain(conn, token)
    live = []
    run = mrsm.Pipeline([a, b], graph=True, instance=conn).sync(on_event=live.append)
    assert run.success, run.message
    success, message = run
    assert success is True and message == run.message
    assert run.inserted == 2 and run.updated == 0 and run.upserted == 0 and run.deleted == 0
    assert run.duration > 0
    assert run.to_dict()['inserted'] == 2

    ### Nothing new: the totals are zero, not missing.
    again = mrsm.Pipeline([a, b], graph=True, instance=conn).sync()
    assert again.success, again.message
    assert again.inserted == 0 and again.updated == 0

    ### A run that never started has no totals.
    assert mrsm.RunResult(False, 'nope').inserted is None

    events = run.events()
    assert events[0]['event'] == 'run_started' and events[-1]['event'] == 'run_finished'
    for event in events:
        assert {'ts', 'run_id', 'seq', 'pipe_ref', 'event', 'payload'} <= set(event)
        for key, value in (event['payload'] or {}).items():
            assert key in event
            if key not in ('ts', 'run_id', 'seq', 'pipe_ref', 'event'):
                assert event[key] == value
    started = events[0]
    assert started['nodes'] == 2 and started['targets']
    finished = next(event for event in events if event['event'] == 'node_finished')
    assert finished['result']['inserted'] == 1
    assert isinstance(started['ts'], str)

    ### A node's `duration` is float seconds everywhere Python shows it (the engine's JSON
    ### spells Rust's `Duration` as `{'secs', 'nanos'}`), equal to the node row's seconds.
    node_seconds = {node['pipe_ref']: node['duration_seconds'] for node in run.record['nodes']}
    assert set(node_seconds) == {_pipe_ref(a), _pipe_ref(b)}
    assert len(run.outcomes) == 2
    for outcome in run.outcomes:
        duration = outcome['result']['duration']
        assert isinstance(duration, float) and duration > 0, outcome
        assert duration == node_seconds[outcome['pipe_ref']]
    live_finished = [event for event in live if event['event'] == 'node_finished']
    persisted_finished = [event for event in events if event['event'] == 'node_finished']
    assert len(live_finished) == len(persisted_finished) == 2
    for event in live_finished + persisted_finished:
        assert event['result']['duration'] == node_seconds[event['pipe_ref']], event
    for event in persisted_finished:
        assert event['payload']['result']['duration'] == event['result']['duration']


def test_node_durations_are_seconds_without_mutating_the_engine_data():
    from meerschaum.core.Pipeline import _decode_events, decode_event
    engine_result = {'success': True, 'inserted': 1, 'duration': {'secs': 1, 'nanos': 500_000_000}}
    data = {
        'run_id': 'r1',
        'duration_seconds': 2.0,
        'outcomes': [
            {'pipe_ref': 'i|c|m|None', 'pipe': 'Pipe(c, m)', 'result': engine_result},
            {'pipe_ref': 'i|c|n|None', 'pipe': 'Pipe(c, n)', 'result': {'duration': None}},
            {'pipe_ref': 'i|c|o|None', 'pipe': 'Pipe(c, o)', 'result': {'duration': 0.25}},
        ],
    }
    run = mrsm.RunResult(True, 'ok', data)
    assert [o['result']['duration'] for o in run.outcomes] == [1.5, None, 0.25]
    assert run.outcomes[0]['result']['inserted'] == 1
    assert engine_result['duration'] == {'secs': 1, 'nanos': 500_000_000}
    assert run.duration == 2.0 and run.to_dict()['duration'] == 2.0

    import pickle
    assert pickle.loads(pickle.dumps(run)).outcomes[0]['result']['duration'] == 1.5

    text = (
        '{"run_id": "r1", "seq": 3, "ts": "2026-01-01T00:00:00Z", "pipe_ref": "i|c|m|None", '
        '"event": "node_finished", "result": {"success": true, '
        '"duration": {"secs": 0, "nanos": 49133}}}'
    )
    live = decode_event(text)
    assert live['result']['duration'] == 49133 / 1e9
    assert live['result']['success'] is True

    persisted = _decode_events([{
        'ts': '2026-01-01T00:00:00Z', 'run_id': 'r1', 'seq': 3, 'pipe_ref': 'i|c|m|None',
        'event': 'node_finished',
        'payload': '{"result": {"duration": {"secs": 2, "nanos": 0}}}',
    }])[0]
    assert persisted['result']['duration'] == 2.0
    assert persisted['payload']['result']['duration'] == 2.0

    ### Other events and other dictionaries are left alone.
    chunk = decode_event(
        '{"run_id": "r1", "seq": 1, "ts": "2026-01-01T00:00:00Z", "pipe_ref": null, '
        '"event": "chunk_finished", "chunk": 0, "duration_ms": 12}'
    )
    assert chunk['duration_ms'] == 12
    odd = decode_event(
        '{"run_id": "r1", "seq": 2, "ts": "2026-01-01T00:00:00Z", "pipe_ref": null, '
        '"event": "node_finished", "result": {"duration": {"secs": 1, "nanos": 0, "x": 1}}}'
    )
    assert odd['result']['duration'] == {'secs': 1, 'nanos': 0, 'x': 1}


def test_named_pipeline_runs_and_get_runs_filters(instance):
    conn, token = instance
    a, b = _chain(conn, token)
    name = f'rc15-{token}'
    pipeline = mrsm.Pipeline([a, b], instance=conn)
    assert pipeline.save(name)[0]
    run = pipeline.sync()
    assert run.success, run.message
    assert re.match(re.escape(name) + r'-\d{4}-', run.run_id)
    unnamed = mrsm.Pipeline(a, instance=conn).sync()
    assert unnamed.success, unnamed.message

    assert [row['run']['run_id'] for row in pipeline.get_runs()] == [run.run_id]
    assert [row['run']['run_id'] for row in mrsm.get_runs(conn, pipeline=name)] == [run.run_id]
    assert [row['run']['run_id'] for row in mrsm.get_runs(conn, pipeline=pipeline)] == [run.run_id]
    by_pipe = [row['run']['run_id'] for row in mrsm.get_runs(conn, pipe=a)]
    assert run.run_id in by_pipe and unnamed.run_id in by_pipe
    assert unnamed.run_id in [row['run']['run_id'] for row in pipeline.get_runs(all_runs=True)]
    assert [row['run']['run_id'] for row in mrsm.get_runs(conn, limit=1)] == [unnamed.run_id]

    ### `since`: runs started at or after a time (a datetime, a timedelta or ISO text).
    recent = {row['run']['run_id'] for row in mrsm.get_runs(conn, since=timedelta(hours=1))}
    assert {run.run_id, unnamed.run_id} <= recent
    assert {row['run']['run_id'] for row in mrsm.get_runs(conn, since='2000-01-01')} == recent
    later = datetime.now(timezone.utc) + timedelta(minutes=10)
    assert mrsm.get_runs(conn, since=later) == []
    assert mrsm.get_runs(conn, since=later.replace(tzinfo=None)) == []
    assert pipeline.get_runs(since=later) == []
    assert [row['run']['run_id'] for row in pipeline.get_runs(since=timedelta(hours=1))] == [
        run.run_id
    ]
    with pytest.raises(ValueError, match='since'):
        mrsm.get_runs(conn, since='not a time')


def test_since_is_sent_as_utc_iso_text():
    from datetime import date
    from meerschaum.core.Pipeline._engine import since_text
    assert since_text(None) is None and since_text('') is None
    assert since_text(datetime(2024, 1, 2, 3, 4, 5)) == '2024-01-02T03:04:05+00:00'
    plus_two = timezone(timedelta(hours=2))
    assert since_text(datetime(2024, 1, 2, 5, 4, 5, tzinfo=plus_two)) == '2024-01-02T03:04:05+00:00'
    assert since_text('2024-01-02 03:04:05') == '2024-01-02T03:04:05+00:00'
    assert since_text(date(2024, 1, 2)) == '2024-01-02T00:00:00+00:00'
    ago = datetime.fromisoformat(since_text(timedelta(hours=1)))
    assert abs((datetime.now(timezone.utc) - ago) - timedelta(hours=1)) < timedelta(minutes=1)
    for invalid in (True, 5, 'not a time', object()):
        with pytest.raises(ValueError):
            since_text(invalid)


def test_top_level_exports():
    import meerschaum
    from meerschaum.core import Pipeline as package
    assert mrsm.Pipeline is package.Pipeline
    assert mrsm.Plan is package.Plan
    assert mrsm.RunResult is package.RunResult
    assert mrsm.get_runs is package.get_runs
    assert mrsm.get_pipelines is package.get_pipelines
    for name in ('Pipeline', 'Plan', 'RunResult', 'get_runs', 'get_pipelines', 'make_executor'):
        assert name in meerschaum.__all__, name
    assert len(meerschaum.__all__) == len(set(meerschaum.__all__))
