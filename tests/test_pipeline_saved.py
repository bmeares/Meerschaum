"""Named pipelines preserve exact membership and stay out of the data graph."""
import re
import uuid

import pytest
import meerschaum as mrsm
from meerschaum import _core
from meerschaum.core.Pipeline import _pipe_ref
from meerschaum.core.Pipeline._saved import (
    delete_pipeline, job_arguments, list_pipelines, load_pipeline, save_pipeline,
)
from tests._worker_sqlite import worker_sqlite

pytestmark = pytest.mark.skipif(_core.load() is None, reason='Requires the built Syncing Engine.')


@pytest.fixture
def saved_members(monkeypatch):
    token = uuid.uuid4().hex[:8]
    conn = worker_sqlite(f'rc4_saved_pipeline_{token}')
    monkeypatch.setenv(f'MRSM_SQL_{conn.label.upper()}', f'sqlite:///{conn.database}')
    monkeypatch.setenv('PYTHONPATH', str(__import__('pathlib').Path(__file__).resolve().parents[1]))
    pipes = [
        mrsm.Pipe(str(conn), f'rc4_{token}_{metric}', location, instance=conn,
                  parameters={'columns': {'id': 'id'},
                              'fetch': {'definition': 'SELECT 1 AS id, 2 AS value'}})
        for metric in ('a', 'b') for location in ('east', 'west')
    ]
    for pipe in pipes:
        assert pipe.register()[0]
    name = f'rc4-{token}'
    yield conn, name, pipes
    delete_pipeline(name, conn)
    for pipe in mrsm.get_pipes(connector_keys=['mrsm'], instance=conn, as_list=True):
        if pipe.metric_key in ('pipeline', 'pipeline_registry'):
            pipe.delete()
    for pipe in pipes:
        pipe.delete()


def test_saved_pipeline_roundtrip_exact_membership(saved_members):
    conn, name, pipes = saved_members
    # A keys filter's Cartesian product would incorrectly include the two other pipes.
    members = [pipes[0], pipes[3]]
    expected = {_pipe_ref(pipe) for pipe in members}
    definition = save_pipeline(name, members, conn, parents=1)
    assert set(definition['targets']) == expected
    assert list_pipelines(conn)[name] == definition
    loaded = load_pipeline(name, conn)
    assert loaded.name == name and loaded.parents == 1
    assert {_pipe_ref(pipe) for pipe in loaded.targets} == expected
    assert {node.pipe_ref for node in loaded.plan().nodes} == expected

    # Edits replace membership in place; a Job must see the updated selection next lap.
    save_pipeline(name, [pipes[1]], conn)
    assert {_pipe_ref(pipe) for pipe in load_pipeline(name, conn).targets} == {_pipe_ref(pipes[1])}
    assert delete_pipeline(name, conn)[0]
    assert name not in list_pipelines(conn)
    assert all(pipe.get_id() is not None for pipe in pipes)


def test_saved_pipeline_access_is_atomic_and_copy_rename_keep_their_metadata(saved_members, monkeypatch):
    """A Pipeline starts with access; copying resets access, and renaming preserves it."""
    from copy import deepcopy
    from meerschaum.core.Pipeline._saved import copy_pipeline, rename_pipeline, registration
    conn, name, pipes = saved_members
    original = mrsm.Pipe.register
    captured = []
    def record(pipe, *args, **kw):
        if pipe.connector_keys == 'mrsm' and pipe.metric_key == 'pipeline':
            captured.append(deepcopy(dict(pipe.parameters)))
        return original(pipe, *args, **kw)
    monkeypatch.setattr(mrsm.Pipe, 'register', record)
    source_access = {'owner': 'alice', 'team': 'data', 'visibility': 'private', 'edit': 'owner'}
    copy_access = {'owner': 'bob', 'team': 'ops', 'visibility': 'team', 'edit': 'team'}
    names = [name + suffix for suffix in ('-copy', '-claimed', '-renamed')]
    try:
        save_pipeline(name, pipes[:1], conn, access=source_access, locks=['sync'])
        assert captured[0]['access'] == source_access
        assert captured[0]['locks'] == ['sync']
        copy_pipeline(name, names[0], conn)
        plain = registration(names[0], conn)
        assert plain.owner is None and plain.team == 'Everyone' and plain.locks == ['sync']
        copy_pipeline(name, names[1], conn, access=copy_access)
        claimed = registration(names[1], conn)
        assert claimed.access == copy_access and claimed.locks == ['sync']
        rename_pipeline(name, names[2], conn)
        renamed = registration(names[2], conn)
        assert renamed.access == source_access and renamed.locks == ['sync']
        assert registration(name, conn).get_id() is None
    finally:
        for candidate in names:
            delete_pipeline(candidate, conn)


@pytest.mark.parametrize('lock', ['parameters', 'delete', 'drop'])
def test_rename_locked_pipeline_refuses_before_creating_another_registration(saved_members, lock):
    from meerschaum.core.Pipeline._saved import rename_pipeline, registration
    conn, name, pipes = saved_members
    save_pipeline(name, pipes[:1], conn)
    source = registration(name, conn)
    assert source.lock(lock)[0]
    try:
        with pytest.raises(ValueError, match='locked'):
            rename_pipeline(name, name + '-renamed', conn)
        assert registration(name + '-renamed', conn).get_id() is None
        assert source.get_id() is not None
    finally:
        source.unlock(lock)


def test_saved_pipeline_metadata_not_in_engine_graph(saved_members):
    conn, name, pipes = saved_members
    save_pipeline(name, pipes[:2], conn)
    from meerschaum.api.dash.engine import plan_graph
    graph = plan_graph(conn)
    assert graph['nodes']
    assert all(node['pipe_ref'].split('|')[1] != 'mrsm' for node in graph['nodes'])


def test_saved_pipeline_run_id_and_execution(saved_members):
    conn, name, pipes = saved_members
    pipeline = mrsm.Pipeline([pipes[0], pipes[3]], instance=conn)
    assert pipeline.save(name)[0]
    result = mrsm.Pipeline.load(name, conn).sync(force=True, yes=True)
    assert result.success, result.message
    assert re.fullmatch(re.escape(name) + r'-\d{4}-\d\d-\d\dT\d\d-\d\d-\d\d\.\d{6}Z-[a-f0-9]{8}', result.run_id)
    assert pipes[0].get_rowcount() == 1 and pipes[3].get_rowcount() == 1
    assert not pipes[1].exists() and not pipes[2].exists()



def test_saved_pipeline_accepts_supported_key_tuples(saved_members):
    conn, name, pipes = saved_members
    target = pipes[0]
    pipeline = mrsm.Pipeline([(target.connector_keys, target.metric_key, target.location_key)],
                             instance=conn)
    success, message = pipeline.save(name)
    assert success, message
    definition = pipeline.definition
    assert definition['targets'] == [_pipe_ref(target)]
    assert {_pipe_ref(pipe) for pipe in mrsm.Pipeline.load(name, conn).targets} == {_pipe_ref(target)}


def test_pipeline_job_arguments_are_native_and_validated():
    args = job_arguments('daily-totals', 'sql:main', 'every 5 minutes', workers=2,
                         retries=3, force=True)
    assert args[:3] == ['sync', 'pipeline', 'daily-totals']
    assert args[args.index('--schedule') + 1] == 'every 5 minutes'
    assert args[args.index('--workers') + 1] == '2'
    assert args[args.index('--retries') + 1] == '3'
    assert '--force' in args
    for options in ({'schedule': ''}, {'schedule': 'daily', 'workers': 0},
                    {'schedule': 'daily', 'retries': -1}):
        with pytest.raises(ValueError):
            job_arguments('daily-totals', 'sql:main', **options)


def test_existing_components_are_not_registered_on_read(saved_members):
    from meerschaum.core.Pipeline._saved import initialize_pipelines
    conn, _, pipes = saved_members
    refs = [_pipe_ref(pipe) for pipe in pipes]
    graph = {'nodes': [{'pipe_ref': ref} for ref in refs],
             'edges': [{'parent': refs[0], 'child': refs[1]}]}
    assert initialize_pipelines(graph, conn) == {}
    assert list_pipelines(conn) == {}


def test_native_saved_pipeline_command_uses_exact_membership(saved_members):
    conn, name, pipes = saved_members
    save_pipeline(name, [pipes[0], pipes[3]], conn)
    success, message = mrsm.entry(['sync', 'pipeline', name, '-i', str(conn),
                                   '--force', '--nopretty', '-y'])
    assert success, message
    assert pipes[0].get_rowcount() == 1 and pipes[3].get_rowcount() == 1
    assert not pipes[1].exists() and not pipes[2].exists()


def test_scheduled_saved_pipeline_resolves_changed_membership(saved_members, tmp_path):
    """A running scheduled action reads the saved targets again on subsequent laps."""
    import os
    import signal
    import subprocess
    import sys
    import time
    conn, name, pipes = saved_members
    save_pipeline(name, [pipes[0]], conn)
    args = ['sync', 'pipeline', name, '-i', str(conn), '--force', '--nopretty', '-y',
            '--schedule', 'every 1 second']
    child_env = dict(os.environ)
    child_env.pop('TMUX', None)
    child_env.pop('TMUX_PANE', None)
    log_path = tmp_path / 'schedule.log'
    with log_path.open('w+') as log:
        child = subprocess.Popen([sys.executable, '-c',
                                  'import meerschaum as m\ntry:\n m.entry(' + repr(args) + ')\nexcept KeyboardInterrupt:\n pass'],
                                 stdout=log, stderr=log, start_new_session=True, env=child_env)
        try:
            deadline = time.monotonic() + 15
            while not pipes[0].exists() and child.poll() is None and time.monotonic() < deadline:
                time.sleep(.1)
            assert pipes[0].exists(), 'First scheduled selection did not sync.'
            save_pipeline(name, [pipes[3]], conn)
            while not pipes[3].exists() and child.poll() is None and time.monotonic() < deadline:
                time.sleep(.1)
            assert pipes[3].exists(), 'Scheduler did not resolve updated saved membership.'
            assert not pipes[1].exists() and not pipes[2].exists()
            while log_path.read_text().count('Run: ' + name) < 2 and time.monotonic() < deadline:
                time.sleep(.1)
        finally:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGINT)
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait(timeout=5)
            log.seek(0)
            output = log.read()
        assert child.returncode in (0, -signal.SIGINT, 128 + signal.SIGINT), output
        assert output.count('Run: ' + name) >= 2, output


def test_derivative_job_settings_preserve_options_and_avoid_name_collisions():
    from meerschaum.core.Pipeline._saved import derivative_job_settings
    args = job_arguments('daily', 'sql:main', 'every 10 minutes', workers=3, retries=2, force=True)
    original = list(args)
    settings = derivative_job_settings('daily-worker', args, ['daily-worker', 'daily-worker-2'])
    assert settings == {'name': 'daily-worker-3', 'schedule': 'every 10 minutes',
                        'workers': 3, 'retries': 2, 'force': True}
    assert args == original


@pytest.mark.parametrize('entry', ['python', 'native'])
def test_saved_pipeline_preserves_induced_dependency_order(saved_members, entry):
    conn, name, pipes = saved_members
    root, child = pipes[0], pipes[3]
    child.parameters['fetch']['definition'] = f"SELECT * FROM {{{{ {root} }}}}"
    assert child.edit(interactive=False)[0]
    save_pipeline(name, [child, root], conn)
    loaded = load_pipeline(name, conn)
    plan = loaded.plan()
    edge = (_pipe_ref(root), _pipe_ref(child))
    assert edge in plan.edges
    assert not plan.flat
    assert {node.pipe_ref for node in plan.nodes} == {_pipe_ref(root), _pipe_ref(child)}
    from meerschaum.api.dash.engine import plan_graph
    graph = plan_graph(conn, loaded.targets)
    assert {'parent': edge[0], 'child': edge[1]} in [
        {'parent': e['parent'], 'child': e['child']} for e in graph['edges']]
    if entry == 'python':
        result = loaded.sync(force=True, yes=True, workers=2)
        assert result.success, result.message
    else:
        success, message = mrsm.entry(['sync', 'pipeline', name, '-i', str(conn),
                                       '--force', '--workers', '2', '--retries', '1',
                                       '--nopretty', '-y'])
        assert success, message
    assert child.get_rowcount() == 1
    assert not pipes[1].exists() and not pipes[2].exists()


def test_analysis_is_read_only_and_stale_proposals_require_reapproval(saved_members):
    from meerschaum.core.Pipeline._saved import analyze_pipeline, apply_pipeline_analysis
    conn, name, pipes = saved_members
    original = save_pipeline(name, [pipes[0]], conn)
    proposal = analyze_pipeline(name, conn)
    assert not proposal['errors']
    assert proposal['changes'][0]['tag'] == 'pipeline-' + name
    assert 'pipeline-' + name not in mrsm.Pipe(pipes[0].connector_keys, pipes[0].metric_key,
                                              pipes[0].location_key, instance=conn).tags
    save_pipeline(name, [pipes[1]], conn, expected_revision=original['revision'])
    stale = apply_pipeline_analysis(proposal, conn)
    assert stale['stale'] and not stale['success']
    fresh = stale['proposal']
    assert apply_pipeline_analysis(fresh, conn)['success']
    refreshed = mrsm.Pipe(pipes[1].connector_keys, pipes[1].metric_key,
                         pipes[1].location_key, instance=conn)
    assert 'pipeline-' + name in refreshed.tags
    with pytest.raises(ValueError, match='revision'):
        save_pipeline(name, [pipes[0]], conn, expected_revision=original['revision'])


def test_preexisting_tag_and_other_owner_survive_deletion(saved_members, monkeypatch):
    from meerschaum.core.Pipeline._saved import analyze_pipeline, apply_pipeline_analysis
    conn, name, pipes = saved_members
    other = worker_sqlite('rc6_other_' + uuid.uuid4().hex[:8])
    monkeypatch.setenv(f'MRSM_SQL_{other.label.upper()}', f'sqlite:///{other.database}')
    pipe = pipes[0]
    tag = 'pipeline-' + name
    pipe.tags = [tag]
    assert pipe.edit(interactive=False)[0]
    first = save_pipeline(name, [pipe], conn)
    second = save_pipeline(name, [pipe], other)
    try:
        assert first['id'] != second['id']
        assert apply_pipeline_analysis(analyze_pipeline(name, conn), conn)['success']
        assert apply_pipeline_analysis(analyze_pipeline(name, other), other)['success']
        assert delete_pipeline(name, conn)[0]
        fresh = mrsm.Pipe(pipe.connector_keys, pipe.metric_key, pipe.location_key, instance=conn)
        assert fresh.parameters['_pipeline_tags'][tag]['owners'] == [second['id']]
        assert delete_pipeline(name, other)[0]
        fresh = mrsm.Pipe(pipe.connector_keys, pipe.metric_key, pipe.location_key, instance=conn)
        assert tag in fresh.tags
    finally:
        delete_pipeline(name, other)


def test_concurrent_owners_keep_both_associations(saved_members):
    from concurrent.futures import ThreadPoolExecutor
    from meerschaum.core.Pipeline._saved import _change_tag
    conn, _, pipes = saved_members
    pipe = pipes[0]
    tag = 'pipeline-concurrent'
    def add(owner):
        fresh = mrsm.Pipe(pipe.connector_keys, pipe.metric_key, pipe.location_key, instance=conn)
        return _change_tag(fresh, tag, owner, True)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(add, ['one', 'two']))
    assert all(result[0] for result in results)
    fresh = mrsm.Pipe(pipe.connector_keys, pipe.metric_key, pipe.location_key, instance=conn)
    assert set(fresh.parameters['_pipeline_tags'][tag]['owners']) == {'one', 'two'}


def test_missing_member_is_never_registered_by_saved_plan(saved_members):
    from meerschaum.core.Pipeline._saved import analyze_pipeline
    conn, name, _ = saved_members
    missing = mrsm.Pipe('sql:missing-source', 'unregistered', instance=conn)
    save_pipeline(name, [missing], conn)
    assert analyze_pipeline(name, conn)['errors']
    assert missing.get_id() is None


def test_empty_saved_draft_does_not_select_every_pipe(saved_members):
    conn, name, _ = saved_members
    save_pipeline(name, [], conn)
    assert load_pipeline(name, conn).plan().nodes == []


def test_copy_and_rename_identity_layout_and_tag_cleanup(saved_members):
    from meerschaum.core.Pipeline._saved import (
        analyze_pipeline, apply_pipeline_analysis, copy_pipeline, rename_pipeline,
    )
    conn, name, pipes = saved_members
    definition = save_pipeline(name, pipes[:1], conn, layout={'positions': {'a': [10, 20]}})
    assert apply_pipeline_analysis(analyze_pipeline(name, conn), conn)['success']
    copy_name, new_name = name + '-copy', name + '-renamed'
    try:
        copied = copy_pipeline(name, copy_name, conn)
        assert copied['id'] != definition['id'] and copied['layout'] == definition['layout']
        renamed = rename_pipeline(name, new_name, conn, definition['revision'])
        assert renamed['id'] == definition['id']
        assert name not in list_pipelines(conn)
        fresh = mrsm.Pipe(pipes[0].connector_keys, pipes[0].metric_key, pipes[0].location_key, instance=conn)
        assert 'pipeline-' + name not in fresh.tags
    finally:
        delete_pipeline(copy_name, conn)
        delete_pipeline(new_name, conn)


def test_tag_reconciliation_preserves_raw_sql_and_nested_parameters(saved_members):
    from meerschaum.core.Pipeline._saved import analyze_pipeline, apply_pipeline_analysis
    conn, name, pipes = saved_members
    child, parent = pipes[0], pipes[1]
    raw = f"SELECT * FROM {{{{ Pipe('{parent.connector_keys}', '{parent.metric_key}', '{parent.location_key}') }}}}"
    child.parameters = {'columns': {'id': 'id'}, 'fetch': {'definition': raw},
                        'nested': {'literal': "{{ Pipe().target }}"}}
    assert child.edit()[0]
    save_pipeline(name, [child], conn, parents=True)
    proposal = analyze_pipeline(name, conn)
    assert not proposal['errors'], proposal
    assert {row['pipe_ref'] for row in proposal['resolved']} == {_pipe_ref(child), _pipe_ref(parent)}
    assert apply_pipeline_analysis(proposal, conn)['success']
    fresh = mrsm.Pipe(child.connector_keys, child.metric_key, child.location_key, instance=conn)
    params = fresh.get_parameters(apply_symlinks=False)
    assert params['fetch']['definition'] == raw
    assert params['nested']['literal'] == "{{ Pipe().target }}"
    assert delete_pipeline(name, conn)[0]
    assert fresh.get_parameters(apply_symlinks=False, refresh=True)['fetch']['definition'] == raw


def test_delete_pipeline_rejects_scheduled_reference(saved_members, monkeypatch):
    from meerschaum.core.Pipeline import _saved
    conn, name, pipes = saved_members
    save_pipeline(name, pipes[:1], conn)
    monkeypatch.setattr(_saved, 'referencing_jobs', lambda *args: ['scheduled-import'])
    success, message = delete_pipeline(name, conn)
    assert not success and 'scheduled-import' in message
    assert name in list_pipelines(conn)


def test_partial_analysis_apply_is_recoverable(saved_members, monkeypatch):
    from meerschaum.core.Pipeline import _saved
    conn, name, pipes = saved_members
    save_pipeline(name, pipes[:2], conn)
    original = _saved._change_tag
    calls = []
    def fail_second(*args):
        calls.append(args)
        return (False, 'simulated metadata write failure') if len(calls) == 2 else original(*args)
    with monkeypatch.context() as patch:
        patch.setattr(_saved, '_change_tag', fail_second)
        result = _saved.apply_pipeline_analysis(_saved.analyze_pipeline(name, conn), conn)
    assert not result['success']
    assert len(list_pipelines(conn)[name]['tag_associations']) == 2
    assert _saved.apply_pipeline_analysis(_saved.analyze_pipeline(name, conn), conn)['success']


def test_execution_defaults_cannot_override_definition_structure(saved_members):
    conn, name, pipes = saved_members
    for key in ('parents', 'children', 'instance', 'targets', 'pipeline_definitions', 'run_id'):
        with pytest.raises(ValueError, match='Unsupported Pipeline execution defaults'):
            save_pipeline(name, pipes[:1], conn, defaults={key: True})
    assert name not in list_pipelines(conn)


def test_saved_bounds_used_when_omitted_and_explicit_none_clears(saved_members, monkeypatch):
    import importlib
    module = importlib.import_module('meerschaum.core.Pipeline')
    conn, name, pipes = saved_members
    save_pipeline(name, pipes[:1], conn, defaults={'begin': 10, 'end': 20, 'workers': 2})
    pipeline = load_pipeline(name, conn)
    calls = []
    def capture(method, request):
        calls.append((method, request))
        return {'plan': {'nodes': [], 'edges': [], 'flat': False}, 'success': True, 'message': 'test'}
    monkeypatch.setattr(module, 'call', capture)
    pipeline.plan()
    assert calls[-1][1]['begin'] == 10 and calls[-1][1]['end'] == 20
    pipeline.sync()
    assert calls[-1][1]['begin'] == 10 and calls[-1][1]['end'] == 20
    pipeline.run()
    assert calls[-1][1]['begin'] == 10 and calls[-1][1]['end'] == 20
    pipeline.plan(begin=None, end=None)
    assert calls[-1][1]['begin'] is None and calls[-1][1]['end'] is None
    pipeline.sync(begin=30, end=40, workers=1)
    assert calls[-1][1]['begin'] == 30 and calls[-1][1]['workers'] == 1


def test_scheduled_chains_resolve_registry_per_step(monkeypatch):
    from types import SimpleNamespace
    from meerschaum.core.Pipeline._saved import referencing_jobs
    monkeypatch.setattr('meerschaum.jobs.get_jobs', lambda **kwargs: {'chained': SimpleNamespace(sysargs=[
        'sync', 'pipelines', 'A', '-i', 'sql:a', '+',
        'sync', 'pipelines', 'B', '-i', 'sql:b', '--schedule', 'daily',
    ])})
    assert referencing_jobs('A', 'sql:a') == ['chained']
    assert referencing_jobs('B', 'sql:b') == ['chained']
    assert referencing_jobs('B', 'sql:a') == []
    assert referencing_jobs('A', 'sql:b') == []
