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
    pipeline.save(name)
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
    definition = pipeline.save(name)
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


def test_existing_components_get_names_once(saved_members):
    from meerschaum.core.Pipeline._saved import initialize_pipelines
    conn, _, pipes = saved_members
    refs = [_pipe_ref(pipe) for pipe in pipes]
    graph = {'nodes': [{'pipe_ref': ref} for ref in refs],
             'edges': [{'parent': refs[0], 'child': refs[1]}]}
    definitions = initialize_pipelines(graph, conn)
    assert len(definitions) == 3
    assert {tuple(d['targets']) for d in definitions.values()} == {
        tuple(sorted(refs[:2])), (refs[2],), (refs[3],),
    }
    assert len(set(definitions)) == 3
    assert initialize_pipelines(graph, conn) == definitions
    for name in definitions:
        assert delete_pipeline(name, conn)[0]
    assert initialize_pipelines(graph, conn) == {}


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
