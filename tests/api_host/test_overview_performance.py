"""Overview reads bounded shared snapshots and rechecks access without a graph walk."""

from tests.api_host._helpers import run_python, sandbox  # noqa: F401


def check(result):
    assert result.returncode == 0, result.stderr[-6000:]


def test_overview_refresh_shares_one_definition_and_history_read(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from meerschaum.api.dash import overview
calls = []
definitions = {'daily': {'name': 'daily', 'targets': []}}
runs = [{'run': {'run_id': 'r1', 'targets': ['sql:apihosttest|fixture|a|None'],
                  'selection': {'pipeline_definitions': [{'name': 'daily'}]}},
         'status': 'succeeded', 'counts': {'succeeded': 1}}]
overview._list_pipelines = lambda *a, **kw: calls.append('registry') or definitions
overview._get_runs = lambda *a, **kw: calls.append('history') or runs
summary = overview.engine_summary('sql:apihosttest')
assert calls == ['registry', 'history'], calls
assert summary['pipelines']['count'] == 1
assert summary['runs'][0]['pipeline'] == 'daily'
# Snapshot metadata cannot reveal a Pipeline absent from the viewer's registry list.
assert overview.run_pipeline_name(runs[0], {}) is None
assert 'no recent run' in str(overview.status_chip(None)).lower()
'''))


def test_saved_definitions_use_bulk_parameters_without_constructing_pipes(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
import meerschaum as mrsm
from meerschaum.core.Pipeline import _saved
conn = mrsm.get_connector('sql:apihosttest')
for name in ('one', 'two', 'three'):
    pipe = mrsm.Pipe('mrsm', 'pipeline', name, instance=conn,
                     parameters={'pipeline': {'targets': [], 'description': name}})
    assert pipe.register()[0]
def refused(*a, **kw):
    raise AssertionError('Per-Pipe construction or metadata read while listing definitions')
mrsm.get_pipes = refused
conn.get_pipe_attributes = refused
definitions = _saved.list_pipelines(conn)
assert set(definitions) == {'one', 'two', 'three'}
assert all(definition['registry'] == str(conn) for definition in definitions.values())
assert definitions['two']['description'] == 'two'
'''))


def test_run_visibility_is_fresh_targeted_and_different_for_each_viewer(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
import json
import meerschaum as mrsm
from meerschaum.api.dash import access, engine
conn = mrsm.get_connector('sql:apihosttest')
public = mrsm.Pipe('fixture', 'visible', instance=conn,
                   parameters={'access': {'visibility': 'everyone'}})
private = mrsm.Pipe('fixture', 'other-team', instance=conn,
                    parameters={'access': {'visibility': 'team', 'team': 'red', 'owner': 'bob'}})
assert public.register()[0] and private.register()[0]
refs = [f'{conn}|fixture|visible|None', f'{conn}|fixture|other-team|None']
# This policy fixture supplies already-captured evidence through the private native RPC.
# The retained-history lifecycle regression exercises real persisted generation checks.
captured_generations = {'fixture-run': True}
engine._call = lambda name, request: (
    {run_id: captured_generations.get(run_id, False) for run_id in request['run_ids']}
    if name == 'engine_run_generations'
    else [{'run': {'run_id': 'fixture-run', 'targets': refs}, 'status': 'succeeded'}]
)
def no_full_scan(*a, **kw):
    raise AssertionError('Run summary walked every registered pipe')
access.visible_refs = no_full_scan
read = access.registered_pipes
filters = []
def targeted(instance, **kw):
    assert kw.get('metric_keys'), kw
    filters.append(kw)
    return read(instance, **kw)
access.registered_pipes = targeted
alice = access.make_viewer('alice', role='viewer')
first = engine.get_runs(conn, viewer=alice)
assert len(first) == 1 and 'other-team' not in json.dumps(first), first
assert len(filters) == 1, filters
bob = access.make_viewer('bob', role='viewer', teams={'red': 'member'})
assert 'other-team' in json.dumps(engine.get_runs(conn, viewer=bob))
public.parameters['access'] = {'visibility': 'private', 'owner': 'bob'}
assert public.edit(interactive=False)[0]
# The same viewer immediately loses the run; no filtered result is reused from a TTL cache.
assert engine.get_runs(conn, viewer=alice) == []
assert engine.get_runs(conn, viewer=access.make_viewer('nobody', permissions=[])) == []
'''))


def test_list_shaped_backend_keys_refresh_saved_definitions(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from types import SimpleNamespace
import meerschaum as mrsm
import meerschaum.utils
from meerschaum.core.Pipeline import _saved
conn = mrsm.get_connector('sql:apihosttest')
# Valkey's public wire shape has a fourth placeholder, rather than SQL's bulk metadata.
meerschaum.utils.fetch_pipes_keys = lambda *a, **kw: [('mrsm', 'pipeline', 'daily', {})]
definition = {'targets': [], 'description': 'first'}
def parameters(**kwargs):
    return {'pipeline': dict(definition)} if kwargs.get('refresh') else {}
_saved.registration = lambda *a: SimpleNamespace(get_parameters=parameters)
assert _saved.list_pipelines(conn)['daily']['description'] == 'first'
definition['description'] = 'edited elsewhere'
assert _saved.list_pipelines(conn)['daily']['description'] == 'edited elsewhere'
'''))
