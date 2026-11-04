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
overview._get_run_activity = lambda *a, **kw: calls.append('activity') or {
    'buckets': [], 'names': [], 'runs': 0, 'oldest': None, 'truncated': False,
}
summary = overview.engine_summary('sql:apihosttest')
assert calls == ['registry', 'history', 'activity'], calls
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


def test_pipelines_history_counts_every_run_of_the_day_not_the_newest_page(sandbox):
    """A Pipeline running every few seconds fills 24 hourly bars, not one capped bar."""
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from datetime import datetime, timedelta, timezone
import meerschaum as mrsm
from meerschaum.api.dash import overview
from meerschaum.core.Pipeline._engine import call, build_request
now = datetime(2026, 11, 4, 12, 30, tzinfo=timezone.utc)
definitions = {'Sync pings!': {'name': 'Sync pings!', 'targets': []}, 'daily': {'name': 'daily', 'targets': []}}
assert overview.run_slug('Sync pings!') == 'Sync-pings-'
assert overview.run_slug('***') == 'pipeline'
hour = lambda h: (datetime(2026, 11, 4, 12, tzinfo=timezone.utc) - timedelta(hours=h)).isoformat()
activity = {
    'buckets': (
        [{'start': hour(h), 'name': 'Sync-pings-', 'status': 'succeeded', 'count': 180} for h in range(24)]
        + [{'start': hour(5), 'name': 'Sync-pings-', 'status': 'failed', 'count': 2},
           {'start': hour(3), 'name': 'hidden', 'status': 'failed', 'count': 9},
           {'start': hour(30), 'name': 'daily', 'status': 'succeeded', 'count': 1}]
    ),
    'names': [
        {'name': 'Sync-pings-', 'latest': {'run_id': 'p-new', 'started': hour(0), 'status': 'running'},
         'last_success': {'run_id': 'p-ok', 'started': hour(0), 'status': 'succeeded'},
         'last_failure': {'run_id': 'p-bad', 'started': hour(5), 'status': 'failed'}},
        {'name': 'hidden', 'latest': {'run_id': 'h', 'started': hour(3), 'status': 'failed'},
         'last_success': None, 'last_failure': {'run_id': 'h', 'started': hour(3), 'status': 'failed'}},
    ],
    'runs': 4325, 'oldest': hour(23), 'truncated': False,
}
### The recent-runs read knows the newest run's worker stopped.
runs = [{'run': {'run_id': 'p-new', 'started': hour(0), 'targets': [],
                 'selection': {'pipeline_definitions': [{'name': 'Sync pings!'}]}},
         'status': 'stopped', 'counts': {}}]
summary = overview.pipelines_summary('sql:apihosttest', definitions=definitions, runs=runs, now=now,
                                     activity=activity)
assert summary['runs_24h'] == 24 * 180 + 2, summary['runs_24h']
assert summary['failed_24h'] == 2
assert [h['succeeded'] for h in summary['hours']] == [180] * 24
assert not any(h['unread'] for h in summary['hours'])
assert summary['states'] == {'running': 0, 'succeeded': 0, 'failed': 0, 'stopped': 1, 'none': 1}, summary['states']
assert summary['last_failure']['run_id'] == 'p-bad' and summary['last_failure']['name'] == 'Sync pings!'
assert summary['last_success']['run_id'] == 'p-ok'

### A count cut short by its limit marks the hours it did not reach.
short = dict(activity, truncated=True, oldest=hour(2))
summary = overview.pipelines_summary('sql:apihosttest', definitions=definitions, runs=[], now=now,
                                     activity=short)
assert [h['unread'] for h in summary['hours']] == [True] * 21 + [False] * 3

### Without a count (an instance that cannot count), the recent runs are counted.
def unavailable(*args, **kwargs):
    raise RuntimeError('no activity')
overview._get_run_activity = unavailable
summary = overview.pipelines_summary('sql:apihosttest', definitions=definitions, runs=runs, now=now)
assert summary['runs_24h'] == 1 and summary['hours'][-1]['stopped'] == 1

### The real binding: an empty instance has nothing to count.
real = call('engine_run_activity', dict(build_request('sql:apihosttest'),
                                         since=(now - timedelta(days=1)).isoformat()))
assert set(real) == {'buckets', 'names', 'runs', 'oldest', 'truncated'}, real
'''))
