"""The Performance page's Engine section reads the engine's history natively, for what a viewer may see."""

from tests.api_host._helpers import SIGN_IN, run_python, sandbox  # noqa: F401


def instance(root, name):
    """A separate SQLite instance per test: the sandbox (and its default instance) is per module."""
    return {'MRSM_SQL_APIHOSTTEST': f"sqlite:///{root / (name + '.db')}"}


def check(result):
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-6000:]


#: Real engine history on the sandbox instance: a saved Pipeline `engine-demo` (`clean` with its
#: parent `raw`), a run of `broken` that fails, and new source rows synced only into `raw`, so
#: `clean` is behind its parent.
SEED = '''
from datetime import datetime, timedelta, timezone
import meerschaum as mrsm
from meerschaum.api import get_api_connector
from meerschaum.core.Pipeline._saved import save_pipeline
c = get_api_connector()
c.exec("CREATE TABLE src (ts TIMESTAMP, id INTEGER, v REAL)")
now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
def add(n, start):
    c.exec_queries([f"INSERT INTO src VALUES ('{start + timedelta(minutes=i)}', {i % 3}, {i})" for i in range(n)])
def pipe(metric, parameters, owner=None):
    access = {'visibility': 'private', 'owner': owner} if owner else {'visibility': 'public'}
    p = mrsm.Pipe('sql:apihosttest', metric, instance=c, parameters={**parameters, 'access': access})
    assert p.register()[0]
    return p, str(c) + '|sql:apihosttest|' + metric + '|None'
cols = {'datetime': 'ts', 'id': 'id'}
add(30, now - timedelta(hours=3))
raw, raw_ref = pipe('raw', {'sql': 'SELECT ts, id, v FROM src', 'columns': cols})
clean, clean_ref = pipe('clean', {'columns': cols, 'sql': (
    "SELECT ts, id, v FROM {{ Pipe('sql:apihosttest', 'raw', instance='sql:apihosttest') }} WHERE v >= 0")},
    owner=OWNER)
broken, broken_ref = pipe('broken', {'fetch': {'definition': 'SELECT * FROM missing_source'}}, owner=OWNER)
save_pipeline('engine-demo', [clean_ref], c, parents=True, access={'visibility': 'public'})
first = mrsm.Pipeline('engine-demo', instance=c).sync()
assert first[0], first
assert not broken.sync(parents=True)[0]
add(10, now - timedelta(minutes=20))
assert raw.sync(parents=True)[0]
'''


def test_engine_section_shows_runs_freshness_backlog_and_failed_nodes(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, SIGN_IN + '''
from unittest.mock import patch
import dash
from meerschaum.api.dash import performance, engine
from meerschaum.api.dash.callbacks import performance as callbacks
admin = sign_in('engine-admin', user_type='admin')
OWNER = 'somebody-else'

# An instance without engine history: a plain empty state.
data = performance.metrics('sql:apihosttest', admin, '24h')
health = performance.engine_health(data, '24h')
assert health['runs']['runs'] == 0 and not health['freshness']['pipes'], health
empty = str(performance.build_engine(health))
assert 'No engine history' in empty and 'Freshness' not in empty
''' + SEED + '''
data = performance.metrics(str(c), admin, '24h')
health = performance.engine_health(data, '24h')
runs = health['runs']
assert (runs['runs'], runs['failed'], runs['unfinished']) == (3, 1, 0), runs
assert runs['p95_seconds'] is not None and len(runs['buckets']) == performance.ENGINE_BUCKETS
fresh = health['freshness']
assert (fresh['failing'], fresh['never_succeeded'], fresh['behind']) == (1, 1, 1), fresh
assert fresh['pipes'][0]['pipe_ref'] == broken_ref
behind = next(item for item in fresh['pipes'] if item['pipe_ref'] == clean_ref)
assert behind['pending'] == 1 and behind['lag_seconds'] > 0, behind
assert [row['child_ref'] for row in health['backlog']['children']] == [clean_ref]
assert [(n['pipe_ref'], n['status']) for n in health['nodes']] == [(broken_ref, 'failed')]

section = performance.build_engine(health)
text = str(section)
for label in ('Engine', 'Runs', 'Failed runs', 'Unfinished runs', 'P95 run duration', 'Pipes failing',
              'Never synced', 'Longest since success', 'Children behind', 'Runs by outcome',
              'Run duration', 'Freshness', 'Backlog', 'Nodes', 'Behind parents', 'Pending ranges',
              'Planner reason', 'Failed', 'Never', '/dash/pipes/sql%3Aapihosttest/broken?instance=sql%3Aapihosttest',
              'Failures in a row', 'no parents in this run'):
    assert label in text, label
# A run of the saved Pipeline links to it on the Pipeline's page; other runs are not links.
run_id = next(r['run_id'] for r in runs['recent'] if r['name'] == 'engine-demo')
assert '/dash/pipelines/engine-demo?run_id=' + run_id.replace(':', '%3A') in text, text
assert text.count('/dash/pipelines/') == 1
figures = performance.engine_figures(health, 'scope')
bars = figures['engine-runs'].to_dict()['data']
assert [trace['name'] for trace in bars] == ['Succeeded', 'Failed', 'Unfinished']
assert sum(sum(trace['y']) for trace in bars) == 3
assert figures['engine-duration'].to_dict()['layout']['yaxis']['title']['text'] == 'Seconds / run'

# The Pipe filter narrows every part to that pipe; a Pipeline filter to its runs' pipes.
one = performance.engine_health(performance.metrics(str(c), admin, '24h', pipe=broken_ref), '24h')
assert one['runs']['runs'] == 1 and [p['pipe_ref'] for p in one['freshness']['pipes']] == [broken_ref]
named = performance.engine_health(performance.metrics(str(c), admin, '24h', pipeline='engine-demo'), '24h')
assert named['runs']['runs'] == 1 and not named['nodes']
assert {p['pipe_ref'] for p in named['freshness']['pipes']} == {raw_ref, clean_ref}

# The callback: the first response builds the section; with the same shape it updates in place.
def render(engine_shape=None):
    return callbacks.render_performance('/dash/performance', None, '24h', None, None, 0, 0, 1,
                                        str(c), 10, admin, None, engine_shape)
first = render()
built, shape = first[11], first[12]
assert 'Freshness' in str(built) and shape[1] == 'body', shape
again = render(shape)
assert again[11] is dash.no_update and again[12] == shape
with patch.object(engine, 'get_engine_health', side_effect=RuntimeError('PRIVATE SQL PASSWORD')):
    failed = render(shape)
    assert 'could not be loaded' in str(failed[11]) and 'PRIVATE' not in str(failed)
    assert 'Performance could not be loaded' not in str(failed[0])
''', extra_env=instance(root, 'engine-admin')))


def test_engine_section_shows_only_what_a_viewer_may_read(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, SIGN_IN + '''
from meerschaum.api.dash import performance
OWNER = 'somebody-else'
''' + SEED + '''
reader = sign_in('engine-reader', role='viewer')
data = performance.metrics(str(c), reader, '24h')
assert data['_visible_refs'] == [raw_ref], data['_visible_refs']
health = performance.engine_health(data, '24h')
# Runs with a visible node: the Pipeline's run (raw, clean) and raw's own run, not broken's.
assert (health['runs']['runs'], health['runs']['failed']) == (2, 0), health['runs']
assert [p['pipe_ref'] for p in health['freshness']['pipes']] == [raw_ref]
assert health['freshness']['failing'] == 0 and health['freshness']['behind'] == 0
assert not health['backlog']['children'] and not health['nodes']
text = str(performance.build_engine(health)) + str(health)
for secret in ('broken', "'clean'", clean_ref, 'missing_source'):
    assert secret not in text, secret

# A planner reason naming a hidden pipe is anonymized for a restricted viewer.
from unittest.mock import patch
from meerschaum.api.dash import engine
leaked = {**health, 'nodes': [{'run_id': 'x', 'run_started': None, 'pipe_ref': raw_ref, 'action': 'incremental',
          'status': 'failed', 'attempts': 2, 'num_chunks': 0, 'duration_seconds': 1.0,
          'reason': "parent Pipe('sql:apihosttest', 'broken', instance='sql:apihosttest') advanced"}]}
leaked.pop('_instance'); leaked.pop('_window'); leaked.pop('_pipe_filter'); leaked.pop('_pipeline_filter'); leaked.pop('_pipelines')
with patch.object(engine, 'get_engine_health', return_value=leaked):
    shown = performance.engine_health(data, '24h')
assert shown['nodes'][0]['reason'] == 'parent a hidden pipe advanced', shown['nodes']

# Pipes it may not read cannot be selected.
try:
    performance.metrics(str(c), reader, '24h', pipe=broken_ref)
except PermissionError:
    pass
else:
    raise AssertionError('A hidden pipe was selectable.')
''', extra_env=instance(root, 'engine-viewer')))


def test_engine_formatting_and_layout(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from meerschaum.api.dash import performance
assert performance.elapsed(45) == '45 s' and performance.elapsed(720) == '12 min'
assert performance.elapsed(3 * 3600 + 720) == '3.2 h' and performance.elapsed(4.1 * 86400) == '4.1 d'
assert performance.elapsed(None) == 'n/a' and performance.elapsed(-1) == 'n/a'
assert performance._ago(None) == 'Never'
assert performance._run_name('engine-demo-2026-11-05T03-58-52.335751Z-cc1872b5') == 'engine-demo'
assert performance._run_name('2026-10-26T16-48-03Z-canvas-1a2b3c4d5e6f') is None
assert performance.engine_shape(None) is None
from meerschaum.api.dash.pages import performance as pages
layout = str(pages.route('/dash/performance', 'http://localhost/dash/performance'))
assert "id='performance-engine'" in layout and 'performance-engine-shape' in layout
assert layout.index('performance-history-controls') < layout.index("id='performance-engine'")
'''))
