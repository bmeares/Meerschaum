"""Readable timestamps, metadata-only relationships, and compact execution views."""
from tests.api_host._helpers import FAKE_VIEWERS, run_python, sandbox

__all__ = ['sandbox']


def check(result):
    assert result.returncode == 0, result.stderr[-7000:]


def test_integer_sync_times_use_declared_precision_and_keep_raw_values(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from datetime import datetime, timezone
import meerschaum as mrsm
from meerschaum.api.dash import pipes_table as table
epoch = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())
for unit, scalar in [('second',1),('millisecond',1000),('microsecond',1000000),('nanosecond',1000000000),('minute',1/60),('hour',1/3600),('day',1/86400)]:
    value = round(epoch * scalar)
    pipe = mrsm.Pipe('sql:source','events',columns={'datetime':'ts'},dtypes={'ts':'int64'},precision=unit)
    fields = table._sync_time_fields(value,pipe=pipe)
    assert fields['sync_time_raw'] == str(value)
    assert fields['sync_time_sort'] == epoch, (unit,fields)
    assert table.sync_time_text(fields) == '2026-01-01 00:00 UTC', (unit,fields)
counter = mrsm.Pipe('sql:source','sequence',columns={'datetime':'seq'},dtypes={'seq':'int64'})
assert table.sync_time_text(table._sync_time_fields(42,pipe=counter)) == '42'
pipe = mrsm.Pipe('sql:source','events',columns={'datetime':'ts'},precision='ms')
assert table.sync_time_text(table._sync_time_fields(0,pipe=pipe)) == '1970-01-01 00:00 UTC'
assert table.sync_time_text(table._sync_time_fields(-1000,pipe=pipe)) == '1969-12-31 23:59 UTC'
assert table._sync_time_fields(10**30,pipe=pipe)['sync_time'] == str(10**30)
'''))


def test_lineage_reads_metadata_without_planning_or_creating_data_tables(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, FAKE_VIEWERS + '''
from urllib.parse import urlencode
import meerschaum as mrsm
from meerschaum.api import get_api_connector
from meerschaum.api.dash import engine, build_dash_app
build_dash_app()
fake_viewers(default={'username':'alice','admin':True})
from meerschaum.api.dash.callbacks import pipeline_pages
conn = get_api_connector()
parent = mrsm.Pipe('sql:apihosttest','raw',instance=conn,columns={'datetime':'ts'},dtypes={'ts':'int64'},precision='ms')
child = mrsm.Pipe('sql:apihosttest','clean',instance=conn,parents=["Pipe('sql:apihosttest','raw')"])
assert parent.register()[0] and child.register()[0]
assert not parent.exists() and not child.exists()
mrsm._core.engine_plan = lambda *a,**k: (_ for _ in ()).throw(AssertionError('Lineage must not plan a sync'))
before = conn.read("SELECT name FROM sqlite_master WHERE type='table'",as_pandas=True)['name'].tolist()
relations = engine.relations_of(parent)
assert relations['parents'] == []
assert [row['pipe_ref'] for row in relations['children']] == ['sql:apihosttest|sql:apihosttest|clean|None']
canvas, _, _ = pipeline_pages.render_explore('/dash/pipelines/explore',
    '?' + urlencode({'pipe':'sql:apihosttest|sql:apihosttest|clean|None'}), {})
assert 'pipeline-canvas-world' in str(canvas), str(canvas)
assert 'raw' in str(canvas) and 'clean' in str(canvas), str(canvas)
after = conn.read("SELECT name FROM sqlite_master WHERE type='table'",as_pandas=True)['name'].tolist()
assert before == after, (before,after)
assert not parent.exists() and not child.exists()
'''))


def test_related_canvas_nodes_are_context_and_do_not_change_membership(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, FAKE_VIEWERS + '''
import json
import dash
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipelines as page
from meerschaum.api.dash import access
fake_viewers(default={'username':'alice','admin':True})
parent = 'sql:apihosttest|sql:source|raw|None'
child = 'sql:apihosttest|sql:source|clean|None'
other = 'sql:apihosttest|sql:source|report|None'
definition = {'id':'daily-id','name':'daily','registry':'sql:apihosttest','targets':[child]}
page._engine.get_instance = lambda *a,**k: 'sql:apihosttest'
page._engine.plan_graph = lambda *a,**k: (_ for _ in ()).throw(AssertionError('canvas planned a sync'))
calls=[]
def graph(instance,pipes,parents=None,children=None,**kw):
    calls.append((parents,children))
    refs = [parent,child,other] if parents is True else [child]
    return {'nodes':[{'pipe_ref':ref,'label':ref,'status':'observe'} for ref in refs],
            'edges':([{'parent':parent,'child':child},{'parent':child,'child':other}] if len(refs)>1 else []),
            'warnings':[]}
page._engine.dependency_graph = graph
access.visible_pipelines = lambda *a,**k: {'daily':definition,'ingest':{'targets':[parent]}}
dash.set_props = lambda *a: None
output = page.render_pipeline_graph('/dash/pipelines/daily',definition,None,0,
    {'sql:apihosttest|daily':{'family':True}},'',{})
serialized = json.dumps(output,default=lambda c:c.to_plotly_json())
assert calls == [(None,None),(True,True)]
assert 'Not included' in serialized and 'Also in: ingest' in serialized
assert serialized.count('"data-context": "true"') == 2
assert definition['targets'] == [child]
assert 'pipeline-show-related' in serialized
'''))


def test_dependency_graph_keeps_cross_instance_parent_identity(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
import os
from pathlib import Path
os.environ['MRSM_SQL_SIDE'] = 'sqlite:///' + str(Path(os.environ['MRSM_ROOT_DIR'])/'side.db')
import meerschaum as mrsm
from meerschaum.api import get_api_connector
from meerschaum.api.dash import engine, access
home, side = get_api_connector(), mrsm.get_connector('sql','side')
parent = mrsm.Pipe('sql:side','raw',instance=side)
child = mrsm.Pipe('sql:apihosttest','cross_clean',instance=home,
    parents=["Pipe('sql:side','raw',instance='sql:side')"])
registered = parent.register()
assert registered[0], str(registered)
registered = child.register()
assert registered[0], str(registered)
graph = engine.dependency_graph(home,[child],parents=1,viewer=access.SYSTEM)
assert {row['pipe_ref'] for row in graph['nodes']} == {
    'sql:side|sql:side|raw|None', 'sql:apihosttest|sql:apihosttest|cross_clean|None'}
assert [(edge['parent'],edge['child']) for edge in graph['edges']] == [
    ('sql:side|sql:side|raw|None','sql:apihosttest|sql:apihosttest|cross_clean|None')]
assert not parent.exists() and not child.exists()
'''))


def test_adding_a_related_pipe_is_an_authorized_undoable_draft_change(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, FAKE_VIEWERS + '''
from types import SimpleNamespace
import dash
from dash.exceptions import PreventUpdate
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipeline_management as page
from meerschaum.api.dash import access
fake_viewers(default={'username':'alice','admin':True})
page._require_session = lambda _: None
parent,child = 'sql:apihosttest|sql:source|raw|None','sql:apihosttest|sql:source|clean|None'
access.ref_is_visible = lambda who,ref: ref in (parent,child)
saved = {'name':'daily','revision':4,'targets':[child]}
dash.ctx = SimpleNamespace(triggered_id='pipeline-definition')
draft, *_ = page.edit_draft(saved,[child],False,False,0,0,0,None,{},None,{})
dash.ctx = SimpleNamespace(triggered_id='pipeline-context')
updated,members,*_ = page.edit_draft(saved,[child],False,False,0,0,0,{'action':'add','ref':parent},{},draft,{})
assert members == [child,parent] and saved['targets'] == [child]
assert len(updated['undo']) == 1
dash.ctx = SimpleNamespace(triggered_id='pipeline-draft-undo')
undone,members,*_ = page.edit_draft(saved,members,False,False,0,0,0,None,{},updated,{})
assert members == [child]
dash.ctx = SimpleNamespace(triggered_id='pipeline-context')
try:
    page.edit_draft(saved,members,False,False,0,0,0,{'action':'add','ref':'sql:private|x|hidden|None'},{},undone,{})
    raise AssertionError('accepted a hidden member')
except PreventUpdate: pass
'''))


def test_run_history_has_one_lane_and_execution_views_have_no_action_column(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
import json
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipelines as page
runs = [{'run':{'run_id':f'run-{i}','started':f'2026-01-01T00:{i:02}:00Z',
                  'finished':f'2026-01-01T00:{i:02}:10Z'},'status':'succeeded'} for i in range(30)]
graph = page.build_runs_overview(runs).figure
assert set(graph['data'][0]['y']) == {'Runs'}
assert graph['data'][0]['customdata'] == [f'run-{i}' for i in range(30)]
assert graph['layout']['height'] < 220
detail = {'run':{'run_id':'run-1'},'bars':[{'pipe_ref':'sql:apihosttest|sql:source|raw|None',
    'action':'root','status':'succeeded','actual_start':'2026-01-01T00:00:00Z',
    'actual_end':'2026-01-01T00:00:01Z'}]}
page._engine.gantt_figure = lambda _: {}
output = page.build_gantt_children(detail)
serialized = json.dumps(output,default=lambda c:c.to_plotly_json())
assert '"data-run-view": "timeline"' in serialized and '"data-run-view": "table"' in serialized
assert '"children": "Action"' not in serialized
assert '"children": "Status"' in serialized
'''))
