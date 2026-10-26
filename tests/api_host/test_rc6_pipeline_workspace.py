"""Pipeline workspace selection, draft, and approval contracts without browser timing."""
import json
from tests.api_host._helpers import run_python, sandbox  # noqa: F401


def check(result):
    assert result.returncode == 0, result.stderr[-6000:]


def test_selection_shell_is_lazy_and_query_resolution_is_display_only(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
import json
from types import SimpleNamespace
import dash
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipelines as page
page.is_session_authenticated = lambda _: True
updates = {}
dash.set_props = lambda key, value: updates.update({key: value})
ref = 'sql:apihosttest|sql:source|selected|None'
dash.ctx = SimpleNamespace(triggered_id={'type':'pipeline-stage','index':json.dumps(['daily',ref])})
original_pipe = page.mrsm.Pipe
# Building a reference is cheap; none of the expensive section calls are allowed for the shell.
page._engine.get_runs = lambda *a, **k: (_ for _ in ()).throw(AssertionError('runs fetched eagerly'))
page._engine.plan_graph = lambda *a, **k: (_ for _ in ()).throw(AssertionError('plan fetched eagerly'))
children = page.inspect_stage([1], None, None, None, {})
assert updates['pipeline-selection']['data']['ref'] == ref
assert updates['pipeline-inspector']['hidden'] is False
assert any(getattr(child, 'id', None) == 'pipeline-inspector-body' for child in children)
selection = updates['pipeline-selection']['data']
from meerschaum.api.dash import _support
calls = []
_support.get_pipe_query = lambda pipe, **kw: calls.append((pipe.instance_keys, kw)) or 'SELECT 1'
result = page.load_inspector_section(selection, 'query', False, 0, {})
assert result['token'] == selection['token'] and result['resolve'] is False
assert calls == [('sql:apihosttest', {'apply_symlinks':False,'warn':False})]
page.load_inspector_section(selection, 'query', True, 0, {})
assert calls[-1][1]['apply_symlinks'] is True
assert len(calls) == 2
'''))


def test_membership_draft_undo_redo_and_discard_preserve_saved_state(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from types import SimpleNamespace
import dash
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipeline_management as page
page._require_session = lambda _: None
saved = {'name':'daily', 'revision':3, 'targets':['sql:a|x|a|None'], 'parents':None, 'children':None}
def call(trigger, draft=None, members=None):
    dash.ctx = SimpleNamespace(triggered_id=trigger)
    return page.edit_draft(saved, members, False, False, 0, 0, 0, None, {'options':[]}, draft, {})
state, members, *rest = call('pipeline-definition')
assert members == saved['targets']
changed, members, *rest = call('pipeline-draft-members', state, ['sql:b|x|b|None'])
assert changed['targets'] == ['sql:b|x|b|None'] and rest[-1] == '2 unsaved changes', rest[-1]
undone, members, *rest = call('pipeline-draft-undo', changed)
assert members == saved['targets']
redone, members, *rest = call('pipeline-draft-redo', undone)
assert members == ['sql:b|x|b|None']
discarded, members, *rest = call('pipeline-draft-discard', redone)
assert members == saved['targets'] and saved['revision'] == 3
assert discarded['undo'] == []
# Late options cannot erase a saved membership, even before discovery has returned it.
dash.ctx = SimpleNamespace(triggered_id='pipeline-member-options')
unchanged, members, parents, children, options, notice = page.edit_draft(
    saved, None, False, False, 0, 0, 0, None, {'options':[]}, discarded, {})
assert unchanged is dash.no_update
assert members == saved['targets'] and options[0]['value'] == members[0]
# A new Pipe joins the draft through the same atomic callback (registered on Save) and supports undo.
changed, members, *rest = page.edit_draft(saved, members, False, False, 0, 0, 0, None,
    {'options':[], 'add':'sql:b|x|new|None', 'new': {'sql:b|x|new|None': {'columns': {}}}}, discarded, {})
assert members == saved['targets'] + ['sql:b|x|new|None'] and changed['undo']
assert changed['new_pipes'] == {'sql:b|x|new|None': {'columns': {}}} and rest[-1] == '1 unsaved change'
# A browser-kept draft is restored only onto the revision it was made from.
dash.ctx = SimpleNamespace(triggered_id='pipeline-context')
restored, members, *rest = page.edit_draft(saved, members, False, False, 0, 0, 0,
    {'action': 'restore', 'revision': 3, 'draft': {'targets': ['sql:c|x|c|None']}}, {'options':[]}, discarded, {})
assert members == ['sql:c|x|c|None']
from dash.exceptions import PreventUpdate
try:
    page.edit_draft(saved, members, False, False, 0, 0, 0,
        {'action': 'restore', 'revision': 2, 'draft': {'targets': []}}, {'options':[]}, discarded, {})
    raise AssertionError('restored a draft of another revision')
except PreventUpdate:
    pass

'''))


def test_page_selection_never_initializes_discovered_components(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from types import SimpleNamespace
import dash
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipeline_management as page
page._require_session = lambda _: None
page.engine.get_instance = lambda *args, **kwargs: 'sql:apihosttest'
page.engine.plan_graph = lambda *a: (_ for _ in ()).throw(AssertionError('discovery on load'))
page.initialize_pipelines = lambda *a: (_ for _ in ()).throw(AssertionError('initialization on load'))
page.list_pipelines = lambda *a: {'saved': {'name':'saved','targets':[]}}
dash.ctx = SimpleNamespace(triggered_id='pipelines-location')
dash.set_props = lambda *a: None
# The Pipeline comes from its path.
options, selected, definition, name = page.read_selection('/dash/pipelines/saved', 'saved', '', 0, {})
assert definition['name'] == 'saved' and name == 'saved' and selected == 'saved'
assert options == [{'label':'saved','value':'saved'}]
# Filtering history must not reset the inspector or an unsaved membership draft.
_, selected, unchanged, unchanged_name = page.read_selection('/dash/pipelines/saved', 'saved', '?run_id=historic', 0, {}, definition)
assert unchanged is dash.no_update and unchanged_name is dash.no_update
# Hydration never rewrites the URL, so a deep-linked run is kept.
updates = {}
dash.set_props = lambda key, value: updates.update({key:value})
dash.ctx = SimpleNamespace(triggered_id='pipeline-saved-name')
page.read_selection('/dash/pipelines/saved', 'saved', '?run_id=historic', 0, {}, definition)
assert 'pipelines-location' not in updates, updates
# A Pipeline missing from its registry says so.
dash.ctx = SimpleNamespace(triggered_id='pipelines-location')
options, selected, definition, _ = page.read_selection('/dash/pipelines/missing', None, '', 0, {})
assert selected == 'missing' and definition in (None, dash.no_update) and options[-1]['value'] == 'missing'
assert updates['pipelines-alert-div']['children'], updates
# Legacy `?pipeline=` links still name their Pipeline.
assert page.name_from_path('/dash/pipelines', '?pipeline=saved') == 'saved'
assert page.name_from_path('/dash/pipelines/a%20b') == 'a b'
assert page.name_from_path('/dash/pipelines/new') is None



'''))


def test_draft_cycle_validation_is_read_only_and_ignores_incremental_self_templates(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from copy import deepcopy
import meerschaum as mrsm
from meerschaum.api.dash.pipeline_editing import dependencies, validate_edits, fingerprint
instance = 'sql:apihosttest'
a = mrsm.Pipe('draft', 'a', instance=instance, parameters={
    'sql': "SELECT MAX(dt) FROM {{ Pipe('draft', 'a') }}"})
b = mrsm.Pipe('draft', 'b', instance=instance, parameters={'parents':[{'connector':'draft','metric':'a'}]})
assert a.register()[0] and b.register()[0]
a_ref, b_ref = instance + '|draft|a|None', instance + '|draft|b|None'
assert dependencies(a_ref, dict(a.get_parameters(apply_symlinks=False))) == set()
assert dependencies(a_ref, {'sql': 'SELECT MAX(dt) FROM {{ Pipe() }}'}) == set()
# Plural keys have the same precedence over singular aliases as the native Pipe model.
assert dependencies(a_ref, {'parents': [], 'parent': {'connector':'draft','metric':'b'}}) == set()
original = deepcopy(dict(a.get_parameters(apply_symlinks=False)))
proposed = {**original, 'parents':[{'connector':'draft','metric':'b'}]}
try:
    validate_edits({a_ref:{'baseline':fingerprint(original),'parameters':proposed}}, [a_ref,b_ref])
    raise AssertionError('Cycle was accepted')
except ValueError as exc:
    assert 'cycle' in str(exc).lower()
assert dict(mrsm.Pipe('draft','a',instance=instance).get_parameters(apply_symlinks=False)) == original
# A late shared-Pipe edit invalidates the staged baseline before any writes.
a.parameters = {**original, 'description':'Changed elsewhere'}
assert a.edit(interactive=False)[0]
try:
    validate_edits({a_ref:{'baseline':fingerprint(original),'parameters':proposed}}, [a_ref])
    raise AssertionError('Stale parameters were accepted')
except ValueError as exc:
    assert 'changed while editing' in str(exc)
'''))


def test_query_helper_keeps_raw_aliases_and_uses_selected_instance_context(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from meerschaum.api.dash._support import get_pipe_query
from meerschaum.utils import pipes
class Source:
    def get_parameters(self, apply_symlinks=True):
        return {'sql': "SELECT * FROM {{ Pipe('source', 'parent') }}"}
    @property
    def parameters(self):
        raise AssertionError('Raw query display unexpectedly resolved parameters')
source = Source()
calls = []
pipes.replace_pipes_syntax = lambda text, _pipe=None: calls.append(_pipe) or 'SELECT * FROM exact_instance_table'
assert '{{ Pipe(' in get_pipe_query(source, apply_symlinks=False)
assert calls == []
assert get_pipe_query(source, apply_symlinks=True) == 'SELECT * FROM exact_instance_table'
assert calls == [source]
'''))


def test_context_sync_does_not_reuse_another_inspectors_bounds(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from types import SimpleNamespace
import dash
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipelines as page
from meerschaum.api.dash import execute, sessions
page.is_session_authenticated = lambda _: True
sessions.get_username_from_session = lambda _: 'alice'
dash.set_props = lambda *a: None
dash.ctx = SimpleNamespace(triggered_id='pipeline-context')
calls = []
execute.run_action = lambda args, user, **kw: calls.append((args,user,kw)) or {'success':True,'job':'execute-test','stream':'/logs/test/stream','msg':'started'}
a, b = 'sql:a|source|a|None', 'sql:b|source|b|None'
page.sync_selected_pipe(0, {'action':'sync','ref':b,'scope':'parents'}, {'ref':a}, 'this', '2020-01-01', '2021-01-01', None, None, {'session-id':'current'})
assert '-i' in calls[0][0] and calls[0][0][3] == 'sql:b'
assert '--parents' in calls[0][0] and '--begin' not in calls[0][0] and '--end' not in calls[0][0]
assert '-canvas-' in calls[0][2]['metadata']['run_id']
page.sync_selected_pipe(0, {'action':'sync','ref':b,'scope':'this'}, {'ref':b}, 'this', '2020-01-01', '2021-01-01', None, None, {'session-id':'current'})
assert '--begin' in calls[1][0] and '--end' in calls[1][0]
'''))


def test_every_open_expanded_run_continues_polling_and_filters_exact_pipe(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from types import SimpleNamespace
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipelines as page
from meerschaum.api.dash import _support, jobs
page.is_session_authenticated = lambda _: True
jobs.is_automatic_job = lambda _: True
jobs.job_visible_to_session = lambda *a: True
runs = [{'job':'job-a','run_id':'run-a','ref':'sql:a|x|a|None','instance':'sql:a','scope':'parents'},
        {'job':'job-b','run_id':'run-b','ref':'sql:b|x|b|None','instance':'sql:b','scope':'children'}]
_support.get_job = lambda name: SimpleNamespace(status='running', ended=None, daemon=SimpleNamespace(properties={'mrsm_execution':{'run_id':'run-' + name[-1]}}))
page._engine.get_instance = lambda instance, **kwargs: instance
calls = []
def events(instance, run_id):
    calls.append((instance,run_id))
    ref = next(row['ref'] for row in runs if row['run_id'] == run_id)
    return [{'seq':1,'run_id':run_id,'pipe_ref':ref,'event':'node_started','payload':{'action':'incremental','attempt':1}},
            {'seq':2,'run_id':run_id,'pipe_ref':'sql:other|x|other|None','event':'node_started','payload':{'attempt':1}}]
page._engine.get_run_events = events
results, statuses = page.node_events(runs, 1, {'session-id':'current'})
assert calls == [('sql:a','run-a'), ('sql:b','run-b')]
assert [row['job'] for row in results] == ['job-a','job-b']
assert all(len(row['lines']) == 1 and 'other' not in row['lines'][0] for row in results)
assert statuses[runs[0]['ref']] == statuses[runs[1]['ref']] == 'running'
'''))


def test_analysis_requires_separate_tag_approval_and_partial_edits_remain_retryable(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from contextlib import nullcontext
from types import SimpleNamespace
import dash
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipeline_management as page
from meerschaum.core.Pipeline import _saved
from meerschaum.api.dash import pipeline_editing
page._require_session = lambda _: None
page.engine.get_instance = lambda *a, **k: 'sql:apihosttest'
updates = {}
dash.set_props = lambda key, value: updates.update({key:value})
proposal = {'definition':{'name':'daily'}, 'resolved':[{'pipe_ref':'sql:a|x|a|None','reason':'explicit member'}],
            'changes':[{'pipe_ref':'sql:a|x|a|None','tag':'pipeline-daily','action':'add'}], 'errors':[]}
calls = []
_saved.analyze_pipeline = lambda *a: proposal
_saved.apply_pipeline_analysis = lambda *a: calls.append(a) or {'success':True,'message':'applied'}
dash.ctx = SimpleNamespace(triggered_id='pipeline-analyze')
preview, stored, alert = page.analyze_selection(1, 0, 'daily', None, '', {})
assert stored is proposal and calls == []
dash.ctx = SimpleNamespace(triggered_id='pipeline-apply-tags')
page.analyze_selection(1, 1, 'daily', proposal, '', {})
assert len(calls) == 1 and calls[0][0] is proposal
# A successful shared-Pipe write is removed from the retry draft even if another instance failed.
_saved._mutation_lock = lambda *a: nullcontext()
page.list_pipelines = lambda *a: {'daily': {'name':'daily','revision':1}}
page.save_pipeline = lambda *a, **k: (_ for _ in ()).throw(AssertionError('definition saved despite partial Pipe failure'))
pipeline_editing.apply_edits = lambda *a: [('good',True,'saved'),('failed',False,'offline')]
dash.ctx = SimpleNamespace(triggered_id='pipeline-draft-save')
draft = {'targets':['good','failed'],'pipe_edits':{'good':{'baseline':'a'},'failed':{'baseline':'b'}},'undo':[{}],'redo':[]}
page.manage_selection(0, 1, 'daily', 'daily', draft, {'revision':1}, '', {})
assert updates['pipeline-draft']['data']['pipe_edits'] == {'failed': {'baseline':'b'}}
assert updates['pipeline-draft']['data']['undo'] == []
'''))


def test_stopped_canvas_worker_overrides_unfinished_running_events(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, '''
from types import SimpleNamespace
from meerschaum.api.dash.callbacks import pipelines as cb
from meerschaum.api.dash import _support, jobs
cb.is_session_authenticated = lambda session: True
jobs.job_visible_to_session = lambda job, session: True
job = SimpleNamespace(status='stopped', ended=None, daemon=SimpleNamespace(properties={'mrsm_execution':{'run_id':'cancelled-run'}}))
_support.get_job = lambda name: job
cb._engine.get_instance = lambda instance, **kwargs: instance
cb._engine.get_run_events = lambda *args: [{'event':'node_started','pipe_ref':'sql:test|sql:test|slow|None','payload':{}}]
run = {'job':'execute-test', 'run_id':'cancelled-run', 'instance':'sql:test', 'ref':'sql:test|sql:test|slow|None'}
events, statuses = cb.node_events([run], 1, {'session-id':'test'})
assert statuses[run['ref']] != 'running', statuses
assert events[0]['status'] == 'stopped'
job.status = 'running'
events, statuses = cb.node_events([run], 2, {'session-id':'test'})
assert statuses[run['ref']] == 'running', statuses
print('JSON:true')
''')
    assert result.returncode == 0, result.stdout + result.stderr


def test_deletion_requires_confirmed_identity_and_preserves_members(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from types import SimpleNamespace
import dash
import meerschaum as mrsm
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipeline_management as page
from meerschaum.core.Pipeline._saved import save_pipeline, list_pipelines
page._require_session = lambda _: None
instance = 'sql:apihosttest'
pipe = mrsm.Pipe('rc7', 'member', instance=instance)
assert pipe.register()[0]
saved = save_pipeline('delete-me', [instance + '|rc7|member|None'], instance)
dash.ctx = SimpleNamespace(triggered_id='pipeline-delete-confirm')
dash.set_props = lambda *a: None
target = {key:saved.get(key) for key in ('name','registry','id','revision')}
args = (1, 0, 'delete-me', 'delete-me', {}, saved, '?instance=sql%3Aapihosttest', {})
page.manage_selection(*args, delete_target={**target, 'id':'different'})
assert 'delete-me' in list_pipelines(instance)
page.manage_selection(*args, delete_target=target)
assert 'delete-me' not in list_pipelines(instance)
assert pipe.get_id() is not None
'''))


def test_newer_stopped_worker_does_not_modify_selected_historical_run(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from types import SimpleNamespace
import dash
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipelines as page
from meerschaum.api.dash import _support
page.is_session_authenticated = lambda _: True
page._engine.get_instance = lambda *a, **k: 'sql:apihosttest'
summary = {'status':'running', 'counts':{}, 'run':{'run_id':'old-run'}}
detail = {'summary':summary, 'bars':[{'pipe_ref':'sql:a|c|m|None','status':'running','actual_start':'2026-10-22T00:00:00Z'}]}
page._engine.get_runs = lambda *a, **k: [summary]
page._engine.get_run_detail = lambda *a: detail
page._engine.run_belongs_to_pipeline = lambda *a: True
page.build_gantt_children = lambda data: data
page.build_runs_overview = lambda *a: []
page.build_runs_table = lambda *a, **k: []
_support.get_job = lambda *a: SimpleNamespace(status='stopped')
dash.ctx = SimpleNamespace(triggered_id='pipeline-selected-run')
dash.set_props = lambda *a: None
result = page.render_pipelines('/dash/pipelines', '', {'name':'daily','id':'identity','registry':'sql:apihosttest'},
    {'pipeline':'daily','pipeline_id':'identity','registry':'sql:apihosttest','run_id':'new-run','job':'worker'},
    'old-run', [], 0, 0, {}, None, {'session-id':'test'})
assert result[0]['summary']['status'] == 'running', result
assert result[0]['bars'][0]['status'] == 'running', result
assert 'actual_end' not in result[0]['bars'][0]
'''))
