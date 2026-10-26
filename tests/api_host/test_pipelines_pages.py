"""The Pipelines pages: routes, the shared instance, the canvas, the stepper and Edit-mode edges."""
from tests.api_host._helpers import run_python, sandbox  # noqa: F401


def check(result):
    assert result.returncode == 0, result.stderr[-6000:]


def test_routes_and_legacy_redirects(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.pages import pipelines as pages
def ids(layout):
    found = set()
    def walk(node):
        if isinstance(node, (list, tuple)):
            for child in node:
                walk(child)
            return
        if node is None or isinstance(node, (str, int, float)):
            return
        if isinstance(getattr(node, 'id', None), str):
            found.add(node.id)
        walk(getattr(node, 'children', None))
    walk(layout)
    return found
base = 'http://localhost/dash/pipelines'
assert 'pipelines-list' in ids(pages.route('/dash/pipelines', base))
assert 'pipeline-new-page' in ids(pages.route('/dash/pipelines/new', base + '/new'))
assert 'pipeline-explore-graph' in ids(pages.route('/dash/pipelines/explore', base + '/explore?pipe=a|b|c|None'))
detail = pages.route('/dash/pipelines/daily%20totals', base + '/daily%20totals')
assert 'pipelines-graph-div' in ids(detail)
saved_name = next(node for node in detail[-1].children if getattr(node, 'id', None) == 'pipeline-saved-name')
assert saved_name.value == 'daily totals'
### Every Pipelines page carries the global instance selector, set to its registry.
assert 'pages-instance-select' in ids(pages.route('/dash/pipelines', base + '?instance=sql:apihosttest'))
### Old `?pipeline=` / `?name=&run=` links redirect to the new routes.
marker = pages.route('/dash/pipelines', base + '?pipeline=daily&run_id=r1&instance=sql:apihosttest')[0]
assert marker.to_plotly_json()['props']['data-mrsm-redirect'] == '/dash/pipelines/daily?run_id=r1&instance=sql%3Aapihosttest'
marker = pages.route('/dash/pipelines', base + '?name=daily&run=r2')[0]
assert marker.to_plotly_json()['props']['data-mrsm-redirect'] == '/dash/pipelines/daily?run_id=r2'
### No subtitles: the page has no muted helper paragraphs.
assert 'text-muted' not in str(detail)
'''))


def test_the_global_instance_is_shared_through_a_cookie(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
import flask
from meerschaum.api.dash import engine
app = flask.Flask('t')
assert engine.global_instance_keys() is None
with app.test_request_context('/', headers={'Cookie': 'mrsm-instance=sql%3Aapihosttest'}):
    assert engine.global_instance_keys() == 'sql:apihosttest'
    assert str(engine.get_instance()) == 'sql:apihosttest'
    ### A deep link overrides it.
    assert str(engine.get_instance('sql:local')) == 'sql:local'
with app.test_request_context('/', headers={'Cookie': 'mrsm-instance=not%20keys'}):
    assert engine.global_instance_keys() is None
'''))


def test_canvas_collapses_traversal_and_groups_large_pipelines(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
import json
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks.pipelines import build_pipeline_canvas, classify_nodes
ref = lambda ck, mk: f'sql:apihosttest|{ck}|{mk}|None'
up, member, down = ref('a', 'up'), ref('a', 'member'), ref('a', 'down')
graph = {'nodes': [{'pipe_ref': r, 'label': r, 'action': 'skip', 'status': 'caught_up'} for r in (up, member, down)],
         'edges': [{'parent': up, 'child': member}, {'parent': member, 'child': down}], 'warnings': []}
assert classify_nodes(graph, [member]) == {up: 'upstream', member: 'explicit', down: 'downstream'}
def world_of(children):
    return children[0].children[1].children
def cards(children):
    return [card.to_plotly_json()['props'] for card in world_of(children).children]
collapsed = cards(build_pipeline_canvas(graph, [member], {}, {'name': 'p'}, registry='sql:apihosttest'))
assert [c['data-ref'] for c in collapsed if 'pipeline-placeholder' in c['className']] == ['__upstream__', '__downstream__']
assert [c['data-ref'] for c in collapsed if 'pipeline-placeholder' not in c['className']] == [member]
labels = {c['data-ref']: str(c['children']) for c in collapsed}
assert '+1 upstream' in labels['__upstream__'] and '+1 downstream' in labels['__downstream__']
assert len(json.loads(world_of(build_pipeline_canvas(graph, [member], {}, None))
                      .to_plotly_json()['props']['data-edges'])) == 2
expanded = cards(build_pipeline_canvas(graph, [member], {'upstream': True, 'downstream': True}, None))
assert {c['data-ref'] for c in expanded} == {up, member, down}
assert {c['data-ref']: c['data-role'] for c in expanded}[up] == 'upstream'
### Instance badges only for members off the registry.
assert 'pipeline-instance-badge' not in str(build_pipeline_canvas(graph, [member], {}, None, registry='sql:apihosttest'))
assert 'pipeline-instance-badge' in str(build_pipeline_canvas(graph, [member], {}, None, registry='sql:other'))
### Over 25 Pipes the canvas starts grouped by connector.
many = [ref('c' + str(i % 3), 'm' + str(i)) for i in range(30)]
big = {'nodes': [{'pipe_ref': r, 'label': r, 'action': 'skip', 'status': 'caught_up'} for r in many], 'edges': [], 'warnings': []}
groups = [c for c in world_of(build_pipeline_canvas(big, many, {}, None)).children if c.className == 'pipeline-group']
assert len(groups) == 3
### A read-only canvas (the explorer) has no inspector or edit controls.
read_only = str(build_pipeline_canvas(graph, [member], {}, None, read_only=True))
assert 'pipeline-explore-stage' not in read_only or 'pipeline-stage' in read_only
assert 'pipeline-edge-handle' not in read_only and 'pipeline-add-node' not in read_only
'''))


def test_member_panel_defers_new_pipes_and_edges_are_drafts(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from types import SimpleNamespace
import dash
import meerschaum as mrsm
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipeline_management as page
instance = 'sql:apihosttest'
### Adding a new Pipe never registers it: the draft carries it until Save (or Create).
data, alert = page.find_or_add_pipe('pipeline-add-register', instance, 'deferred', 'new', None, '{"columns": {}}', {}, 'pipeline')
ref = instance + '|deferred|new|None'
assert data['add'] == ref and data['new'] == {ref: {'columns': {}}} and not alert
assert mrsm.Pipe('deferred', 'new', instance=instance).get_id() is None
data, alert = page.find_or_add_pipe('pipeline-add-register', instance, 'deferred', 'new', None, '[]', {}, 'pipeline')
assert alert, data
### Dragging one node onto another stages `parents` on the child, validated, without writing.
parent = mrsm.Pipe('edge', 'parent', instance=instance)
child = mrsm.Pipe('edge', 'child', instance=instance)
assert parent.register()[0] and child.register()[0]
staged = {}
dash.set_props = lambda key, value: staged.update({key: value})
alert = page.propose_edge({'parent': instance + '|edge|parent|None', 'child': instance + '|edge|child|None'},
                          {'targets': [instance + '|edge|child|None']})
assert alert == [], alert
edit = staged['pipeline-context']['data']
assert edit['action'] == 'edit_pipe' and edit['ref'] == instance + '|edge|child|None'
assert edit['edit']['parameters']['parents'] == [{'connector_keys': 'edge', 'metric_key': 'parent'}]
assert 'parents' not in mrsm.Pipe('edge', 'child', instance=instance).get_parameters(apply_symlinks=False)
### A cycle is refused.
draft = {'targets': [instance + '|edge|child|None'], 'pipe_edits': {edit['ref']: edit['edit']}}
alert = page.propose_edge({'parent': instance + '|edge|child|None', 'child': instance + '|edge|parent|None'}, draft)
assert 'cycle' in str(alert).lower(), alert
'''))


def test_stepper_reviews_then_creates_with_tags(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from types import SimpleNamespace
import dash
import meerschaum as mrsm
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipeline_pages as pages
from meerschaum.core.Pipeline._saved import list_pipelines
instance = 'sql:apihosttest'
pages._authenticated = lambda session: True
existing = mrsm.Pipe('stepper', 'existing', instance=instance)
assert existing.register()[0]
members = [instance + '|stepper|existing|None', instance + '|stepper|new|None']
draft = {**pages._new_draft(members), 'name': 'stepped', 'new_pipes': {members[1]: {}}}
### Moving forward validates each step.
dash.ctx = SimpleNamespace(triggered_id='pipeline-new-next')
updates = {}
dash.set_props = lambda key, value: updates.update({key: value})
prefill = {'members': [], 'registry': instance}
result = pages.update_new_draft(prefill, [0] * 4, None, 1, '', [], {'options': []}, False, None, False, None,
                                None, None, {**draft, 'name': ''}, {}, {})
assert result[0]['step'] == 'name' and result[2], result
result = pages.update_new_draft(prefill, [0] * 4, None, 1, 'stepped', members, {'options': []}, False, None, False,
                                None, None, None, draft, {}, {})
assert result[0]['step'] == 'members', result
### Review: checks, plan and the exact tags; nothing is written yet.
review = pages.review(draft, instance)
assert review['problems'] == [] and review['tag'] == 'pipeline-stepped'
assert {ref for ref, _ in review['tags']} == set(members) and review['new'] == [members[1]]
assert 'stepped' not in list_pipelines(instance)
assert mrsm.Pipe('stepper', 'new', instance=instance).get_id() is None
missing = {**draft, 'members': [instance + '|stepper|ghost|None'], 'new_pipes': {}}
assert pages.review(missing, instance)['problems']
### Create registers the new Pipe, saves the definition and applies the tags.
dash.ctx = SimpleNamespace(triggered_id='pipeline-new-create')
alert = pages.create_pipeline(1, {**draft, 'step': 'review'}, True, prefill, {instance: draft}, {})
assert alert == [], alert
assert list_pipelines(instance)['stepped']['targets'] == sorted(members)
assert mrsm.Pipe('stepper', 'new', instance=instance).get_id() is not None
assert 'pipeline-stepped' in mrsm.Pipe('stepper', 'existing', instance=instance).tags
assert updates['pipelines-navigate']['data'] == '/dash/pipelines/stepped'
assert updates['pipeline-new-drafts']['data'] == {}
'''))


def test_list_cards_count_singular_and_link_their_pages(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from types import SimpleNamespace
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import pipeline_pages as pages
from meerschaum.core.Pipeline import _saved
import meerschaum.jobs
_saved.list_pipelines = lambda instance: {
    'one': {'name': 'one', 'id': '1', 'targets': ['sql:a|x|a|None']},
    'two': {'name': 'two', 'id': '2', 'targets': ['sql:a|x|a|None', 'sql:b|x|b|None']},
}
pages._engine.get_runs = lambda *args, **kwargs: []
meerschaum.jobs.get_jobs = lambda **kwargs: {'j': SimpleNamespace(
    sysargs=['sync', 'pipelines', 'two', '-i', 'sql:apihosttest', '--schedule', 'daily'])}
cards = [str(card) for card in pages.build_list_cards('sql:apihosttest')]
assert '1 member' in cards[0] and '1 instance' in cards[0] and '1 members' not in cards[0]
assert '2 members' in cards[1] and '2 instances' in cards[1] and 'daily' in cards[1]
assert '/dash/pipelines/two' in cards[1] and 'open=plan' in cards[1]
'''))


def test_deep_links_and_the_global_instance_obey_the_apis_instance_permissions(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
import flask
import meerschaum.api as api
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash import engine
from meerschaum.api.dash.pages import pipelines as pages
default = str(api.get_api_connector())
api.permissions_config.setdefault('instances', {})['allowed_instance_keys'] = [default]
### A refused `?instance=` is never used: the API's instance is, and the refusal is returned.
assert engine.resolve_instance('sql:local') == (api.get_api_connector(), 'sql:local')
assert str(engine.get_instance('sql:local')) == default
try:
    engine.get_instance('sql:local', strict=True)
    raise AssertionError('a refused member instance was used')
except PermissionError:
    pass
assert engine.instance_problem(default) is None
assert "'sql:local'" in engine.instance_problem('sql:local') and default in engine.instance_problem('sql:local')
### The same for the console's global instance (the cookie).
app = flask.Flask('t')
with app.test_request_context('/', headers={'Cookie': 'mrsm-instance=sql%3Alocal'}):
    assert str(engine.get_instance()) == default
    assert "'sql:local'" in engine.instance_problem()
    ### The page names it in an error alert above its content (not a subtitle).
    page = pages.route('/dash/pipelines', 'http://localhost/dash/pipelines')
    alert = next(node for node in page[-1].children if getattr(node, 'id', None) == 'pipelines-instance-alert')
    assert alert.color == 'danger' and "'sql:local'" in alert.children
    navbar_select = [node for node in pages._walk(page) if getattr(node, 'id', None) == 'pages-instance-select'][0]
    assert navbar_select.value == default
### Allowed instances are used as asked, without an alert.
api.permissions_config['instances']['allowed_instance_keys'] = ['*']
assert str(engine.get_instance('sql:local')) == 'sql:local'
assert engine.instance_problem('sql:local') is None
detail = pages.route('/dash/pipelines/daily', 'http://localhost/dash/pipelines/daily?instance=sql:local')
assert 'pipelines-instance-alert' not in str(detail)
'''))


def test_an_invalid_requested_instance_falls_back_with_an_alert(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
import meerschaum.api as api
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash import engine
from meerschaum.api.dash.pages import pipelines as pages
default = str(api.get_api_connector())
### `api:nowhere` has no host: invalid, so it is refused like a forbidden one.
problem = engine.instance_problem('api:nowhere')
assert problem and "'api:nowhere'" in problem and default in problem
page = pages.route('/dash/pipelines/new', 'http://localhost/dash/pipelines/new?instance=api:nowhere')
assert "'api:nowhere'" in str(page) and 'pipelines-instance-alert' in str(page)
'''))
