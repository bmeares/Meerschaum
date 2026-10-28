"""rc6 console contracts: dynamic Inputs, worker classification, and custom login."""
import json
from tests.api_host._helpers import FAKE_VIEWERS, run_python, sandbox, write_plugin  # noqa: F401


def test_worker_kind_overrides_legacy_prefix(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, '''
import json
from meerschaum.api.dash.jobs import is_automatic_job
class Job:
    def __init__(self, name, kind=None): self.name, self.kind = name, kind
    def get_daemon_properties(self): return {'mrsm_execution': {'kind': self.kind}} if self.kind else {}
print('JSON:' + json.dumps([
    is_automatic_job(Job('execute-legacy')),
    is_automatic_job(Job('named', 'temporary_execution')),
    is_automatic_job(Job('execute-kept', 'persistent_job')),
    is_automatic_job(Job('manual')),
]))
''')
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.split('JSON:')[-1]) == [True, True, False, False]


def test_direct_pipe_filter_inputs_are_optional(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, '''
import json
from meerschaum.api.dash import dash_app
import meerschaum.api.dash.callbacks.pipes
dash_app._setup_server()
callbacks = [cb for cb in dash_app._callback_list if cb['output'] == 'pipes-location.search']
assert callbacks
inputs = callbacks[0]['inputs']
for value in inputs:
    if value['id'] in ('pipes-connector-keys-dropdown', 'pipes-metric-keys-dropdown',
                       'pipes-location-keys-dropdown', 'pipes-tags-dropdown', 'pipes-clear-all-button'):
        assert value['allow_optional'], value
print('JSON:true')
''')
    assert result.returncode == 0, result.stderr


def test_custom_login_override_survives_disabled_native_login(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'synthetic_login', '''
from meerschaum.plugins import web_page
from dash import html
@web_page('login', login_required=False)
def login():
    return html.Div('Synthetic sign in', id='synthetic-sign-in')
''')
    result = run_python(root, plugins, '''
import os, json
os.environ['MRSM_PATCH'] = json.dumps({'api': {'permissions': {'routes': {'disabled': ['login']}}}})
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.callbacks.dashboard import _paths
from meerschaum.api import route_group_is_allowed
assert not route_group_is_allowed('login')
from meerschaum.api.host import build_app
build_app()
from meerschaum.api.dash.callbacks.custom import add_plugin_pages
add_plugin_pages()
assert '/dash/login' in _paths
assert 'Synthetic sign in' in str(_paths['/dash/login'])
print('JSON:true')
''')
    assert result.returncode == 0, result.stderr


def test_runs_hide_other_users_workers_even_with_custom_names(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, FAKE_VIEWERS + '''
from types import SimpleNamespace
from meerschaum.api.dash.jobs import job_visible_to_session
### Each session is the user of its name (Editors), `admin` an administrator.
fake_viewers({'alice': {}, 'bob': {}, 'admin': {'admin': True}})
worker = SimpleNamespace(name='custom-name', daemon=SimpleNamespace(properties={
    'mrsm_owner': 'alice', 'mrsm_execution': {'kind': 'temporary_execution'}}))
assert job_visible_to_session(worker, 'alice')
assert job_visible_to_session(worker, 'admin')
assert not job_visible_to_session(worker, 'bob')
assert not job_visible_to_session(worker, None)
print('JSON:true')
''')
    assert result.returncode == 0, result.stderr


def test_pipeline_cards_load_definitions_without_history_or_discovery(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, FAKE_VIEWERS + '''
from types import SimpleNamespace
from meerschaum.api.dash import pipeline_cards, engine
import meerschaum.jobs
fake_viewers(default={'username': 'alice', 'admin': True})
pipeline_cards.list_pipelines = lambda instance: {'daily': {'name': 'daily', 'id': 'pipeline-1',
    'revision': 2, 'targets': ['sql:source|sql:source|raw|None', 'sql:dest|sql:source|clean|None']},
    'single': {'name': 'single', 'id': 'pipeline-2', 'revision': 1, 'targets': ['sql:source|sql:source|raw|None']}}
engine.get_runs = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('Card read attempted history fetch'))
engine.run_belongs_to_pipeline = lambda run, name, definition: name == 'daily' and definition['id'] == 'pipeline-1'
engine.plan_graph = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('Card read attempted graph discovery'))
meerschaum.jobs.get_jobs = lambda **kwargs: {'daily-scheduled': SimpleNamespace(
    sysargs=['sync', 'pipeline', 'daily', '-i', 'sql:apihosttest', '--schedule', 'daily'])}
cards, alerts = pipeline_cards.get_pipeline_cards({'instance-select.value': 'sql:apihosttest'})
assert not alerts and len(cards) == 2
text = str(cards[0])
for expected in ('Pipeline(', 'sql:apihosttest', '2 Pipes · 2 instances', 'Runs', 'Manage', 'Sync',
                 '/dash/pipelines/daily?instance='):
    assert expected in text, text
assert '1 Pipe · 1 instance' in str(cards[1]), str(cards[1])
print('JSON:true')
''')
    assert result.returncode == 0, result.stderr


def test_expired_run_summary_exposes_identity_without_dead_log_links(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, '''
from meerschaum.api.dash.jobs import build_archived_run_card
card = build_archived_run_card({'name': 'execute-completed', 'ended': '2026-10-20T12:00:00Z',
    'metadata': {'sysargs': ['sync', 'pipelines', 'daily'], 'result': [True, 'Done'],
        'daemon': {'properties': {'mrsm_execution': {'run_id': 'execution-1',
            'pipeline': {'id': 'identity-1', 'name': 'daily', 'registry': 'sql:apihosttest', 'revision': 3}}}}}})
text = str(card)
assert 'execution-1' in text and 'identity-1' in text
assert 'Output retention expired' in text and 'Execution details' in text
assert '/dash/pipelines?' in text
assert '/logs/' not in text and 'Follow logs' not in text and 'Download logs' not in text
print('JSON:true')
''')
    assert result.returncode == 0, result.stderr
