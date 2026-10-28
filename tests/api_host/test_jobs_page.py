"""The Jobs page: Jobs and Runs tabs, and a job's logs viewed in a modal."""
import json
from tests.api_host._helpers import AS_VIEWER, run_python, sandbox  # noqa: F401


def test_runs_is_a_tab_of_the_jobs_page(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, '''
import json
from types import SimpleNamespace
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.callbacks.dashboard import _paths, _pages
import meerschaum.api.dash.callbacks.jobs as cj
from meerschaum.api.dash import pages

def ids(component):
    found = []
    def walk(c):
        if isinstance(c, (list, tuple)):
            for x in c: walk(x)
            return
        cid = getattr(c, 'id', None)
        if cid is not None: found.append(cid)
        walk(getattr(c, 'children', None) or [])
    walk(component)
    return found

def find(component, cid):
    if isinstance(component, (list, tuple)):
        for c in component:
            found = find(c, cid)
            if found is not None: return found
        return None
    if getattr(component, 'id', None) == cid:
        return component
    children = getattr(component, 'children', None)
    return find(children, cid) if children is not None and not isinstance(children, str) else None

jobs_page = cj.render_job_page_from_url('/dash/jobs', {})
runs_page = cj.render_job_page_from_url('/dash/runs', {})
tabs = find(jobs_page, 'jobs-page-tabs')
runs_tabs = find(runs_page, 'jobs-page-tabs')

### Jobs and runs land in their own tab.
cj.get_jobs = lambda **kw: {
    'manual': SimpleNamespace(name='manual'),
    'execute-1': SimpleNamespace(name='execute-1'),
}
cj.is_automatic_job = lambda job: job.name.startswith('execute-')
cj.job_visible_to_session = lambda job, session: True
cj.build_job_card = lambda job, **kw: job.name
cj.build_cards_grid = lambda cards, n: list(cards)
print('JSON:' + json.dumps({
    'runs_path': _paths['/dash/runs'] is pages.jobs.layout,
    'nav': sorted(_pages),
    'tabs': [t.tab_id for t in tabs.children],
    'active': [tabs.active_tab, runs_tabs.active_tab],
    'content': 'jobs-page-tab-content' in ids(jobs_page),
    'modal': 'job-logs-modal' in ids(pages.jobs.layout) and 'job-logs-modal' in ids(pages.dashboard.layout),
    'jobs_tab': cj.build_jobs_tab(False, None),
    'runs_tab': cj.build_jobs_tab(True, None),
}))
''')
    assert result.returncode == 0, result.stderr
    out = json.loads(result.stdout.split('JSON:')[-1])
    assert out['runs_path']
    assert 'Jobs' in out['nav'] and 'Runs' not in out['nav']
    assert out['tabs'] == ['jobs', 'runs']
    assert out['active'] == ['jobs', 'runs']
    assert out['content'] and out['modal']
    assert out['jobs_tab'] == ['manual']
    assert out['runs_tab'] == ['execute-1']


def test_logs_menu_views_and_downloads(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, '''
import json
from meerschaum.api.dash import dash_app
from meerschaum.jobs import Job
from meerschaum.api.dash.jobs import build_job_card

job = Job('w5logs', 'show version', executor_keys='local')
card = build_job_card(job, authenticated=True)
items = []
def walk(c):
    if isinstance(c, (list, tuple)):
        for x in c: walk(x)
        return
    if type(c).__name__ == 'DropdownMenuItem': items.append(c.children)
    walk(getattr(c, 'children', None) or [])
walk(card)
print('JSON:' + json.dumps(items))
''')
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.split('JSON:')[-1]) == ['View', 'Follow logs', 'Download']


def test_logs_modal_follows_a_running_job_and_sends_only_changes(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, AS_VIEWER + '''
import json
from dash import no_update
from dash.exceptions import PreventUpdate
from meerschaum.api.dash import dash_app
import meerschaum.api.dash.callbacks.jobs as cj

state = {'text': '\\x1b[32mline 1\\x1b[0m\\n', 'status': 'running'}
as_viewer()
cj._read_job_logs = lambda name, session: {'job': name, **state, 'digest': cj._digest(state['text'])}
session = {'session-id': 'abc'}
out = {}

opened = cj.job_logs_outputs({'type': 'job-view-logs-button', 'index': 'w5'}, 1, False, None, session)
out['open'] = [opened[0], sorted(opened[2]), opened[3]['text'], opened[4]]

### A card appearing (no click) opens nothing.
try:
    cj.job_logs_outputs({'type': 'job-view-logs-button', 'index': 'w5'}, None, False, None, session)
    out['appear'] = 'updated'
except PreventUpdate:
    out['appear'] = 'prevented'

### Nothing new: no update.
try:
    cj.job_logs_outputs('job-logs-interval', 3, True, opened[2], session)
    out['same'] = 'updated'
except PreventUpdate:
    out['same'] = 'prevented'

state['text'] += 'line 2\\n'
grown = cj.job_logs_outputs('job-logs-interval', 4, True, opened[2], session)
out['grown'] = [grown[1] is no_update, grown[3]['text'], grown[4]]

state['status'] = 'stopped'
stopped = cj.job_logs_outputs('job-logs-interval', 5, True, grown[2], session)
out['stopped'] = [stopped[1] is no_update, stopped[3] is no_update, stopped[4]]

closed = cj.job_logs_outputs('job-logs-modal', None, False, stopped[2], session)
out['closed'] = [closed[2], closed[3], closed[4]]
print('JSON:' + json.dumps(out))
''')
    assert result.returncode == 0, result.stderr
    out = json.loads(result.stdout.split('JSON:')[-1])
    assert out['open'] == [True, ['digest', 'job', 'status'],
                           '\x1b[32mline 1\x1b[0m\n', False]
    assert out['appear'] == 'prevented'
    assert out['same'] == 'prevented'
    assert out['grown'] == [True, '\x1b[32mline 1\x1b[0m\nline 2\n', False]
    assert out['stopped'] == [False, True, True]
    assert out['closed'] == [None, None, True]
