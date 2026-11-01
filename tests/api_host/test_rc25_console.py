#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
RC25 console fixes, in process on the sandbox's SQLite instance (no server, no browser):
opening a pipe's Data tab never downloads a CSV by itself.
"""

import json

from tests.api_host._helpers import SIGN_IN, run_python, sandbox  # noqa: F401


def _json(result) -> dict:
    assert result.returncode == 0, result.stderr[-6000:]
    line = [l for l in result.stdout.splitlines() if l.startswith('JSON:')][-1]
    return json.loads(line[len('JSON:'):])


def test_data_tab_downloads_csv_only_on_a_click(sandbox):
    """
    The Data tab is a layout chunk holding Download CSV while `download-dataframe-csv` stays in
    the page skeleton, so the renderer runs the download callback when the tab opens (inputs of a
    new chunk whose outputs lie outside it count as changed, whatever `prevent_initial_call`
    says). That request carries the button's rendered `n_clicks`; only a click downloads.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SIGN_IN + '''
import json
import meerschaum as mrsm
from meerschaum.api import get_api_connector
from meerschaum.api.dash import build_dash_app, dash_app, pipe_detail
from meerschaum.api.dash.pages import pipes as pages
build_dash_app()
dash_app._setup_server()
instance = get_api_connector()
session = sign_in('rc25-csv', user_type='admin')
pipe = mrsm.Pipe('rc25', 'csv', instance=instance, columns={'datetime': 'ts'})
assert pipe.register()[0]
assert pipe.sync([{'ts': '2026-11-01 00:00:00', 'v': 1}, {'ts': '2026-11-01 01:00:00', 'v': 2}])[0]
pipe = mrsm.Pipe('rc25', 'csv', instance=instance)

button = next(
    node for node in pages._walk(pipe_detail.data_tab(pipe, allowed=True))
    if isinstance(getattr(node, 'id', None), dict) and node.id.get('type') == 'pipe-download-csv-button'
)
output, registered = next(
    (key, value) for key, value in dash_app.callback_map.items()
    if key.startswith('download-dataframe-csv.data')
)
endpoint = next(str(rule) for rule in dash_app.server.url_map.iter_rules()
                if str(rule).endswith('/_dash-update-component'))
### The renderer's JSON for a dict ID: sorted keys, no spaces.
prop_id = json.dumps(button.id, sort_keys=True, separators=(',', ':')) + '.n_clicks'

def fire(clicks):
    response = dash_app.server.test_client().post(endpoint, json={
        'output': output,
        'outputs': {'id': 'download-dataframe-csv', 'property': 'data'},
        'inputs': [[{'id': button.id, 'property': 'n_clicks', 'value': clicks}]],
        'state': [{'id': 'session-store', 'property': 'data', 'value': session}],
        'changedPropIds': [prop_id],
    })
    body = response.get_json(silent=True) or {}
    data = (body.get('response') or {}).get('download-dataframe-csv', {}).get('data') or {}
    return {'status': response.status_code, 'filename': data.get('filename'), 'content': data.get('content')}

print('JSON:' + json.dumps({
    'opened': fire(getattr(button, 'n_clicks', None)),
    'zero': fire(0),
    'clicked': fire(1),
}))
'''))
    assert out['opened']['status'] == 204, out['opened']
    assert out['zero']['status'] == 204, out['zero']
    clicked = out['clicked']
    assert clicked['status'] == 200, clicked
    assert clicked['filename'].startswith('rc25_csv ') and clicked['filename'].endswith('.csv')
    assert 'ts,v' in clicked['content']


def test_performance_names_pipes_as_python_writes_them(sandbox):
    """
    Performance names a Pipe `Pipe('plugin:noaa', 'weather')` with the Pipes page's colors, in the
    breakdown (rows the browser renders, searches and sorts) and in Recent syncs; a Pipe the viewer
    may not see keeps its placeholder.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from meerschaum.api.dash import performance
ref = 'sql:main|plugin:noaa|weather|None'
located = "sql:main|sql:x|temp|o'hare"
data = {
    'pipes': [{'pipe_ref': ref, 'summary': {'syncs': 1}}, {'pipe_ref': located, 'summary': {}}],
    'history': [{'pipe_ref': ref, 'started': '2026-11-01T00:00:00Z', 'success': True},
                {'pipe_ref': '__hidden__:1', 'started': '2026-11-01T00:00:00Z', 'success': True}],
}
groups = performance.build_groups(data, 'sql:main', '24h')
rows = groups[0].children[0].data['rows']
history = performance.build_history(data).to_plotly_json()
print('JSON:' + json.dumps({
    'labels': [row['label'] for row in rows],
    'repr': rows[0]['repr'],
    'history': json.dumps(history, default=lambda o: o.to_plotly_json()),
}))
'''))
    assert out['labels'] == ["Pipe('plugin:noaa', 'weather')", "Pipe('sql:x', 'temp', \"o'hare\")"]
    assert out['repr'] == [
        ['Pipe(', 'type'], ["'plugin:noaa'", 'connector'], [', ', 'punctuation'],
        ["'weather'", 'metric'], [')', 'type'],
    ]
    history = out['history']
    assert 'mrsm-pipe-repr' in history and 'mrsm-repr-connector' in history
    assert "'plugin:noaa'" in history and '/dash/pipes/plugin%3Anoaa/weather?instance=sql%3Amain' in history
    assert '__hidden__' not in history


def test_installed_plugins_offer_icon_actions_and_update_only_when_newer(sandbox):
    """
    Uninstall and Update are icon buttons named for their plugin; Update starts hidden and the
    update check reveals it only when the plugin's repository has a newer version (this server's
    repository included), for administrators. The detail page links back to All plugins.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SIGN_IN + '''
import json, pathlib
from unittest.mock import patch
from meerschaum.config import paths
from meerschaum.api import get_api_connector
from meerschaum.api.dash import build_dash_app, plugins as pages
build_dash_app()
from meerschaum.api.dash.callbacks import plugins as callbacks
from meerschaum.plugins._origins import write_plugin_origin
admin = sign_in('rc25-plugins-admin', user_type='admin')
reader = sign_in('rc25-plugins-reader', role='viewer')
plugin_root = pathlib.Path(paths.PLUGINS_DIR_PATHS[0])
for name, origin in (('rc25_old', 'api:elsewhere'), ('rc25_same', 'api:this_server'), ('rc25_local', None)):
    (plugin_root / name).mkdir(parents=True)
    (plugin_root / name / '__init__.py').write_text("__version__ = '1.0.0'\\n")
    if origin:
        assert write_plugin_origin(name, origin, plugin_root)
installed = {plugin['name']: plugin for plugin in pages.get_installed_plugins(check_packages=False)}
for plugin in installed.values():
    plugin['version'] = '1.0.0'
table = pages.build_installed_plugins_table(list(installed.values()), admin=True)

def walk(node):
    yield node
    children = getattr(node, 'children', None)
    for child in (children if isinstance(children, list) else [children]):
        if child is not None and not isinstance(child, str):
            yield from walk(child)

nodes = list(walk(table))
buttons = [node.to_plotly_json()['props'] for node in nodes
           if isinstance(getattr(node, 'id', None), dict) and node.id.get('type') == 'plugin-action']
slots = {node.id['index']: node.hidden for node in nodes
         if isinstance(getattr(node, 'id', None), dict) and node.id.get('type') == 'plugin-update-slot'}
store = next(node.data for node in nodes if getattr(node, 'id', None) == 'plugins-update-check')
asked = []
def remote(name, origin):
    asked.append((name, origin))
    return {'rc25_old': '1.2.0', 'rc25_same': '1.0.0'}.get(name)
with patch.object(pages, 'get_installed_plugins', return_value=list(installed.values())), \\
     patch.object(pages, 'remote_plugin_version', side_effect=remote):
    slot_ids = [{'type': 'plugin-update-slot', 'index': name} for name in sorted(slots)]
    button_ids = [{'type': 'plugin-action', 'action': 'update', 'index': name} for name in sorted(slots)]
    shown = callbacks.show_plugin_updates(store, slot_ids, button_ids, admin)
    hidden_for_reader = callbacks.show_plugin_updates(store, slot_ids, button_ids, reader)
### This server's repository is the API's own instance (no `api:this_server` connector here).
with patch.object(type(get_api_connector()), 'get_plugin_version', return_value='2.0.0', create=True) as local:
    this_server = pages.remote_plugin_version('rc25_same', 'api:this_server')
detail = pages.build_installed_plugin_detail('rc25_old', admin)
back = next(node for node in walk(detail) if getattr(node, 'href', None) == '/dash/plugins')
print('JSON:' + json.dumps({
    'buttons': [(b['id']['action'], b['id']['index'], b.get('aria-label'), b.get('title'),
                 'Update' in str(b.get('children')) or 'Uninstall' in str(b.get('children'))) for b in buttons],
    'slots': slots, 'store': sorted(store), 'asked': sorted(asked),
    'shown': shown, 'hidden_for_reader': hidden_for_reader,
    'this_server': this_server, 'local_calls': local.call_count,
    'newer': [pages.is_newer_version(a, b) for a, b in (('1.2.0', '1.0.0'), ('1.0.0', '1.0.0'),
              ('0.9', '1.0.0'), ('1.0.0rc2', '1.0.0rc1'), (None, '1.0.0'), ('1.0.0', None), ('x', '1.0'))],
    'back': [type(back).__name__, back.children, back.className],
    'detail_text': str(detail),
}))
'''))
    assert sorted(out['buttons']) == [
        ['uninstall', 'rc25_local', 'Uninstall rc25_local', 'Uninstall rc25_local', False],
        ['uninstall', 'rc25_old', 'Uninstall rc25_old', 'Uninstall rc25_old', False],
        ['uninstall', 'rc25_same', 'Uninstall rc25_same', 'Uninstall rc25_same', False],
        ['update', 'rc25_old', 'Update rc25_old', 'Update rc25_old', False],
        ['update', 'rc25_same', 'Update rc25_same', 'Update rc25_same', False],
    ]
    ### Update starts hidden; a plugin without an origin has none.
    assert out['slots'] == {'rc25_old': True, 'rc25_same': True}
    assert out['store'] == ['rc25_old', 'rc25_same']
    assert out['asked'] == [['rc25_old', 'api:elsewhere'], ['rc25_same', 'api:this_server']]
    assert out['shown'] == [[False, True], ['Update rc25_old to 1.2.0', 'Update rc25_same']]
    assert out['hidden_for_reader'] == [[True, True], ['Update rc25_old', 'Update rc25_same']]
    assert out['this_server'] == '2.0.0' and out['local_calls'] == 1
    assert out['newer'] == [True, False, False, True, False, False, False]
    assert out['back'] == ['Link', '← All plugins', 'plugin-back-link']
    assert 'Installed plugins' not in out['detail_text']


def test_plugin_source_tree_uses_folder_and_language_icons(sandbox):
    """Folders show a folder icon that opens with them; Python and Rust files show their language."""
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from meerschaum.api.dash import plugin_sources as source
tree = source.source_tree(['__init__.py', 'callbacks/console.py', 'src/lib.rs', 'plugin.toml'], '__init__.py')
text = str(tree)
print('JSON:' + json.dumps({'text': text}))
'''))
    text = out['text']
    assert 'mrsm-icon-folder' in text and 'mrsm-icon-folder-open' in text
    assert "mrsm-icon-python" in text and "mrsm-icon-rust" in text
    assert "title='Python'" in text and "title='Rust'" in text
    ### Other files keep their suffix.
    assert "children='TOML'" in text


def test_a_failed_session_read_never_sends_the_console_to_sign_in(sandbox):
    """
    Console sessions live in the instance (`mrsm_sessions`), read again every few seconds by each
    callback (the Terminal page's jobs interval ticks every 3 s). A failed read (a busy production
    instance: pool or statement timeout, dropped connection) used to read as "no session": the
    callback got `401` + `X-Mrsm-Login`, and `assets/session_guard.js` sent the whole page to
    sign in and back. A read error now keeps the session last read in this process, or answers
    `503` without the login header (the page stays); only a missing session signs out.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SIGN_IN + '''
import json
from starlette.testclient import TestClient
from meerschaum.api.host import build_app
from meerschaum.api.dash import sessions
app = build_app()
client = TestClient(app)
dependencies = client.get('/dash/_dash-dependencies').json()
### Any console callback that needs a session: the guard answers before it runs. (Pipeline Edit
### without a definition is a no-op `204`; the Terminal page's interval is guarded the same way.)
toggle = next(d for d in dependencies if d['output'] == 'pipeline-edit-mode.data')

def fire(session):
    client.cookies.clear()
    response = client.post('/dash/_dash-update-component', json={
        'output': toggle['output'], 'outputs': {'id': 'pipeline-edit-mode', 'property': 'data'},
        'inputs': [{'id': 'pipeline-edit-toggle', 'property': 'n_clicks', 'value': 1},
                   {'id': 'pipeline-draft-discard', 'property': 'n_clicks', 'value': None}],
        'changedPropIds': ['pipeline-edit-toggle.n_clicks'],
        'state': [{'id': 'pipeline-definition', 'property': 'data', 'value': None},
                  {'id': 'session-store', 'property': 'data', 'value': session}],
    })
    return [response.status_code, response.headers.get('x-mrsm-login')]

known = sign_in('rc25-session-admin', user_type='admin')
unseen = sign_in('rc25-session-other', user_type='admin')
type_ = type(sessions.get_instance_sessions_pipe())
real_get_data = type_.get_data
def failing_get_data(self, *args, **kwargs):
    # What `get_data` does when the instance cannot be read (it raises; `silent=False`).
    if getattr(self, 'target', None) == sessions.INSTANCE_SESSIONS_TARGET:
        raise RuntimeError('pool timed out while waiting for an open connection')
    return real_get_data(self, *args, **kwargs)

out = {'healthy': fire(known)}
type_.get_data = failing_get_data
### The 5 s per-process cache has expired; the session was read successfully moments ago.
sessions._session_cache.pop(known['session-id'], None)
out['failed_read_known'] = fire(known)
### A session this process has no good read of: unknown, so `503` (not a sign-in).
sessions._cache_drop(unseen['session-id'])
out['failed_read_unseen'] = fire(unseen)
type_.get_data = real_get_data
### A session that is really gone still signs out.
sessions.delete_session(known['session-id'])
out['deleted'] = fire(known)
print('JSON:' + json.dumps(out))
'''))
    assert out['healthy'][0] in (200, 204), out
    assert out['failed_read_known'][0] in (200, 204), out
    assert out['failed_read_unseen'] == [503, None], out
    assert out['deleted'] == [401, '/dash/login'], out


def test_update_checks_stay_bounded_while_a_repository_hangs(sandbox):
    """
    A repository that never answers holds at most `REMOTE_VERSION_WORKERS` threads and one request
    per plugin, however many pages check: every check answers by its deadline, nothing new starts
    while the same request is pending, queued requests are cancelled, and released requests clean
    up. Updating a plugin forgets its cached repository versions.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json, threading, time
from unittest.mock import patch
import meerschaum as mrsm
from meerschaum.api.dash import plugins as pages
release = threading.Event()
started, lock = [], threading.Lock()

class Hanging:
    def get_plugin_version(self, plugin):
        with lock:
            started.append(plugin.name)
        release.wait(30)
        return '9.9.9'

names = [f'rc25_hang_{i}' for i in range(10)]
installed = [{'name': name, 'origin': 'api:hanging', 'version': '1.0.0'} for name in names]
pages.REMOTE_VERSION_TIMEOUT_SECONDS = 0.3
threads_before = threading.active_count()
durations, results = [], []
with patch.object(pages, 'get_installed_plugins', return_value=installed), \\
     patch.object(mrsm, 'get_connector', return_value=Hanging()):
    for _ in range(25):
        began = time.monotonic()
        results.append(pages.plugin_update_versions(names))
        durations.append(time.monotonic() - began)
    peak_threads = threading.active_count() - threads_before
    started_while_blocked = len(started)
    in_flight_while_blocked = len(pages._remote_versions_in_flight)
    release.set()
    deadline = time.monotonic() + 10
    while pages._remote_versions_in_flight and time.monotonic() < deadline:
        time.sleep(0.05)
    in_flight_after = len(pages._remote_versions_in_flight)
    ### Cached now: every plugin offers 9.9.9 without another request.
    requests_before = len(started)
    after = pages.plugin_update_versions(names)
    after_requests = len(started) - requests_before
    pages.forget_remote_versions('rc25_hang_0')
    pages.plugin_update_versions(names)
    forgotten_requests = len(started) - requests_before
print('JSON:' + json.dumps({
    'max_duration': max(durations), 'results': results[:2], 'peak_threads': peak_threads,
    'started_while_blocked': started_while_blocked, 'distinct_started': len(set(started)),
    'in_flight_while_blocked': in_flight_while_blocked, 'in_flight_after': in_flight_after,
    'after': after, 'after_requests': after_requests, 'forgotten_requests': forgotten_requests,
    'workers': pages.REMOTE_VERSION_WORKERS,
}))
'''))
    workers = out['workers']
    ### Each check answers by its deadline (0.3 s) and offers nothing while the repository hangs.
    assert out['max_duration'] < 1.5, out
    assert out['results'] == [{}, {}]
    ### 25 checks of 10 plugins: only the pool's threads, which started one request each; queued
    ### requests were cancelled, so only the running ones stay in flight.
    assert 0 < out['peak_threads'] <= workers, out
    assert out['started_while_blocked'] == workers, out
    assert out['in_flight_while_blocked'] == workers, out
    ### Released, the requests finish and clean up; the next check asks only for the plugins
    ### whose request had been cancelled, and all ten offer the newer version.
    assert out['in_flight_after'] == 0, out
    assert out['after_requests'] == 10 - workers, out
    assert sorted(out['after']) == sorted(f'rc25_hang_{i}' for i in range(10))
    assert set(out['after'].values()) == {'9.9.9'}
    ### Forgetting one plugin's versions (after an update) asks again for that plugin only.
    assert out['forgotten_requests'] - out['after_requests'] == 1, out
