"""Pages declared inside Dash initializers are mounted after plugin initialization."""

import json

from tests.api_host._helpers import run_python, write_plugin, sandbox  # noqa: F401


DYNAMIC_PLUGIN = '''
from meerschaum.plugins import api_plugin, dash_plugin, web_page

INITIALIZATIONS = []

@api_plugin
def early_registry(app):
    # A plugin may inspect the manifest before Dash callbacks register their pages.
    from meerschaum.api.host.dash import get_page_contract_pages
    get_page_contract_pages()

@dash_plugin
def init(app):
    from dash import html
    INITIALIZATIONS.append('init')

    @web_page('latepages/late', login_required=False)
    def late():
        return html.Div('dynamic Dash layout')

    @web_page('latepages/fragment', login_required=False)
    def fragment():
        return '<p>dynamic fragment</p>'

    @web_page('latepages/locked')
    def locked():
        return '<p>dynamic private fragment</p>'
'''

PAGE_ONLY_PLUGIN = '''
from meerschaum.plugins import web_page

@web_page('pageonly/legacy', login_required=False)
def legacy():
    from dash import html
    return html.Div('page-only layout')

@web_page('pageonly/fragment', login_required=False)
def fragment():
    return '<p>page-only fragment</p>'
'''


def test_pages_registered_during_dash_initialization(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'latepages', DYNAMIC_PLUGIN)
    write_plugin(plugins, 'pageonly', PAGE_ONLY_PLUGIN)
    result = run_python(root, plugins, '''
import json
from starlette.testclient import TestClient
from meerschaum.api.host import build_app
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.callbacks.dashboard import _paths, _required_login
from meerschaum.api.host._internal import web_page_registry
from meerschaum import Plugin

app = build_app()
from meerschaum.api.dash import sessions
assert build_app() is app
client = TestClient(app)
plugin = Plugin('latepages').module
out = {'initializations': plugin.INITIALIZATIONS}
out['registry'] = {page['path']: page for page in web_page_registry()}
out['paths'] = sorted(path for path in _paths if 'latepages' in path or 'pageonly' in path)
out['required'] = sorted(path for path in _required_login if 'latepages' in path)
out['fragment'] = client.get('/_pages/latepages/fragment').text
out['framed'] = client.get('/_pages/latepages/fragment?frame=1').text
out['page_only_fragment'] = client.get('/_pages/pageonly/fragment').text
out['locked_anonymous'] = client.get('/_pages/latepages/locked').status_code
sessions.set_session('dynamic-session', {'username': 'example'})
client.cookies.set('mrsm-session-id', 'dynamic-session')
out['locked_authenticated'] = client.get('/_pages/latepages/locked').status_code
out['layout'] = client.get('/dash/_dash-layout').text

def route(pathname):
    callback = next(key for key in dash_app.callback_map if 'page-layout-div.children' in key)
    outputs = []
    for part in callback.strip('.').split('...'):
        component_id, _, prop = part.partition('.')
        outputs.append({'id': component_id, 'property': prop.split('@')[0]})
    payload = {
        'output': callback,
        'outputs': outputs,
        'inputs': [
            {'id': 'mrsm-location', 'property': 'pathname', 'value': pathname},
            {'id': 'session-store', 'property': 'data', 'value': {'session-id': 'dynamic-session'}},
        ],
        'changedPropIds': ['mrsm-location.pathname'],
        'state': [{'id': 'mrsm-location', 'property': 'href', 'value': 'http://localhost' + pathname}],
    }
    response = client.post('/dash/_dash-update-component', json=payload)
    return response.status_code, response.text

out['route'] = route('/dash/latepages/late')
out['fragment_route'] = route('/dash/latepages/fragment')
out['page_only_route'] = route('/dash/pageonly/legacy')
print('JSON:' + json.dumps(out))
''')
    assert result.returncode == 0, result.stderr[-4000:]
    line = next(line for line in reversed(result.stdout.splitlines()) if line.startswith('JSON:'))
    out = json.loads(line[5:])
    assert out['initializations'] == ['init']
    ### Pages declared inside a `@dash_plugin` function are pages of the one console, at v4 paths.
    assert out['paths'] == [
        '/dash/latepages/fragment', '/dash/latepages/late', '/dash/latepages/locked',
        '/dash/pageonly/fragment', '/dash/pageonly/legacy',
    ]
    assert out['required'] == ['/dash/latepages/locked']
    assert out['fragment'] == '<p>dynamic fragment</p>'
    assert out['page_only_fragment'] == '<p>page-only fragment</p>'
    ### `?frame=1` is the document the console's iframe loads.
    assert out['framed'].startswith('<!DOCTYPE html>') and '<p>dynamic fragment</p>' in out['framed']
    assert '/static/contract/contract.js' in out['framed']
    assert out['locked_anonymous'] == 401
    assert out['locked_authenticated'] == 200
    assert out['registry']['/dash/latepages/late']['mode'] == 'dash'
    assert out['registry']['/dash/latepages/fragment']['mode'] == 'html'
    for component in ('session-store', 'mrsm-location', 'dbc-dark-store'):
        assert component in out['layout']
    status, body = out['route']
    assert status == 200, body
    assert 'dynamic Dash layout' in body
    ### A page-contract page is an iframe of its document.
    status, body = out['fragment_route']
    assert status == 200 and 'frame=1' in body and 'mrsm-contract-frame' in body
    assert 'latepages' in body and 'Iframe' in body
    status, body = out['page_only_route']
    assert status == 200 and 'page-only layout' in body
