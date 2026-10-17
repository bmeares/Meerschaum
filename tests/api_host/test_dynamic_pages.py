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
from meerschaum.api.host.dash import get_dash_app
from meerschaum.api.host._internal import web_page_registry
from meerschaum import Plugin

app = build_app()
from meerschaum.api.dash import sessions
assert build_app() is app
client = TestClient(app)
plugin = Plugin('latepages').module
out = {'initializations': plugin.INITIALIZATIONS}
out['registry'] = {page['path']: page for page in web_page_registry()}
out['pages'] = list(get_dash_app('latepages')._mrsm_pages)
out['fragment'] = client.get('/_pages/latepages/fragment').text
out['page_only_fragment'] = client.get('/_pages/pageonly/fragment').text
out['page_only_layout'] = client.get('/dash/pageonly/_dash-layout').status_code
out['locked_anonymous'] = client.get('/_pages/latepages/locked').status_code
sessions.set_session('dynamic-session', {'username': 'example'})
client.cookies.set('mrsm-session-id', 'dynamic-session')
out['locked_authenticated'] = client.get('/_pages/latepages/locked').status_code
out['layout'] = client.get('/dash/latepages/_dash-layout').text

dash_app = get_dash_app('latepages')
assert dash_app.callback_map
callback = next(key for key in dash_app.callback_map if 'page-layout-div.children' in key)
payload = {
    'output': callback,
    'outputs': [
        {'id': 'page-layout-div', 'property': 'children'},
        {'id': 'session-store', 'property': 'data'},
        {'id': 'dbc-dark-store', 'property': 'data'},
    ],
    'inputs': [{'id': 'mrsm-location', 'property': 'pathname', 'value': '/dash/latepages/late'}],
    'changedPropIds': ['mrsm-location.pathname'],
    'state': [],
}
response = client.post('/dash/latepages/_dash-update-component', json=payload)
out['route_status'] = response.status_code
out['route_body'] = response.text
print('JSON:' + json.dumps(out))
''')
    assert result.returncode == 0, result.stderr[-4000:]
    line = next(line for line in reversed(result.stdout.splitlines()) if line.startswith('JSON:'))
    out = json.loads(line[5:])
    assert out['initializations'] == ['init']
    assert out['pages'] == ['/dash/latepages/late']
    assert out['fragment'] == '<p>dynamic fragment</p>'
    assert out['page_only_fragment'] == '<p>page-only fragment</p>'
    assert out['page_only_layout'] == 200
    assert out['locked_anonymous'] == 401
    assert out['locked_authenticated'] == 200
    assert out['registry']['/dash/latepages/late']['mode'] == 'dash'
    assert out['registry']['/dash/latepages/fragment']['mode'] == 'html'
    for component in ('session-store', 'mrsm-location', 'dbc-dark-store'):
        assert component in out['layout']
    assert out['route_status'] == 200, out['route_body']
    assert 'dynamic Dash layout' in out['route_body']
    assert 'dynamic-session' in out['route_body']
