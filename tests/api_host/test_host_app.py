#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The API host app with a synthetic plugin: import surface, `api_plugin` ordering and identity,
page manifest (page-contract and legacy Dash modes), and the Dash shim's session-store population
from the `mrsm-session-id` cookie. Everything runs in a subprocess in host mode with a temporary
root, plugins directory and SQLite instance.
"""

import json

from tests.api_host._helpers import run_python, write_plugin, sandbox  # noqa: F401

PLUGIN = '''
from meerschaum.plugins import api_plugin, dash_plugin, web_page

ORDER = []

@api_plugin
def first_routes(app):
    ORDER.append('first')
    @app.get('/synth/order')
    def order():
        return {'order': ORDER}

@api_plugin
def second_routes(app):
    ORDER.append('second')

@dash_plugin
def synth_dash(dash_app):
    from dash import html, Input, Output, State
    @dash_app.callback(
        Output('synth-out', 'children'),
        Input('synth-btn', 'n_clicks'),
        State('session-store', 'data'),
        prevent_initial_call=False,
    )
    def _read(_n, session_store):
        return json.dumps(session_store or {})

import json

@web_page('synth/fragment', login_required=False, page_group='Synth Group')
def fragment():
    return '<div data-mrsm-figure="{}">hello</div>'

@web_page('synth/teapot', login_required=False, skip_navbar=True, dark_theme=False)
def teapot():
    return ('<p>short and stout</p>', 418)

@web_page('synth/locked')
def locked():
    return '<p>secret</p>'

@web_page('synth/legacy', login_required=False)
def legacy():
    from dash import html
    return html.Div([html.H1('Legacy'), html.Div(id='synth-out'), html.Button(id='synth-btn')])
'''


def test_import_surface(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, """
import inspect
import meerschaum.api as api
from meerschaum.api import (
    app, endpoints, get_api_connector, get_cache_connector, production, debug,
    ScopedAuth, CurrentScopes, get_pipe,
)
from meerschaum.api._oauth2 import SECRET
from meerschaum.api._tokens import get_token_from_authorization
from meerschaum.api import models
from meerschaum.api.dash.sessions import (
    set_session, get_user_from_session, get_username_from_session, delete_session,
    is_session_active, session_is_admin, EXPIRES_SECONDS, set_session_cookie,
)
from meerschaum.plugins import api_plugin
from fastapi import FastAPI

def init_app(app):
    @app.get('/x')
    def x():
        return 1
assert api_plugin(init_app) is init_app
bare = FastAPI()
init_app(bare)
assert any(getattr(r, 'path', '') == '/x' for r in bare.routes)
assert callable(get_token_from_authorization) and isinstance(EXPIRES_SECONDS, int)
print('ok')
""")
    assert result.returncode == 0, result.stderr[-3000:]
    assert result.stdout.strip().splitlines()[-1] == 'ok'


def test_host_runs_plugins_manifest_and_shim(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'synth', PLUGIN)
    result = run_python(root, plugins, """
import json
from starlette.testclient import TestClient
from meerschaum.api.host import build_app
from meerschaum.api.host._internal import web_page_registry
app = build_app()
client = TestClient(app)
out = {}
out['order'] = client.get('/synth/order').json()
out['fragment'] = [client.get('/_pages/synth/fragment').status_code, client.get('/_pages/synth/fragment').text]
r = client.get('/_pages/synth/teapot')
out['teapot'] = [r.status_code, r.text]
out['locked_anon'] = client.get('/_pages/synth/locked').status_code
from meerschaum.api.dash import sessions
sessions.set_session('synth-session', {'username': 'zed'})
r = client.get('/_pages/synth/locked', cookies={'mrsm-session-id': 'synth-session'})
out['locked_authed'] = [r.status_code, r.text]
out['registry'] = {p['path']: p for p in web_page_registry()}
out['shim_layout'] = client.get('/dash/synth/_dash-layout').text
payload = {
    'output': 'synth-out.children',
    'outputs': {'id': 'synth-out', 'property': 'children'},
    'inputs': [{'id': 'synth-btn', 'property': 'n_clicks', 'value': 1}],
    'changedPropIds': ['synth-btn.n_clicks'],
    'state': [{'id': 'session-store', 'property': 'data', 'value': {'session-id': 'abc'}}],
}
r = client.post('/dash/synth/_dash-update-component', json=payload)
out['callback'] = [r.status_code, r.text]
from meerschaum.api.host.dash import session_store_data
class Req:
    cookies = {'mrsm-session-id': 'synth-session'}
class Anon:
    cookies = {}
out['store_authed'] = session_store_data(Req)
out['store_anon'] = session_store_data(Anon)
print('JSON:' + json.dumps(out))
""")
    assert result.returncode == 0, result.stderr[-4000:]
    line = [l for l in result.stdout.splitlines() if l.startswith('JSON:')][-1]
    out = json.loads(line[len('JSON:'):])

    assert out['order'] == {'order': ['first', 'second']}
    assert out['fragment'] == [200, '<div data-mrsm-figure="{}">hello</div>']
    assert out['teapot'] == [418, '<p>short and stout</p>']
    assert out['locked_anon'] == 401
    assert out['locked_authed'] == [200, '<p>secret</p>']

    registry = out['registry']
    fragment = registry['/dash/synth/fragment']
    assert fragment['mode'] == 'html' and fragment['page_group'] == 'Synth Group'
    assert fragment['login_required'] is False and fragment['skip_navbar'] is False
    assert fragment['route'] == '/_pages/synth/fragment'
    teapot = registry['/dash/synth/teapot']
    assert teapot['mode'] == 'html' and teapot['skip_navbar'] is True
    assert teapot['dark_theme'] is False
    locked = registry['/dash/synth/locked']
    assert locked['mode'] == 'html' and locked['login_required'] is True
    legacy = registry['/dash/synth/legacy']
    assert legacy['mode'] == 'dash' and legacy['page_group'] == 'synth'
    assert legacy['route'] == '/dash/synth/legacy'

    layout = out['shim_layout']
    for component_id in ('session-store', 'mrsm-location', 'dbc-dark-store'):
        assert f'"{component_id}"' in layout
    assert out['callback'][0] == 200 and 'session-id' in out['callback'][1]
    assert out['store_authed'] == {'session-id': 'synth-session'}
    assert out['store_anon'] == {}
