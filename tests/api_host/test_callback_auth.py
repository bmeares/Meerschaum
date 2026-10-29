#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Console callbacks need a console session (rc15 Q21/Q29): the API host answers a callback request
(`POST /dash/_dash-update-component`) without a valid session with `401`, unless the callback is
public: the console's page router, login and registration callbacks, callbacks registered by a
`@dash_plugin(public=True)` function and `@web_callback(..., public=True)`. A callback is named by
the request's `output` (the key Dash runs it by), so naming a public callback runs only that one.
`api:dash:callbacks_require_session: false` and `--no-auth` turn the rule off.
"""

import json

from tests.api_host._helpers import run_python, write_plugin, sandbox  # noqa: F401

PLUGIN = '''
CALLS = []

from meerschaum.plugins import dash_plugin, web_page, web_callback

@dash_plugin
def private_dash(dash_app):
    from dash import Input, Output
    @dash_app.callback(Output('cba-private-out', 'children'), Input('cba-private-btn', 'n_clicks'))
    def private_callback(n_clicks):
        CALLS.append('private')
        return 'private ran'

@dash_plugin(public=True)
def public_dash(dash_app):
    import dash
    from dash import Input, Output
    @dash_app.callback(Output('cba-public-out', 'children'), Input('cba-public-btn', 'n_clicks'))
    def public_callback(n_clicks):
        CALLS.append('public')
        return 'public ran'

    ### `dash.callback` registers globally; it counts as registered by this function too.
    @dash.callback(Output('cba-global-out', 'children'), Input('cba-global-btn', 'n_clicks'))
    def global_callback(n_clicks):
        CALLS.append('global')
        return 'global ran'

from dash import Input, Output, State

@web_callback(Output('cba-webcb-out', 'children'), Input('cba-webcb-btn', 'n_clicks'), public=True)
def web_public(n_clicks):
    CALLS.append('web-public')
    return 'web public ran'

@web_callback(
    Output('cba-webpriv-out', 'children'),
    Input('cba-webpriv-btn', 'n_clicks'),
    State('session-store', 'data'),
)
def web_private(n_clicks, session_store):
    CALLS.append('web-private')
    return 'web private ran'

### `'id.property'` strings instead of Dash objects (Dash is imported when the console starts).
@web_callback(
    output='cba-strings-out.children',
    inputs=['cba-strings-btn.n_clicks'],
    state='session-store.data',
    public=True,
)
def strings_public(n_clicks, session_store):
    CALLS.append('strings')
    return 'strings ran ' + str(sorted((session_store or {}).keys()))

@web_page('cba/page', login_required=False)
def page():
    from dash import html
    return html.Div([
        html.Button(id=name + '-btn') for name in ('cba-private', 'cba-public', 'cba-webcb')
    ])
'''

PRELUDE = '''
import json
from starlette.testclient import TestClient
from meerschaum.api.host import build_app
from meerschaum.api.dash import sessions
from meerschaum import Plugin

app = build_app()
client = TestClient(app)
plugin = Plugin('cbauth').module
dependencies = client.get('/dash/_dash-dependencies').json()


def find(fragment):
    return next(d for d in dependencies if fragment in d['output'])


def outputs_of(output):
    if not output.startswith('..'):
        component_id, _, prop = output.partition('.')
        return {'id': component_id, 'property': prop.split('@')[0]}
    return [
        {'id': part.partition('.')[0], 'property': part.partition('.')[2].split('@')[0]}
        for part in output.strip('.').split('...')
    ]


def fire(fragment, inputs, state=None, cookies=None, output=None, outputs=None):
    dependency = find(fragment)
    payload = {
        'output': output or dependency['output'],
        'outputs': outputs if outputs is not None else outputs_of(dependency['output']),
        'inputs': inputs,
        'changedPropIds': [f"{i['id']}.{i['property']}" for i in inputs[:1]],
        'state': state or [],
    }
    client.cookies.clear()
    response = client.post('/dash/_dash-update-component', json=payload, cookies=cookies or {})
    try:
        body = response.json()
    except Exception:
        body = response.text
    return {
        'status': response.status_code,
        'body': body,
        'login_header': response.headers.get('x-mrsm-login'),
        'set_cookie': response.headers.get('set-cookie'),
    }


def click(name):
    return [{'id': name, 'property': 'n_clicks', 'value': 1}]

out = {}
'''


def _out(result):
    assert result.returncode == 0, result.stderr[-5000:]
    line = next(line for line in reversed(result.stdout.splitlines()) if line.startswith('JSON:'))
    return json.loads(line[len('JSON:'):])


def test_callbacks_need_a_session_unless_public(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'cbauth', PLUGIN)
    out = _out(run_python(root, plugins, PRELUDE + '''
out['private'] = fire('cba-private-out', click('cba-private-btn'))
out['web_private'] = fire('cba-webpriv-out', click('cba-webpriv-btn'))
out['public'] = fire('cba-public-out', click('cba-public-btn'))
out['global'] = fire('cba-global-out', click('cba-global-btn'))
out['web_public'] = fire('cba-webcb-out', click('cba-webcb-btn'))
out['strings'] = fire(
    'cba-strings-out', click('cba-strings-btn'),
    state=[{'id': 'session-store', 'property': 'data', 'value': {}}],
)
out['calls_anonymous'] = list(plugin.CALLS)

### A made-up or expired session counts as none.
out['bogus_cookie'] = fire('cba-private-out', click('cba-private-btn'), cookies={'mrsm-session-id': 'nope'})

CBA_SESSION = '00000000-0000-4000-8000-0000000cba01'
sessions.set_session(CBA_SESSION, {'username': 'alice'})
out['cookie'] = fire('cba-private-out', click('cba-private-btn'), cookies={'mrsm-session-id': CBA_SESSION})
### The console's store alone (no cookie) is a session too, and is copied into the cookie.
store = [{'id': 'session-store', 'property': 'data', 'value': {'session-id': CBA_SESSION}}]
out['store'] = fire('cba-webpriv-out', click('cba-webpriv-btn'), state=store)
out['calls'] = list(plugin.CALLS)

### Naming a public callback runs that callback only, whatever the other fields say.
del plugin.CALLS[:]
public_output = find('cba-public-out')['output']
out['spoof'] = fire(
    'cba-private-out', click('cba-private-btn'), output=public_output,
    outputs={'id': 'cba-private-out', 'property': 'children'},
)
out['spoof_calls'] = list(plugin.CALLS)
out['unknown_output'] = fire('cba-private-out', click('cba-private-btn'), output='nothing.children')

### A plugin callback fired by the renderer (its button never clicked) still needs a session.
out['plugin_untriggered'] = fire(
    'cba-private-out', [{'id': 'cba-private-btn', 'property': 'n_clicks', 'value': None}],
)
### The console's Sign out, rendered on a public plugin page's bar, fires by itself: no update.
sign_out = next(
    (d for d in dependencies if any(i['id'] == 'sign-out-button' for i in d['inputs'])), None,
)
from meerschaum.api.host._callbacks import _untriggered
pattern = {'index': 2, 'type': 'cba'}
out['untriggered'] = [
    _untriggered({'changedPropIds': [], 'inputs': []}),
    _untriggered({'changedPropIds': ['a.n_clicks'], 'inputs': [{'id': 'a', 'property': 'n_clicks', 'value': None}]}),
    _untriggered({'changedPropIds': ['a.n_clicks'], 'inputs': [{'id': 'a', 'property': 'n_clicks', 'value': 0}]}),
    _untriggered({'changedPropIds': ['{"index":2,"type":"cba"}.n_clicks'], 'inputs': [[{'id': pattern, 'property': 'n_clicks', 'value': 3}]]}),
    _untriggered({'changedPropIds': ['{"index":2,"type":"cba"}.n_clicks'], 'inputs': [[{'id': pattern, 'property': 'n_clicks', 'value': None}]]}),
]
if sign_out is not None:
    untouched = [{'id': 'sign-out-button', 'property': 'n_clicks', 'value': None}]
    out['sign_out_untriggered'] = fire(sign_out['output'], untouched, output=sign_out['output'])
    out['sign_out_clicked'] = fire(sign_out['output'], click('sign-out-button'), output=sign_out['output'])
print('JSON:' + json.dumps(out))
'''))
    assert out['plugin_untriggered']['status'] == 401
    assert out['untriggered'] == [True, True, False, False, True]
    if 'sign_out_untriggered' in out:
        assert out['sign_out_untriggered']['status'] == 204, out['sign_out_untriggered']
        assert out['sign_out_clicked']['status'] == 401
    for key in ('private', 'web_private', 'bogus_cookie', 'unknown_output'):
        assert out[key]['status'] == 401, (key, out[key])
        assert out[key]['body'] == {
            'success': False,
            'message': 'Sign in to the web console to continue.',
            'login': '/dash/login',
        }
        assert out[key]['login_header'] == '/dash/login'
    for key, text in (
        ('public', 'public ran'), ('global', 'global ran'), ('web_public', 'web public ran'),
        ('strings', 'strings ran []'),
    ):
        assert out[key]['status'] == 200, (key, out[key])
        assert text in json.dumps(out[key]['body'])
    assert sorted(out['calls_anonymous']) == ['global', 'public', 'strings', 'web-public']

    assert out['cookie']['status'] == 200 and 'private ran' in json.dumps(out['cookie']['body'])
    assert out['store']['status'] == 200 and 'web private ran' in json.dumps(out['store']['body'])
    assert 'mrsm-session-id=00000000-0000-4000-8000-0000000cba01' in (out['store']['set_cookie'] or '')
    assert 'HttpOnly' in out['store']['set_cookie']
    assert out['calls'][-2:] == ['private', 'web-private']

    ### The guard lets the request through as the public callback (which Dash then refuses for its
    ### mismatched fields); the private callback never runs.
    assert out['spoof']['status'] != 401, out['spoof']
    assert 'private' not in out['spoof_calls'], out['spoof_calls']


def test_login_page_works_without_a_session(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'cbauth', PLUGIN)
    out = _out(run_python(root, plugins, PRELUDE + '''
from meerschaum.core import User
from meerschaum.api import get_api_connector
from meerschaum.api.dash.callbacks.login import safe_next_path

conn = get_api_connector()
assert conn.register_user(User('cba-user', 'cba-password-1', instance=conn))[0]

def route(pathname, store=None):
    return fire(
        'page-layout-div',
        [
            {'id': 'mrsm-location', 'property': 'pathname', 'value': pathname},
            {'id': 'session-store', 'property': 'data', 'value': store or {}},
        ],
        state=[{'id': 'mrsm-location', 'property': 'href', 'value': 'http://localhost' + pathname}],
    )

out['router'] = route('/dash/pipes')
out['router_public_page'] = route('/dash/cba/page')

def login(href, username='cba-user', password='cba-password-1'):
    return fire(
        'login-alert-div',
        [
            {'id': 'username-input', 'property': 'n_submit', 'value': 0},
            {'id': 'password-input', 'property': 'n_submit', 'value': 0},
            {'id': 'login-button', 'property': 'n_clicks', 'value': 1},
        ],
        state=[
            {'id': 'username-input', 'property': 'value', 'value': username},
            {'id': 'password-input', 'property': 'value', 'value': password},
            {'id': 'mrsm-location', 'property': 'href', 'value': href},
            {'id': 'mrsm-location', 'property': 'pathname', 'value': href.split('localhost', 1)[1].split('?')[0]},
        ],
    )

out['login_next'] = login('http://localhost/dash/login?next=%2Fdash%2Fpipes%3Ftags%3Dx')
out['login_evil'] = login('http://localhost/dash/login?next=//evil.example/x')
out['login_in_place'] = login('http://localhost/dash/jobs')
out['login_bad'] = login('http://localhost/dash/login', password='wrong')
out['validate_username'] = fire(
    'register-username-input.valid',
    [{'id': 'register-username-input', 'property': 'value', 'value': 'someone-new'}],
)
out['next_paths'] = {
    value: safe_next_path('http://localhost/dash/login?next=' + value)
    for value in ('/dash/jobs', '//evil.example', 'https://evil.example', '/%5Cevil', 'jobs', '/dash/x?y=1')
}

### The store's session (after the cookie expired) renders the page and comes back as the cookie.
sessions.set_session('00000000-0000-4000-8000-0000000cba02', {'username': 'cba-user'})
out['router_store_only'] = route(
    '/dash/pipes', store={'session-id': '00000000-0000-4000-8000-0000000cba02'},
)
print('JSON:' + json.dumps(out))
'''))
    router = out['router']
    assert router['status'] == 200, router
    ### Anonymous visitors to a page that needs a login get the login form in place.
    assert 'login-button' in json.dumps(router['body'])
    assert out['router_public_page']['status'] == 200
    assert 'cba-private-btn' in json.dumps(out['router_public_page']['body'])

    login_next = out['login_next']
    assert login_next['status'] == 200, login_next
    response = login_next['body']['response']
    assert response['location-login']['href'] == '/dash/pipes?tags=x'
    assert 'mrsm-session-id=' in (login_next['set_cookie'] or '')
    ### Another site is never a `next`: the login page goes to the console instead.
    assert out['login_evil']['body']['response']['location-login']['href'] == '/dash'
    ### Signing in on a page that showed the form in place keeps that page.
    assert 'location-login' not in out['login_in_place']['body']['response']
    assert 'session-store' in out['login_in_place']['body']['response']
    bad = out['login_bad']['body']['response']
    assert 'session-store' not in bad and 'is-invalid' in json.dumps(bad)
    ### Self-registration is off by default (and this instance has users): the username check
    ### answers nothing at all, so it cannot tell which usernames exist.
    assert out['validate_username']['status'] == 204

    assert out['next_paths'] == {
        '/dash/jobs': '/dash/jobs',
        '//evil.example': None,
        'https://evil.example': None,
        '/%5Cevil': None,
        'jobs': None,
        '/dash/x?y=1': '/dash/x?y=1',
    }
    store_only = out['router_store_only']
    assert store_only['status'] == 200
    assert 'mrsm-session-id=00000000-0000-4000-8000-0000000cba02' in (store_only['set_cookie'] or '')
    assert 'login-button' not in json.dumps(store_only['body'])


def test_session_ids_that_are_not_uuids_are_no_session(sandbox):
    """
    The session store looks IDs up with a `params` filter, where `'_None'` is `IS NOT NULL` and
    `'_x'` is `!= 'x'`: such a cookie or `session-store` value once passed for an admin's live
    session (and signing out with it deleted every session). Only UUIDs are sessions.
    """
    root, plugins = sandbox
    write_plugin(plugins, 'cbauth', PLUGIN)
    out = _out(run_python(root, plugins, PRELUDE + '''
from meerschaum.core import User
from meerschaum.api import get_api_connector
conn = get_api_connector()
conn.register_user(User('cba-wild-admin', 'cba-password-3', type='admin', instance=conn))
ADMIN = '00000000-0000-4000-8000-0000000cba03'
sessions.set_session(ADMIN, {'username': 'cba-wild-admin'})
del plugin.CALLS[:]
for value in ('_None', '_x', '__None', 'None', '_' + ADMIN, ADMIN.upper()):
    out['cookie ' + value] = fire(
        'cba-private-out', click('cba-private-btn'), cookies={'mrsm-session-id': value},
    )
    out['store ' + value] = fire(
        'cba-webpriv-out', click('cba-webpriv-btn'),
        state=[{'id': 'session-store', 'property': 'data', 'value': {'session-id': value}}],
    )
out['calls'] = list(plugin.CALLS)

### Signing out with `'_None'` in the store (as a signed-in user) ends no other session.
sign_out = next(
    (d for d in dependencies if any(i['id'] == 'sign-out-button' for i in d['inputs'])), None,
)
if sign_out is not None:
    OTHER = '00000000-0000-4000-8000-0000000cba04'
    sessions.set_session(OTHER, {'username': 'cba-wild-admin'})
    out['sign_out'] = fire(
        sign_out['output'], click('sign-out-button'), output=sign_out['output'],
        state=[{'id': 'session-store', 'property': 'data', 'value': {'session-id': '_None'}}],
        cookies={'mrsm-session-id': OTHER},
    )
out['admin_still_signed_in'] = sessions.get_username_from_session(ADMIN)
out['session_ids'] = sorted(
    str(doc['session_id']) for doc in sessions.get_instance_sessions_pipe().get_docs() or []
)
print('JSON:' + json.dumps(out))
'''))
    for key, result in out.items():
        if not key.startswith(('cookie ', 'store ')):
            continue
        assert result['status'] == 401, (key, result)
        assert result['login_header'] == '/dash/login', (key, result)
        ### Never copied into a cookie either.
        assert not result['set_cookie'], (key, result)
    assert out['calls'] == []
    if 'sign_out' in out:
        assert out['sign_out']['status'] == 200, out['sign_out']
    assert out['admin_still_signed_in'] == 'cba-wild-admin'
    assert '00000000-0000-4000-8000-0000000cba03' in out['session_ids']
    ### Nothing but UUIDs was written.
    assert all(len(session_id) == 36 for session_id in out['session_ids']), out['session_ids']


def test_the_rule_can_be_turned_off(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'cbauth', PLUGIN)
    code = PRELUDE + '''
out['private'] = fire('cba-private-out', click('cba-private-btn'))
print('JSON:' + json.dumps(out))
'''
    environments = {
        'off': {
            'MRSM_CONFIG': json.dumps({'api': {'dash': {'callbacks_require_session': False}}}),
        },
        'no_auth': {
            'MRSM_API_HOST_CONFIG': json.dumps(
                {'mrsm_instance': 'sql:apihosttest', 'no_auth': True}
            ),
        },
    }
    for name, extra_env in environments.items():
        out = _out(run_python(root, plugins, code, extra_env=extra_env))
        assert out['private']['status'] == 200, (name, out)
        assert 'private ran' in json.dumps(out['private']['body'])


def test_dash_mcp_needs_a_session_or_a_token(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'cbauth', PLUGIN)
    code = PRELUDE + '''
from meerschaum.core import User
from meerschaum.api import get_api_connector
from meerschaum.api._oauth2 import manager
from datetime import timedelta
conn = get_api_connector()
conn.register_user(User('cba-mcp', 'cba-password-2', instance=conn))
message = {'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {
    'protocolVersion': '2025-06-18', 'capabilities': {}, 'clientInfo': {'name': 't', 'version': '1'},
}}
headers = {'Accept': 'application/json, text/event-stream'}
out['anonymous'] = client.post('/dash/_mcp', json=message, headers=headers).status_code
token = manager.create_access_token(data={'sub': 'cba-mcp'}, expires=timedelta(minutes=5))
out['token'] = client.post(
    '/dash/_mcp', json=message, headers={**headers, 'Authorization': 'Bearer ' + token},
).status_code
ghost = manager.create_access_token(data={'sub': 'nobody-here'}, expires=timedelta(minutes=5))
out['ghost'] = client.post(
    '/dash/_mcp', json=message, headers={**headers, 'Authorization': 'Bearer ' + ghost},
).status_code
print('JSON:' + json.dumps(out))
'''
    out = _out(run_python(root, plugins, code, extra_env={
        'MRSM_CONFIG': json.dumps({'api': {'dash': {'mcp': {'enabled': True}}}}),
    }))
    assert out['anonymous'] == 401
    assert out['ghost'] == 401
    assert out['token'] not in (401, 403, 404), out
