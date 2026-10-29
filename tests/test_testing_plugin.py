#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The `meerschaum.testing` pytest plugin (rc15 Q35): `mrsm_root`, `mrsm_instance`, `mrsm_pipe`,
`mrsm_api` and `render_page`. Each test runs a pytest session with `-p meerschaum.testing` in a
subprocess (`pytester`), so the session's root, environment and API host are its own.
"""

import importlib.util
import json
import site
import textwrap

import pytest

pytest_plugins = ['pytester']

### `pytester` gives the inner session its own `HOME`; the packages installed for the user (pytest
### itself, the API host's) stay importable.
USER_BASE = site.getuserbase()


@pytest.fixture(autouse=True)
def _user_site(monkeypatch):
    monkeypatch.setenv('PYTHONUSERBASE', USER_BASE)


def _run(pytester, *args):
    return pytester.runpytest_subprocess(
        '-p', 'meerschaum.testing', '-p', 'no:cacheprovider', '-q', *args,
    )


def test_mrsm_root_isolates_the_session(pytester, monkeypatch):
    monkeypatch.setenv('MRSM_SQL_LEAK', 'sqlite:////nowhere/leak.db')
    monkeypatch.setenv('MRSM_CONFIG', '{"meerschaum": {"instance": "sql:leak"}}')
    monkeypatch.setenv('TMUX', '/tmp/tmux-0/default,1,0')
    monkeypatch.setenv('TMUX_PANE', '%1')
    report = pytester.path / 'report.json'
    pytester.makeconftest(f'''
import json, os
def pytest_sessionfinish(session, exitstatus):
    import meerschaum.config.paths as paths
    with open({str(report)!r}, 'w') as f:
        json.dump({{
            'leak': os.environ.get('MRSM_SQL_LEAK'),
            'tmux': os.environ.get('TMUX'),
            'root': os.environ.get('MRSM_ROOT_DIR'),
            'paths_root': str(paths.ROOT_DIR_PATH),
        }}, f)
''')
    pytester.makepyfile(test_root='''
import os
import pathlib

def test_root(mrsm_root):
    import meerschaum as mrsm
    import meerschaum.config.paths as paths
    assert os.environ['MRSM_ROOT_DIR'] == str(mrsm_root.path)
    assert 'MRSM_SQL_LEAK' not in os.environ
    assert 'TMUX' not in os.environ and 'TMUX_PANE' not in os.environ
    assert pathlib.Path(paths.ROOT_DIR_PATH) == mrsm_root.path
    assert str(paths.CONFIG_DIR_PATH).startswith(str(mrsm_root.path))
    assert str(paths.VIRTENV_RESOURCES_PATH).startswith(str(mrsm_root.path))
    assert mrsm.get_config('meerschaum', 'instance') == 'sql:main'
    assert 'leak' not in (mrsm.get_config('meerschaum', 'connectors', 'sql') or {})
    assert mrsm_root.plugins_dir in mrsm_root.plugins_dirs

def test_write_plugin(mrsm_root):
    mrsm_root.write_plugin('greeter', """
from meerschaum.plugins import make_action

@make_action(name='greet everyone')
def greet(action=None, **kw):
    return True, 'hello ' + ' '.join(action or [])
""")
    from meerschaum.actions import actions
    assert 'greet_everyone' in actions
    assert actions['greet_everyone'](action=['there']) == (True, 'hello there')
    import meerschaum as mrsm
    assert mrsm.Plugin('greeter').is_installed()
    ### The native dispatcher (and the plugin worker it starts) runs in the session's root too.
    success, message = mrsm.entry(['greet', 'everyone', 'there'])
    assert success, message
    assert message == 'hello there'
''')
    import os
    outer_root = os.environ.get('MRSM_ROOT_DIR', None)
    result = _run(pytester)
    result.assert_outcomes(passed=2)
    after = json.loads(report.read_text())
    ### The session's environment and paths are restored when it ends.
    assert after['leak'] == 'sqlite:////nowhere/leak.db'
    assert after['tmux'] == '/tmp/tmux-0/default,1,0'
    assert after['root'] == outer_root
    assert 'mrsm_root' not in after['paths_root']


def test_mrsm_instance_and_mrsm_pipe(pytester):
    pytester.makepyfile(test_pipes='''
import meerschaum as mrsm

def test_pipe(mrsm_pipe, mrsm_instance):
    assert str(mrsm_instance) == 'sql:test'
    assert mrsm.get_config('meerschaum', 'instance') == 'sql:test'
    pipe = mrsm_pipe('test', 'temps', columns={'datetime': 'ts', 'id': 'station'})
    assert pipe.instance_keys == 'sql:test'
    assert pipe.get_id() is not None
    success, message = pipe.sync([
        {'ts': '2026-01-01', 'station': 'a', 'temp': 1.5},
        {'ts': '2026-01-02', 'station': 'a', 'temp': 2.5},
    ])
    assert success, message
    assert len(pipe.get_data()) == 2
    ### The default instance is the temporary one.
    assert mrsm.Pipe('test', 'temps').instance_keys == 'sql:test'
    assert len(mrsm.Pipe('test', 'temps').get_data()) == 2

def test_each_test_has_a_new_database(mrsm_instance):
    assert mrsm.get_pipes(instance=mrsm_instance, as_list=True) == []

def test_unregistered(mrsm_pipe):
    pipe = mrsm_pipe('test', 'draft', register=False)
    assert pipe.get_id() is None
''')
    result = _run(pytester)
    result.assert_outcomes(passed=3)


API_PLUGIN = '''
from meerschaum.plugins import api_plugin, dash_plugin, web_page, web_callback

@api_plugin
def routes(app):
    import fastapi
    from meerschaum.api import session_user

    @app.get('/apiplug/hello')
    def hello():
        return {'hello': 'world'}

    @app.get('/apiplug/me')
    def me(user=fastapi.Depends(session_user)):
        return {'username': user.username}

@web_page('apiplug/page')
def page():
    from dash import html
    return html.Div([html.H2('Hello page'), html.Button('Go', id='apiplug-btn'), html.Div(id='apiplug-out')])

@web_page('apiplug/fragment', login_required=False)
def fragment():
    return '<p>plain fragment</p>'

from dash import Input, Output, State

@web_callback(
    Output('apiplug-out', 'children'),
    Input('apiplug-btn', 'n_clicks'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def clicked(n_clicks, session_store):
    from meerschaum.api.dash.sessions import get_username_from_session
    return 'clicked by ' + str(get_username_from_session((session_store or {}).get('session-id')))
'''


def test_mrsm_api_and_render_page(pytester):
    ### Look the packages up without importing them in this process: `pytester` restores
    ### `sys.modules` when the test ends, so a module first imported here (FastAPI imports
    ### `pydantic.main`) is dropped while a package imported earlier keeps what it took from it
    ### (`pydantic.BaseModel`), and a later test in this process then cannot build a model.
    for module in ('fastapi', 'httpx', 'dash', 'dash_bootstrap_components', 'dash_ace', 'a2wsgi', 'jwt'):
        if importlib.util.find_spec(module) is None:
            pytest.skip(f"could not import {module!r}")
    plugins = pytester.mkdir('plugins')
    (plugins / 'apiplug').mkdir()
    (plugins / 'apiplug' / '__init__.py').write_text(textwrap.dedent(API_PLUGIN))
    pytester.makeini('''
[pytest]
mrsm_plugins_dirs = plugins
mrsm_env = MRSM_NO_AUTO_INSTALL=1
''')
    pytester.makepyfile(test_api='''
import pytest
from meerschaum.testing._api import CallbackError

def test_routes(mrsm_api):
    assert mrsm_api.client.get('/apiplug/hello').json() == {'hello': 'world'}
    assert mrsm_api.client.get('/apiplug/me').status_code == 401
    client = mrsm_api.client_for('alice')
    assert client.get('/apiplug/me').json() == {'username': 'alice'}
    assert mrsm_api.user('alice').username == 'alice'
    assert mrsm_api.instance.get_user_type(mrsm_api.user('admin')) == 'admin'

def test_render_page(render_page):
    page = render_page('/dash/apiplug/page', as_user='alice')
    assert page.status == 200 and page.kind == 'dash'
    assert 'Hello page' in page.text
    assert {'apiplug-btn', 'apiplug-out'} <= page.ids
    assert page.find('apiplug-out') is not None
    response = page.callback('apiplug-out.children', inputs={'apiplug-btn.n_clicks': 1})
    assert response == {'apiplug-out': {'children': 'clicked by alice'}}

    ### Without a session the plugin's callback is refused, and a page needing a login shows it.
    anonymous = render_page('apiplug/page')
    assert 'login-button' in anonymous.ids
    with pytest.raises(CallbackError) as error:
        anonymous.callback('apiplug-out', inputs={'apiplug-btn.n_clicks': 1})
    assert error.value.status == 401

    fragment = render_page('/dash/apiplug/fragment')
    assert fragment.kind == 'html' and fragment.status == 200
    assert 'plain fragment' in fragment
''')
    result = _run(pytester)
    result.assert_outcomes(passed=2)
