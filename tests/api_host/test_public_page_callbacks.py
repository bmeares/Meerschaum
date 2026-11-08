#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The console warns at startup about a public Dash page (`@web_page(login_required=False)`) whose
plugin registered callbacks but made none of them public: anonymous visitors would see the page,
but its callbacks would answer only signed-in users.
"""

import json

from tests.api_host._helpers import run_python, write_plugin, sandbox  # noqa: F401

PRIVATE_ONLY = '''
from meerschaum.plugins import dash_plugin, web_page

@dash_plugin
def init(dash_app):
    from dash import Input, Output
    @dash_app.callback(Output('ppc-a-out', 'children'), Input('ppc-a-btn', 'n_clicks'))
    def a(n_clicks):
        return 'a'

@web_page('ppc/private-only', login_required=False)
def page():
    from dash import html
    return html.Div(id='ppc-a-out')
'''

WITH_PUBLIC = '''
from meerschaum.plugins import web_page, web_callback

@web_callback(output='ppc-b-out.children', inputs='ppc-b-btn.n_clicks', public=True)
def b(n_clicks):
    return 'b'

@web_callback(output='ppc-b2-out.children', inputs='ppc-b2-btn.n_clicks')
def b2(n_clicks):
    return 'b2'

@web_page('ppc/with-public', login_required=False)
def page():
    from dash import html
    return html.Div(id='ppc-b-out')
'''

NO_CALLBACKS = '''
from meerschaum.plugins import web_page

@web_page('ppc/static', login_required=False)
def page():
    from dash import html
    return html.Div('static')
'''

SIGNED_IN_ONLY = '''
from meerschaum.plugins import dash_plugin, web_page

@dash_plugin
def init(dash_app):
    from dash import Input, Output
    @dash_app.callback(Output('ppc-d-out', 'children'), Input('ppc-d-btn', 'n_clicks'))
    def d(n_clicks):
        return 'd'

@web_page('ppc/members')
def page():
    from dash import html
    return html.Div(id='ppc-d-out')
'''

CODE = '''
import json
from meerschaum.api.host import build_app

build_app()
### Imported after the console is built: importing it first would start the build mid-import.
from meerschaum.api.dash.callbacks import custom
print('RESULT' + json.dumps(custom.warn_private_callbacks_on_public_pages()))
'''


def _warned(result):
    assert result.returncode == 0, result.stderr[-3000:]
    line = next(line for line in result.stdout.splitlines() if line.startswith('RESULT'))
    return json.loads(line[len('RESULT'):])


def test_public_page_with_only_private_callbacks_is_warned(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'ppc_private_only', PRIVATE_ONLY)
    write_plugin(plugins, 'ppc_with_public', WITH_PUBLIC)
    write_plugin(plugins, 'ppc_no_callbacks', NO_CALLBACKS)
    write_plugin(plugins, 'ppc_signed_in_only', SIGNED_IN_ONLY)

    result = run_python(root, plugins, CODE)
    assert _warned(result) == ['/dash/ppc/private-only']
    ### The startup check (inside `build_app()`) printed the warning too.
    output = result.stdout + result.stderr
    assert "The page '/dash/ppc/private-only' of plugin 'ppc_private_only' is public" in output
    for quiet in ('with-public', 'static', 'members'):
        assert f"The page '/dash/ppc/{quiet}'" not in output

    ### With the session guard off, every callback is public: nothing to warn about.
    off = run_python(
        root, plugins, CODE,
        extra_env={'MRSM_CONFIG': json.dumps(
            {'api': {'dash': {'callbacks_require_session': False}}}
        )},
    )
    assert _warned(off) == []
