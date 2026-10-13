#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The API host's `--no-dash` / webterm wiring and its internal-token transport (v5 design §7.3,
§8.4). The Rust server passes its flags in `MRSM_API_HOST_CONFIG`; the host serves no plugin
pages or Dash apps under `no_dash` (v4 served no Dash app at all), serves the web terminal only
when the flags allow it, and accepts the signed internal token as `X-Mrsm-Host-Token` or as a
bearer `Authorization` header (what the console's registry fetch sends).
"""

import json

from tests.api_host._helpers import run_python, write_plugin, sandbox  # noqa: F401

PLUGIN = '''
from meerschaum.plugins import api_plugin, dash_plugin, web_page

@api_plugin
def flag_routes(app):
    @app.get('/flagged/ping')
    def ping():
        return {'pong': True}

@dash_plugin
def flag_dash(dash_app):
    pass

@web_page('flagged/fragment', login_required=False)
def fragment():
    return '<p>fragment</p>'

@web_page('flagged/legacy', login_required=False)
def legacy():
    from dash import html
    return html.Div('legacy')
'''


def _run(root, plugins, config: dict, body: str):
    code = (
        "import os, json\n"
        f"os.environ['MRSM_API_HOST_CONFIG'] = json.dumps({{'mrsm_instance': 'sql:apihosttest', **{config!r}}})\n"
        "from starlette.testclient import TestClient\n"
        "from meerschaum.api.host import build_app\n"
        "app = build_app()\n"
        "client = TestClient(app)\n"
        "out = {}\n"
        + body
        + "\nprint('JSON:' + json.dumps(out))\n"
    )
    result = run_python(root, plugins, code)
    assert result.returncode == 0, result.stderr[-4000:]
    line = [l for l in result.stdout.splitlines() if l.startswith('JSON:')][-1]
    return json.loads(line[len('JSON:'):])


def test_no_dash_serves_no_plugin_pages(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'flagged', PLUGIN)
    body = """
from meerschaum.api.host._internal import web_page_registry
out['registry'] = web_page_registry()
out['ping'] = client.get('/flagged/ping').json()
out['fragment'] = client.get('/_pages/flagged/fragment').status_code
out['dash_layout'] = client.get('/dash/flagged/_dash-layout').status_code
from meerschaum.api.host import dash_enabled, webterm_enabled
out['flags'] = [dash_enabled(), webterm_enabled()]
"""
    enabled = _run(root, plugins, {'no_dash': False}, body)
    assert enabled['ping'] == {'pong': True}
    assert enabled['fragment'] == 200 and enabled['dash_layout'] == 200
    assert {p['path'] for p in enabled['registry']} == {'/dash/flagged/fragment', '/dash/flagged/legacy'}
    assert enabled['flags'] == [True, False]

    ### The default (no flag) serves the pages, as before.
    default = _run(root, plugins, {}, body)
    assert default['fragment'] == 200 and default['flags'] == [True, False]

    disabled = _run(root, plugins, {'no_dash': True, 'no_webterm': False}, body)
    assert disabled['ping'] == {'pong': True}
    assert disabled['registry'] == []
    assert disabled['fragment'] == 404 and disabled['dash_layout'] == 404
    ### The terminal needs the console.
    assert disabled['flags'] == [False, False]


def test_internal_token_as_header_or_bearer(sandbox):
    root, plugins = sandbox
    body = """
import time
from jose import jwt
from meerschaum.api._oauth2 import SECRET
from meerschaum.api.host import REGISTRY_PATH
good = jwt.encode({'mrsm_host': 'mcp', 'scopes': [], 'exp': int(time.time()) + 60}, SECRET, algorithm='HS256')
wrong_claim = jwt.encode({'other': 1, 'exp': int(time.time()) + 60}, SECRET, algorithm='HS256')
out['header'] = client.get(REGISTRY_PATH, headers={'x-mrsm-host-token': good}).status_code
out['bearer'] = client.get(REGISTRY_PATH, headers={'authorization': 'Bearer ' + good}).status_code
out['none'] = client.get(REGISTRY_PATH).status_code
out['bad_bearer'] = client.get(REGISTRY_PATH, headers={'authorization': 'Bearer nope'}).status_code
out['wrong_claim'] = client.get(REGISTRY_PATH, headers={'authorization': 'Bearer ' + wrong_claim}).status_code
out['basic'] = client.get(REGISTRY_PATH, headers={'authorization': 'Basic ' + good}).status_code
"""
    out = _run(root, plugins, {}, body)
    assert out == {
        'header': 200, 'bearer': 200, 'none': 403, 'bad_bearer': 403,
        'wrong_claim': 403, 'basic': 403,
    }


def test_webterm_routes_follow_the_flags(sandbox):
    root, plugins = sandbox
    ### The terminal process itself is not started here: `start_webterm` is replaced.
    config = {'no_dash': False, 'no_webterm': False, 'webterm_port': 18766}
    script = (
        "import os, json\n"
        f"os.environ['MRSM_API_HOST_CONFIG'] = json.dumps({{'mrsm_instance': 'sql:apihosttest', **{config!r}}})\n"
        "started = []\n"
        "import meerschaum.api\n"
        "from meerschaum.api.host import dash as host_dash\n"
        "host_dash.install_dash_surface()\n"
        "import meerschaum.api.dash.webterm as wt\n"
        "wt.start_webterm = lambda webterm_port=None: started.append(webterm_port)\n"
        "from starlette.testclient import TestClient\n"
        "from meerschaum.api.host import build_app, webterm_enabled\n"
        "app = build_app()\n"
        "client = TestClient(app)\n"
        "out = {'enabled': webterm_enabled(), 'started': started}\n"
        "r = client.get('/webterm/not-a-session')\n"
        "out['unauthorized'] = [r.status_code, 'Unauthorized' in r.text]\n"
        "print('JSON:' + json.dumps(out))\n"
    )
    result = run_python(root, plugins, script)
    assert result.returncode == 0, result.stderr[-4000:]
    out = json.loads([l for l in result.stdout.splitlines() if l.startswith('JSON:')][-1][5:])
    assert out['enabled'] is True
    assert out['started'] == [18766]
    assert out['unauthorized'] == [401, True]

    ### Off by default and under `--no-webterm`, `--no-dash` or `--no-auth`.
    for flags in ({}, {'no_webterm': True}, {'no_dash': True, 'no_webterm': False},
                  {'no_webterm': False, 'no_auth': True}):
        result = run_python(root, plugins, (
            "import os, json\n"
            f"os.environ['MRSM_API_HOST_CONFIG'] = json.dumps({{'mrsm_instance': 'sql:apihosttest', **{flags!r}}})\n"
            "from meerschaum.api.host import build_app, webterm_enabled\n"
            "app = build_app()\n"
            "print('JSON:' + json.dumps([webterm_enabled(), any(r.path.startswith('/webterm') for r in app.routes)]))\n"
        ))
        assert result.returncode == 0, result.stderr[-3000:]
        enabled, routed = json.loads([l for l in result.stdout.splitlines() if l.startswith('JSON:')][-1][5:])
        assert enabled is False and routed is False, flags
