#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The API host's `--no-dash` wiring and its internal-token transport (v5 design §7.3, §8.4). The
Rust server passes its flags in `MRSM_API_HOST_CONFIG`; the host serves no plugin pages or Dash
apps under `no_dash` (v4 served no Dash app at all), never serves the web terminal (the Rust
server does, §7.1), and accepts the signed internal token as `X-Mrsm-Host-Token` or as a bearer
`Authorization` header (what the console's registry fetch sends).
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
from meerschaum.api.host import dash_enabled
out['flags'] = dash_enabled()
"""
    enabled = _run(root, plugins, {'no_dash': False}, body)
    assert enabled['ping'] == {'pong': True}
    assert enabled['fragment'] == 200 and enabled['dash_layout'] == 200
    assert {p['path'] for p in enabled['registry']} == {'/dash/flagged/fragment', '/dash/flagged/legacy'}
    assert enabled['flags'] is True

    ### The default (no flag) serves the pages, as before.
    default = _run(root, plugins, {}, body)
    assert default['fragment'] == 200 and default['flags'] is True

    disabled = _run(root, plugins, {'no_dash': True, 'no_webterm': False}, body)
    assert disabled['ping'] == {'pong': True}
    assert disabled['registry'] == []
    assert disabled['fragment'] == 404 and disabled['dash_layout'] == 404
    assert disabled['flags'] is False


def test_internal_token_as_header_or_bearer(sandbox):
    root, plugins = sandbox
    body = """
import time
import jwt
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


def test_the_host_never_mounts_the_web_terminal(sandbox):
    """The Rust server serves `/webterm/<session>` and its websocket; the host asks for neither."""
    root, plugins = sandbox
    for flags in ({}, {'no_webterm': False}, {'no_dash': True, 'no_webterm': False}):
        result = run_python(root, plugins, (
            "import os, json, sys\n"
            f"os.environ['MRSM_API_HOST_CONFIG'] = json.dumps({{'mrsm_instance': 'sql:apihosttest', **{flags!r}}})\n"
            "from meerschaum.api.host import build_app\n"
            "app = build_app()\n"
            "paths = [getattr(r, 'path', '') for r in app.routes]\n"
            "print('JSON:' + json.dumps([\n"
            "    any(p.startswith(('/webterm', '/websocket')) for p in paths),\n"
            "    'meerschaum.api.routes._webterm' in sys.modules,\n"
            "    'meerschaum.api.dash.webterm' in sys.modules,\n"
            "]))\n"
        ))
        assert result.returncode == 0, result.stderr[-3000:]
        routed, route_module, dash_module = json.loads(
            [l for l in result.stdout.splitlines() if l.startswith('JSON:')][-1][5:]
        )
        assert (routed, route_module, dash_module) == (False, False, False), flags


def test_the_host_app_builds_with_its_openapi_tags(sandbox):
    """
    `meerschaum.api` passes `openapi_tags` (v4 passed the misspelled `open_api_tags`, which older
    FastAPI releases swallowed and newer ones reject with a `TypeError`, so the host never
    started). The host's own OpenAPI document now carries the tag descriptions.
    """
    root, plugins = sandbox
    body = """
document = client.get(app.openapi_url).json()
out['tags'] = [tag['name'] for tag in document.get('tags', [])]
"""
    out = _run(root, plugins, {}, body)
    assert out['tags'] == [
        'Pipes', 'Actions', 'Connectors', 'Users', 'Plugins', 'Misc', 'Version', 'MCP',
    ]
