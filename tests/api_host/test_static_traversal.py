#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Wave 18 (FX7, RV2 follow-up): the static-asset routes of the API host cannot be walked out of their
directories. Dash's `/dash/assets/*`, the plugin assets (`/dash/_plugin-assets/<plugin>/*`, Flask's
`send_from_directory`), the console's `/static/*`, and the page-contract assets
(`/_pages/_assets/*`) are asked for files outside their directories with every spelling of `..`
(literal, percent-encoded, double-encoded, backslashes, doubled dots, NUL), by the decoded path an
ASGI server hands the app. A canary file beside each directory must never come back.
"""

import json

from tests.api_host._helpers import run_python, write_plugin, sandbox  # noqa: F401
from tests.api_host.test_console_support import _json


ASSET_PLUGIN = '''
from meerschaum.plugins import dash_plugin

@dash_plugin
def init(app):
    pass
'''


def test_asset_routes_do_not_leave_their_directories(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'assetplug', ASSET_PLUGIN)
    (plugins / 'assetplug' / 'assets').mkdir(exist_ok=True)
    (plugins / 'assetplug' / 'assets' / 'ok.css').write_text('.ok { color: red }\n')
    (plugins / 'assetplug' / 'CANARY_PLUGIN_SOURCE.txt').write_text('canary-beside-the-assets')
    out = _json(run_python(root, plugins, '''
import asyncio, json, os, pathlib
from urllib.parse import quote
from meerschaum.api.host import build_app
import meerschaum.api.dash as dash_package
import meerschaum.api as api_package

app = build_app()
plugins = pathlib.Path(os.environ['MRSM_PLUGINS_DIR'])
dash_dir = pathlib.Path(dash_package.__file__).parent
canaries = {
    'canary-beside-the-assets',
    'def run_action',                   # meerschaum/api/dash/execute.py, one level above assets/
    'root:x:0:0',                       # /etc/passwd
    '[project]',                        # pyproject.toml
}

async def get(path):
    sent = []
    scope = {
        'type': 'http', 'asgi': {'version': '3.0'}, 'http_version': '1.1', 'method': 'GET',
        'scheme': 'http', 'path': path, 'raw_path': path.encode(), 'query_string': b'',
        'root_path': '', 'headers': [(b'host', b'localhost')], 'client': ('127.0.0.1', 1),
        'server': ('127.0.0.1', 80),
    }
    async def receive():
        return {'type': 'http.request', 'body': b'', 'more_body': False}
    async def send(message):
        sent.append(message)
    await app(scope, receive, send)
    status = next((m['status'] for m in sent if m['type'] == 'http.response.start'), None)
    body = b''.join(m.get('body', b'') for m in sent if m['type'] == 'http.response.body')
    return status, body.decode('utf-8', 'replace')

DOTS = [
    '..', '../..', '../../..', '../../../..', '../../../../../../../../etc/passwd',
    '%2e%2e', '%2e%2e/%2e%2e', '..%2f..%2f..%2f..%2fetc%2fpasswd', '%2e%2e%2f%2e%2e%2fetc%2fpasswd',
    '%252e%252e/%252e%252e/etc/passwd', '....//....//....//etc/passwd', '..;/..;/etc/passwd',
    '..\\\\..\\\\etc\\\\passwd', '%5c..%5c..%5cetc%5cpasswd', '..%00/', 'ok.css%00/../../x',
    '/etc/passwd', '//etc/passwd', 'C:/Windows/win.ini', '.%2e/%2e./etc/passwd',
]
bases = [
    '/dash/assets/', '/dash/_plugin-assets/assetplug/', '/static/', '/_pages/_assets/',
    '/dash/_dash-component-suites/', '/dash/_favicon.ico/',
]
out = {'served': {}, 'leaks': [], 'statuses': {}, 'ok_not_index': [], 'errors': []}
# Controls: the legitimate files are served.
out['served']['ok_css'] = (asyncio.run(get('/dash/_plugin-assets/assetplug/ok.css'))[0])
for base in bases:
    statuses = set()
    for dots in DOTS:
        for path in (base + dots, base + dots.replace('%2e', '.').replace('%2f', '/').replace('%5c', '\\\\')):
            status, body = asyncio.run(get(path))
            statuses.add(status)
            ### Dash answers an unknown path under `/dash` with its index page (the router of the
            ### single-page app); that is not a file.
            ### Dash's own component route answers a path that is not one of the registered files with
            ### a `DependencyException` (a 500, never a file); every other route must not fail.
            if status == 500 and 'component-suites' not in base:
                out['errors'].append([path, body[:200]])
            if status == 200 and '_dash-config' not in body:
                out['ok_not_index'].append([path, body[:80]])
            for canary in canaries:
                if canary in body:
                    out['leaks'].append([path, canary])
    out['statuses'][base] = sorted(s for s in statuses if s is not None)
print('JSON:' + json.dumps(out))
'''))
    assert out['served']['ok_css'] == 200, out
    assert out['leaks'] == [], out['leaks']
    ### A 200 for a path that leaves the directory is only ever the console's index page.
    assert out['ok_not_index'] == [], out['ok_not_index']
    assert out['errors'] == [], out['errors']
    for base, statuses in out['statuses'].items():
        assert all(status in (200, 301, 307, 308, 400, 403, 404, 405, 422, 500) for status in statuses), (
            base, statuses,
        )
