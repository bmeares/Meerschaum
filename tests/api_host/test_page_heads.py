#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`@web_page(theme=..., head=[...])` and `add_head(...)` (rc15 Q23): the console's index carries the
`add_head()` tags and the script that applies each page's theme and tags; a page-contract page's
document loads exactly one theme stylesheet and its tags; `theme='light'` and custom themes turn
the `dbc_dark` overrides off unless asked for.
"""

import json
import re

from tests.api_host._helpers import run_python, write_plugin, sandbox  # noqa: F401

PLUGIN = '''
from meerschaum.plugins import web_page, add_head

add_head(
    '<meta name="rc15-global" content="yes">',
    {'tag': 'link', 'rel': 'preconnect', 'href': 'https://fonts.example'},
)

@web_page('heads/light', theme='light', login_required=False)
def light():
    from dash import html
    return html.Div('light page')

@web_page(
    'heads/custom',
    theme='/static/css/bootstrap_light.min.css?custom=1',
    head=[
        {'tag': 'meta', 'name': 'rc15-page', 'content': 'custom'},
        '<script>window.rc15PageScript = (window.rc15PageScript || 0) + 1;</script>',
    ],
    login_required=False,
)
def custom():
    from dash import html
    return html.Div('custom page')

@web_page('heads/dark-custom', theme='https://cdn.example/cyborg.css', dark_theme=True)
def dark_custom():
    from dash import html
    return html.Div('dark custom')

@web_page(
    'heads/fragment',
    theme='https://cdn.example/lux.css',
    head=['<meta name="rc15-fragment" content="1">'],
    login_required=False,
)
def fragment():
    return '<p>fragment page</p>'

@web_page('heads/plain', login_required=False)
def plain():
    return '<p>plain fragment</p>'
'''


def test_page_themes_and_head_tags(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'heads', PLUGIN)
    result = run_python(root, plugins, '''
import json
from starlette.testclient import TestClient
from meerschaum.api.host import build_app
from meerschaum.plugins import _plugin_endpoints_to_pages

app = build_app()
from meerschaum.api.dash.callbacks.dashboard import _paths_without_dbc_dark
client = TestClient(app)
out = {
    'index': client.get('/dash/').text,
    'index_page': client.get('/dash/heads/custom').text,
    'fragment_document': client.get('/_pages/heads/fragment?frame=1').text,
    'plain_document': client.get('/_pages/heads/plain?frame=1').text,
    'without_dbc_dark': sorted(path for path in _paths_without_dbc_dark if 'heads' in path),
    'pages': {
        path: {'theme': page['theme'], 'dark_theme': page['dark_theme'], 'head': page['head']}
        for pages in _plugin_endpoints_to_pages.values()
        for path, page in pages.items()
        if 'heads' in path
    },
}
print('JSON:' + json.dumps(out))
''')
    assert result.returncode == 0, result.stderr[-5000:]
    line = next(line for line in reversed(result.stdout.splitlines()) if line.startswith('JSON:'))
    out = json.loads(line[len('JSON:'):])

    pages = out['pages']
    assert pages['/dash/heads/light'] == {'theme': 'light', 'dark_theme': False, 'head': []}
    assert pages['/dash/heads/custom']['theme'] == '/static/css/bootstrap_light.min.css?custom=1'
    assert pages['/dash/heads/custom']['dark_theme'] is False
    assert pages['/dash/heads/custom']['head'][0] == '<meta name="rc15-page" content="custom">'
    assert pages['/dash/heads/dark-custom']['dark_theme'] is True
    assert pages['/dash/heads/plain'] == {'theme': 'dark', 'dark_theme': True, 'head': []}
    assert out['without_dbc_dark'] == [
        '/dash/heads/custom', '/dash/heads/fragment', '/dash/heads/light',
    ]

    for index in (out['index'], out['index_page']):
        head = index.split('</head>', 1)[0]
        assert '<meta name="rc15-global" content="yes">' in head
        assert '<link rel="preconnect" href="https://fonts.example">' in head
        ### The console's two theme stylesheets stay; the script swaps them per page.
        assert 'id="mrsm-theme-dark"' in head and 'id="mrsm-theme-light"' in head
        match = re.search(r'<script id="mrsm-page-heads">\(function\(\)\{\nvar pages=(.*?);\n', head)
        assert match, head[-3000:]
        registry = json.loads(match.group(1).replace('<\\/', '</'))
        assert registry['/dash/heads/custom'] == {
            'theme': '/static/css/bootstrap_light.min.css?custom=1',
            'head': [
                '<meta name="rc15-page" content="custom">',
                '<script>window.rc15PageScript = (window.rc15PageScript || 0) + 1;</script>',
            ],
        }
        ### A page-contract page's tags are its document's; the console only follows its theme.
        assert registry['/dash/heads/fragment'] == {'theme': 'https://cdn.example/lux.css', 'head': []}
        assert '/dash/heads/light' not in registry and '/dash/heads/plain' not in registry
        ### No `</script>` can end the script early.
        assert '</script>' not in match.group(1)

    document = out['fragment_document']
    head = document.split('</head>', 1)[0]
    assert head.count('rel="stylesheet" href="https://cdn.example/lux.css"') == 1
    assert 'bootstrap.min.css' not in head and 'dbc_dark.css' not in head
    assert '<meta name="rc15-fragment" content="1">' in head
    assert '<meta name="rc15-global" content="yes">' in head
    assert '<p>fragment page</p>' in document

    plain = out['plain_document'].split('</head>', 1)[0]
    assert '/static/css/bootstrap.min.css' in plain and '/static/css/dbc_dark.css' in plain
    assert 'bootstrap_light' not in plain
