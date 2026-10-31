"""Compressed Dash bundles revalidate without downloading unchanged JavaScript again."""

from tests.api_host._helpers import run_python, sandbox  # noqa: F401


def test_component_bundles_accept_weak_etags_and_reload_changed_content(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, '''
from unittest.mock import patch
from dash import dcc
from meerschaum.api.dash import dash_app
dash_app.layout = dcc.Graph(figure={'data': []})
client = dash_app.server.test_client()
prefix = dash_app.config.routes_pathname_prefix
assert client.get(prefix).status_code == 200
path = prefix + '_dash-component-suites/plotly/package_data/plotly.min.js'
first = client.get(path)
assert first.status_code == 200 and len(first.data) > 100000, (first.status_code, first.data[:600])
etag = first.headers['ETag'].removeprefix('W/')
for value in (etag, 'W/' + etag, '"other-bundle", W/' + etag):
    response = client.get(path, headers={'If-None-Match': value})
    assert response.status_code == 304 and not response.data, (value, response.status_code)
head = client.head(path, headers={'If-None-Match': 'W/' + etag})
assert head.status_code == 304 and not head.data
with patch('pkgutil.get_data', return_value=b'console.log("new bundle");'):
    changed = client.get(path, headers={'If-None-Match': 'W/' + etag})
assert changed.status_code == 200 and changed.data == b'console.log("new bundle");'
assert changed.headers['ETag'] != first.headers['ETag']
''')
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-4000:]
