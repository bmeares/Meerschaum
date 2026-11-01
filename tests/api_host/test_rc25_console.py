#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
RC25 console fixes, in process on the sandbox's SQLite instance (no server, no browser):
opening a pipe's Data tab never downloads a CSV by itself.
"""

import json

from tests.api_host._helpers import SIGN_IN, run_python, sandbox  # noqa: F401


def _json(result) -> dict:
    assert result.returncode == 0, result.stderr[-6000:]
    line = [l for l in result.stdout.splitlines() if l.startswith('JSON:')][-1]
    return json.loads(line[len('JSON:'):])


def test_data_tab_downloads_csv_only_on_a_click(sandbox):
    """
    The Data tab is a layout chunk holding Download CSV while `download-dataframe-csv` stays in
    the page skeleton, so the renderer runs the download callback when the tab opens (inputs of a
    new chunk whose outputs lie outside it count as changed, whatever `prevent_initial_call`
    says). That request carries the button's rendered `n_clicks`; only a click downloads.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SIGN_IN + '''
import json
import meerschaum as mrsm
from meerschaum.api import get_api_connector
from meerschaum.api.dash import build_dash_app, dash_app, pipe_detail
from meerschaum.api.dash.pages import pipes as pages
build_dash_app()
dash_app._setup_server()
instance = get_api_connector()
session = sign_in('rc25-csv', user_type='admin')
pipe = mrsm.Pipe('rc25', 'csv', instance=instance, columns={'datetime': 'ts'})
assert pipe.register()[0]
assert pipe.sync([{'ts': '2026-11-01 00:00:00', 'v': 1}, {'ts': '2026-11-01 01:00:00', 'v': 2}])[0]
pipe = mrsm.Pipe('rc25', 'csv', instance=instance)

button = next(
    node for node in pages._walk(pipe_detail.data_tab(pipe, allowed=True))
    if isinstance(getattr(node, 'id', None), dict) and node.id.get('type') == 'pipe-download-csv-button'
)
output, registered = next(
    (key, value) for key, value in dash_app.callback_map.items()
    if key.startswith('download-dataframe-csv.data')
)
endpoint = next(str(rule) for rule in dash_app.server.url_map.iter_rules()
                if str(rule).endswith('/_dash-update-component'))
### The renderer's JSON for a dict ID: sorted keys, no spaces.
prop_id = json.dumps(button.id, sort_keys=True, separators=(',', ':')) + '.n_clicks'

def fire(clicks):
    response = dash_app.server.test_client().post(endpoint, json={
        'output': output,
        'outputs': {'id': 'download-dataframe-csv', 'property': 'data'},
        'inputs': [[{'id': button.id, 'property': 'n_clicks', 'value': clicks}]],
        'state': [{'id': 'session-store', 'property': 'data', 'value': session}],
        'changedPropIds': [prop_id],
    })
    body = response.get_json(silent=True) or {}
    data = (body.get('response') or {}).get('download-dataframe-csv', {}).get('data') or {}
    return {'status': response.status_code, 'filename': data.get('filename'), 'content': data.get('content')}

print('JSON:' + json.dumps({
    'opened': fire(getattr(button, 'n_clicks', None)),
    'zero': fire(0),
    'clicked': fire(1),
}))
'''))
    assert out['opened']['status'] == 204, out['opened']
    assert out['zero']['status'] == 204, out['zero']
    clicked = out['clicked']
    assert clicked['status'] == 200, clicked
    assert clicked['filename'].startswith('rc25_csv ') and clicked['filename'].endswith('.csv')
    assert 'ts,v' in clicked['content']


def test_performance_names_pipes_as_python_writes_them(sandbox):
    """
    Performance names a Pipe `Pipe('plugin:noaa', 'weather')` with the Pipes page's colors, in the
    breakdown (rows the browser renders, searches and sorts) and in Recent syncs; a Pipe the viewer
    may not see keeps its placeholder.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from meerschaum.api.dash import performance
ref = 'sql:main|plugin:noaa|weather|None'
located = "sql:main|sql:x|temp|o'hare"
data = {
    'pipes': [{'pipe_ref': ref, 'summary': {'syncs': 1}}, {'pipe_ref': located, 'summary': {}}],
    'history': [{'pipe_ref': ref, 'started': '2026-11-01T00:00:00Z', 'success': True},
                {'pipe_ref': '__hidden__:1', 'started': '2026-11-01T00:00:00Z', 'success': True}],
}
groups = performance.build_groups(data, 'sql:main', '24h')
rows = groups[0].children[0].data['rows']
history = performance.build_history(data).to_plotly_json()
print('JSON:' + json.dumps({
    'labels': [row['label'] for row in rows],
    'repr': rows[0]['repr'],
    'history': json.dumps(history, default=lambda o: o.to_plotly_json()),
}))
'''))
    assert out['labels'] == ["Pipe('plugin:noaa', 'weather')", "Pipe('sql:x', 'temp', \"o'hare\")"]
    assert out['repr'] == [
        ['Pipe(', 'type'], ["'plugin:noaa'", 'connector'], [', ', 'punctuation'],
        ["'weather'", 'metric'], [')', 'type'],
    ]
    history = out['history']
    assert 'mrsm-pipe-repr' in history and 'mrsm-repr-connector' in history
    assert "'plugin:noaa'" in history and '/dash/pipes/plugin%3Anoaa/weather?instance=sql%3Amain' in history
    assert '__hidden__' not in history
