"""Connector modal permissions and literal, highlighted plugin source previews."""

from tests.api_host._helpers import SIGN_IN, run_python, sandbox  # noqa: F401


def check(result):
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-6000:]


def test_connector_modals_recheck_roles_and_team_membership(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, SIGN_IN + '''
import json, os
os.environ['MRSM_PATCH'] = json.dumps({'meerschaum': {'connectors': {'sql': {
    'modal_ops': {'flavor':'postgresql', 'host':'db.invalid', 'database':'readings',
                  'schema':'weather', 'username':'reader', 'password':'modal-private-password',
                  'teams':['Ops']},
}}}})
from meerschaum.config.environment import apply_environment_patches
apply_environment_patches()
from types import SimpleNamespace
from unittest.mock import patch
from meerschaum.api import get_api_connector
from meerschaum.api.dash import dash_app, connector_listing, sessions, access
from meerschaum.api.dash.callbacks import connectors_page as callbacks
from meerschaum.api.dash.pages import connectors
from meerschaum.core import User
admin = sign_in('modal-admin', user_type='admin')
ops = sign_in('modal-ops', role='viewer', teams={'Ops':'member'})
other = sign_in('modal-other', role='viewer', teams={'Sales':'member'})
trigger = SimpleNamespace(triggered_id={'type':'connector-info-button','index':'sql:modal_ops'})
with patch.object(callbacks.dash, 'ctx', trigger), \\
     patch.object(connector_listing, 'test_connector', side_effect=AssertionError('Inspection connected.')):
    shown = callbacks.connector_info_click([1], admin)
    assert shown[0] and 'db.invalid' in str(shown[2]) and '5432' in str(shown[2])
    # The settings are shown at once; no Inspect or Close button (the X and the backdrop close).
    assert 'Supported keys' in str(shown[2]) and 'weather' in str(shown[2])
    assert 'Test connection' in str(shown[3]) and shown[4] == {}
    assert 'Inspect' not in str(shown[2:4]) and 'Close' not in str(shown[2:4])
    assert 'modal-private-password' not in str(shown)
    for session in (ops, other, {'session-id':'forged','admin':True}, {}):
        result = callbacks.connector_info_click([1], session)
        assert 'db.invalid' not in str(result) and 'Test connection' not in str(result)
        assert 'Supported keys' not in str(result) and result[4] == {'display':'none'}
    # An open administrator page cannot retain settings after demotion.
    assert get_api_connector().edit_user(User('modal-admin', type='user', instance=get_api_connector()))[0]
    result = callbacks.connector_info_click([2], admin)
    assert 'db.invalid' not in str(result) and 'Supported keys' not in str(result)
    with patch.object(access, '_secure', return_value=True):
        for session in (ops, admin):
            result = callbacks.connector_info_click([3], session)
            assert result[1] == 'Connector' and 'not available' in str(result[2])
plugin_row = {'keys':'custom:local','type':'custom','source':'Plugin','plugin':'modal_source',
              'source_detail':None,'attributes':{},'fields':{}}
assert '/dash/plugins/installed/modal_source' in str(connectors.build_table([plugin_row], False))
assert '/dash/plugins/installed/modal_source' in str(connectors.connector_info(plugin_row, True, False))
# RC28: a plugin's connector links to it once ("View source"), without restating its name.
data_plugin = {'keys':'plugin:modal_weather','type':'plugin','label':'modal_weather','source':'Plugin',
               'source_detail':'modal_weather','plugin':None,'attributes':{},'fields':{}}
def text(node):
    """The text a component renders (its string children), without its props."""
    if isinstance(node, (list, tuple)):
        return ' '.join(text(child) for child in node)
    if isinstance(node, (str, int, float)):
        return str(node)
    return text(getattr(node, 'children', None)) if node is not None else ''
listed = text(connectors.build_table([data_plugin, plugin_row], False))
assert listed.count('View source') == 2 and 'Plugin:' not in listed
assert listed.count('modal_weather') == 1 and 'modal_source' not in listed, listed
modal = connectors.connector_info(data_plugin, True, False)
assert 'modal_weather' not in text(modal) and 'Settings' not in text(modal), text(modal)
assert '/dash/plugins/installed/modal_weather' in str(modal)
# One group per type, `sql` first; a summary value is not repeated in the other settings.
sql_row = {'keys':'sql:modal_reports','type':'sql','source':'Config','source_detail':None,
           'host':'db.invalid','database':'warehouse','attributes':{'host':'db.invalid',
           'database':'warehouse','username':'reader'},'fields':{}}
table = connectors.build_table([sql_row, data_plugin], False)
assert [group.to_plotly_json()['props']['data-type'] for group in table.children[1:]] == ['sql','plugin']
modal = text(connectors.connector_info(sql_row, True, False))
assert modal.count('db.invalid') == 1 and modal.count('warehouse') == 1 and 'reader' in modal, modal
dash_app._setup_server()
assert any('connector-info-modal.is_open' in key for key in dash_app.callback_map)
'''))


def test_plugin_tree_metadata_and_highlighting_keep_source_literal(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, SIGN_IN + '''
import json, pathlib
from meerschaum.config import paths
from meerschaum.api.dash import plugin_sources as source, plugins as pages
from meerschaum.api.dash.callbacks import plugins as callbacks
admin = sign_in('tree-admin', user_type='admin')
package = pathlib.Path(paths.PLUGINS_DIR_PATHS[0]) / 'tree_source'
(package / 'helpers').mkdir(parents=True)
(package / '__init__.py').write_text('"""Read synthetic weather data."""\\nraise RuntimeError("DO_NOT_IMPORT")\\n')
(package / 'helpers' / 'readings.py').write_text('def fetch():\\n    return 42\\n')
cache = pathlib.Path(paths.ROOT_DIR_PATH) / 'cache'
cache.mkdir(exist_ok=True)
(cache / 'plugins_manifest.json').write_text(json.dumps({'plugins':{'tree_source':{
    'version':'1.2.3', 'required':['requests','plugin:helper'], 'connectors':['weather'],
    'data_functions':['fetch'], 'actions':[{'name':'show_weather'}],
}}}))
detail = str(pages.build_installed_plugin_detail('tree_source', admin))
for expected in ('Read synthetic weather data.', '1.2.3', 'Python', 'Package',
                 'Connector: weather', 'Data source', 'show weather', 'requests',
                 'plugin:helper', 'plugin-source-tree', 'helpers', 'readings.py'):
    assert expected in detail, expected
assert str(package) not in detail
preview = callbacks.show_plugin_source('helpers/readings.py',
    {'kind':'installed','name':'tree_source'}, admin)

def data_of(preview):
    return next(node.data for node in [preview, *preview.children] if getattr(node, 'id', None) == 'plugin-source-data')

def utf16(text):
    return len(text.encode('utf-16-le')) // 2

def styled(data, name):
    # The text of each run with style `name` (the browser slices by UTF-16 lengths).
    units = data['source'].encode('utf-16-le')
    out, offset = [], 0
    for length, style in data['runs']:
        if style >= 0 and data['styles'][style] == name:
            out.append(units[offset*2:(offset+length)*2].decode('utf-16-le'))
        offset += length
    return out

data = data_of(preview)
assert 'return 42' in data['source'] and 'return' in styled(data, 'keyword')
text = 'fn main() { }\\n```\\n<script>window.sourceExecuted = true</script>\\n````'
preview = source.source_preview('src/plugin.rs', text)
data = data_of(preview)
assert data['mode'] == 'rust' and 'language-rust' in str(preview)
assert data['source'] == text and sum(length for length, _ in data['runs']) == utf16(text)
### The page never carries the source as components (only as the Store's text).
assert '<script>' not in str(preview.children[:2])
assert 'language-toml' in str(source.source_preview('plugin.toml', '[plugin]\\nname="demo"'))
for path, text in (('notes.txt','literal <script>\\r\\nα\\tβ 🐍'), ('unknown.config','RAW = 1\\n'),
                   ('large.rs','fn value() { let x = 1; }\\n'*1500)):
    data = data_of(source.source_preview(path,text))
    assert data['source'] == text and sum(length for length, _ in data['runs']) == utf16(text)
### RC25: a large file stays highlighted (it used to fall back to plain text past 5000 runs).
large = data_of(source.source_preview('large.rs', 'fn value() { let x = 1; }\\n'*1500))
assert len(large['runs']) > 5000 and styled(large, 'keyword').count('fn') == 1500
### A hostile, fragmented file is highlighted up to the bound, then plain.
hostile = data_of(source.source_preview('hostile.py', '1+' * 100000))
assert len(hostile['runs']) <= source.MAX_TOKEN_RUNS + 1 and hostile['runs'][-1][1] == -1
assert sum(length for length, _ in hostile['runs']) == 200000
tree = str(source.source_tree(['src/plugin.rs','src/nested/helper.rs','plugin.toml'],'src/plugin.rs'))
assert 'plugin-source-select' in tree and 'src/nested/helper.rs' in tree
assert 'open=True' in tree and 'open=False' in tree
'''))
