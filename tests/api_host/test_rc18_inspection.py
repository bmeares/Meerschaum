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
    shown = callbacks.connector_info_click([1], 0, admin)
    assert shown[0] and 'db.invalid' in str(shown[2]) and '5432' in str(shown[2])
    assert 'Test connection' in str(shown[3]) and 'Inspect' in str(shown[3])
    assert 'modal-private-password' not in str(shown)
    for session in (ops, other, {'session-id':'forged','admin':True}, {}):
        result = callbacks.connector_info_click([1], 0, session)
        assert 'db.invalid' not in str(result) and 'Test connection' not in str(result)
        assert 'Inspect' not in str(result[3])
    with patch.object(callbacks.dash, 'ctx', SimpleNamespace(triggered_id={
            'type':'connector-inspect-button','index':'sql:modal_ops'})):
        inspected = callbacks.inspect_connector_click(1, admin)
        assert 'Supported keys' in str(inspected) and 'weather' in str(inspected)
        assert 'modal-private-password' not in str(inspected)
        for session in (ops, other, {}):
            assert 'not available' in str(callbacks.inspect_connector_click(1, session))
    # An open administrator page cannot retain settings after demotion.
    assert get_api_connector().edit_user(User('modal-admin', type='user', instance=get_api_connector()))[0]
    result = callbacks.connector_info_click([2], 0, admin)
    assert 'db.invalid' not in str(result) and 'Inspect' not in str(result[3])
    assert 'not available' in str(callbacks.inspect_connector_click(1, admin))
    with patch.object(access, '_secure', return_value=True):
        for session in (ops, admin):
            result = callbacks.connector_info_click([3], 0, session)
            assert result[1] == 'Connector' and 'not available' in str(result[2])
plugin_row = {'keys':'custom:local','type':'custom','source':'Plugin','plugin':'modal_source',
              'source_detail':None,'attributes':{},'fields':{}}
assert '/dash/plugins/installed/modal_source' in str(connectors.build_table([plugin_row], False))
assert '/dash/plugins/installed/modal_source' in str(connectors.connector_info(plugin_row, True, False))
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
assert 'source-token-keyword' in str(preview) and 'return 42' in str(preview)
text = 'fn main() { }\\n```\\n<script>window.sourceExecuted = true</script>\\n````'
preview = source.source_preview('src/plugin.rs', text)
code = preview.children[1].children.children
assert code.className == 'language-rust'
assert ''.join(value.children if hasattr(value,'children') else value for value in code.children) == text
assert all(isinstance(value,str) or value.to_plotly_json()['type']=='Span' for value in code.children)
assert 'language-toml' in str(source.source_preview('plugin.toml', '[plugin]\\nname="demo"'))
for path, text in (('notes.txt','literal <script>\\r\\nα\\tβ'), ('unknown.config','RAW = 1\\n'),
                   ('large.rs','fn value() { let x = 1; }\\n'*1500)):
    code = source.source_preview(path,text).children[1].children.children
    assert ''.join(value.children if hasattr(value,'children') else value for value in code.children) == text
    assert len(code.children) <= 5000
tree = str(source.source_tree(['src/plugin.rs','src/nested/helper.rs','plugin.toml'],'src/plugin.rs'))
assert 'plugin-source-select' in tree and 'src/nested/helper.rs' in tree
assert 'open=True' in tree and 'open=False' in tree
'''))
