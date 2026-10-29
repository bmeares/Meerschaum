"""rc16 Lineage routing, connector inspection, and bounded plugin source readers."""

from tests.api_host._helpers import AS_VIEWER, SIGN_IN, run_python, sandbox  # noqa: F401


def check(result):
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-6000:]


def test_standalone_lineage_never_loads_a_none_pipeline(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, AS_VIEWER + '''
from urllib.parse import urlencode
from dash.exceptions import PreventUpdate
import meerschaum as mrsm
from meerschaum.api import get_api_connector
from meerschaum.api.dash import pipe_detail, engine
from meerschaum.api.dash.pages import pipelines as pages
from meerschaum.api.dash.callbacks import pipeline_management as management, pipelines, pipeline_pages
as_viewer()
c = get_api_connector()
parent = mrsm.Pipe('rc16', 'parent', instance=c)
child = mrsm.Pipe('rc16', 'child', instance=c,
                  parameters={'parents': ["Pipe('rc16', 'parent')"]})
assert parent.register()[0] and child.register()[0]
ref = str(c) + '|rc16|child|None'
for legacy in ({'pipe': ref}, {'pipe': ref, 'pipeline': 'None'}, {'pipe': ref, 'name': 'null'}):
    marker = pages.route('/dash/pipelines', 'http://localhost/dash/pipelines?' + urlencode(legacy))[0]
    target = marker.to_plotly_json()['props']['data-mrsm-redirect']
    assert target.startswith('/dash/pipelines/explore?'), target
    assert '/pipelines/None' not in target and 'pipeline=' not in target
layout = pages.route('/dash/pipelines/explore', 'http://localhost/dash/pipelines/explore?' + urlencode({'pipe': ref}))
assert any(getattr(node, 'id', None) == 'pipeline-explore-page' for node in pages._walk(layout))
assert not any(getattr(node, 'id', None) == 'pipeline-saved-name' for node in pages._walk(layout))
link = next(node for node in pages._walk(pipe_detail.lineage_tab(child)) if getattr(node, 'className', '') == 'btn btn-sm btn-secondary pipes-explore-link')
assert link.href.startswith('/dash/pipelines/explore?pipe=') and '&instance=' in link.href
canvas, title, save = pipeline_pages.render_explore('/dash/pipelines/explore', '?' + urlencode({'pipe': ref}), {})
text = str(canvas)
assert 'rc16' in text and 'parent' in text and 'child' in text, text
assert 'unavailable' not in text and "Pipeline('None')" not in text
for path in ('/dash/pipelines/explore', '/dash/pipelines/new'):
    assert management.name_from_path(path, '?pipeline=None') is None
    try:
        management.read_selection(path, None, '?pipeline=None', 0, {})
    except PreventUpdate:
        pass
    else:
        raise AssertionError('A standalone page loaded saved Pipeline state.')
    try:
        pipelines.render_pipeline_graph(path, {'name': 'stale'}, None, 0, {}, '', {})
    except PreventUpdate:
        pass
    else:
        raise AssertionError('A standalone page loaded a saved Pipeline graph.')
'''))


def test_connector_inspection_masks_secrets_and_obeys_team_visibility_without_connecting(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
import json, os
os.environ['MRSM_PATCH'] = json.dumps({'meerschaum': {'connectors': {'sql': {
    'rc16_ops': {'flavor':'postgresql', 'host':'db.invalid', 'database':'readings', 'schema':'weather',
                 'username':'demo', 'password':'secret-main-321', 'teams':['Ops'],
                 'options': {'sslmode':'require','api_key':'nested-token-654'},
                 'uri':'postgresql://demo:secret-main-321@db.invalid/readings?sslmode=require&token=query-secret-987'},
    'rc16_sales': {'flavor':'sqlite', 'database':'sales-private.db', 'teams':['Sales']},
}}}})
from meerschaum.config.environment import apply_environment_patches
apply_environment_patches()
import meerschaum as mrsm
from meerschaum.api.dash import access, connector_listing as listing
from meerschaum.api.dash.pages import connectors
from meerschaum.connectors.sql import SQLConnector
from meerschaum.connectors import connectors as runtime
from types import SimpleNamespace
from unittest.mock import patch
admin = access.make_viewer('admin', admin=True)
ops = access.make_viewer('ops', role='viewer', teams={'Ops':'member'})
# A cached / ad hoc object with the same keys cannot relax the configured team restriction.
runtime.setdefault('sql', {})['rc16_sales'] = SimpleNamespace(teams=['Ops'],database='PRIVATE_RUNTIME')
with patch.object(mrsm, 'get_connector', side_effect=AssertionError('Inspection connected.')) as factory, \
     patch.object(SQLConnector, '__init__', side_effect=AssertionError('Inspection constructed SQL.')) as constructor:
    rows = {row['keys']: row for row in listing.list_connectors(viewer=ops)}
    assert factory.call_count == 0 and constructor.call_count == 0
assert 'sql:rc16_ops' in rows and 'sql:rc16_sales' not in rows
assert not listing.connector_team_allows('sql:rc16_sales', ops)
row = rows['sql:rc16_ops']
assert row['schema'] == 'weather' and row['host'] == 'db.invalid'
assert row['port'] == 5432 and row['source'] == 'Environment'
assert {'host','username','password','database','flavor'} <= set(row['fields']['required'])
assert {'schema','uri','teams'} <= set(row['fields']['supported'])
shown = str(connectors.build_table([row], False, details=True))
assert 'connector-info-button' in shown and 'Inspect' not in shown
assert 'Supported keys' not in shown
body, footer = connectors.connector_info(row, details=True, testable=True)
shown += str(body) + str(footer) + str(connectors.connector_inspection(row))
assert 'Inspect' in shown and 'Test connection' in shown
assert 'Supported keys' in shown and 'weather' in shown
for secret in ('secret-main-321','nested-token-654','query-secret-987'):
    assert secret not in shown and secret not in json.dumps(rows), secret
public = str(connectors.build_table([row], False, details=False))
for private in ('weather','db.invalid','Supported keys','password','nested-token-654'):
    assert private not in public, private
all_rows = {row['keys'] for row in listing.list_connectors(viewer=admin)}
assert 'sql:rc16_sales' in all_rows
assert not listing.connector_visible({'teams':['Ops']}, access.ANONYMOUS)
assert listing.list_connectors(viewer=access.ANONYMOUS) == []
assert listing.connector_visible({'teams':['Everyone']}, ops)
assert not listing.connector_visible({'teams':[]}, ops)
'''))


def test_plugin_source_is_bounded_read_only_and_freshly_authorized(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, SIGN_IN + '''
import io, os, pathlib, tarfile
from meerschaum.api import get_api_connector
from meerschaum.config import paths
from meerschaum.core import Plugin, User
from meerschaum.api.dash import plugin_sources as source
admin = sign_in('rc16-source-admin', user_type='admin')
reader = sign_in('rc16-source-reader', role='viewer')
denied = sign_in('rc16-source-denied', role='custom', permissions=[])
plugin_root = pathlib.Path(paths.PLUGINS_DIR_PATHS[0])
package = plugin_root / 'rc16_source'
package.mkdir()
code = "raise RuntimeError('source must never execute')\\n"
(package / '__init__.py').write_text(code)
(package / 'helper.py').write_text('VALUE = 42\\n')
(package / 'binary.py').write_bytes(b'a\\0b')
(package / 'huge.py').write_bytes(b'x' * (source.MAX_FILE_BYTES + 1))
outside = pathlib.Path(paths.ROOT_DIR_PATH) / 'outside.py'
outside.write_text('PRIVATE_OUTSIDE = 1')
(package / 'linked.py').symlink_to(outside)
(package / 'linked_dir').symlink_to(outside.parent, target_is_directory=True)
(plugin_root / 'rc16_alias').symlink_to(package, target_is_directory=True)
assert '__init__.py' in source.source_files('installed','rc16_source',admin)
assert 'linked.py' not in source.source_files('installed','rc16_source',admin)
assert source.read_source('installed','rc16_source','__init__.py',admin) == code
from unittest.mock import patch
with patch.object(source.os, 'supports_dir_fd', set()):
    assert source.read_source('installed','rc16_source','__init__.py',admin) == code
def refused(call, error=(ValueError, PermissionError, OSError)):
    try: call()
    except error: return
    raise AssertionError('Unsafe source request succeeded.')
with patch.object(source.os, 'supports_dir_fd', set()):
    refused(lambda: source._read_file(package, 'linked.py'))
for file in ('../outside.py','/outside.py','linked.py','linked_dir/outside.py','binary.py','huge.py','..\\\\outside.py'):
    refused(lambda file=file: source.read_source('installed','rc16_source',file,admin))
refused(lambda: source.source_files('installed','rc16_alias',admin))
for session in (reader, denied, {}):
    refused(lambda session=session: source.read_source('installed','rc16_source','__init__.py',session), PermissionError)

c = get_api_connector()
plugin = Plugin('rc16_repository', user_id=c.get_user_id(User('rc16-source-admin', instance=c)))
assert c.register_plugin(plugin)[0]
archives = pathlib.Path(paths.PLUGINS_ARCHIVES_RESOURCES_PATH)
archives.mkdir(parents=True, exist_ok=True)
with tarfile.open(archives / 'rc16_repository.tar.gz', 'w:gz') as archive:
    for name, content in [('rc16_repository/__init__.py', code), ('../outside.py','ESCAPE'), ('/absolute.py','ESCAPE')]:
        data=content.encode(); info=tarfile.TarInfo(name); info.size=len(data)
        archive.addfile(info, io.BytesIO(data))
    info=tarfile.TarInfo('rc16_repository/link.py'); info.type=tarfile.SYMTYPE; info.linkname=str(outside)
    archive.addfile(info)
files = source.source_files('repository','rc16_repository',reader)
assert files == ['rc16_repository/__init__.py'], files
assert source.read_source('repository','rc16_repository',files[0],reader) == code
assert not (pathlib.Path(paths.ROOT_DIR_PATH) / 'rc16_repository').exists()
for file in ('../outside.py','/absolute.py','rc16_repository/link.py'):
    refused(lambda file=file: source.read_source('repository','rc16_repository',file,reader))
refused(lambda: source.source_files('repository','rc16_repository',denied), PermissionError)
refused(lambda: source.source_files('repository','rc16_repository',{}), PermissionError)
# Revoking plugins:read takes effect for the next file selection, without a viewer TTL.
assert c.set_user_attributes(User('rc16-source-reader', instance=c), {'role':'custom','scopes':[]})[0]
refused(lambda: source.read_source('repository','rc16_repository',files[0],reader), PermissionError)
# A small archive that expands past the preview budget is refused before tar metadata is read.
source.MAX_SCAN_BYTES = 1024
refused(lambda: source.source_files('repository','rc16_repository',admin))
'''))
