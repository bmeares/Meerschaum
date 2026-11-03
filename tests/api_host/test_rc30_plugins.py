"""Publish packages current source without importing it; non-admin calls are refused."""

import json

from tests.api_host._helpers import run_python
from tests.api_host.test_rc28_plugins import _json, _run, plugins_dir  # noqa: F401
from tests.api_host._helpers import sandbox  # noqa: F401


def test_publish_archive_uses_current_sources_and_cli_filters(plugins_dir):
    root, plugins = plugins_dir
    package = plugins / 'rc30_package'
    package.mkdir()
    (package / '__init__.py').write_text(
        '"""Package description."""\n__version__ = "2.3.4"\nraise RuntimeError("must not import")\n')
    (package / 'helper.py').write_text('VALUE = 30\n')
    (package / '__pycache__').mkdir()
    (package / '__pycache__' / 'helper.pyc').write_bytes(b'not source')
    (package / '.private').write_text('not for upload')
    (plugins / 'rc30_single.py').write_text('"""Single file."""\n__version__ = "1.0"\nVALUE = 1\n')
    out = _json(run_python(root, plugins, '''
import io, json, pathlib, tarfile
from meerschaum import _core
out = {}
for name in ('rc30_single', 'rc30_package'):
    package = _core.plugin_archive(name)
    with tarfile.open(fileobj=io.BytesIO(package.pop('archive')), mode='r:gz') as archive:
        package['files'] = {member.name: archive.extractfile(member).read().decode()
                            for member in archive if member.isfile()}
    out[name] = package
ok, msg, created = _core.bootstrap_plugin('rc30_native', 'Rust', True, ['fetch'])
assert ok, msg
package = _core.plugin_archive('rc30_native')
with tarfile.open(fileobj=io.BytesIO(package.pop('archive')), mode='r:gz') as archive:
    package['files'] = sorted(archive.getnames())
out['native'] = package
print('JSON:' + json.dumps(out))
'''))
    assert out['rc30_single']['version'] == '1.0'
    assert out['rc30_single']['description'] == 'Single file.'
    assert list(out['rc30_single']['files']) == ['rc30_single.py']
    assert out['rc30_package']['version'] == '2.3.4'
    assert out['rc30_package']['description'] == 'Package description.'
    assert sorted(out['rc30_package']['files']) == ['rc30_package/__init__.py', 'rc30_package/helper.py']
    assert out['rc30_package']['files']['rc30_package/helper.py'] == 'VALUE = 30\n'
    assert out['native']['files'] == ['rc30_native/Cargo.toml', 'rc30_native/plugin.toml',
                                      'rc30_native/src/lib.rs']


def test_publish_refuses_non_admins_and_unconfigured_repositories(plugins_dir):
    root, plugins = plugins_dir
    (plugins / 'rc30_access.py').write_text('VALUE = 30\n')
    out = _json(_run(root, plugins, '''
import json, uuid
from meerschaum.api.dash import plugin_publish as publish
admin = sign_in('rc30-admin', user_type='admin')
viewer = sign_in('rc30-viewer', role='viewer')
context = {'name': 'rc30_access', 'nonce': str(uuid.uuid4())}
print('JSON:' + json.dumps({
    'anonymous': publish.publish(context, 'api:mrsm', {}),
    'viewer': publish.publish(context, 'api:mrsm', viewer),
    'login': publish.login_repository(context, 'api:mrsm', viewer, 'password',
                                      'someone', 'secret', '', '', ''),
    'unknown': publish.publish(context, 'api:notconfigured', admin),
    'traversal': publish.publish({'name': '../rc30_access'}, 'api:mrsm', admin),
    'repositories': publish.repositories(viewer),
}))
'''))
    for key in ('anonymous', 'viewer', 'login'):
        assert out[key] == [False, 'Only administrators can publish installed plugins.']
    assert out['unknown'] == [False, 'Select a configured API repository.']
    assert out['traversal'] == [False, 'This plugin is not installed.']
    assert out['repositories'] == []
