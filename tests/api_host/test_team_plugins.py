#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Teams and the Plugins page, in process on the sandbox's SQLite instance (no server, no browser):
a walled-off viewer sees only their teams' plugins; the members of a team that may manage
plugins (`plugins_write`) install, create, upload, edit, publish and uninstall their team's
plugins and nobody else's; and an upload is validated before a byte is written
(`meerschaum.api.dash.plugin_upload`).
"""

import json
import pathlib
import shutil
import tempfile

import pytest

from tests.api_host._helpers import SIGN_IN, run_python, sandbox  # noqa: F401

#: The host configuration of a server that serves the web terminal.
WEBTERM_ON = {'mrsm_instance': 'sql:apihosttest', 'webterm': True}

AUTHORING_ADMINS = 'Only administrators and team plugin managers can create and edit plugins.'
AUTHORING_OFF = 'Creating and editing plugins is turned off on this server.'
MANAGE_REFUSAL = 'Only administrators and team plugin managers can manage plugins.'


def _json(result) -> dict:
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-6000:]
    line = [l for l in result.stdout.splitlines() if l.startswith('JSON:')][-1]
    return json.loads(line[len('JSON:'):])


@pytest.fixture
def plugins_dir(sandbox):
    """A fresh plugins directory per test (the sandbox's root is shared)."""
    root, _ = sandbox
    plugins = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-team-plugins-', dir=root.parent))
    try:
        yield root, plugins
    finally:
        shutil.rmtree(plugins, ignore_errors=True)


def _run(root, plugins, code, host_config=None, extra_env=None):
    env = {'MRSM_API_HOST_CONFIG': json.dumps(host_config or WEBTERM_ON)}
    env.update(extra_env or {})
    return run_python(root, plugins, SIGN_IN + TEAMS + code, extra_env=env)


#: `team_settings(name, **attributes)` sets a team's attributes (`plugins`, `plugins_write`,
#: `isolated`) on the API's instance; `team_plugins(name)` reads its `plugins` back.
TEAMS = '''
def team_settings(name, **attributes):
    import meerschaum as _mrsm
    from meerschaum.api import get_api_connector as _api_connector
    from meerschaum.api.dash import access as _access
    _team = _mrsm.Team(name, instance=_api_connector())
    _team.attributes.update(attributes)
    _ok, _msg = _team.edit()
    assert _ok, _msg
    _access.forget()

def team_plugins(name):
    import meerschaum as _mrsm
    from meerschaum.api import get_api_connector as _api_connector
    return list(_mrsm.Team(name, instance=_api_connector()).attributes.get('plugins') or [])
'''

#: Builders of the uploads the tests send (`data_url` is how `dcc.Upload` encodes a file).
ARCHIVES = '''
import base64, io, json, os, pathlib, stat, tarfile, zipfile

def zip_of(entries, links=()):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
        for path, data in entries:
            archive.writestr(path, data)
        for path, target in links:
            info = zipfile.ZipInfo(path)
            info.external_attr = (stat.S_IFLNK | 0o777) << 16
            archive.writestr(info, target)
    return buffer.getvalue()

def tar_of(entries, special=()):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode='w:gz') as archive:
        for path, data in entries:
            info = tarfile.TarInfo(path)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
        for path, kind, target in special:
            info = tarfile.TarInfo(path)
            info.type = kind
            info.linkname = target
            archive.addfile(info)
    return buffer.getvalue()

def data_url(data):
    return 'data:application/octet-stream;base64,' + base64.b64encode(data).decode()

def tree(directory):
    directory = pathlib.Path(directory)
    return sorted(
        str(path.relative_to(directory)) for path in directory.rglob('*')
        if path.is_file() or path.is_symlink()
    )
'''

INIT = b'"""An uploaded package."""\\n__version__ = "1.0"\\n'


########################################################################################
# plugin_upload: what an upload is, and what is refused.
########################################################################################

def test_uploads_install_single_files_archives_and_loose_files(plugins_dir):
    """
    One `.py` or `.rs` file, an archive with one top-level directory, an archive of root files, an
    archive of one file, and loose files each install as the plugin they describe: caches and
    `.git` are left out, nothing temporary stays behind, and an existing plugin is replaced only
    when asked.
    """
    root, plugins = plugins_dir
    out = _json(_run(root, plugins, ARCHIVES + f'''
from meerschaum.api.dash import plugin_upload as up
dest = pathlib.Path(os.environ['MRSM_PLUGINS_DIR'])
INIT = {INIT!r}

def install(uploads, name=None, replace=False):
    try:
        plan = up.plan_upload(uploads, name=name)
        path = up.write_plugin(plan, dest, replace=replace)
        return [True, plan['name'], plan['layout'], path.name]
    except up.UploadError as e:
        return [False, str(e)]

out = {{}}
out['py'] = install([('solo.py', b'VALUE = 1\\n')])
out['rs'] = install([('rusty.rs', b'// rust\\n')])
out['named'] = install([('anything.py', b'VALUE = 2\\n')], name='chosen')
out['zip_dir'] = install([('whatever.zip', zip_of([
    ('pkgzip/', b''),
    ('pkgzip/__init__.py', INIT),
    ('pkgzip/sub/mod.py', b'X = 1\\n'),
    ('pkgzip/__pycache__/mod.cpython-313.pyc', b'cache'),
    ('pkgzip/stale.pyc', b'cache'),
    ('pkgzip/.git/config', b'[core]'),
    ('__MACOSX/pkgzip/._mod.py', b'junk'),
]))])
out['tar_root'] = install([('rooted.tar.gz', tar_of([
    ('./__init__.py', INIT), ('./helper.py', b'Y = 2\\n'),
]))])
out['tar_named'] = install([('rooted.tgz', tar_of([
    ('plugin.toml', b'[plugin]\\nname = "native"\\n'), ('src/lib.rs', b'// lib\\n'),
]))], name='native')
out['tar_single'] = install([('bundle.tgz', tar_of([('one.py', b'Z = 3\\n')]))])
out['loose'] = install([('__init__.py', INIT), ('util.py', b'U = 4\\n'), ('stale.pyc', b'c')], name='loose')
out['loose_unnamed'] = install([('__init__.py', INIT), ('util.py', b'U = 4\\n')])
out['tree'] = tree(dest)
out['exists'] = install([('solo.py', b'VALUE = 9\\n')])
out['kept'] = (dest / 'solo.py').read_text()
out['replaced'] = install([('solo.py', b'VALUE = 9\\n')], replace=True)
out['new_text'] = (dest / 'solo.py').read_text()
out['other_layout'] = install([('solo.zip', zip_of([('solo/__init__.py', INIT)]))], replace=True)
out['pkg_exists'] = install([('pkgzip.zip', zip_of([('pkgzip/__init__.py', b'NEW = 1\\n')]))])
out['pkg_replaced'] = install([('pkgzip.zip', zip_of([('pkgzip/__init__.py', b'NEW = 1\\n')]))], replace=True)
out['final'] = tree(dest)
out['hidden'] = sorted(path.name for path in dest.iterdir() if path.name.startswith('.'))
out['suggested'] = [
    up.suggested_name('demo.py'), up.suggested_name(['demo-pkg.tar.gz']), up.suggested_name('x.TGZ'),
    up.suggested_name(['a.py', 'b.py']), up.suggested_name('../x.zip'), up.suggested_name('.py'),
]
out['decoded'] = [name for name, _ in up.decode_uploads(data_url(b'A = 1\\n'), 'a.py')]
out['browser_limit'] = [up.upload_limit_bytes(), up.size_label(up.upload_limit_bytes())]
print('JSON:' + json.dumps(out))
'''))
    assert out['py'] == [True, 'solo', 'file', 'solo.py']
    assert out['rs'] == [True, 'rusty', 'file', 'rusty.rs']
    assert out['named'] == [True, 'chosen', 'file', 'chosen.py']
    assert out['zip_dir'] == [True, 'pkgzip', 'directory', 'pkgzip']
    assert out['tar_root'] == [True, 'rooted', 'directory', 'rooted']
    assert out['tar_named'] == [True, 'native', 'directory', 'native']
    assert out['tar_single'] == [True, 'one', 'file', 'one.py']
    assert out['loose'] == [True, 'loose', 'directory', 'loose']
    assert out['loose_unnamed'] == [False, 'Enter the name of the plugin these files make.']
    assert out['tree'] == [
        'chosen.py', 'loose/__init__.py', 'loose/util.py', 'native/plugin.toml', 'native/src/lib.rs',
        'one.py', 'pkgzip/__init__.py', 'pkgzip/sub/mod.py', 'rooted/__init__.py',
        'rooted/helper.py', 'rusty.rs', 'solo.py',
    ]
    assert out['exists'] == [False, "Plugin 'solo' already exists."]
    assert out['kept'] == 'VALUE = 1\n'
    assert out['replaced'][0] is True and out['new_text'] == 'VALUE = 9\n'
    ### A package replaces the single file of the same name (never both).
    assert out['other_layout'] == [True, 'solo', 'directory', 'solo']
    assert out['pkg_exists'] == [False, "Plugin 'pkgzip' already exists."]
    assert out['pkg_replaced'][0] is True
    assert 'solo.py' not in out['final'] and 'solo/__init__.py' in out['final']
    ### The old package's other files went with it.
    assert 'pkgzip/sub/mod.py' not in out['final'] and 'pkgzip/__init__.py' in out['final']
    assert out['hidden'] == []
    assert out['suggested'] == ['demo', 'demo-pkg', 'x', None, 'x', None]
    assert out['decoded'] == ['a.py']
    ### What the browser is offered fits one proxied request (`api:limits:proxy_bytes`, 16 MiB).
    assert out['browser_limit'] == [(16 * 1024 * 1024 - 64 * 1024) * 3 // 4, '11 MiB']


def test_uploads_refuse_unsafe_archives_names_and_sizes(plugins_dir):
    """
    Path traversal, absolute paths, links, devices, oversized and overfull uploads, unsafe names
    and unrecognized layouts are refused with a reason, and nothing is written anywhere.
    """
    root, plugins = plugins_dir
    out = _json(_run(root, plugins, ARCHIVES + f'''
from meerschaum.api.dash import plugin_upload as up
dest = pathlib.Path(os.environ['MRSM_PLUGINS_DIR'])
INIT = {INIT!r}

def refuse(uploads, name=None, **kwargs):
    try:
        plan = up.plan_upload(uploads, name=name, **kwargs)
        up.write_plugin(plan, dest)
        return [True, plan['name']]
    except up.UploadError as e:
        return [False, str(e)]

good = ('pkg/__init__.py', INIT)
out = {{
    'zip_traversal': refuse([('a.zip', zip_of([good, ('pkg/../../evil.py', b'x')]))]),
    'zip_parent': refuse([('a.zip', zip_of([('../evil.py', b'x')]))]),
    'zip_absolute': refuse([('a.zip', zip_of([good, ('/abs/evil.py', b'x')]))]),
    'zip_backslash': refuse([('a.zip', zip_of([good, ('pkg\\\\..\\\\evil.py', b'x')]))]),
    'zip_drive': refuse([('a.zip', zip_of([good, ('C:/evil.py', b'x')]))]),
    'zip_symlink': refuse([('a.zip', zip_of([good], links=[('pkg/link.py', '/etc/passwd')]))]),
    'tar_traversal': refuse([('a.tar.gz', tar_of([good, ('pkg/../../evil.py', b'x')]))]),
    'tar_absolute': refuse([('a.tar.gz', tar_of([good, ('/abs/evil.py', b'x')]))]),
    'tar_symlink': refuse([('a.tar.gz', tar_of([good], special=[('pkg/link.py', tarfile.SYMTYPE, '/etc/passwd')]))]),
    'tar_hardlink': refuse([('a.tgz', tar_of([good], special=[('pkg/hard.py', tarfile.LNKTYPE, 'pkg/__init__.py')]))]),
    'tar_device': refuse([('a.tgz', tar_of([good], special=[('pkg/dev', tarfile.CHRTYPE, '')]))]),
    'tar_fifo': refuse([('a.tgz', tar_of([good], special=[('pkg/fifo', tarfile.FIFOTYPE, '')]))]),
    'not_gzip': refuse([('a.tar.gz', b'not an archive')]),
    'not_zip': refuse([('a.zip', b'not an archive')]),
    'too_big': refuse([('big.py', b'x' * 4096)], limit=1024),
    'zip_bomb': refuse([('a.zip', zip_of([good, ('pkg/zeros.bin', bytes(1024 * 1024))]))], limit=64 * 1024),
    'tar_bomb': refuse([('a.tgz', tar_of([good, ('pkg/zeros.bin', bytes(1024 * 1024))]))], limit=64 * 1024),
    'duplicate': refuse([('a.tgz', tar_of([good, good]))]),
    'file_and_dir': refuse([('a.tgz', tar_of([good, ('pkg/x', b'1'), ('pkg/x/y.py', b'2')]))]),
    'no_entry_point': refuse([('a.zip', zip_of([('pkg/readme.md', b'hi')]))]),
    'empty_archive': refuse([('a.zip', zip_of([('pkg/__pycache__/x.pyc', b'c')]))]),
    'archive_and_file': refuse([('a.zip', zip_of([good])), ('b.py', b'x')]),
    'two_archives': refuse([('a.zip', zip_of([good])), ('b.zip', zip_of([good]))]),
    'unknown_single': refuse([('notes.txt', b'hi')]),
    'nothing': refuse([]),
    'loose_path': refuse([('sub/__init__.py', INIT), ('b.py', b'x')], name='loose'),
    'loose_parent': refuse([('../__init__.py', INIT), ('b.py', b'x')], name='loose'),
    'name_traversal': refuse([('ok.py', b'x')], name='../escape'),
    'name_slash': refuse([('ok.py', b'x')], name='a/b'),
    'name_reserved': refuse([('ok.py', b'x')], name='mrsm'),
    'name_hidden': refuse([('ok.py', b'x')], name='_hidden'),
    'name_from_dir': refuse([('a.zip', zip_of([('bad name/__init__.py', INIT)]))]),
    'rust_name': refuse([('2nd.rs', b'// x')]),
}}
up.MAX_UPLOAD_FILES = 3
out['too_many'] = refuse([('a.zip', zip_of([good] + [(f'pkg/f{{i}}.py', b'x') for i in range(5)]))])
out['too_many_loose'] = refuse([(f'f{{i}}.py', b'x') for i in range(5)], name='many')
out['decode_big'] = None
try:
    up.decode_uploads([data_url(b'x' * 4096)], ['big.py'], limit=1024)
except up.UploadError as e:
    out['decode_big'] = str(e)
out['decode_bad'] = []
for contents, names in (('nonsense', 'a.py'), ('data:x;base64,@@@', 'a.py'), (None, None),
                        ([data_url(b'x')], ['a.py', 'b.py'])):
    try:
        up.decode_uploads(contents, names)
        out['decode_bad'].append(None)
    except up.UploadError as e:
        out['decode_bad'].append(str(e))
### Writing checks the name and the destination again, whatever the plan says.
out['write'] = []
for plan, directory in (
    ({{'name': '../escape', 'layout': 'file', 'suffix': '.py', 'files': {{'x.py': b'x'}}}}, dest),
    ({{'name': 'pkg', 'layout': 'directory', 'suffix': '', 'files': {{'../evil.py': b'x', '__init__.py': b''}}}}, dest),
    ({{'name': 'pkg', 'layout': 'directory', 'suffix': '', 'files': {{'/abs/evil.py': b'x'}}}}, dest),
    ({{'name': 'pkg', 'layout': 'file', 'suffix': '.py', 'files': {{'pkg.py': b'x'}}}}, dest / 'missing'),
):
    try:
        up.write_plugin(plan, directory)
        out['write'].append(None)
    except up.UploadError as e:
        out['write'].append(str(e))
(dest / 'linked.py').symlink_to(dest.parent / 'elsewhere.py')
try:
    up.write_plugin(up.plan_upload([('linked.py', b'x')]), dest, replace=True)
    out['linked'] = None
except up.UploadError as e:
    out['linked'] = str(e)
(dest / 'linked.py').unlink()
out['tree'] = tree(dest)
out['hidden'] = sorted(path.name for path in dest.iterdir())
out['outside'] = sorted(
    path.name for path in dest.parent.iterdir() if path.name in ('evil.py', 'escape.py', 'elsewhere.py')
) + [str(path) for path in (pathlib.Path('/abs'), pathlib.Path('/abs/evil.py')) if path.exists()]
print('JSON:' + json.dumps(out))
'''))
    refused = {key: value for key, value in out.items() if isinstance(value, list) and len(value) == 2
               and isinstance(value[0], bool)}
    assert len(refused) >= 35
    for key, value in refused.items():
        assert value[0] is False, (key, value)
    for key in ('zip_traversal', 'zip_parent', 'tar_traversal', 'zip_backslash', 'loose_parent'):
        assert 'Invalid path in the upload' in out[key][1], (key, out[key])
    for key in ('zip_absolute', 'tar_absolute', 'zip_drive'):
        assert 'Absolute paths are not allowed' in out[key][1], (key, out[key])
    for key in ('zip_symlink', 'tar_symlink', 'tar_hardlink', 'tar_device', 'tar_fifo'):
        assert 'is a link or a special file' in out[key][1], (key, out[key])
    assert out['too_big'] == [False, 'Uploads are limited to 1 KiB.']
    assert out['zip_bomb'] == [False, 'Uploads are limited to 64 KiB once unpacked.']
    assert out['tar_bomb'] == [False, 'Uploads are limited to 64 KiB once unpacked.']
    assert out['too_many'] == [False, 'Uploads are limited to 3 files.']
    assert out['too_many_loose'] == [False, 'Uploads are limited to 3 files.']
    assert 'more than once' in out['duplicate'][1]
    assert 'both a file and a directory' in out['file_and_dir'][1]
    assert '__init__.py' in out['no_entry_point'][1] and 'plugin.toml' in out['no_entry_point'][1]
    assert out['empty_archive'] == [False, 'This archive holds no files to install.']
    assert 'one archive on its own' in out['archive_and_file'][1]
    assert 'one archive on its own' in out['two_archives'][1]
    assert out['nothing'] == [False, 'Choose the files to upload.']
    assert 'Invalid file name' in out['loose_path'][1]
    for key in ('name_traversal', 'name_slash', 'name_reserved', 'name_hidden', 'name_from_dir'):
        assert 'Invalid plugin name' in out[key][1], (key, out[key])
    assert 'must start with a letter' in out['rust_name'][1]
    assert out['decode_big'] == 'Uploads are limited to 1 KiB.'
    assert all(out['decode_bad']), out['decode_bad']
    assert all(out['write']), out['write']
    assert 'Invalid plugin name' in out['write'][0]
    assert 'Invalid path' in out['write'][1] and 'Absolute paths' in out['write'][2]
    assert 'is a link' in out['linked']
    ### Nothing was written, inside or outside the plugins directory, and nothing was left over.
    assert out['tree'] == [] and out['hidden'] == [] and out['outside'] == []


########################################################################################
# Who sees and manages which plugins.
########################################################################################

def test_policy_matrix_for_administrators_team_managers_and_members(plugins_dir):
    """
    `may_manage_plugins`, `may_manage_plugin`, `can_see_plugin` and `plugin_action_refusal` for an
    administrator, a team's plugin manager (their own and a foreign plugin), a plain member, a
    walled-off member and a visitor.
    """
    root, plugins = plugins_dir
    out = _json(_run(root, plugins, '''
import json
from meerschaum.api.dash import access, plugins as mp
people = {
    'admin': access.make_viewer('admin', admin=True),
    'writer': access.make_viewer('writer', role='editor', teams={'builders': 'member'},
                                 grants={'plugins': ['mine'], 'plugins_write': True}),
    'member': access.make_viewer('member', role='editor', teams={'readers': 'member'},
                                 grants={'plugins': ['mine']}),
    'walled': access.make_viewer('walled', role='editor', teams={'island': 'member'},
                                 grants={'isolated': True, 'plugins': ['mine']}),
    'walled_writer': access.make_viewer('walled-writer', role='editor', teams={'island': 'member'},
                                        grants={'isolated': True, 'plugins': ['mine'], 'plugins_write': True}),
    'no_read': access.make_viewer('no-read', permissions=['pipes:read'], teams={'builders': 'member'},
                                  grants={'plugins': ['mine'], 'plugins_write': True}),
    'anonymous': access.ANONYMOUS,
}
installed = {'mine', 'foreign'}
out = {}
for key, who in people.items():
    out[key] = {
        'manage_any': mp.may_manage_plugins(who),
        'manage_mine': mp.may_manage_plugin(who, 'mine'),
        'manage_foreign': mp.may_manage_plugin(who, 'foreign'),
        'see': [name for name in ('mine', 'foreign', 'new') if mp.can_see_plugin(who, name)],
        'visible': mp.visible_plugin_names(who, ['foreign', 'mine', 'new']),
        'install_card': [name for name in ('mine', 'foreign', 'new')
                         if mp.may_install_plugin(who, name, installed)],
        'actions': {
            f'{action} {name}': mp.plugin_action_refusal(who, action, name, installed)
            for action in ('install', 'reinstall', 'update', 'uninstall')
            for name in ('mine', 'foreign', 'new')
        },
    }
print('JSON:' + json.dumps(out))
'''))
    admin = out['admin']
    assert admin['manage_any'] and admin['manage_mine'] and admin['manage_foreign']
    assert admin['see'] == ['mine', 'foreign', 'new']
    assert set(admin['actions'].values()) == {None}

    writer = out['writer']
    assert writer['manage_any'] and writer['manage_mine'] and not writer['manage_foreign']
    assert writer['see'] == ['mine', 'foreign', 'new']
    assert writer['install_card'] == ['mine', 'new']
    foreign = "Plugin 'foreign' is already installed and does not belong to your team."
    for action in ('install', 'reinstall', 'update', 'uninstall'):
        assert writer['actions'][f'{action} mine'] is None, action
        assert writer['actions'][f'{action} foreign'] == foreign, action
    assert writer['actions']['install new'] is None
    for action in ('reinstall', 'update', 'uninstall'):
        assert writer['actions'][f'{action} new'] == "Plugin 'new' does not belong to your team."

    for key in ('member', 'walled', 'no_read', 'anonymous'):
        person = out[key]
        assert not person['manage_any'] and not person['manage_mine'] and not person['manage_foreign'], key
        assert person['install_card'] == [], key
        assert set(person['actions'].values()) == {MANAGE_REFUSAL}, key
    assert out['member']['see'] == ['mine', 'foreign', 'new']
    assert out['walled']['see'] == ['mine'] and out['walled']['visible'] == ['mine']
    assert out['anonymous']['see'] == []

    walled = out['walled_writer']
    assert walled['manage_mine'] and not walled['manage_foreign']
    assert walled['see'] == ['mine'] and walled['install_card'] == ['mine']
    ### What they cannot see is not told apart from what does not exist.
    for name in ('foreign', 'new'):
        assert walled['actions'][f'install {name}'] == f"Plugin '{name}' is not available."


def test_walled_off_viewers_see_only_their_teams_plugins(plugins_dir):
    """
    The Installed tab, the Repository tab (its cards and its counts), both detail pages, the
    update check and the source view list only the plugins a walled-off viewer's teams hold; a
    hidden plugin reads exactly as a missing one.
    """
    root, plugins = plugins_dir
    for name in ('mine', 'foreign', 'other'):
        (plugins / f'{name}.py').write_text(f'"""{name}."""\n__version__ = "1.0"\n')
    out = _json(_run(root, plugins, '''
import json
from meerschaum.api.dash import access, plugins as mp, plugin_sources as ps, plugin_publish as pub

class Repository:
    """The repository's reads the cards make (three published plugins)."""
    names = ['foreign', 'mine', 'other']
    def get_plugins(self, search_term=None, **kwargs):
        return [name for name in self.names if not search_term or search_term in name]
    def get_plugin_attributes(self, plugin, **kwargs):
        return {'description': 'About ' + plugin.name}
    def get_plugin_username(self, plugin, **kwargs):
        return 'publisher'
    def get_plugin_version(self, plugin, **kwargs):
        return '1.0'

mp.get_api_connector = lambda: Repository()
admin = access.make_viewer('admin', admin=True)
member = access.make_viewer('member', role='editor', teams={'readers': 'member'})
walled = access.make_viewer('walled', role='editor', teams={'island': 'member'},
                            grants={'isolated': True, 'plugins': ['mine']})
writer = access.make_viewer('writer', role='editor', teams={'builders': 'member'},
                            grants={'plugins': ['mine'], 'plugins_write': True})

def rows(component):
    text = json.dumps(component.to_plotly_json(), default=lambda o: o.to_plotly_json())
    return {
        'rows': [name for name in ('mine', 'foreign', 'other')
                 if f'"index": "{name}", "type": "installed-plugin-row"' in text
                 or f'"type": "installed-plugin-row", "index": "{name}"' in text],
        'links': [name for name in ('mine', 'foreign', 'other') if f'/dash/plugins/installed/{name}"' in text],
        'actions': text.count('"plugin-action"'),
        'check': '"plugins-update-check"' in text,
    }

def cards(who, search=None):
    built, _, pages, total = mp.get_plugins_cards(search_term=search, session_data={}, viewer=who)
    text = json.dumps([card.to_plotly_json() for card in built], default=lambda o: o.to_plotly_json())
    return {
        'ids': [card.id for card in built], 'pages': pages, 'total': total,
        'install': [name for name in ('mine', 'foreign', 'other')
                    if f'"index": "{name}"' in text and '"plugin-action"' in text
                    and any(f'"action": "{action}", "index": "{name}"' in text
                            or f'"index": "{name}", "action": "{action}"' in text
                            for action in ('install', 'reinstall'))],
    }

def detail(who, name):
    text = json.dumps(mp.build_plugin_detail(name, {}, viewer=who).to_plotly_json(),
                      default=lambda o: o.to_plotly_json())
    return '404' in text

ps.source_viewer = pub.source_viewer = lambda session: {
    'walled': walled, 'writer': writer, 'admin': admin,
}.get((session or {}).get('who'), access.ANONYMOUS)

def installed_page(who, name):
    text = json.dumps(mp.build_installed_plugin_detail(name, {'who': who}).to_plotly_json(),
                      default=lambda o: o.to_plotly_json())
    return ('missing' if 'This plugin is not installed.' in text
            else 'private' if 'Installed source is available' in text else 'shown')

def source(who, kind, name):
    try:
        return ps.source_files(kind, name, {'who': who})
    except PermissionError as e:
        return 'PermissionError: ' + str(e)
    except ValueError as e:
        return 'ValueError: ' + str(e)

mp.remote_plugin_version = lambda name, origin: '9.0'
real_installed = mp.get_installed_plugins
mp.get_installed_plugins = lambda check_packages=True: [
    {**plugin, 'origin': 'api:somewhere', 'version': '1.0'}
    for plugin in real_installed(check_packages=check_packages)
]
out = {
    'installed': {key: rows(mp.build_installed_plugins_tab(viewer=who))
                  for key, who in (('admin', admin), ('member', member), ('walled', walled), ('writer', writer))},
    'cards': {key: cards(who)
              for key, who in (('admin', admin), ('member', member), ('walled', walled), ('writer', writer))},
    'search_hidden': cards(walled, 'foreign'),
    'search_own': cards(walled, 'min'),
    'not_found': {name: detail(walled, name) for name in ('mine', 'foreign', 'missing')},
    'not_found_member': {name: detail(member, name) for name in ('mine', 'foreign', 'missing')},
    'installed_page': {
        'walled mine': installed_page('walled', 'mine'),
        'walled foreign': installed_page('walled', 'foreign'),
        'walled missing': installed_page('walled', 'missing'),
        'writer mine': installed_page('writer', 'mine'),
        'writer foreign': installed_page('writer', 'foreign'),
        'writer missing': installed_page('writer', 'missing'),
        'admin foreign': installed_page('admin', 'foreign'),
    },
    'updates': {
        'admin': sorted(mp.plugin_update_versions(['mine', 'foreign'], viewer=admin)),
        'writer': sorted(mp.plugin_update_versions(['mine', 'foreign'], viewer=writer)),
        'walled': sorted(mp.plugin_update_versions(['mine', 'foreign'], viewer=walled)),
    },
    'source': {
        'writer mine': source('writer', 'installed', 'mine'),
        'writer foreign': source('writer', 'installed', 'foreign'),
        'walled mine': source('walled', 'installed', 'mine'),
        'walled repository foreign': source('walled', 'repository', 'foreign'),
        'walled repository missing': source('walled', 'repository', 'missing'),
    },
    'publish': {
        'walled': pub.repositories({'who': 'walled'}, 'mine'),
        'writer foreign': pub.repositories({'who': 'writer'}, 'foreign'),
        'writer foreign publish': pub.publish({'name': 'foreign', 'nonce': 'n'}, 'api:mrsm', {'who': 'writer'}),
        'writer missing publish': pub.publish({'name': 'missing', 'nonce': 'n'}, 'api:mrsm', {'who': 'writer'}),
        'walled publish': pub.publish({'name': 'mine', 'nonce': 'n'}, 'api:mrsm', {'who': 'walled'}),
    },
}
print('JSON:' + json.dumps(out))
'''))
    installed = out['installed']
    assert installed['admin'] == {
        'rows': ['mine', 'foreign', 'other'], 'links': ['mine', 'foreign', 'other'],
        'actions': 6, 'check': True,
    }
    ### As before teams: everything listed, nothing to act on.
    assert installed['member'] == {'rows': ['mine', 'foreign', 'other'], 'links': [], 'actions': 0, 'check': False}
    assert installed['walled'] == {'rows': ['mine'], 'links': [], 'actions': 0, 'check': False}
    ### A team's plugin manager: every plugin listed, their own linked with Update and Uninstall.
    assert installed['writer'] == {
        'rows': ['mine', 'foreign', 'other'], 'links': ['mine'], 'actions': 2, 'check': True,
    }

    cards = out['cards']
    assert cards['admin'] == {'ids': ['foreign_card', 'mine_card', 'other_card'], 'pages': 1, 'total': 3,
                              'install': ['mine', 'foreign', 'other']}
    assert cards['member'] == {'ids': ['foreign_card', 'mine_card', 'other_card'], 'pages': 1, 'total': 3,
                               'install': []}
    assert cards['walled'] == {'ids': ['mine_card'], 'pages': 1, 'total': 1, 'install': []}
    ### Installed plugins of other teams offer the manager no Reinstall.
    assert cards['writer']['total'] == 3 and cards['writer']['install'] == ['mine']
    assert out['search_hidden'] == {'ids': [], 'pages': 1, 'total': 0, 'install': []}
    assert out['search_own']['ids'] == ['mine_card'] and out['search_own']['total'] == 1
    assert out['not_found'] == {'mine': False, 'foreign': True, 'missing': True}
    assert out['not_found_member'] == {'mine': False, 'foreign': False, 'missing': True}

    assert out['installed_page'] == {
        'walled mine': 'private',
        'walled foreign': 'missing',
        'walled missing': 'missing',
        'writer mine': 'shown',
        'writer foreign': 'private',
        'writer missing': 'missing',
        'admin foreign': 'shown',
    }
    assert out['updates'] == {'admin': ['foreign', 'mine'], 'writer': ['mine'], 'walled': []}
    source = out['source']
    assert source['writer mine'] == ['mine.py']
    assert source['writer foreign'].startswith('PermissionError')
    assert source['walled mine'].startswith('PermissionError')
    assert source['walled repository foreign'] == source['walled repository missing'] == (
        'ValueError: This plugin is not in the repository.'
    )
    publish = out['publish']
    assert publish['walled'] == [] and publish['writer foreign'] == []
    assert publish['writer foreign publish'] == publish['writer missing publish'] == [
        False, 'This plugin is not installed.',
    ]
    assert publish['walled publish'] == [
        False, 'Only administrators and team plugin managers can publish installed plugins.',
    ]


def test_authoring_matrix(plugins_dir):
    """
    Creating, uploading and editing: administrators as the web terminal is gated; the members
    of a team that may manage plugins without the terminal (their team's plugins only); nobody
    else; and nobody when the server refuses plugins or runs without authentication.
    """
    root, plugins = plugins_dir
    (plugins / 'foreign.py').write_text('VALUE = 1\n')
    code = '''
import json
from meerschaum.api.dash import plugin_sources as ps
admin = sign_in('tp-auth-admin', user_type='admin')
writer = sign_in('tp-auth-writer', role='editor', teams={'tp-auth-builders': 'member'})
manager = sign_in('tp-auth-manager', role='editor', teams={'tp-auth-readers': 'manager'})
viewer = sign_in('tp-auth-viewer', role='viewer')
team_settings('tp-auth-builders', plugins_write=True, plugins=['mine'])
team_settings('tp-auth-readers', plugins=['mine'])
print('JSON:' + json.dumps({
    'admin': ps.authoring_refusal(admin),
    'writer': ps.authoring_refusal(writer),
    'writer mine': ps.authoring_refusal(writer, 'mine'),
    'writer foreign': ps.authoring_refusal(writer, 'foreign'),
    'admin foreign': ps.authoring_refusal(admin, 'foreign'),
    'manager': ps.authoring_refusal(manager),
    'viewer': ps.authoring_refusal(viewer),
    'anonymous': ps.authoring_refusal({}),
    'may_author': [ps.may_author(writer), ps.may_author(writer, 'foreign'), ps.may_author(manager)],
}))
'''
    served = _json(_run(root, plugins, code))
    assert served['admin'] is None and served['admin foreign'] is None
    assert served['writer'] is None and served['writer mine'] is None
    assert served['writer foreign'] == "Plugin 'foreign' does not belong to your team."
    ### Managing a team is not managing plugins: only the team's `plugins_write` is.
    for who in ('manager', 'viewer', 'anonymous'):
        assert served[who] == AUTHORING_ADMINS, who
    assert served['may_author'] == [True, False, False]

    ### Without the terminal: administrators are gated as it is, the team's switch still stands.
    no_webterm = _json(_run(root, plugins, code, host_config={'mrsm_instance': 'sql:apihosttest'}))
    assert no_webterm['admin'] == AUTHORING_OFF
    assert no_webterm['writer'] is None and no_webterm['writer mine'] is None
    for who in ('manager', 'viewer', 'anonymous'):
        assert no_webterm[who] == AUTHORING_OFF, who

    refused = _json(_run(root, plugins, code, extra_env={
        'MRSM_CONFIG': json.dumps({'api': {'permissions': {'registration': {'plugins': False}}}}),
    }))
    for who in ('admin', 'writer', 'writer mine', 'manager', 'viewer', 'anonymous'):
        assert refused[who] == AUTHORING_OFF, who

    secure = _json(_run(root, plugins, code, extra_env={
        'MRSM_CONFIG': json.dumps({'api': {'permissions': {'actions': {'non_admin': False}}}}),
    }))
    assert secure['admin'] is None
    assert secure['writer'] == AUTHORING_ADMINS and secure['writer mine'] == AUTHORING_ADMINS

    no_auth = _json(_run(root, plugins, code, host_config={
        'mrsm_instance': 'sql:apihosttest', 'no_auth': True, 'webterm': False,
    }))
    for who in ('admin', 'writer', 'viewer', 'anonymous'):
        assert no_auth[who] == AUTHORING_OFF, who


def test_team_managers_create_upload_edit_and_claim_their_plugins(plugins_dir):
    """
    A team's plugin manager creates and uploads plugins, which become their team's (recorded in
    its `plugins`, read fresh), edits them, and is refused another team's or an administrator's
    plugin by name; a finished install is recorded from the job's own record, and an uninstall
    takes the plugin off the list.
    """
    root, plugins = plugins_dir
    (plugins / 'foreign.py').write_text('VALUE = 1\n')
    out = _json(_run(root, plugins, ARCHIVES + '''
from types import SimpleNamespace
from meerschaum.api.dash import plugins as mp, plugin_sources as ps, plugin_upload as up
dest = pathlib.Path(os.environ['MRSM_PLUGINS_DIR'])
admin = sign_in('tp-admin', user_type='admin')
writer = sign_in('tp-writer', role='editor', teams={'tp-builders': 'member', 'tp-aaa': 'member'})
other = sign_in('tp-other', role='editor', teams={'tp-rivals': 'member'})
member = sign_in('tp-member', role='editor', teams={'tp-readers': 'member'})
team_settings('tp-builders', plugins_write=True)
team_settings('tp-rivals', plugins_write=True)
out = {'writing_team': mp.writing_team(ps.source_viewer(writer))}

def upload(session, files, name=None, replace=False, directory=None):
    contents = [data_url(data) for _, data in files]
    return list(up.upload_plugin(contents, [filename for filename, _ in files], name, directory, replace, session))

out['create'] = mp.create_new_plugin('made', 'Python', 'file', ['fetch'], '', 'ts', None, writer)
out['create_foreign'] = mp.create_new_plugin('foreign', 'Python', 'file', ['fetch'], '', 'ts', None, writer)
out['create_member'] = mp.create_new_plugin('nope', 'Python', 'file', ['fetch'], '', 'ts', None, member)
out['upload'] = upload(writer, [('sent.py', b'VALUE = 1\\n')])
out['upload_pkg'] = upload(writer, [('pkg.zip', zip_of([('pkg/__init__.py', b'A = 1\\n')]))], name='sentpkg')
out['upload_foreign'] = upload(writer, [('foreign.py', b'VALUE = 2\\n')], replace=True)
out['upload_rival'] = upload(other, [('sent.py', b'VALUE = 3\\n')], replace=True)
out['upload_member'] = upload(member, [('m.py', b'VALUE = 1\\n')])
out['upload_anonymous'] = upload({}, [('a.py', b'VALUE = 1\\n')])
out['upload_again'] = upload(writer, [('sent.py', b'VALUE = 4\\n')])
out['upload_replace'] = upload(writer, [('sent.py', b'VALUE = 4\\n')], replace=True)
out['upload_truthy'] = upload(writer, [('sent.py', b'VALUE = 5\\n')], replace='yes')
out['upload_elsewhere'] = upload(writer, [('away.py', b'VALUE = 1\\n')], directory=str(dest.parent))
out['upload_unsafe'] = upload(writer, [('a.zip', zip_of([('pkg/__init__.py', b''), ('pkg/../../evil.py', b'x')]))])
out['upload_admin'] = upload(admin, [('byadmin.py', b'VALUE = 1\\n')])
out['admin_replace_foreign'] = upload(admin, [('foreign.py', b'VALUE = 7\\n')], replace=True)
out['team'] = team_plugins('tp-builders')
out['other_team'] = team_plugins('tp-rivals')
out['sent'] = (dest / 'sent.py').read_text()
out['foreign'] = (dest / 'foreign.py').read_text()
out['tree'] = tree(dest)

text, digest = ps.read_source_with_digest('installed', 'made', 'made.py', writer)
out['save'] = ps.save_source('made', 'made.py', text + '# mine\\n', digest, writer)[:2]
out['save_foreign'] = ps.save_source('foreign', 'foreign.py', 'X = 1\\n', ps.source_digest(b'VALUE = 7\\n'), writer)[:2]
out['save_rival'] = ps.save_source('made', 'made.py', 'X = 1\\n', ps.source_digest((text + '# mine\\n').encode()), other)[:2]
out['read_rival'] = ps.may_read_source('installed', other, 'made')
out['read_own'] = ps.may_read_source('installed', writer, 'made')
out['read_unnamed'] = ps.may_read_source('installed', writer)

### The record of a run: read from the job, for its owner only.
def job(owner, plugin, action, sysargs, origin='console_plugins'):
    return SimpleNamespace(
        daemon=SimpleNamespace(properties={'mrsm_execution': {
            'origin': origin, 'owner': owner, 'plugin': plugin, 'action': action}}),
        sysargs=sysargs,
    )
install = ['install', 'plugins', 'fromrepo', '-r', 'api:this_server']
out['records'] = {
    'own': mp.plugin_run_record(job('tp-writer', 'fromrepo', 'install', install), 'tp-writer'),
    'someone_else': mp.plugin_run_record(job('tp-other', 'fromrepo', 'install', install), 'tp-writer'),
    'other_plugin': mp.plugin_run_record(job('tp-writer', 'foreign', 'install', install), 'tp-writer'),
    'other_action': mp.plugin_run_record(job('tp-writer', 'fromrepo', 'uninstall', install), 'tp-writer'),
    'other_origin': mp.plugin_run_record(job('tp-writer', 'fromrepo', 'install', install, 'console'), 'tp-writer'),
    'unsafe': mp.plugin_run_record(job('tp-writer', '../x', 'install', ['install', 'plugins', '../x']), 'tp-writer'),
    'uninstall': mp.plugin_run_record(
        job('tp-writer', 'made', 'uninstall', ['uninstall', 'plugins', 'made', '--yes']), 'tp-writer'),
}
who = ps.source_viewer(writer)
out['claim'] = mp.claim_plugin(who, 'fromrepo')
out['claim_twice'] = mp.claim_plugin(ps.source_viewer(writer), 'fromrepo')
out['claim_member'] = mp.claim_plugin(ps.source_viewer(member), 'fromrepo')
out['claim_unsafe'] = mp.claim_plugin(ps.source_viewer(writer), '../x')
out['claim_admin'] = mp.claim_plugin(ps.source_viewer(admin), 'byadmin')
out['claimed'] = team_plugins('tp-builders')
out['manage_after'] = mp.may_manage_plugin(ps.source_viewer(writer), 'fromrepo')
out['release'] = mp.release_plugin(ps.source_viewer(writer), 'fromrepo')
out['release_member'] = mp.release_plugin(ps.source_viewer(member), 'made')
out['released'] = team_plugins('tp-builders')
team_settings('tp-builders', plugins_write=False)
out['after_switch_off'] = {
    'manage': mp.may_manage_plugin(ps.source_viewer(writer), 'made'),
    'authoring': ps.authoring_refusal(writer),
    'upload': upload(writer, [('late.py', b'VALUE = 1\\n')]),
    'claim': mp.claim_plugin(ps.source_viewer(writer), 'late'),
}
print('JSON:' + json.dumps(out))
'''))
    ### Of their two teams, the one that may manage plugins is the one recorded for.
    assert out['writing_team'] == 'tp-builders'
    assert out['create'][0] is True and out['create'][2].startswith('/dash/plugins/installed/made?')
    foreign = "Plugin 'foreign' is already installed and does not belong to your team."
    assert out['create_foreign'] == [False, foreign, None]
    assert out['create_member'] == [False, AUTHORING_ADMINS, None]
    assert out['upload'] == [True, "Uploaded plugin 'sent'.", '/dash/plugins/installed/sent']
    assert out['upload_pkg'] == [True, "Uploaded plugin 'sentpkg'.", '/dash/plugins/installed/sentpkg']
    assert out['upload_foreign'] == [False, foreign, None]
    assert out['upload_rival'] == [
        False, "Plugin 'sent' is already installed and does not belong to your team.", None,
    ]
    assert out['upload_member'] == [False, AUTHORING_ADMINS, None]
    assert out['upload_anonymous'] == [False, AUTHORING_ADMINS, None]
    assert out['upload_again'] == [
        False, "Plugin 'sent' already exists. Tick Replace existing to overwrite it.", None,
    ]
    assert out['upload_replace'][0] is True
    ### Only the checkbox's own `True` replaces.
    assert out['upload_truthy'][0] is False
    assert 'is not one of the plugins directories' in out['upload_elsewhere'][1]
    assert out['upload_unsafe'][0] is False and 'Invalid path' in out['upload_unsafe'][1]
    assert out['upload_admin'][0] is True and out['admin_replace_foreign'][0] is True
    assert out['team'] == ['made', 'sent', 'sentpkg']
    assert out['other_team'] == []
    assert out['sent'] == 'VALUE = 4\n' and out['foreign'] == 'VALUE = 7\n'
    assert out['tree'] == ['byadmin.py', 'foreign.py', 'made.py', 'sent.py', 'sentpkg/__init__.py']

    assert out['save'] == [True, 'Saved made.py.']
    assert out['save_foreign'] == [False, "Plugin 'foreign' does not belong to your team."]
    assert out['save_rival'] == [False, "Plugin 'made' does not belong to your team."]
    assert out['read_own'] is True and out['read_rival'] is False and out['read_unnamed'] is False

    records = out['records']
    assert records['own'] == {'plugin': 'fromrepo', 'action': 'install'}
    assert records['uninstall'] == {'plugin': 'made', 'action': 'uninstall'}
    for key in ('someone_else', 'other_plugin', 'other_action', 'other_origin', 'unsafe'):
        assert records[key] is None, key

    assert out['claim'] == [True, "Plugin 'fromrepo' belongs to team 'tp-builders'."]
    assert out['claim_twice'][0] is True
    assert out['claim_member'] == [False, MANAGE_REFUSAL]
    assert out['claim_unsafe'][0] is False
    assert out['claim_admin'] == [True, '']
    assert out['claimed'] == ['made', 'sent', 'sentpkg', 'fromrepo']
    assert out['manage_after'] is True
    assert out['release'] == [True, '']
    assert out['release_member'] == [False, MANAGE_REFUSAL]
    assert out['released'] == ['made', 'sent', 'sentpkg']
    ### The switch is read fresh: turning it off ends every right at once.
    after = out['after_switch_off']
    assert after['manage'] is False and after['authoring'] == AUTHORING_ADMINS
    assert after['upload'] == [False, AUTHORING_ADMINS, None]
    assert after['claim'] == [False, MANAGE_REFUSAL]
    assert not (plugins / 'late.py').exists()


def _fire_code() -> str:
    return '''
import json
from meerschaum.api.dash import build_dash_app, dash_app
build_dash_app()
dash_app._setup_server()
client = dash_app.server.test_client()
endpoint = next(str(rule) for rule in dash_app.server.url_map.iter_rules()
                if str(rule).endswith('/_dash-update-component'))

def outputs_of(key):
    out = []
    for part in key.strip('.').split('...'):
        ident, prop = part.rsplit('.', 1)
        out.append({'id': ident, 'property': prop})
    return out if key.startswith('..') else out[0]

def fire(changed, values):
    """
    Post the callback that `changed` (`id.property`) triggers, as the browser does: `values` is
    `{'id.property': value}` for its inputs and state.
    """
    ident, prop = changed.rsplit('.', 1)
    key, callback = next(
        (key, value) for key, value in dash_app.callback_map.items()
        if any(item['id'] == ident and item['property'] == prop for item in value['inputs'])
    )
    def items(specs):
        return [{'id': spec['id'], 'property': spec['property'],
                 'value': values.get(f"{spec['id']}.{spec['property']}")} for spec in specs]
    response = client.post(endpoint, json={
        'output': key, 'outputs': outputs_of(key), 'changedPropIds': [changed],
        'inputs': items(callback['inputs']), 'state': items(callback['state']),
    })
    return response.status_code, json.dumps(response.get_json(silent=True) or {})
'''


def test_callbacks_recheck_the_session_for_every_plugin_change(plugins_dir):
    """
    The callbacks themselves: Upload, an install and the Uninstall dialog ask the policy with the
    session's viewer read again, whatever the browser sends.
    """
    root, plugins = plugins_dir
    (plugins / 'foreign.py').write_text('VALUE = 1\n')
    (plugins / 'mine.py').write_text('VALUE = 1\n')
    out = _json(_run(root, plugins, ARCHIVES + _fire_code() + '''
from meerschaum.api.dash import plugins as mp
from meerschaum.api.dash.callbacks import plugins as callbacks
dest = pathlib.Path(os.environ['MRSM_PLUGINS_DIR'])
writer = sign_in('tp-cb-writer', role='editor', teams={'tp-cb-builders': 'member'})
member = sign_in('tp-cb-member', role='editor', teams={'tp-cb-readers': 'member'})
team_settings('tp-cb-builders', plugins_write=True, plugins=['mine'])
started = []
callbacks.start_plugin_run = lambda sysargs, username, env=None, execution=None: (
    started.append([sysargs, username, execution]) or ((True, 'Started.'), 'execute-test0001'))
mp.this_server_env = lambda username=None: {'MRSM_API_THIS_SERVER': 'http://127.0.0.1:1'}

def text(alert):
    return json.dumps(alert.to_plotly_json(), default=lambda o: o.to_plotly_json())

def run(action, name, session):
    alert, run_data, _ = callbacks.run_plugin_action(action, name, session)
    return [text(alert), run_data if isinstance(run_data, dict) else None]

out = {
    'install_new': run('install', 'fresh', writer),
    'install_foreign': run('install', 'foreign', writer),
    'uninstall_foreign': run('uninstall', 'foreign', writer),
    'uninstall_mine': run('uninstall', 'mine', writer),
    'member': run('install', 'fresh', member),
    'anonymous': run('install', 'fresh', {}),
    'unknown': run('purge', 'mine', writer),
    'started': started,
}
def upload(session, replace=False):
    return fire('plugins-new-upload.n_clicks', {
        'plugins-new-upload.n_clicks': 1,
        'plugins-upload-files.contents': [data_url(b'VALUE = 2\\n')],
        'plugins-upload-files.filename': ['cbsent.py'],
        'plugins-new-name.value': 'cbsent',
        'plugins-new-directory.value': None,
        'plugins-upload-replace.value': replace,
        'session-store.data': session,
    })
out['upload_member'] = upload(member)
out['member_wrote'] = (dest / 'cbsent.py').exists()
out['upload_writer'] = upload(writer)
out['writer_wrote'] = (dest / 'cbsent.py').exists()
out['team'] = team_plugins('tp-cb-builders')
out['chosen'] = fire('plugins-upload-files.filename',
                     {'plugins-upload-files.filename': ['demo_pkg.tar.gz']})

### A finished run: the job says whose it was and what it did, the browser only names it.
from types import SimpleNamespace
import meerschaum.api.dash._support as support
import meerschaum.api.dash.jobs as jobs_page
jobs_page.job_visible_to_session = lambda job, session_id: True

def watch(session, owner, plugin, action, said, result=(True, 'Success')):
    verb = 'uninstall' if action == 'uninstall' else 'install'
    job = SimpleNamespace(
        name='execute-test0001', exists=lambda: True, status='stopped', result=result,
        sysargs=[verb, 'plugins', plugin, '--yes'],
        daemon=SimpleNamespace(properties={'mrsm_execution': {
            'origin': 'console_plugins', 'owner': owner, 'plugin': plugin, 'action': action}}),
    )
    support.get_job = lambda name, sysargs=None: job
    status, body = fire('plugins-run-interval.n_intervals', {
        'plugins-run-interval.n_intervals': 1,
        'plugins-run.data': {'job': 'execute-test0001', **said},
        'plugins-refresh.data': 0,
        'session-store.data': session,
    })
    return [status, body, team_plugins('tp-cb-builders')]

out['watch'] = {
    'installed': watch(writer, 'tp-cb-writer', 'fresh', 'install', {'action': 'install', 'plugin': 'fresh'}),
    'forged_plugin': watch(writer, 'tp-cb-writer', 'fresh2', 'install', {'action': 'install', 'plugin': 'foreign'}),
    'someone_elses': watch(writer, 'tp-cb-member', 'theirs', 'install', {'action': 'install', 'plugin': 'theirs'}),
    'failed': watch(writer, 'tp-cb-writer', 'broken', 'install', {'action': 'install', 'plugin': 'broken'},
                    result=(False, 'No.')),
    'member': watch(member, 'tp-cb-member', 'sneaky', 'install', {'action': 'install', 'plugin': 'sneaky'}),
    'uninstalled': watch(writer, 'tp-cb-writer', 'mine', 'uninstall', {'action': 'install', 'plugin': 'mine'}),
}
print('JSON:' + json.dumps(out))
'''))
    assert "Installing plugin 'fresh'" in out['install_new'][0]
    assert out['install_new'][1] == {'job': 'execute-test0001', 'action': 'install', 'plugin': 'fresh'}
    assert 'already installed and does not belong to your team' in out['install_foreign'][0]
    assert 'already installed and does not belong to your team' in out['uninstall_foreign'][0]
    assert out['uninstall_mine'][1] == {'job': 'execute-test0001', 'action': 'uninstall', 'plugin': 'mine'}
    assert MANAGE_REFUSAL in out['member'][0] and out['member'][1] is None
    assert 'Sign in to manage plugins.' in out['anonymous'][0]
    assert 'Unknown plugin action' in out['unknown'][0]
    ### Only the two allowed runs started, each recording its plugin and action on the job.
    assert [(run[0][:3], run[1], run[2]) for run in out['started']] == [
        (['install', 'plugins', 'fresh'], 'tp-cb-writer', {'plugin': 'fresh', 'action': 'install'}),
        (['uninstall', 'plugins', 'mine'], 'tp-cb-writer', {'plugin': 'mine', 'action': 'uninstall'}),
    ]
    status, body = out['upload_member']
    assert status == 200 and AUTHORING_ADMINS in body and out['member_wrote'] is False
    status, body = out['upload_writer']
    assert status == 200 and '/dash/plugins/installed/cbsent' in body and out['writer_wrote'] is True
    assert out['team'] == ['mine', 'cbsent']
    status, body = out['chosen']
    assert status == 200 and 'demo_pkg.tar.gz' in body and '"demo_pkg"' in body

    watch = out['watch']
    status, body, team = watch['installed']
    assert status == 200 and "Installed plugin 'fresh'." in body
    assert team == ['mine', 'cbsent', 'fresh']
    ### The plugin recorded is the job's, not the one the browser names.
    status, body, team = watch['forged_plugin']
    assert "Installed plugin 'fresh2'." in body and team == ['mine', 'cbsent', 'fresh', 'fresh2']
    ### Someone else's run, a failed run and a member's forged run record nothing.
    for key in ('someone_elses', 'failed'):
        status, body, team = watch[key]
        assert status == 200 and 'Failed to install plugin' in body, (key, body)
        assert team == ['mine', 'cbsent', 'fresh', 'fresh2'], key
    assert watch['member'][2] == ['mine', 'cbsent', 'fresh', 'fresh2']
    assert 'sneaky' not in watch['member'][1]
    status, body, team = watch['uninstalled']
    assert "Uninstalled plugin 'mine'." in body and team == ['cbsent', 'fresh', 'fresh2']
